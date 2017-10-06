//! # `context` - manage a valid repository.
//!
//! `Context` is the main point of entry for the Attaca API. Important pieces of functionality in
//! this module include:
//!
//! * Creating/using `Context` and `RemoteContext`s.

pub mod local;
pub mod remote;

use std::sync::{Arc, Mutex};

use futures::prelude::*;
use futures_cpupool::CpuPool;

use WRITE_FUTURE_BUFFER_SIZE;
use batch::Batch;
use catalog::Registry;
use errors::*;
use marshal::{ObjectHash, Object, Hashed};
use repository::{Repository, RemoteCfg};
use trace::{Trace, WriteDestination, WriteTrace};

pub use context::local::Local;
pub use context::remote::Remote;


pub trait Store: Send + Sync + Clone + 'static {
    type Read: Future<Item = Object, Error = Error> + Send;
    type Write: Future<Item = bool, Error = Error> + Send;

    fn read_object(&mut self, object_hash: ObjectHash) -> Self::Read;
    fn write_object(&mut self, hashed: Hashed) -> Self::Write;
}


/// A context for marshalling and local operations on a repository. `RemoteContext`s must be built
/// from a `Context`.
///
/// `Context` may optionally be supplied with a type `T` implementing `Trace`. This "trace object"
/// is useful for doing things like tracking the progress of long-running operations.
#[derive(Debug)]
pub struct Context<T: Trace, S: Store> {
    trace: T,
    store: S,

    io_pool: CpuPool,
}


impl<T: Trace, S: Store> Context<T, S> {
    /// Create a context from a loaded repository, with a supplied trace object.
    pub fn new(trace: T, store: S, repository: Repository) -> Self {
        Self {
            trace,
            store,

            io_pool: CpuPool::new(1),
        }
    }

    pub fn write_batch(
        &mut self,
        batch: Batch<T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error>> {
        let trace = Arc::new(Mutex::new(
            self.trace.on_write(&batch, WriteDestination::Local),
        ));

        let io_pool = self.io_pool.clone();
        let mut store = self.store.clone();

        let writes = batch
            .into_stream()
            .map(move |hashed| {
                let hash = *hashed.as_hash();
                let trace = trace.clone();

                trace.lock().unwrap().on_begin(&hash);
                let write = store.write_object(hashed).map(move |fresh| {
                    trace.lock().unwrap().on_complete(&hash, fresh);
                });
                io_pool.spawn(write)
            })
            .buffer_unordered(WRITE_FUTURE_BUFFER_SIZE)
            .for_each(|_| Ok(()));

        Box::new(self.io_pool.spawn(writes))
    }
}


// pub struct LocalContext<'a, T: Trace = ()> {
//     ctx: &'a mut Context<T>,
//     local: Local,
// }
//
//
// impl<'a, T: Trace> LocalContext<'a, T> {
//     /// Read a single object from the local repository.
//     pub fn read_object(
//         &mut self,
//         object_hash: ObjectHash,
//     ) -> Box<Future<Item = Object, Error = Error>> {
//         self.local.read_object(object_hash)
//     }
//
//     /// Write a fully marshalled batch to the local repository.
//     pub fn write_batch(
//         &mut self,
//         batch: Batch<T::BatchTrace>,
//     ) -> Box<Future<Item = (), Error = Error>> {
//         unimplemented!()
//         // self.local.write_batch(batch)
//     }
// }
//
//
// /// A `RemoteContext` is a context for dealing with a specific remote of this repository.
// // TODO: Abstract away the backend so that other K/V stores than Ceph/RADOS may be used.
// pub struct RemoteContext<'a, T: Trace> {
//     ctx: &'a mut Context<T>,
//     name: String,
//     remote: Remote,
// }
//
//
// impl<'a, T: Trace> RemoteContext<'a, T> {
//     pub fn read_object(
//         &mut self,
//         object_hash: ObjectHash,
//     ) -> Box<Future<Item = Object, Error = Error>> {
//         self.remote.read_object(object_hash)
//     }
//
//     /// Write a fully marshalled batch to the remote repository.
//     // TODO: Recover from errors when sending objects.
//     pub fn write_batch(
//         &mut self,
//         batch: Batch<T::BatchTrace>,
//     ) -> Box<Future<Item = (), Error = Error>> {
//         let trace = Arc::new(Mutex::new(self.ctx.trace.on_write(
//             &batch,
//             WriteDestination::Remote(&self.name),
//         )));
//
//         let remote = self.remote.clone();
//
//         let result = batch
//             .into_stream()
//             .map(move |hashed| {
//                 let hash = *hashed.as_hash();
//                 let trace = trace.clone();
//
//                 trace.lock().unwrap().on_begin(&hash);
//                 remote.write_object(hashed).map(move |fresh| {
//                     trace.lock().unwrap().on_complete(&hash, fresh);
//                 })
//             })
//             .buffer_unordered(WRITE_FUTURE_BUFFER_SIZE)
//             .for_each(|_| Ok(()));
//
//         Box::new(self.ctx.io_pool.spawn(result))
//     }
// }
