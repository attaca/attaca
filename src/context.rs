//! # `context` - manage a valid repository.
//!
//! `Context` is the main point of entry for the Attaca API. Important pieces of functionality in
//! this module include:
//!
//! * Creating/using `Context` and `RemoteContext`s.

use std::sync::{Arc, Mutex};

use futures::prelude::*;
use futures_cpupool::CpuPool;

use WRITE_FUTURE_BUFFER_SIZE;
use batch::Batch;
use catalog::Registry;
use errors::*;
use local::Local;
use marshal::{ObjectHash, Object};
use remote::Remote;
use repository::{Repository, RemoteCfg};
use trace::{Trace, WriteDestination, WriteTrace};


/// A context for marshalling and local operations on a repository. `RemoteContext`s must be built
/// from a `Context`.
///
/// `Context` may optionally be supplied with a type `T` implementing `Trace`. This "trace object"
/// is useful for doing things like tracking the progress of long-running operations.
#[derive(Debug)]
pub struct Context<T: Trace = ()> {
    trace: T,

    marshal_pool: CpuPool,
    io_pool: CpuPool,

    repository: Repository,
}


impl Context {
    /// Create a context from a loaded repository.
    pub fn new(repository: Repository) -> Self {
        Self::with_trace(repository, ())
    }
}


impl<T: Trace> Context<T> {
    /// Create a context from a loaded repository, with a supplied trace object.
    pub fn with_trace(repository: Repository, trace: T) -> Self {
        Context {
            trace,

            marshal_pool: CpuPool::new(1),
            io_pool: CpuPool::new(1),

            repository,
        }
    }

    pub fn repository(&self) -> &Repository {
        &self.repository
    }

    pub fn catalogs(&mut self) -> &mut Registry {
        &mut self.repository.catalogs
    }

    pub fn marshal_pool(&self) -> &CpuPool {
        &self.marshal_pool
    }

    pub fn io_pool(&self) -> &CpuPool {
        &self.io_pool
    }

    pub fn get_remote_cfg<U: AsRef<str> + Into<String>>(
        &self,
        remote_name: U,
    ) -> Result<RemoteCfg> {
        match self.repository.config.remotes.get(remote_name.as_ref()) {
            Some(remote_cfg) => Ok(remote_cfg.to_owned()),
            None => bail!(ErrorKind::RemoteNotFound(remote_name.into())),
        }
    }

    pub fn with_batch(&mut self) -> Batch<T::BatchTrace> {
        Batch::with_trace(self.marshal_pool.clone(), self.trace.on_batch())
    }

    pub fn with_local(&mut self) -> Result<LocalContext<T>> {
        let local = Local::new(self)?;

        Ok(LocalContext { ctx: self, local })
    }

    /// Load a remote configuration, producing a `RemoteContext`.
    pub fn with_remote<U: AsRef<str> + Into<String>>(
        &mut self,
        remote_name: U,
    ) -> Result<RemoteContext<T>> {
        let name = remote_name.as_ref().to_owned();
        let remote = Remote::connect(self, remote_name)?;

        Ok(RemoteContext {
            ctx: self,
            name,
            remote,
        })
    }
}


pub struct LocalContext<'a, T: Trace = ()> {
    ctx: &'a mut Context<T>,
    local: Local,
}


impl<'a, T: Trace> LocalContext<'a, T> {
    /// Read a single object from the local repository.
    pub fn read_object(
        &mut self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = Object, Error = Error>> {
        self.local.read_object(object_hash)
    }

    /// Write a fully marshalled batch to the local repository.
    pub fn write_batch(
        &mut self,
        batch: Batch<T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error>> {
        let trace = Arc::new(Mutex::new(
            self.ctx.trace.on_write(&batch, WriteDestination::Local),
        ));

        let local = self.local.clone();
        let io_pool = self.ctx.io_pool.clone();

        let writes = batch
            .into_stream()
            .map(move |hashed| {
                let hash = *hashed.as_hash();
                let trace = trace.clone();

                trace.lock().unwrap().on_begin(&hash);
                let write = local.write_object(hashed).map(move |fresh| {
                    trace.lock().unwrap().on_complete(&hash, fresh);
                });
                io_pool.spawn(write)
            })
            .buffer_unordered(WRITE_FUTURE_BUFFER_SIZE)
            .for_each(|_| Ok(()));

        Box::new(self.ctx.io_pool.spawn(writes))
    }
}


/// A `RemoteContext` is a context for dealing with a specific remote of this repository.
// TODO: Abstract away the backend so that other K/V stores than Ceph/RADOS may be used.
pub struct RemoteContext<'a, T: Trace> {
    ctx: &'a mut Context<T>,
    name: String,
    remote: Remote,
}


impl<'a, T: Trace> RemoteContext<'a, T> {
    pub fn read_object(
        &mut self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = Object, Error = Error>> {
        self.remote.read_object(object_hash)
    }

    /// Write a fully marshalled batch to the remote repository.
    // TODO: Recover from errors when sending objects.
    pub fn write_batch(
        &mut self,
        batch: Batch<T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error>> {
        let trace = Arc::new(Mutex::new(self.ctx.trace.on_write(
            &batch,
            WriteDestination::Remote(&self.name),
        )));

        let remote = self.remote.clone();

        let result = batch
            .into_stream()
            .map(move |hashed| {
                let hash = *hashed.as_hash();
                let trace = trace.clone();

                trace.lock().unwrap().on_begin(&hash);
                remote.write_object(hashed).map(move |fresh| {
                    trace.lock().unwrap().on_complete(&hash, fresh);
                })
            })
            .buffer_unordered(WRITE_FUTURE_BUFFER_SIZE)
            .for_each(|_| Ok(()));

        Box::new(self.ctx.io_pool.spawn(result))
    }
}
