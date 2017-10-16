//! # `context` - manage a valid repository.

use std::fmt;
use std::path::{Path, PathBuf};

use futures::future::{self, Either};
use futures::prelude::*;
use futures::stream;
use futures::sync::mpsc::{self, Sender};
use futures_cpupool::CpuPool;
use memmap::{Mmap, Protection};

use {BATCH_FUTURE_BUFFER_SIZE, WRITE_FUTURE_BUFFER_SIZE};
use arc_slice::{self, ArcSlice};
use errors::*;
use index::{Index, Cached};
use marshal::{ObjectHash, Marshaller, Hashed, DirTree};
use split::SliceChunker;
use store::{Store, Empty};
use trace::Trace;


/// A context for marshalling and local operations on a repository. `RemoteContext`s must be built
/// from a `Context`.
///
/// `Context` may optionally be supplied with a type `T` implementing `Trace`. This "trace object"
/// is useful for doing things like tracking the progress of long-running operations.
pub struct Context<T: Trace, S: Store> {
    trace: T,
    store: S,

    marshal_pool: CpuPool,
    io_pool: CpuPool,

    marshal_tx: Sender<Hashed>,
    writes: Box<Future<Item = (), Error = Error> + Send>,
}


impl<T: Trace + fmt::Debug, S: Store + fmt::Debug> fmt::Debug for Context<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context")
            .field("trace", &self.trace)
            .field("store", &self.store)
            .field("marshal_pool", &self.marshal_pool)
            .field("io_pool", &self.io_pool)
            .field("marshal_tx", &self.marshal_tx)
            .finish()
    }
}


impl<T: Trace, S: Store> Context<T, S> {
    /// Create a context from a loaded repository, with a supplied trace object.
    pub fn new(trace: T, store: S, marshal_pool: &CpuPool, io_pool: &CpuPool) -> Self {
        let (marshal_tx, marshal_rx) = mpsc::channel(BATCH_FUTURE_BUFFER_SIZE);

        let writes = {
            let trace = trace.clone();
            let store = store.clone();
            let writes_unboxed = marshal_rx
                .map_err(|()| unreachable!("mpsc receivers never error"))
                .map(move |hashed: Hashed| {
                    let hash = *hashed.as_hash();
                    let trace = trace.clone();

                    trace.on_write_object_start(&hash);
                    store.write_object(hashed).map(move |fresh| {
                        trace.on_write_object_finish(&hash, fresh);
                    })
                })
                .buffer_unordered(WRITE_FUTURE_BUFFER_SIZE)
                .for_each(|_| Ok(()));

            Box::new(io_pool.spawn(writes_unboxed))
        };

        Self {
            trace,
            store,

            marshal_pool: marshal_pool.clone(),
            io_pool: io_pool.clone(),

            marshal_tx,
            writes,
        }
    }

    pub fn split_file<P: AsRef<Path>>(
        &mut self,
        path: P,
    ) -> Box<Stream<Item = ArcSlice, Error = Error> + Send> {
        let trace = self.trace.clone();
        let slice_res = Mmap::open_path(path, Protection::Read).map(|mmap| {
            trace.on_split_begin(mmap.len() as u64);
            arc_slice::mapped(mmap)
        });

        let stream_future = {
            async_block! {
                let mut offset = 0u64;
                let slices = SliceChunker::new(slice_res?).inspect(move |chunk| {
                    trace.on_split_chunk(offset, chunk);
                    offset += chunk.len() as u64;
                });

                Ok(stream::iter_ok(slices))
            }
        };

        Box::new(stream_future.flatten_stream())
    }

    pub fn hash_file<U>(
        &mut self,
        stream: U,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send>
    where
        U: Stream<Item = ArcSlice, Error = Error> + Send + 'static,
    {
        let marshal_tx = self.marshal_tx.clone();
        let marshaller = Marshaller::with_trace(marshal_tx, self.trace.clone());

        Box::new(self.marshal_pool.spawn(marshaller.process_chunks(stream)))
    }

    pub fn hash_subtree<U>(
        &mut self,
        stream: U,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send>
    where
        U: Stream<Item = (PathBuf, ObjectHash), Error = Error> + Send + 'static,
    {
        let marshal_tx = self.marshal_tx.clone();
        let marshaller = Marshaller::with_trace(marshal_tx, self.trace.clone());
        let hash_future = stream
            .map(|(path, hash)| (path, Some(hash)))
            .collect()
            .and_then(|entries| {
                DirTree::delta(Empty, None, entries).map_err(|err| {
                    Error::with_chain(Error::from_kind(ErrorKind::DirTreeDelta), err)
                })
            })
            .and_then(move |dir_tree| marshaller.process_dir_tree(dir_tree));

        Box::new(self.marshal_pool.spawn(hash_future))
    }

    // TODO: Add inclusion and exclusion.
    pub fn hash_subtree_delta(
        &mut self,
        root: Option<ObjectHash>,
        index: &mut Index,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let ops = {
            let op_futures = index.iter_tracked().map(|(path, cached_opt)| {
                match cached_opt {
                    Some(Cached::Hashed(object_hash)) => Either::A(future::ok(
                        (path.to_owned(), Some(object_hash)),
                    )),
                    Some(Cached::Removed) => Either::A(future::ok((path.to_owned(), None))),

                    // If the file has no hash in the cache *or* has an invalid cache entry, we must
                    // split and hash it.
                    Some(Cached::Unhashed) |
                    None => {
                        let path = path.to_owned();
                        let chunk_stream = self.split_file(&path);
                        let hash_future = self.hash_file(chunk_stream);

                        Either::B(hash_future.map(|object_hash| (path, Some(object_hash))))
                    }
                }
            });

            match stream::futures_unordered(op_futures).collect().wait() {
                Ok(ops) => ops,
                Err(err) => return Box::new(future::err(err)),
            }
        };

        {
            let updates = ops.iter().filter_map(|&(ref path, ref hash_opt)| {
                hash_opt.map(|hash| (path, hash))
            });

            for (path, hash) in updates {
                if let Err(err) = index.clean(path, hash) {
                    return Box::new(future::err(err));
                }
            }
        }

        let marshaller = Marshaller::with_trace(self.marshal_tx.clone(), self.trace.clone());
        let result = DirTree::delta(self.store.clone(), root, ops).and_then(
            move |dir_tree| marshaller.process_dir_tree(dir_tree),
        );

        Box::new(result)
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn close(self) -> Box<Future<Item = (), Error = Error> + Send> {
        self.writes
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
