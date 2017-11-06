//! # `context` - manage a valid repository.

use std::ops::{Deref, DerefMut};
use std::fmt;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use chrono::prelude::*;
use futures::future::{self, Either};
use futures::prelude::*;
use futures::stream;
use futures::sync::mpsc::{self, Sender, Receiver};
use futures_cpupool::CpuPool;
use globset::GlobSet;
use memmap::{Mmap, Protection};

use {BATCH_FUTURE_BUFFER_SIZE, WRITE_FUTURE_BUFFER_SIZE};
use arc_slice::{self, ArcSlice};
use errors::*;
use index::Cached;
use marshal::{ObjectHash, Marshaller, Hashed, Object, CommitObject, Tree, BackedTree, TreeOp};
use repository::Repository;
use split::SliceChunker;
use store::{Store, Empty};
use trace::Trace;


/// A context for marshalling and local operations on a repository. `RemoteContext`s must be built
/// from a `Context`.
///
/// `Context` may optionally be supplied with a type `T` implementing `Trace`. This "trace object"
/// is useful for doing things like tracking the progress of long-running operations.
pub struct Context<'a, T: Trace, S: Store> {
    repository: &'a mut Repository,

    trace: T,
    store: S,

    marshal_pool: CpuPool,

    marshal_tx: Sender<Hashed>,
    writes: Box<Future<Item = (), Error = Error> + Send>,

    index_tx: Sender<(PathBuf, ObjectHash)>,
    index_rx: Receiver<(PathBuf, ObjectHash)>,
}


impl<'a, T: Trace, S: Store> Deref for Context<'a, T, S> {
    type Target = Repository;

    fn deref(&self) -> &Self::Target {
        &*self.repository
    }
}


impl<'a, T: Trace, S: Store> DerefMut for Context<'a, T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.repository
    }
}


impl<'a, T: Trace + fmt::Debug, S: Store + fmt::Debug> fmt::Debug for Context<'a, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context")
            .field("repository", &self.repository)
            .field("trace", &self.trace)
            .field("store", &self.store)
            .field("marshal_pool", &self.marshal_pool)
            .field("marshal_tx", &self.marshal_tx)
            .finish()
    }
}


impl<'a, T: Trace, S: Store> Context<'a, T, S> {
    /// Create a context from a loaded repository, with a supplied trace object.
    pub fn new(
        repository: &'a mut Repository,
        trace: T,
        store: S,
        marshal_pool: &CpuPool,
        io_pool: &CpuPool,
    ) -> Self {
        let (marshal_tx, marshal_rx) = mpsc::channel(BATCH_FUTURE_BUFFER_SIZE);
        let (index_tx, index_rx) = mpsc::channel(BATCH_FUTURE_BUFFER_SIZE);

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
            repository,

            trace,
            store,

            marshal_pool: marshal_pool.clone(),

            marshal_tx,
            writes,

            index_tx,
            index_rx,
        }
    }

    pub fn split_file<P: AsRef<Path>>(
        &self,
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

    pub fn read_object(
        &self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = Object, Error = Error> + Send> {
        Box::new(self.store.read_object(object_hash))
    }

    pub fn read_commit(
        &self,
        commit_hash: ObjectHash,
    ) -> Box<Future<Item = CommitObject, Error = Error> + Send> {
        let async = self.store.read_object(commit_hash).and_then(
            move |object| {
                match object {
                    Object::Commit(commit_object) => Ok(commit_object),
                    _ => bail!(ErrorKind::ObjectNotACommit(commit_hash)),
                }
            },
        );

        Box::new(async)
    }

    pub fn read_head(&self) -> Box<Future<Item = Option<CommitObject>, Error = Error> + Send> {
        match self.refs.head() {
            Some(commit_hash) => Box::new(self.read_commit(commit_hash).map(Some)),
            None => Box::new(future::ok(None)),
        }
    }

    pub fn write_file<U>(&self, stream: U) -> Box<Future<Item = ObjectHash, Error = Error> + Send>
    where
        U: Stream<Item = ArcSlice, Error = Error> + Send + 'static,
    {
        let marshal_tx = self.marshal_tx.clone();
        let marshaller = Marshaller::with_trace(marshal_tx, self.trace.clone());

        Box::new(self.marshal_pool.spawn(marshaller.process_chunks(stream)))
    }

    pub fn write_subtree<U>(
        &self,
        stream: U,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send>
    where
        U: Stream<Item = (PathBuf, ObjectHash), Error = Error> + Send + 'static,
    {
        let marshal_tx = self.marshal_tx.clone();
        let marshaller = Marshaller::with_trace(marshal_tx, self.trace.clone());
        let hash_future = stream.collect().and_then(move |entries| {
            marshaller.process_tree(Tree::from_iter(entries))
        });

        Box::new(self.marshal_pool.spawn(hash_future))
    }

    pub fn write_commit(
        &self,
        include_opt: Option<&GlobSet>,
        exclude_opt: Option<&GlobSet>,
        parents: Vec<ObjectHash>,
        message: String,
        timestamp: DateTime<Utc>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let marshaller = Marshaller::with_trace(self.marshal_tx.clone(), self.trace.clone());

        let subtree_future = {
            let entries_iter = self.index.iter()
                .filter(|&(path, entry)| {
                    let is_included = include_opt
                        .map(|include| include.is_match(path))
                        .unwrap_or(false);
                    let is_excluded = exclude_opt
                        .map(|exclude| exclude.is_match(path))
                        .unwrap_or(false);

                    (is_included || entry.added || entry.tracked) && !is_excluded
                })
                .map(|(path, entry)| {
                    match entry.get() {
                        Some(Cached::Hashed(object_hash)) => Either::A(future::ok(TreeOp::Insert(path.to_owned(), object_hash))),
                        Some(Cached::Removed) => Either::A(future::ok(TreeOp::Remove(path.to_owned()))),

                        // If the file has no hash in the cache *or* has an invalid cache entry, we must
                        // split and hash it.
                        Some(Cached::Unhashed) | None => {
                            let path = path.to_owned();
                            let chunk_stream = self.split_file(&path);
                            let index_tx = self.index_tx.clone();
                            let hash_future = self.write_file(chunk_stream);

                            Either::B(hash_future.and_then(|object_hash| {
                                index_tx
                                    .send((path.clone(), object_hash))
                                    .map(move |_| TreeOp::Insert(path, object_hash))
                                    .map_err(|_| Error::from_kind(ErrorKind::Absurd))
                            }))
                        }
                    }
                });

            let marshaller = marshaller.clone();
            let store = self.store.clone();
            let future_head_opt = self.read_head();
            let future_ops = stream::futures_unordered(entries_iter).collect();

            async_block! {
                let (ops, head_opt) = await!(future_ops.join(future_head_opt))?;
                let tree = match head_opt {
                    Some(commit) => await!(BackedTree::new(store, commit.subtree).operate(ops))?.into(),
                    None => Tree::from_iter(ops.into_iter().filter_map(TreeOp::into_insert)),
                };

                await!(marshaller.process_tree(tree))
            }
        };

        let commit_future = subtree_future.and_then(move |subtree| {
            marshaller.process(CommitObject {
                subtree,
                parents,
                message,
                timestamp,
            })
        });

        Box::new(self.marshal_pool.spawn(commit_future))
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn close(self) -> Box<Future<Item = (), Error = Error> + Send + 'a> {
        let repository = self.repository;
        let close_future = self.writes.join(
            self.index_rx.map_err(|_| Error::from_kind(ErrorKind::Absurd)).for_each(move |(path, object_hash)| {
                repository.index.clean(path, object_hash)
            }),
        ).map(|((), ())| ());

        Box::new(close_future)
    }
}
