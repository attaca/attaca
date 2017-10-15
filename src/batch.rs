//! # `batch` - batches of files to marshal.
//!
//! A `Batch` represents a collection of files which are being marshalled and a stream of hashed
//! output objects. For a single `Batch`, there should correspond a set of valid marshalled files
//! and a stream of valid hashed objects produced from marshalling/hashing the batched files.
//!
//! Marshalling files in a `Batch` will spawn files to a specified threadpool provided by the
//! `Context`.

use std::fs::File;
use std::path::Path;

use futures::prelude::*;
use futures::sync::mpsc::{self, Sender, Receiver};
use futures_cpupool::CpuPool;
use memmap::{Mmap, Protection};

use BATCH_FUTURE_BUFFER_SIZE;
use arc_slice::{self, ArcSlice};
use errors::*;
use marshal::{Hashed, Hasher, ObjectHash, SmallRecord, DataTree, DirTree};
use split::{self, Chunked};
use trace::BatchTrace;


/// A batch of files being marshalled.
pub struct Batch<T: BatchTrace = ()> {
    trace: T,

    marshal_tx: Sender<Hashed>,
    marshal_rx: Receiver<Hashed>,

    marshal_pool: CpuPool,

    len: usize,
}


impl<T: BatchTrace> Batch<T> {
    pub fn new(marshal_pool: &CpuPool, trace: T) -> Self {
        let (marshal_tx, marshal_rx) = mpsc::channel(BATCH_FUTURE_BUFFER_SIZE);

        Batch {
            trace,

            marshal_tx,
            marshal_rx,

            marshal_pool,

            len: 0,
        }
    }

    /// Read a file as a byte slice. This will memory-map the underlying file.
    ///
    /// * `file` - the file to read. *Must be opened with read permissions!*
    fn read(&mut self, file: &File) -> Result<ArcSlice> {
        Ok(arc_slice::mapped(Mmap::open(file, Protection::Read)?))
    }

    /// Read a file as a byte slice. This will memory-map the underlying file.
    fn read_path<P: AsRef<Path>>(&mut self, path: P) -> Result<ArcSlice> {
        self.read(&File::open(path)?)
    }

    /// Chunk the file at the given path.
    pub fn chunk_file<P: AsRef<Path>>(&mut self, path: P) -> Result<Chunked> {
        let slice = self.read_path(path)?;
        let mut trace = self.trace.on_split(slice.len() as u64);
        let chunked = split::chunk_with_trace(slice, &mut trace);

        Ok(chunked)
    }

    pub fn load_file<P: AsRef<Path>>(&mut self, path: P) -> Result<DataTree> {
        let chunked = self.chunk_file(path)?;
        let data_tree = DataTree::load(chunked.to_vec().into_iter().map(SmallRecord::from));

        Ok(data_tree)
    }

    pub fn load_subtree<P: AsRef<Path>>(&mut self, path: P) -> Result<DirTree> {
        let path_ref = path.as_ref();
        let mut dir_tree = DirTree::new();

        for entry_res in path_ref.read_dir()? {
            let entry = entry_res?;
            let path = entry.path();
            let entry_path = path.strip_prefix(path_ref).unwrap();

            if path.is_dir() {
                dir_tree.insert(entry_path, self.load_subtree(&path)?);
            } else {
                dir_tree.insert(entry_path, self.load_file(&path)?);
            }
        }

        Ok(dir_tree)
    }

    /// Marshal a chunked file into a tree of objects, returning the marshalled objects along with
    /// the hash of the root object.
    pub fn marshal_file(
        &mut self,
        chunked: Chunked,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let tree = DataTree::load(chunked.to_vec().into_iter().map(SmallRecord::from));
        let tree_total = tree.total();

        self.len += tree_total;

        let marshal = tree.marshal(Hasher::with_trace(
            self.marshal_tx.clone(),
            self.trace.on_marshal(tree_total),
        ));

        Box::new(self.marshal_pool.spawn(marshal))
    }

    pub fn marshal_subtree<P: AsRef<Path>>(
        &mut self,
        path: P,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let result = self.load_subtree(path)
            .map(|tree| {
                let tree_total = tree.total();

                self.len += tree_total;

                let marshal = tree.marshal(Hasher::with_trace(
                    self.marshal_tx.clone(),
                    self.trace.on_marshal(tree_total),
                ));

                self.marshal_pool.spawn(marshal)
            })
            .into_future()
            .flatten();

        Box::new(result)
    }

    /// The total size of the batch, in chunks.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Convert this `Batch` into a stream of `Hashed` objects.
    pub fn into_stream(self) -> Box<Stream<Item = Hashed, Error = Error> + Send> {
        Box::new(self.marshal_rx.map_err(|()| "Upstream error!".into()))
    }
}
