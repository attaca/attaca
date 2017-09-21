use std::fs::File;
use std::path::Path;

use futures::prelude::*;
use futures::sync::mpsc::{self, Sender, Receiver};
use futures_cpupool::CpuPool;
use memmap::{Mmap, Protection};

use ::BATCH_FUTURE_BUFFER_SIZE;
use arc_slice::{self, ArcSlice};
use errors::*;
use marshal::{Hashed, Hasher, ObjectHash, SmallRecord, Tree};
use split::{self, Chunked};
use trace::BatchTrace;


/// A batch of files being marshalled.
pub struct Batch<T: BatchTrace = ()> {
    trace: T,

    marshal_tx: Sender<Hashed>,
    marshal_rx: Receiver<Hashed>,

    marshal_pool: CpuPool,

    size_chunks: usize,
}


impl<T: BatchTrace> Batch<T> {
    pub fn with_trace(marshal_pool: CpuPool, trace: T) -> Self {
        let (marshal_tx, marshal_rx) = mpsc::channel(BATCH_FUTURE_BUFFER_SIZE);

        Batch {
            trace,

            marshal_tx,
            marshal_rx,

            marshal_pool,

            size_chunks: 0,
        }
    }


    /// Read a file as a byte slice. This will memory-map the underlying file.
    ///
    /// * `file` - the file to read. *Must be opened with read permissions!*
    fn read(&mut self, file: &File) -> Result<ArcSlice> {
        Ok(
            arc_slice::mapped(Mmap::open(file, Protection::Read)?),
        )
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


    /// Marshal a chunked file into a tree of objects, returning the marshalled objects along with
    /// the hash of the root object.
    pub fn marshal_file(
        &mut self,
        chunked: Chunked,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let tree = Tree::load(chunked.to_vec().into_iter().map(SmallRecord::from));
        let tree_len = tree.len();

        self.size_chunks += tree_len;

        let marshal = tree.marshal(Hasher::with_trace(
            self.marshal_tx.clone(),
            self.trace.on_marshal(tree_len),
        ));

        Box::new(self.marshal_pool.spawn(marshal))
    }


    pub fn len(&self) -> usize {
        self.size_chunks
    }


    /// Convert this `Batch` into a stream of `Hashed` objects.
    pub fn into_stream(self) -> Box<Stream<Item = Hashed, Error = Error> + Send> {
        Box::new(self.marshal_rx.map_err(|()| "Upstream error!".into()))
    }
}