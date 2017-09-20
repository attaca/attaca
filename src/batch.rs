use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::fs::File;
use std::path::Path;

use colosseum::unsync::Arena;
use futures::prelude::*;
use futures::unsync::mpsc::{self, Sender, Receiver};
use memmap::{Mmap, Protection};

use errors::*;
use marshal::{Hashed, Hasher, ObjectHash, SmallObject, SmallRecord, Tree};
use split::{self, Chunked};
use trace::BatchTrace;


const MARSHAL_BUFFER_SIZE: usize = 64;


pub struct Files {
    arena: Arena<Mmap>,
}


impl Files {
    pub fn new() -> Self {
        Self { arena: Arena::new() }
    }
}


/// A batch of files being marshalled.
pub struct Batch<'data, T: BatchTrace = ()> {
    trace: RefCell<T>,

    files: &'data Files,

    marshal_tx: Sender<Hashed>,
    marshal_rx: Receiver<Hashed>,

    size_chunks: Cell<usize>,
}


impl<'data, T: BatchTrace> Batch<'data, T> {
    pub fn with_trace(files: &'data Files, trace: T) -> Self {
        let (marshal_tx, marshal_rx) = mpsc::channel(MARSHAL_BUFFER_SIZE);

        Batch {
            trace: RefCell::new(trace),

            files,

            marshal_tx,
            marshal_rx,

            size_chunks: Cell::new(0),
        }
    }


    /// Read a file as a byte slice. This will memory-map the underlying file.
    ///
    /// * `file` - the file to read. *Must be opened with read permissions!*
    fn read(&self, file: &File) -> Result<&'data [u8]> {
        let mmap = self.files.arena.alloc(Mmap::open(file, Protection::Read)?);
        Ok(unsafe { mmap.as_slice() })
    }


    /// Read a file as a byte slice. This will memory-map the underlying file.
    fn read_path<P: AsRef<Path>>(&self, path: P) -> Result<&'data [u8]> {
        self.read(&File::open(path)?)
    }


    /// Chunk the file at the given path.
    pub fn chunk_file<P: AsRef<Path>>(&self, path: P) -> Result<Chunked<'data>> {
        let slice = self.read_path(path)?;
        let chunked = split::chunk_with_trace(
            slice,
            &mut self.trace.borrow_mut().on_split(slice.len() as u64),
        );

        Ok(chunked)
    }


    /// Marshal a chunked file into a tree of objects, returning the marshalled objects along with
    /// the hash of the root object.
    pub fn marshal_file(&self, chunked: Chunked<'data>) -> Box<Future<Item = ObjectHash, Error = Error> + 'data> {
        let tree = Tree::load(chunked.to_vec().into_iter().map(|chunk| {
            SmallRecord::Deep(SmallObject { chunk: Cow::Borrowed(chunk) })
        }));

        let tree_len = tree.len();

        self.size_chunks.set(self.size_chunks.get() + tree_len);

        let result = tree.marshal(Hasher::with_trace(
            self.marshal_tx.clone(),
            self.trace.borrow_mut().on_marshal(tree_len),
        ));

        Box::new(result)
    }


    pub fn len(&self) -> usize {
        self.size_chunks.get()
    }


    /// Convert this `Batch` into a stream of `Hashed` objects.
    pub fn into_stream(self) -> Box<Stream<Item = Hashed, Error = Error> + 'data> {
        Box::new(self.marshal_rx.map_err(|()| "Upstream error!".into()))
    }
}
