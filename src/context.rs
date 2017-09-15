//! # `context` - manage a valid repository.
//!
//! `Context` is the main point of entry for the Attaca API. Important pieces of functionality in
//! this module include:
//!
//! * Creating/using `Context` and `RemoteContext`s.

use std::fs::File;
use std::path::Path;
use std::rc::Rc;

use typed_arena::Arena;

use errors::Result;
use local::Local;
use marshal::{Marshaller, Marshalled, ObjectHash};
use marshal::tree::{Record, Tree};
use remote::Remote;
use repository::Repository;
use split::{FileChunker, ChunkedFile};
use trace::{Trace, WriteMarshalledTrace};


/// A batch of files being marshalled.
pub struct Files {
    arena: Arena<FileChunker>,
}


impl Files {
    pub fn new() -> Files {
        Files { arena: Arena::new() }
    }
}


/// A context for marshalling and local operations on a repository. `RemoteContext`s must be built
/// from a `Context`.
///
/// `Context` may optionally be supplied with a type `T` implementing `Trace`. This "trace object"
/// is useful for doing things like tracking the progress of long-running operations.
pub struct Context<T: Trace = ()> {
    trace: T,

    repository: Rc<Repository>,

    local: Local,
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
        let repository = Rc::new(repository);
        let local = Local::new(repository.clone());

        Context {
            trace,
            repository,
            local,
        }
    }


    /// Chunk the file at the given path.
    pub fn chunk_file<'files, P: AsRef<Path>>(
        &mut self,
        files: &'files mut Files,
        path: P,
    ) -> Result<ChunkedFile<'files>> {
        let file = File::open(path)?;
        let chunker = files.arena.alloc(FileChunker::new(&file)?);

        let mut chunk_trace = self.trace.on_split(chunker.len() as u64);
        let chunked = chunker.chunk_with_trace(&mut chunk_trace);

        Ok(chunked)
    }


    /// Marshal a chunked file into a tree of objects, returning the marshalled objects along with
    /// the hash of the root object.
    pub fn marshal_file<'files>(
        &mut self,
        chunked: ChunkedFile<'files>,
    ) -> (ObjectHash, Marshalled<'files>) {
        let tree = Tree::load(chunked.chunks().iter().map(
            |chunk| Record::Deep(chunk.clone()),
        ));

        let mut marshaller = Marshaller::with_trace(self.trace.on_marshal(tree.len()));
        let object_hash = marshaller.put(tree);
        let marshalled = marshaller.finish();

        (object_hash, marshalled)
    }


    /// Write a fully marshalled batch to the local repository.
    pub fn write_marshalled(&mut self, marshalled: &Marshalled) -> Result<()> {
        let mut wm_trace = self.trace.on_write_marshalled(marshalled.len());

        for (object_hash, entry) in marshalled.iter() {
            match *entry {
                Some(ref object) => {
                    wm_trace.on_write(object_hash);
                    self.local.write_object(object_hash, object)?;
                }
                None => {} // If the entry is empty, that means it's already stored locally.
            }
        }

        return Ok(());
    }


    /// Load a remote configuration, producing a `RemoteContext`.
    pub fn with_remote<U: AsRef<str>>(&mut self, remote: U) -> Result<RemoteContext<T>> {
        let remote = {
            let remote_cfg = match self.repository.config.remotes.get(remote.as_ref()) {
                Some(remote_cfg) => remote_cfg,
                None => bail!("Unknown remote!"),
            };

            Remote::connect(remote_cfg)?
        };

        Ok(RemoteContext { ctx: self, remote })
    }
}


/// A `RemoteContext` is a context for dealing with a specific remote of this repository.
// TODO: Abstract away the backend so that other K/V stores than Ceph/RADOS may be used.
pub struct RemoteContext<'ctx, T: Trace + 'ctx = ()> {
    ctx: &'ctx mut Context<T>,

    remote: Remote,
}


impl<'local> RemoteContext<'local> {
    /// Write a fully marshalled batch to the remote repository.
    // TODO: Make asynchronous.
    // TODO: Recover from errors when sending objects.
    pub fn write_marshalled(&mut self, marshalled: &Marshalled) -> Result<()> {
        for (object_hash, entry) in marshalled.iter() {
            match *entry {
                Some(ref object) => self.remote.write_object(object_hash, object)?,
                None => {
                    // Fetch data from local blob store - it's here, but it's not in memory.
                    let local_object = self.ctx.local.read_object(object_hash)?;
                    self.remote.write_object(object_hash, local_object)?;
                }
            }
        }

        return Ok(());
    }
}
