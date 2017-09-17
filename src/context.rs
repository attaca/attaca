//! # `context` - manage a valid repository.
//!
//! `Context` is the main point of entry for the Attaca API. Important pieces of functionality in
//! this module include:
//!
//! * Creating/using `Context` and `RemoteContext`s.

use std::fs::File;
use std::path::Path;
use std::rc::Rc;
use std::sync::Mutex;

use futures::prelude::*;
use futures::stream;
use typed_arena::Arena;

use errors::Result;
use local::Local;
use marshal::{Marshaller, Marshalled, ObjectHash, SmallObject, Record, SmallRecord};
use marshal::tree::Tree;
use remote::Remote;
use repository::{Repository, RemoteCfg};
use split::{FileChunker, ChunkedFile};
use trace::{Trace, WriteDestination, WriteMarshalledTrace};


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
        let tree = Tree::load(chunked.chunks().iter().map(|chunk| {
            SmallRecord::Deep(SmallObject { chunk: chunk.clone() })
        }));

        let marshaller = Marshaller::with_trace(self.trace.on_marshal(tree.len()));
        let object_hash = tree.marshal(&marshaller).wait().unwrap();
        let marshalled = marshaller.finish();

        (object_hash, marshalled)
    }


    /// Write a fully marshalled batch to the local repository.
    pub fn write_marshalled(&mut self, marshalled: &Marshalled) -> Result<()> {
        let mut wm_trace = self.trace.on_write_marshalled(
            marshalled.len(),
            WriteDestination::Local,
        );

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
        let remote_cfg = match self.repository.config.remotes.get(remote.as_ref()) {
            Some(remote_cfg) => remote_cfg.to_owned(),
            None => bail!("Unknown remote!"),
        };

        self.with_remote_from_cfg(Some(remote.as_ref().to_owned()), remote_cfg)
    }


    pub fn with_remote_from_cfg(
        &mut self,
        name: Option<String>,
        cfg: RemoteCfg,
    ) -> Result<RemoteContext<T>> {
        let remote = Remote::connect(&cfg)?;

        Ok(RemoteContext {
            ctx: self,

            name,
            cfg,

            remote,
        })
    }
}


/// A `RemoteContext` is a context for dealing with a specific remote of this repository.
// TODO: Abstract away the backend so that other K/V stores than Ceph/RADOS may be used.
pub struct RemoteContext<'ctx, T: Trace + 'ctx = ()> {
    ctx: &'ctx mut Context<T>,

    name: Option<String>,
    cfg: RemoteCfg,

    remote: Remote,
}


impl<'ctx, T: Trace + 'ctx> RemoteContext<'ctx, T> {
    /// Write a fully marshalled batch to the remote repository.
    // TODO: Make asynchronous.
    // TODO: Recover from errors when sending objects.
    pub fn write_marshalled(&mut self, marshalled: &Marshalled) -> Result<()> {
        let ref wm_trace = Mutex::new(self.ctx.trace.on_write_marshalled(
            marshalled.len(),
            WriteDestination::Remote(
                &self.name,
                &self.cfg,
            ),
        ));

        let futures = marshalled.iter().map(|(object_hash, entry)| {
            let result = match *entry {
                Some(ref object) => self.remote.write_object(&object_hash, object),
                None => {
                    self.ctx.local.read_object(&object_hash).and_then(
                        |local_object| {
                            self.remote.write_object(&object_hash, local_object)
                        },
                    )
                }
            };

            result.into_future().flatten().map(move |_| { wm_trace.lock().unwrap().on_write(&object_hash); })
        });

        stream::iter_ok(futures)
            .buffer_unordered(64)
            .wait()
            .collect::<::std::result::Result<Vec<_>, _>>()?;

        Ok(())
    }
}
