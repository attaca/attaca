//! # `context` - manage a valid repository.
//!
//! `Context` is the main point of entry for the Attaca API. Important pieces of functionality in
//! this module include:
//!
//! * Creating/using `Context` and `RemoteContext`s.

use std::cell::RefCell;
use std::rc::Rc;

use futures::prelude::*;

use batch::{Files, Batch};
use errors::*;
use local::Local;
use remote::Remote;
use repository::{Repository, RemoteCfg};
use trace::{Trace, WriteDestination, WriteTrace};


/// A context for marshalling and local operations on a repository. `RemoteContext`s must be built
/// from a `Context`.
///
/// `Context` may optionally be supplied with a type `T` implementing `Trace`. This "trace object"
/// is useful for doing things like tracking the progress of long-running operations.
pub struct Context<T: Trace = ()> {
    trace: RefCell<T>,

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
        let trace = RefCell::new(trace);
        let repository = Rc::new(repository);
        let local = Local::new(repository.clone());

        Context {
            trace,
            repository,
            local,
        }
    }


    pub fn batch<'data>(&self, files: &'data Files) -> Batch<'data, T::BatchTrace> {
        Batch::with_trace(files, self.trace.borrow_mut().on_batch())
    }


    /// Write a fully marshalled batch to the local repository.
    pub fn write_batch<'future, 'ctx, 'data>(
        &'ctx self,
        batch: Batch<'data, T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error> + 'future>
    where
        'ctx: 'future,
        'data: 'future,
    {
        let trace = Rc::new(RefCell::new(self.trace.borrow_mut().on_write(
            &batch,
            WriteDestination::Local,
        )));

        let result = batch.into_stream().for_each(move |hashed| {
            let hash = *hashed.as_hash();
            let trace = trace.clone();

            let written = self.local.write_object(hashed);
            written.map(move |_| { trace.borrow_mut().on_write(&hash); })
        });

        Box::new(result)
    }


    /// Load a remote configuration, producing a `RemoteContext`.
    pub fn with_remote<U: AsRef<str>>(&self, remote: U) -> Result<RemoteContext<T>> {
        let remote_cfg = match self.repository.config.remotes.get(remote.as_ref()) {
            Some(remote_cfg) => remote_cfg.to_owned(),
            None => bail!("Unknown remote!"),
        };

        self.with_remote_from_cfg(Some(remote.as_ref().to_owned()), remote_cfg)
    }


    pub fn with_remote_from_cfg(
        &self,
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
    ctx: &'ctx Context<T>,

    name: Option<String>,
    cfg: RemoteCfg,

    remote: Remote,
}


impl<'ctx, T: Trace> RemoteContext<'ctx, T> {
    /// Write a fully marshalled batch to the remote repository.
    // TODO: Recover from errors when sending objects.
    pub fn write_batch<'data, 'future>(
        &'ctx self,
        batch: Batch<'data, T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error> + 'future>
    where
        'ctx: 'future,
        'data: 'future,
    {
        let trace = Rc::new(RefCell::new(self.ctx.trace.borrow_mut().on_write(
            &batch,
            WriteDestination::Remote(
                &self.name,
                &self.cfg,
            ),
        )));

        let result = batch.into_stream().for_each(move |hashed| {
            let hash = *hashed.as_hash();
            let trace = trace.clone();

            let written = self.remote.write_object(hashed).into_future().flatten();
            written.map(move |_| { trace.borrow_mut().on_write(&hash); })
        });

        Box::new(result)
    }
}
