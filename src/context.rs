//! # `context` - manage a valid repository.
//!
//! `Context` is the main point of entry for the Attaca API. Important pieces of functionality in
//! this module include:
//!
//! * Creating/using `Context` and `RemoteContext`s.

use std::sync::{Arc, Mutex};

use futures::prelude::*;
use futures_cpupool::CpuPool;

use ::WRITE_FUTURE_BUFFER_SIZE;
use batch::Batch;
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
#[derive(Debug)]
pub struct Context<T: Trace = ()> {
    marshal_pool: CpuPool,
    write_pool: CpuPool,

    inner: Arc<ContextInner<T>>,
}


#[derive(Debug)]
struct ContextInner<T: Trace = ()> {
    trace: Mutex<T>,
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
            marshal_pool: CpuPool::new(1),
            write_pool: CpuPool::new(1),

            inner: Arc::new(ContextInner {
                trace: Mutex::new(trace),
                repository,
            }),
        }
    }


    pub fn as_repository(&self) -> &Repository {
        &self.inner.repository
    }


    pub fn marshal_pool(&self) -> &CpuPool {
        &self.marshal_pool
    }


    pub fn write_pool(&self) -> &CpuPool {
        &self.write_pool
    }


    pub fn with_batch(&self) -> Batch<T::BatchTrace> {
        Batch::with_trace(
            self.marshal_pool.clone(),
            self.inner.trace.lock().unwrap().on_batch(),
        )
    }


    pub fn with_local(&self) -> LocalContext<T> {
        LocalContext {
            ctx: self.clone(),
            local: Local::new(self.clone()),
        }
    }


    /// Load a remote configuration, producing a `RemoteContext`.
    pub fn with_remote<U: AsRef<str>>(&self, remote: U) -> Result<RemoteContext<T>> {
        let remote_cfg = match self.inner.repository.config.remotes.get(remote.as_ref()) {
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
        let remote = Remote::connect(self.clone(), &cfg)?;

        Ok(RemoteContext {
            ctx: self.clone(),

            name,
            cfg,

            remote,
        })
    }
}


pub struct LocalContext<T: Trace = ()> {
    ctx: Context<T>,
    local: Local<T>,
}


impl<T: Trace> LocalContext<T> {
    /// Write a fully marshalled batch to the local repository.
    pub fn write_batch(
        &self,
        batch: Batch<T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error>> {
        let trace = Arc::new(Mutex::new(self.ctx.inner.trace.lock().unwrap().on_write(
            &batch,
            WriteDestination::Local,
        )));

        let local = self.local.clone();
        let write_pool = self.ctx.write_pool.clone();

        let writes = batch
            .into_stream()
            .map(move |hashed| {
                let hash = *hashed.as_hash();
                let trace = trace.clone();

                let write = local.write_object(hashed).map(move |_| { trace.lock().unwrap().on_write(&hash); });
                write_pool.spawn(write)
            })
            .buffered(WRITE_FUTURE_BUFFER_SIZE)
            .for_each(|_| Ok(()));

        Box::new(self.ctx.write_pool.spawn(writes))
    }
}


/// A `RemoteContext` is a context for dealing with a specific remote of this repository.
// TODO: Abstract away the backend so that other K/V stores than Ceph/RADOS may be used.
pub struct RemoteContext<T: Trace> {
    ctx: Context<T>,

    name: Option<String>,
    cfg: RemoteCfg,

    remote: Remote<T>,
}


impl<T: Trace> RemoteContext<T> {
    /// Write a fully marshalled batch to the remote repository.
    // TODO: Recover from errors when sending objects.
    pub fn write_batch(
        &self,
        batch: Batch<T::BatchTrace>,
    ) -> Box<Future<Item = (), Error = Error>> {
        let trace = Arc::new(Mutex::new(self.ctx.inner.trace.lock().unwrap().on_write(
            &batch,
            WriteDestination::Remote(&self.name, &self.cfg),
        )));

        let remote = self.remote.clone();

        let result = batch.into_stream().for_each(move |hashed| {
            let hash = *hashed.as_hash();
            let trace = trace.clone();

            let written = remote.write_object(hashed).into_future().flatten();
            written.map(move |_| { trace.lock().unwrap().on_write(&hash); })
        });

        Box::new(result)
    }
}


impl<T: Trace> Clone for Context<T> {
    fn clone(&self) -> Self {
        Context {
            marshal_pool: self.marshal_pool.clone(),
            write_pool: self.write_pool.clone(),

            inner: self.inner.clone(),
        }
    }
}
