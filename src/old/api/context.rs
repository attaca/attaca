use std::collections::{BTreeMap, HashMap};
use std::{mem, io::Read, sync::{Arc, RwLock, Weak}};

use failure::Error;
use futures::{prelude::*, stream::FuturesUnordered, sync::oneshot};

use arc_slice::ArcSlice;
use api::object::{Cache, Content, Digest, Object};

#[derive(Debug, Default)]
pub struct Context {
    cache: Arc<Cache>,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(this: &Arc<Context>, url: &str) -> Arc<Repository> {
        unimplemented!()
    }

    pub fn open(this: &Arc<Context>, url: &str) -> Arc<Workspace> {
        unimplemented!()
    }

    pub fn object_builder(this: &Arc<Context>) -> ObjectBuilder {
        ObjectBuilder { context: this }
    }
}

#[derive(Debug)]
pub struct ObjectBuilder {
    context: Arc<Context>,
}

impl ObjectBuilder {
    pub fn commit(&mut self, subtree: Arc<Object>) -> CommitBuilder {
        CommitBuilder {
            context: self.context.clone(),

            subtree: subtree,
            parents: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct CommitBuilder<'a> {
    builder: &'a mut ObjectBuilder,

    subtree: Arc<Object>,
    parents: Vec<Arc<Object>>,
}

impl<'a> CommitBuilder<'a> {
    pub fn parents<I: IntoIterator<Item = Arc<Object>>>(&mut self, iterable: I) -> &mut Self {
        self.parents.extend(iterable);
        self
    }

    pub fn finish(&mut self) -> Arc<Object> {
    }
}

#[derive(Debug)]
pub struct Repository {
    context: Arc<Context>,
    store: (),
}

#[derive(Debug)]
pub struct Workspace {
    repository: Arc<Repository>,
}
