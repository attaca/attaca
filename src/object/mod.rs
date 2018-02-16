pub mod decode;
pub mod encode;

use std::{collections::BTreeMap, ops::{Deref, DerefMut}};

use failure::Error;
use futures::{future::{Flatten, FutureResult}, prelude::*};

use store::{Handle, HandleBuilder, Store};

#[derive(Debug, Clone)]
pub enum Object<H: Handle> {
    Small(Small),
    Large(Large<H>),
    Tree(Tree<H>),
    Commit(Commit<H>),
}

impl<H: Handle> Object<H> {
    pub fn kind(&self) -> ObjectKind {
        match *self {
            Object::Small(_) => ObjectKind::Small,
            Object::Large(_) => ObjectKind::Large,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Commit(_) => ObjectKind::Commit,
        }
    }

    pub fn send<S>(&self, store: &S) -> SendFuture<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        let blocking = match *self {
            Object::Small(ref small) => encode::small(&mut builder, small),
            Object::Large(ref large) => encode::large(&mut builder, large),
            Object::Tree(ref tree) => encode::tree(&mut builder, tree),
            Object::Commit(ref _commit) => unimplemented!(),
        }.map(|()| builder.finish())
            .into_future()
            .flatten();

        SendFuture {
            kind: self.kind(),
            blocking,
        }
    }
}

pub struct SendFuture<S: Store> {
    kind: ObjectKind,
    blocking: Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
}

impl<S: Store> Future for SendFuture<S> {
    type Item = ObjectRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking.poll()?.map(|handle| match self.kind {
            ObjectKind::Small => ObjectRef::Small(handle),
            ObjectKind::Large => ObjectRef::Large(handle),
            ObjectKind::Tree => ObjectRef::Tree(handle),
            ObjectKind::Commit => ObjectRef::Commit(handle),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectKind {
    Small,
    Large,
    Tree,
    Commit,
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectRef<H: Handle> {
    Small(H),
    Large(H),
    Tree(H),
    Commit(H),
}

impl<H: Handle> ObjectRef<H> {
    pub fn handle(&self) -> &H {
        match *self {
            ObjectRef::Small(ref h)
            | ObjectRef::Large(ref h)
            | ObjectRef::Tree(ref h)
            | ObjectRef::Commit(ref h) => h,
        }
    }

    pub fn kind(&self) -> ObjectKind {
        match *self {
            ObjectRef::Small(_) => ObjectKind::Small,
            ObjectRef::Large(_) => ObjectKind::Large,
            ObjectRef::Tree(_) => ObjectKind::Tree,
            ObjectRef::Commit(_) => ObjectKind::Commit,
        }
    }

    pub fn fetch(&self) -> FetchFuture<H> {
        FetchFuture {
            kind: self.kind(),
            blocking: self.handle().load(),
        }
    }
}

pub struct FetchFuture<H: Handle> {
    kind: ObjectKind,
    blocking: H::FutureLoad,
}

impl<H: Handle> Future for FetchFuture<H> {
    type Item = Object<H>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.blocking.poll()? {
            Async::Ready((content, refs_iter)) => {
                let obj = match self.kind {
                    ObjectKind::Small => Object::Small(decode::small::<H>(content)?),
                    ObjectKind::Large => Object::Large(decode::large::<H>(content, refs_iter)?),
                    ObjectKind::Tree => Object::Tree(decode::tree::<H>(content, refs_iter)?),
                    ObjectKind::Commit => unimplemented!(),
                };

                Ok(Async::Ready(obj))
            }
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Small {
    data: Vec<u8>,
}

impl Small {
    pub fn send<S>(&self, store: &S) -> SendFuture<S>
    where
        S: Store,
    {
        let mut builder = store.handle_builder();
        let blocking = encode::small(&mut builder, self)
            .map(|()| builder.finish())
            .into_future()
            .flatten();

        SendFuture {
            kind: ObjectKind::Small,
            blocking,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Large<H: Handle> {
    children: Vec<(u64, ObjectRef<H>)>,
}

impl<H: Handle> Large<H> {
    pub fn send<S>(&self, store: &S) -> SendFuture<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        let blocking = encode::large(&mut builder, self)
            .map(|()| builder.finish())
            .into_future()
            .flatten();

        SendFuture {
            kind: ObjectKind::Large,
            blocking,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tree<H: Handle> {
    entries: BTreeMap<String, ObjectRef<H>>,
}

impl<H: Handle> Deref for Tree<H> {
    type Target = BTreeMap<String, ObjectRef<H>>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<H: Handle> Tree<H> {
    pub fn diverge(self) -> TreeBuilder<H> {
        TreeBuilder(self)
    }

    pub fn send<S>(&self, store: &S) -> SendFuture<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        let blocking = encode::tree(&mut builder, self)
            .map(|()| builder.finish())
            .into_future()
            .flatten();

        SendFuture {
            kind: ObjectKind::Tree,
            blocking,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TreeBuilder<H: Handle>(Tree<H>);

impl<H: Handle> Deref for TreeBuilder<H> {
    type Target = BTreeMap<String, ObjectRef<H>>;

    fn deref(&self) -> &Self::Target {
        &self.0.entries
    }
}

impl<H: Handle> DerefMut for TreeBuilder<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.entries
    }
}

impl<H: Handle> TreeBuilder<H> {
    pub fn new() -> Self {
        TreeBuilder(Tree {
            entries: BTreeMap::new(),
        })
    }

    pub fn as_tree(&self) -> &Tree<H> {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct Commit<H: Handle> {
    subtree: H,
    parents: Vec<H>,
}
