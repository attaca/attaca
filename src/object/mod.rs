pub mod decode;
pub mod encode;
pub mod metadata;

use std::{mem, borrow::Borrow, collections::{btree_map, BTreeMap, Bound}, io::{self, Read, Write},
          ops::{Deref, DerefMut, Range}};

use chrono::prelude::*;
use failure::Error;
use futures::{prelude::*, stream::FuturesOrdered};

use digest::prelude::*;
use split::{Parameters, Splitter};
use store::prelude::*;

#[derive(Debug, Clone)]
pub enum Object<H> {
    Small(Small),
    Large(Large<H>),
    Tree(Tree<H>),
    Commit(Commit<H>),
}

impl<B: Backend> Object<Handle<B>> {
    pub fn kind(&self) -> ObjectKind {
        match *self {
            Object::Small(_) => ObjectKind::Small,
            Object::Large(_) => ObjectKind::Large,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Commit(_) => ObjectKind::Commit,
        }
    }

    pub fn send(&self, store: &Store<B>) -> FutureObjectHandle<B> {
        match *self {
            Object::Small(ref small) => FutureObjectHandle::Small(small.send(store)),
            Object::Large(ref large) => FutureObjectHandle::Large(large.send(store)),
            Object::Tree(ref tree) => FutureObjectHandle::Tree(tree.send(store)),
            Object::Commit(ref commit) => FutureObjectHandle::Commit(commit.send(store)),
        }
    }
}

pub enum FutureObjectHandle<B: Backend> {
    Small(FutureSmallHandle<B>),
    Large(FutureLargeHandle<B>),
    Tree(FutureTreeHandle<B>),
    Commit(FutureCommitHandle<B>),
}

impl<B: Backend> Future for FutureObjectHandle<B> {
    type Item = ObjectRef<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureObjectHandle::Small(ref mut small) => Ok(small.poll()?.map(ObjectRef::Small)),
            FutureObjectHandle::Large(ref mut large) => Ok(large.poll()?.map(ObjectRef::Large)),
            FutureObjectHandle::Tree(ref mut tree) => Ok(tree.poll()?.map(ObjectRef::Tree)),
            FutureObjectHandle::Commit(ref mut commit) => Ok(commit.poll()?.map(ObjectRef::Commit)),
        }
    }
}

pub enum FutureObjectDigest<D: Digest> {
    Small(FutureSmallDigest<D>),
    Large(FutureLargeDigest<D>),
    Tree(FutureTreeDigest<D>),
    Commit(FutureCommitDigest<D>),
}

impl<D: Digest> Future for FutureObjectDigest<D> {
    type Item = ObjectRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureObjectDigest::Small(ref mut small) => Ok(small.poll()?.map(ObjectRef::Small)),
            FutureObjectDigest::Large(ref mut large) => Ok(large.poll()?.map(ObjectRef::Large)),
            FutureObjectDigest::Tree(ref mut tree) => Ok(tree.poll()?.map(ObjectRef::Tree)),
            FutureObjectDigest::Commit(ref mut commit) => Ok(commit.poll()?.map(ObjectRef::Commit)),
        }
    }
}

pub enum FutureObjectId<B: Backend> {
    Small(FutureSmallId<B>),
    Large(FutureLargeId<B>),
    Tree(FutureTreeId<B>),
    Commit(FutureCommitId<B>),
}

impl<B: Backend> Future for FutureObjectId<B> {
    type Item = ObjectRef<OwnedLocalId<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureObjectId::Small(ref mut small) => Ok(small.poll()?.map(ObjectRef::Small)),
            FutureObjectId::Large(ref mut large) => Ok(large.poll()?.map(ObjectRef::Large)),
            FutureObjectId::Tree(ref mut tree) => Ok(tree.poll()?.map(ObjectRef::Tree)),
            FutureObjectId::Commit(ref mut commit) => Ok(commit.poll()?.map(ObjectRef::Commit)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectKind {
    Small,
    Large,
    Tree,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectRef<H> {
    Small(SmallRef<H>),
    Large(LargeRef<H>),
    Tree(TreeRef<H>),
    Commit(CommitRef<H>),
}

impl<H> ObjectRef<H> {
    pub fn map<I, F: FnOnce(H) -> I>(self, func: F) -> ObjectRef<I> {
        match self {
            ObjectRef::Small(small) => ObjectRef::Small(small.map(func)),
            ObjectRef::Large(large) => ObjectRef::Large(large.map(func)),
            ObjectRef::Tree(tree) => ObjectRef::Tree(tree.map(func)),
            ObjectRef::Commit(commit) => ObjectRef::Commit(commit.map(func)),
        }
    }

    pub fn into_inner(self) -> H {
        match self {
            ObjectRef::Small(small) => small.into_inner(),
            ObjectRef::Large(large) => large.into_inner(),
            ObjectRef::Tree(tree) => tree.into_inner(),
            ObjectRef::Commit(commit) => commit.into_inner(),
        }
    }

    pub fn as_inner(&self) -> &H {
        match *self {
            ObjectRef::Small(ref small) => small.as_inner(),
            ObjectRef::Large(ref large) => large.as_inner(),
            ObjectRef::Tree(ref tree) => tree.as_inner(),
            ObjectRef::Commit(ref commit) => commit.as_inner(),
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

    pub fn as_ref(&self) -> ObjectRef<&H> {
        match *self {
            ObjectRef::Small(ref small_ref) => ObjectRef::Small(small_ref.as_ref()),
            ObjectRef::Large(ref large_ref) => ObjectRef::Large(large_ref.as_ref()),
            ObjectRef::Tree(ref tree_ref) => ObjectRef::Tree(tree_ref.as_ref()),
            ObjectRef::Commit(ref commit_ref) => ObjectRef::Commit(commit_ref.as_ref()),
        }
    }
}

impl<B: Backend> ObjectRef<Handle<B>> {
    pub fn fetch(&self) -> FutureObject<B> {
        match *self {
            ObjectRef::Small(ref small_ref) => FutureObject::Small(small_ref.fetch()),
            ObjectRef::Large(ref large_ref) => FutureObject::Large(large_ref.fetch()),
            ObjectRef::Tree(ref tree_ref) => FutureObject::Tree(tree_ref.fetch()),
            ObjectRef::Commit(ref commit_ref) => FutureObject::Commit(commit_ref.fetch()),
        }
    }

    pub fn digest<D: Digest>(&self) -> FutureObjectDigest<D> {
        match *self {
            ObjectRef::Small(ref small_ref) => FutureObjectDigest::Small(small_ref.digest()),
            ObjectRef::Large(ref large_ref) => FutureObjectDigest::Large(large_ref.digest()),
            ObjectRef::Tree(ref tree_ref) => FutureObjectDigest::Tree(tree_ref.digest()),
            ObjectRef::Commit(ref commit_ref) => FutureObjectDigest::Commit(commit_ref.digest()),
        }
    }

    pub fn id(&self) -> FutureObjectId<B> {
        match *self {
            ObjectRef::Small(ref small_ref) => FutureObjectId::Small(small_ref.id()),
            ObjectRef::Large(ref large_ref) => FutureObjectId::Large(large_ref.id()),
            ObjectRef::Tree(ref tree_ref) => FutureObjectId::Tree(tree_ref.id()),
            ObjectRef::Commit(ref commit_ref) => FutureObjectId::Commit(commit_ref.id()),
        }
    }
}

pub enum FutureResolvedDigestObject<B: Backend> {
    Small(FutureResolvedDigestSmall<B>),
    Large(FutureResolvedDigestLarge<B>),
    Tree(FutureResolvedDigestTree<B>),
    Commit(FutureResolvedDigestCommit<B>),
}

impl<B: Backend> Future for FutureResolvedDigestObject<B> {
    type Item = Option<ObjectRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureResolvedDigestObject::Small(ref mut small) => {
                Ok(small.poll()?.map(|opt| opt.map(ObjectRef::Small)))
            }
            FutureResolvedDigestObject::Large(ref mut large) => {
                Ok(large.poll()?.map(|opt| opt.map(ObjectRef::Large)))
            }
            FutureResolvedDigestObject::Tree(ref mut tree) => {
                Ok(tree.poll()?.map(|opt| opt.map(ObjectRef::Tree)))
            }
            FutureResolvedDigestObject::Commit(ref mut commit) => {
                Ok(commit.poll()?.map(|opt| opt.map(ObjectRef::Commit)))
            }
        }
    }
}

pub enum FutureResolvedIdObject<B: Backend> {
    Small(FutureResolvedIdSmall<B>),
    Large(FutureResolvedIdLarge<B>),
    Tree(FutureResolvedIdTree<B>),
    Commit(FutureResolvedIdCommit<B>),
}

impl<B: Backend> Future for FutureResolvedIdObject<B> {
    type Item = Option<ObjectRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureResolvedIdObject::Small(ref mut small) => {
                Ok(small.poll()?.map(|opt| opt.map(ObjectRef::Small)))
            }
            FutureResolvedIdObject::Large(ref mut large) => {
                Ok(large.poll()?.map(|opt| opt.map(ObjectRef::Large)))
            }
            FutureResolvedIdObject::Tree(ref mut tree) => {
                Ok(tree.poll()?.map(|opt| opt.map(ObjectRef::Tree)))
            }
            FutureResolvedIdObject::Commit(ref mut commit) => {
                Ok(commit.poll()?.map(|opt| opt.map(ObjectRef::Commit)))
            }
        }
    }
}

impl<I> ObjectRef<I> {
    pub fn resolve_id<B: Backend>(&self, store: &Store<B>) -> FutureResolvedIdObject<B>
    where
        I: Borrow<B::Id>,
    {
        match *self {
            ObjectRef::Small(ref small_ref) => {
                FutureResolvedIdObject::Small(small_ref.resolve_id(store))
            }
            ObjectRef::Large(ref large_ref) => {
                FutureResolvedIdObject::Large(large_ref.resolve_id(store))
            }
            ObjectRef::Tree(ref tree_ref) => {
                FutureResolvedIdObject::Tree(tree_ref.resolve_id(store))
            }
            ObjectRef::Commit(ref commit_ref) => {
                FutureResolvedIdObject::Commit(commit_ref.resolve_id(store))
            }
        }
    }
}

pub enum FutureObject<B: Backend> {
    Small(FutureSmall<B>),
    Large(FutureLarge<B>),
    Tree(FutureTree<B>),
    Commit(FutureCommit<B>),
}

impl<B: Backend> Future for FutureObject<B> {
    type Item = Object<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureObject::Small(ref mut small) => Ok(small.poll()?.map(Object::Small)),
            FutureObject::Large(ref mut large) => Ok(large.poll()?.map(Object::Large)),
            FutureObject::Tree(ref mut tree) => Ok(tree.poll()?.map(Object::Tree)),
            FutureObject::Commit(ref mut commit) => Ok(commit.poll()?.map(Object::Commit)),
        }
    }
}

pub struct FutureSmall<B: Backend>(FutureContent<B>);

impl<B: Backend> Future for FutureSmall<B> {
    type Item = Small;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready(content) => Ok(Async::Ready(decode::small::<B>(content)?)),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SmallRef<H>(H, u64);

impl<H> SmallRef<H> {
    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn new(size: u64, handle: H) -> Self {
        SmallRef(handle, size)
    }

    pub fn into_inner(self) -> H {
        self.0
    }

    pub fn as_inner(&self) -> &H {
        &self.0
    }

    pub fn map<I, F: FnOnce(H) -> I>(self, func: F) -> SmallRef<I> {
        SmallRef(func(self.0), self.1)
    }

    pub fn as_ref(&self) -> SmallRef<&H> {
        SmallRef(&self.0, self.1)
    }
}

impl<B: Backend> SmallRef<Handle<B>> {
    pub fn fetch(&self) -> FutureSmall<B> {
        FutureSmall(self.0.load())
    }

    pub fn digest<D: Digest>(&self) -> FutureSmallDigest<D> {
        FutureSmallDigest {
            blocking: self.0.digest(),
            size: self.1,
        }
    }

    pub fn id(&self) -> FutureSmallId<B> {
        FutureSmallId {
            blocking: self.0.id(),
            size: self.1,
        }
    }
}

impl<D: Digest> SmallRef<D> {
    pub fn resolve_digest<B: Backend>(&self, store: &Store<B>) -> FutureResolvedDigestSmall<B> {
        FutureResolvedDigestSmall {
            blocking: store.resolve_digest(self.as_inner().clone()),
            size: self.size(),
        }
    }
}

pub struct FutureResolvedDigestSmall<B: Backend> {
    blocking: FutureResolveDigest<B>,
    size: u64,
}

impl<B: Backend> Future for FutureResolvedDigestSmall<B> {
    type Item = Option<SmallRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| SmallRef::new(self.size, handle))))
    }
}

impl<I> SmallRef<I> {
    pub fn resolve_id<B: Backend>(&self, store: &Store<B>) -> FutureResolvedIdSmall<B>
    where
        I: Borrow<B::Id>,
    {
        FutureResolvedIdSmall {
            blocking: store.resolve_id(self.as_inner().borrow()),
            size: self.size(),
        }
    }
}

pub struct FutureResolvedIdSmall<B: Backend> {
    blocking: FutureResolveId<B>,
    size: u64,
}

impl<B: Backend> Future for FutureResolvedIdSmall<B> {
    type Item = Option<SmallRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| SmallRef::new(self.size, handle))))
    }
}

pub struct FutureSmallHandle<B: Backend> {
    blocking: FutureFinish<B>,
    size: u64,
}

impl<B: Backend> Future for FutureSmallHandle<B> {
    type Item = SmallRef<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|handle| SmallRef(handle, self.size)))
    }
}

pub struct FutureSmallDigest<D: Digest> {
    blocking: FutureDigest<D>,
    size: u64,
}

impl<D: Digest> Future for FutureSmallDigest<D> {
    type Item = SmallRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|digest| SmallRef(digest, self.size)))
    }
}

pub struct FutureSmallId<B: Backend> {
    blocking: FutureId<B>,
    size: u64,
}

impl<B: Backend> Future for FutureSmallId<B> {
    type Item = SmallRef<OwnedLocalId<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking.poll()?.map(|id| SmallRef(id, self.size)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Small {
    data: Vec<u8>,
}

impl Deref for Small {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl Small {
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn send<B: Backend>(&self, store: &Store<B>) -> FutureSmallHandle<B> {
        let mut builder = store.builder();
        FutureSmallHandle {
            blocking: Box::new(
                encode::small(&mut builder, self)
                    .map(|()| builder.finish())
                    .into_future()
                    .flatten(),
            ),
            size: self.size(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SmallBuilder(Small);

impl Default for SmallBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for SmallBuilder {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0.data
    }
}

impl DerefMut for SmallBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.data
    }
}

impl Write for SmallBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.data.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Write::flush(&mut self.0.data)
    }
}

impl SmallBuilder {
    pub fn new() -> Self {
        SmallBuilder(Small { data: Vec::new() })
    }

    pub fn as_small(&self) -> &Small {
        &self.0
    }
}

pub struct FutureLarge<B: Backend> {
    blocking: FutureContent<B>,
    size: u64,
    depth: u8,
}

impl<B: Backend> Future for FutureLarge<B> {
    type Item = Large<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.blocking.poll()? {
            Async::Ready(content) => Ok(Async::Ready(decode::large::<B>(
                content,
                self.size,
                self.depth,
            )?)),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LargeRef<H> {
    inner: H,
    size: u64,
    depth: u8,
}

impl<H> LargeRef<H> {
    pub fn new(size: u64, depth: u8, inner: H) -> Self {
        assert!(depth > 0, "All large blobs must have depth > 0!");

        Self { inner, size, depth }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn depth(&self) -> u8 {
        self.depth
    }

    pub fn into_inner(self) -> H {
        self.inner
    }

    pub fn as_inner(&self) -> &H {
        &self.inner
    }

    pub fn map<I, F: FnOnce(H) -> I>(self, func: F) -> LargeRef<I> {
        LargeRef {
            inner: func(self.inner),
            size: self.size,
            depth: self.depth,
        }
    }

    pub fn as_ref(&self) -> LargeRef<&H> {
        LargeRef {
            inner: &self.inner,
            size: self.size,
            depth: self.depth,
        }
    }
}

impl<B: Backend> LargeRef<Handle<B>> {
    pub fn fetch(&self) -> FutureLarge<B> {
        FutureLarge {
            blocking: self.inner.load(),
            size: self.size,
            depth: self.depth,
        }
    }

    pub fn digest<D: Digest>(&self) -> FutureLargeDigest<D> {
        FutureLargeDigest {
            blocking: self.inner.digest(),
            size: self.size,
            depth: self.depth,
        }
    }

    pub fn id(&self) -> FutureLargeId<B> {
        FutureLargeId {
            blocking: self.inner.id(),
            size: self.size,
            depth: self.depth,
        }
    }
}

impl<D: Digest> LargeRef<D> {
    pub fn resolve_digest<B: Backend>(&self, store: &Store<B>) -> FutureResolvedDigestLarge<B> {
        FutureResolvedDigestLarge {
            blocking: store.resolve_digest(self.as_inner().clone()),
            size: self.size,
            depth: self.depth,
        }
    }
}

pub struct FutureResolvedDigestLarge<B: Backend> {
    blocking: FutureResolveDigest<B>,
    size: u64,
    depth: u8,
}

impl<B: Backend> Future for FutureResolvedDigestLarge<B> {
    type Item = Option<LargeRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking.poll()?.map(|opt_handle| {
            opt_handle.map(|handle| LargeRef::new(self.size, self.depth, handle))
        }))
    }
}

impl<I> LargeRef<I> {
    pub fn resolve_id<B: Backend>(&self, store: &Store<B>) -> FutureResolvedIdLarge<B>
    where
        I: Borrow<B::Id>,
    {
        FutureResolvedIdLarge {
            blocking: store.resolve_id(self.as_inner().borrow()),
            size: self.size(),
            depth: self.depth(),
        }
    }
}

pub struct FutureResolvedIdLarge<B: Backend> {
    blocking: FutureResolveId<B>,
    size: u64,
    depth: u8,
}

impl<B: Backend> Future for FutureResolvedIdLarge<B> {
    type Item = Option<LargeRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking.poll()?.map(|opt_handle| {
            opt_handle.map(|handle| LargeRef::new(self.size, self.depth, handle))
        }))
    }
}

pub struct FutureLargeHandle<B: Backend> {
    blocking: FutureFinish<B>,
    size: u64,
    depth: u8,
}

impl<B: Backend> Future for FutureLargeHandle<B> {
    type Item = LargeRef<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|handle| LargeRef::new(self.size, self.depth, handle)))
    }
}

pub struct FutureLargeDigest<D: Digest> {
    blocking: FutureDigest<D>,
    size: u64,
    depth: u8,
}

impl<D: Digest> Future for FutureLargeDigest<D> {
    type Item = LargeRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|digest| LargeRef::new(self.size, self.depth, digest)))
    }
}

pub struct FutureLargeId<B: Backend> {
    blocking: FutureId<B>,
    size: u64,
    depth: u8,
}

impl<B: Backend> Future for FutureLargeId<B> {
    type Item = LargeRef<OwnedLocalId<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|id| LargeRef::new(self.size, self.depth, id)))
    }
}

#[derive(Debug)]
pub struct LargeIntoIter<H> {
    iter: btree_map::IntoIter<u64, (u64, ObjectRef<H>)>,
}

impl<H> Iterator for LargeIntoIter<H> {
    type Item = (Range<u64>, ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(start, (end, objref))| (start..end, objref))
    }
}

#[derive(Debug)]
pub struct LargeIter<'a, H: 'a> {
    iter: btree_map::Iter<'a, u64, (u64, ObjectRef<H>)>,
}

impl<'a, H> Iterator for LargeIter<'a, H> {
    type Item = (Range<u64>, &'a ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(&start, &(end, ref objref))| (start..end, objref))
    }
}

#[derive(Debug)]
pub struct LargeRangeIter<'a, H: 'a> {
    range: btree_map::Range<'a, u64, (u64, ObjectRef<H>)>,
}

impl<'a, H: 'a> Iterator for LargeRangeIter<'a, H> {
    type Item = (Range<u64>, &'a ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .next()
            .map(|(&start, &(end, ref objref))| (start..end, objref))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Large<H> {
    size: u64,
    depth: u8,
    entries: BTreeMap<u64, (u64, ObjectRef<H>)>,
}

impl<H> IntoIterator for Large<H> {
    type Item = (Range<u64>, ObjectRef<H>);
    type IntoIter = LargeIntoIter<H>;

    fn into_iter(self) -> Self::IntoIter {
        LargeIntoIter {
            iter: self.entries.into_iter(),
        }
    }
}

impl<H> Large<H> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn depth(&self) -> u8 {
        self.depth
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn range(&self, range: Range<u64>) -> LargeRangeIter<H> {
        let start = match self.entries.range(..range.start).rev().next() {
            // If there's an overlapping entry which comes before range.start, include it.
            Some((end, _)) if end > &range.start => Bound::Included(end),
            // Otherwise, it's just a standard inclusive bound on range.start.
            Some((_, _)) => Bound::Included(&range.start),
            None => Bound::Unbounded,
        };

        LargeRangeIter {
            range: self.entries.range((start, Bound::Excluded(&range.end))),
        }
    }

    pub fn iter(&self) -> LargeIter<H> {
        LargeIter {
            iter: self.entries.iter(),
        }
    }
}

impl<B: Backend> Large<Handle<B>> {
    pub fn send(&self, store: &Store<B>) -> FutureLargeHandle<B> {
        let mut builder = store.builder();
        FutureLargeHandle {
            blocking: Box::new(
                encode::large(&mut builder, self)
                    .map(|()| builder.finish())
                    .into_future()
                    .flatten(),
            ),
            size: self.size,
            depth: self.depth,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LargeBuilder<H>(Large<H>);

impl<H> LargeBuilder<H> {
    pub fn new(depth: u8) -> Self {
        LargeBuilder(Large {
            size: 0,
            depth,
            entries: BTreeMap::new(),
        })
    }

    pub fn as_large(&self) -> &Large<H> {
        &self.0
    }

    pub fn into_large(self) -> Large<H> {
        self.0
    }

    pub fn push(&mut self, objref: ObjectRef<H>) {
        match objref {
            ObjectRef::Small(ref small_ref) => {
                assert!(self.0.depth == 1);
                let start = self.0.size;
                let end = self.0.size + small_ref.size();
                self.0.size += small_ref.size();
                self.0.entries.insert(start, (end, objref));
            }
            ObjectRef::Large(ref large_ref) => {
                assert!(self.0.depth == large_ref.depth() + 1);
                let start = self.0.size;
                let end = self.0.size + large_ref.size();
                self.0.size += large_ref.size();
                self.0.entries.insert(start, (end, objref));
            }
            _ => unreachable!(),
        }
    }
}

pub struct FutureTree<B: Backend>(FutureContent<B>);

impl<B: Backend> Future for FutureTree<B> {
    type Item = Tree<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready(content) => Ok(Async::Ready(decode::tree::<B>(content)?)),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeRef<H>(H);

impl<H> TreeRef<H> {
    pub fn new(handle: H) -> Self {
        TreeRef(handle)
    }

    pub fn into_inner(self) -> H {
        self.0
    }

    pub fn as_inner(&self) -> &H {
        &self.0
    }

    pub fn map<I, F: FnOnce(H) -> I>(self, func: F) -> TreeRef<I> {
        TreeRef(func(self.0))
    }

    pub fn as_ref(&self) -> TreeRef<&H> {
        TreeRef(&self.0)
    }
}

impl<B: Backend> TreeRef<Handle<B>> {
    pub fn fetch(&self) -> FutureTree<B> {
        FutureTree(self.0.load())
    }

    pub fn digest<D: Digest>(&self) -> FutureTreeDigest<D> {
        FutureTreeDigest(self.0.digest())
    }

    pub fn id(&self) -> FutureTreeId<B> {
        FutureTreeId(self.0.id())
    }
}

impl<D: Digest> TreeRef<D> {
    pub fn resolve_digest<B: Backend>(&self, store: &Store<B>) -> FutureResolvedDigestTree<B> {
        FutureResolvedDigestTree {
            blocking: store.resolve_digest(self.as_inner().clone()),
        }
    }
}

pub struct FutureResolvedDigestTree<B: Backend> {
    blocking: FutureResolveDigest<B>,
}

impl<B: Backend> Future for FutureResolvedDigestTree<B> {
    type Item = Option<TreeRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| TreeRef::new(handle))))
    }
}

impl<I> TreeRef<I> {
    pub fn resolve_id<B: Backend>(&self, store: &Store<B>) -> FutureResolvedIdTree<B>
    where
        I: Borrow<B::Id>,
    {
        FutureResolvedIdTree {
            blocking: store.resolve_id(self.as_inner().borrow()),
        }
    }
}

pub struct FutureResolvedIdTree<B: Backend> {
    blocking: FutureResolveId<B>,
}

impl<B: Backend> Future for FutureResolvedIdTree<B> {
    type Item = Option<TreeRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| TreeRef::new(handle))))
    }
}

pub struct FutureTreeHandle<B: Backend>(FutureFinish<B>);

impl<B: Backend> Future for FutureTreeHandle<B> {
    type Item = TreeRef<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(TreeRef))
    }
}

pub struct FutureTreeDigest<D: Digest>(FutureDigest<D>);

impl<D: Digest> Future for FutureTreeDigest<D> {
    type Item = TreeRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(TreeRef))
    }
}

pub struct FutureTreeId<B: Backend>(FutureId<B>);

impl<B: Backend> Future for FutureTreeId<B> {
    type Item = TreeRef<OwnedLocalId<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(TreeRef))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tree<H> {
    entries: BTreeMap<String, ObjectRef<H>>,
}

impl<H> Deref for Tree<H> {
    type Target = BTreeMap<String, ObjectRef<H>>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<H> IntoIterator for Tree<H> {
    type Item = (String, ObjectRef<H>);
    type IntoIter = ::std::collections::btree_map::IntoIter<String, ObjectRef<H>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<H> Tree<H> {
    pub fn diverge(self) -> TreeBuilder<H> {
        TreeBuilder(self)
    }
}

impl<B: Backend> Tree<Handle<B>> {
    pub fn send(&self, store: &Store<B>) -> FutureTreeHandle<B> {
        let mut builder = store.builder();
        FutureTreeHandle(Box::new(
            encode::tree(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct TreeBuilder<H>(Tree<H>);

impl<H> Default for TreeBuilder<H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H> Deref for TreeBuilder<H> {
    type Target = BTreeMap<String, ObjectRef<H>>;

    fn deref(&self) -> &Self::Target {
        &self.0.entries
    }
}

impl<H> DerefMut for TreeBuilder<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.entries
    }
}

impl<H> TreeBuilder<H> {
    pub fn new() -> Self {
        TreeBuilder(Tree {
            entries: BTreeMap::new(),
        })
    }

    pub fn as_tree(&self) -> &Tree<H> {
        &self.0
    }

    pub fn into_tree(self) -> Tree<H> {
        self.0
    }
}

pub struct FutureCommit<B: Backend>(FutureContent<B>);

impl<B: Backend> Future for FutureCommit<B> {
    type Item = Commit<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready(content) => Ok(Async::Ready(decode::commit::<B>(content)?)),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitRef<H>(H);

impl<H> CommitRef<H> {
    pub fn new(handle: H) -> Self {
        CommitRef(handle)
    }

    pub fn into_inner(self) -> H {
        self.0
    }

    pub fn as_inner(&self) -> &H {
        &self.0
    }

    pub fn map<I, F: FnOnce(H) -> I>(self, func: F) -> CommitRef<I> {
        CommitRef(func(self.0))
    }

    pub fn as_ref(&self) -> CommitRef<&H> {
        CommitRef(&self.0)
    }
}

impl<B: Backend> CommitRef<Handle<B>> {
    pub fn fetch(&self) -> FutureCommit<B> {
        FutureCommit(self.0.load())
    }

    pub fn digest<D: Digest>(&self) -> FutureCommitDigest<D> {
        FutureCommitDigest(self.0.digest())
    }

    pub fn id(&self) -> FutureCommitId<B> {
        FutureCommitId(self.0.id())
    }
}

impl<D: Digest> CommitRef<D> {
    pub fn resolve_digest<B: Backend>(&self, store: &Store<B>) -> FutureResolvedDigestCommit<B> {
        FutureResolvedDigestCommit {
            blocking: store.resolve_digest(self.as_inner().clone()),
        }
    }
}

pub struct FutureResolvedDigestCommit<B: Backend> {
    blocking: FutureResolveDigest<B>,
}

impl<B: Backend> Future for FutureResolvedDigestCommit<B> {
    type Item = Option<CommitRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| CommitRef::new(handle))))
    }
}

impl<I> CommitRef<I> {
    pub fn resolve_id<B: Backend>(&self, store: &Store<B>) -> FutureResolvedIdCommit<B>
    where
        I: Borrow<B::Id>,
    {
        FutureResolvedIdCommit {
            blocking: store.resolve_id(self.as_inner().borrow()),
        }
    }
}

pub struct FutureResolvedIdCommit<B: Backend> {
    blocking: FutureResolveId<B>,
}

impl<B: Backend> Future for FutureResolvedIdCommit<B> {
    type Item = Option<CommitRef<Handle<B>>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| CommitRef::new(handle))))
    }
}

pub struct FutureCommitHandle<B: Backend>(FutureFinish<B>);

impl<B: Backend> Future for FutureCommitHandle<B> {
    type Item = CommitRef<Handle<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(CommitRef))
    }
}

pub struct FutureCommitDigest<D: Digest>(FutureDigest<D>);

impl<D: Digest> Future for FutureCommitDigest<D> {
    type Item = CommitRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(CommitRef))
    }
}

pub struct FutureCommitId<B: Backend>(FutureId<B>);

impl<B: Backend> Future for FutureCommitId<B> {
    type Item = CommitRef<OwnedLocalId<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(CommitRef))
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitAuthor {
    pub name: Option<String>,
    pub mbox: Option<String>,
}

impl CommitAuthor {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Commit<H> {
    subtree: TreeRef<H>,
    parents: Vec<CommitRef<H>>,

    timestamp: DateTime<FixedOffset>,
    author: CommitAuthor,
    message: Option<String>,
}

impl<H> Commit<H> {
    pub fn diverge(self) -> CommitBuilder<H> {
        CommitBuilder::Complete(self)
    }

    pub fn as_subtree(&self) -> &TreeRef<H> {
        &self.subtree
    }

    pub fn as_parents(&self) -> &[CommitRef<H>] {
        &self.parents
    }

    pub fn as_author(&self) -> &CommitAuthor {
        &self.author
    }

    pub fn as_timestamp(&self) -> &DateTime<FixedOffset> {
        &self.timestamp
    }

    pub fn as_message(&self) -> Option<&str> {
        self.message.as_ref().map(String::as_str)
    }
}

impl<B: Backend> Commit<Handle<B>> {
    pub fn send(&self, store: &Store<B>) -> FutureCommitHandle<B> {
        let mut builder = store.builder();
        FutureCommitHandle(Box::new(
            encode::commit(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum CommitBuilder<H> {
    Incomplete {
        parents: Vec<CommitRef<H>>,
        timestamp: DateTime<FixedOffset>,
        author: CommitAuthor,
        message: Option<String>,
    },
    Complete(Commit<H>),
}

impl<H> From<Commit<H>> for CommitBuilder<H> {
    fn from(commit: Commit<H>) -> Self {
        CommitBuilder::Complete(commit)
    }
}

impl<H> Default for CommitBuilder<H> {
    fn default() -> Self {
        CommitBuilder::Incomplete {
            parents: Default::default(),
            timestamp: {
                let local = Local::now();
                local.with_timezone(local.offset())
            },
            author: Default::default(),
            message: Default::default(),
        }
    }
}

impl<H> CommitBuilder<H> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_commit(&self) -> Result<&Commit<H>, Error> {
        match *self {
            CommitBuilder::Complete(ref commit) => Ok(commit),
            CommitBuilder::Incomplete { .. } => bail!("Cannot build commit: no subtree provided!"),
        }
    }

    pub fn into_commit(self) -> Result<Commit<H>, Error> {
        match self {
            CommitBuilder::Complete(commit) => Ok(commit),
            CommitBuilder::Incomplete { .. } => bail!("Cannot build commit: no subtree provided!"),
        }
    }

    pub fn message(&mut self, new_message: String) -> &mut Self {
        match *self {
            CommitBuilder::Complete(ref mut commit) => commit.message = Some(new_message),
            CommitBuilder::Incomplete {
                ref mut message, ..
            } => *message = Some(new_message),
        }
        self
    }

    pub fn parents<I>(&mut self, iterable: I) -> &mut Self
    where
        I: IntoIterator<Item = CommitRef<H>>,
    {
        let parents = match *self {
            CommitBuilder::Complete(ref mut commit) => &mut commit.parents,
            CommitBuilder::Incomplete {
                ref mut parents, ..
            } => parents,
        };
        parents.clear();
        parents.extend(iterable);
        self
    }

    pub fn author(&mut self, new_author: CommitAuthor) -> &mut Self {
        match *self {
            CommitBuilder::Complete(ref mut commit) => commit.author = new_author,
            CommitBuilder::Incomplete { ref mut author, .. } => *author = new_author,
        }
        self
    }

    pub fn subtree(&mut self, new_subtree: TreeRef<H>) -> &mut Self {
        let tmp = match mem::replace(self, CommitBuilder::default()) {
            CommitBuilder::Complete(commit) => Commit {
                subtree: new_subtree,
                ..commit
            },
            CommitBuilder::Incomplete {
                parents,
                author,
                message,
                timestamp,
            } => Commit {
                subtree: new_subtree,
                parents,
                timestamp,
                author,
                message,
            },
        };
        *self = CommitBuilder::Complete(tmp);
        self
    }

    pub fn timestamp(&mut self, new_timestamp: DateTime<FixedOffset>) -> &mut Self {
        match *self {
            CommitBuilder::Complete(ref mut commit) => commit.timestamp = new_timestamp,
            CommitBuilder::Incomplete {
                ref mut timestamp, ..
            } => *timestamp = new_timestamp,
        }
        self
    }
}

pub fn share<R: Read, B: Backend>(
    reader: R,
    store: Store<B>,
) -> impl Future<Item = ObjectRef<Handle<B>>, Error = Error> {
    async_block! {
        let mut splitter = Splitter::new(reader, Parameters::default());

        let mut small_builder = SmallBuilder::new();
        let mut chunks = FuturesOrdered::new();
        while let Some(_) = splitter.find(&mut small_builder)? {
            chunks.push(small_builder.as_small().send(&store));
            small_builder.clear();
        }

        match chunks.len() {
            0 => {
                // No chunks means the reader was empty.
                let small_ref = await!(SmallBuilder::new().as_small().send(&store))?;
                Ok(ObjectRef::Small(small_ref))
            }
            1 => {
                // If we have a single chunk, no need to put it into a "large" chunk.
                let small_ref = await!(chunks.into_future()).map_err(|t| t.0)?.0.unwrap();
                Ok(ObjectRef::Small(small_ref))
            }
            _ => {
                // TODO: "deep" hashsplitting. This is why the "depth" of the `LargeBuilder` is `1`.
                let mut large_builder = LargeBuilder::new(1);

                #[async]
                for chunk in chunks {
                    large_builder.push(ObjectRef::Small(chunk));
                }
                let large_ref = await!(large_builder.as_large().send(&store))?;

                Ok(ObjectRef::Large(large_ref))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::dummy::*;

    use std::io::Cursor;

    use proptest::prelude::*;

    prop_compose! {
        fn arb_small()
                (data in any::<Vec<u8>>()) -> Small {
            Small { data }
        }
    }

    prop_compose! {
        fn arb_small_ref(store: Store<DummyBackend>)
                (size in 0u64..1 << 20, handle in dummy_handle(store)) -> SmallRef<Handle<DummyBackend>> {
            SmallRef(handle, size)
        }
    }

    prop_compose! {
        fn arb_large(store: Store<DummyBackend>)
                (refs in prop::collection::vec(arb_small_ref(store), 1..1024)) -> Large<Handle<DummyBackend>> {
            let mut builder = LargeBuilder::new(1);
            for small_ref in refs {
                builder.push(ObjectRef::Small(small_ref));
            }
            builder.into_large()
        }
    }

    prop_compose! {
        fn arb_large_ref(store: Store<DummyBackend>)
                (size in any::<u64>(), depth in 1u8..255u8, inner in dummy_handle(store)) -> LargeRef<Handle<DummyBackend>> {

            LargeRef { inner, size, depth }
        }
    }

    prop_compose! {
        fn arb_tree(store: Store<DummyBackend>)
                (entries in
                    prop::collection::vec(
                        (
                            ".*",
                            prop_oneof![
                               1 => arb_small_ref(store.clone()).prop_map(ObjectRef::Small),
                               1 => arb_large_ref(store.clone()).prop_map(ObjectRef::Large),
                               1 => arb_tree_ref(store.clone()).prop_map(ObjectRef::Tree)
                            ]
                        ),
                        0..1024,
                    )
                ) -> Tree<Handle<DummyBackend>> {
            let mut tree_builder = TreeBuilder::new();
            for (name, handle) in entries {
                tree_builder.insert(name, handle);
            }
            tree_builder.into_tree()
        }
    }

    prop_compose! {
        fn arb_tree_ref(store: Store<DummyBackend>)
                (handle in dummy_handle(store)) -> TreeRef<Handle<DummyBackend>> {
            TreeRef(handle)
        }
    }

    prop_compose! {
        fn arb_timestamp()
                (seconds in 0i64..8_210_298_412_800,
                 hour_offset in -23..23) -> DateTime<FixedOffset> {
            FixedOffset::east(3600 * hour_offset).timestamp(seconds, 0)
        }
    }

    prop_compose! {
        /// Worth noting: only testing printable characters here in the metadata because we don't
        /// have an RDF N-triples parser/formatter which will correctly deal with
        /// non-printable/non-ASCII characters. TODO: Find or write one.
        fn arb_commit(store: Store<DummyBackend>)
                (subtree in arb_tree_ref(store.clone()),
                 parents in prop::collection::vec(arb_commit_ref(store.clone()), 0..4),
                 name in prop::option::of("[ -~]*"),
                 mbox in prop::option::of("[ -~]*"),
                 timestamp in arb_timestamp(),
                 message in prop::option::of("[ -~]*")) -> Commit<Handle<DummyBackend>> {
            let mut builder = CommitBuilder::new();
            builder.subtree(subtree).parents(parents);
            builder.timestamp(timestamp);
            builder.author(CommitAuthor { name, mbox });

            if let Some(msg) = message {
                builder.message(msg);
            }

            builder.into_commit().unwrap()
        }
    }

    prop_compose! {
        fn arb_commit_ref(store: Store<DummyBackend>)
                (handle in dummy_handle(store)) -> CommitRef<Handle<DummyBackend>> {
            CommitRef(handle)
        }
    }

    proptest! {
         #[test]
         fn roundtrip_small((ref small, ref store) in
                             Just(Store::default()).prop_flat_map(|store|
                                (arb_small(), Just(store.clone())))) {
             let mut builder = store.builder();
             super::encode::small(&mut builder, small).unwrap();
             let battered_small =
                 super::decode::small::<DummyBackend>(DummyContent::new(builder, store.clone()))
                     .unwrap();
             assert_eq!(small, &battered_small);
         }

         #[test]
         fn roundtrip_large((ref large, ref store) in
                             Just(Store::default()).prop_flat_map(|store|
                                (arb_large(store.clone()), Just(store.clone())))) {
             let mut builder = store.builder();
             super::encode::large(&mut builder, large).unwrap();
             let battered_large =
                 super::decode::large::<DummyBackend>(DummyContent::new(builder, store.clone()), large.size(), large.depth())
                     .unwrap();
             assert_eq!(large, &battered_large);
         }

         #[test]
         fn roundtrip_tree((ref tree, ref store) in
                             Just(Store::default()).prop_flat_map(|store|
                                (arb_tree(store.clone()), Just(store.clone())))) {
             let mut builder = store.builder();
             super::encode::tree(&mut builder, tree).unwrap();
             let battered_tree =
                 super::decode::tree::<DummyBackend>(DummyContent::new(builder, store.clone()))
                     .unwrap();
             assert_eq!(tree, &battered_tree);
         }

         #[test]
         fn roundtrip_commit((ref commit, ref store) in
                             Just(Store::default()).prop_flat_map(|store|
                                (arb_commit(store.clone()), Just(store.clone())))) {
             let mut builder = store.builder();
             super::encode::commit(&mut builder, commit).unwrap();
             let battered_commit =
                 super::decode::commit::<DummyBackend>(DummyContent::new(builder, store.clone()))
                     .unwrap();
             assert_eq!(commit, &battered_commit);
         }
    }
}
