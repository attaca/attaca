pub mod decode;
pub mod encode;
pub mod metadata;

use std::{collections::{btree_map, BTreeMap, Bound, VecDeque}, io::{self, Read, Write},
          ops::{Deref, DerefMut, Range}};

use failure::Error;
use futures::{future::{Flatten, FutureResult}, prelude::*, stream::FuturesOrdered};

use digest::Digest;
use path::ObjectPath;
use split::{Parameters, Splitter};
use store::{Handle, HandleBuilder, HandleDigest, Store};

use self::metadata::Metadata;

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

    pub fn send<S>(&self, store: &S) -> FutureObjectHandle<S>
    where
        S: Store<Handle = H>,
    {
        match *self {
            Object::Small(ref small) => FutureObjectHandle::Small(small.send(store)),
            Object::Large(ref large) => FutureObjectHandle::Large(large.send(store)),
            Object::Tree(ref tree) => FutureObjectHandle::Tree(tree.send(store)),
            Object::Commit(ref commit) => FutureObjectHandle::Commit(commit.send(store)),
        }
    }
}

pub enum FutureObjectHandle<S: Store> {
    Small(FutureSmallHandle<S>),
    Large(FutureLargeHandle<S>),
    Tree(FutureTreeHandle<S>),
    Commit(FutureCommitHandle<S>),
}

impl<S: Store> Future for FutureObjectHandle<S> {
    type Item = ObjectRef<S::Handle>;
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

pub enum FutureResolvedObject<S: Store> {
    Small(FutureResolvedSmall<S>),
    Large(FutureResolvedLarge<S>),
    Tree(FutureResolvedTree<S>),
    Commit(FutureResolvedCommit<S>),
}

impl<S: Store> Future for FutureResolvedObject<S> {
    type Item = Option<ObjectRef<S::Handle>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureResolvedObject::Small(ref mut small) => {
                Ok(small.poll()?.map(|opt| opt.map(ObjectRef::Small)))
            }
            FutureResolvedObject::Large(ref mut large) => {
                Ok(large.poll()?.map(|opt| opt.map(ObjectRef::Large)))
            }
            FutureResolvedObject::Tree(ref mut tree) => {
                Ok(tree.poll()?.map(|opt| opt.map(ObjectRef::Tree)))
            }
            FutureResolvedObject::Commit(ref mut commit) => {
                Ok(commit.poll()?.map(|opt| opt.map(ObjectRef::Commit)))
            }
        }
    }
}

pub enum FutureObjectDigest<D: Digest, H: HandleDigest<D>> {
    Small(FutureSmallDigest<D, H>),
    Large(FutureLargeDigest<D, H>),
    Tree(FutureTreeDigest<D, H>),
    Commit(FutureCommitDigest<D, H>),
}

impl<D: Digest, H: HandleDigest<D>> Future for FutureObjectDigest<D, H> {
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
}

impl<H: Handle> ObjectRef<H> {
    pub fn fetch(&self) -> FutureObject<H> {
        match *self {
            ObjectRef::Small(ref small_ref) => FutureObject::Small(small_ref.fetch()),
            ObjectRef::Large(ref large_ref) => FutureObject::Large(large_ref.fetch()),
            ObjectRef::Tree(ref tree_ref) => FutureObject::Tree(tree_ref.fetch()),
            ObjectRef::Commit(ref commit_ref) => FutureObject::Commit(commit_ref.fetch()),
        }
    }

    pub fn digest<D: Digest>(&self) -> FutureObjectDigest<D, H>
    where
        H: HandleDigest<D>,
    {
        match *self {
            ObjectRef::Small(ref small_ref) => FutureObjectDigest::Small(small_ref.digest()),
            ObjectRef::Large(ref large_ref) => FutureObjectDigest::Large(large_ref.digest()),
            ObjectRef::Tree(ref tree_ref) => FutureObjectDigest::Tree(tree_ref.digest()),
            ObjectRef::Commit(ref commit_ref) => FutureObjectDigest::Commit(commit_ref.digest()),
        }
    }
}

impl<D: Digest> ObjectRef<D> {
    pub fn resolve<S: Store>(&self, store: &S) -> FutureResolvedObject<S>
    where
        S::Handle: HandleDigest<D>,
    {
        match *self {
            ObjectRef::Small(ref small_ref) => {
                FutureResolvedObject::Small(small_ref.resolve(store))
            }
            ObjectRef::Large(ref large_ref) => {
                FutureResolvedObject::Large(large_ref.resolve(store))
            }
            ObjectRef::Tree(ref tree_ref) => FutureResolvedObject::Tree(tree_ref.resolve(store)),
            ObjectRef::Commit(ref commit_ref) => {
                FutureResolvedObject::Commit(commit_ref.resolve(store))
            }
        }
    }
}

pub enum FutureObject<H: Handle> {
    Small(FutureSmall<H>),
    Large(FutureLarge<H>),
    Tree(FutureTree<H>),
    Commit(FutureCommit<H>),
}

impl<H: Handle> Future for FutureObject<H> {
    type Item = Object<H>;
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

pub struct FutureSmall<H: Handle>(H::FutureLoad);

impl<H: Handle> Future for FutureSmall<H> {
    type Item = Small;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready((content, _)) => Ok(Async::Ready(decode::small::<H>(content)?)),
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
}

impl<H: Handle> SmallRef<H> {
    pub fn fetch(&self) -> FutureSmall<H> {
        FutureSmall(self.0.load())
    }

    pub fn digest<D: Digest>(&self) -> FutureSmallDigest<D, H>
    where
        H: HandleDigest<D>,
    {
        FutureSmallDigest {
            blocking: self.0.digest(),
            size: self.1,
        }
    }
}

impl<D: Digest> SmallRef<D> {
    pub fn resolve<S: Store>(&self, store: &S) -> FutureResolvedSmall<S>
    where
        S::Handle: HandleDigest<D>,
    {
        FutureResolvedSmall {
            blocking: store.resolve(self.as_inner()),
            size: self.size(),
        }
    }
}

pub struct FutureResolvedSmall<S: Store> {
    blocking: S::FutureResolve,
    size: u64,
}

impl<S: Store> Future for FutureResolvedSmall<S> {
    type Item = Option<SmallRef<S::Handle>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| SmallRef::new(self.size, handle))))
    }
}

pub struct FutureSmallHandle<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
    u64,
);

impl<S: Store> Future for FutureSmallHandle<S> {
    type Item = SmallRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(|handle| SmallRef(handle, self.1)))
    }
}

pub struct FutureSmallDigest<D: Digest, H: HandleDigest<D>> {
    blocking: H::FutureDigest,
    size: u64,
}

impl<D: Digest, H: HandleDigest<D>> Future for FutureSmallDigest<D, H> {
    type Item = SmallRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|digest| SmallRef(digest, self.size)))
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

    pub fn send<S>(&self, store: &S) -> FutureSmallHandle<S>
    where
        S: Store,
    {
        let mut builder = store.handle_builder();
        FutureSmallHandle(
            encode::small(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
            self.size(),
        )
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

pub struct FutureLarge<H: Handle> {
    blocking: H::FutureLoad,
    size: u64,
    depth: u8,
}

impl<H: Handle> Future for FutureLarge<H> {
    type Item = Large<H>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.blocking.poll()? {
            Async::Ready((content, refs_iter)) => Ok(Async::Ready(decode::large::<H>(
                content,
                refs_iter,
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
}

impl<H: Handle> LargeRef<H> {
    pub fn fetch(&self) -> FutureLarge<H> {
        FutureLarge {
            blocking: self.inner.load(),
            size: self.size,
            depth: self.depth,
        }
    }

    pub fn digest<D: Digest>(&self) -> FutureLargeDigest<D, H>
    where
        H: HandleDigest<D>,
    {
        FutureLargeDigest {
            blocking: self.inner.digest(),
            size: self.size,
            depth: self.depth,
        }
    }
}

impl<D: Digest> LargeRef<D> {
    pub fn resolve<S: Store>(&self, store: &S) -> FutureResolvedLarge<S>
    where
        S::Handle: HandleDigest<D>,
    {
        FutureResolvedLarge {
            blocking: store.resolve(self.as_inner()),
            size: self.size,
            depth: self.depth,
        }
    }
}

pub struct FutureResolvedLarge<S: Store> {
    blocking: S::FutureResolve,
    size: u64,
    depth: u8,
}

impl<S: Store> Future for FutureResolvedLarge<S> {
    type Item = Option<LargeRef<S::Handle>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking.poll()?.map(|opt_handle| {
            opt_handle.map(|handle| LargeRef::new(self.size, self.depth, handle))
        }))
    }
}

pub struct FutureLargeHandle<S: Store> {
    blocking: Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
    size: u64,
    depth: u8,
}

impl<S: Store> Future for FutureLargeHandle<S> {
    type Item = LargeRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|handle| LargeRef::new(self.size, self.depth, handle)))
    }
}

pub struct FutureLargeDigest<D: Digest, H: HandleDigest<D>> {
    blocking: H::FutureDigest,
    size: u64,
    depth: u8,
}

impl<D: Digest, H: HandleDigest<D>> Future for FutureLargeDigest<D, H> {
    type Item = LargeRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|digest| LargeRef::new(self.size, self.depth, digest)))
    }
}

#[derive(Debug)]
pub struct LargeIntoIter<H: Handle> {
    iter: btree_map::IntoIter<u64, (u64, ObjectRef<H>)>,
}

impl<H: Handle> Iterator for LargeIntoIter<H> {
    type Item = (Range<u64>, ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(start, (end, objref))| (start..end, objref))
    }
}

#[derive(Debug)]
pub struct LargeIter<'a, H: Handle> {
    iter: btree_map::Iter<'a, u64, (u64, ObjectRef<H>)>,
}

impl<'a, H: Handle> Iterator for LargeIter<'a, H> {
    type Item = (Range<u64>, &'a ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(&start, &(end, ref objref))| (start..end, objref))
    }
}

#[derive(Debug)]
pub struct LargeRangeIter<'a, H: Handle> {
    range: btree_map::Range<'a, u64, (u64, ObjectRef<H>)>,
}

impl<'a, H: Handle> Iterator for LargeRangeIter<'a, H> {
    type Item = (Range<u64>, &'a ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .next()
            .map(|(&start, &(end, ref objref))| (start..end, objref))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Large<H: Handle> {
    size: u64,
    depth: u8,
    entries: BTreeMap<u64, (u64, ObjectRef<H>)>,
}

impl<H: Handle> IntoIterator for Large<H> {
    type Item = (Range<u64>, ObjectRef<H>);
    type IntoIter = LargeIntoIter<H>;

    fn into_iter(self) -> Self::IntoIter {
        LargeIntoIter {
            iter: self.entries.into_iter(),
        }
    }
}

impl<H: Handle> Large<H> {
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

    pub fn send<S>(&self, store: &S) -> FutureLargeHandle<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        FutureLargeHandle {
            blocking: encode::large(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
            size: self.size,
            depth: self.depth,
        }
    }

    pub fn iter(&self) -> LargeIter<H> {
        LargeIter {
            iter: self.entries.iter(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LargeBuilder<H: Handle>(Large<H>);

impl<H: Handle> LargeBuilder<H> {
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
                self.0.entries.insert(start, (end, objref));
            }
            ObjectRef::Large(ref large_ref) => {
                assert!(self.0.depth == large_ref.depth() + 1);
                let start = self.0.size;
                let end = self.0.size + large_ref.size();
                self.0.entries.insert(start, (end, objref));
            }
            _ => unreachable!(),
        }
    }
}

pub struct FutureTree<H: Handle>(H::FutureLoad);

impl<H: Handle> Future for FutureTree<H> {
    type Item = Tree<H>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready((content, refs_iter)) => {
                Ok(Async::Ready(decode::tree::<H>(content, refs_iter)?))
            }
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
}

impl<H: Handle> TreeRef<H> {
    pub fn fetch(&self) -> FutureTree<H> {
        FutureTree(self.0.load())
    }

    pub fn digest<D: Digest>(&self) -> FutureTreeDigest<D, H>
    where
        H: HandleDigest<D>,
    {
        FutureTreeDigest(self.0.digest())
    }
}

impl<D: Digest> TreeRef<D> {
    pub fn resolve<S: Store>(&self, store: &S) -> FutureResolvedTree<S>
    where
        S::Handle: HandleDigest<D>,
    {
        FutureResolvedTree {
            blocking: store.resolve(self.as_inner()),
        }
    }
}

pub struct FutureResolvedTree<S: Store> {
    blocking: S::FutureResolve,
}

impl<S: Store> Future for FutureResolvedTree<S> {
    type Item = Option<TreeRef<S::Handle>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| TreeRef::new(handle))))
    }
}

pub struct FutureTreeHandle<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
);

impl<S: Store> Future for FutureTreeHandle<S> {
    type Item = TreeRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(TreeRef))
    }
}

pub struct FutureTreeDigest<D: Digest, H: HandleDigest<D>>(H::FutureDigest);

impl<D: Digest, H: HandleDigest<D>> Future for FutureTreeDigest<D, H> {
    type Item = TreeRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(TreeRef))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tree<H: Handle> {
    entries: BTreeMap<String, ObjectRef<H>>,
}

impl<H: Handle> Deref for Tree<H> {
    type Target = BTreeMap<String, ObjectRef<H>>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<H: Handle> IntoIterator for Tree<H> {
    type Item = (String, ObjectRef<H>);
    type IntoIter = ::std::collections::btree_map::IntoIter<String, ObjectRef<H>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<H: Handle> Tree<H> {
    pub fn diverge(self) -> TreeBuilder<H> {
        TreeBuilder(self)
    }

    pub fn files(self) -> Box<Stream<Item = (ObjectPath, ObjectRef<H>), Error = Error>> {
        let stream = async_stream_block! {
            let mut streams = VecDeque::new();
            for (name, objref) in self.entries {
                match objref {
                    ObjectRef::Tree(tree_ref) => {
                        let path = ObjectPath::new().push_back(name);
                        let stream_future = async_block! {
                            match await!(ObjectRef::Tree(tree_ref).fetch())? {
                                Object::Tree(tree_obj) => Ok(tree_obj.files()),
                                _ => unreachable!(),
                            }
                        };
                        streams.push_back((path, stream_future.flatten_stream()))
                    }
                    other => stream_yield!((ObjectPath::new().push_back(name), other)),
                }
            }

            while let Some((prefix, mut stream)) = streams.pop_front() {
                match stream.poll()? {
                    // If the stream yields readily, yield the item and then keep it at the front
                    // of the deque since it may be ready to yield more.
                    Async::Ready(Some((postfix, objref))) => {
                        stream_yield!((&prefix + &postfix, objref));
                        streams.push_front((prefix, stream));
                    }
                    // If the stream yields none, don't put it back in the deque. It's finished.
                    Async::Ready(None) => {}
                    // If the stream isn't ready, throw it to the back of the deque.
                    Async::NotReady => streams.push_back((prefix, stream)),
                }
            }

            Ok(())
        };

        Box::new(stream)
    }

    pub fn send<S>(&self, store: &S) -> FutureTreeHandle<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        FutureTreeHandle(
            encode::tree(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct TreeBuilder<H: Handle>(Tree<H>);

impl<H: Handle> Default for TreeBuilder<H> {
    fn default() -> Self {
        Self::new()
    }
}

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

    pub fn into_tree(self) -> Tree<H> {
        self.0
    }
}

pub struct FutureCommit<H: Handle>(H::FutureLoad);

impl<H: Handle> Future for FutureCommit<H> {
    type Item = Commit<H>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready((content, refs_iter)) => {
                Ok(Async::Ready(decode::commit::<H>(content, refs_iter)?))
            }
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
}

impl<H: Handle> CommitRef<H> {
    pub fn fetch(&self) -> FutureCommit<H> {
        FutureCommit(self.0.load())
    }

    pub fn digest<D: Digest>(&self) -> FutureCommitDigest<D, H>
    where
        H: HandleDigest<D>,
    {
        FutureCommitDigest(self.0.digest())
    }
}

impl<D: Digest> CommitRef<D> {
    pub fn resolve<S: Store>(&self, store: &S) -> FutureResolvedCommit<S>
    where
        S::Handle: HandleDigest<D>,
    {
        FutureResolvedCommit {
            blocking: store.resolve(self.as_inner()),
        }
    }
}

pub struct FutureResolvedCommit<S: Store> {
    blocking: S::FutureResolve,
}

impl<S: Store> Future for FutureResolvedCommit<S> {
    type Item = Option<CommitRef<S::Handle>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking
            .poll()?
            .map(|opt_handle| opt_handle.map(|handle| CommitRef::new(handle))))
    }
}

pub struct FutureCommitHandle<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
);

impl<S: Store> Future for FutureCommitHandle<S> {
    type Item = CommitRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(CommitRef))
    }
}

pub struct FutureCommitDigest<D: Digest, H: HandleDigest<D>>(H::FutureDigest);

impl<D: Digest, H: HandleDigest<D>> Future for FutureCommitDigest<D, H> {
    type Item = CommitRef<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(CommitRef))
    }
}

#[derive(Debug, Clone)]
pub struct Commit<H: Handle> {
    subtree: TreeRef<H>,
    parents: Vec<CommitRef<H>>,
    metadata: Metadata<H>,
}

impl<H: Handle> Commit<H> {
    pub fn as_subtree(&self) -> &TreeRef<H> {
        &self.subtree
    }

    pub fn as_parents(&self) -> &[CommitRef<H>] {
        &self.parents
    }

    pub fn send<S>(&self, store: &S) -> FutureCommitHandle<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        FutureCommitHandle(
            encode::commit(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
        )
    }
}

pub fn share<R: Read, S: Store>(
    reader: R,
    store: S,
) -> impl Future<Item = ObjectRef<S::Handle>, Error = Error> {
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

    use std::io::Cursor;

    use proptest::prelude::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct TestHandle(u64);

    impl Handle for TestHandle {
        type Content = Cursor<Vec<u8>>;
        type Refs = ::std::vec::IntoIter<Self>;

        type FutureLoad = FutureResult<(Self::Content, Self::Refs), Error>;
        fn load(&self) -> Self::FutureLoad {
            unimplemented!();
        }
    }

    #[derive(Default)]
    struct TestBuilder {
        data: Vec<u8>,
        refs: Vec<TestHandle>,
    }

    impl Write for TestBuilder {
        fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
            self.data.write(buf)
        }

        fn flush(&mut self) -> Result<(), io::Error> {
            Write::flush(&mut self.data)
        }
    }

    impl HandleBuilder for TestBuilder {
        type Handle = TestHandle;

        fn add_reference(&mut self, handle: Self::Handle) {
            self.refs.push(handle);
        }

        type FutureHandle = FutureResult<TestHandle, Error>;
        fn finish(self) -> Self::FutureHandle {
            unimplemented!();
        }
    }

    impl TestBuilder {
        fn destruct(self) -> (Cursor<Vec<u8>>, ::std::vec::IntoIter<TestHandle>) {
            (Cursor::new(self.data), self.refs.into_iter())
        }
    }

    prop_compose! {
        fn arb_handle()
                (id in any::<u64>()) -> TestHandle {
            TestHandle(id)
        }
    }

    prop_compose! {
        fn arb_small()
                (data in any::<Vec<u8>>()) -> Small {
            Small { data }
        }
    }

    prop_compose! {
        fn arb_small_ref()
                (size in any::<u64>(), handle in arb_handle()) -> SmallRef<TestHandle> {
            SmallRef(handle, size)
        }
    }

    prop_compose! {
        fn arb_large()
                (refs in prop::collection::vec(arb_small_ref(), 1..1024)) -> Large<TestHandle> {
            let mut builder = LargeBuilder::new(1);
            for small_ref in refs {
                builder.push(ObjectRef::Small(small_ref));
            }
            builder.into_large()
        }
    }

    prop_compose! {
        fn arb_large_ref()
                (size in any::<u64>(), depth in 1u8..255u8, inner in arb_handle()) -> LargeRef<TestHandle> {

            LargeRef { inner, size, depth }
        }
    }

    prop_compose! {
        fn arb_tree()
                (entries in
                    prop::collection::vec(
                        (
                            ".*",
                            prop_oneof![
                               1 => arb_small_ref().prop_map(ObjectRef::Small),
                               1 => arb_large_ref().prop_map(ObjectRef::Large),
                               1 => arb_tree_ref().prop_map(ObjectRef::Tree)
                            ]
                        ),
                        0..1024,
                    )
                ) -> Tree<TestHandle> {
            let mut tree_builder = TreeBuilder::new();
            for (name, handle) in entries {
                tree_builder.insert(name, handle);
            }
            tree_builder.into_tree()
        }
    }

    prop_compose! {
        fn arb_tree_ref()
                (handle in arb_handle()) -> TreeRef<TestHandle> {
            TreeRef(handle)
        }
    }

    proptest! {
        #[test]
        fn roundtrip_small(ref small in arb_small()) {
            let mut builder = TestBuilder::default();
            super::encode::small(&mut builder, small).unwrap();
            let (content, _) = builder.destruct();
            let battered_small = super::decode::small::<TestHandle>(content).unwrap();
            assert_eq!(small, &battered_small);
        }

        #[test]
        fn roundtrip_large(ref large in arb_large()) {
            let mut builder = TestBuilder::default();
            super::encode::large(&mut builder, large).unwrap();
            let (content, refs) = builder.destruct();
            let battered_large = super::decode::large::<TestHandle>(content, refs, large.size(), large.depth()).unwrap();
            assert_eq!(large, &battered_large);
        }

        #[test]
        fn roundtrip_tree(ref tree in arb_tree()) {
            let mut builder = TestBuilder::default();
            super::encode::tree(&mut builder, tree).unwrap();
            let (content, refs) = builder.destruct();
            let battered_tree = super::decode::tree::<TestHandle>(content, refs).unwrap();
            assert_eq!(tree, &battered_tree);
        }
    }
}
