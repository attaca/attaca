pub mod decode;
pub mod encode;
pub mod metadata;

use std::{collections::{btree_map, BTreeMap, Bound, VecDeque}, io::{self, Write},
          ops::{Deref, DerefMut, Range}};

use failure::Error;
use futures::{future::{Flatten, FutureResult}, prelude::*};

use path::ObjectPath;
use store::{Handle, HandleBuilder, Store};

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

    pub fn send<S>(&self, store: &S) -> FutureObjectRef<S>
    where
        S: Store<Handle = H>,
    {
        match *self {
            Object::Small(ref small) => FutureObjectRef::Small(small.send(store)),
            Object::Large(ref large) => FutureObjectRef::Large(large.send(store)),
            Object::Tree(ref tree) => FutureObjectRef::Tree(tree.send(store)),
            Object::Commit(ref commit) => FutureObjectRef::Commit(commit.send(store)),
        }
    }
}

pub enum FutureObjectRef<S: Store> {
    Small(FutureSmallRef<S>),
    Large(FutureLargeRef<S>),
    Tree(FutureTreeRef<S>),
    Commit(FutureCommitRef<S>),
}

impl<S: Store> Future for FutureObjectRef<S> {
    type Item = ObjectRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureObjectRef::Small(ref mut small) => Ok(small.poll()?.map(ObjectRef::Small)),
            FutureObjectRef::Large(ref mut large) => Ok(large.poll()?.map(ObjectRef::Large)),
            FutureObjectRef::Tree(ref mut tree) => Ok(tree.poll()?.map(ObjectRef::Tree)),
            FutureObjectRef::Commit(ref mut commit) => Ok(commit.poll()?.map(ObjectRef::Commit)),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectRef<H: Handle> {
    Small(SmallRef<H>),
    Large(LargeRef<H>),
    Tree(TreeRef<H>),
    Commit(CommitRef<H>),
}

impl<H: Handle> ObjectRef<H> {
    pub fn into_handle(self) -> H {
        match self {
            ObjectRef::Small(small) => small.into_handle(),
            ObjectRef::Large(large) => large.into_handle(),
            ObjectRef::Tree(tree) => tree.into_handle(),
            ObjectRef::Commit(commit) => commit.into_handle(),
        }
    }

    pub fn as_handle(&self) -> &H {
        match *self {
            ObjectRef::Small(ref small) => small.as_handle(),
            ObjectRef::Large(ref large) => large.as_handle(),
            ObjectRef::Tree(ref tree) => tree.as_handle(),
            ObjectRef::Commit(ref commit) => commit.as_handle(),
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

    pub fn fetch(&self) -> FutureObject<H> {
        match *self {
            ObjectRef::Small(ref small_ref) => FutureObject::Small(small_ref.fetch()),
            ObjectRef::Large(ref large_ref) => FutureObject::Large(large_ref.fetch()),
            ObjectRef::Tree(ref tree_ref) => FutureObject::Tree(tree_ref.fetch()),
            ObjectRef::Commit(ref commit_ref) => FutureObject::Commit(commit_ref.fetch()),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SmallRef<H: Handle>(H);

impl<H: Handle> SmallRef<H> {
    pub fn from_handle(handle: H) -> Self {
        SmallRef(handle)
    }

    pub fn into_handle(self) -> H {
        self.0
    }

    pub fn as_handle(&self) -> &H {
        &self.0
    }

    pub fn fetch(&self) -> FutureSmall<H> {
        FutureSmall(self.0.load())
    }
}

pub struct FutureSmallRef<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
);

impl<S: Store> Future for FutureSmallRef<S> {
    type Item = SmallRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(SmallRef))
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
    pub fn send<S>(&self, store: &S) -> FutureSmallRef<S>
    where
        S: Store,
    {
        let mut builder = store.handle_builder();
        FutureSmallRef(
            encode::small(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
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

pub struct FutureLarge<H: Handle>(H::FutureLoad);

impl<H: Handle> Future for FutureLarge<H> {
    type Item = Large<H>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll()? {
            Async::Ready((content, refs_iter)) => {
                Ok(Async::Ready(decode::large::<H>(content, refs_iter)?))
            }
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LargeRef<H: Handle>(H);

impl<H: Handle> LargeRef<H> {
    pub fn from_handle(handle: H) -> Self {
        LargeRef(handle)
    }

    pub fn into_handle(self) -> H {
        self.0
    }

    pub fn as_handle(&self) -> &H {
        &self.0
    }

    pub fn fetch(&self) -> FutureLarge<H> {
        FutureLarge(self.0.load())
    }
}

pub struct FutureLargeRef<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
);

impl<S: Store> Future for FutureLargeRef<S> {
    type Item = LargeRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(LargeRef))
    }
}

#[derive(Debug)]
pub struct LargeRangeIter<'a, H: Handle> {
    range: btree_map::Range<'a, u64, (u64, ObjectRef<H>)>,
}

impl<'a, H: Handle> Iterator for LargeRangeIter<'a, H> {
    type Item = (Range<u64>, ObjectRef<H>);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .next()
            .map(|(&start, &(end, ref objref))| (start..end, objref.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct Large<H: Handle> {
    entries: BTreeMap<u64, (u64, ObjectRef<H>)>,
}

impl<H: Handle> Large<H> {
    pub fn size(&self) -> u64 {
        match self.entries.range(..).rev().next() {
            Some((&end, _)) => end,
            None => 0,
        }
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

    pub fn send<S>(&self, store: &S) -> FutureLargeRef<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        FutureLargeRef(
            encode::large(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct LargeBuilder<H: Handle>(Large<H>);

impl<H: Handle> LargeBuilder<H> {
    
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeRef<H: Handle>(H);

impl<H: Handle> TreeRef<H> {
    pub fn from_handle(handle: H) -> Self {
        TreeRef(handle)
    }

    pub fn into_handle(self) -> H {
        self.0
    }

    pub fn as_handle(&self) -> &H {
        &self.0
    }

    pub fn fetch(&self) -> FutureTree<H> {
        FutureTree(self.0.load())
    }
}

pub struct FutureTreeRef<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
);

impl<S: Store> Future for FutureTreeRef<S> {
    type Item = TreeRef<S::Handle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?.map(TreeRef))
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

    pub fn send<S>(&self, store: &S) -> FutureTreeRef<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        FutureTreeRef(
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitRef<H: Handle>(H);

impl<H: Handle> CommitRef<H> {
    pub fn from_handle(handle: H) -> Self {
        CommitRef(handle)
    }

    pub fn into_handle(self) -> H {
        self.0
    }

    pub fn as_handle(&self) -> &H {
        &self.0
    }

    pub fn fetch(&self) -> FutureCommit<H> {
        FutureCommit(self.0.load())
    }
}

pub struct FutureCommitRef<S: Store>(
    Flatten<FutureResult<<S::HandleBuilder as HandleBuilder>::FutureHandle, Error>>,
);

impl<S: Store> Future for FutureCommitRef<S> {
    type Item = CommitRef<S::Handle>;
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

    pub fn send<S>(&self, store: &S) -> FutureCommitRef<S>
    where
        S: Store<Handle = H>,
    {
        let mut builder = store.handle_builder();
        FutureCommitRef(
            encode::commit(&mut builder, self)
                .map(|()| builder.finish())
                .into_future()
                .flatten(),
        )
    }
}
