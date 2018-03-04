use std::{fmt, iter, any::Any, cmp::Ordering, collections::HashMap, hash::{Hash, Hasher},
          io::{self, Read, Write}, sync::Arc};

use failure::Error;
use futures::{stream, prelude::*, sync::mpsc};

use canonical;
use digest::prelude::*;
use hex::ToHex;

pub type BoxedFuture<T, E> = Box<Future<Item = T, Error = E>>;

pub type FutureContent<B> = BoxedFuture<Content<B>, Error>;
pub type FutureDigest<D> = BoxedFuture<D, Error>;
pub type FutureResolve<B> = BoxedFuture<Option<Handle<B>>, Error>;
pub type FutureLoadBranches<B> = BoxedFuture<HashMap<String, Handle<B>>, Error>;
pub type FutureSwapBranches = BoxedFuture<(), Error>;
pub type FutureFinish<B> = BoxedFuture<Handle<B>, Error>;

const FSCK_CHANNEL_SIZE: usize = 16;

/// Convenience module reexporting all important traits.
pub mod prelude {
    pub use super::{Backend, Builder, Content, FutureContent, FutureDigest, FutureFinish,
                    FutureLoadBranches, FutureResolve, FutureSwapBranches, Handle, Store};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawHandle(pub u64);

#[derive(Debug, Default)]
struct Inner<B: Backend> {
    id: u64,
    backend: B,
}

#[derive(Debug, Default)]
pub struct Store<B: Backend> {
    inner: Arc<Inner<B>>,
}

impl<B: Backend> Clone for Store<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: Backend> Store<B> {
    pub fn builder(&self) -> Builder<B> {
        Builder {
            store: self.clone(),
            builder: self.inner.backend.builder(),
        }
    }

    pub fn resolve<D: Digest>(&self, digest: D) -> FutureResolve<B> {
        let store = self.clone();
        let blocking = async_block! {
            let maybe_id = await!(store.inner.backend.resolve(D::SIGNATURE, digest.as_bytes()))?;
            Ok(maybe_id.map(|id| Handle { store, id }))
        };
        Box::new(blocking)
    }

    pub fn load_branches(&self) -> FutureLoadBranches<B> {
        let store = self.clone();
        let blocking = async_block! {
            let branches = await!(store.inner.backend.load_branches())?;
            let wrapped = branches
                .into_iter()
                .map(|(key, id)| {
                    let handle = Handle {
                        id,
                        store: store.clone(),
                    };
                    (key, handle)
                })
                .collect();
            Ok(wrapped)
        };
        Box::new(blocking)
    }

    pub fn swap_branches(
        &self,
        old: HashMap<String, Handle<B>>,
        new: HashMap<String, Handle<B>>,
    ) -> FutureSwapBranches {
        let store = self.clone();
        let blocking = async_block! {
            let old_stripped = old.into_iter()
                .map(|(key, value)| (key, value.id))
                .collect();
            let new_stripped = new.into_iter()
                .map(|(key, value)| (key, value.id))
                .collect();
            await!(store.inner.backend.swap_branches(old_stripped, new_stripped))?;
            Ok(())
        };
        Box::new(blocking)
    }
}

pub struct Content<B: Backend> {
    store: Store<B>,
    content: B::Content,
}

impl<B: Backend> Read for Content<B> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.content.read(buf)
    }
}

impl<B: Backend> Iterator for Content<B> {
    type Item = Handle<B>;

    fn next(&mut self) -> Option<Self::Item> {
        self.content.next().map(|id| Handle {
            store: self.store.clone(),
            id,
        })
    }
}

#[derive(Debug)]
pub struct Handle<B: Backend> {
    store: Store<B>,
    id: RawHandle,
}

impl<B: Backend> Clone for Handle<B> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            id: self.id,
        }
    }
}

impl<B: Backend> PartialEq for Handle<B> {
    fn eq(&self, rhs: &Self) -> bool {
        self.id == rhs.id && self.store.inner.id == rhs.store.inner.id
    }
}

impl<B: Backend> Eq for Handle<B> {}

impl<B: Backend> PartialOrd for Handle<B> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

impl<B: Backend> Ord for Handle<B> {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.id
            .cmp(&rhs.id)
            .then(self.store.inner.id.cmp(&rhs.store.inner.id))
    }
}

impl<B: Backend> Hash for Handle<B> {
    fn hash<H>(&self, hasher: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(hasher);
        self.store.inner.id.hash(hasher);
    }
}

impl<B: Backend> Handle<B> {
    pub fn load(&self) -> FutureContent<B> {
        let store = self.store.clone();
        let id = self.id;
        let blocking = async_block! {
            let content = await!(store.inner.backend.load(id))?;
            Ok(Content { store, content })
        };
        Box::new(blocking)
    }

    pub fn digest<D: Digest>(&self) -> FutureDigest<D> {
        let store = self.store.clone();
        let id = self.id;
        let blocking = async_block! {
            let any_digest = await!(store.inner.backend.digest(D::SIGNATURE, id))?;
            let digest = any_digest.into_digest::<D>().unwrap();
            Ok(digest)
        };
        Box::new(blocking)
    }
}

pub struct Builder<B: Backend> {
    store: Store<B>,
    builder: B::Builder,
}

impl<B: Backend> Write for Builder<B> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.builder.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.builder.flush()
    }
}

impl<B: Backend> Extend<Handle<B>> for Builder<B> {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Handle<B>>,
    {
        let store_id = self.store.inner.id;
        self.builder.extend(iter.into_iter().map(|handle| {
            assert!(handle.store.inner.id == store_id);
            handle.id
        }));
    }
}

impl<B: Backend> Builder<B> {
    pub fn push(&mut self, handle: Handle<B>) {
        self.extend(iter::once(handle));
    }

    pub fn finish(self) -> FutureFinish<B> {
        let blocking = async_block! {
            let store = self.store;
            let id = await!(store.inner.backend.finish(self.builder))?;
            Ok(Handle {
                store,
                id,
            })
        };
        Box::new(blocking)
    }
}

pub trait ErasedDigest: Send + 'static {
    fn into_digest<D: Digest>(self) -> Option<D>;
}

impl ErasedDigest for Box<Any + Send> {
    fn into_digest<D: Digest>(self) -> Option<D> {
        self.downcast::<D>().ok().map(|boxed| *boxed)
    }
}

impl<D: Digest> ErasedDigest for D {
    fn into_digest<E: Digest>(self) -> Option<E> {
        if D::SIGNATURE == E::SIGNATURE {
            Some(E::from_bytes(self.as_bytes()))
        } else {
            None
        }
    }
}

pub trait Backend: Send + Sync + 'static {
    type Builder: Write + Extend<RawHandle> + 'static;
    type FutureFinish: Future<Item = RawHandle, Error = Error>;
    fn builder(&self) -> Self::Builder;
    fn finish(&self, Self::Builder) -> Self::FutureFinish;

    type Content: Read + Iterator<Item = RawHandle> + 'static;
    type FutureContent: Future<Item = Self::Content, Error = Error>;
    fn load(&self, id: RawHandle) -> Self::FutureContent;

    type Digest: ErasedDigest;
    type FutureDigest: Future<Item = Self::Digest, Error = Error>;
    fn digest(&self, signature: DigestSignature, id: RawHandle) -> Self::FutureDigest;

    type FutureLoadBranches: Future<Item = HashMap<String, RawHandle>, Error = Error>;
    fn load_branches(&self) -> Self::FutureLoadBranches;

    type FutureSwapBranches: Future<Item = (), Error = Error>;
    fn swap_branches(
        &self,
        previous: HashMap<String, RawHandle>,
        new: HashMap<String, RawHandle>,
    ) -> Self::FutureSwapBranches;

    type FutureResolve: Future<Item = Option<RawHandle>, Error = Error>;
    fn resolve(&self, signature: DigestSignature, bytes: &[u8]) -> Self::FutureResolve;
}

trait AnyBuilder: 'static {
    fn as_write(&mut self) -> &mut Write;
    fn do_extend(&mut self, iterator: &mut Iterator<Item = RawHandle>);
    fn into_any(self: Box<Self>) -> Box<Any>;
}

impl<T: Write + Extend<RawHandle> + 'static> AnyBuilder for T {
    fn as_write(&mut self) -> &mut Write {
        self
    }

    fn do_extend(&mut self, iterator: &mut Iterator<Item = RawHandle>) {
        self.extend(iterator);
    }

    fn into_any(self: Box<Self>) -> Box<Any> {
        self
    }
}

pub struct ErasedBuilder {
    boxed: Box<AnyBuilder>,
}

impl Write for ErasedBuilder {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.boxed.as_write().write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.boxed.as_write().flush()
    }
}

impl Extend<RawHandle> for ErasedBuilder {
    fn extend<I>(&mut self, iterable: I)
    where
        I: IntoIterator<Item = RawHandle>,
    {
        self.do_extend(&mut iterable.into_iter());
    }
}

impl ErasedBuilder {
    fn new<T: Write + Extend<RawHandle> + 'static>(builder: T) -> Self {
        Self {
            boxed: Box::new(builder),
        }
    }
}

trait AnyContent: 'static {
    fn as_read(&mut self) -> &mut Read;
    fn as_iterator(&mut self) -> &mut Iterator<Item = RawHandle>;
}

impl<T: Read + Iterator<Item = RawHandle> + 'static> AnyContent for T {
    fn as_read(&mut self) -> &mut Read {
        self
    }

    fn as_iterator(&mut self) -> &mut Iterator<Item = RawHandle> {
        self
    }
}

pub struct ErasedContent {
    boxed: Box<AnyContent>,
}

impl Read for ErasedContent {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.boxed.as_read().read(buf)
    }
}

impl Iterator for ErasedContent {
    type Item = RawHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.boxed.as_iterator().next()
    }
}

impl ErasedContent {
    fn new<T: Read + Iterator<Item = RawHandle> + 'static>(content: T) -> Self {
        Self {
            boxed: Box::new(content),
        }
    }
}

pub struct BoxWrapped<B: Backend> {
    backend: B,
}

impl<B: Backend> Backend for BoxWrapped<B> {
    type Builder = ErasedBuilder;
    type FutureFinish = Box<Future<Item = RawHandle, Error = Error>>;
    fn builder(&self) -> Self::Builder {
        ErasedBuilder::new(self.backend.builder())
    }
    fn finish(&self, builder: ErasedBuilder) -> Self::FutureFinish {
        let builder = *builder.boxed.into_any().downcast::<B::Builder>().unwrap();
        Box::new(self.backend.finish(builder))
    }

    type Content = ErasedContent;
    type FutureContent = Box<Future<Item = Self::Content, Error = Error>>;
    fn load(&self, id: RawHandle) -> Self::FutureContent {
        Box::new(self.backend.load(id).map(ErasedContent::new))
    }

    type Digest = Box<Any + Send>;
    type FutureDigest = Box<Future<Item = Self::Digest, Error = Error>>;
    fn digest(&self, signature: DigestSignature, id: RawHandle) -> Self::FutureDigest {
        Box::new(
            self.backend
                .digest(signature, id)
                .map(|digest| -> Box<Any + Send> { Box::new(digest) }),
        )
    }

    type FutureLoadBranches = Box<Future<Item = HashMap<String, RawHandle>, Error = Error>>;
    fn load_branches(&self) -> Self::FutureLoadBranches {
        Box::new(self.backend.load_branches())
    }

    type FutureSwapBranches = Box<Future<Item = (), Error = Error>>;
    fn swap_branches(
        &self,
        old: HashMap<String, RawHandle>,
        new: HashMap<String, RawHandle>,
    ) -> Self::FutureSwapBranches {
        Box::new(self.backend.swap_branches(old, new))
    }

    type FutureResolve = Box<Future<Item = Option<RawHandle>, Error = Error>>;
    fn resolve(&self, signature: DigestSignature, bytes: &[u8]) -> Self::FutureResolve {
        Box::new(self.backend.resolve(signature, bytes))
    }
}

impl<B: Backend> BoxWrapped<B> {
    fn new(backend: B) -> Self {
        Self { backend }
    }
}

pub struct ErasedBackend {
    boxed: Box<
        Backend<
            Builder = ErasedBuilder,
            FutureFinish = Box<Future<Item = RawHandle, Error = Error>>,
            Content = ErasedContent,
            FutureContent = Box<Future<Item = ErasedContent, Error = Error>>,
            Digest = Box<Any + Send>,
            FutureDigest = Box<Future<Item = Box<Any + Send>, Error = Error>>,
            FutureLoadBranches = Box<Future<Item = HashMap<String, RawHandle>, Error = Error>>,
            FutureSwapBranches = Box<Future<Item = (), Error = Error>>,
            FutureResolve = Box<Future<Item = Option<RawHandle>, Error = Error>>,
        >,
    >,
}

impl Backend for ErasedBackend {
    type Builder = ErasedBuilder;
    type FutureFinish = Box<Future<Item = RawHandle, Error = Error>>;
    fn builder(&self) -> Self::Builder {
        self.boxed.builder()
    }
    fn finish(&self, builder: ErasedBuilder) -> Self::FutureFinish {
        self.boxed.finish(builder)
    }

    type Content = ErasedContent;
    type FutureContent = Box<Future<Item = Self::Content, Error = Error>>;
    fn load(&self, id: RawHandle) -> Self::FutureContent {
        self.boxed.load(id)
    }

    type Digest = Box<Any + Send>;
    type FutureDigest = Box<Future<Item = Self::Digest, Error = Error>>;
    fn digest(&self, signature: DigestSignature, id: RawHandle) -> Self::FutureDigest {
        self.boxed.digest(signature, id)
    }

    type FutureLoadBranches = Box<Future<Item = HashMap<String, RawHandle>, Error = Error>>;
    fn load_branches(&self) -> Self::FutureLoadBranches {
        self.boxed.load_branches()
    }

    type FutureSwapBranches = Box<Future<Item = (), Error = Error>>;
    fn swap_branches(
        &self,
        old: HashMap<String, RawHandle>,
        new: HashMap<String, RawHandle>,
    ) -> Self::FutureSwapBranches {
        self.boxed.swap_branches(old, new)
    }

    type FutureResolve = Box<Future<Item = Option<RawHandle>, Error = Error>>;
    fn resolve(&self, signature: DigestSignature, bytes: &[u8]) -> Self::FutureResolve {
        self.boxed.resolve(signature, bytes)
    }
}

impl ErasedBackend {
    pub fn new<B: Backend>(backend: B) -> ErasedBackend {
        Self {
            boxed: Box::new(BoxWrapped::new(backend)),
        }
    }
}

#[async(boxed)]
pub fn copy<B, C>(root: Handle<B>, target: Store<C>) -> Result<Handle<C>, Error>
where
    B: Backend,
    C: Backend,
{
    let mut content = await!(root.load())?;
    let mut builder = target.builder();

    io::copy(&mut content, &mut builder)?;

    // TODO: buffer?
    let refs = {
        let future_refs = stream::iter_ok(content)
            .and_then(move |r| copy(r, target.clone()))
            .collect();
        await!(future_refs)?
    };
    builder.extend(refs);

    Ok(await!(builder.finish())?)
}

#[derive(Debug, Clone, Copy, Fail)]
pub struct FsckError<D: Digest> {
    pub received: D,
    pub calculated: D,
}

impl<D: Digest> fmt::Display for FsckError<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Fsck error: digest mismatch: calculated ")?;
        self.calculated.as_bytes().write_hex(f)?;
        write!(f, " but received ")?;
        self.received.as_bytes().write_hex(f)?;
        write!(f, " from store")?;

        Ok(())
    }
}

// This is *not* a stream because we want to both put out a stream through it *and* return the
// calculated digest.
#[async(boxed)]
fn do_fsck<D, B>(root: Handle<B>, tx: mpsc::Sender<FsckError<D>>) -> Result<D, Error>
where
    D: Digest,
    B: Backend,
{
    let (mut content, store_digest) = await!(root.load().join(root.digest()))?;

    let mut content_buf = {
        let mut buf = Vec::new();
        content.read_to_end(&mut buf)?;
        buf
    };
    let digests = {
        let iter_tx = tx.clone();
        let future_digests = stream::iter_ok(content)
            .and_then(move |r| do_fsck(r, iter_tx.clone()))
            .collect();
        await!(future_digests)?
    };

    let mut writer = D::writer();
    canonical::encode(&mut writer, &content_buf, &digests)?;
    let checked_digest = writer.finish();

    if store_digest != checked_digest {
        await!(tx.send(FsckError {
            received: store_digest,
            calculated: checked_digest.clone(),
        }))?;
    }

    Ok(checked_digest)
}

#[async_stream(item = FsckError<D>)]
pub fn fsck<D, B>(root: Handle<B>) -> Result<(), Error>
where
    D: Digest,
    B: Backend,
{
    let (tx, rx) = mpsc::channel(FSCK_CHANNEL_SIZE);
    let blocking = do_fsck(root, tx).into_stream();

    // Use `select` here because we want to drive the channel to completion alongside the
    // "blocking" future which is producing the stream.
    #[async]
    for item in rx.map(Some)
        .map_err(|_| unreachable!())
        .select(blocking.map(|_| None))
    {
        match item {
            Some(error) => stream_yield!(error),
            None => return Ok(()),
        }
    }

    Ok(())
}

#[cfg(test)]
pub mod dummy {
    use super::*;

    use std::io::Cursor;

    use proptest::prelude::*;

    #[derive(Debug, Default)]
    pub struct DummyBuilder {
        pub blob: Vec<u8>,
        pub refs: Vec<RawHandle>,
    }

    impl Write for DummyBuilder {
        fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
            self.blob.write(buf)
        }

        fn flush(&mut self) -> Result<(), io::Error> {
            Write::flush(&mut self.blob)
        }
    }

    impl Extend<RawHandle> for DummyBuilder {
        fn extend<I>(&mut self, iterable: I)
        where
            I: IntoIterator<Item = RawHandle>,
        {
            self.refs.extend(iterable);
        }
    }

    #[derive(Debug)]
    pub struct DummyContent {
        blob: Cursor<Vec<u8>>,
        refs: ::std::vec::IntoIter<RawHandle>,
    }

    impl Read for DummyContent {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
            self.blob.read(buf)
        }
    }

    impl Iterator for DummyContent {
        type Item = RawHandle;

        fn next(&mut self) -> Option<Self::Item> {
            self.refs.next()
        }
    }

    impl DummyContent {
        pub fn new(
            builder: Builder<DummyBackend>,
            store: Store<DummyBackend>,
        ) -> Content<DummyBackend> {
            Content {
                store,
                content: Self {
                    blob: Cursor::new(builder.builder.blob),
                    refs: builder.builder.refs.into_iter(),
                },
            }
        }
    }

    #[derive(Debug, Default)]
    pub struct DummyDigestWriter;

    impl Write for DummyDigestWriter {
        fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
            unimplemented!();
        }

        fn flush(&mut self) -> Result<(), io::Error> {
            unimplemented!();
        }
    }

    impl DigestWriter for DummyDigestWriter {
        type Output = DummyDigest;

        fn finish(self) -> Self::Output {
            Default::default()
        }
    }

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct DummyDigest;

    impl Digest for DummyDigest {
        const SIGNATURE: DigestSignature = DigestSignature {
            name: "DUMMY",
            size: 0,
        };

        type Writer = DummyDigestWriter;
        fn writer() -> Self::Writer {
            Default::default()
        }

        fn from_bytes(_: &[u8]) -> Self {
            Default::default()
        }

        fn as_bytes(&self) -> &[u8] {
            Default::default()
        }
    }

    impl ErasedDigest for DummyDigest {
        fn into_digest<D: Digest>(self) -> Option<D> {
            if D::SIGNATURE == Self::SIGNATURE {
                Some(D::from_bytes(self.as_bytes()))
            } else {
                None
            }
        }
    }

    #[derive(Debug, Default)]
    pub struct DummyBackend;

    impl Backend for DummyBackend {
        type Builder = DummyBuilder;
        type FutureFinish = Box<Future<Item = RawHandle, Error = Error>>;
        fn builder(&self) -> Self::Builder {
            DummyBuilder::default()
        }
        fn finish(&self, _: Self::Builder) -> Self::FutureFinish {
            unimplemented!();
        }

        type Content = DummyContent;
        type FutureContent = Box<Future<Item = Self::Content, Error = Error>>;
        fn load(&self, id: RawHandle) -> Self::FutureContent {
            unimplemented!();
        }

        type Digest = DummyDigest;
        type FutureDigest = Box<Future<Item = Self::Digest, Error = Error>>;
        fn digest(&self, signature: DigestSignature, id: RawHandle) -> Self::FutureDigest {
            unimplemented!();
        }

        type FutureLoadBranches = Box<Future<Item = HashMap<String, RawHandle>, Error = Error>>;
        fn load_branches(&self) -> Self::FutureLoadBranches {
            unimplemented!();
        }

        type FutureSwapBranches = Box<Future<Item = (), Error = Error>>;
        fn swap_branches(
            &self,
            previous: HashMap<String, RawHandle>,
            new: HashMap<String, RawHandle>,
        ) -> Self::FutureSwapBranches {
            unimplemented!();
        }

        type FutureResolve = Box<Future<Item = Option<RawHandle>, Error = Error>>;
        fn resolve(&self, signature: DigestSignature, bytes: &[u8]) -> Self::FutureResolve {
            unimplemented!();
        }
    }

    pub fn dummy_handle(store: Store<DummyBackend>) -> BoxedStrategy<Handle<DummyBackend>> {
        any::<u64>()
            .prop_map(move |n| Handle {
                store: store.clone(),
                id: RawHandle(n),
            })
            .boxed()
    }
}
