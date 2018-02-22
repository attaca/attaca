pub mod battery;

use std::{cell::Cell, io::{self, BufRead, Cursor, Read, Write}, path::Path, sync::Arc};

use attaca::{Handle, HandleBuilder, HandleDigest, Open, Store, digest::Digest};
use chashmap::CHashMap;
use failure::Error;
use futures::future::{self, FutureResult};
use leb128;
use owning_ref::ArcRef;

#[derive(Debug)]
pub struct DummyHandleBuilder {
    store_objects: Arc<CHashMap<Vec<u8>, DummyHandle>>,
    inner: DummyHandleInner,
}

impl Write for DummyHandleBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.content.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Write::flush(&mut self.inner.content)
    }
}

impl HandleBuilder for DummyHandleBuilder {
    type Handle = DummyHandle;

    fn add_reference(&mut self, handle: Self::Handle) {
        self.inner.refs.push(handle);
    }

    type FutureHandle = FutureResult<Self::Handle, Error>;
    fn finish(self) -> Self::FutureHandle {
        future::ok(DummyHandle {
            inner: Arc::new(self.inner),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct DummyStore {
    objects: Arc<CHashMap<Vec<u8>, DummyHandle>>,
    branches: Arc<CHashMap<String, DummyHandle>>,
}

impl Open for DummyStore {
    const SCHEMES: &'static [&'static str] = &[];

    fn open(_s: &str) -> Result<Self, Error> {
        Ok(Self::new())
    }

    fn open_path(_path: &Path) -> Result<Self, Error> {
        Ok(Self::new())
    }
}

impl Store for DummyStore {
    type Handle = DummyHandle;

    type HandleBuilder = DummyHandleBuilder;
    fn handle_builder(&self) -> Self::HandleBuilder {
        DummyHandleBuilder {
            store_objects: self.objects.clone(),
            inner: DummyHandleInner::default(),
        }
    }

    type FutureLoadBranch = FutureResult<Option<Self::Handle>, Error>;
    fn load_branch(&self, branch: String) -> Self::FutureLoadBranch {
        future::ok(self.branches.get(&branch).map(|guard| (*guard).clone()))
    }

    type FutureSwapBranch = FutureResult<(), Error>;
    fn swap_branch(
        &self,
        branch: String,
        previous: Option<Self::Handle>,
        new: Option<Self::Handle>,
    ) -> Self::FutureSwapBranch {
        let flag = Cell::new(false);

        self.branches.alter(branch, |handle_opt| {
            if handle_opt == previous {
                flag.set(true);
                new
            } else {
                handle_opt
            }
        });

        if flag.into_inner() {
            future::ok(())
        } else {
            future::err(format_err!("Failed to swap: previous value did not match!"))
        }
    }

    type FutureResolve = FutureResult<Option<Self::Handle>, Error>;
    fn resolve<D: Digest>(&self, digest: &D) -> Self::FutureResolve
    where
        Self::Handle: HandleDigest<D>,
    {
        let mut buf = Vec::with_capacity(128);
        buf.extend(D::NAME.as_bytes());
        leb128::write::unsigned(&mut buf, D::SIZE as u64).unwrap();
        buf.extend(digest.as_bytes());
        future::ok(self.objects.get(&buf).map(|guard| (*guard).clone()))
    }
}

impl DummyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct DummyContent {
    cursor: Cursor<ArcRef<DummyHandleInner, [u8]>>,
}

impl DummyContent {
    fn new(handle: &DummyHandle) -> Self {
        Self {
            cursor: Cursor::new(
                ArcRef::new(handle.inner.clone()).map(|inner| inner.content.as_slice()),
            ),
        }
    }
}

impl Read for DummyContent {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl BufRead for DummyContent {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.cursor.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.cursor.consume(amt);
    }
}

#[derive(Debug)]
pub struct DummyRefs {
    refs: ArcRef<DummyHandleInner, [DummyHandle]>,
}

impl Iterator for DummyRefs {
    type Item = DummyHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.refs.first().cloned().map(|handle| {
            self.refs = self.refs
                .clone()
                .map(|slice| slice.split_first().unwrap().1);
            handle
        })
    }
}

impl DummyRefs {
    fn new(handle: &DummyHandle) -> Self {
        Self {
            refs: ArcRef::new(handle.inner.clone()).map(|inner| inner.refs.as_slice()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct DummyHandleInner {
    content: Vec<u8>,
    refs: Vec<DummyHandle>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DummyHandle {
    inner: Arc<DummyHandleInner>,
}

impl Handle for DummyHandle {
    type Content = DummyContent;
    type Refs = DummyRefs;

    type FutureLoad = FutureResult<(Self::Content, Self::Refs), Error>;
    fn load(&self) -> Self::FutureLoad {
        future::ok((DummyContent::new(self), DummyRefs::new(self)))
    }
}
