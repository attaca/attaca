use std::{fmt, mem, boxed::FnBox, cell::Cell, cmp::Ordering, hash::{Hash, Hasher},
          io::{self, Cursor, Read, Write}, sync::{Arc, Mutex, Weak}, thread::{self, JoinHandle},
          time::Duration};

use atom::AtomSetOnce;
use chashmap::CHashMap;
use etcd::Client;
use failure::{self, Compat, Error, ResultExt, SyncFailure};
use futures::{future::{self, Either, Executor, FutureResult}, prelude::*,
              stream::FuturesUnordered, sync::{mpsc, oneshot}};
use hex;
use hyper::client::HttpConnector;
use leb128;
use owning_ref::{ArcRef, VecRefMut};
use rad::{rados, Connection, Context};
use tokio_core::reactor::Core;

use canonical::{self, Item};
use digest::{Digest, DigestWriter, Sha3Digest};
use memo::{FutureMap, FutureValue};
use store::{Handle, HandleBuilder, HandleDigest, Store};

#[derive(Debug, Clone)]
pub struct ClusterStore {
    inner: Arc<StoreInner>,
}

impl Store for ClusterStore {
    type Handle = ClusterHandle;

    type HandleBuilder = ClusterHandleBuilder;
    fn handle_builder(&self) -> Self::HandleBuilder {
        ClusterHandleBuilder {
            store: self.inner.clone(),

            blob: Vec::new(),
            refs: Vec::new(),
        }
    }

    type FutureLoadBranch = FutureResult<Option<Self::Handle>, Error>;
    fn load_branch(&self, _branch: String) -> Self::FutureLoadBranch {
        unimplemented!();
    }

    type FutureSwapBranch = FutureResult<(), Error>;
    fn swap_branch(
        &self,
        _branch: String,
        _previous: Option<Self::Handle>,
        _new: Self::Handle,
    ) -> Self::FutureSwapBranch {
        unimplemented!();
    }

    type FutureResolve = FutureResult<Option<Self::Handle>, Error>;
    fn resolve<D: Digest>(&self, _digest: &D) -> Self::FutureResolve
    where
        Self::Handle: HandleDigest<D>,
    {
        unimplemented!();
    }
}

struct StoreInner {
    conn: Arc<Mutex<Connection>>,
    pool: String,

    ctx: Arc<Mutex<Context>>,

    handles: CHashMap<Sha3Digest, ClusterHandle>,
    memo: FutureMap<Sha3Digest, Either<ClusterRead, ClusterWrite>>,
}

impl fmt::Debug for StoreInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StoreInner")
            .field("conn", &"OPAQUE")
            .field("pool", &self.pool)
            .field("memo", &self.memo)
            .finish()
    }
}

impl StoreInner {
    fn get_or_new(this: &Arc<Self>, digest: Sha3Digest) -> ClusterHandle {
        let output = Cell::new(None);
        this.handles.upsert(
            digest,
            || {
                let handle = ClusterHandle {
                    inner: Arc::new(HandleInner {
                        store: Arc::downgrade(this),

                        digest,
                        content: AtomSetOnce::empty(),
                    }),
                };
                output.set(Some(handle.clone()));
                handle
            },
            |pre| {
                output.set(Some(pre.clone()));
            },
        );
        output.into_inner().unwrap()
    }
}

trait EtcdOp: Send {
    fn go(self: Box<Self>, &Client<HttpConnector>) -> Box<Future<Item = (), Error = Error>>;
}

#[derive(Debug)]
struct EtcdOpClosure<F>(F)
where
    F: FnOnce(&Client<HttpConnector>) -> Box<Future<Item = (), Error = Error>> + Send;

impl<F> EtcdOp for EtcdOpClosure<F>
where
    F: FnOnce(&Client<HttpConnector>) -> Box<Future<Item = (), Error = Error>> + Send,
{
    fn go(
        self: Box<Self>,
        client: &Client<HttpConnector>,
    ) -> Box<Future<Item = (), Error = Error>> {
        (self.0)(client)
    }
}

struct EtcdClient {
    thread: JoinHandle<Result<(), Error>>,
    op_tx: mpsc::Sender<Box<EtcdOp>>,
    shutdown_tx: oneshot::Sender<()>,
}

impl fmt::Debug for EtcdClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EtcdClient")
            .field("thread", &self.thread)
            .field("op_tx", &"OPAQUE")
            .finish()
    }
}

impl EtcdClient {
    fn new(endpoints: &[&str]) -> Self {
        let (op_tx, op_rx) = mpsc::channel(64);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let endpoints_owned = endpoints
            .iter()
            .cloned()
            .map(String::from)
            .collect::<Vec<_>>();
        let thread = thread::spawn(move || {
            let endpoints_slices = endpoints_owned
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>();

            let mut core = Core::new()?;
            let client = Client::new(&core.handle(), endpoints_slices.as_slice(), None)?;

            let queued = op_rx
                .then(|op_res| match op_res {
                    Ok(op) => Ok(EtcdOp::go(op, &client)),
                    Err(()) => unreachable!(),
                })
                .buffer_unordered(8)
                .for_each(|()| {
                    ensure!(Async::NotReady == shutdown_rx.poll()?, "Shutdown!");
                    Ok(())
                });
            core.run(queued)?;

            Ok(())
        });

        Self {
            thread,
            op_tx,
            shutdown_tx,
        }
    }

    fn do_etcd_op<T, F, G>(&self, thunk: G) -> EtcdFuture<T>
    where
        T: Send + 'static,
        F: Future<Item = T, Error = Error> + 'static,
        G: FnOnce(&Client<HttpConnector>) -> F + Send + 'static,
    {
        let (wormhole_tx, wormhole_rx) = oneshot::channel();
        let closure = move |client: &Client<_>| -> Box<Future<Item = (), Error = Error>> {
            Box::new(
                thunk(client)
                    .and_then(|item| wormhole_tx.send(item).map_err(|_| format_err!("oops"))),
            )
        };

        if let Err(_) = self.op_tx.clone().send(Box::new(EtcdOpClosure(closure))).wait() {
            panic!("oops");
        }

        EtcdFuture(wormhole_rx)
    }
}

#[derive(Debug)]
struct EtcdFuture<T>(oneshot::Receiver<T>);

impl<T> Future for EtcdFuture<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.0.poll()?)
    }
}

#[derive(Debug)]
pub struct Object {
    blob: Vec<u8>,
    refs: Vec<Sha3Digest>,
}

impl Object {
    pub fn encode<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        leb128::write::unsigned(w, self.blob.len() as u64)?; // `C.length || C`
        w.write_all(&self.blob)?;
        canonical::encode(w, &self.blob, &self.refs)?; // `EncodedRefs(C)`

        Ok(())
    }

    pub fn decode<R: Read>(r: &mut R) -> Result<Self, Error> {
        let mut blob = vec![0; leb128::read::unsigned(r)? as usize]; // `C.length || C`
        r.read_exact(&mut blob)?;
        let refs = canonical::decode(r)?.finish::<Sha3Digest>()?.refs; // `EncodedRefs(C)`

        Ok(Self { blob, refs })
    }
}

#[derive(Debug, Clone)]
pub struct ClusterHandleContent(Cursor<ArcRef<Object, [u8]>>);

impl From<Arc<Object>> for ClusterHandleContent {
    fn from(arc_obj: Arc<Object>) -> Self {
        ClusterHandleContent(Cursor::new(
            ArcRef::new(arc_obj.clone()).map(|obj| obj.blob.as_slice()),
        ))
    }
}

impl Read for ClusterHandleContent {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.0.read(buf)
    }
}

#[derive(Debug, Clone)]
pub struct ClusterHandleRefs {
    store: Arc<StoreInner>,
    digests: ArcRef<Object, [Sha3Digest]>,
}

impl ClusterHandleRefs {
    fn new(store: Arc<StoreInner>, arc_obj: Arc<Object>) -> Self {
        ClusterHandleRefs {
            store,
            digests: ArcRef::new(arc_obj).map(|obj| obj.refs.as_slice()),
        }
    }
}

impl Iterator for ClusterHandleRefs {
    type Item = ClusterHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.digests.first().cloned().map(|digest| {
            self.digests = self.digests.clone().map(|slice| &slice[1..]);
            StoreInner::get_or_new(&self.store, digest)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClusterLoadFuture(ClusterLoadFuture_);

impl Future for ClusterLoadFuture {
    type Item = (ClusterHandleContent, ClusterHandleRefs);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

#[derive(Debug, Clone)]
enum ClusterLoadFuture_ {
    Waiting(
        ClusterHandle,
        FutureValue<Sha3Digest, Either<ClusterRead, ClusterWrite>>,
    ),
    Done(Option<(ClusterHandleContent, ClusterHandleRefs)>),
}

impl Future for ClusterLoadFuture_ {
    type Item = (ClusterHandleContent, ClusterHandleRefs);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            ClusterLoadFuture_::Waiting(ref handle, ref mut blocking) => {
                Ok(blocking.poll()?.map(|()| {
                    let arc_obj = handle.inner.content.dup().unwrap();
                    let handle_contents = arc_obj.clone().into();
                    let handle_refs = ClusterHandleRefs::new(
                        Weak::upgrade(&handle.inner.store).unwrap(),
                        arc_obj,
                    );
                    (handle_contents, handle_refs)
                }))
            }
            ClusterLoadFuture_::Done(ref mut loaded) => Ok(Async::Ready(loaded.take().unwrap())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterHandle {
    inner: Arc<HandleInner>,
}

impl PartialEq for ClusterHandle {
    fn eq(&self, rhs: &ClusterHandle) -> bool {
        self.inner.digest == rhs.inner.digest
    }
}

impl Eq for ClusterHandle {}

impl PartialOrd for ClusterHandle {
    fn partial_cmp(&self, rhs: &ClusterHandle) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

impl Ord for ClusterHandle {
    fn cmp(&self, rhs: &ClusterHandle) -> Ordering {
        self.inner.digest.cmp(&rhs.inner.digest)
    }
}

impl Hash for ClusterHandle {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.inner.digest.hash(state);
    }
}

impl Handle for ClusterHandle {
    type Content = ClusterHandleContent;
    type Refs = ClusterHandleRefs;

    type FutureLoad = ClusterLoadFuture;
    fn load(&self) -> Self::FutureLoad {
        let store = Weak::upgrade(&self.inner.store).unwrap();

        match self.inner.content.dup() {
            Some(arc_obj) => {
                let handle_contents = ClusterHandleContent::from(arc_obj.clone());
                let handle_refs = ClusterHandleRefs::new(store, arc_obj);
                ClusterLoadFuture(ClusterLoadFuture_::Done(Some((
                    handle_contents,
                    handle_refs,
                ))))
            }
            None => {
                let future_value = store.memo.get_or_insert(self.inner.digest, || {
                    let read = ClusterRead::new(self.clone());
                    Either::A(read)
                });
                ClusterLoadFuture(ClusterLoadFuture_::Waiting(self.clone(), future_value))
            }
        }
    }
}

impl<D: Digest> HandleDigest<D> for ClusterHandle {
    type FutureDigest = FutureResult<D, Error>;
    fn digest(&self) -> Self::FutureDigest {
        if D::NAME == Sha3Digest::NAME && D::SIZE == Sha3Digest::SIZE {
            future::ok(D::from_bytes(self.inner.digest.as_bytes()))
        } else {
            future::err(failure::err_msg(
                "ClusterHandle currently only supports SHA-3 digests!",
            ))
        }
    }
}

#[derive(Debug)]
pub struct HandleInner {
    store: Weak<StoreInner>,

    digest: Sha3Digest,
    content: AtomSetOnce<Arc<Object>>,
}

#[derive(Debug)]
pub struct ClusterHandleBuilder {
    store: Arc<StoreInner>,

    blob: Vec<u8>,
    refs: Vec<ClusterHandle>,
}

impl Write for ClusterHandleBuilder {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.blob.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl HandleBuilder<ClusterHandle> for ClusterHandleBuilder {
    fn add_reference(&mut self, reference: ClusterHandle) {
        self.refs.push(reference);
    }

    type FutureHandle = ClusterHandleFuture;
    fn finish(self) -> Self::FutureHandle {
        let object = Object {
            blob: self.blob,
            refs: self.refs.into_iter().map(|ch| ch.inner.digest).collect(),
        };

        let digest = {
            let mut writer = Sha3Digest::writer();
            object.encode(&mut writer).unwrap();
            writer.finish()
        };

        let handle = StoreInner::get_or_new(&self.store, digest);

        let blocking = self.store.memo.get_or_insert(digest, || {
            let write = ClusterWrite::new(handle.clone());
            Either::B(write)
        });

        ClusterHandleFuture { handle, blocking }
    }
}

#[derive(Debug)]
pub struct ClusterHandleFuture {
    handle: ClusterHandle,
    blocking: FutureValue<Sha3Digest, Either<ClusterRead, ClusterWrite>>,
}

impl Future for ClusterHandleFuture {
    type Item = ClusterHandle;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.blocking.poll()?.map(|()| self.handle.clone()))
    }
}

enum ReadState {
    Stat(rados::StatFuture),
    Read(FullReadState),
}

impl fmt::Debug for ReadState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ReadState::Stat(_) => f.debug_tuple("Stat").field(&"OPAQUE").finish(),
            ReadState::Read(ref rs) => f.debug_tuple("Read").field(rs).finish(),
        }
    }
}

struct FullReadState {
    blocking: rados::ReadFuture<VecRefMut<u8, [u8]>>,

    total_read: usize,
    size: usize,
}

impl fmt::Debug for FullReadState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FullReadState")
            .field("blocking", &"OPAQUE")
            .field("total_read", &self.total_read)
            .field("size", &self.size)
            .finish()
    }
}

pub struct ClusterRead {
    ctx: Arc<Mutex<Context>>,

    object: String,
    handle: ClusterHandle,

    state: ReadState,
}

impl fmt::Debug for ClusterRead {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClusterRead")
            .field("ctx", &"OPAQUE")
            .field("object", &self.object)
            .field("handle", &self.handle)
            .field("state", &self.state)
            .finish()
    }
}

impl Future for ClusterRead {
    type Item = ();
    type Error = Compat<Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state {
            ReadState::Stat(ref mut stat_future) => {
                let size = match stat_future
                    .poll()
                    .map_err(SyncFailure::new)
                    .map_err(Error::from)
                    .compat()?
                {
                    Async::Ready(stat) => stat.size as usize,
                    Async::NotReady => return Ok(Async::NotReady),
                };

                self.state = ReadState::Read(FullReadState {
                    blocking: self.ctx.lock().unwrap().read_async(
                        &self.object,
                        VecRefMut::new(vec![0; size]).map_mut(|b| &mut b[..]),
                        0,
                    ),

                    total_read: 0,
                    size,
                });

                self.poll()
            }
            ReadState::Read(ref mut rs) => {
                let (bytes_read, buf) = match rs.blocking
                    .poll()
                    .map_err(SyncFailure::new)
                    .map_err(Error::from)
                    .compat()?
                {
                    Async::Ready((br, buf)) => (br as usize, buf.into_inner()),
                    Async::NotReady => return Ok(Async::NotReady),
                };

                if bytes_read == 0 {
                    return Err(
                        failure::err_msg("Failed to read a nonzero number of bytes!").compat(),
                    );
                }
                rs.total_read += bytes_read;

                if rs.total_read < rs.size {
                    rs.blocking = self.ctx.lock().unwrap().read_async(
                        &self.object,
                        VecRefMut::new(buf).map_mut(|b| &mut b[rs.total_read..]),
                        rs.total_read as u64,
                    );

                    self.poll()
                } else {
                    self.handle
                        .inner
                        .content
                        .set_if_none(Arc::new(Object::decode(&mut buf.as_slice()).compat()?))
                        .unwrap();

                    Ok(Async::Ready(()))
                }
            }
        }
    }
}

impl ClusterRead {
    pub fn new(handle: ClusterHandle) -> Self {
        let ctx = Weak::upgrade(&handle.inner.store).unwrap().ctx.clone();
        let object = hex::encode(handle.inner.digest.as_bytes());
        let state = ReadState::Stat(ctx.lock().unwrap().stat_async(&object));

        Self {
            ctx,

            object,
            state,

            handle,
        }
    }
}

enum WriteState {
    Check(rados::ExistsFuture),
    Write(rados::UnitFuture),
}

impl fmt::Debug for WriteState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WriteState::Check(_) => f.debug_tuple("Check").field(&"OPAQUE").finish(),
            WriteState::Write(_) => f.debug_tuple("Write").field(&"OPAQUE").finish(),
        }
    }
}

struct ClusterWrite {
    ctx: Arc<Mutex<Context>>,

    object: String,
    handle: ClusterHandle,

    state: WriteState,
}

impl fmt::Debug for ClusterWrite {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClusterWrite")
            .field("ctx", &"OPAQUE")
            .field("object", &self.object)
            .field("handle", &self.handle)
            .field("state", &self.state)
            .finish()
    }
}

impl Future for ClusterWrite {
    type Item = ();
    type Error = Compat<Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state {
            WriteState::Check(ref mut exists) => match exists
                .poll()
                .map_err(SyncFailure::new)
                .map_err(Error::from)
                .compat()?
            {
                Async::Ready(true) => Ok(Async::Ready(())),
                Async::Ready(false) => {
                    let data = {
                        let mut buf = Vec::new();
                        self.handle
                            .inner
                            .content
                            .get()
                            .unwrap()
                            .encode(&mut buf)
                            .unwrap();
                        buf
                    };
                    let blocking = self.ctx
                        .lock()
                        .unwrap()
                        .write_full_async(&self.object, &data);
                    self.state = WriteState::Write(blocking);
                    self.poll()
                }
                Async::NotReady => Ok(Async::NotReady),
            },
            WriteState::Write(ref mut write) => match write
                .poll()
                .map_err(SyncFailure::new)
                .map_err(Error::from)
                .compat()?
            {
                Async::Ready(()) => Ok(Async::Ready(())),
                Async::NotReady => Ok(Async::NotReady),
            },
        }
    }
}

impl ClusterWrite {
    pub fn new(handle: ClusterHandle) -> Self {
        let ctx = Weak::upgrade(&handle.inner.store).unwrap().ctx.clone();
        let object = hex::encode(handle.inner.digest.as_bytes());
        let state = WriteState::Check(ctx.lock().unwrap().exists_async(&object));

        Self {
            ctx,

            object,
            handle,

            state,
        }
    }
}
