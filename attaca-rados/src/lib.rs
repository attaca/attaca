#![feature(proc_macro, generators, conservative_impl_trait)]

extern crate attaca;
extern crate base64;
extern crate bytes;
extern crate capnp;
extern crate failure;
extern crate futures_await as futures;
extern crate leb128;
extern crate owning_ref;
extern crate rad;
extern crate url;
extern crate uuid;

#[allow(dead_code)]
mod branch_set_capnp {
    include!(concat!(env!("OUT_DIR"), "/branch_set_capnp.rs"));
}

mod mapping;

use std::{collections::HashMap, io::{self, BufRead, Cursor, Read, Write}, path::Path,
          sync::{Arc, Mutex}};

use attaca::{canonical, Open, digest::{Sha3Digest, prelude::*}, store::{RawHandle, prelude::*}};
use bytes::{BufMut, IntoBuf};
use capnp::{message, serialize_packed};
use failure::*;
use futures::{future::{Either, Flatten, FutureResult}, prelude::*};
use owning_ref::VecRefMut;
use rad::{rados, ConnectionBuilder, Context};
use url::Url;
use uuid::Uuid;

use mapping::Mapping;

const BRANCHES_KEY: &'static [u8] = b"BRANCHES";
const UUID_KEY: &'static [u8] = b"UUID";
const BLOB_KEY: &'static [u8] = b"BLOB";

impl Open for RadosBackend {
    const SCHEMES: &'static [&'static str] = &["ceph"];

    fn open(s: &str) -> Result<Self, Error> {
        let url = Url::parse(s)?;
        ensure!(
            Self::SCHEMES.contains(&url.scheme()),
            "bad URL scheme (not in {:?})",
            Self::SCHEMES
        );

        let mut builder = if url.has_authority() {
            ConnectionBuilder::with_user(url.username()).map_err(SyncFailure::new)?
        } else {
            ConnectionBuilder::new().map_err(SyncFailure::new)?
        };

        if url.path().starts_with("/") && url.path().len() > 1 {
            builder = builder
                .read_conf_file(Path::new(&url.path()[1..]))
                .map_err(SyncFailure::new)?;
        }

        let mut query_pairs = url.query_pairs().collect::<HashMap<_, _>>();

        let pool_name = query_pairs
            .remove("pool")
            .ok_or_else(|| format_err!("missing pool name from URL query string"))?;

        let builder = query_pairs
            .into_iter()
            .fold(Ok(builder), |builder, (key, val)| {
                builder?.conf_set(&key, &val)
            })
            .map_err(SyncFailure::new)?;

        let mut connection = builder.connect().map_err(SyncFailure::new)?;
        let mut context = connection
            .get_pool_context(&pool_name)
            .map_err(SyncFailure::new)?;
        let uuid = {
            let mut buf = [0; 16];

            let mut total = 0;
            while total < buf.len() {
                let bytes_read = context
                    .read(
                        &Key::Uuid.into_object(&[][..]),
                        &mut buf[total..],
                        total as u64,
                    )
                    .map_err(SyncFailure::new)?;
                total += bytes_read as usize;
            }

            Uuid::from_bytes(&buf)?
        };

        Ok(RadosBackend {
            uuid,
            context: Arc::new(Mutex::new(context)),
            mapping: Arc::new(Mapping::new()),
        })
    }

    fn open_path(_: &Path) -> Result<Self, Error> {
        bail!("Attaca RADOS backend does not support opening via path! More information is needed, provided in the URL query string.");
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Key {
    Branches,
    Uuid,
    Blob,
}

impl Key {
    fn into_object<I: IntoBuf>(self, with: I) -> String {
        let mut buf = Vec::new();
        match self {
            Key::Branches => buf.put(BRANCHES_KEY),
            Key::Uuid => buf.put(UUID_KEY),
            Key::Blob => buf.put(BLOB_KEY),
        }
        buf.put(with);
        base64::encode(&buf)
    }
}

fn decode_branch_set<R: BufRead>(reader: &mut R) -> Result<Vec<(String, Sha3Digest)>, Error> {
    use branch_set_capnp::*;

    let message_reader = serialize_packed::read_message(reader, message::ReaderOptions::new())?;
    let branch_set_reader = message_reader.get_root::<branch_set::Reader>()?;

    let mut branches = Vec::new();

    for entry in branch_set_reader.get_entries()?.iter() {
        let digest = Sha3Digest::from_bytes(entry.get_hash()?);
        let name = String::from(entry.get_name()?);

        branches.push((name, digest));
    }

    Ok(branches)
}

fn encode_branch_set<W: Write, I>(
    writer: &mut W,
    branches: I,
    branches_len: usize,
) -> Result<(), Error>
where
    I: IntoIterator<Item = (String, Sha3Digest)>,
{
    use branch_set_capnp::*;

    let mut message = message::Builder::new_default();

    {
        let mut branch_set_builder = message.init_root::<branch_set::Builder>();
        let mut entries_builder = branch_set_builder
            .borrow()
            .init_entries(branches_len as u32);

        for (i, (branch, digest)) in branches.into_iter().take(branches_len).enumerate() {
            let mut entry_builder = entries_builder.borrow().get(i as u32);
            entry_builder.set_name(&branch);
            entry_builder.set_hash(digest.as_bytes());
        }
    }

    serialize_packed::write_message(writer, &message)?;

    Ok(())
}

pub struct RadosBuilder {
    blob: Vec<u8>,
    refs: Vec<RawHandle>,
}

impl Write for RadosBuilder {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.blob.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Write::flush(&mut self.blob)
    }
}

impl Extend<RawHandle> for RadosBuilder {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = RawHandle>,
    {
        self.refs.extend(iter);
    }
}

#[derive(Debug)]
pub struct RadosFinish {
    blocking: rados::UnitFuture,
    id: RawHandle,
}

impl Future for RadosFinish {
    type Item = RawHandle;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.blocking
            .poll()
            .map(|async| async.map(|()| self.id))
            .map_err(SyncFailure::new)
            .map_err(Error::from)
    }
}

#[derive(Debug)]
pub struct RadosContent {
    blob: Cursor<Vec<u8>>,
    refs: <Vec<RawHandle> as IntoIterator>::IntoIter,
}

impl Read for RadosContent {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.blob.read(buf)
    }
}

impl Iterator for RadosContent {
    type Item = RawHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.refs.next()
    }
}

pub struct RadosLoad {
    blocking: Box<Future<Item = Vec<u8>, Error = Error>>,
    mapping: Arc<Mapping>,
}

impl Future for RadosLoad {
    type Item = RadosContent;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.blocking.poll()? {
            Async::Ready(buf) => {
                let mut reader = Cursor::new(buf);
                let blob = {
                    let mut buf = vec![0; leb128::read::unsigned(&mut reader)? as usize];
                    reader.read_exact(&mut buf)?;
                    Cursor::new(buf)
                };
                let refs = {
                    let ref_digests = canonical::decode(&mut reader)?.finish::<Sha3Digest>()?.refs;
                    self.mapping
                        .map_digests_to_ids(ref_digests)
                        .collect::<Vec<_>>()
                        .into_iter()
                };

                Ok(Async::Ready(RadosContent { blob, refs }))
            }
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub struct RadosResolve {
    blocking: rados::ExistsFuture,
    mapping: Arc<Mapping>,
    digest: Sha3Digest,
}

impl Future for RadosResolve {
    type Item = Option<RawHandle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.blocking
            .poll()
            .map(|async| {
                async.map(|exists| match exists {
                    true => Some(self.mapping.reserve(self.digest).unwrap_or_else(|e| e)),
                    false => None,
                })
            })
            .map_err(SyncFailure::new)
            .map_err(Error::from)
    }
}

pub struct RadosLoadBranches {
    blocking: Box<Future<Item = HashMap<String, RawHandle>, Error = Error>>,
}

impl Future for RadosLoadBranches {
    type Item = HashMap<String, RawHandle>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.blocking.poll()
    }
}

pub struct RadosSwapBranches {
    blocking: Box<Future<Item = (), Error = Error>>,
}

impl Future for RadosSwapBranches {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.blocking.poll()
    }
}

pub struct RadosBackend {
    uuid: Uuid,
    context: Arc<Mutex<Context>>,
    mapping: Arc<Mapping>,
}

impl RadosBackend {
    fn do_digest(&self, signature: DigestSignature, id: RawHandle) -> Result<Sha3Digest, Error> {
        ensure!(signature == Sha3Digest::SIGNATURE, "bad digest signature");
        Ok(self.mapping.digest(id))
    }

    fn do_resolve_id(&self, digest: &Sha3Digest) -> RadosResolve {
        let digest = *digest;
        let obj = Key::Blob.into_object(digest.as_bytes());

        let blocking = self.context.lock().unwrap().exists_async(&obj);
        let mapping = self.mapping.clone();

        RadosResolve {
            blocking,
            mapping,
            digest,
        }
    }

    fn do_resolve_digest(
        &self,
        signature: DigestSignature,
        bytes: &[u8],
    ) -> Result<RadosResolve, Error> {
        ensure!(signature == Sha3Digest::SIGNATURE, "bad digest signature");
        Ok(self.do_resolve_id(&Sha3Digest::from_bytes(bytes)))
    }
}

impl Backend for RadosBackend {
    fn uuid(&self) -> [u8; 16] {
        *self.uuid.as_bytes()
    }

    type Builder = RadosBuilder;
    type FutureFinish = Either<RadosFinish, FutureResult<RawHandle, Error>>;

    fn builder(&self) -> Self::Builder {
        RadosBuilder {
            blob: Vec::new(),
            refs: Vec::new(),
        }
    }

    fn finish(&self, builder: Self::Builder) -> Self::FutureFinish {
        let blob = builder.blob;
        let refs = self.mapping
            .map_ids_to_digests(builder.refs.into_iter())
            .collect::<Vec<_>>();

        let mut hasher = Sha3Digest::writer();
        canonical::encode(&mut hasher, &blob, &refs).unwrap();
        let digest = hasher.finish();

        match self.mapping.reserve(digest) {
            Ok(id) => {
                let mut blob_buf = Vec::new();
                leb128::write::unsigned(&mut blob_buf, blob.len() as u64).unwrap();
                blob_buf.write_all(&blob).unwrap();
                canonical::encode(&mut blob_buf, &blob, &refs).unwrap();

                let obj = Key::Blob.into_object(digest.as_bytes());
                let blocking = self.context
                    .lock()
                    .unwrap()
                    .write_full_async(&obj, &blob_buf);

                Either::A(RadosFinish { blocking, id })
            }
            Err(id) => Either::B(Ok(id).into_future()),
        }
    }

    type Content = RadosContent;
    type FutureContent = RadosLoad;

    fn load(&self, id: RawHandle) -> Self::FutureContent {
        let context = self.context.clone();
        let mapping = self.mapping.clone();

        let digest = mapping.digest(id);
        let obj = Key::Blob.into_object(digest.as_bytes());

        let blocking = async_block! {
            let stat = await!(context.lock().unwrap().stat_async(&obj)).map_err(SyncFailure::new)?;

            let mut buf_ref =
                VecRefMut::new(vec![0; stat.size as usize]).map_mut(|buf| &mut buf[..]);
            let mut total = 0;

            while buf_ref.len() > 0 {
                let (bytes_read, dirty_buf) = await!(context.lock().unwrap().read_async(
                    &obj,
                    buf_ref,
                    total as u64
                )).map_err(SyncFailure::new)?;
                total += bytes_read as usize;
                buf_ref = VecRefMut::new(dirty_buf.into_inner())
                    .map_mut(|buf| &mut buf[total as usize..]);
            }

            Ok(buf_ref.into_inner())
        };

        RadosLoad {
            blocking: Box::new(blocking),
            mapping,
        }
    }

    type Id = Sha3Digest;
    type FutureId = FutureResult<Self::Id, Error>;

    fn id(&self, id: RawHandle) -> Self::FutureId {
        Ok(self.mapping.digest(id)).into_future()
    }

    type Digest = Sha3Digest;
    type FutureDigest = FutureResult<Self::Digest, Error>;

    fn digest(&self, signature: DigestSignature, id: RawHandle) -> Self::FutureDigest {
        self.do_digest(signature, id).into_future()
    }

    type FutureResolveId = RadosResolve;
    fn resolve_id(&self, digest: &Sha3Digest) -> Self::FutureResolveId {
        self.do_resolve_id(digest)
    }

    type FutureResolveDigest = Flatten<FutureResult<RadosResolve, Error>>;
    fn resolve_digest(
        &self,
        signature: DigestSignature,
        bytes: &[u8],
    ) -> Self::FutureResolveDigest {
        self.do_resolve_digest(signature, bytes)
            .into_future()
            .flatten()
    }

    type FutureLoadBranches = RadosLoadBranches;
    fn load_branches(&self) -> Self::FutureLoadBranches {
        let context = self.context.clone();
        let mapping = self.mapping.clone();

        let blocking = async_block! {
            let obj = Key::Branches.into_object(&[][..]);

            let decoded = {
                if await!(context.lock().unwrap().exists_async(&obj)).map_err(SyncFailure::new)? {
                    let stat =
                        await!(context.lock().unwrap().stat_async(&obj)).map_err(SyncFailure::new)?;

                    let mut buf_ref =
                        VecRefMut::new(vec![0; stat.size as usize]).map_mut(|buf| &mut buf[..]);
                    let mut total = 0;

                    while buf_ref.len() > 0 {
                        let (bytes_read, dirty_buf) = await!(context.lock().unwrap().read_async(
                            &obj,
                            buf_ref,
                            total as u64
                        )).map_err(SyncFailure::new)?;
                        total += bytes_read as usize;
                        buf_ref = VecRefMut::new(dirty_buf.into_inner())
                            .map_mut(|buf| &mut buf[total as usize..]);
                    }

                    decode_branch_set(&mut Cursor::new(buf_ref.into_inner()))?
                } else {
                    Vec::new()
                }
            };

            let resolved = decoded
                .into_iter()
                .map(|(name, digest)| (name, mapping.reserve(digest).unwrap_or_else(|e| e)))
                .collect();
            Ok(resolved)
        };

        RadosLoadBranches {
            blocking: Box::new(blocking),
        }
    }

    // TODO currently not atomic! v. v. bad, needs atomic ops in rad-rs
    //
    // When atomic ops come in, will need a rewrite.
    type FutureSwapBranches = RadosSwapBranches;
    fn swap_branches(
        &self,
        old: HashMap<String, RawHandle>,
        new: HashMap<String, RawHandle>,
    ) -> Self::FutureSwapBranches {
        let current_future = self.load_branches();
        let context = self.context.clone();
        let mapping = self.mapping.clone();
        let blocking = async_block! {
            let current = await!(current_future)?;
            ensure!(old == current, "compare failed");

            let obj = Key::Branches.into_object(&[][..]);

            let mut buf = Vec::new();
            let new_len = new.len();
            encode_branch_set(
                &mut buf,
                new.into_iter().map(|(name, id)| (name, mapping.digest(id))),
                new_len,
            )?;
            await!(context.lock().unwrap().write_full_async(&obj, &buf)).map_err(SyncFailure::new)?;

            Ok(())
        };

        RadosSwapBranches {
            blocking: Box::new(blocking),
        }
    }
}

// use std::{fmt, mem, boxed::FnBox, cell::Cell, cmp::Ordering, hash::{Hash, Hasher},
//           io::{self, Cursor, Read, Write}, sync::{Arc, Mutex, Weak}, thread::{self, JoinHandle},
//           time::Duration};
//
// use atom::AtomSetOnce;
// use chashmap::CHashMap;
// use etcd::Client;
// use failure::{self, Compat, Error, ResultExt, SyncFailure};
// use futures::{future::{self, Either, Executor, FutureResult}, prelude::*,
//               stream::FuturesUnordered, sync::{mpsc, oneshot}};
// use hex;
// use hyper::client::HttpConnector;
// use leb128;
// use owning_ref::{ArcRef, VecRefMut};
// use rad::{rados, Connection, Context};
// use tokio_core::reactor::Core;
//
// use canonical::{self, Item};
// use digest::{Digest, DigestWriter, Sha3Digest};
// use memo::{FutureMap, FutureValue};
// use store::{Handle, HandleBuilder, HandleDigest, Store};
//
// #[derive(Debug, Clone)]
// pub struct ClusterStore {
//     inner: Arc<StoreInner>,
// }
//
// impl Store for ClusterStore {
//     type Handle = ClusterHandle;
//
//     type HandleBuilder = ClusterHandleBuilder;
//     fn handle_builder(&self) -> Self::HandleBuilder {
//         ClusterHandleBuilder {
//             store: self.inner.clone(),
//
//             blob: Vec::new(),
//             refs: Vec::new(),
//         }
//     }
//
//     type FutureLoadBranch = FutureResult<Option<Self::Handle>, Error>;
//     fn load_branch(&self, _branch: String) -> Self::FutureLoadBranch {
//         unimplemented!();
//     }
//
//     type FutureSwapBranch = FutureResult<(), Error>;
//     fn swap_branch(
//         &self,
//         _branch: String,
//         _previous: Option<Self::Handle>,
//         _new: Self::Handle,
//     ) -> Self::FutureSwapBranch {
//         unimplemented!();
//     }
//
//     type FutureResolve = FutureResult<Option<Self::Handle>, Error>;
//     fn resolve<D: Digest>(&self, _digest: &D) -> Self::FutureResolve
//     where
//         Self::Handle: HandleDigest<D>,
//     {
//         unimplemented!();
//     }
// }
//
// struct StoreInner {
//     conn: Arc<Mutex<Connection>>,
//     pool: String,
//
//     ctx: Arc<Mutex<Context>>,
//
//     handles: CHashMap<Sha3Digest, ClusterHandle>,
//     memo: FutureMap<Sha3Digest, Either<ClusterRead, ClusterWrite>>,
// }
//
// impl fmt::Debug for StoreInner {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.debug_struct("StoreInner")
//             .field("conn", &"OPAQUE")
//             .field("pool", &self.pool)
//             .field("memo", &self.memo)
//             .finish()
//     }
// }
//
// impl StoreInner {
//     fn get_or_new(this: &Arc<Self>, digest: Sha3Digest) -> ClusterHandle {
//         let output = Cell::new(None);
//         this.handles.upsert(
//             digest,
//             || {
//                 let handle = ClusterHandle {
//                     inner: Arc::new(HandleInner {
//                         store: Arc::downgrade(this),
//
//                         digest,
//                         content: AtomSetOnce::empty(),
//                     }),
//                 };
//                 output.set(Some(handle.clone()));
//                 handle
//             },
//             |pre| {
//                 output.set(Some(pre.clone()));
//             },
//         );
//         output.into_inner().unwrap()
//     }
// }
//
// trait EtcdOp: Send {
//     fn go(self: Box<Self>, &Client<HttpConnector>) -> Box<Future<Item = (), Error = Error>>;
// }
//
// #[derive(Debug)]
// struct EtcdOpClosure<F>(F)
// where
//     F: FnOnce(&Client<HttpConnector>) -> Box<Future<Item = (), Error = Error>> + Send;
//
// impl<F> EtcdOp for EtcdOpClosure<F>
// where
//     F: FnOnce(&Client<HttpConnector>) -> Box<Future<Item = (), Error = Error>> + Send,
// {
//     fn go(
//         self: Box<Self>,
//         client: &Client<HttpConnector>,
//     ) -> Box<Future<Item = (), Error = Error>> {
//         (self.0)(client)
//     }
// }
//
// struct EtcdClient {
//     thread: JoinHandle<Result<(), Error>>,
//     op_tx: mpsc::Sender<Box<EtcdOp>>,
//     shutdown_tx: oneshot::Sender<()>,
// }
//
// impl fmt::Debug for EtcdClient {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.debug_struct("EtcdClient")
//             .field("thread", &self.thread)
//             .field("op_tx", &"OPAQUE")
//             .finish()
//     }
// }
//
// impl EtcdClient {
//     fn new(endpoints: &[&str]) -> Self {
//         let (op_tx, op_rx) = mpsc::channel(64);
//         let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
//         let endpoints_owned = endpoints
//             .iter()
//             .cloned()
//             .map(String::from)
//             .collect::<Vec<_>>();
//         let thread = thread::spawn(move || {
//             let endpoints_slices = endpoints_owned
//                 .iter()
//                 .map(String::as_str)
//                 .collect::<Vec<&str>>();
//
//             let mut core = Core::new()?;
//             let client = Client::new(&core.handle(), endpoints_slices.as_slice(), None)?;
//
//             let queued = op_rx
//                 .then(|op_res| match op_res {
//                     Ok(op) => Ok(EtcdOp::go(op, &client)),
//                     Err(()) => unreachable!(),
//                 })
//                 .buffer_unordered(8)
//                 .for_each(|()| {
//                     ensure!(Async::NotReady == shutdown_rx.poll()?, "Shutdown!");
//                     Ok(())
//                 });
//             core.run(queued)?;
//
//             Ok(())
//         });
//
//         Self {
//             thread,
//             op_tx,
//             shutdown_tx,
//         }
//     }
//
//     fn do_etcd_op<T, F, G>(&self, thunk: G) -> EtcdFuture<T>
//     where
//         T: Send + 'static,
//         F: Future<Item = T, Error = Error> + 'static,
//         G: FnOnce(&Client<HttpConnector>) -> F + Send + 'static,
//     {
//         let (wormhole_tx, wormhole_rx) = oneshot::channel();
//         let closure = move |client: &Client<_>| -> Box<Future<Item = (), Error = Error>> {
//             Box::new(
//                 thunk(client)
//                     .and_then(|item| wormhole_tx.send(item).map_err(|_| format_err!("oops"))),
//             )
//         };
//
//         if let Err(_) = self.op_tx.clone().send(Box::new(EtcdOpClosure(closure))).wait() {
//             panic!("oops");
//         }
//
//         EtcdFuture(wormhole_rx)
//     }
// }
//
// #[derive(Debug)]
// struct EtcdFuture<T>(oneshot::Receiver<T>);
//
// impl<T> Future for EtcdFuture<T> {
//     type Item = T;
//     type Error = Error;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         Ok(self.0.poll()?)
//     }
// }
//
// #[derive(Debug)]
// pub struct Object {
//     blob: Vec<u8>,
//     refs: Vec<Sha3Digest>,
// }
//
// impl Object {
//     pub fn encode<W: Write>(&self, w: &mut W) -> Result<(), Error> {
//         leb128::write::unsigned(w, self.blob.len() as u64)?; // `C.length || C`
//         w.write_all(&self.blob)?;
//         canonical::encode(w, &self.blob, &self.refs)?; // `EncodedRefs(C)`
//
//         Ok(())
//     }
//
//     pub fn decode<R: Read>(r: &mut R) -> Result<Self, Error> {
//         let mut blob = vec![0; leb128::read::unsigned(r)? as usize]; // `C.length || C`
//         r.read_exact(&mut blob)?;
//         let refs = canonical::decode(r)?.finish::<Sha3Digest>()?.refs; // `EncodedRefs(C)`
//
//         Ok(Self { blob, refs })
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct ClusterHandleContent(Cursor<ArcRef<Object, [u8]>>);
//
// impl From<Arc<Object>> for ClusterHandleContent {
//     fn from(arc_obj: Arc<Object>) -> Self {
//         ClusterHandleContent(Cursor::new(
//             ArcRef::new(arc_obj.clone()).map(|obj| obj.blob.as_slice()),
//         ))
//     }
// }
//
// impl Read for ClusterHandleContent {
//     fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
//         self.0.read(buf)
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct ClusterHandleRefs {
//     store: Arc<StoreInner>,
//     digests: ArcRef<Object, [Sha3Digest]>,
// }
//
// impl ClusterHandleRefs {
//     fn new(store: Arc<StoreInner>, arc_obj: Arc<Object>) -> Self {
//         ClusterHandleRefs {
//             store,
//             digests: ArcRef::new(arc_obj).map(|obj| obj.refs.as_slice()),
//         }
//     }
// }
//
// impl Iterator for ClusterHandleRefs {
//     type Item = ClusterHandle;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self.digests.first().cloned().map(|digest| {
//             self.digests = self.digests.clone().map(|slice| &slice[1..]);
//             StoreInner::get_or_new(&self.store, digest)
//         })
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct ClusterLoadFuture(ClusterLoadFuture_);
//
// impl Future for ClusterLoadFuture {
//     type Item = (ClusterHandleContent, ClusterHandleRefs);
//     type Error = Error;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         self.0.poll()
//     }
// }
//
// #[derive(Debug, Clone)]
// enum ClusterLoadFuture_ {
//     Waiting(
//         ClusterHandle,
//         FutureValue<Sha3Digest, Either<ClusterRead, ClusterWrite>>,
//     ),
//     Done(Option<(ClusterHandleContent, ClusterHandleRefs)>),
// }
//
// impl Future for ClusterLoadFuture_ {
//     type Item = (ClusterHandleContent, ClusterHandleRefs);
//     type Error = Error;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         match *self {
//             ClusterLoadFuture_::Waiting(ref handle, ref mut blocking) => {
//                 Ok(blocking.poll()?.map(|()| {
//                     let arc_obj = handle.inner.content.dup().unwrap();
//                     let handle_contents = arc_obj.clone().into();
//                     let handle_refs = ClusterHandleRefs::new(
//                         Weak::upgrade(&handle.inner.store).unwrap(),
//                         arc_obj,
//                     );
//                     (handle_contents, handle_refs)
//                 }))
//             }
//             ClusterLoadFuture_::Done(ref mut loaded) => Ok(Async::Ready(loaded.take().unwrap())),
//         }
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct ClusterHandle {
//     inner: Arc<HandleInner>,
// }
//
// impl PartialEq for ClusterHandle {
//     fn eq(&self, rhs: &ClusterHandle) -> bool {
//         self.inner.digest == rhs.inner.digest
//     }
// }
//
// impl Eq for ClusterHandle {}
//
// impl PartialOrd for ClusterHandle {
//     fn partial_cmp(&self, rhs: &ClusterHandle) -> Option<Ordering> {
//         Some(self.cmp(rhs))
//     }
// }
//
// impl Ord for ClusterHandle {
//     fn cmp(&self, rhs: &ClusterHandle) -> Ordering {
//         self.inner.digest.cmp(&rhs.inner.digest)
//     }
// }
//
// impl Hash for ClusterHandle {
//     fn hash<H>(&self, state: &mut H)
//     where
//         H: Hasher,
//     {
//         self.inner.digest.hash(state);
//     }
// }
//
// impl Handle for ClusterHandle {
//     type Content = ClusterHandleContent;
//     type Refs = ClusterHandleRefs;
//
//     type FutureLoad = ClusterLoadFuture;
//     fn load(&self) -> Self::FutureLoad {
//         let store = Weak::upgrade(&self.inner.store).unwrap();
//
//         match self.inner.content.dup() {
//             Some(arc_obj) => {
//                 let handle_contents = ClusterHandleContent::from(arc_obj.clone());
//                 let handle_refs = ClusterHandleRefs::new(store, arc_obj);
//                 ClusterLoadFuture(ClusterLoadFuture_::Done(Some((
//                     handle_contents,
//                     handle_refs,
//                 ))))
//             }
//             None => {
//                 let future_value = store.memo.get_or_insert(self.inner.digest, || {
//                     let read = ClusterRead::new(self.clone());
//                     Either::A(read)
//                 });
//                 ClusterLoadFuture(ClusterLoadFuture_::Waiting(self.clone(), future_value))
//             }
//         }
//     }
// }
//
// impl<D: Digest> HandleDigest<D> for ClusterHandle {
//     type FutureDigest = FutureResult<D, Error>;
//     fn digest(&self) -> Self::FutureDigest {
//         if D::NAME == Sha3Digest::NAME && D::SIZE == Sha3Digest::SIZE {
//             future::ok(D::from_bytes(self.inner.digest.as_bytes()))
//         } else {
//             future::err(failure::err_msg(
//                 "ClusterHandle currently only supports SHA-3 digests!",
//             ))
//         }
//     }
// }
//
// #[derive(Debug)]
// pub struct HandleInner {
//     store: Weak<StoreInner>,
//
//     digest: Sha3Digest,
//     content: AtomSetOnce<Arc<Object>>,
// }
//
// #[derive(Debug)]
// pub struct ClusterHandleBuilder {
//     store: Arc<StoreInner>,
//
//     blob: Vec<u8>,
//     refs: Vec<ClusterHandle>,
// }
//
// impl Write for ClusterHandleBuilder {
//     fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
//         self.blob.write(buf)
//     }
//
//     fn flush(&mut self) -> Result<(), io::Error> {
//         Ok(())
//     }
// }
//
// impl HandleBuilder<ClusterHandle> for ClusterHandleBuilder {
//     fn add_reference(&mut self, reference: ClusterHandle) {
//         self.refs.push(reference);
//     }
//
//     type FutureHandle = ClusterHandleFuture;
//     fn finish(self) -> Self::FutureHandle {
//         let object = Object {
//             blob: self.blob,
//             refs: self.refs.into_iter().map(|ch| ch.inner.digest).collect(),
//         };
//
//         let digest = {
//             let mut writer = Sha3Digest::writer();
//             object.encode(&mut writer).unwrap();
//             writer.finish()
//         };
//
//         let handle = StoreInner::get_or_new(&self.store, digest);
//
//         let blocking = self.store.memo.get_or_insert(digest, || {
//             let write = ClusterWrite::new(handle.clone());
//             Either::B(write)
//         });
//
//         ClusterHandleFuture { handle, blocking }
//     }
// }
//
// #[derive(Debug)]
// pub struct ClusterHandleFuture {
//     handle: ClusterHandle,
//     blocking: FutureValue<Sha3Digest, Either<ClusterRead, ClusterWrite>>,
// }
//
// impl Future for ClusterHandleFuture {
//     type Item = ClusterHandle;
//     type Error = Error;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         Ok(self.blocking.poll()?.map(|()| self.handle.clone()))
//     }
// }
//
// enum ReadState {
//     Stat(rados::StatFuture),
//     Read(FullReadState),
// }
//
// impl fmt::Debug for ReadState {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match *self {
//             ReadState::Stat(_) => f.debug_tuple("Stat").field(&"OPAQUE").finish(),
//             ReadState::Read(ref rs) => f.debug_tuple("Read").field(rs).finish(),
//         }
//     }
// }
//
// struct FullReadState {
//     blocking: rados::ReadFuture<VecRefMut<u8, [u8]>>,
//
//     total_read: usize,
//     size: usize,
// }
//
// impl fmt::Debug for FullReadState {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.debug_struct("FullReadState")
//             .field("blocking", &"OPAQUE")
//             .field("total_read", &self.total_read)
//             .field("size", &self.size)
//             .finish()
//     }
// }
//
// pub struct ClusterRead {
//     ctx: Arc<Mutex<Context>>,
//
//     object: String,
//     handle: ClusterHandle,
//
//     state: ReadState,
// }
//
// impl fmt::Debug for ClusterRead {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.debug_struct("ClusterRead")
//             .field("ctx", &"OPAQUE")
//             .field("object", &self.object)
//             .field("handle", &self.handle)
//             .field("state", &self.state)
//             .finish()
//     }
// }
//
// impl Future for ClusterRead {
//     type Item = ();
//     type Error = Compat<Error>;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         match self.state {
//             ReadState::Stat(ref mut stat_future) => {
//                 let size = match stat_future
//                     .poll()
//                     .map_err(SyncFailure::new)
//                     .map_err(Error::from)
//                     .compat()?
//                 {
//                     Async::Ready(stat) => stat.size as usize,
//                     Async::NotReady => return Ok(Async::NotReady),
//                 };
//
//                 self.state = ReadState::Read(FullReadState {
//                     blocking: self.ctx.lock().unwrap().read_async(
//                         &self.object,
//                         VecRefMut::new(vec![0; size]).map_mut(|b| &mut b[..]),
//                         0,
//                     ),
//
//                     total_read: 0,
//                     size,
//                 });
//
//                 self.poll()
//             }
//             ReadState::Read(ref mut rs) => {
//                 let (bytes_read, buf) = match rs.blocking
//                     .poll()
//                     .map_err(SyncFailure::new)
//                     .map_err(Error::from)
//                     .compat()?
//                 {
//                     Async::Ready((br, buf)) => (br as usize, buf.into_inner()),
//                     Async::NotReady => return Ok(Async::NotReady),
//                 };
//
//                 if bytes_read == 0 {
//                     return Err(
//                         failure::err_msg("Failed to read a nonzero number of bytes!").compat(),
//                     );
//                 }
//                 rs.total_read += bytes_read;
//
//                 if rs.total_read < rs.size {
//                     rs.blocking = self.ctx.lock().unwrap().read_async(
//                         &self.object,
//                         VecRefMut::new(buf).map_mut(|b| &mut b[rs.total_read..]),
//                         rs.total_read as u64,
//                     );
//
//                     self.poll()
//                 } else {
//                     self.handle
//                         .inner
//                         .content
//                         .set_if_none(Arc::new(Object::decode(&mut buf.as_slice()).compat()?))
//                         .unwrap();
//
//                     Ok(Async::Ready(()))
//                 }
//             }
//         }
//     }
// }
//
// impl ClusterRead {
//     pub fn new(handle: ClusterHandle) -> Self {
//         let ctx = Weak::upgrade(&handle.inner.store).unwrap().ctx.clone();
//         let object = hex::encode(handle.inner.digest.as_bytes());
//         let state = ReadState::Stat(ctx.lock().unwrap().stat_async(&object));
//
//         Self {
//             ctx,
//
//             object,
//             state,
//
//             handle,
//         }
//     }
// }
//
// enum WriteState {
//     Check(rados::ExistsFuture),
//     Write(rados::UnitFuture),
// }
//
// impl fmt::Debug for WriteState {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match *self {
//             WriteState::Check(_) => f.debug_tuple("Check").field(&"OPAQUE").finish(),
//             WriteState::Write(_) => f.debug_tuple("Write").field(&"OPAQUE").finish(),
//         }
//     }
// }
//
// struct ClusterWrite {
//     ctx: Arc<Mutex<Context>>,
//
//     object: String,
//     handle: ClusterHandle,
//
//     state: WriteState,
// }
//
// impl fmt::Debug for ClusterWrite {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.debug_struct("ClusterWrite")
//             .field("ctx", &"OPAQUE")
//             .field("object", &self.object)
//             .field("handle", &self.handle)
//             .field("state", &self.state)
//             .finish()
//     }
// }
//
// impl Future for ClusterWrite {
//     type Item = ();
//     type Error = Compat<Error>;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         match self.state {
//             WriteState::Check(ref mut exists) => match exists
//                 .poll()
//                 .map_err(SyncFailure::new)
//                 .map_err(Error::from)
//                 .compat()?
//             {
//                 Async::Ready(true) => Ok(Async::Ready(())),
//                 Async::Ready(false) => {
//                     let data = {
//                         let mut buf = Vec::new();
//                         self.handle
//                             .inner
//                             .content
//                             .get()
//                             .unwrap()
//                             .encode(&mut buf)
//                             .unwrap();
//                         buf
//                     };
//                     let blocking = self.ctx
//                         .lock()
//                         .unwrap()
//                         .write_full_async(&self.object, &data);
//                     self.state = WriteState::Write(blocking);
//                     self.poll()
//                 }
//                 Async::NotReady => Ok(Async::NotReady),
//             },
//             WriteState::Write(ref mut write) => match write
//                 .poll()
//                 .map_err(SyncFailure::new)
//                 .map_err(Error::from)
//                 .compat()?
//             {
//                 Async::Ready(()) => Ok(Async::Ready(())),
//                 Async::NotReady => Ok(Async::NotReady),
//             },
//         }
//     }
// }
//
// impl ClusterWrite {
//     pub fn new(handle: ClusterHandle) -> Self {
//         let ctx = Weak::upgrade(&handle.inner.store).unwrap().ctx.clone();
//         let object = hex::encode(handle.inner.digest.as_bytes());
//         let state = WriteState::Check(ctx.lock().unwrap().exists_async(&object));
//
//         Self {
//             ctx,
//
//             object,
//             handle,
//
//             state,
//         }
//     }
// }
