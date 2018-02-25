use std::{fmt, str, cell::RefCell, cmp::Ordering, hash::{Hash, Hasher},
          io::{self, BufRead, Cursor, Read, Write}, path::Path, sync::{Arc, RwLock, Weak}};

use attaca::{canonical, Init, Open, digest::{Digest, DigestWriter, Sha3Digest},
             store::{Handle, HandleBuilder, HandleDigest, Store}};
use chashmap::CHashMap;
use db_key::Key;
use failure::{self, *};
use futures::{future::{self, FutureResult}, prelude::*};
use leb128;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};
use owning_ref::ArcRef;
use parking_lot::Mutex;
use smallvec::SmallVec;
use url::Url;

use DbKey;

impl Open for LevelStore {
    const SCHEMES: &'static [&'static str] = &["file"];

    fn open(url_str: &str) -> Result<Self, Error> {
        let url = Url::parse(url_str)?;
        ensure!(
            Self::SCHEMES.contains(&url.scheme()),
            "Unsupported URL scheme!"
        );
        let path = url.to_file_path()
            .map_err(|_| format_err!("URL is not a path!"))?;
        Self::open_path(&path)
    }

    fn open_path(path: &Path) -> Result<Self, Error> {
        let db = Database::open(&path, Options::new())?;
        Ok(Self::new(Arc::new(RwLock::new(db))))
    }
}

impl Init for LevelStore {
    fn init(url_str: &str) -> Result<Self, Error> {
        let url = Url::parse(url_str)?;
        ensure!(
            Self::SCHEMES.contains(&url.scheme()),
            "Unsupported URL scheme!"
        );
        let path = url.to_file_path()
            .map_err(|_| format_err!("URL is not a path!"))?;
        Self::init_path(&path)
    }

    fn init_path(path: &Path) -> Result<Self, Error> {
        let db = Database::open(
            &path,
            Options {
                create_if_missing: true,
                error_if_exists: true,
                ..Options::new()
            },
        )?;
        Ok(Self::new(Arc::new(RwLock::new(db))))
    }
}

#[derive(Debug, Clone)]
pub struct LevelStore {
    inner: Arc<StoreInner>,
}

impl Store for LevelStore {
    type Handle = LevelHandle;

    type HandleBuilder = LevelHandleBuilder;
    fn handle_builder(&self) -> Self::HandleBuilder {
        LevelHandleBuilder {
            store: self.inner.clone(),

            blob: Vec::new(),
            refs: Vec::new(),
        }
    }

    type FutureLoadBranch = FutureResult<Option<Self::Handle>, Error>;
    fn load_branch(&self, branch: String) -> Self::FutureLoadBranch {
        let db_lock = self.inner.db.write().unwrap();
        let key = branch + " (branch)";

        match db_lock.get(
            ReadOptions::new(),
            DbKey(SmallVec::from_vec(key.into_bytes())),
        ) {
            Ok(opt) => future::ok(opt.map(|bytes| {
                Self::handle_from_digest(&self.inner, &Sha3Digest::from_bytes(&bytes))
            })),
            Err(err) => future::err(err.into()),
        }
    }

    type FutureSwapBranch = FutureResult<(), Error>;
    fn swap_branch(
        &self,
        branch: String,
        previous: Option<Self::Handle>,
        new: Option<Self::Handle>,
    ) -> Self::FutureSwapBranch {
        let db_lock = self.inner.db.write().unwrap();
        let key = branch + " (branch)";

        match db_lock.get(ReadOptions::new(), DbKey(SmallVec::from(key.as_bytes()))) {
            Ok(Some(bytes)) => {
                let previous_handle = match previous {
                    Some(ph) => ph,
                    None => {
                        return future::err(format_err!(
                            "Branch compare-and-swap mismatch: expected empty branch, but found nonempty!!"
                        ))
                    }
                };

                let current_digest = Sha3Digest::from_bytes(&bytes);
                let expected_digest = previous_handle.inner.digest;

                if current_digest != expected_digest {
                    return future::err(format_err!("Branch compare-and-swap mismatch: current branch digest does not match expected digest!"));
                }
            }
            Ok(None) => {
                if previous.is_some() {
                    return future::err(format_err!("Branch compare-and-swap mismatch: expected nonempty branch, but found empty!"));
                }
            }
            Err(err) => return future::err(err.into()),
        }

        let result = match new {
            Some(new_handle) => db_lock.put(
                WriteOptions::new(),
                DbKey(SmallVec::from_vec(key.into_bytes())),
                new_handle.inner.digest.as_bytes(),
            ),
            None => db_lock.delete(
                WriteOptions::new(),
                DbKey(SmallVec::from_vec(key.into_bytes())),
            ),
        };

        match result {
            Ok(()) => future::ok(()),
            Err(err) => future::err(err.into()),
        }
    }

    type FutureResolve = FutureResult<Option<Self::Handle>, Error>;
    fn resolve<D: Digest>(&self, digest: &D) -> Self::FutureResolve
    where
        Self::Handle: HandleDigest<D>,
    {
        let digest = if D::NAME == Sha3Digest::NAME && D::SIZE == Sha3Digest::SIZE {
            Sha3Digest::from_bytes(digest.as_bytes())
        } else {
            return future::err(failure::err_msg(
                "LevelHandle currently only supports SHA-3 digests!",
            ));
        };

        match self.inner.handles.get(&digest).map(|g| (*g).clone()) {
            Some(handle) => future::ok(Some(handle)),
            None => match LevelStore::object(&self.inner, &digest) {
                Ok(Some(arc_obj)) => {
                    let handle = Self::handle_from_digest(&self.inner, &digest);
                    let arc_obj = {
                        let content_lock = handle.inner.content.lock();

                        match Weak::upgrade(&content_lock) {
                            Some(arc_obj) => arc_obj,
                            None => {
                                let cl_cell = RefCell::new(content_lock);
                                self.inner.objects.upsert(
                                    digest,
                                    || {
                                        **cl_cell.borrow_mut() = Arc::downgrade(&arc_obj);
                                        arc_obj
                                    },
                                    |arc_obj| {
                                        **cl_cell.borrow_mut() = Arc::downgrade(arc_obj);
                                    },
                                );

                                Weak::upgrade(&cl_cell.into_inner()).unwrap()
                            }
                        }
                    };
                    self.inner.objects.insert(digest, arc_obj);
                    future::ok(Some(handle))
                }
                Ok(None) => future::ok(None),
                Err(err) => future::err(err),
            },
        }
    }
}

struct StoreInner {
    db: Arc<RwLock<Database<DbKey>>>,

    handles: CHashMap<Sha3Digest, LevelHandle>,
    objects: CHashMap<Sha3Digest, Arc<Object>>,
}

impl fmt::Debug for StoreInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StoreInner")
            .field("db", &"OPAQUE")
            .field("handles", &self.handles)
            .field("objects", &self.objects)
            .finish()
    }
}

impl LevelStore {
    pub fn new(db: Arc<RwLock<Database<DbKey>>>) -> Self {
        Self {
            inner: Arc::new(StoreInner {
                db,

                handles: CHashMap::new(),
                objects: CHashMap::new(),
            }),
        }
    }

    fn handle_from_digest(this: &Arc<StoreInner>, digest: &Sha3Digest) -> LevelHandle {
        let out = RefCell::new(None);
        this.handles.upsert(
            *digest,
            || {
                let handle = LevelHandle {
                    inner: Arc::new(HandleInner {
                        store: Arc::downgrade(this),

                        digest: *digest,
                        content: Mutex::new(Weak::new()),
                    }),
                };
                *out.borrow_mut() = Some(handle.clone());
                handle
            },
            |handle| {
                *out.borrow_mut() = Some(handle.clone());
            },
        );
        out.into_inner().unwrap()
    }

    fn handle_from_object(this: &Arc<StoreInner>, object: Object) -> Result<LevelHandle, Error> {
        let digest = {
            let mut writer = Sha3Digest::writer();
            object.encode(&mut writer).unwrap();
            writer.finish()
        };

        let handle = Self::handle_from_digest(this, &digest);
        let arc_obj = {
            let content_lock = handle.inner.content.lock();

            match Weak::upgrade(&content_lock) {
                Some(arc_obj) => arc_obj,
                None => {
                    let cl_cell = RefCell::new(content_lock);
                    this.objects.upsert(
                        digest,
                        || {
                            let arc_obj = Arc::new(object);
                            **cl_cell.borrow_mut() = Arc::downgrade(&arc_obj);
                            arc_obj
                        },
                        |arc_obj| {
                            **cl_cell.borrow_mut() = Arc::downgrade(arc_obj);
                        },
                    );

                    Weak::upgrade(&cl_cell.into_inner()).unwrap()
                }
            }
        };

        let data = {
            let mut buf = Vec::new();
            arc_obj.encode(&mut buf).unwrap();
            buf
        };

        this.db.read().unwrap().put(
            WriteOptions::new(),
            DbKey(SmallVec::from(digest.as_bytes())),
            &data,
        )?;
        this.objects.insert(digest, arc_obj);

        Ok(handle)
    }

    fn object(this: &Arc<StoreInner>, digest: &Sha3Digest) -> Result<Option<Arc<Object>>, Error> {
        match this.objects.get(&digest).map(|g| (*g).clone()) {
            Some(arc_object) => Ok(Some(arc_object)),
            None => match this.db
                .read()
                .unwrap()
                .get(ReadOptions::new(), DbKey(SmallVec::from(digest.as_bytes())))
                .context("Error reading object from database")?
            {
                Some(bytes) => {
                    let arc_obj =
                        Arc::new(Object::decode(&mut &bytes[..]).context("Error decoding object")?);
                    this.objects.insert(*digest, arc_obj.clone());
                    Ok(Some(arc_obj))
                }
                None => Ok(None),
            },
        }
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
pub struct LevelHandleContent(Cursor<ArcRef<Object, [u8]>>);

impl From<Arc<Object>> for LevelHandleContent {
    fn from(arc_obj: Arc<Object>) -> Self {
        LevelHandleContent(Cursor::new(
            ArcRef::new(arc_obj.clone()).map(|obj| obj.blob.as_slice()),
        ))
    }
}

impl Read for LevelHandleContent {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.0.read(buf)
    }
}

impl BufRead for LevelHandleContent {
    fn fill_buf(&mut self) -> Result<&[u8], io::Error> {
        self.0.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.0.consume(amt);
    }
}

#[derive(Debug, Clone)]
pub struct LevelHandleRefs {
    store: Arc<StoreInner>,
    digests: ArcRef<Object, [Sha3Digest]>,
}

impl LevelHandleRefs {
    fn new(store: Arc<StoreInner>, arc_obj: Arc<Object>) -> Self {
        LevelHandleRefs {
            store,
            digests: ArcRef::new(arc_obj).map(|obj| obj.refs.as_slice()),
        }
    }
}

impl Iterator for LevelHandleRefs {
    type Item = LevelHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.digests.first().cloned().map(|digest| {
            self.digests = self.digests.clone().map(|slice| &slice[1..]);
            LevelStore::handle_from_digest(&self.store, &digest)
        })
    }
}

#[derive(Debug, Clone)]
pub struct LevelHandle {
    inner: Arc<HandleInner>,
}

impl PartialEq for LevelHandle {
    fn eq(&self, rhs: &LevelHandle) -> bool {
        self.inner.digest == rhs.inner.digest
    }
}

impl Eq for LevelHandle {}

impl PartialOrd for LevelHandle {
    fn partial_cmp(&self, rhs: &LevelHandle) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

impl Ord for LevelHandle {
    fn cmp(&self, rhs: &LevelHandle) -> Ordering {
        self.inner.digest.cmp(&rhs.inner.digest)
    }
}

impl Hash for LevelHandle {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.inner.digest.hash(state);
    }
}

impl Handle for LevelHandle {
    type Content = LevelHandleContent;
    type Refs = LevelHandleRefs;

    type FutureLoad = FutureResult<(Self::Content, Self::Refs), Error>;
    fn load(&self) -> Self::FutureLoad {
        let store = Weak::upgrade(&self.inner.store).unwrap();

        let mut lock = self.inner.content.lock();
        let arc_obj = match Weak::upgrade(&lock) {
            Some(arc_obj) => arc_obj.clone(),
            None => match LevelStore::object(&store, &self.inner.digest) {
                Ok(Some(arc_obj)) => {
                    *lock = Arc::downgrade(&arc_obj);
                    arc_obj
                }
                Ok(None) => return future::err(format_err!("Bad handle: no such object!")),
                Err(err) => return future::err(err),
            },
        };

        let handle_content = LevelHandleContent::from(arc_obj.clone());
        let handle_refs = LevelHandleRefs::new(store, arc_obj);

        future::ok((handle_content, handle_refs))
    }
}

impl<D: Digest> HandleDigest<D> for LevelHandle {
    type FutureDigest = FutureResult<D, Error>;
    fn digest(&self) -> Self::FutureDigest {
        if D::NAME == Sha3Digest::NAME && D::SIZE == Sha3Digest::SIZE {
            future::ok(D::from_bytes(self.inner.digest.as_bytes()))
        } else {
            future::err(failure::err_msg(
                "LevelHandle currently only supports SHA-3 digests!",
            ))
        }
    }
}

#[derive(Debug)]
struct HandleInner {
    store: Weak<StoreInner>,

    digest: Sha3Digest,
    content: Mutex<Weak<Object>>,
}

#[derive(Debug)]
pub struct LevelHandleBuilder {
    store: Arc<StoreInner>,

    blob: Vec<u8>,
    refs: Vec<LevelHandle>,
}

impl Write for LevelHandleBuilder {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.blob.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl HandleBuilder for LevelHandleBuilder {
    type Handle = LevelHandle;

    fn add_reference(&mut self, reference: LevelHandle) {
        self.refs.push(reference);
    }

    type FutureHandle = FutureResult<LevelHandle, Error>;
    fn finish(self) -> Self::FutureHandle {
        let object = Object {
            blob: self.blob,
            refs: self.refs.into_iter().map(|ch| ch.inner.digest).collect(),
        };

        LevelStore::handle_from_object(&self.store, object).into_future()
    }
}
