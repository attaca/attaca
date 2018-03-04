use std::{fmt, str, collections::HashMap, io::{self, BufRead, Cursor, Read, Write}, path::Path,
          sync::RwLock};

use attaca::{canonical, Init, Open, digest::{Sha3Digest, prelude::*}, store::{RawHandle, prelude::*}};
use capnp::{message, serialize_packed};
use failure::*;
use futures::{future::FutureResult, prelude::*};
use leb128;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};
use url::Url;

use Key;

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

impl Open for LevelDbBackend {
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
        Ok(Self::new(db))
    }
}

impl Init for LevelDbBackend {
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
        Ok(Self::new(db))
    }
}

#[derive(Debug)]
pub struct LevelDbBuilder {
    blob: Vec<u8>,
    refs: Vec<RawHandle>,
}

impl Write for LevelDbBuilder {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.blob.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Write::flush(&mut self.blob)
    }
}

impl Extend<RawHandle> for LevelDbBuilder {
    fn extend<I>(&mut self, iterable: I)
    where
        I: IntoIterator<Item = RawHandle>,
    {
        self.refs.extend(iterable);
    }
}

#[derive(Debug)]
pub struct LevelDbContent {
    blob: Cursor<Vec<u8>>,
    refs: <Vec<RawHandle> as IntoIterator>::IntoIter,
}

impl Read for LevelDbContent {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.blob.read(buf)
    }
}

impl Iterator for LevelDbContent {
    type Item = RawHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.refs.next()
    }
}

struct Inner {
    db: Database<Key>,

    ids: HashMap<Sha3Digest, RawHandle>,
    handles: HashMap<RawHandle, Sha3Digest>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Inner")
            .field("db", &"Database")
            .field("ids", &self.ids)
            .field("handles", &self.handles)
            .finish()
    }
}

#[derive(Debug)]
pub struct LevelDbBackend {
    inner: RwLock<Inner>,
}

impl LevelDbBackend {
    fn new(db: Database<Key>) -> Self {
        Self {
            inner: RwLock::new(Inner {
                db,
                ids: HashMap::new(),
                handles: HashMap::new(),
            }),
        }
    }

    // This function returns `Ok` if the ID is fresh and `Err` if it is not.
    fn reserve(&self, digest: Sha3Digest) -> Result<RawHandle, RawHandle> {
        let attempt = self.inner.read().unwrap().ids.get(&digest).cloned();
        match attempt {
            Some(id) => Err(id),
            None => {
                let mut inner = self.inner.write().unwrap();

                match inner.ids.get(&digest).cloned() {
                    Some(id) => Err(id),
                    None => {
                        let new_id = RawHandle(inner.ids.len() as u64);
                        inner.ids.insert(digest, new_id);
                        inner.handles.insert(new_id, digest);
                        Ok(new_id)
                    }
                }
            }
        }
    }

    fn do_finish(&self, builder: LevelDbBuilder) -> Result<RawHandle, Error> {
        let inner = self.inner.read().unwrap();

        let blob = builder.blob;
        let refs = builder
            .refs
            .into_iter()
            .map(|id| inner.handles[&id])
            .collect::<Vec<_>>();

        let mut hasher = Sha3Digest::writer();
        canonical::encode(&mut hasher, &blob, &refs).unwrap();
        let digest = hasher.finish();

        let _ = inner;

        match self.reserve(digest) {
            Ok(id) => {
                let mut buf = Vec::new();
                leb128::write::unsigned(&mut buf, blob.len() as u64)?; // `C.length || C`
                buf.write_all(&blob)?;
                canonical::encode(&mut buf, &blob, &refs)?; // `EncodedRefs(C)`
                self.inner.read().unwrap().db.put(
                    WriteOptions::new(),
                    &Key::blob(digest.as_bytes()),
                    &buf,
                )?;

                Ok(id)
            }
            Err(id) => Ok(id),
        }
    }

    fn do_load(&self, id: RawHandle) -> Result<LevelDbContent, Error> {
        let inner = self.inner.read().unwrap();
        let digest = inner.handles[&id];
        let mut data = Cursor::new(
            inner
                .db
                .get(ReadOptions::new(), &Key::blob(digest.as_bytes()))?
                .expect("bad ID!"),
        );
        let mut blob = vec![0; leb128::read::unsigned(&mut data)? as usize]; // `C.length || C`
        data.read_exact(&mut blob)?;
        let ref_digests = canonical::decode(&mut data)?.finish::<Sha3Digest>()?.refs; // `EncodedRefs(C)`

        // Discard the read lock so we don't deadlock ourselves, because `reserve` may attempt to
        // take a write lock.
        let _ = inner;
        let refs: Vec<_> = ref_digests
            .into_iter()
            .map(|digest| self.reserve(digest).unwrap_or_else(|e| e))
            .collect();

        Ok(LevelDbContent {
            blob: Cursor::new(blob),
            refs: refs.into_iter(),
        })
    }

    fn do_digest(&self, signature: DigestSignature, id: RawHandle) -> Result<Sha3Digest, Error> {
        ensure!(signature == Sha3Digest::SIGNATURE, "bad digest");

        Ok(self.inner.read().unwrap().handles[&id])
    }

    fn do_load_branches(&self) -> Result<HashMap<String, RawHandle>, Error> {
        let data = self.inner
            .read()
            .unwrap()
            .db
            .get(ReadOptions::new(), &Key::branches())?;
        let decoded = match data {
            Some(bytes) => decode_branch_set(&mut Cursor::new(bytes))?,
            None => Vec::new(),
        };
        let resolved = decoded
            .into_iter()
            .map(|(name, digest)| (name, self.reserve(digest).unwrap_or_else(|e| e)))
            .collect();
        Ok(resolved)
    }

    fn do_swap_branches(
        &self,
        old: HashMap<String, RawHandle>,
        new: HashMap<String, RawHandle>,
    ) -> Result<(), Error> {
        // This is an atomic operation. Take a write lock.
        let inner = self.inner.write().unwrap();

        let data = inner.db.get(ReadOptions::new(), &Key::branches())?;
        let decoded = match data {
            Some(bytes) => decode_branch_set(&mut Cursor::new(bytes))?,
            None => Vec::new(),
        };

        let current = decoded
            .into_iter()
            .map(|(name, digest)| (name, inner.ids[&digest]))
            .collect::<HashMap<_, _>>();

        ensure!(old == current, "compare failed");

        let mut buf = Vec::new();
        let new_len = new.len();
        encode_branch_set(
            &mut buf,
            new.into_iter().map(|(name, id)| (name, inner.handles[&id])),
            new_len,
        )?;
        inner.db.put(WriteOptions::new(), &Key::branches(), &buf)?;

        Ok(())
    }

    fn do_resolve(&self, signature: DigestSignature, bytes: &[u8]) -> Result<Option<RawHandle>, Error> {
        ensure!(
            signature == Sha3Digest::SIGNATURE,
            "unsupported digest {:?}",
            signature
        );

        let digest = Sha3Digest::from_bytes(bytes);
        let id = self.reserve(digest).unwrap_or_else(|e| e);
        let inner = self.inner.read().unwrap();
        let db_contains_digest = inner
            .db
            .get(ReadOptions::new(), &Key::blob(digest.as_bytes()))?
            .is_some();

        if db_contains_digest {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}

impl Backend for LevelDbBackend {
    type Builder = LevelDbBuilder;
    type FutureFinish = FutureResult<RawHandle, Error>;

    fn builder(&self) -> Self::Builder {
        LevelDbBuilder {
            blob: Vec::new(),
            refs: Vec::new(),
        }
    }

    fn finish(&self, builder: Self::Builder) -> Self::FutureFinish {
        self.do_finish(builder).into_future()
    }

    type Content = LevelDbContent;
    type FutureContent = FutureResult<Self::Content, Error>;

    fn load(&self, id: RawHandle) -> Self::FutureContent {
        self.do_load(id).into_future()
    }

    type Digest = Sha3Digest;
    type FutureDigest = FutureResult<Self::Digest, Error>;

    fn digest(&self, signature: DigestSignature, id: RawHandle) -> Self::FutureDigest {
        self.do_digest(signature, id).into_future()
    }

    type FutureLoadBranches = FutureResult<HashMap<String, RawHandle>, Error>;

    fn load_branches(&self) -> Self::FutureLoadBranches {
        self.do_load_branches().into_future()
    }

    type FutureSwapBranches = FutureResult<(), Error>;

    fn swap_branches(
        &self,
        previous: HashMap<String, RawHandle>,
        new: HashMap<String, RawHandle>,
    ) -> Self::FutureSwapBranches {
        self.do_swap_branches(previous, new).into_future()
    }

    type FutureResolve = FutureResult<Option<RawHandle>, Error>;
    fn resolve(&self, signature: DigestSignature, bytes: &[u8]) -> Self::FutureResolve {
        self.do_resolve(signature, bytes).into_future()
    }
}
