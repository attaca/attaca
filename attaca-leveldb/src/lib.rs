extern crate attaca;
extern crate capnp;
extern crate chashmap;
extern crate db_key;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate leb128;
extern crate leveldb;
extern crate owning_ref;
extern crate parking_lot;
extern crate smallvec;
extern crate url;
extern crate uuid;

#[allow(dead_code)]
mod branch_set_capnp {
    include!(concat!(env!("OUT_DIR"), "/branch_set_capnp.rs"));
}

mod store;

pub use store::*;

use smallvec::SmallVec;

const BRANCHES_KEY: &'static [u8] = b"BRANCHES";
const BLOB_PREFIX: &'static [u8] = b"#";
const UUID_KEY: &'static [u8] = b"UUID";

#[derive(Debug, Clone)]
pub enum Key {
    Owned(SmallVec<[u8; 32]>),
    Borrowed(&'static [u8]),
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        match *self {
            Key::Owned(ref smallvec) => smallvec.as_slice(),
            Key::Borrowed(slice) => slice,
        }
    }
}

impl ::db_key::Key for Key {
    fn from_u8(key: &[u8]) -> Self {
        Key::Owned(SmallVec::from(key))
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.as_ref())
    }
}

impl Key {
    pub fn branches() -> Self {
        Key::Borrowed(BRANCHES_KEY)
    }

    pub fn uuid() -> Self {
        Key::Borrowed(UUID_KEY)
    }

    pub fn blob(bytes: &[u8]) -> Self {
        let mut buf = SmallVec::from(BLOB_PREFIX);
        buf.extend_from_slice(bytes);
        Key::Owned(buf)
    }

    pub fn is_blob(&self) -> bool {
        self.as_ref().starts_with(BLOB_PREFIX)
    }
}
