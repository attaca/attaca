use std::borrow::Cow;

use attaca::path::ObjectPath;
use failure::Error;
use smallvec::SmallVec;

const CACHE_PREFIX: &'static [u8] = b"CH";
const STATE_KEY: &'static [u8] = b"STATE";

#[derive(Debug, Clone)]
pub struct Key(SmallVec<[u8; 32]>);

impl ::db_key::Key for Key {
    fn from_u8(key: &[u8]) -> Self {
        Key(SmallVec::from(key))
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.0.as_slice())
    }
}

impl Key {
    pub fn state() -> Self {
        Key(SmallVec::from(STATE_KEY))
    }

    pub fn cache(path: &ObjectPath) -> Self {
        let mut buf = SmallVec::from(CACHE_PREFIX);
        path.encode(&mut buf).unwrap();
        Key(buf)
    }

    pub fn is_from_cache(&self) -> bool {
        &self.0[..2] == CACHE_PREFIX
    }
}
