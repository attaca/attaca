use attaca::path::ObjectPath;
use smallvec::SmallVec;

const CACHE_PREFIX: &'static [u8] = b"CH";
const CONFIG_KEY: &'static [u8] = b"CONFIG";
const STATE_KEY: &'static [u8] = b"STATE";

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
    pub fn config() -> Self {
        Key::Borrowed(CONFIG_KEY)
    }

    pub fn state() -> Self {
        Key::Borrowed(STATE_KEY)
    }

    pub fn cache(path: &ObjectPath) -> Self {
        let mut buf = SmallVec::from(CACHE_PREFIX);
        path.encode(&mut buf).unwrap();
        Key::Owned(buf)
    }

    pub fn is_from_cache(&self) -> bool {
        &self.as_ref()[..2] == CACHE_PREFIX
    }
}
