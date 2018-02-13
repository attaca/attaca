use std::borrow::Cow;

use failure::Error;
use smallvec::SmallVec;

const CHANGESET_PREFIX: &'static [u8] = b"CH";

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
    pub fn changeset(path: &ObjectPath) -> Self {
        let mut buf = SmallVec::from(CHANGESET_PREFIX);
        path.encode(&mut buf).unwrap();
        Key(buf)
    }
}
