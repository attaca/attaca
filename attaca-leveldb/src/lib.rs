extern crate attaca;
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

mod store;
mod workspace;

pub use store::*;
pub use workspace::*;

use std::{str, borrow::Cow, path::{Path, PathBuf}};

use db_key::Key;
use failure::Error;
use smallvec::SmallVec;

#[derive(Debug, Clone)]
pub struct DbKey(SmallVec<[u8; 32]>);

impl Key for DbKey {
    fn from_u8(key: &[u8]) -> Self {
        DbKey(SmallVec::from(key))
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.0.as_slice())
    }
}

impl DbKey {
    pub fn from_path(path: Cow<Path>) -> Result<Self, Error> {
        match path {
            Cow::Borrowed(path) => Ok(DbKey(SmallVec::from(
                path.to_str()
                    .ok_or_else(|| format_err!("Path is not valid UTF-8!"))?
                    .as_bytes(),
            ))),
            Cow::Owned(path_buf) => Ok(DbKey(SmallVec::from(
                path_buf
                    .into_os_string()
                    .into_string()
                    .map_err(|_| format_err!("Path is not valid UTF-8!"))?
                    .into_bytes(),
            ))),
        }
    }

    pub fn from_str(s: Cow<str>) -> Self {
        match s {
            Cow::Borrowed(s) => DbKey(SmallVec::from(s.as_bytes())),
            Cow::Owned(s) => DbKey(SmallVec::from(s.into_bytes())),
        }
    }
}
