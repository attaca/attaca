use futures::future::{self, FutureResult};

use context::Store;
use errors::*;
use marshal::{Object, ObjectHash, Hashed};


#[derive(Debug, Clone, Copy)]
pub struct Empty;


impl Store for Empty {
    type Read = FutureResult<Object, Error>;
    type Write = FutureResult<bool, Error>;

    fn read_object(&self, _object_hash: ObjectHash) -> Self::Read {
        future::err(Error::from_kind(ErrorKind::EmptyStore))
    }

    fn write_object(&self, _hashed: Hashed) -> Self::Write {
        future::err(Error::from_kind(ErrorKind::EmptyStore))
    }
}
