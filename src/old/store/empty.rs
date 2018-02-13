use futures::future::{self, FutureResult};

use errors::*;
use marshal::{Object, ObjectHash, Hashed};
use store::{RefStore, ObjectStore};


#[derive(Debug, Clone, Copy)]
pub struct Empty;


impl RefStore for Empty {
    type CompareAndSwap = FutureResult<ObjectHash, Error>;
    type Get = FutureResult<ObjectHash, Error>;

    fn compare_and_swap_branch(
        &self,
        _branch: String,
        _prev_hash: ObjectHash,
        _new_hash: ObjectHash,
    ) -> Self::CompareAndSwap {
        future::err(Error::from_kind(ErrorKind::EmptyStore))
    }

    fn get_branch(&self, _branch: String) -> Self::Get {
        future::err(Error::from_kind(ErrorKind::EmptyStore))
    }
}


impl ObjectStore for Empty {
    type Read = FutureResult<Object, Error>;
    type Write = FutureResult<bool, Error>;

    fn read_object(&self, _object_hash: ObjectHash) -> Self::Read {
        future::err(Error::from_kind(ErrorKind::EmptyStore))
    }

    fn write_object(&self, _hashed: Hashed) -> Self::Write {
        future::err(Error::from_kind(ErrorKind::EmptyStore))
    }
}
