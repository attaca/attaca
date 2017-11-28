use failure::Error;
use futures::future::{self, FutureResult};

use arc_slice::ArcSlice;
use object_hash::ObjectHash;
use repository::Repository;


#[derive(Debug, Fail)]
#[fail(display = "attempted to operate on the empty store")]
pub struct EmptyError;


#[derive(Debug, Clone, Copy)]
pub struct Empty;


impl Repository for Empty {
    type ReadHashed = FutureResult<Option<ArcSlice>, Error>;
    type WriteHashed = FutureResult<bool, Error>;

    fn read_hashed(&self, _object_hash: ObjectHash) -> Self::ReadHashed {
        future::ok(None)
    }

    fn write_hashed(&self, _object_hash: ObjectHash, _bytes: ArcSlice) -> Self::WriteHashed {
        future::ok(true)
    }

    fn compare_and_swap_branch(
        &self,
        _branch: String,
        _prev_hash: Option<ObjectHash>,
        _new_hash: ObjectHash,
    ) -> Result<Option<ObjectHash>, Error> {
        Err(Error::from(EmptyError))
    }

    fn get_branch(&self, _branch: String) -> Result<Option<ObjectHash>, Error> {
        Err(Error::from(EmptyError))
    }
}
