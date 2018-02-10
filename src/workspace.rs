use failure::Error;

use Search;
use store::Store;

pub trait Workspace: Search {
    type Backend: Store;

    fn head(&self) -> Result<<Self::Backend as Store>::Handle, Error>;
    fn merge(&self, handle: <Self::Backend as Store>::Handle) -> Result<(), Error>;
    fn checkout(&self, handle: <Self::Backend as Store>::Handle) -> Result<(), Error>;
}
