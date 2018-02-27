use std::fmt;

use attaca::{store, HandleDigest, Store, digest::Digest};
use failure::*;
use futures::prelude::*;
use hex;

use Repository;
use quantified::{QuantifiedOutput, QuantifiedRef};

/// Check repository integrity, verifying hashes of all objects.
#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "fsck")]
pub struct FsckArgs {}

impl<'r> QuantifiedOutput<'r> for FsckArgs {
    type Output = FsckOut<'r>;
}

impl QuantifiedRef for FsckArgs {
    fn apply_ref<'r, S, D>(self, repository: &'r Repository<S, D>) -> Result<FsckOut<'r>, Error>
    where
        S: Store,
        D: Digest,
        S::Handle: HandleDigest<D>,
    {
        Ok(repository.fsck(self))
    }
}

pub struct FsckMismatch {
    pub received: String,
    pub calculated: String,
}

pub struct FsckOut<'r> {
    pub errors: Box<Stream<Item = FsckMismatch, Error = Error> + 'r>,
}

impl<'r> fmt::Debug for FsckOut<'r> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsckOut")
            .field("errors", &"OPAQUE")
            .finish()
    }
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn fsck<'r>(&'r self, _args: FsckArgs) -> FsckOut<'r> {
        let errors = async_stream_block! {
            let state = self.get_state()?;
            if let Some(head) = state.head {
                #[async]
                for error in store::fsck::<D, S::Handle>(head.as_inner().clone()) {
                    stream_yield!(FsckMismatch {
                        received: hex::encode(error.received.as_bytes()),
                        calculated: hex::encode(error.calculated.as_bytes()),
                    });
                }
            }

            Ok(())
        };

        FsckOut {
            errors: Box::new(errors),
        }
    }
}
