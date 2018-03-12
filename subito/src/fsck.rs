use std::fmt;

use attaca::{digest::prelude::*, store::{self, prelude::*}};
use failure::*;
use futures::prelude::*;
use hex;

use Repository;
use state::Head;

/// Check repository integrity, verifying hashes of all objects.
#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "fsck")]
pub struct FsckArgs {
    /// The digest function to use for checking validity.
    #[structopt(long = "digest",
                raw(possible_values = r#"digest_names!()"#,
                    default_value = "::attaca::digest::Sha3Digest::SIGNATURE.name"))]
    digest_name: String,
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

macro_rules! digest_fsck {
    (@inner $name:expr, $head:expr, $($dty:ty),*) => {
        match $name {
            $(ref name if name == <$dty>::SIGNATURE.name => store::fsck::<$dty, _>($head),)*
            _ => unreachable!("bad digest name"),
        }
    };
    ($($stuff:tt)*) => {
        all_digests!(digest_fsck!(@inner $($stuff)*))
    };
}

impl<B: Backend> Repository<B> {
    pub fn fsck<'r>(&'r self, args: FsckArgs) -> FsckOut<'r> {
        let errors = async_stream_block! {
            let state = self.get_state()?;

            let maybe_head = match state.head {
                Head::Empty => None,
                Head::Detached(commit_ref) => Some(commit_ref.as_inner().clone()),
                Head::Branch(branch) => await!(self.store.load_branches())?.get(branch.as_str()).cloned(),
            };

            if let Some(head) = maybe_head {
                #[async]
                for error in digest_fsck!(args.digest_name, head) {
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
