use std::fmt;

use attaca::{HandleDigest, Store, digest::Digest, object::Commit};
use failure::*;
use futures::prelude::*;

use Repository;
use quantified::{QuantifiedOutput, QuantifiedRef};

/// Show commit history sorted chronologically.
#[derive(Default, Debug, StructOpt, Builder)]
#[structopt(name = "log")]
pub struct LogArgs {}

impl<'r> QuantifiedOutput<'r> for LogArgs {
    type Output = LogOut<'r>;
}

impl QuantifiedRef for LogArgs {
    fn apply_ref<'r, S, D>(self, repository: &'r Repository<S, D>) -> Result<LogOut<'r>, Error>
    where
        S: Store,
        D: Digest,
        S::Handle: HandleDigest<D>,
    {
        Ok(repository.log(self))
    }
}

#[must_use = "LogOut contains futures which must be driven to completion!"]
pub struct LogOut<'r> {
    pub entries: Box<Stream<Item = Commit<String>, Error = Error> + 'r>,
}

impl<'r> fmt::Debug for LogOut<'r> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogOut")
            .field("entries", &"OPAQUE")
            .finish()
    }
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn log<'r>(&'r self, _args: LogArgs) -> LogOut<'r> {
        unimplemented!();
    }
}
