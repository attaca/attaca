use std::collections::HashMap;

use attaca::{Open, object::CommitRef, store::{self, prelude::*}};
use attaca_leveldb::LevelDbBackend;
use failure::Error;
use futures::prelude::*;
use url::Url;

use Repository;
use config::StoreKind;
use plumbing;
use state::State;
use syntax::Name;

/// Fetch objects from a remote repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "fetch")]
pub struct FetchArgs {
    /// Name of the remote to fetch objects and branches from.
    #[structopt(name = "REMOTE")]
    remote: Name,
}

pub struct FetchOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<B: Backend> Repository<B> {
    pub fn fetch<'r>(&'r mut self, args: FetchArgs) -> FetchOut<'r> {
        FetchOut {
            blocking: Box::new(plumbing::fetch::remote(self, args.remote).map(|_| ())),
        }
    }
}
