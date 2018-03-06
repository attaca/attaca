use std::collections::HashMap;

use attaca::{Open, object::CommitRef, store::{self, prelude::*}};
use failure::Error;
use futures::prelude::*;
use url::Url;

use Repository;
use config::{StoreConfig, StoreKind};
use state::State;

/// Manipulate remotes of a repository.
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "remote")]
pub enum RemoteArgs {
    #[structopt(name = "add")]
    Add(RemoteAddArgs),

    #[structopt(name = "list")]
    List(RemoteListArgs),
}

#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "add")]
pub struct RemoteAddArgs {
    #[structopt(name = "NAME")]
    name: String,

    #[structopt(name = "URL", parse(try_from_str = "Url::parse"))]
    url: Url,
}

#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "list")]
pub struct RemoteListArgs {}

pub struct RemoteOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<B: Backend> Repository<B> {
    pub fn remote<'r>(&'r mut self, args: RemoteArgs) -> RemoteOut<'r> {
        match args {
            RemoteArgs::Add(add_args) => RemoteOut {
                blocking: Box::new(self.remote_add(add_args).into_future()),
            },
            RemoteArgs::List(list_args) => RemoteOut {
                blocking: Box::new(self.remote_list(list_args).into_future()),
            },
        }
    }

    pub fn remote_add<'r>(&'r mut self, args: RemoteAddArgs) -> Result<(), Error> {
        let RemoteAddArgs { name, url } = args;

        let mut config = self.get_config()?;
        // TODO get store kind from URL
        ensure!(!config.remotes.contains_key(&name), "remote already exists");
        config.remotes.insert(
            name,
            StoreConfig {
                url,
                kind: StoreKind::LevelDb,
            },
        );
        self.set_config(&config)?;
        Ok(())
    }

    pub fn remote_list<'r>(&'r mut self, args: RemoteListArgs) -> Result<(), Error> {
        let RemoteListArgs {} = args;

        let mut config = self.get_config()?;
        // TODO log this somehow instead of just printlning it, or maybe stream it to some
        // receiving end through `RemoteOut`.
        for (remote_name, remote_config) in &config.remotes {
            println!("{} => {}", remote_name, remote_config.url.as_str());
        }
        Ok(())
    }
}
