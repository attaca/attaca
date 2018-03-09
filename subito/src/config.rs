use std::{collections::HashMap, io::{BufRead, Write}};

use attaca::store::prelude::*;
use capnp::{message, serialize_packed};
use failure::*;
use leveldb::{kv::KV, options::{ReadOptions, WriteOptions}};
use url::Url;

use Repository;
use db::Key;

use config_capnp::*;

#[derive(Debug, Clone, Copy)]
pub enum StoreKind {
    LevelDb,
    Rados,
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub url: Url,
    pub kind: StoreKind,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub store: StoreConfig,
    pub remotes: HashMap<String, StoreConfig>,
}

// TODO codegen match statements/sets for this through the all_backends! macro.
impl Config {
    pub fn decode<R: BufRead>(reader: &mut R) -> Result<Self, Error> {
        let message_reader = serialize_packed::read_message(reader, message::ReaderOptions::new())?;
        let config_reader = message_reader.get_root::<config::Reader>()?;

        let store = {
            let store_reader = config_reader.get_store()?;
            let url = Url::parse(store_reader.get_url()?)?;
            let kind = match store_reader.which()? {
                store::LevelDb(()) => StoreKind::LevelDb,
                store::Rados(()) => StoreKind::Rados,
            };
            StoreConfig { url, kind }
        };

        let remotes = {
            let remotes_reader = config_reader.get_remotes()?;
            remotes_reader
                .iter()
                .map(|remote_reader| {
                    let name = String::from(remote_reader.get_name()?);
                    let store = {
                        let store_reader = remote_reader.get_store()?;
                        let url = Url::parse(store_reader.get_url()?)?;
                        let kind = match store_reader.which()? {
                            store::LevelDb(()) => StoreKind::LevelDb,
                            store::Rados(()) => StoreKind::Rados,
                        };
                        StoreConfig { url, kind }
                    };
                    Ok((name, store))
                })
                .collect::<Result<HashMap<_, _>, Error>>()?
        };

        Ok(Config { store, remotes })
    }

    pub fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        let mut message = message::Builder::new_default();

        {
            let mut config_builder = message.init_root::<config::Builder>();
            {
                let mut store_builder = config_builder.borrow().init_store();
                match self.store.kind {
                    StoreKind::LevelDb => store_builder.set_level_db(()),
                    StoreKind::Rados => store_builder.set_rados(()),
                }
                store_builder.set_url(self.store.url.as_str());
            }
            {
                let mut remotes_builder = config_builder
                    .borrow()
                    .init_remotes(self.remotes.len() as u32);
                for (i, (name, remote)) in self.remotes.iter().enumerate() {
                    let mut remote_builder = remotes_builder.borrow().get(i as u32);
                    remote_builder.set_name(name);
                    {
                        let mut store_builder = remote_builder.get_store()?;
                        match remote.kind {
                            StoreKind::LevelDb => store_builder.set_level_db(()),
                            StoreKind::Rados => store_builder.set_rados(()),
                        }
                        store_builder.set_url(remote.url.as_str());
                    }
                }
            }
        }

        serialize_packed::write_message(writer, &message)?;

        Ok(())
    }
}

impl<B: Backend> Repository<B> {
    pub fn get_config(&self) -> Result<Config, Error> {
        let raw_config = self.db
            .read()
            .unwrap()
            .get(ReadOptions::new(), &Key::config())?
            .ok_or_else(|| format_err!("Missing configuration entry"))?;

        Ok(Config::decode(&mut &raw_config[..])?)
    }

    pub fn set_config(&self, config: &Config) -> Result<(), Error> {
        let mut buf = Vec::new();
        config.encode(&mut buf)?;

        self.db
            .read()
            .unwrap()
            .put(WriteOptions::new(), &Key::config(), &buf)?;

        Ok(())
    }
}
