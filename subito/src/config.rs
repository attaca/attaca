use std::io::{BufRead, Write};

use attaca::{HandleDigest, Store, digest::Digest};
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
    Ceph,
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub url: Url,
    pub kind: StoreKind,
    pub digest: (String, usize),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub store: StoreConfig,
}

impl Config {
    pub fn decode<R: BufRead>(reader: &mut R) -> Result<Self, Error> {
        let message_reader = serialize_packed::read_message(reader, message::ReaderOptions::new())?;
        let config_reader = message_reader.get_root::<config::Reader>()?;

        let store = {
            let store_reader = config_reader.get_store()?;
            let url = Url::parse(store_reader.get_url()?)?;
            let kind = match store_reader.which()? {
                store::LevelDb(()) => StoreKind::LevelDb,
                store::Ceph(()) => StoreKind::Ceph,
            };
            let digest = {
                let digest_reader = store_reader.get_digest()?;
                let name = String::from(digest_reader.get_name()?);
                let size = digest_reader.get_size() as usize;
                (name, size)
            };
            StoreConfig { url, kind, digest }
        };

        Ok(Config { store })
    }

    pub fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        let mut message = message::Builder::new_default();

        {
            let mut config_builder = message.init_root::<config::Builder>();
            {
                let mut store_builder = config_builder.borrow().init_store();
                match self.store.kind {
                    StoreKind::LevelDb => store_builder.set_level_db(()),
                    StoreKind::Ceph => store_builder.set_ceph(()),
                }
                store_builder.set_url(self.store.url.as_str());
                let mut digest_builder = store_builder.init_digest();
                digest_builder.set_name(&self.store.digest.0);
                digest_builder.set_size(self.store.digest.1 as u32);
            }
        }

        serialize_packed::write_message(writer, &message)?;

        Ok(())
    }
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
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
