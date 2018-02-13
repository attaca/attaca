use std::{fmt, path::{Path, PathBuf}, sync::{Arc, RwLock}};

use attaca::{Open, Search};
use db_key::Key;
use failure::Error;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};
use smallvec::SmallVec;
use url::Url;

use DbKey;
use store::LevelStore;

pub struct LevelWorkspace {
    store: LevelStore,

    db: Arc<RwLock<Database<DbKey>>>,
}

impl Open for LevelWorkspace {
    const SCHEMES: &'static [&'static str] = &["attaca+leveldb+file"];

    fn open(url_str: &str) -> Result<Self, Error> {
        let url = Url::parse(url_str)?;
        ensure!(
            Self::SCHEMES.contains(&url.scheme()),
            "Unsupported URL scheme!"
        );
        let path = url.to_file_path()
            .map_err(|_| format_err!("URL is not a path!"))?;
        let db = Database::open(&path, Options::new())?;
        Ok(Self::new(Arc::new(RwLock::new(db))))
    }
}

impl Search for LevelWorkspace {
    fn search() -> Result<Option<Self>, Error> {
        Ok(None)
    }
}

impl LevelWorkspace {
    pub(crate) fn new(db: Arc<RwLock<Database<DbKey>>>) -> Self {
        let store = LevelStore::new(db.clone());
        Self { store, db }
    }
}
