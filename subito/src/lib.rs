#![recursion_limit = "128"]
#![feature(proc_macro, conservative_impl_trait, generators)]

pub extern crate attaca;
extern crate attaca_leveldb;
extern crate capnp;
extern crate db_key;
#[macro_use]
extern crate derive_builder;
#[macro_use]
pub extern crate failure;
pub extern crate futures_await as futures;
extern crate hex;
extern crate ignore;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate leveldb;
extern crate memmap;
#[macro_use]
extern crate nix;
extern crate regex;
extern crate smallvec;
#[macro_use]
extern crate structopt;
extern crate url;

pub mod reexports {
    pub use attaca;
    pub use failure;
    pub use futures;
    pub use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions}};

    pub use super::db::Key;
}

#[allow(dead_code)]
mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/cache_capnp.rs"));
}

#[allow(dead_code)]
mod config_capnp {
    include!(concat!(env!("OUT_DIR"), "/config_capnp.rs"));
}

#[allow(dead_code)]
mod digest_capnp {
    include!(concat!(env!("OUT_DIR"), "/digest_capnp.rs"));
}

#[allow(dead_code)]
mod object_ref_capnp {
    include!(concat!(env!("OUT_DIR"), "/object_ref_capnp.rs"));
}

#[allow(dead_code)]
mod state_capnp {
    include!(concat!(env!("OUT_DIR"), "/state_capnp.rs"));
}

#[macro_export]
macro_rules! unpack_digests {
    ($submac:ident!($($args:tt)*)($($dty:ty),*)) => { $submac!($($args)* , $($dty),*) };
}

#[macro_export]
macro_rules! digests {
    ($($dty:ty),* $(,)*) => {
        #[macro_export]
        macro_rules! all_digests {
            ($submac:ident ! $arg:tt) => { unpack_digests!($submac!$arg($($dty),*)) };
        }
    };
}

#[macro_export]
macro_rules! digest_names {
    (@inner, $($dty:ty),*) => {
        &[$(<$dty>::SIGNATURE.name),*]
    };
    () => {
        all_digests!(digest_names!(@inner))
    };
}

#[macro_export]
macro_rules! unpack_backends {
    ($submac:ident!($($args:tt)*)($($lcname:ident, $ccname:ident : $dty:ty),*)) => { $submac!($($args)* , $($lcname, $ccname : $dty),*) };
}

#[macro_export]
macro_rules! backends {
    ($($lcname:ident, $ccname:ident : $dty:ty),* $(,)*) => {
        #[macro_export]
        macro_rules! all_backends {
            ($submac:ident!$arg:tt) => { unpack_backends!($submac!$arg($($lcname, $ccname : $dty),*)) };
        }
    };
}

#[macro_export]
macro_rules! backend_names {
    (@inner, $($lcname:ident, $ccname:ident : $ignore:ty),*) => {
        &[$(stringify!($name)),*]
    };
    () => {
        all_backends!(backend_names!(@inner));
    };
}

digests! {
    ::attaca::digest::Sha3Digest,
}

backends! {
    leveldb, LevelDb : ::attaca_leveldb::LevelDbBackend,
}

mod cache;
mod db;
mod state;

pub mod branch;
pub mod candidate;
pub mod checkout;
pub mod config;
pub mod fetch;
pub mod fsck;
pub mod plumbing;
pub mod remote;
pub mod show;
pub mod status;
pub mod syntax;
pub mod log;

#[macro_use]
pub mod init;
#[macro_use]
pub mod clone;
#[macro_use]
pub mod open;

use std::{env, fmt, io::Cursor, path::PathBuf, sync::{Arc, RwLock}};

use attaca::{Open, digest::{Sha3Digest, prelude::*}, store::prelude::*};
use failure::Error;
use futures::prelude::*;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};

use cache::Cache;
use db::Key;
use state::State;

pub use branch::BranchArgs;
pub use candidate::{CommitArgs, StageArgs};
pub use checkout::CheckoutArgs;
pub use clone::{clone, CloneArgs};
pub use fetch::FetchArgs;
pub use fsck::FsckArgs;
pub use init::InitArgs;
pub use log::LogArgs;
pub use remote::RemoteArgs;
pub use show::ShowArgs;
pub use state::Head;
pub use status::StatusArgs;

pub struct Repository<B: Backend> {
    store: Store<B>,
    db: Arc<RwLock<Database<Key>>>,

    cache: Cache<B>,
    path: Arc<PathBuf>,
}

impl<B: Backend + fmt::Debug> fmt::Debug for Repository<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Repository")
            .field("store", &self.store)
            .field("cache", &self.cache)
            .field("path", &self.path)
            .finish()
    }
}

impl<B: Backend + Open> Repository<B> {
    pub fn open(path: PathBuf) -> Result<Self, Error> {
        let db = Database::open(&path.join(".attaca/workspace"), Options::new())?;

        // TODO: Load from URL, absolute, or relative path instead of this default. Add a database
        // key to facilitate this.
        let backend = B::open_path(&path.join(".attaca/store"))?;

        Ok(Self::new(path, db, backend))
    }

    pub fn search() -> Result<Option<Self>, Error> {
        let mut wd = env::current_dir()?;

        while !wd.join(".attaca").exists() {
            if !wd.pop() {
                return Ok(None);
            }
        }

        Ok(Some(Self::open(wd)?))
    }
}

impl<B: Backend> Repository<B> {
    pub fn new(path: PathBuf, db: Database<Key>, backend: B) -> Self {
        let store = Store::new(backend);
        let db = Arc::new(RwLock::new(db));
        let cache = Cache::from(db.clone());

        Self {
            store,
            db,

            cache,
            path: Arc::new(path),
        }
    }

    fn set_state(&self, state: &State<Handle<B>>) -> Result<(), Error> {
        let mut buf = Vec::new();
        state.encode(&mut buf).wait()?;
        self.db
            .read()
            .unwrap()
            .put(WriteOptions::new(), &Key::state(), &buf)?;

        Ok(())
    }

    fn get_state(&self) -> Result<State<Handle<B>>, Error> {
        match self.db
            .read()
            .unwrap()
            .get(ReadOptions::new(), &Key::state())?
        {
            Some(bytes) => State::decode(Cursor::new(bytes), self.store.clone()).wait(),
            None => {
                let state = State::default();
                self.set_state(&state)?;
                Ok(state)
            }
        }
    }
}
