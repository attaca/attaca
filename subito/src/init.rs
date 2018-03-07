use std::{env, fs, path::{Path, PathBuf}};

use attaca::{Init, Open, digest::prelude::*, store::prelude::*};
use attaca_leveldb::LevelDbBackend;
use failure::*;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};
use url::{self, Url};

use Repository;
use config::{Config, StoreConfig, StoreKind};
use db::Key;

/// Create a local repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "init")]
pub struct InitArgs {
    /// Path to a directory to initialize as a repository. This defaults to the current directory.
    #[structopt(name = "PATH", parse(from_os_str))]
    pub path: Option<PathBuf>,

    #[structopt(subcommand)]
    pub store: Option<InitStore>,
}

#[derive(Debug, Clone, StructOpt)]
pub enum InitStore {
    #[structopt(name = "leveldb")]
    LevelDb(InitLevelDb),
}

impl Default for InitStore {
    fn default() -> Self {
        InitStore::LevelDb(Default::default())
    }
}

#[derive(Debug, Clone, Default, StructOpt)]
pub struct InitLevelDb {
    /// Path or URL of the LevelDB repository to open/initialize.
    #[structopt(name = "LOCATION")]
    location: Option<String>,

    /// Fail unless the LevelDB repository already exists.
    #[structopt(name = "no-init", long = "no-init", raw(requires = r#""URL""#))]
    no_init: bool,
}

#[macro_export]
macro_rules! init {
    (@inner $args:expr, $repo:ident,  $generic:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
        {
            match $args.store.unwrap_or_default() {
                $($crate::init::InitStore::$ccname(spec_args) => {
                    $args.path
                        .map(Ok)
                        .unwrap_or_else(::std::env::current_dir)
                        .map_err($crate::reexports::failure::err_msg)
                        .and_then(|path| {
                            #[allow(unused_mut)]
                            let mut $repo =
                                $crate::Repository::init_with(
                                    path,
                                    |path| $crate::init::$lcname(path, spec_args)
                                )?;
                            Ok({
                                #[warn(unused_mut)]
                                $generic
                            })
                        })
                })*
            }
        }
    };
    ($args:expr, $repo:ident, $generic:expr) => {
        all_backends!(init!(@inner $args, $repo, $generic))
    };
}

pub fn leveldb<P: AsRef<Path>>(
    path: P,
    args: InitLevelDb,
) -> Result<(StoreConfig, LevelDbBackend), Error> {
    let InitLevelDb { location, no_init } = args;

    let url = match location {
        Some(location) => match Url::parse(&location) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                let full_path = Path::new(&location)
                    .canonicalize()
                    .with_context(|_| format_err!("Path {} does not exist", location))?;
                Url::from_file_path(full_path).unwrap()
            }
            Err(err) => bail!(err.context(format_err!(
                "Unable to parse \"{}\" as path or URL",
                location
            ))),
        },
        None => Url::from_file_path(path.as_ref().join(".attaca/store")).unwrap(),
    };

    let backend = if no_init {
        LevelDbBackend::open(url.as_str())?
    } else {
        LevelDbBackend::init(url.as_str())?
    };

    let store_config = StoreConfig {
        url,
        kind: StoreKind::LevelDb,
    };

    Ok((store_config, backend))
}

impl<B: Backend> Repository<B> {
    pub fn init_with<F: FnOnce(&Path) -> Result<(StoreConfig, B), Error>>(
        path: PathBuf,
        backend: F,
    ) -> Result<Self, Error> {
        fs::create_dir_all(&path.join(".attaca"))?;
        let db = Database::open(
            &path.join(".attaca/repository"),
            Options {
                create_if_missing: true,
                error_if_exists: true,
                ..Options::new()
            },
        )?;
        let (store_config, backend) = backend(&path)?;
        let config = Config {
            store: store_config,
            remotes: Default::default(),
        };
        let mut buf = Vec::new();
        config.encode(&mut buf)?;
        db.put(WriteOptions::new(), &Key::config(), &buf)?;

        Ok(Self::new(path, db, backend))
    }
}
