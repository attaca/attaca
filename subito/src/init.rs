use std::{env, fs, path::{Path, PathBuf}};

use attaca::{HandleDigest, Init, Open, Store, digest::{Digest, Sha3Digest}};
use attaca_leveldb::LevelStore;
use failure::*;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions}};
use url::{self, Url};

use {PerDigest, Repository, Universe};
use config::{Config, StoreConfig, StoreKind};
use db::Key;

/// Create a local repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "init")]
pub struct InitArgs {
    /// Path to a directory to initialize as a repository. This defaults to the current directory.
    #[structopt(name = "PATH", parse(from_os_str))]
    path: Option<PathBuf>,

    #[structopt(subcommand)]
    store: InitStore,
}

#[derive(Debug, Clone, StructOpt)]
pub enum InitStore {
    #[structopt(name = "leveldb")]
    LevelDb {
        /// Path or URL of the LevelDB repository to open/initialize.
        #[structopt(name = "LOCATION")]
        location: Option<String>,

        /// Fail unless the LevelDB repository already exists.
        #[structopt(name = "no-init", long = "no-init", raw(requires = r#""URL""#))]
        no_init: bool,
    },
}

pub fn init(args: InitArgs) -> Result<Universe, Error> {
    match args.store {
        InitStore::LevelDb { location, no_init } => {
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
                None => Url::from_file_path(env::current_dir()?.join(".attaca/store")).unwrap(),
            };

            let path = args.path.map_or_else(|| env::current_dir(), Ok)?;
            fs::create_dir(path.join(".attaca"))?;

            let store = if no_init {
                LevelStore::open(url.as_str())?
            } else {
                LevelStore::init(url.as_str())?
            };

            let mut repository = Repository::init_with(path, store)?;
            repository.set_config(&Config {
                store: StoreConfig {
                    url,
                    kind: StoreKind::LevelDb,
                    digest: (String::from(Sha3Digest::NAME), Sha3Digest::SIZE),
                },
            })?;
            let universe = Universe::Sha3(PerDigest::LevelDb(repository));

            Ok(universe)
        }
    }
}

pub fn open<P: Into<PathBuf>>(path: P) -> Result<Universe, Error> {
    let path = path.into();

    let db = Database::open(&path.join(".attaca/repository"), Options::new())?;
    let raw_config = db.get(ReadOptions::new(), &Key::config())?
        .ok_or_else(|| format_err!("Malformed repository: missing configuration entry"))?;
    let config = Config::decode(&mut &raw_config[..])?;
    let digest = (config.store.digest.0.as_str(), config.store.digest.1);

    let universe = match digest {
        (Sha3Digest::NAME, Sha3Digest::SIZE) => {
            let sha3_verse = open_with::<Sha3Digest>(path, db, &config)?;
            Universe::Sha3(sha3_verse)
        }
        _ => bail!("Unknown digest function in configuration entry"),
    };

    Ok(universe)
}

fn open_with<D: Digest>(
    path: PathBuf,
    db: Database<Key>,
    config: &Config,
) -> Result<PerDigest<D>, Error> {
    let pd = match config.store.kind {
        StoreKind::LevelDb => {
            let store = LevelStore::open(config.store.url.as_str())?;
            let repository = Repository::new(path, db, store);
            PerDigest::LevelDb(repository)
        }
        StoreKind::Ceph => unimplemented!(),
    };

    Ok(pd)
}

pub fn search() -> Result<Option<Universe>, Error> {
    let mut wd = env::current_dir()?;

    while !wd.join(".attaca").exists() {
        if !wd.pop() {
            return Ok(None);
        }
    }

    Ok(Some(open(wd)?))
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    fn init_with(path: PathBuf, store: S) -> Result<Self, Error> {
        fs::create_dir_all(&path.join(".attaca"))?;
        let db = Database::open(
            &path.join(".attaca/repository"),
            Options {
                create_if_missing: true,
                error_if_exists: true,
                ..Options::new()
            },
        )?;

        Ok(Self::new(path, db, store))
    }
}
