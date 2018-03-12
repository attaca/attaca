use std::{env, path::PathBuf};

use attaca_leveldb::LevelDbBackend;
use attaca_rados::RadosBackend;
use failure::*;

use Open;
use config::Config;

#[macro_export]
macro_rules! open {
    (@inner $path:expr, $repo:ident, $generic:expr, $($lcname:ident, $ccname:ident : $dty:ty),*) => {
        {
            fn go(path: &::std::path::Path) ->
                    Result<($crate::reexports::Database<$crate::reexports::Key>,
                            $crate::config::Config), $crate::reexports::failure::Error> {
                use $crate::config::Config;
                use $crate::reexports::*;

                let db = Database::open(&path.join(".attaca/repository"), Options::new())?;
                let raw_config = db.get(ReadOptions::new(), &Key::config())?
                    .ok_or_else(|| format_err!("Malformed repository: missing configuration entry"))?;
                let config = Config::decode(&mut &raw_config[..])?;

                Ok((db, config))
            }

            let path = ::std::path::PathBuf::from($path);
            go(&path).and_then(|(db, config)| {
                #[allow(unused_mut)]

                match config.store.kind {
                    $($crate::config::StoreKind::$ccname =>
                        $crate::open::$lcname(config)
                            .map(|backend| $crate::Repository::new(path, db, backend))
                            .map(|mut $repo: $crate::Repository<$dty>| {
                                #[warn(unused_mut)]

                                $generic
                            }),)*
                }
            })
        }
    };
    ($path:expr, $repo:ident, $generic:expr) => {
        all_backends!(open!(@inner $path, $repo, $generic))
    };
}

pub fn leveldb(config: Config) -> Result<LevelDbBackend, Error> {
    Ok(LevelDbBackend::open(config.store.url.as_str())?)
}

pub fn rados(config: Config) -> Result<RadosBackend, Error> {
    Ok(RadosBackend::open(config.store.url.as_str())?)
}

#[macro_export]
macro_rules! search {
    ($repo:ident, $generic:expr) => {
        $crate::open::search()
            .and_then(|opt_path|
                opt_path.ok_or_else(|| $crate::reexports::failure::err_msg("Repository not found!")))
            .and_then(|path| open!(path, $repo, $generic))
    };
}

// TODO more robust searching
pub fn search() -> Result<Option<PathBuf>, Error> {
    let mut wd = env::current_dir()?;

    while !wd.join(".attaca").exists() {
        if !wd.pop() {
            return Ok(None);
        }
    }

    Ok(Some(wd))
}
