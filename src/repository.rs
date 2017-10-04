/// # `repository` - configuration data and other persistent data w.r.t. a single repository.
///
/// An Attaca repository's metadata is stored in the `.attaca` folder of a repository. The internal
/// structure of the metadata folder looks like this:
///
/// ```ignore
/// _ .attaca
/// +-- config.toml
/// +-_ remote-catalogs
///    +-- <remote-name>.catalog
/// +-- local.catalog
/// +-_ blobs
///    +-- ... locally stored blobs named by hash
/// ```


use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use itertools::Itertools;
use toml;

use {METADATA_PATH, BLOBS_PATH, CONFIG_PATH, REMOTE_CATALOGS_PATH, LOCAL_CATALOG_PATH};
use catalog::{Catalog, CatalogTrie};
use errors::*;
use index::Index;


/// The persistent configuration data for a single pool of a RADOS cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CephCfg {
    /// Path to a ceph.conf file.
    pub conf_file: Option<PathBuf>,

    /// The working RADOS object pool.
    pub pool: String,

    /// The working RADOS user.
    pub user: String,

    /// Ceph configuration options, stored directly.
    #[serde(serialize_with = "toml::ser::tables_last")]
    pub conf_options: HashMap<String, String>,
}


/// The persistent configuration data for an etcd cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EtcdCfg {
    /// A list of cluster members to attempt connections to.
    pub cluster: Vec<String>,
}


/// The persistent configuration data for a single remote.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteCfg {
    /// The remote object store.
    ///
    /// TODO: Support object stores other than Ceph/RADOS.
    pub object_store: CephCfg,

    /// The remote ref store.
    ///
    /// TODO: Support ref stores other than etcd.
    pub ref_store: EtcdCfg,
}


/// The persistent data, stored in a repository's config.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Named remotes for this repository.
    #[serde(serialize_with = "toml::ser::tables_last")]
    pub remotes: HashMap<String, RemoteCfg>,
}


impl Default for Config {
    fn default() -> Config {
        Config { remotes: HashMap::new() }
    }
}


/// The type of a valid repository.
#[derive(Debug)]
pub struct Repository {
    /// Loaded configuration from `.attaca/config.toml`.
    pub config: Config,

    /// The local index.
    pub index: Index,

    /// Catalogs for local and remote object stores.
    catalogs: HashMap<Option<String>, Option<Catalog>>,

    /// The absolute path to the root of the repository.
    path: PathBuf,

    /// The absolute path to the repository's config file.
    config_path: PathBuf,

    /// The absolute path to the blob store of the repository.
    blob_path: PathBuf,

    /// The absolute path to the local catalog.
    local_catalog_path: PathBuf,

    /// The absolute path to the remote catalog directory of the repository.
    remote_catalogs_path: PathBuf,
}


impl Repository {
    /// Initialize a repository.
    pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
        let path_ref = path.as_ref();

        let metadata_path = path_ref.join(*METADATA_PATH);
        if metadata_path.is_dir() {
            bail!(
                "a repository already exists at {}!",
                metadata_path.to_string_lossy()
            );
        }

        fs::create_dir_all(&metadata_path).chain_err(|| {
            format!("error creating {}", metadata_path.display())
        })?;

        let blobs_path = path_ref.join(&*BLOBS_PATH);
        fs::create_dir_all(&blobs_path).chain_err(|| {
            format!("error creating {}", blobs_path.display())
        })?;

        let remote_catalogs_path = path_ref.join(&*REMOTE_CATALOGS_PATH);
        fs::create_dir_all(&remote_catalogs_path).chain_err(|| {
            format!("error creating {}", remote_catalogs_path.display())
        })?;

        let config_path = path_ref.join(&*CONFIG_PATH);
        File::create(&config_path)
            .and_then(|mut cfg_file| {
                cfg_file.write_all(&toml::to_vec(&Config::default()).unwrap())
            })
            .chain_err(|| format!("error creating {}", config_path.display()))?;

        Ok(())
    }


    /// Load repository data.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let path_ref = path.as_ref();

        let metadata_path = path_ref.join(&*METADATA_PATH);

        if !metadata_path.is_dir() {
            bail!(
                "no repository found at {}!",
                metadata_path.to_string_lossy()
            );
        }

        let config_path = path_ref.join(&*CONFIG_PATH);

        let config = {
            let mut config_file = File::open(&config_path).chain_err(|| {
                format!(
                    "malformed repository at {}",
                    metadata_path.to_string_lossy()
                )
            })?;
            let mut config_string = String::new();
            config_file.read_to_string(&mut config_string)?;

            toml::from_str::<Config>(&config_string)?
        };

        let catalogs = {
            let mut catalog_map: HashMap<_, _> = config
                .remotes
                .keys()
                .map(|key| (Some(key.to_owned()), None))
                .collect();
            catalog_map.insert(None, None);

            catalog_map
        };

        let index = Index::open(path_ref)?;

        Ok(Repository {
            config,
            catalogs,
            index,
            path: path_ref.to_owned(),
            config_path,
            blob_path: path_ref.join(&*BLOBS_PATH),
            local_catalog_path: path_ref.join(&*LOCAL_CATALOG_PATH),
            remote_catalogs_path: path_ref.join(&*REMOTE_CATALOGS_PATH),
        })
    }


    /// Move backwards towards the root of the filesystem searching for a valid repository.
    pub fn find<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let mut attaca_path = path.as_ref().to_owned();

        while !attaca_path.join(".attaca").is_dir() {
            if !attaca_path.pop() {
                bail!(ErrorKind::RepositoryNotFound(path.as_ref().to_owned()));
            }
        }

        Self::load(attaca_path)
    }


    pub fn make_local_catalog(&self) -> Result<Catalog> {
        let mut objects = CatalogTrie::new();

        for byte0_res in self.blob_path.read_dir()? {
            let byte0 = byte0_res?;

            for byte1_res in byte0.path().read_dir()? {
                let byte1 = byte1_res?;

                for object_res in byte1.path().read_dir()? {
                    let object = object_res?;

                    // Turn the path back into a hash.
                    let path = object.path();
                    let hash_ending = path.file_name().unwrap().to_string_lossy();
                    let ending_parent = path.parent().unwrap();
                    let hash_second = ending_parent.file_name().unwrap().to_string_lossy();
                    let hash_first = ending_parent
                        .parent()
                        .unwrap()
                        .file_name()
                        .unwrap()
                        .to_string_lossy();

                    let hash_string = [hash_first, hash_second, hash_ending].into_iter().join("");
                    let hash = hash_string.parse()?;

                    objects.insert(hash);
                }
            }
        }

        Catalog::new(objects, self.local_catalog_path.clone())
    }


    pub fn get_catalog(&mut self, name_opt: Option<String>) -> Result<Catalog> {
        match self.catalogs.get(&name_opt) {
            Some(&Some(ref catalog)) => return Ok(catalog.clone()),
            Some(&None) => {}
            None => bail!(ErrorKind::CatalogNotFound(name_opt)),
        }

        let catalog_path = match name_opt {
            Some(ref name) => {
                Cow::Owned(self.remote_catalogs_path.join(format!("{}.catalog", name)))
            }
            None => Cow::Borrowed(&self.local_catalog_path),
        };
        let catalog = Catalog::load(catalog_path.clone().into_owned()).chain_err(
            || {
                ErrorKind::CatalogLoad(catalog_path.into_owned())
            },
        )?;
        self.catalogs.insert(name_opt, Some(catalog.clone()));

        Ok(catalog)
    }


    pub fn clear_catalogs(&mut self) -> Result<()> {
        let catalog_names = self.catalogs.keys().cloned().collect::<Vec<_>>();

        for name in catalog_names {
            self.get_catalog(name)?.clear()?;
        }

        Ok(())
    }


    pub fn get_blob_path(&self) -> &Path {
        &self.blob_path
    }


    /// Update the `config.toml` file.
    fn write_config(&mut self) -> Result<()> {
        let config = toml::to_vec(&self.config)?;
        let mut file = File::create(&self.config_path)?;
        file.write_all(&config)?;
        Ok(())
    }
}


impl Drop for Repository {
    fn drop(&mut self) {
        self.write_config().unwrap();
    }
}
