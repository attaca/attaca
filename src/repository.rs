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
use std::sync::Arc;

use itertools::Itertools;
use toml;

use {METADATA_PATH, BLOBS_PATH, CONFIG_PATH, REMOTE_CATALOGS_PATH, LOCAL_CATALOG_PATH, INDEX_PATH};
use catalog::{Registry, Catalog, CatalogTrie};
use errors::*;
use index::Index;


pub type ArcPath = Arc<PathBuf>;


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


impl Config {
    pub fn open(paths: &Paths) -> Result<Self> {
        let mut config_file = File::open(paths.config()).chain_err(|| {
            format!("malformed repository at {}", paths.metadata().display())
        })?;
        let mut config_string = String::new();
        config_file.read_to_string(&mut config_string)?;

        Ok(toml::from_str::<Config>(&config_string)?)
    }
}


#[derive(Debug)]
struct PathsInner {
    base: PathBuf,
    metadata: PathBuf,
    config: PathBuf,
    blobs: PathBuf,
    local_catalog: PathBuf,
    remote_catalogs: PathBuf,
    index: PathBuf,
}


/// Paths for accessing persistent repository data.
#[derive(Debug, Clone)]
pub struct Paths {
    inner: Arc<PathsInner>,
}


impl Paths {
    pub fn new<P: AsRef<Path>>(base_ref: P) -> Self {
        let base = base_ref.as_ref().to_owned();
        let metadata = base.join(&*METADATA_PATH);
        let config = base.join(&*CONFIG_PATH);
        let blobs = base.join(&*BLOBS_PATH);
        let local_catalog = base.join(&*LOCAL_CATALOG_PATH);
        let remote_catalogs = base.join(&*REMOTE_CATALOGS_PATH);
        let index = base.join(&*INDEX_PATH);

        Self {
            inner: Arc::new(PathsInner {
                base,
                metadata,
                blobs,
                config,
                local_catalog,
                remote_catalogs,
                index,
            }),
        }
    }

    pub fn base(&self) -> &Path {
        &self.inner.base
    }

    pub fn metadata(&self) -> &Path {
        &self.inner.metadata
    }

    pub fn config(&self) -> &Path {
        &self.inner.config
    }

    pub fn blobs(&self) -> &Path {
        &self.inner.blobs
    }

    pub fn local_catalog(&self) -> &Path {
        &self.inner.local_catalog
    }

    pub fn remote_catalogs(&self) -> &Path {
        &self.inner.remote_catalogs
    }
    
    pub fn index(&self) -> &Path {
        &self.inner.index
    }
}


/// The type of a valid repository.
#[derive(Debug)]
pub struct Repository {
    /// Loaded configuration from `.attaca/config.toml`.
    pub config: Config,

    /// The local index.
    pub index: Index,

    /// Paths to persistent repository data.
    pub paths: Paths,

    /// Catalogs for local and remote object stores.
    pub catalogs: Registry,
}


impl Repository {
    /// Initialize a repository.
    pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
        let paths = Paths::new(path);

        if paths.metadata().is_dir() {
            bail!(
                "a repository already exists at {}!",
                paths.metadata().display()
            );
        }

        fs::create_dir_all(paths.metadata()).chain_err(|| {
            format!("error creating {}", paths.metadata().display())
        })?;

        fs::create_dir_all(paths.blobs()).chain_err(|| {
            format!("error creating {}", paths.blobs().display())
        })?;

        fs::create_dir_all(paths.remote_catalogs()).chain_err(|| {
            format!("error creating {}", paths.remote_catalogs().display())
        })?;

        File::create(paths.config())
            .and_then(|mut cfg_file| {
                cfg_file.write_all(&toml::to_vec(&Config::default()).unwrap())
            })
            .chain_err(|| format!("error creating {}", paths.config().display()))?;

        Ok(())
    }


    /// Load repository data.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let paths = Paths::new(path);

        if !paths.metadata().is_dir() {
            bail!(
                "no repository found at {}!",
                paths.metadata().display(),
            );
        }

        let config = Config::open(&paths)?;
        let catalogs = Registry::new(&config, &paths);
        let index = Index::open(&paths)?;

        Ok(Repository {
            config,
            catalogs,
            paths,
            index,
        })
    }


    /// Move backwards towards the root of the filesystem searching for a valid repository.
    pub fn find<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let mut attaca_path = path.as_ref().to_owned();

        while !attaca_path.join(&*METADATA_PATH).is_dir() {
            if !attaca_path.pop() {
                bail!(ErrorKind::RepositoryNotFound(path.as_ref().to_owned()));
            }
        }

        Self::load(attaca_path)
    }


    pub fn make_local_catalog(&self) -> Result<Catalog> {
        let mut objects = CatalogTrie::new();

        for byte0_res in self.paths.blobs().read_dir()? {
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

        Catalog::new(objects, self.paths.local_catalog().to_owned())
    }


    /// Update the `config.toml` file.
    fn write_config(&mut self) -> Result<()> {
        let config = toml::to_vec(&self.config)?;
        let mut file = File::create(&self.paths.config())?;
        file.write_all(&config)?;
        Ok(())
    }
}


impl Drop for Repository {
    fn drop(&mut self) {
        self.write_config().unwrap();
    }
}
