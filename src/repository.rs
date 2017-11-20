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
/// +-- HEAD
/// +-_ blobs
///    +-- ... locally stored blobs named by hash
/// ```


use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bincode;
use futures_cpupool::CpuPool;
use itertools::Itertools;
use toml;

use {METADATA_PATH, BLOBS_PATH, CONFIG_PATH, REMOTE_CATALOGS_PATH, LOCAL_CATALOG_PATH, INDEX_PATH,
     REFS_PATH};
use catalog::{Registry, Catalog, CatalogTrie};
use context::Context;
use errors::*;
use index::Index;
use marshal::ObjectHash;
use store::{Local, Remote, Ceph};
use trace::Trace;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshCfg {
    /// Socket address to connect to with SSH.
    pub address: SocketAddr,

    /// SSH username.
    pub username: String,
}


/// The persistent configuration data for a single pool of a RADOS cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CephCfg {
    /// Path to a ceph.conf file.
    // TODO: Disallow this!
    pub conf_file: Option<PathBuf>,

    /// The working RADOS object pool.
    pub pool: String,

    /// The working RADOS user.
    pub user: String,

    /// Ceph configuration options, stored directly.
    #[serde(serialize_with = "toml::ser::tables_last")]
    pub conf_options: HashMap<String, String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectStoreCfg {
    Ceph(CephCfg),
    Ssh(SshCfg),
}


/// The persistent configuration data for an etcd cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EtcdCfg {
    /// A list of cluster members to attempt connections to.
    pub cluster: Vec<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefStoreCfg {
    Etcd(EtcdCfg),
    Ssh(SshCfg),
}


/// The persistent configuration data for a single remote.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteCfg {
    /// The remote object store.
    ///
    /// TODO: Support object stores other than Ceph/RADOS.
    pub object_store: ObjectStoreCfg,

    /// The remote ref store.
    ///
    /// TODO: Support ref stores other than etcd.
    pub ref_store: RefStoreCfg,
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
        let mut config_file = File::open(&paths.config)?;
        let mut config_string = String::new();
        config_file.read_to_string(&mut config_string)?;

        Ok(toml::from_str::<Config>(&config_string)?)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Head {
    Detached(ObjectHash),
    LocalRef(String),
    RemoteRef(String, String),
    Root,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refs {
    pub head: Head,
    pub branches: HashMap<String, ObjectHash>,
    pub remotes: HashMap<String, HashMap<String, ObjectHash>>,
}


impl Refs {
    pub fn open(paths: &Paths) -> Result<Self> {
        if paths.refs.exists() {
            let mut refs_bytes = Vec::new();
            File::open(&paths.refs)
                .map_err(Error::from)
                .and_then(|mut refs_file| {
                    refs_file.read_to_end(&mut refs_bytes).map_err(Error::from)
                })
                .and_then(|_| {
                    bincode::deserialize::<Refs>(&refs_bytes).map_err(Error::from)
                })
                .chain_err(|| ErrorKind::OpenRefs(paths.refs.to_owned()))
        } else {
            Ok(Self {
                head: Head::Root,
                branches: HashMap::new(),
                remotes: HashMap::new(),
            })
        }
    }

    pub fn from_bytes<R: Read>(reader: &mut R) -> Result<Self> {
        bincode::deserialize_from(reader, bincode::Infinite).map_err(Error::from)
    }

    pub fn close(self, paths: &Paths) -> Result<()> {
        let mut refs_bytes = Vec::new();

        bincode::serialize_into(&mut refs_bytes, &self, bincode::Infinite)
            .map_err(Error::from)
            .and_then(|_| File::create(&paths.refs).map_err(Error::from))
            .and_then(|mut refs_file| {
                refs_file.write_all(&refs_bytes).map_err(Error::from)
            })
            .chain_err(|| ErrorKind::CloseRefs(paths.refs.to_owned()))
    }

    pub fn head_as_hash(&self) -> Option<ObjectHash> {
        match self.head {
            Head::Detached(hash) => Some(hash),
            Head::LocalRef(ref branch) => self.branches.get(branch).cloned(),
            Head::RemoteRef(ref remote, ref branch) => {
                self.remotes
                    .get(remote)
                    .and_then(|remote| remote.get(branch))
                    .cloned()
            }
            Head::Root => None,
        }
    }
}


#[derive(Debug)]
pub struct Paths {
    pub base: PathBuf,
    pub metadata: PathBuf,
    pub config: PathBuf,
    pub blobs: PathBuf,
    pub local_catalog: PathBuf,
    pub remote_catalogs: PathBuf,
    pub index: PathBuf,
    pub refs: PathBuf,
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
        let refs = base.join(&*REFS_PATH);

        Self {
            base,
            metadata,
            blobs,
            config,
            local_catalog,
            remote_catalogs,
            index,
            refs,
        }
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
    pub paths: Arc<Paths>,

    /// Catalogs for local and remote object stores.
    pub catalogs: Registry,

    /// Refs for local branches, remotes, and also the HEAD.
    pub refs: Refs,
}


impl Repository {
    /// Initialize a repository.
    pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
        let paths = Paths::new(path);

        if paths.metadata.is_dir() {
            bail!(
                "a repository already exists at {}!",
                paths.metadata.display()
            );
        }

        fs::create_dir_all(&paths.metadata).chain_err(|| {
            format!("error creating {}", paths.metadata.display())
        })?;

        fs::create_dir_all(&paths.blobs).chain_err(|| {
            format!("error creating {}", paths.blobs.display())
        })?;

        fs::create_dir_all(&paths.remote_catalogs).chain_err(|| {
            format!("error creating {}", paths.remote_catalogs.display())
        })?;

        File::create(&paths.config)
            .and_then(|mut cfg_file| {
                cfg_file.write_all(&toml::to_vec(&Config::default()).unwrap())
            })
            .chain_err(|| format!("error creating {}", paths.config.display()))?;

        Ok(())
    }

    /// Load repository data.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let paths = Arc::new(Paths::new(path));

        if !paths.metadata.is_dir() {
            bail!(
                "no repository found at {}!",
                paths.metadata.display(),
            );
        }

        let config = Config::open(&paths)?;
        let catalogs = Registry::new(&config, &paths);
        let index = Index::open(&paths)?;
        let refs = Refs::open(&paths)?;

        Ok(Repository {
            config,
            catalogs,
            paths,
            index,
            refs,
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

        for byte0_res in self.paths.blobs.read_dir()? {
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

        Catalog::new(objects, self.paths.local_catalog.to_owned())
    }

    /// Update the `config.toml` file.
    fn write_config(&mut self) -> Result<()> {
        let config = toml::to_vec(&self.config)?;
        let mut file = File::create(&self.paths.config)?;
        file.write_all(&config)?;
        Ok(())
    }

    /// Procure a context for working with the local object store.
    pub fn local_with_pools<T: Trace>(
        &mut self,
        marshal_pool: &CpuPool,
        io_pool: &CpuPool,
        trace: T,
    ) -> Result<Context<T, Local>> {
        let catalog = self.catalogs.get(None)?;
        let store = Local::new(&self.paths, &catalog, io_pool);

        Ok(Context::new(self, trace, store, marshal_pool, io_pool))
    }

    pub fn local<T: Trace>(&mut self, trace: T) -> Result<Context<T, Local>> {
        let marshal_pool = CpuPool::new(1);
        let io_pool = CpuPool::new(1);

        self.local_with_pools(&marshal_pool, &io_pool, trace)
    }

    /// Procure a context for working with a remote object store.
    pub fn remote_with_pools<T: Trace, U: AsRef<str>>(
        &mut self,
        remote_name: U,
        marshal_pool: &CpuPool,
        io_pool: &CpuPool,
        trace: T,
    ) -> Result<Context<T, Remote>> {
        let remote = {
            let local_catalog = self.catalogs.get(None)?;
            let remote_catalog = self.catalogs.get(Some(remote_name.as_ref().to_owned()))?;
            let remote_config = self.config.remotes.get(remote_name.as_ref()).ok_or_else(
                || {
                    Error::from_kind(ErrorKind::RemoteNotFound(remote_name.as_ref().to_owned()))
                },
            )?;
            let local = Local::new(&self.paths, &local_catalog, io_pool);

            match remote_config.object_store {
                ObjectStoreCfg::Ceph(ref ceph_cfg) => {
                    Remote::Ceph(Ceph::connect(local, &remote_catalog, ceph_cfg, io_pool)?)
                }
                ObjectStoreCfg::Ssh(ref _ssh_cfg) => unimplemented!(),
            }
        };

        Ok(Context::new(self, trace, remote, marshal_pool, io_pool))
    }

    pub fn remote<T: Trace, U: AsRef<str>>(
        &mut self,
        remote_name: U,
        trace: T,
    ) -> Result<Context<T, Remote>> {
        let marshal_pool = CpuPool::new(1);
        let io_pool = CpuPool::new(1);

        self.remote_with_pools(remote_name, &marshal_pool, &io_pool, trace)
    }

    /// Clean up and drop the `Repository`, writing persistent data to the filesystem.
    pub fn cleanup(mut self) -> Result<()> {
        self.write_config()?;
        self.refs.close(&self.paths)?;
        self.index.cleanup()?;

        Ok(())
    }
}
