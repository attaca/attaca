/// `repository` - configuration data and other persistent data w.r.t. a single repository.
///
/// An Attaca repository's metadata is stored in the `.attaca` folder of a repository. The internal
/// structure of the metadata folder looks like this:
///
/// ```ignore
/// _ .attaca
/// +-- config.toml
/// +-_ ceph
/// | +-- ceph.conf
/// | +-- ceph.client.admin.keyring
/// | +-- ceph.*.keyring
/// +-_ blobs
///    +-- ... locally stored blobs named by hash
/// ```


use std::collections::HashMap;
use std::ffi::CString;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use toml;

use errors::{Result, ResultExt};


/// The persistent configuration data for a single pool of a RADOS cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadosCfg {
    /// The directory in which `ceph.conf` is stored.
    pub conf_dir: PathBuf,

    /// The working RADOS object pool.
    pub pool: CString,

    /// The working RADOS user.
    pub user: CString,
}


/// The persistent configuration data for an etcd cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub object_store: RadosCfg,

    /// The remote ref store.
    ///
    /// TODO: Support ref stores other than etcd.
    pub ref_store: EtcdCfg,
}


/// The persistent data, stored in a repository's config.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Named remotes for this repository.
    pub remotes: HashMap<String, RemoteCfg>,
}


impl Default for Config {
    fn default() -> Config {
        Config { remotes: HashMap::new() }
    }
}


/// The type of a valid repository.
#[derive(Debug, Clone)]
pub struct Repository {
    /// Loaded configuration from `.attaca/config.toml`.
    pub config: Config,

    /// The absolute path to the root of the repository.
    pub path: PathBuf,

    /// The absolute path to the blob store of the repository.
    // TODO: Better manage this. It should probably be configurable.
    pub blob_path: PathBuf,
}


impl Repository {
    /// Initialize a repository.
    pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
        let mut attaca_path = path.as_ref().to_owned();

        attaca_path.push(".attaca/");
        if attaca_path.is_dir() {
            bail!(
                "a repository already exists at {}!",
                attaca_path.to_string_lossy()
            );
        }

        fs::create_dir_all(&attaca_path).chain_err(
            || "error creating .attaca",
        )?;

        fs::create_dir_all(&attaca_path.join("blobs")).chain_err(
            || "error creating .attaca/blobs",
        )?;

        File::create(&attaca_path.join("config.toml"))
            .and_then(|mut cfg_file| {
                cfg_file.write_all(&toml::to_vec(&Config::default()).unwrap())
            })
            .chain_err(|| "error writing default .attaca/config.toml")?;

        Ok(())
    }


    /// Load repository data.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let attaca_path = path.as_ref().join(".attaca");

        if !attaca_path.is_dir() {
            bail!("no repository found at {}!", attaca_path.to_string_lossy());
        }

        let config = {
            let cfg_path = path.as_ref().join(".attaca/config.toml");

            let mut config_file = File::open(&cfg_path).chain_err(|| {
                format!("malformed repository at {}", attaca_path.to_string_lossy())
            })?;
            let mut config_string = String::new();
            config_file.read_to_string(&mut config_string)?;

            toml::from_str(&config_string)?
        };

        Ok(Repository {
            config,
            path: path.as_ref().to_owned(),
            blob_path: path.as_ref().join(".attaca/blobs"),
        })
    }


    /// Move backwards towards the root of the filesystem searching for a valid repository.
    pub fn find<P: AsRef<Path>>(path: P) -> Result<Repository> {
        let mut attaca_path = path.as_ref().to_owned();

        while !attaca_path.join(".attaca").is_dir() {
            println!("Searching: {:?}", attaca_path);

            if !attaca_path.pop() {
                bail!("the directory {} is not a subdirectory of a repository!", path.as_ref().to_string_lossy());
            }
        }

        Self::load(attaca_path)
    }
}
