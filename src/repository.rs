/// `repository` - configuration data and other persistent data w.r.t. a single repository.
///
/// An Attaca repository's metadata is stored in the `.attaca` folder of a repository. The internal
/// structure of the metadata folder looks like this:
///
/// ```
/// .attaca
/// +-- config.toml
/// +--_ceph
/// |  +-- ceph.conf
/// |  +-- ceph.client.admin.keyring
/// |  +-- ceph.*.keyring
/// +--_blobs
///    +-- ... locally stored blobs named by hash
/// ```


use std::collections::HashMap;
use std::ffi::CString;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use toml;

use errors::{Result, ResultExt};


#[derive(Clone, Serialize, Deserialize)]
pub struct RadosCfg {
    /// The directory in which `ceph.conf` is stored.
    pub conf_dir: PathBuf,

    /// The working RADOS object pool.
    pub pool: CString,

    /// The working RADOS user.
    pub user: CString,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct EtcdCfg {
    /// A list of cluster members to attempt connections to.
    pub cluster: Vec<String>,
}


#[derive(Clone, Serialize, Deserialize)]
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


#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    /// Named remotes for this repository.
    pub remotes: HashMap<String, RemoteCfg>,
}


impl Default for Config {
    fn default() -> Config {
        Config { remotes: HashMap::new() }
    }
}


#[derive(Clone)]
pub struct Attaca {
    /// Loaded configuration from `.attaca/config.toml`.
    config: Config,

    /// The absolute path to the root of the repository.
    path: PathBuf,
}


impl Attaca {
    /// Initialize a repository.
    pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
        let mut attaca_path = path.as_ref().to_owned();

        attaca_path.push(".attaca");
        if attaca_path.is_dir() {
            bail!(
                "a repository already exists at {}!",
                attaca_path.to_string_lossy()
            );
        }

        fs::create_dir_all(&attaca_path).chain_err(
            || "error creating .attaca",
        )?;

        attaca_path.set_file_name("blobs");
        fs::create_dir_all(&attaca_path).chain_err(
            || "error creating .attaca/blobs",
        )?;

        attaca_path.set_file_name("config.toml");
        File::create(&attaca_path)
            .and_then(|mut cfg_file| {
                cfg_file.write_all(&toml::to_vec(&Config::default()).unwrap())
            })
            .chain_err(|| "error writing default .attaca/config.toml")?;

        Ok(())
    }


    /// Load repository data.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Attaca> {
        let mut attaca_path = path.as_ref().to_owned();
        attaca_path.push(".attaca");

        if !attaca_path.is_dir() {
            bail!("no repository found at {}!", attaca_path.to_string_lossy());
        }

        let config = {
            let mut cfg_path = path.as_ref().to_owned();
            cfg_path.push(".attaca/config.toml");

            let mut config_file = File::open(&cfg_path).chain_err(|| {
                format!("malformed repository at {}", attaca_path.to_string_lossy())
            })?;
            let mut config_string = String::new();
            config_file.read_to_string(&mut config_string)?;

            toml::from_str(&config_string)?
        };

        Ok(Attaca {
            config,
            path: path.as_ref().to_owned(),
        })
    }
}
