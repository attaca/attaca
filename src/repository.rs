/// `repository` - configuration data and other persistent data w.r.t. a single repository.

use std::ffi::CString;
use std::path::PathBuf;


#[derive(Clone, Serialize, Deserialize)]
pub struct CephConfig {
    /// The directory in which `ceph.conf` is stored.
    pub conf_dir: PathBuf,

    /// The working RADOS object pool.
    pub pool: CString,

    /// The working RADOS user.
    pub user: CString,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub ceph: CephConfig,
}


#[derive(Clone)]
pub struct Repository {
    config: Config,
}
