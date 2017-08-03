//! Interface with distributed storage - e.g. Ceph/RADOS and etcd.
//!
//! The `store` module has several key pieces of functionality:
//! - Sending/receiving marshaled chunks/encoded files/subtrees/commits to and from the store.
//! - Caching already sent/received blobs.
//! - Coordinating refs with the consensus.
//!
//! As marshaling coalesces into a usable module, it will become more obvious exactly what
//! functionality is required.

use std::ffi::CString;

use rad::{RadosConnectionBuilder, RadosConnection, RadosContext};

use errors::Result;
use marshal::Chunk;
use repository::CephConfig;


pub struct ObjectStore {
    connection: RadosConnection,
}


impl ObjectStore {
    pub fn connect(conf: &CephConfig) -> Result<ObjectStore> {
        let mut ceph_conf = conf.conf_dir.clone();
        ceph_conf.push("ceph.conf");

        let mut ceph_keyring = conf.conf_dir.clone();
        ceph_keyring.push(format!("ceph.{}.keyring", conf.user.to_str().unwrap()));

        let connection = RadosConnectionBuilder::with_user(&*conf.user)?
            .read_conf_file(CString::new(ceph_conf.to_str().unwrap()).unwrap())?
            .conf_set(
                c!("keyring"),
                CString::new(ceph_keyring.to_str().unwrap()).unwrap(),
            )?
            .connect()?;

        Ok(ObjectStore { connection })
    }
}


pub struct ObjectStoreContext {
    context: RadosContext,
}
