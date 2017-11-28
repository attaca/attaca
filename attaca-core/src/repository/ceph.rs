//! # `remote` - operations on remote repositories.
//!
//! `Ceph` contains a `RadosConnection` object, along with a reference to the parent context.
//!
//! At current the only supported remote is a Ceph/RADOS cluster.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use failure::{Error, SyncFailure};
use futures::prelude::*;
use futures_cpupool::CpuPool;
use memmap::MmapMut;
use owning_ref::OwningRefMut;
use rad::{Connection, ConnectionBuilder};
use toml;

use arc_slice::{self, ArcSlice};
use hasher::Hashed;
use lockmap::LockMap;
use object::Object;
use object_hash::ObjectHash;
use repository::ObjectStore;


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


/// The type of a remote repository.
// TODO: Abstract into a trait.
// TODO: Locally store what objects we know the remote to contain so that we can avoid writing them
//       when the remote already contains them.
// TODO: Make the act of writing an object asynchronous - return a future instead of a `Result.
#[derive(Clone)]
pub struct Ceph {
    lockmap: LockMap,
    inner: Arc<CephInner>,
}


struct CephInner {
    conn: Mutex<Connection>,
    pool: String,
}


impl Ceph {
    /// Connect to a remote repository, given appropriate configuration data.
    pub fn open(remote_config: CephCfg) -> Result<Self, Error> {
        let conn = {
            let mut builder =
                ConnectionBuilder::with_user(&remote_config.user).map_err(SyncFailure::new)?;
            if let Some(conf_path) = remote_config.conf_file {
                builder = builder
                    .read_conf_file(&conf_path)
                    .map_err(SyncFailure::new)?;
            }

            builder = remote_config
                .conf_options
                .iter()
                .fold(Ok(builder), |acc, (key, value)| {
                    acc.and_then(|conn| conn.conf_set(key, value))
                })
                .map_err(SyncFailure::new)?;

            Mutex::new(builder.connect().map_err(SyncFailure::new)?)
        };

        let pool = remote_config.pool.clone();

        Ok(Ceph {
            lockmap: LockMap::new(),
            inner: Arc::new(CephInner { conn, pool }),
        })
    }
}


impl ObjectStore for Ceph {
    type ReadHashed = Box<Future<Item = Option<ArcSlice>, Error = Error> + Send>;
    type WriteHashed = Box<Future<Item = bool, Error = Error> + Send>;

    /// Read a single object from the remote repository.
    fn read_hashed(&self, object_hash: ObjectHash) -> Self::ReadHashed {
        struct MmapWrapper(MmapMut);

        impl ::std::ops::Deref for MmapWrapper {
            type Target = [u8];
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::ops::DerefMut for MmapWrapper {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        unsafe impl ::owning_ref::StableAddress for MmapWrapper {}

        let this = self.clone();

        let result = async_block! {
            let mut ctx = this.inner
                .conn
                .lock()
                .unwrap()
                .get_pool_context(&this.inner.pool)
                .map_err(SyncFailure::new)?;

            let object_id = object_hash.to_string();

            if await!(ctx.exists_async(&object_id)).map_err(SyncFailure::new)? {
                let stat = await!(ctx.stat_async(&object_id)).map_err(SyncFailure::new)?;

                let mut buf =
                    OwningRefMut::new(MmapWrapper(MmapMut::map_anon(stat.size as usize)?));
                let mut total_read = 0;
                let finished_buf = loop {
                    let (bytes_read_u64, new_buf) = await!(ctx.read_async(
                        &object_id,
                        buf.map_mut(|slice| &mut slice[total_read..]),
                        total_read as u64,
                    )).map_err(SyncFailure::new)?;

                    let bytes_read = bytes_read_u64 as usize;
                    total_read += bytes_read;

                    if bytes_read == new_buf.len() {
                        break new_buf.into_inner().0;
                    }

                    buf = new_buf;
                };

                let slice = arc_slice::mapped(finished_buf.make_read_only()?);

                Ok(Some(slice))
            } else {
                Ok(None)
            }
        };

        Box::new(result)
    }

    /// WriteHashed a single object to the remote repository. Returns `false` and performs no I/O if the
    /// catalog shows that the remote already contains the object; `true` otherwise.
    // TODO: Query the remote to see if it contains the object already. If so, don't send.
    fn write_hashed(&self, object_hash: ObjectHash, bytes: ArcSlice) -> Self::WriteHashed {
        let this = self.clone();

        let result = async_block! {
            if !this.lockmap.contains_key(object_hash) {
                let mut ctx = this.inner
                    .conn
                    .lock()
                    .unwrap()
                    .get_pool_context(&this.inner.pool)
                    .map_err(SyncFailure::new)?;

                await!(ctx.write_full_async(&object_hash.to_string(), &bytes)).map_err(SyncFailure::new)?;

                Ok(true)
            } else {
                Ok(false)
            }
        };

        Box::new(result)
    }
}
