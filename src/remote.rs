//! `remote` - operations on remote repositories.
//!
//! At current the only supported remote is a Ceph/RADOS cluster.

use std::ffi::CString;
use std::sync::{Arc, Mutex};

use futures::prelude::*;
use rad::{RadosConnectionBuilder, RadosConnection};
use rad::async::RadosCaution;

use context::Context;
use errors::*;
use marshal::Hashed;
use repository::RemoteCfg;
use trace::Trace;


/// The type of a remote repository.
// TODO: Abstract into a trait.
// TODO: Locally store what objects we know the remote to contain so that we can avoid writing them
//       when the remote already contains them.
// TODO: Make the act of writing an object asynchronous - return a future instead of a `Result.
pub struct Remote<T: Trace> {
    ctx: Context<T>,

    inner: Arc<RemoteInner>,
}


struct RemoteInner {
    conn: Mutex<RadosConnection>,
    pool: CString,
}


impl<T: Trace> Remote<T> {
    /// Connect to a remote repository, given appropriate configuration data.
    pub fn connect(ctx: Context<T>, cfg: &RemoteCfg) -> Result<Self> {
        let conf_dir = CString::new(cfg.object_store.conf.to_str().unwrap()).unwrap();
        let keyring_dir = cfg.object_store.keyring.as_ref().map(|keyring| {
            CString::new(keyring.to_str().unwrap()).unwrap()
        });

        let conn = {
            let mut builder = RadosConnectionBuilder::with_user(cfg.object_store.user.as_c_str())?
                .read_conf_file(conf_dir.as_c_str())?;

            if let Some(ref keyring) = keyring_dir {
                builder = builder.conf_set(
                    CString::new("keyring")?,
                    keyring.as_c_str(),
                )?;
            }

            Mutex::new(builder.connect()?)
        };

        let pool = cfg.object_store.pool.clone();

        Ok(Remote {
            ctx,
            inner: Arc::new(RemoteInner { conn, pool }),
        })
    }


    /// Write a single object to the remote repository.
    // TODO: Make asynchronous.
    // TODO: Don't send the object if we know the remote already contains it.
    // TODO: Query the remote to see if it contains the object already. If so, don't send.
    pub fn write_object(
        &self,
        hashed: Hashed,
    ) -> Result<Box<Future<Item = (), Error = Error> + Send>> {
        match hashed.as_bytes() {
            Some(bytes) => {
                let mut ctx = self.inner.conn.lock().unwrap().get_pool_context(&*self.inner.pool)?;
                let object_id = CString::new(hashed.as_hash().to_string()).unwrap();

                Ok(Box::new(
                    ctx.write_full_async(
                        RadosCaution::Complete,
                        &*object_id,
                        bytes,
                    )?
                        .from_err(),
                ))
            }

            None => {
                unimplemented!("Must load local blob!");
            }
        }
    }
}


impl<T: Trace> Clone for Remote<T> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            inner: self.inner.clone(),
        }
    }
}
