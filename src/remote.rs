//! # `remote` - operations on remote repositories.
//!
//! `Remote` contains a `RadosConnection` object, along with a reference to the parent context.
//!
//! At current the only supported remote is a Ceph/RADOS cluster.

use std::sync::{Arc, Mutex};

use futures::prelude::*;
use owning_ref::OwningRefMut;
use rad::{ConnectionBuilder, Connection};

use catalog::Catalog;
use context::Context;
use errors::*;
use local::Local;
use marshal::{Hashed, ObjectHash, Object};
use repository::RemoteCfg;
use trace::Trace;


/// The type of a remote repository.
// TODO: Abstract into a trait.
// TODO: Locally store what objects we know the remote to contain so that we can avoid writing them
//       when the remote already contains them.
// TODO: Make the act of writing an object asynchronous - return a future instead of a `Result.
#[derive(Clone)]
pub struct Remote {
    local: Local,

    catalog: Catalog,
    inner: Arc<RemoteInner>,
}


struct RemoteInner {
    conn: Mutex<Connection>,
    pool: String,
}


impl Remote {
    /// Connect to a remote repository, given appropriate configuration data.
    pub fn connect<T: Trace>(ctx: &Context<T>, cfg: &RemoteCfg, catalog: Catalog) -> Result<Self> {
        let local = Local::new(ctx).chain_err(|| ErrorKind::LocalLoad)?;

        let conn = {
            let mut builder = ConnectionBuilder::with_user(&cfg.object_store.user)
                .chain_err(|| ErrorKind::RemoteConnectInit)?;

            if let Some(ref conf_path) = cfg.object_store.conf_file {
                builder = builder.read_conf_file(conf_path).chain_err(|| {
                    ErrorKind::RemoteConnectReadConf
                })?;
            }

            builder = cfg.object_store
                .conf_options
                .iter()
                .fold(Ok(builder), |acc, (key, value)| {
                    acc.and_then(|conn| conn.conf_set(key, value))
                })
                .chain_err(|| ErrorKind::RemoteConnectConfig)?;

            Mutex::new(builder.connect().chain_err(|| ErrorKind::RemoteConnect)?)
        };

        let pool = cfg.object_store.pool.clone();

        Ok(Remote {
            local,
            catalog,
            inner: Arc::new(RemoteInner { conn, pool }),
        })
    }


    /// Write a single object to the remote repository.
    // TODO: Don't send the object if we know the remote already contains it.
    // TODO: Query the remote to see if it contains the object already. If so, don't send.
    pub fn write_object(&self, hashed: Hashed) -> Box<Future<Item = (), Error = Error> + Send> {
        let lock = match self.catalog.try_lock(*hashed.as_hash()) {
            Ok(lock) => lock,
            Err(future) => return Box::new(future),
        };
        let (hash, bytes_opt) = hashed.into_components();

        match bytes_opt {
            Some(bytes) => {
                let ctx_res = self.inner.conn.lock().unwrap().get_pool_context(
                    &self.inner.pool,
                );
                let result = {
                    async_block! {
                        let mut ctx = ctx_res?;
                        await!(ctx.write_full_async(&hash.to_string(), &bytes))?;
                        lock.release();
                        Ok(())
                    }
                };

                Box::new(result)
            }

            None => {
                unimplemented!("Must load local blob!");
            }
        }
    }


    /// Read a single object from the remote repository.
    ///
    /// This will instead read a local file if the object is already present on disk in the local
    /// blob store.
    pub fn read_object(
        &self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = Object, Error = Error> + Send> {
        let local_future = self.local.read_or_allocate_object(object_hash);
        let ctx_res = self.inner.conn.lock().unwrap().get_pool_context(
            &self.inner.pool,
        );

        let result = {
            async_block! {
                match await!(local_future)? {
                    Ok(object) => Ok(object),
                    Err(factory) => {
                        let mut ctx = ctx_res?;

                        let object_id = object_hash.to_string();
                        let stat = await!(ctx.stat_async(&object_id))?;

                        let mut buf = OwningRefMut::new(factory.with_size(stat.size as usize)?);

                        let mut total_read = 0;
                        let written_buf = loop {
                            let (bytes_read_u64, new_buf) =
                                await!(ctx.read_async(
                                    &object_id,
                                    buf.map_mut(|slice| &mut slice[total_read..]),
                                    total_read as u64,
                                ))?;

                            let bytes_read = bytes_read_u64 as usize;
                            total_read += bytes_read;

                            if bytes_read == new_buf.len() {
                                break new_buf.into_inner();
                            }

                            buf = new_buf;
                        };

                        await!(written_buf.finish())
                    }
                }
            }
        };

        Box::new(result)
    }
}
