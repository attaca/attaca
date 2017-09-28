//! # `remote` - operations on remote repositories.
//!
//! `Remote` contains a `RadosConnection` object, along with a reference to the parent context.
//!
//! At current the only supported remote is a Ceph/RADOS cluster.

use std::sync::{Arc, Mutex};

use futures::prelude::*;
use memmap::Mmap;
use owning_ref::BoxRefMut;
use rad::{ConnectionBuilder, Connection};

use arc_slice;
use catalog::{Catalog, CatalogLock};
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

    catalog: Option<Catalog>,
    inner: Arc<RemoteInner>,
}


struct RemoteInner {
    conn: Mutex<Connection>,
    pool: String,
}


impl Remote {
    /// Connect to a remote repository, given appropriate configuration data.
    pub fn connect<T: Trace>(
        ctx: &Context<T>,
        cfg: &RemoteCfg,
        catalog: Option<Catalog>,
    ) -> Result<Self> {
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
        let lock_opt_res = self.catalog.as_ref().map(|catalog| {
            catalog.try_lock(*hashed.as_hash())
        });
        let lock_opt = match lock_opt_res {
            Some(Ok(lock)) => Some(lock),
            None => None,
            Some(Err(future)) => return Box::new(future),
        };
        let (hash, bytes_opt) = hashed.into_components();

        match bytes_opt {
            Some(bytes) => {
                let ctx_res = self.inner.conn.lock().unwrap().get_pool_context(
                    &self.inner.pool,
                );
                let result =
                    async_block! {
                    let mut ctx = ctx_res?;
                    await!(ctx.write_full_async(&hash.to_string(), &bytes))?;
                    lock_opt.map(CatalogLock::release);
                    Ok(())
                };

                Box::new(result)
            }

            None => {
                unimplemented!("Must load local blob!");
            }
        }
    }


    /// Read a single object from the remote repository.
    pub fn read_object<F>(
        &self,
        object_hash: ObjectHash,
        factory: F,
    ) -> Box<Future<Item = Object, Error = Error> + Send>
    where
        F: FnOnce(usize) -> Mmap + Send + 'static,
    {
        let ctx_res = self.inner.conn.lock().unwrap().get_pool_context(
            &self.inner.pool,
        );

        let result =
            async_block! {
            let mut ctx = ctx_res?;

            let object_id = object_hash.to_string();
            let stat = await!(ctx.stat_async(&object_id))?;

            let mut buf = BoxRefMut::new(Box::new(factory(stat.size as usize)))
                .map_mut(|mmap| unsafe { mmap.as_mut_slice() });

            let mut total_read = 0;
            let mmap = loop {
                let (bytes_read_u64, new_buf) = await!(ctx.read_async(
                    &object_id,
                    buf.map_mut(
                        |slice| &mut slice[total_read..],
                    ),
                    total_read as u64,
                ))?;

                let bytes_read = bytes_read_u64 as usize;
                total_read += bytes_read;

                if bytes_read == new_buf.len() {
                    break *new_buf.into_inner();
                }

                buf = new_buf;
            };

            let slice = arc_slice::mapped(mmap);
            let object = Object::from_bytes(slice)?;

            Ok(object)
        };

        //             .and_then(|ctx| {
        //                 ctx.read_async(&object_hash.to_string());
        //             })
        //             .into_future()
        //             .from_err()
        //             .flatten();

        Box::new(result)
    }
}
