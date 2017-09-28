//! # `local` - operate on the locally stored files and blobs of a given repository.
//!
//! The `Local` type represents a properly configured local (file system) object store.
//! Writing/reading objects in a `Local` store is asynchronous.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::future;
use futures::prelude::*;
use futures_bufio::BufWriter;
use futures_cpupool::CpuPool;
use memmap::{Mmap, Protection};

use arc_slice;
use catalog::Catalog;
use context::Context;
use errors::*;
use marshal::{Hashed, ObjectHash, Object};
use trace::Trace;


/// The type of a local object store.
// TODO: Store `Weak` references to `Mmap`s in `Local` so that we cut down on the number of file
// descriptors that our process owns.
#[derive(Debug, Clone)]
pub struct Local {
    blob_path: Arc<PathBuf>,
    io_pool: CpuPool,
    catalog: Catalog,
    objects: Arc<Mutex<HashMap<ObjectHash, Object>>>,
}


impl Local {
    pub fn new<T: Trace>(ctx: &Context<T>) -> Result<Self> {
        Ok(Local {
            blob_path: Arc::new(ctx.get_repository().get_blob_path().to_owned()),
            io_pool: ctx.get_io_pool().clone(),
            catalog: ctx.get_repository().get_catalog(None)?,
            objects: Arc::new(Mutex::new(HashMap::new())),
        })
    }


    /// Write an object to the file system. This will open and then close a file.
    pub fn write_object(&self, hashed: Hashed) -> Box<Future<Item = (), Error = Error> + Send> {
        match self.catalog.try_lock(*hashed.as_hash()) {
            Ok(lock) => {
                match hashed.into_components() {
                    (hash, Some(bytes)) => {
                        let path = self.blob_path.join(hash.to_path());
                        let io_pool = self.io_pool.clone();

                        let result = {
                            async_block! {
                                fs::create_dir_all(path.parent().unwrap())?;
                                let file = File::create(path)?;
                                let bufwriter =
                                    BufWriter::with_pool_and_capacity(io_pool, 4096, file);

                                let bufwriter = match await!(bufwriter.write_all(bytes)) {
                                    Ok((writer, _)) => Ok(writer),
                                    Err((_, _, err)) => Err(err),
                                }?;

                                let bufwriter = await!(bufwriter.flush_buf()).map_err(|(_, err)| err)?;
                                await!(bufwriter.flush_inner()).map_err(|(_, err)| err)?;

                                lock.release();

                                Ok(())
                            }
                        };

                        return Box::new(result);
                    }

                    (_, None) => {
                        panic!("Attempted to locally write file which should exist locally!");
                    }
                }
            }
            Err(entry) => Box::new(entry),
        }
    }


    /// Load an object from the file system. This will open a file if the object has not already
    /// been loaded.
    /// TODO: Make async.
    pub fn read_object(
        &self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = Object, Error = Error> + Send> {
        if let Some(local) = self.objects.lock().unwrap().get(&object_hash) {
            return Box::new(future::ok(local.clone()));
        }

        let path = self.blob_path.join(object_hash.to_path());
        let objects = self.objects.clone();

        let result = future::lazy(move || {
            let bytes = arc_slice::mapped(Mmap::open_path(path, Protection::Read)?);
            let object = Object::from_bytes(bytes)?;

            objects.lock().unwrap().insert(object_hash, object.clone());

            Ok(object)
        });

        return Box::new(self.io_pool.spawn(result));
    }
}
