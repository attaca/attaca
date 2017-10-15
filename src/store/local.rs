//! # `local` - operate on the locally stored files and blobs of a given repository.
//!
//! The `Local` type represents a properly configured local (file system) object store.
//! Writing/reading objects in a `Local` store is asynchronous.

use std::collections::HashMap;
use std::fs::{self, File};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};

use futures::prelude::*;
use futures_bufio::BufWriter;
use futures_cpupool::CpuPool;
use memmap::{Mmap, Protection};
use stable_deref_trait::StableDeref;

use arc_slice;
use catalog::{Catalog, CatalogLock};
use errors::*;
use marshal::{Hashed, ObjectHash, Object};
use repository::Paths;
use store::Store;


pub struct LocalBufferFactory {
    catalog_lock: CatalogLock,
    object_hash: ObjectHash,
    objects: Arc<Mutex<HashMap<ObjectHash, Object>>>,
    path: PathBuf,
}


impl LocalBufferFactory {
    pub fn with_size(self, sz: usize) -> Result<LocalBuffer> {
        fs::create_dir_all(self.path.parent().unwrap())?;
        let file = File::create(&self.path)?;
        file.set_len(sz as u64)?;
        file.sync_all()?;
        let mmap = Mmap::open(&file, Protection::ReadWrite)?;

        Ok(LocalBuffer {
            catalog_lock: self.catalog_lock,
            object_hash: self.object_hash,
            objects: self.objects,
            mmap,
        })
    }
}


pub struct LocalBuffer {
    catalog_lock: CatalogLock,
    object_hash: ObjectHash,
    objects: Arc<Mutex<HashMap<ObjectHash, Object>>>,
    mmap: Mmap,
}


impl LocalBuffer {
    pub fn finish(self) -> Box<Future<Item = Object, Error = Error> + Send> {
        let result = {
            async_block! {
                self.mmap.flush()?;
                let slice = arc_slice::mapped(self.mmap);
                let object = Object::from_bytes(slice)?;
                self.objects.lock().unwrap().insert(self.object_hash, object.clone());
                self.catalog_lock.release();
                Ok(object)
            }
        };

        Box::new(result)
    }
}


unsafe impl StableDeref for LocalBuffer {}


impl Deref for LocalBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.mmap.as_slice() }
    }
}


impl DerefMut for LocalBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.mmap.as_mut_slice() }
    }
}


/// The type of a local object store.
// TODO: Store `Weak` references to `Mmap`s in `Local` so that we cut down on the number of file
// descriptors that our process owns.
#[derive(Debug, Clone)]
pub struct Local {
    paths: Arc<Paths>,
    io_pool: CpuPool,
    catalog: Catalog,
    objects: Arc<Mutex<HashMap<ObjectHash, Object>>>,
}


impl Local {
    pub fn new(paths: &Arc<Paths>, catalog: &Catalog, io_pool: &CpuPool) -> Self {
        Self {
            paths: paths.clone(),
            io_pool: io_pool.clone(),
            catalog: catalog.clone(),
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Write an object to the file system. Assuming the file has not yet been written, this will
    /// open and then close a file, and the resulting future will return `true` if the object has
    /// not been written and `false` if the object already exists in the catalog and no I/O was
    /// performed.
    pub fn write_object(&self, hashed: Hashed) -> Box<Future<Item = bool, Error = Error> + Send> {
        match self.catalog.try_lock(*hashed.as_hash()) {
            Ok(lock) => {
                match hashed.into_components() {
                    (hash, Some(bytes)) => {
                        let path = self.paths.blobs.join(hash.to_path());
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

                                Ok(true)
                            }
                        };

                        return Box::new(result);
                    }

                    (_, None) => {
                        panic!("Attempted to locally write file which should exist locally!");
                    }
                }
            }
            Err(entry) => Box::new(entry.map(|_| false)),
        }
    }

    /// Load an object from the file system. This will open a file if the object has not already
    /// been loaded.
    /// TODO: Make async.
    pub fn read_object(
        &self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = Object, Error = Error> + Send> {
        let path = self.paths.blobs.join(object_hash.to_path());
        let objects = self.objects.clone();
        let entry_opt = self.catalog.get(object_hash);

        let result = {
            async_block! {
                if let Some(entry) = entry_opt {
                    await!(entry)?;
                }

                if let Some(local) = objects.lock().unwrap().get(&object_hash) {
                    return Ok(local.clone());
                }

                let bytes = arc_slice::mapped(Mmap::open_path(path, Protection::Read)?);
                let object = Object::from_bytes(bytes)?;

                objects.lock().unwrap().insert(object_hash, object.clone());

                Ok(object)
            }
        };

        return Box::new(self.io_pool.spawn(result));
    }

    /// Load an object from the file system, *or*, create a new buffer for writing an object. This
    /// is used for remotes: either load an object from the file system instead of fetching it from
    /// a remote, or create a memory-mapped file buffer, write the serialized object to the buffer,
    /// and then re-protect the buffer as read-only and deserialize the object from it.
    pub fn read_or_allocate_object(
        &self,
        object_hash: ObjectHash,
    ) -> Box<Future<Item = StdResult<Object, LocalBufferFactory>, Error = Error> + Send> {
        let lock_res = self.catalog.try_lock(object_hash);

        match lock_res {
            Ok(lock) => {
                let path = self.paths.blobs.join(object_hash.to_path());
                let objects = self.objects.clone();

                let result = {
                    async_block! {
                        let local_buffer_factory = LocalBufferFactory {
                            catalog_lock: lock,
                            object_hash,
                            objects,
                            path,
                        };

                        Ok(Err(local_buffer_factory))
                    }
                };

                Box::new(result)
            }

            // The object is in the catalog or is currently being written. Defer to `read_object`
            // in order to wait for any potential lock to finish and then do an asynchronous read
            // of the object file.
            Err(_) => Box::new(self.read_object(object_hash).map(Ok)),
        }
    }

    //     /// Write a fully marshalled batch to the local repository.
    //     pub fn write_batch<T: Trace>(
    //         &mut self,
    //         batch: Batch<T::BatchTrace>,
    //     ) -> Box<Future<Item = (), Error = Error>> {
    //         unimplemented!()
    //                 let trace = Arc::new(Mutex::new(self.trace.lock().unwrap().on_write(
    //                     &batch,
    //                     WriteDestination::Local,
    //                 )));
    //
    //                 let local = self.clone();
    //
    //                 let writes = batch
    //                     .into_stream()
    //                     .map(move |hashed| {
    //                         let hash = *hashed.as_hash();
    //                         let trace = trace.clone();
    //
    //                         trace.lock().unwrap().on_begin(&hash);
    //                         let write = local.write_object(hashed).map(move |fresh| {
    //                             trace.lock().unwrap().on_complete(&hash, fresh);
    //                         });
    //                         local.io_pool.spawn(write)
    //                     })
    //                     .buffer_unordered(WRITE_FUTURE_BUFFER_SIZE)
    //                     .for_each(|_| Ok(()));
    //
    //                 Box::new(self.io_pool.spawn(writes))
    //     }
}


impl Store for Local {
    type Read = Box<Future<Item = Object, Error = Error> + Send>;
    type Write = Box<Future<Item = bool, Error = Error> + Send>;

    fn read_object(&self, object_hash: ObjectHash) -> Self::Read {
        self.read_object(object_hash)
    }

    fn write_object(&self, hashed: Hashed) -> Self::Write {
        self.write_object(hashed)
    }
}
