//! `local` - operate on the locally stored files and blobs of a given repository.

use std::collections::HashMap;
use std::fs::{self, File};
use std::sync::{Arc, Mutex};

use futures::future;
use futures::prelude::*;
use futures_bufio::BufWriter;
use memmap::{Mmap, Protection};

use context::Context;
use arc_slice::Source;
use errors::*;
use marshal::{Hashed, ObjectHash, Object};
use trace::Trace;


/// The type of a local object store.
// TODO: Store `Weak` references to `Mmap`s in `Local` so that we cut down on the number of file
// descriptors that our process owns.
#[derive(Debug)]
pub struct Local<T: Trace> {
    ctx: Context<T>,

    objects: Arc<Mutex<HashMap<ObjectHash, Object>>>,
}


impl<T: Trace> Local<T> {
    pub fn new(ctx: Context<T>) -> Self {
        Local {
            ctx,

            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }


    /// Write an object to the file system. This will open and then close a file.
    pub fn write_object(&self, hashed: Hashed) -> Box<Future<Item = (), Error = Error> + Send> {
        if self.objects.lock().unwrap().contains_key(hashed.as_hash()) {
            return Box::new(future::ok(()));
        }

        match hashed.into_components() {
            (hash, Some(bytes)) => {
                let path = self.ctx.as_repository().blob_path.join(hash.to_path());

                if !path.is_file() {
                    let dir_path = {
                        let mut dp = path.clone();
                        dp.pop();
                        dp
                    };

                    match fs::create_dir_all(dir_path).and_then(|_| File::create(path)) {
                        Ok(file) => {
                            let writer = BufWriter::with_pool_and_capacity(
                                self.ctx.write_pool().clone(),
                                4096,
                                file,
                            );

                            let result = writer
                                .write_all(bytes)
                                .then(|res| -> Box<Future<Item = _, Error = _> + Send> {
                                    match res {
                                        Ok((writer, _)) => Box::new(writer.flush_buf()),
                                        Err((writer, _, err)) => Box::new(
                                            future::err((writer, err)),
                                        ),
                                    }
                                })
                                .and_then(|writer| writer.flush_inner())
                                .then(|res| match res {
                                    Ok(_) => Ok(()),
                                    Err((_, err)) => Err(err.into()),
                                });

                            Box::new(result)
                        }

                        Err(err) => Box::new(future::err(err.into())),
                    }
                } else {
                    Box::new(future::ok(()))
                }
            }

            (_, None) => {
                panic!("Attempted to locally write file which should exist locally!");
            }
        }
    }


    /// Load an object from the file system. This will open a file if the object has not already
    /// been loaded.
    pub fn read_object<'local>(&'local self, object_hash: &ObjectHash) -> Result<Object> {
        if let Some(local) = self.objects.lock().unwrap().get(object_hash) {
            return Ok(local.clone());
        }

        let path = self.ctx.as_repository().blob_path.join(object_hash.to_path());
        let bytes = Source::Mapped(Mmap::open_path(path, Protection::Read)?).into_bytes();
        let object = Object::from_bytes(bytes)?;

        self.objects.lock().unwrap().insert(
            *object_hash,
            object.clone(),
        );

        return Ok(object);
    }
}


impl<T: Trace> Clone for Local<T> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            objects: self.objects.clone(),
        }
    }
}
