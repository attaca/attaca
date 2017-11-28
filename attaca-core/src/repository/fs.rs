//! # `local` - operate on the locally stored files and blobs of a given repository.
//!
//! The `FileSystem` type represents a properly configured local (file system) object store.
//! Writing/reading objects in a `FileSystem` store is asynchronous.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};

use failure::Error;
use futures::prelude::*;
use futures_cpupool::CpuPool;
use memmap::{Mmap, MmapMut, MmapOptions};

use arc_slice::{self, ArcSlice};
use hasher::Hashed;
use lockmap::LockMap;
use object::Object;
use object_hash::ObjectHash;
use repository::Repository;


/// The type of a local object store.
// TODO: ObjectStore `Weak` references to `Mmap`s in `FileSystem` so that we cut down on the number of file
// descriptors that our process owns.
#[derive(Debug, Clone)]
pub struct FileSystem {
    lock_path: Arc<PathBuf>,
    path: Arc<PathBuf>,
    lockmap: LockMap,
    io_pool: CpuPool,
}


impl FileSystem {
    pub fn open(cfg: FileSystemCfg) -> Result<Self, Error> {
        let path = cfg.path;
        let lock_path = path.join("LOCKED");
        let _ = OpenOptions::new().create_new(true).open(&lock_path)?;

        Ok(Self {
            lock_path: Arc::new(lock_path),
            path: Arc::new(path),
            lockmap: LockMap::new(),
            io_pool: CpuPool::new(1),
        })
    }

    pub fn open_branch<S: AsRef<str>>(&self, branch: S, options: &OpenOptions) -> io::Result<File> {
        options.open(self.path.join("branch").join(branch.as_ref()))
    }
}


impl Drop for FileSystem {
    fn drop(&mut self) {
        if let Err(io_error) = fs::remove_file(&*self.lock_path) {
            panic!(
                "Unable to unlock filesystem repository at {}, due to error: {}",
                self.lock_path.display(),
                io_error
            );
        }
    }
}


impl Repository for FileSystem {
    type ReadHashed = Box<Future<Item = Option<ArcSlice>, Error = Error> + Send>;
    type WriteHashed = Box<Future<Item = bool, Error = Error> + Send>;

    /// Load an object from the file system. This will open a file if the object has not already
    /// been loaded.
    /// TODO: Make async.
    fn read_hashed(&self, object_hash: ObjectHash) -> Self::ReadHashed {
        let path = self.path.clone();
        let lockmap = self.lockmap.clone();

        let result = async_block! {
            let lock_attempt = lockmap.get_or_lock(object_hash);
            match lock_attempt {
                Ok(value) => await!(value).map(Some),
                Err(lock) => {
                    let object_path = path.join(object_hash.to_path());

                    match File::open(object_path) {
                        Ok(file) => {
                            let bytes = arc_slice::mapped(unsafe { Mmap::map(&file) }?);
                            lock.fill(bytes.clone())?;
                            Ok(Some(bytes))
                        }
                        Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(None),
                        Err(io_error) => Err(Error::from(io_error)),
                    }
                }
            }
        };

        Box::new(self.io_pool.spawn(result))
    }

    /// Write an object to the file system. Assuming the file has not yet been written, this will
    /// open and then close a file, and the resulting future will return `true` if the object has
    /// not been written and `false` if the object already exists in the catalog and no I/O was
    /// performed.
    fn write_hashed(&self, object_hash: ObjectHash, bytes: ArcSlice) -> Self::WriteHashed {
        let path = self.path.clone();
        let lockmap = self.lockmap.clone();

        let result = async_block! {
            if !lockmap.contains_key(object_hash) {
                let object_path = path.join(object_hash.to_path());

                fs::create_dir_all(object_path.parent().unwrap())?;

                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(object_path)
                {
                    Ok(file) => {
                        file.set_len(bytes.len() as u64)?;

                        let mut mmap_mut = unsafe { MmapMut::map_mut(&file) }?;
                        mmap_mut.copy_from_slice(&bytes);

                        return Ok(true);
                    }
                    Err(ref io_error) if io_error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(io_error) => return Err(Error::from(io_error)),
                }
            }

            Ok(false)
        };

        Box::new(self.io_pool.spawn(result))
    }

    fn compare_and_swap_branch(
        &self,
        branch: String,
        previous_hash: Option<ObjectHash>,
        new_hash: ObjectHash,
    ) -> Result<Option<ObjectHash>, Error> {
        match previous_hash {
            Some(previous_hash) => {
                let mut branch_file =
                    self.open_branch(branch, OpenOptions::new().read(true).write(true))?;

                let current_hash = {
                    let mut buf = String::new();
                    branch_file.read_to_string(&mut buf)?;
                    buf.parse::<ObjectHash>()?
                };

                if current_hash == previous_hash {
                    branch_file.seek(SeekFrom::Start(0))?;
                    write!(branch_file, "{}", new_hash)?;
                    Ok(Some(previous_hash))
                } else {
                    Ok(Some(current_hash))
                }
            }
            None => {
                let mut branch_file = self.open_branch(
                    branch,
                    OpenOptions::new().read(true).write(true).create_new(true),
                )?;

                branch_file.write_all(&new_hash)?;

                Ok(None)
            }
        }
    }

    fn get_branch(&self, branch: String) -> Result<Option<ObjectHash>, Error> {
        match self.open_branch(branch, OpenOptions::new().read(true)) {
            Ok(mut branch_file) => {
                let mut buf = String::new();
                branch_file.read_to_string(&mut buf)?;
                let object_hash = buf.parse::<ObjectHash>()?;
                Ok(Some(object_hash))
            }
            Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(io_error) => Err(Error::from(io_error)),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSystemCfg {
    path: PathBuf,
}
