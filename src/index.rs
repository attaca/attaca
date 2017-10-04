use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io::Error as IoError;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::OsStrExt;

use bincode;
use chrono::prelude::*;
use futures::prelude::*;
use libc;

use {INDEX_PATH, DEFAULT_IGNORES};
use errors::*;
use marshal::ObjectHash;


#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMetadata {
    mtime: DateTime<Utc>,
    ctime: DateTime<Utc>,

    uid: u32,
    gid: u32,
    ino: u64,
    mode: u32,
    size: i64,
}


#[cfg(any(target_os = "linux", target_os = "macos"))]
impl IndexMetadata {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<IndexMetadata> {
        let stat = unsafe {
            let path_c_string = CString::new(path.as_ref().as_os_str().as_bytes())?;
            let path_ptr = path_c_string.as_ptr() as *const i8;

            let mut stat = mem::zeroed();
            if libc::lstat64(path_ptr, &mut stat) != 0 {
                bail!(Error::with_chain(
                    IoError::last_os_error(),
                    ErrorKind::IndexStat(path.as_ref().to_owned()),
                ));
            }

            let _ = path_c_string;

            stat
        };

        let mtime = Utc.timestamp(stat.st_mtime, 0);
        let ctime = Utc.timestamp(stat.st_ctime, 0);

        let uid = stat.st_uid;
        let gid = stat.st_gid;
        let ino = stat.st_ino;
        let mode = stat.st_mode;
        let size = stat.st_size;

        Ok(IndexMetadata {
            mtime,
            ctime,
            uid,
            gid,
            ino,
            mode,
            size,
        })
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hygiene {
    Clean,
    Dodgy,
    Dirty,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct IndexEntry {
    hygiene: Hygiene,
    cached: IndexMetadata,

    object_hash: Option<ObjectHash>,
}


impl IndexEntry {
    fn update(&mut self, fresh: &IndexMetadata, timestamp: &DateTime<Utc>) {
        if self.hygiene == Hygiene::Clean {
            self.hygiene = {
                if &self.cached == fresh {
                    // If the timestamp of the index is older or the same as the cached mtime...
                    // then this entry is dodgy.
                    if timestamp <= &self.cached.mtime {
                        Hygiene::Dodgy
                    } else {
                        Hygiene::Clean
                    }
                } else {
                    Hygiene::Dirty
                }
            };
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct IndexData {
    timestamp: DateTime<Utc>,
    entries: Mutex<HashMap<PathBuf, IndexEntry>>,
}


impl IndexData {
    pub fn new() -> Self {
        IndexData {
            timestamp: Utc::now().with_nanosecond(0).unwrap(),
            entries: Mutex::new(HashMap::new()),
        }
    }
}


#[derive(Debug)]
pub struct Index {
    base: PathBuf,
    data: IndexData,
}


impl Index {
    pub fn open<P: AsRef<Path>>(base: P) -> Result<Index> {
        let base_ref = base.as_ref();
        let index_base = base_ref.join(&*INDEX_PATH);

        let data = if index_base.is_file() {
            let mut index_file = File::open(index_base).chain_err(|| ErrorKind::IndexOpen)?;
            bincode::deserialize_from(&mut index_file, bincode::Infinite)
                .chain_err(|| ErrorKind::IndexParse)?
        } else {
            IndexData::new()
        };

        let mut index = Index {
            base: base_ref.to_owned(),
            data,
        };

        index.update().chain_err(|| ErrorKind::IndexUpdate)?;

        Ok(index)
    }


    pub fn update(&mut self) -> Result<()> {
        // Create a new timestamp for when we *begin* indexing.
        let fresh_timestamp = Utc::now().with_nanosecond(0).unwrap();
        let entries_mut = self.data.entries.get_mut().unwrap();

        *entries_mut = {
            let mut new_entries = HashMap::new();
            let mut stack = Vec::new();
            stack.push(self.base.read_dir()?);

            while let Some(iter) = stack.pop() {
                for dir_entry_res in iter {
                    let dir_entry = dir_entry_res?;
                    let absolute_path = dir_entry.path();
                    let relative_path = absolute_path.strip_prefix(&self.base).unwrap();

                    if DEFAULT_IGNORES.contains(relative_path) {
                        continue;
                    }

                    if absolute_path.is_dir() {
                        stack.push(absolute_path.read_dir()?);
                    } else {
                        let fresh = IndexMetadata::load(&absolute_path)?;
                        let mut entry = entries_mut.remove(relative_path).unwrap_or_else(|| {
                            IndexEntry {
                                hygiene: Hygiene::Clean,
                                cached: fresh,
                                object_hash: None,
                            }
                        });

                        entry.update(&fresh, &self.data.timestamp);
                        new_entries.insert(relative_path.to_owned(), entry);
                    }
                }
            }

            // All dirty entries can be cleared.
            new_entries.retain(|_, entry| entry.hygiene != Hygiene::Dirty);

            new_entries
        };

        // Timestamps refer to the instant at which indexing starts, rather than the instant at
        // which it ends (the mtime of the index itself.)
        self.data.timestamp = fresh_timestamp;

        Ok(())
    }


    pub fn clear(&mut self) {
        self.data.entries.get_mut().unwrap().clear();
    }


    pub fn entries(&mut self) -> &HashMap<PathBuf, IndexEntry> {
        self.data.entries.get_mut().unwrap()
    }


    pub fn share(self) -> SharedIndex {
        SharedIndex { inner: Arc::new(self) }
    }
}


impl Drop for Index {
    fn drop(&mut self) {
        let mut file = File::create(self.base.join(&*INDEX_PATH)).unwrap();
        bincode::serialize_into(&mut file, &self.data, bincode::Infinite).unwrap();
    }
}


#[derive(Debug, Clone)]
pub struct SharedIndex {
    inner: Arc<Index>,
}


impl SharedIndex {
    pub fn get_or_insert<F, G>(
        &mut self,
        path: PathBuf,
        thunk: G,
    ) -> Box<Future<Item = ObjectHash, Error = Error>>
    where
        F: IntoFuture<Item = ObjectHash, Error = Error> + 'static,
        G: FnOnce() -> F + 'static,
    {
        let this = self.inner.clone();

        let result = {
            async_block! {
                if let Some(&IndexEntry {
                                hygiene: Hygiene::Clean,
                                object_hash: Some(object_hash),
                                ..
                            }) = this.data.entries.lock().unwrap().get(&path)
                {
                    return Ok(object_hash);
                }

                let full_path = this.base.join(&path);

                let meta_before = IndexMetadata::load(&full_path)?;
                let object_hash = await!(thunk().into_future())?;
                let meta_after = IndexMetadata::load(&full_path)?;

                let mut entry = IndexEntry {
                    hygiene: Hygiene::Clean,
                    cached: meta_before,
                    object_hash: Some(object_hash),
                };

                entry.update(&meta_after, &this.data.timestamp);
                ensure!(
                    entry.hygiene == Hygiene::Clean,
                    ErrorKind::IndexConcurrentModification(full_path)
                );

                this.data.entries.lock().unwrap().insert(path, entry);

                Ok(object_hash)
            }
        };

        Box::new(result)
    }
}
