use std::collections::HashMap;
use std::fs::File;
use std::io::Error as IoError;
use std::mem;
use std::path::{Path, PathBuf};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::OsStrExt;

use bincode;
use chrono::prelude::*;
use libc;

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
            let path_ptr = path.as_ref().as_os_str().as_bytes().as_ptr() as *const i8;
            let mut stat = mem::zeroed();
            if libc::lstat(path_ptr, &mut stat) != 0 {
                bail!(IoError::last_os_error());
            }
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

    object_hash: ObjectHash,
}


impl IndexEntry {
    fn update(&mut self, fresh: &IndexMetadata, timestamp: &DateTime<Utc>) {
        if self.hygiene == Hygiene::Clean {
            self.hygiene = {
                if &self.cached == fresh {
                    if timestamp > &self.cached.mtime {
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
    entries: HashMap<PathBuf, IndexEntry>,
}


impl IndexData {
    pub fn new() -> Self {
        IndexData {
            timestamp: Utc::now(),
            entries: HashMap::new(),
        }
    }
}


#[derive(Debug)]
pub struct Index {
    path: PathBuf,
    data: IndexData,
}


impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Index> {
        let path_ref = path.as_ref();

        let data = if path_ref.is_file() {
            let mut index_file = File::open(path_ref)?;
            bincode::deserialize_from(&mut index_file, bincode::Infinite)?
        } else {
            IndexData::new()
        };

        Ok(Index {
            path: path_ref.to_owned(),
            data,
        })
    }


    pub fn update<P: AsRef<Path>>(&mut self, base: P) -> Result<()> {
        // Create a new timestamp for when we *begin* indexing.
        let fresh_timestamp = Utc::now();

        for (path, entry) in &mut self.data.entries {
            entry.update(
                &IndexMetadata::load(base.as_ref().join(path))?,
                &self.data.timestamp,
            );
        }

        // Timestamps refer to the instant at which indexing starts, rather than the instant at
        // which it ends (the mtime of the index itself.)
        self.data.timestamp = fresh_timestamp;

        Ok(())
    }
}


impl Drop for Index {
    fn drop(&mut self) {
        let mut file = File::create(&self.path).unwrap();
        bincode::serialize_into(&mut file, &self.data, bincode::Infinite).unwrap();
    }
}
