use std::collections::HashMap;
use std::io::Error as IoError;
use std::mem;
use std::path::{Path, PathBuf};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::OsStrExt;

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
pub struct Index {
    timestamp: DateTime<Utc>,
    entries: HashMap<PathBuf, IndexEntry>,
}


impl Index {
    pub fn update(&mut self, base: &Path) -> Result<()> {
        // Create a new timestamp for when we *begin* indexing.
        let fresh_timestamp = Utc::now();

        for (path, entry) in &mut self.entries {
            entry.update(&IndexMetadata::load(base.join(path))?, &self.timestamp);
        }

        // Timestamps refer to the instant at which indexing starts, rather than the instant at
        // which it ends (the mtime of the index itself.)
        self.timestamp = fresh_timestamp;

        Ok(())
    }
}
