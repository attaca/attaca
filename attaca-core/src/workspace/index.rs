use std::collections::hash_map::{Entry, HashMap};
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::OsStrExt;

use bincode;
use chrono::prelude::*;
use failure::{self, Error};
use globset::GlobSet;
use libc;

use object_hash::ObjectHash;


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
    pub fn load<P: AsRef<Path>>(path: P) -> Result<IndexMetadata, Error> {
        let stat = unsafe {
            let path_c_string = CString::new(path.as_ref().as_os_str().as_bytes())?;
            let path_ptr = path_c_string.as_ptr() as *const i8;

            let mut stat = mem::zeroed();
            if libc::lstat64(path_ptr, &mut stat) != 0 {
                return Err(Error::from(io::Error::last_os_error()));
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


#[derive(Debug, Clone, Copy)]
pub enum Indexed {
    Valid(Cached),
    Invalid,
    Untracked,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hygiene {
    Clean,
    Dodgy,
    Dirty,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cached {
    Hashed(ObjectHash, u64),
    Unhashed,
    Removed,
}


#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct IndexEntry {
    pub hygiene: Hygiene,
    metadata: IndexMetadata,

    pub tracked: bool,

    // We tell serde to skip this value so that it's set to false whenever deserialized. If we
    // switch over to git-style `add`, then we'll stop skipping it so that `add`s persist between
    // index operations.
    #[serde(skip)] pub added: bool,

    pub cached: Cached,
}


impl IndexEntry {
    pub fn fresh(metadata: IndexMetadata, cached: Cached) -> IndexEntry {
        IndexEntry {
            hygiene: Hygiene::Clean,
            metadata,
            tracked: false,
            added: false,
            cached,
        }
    }

    pub fn track(&mut self, tracked: bool) -> &mut Self {
        self.tracked = tracked;

        self
    }

    pub fn add(&mut self, added: bool) -> &mut Self {
        self.added = added;

        self
    }

    pub fn update(&mut self, fresh: &IndexMetadata, timestamp: &DateTime<Utc>) -> &mut Self {
        if self.hygiene != Hygiene::Dirty {
            self.hygiene = {
                if &self.metadata == fresh {
                    // If the timestamp of the index is older or the same as the cached mtime...
                    // then this entry is dodgy.
                    if timestamp <= &self.metadata.mtime {
                        Hygiene::Dodgy
                    } else {
                        Hygiene::Clean
                    }
                } else {
                    Hygiene::Dirty
                }
            };
        }

        self
    }

    pub fn clean(
        &mut self,
        fresh: &IndexMetadata,
        timestamp: &DateTime<Utc>,
        object_hash: ObjectHash,
    ) -> Result<(), Error> {
        self.update(&fresh, &timestamp);
        // We check to ensure the self does not seem to have been modified since its last
        // update, and thus that it has not been changed since its hash was calculated.
        if self.hygiene != Hygiene::Clean {
            return Err(failure::err_msg("Entry concurrently modified!"));
        }
        self.cached = Cached::Hashed(object_hash, fresh.size as u64);

        Ok(())
    }

    pub fn get(&self) -> Option<Cached> {
        if self.hygiene == Hygiene::Clean {
            Some(self.cached)
        } else {
            None
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
            timestamp: Utc::now().with_nanosecond(0).unwrap(),
            entries: HashMap::new(),
        }
    }
}


#[derive(Debug)]
pub struct Index {
    now: DateTime<Utc>,
    data: IndexData,
}


impl Index {
    pub fn new(data: IndexData) -> Self {
        Self {
            now: Utc::now().with_nanosecond(0).unwrap(),
            data,
        }
    }

    pub fn get<P: ?Sized + AsRef<Path>>(&self, path: &P) -> Option<Cached> {
        self.data
            .entries
            .get(path.as_ref())
            .and_then(|entry| entry.get())
    }

    pub fn update<P: ?Sized + AsRef<Path>>(&mut self, workspace_path: &P) -> Result<(), Error> {
        // Create a new timestamp for when we *begin* indexing.
        let fresh_timestamp = Utc::now().with_nanosecond(0).unwrap();

        self.data.entries = {
            let timestamp_ref = &self.data.timestamp;

            mem::replace(&mut self.data.entries, HashMap::new())
                .into_iter()
                .map(|(relative_path, mut entry)| {
                    if entry.tracked {
                        let absolute_path = workspace_path.as_ref().join(&relative_path);
                        if absolute_path.exists() {
                            let file_type = absolute_path.symlink_metadata()?.file_type();
                            if file_type.is_file() || file_type.is_symlink() {
                                let fresh = IndexMetadata::load(absolute_path)?;
                                entry.update(&fresh, timestamp_ref);
                                if entry.hygiene != Hygiene::Dirty {
                                    return Ok(Some((relative_path, entry)));
                                }
                            }
                        } else {
                            entry.cached = Cached::Removed;
                            return Ok(Some((relative_path, entry)));
                        }

                        Ok(None)
                    } else {
                        Ok(Some((relative_path, entry)))
                    }
                })
                .filter_map(|kv_opt_res| match kv_opt_res {
                    Ok(Some(kv)) => Some(Ok(kv)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                })
                .collect::<Result<HashMap<PathBuf, IndexEntry>, Error>>()?
        };

        // Timestamps refer to the instant at which indexing starts, rather than the instant at
        // which it ends (the mtime of the index itself.)
        self.data.timestamp = fresh_timestamp;

        Ok(())
    }

    pub fn clean<P, Q>(
        &mut self,
        absolute: &P,
        relative: &Q,
        object_hash: ObjectHash,
    ) -> Result<(), Error>
    where
        P: ?Sized + AsRef<Path>,
        Q: ?Sized + AsRef<Path>,
    {
        match self.data.entries.get_mut(relative.as_ref()) {
            Some(entry) => entry.clean(
                &IndexMetadata::load(absolute)?,
                &self.data.timestamp,
                object_hash,
            ),
            None => Err(failure::err_msg("Cannot clean untracked entry.")),
        }
    }

    // TODO: Take an iterator of string slices instead of a `GlobSet`, and attempt to parse those
    // string slices into `Glob`s of their own. Then, only visit subdirectories which we know might
    // contain the files we're looking for.
    //
    // Then again, not doing that is fast enough for ripgrep.
    pub fn register<P: ?Sized + AsRef<Path>>(
        &mut self,
        base: &P,
        pattern: &GlobSet,
    ) -> Result<(), Error> {
        let mut stack = Vec::new();
        stack.push(base.as_ref().read_dir()?);

        while let Some(iter) = stack.pop() {
            for dir_entry_res in iter {
                let dir_entry = dir_entry_res?;
                let absolute_path = dir_entry.path();
                let relative_path = absolute_path.strip_prefix(&base).unwrap().to_owned();

                // TODO: Find a better way to ignore `.attaca`.
                if relative_path == Path::new(".attaca") {
                    continue;
                }

                if absolute_path.symlink_metadata()?.is_dir() {
                    stack.push(absolute_path.read_dir()?);
                } else if pattern.is_match(&relative_path) {
                    let fresh = IndexMetadata::load(absolute_path)?;

                    match self.data.entries.entry(relative_path) {
                        Entry::Occupied(mut occupied) => {
                            occupied.get_mut().update(&fresh, &self.data.timestamp);
                        }
                        Entry::Vacant(vacant) => {
                            vacant.insert(IndexEntry::fresh(fresh, Cached::Unhashed));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a Path, &'a IndexEntry)> {
        self.data
            .entries
            .iter()
            .map(|(path, entry)| (path.as_ref(), entry))
    }

    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item = (&'a Path, &'a mut IndexEntry)> {
        self.data
            .entries
            .iter_mut()
            .map(|(path, entry)| (path.as_ref(), entry))
    }

    pub fn prune(&mut self) {
        self.data
            .entries
            .retain(|_, entry| entry.cached != Cached::Removed);
    }

    pub fn into_data(self) -> IndexData {
        self.data
    }
}
