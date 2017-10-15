use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io::Error as IoError;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::OsStrExt;

use bincode;
use chrono::prelude::*;
use globset::GlobSet;
use libc;

use DEFAULT_IGNORES;
use errors::*;
use marshal::ObjectHash;
use repository::Paths;


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
    Hashed(ObjectHash),
    Unhashed,
    Removed,
}


#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct IndexEntry {
    hygiene: Hygiene,
    metadata: IndexMetadata,
    cached: Cached,
}


impl IndexEntry {
    fn fresh(metadata: IndexMetadata, cached: Cached) -> IndexEntry {
        IndexEntry {
            hygiene: Hygiene::Clean,
            metadata,
            cached,
        }
    }

    fn update(&mut self, fresh: &IndexMetadata, timestamp: &DateTime<Utc>) {
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
    }

    fn get(&self) -> Option<Cached> {
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
    data: IndexData,
    paths: Arc<Paths>,
}


impl Index {
    pub fn open(paths: &Arc<Paths>) -> Result<Index> {
        let data = if paths.index.exists() {
            let mut index_file = File::open(&paths.index).chain_err(|| ErrorKind::IndexOpen)?;
            bincode::deserialize_from(&mut index_file, bincode::Infinite)
                .chain_err(|| ErrorKind::IndexParse)?
        } else {
            IndexData::new()
        };

        let index = Index {
            data,
            paths: paths.clone(),
        };

        Ok(index)
    }

    pub fn update(&mut self) -> Result<()> {
        // Create a new timestamp for when we *begin* indexing.
        let fresh_timestamp = Utc::now().with_nanosecond(0).unwrap();

        self.data.entries = {
            let base_ref = &self.paths.base;
            let timestamp_ref = &self.data.timestamp;

            mem::replace(&mut self.data.entries, HashMap::new())
                .into_iter()
                .map(|(relative_path, mut entry)| {
                    let absolute_path = base_ref.join(&relative_path);
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
                })
                .filter_map(|kv_opt_res| match kv_opt_res {
                    Ok(Some(kv)) => Some(Ok(kv)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                })
                .collect::<Result<HashMap<PathBuf, IndexEntry>>>()?
        };

        // Timestamps refer to the instant at which indexing starts, rather than the instant at
        // which it ends (the mtime of the index itself.)
        self.data.timestamp = fresh_timestamp;

        Ok(())
    }

    pub fn update_entry<P: AsRef<Path>>(&mut self, path: P, object_hash: ObjectHash) -> Result<()> {
        match self.data.entries.get_mut(path.as_ref()) {
            Some(entry) => {
                entry.update(
                    &IndexMetadata::load(self.paths.base.join(&path))?,
                    &self.data.timestamp,
                );

                // We check to ensure the entry does not seem to have been modified since its last
                // update, and thus that it has not been changed since its hash was calculated.
                ensure!(
                    entry.hygiene == Hygiene::Clean,
                    ErrorKind::IndexConcurrentModification(path.as_ref().to_owned())
                );

                entry.cached = Cached::Hashed(object_hash);
            }

            None => bail!(ErrorKind::IndexUpdateUntracked),
        }

        Ok(())
    }

    fn track_file(
        entries_mut: &mut HashMap<PathBuf, IndexEntry>,
        timestamp: &DateTime<Utc>,
        absolute_path: &Path,
        relative_path: PathBuf,
    ) -> Result<()> {
        let fresh = IndexMetadata::load(absolute_path)?;

        entries_mut
            .entry(relative_path)
            .or_insert_with(|| IndexEntry::fresh(fresh, Cached::Unhashed))
            .update(&fresh, timestamp);

        Ok(())
    }

    // TODO: Take an iterator of string slices instead of a `GlobSet`, and attempt to parse those
    // string slices into `Glob`s of their own. Then, only visit subdirectories which we know might
    // contain the files we're looking for.
    pub fn track(&mut self, pattern: &GlobSet) -> Result<()> {
        let mut stack = Vec::new();
        stack.push(self.paths.base.read_dir()?);

        while let Some(iter) = stack.pop() {
            for dir_entry_res in iter {
                let dir_entry = dir_entry_res?;
                let absolute_path = dir_entry.path();
                let relative_path = absolute_path.strip_prefix(&self.paths.base).unwrap();

                // TODO: Replace `DEFAULT_IGNORES` with a `Gitignore` from the `gitignore` crate.
                if DEFAULT_IGNORES.contains(relative_path) {
                    continue;
                }

                if absolute_path.symlink_metadata()?.is_dir() {
                    stack.push(absolute_path.read_dir()?);
                } else if pattern.is_match(relative_path) {
                    Self::track_file(
                        &mut self.data.entries,
                        &self.data.timestamp,
                        &absolute_path,
                        relative_path.to_owned(),
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn untrack(&mut self, pattern: &GlobSet) {
        self.data.entries.retain(|path, _| !pattern.is_match(path));
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a PathBuf, Option<Cached>)> {
        self.data.entries.iter().map(
            |(path, entry)| (path, entry.get()),
        )
    }

    pub fn prune(&mut self) {
        self.data.entries.retain(
            |_, entry| entry.cached != Cached::Removed,
        );
    }

    pub fn cleanup(self) -> Result<()> {
        let mut file = File::create(&self.paths.index)?;
        bincode::serialize_into(&mut file, &self.data, bincode::Infinite)?;

        Ok(())
    }
}
