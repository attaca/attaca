use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::iter;
use std::path::{Path, PathBuf};

use bincode;
use failure::{self, Error};
use futures::prelude::*;
use futures::stream::{self, FuturesUnordered};
use futures::sync::mpsc;
use memmap::{Mmap, MmapMut};

use arc_slice;
use hasher::Hasher;
use object::{DataObject, Object, SubtreeEntry, SubtreeObject};
use object_hash::ObjectHash;
use repository::{ReadObject, Repository, RepositoryCfg};
use split::SliceChunker;

mod index;

use self::index::{Cached, Index, IndexData};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum State {
    Empty,
    Stable(Head),
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeadRef {
    Remote(String, String),
    Local(String),
    Detached,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Head {
    commit_hash: ObjectHash,
    reference: HeadRef,
}


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    local: Option<RepositoryCfg>,
    remotes: HashMap<String, RepositoryCfg>,
}


#[derive(Debug)]
pub struct Workspace {
    path: PathBuf,
    config: Config,
    state: State,
    index: Index,
}


impl Workspace {
    fn open_config_file<P: ?Sized + AsRef<Path>>(
        path: &P,
        options: &OpenOptions,
    ) -> Result<Option<File>, Error> {
        match options.open(path.as_ref().join(".attaca/config.toml")) {
            Ok(file) => Ok(Some(file)),
            Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(io_error) => Err(Error::from(io_error)),
        }
    }

    fn open_index_file<P: ?Sized + AsRef<Path>>(
        path: &P,
        options: &OpenOptions,
    ) -> Result<Option<File>, Error> {
        match options.open(path.as_ref().join(".attaca/index.bin")) {
            Ok(file) => Ok(Some(file)),
            Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(io_error) => Err(Error::from(io_error)),
        }
    }

    fn open_state_file<P: ?Sized + AsRef<Path>>(
        path: &P,
        options: &OpenOptions,
    ) -> Result<Option<File>, Error> {
        match options.open(path.as_ref().join(".attaca/state.bin")) {
            Ok(file) => Ok(Some(file)),
            Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(io_error) => Err(Error::from(io_error)),
        }
    }

    pub fn init(path: PathBuf) -> Result<(), Error> {
        fs::create_dir(path.join(".attaca"))?;

        let mut config_file =
            Self::open_config_file(&path, OpenOptions::new().write(true).create_new(true))?
                .unwrap();
        bincode::serialize_into(&mut config_file, &Config::default(), bincode::Infinite)?;

        let mut index_file =
            Self::open_index_file(&path, OpenOptions::new().write(true).create_new(true))?.unwrap();
        bincode::serialize_into(&mut index_file, &IndexData::new(), bincode::Infinite)?;

        let mut state_file =
            Self::open_state_file(&path, OpenOptions::new().write(true).create_new(true))?.unwrap();
        bincode::serialize_into(&mut state_file, &State::Empty, bincode::Infinite)?;

        Ok(())
    }

    pub fn open(path: PathBuf) -> Result<Self, Error> {
        let config = match Self::open_config_file(&path, OpenOptions::new().read(true))? {
            Some(mut config_file) => {
                bincode::deserialize_from(&mut config_file, bincode::Infinite)?
            }
            None => Config::default(),
        };

        let mut index = match Self::open_index_file(&path, OpenOptions::new().read(true))? {
            Some(mut index_file) => Index::new(bincode::deserialize_from(
                &mut index_file,
                bincode::Infinite,
            )?),
            None => Index::new(IndexData::new()),
        };

        index.update(&path)?;

        let state = match Self::open_state_file(&path, OpenOptions::new().read(true))? {
            Some(mut state_file) => bincode::deserialize_from(&mut state_file, bincode::Infinite)?,
            None => State::Empty,
        };

        Ok(Self {
            path,
            config,
            state,
            index,
        })
    }

    pub fn close(self) -> Result<(), Error> {
        let mut config_file =
            Self::open_config_file(&self.path, OpenOptions::new().write(true).create(true))?
                .unwrap();
        bincode::serialize_into(&mut config_file, &self.config, bincode::Infinite)?;

        let mut index_file =
            Self::open_index_file(&self.path, OpenOptions::new().write(true).create(true))?
                .unwrap();
        bincode::serialize_into(&mut index_file, &self.index.into_data(), bincode::Infinite)?;

        let mut state_file =
            Self::open_state_file(&self.path, OpenOptions::new().write(true).create(true))?
                .unwrap();
        bincode::serialize_into(&mut state_file, &self.state, bincode::Infinite)?;

        Ok(())
    }

    fn open_workspace_file<P: AsRef<Path>>(
        &self,
        path: &P,
        options: &OpenOptions,
    ) -> Result<File, Error> {
        Ok(options.open(self.path.join(path))?)
    }

    pub fn hash_file<P: ?Sized + AsRef<Path>, R: Repository>(
        &mut self,
        path: &P,
        repository: &R,
    ) -> Result<Option<ObjectHash>, Error> {
        let file_path = self.path.join(path);
        let file_type = match file_path.symlink_metadata() {
            Ok(metadata) => metadata.file_type(),
            Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(io_error) => return Err(Error::from(io_error)),
        };

        if !file_type.is_file() && !file_type.is_symlink() {
            return Err(failure::err_msg("Unknown file type!"));
        }

        if let Some(Cached::Hashed(data_hash, _data_size)) = self.index.get(path.as_ref()) {
            return Ok(Some(data_hash));
        }

        let slice = {
            let file = File::open(&file_path)?;
            let mmap = unsafe { Mmap::map(&file)? };
            arc_slice::mapped(mmap)
        };

        let chunks = SliceChunker::new(slice);
        let (tx, rx) = mpsc::channel(64);
        let hasher = Hasher::new(tx);
        let sender = rx.map_err(|()| {
            failure::err_msg("futures::sync::mpsc::Receiver never errors")
        }).for_each(|hashed| {
                let (object_hash, bytes) = hashed.into_components();
                repository.write_hashed(object_hash, bytes).map(|_| ())
            });

        let file_hash = hasher
            .process_chunks(stream::iter_ok(chunks))
            .join(sender)
            .map(|(object_hash, ())| object_hash)
            .wait()?;

        self.index.clean(&file_path, path, file_hash)?;

        Ok(Some(file_hash))
    }

    pub fn write_file<P: AsRef<Path>, R: Repository>(
        &mut self,
        path: &P,
        repository: &R,
        data_hash: ObjectHash,
        data_size: u64,
    ) -> Result<(), Error> {
        struct Item<R: Repository> {
            offset: u64,
            object_hash: ObjectHash,
            future: ReadObject<R::ReadHashed>,
        }

        impl<R: Repository> Future for Item<R> {
            type Item = (u64, ObjectHash, Option<Object>);
            type Error = Error;

            fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
                match self.future.poll() {
                    Ok(async) => Ok(async.map(|object| (self.offset, self.object_hash, object))),
                    Err(err) => Err(Error::from(err)),
                }
            }
        }

        let file =
            self.open_workspace_file(path, OpenOptions::new().read(true).write(true).create(true))?;
        file.set_len(data_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let mut queue = iter::once(Item {
            offset: 0,
            object_hash: data_hash,
            future: repository.read_object(data_hash),
        }).collect::<FuturesUnordered<Item<R>>>();

        // N.B. _object_hash currently ignored, to be used for error reporting.
        while let (Some((mut offset, _object_hash, object)), mut polled_queue) =
            queue.into_future().wait().map_err(|(err, _)| err)?
        {
            match object {
                Some(Object::Data(DataObject::Small(small_object))) => {
                    mmap[offset as usize..offset as usize + small_object.chunk.len()]
                        .copy_from_slice(&small_object.chunk);
                }
                Some(Object::Data(DataObject::Large(large_object))) => {
                    for (child_size, child_hash) in large_object.children {
                        polled_queue.push(Item {
                            offset: offset,
                            object_hash: child_hash,
                            future: repository.read_object(child_hash),
                        });
                        offset += child_size;
                    }
                }
                None => return Err(failure::err_msg("Missing data child!")),
                _ => return Err(failure::err_msg("Not a data object!")),
            }

            queue = polled_queue;
        }

        Ok(())
    }

    pub fn write_subtree<P: ?Sized + AsRef<Path>, R: Repository>(
        &mut self,
        path: &P,
        repository: &R,
        subtree_hash: ObjectHash,
    ) -> Result<(), Error> {
        let diff = self.diff_directory_to_subtree(path, repository, subtree_hash)?;
        self.write_diff(repository, &diff)?;

        Ok(())
    }

    pub fn write_diff<R: Repository>(
        &mut self,
        repository: &R,
        diff: &WorkspaceDiff,
    ) -> Result<(), Error> {
        for file_path in &diff.file_removals {
            fs::remove_file(self.path.join(file_path))?;
        }

        for dir_path in &diff.directory_removals {
            fs::remove_dir(self.path.join(dir_path))?;
        }

        for dir_path in &diff.directory_insertions {
            fs::create_dir(self.path.join(dir_path))?;
        }

        for (file_path, &(file_hash, file_size)) in &diff.file_insertions {
            self.write_file(file_path, repository, file_hash, file_size)?;
        }

        Ok(())
    }

    pub fn read_dir<P: ?Sized + AsRef<Path>>(
        &self,
        path: &P,
    ) -> Result<Option<HashMap<OsString, PathBuf>>, Error> {
        match fs::read_dir(path) {
            Ok(read_dir) => read_dir
                .map(|entry_res| {
                    let entry = entry_res?;

                    let name = entry.file_name();
                    let path = entry.path();

                    Ok((name, path))
                })
                .collect::<Result<_, _>>()
                .map(Some),
            Err(ref io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(io_error) => Err(Error::from(io_error)),
        }
    }
}


#[derive(Debug, Clone, Default)]
pub struct WorkspaceDiff {
    dirty: bool,

    pub file_removals: HashSet<PathBuf>,
    pub directory_removals: BTreeSet<PathBuf>,

    pub directory_insertions: BTreeSet<PathBuf>,
    pub file_insertions: HashMap<PathBuf, (ObjectHash, u64)>,
}


impl WorkspaceDiff {
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}


impl Workspace {
    fn read_dir_recursive<P: ?Sized + AsRef<Path>>(
        &mut self,
        path: &P,
        directories: &mut BTreeSet<PathBuf>,
        files: &mut HashMap<PathBuf, Option<ObjectHash>>,
    ) -> Result<(), Error> {
        for (entry_name, entry_path) in self.read_dir(path)?.unwrap() {
            let relative_path = path.as_ref().join(entry_name);
            let entry_type = entry_path.symlink_metadata()?.file_type();

            if entry_type.is_file() || entry_type.is_symlink() {
                let index_entry = match self.index.get(&relative_path) {
                    Some(Cached::Hashed(object_hash, _)) => Some(object_hash),
                    _ => None,
                };
                files.insert(relative_path, index_entry);
            } else if entry_type.is_dir() {
                self.read_dir_recursive(&relative_path, directories, files)?;
                directories.insert(relative_path);
            } else {
                return Err(failure::err_msg("Unknown file type!"));
            }
        }

        Ok(())
    }

    fn read_subtree_recursive<P: ?Sized + AsRef<Path>, R: Repository>(
        path: &P,
        subtrees: &mut BTreeSet<PathBuf>,
        files: &mut HashMap<PathBuf, (ObjectHash, u64)>,
        repository: &R,
        subtree_hash: ObjectHash,
    ) -> Result<(), Error> {
        let subtree_object = match repository.read_object(subtree_hash).wait()?.unwrap() {
            Object::Subtree(subtree_object) => subtree_object,
            _ => return Err(failure::err_msg("Not a subtree!")),
        };

        for (entry_name, subtree_entry) in subtree_object.entries {
            let entry_path = path.as_ref().join(entry_name);
            match subtree_entry {
                SubtreeEntry::File(file_hash, file_size) => {
                    files.insert(entry_path, (file_hash, file_size));
                }
                SubtreeEntry::Subtree(dir_hash) => {
                    Self::read_subtree_recursive(
                        &entry_path,
                        subtrees,
                        files,
                        repository,
                        dir_hash,
                    )?;
                    subtrees.insert(entry_path);
                }
            }
        }

        Ok(())
    }

    pub fn diff_directory_to_subtree<P: ?Sized + AsRef<Path>, R: Repository>(
        &mut self,
        path: &P,
        repository: &R,
        subtree_hash: ObjectHash,
    ) -> Result<WorkspaceDiff, Error> {
        let mut workspace_directories = BTreeSet::new();
        let mut workspace_files = HashMap::new();
        self.read_dir_recursive(path, &mut workspace_directories, &mut workspace_files)?;

        let mut subtree_directories = BTreeSet::new();
        let mut subtree_files = HashMap::new();
        Self::read_subtree_recursive(
            path,
            &mut subtree_directories,
            &mut subtree_files,
            repository,
            subtree_hash,
        )?;

        let directory_removals = &workspace_directories - &subtree_directories;
        let directory_insertions = &subtree_directories - &workspace_directories;

        let file_removals = workspace_files
            .iter()
            .filter_map(|(path, _)| {
                if !subtree_files.contains_key(path) {
                    Some(path.to_owned())
                } else {
                    None
                }
            })
            .collect();

        let file_insertions = subtree_files
            .into_iter()
            .filter_map(|(path, (file_hash, file_size))| {
                match workspace_files.get(&path) {
                    Some(&Some(ref cached_hash)) if cached_hash != &file_hash => {
                        Some((path, (file_hash, file_size)))
                    }
                    _ => None,
                }
            })
            .collect();

        let dirty = workspace_files.values().any(Option::is_none);

        Ok(WorkspaceDiff {
            dirty,

            file_removals,
            directory_removals,

            directory_insertions,
            file_insertions,
        })
    }
}
