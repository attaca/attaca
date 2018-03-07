use std::{usize, collections::BTreeSet, fs::{self, File, OpenOptions}, path::Path};

use attaca::{hierarchy::Hierarchy, object::{Large, Object, ObjectRef, TreeRef}, path::ObjectPath,
             store::prelude::*};
use failure::*;
use futures::{stream, prelude::*};
use memmap::MmapMut;
use ignore::WalkBuilder;

use super::*;
use Repository;
use cache::{Certainty, Status};

const LARGE_CHILD_LOOKAHEAD_BUFFER_SIZE: usize = 32;

impl<B: Backend> Repository<B> {
    #[async]
    fn checkout_data_from_large_with_previous(
        new_large: Large<Handle<B>>,
        old_large: Large<Handle<B>>,
        mut mmap: MmapMut,
    ) -> Result<(), Error> {
        assert!(new_large.size() <= usize::MAX as u64);
        assert!(mmap.len() == new_large.size() as usize);
        assert!(new_large.depth() == 1 && old_large.depth() == 1);

        let new_entries = new_large
            .into_iter()
            .map(|(range, objref)| (range.start, range.end, objref))
            .collect::<BTreeSet<_>>();
        let old_entries = old_large
            .into_iter()
            .map(|(range, objref)| (range.start, range.end, objref))
            .collect::<BTreeSet<_>>();
        let entries = new_entries
            .difference(&old_entries)
            .cloned()
            .map(|(start, end, objref)| {
                assert!(start <= usize::MAX as u64 && end <= usize::MAX as u64);
                let range = start as usize..end as usize;
                (range, objref)
            })
            .collect::<Vec<_>>();

        let futures = entries.into_iter().map(|(range, objref)| match objref {
            ObjectRef::Small(small_ref) => small_ref.fetch().map(|small| (range, small)),
            _ => unreachable!("depth == 1, all refs are to small objects"),
        });
        let buffered = stream::iter_ok(futures).buffered(LARGE_CHILD_LOOKAHEAD_BUFFER_SIZE);

        #[async]
        for (range, small) in buffered {
            mmap[range].copy_from_slice(&small);
        }
        mmap.flush()?;

        Ok(())
    }

    #[async]
    fn checkout_data_from_large_without_previous(
        new_large: Large<Handle<B>>,
        mut mmap: MmapMut,
    ) -> Result<(), Error> {
        let entries = new_large.into_iter().map(|(range, objref)| {
            assert!(range.start <= usize::MAX as u64 && range.end <= usize::MAX as u64);
            let range = range.start as usize..range.end as usize;
            (range, objref)
        });

        let futures = entries.into_iter().map(|(range, objref)| match objref {
            ObjectRef::Small(small_ref) => small_ref.fetch().map(|small| (range, small)),
            _ => unreachable!("depth == 1, all refs are to small objects"),
        });
        let buffered = stream::iter_ok(futures).buffered(LARGE_CHILD_LOOKAHEAD_BUFFER_SIZE);

        #[async]
        for (range, small) in buffered {
            mmap[range].copy_from_slice(&small);
        }
        mmap.flush()?;

        Ok(())
    }

    pub fn checkout_file_from_data(
        this: &mut Self,
        data_ref: ObjectRef<Handle<B>>,
        previous_ref: Option<ObjectRef<Handle<B>>>,
        file: File,
    ) -> FutureUnit {
        let blocking = async_block! {
            let size = match data_ref {
                ObjectRef::Small(ref small_ref) => small_ref.size(),
                ObjectRef::Large(ref large_ref) => large_ref.size(),
                _ => unreachable!(),
            };

            let future_previous = previous_ref.and_then(|object_ref| match object_ref {
                ObjectRef::Large(large_ref) => Some(large_ref.fetch()),
                _ => None,
            });
            let futures = data_ref.fetch().join(future_previous);

            file.set_len(size)?;
            if size == 0 {
                // NB mmap will error if we try to map a zero byte file.
                return Ok(());
            }

            let mut mmap = unsafe { MmapMut::map_mut(&file)? };

            match await!(futures)? {
                (Object::Small(small), _) => {
                    assert!(small.size() <= usize::MAX as u64);
                    assert!(mmap.len() == small.size() as usize);

                    mmap.copy_from_slice(&small);
                    Ok(())
                }
                (Object::Large(new_large), Some(old_large)) => await!(
                    Self::checkout_data_from_large_with_previous(new_large, old_large, mmap)
                ),
                (Object::Large(new_large), None) => await!(
                    Self::checkout_data_from_large_without_previous(new_large, mmap)
                ),
                _ => unreachable!(),
            }
        };

        Box::new(blocking)
    }

    /// Panics if `data` is not a `Small` or `Large` ref.
    pub fn checkout_path_from_data(
        this: &mut Self,
        data_ref: ObjectRef<Handle<B>>,
        path: ObjectPath,
    ) -> FutureUnit {
        let blocking = async_block! {
            let maybe_previous_ref = match this.cache.status(&path)? {
                Status::Extant(Certainty::Positive, snapshot) => {
                    let maybe_pre_ref = await!(
                        snapshot
                            .as_object_ref()
                            .map(|pre_digest| pre_digest.resolve_id(&this.store))
                    )?;

                    match maybe_pre_ref.and_then(|x| x) {
                        Some(pre_ref) => {
                            if pre_ref == data_ref {
                                return Ok(());
                            }

                            match pre_ref {
                                // Not worth diffing two small objects for now, just overwrite.
                                ObjectRef::Small(_) => None,
                                objref@ObjectRef::Large(_) => Some(objref),
                                _ => unreachable!(),
                            }
                        }
                        None => None,
                    }
                }
                _ => None,
            };

            let absolute_path = path.with_base(&*this.path);
            let file = if absolute_path.exists() {
                let metadata = absolute_path.symlink_metadata()?;
                let file_type = metadata.file_type();

                if file_type.is_symlink() || file_type.is_file() {
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&absolute_path)?
                } else if file_type.is_dir() {
                    fs::remove_dir_all(&absolute_path)?;
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(&absolute_path)?
                } else {
                    bail!("Unknown file type for {}", absolute_path.display());
                }
            } else {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&absolute_path)?
            };

            await!(Self::checkout_file_from_data(this, data_ref, maybe_previous_ref, file))
        };

        Box::new(blocking)
    }

    pub fn checkout_path_from_tree(
        this: &mut Self,
        tree_ref: TreeRef<Handle<B>>,
        path: ObjectPath,
    ) -> FutureUnit {
        let blocking = async_block! {
            let tree = await!(tree_ref.fetch())?;

            if tree.is_empty() {
                return Ok(());
            }

            let absolute_path = path.with_base(&*this.path);
            if absolute_path.exists() {
                let metadata = absolute_path.symlink_metadata()?;

                if !metadata.is_dir() {
                    fs::remove_file(&absolute_path)?;
                    fs::create_dir(&absolute_path)?;
                }
            } else {
                fs::create_dir(&absolute_path)?;
            }

            // Use a WalkBuilder in order to respect ignores. This happens to also nicely ignore
            // `.attaca`.
            //
            // TODO: More robust way to avoid clobbering `.attaca`: add specialized ignores/overrides
            // to all WalkBuilders.
            // TODO #33
            let mut entries = WalkBuilder::new(&absolute_path)
                .max_depth(Some(1))
                .build()
                .map(|direntry| {
                    Ok((
                        direntry?
                            .file_name()
                            .to_os_string()
                            .into_string()
                            .map_err(|os_string| {
                                format_err!("Unable to convert {:?} into a UTF-8 string", os_string)
                            })?,
                        None,
                    ))
                })
                .collect::<Result<HashMap<_, _>, Error>>()?;
            entries.extend(tree.into_iter().map(|(name, objref)| (name, Some(objref))));

            for (name, maybe_objref) in entries {
                match maybe_objref {
                    Some(objref) => {
                        await!(Self::checkout_path_from_object(
                            this,
                            objref,
                            path.push_back(name),
                        ))?;
                    }
                    None => {
                        let child_path = absolute_path.join(&name);
                        if child_path.exists() {
                            let metadata = child_path.symlink_metadata()?;
                            let file_type = metadata.file_type();

                            if file_type.is_file() || file_type.is_symlink() {
                                fs::remove_file(&child_path)?;
                            } else if file_type.is_dir() {
                                fs::remove_dir_all(&child_path)?;
                            } else {
                                bail!("Unrecognized file type {:?}", file_type);
                            }
                        }
                    }
                }
            }

            Ok(())
        };

        Box::new(blocking)
    }

    /// This function will panic if given an `ObjectRef::Commit`.
    pub fn checkout_path_from_object(
        this: &mut Self,
        object_ref: ObjectRef<Handle<B>>,
        path: ObjectPath,
    ) -> FutureUnit {
        match object_ref {
            ObjectRef::Small(_) | ObjectRef::Large(_) => {
                Self::checkout_path_from_data(this, object_ref, path)
            }
            ObjectRef::Tree(tree_ref) => Self::checkout_path_from_tree(this, tree_ref, path),
            ObjectRef::Commit(_) => unreachable!(),
        }
    }

    /// Selectively checkout paths from a tree.
    pub fn checkout_paths_from_tree<'r, I>(
        this: &'r mut Self,
        tree: TreeRef<Handle<B>>,
        base_path: ObjectPath,
        paths: I,
    ) -> FutureUnit
    where
        I: IntoIterator<Item = ObjectPath> + 'r,
    {
        let blocking = async_block! {
            let subtree = Hierarchy::from(tree);

            for object_path in paths {
                let maybe_object_ref = await!(subtree.get(object_path.clone()))?;
                match maybe_object_ref {
                    Some(object_ref) => await!(Self::checkout_path_from_object(
                        this,
                        object_ref,
                        &base_path + object_path,
                    ))?,
                    None => bail!("No such object in the previous commit!"),
                }
            }

            Ok(())
        };

        Box::new(blocking)
    }
}
