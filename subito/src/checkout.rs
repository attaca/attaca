use std::{fmt, usize, collections::{BTreeSet, HashMap}, fs::{self, OpenOptions}, path::PathBuf,
          sync::Arc};

use attaca::{HandleDigest, Store, digest::Digest, hierarchy::Hierarchy,
             object::{Object, ObjectRef, TreeRef}, path::ObjectPath};
use failure::*;
use futures::{stream, prelude::*};
use hex;
use memmap::MmapMut;

use Repository;
use cache::{Cache, Certainty, Status};
use quantified::{QuantifiedOutput, QuantifiedRefMut};

const LARGE_CHILD_LOOKAHEAD_BUFFER_SIZE: usize = 32;

#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "checkout")]
pub struct CheckoutArgs {
    /// Paths files to checkout. If left empty, the whole tree is checked out.
    #[structopt(name = "PATH", parse(from_os_str))]
    pub paths: Vec<PathBuf>,
}

impl<'r> QuantifiedOutput<'r> for CheckoutArgs {
    type Output = CheckoutOut<'r>;
}

impl QuantifiedRefMut for CheckoutArgs {
    fn apply_mut<'r, S: Store, D: Digest>(
        self,
        repository: &'r mut Repository<S, D>,
    ) -> Result<CheckoutOut<'r>, Error>
    where
        S::Handle: HandleDigest<D>,
    {
        Ok(repository.checkout(self))
    }
}

#[must_use = "CheckoutOut contains futures which must be driven to completion!"]
pub struct CheckoutOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<'r> fmt::Debug for CheckoutOut<'r> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CheckoutOut")
            .field("blocking", &"OPAQUE")
            .finish()
    }
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    #[async]
    fn do_checkout_data(
        store: S,
        cache: Cache<D>,
        base_path: Arc<PathBuf>,
        data_path: ObjectPath,
        data_ref: ObjectRef<S::Handle>,
    ) -> Result<(), Error> {
        let pre = match cache.status(&data_path)? {
            Status::Extant(Certainty::Positive, snapshot) => {
                let maybe_pre_ref = await!(
                    snapshot
                        .as_object_ref()
                        .map(|pre_digest| pre_digest.resolve(&store))
                )?;

                match maybe_pre_ref.and_then(|x| x) {
                    Some(pre_ref) => {
                        if pre_ref == data_ref {
                            return Ok(());
                        }

                        match pre_ref {
                            // Not worth diffing two small objects for now, just overwrite.
                            ObjectRef::Small(_small_ref) => None,
                            ObjectRef::Large(large_ref) => Some(large_ref.fetch()),
                            _ => unreachable!(),
                        }
                    }
                    None => None,
                }
            }
            _ => None,
        };
        let size = match data_ref {
            ObjectRef::Small(ref small_ref) => small_ref.size(),
            ObjectRef::Large(ref large_ref) => large_ref.size(),
            _ => unreachable!(),
        };
        let data = data_ref.fetch();

        let absolute_path = data_path.with_base(&*base_path);
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
        file.set_len(size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        match await!(data.join(pre))? {
            (Object::Small(small), _) => {
                assert!(small.size() <= usize::MAX as u64);
                assert!(mmap.len() == small.size() as usize);

                mmap.copy_from_slice(&small);
                Ok(())
            }
            (Object::Large(new_large), Some(old_large)) => {
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
            (Object::Large(new_large), None) => {
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
            _ => unreachable!(),
        }
    }

    #[async]
    fn do_checkout_tree(
        store: S,
        cache: Cache<D>,
        base_path: Arc<PathBuf>,
        tree_path: ObjectPath,
        tree_ref: TreeRef<S::Handle>,
    ) -> Result<(), Error> {
        let tree = await!(tree_ref.fetch())?;

        if tree.is_empty() {
            return Ok(());
        }

        let absolute_path = tree_path.with_base(&*base_path);
        if absolute_path.exists() {
            let metadata = absolute_path.symlink_metadata()?;

            if !metadata.is_dir() {
                fs::remove_file(&absolute_path)?;
                fs::create_dir(&absolute_path)?;
            }
        } else {
            fs::create_dir(&absolute_path)?;
        }

        let mut entries = absolute_path
            .read_dir()?
            .map(|direntry| {
                Ok((
                    direntry?.file_name().into_string().map_err(|os_string| {
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
                    await!(Self::do_checkout(
                        store.clone(),
                        cache.clone(),
                        base_path.clone(),
                        tree_path.push_back(name),
                        objref,
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
    }

    #[async(boxed)]
    fn do_checkout(
        store: S,
        cache: Cache<D>,
        base_path: Arc<PathBuf>,
        object_path: ObjectPath,
        object_ref: ObjectRef<S::Handle>,
    ) -> Result<(), Error> {
        match object_ref {
            ObjectRef::Small(_) | ObjectRef::Large(_) => await!(Self::do_checkout_data(
                store,
                cache,
                base_path,
                object_path,
                object_ref
            )),
            ObjectRef::Tree(tree_ref) => await!(Self::do_checkout_tree(
                store,
                cache,
                base_path,
                object_path,
                tree_ref,
            )),
            ObjectRef::Commit(_) => unreachable!(),
        }
    }

    fn checkout<'r>(&'r mut self, args: CheckoutArgs) -> CheckoutOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            let subtree = Hierarchy::from(
                await!(
                    state
                        .head
                        .ok_or_else(|| format_err!("Cannot checkout from empty virtual directory"))?
                        .fetch()
                )?.as_subtree().clone(),
            );

            for path in args.paths {
                let object_path = ObjectPath::from_path(&path)?;
                let maybe_object_ref = await!(subtree.get(object_path.clone()))?;

                match maybe_object_ref {
                    Some(object_ref) => await!(Self::do_checkout(
                        self.store.clone(),
                        self.cache.clone(),
                        self.path.clone(),
                        object_path,
                        object_ref
                    ))?,
                    None => {
                        if path.exists() {
                            let file_type = path.symlink_metadata()?.file_type();

                            if file_type.is_symlink() || file_type.is_file() {
                                fs::remove_file(&path)?;
                            } else if file_type.is_dir() {
                                fs::remove_dir_all(&path)?;
                            } else {
                                bail!("Unrecognized file type {:?}", file_type);
                            }
                        }
                    }
                }
            }

            Ok(())
        };

        CheckoutOut {
            blocking: Box::new(blocking),
        }
    }
}
