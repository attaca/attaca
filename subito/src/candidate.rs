use std::{fmt, cmp::Ordering, collections::{BTreeMap, BTreeSet}, fs::File, path::{Path, PathBuf},
          sync::Arc};

use attaca::{Handle, HandleDigest, Store,
             batch::{Batch as ObjectBatch, Operation as ObjectOperation}, digest::Digest,
             hierarchy::Hierarchy, object::{self, ObjectRef, TreeBuilder, TreeRef},
             path::ObjectPath};
use failure::{self, *};
use futures::{stream, future::Either, prelude::*};
use ignore::WalkBuilder;
use itertools::Itertools;
use sequence_trie::SequenceTrie;

use {Repository, State};
use cache::{Cache, Certainty, Status};

/// Load files into the virtual workspace.
#[derive(Debug, StructOpt)]
#[structopt(name = "stage")]
pub struct StageArgs {
    /// Paths of files to load.
    #[structopt(name = "PATH", parse(from_os_str), raw(required = r#"true"#))]
    paths: Vec<PathBuf>,

    /// Load files from the previous commit into the virtual workspace instead.
    #[structopt(short = "p", long = "previous")]
    previous: bool,

    /// Do not show progress.
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,
}

#[derive(Debug)]
pub struct FileProgress {
    file_path: PathBuf,
    object_path: ObjectPath,

    processed_bytes: u64,
    total_bytes: u64,
}

pub struct StageOut {
    pub files: Box<Stream<Item = FileProgress, Error = Error>>,
}

impl fmt::Debug for StageOut {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StageOut")
            .field("files", &"OPAQUE")
            .finish()
    }
}

/// Type for the two kinds of possible operations on the candidate tree.
#[derive(Debug, Clone, Copy)]
pub enum OpKind {
    /// Synchronize a child object of the candidate tree to its workspace counterpart.
    Stage,

    /// Reset a child object of the candidate tree to its HEAD counterpart.
    Unstage,
}

/// Type for staging/unstaging operations.
#[derive(Debug, Clone)]
pub struct BatchOp {
    /// An absolute or relative path to an object in the repository to stage or unstage. It does
    /// not necessarily need to resolve to an actual file; `Stage` operations on nonexistent files
    /// become `Delete` operations on the candidate while paths for `Unstage` operations do not
    /// refer to the local filesystem at all, only the HEAD tree.
    pub path: PathBuf,

    /// The operation type (stage operation, for adding local files to the candidate tree, or
    /// unstage operation, for resetting objects to the state of the HEAD tree.)
    pub op: OpKind,
}

impl BatchOp {
    pub fn stage(path: PathBuf) -> Self {
        Self {
            path,
            op: OpKind::Stage,
        }
    }

    pub fn unstage(path: PathBuf) -> Self {
        Self {
            path,
            op: OpKind::Unstage,
        }
    }
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn stage<'r>(&'r mut self, args: StageArgs) -> impl Future<Item = (), Error = Error> + 'r {
        let op = if args.previous {
            OpKind::Unstage
        } else {
            OpKind::Stage
        };
        let batch = args.paths.into_iter().map(|path| BatchOp { path, op });

        async_block! {
            await!(self.stage_batch(batch))?;
            Ok(())
        }
    }

    #[async]
    fn do_process_file(
        store: S,
        cache: Cache<D>,
        absolute_path: PathBuf,
        object_path: ObjectPath,
    ) -> Result<ObjectRef<S::Handle>, Error> {
        let status = cache
            .status(&object_path)
            .context("Error during cache lookup for file")?;

        let pre_resolution = match status {
            Status::Extant(Certainty::Positive, ref snapshot) => {
                let resolution = snapshot
                    .as_object_ref()
                    .cloned()
                    .map(|odr| odr.resolve(&store));
                resolution
            }
            _ => None,
        };

        if let Some(resolved) = await!(pre_resolution)
            .context("Error resolving cached digest")?
            .and_then(|x| x)
        {
            return Ok(resolved);
        }

        match status {
            // TODO: Respect cache and reuse hash.
            Status::Extant(_, snapshot) | Status::New(snapshot) => {
                let mut file = File::open(&absolute_path).context("Error opening local file")?;
                let objref =
                    await!(object::share(file, store)).context("Error hashing/sending local file")?;
                let digest = await!(objref.digest()).context("Error fetching object digest")?;
                cache
                    .resolve(snapshot, digest)
                    .context("Error during cache resolution for file")?;

                Ok(objref)
            }
            Status::Removed | Status::Extinct => bail!("File removed during processing!"),
        }
    }

    #[async]
    fn do_process(
        store: S,
        cache: Cache<D>,
        absolute_path: PathBuf,
        object_path: ObjectPath,
    ) -> Result<Option<ObjectRef<S::Handle>>, Error> {
        if !absolute_path.exists() {
            return Ok(None);
        }

        let file_type = absolute_path.symlink_metadata()?.file_type();
        if file_type.is_symlink() || file_type.is_file() {
            let objref = await!(Self::do_process_file(
                store,
                cache,
                absolute_path,
                object_path
            ))?;
            Ok(Some(objref))
        } else {
            let mut object_batch = ObjectBatch::<S::Handle>::new();
            let walk = WalkBuilder::new(&absolute_path).build();

            for direntry_res in walk {
                let direntry = direntry_res?;
                let file_type = direntry.file_type().unwrap();

                if file_type.is_dir() {
                    continue;
                }

                let object_path =
                    ObjectPath::from_path(direntry.path().strip_prefix(&absolute_path)?)?;
                // TODO: Concurrency here? Or more efficient not to?
                let object_ref = await!(Self::do_process_file(
                    store.clone(),
                    cache.clone(),
                    direntry.path().to_owned(),
                    object_path.clone(),
                ))?;
                object_batch =
                    await!(object_batch.add(ObjectOperation::Add(object_path, object_ref)))?;
            }

            let built = await!(object_batch.run(store.clone(), TreeBuilder::new()))?;
            Ok(Some(ObjectRef::Tree(await!(built.as_tree().send(&store))?)))
        }
    }

    pub fn process<'r>(
        &'r self,
        absolute_path: PathBuf,
        object_path: ObjectPath,
    ) -> impl Future<Item = Option<ObjectRef<S::Handle>>, Error = Error> {
        Self::do_process(
            self.store.clone(),
            self.cache.clone(),
            absolute_path,
            object_path,
        )
    }

    fn process_operation<'r>(
        &'r self,
        hierarchy: Hierarchy<S::Handle>,
        batch_op: BatchOp,
    ) -> impl Future<Item = ObjectOperation<S::Handle>, Error = Error> {
        let BatchOp { path: raw_path, op } = batch_op;

        let paths_res = if raw_path.is_absolute() {
            raw_path
                .strip_prefix(&self.path)
                .map_err(failure::err_msg)
                .and_then(|relative_path| ObjectPath::from_path(relative_path))
                .map(|object_path| (raw_path, object_path))
        } else {
            ObjectPath::from_path(&raw_path)
                .map(|object_path| (self.path.join(&raw_path), object_path))
        };

        let future_res = paths_res.map(|(absolute_path, object_path)| {
            let future = match op {
                OpKind::Unstage => Either::A(
                    hierarchy
                        .get(object_path.clone())
                        .map_err(|e| e.context("Error processing file from previous commit")),
                ),
                OpKind::Stage => Either::B(
                    self.process(absolute_path, object_path.clone())
                        .map_err(|e| e.context("Error processing local file")),
                ),
            };
            future.map(|objref_opt| (object_path, objref_opt))
        });
        async_block! {
            let (object_path, objref_opt) = await!(future_res?)?;
            let operation = match objref_opt {
                Some(objref) => ObjectOperation::Add(object_path, objref),
                None => ObjectOperation::Delete(object_path),
            };
            Ok(operation)
        }
    }

    pub fn stage_batch<'r, I>(&'r mut self, batch: I) -> impl Future<Item = (), Error = Error> + 'r
    where
        I: IntoIterator<Item = BatchOp> + 'r,
    {
        async_block! {
            let state = self.get_state()?;
            let hierarchy = match state.head {
                Some(head_ref) => Hierarchy::from(
                    await!(head_ref.fetch())
                        .context("Error while fetching head")?
                        .as_subtree()
                        .clone(),
                ),
                None => Hierarchy::new(),
            };
            let queue = stream::futures_ordered(
                batch.into_iter().map(|batch_op| self.process_operation(hierarchy.clone(), batch_op)),
            );
            let batch: ObjectBatch<S::Handle> =
                await!(queue.fold(ObjectBatch::new(), |batch, op| batch.add(op)))
                    .context("Error while batching stage operations")?;
            await!(self.stage_objects(batch)).context("Error while staging objects")?;

            Ok(())
        }
    }

    pub fn stage_objects<'r>(
        &'r mut self,
        batch: ObjectBatch<S::Handle>,
    ) -> impl Future<Item = (), Error = Error> + 'r {
        async_block! {
            let state = self.get_state().context("Error while fetching state")?;
            let tree_builder = match state.candidate.clone() {
                Some(candidate_ref) => await!(candidate_ref.fetch())
                    .context("Error while fetching candidate")?
                    .diverge(),
                None => TreeBuilder::new(),
            };

            let new_candidate_built = await!(batch.run(self.store.clone(), tree_builder))
                .context("Error running batch on candidate")?;
            let candidate = if new_candidate_built.is_empty() && state.head.is_none() {
                None
            } else {
                Some(await!(new_candidate_built.as_tree().send(&self.store))
                    .context("Error sending new candidate to store")?)
            };

            self.set_state(&State { candidate, ..state })
                .context("Error while updating state")?;

            Ok(())
        }
    }
}
