use std::{fmt, borrow::Borrow, ffi::OsStr, fs::File, path::PathBuf};

use attaca::{batch::{Batch as ObjectBatch, Operation as ObjectOperation}, digest::prelude::*,
             hierarchy::Hierarchy,
             object::{self, CommitAuthor, CommitBuilder, CommitRef, ObjectRef, TreeBuilder},
             path::ObjectPath, store::prelude::*};
use failure::{self, *};
use futures::{stream, future::Either, prelude::*};
use ignore::WalkBuilder;

use {Repository, State};
use cache::{Cache, Certainty, Status};
use state::Head;

/// Save the virtual workspace as a child commit of the previous commit.
#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "commit")]
pub struct CommitArgs {
    /// Add a commit message.
    #[structopt(short = "m", long = "m")]
    pub message: Option<String>,

    /// Add a commit author.
    #[structopt(long = "author")]
    pub author: Option<String>,

    /// Instead of making a new commit, load the previous commit and update it.
    #[structopt(long = "amend")]
    pub amend: bool,

    /// Force a commit regardless of warnings.
    #[structopt(long = "force")]
    pub force: bool,
}

#[must_use = "CommitOut contains futures which must be driven to completion!"]
pub struct CommitOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<'r> fmt::Debug for CommitOut<'r> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CommitOut")
            .field("blocking", &"OPAQUE")
            .finish()
    }
}

/// Load files into the virtual workspace.
#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "stage")]
pub struct StageArgs {
    /// Paths of files to load.
    #[structopt(name = "PATH", parse(from_os_str), raw(required = r#"true"#))]
    pub paths: Vec<PathBuf>,

    /// Load files from the previous commit into the virtual workspace instead.
    #[structopt(short = "p", long = "previous")]
    pub previous: bool,

    /// Do not track progress.
    #[structopt(short = "q", long = "quiet")]
    pub quiet: bool,
}

#[must_use = "StageOut contains futures which must be driven to completion!"]
pub struct StageOut<'r> {
    pub progress: Box<Stream<Item = (), Error = Error> + 'r>,
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<'r> fmt::Debug for StageOut<'r> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StageOut")
            .field("files", &"OPAQUE")
            .field("blocking", &"OPAQUE")
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
    path: PathBuf,
    op: OpKind,
}

// NOTE stage and unstage iterate and then collect in order to normalize the path.
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

impl<B: Backend> Repository<B> {
    pub fn commit<'r>(&'r mut self, args: CommitArgs) -> CommitOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            let branches = await!(self.store.load_branches())?;

            let candidate = state.candidate.clone().ok_or_else(|| {
                format_err!(
                    "No virtual workspace to commit. \
                     Add some files to the virtual workspace first!"
                )
            })?;

            let maybe_head_ref = match state.head {
                Head::Empty => None,
                Head::Detached(ref commit_ref) => Some(commit_ref.clone()),
                Head::Branch(ref branch) => branches.get(branch.as_str()).cloned().map(CommitRef::new),
            };
            let maybe_head = await!(maybe_head_ref.as_ref().map(CommitRef::fetch))?;

            if let Some(ref head_commit) = maybe_head {
                ensure!(
                    head_commit.as_subtree() != &candidate || args.force,
                    "Previous commit is identical to virtual workspace! \
                     No changes will be committed - use --force to override."
                );
            }

            let mut commit_builder = if args.amend {
                match maybe_head {
                    Some(head_commit) => head_commit.diverge(),
                    None => bail!("No previous commit to amend!"),
                }
            } else {
                let mut builder = CommitBuilder::new();
                builder.parents(maybe_head_ref.into_iter());
                builder
            };

            commit_builder.subtree(candidate);

            if let Some(message) = args.message {
                commit_builder.message(message.to_string());
            }

            if let Some(author) = args.author {
                commit_builder.author(CommitAuthor {
                    name: Some(author.to_string()),
                    mbox: None,
                });
            }

            let commit_ref = await!(commit_builder.into_commit()?.send(&self.store))?;

            match state.head {
                Head::Empty | Head::Detached(_) => {
                    self.set_state(&State {
                        head: Head::Detached(commit_ref),
                        ..state
                    })?;
                }
                Head::Branch(branch) => {
                    let mut new_branches = branches.clone();
                    new_branches.insert(branch.into_string(), commit_ref.into_inner());
                    await!(self.store.swap_branches(branches, new_branches))?;
                }
            }

            Ok(())
        };

        CommitOut {
            blocking: Box::new(blocking),
        }
    }

    pub fn stage<'r>(&'r mut self, args: StageArgs) -> StageOut<'r> {
        let op = if args.previous {
            OpKind::Unstage
        } else {
            OpKind::Stage
        };
        let batch = args.paths.into_iter().map(move |path| BatchOp { path, op });
        let progress = stream::empty();
        let blocking = async_block! {
            await!(self.stage_batch(batch))?;
            Ok(())
        };

        StageOut {
            progress: Box::new(progress),
            blocking: Box::new(blocking),
        }
    }

    #[async]
    fn do_process_file(
        store: Store<B>,
        cache: Cache<B>,
        absolute_path: PathBuf,
        object_path: ObjectPath,
    ) -> Result<ObjectRef<Handle<B>>, Error> {
        let status = cache
            .status(&object_path)
            .context("Error during cache lookup for file")?;

        let pre_resolution = match status {
            Status::Extant(Certainty::Positive, ref snapshot) => {
                let resolution = snapshot.as_object_ref().map(|odr| odr.resolve_id(&store));
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
                let id = await!(objref.id()).context("Error fetching object digest")?;
                cache
                    .resolve(snapshot, id)
                    .context("Error during cache resolution for file")?;

                Ok(objref)
            }
            Status::Removed | Status::Extinct => bail!("File removed during processing!"),
        }
    }

    #[async]
    fn do_process(
        store: Store<B>,
        cache: Cache<B>,
        absolute_path: PathBuf,
        object_path: ObjectPath,
    ) -> Result<Option<ObjectRef<Handle<B>>>, Error> {
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
            let mut object_batch = ObjectBatch::<B>::new();
            // TODO #33
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
    ) -> impl Future<Item = Option<ObjectRef<Handle<B>>>, Error = Error> {
        Self::do_process(
            self.store.clone(),
            self.cache.clone(),
            absolute_path,
            object_path,
        )
    }

    fn do_process_operation<'r>(
        &'r self,
        hierarchy: Hierarchy<B>,
        batch_op: BatchOp,
    ) -> Result<impl Future<Item = ObjectOperation<B>, Error = Error>, Error> {
        let BatchOp { path: raw_path, op } = batch_op;

        ensure!(
            !raw_path
                .iter()
                .any(|component| [OsStr::new("."), OsStr::new("..")].contains(&component)),
            "TODO #35: Better path parsing: currently normalizing `.` and `..` is not supported."
        );

        let paths_res = if raw_path.is_absolute() {
            raw_path
                .strip_prefix(&*self.path)
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

        let future = async_block! {
            let (object_path, objref_opt) = await!(future_res?)?;
            let operation = match objref_opt {
                Some(objref) => ObjectOperation::Add(object_path, objref),
                None => ObjectOperation::Delete(object_path),
            };
            Ok(operation)
        };

        Ok(future)
    }

    fn process_operation<'r>(
        &'r self,
        hierarchy: Hierarchy<B>,
        batch_op: BatchOp,
    ) -> impl Future<Item = ObjectOperation<B>, Error = Error> {
        self.do_process_operation(hierarchy, batch_op)
            .into_future()
            .flatten()
    }

    pub fn stage_batch<'r, I>(&'r mut self, batch: I) -> impl Future<Item = (), Error = Error> + 'r
    where
        I: IntoIterator<Item = BatchOp> + 'r,
    {
        async_block! {
            let state = self.get_state()?;
            let branches = await!(self.store.load_branches())?;
            let maybe_head_ref = match state.head {
                Head::Empty => None,
                Head::Detached(ref commit_ref) => Some(commit_ref.clone()),
                Head::Branch(ref branch) => branches.get(branch.as_str()).cloned().map(CommitRef::new),
            };
            let hierarchy = match maybe_head_ref {
                Some(head_ref) => Hierarchy::from(
                    await!(head_ref.fetch())
                        .context("Error while fetching head")?
                        .as_subtree()
                        .clone(),
                ),
                None => Hierarchy::new(),
            };
            let queue = stream::futures_ordered(
                batch
                    .into_iter()
                    .map(|batch_op| self.process_operation(hierarchy.clone(), batch_op)),
            );
            let batch: ObjectBatch<B> = await!(
                queue.fold(ObjectBatch::new(), |batch, op| batch.add(op))
            ).context("Error while batching stage operations")?;
            await!(self.stage_objects(batch)).context("Error while staging objects")?;

            Ok(())
        }
    }

    pub fn stage_objects<'r>(
        &'r mut self,
        batch: ObjectBatch<B>,
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
            let candidate = if new_candidate_built.is_empty() && state.head.is_empty() {
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
