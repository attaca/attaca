use std::{fmt, path::PathBuf};

use attaca::{path::ObjectPath, store::prelude::*};
use failure::*;
use futures::prelude::*;

use Repository;
use state::Head;
use syntax::{BranchRef, Ref};
use plumbing;

/// Copy files from the repository into the local workspace.
#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "checkout")]
pub struct CheckoutArgs {
    /// The ref to checkout from.
    #[structopt(name = "REF", default_value = "HEAD")]
    pub refr: Ref,

    /// Paths files to checkout. If left empty, the whole tree is checked out.
    #[structopt(name = "PATHS", last = true, parse(from_os_str))]
    pub paths: Vec<PathBuf>,
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

impl<B: Backend> Repository<B> {
    pub fn checkout<'r>(&'r mut self, args: CheckoutArgs) -> CheckoutOut<'r> {
        let blocking = async_block! {
            if args.paths.is_empty() {
                // If there are no paths specified, we checkout an entire branch and update the
                // HEAD (unless the HEAD is being checked out. Because that's silly.)
                match args.refr {
                    Ref::Head => await!(plumbing::checkout::head(self))?,
                    Ref::Branch(BranchRef::Local(name)) => {
                        let maybe_commit_ref =
                            await!(plumbing::resolve_local_opt(self, name.clone()))?;
                        let commit_ref = match maybe_commit_ref {
                            Some(commit_ref) => commit_ref,
                            None => {
                                let state = self.get_state()?;
                                let remote_ref = state
                                    .upstreams
                                    .get(&name)
                                    .ok_or_else(|| {
                                        format_err!("no such branch or matching upstream")
                                    })?
                                    .clone();

                                await!(plumbing::resolve_remote(
                                    self,
                                    remote_ref.remote,
                                    remote_ref.branch
                                ))?
                            }
                        };

                        let tree_ref = await!(commit_ref.fetch())?.as_subtree().clone();
                        await!(plumbing::checkout::tree(self, tree_ref))?;
                        await!(plumbing::set_head(self, Head::Branch(name)))?;
                    }
                    Ref::Branch(BranchRef::Remote(remote_ref)) => {
                        let commit_ref = await!(plumbing::resolve_remote(
                            self,
                            remote_ref.remote,
                            remote_ref.branch
                        ))?;
                        let tree_ref = await!(commit_ref.fetch())?.as_subtree().clone();
                        await!(plumbing::checkout::tree(self, tree_ref))?;
                    }
                }
            } else {
                // If there are paths specified, we do not update HEAD.
                let paths = args.paths
                    .into_iter()
                    .map(ObjectPath::from_path)
                    .collect::<Result<Vec<_>, _>>()?;

                let commit_ref = await!(plumbing::resolve(self, args.refr))?;
                let tree_ref = await!(commit_ref.fetch())?.as_subtree().clone();

                await!(plumbing::checkout::subpaths_from_tree(
                    self,
                    tree_ref,
                    ObjectPath::new(),
                    paths,
                ))?;
            }

            Ok(())
        };

        CheckoutOut {
            blocking: Box::new(blocking),
        }
    }
}
