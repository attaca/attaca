use std::{fmt, usize, collections::{BTreeSet, HashMap}, fs::{self, OpenOptions}, path::PathBuf,
          sync::Arc};

use attaca::{digest::prelude::*, hierarchy::Hierarchy,
             object::{CommitRef, Object, ObjectRef, TreeRef}, path::ObjectPath, store::prelude::*};
use failure::*;
use futures::{stream, prelude::*};
use ignore::WalkBuilder;
use memmap::MmapMut;

use Repository;
use cache::{Cache, Certainty, Status};
use state::Head;
use syntax::Ref;

const LARGE_CHILD_LOOKAHEAD_BUFFER_SIZE: usize = 32;

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
            // Checkout the previous subtree in its entirety if there are no paths specified.
            let paths = if args.paths.is_empty() {
                vec![ObjectPath::new()]
            } else {
                args.paths
                    .into_iter()
                    .map(ObjectPath::from_path)
                    .collect::<Result<_, _>>()?
            };

            let commit_ref = await!(Self::resolve(self, args.refr))?;
            let tree_ref = await!(commit_ref.fetch())?.as_subtree().clone();

            await!(Self::checkout_paths_from_tree(
                self,
                tree_ref,
                ObjectPath::new(),
                paths,
            ))?;

            Ok(())
        };

        CheckoutOut {
            blocking: Box::new(blocking),
        }
    }
}
