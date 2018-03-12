use std::path::PathBuf;

use attaca::store::prelude::*;
use failure::*;
use futures::prelude::*;
use url::Url;

use Repository;
use init::{InitArgs, InitStore};
use plumbing;
use state::Head;
use syntax::{Name, RemoteRef};

/// Create a local repository by cloning data from a remote repository. The default store type is `leveldb`.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "clone")]
pub struct CloneArgs {
    /// URL of the repository to clone.
    #[structopt(name = "URL", parse(try_from_str = "Url::parse"))]
    url: Url,

    /// Path to a directory to initialize as a repository. This defaults to the current directory.
    #[structopt(name = "PATH", parse(from_os_str))]
    path: Option<PathBuf>,

    #[structopt(subcommand)]
    store: Option<InitStore>,
}

pub struct CloneOut {
    pub blocking: Box<Future<Item = (), Error = Error>>,
}

#[macro_export]
macro_rules! clone {
    (@inner $args:expr, $repo:ident, $generic:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
        {
            let args: CloneArgs = $args;
            let init_args = InitArgs {
                path: args.path,
                store: args.store,
            };

            match () {
                $(_ if <$type as $crate::reexports::attaca::Open>::SCHEMES
                    .contains(args.url.scheme()) => {
                    init!(init_args, repository, {
                        let head;
                        let candidate;

                        let repository = {
                            use $crate::reexports::futures::prelude::*;
                            let store =
                                Store::new(<$type as $crate::reexports::attaca::Open>
                                    ::open(args.url.as_str())?);

                            let branches = store.load_branches().wait()?;
                            let local_master =
                                store::copy(branches["master"].clone(), repository.store.clone()).wait()?;
                            let head = CommitRef::new(local_master);
                            let candidate = head.fetch().wait()?.as_subtree().clone();
                        };

                        {
                            #[allow(unused_mut)]
                            let mut $repo = repository;
                            Ok({
                                #[warn(unused_mut)]
                                $generic
                            })
                        }
                    })
                })*
                _ => unreachable!(),
            }
        }
    };
    ($args:expr, $repo:ident, $generic:expr) => {
        all_backends!(clone!(@inner $args, $repo, $generic))
    };
}

pub fn clone(args: CloneArgs) -> CloneOut {
    let init_args = InitArgs {
        path: args.path,
        store: args.store,
    };
    let url = args.url;
    let blocking = init!(init_args, repository, clone_from(repository, url));

    CloneOut {
        blocking: Box::new(blocking.into_future().flatten()),
    }
}

fn clone_from<B: Backend>(
    mut this: Repository<B>,
    url: Url,
) -> Box<Future<Item = (), Error = Error>> {
    use plumbing::branch::Exists;

    let blocking = async_block! {
        let origin = "origin".parse::<Name>()?;
        let master = "master".parse::<Name>()?;
        // NB wait here because of issues w/ borrowing in generators.
        plumbing::remote::add(&mut this, origin.clone(), url).wait()?;
        plumbing::fetch::remote(&mut this, origin.clone()).wait()?;
        let commit_ref = plumbing::resolve_remote(&mut this, origin.clone(), master.clone()).wait()?;
        let tree_ref = commit_ref.fetch().wait()?.as_subtree().clone();
        plumbing::branch::create(&mut this, Exists::Error, master.clone(), commit_ref).wait()?;
        plumbing::branch::set_upstream(&mut this, master.clone(), Some(RemoteRef::new(origin.clone(), master.clone()))).wait()?;
        plumbing::checkout::tree(&mut this, tree_ref).wait()?;
        plumbing::set_head(&mut this, Head::Branch(master.clone())).wait()?;

        Ok(())
    };

    Box::new(blocking)
}
