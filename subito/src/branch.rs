use attaca::store::prelude::*;
use failure::Error;
use futures::prelude::*;

use Repository;
use state::Head;
use plumbing::{self, branch};
use syntax::{BranchRef, Name, RemoteRef};

#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "branch")]
pub struct BranchArgs {
    #[structopt(name = "BRANCH")]
    name: Option<Name>,

    #[structopt(long = "set-upstream")]
    set_upstream: Option<RemoteRef>,

    #[structopt(long = "unset-upstream", conflicts_with = "set-upstream")]
    unset_upstream: bool,

    #[structopt(long = "remotes", short = "r", conflicts_with = "set-upstream",
                conflicts_with = "unset-upstream")]
    remotes: bool,
}

pub struct BranchOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<B: Backend> Repository<B> {
    pub fn branch<'r>(&'r mut self, args: BranchArgs) -> BranchOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            let maybe_head_branch = match state.head {
                Head::Branch(name) => Some(name),
                _ => None,
            };

            if args.name.is_none() && args.set_upstream.is_none() && !args.unset_upstream {
                #[async]
                for branch_ref in plumbing::branch::iterate(self, Some(branch::Type::Local)) {
                    match branch_ref {
                        BranchRef::Local(ref name) if Some(name) == maybe_head_branch.as_ref() => {
                            print!("* ")
                        }
                        _ => print!("  "),
                    }

                    print!("{}", branch_ref);

                    if let BranchRef::Local(ref name) = branch_ref {
                        if let Some(upstream) = state.upstreams.get(name) {
                            print!(" tracking {}", upstream);
                        }
                    }

                    println!("");
                }

                Ok(())
            } else {
                let name = args.name.or(maybe_head_branch).ok_or_else(|| {
                    format_err!("no branch specified and HEAD does not point to a branch")
                })?;

                let state = self.get_state()?;
                let maybe_commit_ref = match state.head {
                    Head::Empty => None,
                    Head::Detached(commit_ref) => Some(commit_ref),
                    Head::Branch(branch) => await!(plumbing::resolve_local_opt(self, branch))?,
                };
                let commit_ref = maybe_commit_ref.ok_or_else(|| format_err!("no prior commits"))?;

                if args.unset_upstream || args.set_upstream.is_some() {
                    await!(plumbing::branch::create(
                        self,
                        branch::Exists::DoNothing,
                        name.clone(),
                        commit_ref,
                    ))?;

                    await!(plumbing::branch::set_upstream(
                        self,
                        name.clone(),
                        args.set_upstream,
                    ))?;
                } else {
                    await!(plumbing::branch::create(
                        self,
                        branch::Exists::Error,
                        name.clone(),
                        commit_ref,
                    ))?;
                }

                Ok(())
            }
        };

        BranchOut {
            blocking: Box::new(blocking),
        }
    }

    //     pub fn branch_list<'r>(&'r mut self, args: BranchListArgs) -> BranchOut<'r> {
    //         let blocking = async_block! {
    //             let state = self.get_state()?;
    //             match args.remote {
    //                 Some(remote) => {
    //                     ensure!(state.remote_branches.contains_key(&remote), "no such remote");
    //
    //                     for (branch_name, _) in &state.remote_branches[&remote] {
    //                         println!("   {}", branch_name);
    //                     }
    //                 }
    //                 None => {
    //                     let maybe_branch = match state.head {
    //                         Head::Empty => None,
    //                         Head::Detached(_) => None,
    //                         Head::Branch(branch) => Some(branch),
    //                     };
    //                     let branches = await!(self.store.load_branches())?;
    //                     // TODO better and more flexible output instead of just printlning everything
    //                     for (branch_name, _) in branches {
    //                         match maybe_branch {
    //                             Some(ref name) if name.as_str() == &branch_name => {
    //                                 println!("=> {}", branch_name)
    //                             }
    //                             _ => println!("   {}", branch_name),
    //                         }
    //                     }
    //                 }
    //             }
    //             Ok(())
    //         };
    //
    //         BranchOut {
    //             blocking: Box::new(blocking),
    //         }
    //     }
}
