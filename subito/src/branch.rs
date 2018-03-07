use std::collections::HashMap;

use attaca::{Open, object::CommitRef, store::{self, prelude::*}};
use failure::Error;
use futures::prelude::*;
use url::Url;

use Repository;
use config::{StoreConfig, StoreKind};
use state::{Head, State};
use syntax::Name;

#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "branch")]
pub struct BranchArgs {
    #[structopt(subcommand)]
    cmd: Option<BranchCommand>,
}

#[derive(Debug, Clone, StructOpt)]
pub enum BranchCommand {
    #[structopt(name = "create")]
    Create(BranchCreateArgs),

    #[structopt(name = "list")]
    List(BranchListArgs),
}

impl Default for BranchCommand {
    fn default() -> Self {
        BranchCommand::List(BranchListArgs::default())
    }
}

#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "create")]
pub struct BranchCreateArgs {
    #[structopt(name = "BRANCH")]
    name: Name,
}

#[derive(Debug, Clone, Default, StructOpt, Builder)]
#[structopt(name = "list")]
pub struct BranchListArgs {
    #[structopt(long = "remote", short = "r")]
    remote: Option<Name>,
}

pub struct BranchOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<B: Backend> Repository<B> {
    pub fn branch<'r>(&'r mut self, args: BranchArgs) -> BranchOut<'r> {
        match args.cmd.unwrap_or_default() {
            BranchCommand::Create(create_args) => self.branch_create(create_args),
            BranchCommand::List(list_args) => self.branch_list(list_args),
        }
    }

    pub fn branch_create<'r>(&'r mut self, args: BranchCreateArgs) -> BranchOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            let branches = await!(self.store.load_branches())?;
            ensure!(!branches.contains_key(args.name.as_str()), "branch already exists");
            let commit_ref = match state.head {
                Head::Empty => bail!("no prior commits"),
                Head::Detached(commit_ref) => commit_ref.into_inner(),
                Head::Branch(branch) => branches[branch.as_str()].clone(),
            };
            let mut new_branches = branches.clone();
            new_branches.insert(args.name.into_string(), commit_ref);
            await!(self.store.swap_branches(branches, new_branches))?;
            Ok(())
        };

        BranchOut {
            blocking: Box::new(blocking),
        }
    }

    pub fn branch_list<'r>(&'r mut self, args: BranchListArgs) -> BranchOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            match args.remote {
                Some(remote) => {
                    ensure!(state.remote_refs.contains_key(&remote), "no such remote");

                    for (branch_name, _) in &state.remote_refs[&remote] {
                        println!("   {}", branch_name);
                    }
                }
                None => {
                    let maybe_branch = match state.head {
                        Head::Empty => None,
                        Head::Detached(_) => None,
                        Head::Branch(branch) => Some(branch),
                    };
                    let branches = await!(self.store.load_branches())?;
                    // TODO better and more flexible output instead of just printlning everything
                    for (branch_name, _) in branches {
                        match maybe_branch {
                            Some(ref name) if name.as_str() == &branch_name => {
                                println!("=> {}", branch_name)
                            }
                            _ => println!("   {}", branch_name),
                        }
                    }
                }
            }
            Ok(())
        };

        BranchOut {
            blocking: Box::new(blocking),
        }
    }
}
