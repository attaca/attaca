use attaca::store::prelude::*;
use failure::Error;
use futures::prelude::*;

use Repository;
use plumbing;
use state::Head;

/// Pull changes from a remote repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "pull")]
pub struct PullArgs {}

pub struct PullOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<B: Backend> Repository<B> {
    pub fn pull<'r>(&'r mut self, _args: PullArgs) -> PullOut<'r> {
        use plumbing::branch::Exists;

        let blocking = async_block! {
            let (branch, remote_ref) = {
                let state = self.get_state()?;
                let branch = match state.head {
                    Head::Empty | Head::Detached(_) => bail!("empty or detached head"),
                    Head::Branch(name) => name,
                };
                let remote_ref = state
                    .upstreams
                    .get(&branch)
                    .ok_or_else(|| format_err!("no upstream for branch {}", branch))?
                    .clone();
                (branch, remote_ref)
            };
            await!(plumbing::fetch::remote(self, remote_ref.remote.clone()))?;
            let commit_ref = await!(plumbing::resolve_remote(self, remote_ref.remote.clone(), remote_ref.branch.clone()))?;
            let tree_ref = await!(commit_ref.fetch())?.as_subtree().clone();
            await!(plumbing::branch::create(self, Exists::Overwrite, branch, commit_ref))?;
            await!(plumbing::checkout::tree(self, tree_ref))?;

            Ok(())
        };

        PullOut {
            blocking: Box::new(blocking),
        }
    }
}
