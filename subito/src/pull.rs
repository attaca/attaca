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
        let blocking = async_block! {
            let remote = {
                let state = self.get_state()?;
                let branch = match state.head {
                    Head::Empty | Head::Detached(_) => bail!("empty or detached head"),
                    Head::Branch(name) => name,
                };
                let upstream = state
                    .upstreams
                    .get(&branch)
                    .ok_or_else(|| format_err!("no upstream for branch {}", branch))?;
                upstream.remote.clone()
            };
            await!(plumbing::fetch::remote(self, remote))?;
            await!(plumbing::checkout::head(self))?;

            Ok(())
        };

        PullOut {
            blocking: Box::new(blocking),
        }
    }
}
