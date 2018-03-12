use attaca::store::prelude::*;
use failure::Error;
use futures::prelude::*;

use Repository;
use plumbing;
use state::Head;

/// Push objects to a remote repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "push")]
pub struct PushArgs {}

pub struct PushOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<B: Backend> Repository<B> {
    pub fn push<'r>(&'r self, _args: PushArgs) -> PushOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            let name = match state.head {
                Head::Empty | Head::Detached(_) => bail!("head does not reference a branch"),
                Head::Branch(name) => name,
            };

            await!(plumbing::push::upstream(self, name))?;

            Ok(())
        };

        PushOut {
            blocking: Box::new(blocking),
        }
    }
}
