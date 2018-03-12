use std::fmt;

use attaca::{object::ObjectRef, store::prelude::*};
use failure::Error;
use futures::prelude::*;

use Repository;
use plumbing;
use syntax::Ref;

/// Show information about specific objects in the repository. TODO: currently a stub
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "show")]
pub struct ShowArgs {
    #[structopt(name = "REF", default_value = "HEAD")]
    refr: Ref,
}

#[must_use = "ShowOut contains futures which must be driven to completion!"]
pub struct ShowOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

impl<'r> fmt::Debug for ShowOut<'r> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ShowOut")
            .field("staged", &"OPAQUE")
            .finish()
    }
}

impl<B: Backend> Repository<B> {
    pub fn show<'r>(&'r self, args: ShowArgs) -> ShowOut<'r> {
        let blocking = async_block! {
            let resolved_ref = await!(plumbing::resolve(self, args.refr))?;
            let subtree = await!(await!(resolved_ref.fetch())?.as_subtree().fetch())?;

            for (name, objref) in subtree {
                match objref {
                    ObjectRef::Small(small_ref) => println!("{} => Small {}", name, small_ref.size()),
                    ObjectRef::Large(large_ref) => println!("{} => Large {} {}", name, large_ref.depth(), large_ref.size()),
                    ObjectRef::Tree(_) => println!("{} => Tree", name),
                    _ => unreachable!(),
                }
            }

            Ok(())
        };

        ShowOut {
            blocking: Box::new(blocking),
        }
    }
}
