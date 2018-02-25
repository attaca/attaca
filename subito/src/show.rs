use std::{collections::BTreeMap, fmt};

use attaca::{HandleDigest, Store, digest::Digest, object::ObjectRef};
use failure::Error;
use futures::prelude::*;
use hex;

use Repository;
use quantified::{QuantifiedOutput, QuantifiedRef};

#[derive(Debug, Clone, StructOpt)]
pub enum ShowCommand {
    /// Display information about the supplied object assuming it is a small object.
    #[structopt(name = "small")]
    Small,

    /// Display information about the supplied object assuming it is a large object.
    #[structopt(name = "large")]
    Large,

    #[structopt(name = "tree")]
    Tree,

    #[structopt(name = "commit")]
    Commit,
}

/// Show information about specific objects in the repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "show")]
pub struct ShowArgs {
    /// The object to lookup.
    #[structopt(name = "OBJECT", required = true, parse(try_from_str = "hex::decode"))]
    pub object: Option<Vec<u8>>,

    /// Dump the binary contents of the associated handle instead of interpreting the object
    /// directly.
    #[structopt(long = "dump")]
    pub dump: bool,

    #[structopt(subcommand)]
    pub command: Option<ShowCommand>,
}

impl<'r> QuantifiedOutput<'r> for ShowArgs {
    type Output = ShowOut<'r>;
}

impl QuantifiedRef for ShowArgs {
    fn apply_ref<'r, S, D>(self, repository: &'r Repository<S, D>) -> Result<ShowOut<'r>, Error>
    where
        S: Store,
        D: Digest,
        S::Handle: HandleDigest<D>,
    {
        Ok(repository.show(self))
    }
}

pub enum Show {
    Small {
        size: usize,
    },
    Large {
        size: usize,
    },
    Tree {
        entries: BTreeMap<String, ObjectRef<Vec<u8>>>,
    },
    Commit,
    Dump(Vec<u8>),
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

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn show<'r>(&'r self, args: ShowArgs) -> ShowOut<'r> {
        let blocking = async_block! {
            let state = self.get_state()?;
            let subtree = await!(await!(state.head.unwrap().fetch())?.as_subtree().fetch())?;

            for (name, objref) in subtree {
                match objref {
                    ObjectRef::Small(small_ref) => println!("{} => Small {}", name, small_ref.size()),
                    ObjectRef::Large(large_ref) => println!("{} => Large {} {}", name, large_ref.depth(), large_ref.size()),
                    ObjectRef::Tree(tree_ref) => println!("{} => Tree", name),
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
