use std::collections::HashMap;

use attaca::{Open, object::CommitRef, store::{self, prelude::*}};
use attaca_leveldb::LevelDbBackend;
use failure::Error;
use futures::prelude::*;
use url::Url;

use Repository;
use config::StoreKind;
use state::State;

/// Fetch objects from a remote repository.
#[derive(Debug, Clone, StructOpt, Builder)]
#[structopt(name = "fetch")]
pub struct FetchArgs {
    /// Name of the remote to fetch objects and branches from.
    #[structopt(name = "REMOTE")]
    remote: String,
}

pub struct FetchOut<'r> {
    pub blocking: Box<Future<Item = (), Error = Error> + 'r>,
}

// macro_rules! backend_fetch {
//     (@inner $remote:expr, $repo:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
//         match $remote.kind {
//             $(() if $type::SCHEMES.contains($url.scheme()) => {
//
//             })*
//         }
//     };
//     ($url:expr, $repo:expr) => {
//         all_backends!(backend_fetch!(@inner $url, $repo))
//     };
// }

impl<B: Backend> Repository<B> {
    pub fn fetch<'r>(&'r mut self, args: FetchArgs) -> FetchOut<'r> {
        let blocking = async_block! {
            let config = self.get_config()?;
            let secondary = {
                let remote = &config.remotes[&args.remote];

                match remote.kind {
                    StoreKind::LevelDb => self.do_fetch(
                        args.remote.clone(),
                        LevelDbBackend::open(remote.url.as_str())?,
                        ),
                    _ => unimplemented!(),
                }
            };
            await!(secondary)?;
            Ok(())
        };

        FetchOut {
            blocking: Box::new(blocking),
        }
    }

    fn do_fetch<'r, C: Backend>(
        &'r mut self,
        remote_name: String,
        remote_backend: C,
    ) -> impl Future<Item = (), Error = Error> + 'r {
        async_block! {
            let remote = Store::new(remote_backend);
            let branches = await!(remote.load_branches())?;

            let mut new_branches = HashMap::new();
            for (branch_name, commit_handle) in branches {
                let commit_ref = CommitRef::new(await!(store::copy(commit_handle, self.store.clone()))?);
                new_branches.insert(branch_name, commit_ref);
            }

            let mut state = self.get_state()?;
            state.remote_refs.insert(remote_name, new_branches);
            self.set_state(&state)?;

            Ok(())
        }
    }
}
