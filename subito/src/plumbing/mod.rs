mod checkout;

use std::collections::HashMap;

use attaca::{Open, object::CommitRef, store::{self, prelude::*}};
use failure::*;
use futures::prelude::*;

use Repository;
use config::StoreKind;
use state::Head;
use syntax::{LocalRef, Ref};

pub use self::checkout::*;

pub type Branches<B> = HashMap<String, CommitRef<Handle<B>>>;

pub type FutureBranches<'r, B> = Box<Future<Item = Branches<B>, Error = Error> + 'r>;
pub type FutureCommitRef<'r, B> = Box<Future<Item = CommitRef<Handle<B>>, Error = Error> + 'r>;
pub type FutureUnit<'r> = Box<Future<Item = (), Error = Error> + 'r>;

macro_rules! dispatch_fetch {
    (@inner $this:expr, $remote:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
        {
            match $remote.kind {
                $(StoreKind::$ccname => await!(Self::fetch_by_backend($this, <$type>::open($remote.url.as_str())?))?,)*
            }
        }
    };
    ($this:expr, $remote:expr) => {
        all_backends!(dispatch_fetch!(@inner $this, $remote))
    };
}

impl<B: Backend> Repository<B> {
    pub fn fetch_by_name<S: Into<String>>(this: &mut Self, remote_name: S) -> FutureBranches<B> {
        let remote = remote_name.into();
        let blocking = async_block! {
            let new_branches = {
                let config = this.get_config()?;
                let remote = config.remotes[&remote].clone();
                dispatch_fetch!(this, remote)
            };
            let mut state = this.get_state()?;
            state.remote_refs.insert(remote, new_branches.clone());
            this.set_state(&state)?;
            Ok(new_branches)
        };

        Box::new(blocking)
    }

    pub fn fetch_by_backend<C: Backend>(this: &mut Self, remote_backend: C) -> FutureBranches<B> {
        let blocking = async_block! {
            let remote = Store::new(remote_backend);
            let branches = await!(remote.load_branches())?;

            let mut new_branches = HashMap::new();
            for (branch_name, commit_handle) in branches {
                let commit_ref = CommitRef::new(await!(store::copy(commit_handle, this.store.clone()))?);
                new_branches.insert(branch_name, commit_ref);
            }

            Ok(new_branches)
        };

        Box::new(blocking)
    }

    // NB eventually get_state will end up async since it talks to the local store, which is why
    // this is async.
    pub fn load_remote_branches<S: Into<String>>(this: &Self, remote_name: S) -> FutureBranches<B> {
        let remote = remote_name.into();
        let blocking = async_block! {
            let mut state = this.get_state()?;
            let branches = state
                .remote_refs
                .remove(&remote)
                .ok_or_else(|| format_err!("no branches for remote"))?;
            Ok(branches)
        };

        Box::new(blocking)
    }

    pub fn load_branches(this: &Self) -> FutureBranches<B> {
        let blocking = async_block! {
            let handles = await!(this.store.load_branches())?;
            let branches = handles
                .into_iter()
                .map(|(name, handle)| (name, CommitRef::new(handle)))
                .collect();
            Ok(branches)
        };

        Box::new(blocking)
    }

    pub fn swap_branches(this: &mut Self, previous: Branches<B>, new: Branches<B>) -> FutureUnit {
        let blocking = async_block! {
            let previous_handles = previous
                .into_iter()
                .map(|(name, commit_ref)| (name, commit_ref.into_inner()))
                .collect();
            let new_handles = new.into_iter()
                .map(|(name, commit_ref)| (name, commit_ref.into_inner()))
                .collect();
            await!(this.store.swap_branches(previous_handles, new_handles))?;
            Ok(())
        };

        Box::new(blocking)
    }

    pub fn resolve(this: &Self, refr: Ref) -> FutureCommitRef<B> {
        match refr {
            Ref::Local(local_ref) => Self::resolve_local(this, local_ref),
            Ref::Remote(remote, local_ref) => Self::resolve_remote(this, remote, local_ref),
            Ref::Head => Self::resolve_head(this),
        }
    }

    pub fn resolve_local(this: &Self, local_ref: LocalRef) -> FutureCommitRef<B> {
        let blocking = async_block! {
            match local_ref {
                LocalRef::Branch(branch) => Ok(await!(Self::load_branches(this))?[&branch].clone()),
            }
        };

        Box::new(blocking)
    }

    pub fn resolve_remote<S: Into<String>>(
        this: &Self,
        remote_name: S,
        local_ref: LocalRef,
    ) -> FutureCommitRef<B> {
        let remote = remote_name.into();
        let blocking = async_block! {
            let branches = await!(Self::load_remote_branches(this, &*remote))?;

            match local_ref {
                LocalRef::Branch(branch) => Ok(branches
                    .get(&branch)
                    .ok_or_else(|| format_err!("no such branch {} on remote {}", branch, remote))?
                    .clone()),
            }
        };

        Box::new(blocking)
    }

    pub fn resolve_head(this: &Self) -> FutureCommitRef<B> {
        let blocking = async_block! {
            let state = this.get_state()?; // NB soon to be async
            match state.head {
                Head::Empty => bail!("empty head"),
                Head::Detached(commit_ref) => Ok(commit_ref),
                Head::Branch(branch) => await!(Self::resolve_local(this, LocalRef::Branch(branch))),
            }
        };

        Box::new(blocking)
    }

}
