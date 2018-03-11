pub mod branch;
pub mod checkout;
pub mod fetch;
pub mod push;
pub mod remote;

use std::collections::HashMap;

use attaca::{Open, object::CommitRef, store::{self, prelude::*}};
use failure::*;
use futures::prelude::*;

use Repository;
use config::StoreKind;
use state::{Head, State};
use syntax::{Name, Ref, BranchRef};

pub type Branches<B> = HashMap<Name, CommitRef<Handle<B>>>;

pub type FutureBranches<'r, B> = Box<Future<Item = Branches<B>, Error = Error> + 'r>;
pub type FutureCommitRef<'r, B> = Box<Future<Item = CommitRef<Handle<B>>, Error = Error> + 'r>;
pub type FutureOptionCommitRef<'r, B> =
    Box<Future<Item = Option<CommitRef<Handle<B>>>, Error = Error> + 'r>;
pub type FutureUnit<'r> = Box<Future<Item = (), Error = Error> + 'r>;

// NB eventually get_state will end up async since it talks to the local store, which is why
// this is async.
pub fn load_remote_branches<B: Backend>(
    this: &Repository<B>,
    remote_name: Name,
) -> FutureBranches<B> {
    let remote = remote_name.into();
    let blocking = async_block! {
        let mut state = this.get_state()?;
        let branches = state
            .remote_branches
            .remove(&remote)
            .ok_or_else(|| format_err!("no branches for remote"))?;
        Ok(branches)
    };

    Box::new(blocking)
}

pub fn load_branches<B: Backend>(this: &Repository<B>) -> FutureBranches<B> {
    let blocking = async_block! {
        let handles = await!(this.store.load_branches())?;
        let branches = handles
            .into_iter()
            .map(|(name, handle)| Ok((Name::from_string(name)?, CommitRef::new(handle))))
            .collect::<Result<_, Error>>()?;
        Ok(branches)
    };

    Box::new(blocking)
}

pub fn swap_branches<B: Backend>(
    this: &mut Repository<B>,
    previous: Branches<B>,
    new: Branches<B>,
) -> FutureUnit {
    let blocking = async_block! {
        let previous_handles = previous
            .into_iter()
            .map(|(name, commit_ref)| (name.into_string(), commit_ref.into_inner()))
            .collect();
        let new_handles = new.into_iter()
            .map(|(name, commit_ref)| (name.into_string(), commit_ref.into_inner()))
            .collect();
        await!(this.store.swap_branches(previous_handles, new_handles))?;
        Ok(())
    };

    Box::new(blocking)
}

pub fn resolve_opt<B: Backend>(this: &Repository<B>, refr: Ref) -> FutureOptionCommitRef<B> {
    match refr {
        Ref::Head => resolve_head_opt(this),
        Ref::Branch(BranchRef::Local(branch)) => resolve_local_opt(this, branch),
        Ref::Branch(BranchRef::Remote(remote_ref)) => resolve_remote_opt(this, remote_ref.remote, remote_ref.branch),
    }
}

pub fn resolve<B: Backend>(this: &Repository<B>, refr: Ref) -> FutureCommitRef<B> {
    match refr {
        Ref::Head => resolve_head(this),
        Ref::Branch(BranchRef::Local(branch)) => resolve_local(this, branch),
        Ref::Branch(BranchRef::Remote(remote_ref)) => resolve_remote(this, remote_ref.remote, remote_ref.branch),
    }
}

pub fn resolve_local_opt<B: Backend>(
    this: &Repository<B>,
    local_ref: Name,
) -> FutureOptionCommitRef<B> {
    let blocking = async_block! {
        Ok(await!(load_branches(this))?.get(&local_ref).cloned())
    };

    Box::new(blocking)
}

pub fn resolve_local<B: Backend>(this: &Repository<B>, local_ref: Name) -> FutureCommitRef<B> {
    Box::new(
        resolve_local_opt(this, local_ref.clone()).and_then(|maybe_ref| {
            maybe_ref.ok_or_else(move || format_err!("no such branch {}", local_ref))
        }),
    )
}

pub fn resolve_remote_opt<B: Backend>(
    this: &Repository<B>,
    remote_name: Name,
    branch_name: Name,
) -> FutureOptionCommitRef<B> {
    let blocking = async_block! {
        let branches = await!(load_remote_branches(this, remote_name))?;
        Ok(branches.get(&branch_name).cloned())
    };

    Box::new(blocking)
}

pub fn resolve_remote<B: Backend>(
    this: &Repository<B>,
    remote_name: Name,
    branch_name: Name,
) -> FutureCommitRef<B> {
    let blocking = async_block! {
        let branches = await!(load_remote_branches(this, remote_name.clone()))?;
        Ok(branches
            .get(&branch_name)
            .ok_or_else(|| format_err!("no such branch {} on remote {}", branch_name, remote_name.clone()))?
            .clone())
    };

    Box::new(blocking)
}

pub fn resolve_head_opt<B: Backend>(this: &Repository<B>) -> FutureOptionCommitRef<B> {
    let blocking = async_block! {
        let state = this.get_state()?; // NB soon to be async
        match state.head {
            Head::Empty => Ok(None),
            Head::Detached(commit_ref) => Ok(Some(commit_ref)),
            Head::Branch(branch) => await!(resolve_local_opt(this, branch)),
        }
    };

    Box::new(blocking)
}

pub fn resolve_head<B: Backend>(this: &Repository<B>) -> FutureCommitRef<B> {
    let blocking = async_block! {
        let state = this.get_state()?; // NB soon to be async
        match state.head {
            Head::Empty => bail!("empty head"),
            Head::Detached(commit_ref) => Ok(commit_ref),
            Head::Branch(branch) => await!(resolve_local(this, branch)),
        }
    };

    Box::new(blocking)
}

pub fn set_head<B: Backend>(this: &mut Repository<B>, head: Head<Handle<B>>) -> FutureUnit {
    let blocking = async_block! {
        let state = this.get_state()?;
        this.set_state(&State { head, ..state })?;
        Ok(())
    };

    Box::new(blocking)
}
