use attaca::store::prelude::*;
use failure::*;
use futures::prelude::*;

use super::*;
use Repository;
use syntax::{Name, RemoteRef};

/// Modes for create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exists {
    /// If the branch already exists, overwrite it with the provided ref.
    Overwrite,

    /// If the branch already exists, do nothing.
    DoNothing,

    /// If the branch already exists, fail.
    Error,
}

/// Create a new branch using HEAD.
pub fn create<B: Backend>(
    this: &mut Repository<B>,
    exists: Exists,
    name: Name,
    commit_ref: CommitRef<Handle<B>>,
) -> FutureUnit {
    let blocking = async_block! {
        let branches = await!(this.store.load_branches())?;

        if let Exists::Error = exists {
            ensure!(
                !branches.contains_key(name.as_str()),
                "branch already exists"
            );
        }

        let mut new_branches = branches.clone();
        if exists != Exists::DoNothing || new_branches.contains_key(name.as_str()) {
            new_branches.insert((*name).to_owned(), commit_ref.into_inner());
        }
        await!(this.store.swap_branches(branches, new_branches))?;

        Ok(())
    };

    Box::new(blocking)
}

/// Delete a branch.
pub fn delete<B: Backend>(this: &mut Repository<B>, name: Name) -> FutureUnit {
    let blocking = async_block! {
        let branches = await!(this.store.load_branches())?;
        ensure!(branches.contains_key(name.as_str()), "no such branch {}", name);
        let mut new_branches = branches.clone();
        new_branches.remove(name.as_str());
        await!(this.store.swap_branches(branches, new_branches))?;

        let mut state = this.get_state()?;
        state.upstreams.remove(&name);
        this.set_state(&state)?;

        Ok(())
    };

    Box::new(blocking)
}

/// Set a branch upstream.
pub fn set_upstream<B: Backend>(
    this: &mut Repository<B>,
    name: Name,
    maybe_upstream: Option<RemoteRef>,
) -> FutureUnit {
    let blocking = async_block! {
        let mut state = this.get_state()?;
        match maybe_upstream {
            Some(upstream) => { state.upstreams.insert(name, upstream); }
            None => { state.upstreams.remove(&name); }
        }
        this.set_state(&state)?;

        Ok(())
    };

    Box::new(blocking)
}

#[derive(Debug, Clone, Copy)]
pub enum Type {
    Local,
    Remote,
}

/// Iterate over branches.
pub fn iterate<'r, B: Backend>(
    this: &'r mut Repository<B>,
    ty: Option<Type>,
) -> Box<Stream<Item = BranchRef, Error = Error> + 'r> {
    let blocking = async_stream_block! {
        match ty {
            Some(Type::Local) | None => {
                for branch in await!(load_branches(this))?.into_iter().map(|(name, _)| name) {
                    stream_yield!(BranchRef::Local(branch));
                }
            }
            _ => {}
        }

        match ty {
            Some(Type::Remote) | None => {
                let state = this.get_state()?;
                for (remote, branches) in state.remote_branches {
                    for (branch, _) in branches {
                        stream_yield!(BranchRef::Remote(RemoteRef::new(remote.clone(), branch)));
                    }
                }
            }
            _ => {}
        }

        Ok(())
    };

    Box::new(blocking)
}
