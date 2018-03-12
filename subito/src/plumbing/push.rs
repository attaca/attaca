use super::*;

use attaca::Open;

macro_rules! dispatch_push {
    (@inner $this:expr, $remote:expr, $branch:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
        {
            match $remote.kind {
                $(StoreKind::$ccname => await!(backend($this, <$type>::open($remote.url.as_str())?, $branch))?,)*
            }
        }
    };
    ($this:expr, $remote:expr, $branch:expr) => {
        all_backends!(dispatch_push!(@inner $this, $remote, $branch))
    };
}

pub fn upstream<B: Backend>(this: &Repository<B>, branch: Name) -> FutureUnit {
    let blocking = async_block! {
        let state = this.get_state()?;
        let upstream = state
            .upstreams
            .get(&branch)
            .ok_or_else(|| format_err!("no upstream for branch {}", branch))?
            .clone();
        let config = this.get_config()?;
        let remote = config
            .remotes
            .get(upstream.remote.as_str())
            .ok_or_else(|| format_err!("no such remote {}", upstream.remote))?
            .clone();
        dispatch_push!(this, remote, upstream.branch);

        Ok(())
    };

    Box::new(blocking)
}

pub fn backend<B: Backend, C: Backend>(
    this: &Repository<B>,
    remote_backend: C,
    branch: Name,
) -> FutureUnit {
    let blocking = async_block! {
        let local_branches = await!(this.store.load_branches())?;
        let local_commit_handle = local_branches
            .get(branch.as_str())
            .ok_or_else(|| format_err!("no such branch"))?
            .clone();

        let remote_store = Store::new(remote_backend);
        let remote_commit_handle = await!(store::copy(local_commit_handle, remote_store.clone()))?;

        let remote_branches = await!(remote_store.load_branches())?;
        let mut new_remote_branches = remote_branches.clone();
        new_remote_branches.insert(branch.into_string(), remote_commit_handle);
        await!(remote_store.swap_branches(remote_branches, new_remote_branches))?;

        Ok(())
    };

    Box::new(blocking)
}
