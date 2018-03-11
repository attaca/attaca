use super::*;

macro_rules! dispatch_fetch {
    (@inner $this:expr, $remote:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
        {
            match $remote.kind {
                $(StoreKind::$ccname => await!(backend($this, <$type>::open($remote.url.as_str())?))?,)*
            }
        }
    };
    ($this:expr, $remote:expr) => {
        all_backends!(dispatch_fetch!(@inner $this, $remote))
    };
}

pub fn remote<B: Backend>(this: &mut Repository<B>, remote_name: Name) -> FutureBranches<B> {
    let blocking = async_block! {
        let new_remote_branches = {
            let config = this.get_config()?;
            let remote = config.remotes[remote_name.as_str()].clone();
            dispatch_fetch!(this, remote)
        };

        let mut state = this.get_state()?;

        state
            .remote_branches
            .insert(remote_name.clone(), new_remote_branches.clone());

        this.set_state(&state)?;

        let tracking_branches = state
            .upstreams
            .into_iter()
            .filter(|&(_, ref remote_ref)| remote_ref.remote == remote_name)
            .map(|(branch, remote_ref)| (branch, new_remote_branches[&remote_ref.branch].clone()))
            .collect::<Vec<_>>();

        let branches = await!(load_branches(this))?;
        let mut new_branches = branches.clone();
        new_branches.extend(tracking_branches);
        await!(swap_branches(this, branches, new_branches))?;

        Ok(new_remote_branches)
    };

    Box::new(blocking)
}

pub fn backend<B: Backend, C: Backend>(
    this: &mut Repository<B>,
    remote_backend: C,
) -> FutureBranches<B> {
    let blocking = async_block! {
        let remote = Store::new(remote_backend);
        let branches = await!(remote.load_branches())?;

        let mut new_branches = HashMap::new();
        for (branch_name, commit_handle) in branches {
            let commit_ref = CommitRef::new(await!(store::copy(commit_handle, this.store.clone()))?);
            new_branches.insert(Name::from_string(branch_name)?, commit_ref);
        }

        Ok(new_branches)
    };

    Box::new(blocking)
}
