use std::{collections::HashMap, io::{BufRead, Write}};

use attaca::{digest::prelude::*, object::{CommitRef, TreeRef}, store::prelude::*};
use capnp::{message, serialize_packed};
use failure::*;
use futures::{stream, prelude::*};

use plumbing::Branches;
use syntax::Name;

#[derive(Debug, Clone)]
pub enum Head<H> {
    Empty,
    Detached(CommitRef<H>),
    Branch(Name),
}

impl<H> Default for Head<H> {
    fn default() -> Self {
        Head::Empty
    }
}

impl<H> Head<H> {
    pub fn is_empty(&self) -> bool {
        match *self {
            Head::Empty => true,
            _ => false,
        }
    }

    fn resolve_id<B: Backend>(
        &self,
        store: &Store<B>,
    ) -> Box<Future<Item = Option<Head<Handle<B>>>, Error = Error>>
    where
        H: ::std::borrow::Borrow<B::Id>,
    {
        match *self {
            Head::Empty => Box::new(Ok(Some(Head::Empty)).into_future()),
            Head::Detached(ref commit_id) => Box::new(
                commit_id
                    .resolve_id(store)
                    .map(|opt| opt.map(Head::Detached)),
            ),
            Head::Branch(ref branch) => {
                Box::new(Ok(Some(Head::Branch(branch.clone()))).into_future())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct State<H> {
    pub candidate: Option<TreeRef<H>>,
    pub head: Head<H>,
    pub remote_refs: HashMap<Name, HashMap<Name, CommitRef<H>>>,
}

impl<H> Default for State<H> {
    fn default() -> Self {
        Self {
            candidate: None,
            head: Head::Empty,
            remote_refs: HashMap::new(),
        }
    }
}

#[async]
fn resolve_refs<B: Backend>(
    raw: Vec<(Name, Vec<(Name, OwnedLocalId<B>)>)>,
    store: Store<B>,
) -> Result<HashMap<Name, Branches<B>>, Error> {
    let mut remote_refs = HashMap::new();
    for (remote_name, branch_ids) in raw {
        let mut branches = HashMap::new();
        for (branch_name, commit_id) in branch_ids {
            let commit_handle =
                await!(store.resolve_id(&commit_id))?.ok_or_else(|| format_err!("Missing ref!"))?;
            let commit_ref = CommitRef::new(commit_handle);
            branches.insert(branch_name, commit_ref);
        }
        remote_refs.insert(remote_name, branches);
    }

    Ok(remote_refs)
}

impl<B: Backend> State<Handle<B>> {
    pub fn decode<R>(mut reader: R, store: Store<B>) -> impl Future<Item = Self, Error = Error>
    where
        R: BufRead,
    {
        use state_capnp::state::{self, candidate, head};

        async_block! {
            let future_head;
            let future_candidate;
            let future_remote_refs;

            {
                let message_reader =
                    serialize_packed::read_message(&mut reader, message::ReaderOptions::new())?;
                let state = message_reader.get_root::<state::Reader>()?;

                let head_id = match state.get_head().which()? {
                    head::Empty(()) => Head::Empty,
                    head::Detached(bytes_res) => {
                        Head::Detached(CommitRef::new(LocalId::<B>::from_bytes(bytes_res?)))
                    }
                    head::Branch(name) => Head::Branch(name?.parse()?),
                };

                let candidate_id = match state.get_candidate().which()? {
                    candidate::Some(bytes_res) => {
                        Some(TreeRef::new(LocalId::<B>::from_bytes(bytes_res?)))
                    }
                    candidate::None(()) => None,
                };

                let remote_refs = state
                    .get_remote_refs()?
                    .iter()
                    .map(|remote| {
                        let branches = remote
                            .get_branches()?
                            .iter()
                            .map(|branch| {
                                let name = branch.get_name()?.parse()?;
                                let commit_id = LocalId::<B>::from_bytes(branch.get_commit_id()?);
                                Ok((name, commit_id))
                            })
                            .collect::<Result<Vec<_>, Error>>()?;

                        Ok((remote.get_name()?.parse()?, branches))
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                future_head = head_id
                    .resolve_id(&store)
                    .and_then(|rs| rs.ok_or_else(|| format_err!("Head does not exist!")));
                future_candidate = candidate_id.map(|id| {
                    id.resolve_id(&store)
                        .and_then(|rs| rs.ok_or_else(|| format_err!("Candidate does not exist!")))
                });
                future_remote_refs = resolve_refs(remote_refs, store.clone());
            }

            let (candidate, head, remote_refs) =
                await!(future_candidate.join3(future_head, future_remote_refs))?;

            Ok(State {
                candidate,
                head,
                remote_refs,
            })
        }
    }

    pub fn encode<'b, W: Write + 'b>(
        &self,
        mut buf: W,
    ) -> impl Future<Item = (), Error = Error> + 'b {
        use state_capnp::state;

        let state = self.clone();
        async_block! {
            let head = match state.head {
                Head::Empty => Head::Empty,
                Head::Detached(commit_ref) => Head::Detached(await!(commit_ref.id())?),
                Head::Branch(branch) => Head::Branch(branch),
            };

            let candidate = match state.candidate {
                Some(tree_ref) => Some(await!(tree_ref.id())?.into_inner()),
                None => None,
            };

            let mut remote_refs = HashMap::new();
            for (remote_name, remote_branches) in state.remote_refs {
                let mut branches = HashMap::new();
                for (branch_name, commit_ref) in remote_branches {
                    let commit_id = await!(commit_ref.id())?.into_inner();
                    branches.insert(branch_name, commit_id);
                }
                remote_refs.insert(remote_name, branches);
            }

            let mut message = message::Builder::new_default();

            {
                let mut state_builder = message.init_root::<state::Builder>();
                {
                    let mut candidate_builder = state_builder.borrow().get_candidate();
                    {
                        use std::borrow::Borrow;
                        match candidate {
                            Some(id) => candidate_builder.set_some(id.borrow().as_bytes()),
                            None => candidate_builder.set_none(()),
                        }
                    }
                }
                {
                    let mut head_builder = state_builder.borrow().get_head();
                    {
                        use std::borrow::Borrow;
                        match head {
                            Head::Empty => head_builder.set_empty(()),
                            Head::Detached(id) => head_builder.set_detached(id.as_inner().borrow().as_bytes()),
                            Head::Branch(branch) => head_builder.set_branch(&*branch),
                        }
                    }
                }
                {
                    let mut remote_refs_builder =
                        state_builder.init_remote_refs(remote_refs.len() as u32);
                    for (i, (remote_name, branches)) in remote_refs.into_iter().enumerate() {
                        let mut remote_builder = remote_refs_builder.borrow().get(i as u32);
                        remote_builder.set_name(&remote_name);
                        let mut remote_branches_builder = remote_builder.init_branches(branches.len() as u32);
                        for (i, (branch_name, commit_id)) in branches.into_iter().enumerate() {
                            let mut branch_builder = remote_branches_builder.borrow().get(i as u32);
                            branch_builder.set_name(&branch_name);
                            branch_builder.set_commit_id({ use std::borrow::Borrow; commit_id.borrow().as_bytes() });
                        }
                    }
                }
            }

            serialize_packed::write_message(&mut buf, &message)?;

            Ok(())
        }
    }
}
