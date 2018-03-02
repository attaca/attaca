use std::io::{BufRead, Write};

use attaca::{Handle, HandleDigest, Store, digest::Digest, object::{CommitRef, TreeRef}};
use capnp::{message, serialize_packed};
use failure::*;
use futures::prelude::*;

#[derive(Debug, Clone)]
pub struct State<H: Handle> {
    pub candidate: Option<TreeRef<H>>,
    pub head: Option<CommitRef<H>>,
    pub active_branch: Option<String>,
}

impl<H: Handle> Default for State<H> {
    fn default() -> Self {
        Self {
            candidate: None,
            head: None,
            active_branch: None,
        }
    }
}

impl<H: Handle> State<H> {
    pub fn decode<R, S, D>(mut reader: R, store: S) -> impl Future<Item = Self, Error = Error>
    where
        R: BufRead,
        S: Store<Handle = H>,
        D: Digest,
        H: HandleDigest<D>,
    {
        use state_capnp::state::{self, active_branch, candidate, head};

        async_block! {
            let future_head;
            let future_candidate;
            let active_branch;

            {
                let message_reader =
                    serialize_packed::read_message(&mut reader, message::ReaderOptions::new())?;
                let state = message_reader.get_root::<state::Reader>()?;

                let digest = state.get_digest()?;
                let name = digest.get_name()?;
                let size = digest.get_size() as usize;

                ensure!(
                    name == D::NAME && size == D::SIZE,
                    "Digest mismatch: expected {}/{}, got {}/{}",
                    D::NAME,
                    D::SIZE,
                    name,
                    size
                    );

                let head_digest = match state.get_head().which()? {
                    head::Some(bytes_res) => {
                        let bytes = bytes_res?;
                        ensure!(
                            bytes.len() == D::SIZE,
                            "Invalid head digest: expected {} bytes, found {}",
                            D::SIZE,
                            bytes.len()
                            );
                        Some(D::from_bytes(bytes))
                    }
                    head::None(()) => None,
                };
                future_head = head_digest.map(|dg| {
                    store
                        .resolve(&dg)
                        .and_then(|rs| rs.ok_or_else(|| format_err!("Head does not exist!")))
                        .map(CommitRef::new)
                });
                let candidate_digest = match state.get_candidate().which()? {
                    candidate::Some(bytes_res) => {
                        let bytes = bytes_res?;
                        ensure!(
                            bytes.len() == D::SIZE,
                            "Invalid candidate digest: expected {} bytes, found {}",
                            D::SIZE,
                            bytes.len()
                            );
                        Some(D::from_bytes(bytes))
                    }
                    candidate::None(()) => None,
                };
                future_candidate = candidate_digest.map(|dg| {
                    store
                        .resolve(&dg)
                        .and_then(|rs| rs.ok_or_else(|| format_err!("Candidate does not exist!")))
                        .map(TreeRef::new)
                });
                active_branch = match state.get_active_branch().which()? {
                    active_branch::Some(name) => Some(String::from(name?)),
                    active_branch::None(()) => None,
                };
            }

            let (candidate, head) = await!(future_candidate.join(future_head))?;
            Ok(State {
                candidate,
                head,
                active_branch,
            })
        }
    }

    pub fn encode<'b, W: Write + 'b, D: Digest>(
        &self,
        mut buf: W,
    ) -> impl Future<Item = (), Error = Error> + 'b
    where
        H: HandleDigest<D>,
    {
        use state_capnp::state;

        let state = self.clone();
        async_block! {
            let joined_future = state.candidate
                .as_ref()
                .map(TreeRef::as_inner)
                .map(HandleDigest::digest)
                .join(
                    state.head
                        .as_ref()
                        .map(CommitRef::as_inner)
                        .map(HandleDigest::digest),
                );
            let (candidate_digest, head_digest) = await!(joined_future)?;

            let mut message = message::Builder::new_default();

            {
                let mut state_builder = message.init_root::<state::Builder>();
                {
                    let mut digest = state_builder.borrow().get_digest()?;
                    digest.set_name(D::NAME);
                    digest.set_size(D::SIZE as u32);
                }
                {
                    let mut candidate = state_builder.borrow().get_candidate();
                    match candidate_digest {
                        Some(ref digest) => candidate.set_some(digest.as_bytes()),
                        None => candidate.set_none(()),
                    }
                }
                {
                    let mut head = state_builder.borrow().get_head();
                    match head_digest {
                        Some(ref digest) => head.set_some(digest.as_bytes()),
                        None => head.set_none(()),
                    }
                }
                {
                    let mut active_branch = state_builder.borrow().get_active_branch();
                    match state.active_branch {
                        Some(ref name) => active_branch.set_some(name),
                        None => active_branch.set_none(()),
                    }
                }
            }

            serialize_packed::write_message(&mut buf, &message)?;

            Ok(())
        }
    }
}
