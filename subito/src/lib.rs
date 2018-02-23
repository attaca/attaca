#![feature(proc_macro, conservative_impl_trait, generators)]

extern crate attaca;
extern crate attaca_leveldb;
extern crate capnp;
extern crate db_key;
#[macro_use]
extern crate derive_builder;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate hex;
extern crate ignore;
extern crate itertools;
extern crate leveldb;
#[macro_use]
extern crate nix;
extern crate smallvec;
#[macro_use]
extern crate structopt;

#[allow(dead_code)]
mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/cache_capnp.rs"));
}

#[allow(dead_code)]
mod digest_capnp {
    include!(concat!(env!("OUT_DIR"), "/digest_capnp.rs"));
}

#[allow(dead_code)]
mod object_ref_capnp {
    include!(concat!(env!("OUT_DIR"), "/object_ref_capnp.rs"));
}

#[allow(dead_code)]
mod state_capnp {
    include!(concat!(env!("OUT_DIR"), "/state_capnp.rs"));
}

mod cache;
mod db;

pub mod candidate;
pub mod status;

use std::{env, marker::PhantomData, path::{Path, PathBuf}, sync::{Arc, RwLock}};

use attaca::{Handle, HandleDigest, Store, digest::Digest, object::{CommitRef, TreeRef}};
use capnp::{serialize_packed, Word, message::{self, ScratchSpace, ScratchSpaceHeapAllocator}};
use failure::Error;
use futures::prelude::*;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};

use cache::Cache;
use db::Key;

#[derive(Debug, Clone)]
struct State<H: Handle> {
    candidate: Option<TreeRef<H>>,
    head: Option<CommitRef<H>>,
    active_branch: Option<String>,
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

pub struct Repository<S: Store, D: Digest>
where
    S::Handle: HandleDigest<D>,
{
    _digest: PhantomData<D>,

    store: S,
    db: Arc<RwLock<Database<Key>>>,

    cache: Cache<D>,
    path: PathBuf,
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn open(path: &Path) -> Result<Self, Error> {
        let store = S::open_path(&path.join(".attaca/store"))?;
        let db = Arc::new(RwLock::new(Database::open(
            &path.join(".attaca/workspace"),
            Options::new(),
        )?));
        let cache = Cache::from(db.clone());

        Ok(Self {
            _digest: PhantomData,
            store,
            db,

            cache,
            path: path.to_owned(),
        })
    }

    pub fn search() -> Result<Option<Self>, Error> {
        let mut wd = env::current_dir()?;

        while !wd.join(".attaca").exists() {
            if !wd.pop() {
                return Ok(None);
            }
        }

        Ok(Some(Self::open(&wd)?))
    }

    fn set_state(&self, state: &State<S::Handle>) -> Result<(), Error> {
        use state_capnp::state;

        let mut scratch_bytes = [0u8; 1024];
        let mut scratch_space = ScratchSpace::new(Word::bytes_to_words_mut(&mut scratch_bytes));
        let mut message = message::Builder::new(ScratchSpaceHeapAllocator::new(&mut scratch_space));

        let (candidate_digest, head_digest) = state
            .candidate
            .as_ref()
            .map(TreeRef::as_inner)
            .map(HandleDigest::digest)
            .join(
                state
                    .head
                    .as_ref()
                    .map(CommitRef::as_inner)
                    .map(HandleDigest::digest),
            )
            .wait()?;

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

        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &message)?;

        self.db
            .read()
            .unwrap()
            .put(WriteOptions::new(), &Key::state(), &buf)?;

        Ok(())
    }

    fn get_state(&self) -> Result<State<S::Handle>, Error> {
        match self.db
            .read()
            .unwrap()
            .get(ReadOptions::new(), &Key::state())?
        {
            Some(bytes) => {
                use state_capnp::state::{self, active_branch, candidate, head};

                let message_reader =
                    serialize_packed::read_message(&mut &bytes[..], message::ReaderOptions::new())?;
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
                let future_head = head_digest.map(|dg| {
                    self.store
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
                let future_candidate = candidate_digest.map(|dg| {
                    self.store
                        .resolve(&dg)
                        .and_then(|rs| rs.ok_or_else(|| format_err!("Candidate does not exist!")))
                        .map(TreeRef::new)
                });
                let active_branch = match state.get_active_branch().which()? {
                    active_branch::Some(name) => Some(String::from(name?)),
                    active_branch::None(()) => None,
                };

                let (candidate, head) = future_candidate.join(future_head).wait()?;
                Ok(State {
                    candidate,
                    head,
                    active_branch,
                })
            }
            None => {
                let state = State::default();
                self.set_state(&state)?;
                Ok(state)
            }
        }
    }
}
