#![feature(proc_macro, conservative_impl_trait, generators, use_nested_groups)]

extern crate attaca;
extern crate attaca_leveldb;
extern crate capnp;
extern crate db_key;
#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate ignore;
extern crate leveldb;
#[macro_use]
extern crate nix;
extern crate num_traits;
extern crate smallvec;

mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/cache_capnp.rs"));
}

mod digest_capnp {
    include!(concat!(env!("OUT_DIR"), "/digest_capnp.rs"));
}

mod state_capnp {
    include!(concat!(env!("OUT_DIR"), "/state_capnp.rs"));
}

mod cache;
mod db;

use std::{io::{BufRead, Write}, marker::PhantomData, path::{Path, PathBuf}, sync::{Arc, RwLock}};

use attaca::{Handle, HandleDigest, Open, Store, digest::{Digest, Sha3Digest}, object::TreeBuilder};
use capnp::{serialize_packed, Word, message::{self, ScratchSpace, ScratchSpaceHeapAllocator}};
use failure::Error;
use futures::{future::{self, Either}, prelude::*};
use ignore::WalkBuilder;
use leveldb::{database::Database, kv::KV, options::{Options, ReadOptions, WriteOptions}};

use cache::Cache;
use db::Key;

#[derive(Default)]
struct State<H: Handle> {
    candidate: H,
    head: Option<H>,
    active_branch: Option<String>,
}

impl<H: Handle> State<H> {
    //     fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
    //         use state_capnp::state;
    //
    //         let mut scratch_bytes = [0u8; 1024];
    //         let mut scratch_space = ScratchSpace::new(Word::bytes_to_words_mut(&mut scratch_bytes));
    //         let mut message = message::Builder::new(ScratchSpaceHeapAllocator::new(&mut scratch_space));
    //
    //         {
    //             let mut state = message.init_root::<state::Builder>();
    //             {
    //                 let mut digest = state.borrow().get_digest()?;
    //                 digest.set_name(D::NAME);
    //                 digest.set_size(D::SIZE as u32);
    //             }
    //             {
    //                 let mut head = state.borrow().get_head();
    //                 match self.head {
    //                     Some(ref digest) => head.set_some(digest.as_bytes()),
    //                     None => head.set_none(()),
    //                 }
    //             }
    //             {
    //                 let mut active_branch = state.borrow().get_active_branch();
    //                 match self.active_branch {
    //                     Some(ref name) => active_branch.set_some(name),
    //                     None => active_branch.set_none(()),
    //                 }
    //             }
    //         }
    //
    //         serialize_packed::write_message(writer, &message)?;
    //
    //         Ok(())
    //     }
}

pub struct Repository<S: Store, D: Digest>
where
    S::Handle: HandleDigest<D>,
{
    _digest: PhantomData<D>,
    store: S,
    db: Arc<RwLock<Database<Key>>>,

    cache: Cache,
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

    fn set_state(&self, state: &State<S::Handle>) -> Result<(), Error> {
        use state_capnp::state;

        let mut scratch_bytes = [0u8; 1024];
        let mut scratch_space = ScratchSpace::new(Word::bytes_to_words_mut(&mut scratch_bytes));
        let mut message = message::Builder::new(ScratchSpaceHeapAllocator::new(&mut scratch_space));

        let (candidate_digest, head_digest) = state
            .candidate
            .digest()
            .join(state.head.as_ref().map(HandleDigest::digest))
            .wait()?;

        {
            let mut state_builder = message.init_root::<state::Builder>();
            state_builder.set_candidate(candidate_digest.as_bytes());
            {
                let mut digest = state_builder.borrow().get_digest()?;
                digest.set_name(D::NAME);
                digest.set_size(D::SIZE as u32);
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

        Ok(())
    }

    fn get_state(&self) -> Result<State<S::Handle>, Error> {
        match self.db
            .read()
            .unwrap()
            .get(ReadOptions::new(), &Key::state())?
        {
            Some(bytes) => {
                use state_capnp::state::{self, active_branch, head};

                let message_reader =
                    serialize_packed::read_message(&mut &bytes[..], message::ReaderOptions::new())?;
                let state = message_reader.get_root::<state::Reader>()?;

                let digest = state.get_digest()?;
                let name = digest.get_name()?;
                let size = digest.get_size() as usize;

                ensure!(name == D::NAME && size == D::SIZE, "Digest mismatch!");

                let head_digest = match state.get_head().which()? {
                    head::Some(bytes) => Some(D::from_bytes(&bytes?)),
                    head::None(()) => None,
                };
                let future_head = head_digest.map(|dg| {
                    self.store
                        .resolve(&dg)
                        .and_then(|rs| rs.ok_or_else(|| format_err!("Head does not exist!")))
                });
                let future_candidate = self.store
                    .resolve(&D::from_bytes(state.get_candidate()?))
                    .and_then(|resolved| {
                        resolved.ok_or_else(|| format_err!("Candidate does not exist!"))
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
                let candidate = TreeBuilder::new()
                    .as_tree()
                    .send(&self.store)
                    .wait()?
                    .into_handle();
                let state = State {
                    candidate,
                    head: None,
                    active_branch: None,
                };
                self.set_state(&state)?;
                Ok(state)
            }
        }
    }

    // Status. We have:
    //
    // 1. The changeset (the current "candidate" tree.)
    // 2. The cache.
    //
    // The candidate tree tells us the tree which will be committed upon running the "commit"
    // command. The cache tells us what objects have and have not been changed.
    pub fn status(&mut self) -> Result<Self, Error> {
        unimplemented!()
    }
}