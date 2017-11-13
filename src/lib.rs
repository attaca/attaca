//! # `attaca` - distributed, resilient version control with a git-like interface

#![feature(proc_macro, conservative_impl_trait, generators, offset_to)]
#![recursion_limit="256"]

#[cfg(not(target_pointer_width = "64"))]
compile_error!(
    "Must be compiled on a 64-bit architecture due to the use of memory-mapping for potentially extremely large files!"
);

#[cfg(test)]
extern crate histogram;

#[cfg(test)]
extern crate rand;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

extern crate bincode;
extern crate chrono;
extern crate digest_writer;
#[macro_use]
extern crate error_chain;
extern crate futures_await as futures;
extern crate futures_bufio;
extern crate futures_cpupool;
extern crate generic_array;
extern crate globset;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate memmap;
extern crate owning_ref;
extern crate qp_trie;
extern crate rad;
extern crate seahash;
#[macro_use]
extern crate serde_derive;
extern crate sequence_trie;
extern crate sha3;
extern crate stable_deref_trait;
extern crate toml;
extern crate typenum;

pub mod arc_slice;
pub mod catalog;
pub mod context;
pub mod errors;
pub mod index;
pub mod marshal;
pub mod repository;
pub mod split;
pub mod store;
pub mod trace;

pub use errors::*;
pub use repository::Repository;
pub use store::ObjectStore;

use std::collections::HashSet;
use std::path::{Path, PathBuf};


/// Controls the size of the MPSC queue between marshal tasks and write tasks.
const BATCH_FUTURE_BUFFER_SIZE: usize = 64;


/// Controls the size of buffers over buffered streams created when writing to remotes.
const WRITE_FUTURE_BUFFER_SIZE: usize = 64;


lazy_static! {
    /// Controls the name of the "hidden" `.attaca` repository metadata directory.
    static ref METADATA_PATH: &'static Path = Path::new(".attaca");


    /// The relative path of the repository config file.
    static ref CONFIG_PATH: PathBuf = METADATA_PATH.join("config.toml");


    /// The relative path of the blob directory within a repository.
    static ref BLOBS_PATH: PathBuf = METADATA_PATH.join("blobs");

    
    /// The relative path of the remote catalog directory.
    static ref REMOTE_CATALOGS_PATH: PathBuf = METADATA_PATH.join("remote-catalogs");


    /// The location of the local catalog file.
    static ref LOCAL_CATALOG_PATH: PathBuf = METADATA_PATH.join("local.catalog");


    /// The location of the index file.
    static ref INDEX_PATH: PathBuf = METADATA_PATH.join("index.bin");


    /// The location of the HEAD file.
    static ref REFS_PATH: PathBuf = METADATA_PATH.join("refs.bin");


    /// Default paths to ignore.
    static ref DEFAULT_IGNORES: HashSet<PathBuf> = {
        let mut set = HashSet::new();

        set.insert(METADATA_PATH.to_owned());
        set.insert(PathBuf::from(".git"));
        set.insert(PathBuf::from("insta-rados")); // HACK HACK HACK!

        set
    };
}
