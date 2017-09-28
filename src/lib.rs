//! # `attaca` - distributed, resilient version control with a git-like interface

#![feature(proc_macro, conservative_impl_trait, generators, offset_to)]
#![recursion_limit="128"]

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
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate memmap;
extern crate owning_ref;
extern crate qp_trie;
extern crate rad;
#[macro_use]
extern crate serde_derive;
extern crate seahash;
extern crate sha3;
extern crate toml;
extern crate typenum;

pub mod arc_slice;
pub mod batch;
pub mod catalog;
pub mod context;
pub mod errors;
pub mod local;
pub mod marshal;
pub mod remote;
pub mod repository;
pub mod split;
pub mod trace;

pub use errors::{Error, ErrorKind, Result};

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
}
