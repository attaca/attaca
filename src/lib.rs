//! # `attaca` - distributed, resilient version control with a git-like interface

#![feature(offset_to)]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

extern crate bincode;
extern crate chrono;
extern crate digest_writer;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate futures_bufio;
extern crate futures_cpupool;
extern crate generic_array;
#[macro_use]
extern crate lazy_static;
extern crate memmap;
extern crate owning_ref;
extern crate rad;
#[macro_use]
extern crate serde_derive;
extern crate seahash;
extern crate sha3;
extern crate toml;
extern crate typenum;
extern crate void;

pub mod arc_slice;
pub mod batch;
pub mod context;
pub mod errors;
pub mod local;
pub mod marshal;
pub mod remote;
pub mod repository;
pub mod split;
pub mod trace;

pub use errors::{Error, ErrorKind, Result};


/// Controls the size of buffers over buffered streams created when marshalling a tree.
const MARSHAL_FUTURE_BUFFER_SIZE: usize = 64;


/// Controls the size of the MPSC queue between marshal tasks and write tasks.
const BATCH_FUTURE_BUFFER_SIZE: usize = 64;


/// Controls the size of buffers over buffered streams created when writing to remotes.
const WRITE_FUTURE_BUFFER_SIZE: usize = 64;
