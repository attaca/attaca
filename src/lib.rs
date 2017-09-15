#![feature(conservative_impl_trait)]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

extern crate bincode;
extern crate chrono;
extern crate digest_writer;
#[macro_use]
extern crate error_chain;
extern crate generic_array;
extern crate memmap;
extern crate rad;
#[macro_use]
extern crate serde_derive;
extern crate seahash;
extern crate sha3;
extern crate toml;
extern crate typed_arena;
extern crate typenum;

pub mod context;
pub mod errors;
pub mod local;
pub mod marshal;
pub mod remote;
pub mod repository;
pub mod split;
pub mod trace;

pub use errors::{Error, ErrorKind, Result};
