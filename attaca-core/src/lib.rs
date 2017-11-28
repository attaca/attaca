#![feature(proc_macro, generators, conservative_impl_trait, offset_to)]

#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate serde_derive;

extern crate bincode;
extern crate chrono;
extern crate digest;
extern crate failure;
#[macro_use]
extern crate futures_await as futures;
extern crate futures_cpupool;
extern crate generic_array;
extern crate globset;
#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate memmap;
extern crate owning_ref;
extern crate rad;
extern crate seahash;
extern crate serde;
extern crate sha3;
extern crate toml;
extern crate typenum;


mod lockmap;
mod split;

pub mod arc_slice;
pub mod hasher;
pub mod object;
pub mod object_hash;
pub mod record;
pub mod repository;
pub mod tree;
pub mod workspace;
