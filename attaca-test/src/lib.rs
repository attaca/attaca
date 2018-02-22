#![feature(conservative_impl_trait, generators, proc_macro)]

extern crate attaca;
extern crate chashmap;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate leb128;
extern crate owning_ref;
extern crate proptest;

pub mod store;
pub mod test;
