#[macro_use]
extern crate error_chain;
extern crate histogram;
extern crate memmap;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate seahash;
extern crate sha3;

pub mod errors;
pub mod marshal;
pub mod split;
pub mod store;

pub use errors::{Error, ErrorKind, Result};
