#![feature(use_nested_groups)]

extern crate attaca;
extern crate db_key;
#[macro_use]
extern crate failure;
extern crate leveldb;
extern crate libc;
extern crate smallvec;

mod change_set;
mod db;

use attaca::Store;

use change_set::ChangeSet;

pub struct Repository<S: Store> {
    store: S,
    change_set: ChangeSet,
}
