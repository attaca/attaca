extern crate attaca;
extern crate attaca_leveldb;
extern crate attaca_test;
extern crate futures_await as futures;
extern crate leveldb;
#[macro_use]
extern crate proptest;
extern crate tempdir;

use std::{io::{self, Cursor, Read}, sync::{Arc, RwLock}};

use attaca::digest::Sha3Digest;
use attaca_leveldb::LevelStore;
use attaca_test::{store::battery, test::Test};
use futures::{prelude::*, stream::FuturesUnordered};
use leveldb::{database::Database, options::Options};
use tempdir::TempDir;

#[test]
fn basics() {
    let tempdir = TempDir::new("test").unwrap();
    let database = Database::<_>::open(
        tempdir.path(),
        Options {
            create_if_missing: true,
            ..Options::new()
        },
    ).unwrap();
    let store = LevelStore::new(Arc::new(RwLock::new(database)));

    let mut tests = FuturesUnordered::new();
    tests.push(Test::new(
        "send_and_fetch_small",
        battery::send_and_fetch_small(store.clone(), Cursor::new(vec![42u8; 1024])),
    ));
    tests.push(Test::new(
        "send_and_fetch_large",
        battery::send_and_fetch_large(store.clone(), io::repeat(7).take(1_000_000)),
    ));
    tests.push(Test::new(
        "share_and_resolve",
        battery::share_and_resolve::<_, _, Sha3Digest>(
            store.clone(),
            io::repeat(13).take(1_000_000),
        ),
    ));

    let failures = tests
        .filter_map(|test_result| {
            if test_result.is_failure() {
                Some(test_result)
            } else {
                None
            }
        })
        .collect()
        .wait()
        .unwrap();

    if !failures.is_empty() {
        for failure in failures {
            println!("{}", failure);
        }

        panic!("Test failed!");
    }
}
