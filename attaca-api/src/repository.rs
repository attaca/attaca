use std::fmt;
use std::iter;

use failure::Error;
use futures::prelude::*;
use futures::stream::FuturesUnordered;

use attaca::arc_slice::ArcSlice;
use attaca::object::{DataObject, Object};
use attaca::object_hash::ObjectHash;
use attaca::repository::Repository;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FsckDepth {
    Commit,
    Subtree,
    Data,
}


pub struct FsckOptions {
    depth: FsckDepth,
}


impl FsckOptions {
    pub fn new() -> Self {
        FsckOptions {
            depth: FsckDepth::Data,
        }
    }

    pub fn depth(&mut self, depth: FsckDepth) -> &mut Self {
        self.depth = depth;
        self
    }
}


#[derive(Debug, Fail)]
pub struct FsckError {
    root_hash: ObjectHash,
    mismatches: Vec<FsckMismatch>,
}


#[derive(Debug, Fail)]
pub struct FsckMismatch {
    expected_hash: ObjectHash,
    actual_hash: ObjectHash,
}


pub fn fsck<R: Repository>(root_hash: ObjectHash, options: FsckOptions) -> Result<(), Error> {
    let depth = options.depth;

    let errors = {

    };
}

pub fn deep_copy<B, S, T>(object_hash: ObjectHash, source: &S, target: &T) -> Result<(), Error>
where
    B: AsRef<str>,
    S: Repository,
    T: Repository,
{
    struct Item<R: Repository> {
        object_hash: ObjectHash,
        future: R::ReadHashed,
    }

    impl<R: Repository> Item<R> {
        fn new(repository: &R, object_hash: ObjectHash) -> Self {
            Self {
                object_hash,
                future: repository.read_hashed(object_hash),
            }
        }
    }

    impl<R: Repository> Future for Item<R> {
        type Item = (ObjectHash, ArcSlice);
        type Error = Error;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.future.poll().map(|async| {
                async.map(|bytes_opt| (self.object_hash, bytes_opt.unwrap()))
            })
        }
    }

    let mut queue = iter::once(Item::new(source, object_hash)).collect::<FuturesUnordered<_>>();
    let mut backlog = FuturesUnordered::new();

    while let (Some((object_hash, bytes)), polled_queue) =
        queue.into_future().wait().map_err(|(err, _)| err)?
    {
        queue = polled_queue;

        match Object::from_bytes(&bytes)? {
            Object::Data(DataObject::Small(_)) => {}
            Object::Data(DataObject::Large(large_object)) => {
                large_object
                    .children
                    .into_iter()
                    .for_each(|(_, child_hash)| queue.push(Item::new(source, child_hash)));
            }
            Object::Subtree(subtree_object) => {
                subtree_object
                    .entries
                    .into_iter()
                    .for_each(|(_, subtree_entry)| {
                        queue.push(Item::new(source, subtree_entry.hash()))
                    });
            }
            Object::Commit(commit_object) => {
                queue.push(Item::new(source, commit_object.subtree));
                commit_object
                    .parents
                    .into_iter()
                    .for_each(|parent_hash| queue.push(Item::new(source, parent_hash)));
            }
        }

        backlog.push(target.write_hashed(object_hash, bytes));
    }

    let _ = backlog.collect().wait()?;

    Ok(())
}
