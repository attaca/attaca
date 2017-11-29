use std::collections::HashSet;
use std::fmt::{self, Write};
use std::iter;

use failure::Error;
use futures::prelude::*;
use futures::stream::{self, FuturesUnordered};

use attaca::arc_slice::ArcSlice;
use attaca::hasher;
use attaca::object::{DataObject, Object, SubtreeEntry};
use attaca::object_hash::ObjectHash;
use attaca::repository::Repository;

use utils::Indenter;

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


impl fmt::Display for FsckError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            fmt,
            "failed to validate object with hash {}, due to mismatches:",
            self.root_hash
        )?;

        let mut writer = Indenter::new(fmt);
        for mismatch in &self.mismatches {
            writeln!(
                writer,
                "hash {} resolved to an object with hash {}",
                mismatch.expected_hash,
                mismatch.actual_hash
            )?;
        }

        Ok(())
    }
}


#[derive(Debug)]
pub struct FsckMismatch {
    expected_hash: ObjectHash,
    actual_hash: ObjectHash,
}


pub fn fsck<R: Repository>(
    root_hash: ObjectHash,
    repository: &R,
    options: FsckOptions,
) -> Result<(), Error> {
    let depth = options.depth;

    let mut mismatches = Vec::new();
    let mut hashes = vec![root_hash];
    let mut visited = HashSet::new();

    while !hashes.is_empty() {
        let object_stream = {
            let next_hashes = hashes.drain(..).filter(|&hash| visited.insert(hash));
            stream::futures_unordered(next_hashes.map(|hash| {
                repository
                    .read_object(hash)
                    .map(move |object| (hash, object.unwrap()))
            }))
        };

        object_stream
            .for_each(|(hash, object)| {
                let real_hash = hasher::hash(&object);

                if hash != real_hash {
                    mismatches.push(FsckMismatch {
                        expected_hash: hash,
                        actual_hash: real_hash,
                    });
                } else {
                    match object {
                        Object::Data(DataObject::Large(ref large_object))
                            if depth >= FsckDepth::Data =>
                        {
                            hashes.extend(large_object.children.iter().map(|&(_, hash)| hash));
                        }
                        Object::Subtree(ref subtree_object) if depth >= FsckDepth::Subtree => {
                            hashes.extend(subtree_object.entries.iter().filter_map(
                                |(_, entry)| match *entry {
                                    SubtreeEntry::File(hash, _) if depth >= FsckDepth::Data => {
                                        Some(hash)
                                    }
                                    SubtreeEntry::Subtree(hash) => Some(hash),
                                    _ => None,
                                },
                            ));
                        }
                        Object::Commit(commit_object) => {
                            hashes.extend(commit_object.parents);
                            if depth >= FsckDepth::Subtree {
                                hashes.push(commit_object.subtree);
                            }
                        }
                        _ => {}
                    }
                }

                Ok(())
            })
            .wait()?;
    }

    if mismatches.len() > 0 {
        Err(Error::from(FsckError {
            root_hash,
            mismatches,
        }))
    } else {
        Ok(())
    }
}

pub fn copy<B, S, T>(object_hash: ObjectHash, source: &S, target: &T) -> Result<(), Error>
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
