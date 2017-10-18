use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fmt::Write;

use clap::{App, SubCommand, ArgMatches};
use futures::prelude::*;
use futures::stream;

use attaca::marshal::{ObjectHash, CommitObject};
use attaca::Repository;

use errors::*;


#[derive(Eq)]
struct TimeOrdered {
    hash: ObjectHash,
    commit: CommitObject,
}


impl PartialEq for TimeOrdered {
    fn eq(&self, other: &Self) -> bool {
        self.commit.timestamp == other.commit.timestamp
    }
}


impl PartialOrd for TimeOrdered {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


impl Ord for TimeOrdered {
    fn cmp(&self, other: &Self) -> Ordering {
        self.commit.timestamp.cmp(&other.commit.timestamp)
    }
}


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("log").about("View repository commit history.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    let mut commits = {
        let ctx = repository.local(())?;

        let mut commits = BinaryHeap::new();
        let mut hashes = ctx.refs.head().into_iter().collect::<Vec<_>>();
        let mut visited = HashSet::new();

        while !hashes.is_empty() {
            let commit_stream = {
                let next_hashes = hashes.drain(..).filter(|&hash| visited.insert(hash));
                stream::futures_unordered(next_hashes.map(|hash| {
                    ctx.read_commit(hash).map(move |commit| (hash, commit))
                }))
            };

            commit_stream
                .for_each(|(hash, commit)| {
                    hashes.extend(commit.parents.iter().cloned());
                    commits.push(TimeOrdered { hash, commit });

                    Ok(())
                })
                .wait()?;
        }

        ctx.close().wait()?;

        commits
    };

    let mut buf = String::new();

    if let Some(TimeOrdered { hash, commit }) = commits.pop() {
        write!(
            buf,
            "commit {} \nDate: {}\n\t{}\n",
            hash,
            commit.timestamp,
            commit.message
        )?;
    }

    for TimeOrdered { hash, commit } in commits {
        write!(
            buf,
            "\ncommit {}\nDate: {}\n\t{}\n",
            hash,
            commit.timestamp,
            commit.message
        )?;
    }

    print!("{}", buf);

    Ok(())
}
