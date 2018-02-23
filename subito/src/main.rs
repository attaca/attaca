#![feature(conservative_impl_trait, generators, proc_macro)]

extern crate attaca;
extern crate attaca_leveldb;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate hex;
extern crate leveldb;
extern crate subito;

use std::{env, fs, path::PathBuf};

use attaca::{digest::{Digest, Sha3Digest},
             object::{CommitAuthor, CommitBuilder, CommitRef, TreeRef}};
use attaca_leveldb::LevelStore;
use clap::App;
use failure::Error;
use futures::prelude::*;
use leveldb::{database::Database, options::Options};
use subito::{Repository, State, candidate::{Batch, BatchOp, OpKind}};

fn main() {
    match run() {
        Ok(()) => {}
        Err(err) => {
            for cause in err.causes() {
                eprintln!("{}", cause);
            }
            ::std::process::exit(1);
        }
    }
}

fn run() -> Result<(), Error> {
    let yml = load_yaml!("main.yml");
    let app = App::from_yaml(yml);
    let matches = app.get_matches();

    match matches.subcommand() {
        ("init", Some(_sub_m)) => {
            let current_dir = env::current_dir()?;
            fs::create_dir(current_dir.join(".attaca"))?;
            let _ = Database::<i32>::open(
                &current_dir.join(".attaca/store"),
                Options {
                    create_if_missing: true,
                    ..Options::new()
                },
            )?;
            let _ = Database::<i32>::open(
                &current_dir.join(".attaca/workspace"),
                Options {
                    create_if_missing: true,
                    ..Options::new()
                },
            )?;

            return Ok(());
        }
        ("stage", Some(sub_m)) => {
            let mut repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;

            repository
                .stage_batch(sub_m.values_of("PATH").unwrap().map(|path| BatchOp {
                    path: PathBuf::from(path),
                    op: OpKind::Stage,
                }))
                .wait()?;
            Ok(())
        }
        ("unstage", Some(sub_m)) => {
            let mut repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;

            repository
                .stage_batch(sub_m.values_of("PATH").unwrap().map(|path| BatchOp {
                    path: PathBuf::from(path),
                    op: OpKind::Unstage,
                }))
                .wait()?;
            Ok(())
        }
        ("commit", Some(sub_m)) => {
            let mut repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;
            let state = repository.get_state()?;
            let candidate = state.candidate.clone().ok_or_else(|| {
                format_err!(
                    "No virtual workspace to commit. \
                     Add some files to the virtual workspace first!"
                )
            })?;

            let maybe_head = state.head.clone().map(|head_ref| head_ref.fetch()).wait()?;

            if let Some(ref head_commit) = maybe_head {
                ensure!(
                    head_commit.as_subtree() != &candidate || sub_m.is_present("force"),
                    "Previous commit is identical to virtual workspace! \
                     No changes will be committed - use --force to override."
                );
            }

            let mut commit_builder = if sub_m.is_present("amend") {
                match maybe_head {
                    Some(head_commit) => head_commit.diverge(),
                    None => bail!("No previous commit to amend!"),
                }
            } else {
                let mut builder = CommitBuilder::new();
                builder.parents(state.head.clone().into_iter());
                builder
            };

            commit_builder.subtree(candidate);

            if let Some(message) = sub_m.value_of("message") {
                commit_builder.message(message.to_string());
            }

            if let Some(author) = sub_m.value_of("author") {
                commit_builder.author(CommitAuthor {
                    name: Some(author.to_string()),
                    mbox: None,
                });
            }

            let commit_ref = commit_builder
                .into_commit()?
                .send(repository.as_store())
                .wait()?;
            repository.set_state(&State {
                head: Some(commit_ref),
                ..state
            })?;
            Ok(())
        }
        ("status", Some(_sub_m)) => {
            let repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;
            let state = repository.get_state()?;
            let (head, candidate) = state
                .head
                .map(|head| {
                    head.digest()
                        .map(|d: CommitRef<Sha3Digest>| hex::encode(&d.as_inner().as_bytes()[..5]))
                })
                .join(state.candidate.map(|candidate| {
                    candidate
                        .digest()
                        .map(|d: TreeRef<Sha3Digest>| hex::encode(&d.as_inner().as_bytes()[..5]))
                }))
                .wait()?;

            match (head, candidate) {
                (Some(head), Some(candidate)) => {
                    println!("On commit {} with virtual workspace {}", head, candidate)
                }
                (Some(head), None) => println!("On commit {} without any virtual workspace", head),
                (None, Some(candidate)) => {
                    println!("No prior commit with virtual workspace {}", candidate)
                }
                (None, None) => println!("No prior commit or virtual workspace"),
            }

            let staged_changes = repository.staged_changes().collect().wait()?;

            println!();
            if staged_changes.is_empty() {
                println!("No changes to be committed. The virtual workspace and previous commit are the same.");
            } else {
                for change in staged_changes {
                    println!("{:?}", change);
                }
            }
            Ok(())
        }
        (name, Some(_)) => unreachable!("Unhandled subcommand {}", name),
        (_, None) => {
            println!("{}", matches.usage());
            return Ok(());
        }
    }
}
