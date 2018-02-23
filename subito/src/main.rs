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
extern crate structopt;
extern crate subito;

use std::{env, fs};

use attaca::{digest::{Digest, Sha3Digest}, object::{CommitRef, TreeRef}};
use attaca_leveldb::LevelStore;
use clap::App;
use failure::Error;
use futures::prelude::*;
use leveldb::{database::Database, options::Options};
use structopt::StructOpt;
use subito::{Repository, candidate::{CommitArgs, StageArgs}, status::StatusArgs};

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
            let mut args = StageArgs::from_clap(sub_m);
            args.quiet = true;
            repository.stage(args).blocking.wait()?;
            Ok(())
        }
        ("unstage", Some(sub_m)) => {
            let mut repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = StageArgs::from_clap(sub_m);
            args.quiet = true;
            args.previous = true;
            repository.stage(args).blocking.wait()?;
            Ok(())
        }
        ("commit", Some(sub_m)) => {
            let mut repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = CommitArgs::from_clap(sub_m);
            repository.commit(args).blocking.wait()?;
            Ok(())
        }
        ("status", Some(sub_m)) => {
            let repository = Repository::<LevelStore, Sha3Digest>::search()?
                .ok_or_else(|| format_err!("Repository not found!"))?;
            let args = StatusArgs::from_clap(sub_m);
            let state = repository.status(args);

            let future_head_display = state.head.map(|opt_ref| {
                opt_ref.map(|d: CommitRef<Sha3Digest>| hex::encode(&d.as_inner().as_bytes()[..5]))
            });
            let future_cand_display = state.candidate.map(|opt_ref| {
                opt_ref.map(|d: TreeRef<Sha3Digest>| hex::encode(&d.as_inner().as_bytes()[..5]))
            });

            let (head_display, cand_display) =
                future_head_display.join(future_cand_display).wait()?;

            match (head_display, cand_display) {
                (Some(h), Some(c)) => println!("On commit {} with virtual workspace {}", h, c),
                (Some(h), None) => println!("On commit {} without any virtual workspace", h),
                (None, Some(c)) => println!("No prior commit with virtual workspace {}", c),
                (None, None) => println!("No prior commit or virtual workspace"),
            }

            println!();

            let staged_changes = state.staged.collect().wait()?;
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
