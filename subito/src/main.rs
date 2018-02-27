#![recursion_limit = "128"]
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

use clap::App;
use failure::Error;
use futures::prelude::*;
use structopt::StructOpt;
use subito::{CheckoutArgs, CommitArgs, FsckArgs, InitArgs, ShowArgs, StageArgs, StatusArgs};

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
    let app = App::from_yaml(yml)
        .subcommand(CheckoutArgs::clap())
        .subcommand(CommitArgs::clap())
        .subcommand(FsckArgs::clap())
        .subcommand(InitArgs::clap())
        .subcommand(ShowArgs::clap())
        .subcommand(StatusArgs::clap());
    let matches = app.get_matches();

    match matches.subcommand() {
        ("checkout", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = CheckoutArgs::from_clap(sub_m);
            universe.apply_mut(args)?.blocking.wait()?;
            Ok(())
        }
        ("fsck", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = FsckArgs::from_clap(sub_m);
            let errored = universe
                .apply_ref(args)?
                .errors
                .fold(false, |_, error| -> Result<bool, Error> {
                    println!(
                        "Fsck error: digest mismatch: calculated {}, received {}",
                        error.calculated, error.received
                    );
                    Ok(true)
                })
                .wait()?;

            if !errored {
                println!("No errors found.");
            }

            Ok(())
        }
        ("init", Some(sub_m)) => {
            let args = InitArgs::from_clap(sub_m);
            subito::init(args)?;
            Ok(())
        }
        ("stage", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = StageArgs::from_clap(sub_m);
            args.quiet = true;
            universe.apply_mut(args)?.blocking.wait()?;
            Ok(())
        }
        ("unstage", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = StageArgs::from_clap(sub_m);
            args.quiet = true;
            args.previous = true;
            universe.apply_mut(args)?.blocking.wait()?;
            Ok(())
        }
        ("commit", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = CommitArgs::from_clap(sub_m);
            universe.apply_mut(args)?.blocking.wait()?;
            Ok(())
        }
        ("show", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let mut args = ShowArgs::from_clap(sub_m);
            universe.apply_ref(args)?.blocking.wait()?;
            Ok(())
        }
        ("status", Some(sub_m)) => {
            let mut universe =
                subito::search()?.ok_or_else(|| format_err!("Repository not found!"))?;
            let args = StatusArgs::from_clap(sub_m);
            let status = universe.apply_ref(args)?;
            let (head, cand) = status.head.join(status.candidate).wait()?;
            let head_display = head.as_ref().map(|s| &s[..8]);
            let cand_display = cand.as_ref().map(|s| &s[..8]);

            match (head_display, cand_display) {
                (Some(h), Some(c)) => println!("On commit {} with virtual workspace {}", h, c),
                (Some(h), None) => println!("On commit {} without any virtual workspace", h),
                (None, Some(c)) => println!("No prior commit with virtual workspace {}", c),
                (None, None) => println!("No prior commit or virtual workspace"),
            }

            println!();

            let staged_changes = status.staged.collect().wait()?;
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
