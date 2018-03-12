#![recursion_limit = "128"]
#![feature(conservative_impl_trait, generators, proc_macro)]

extern crate attaca;
extern crate attaca_leveldb;
extern crate attaca_rados;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate hex;
extern crate leveldb;
extern crate structopt;
#[macro_use]
extern crate subito;

use std::fmt::Write;

use attaca::object::CommitAuthor;
use clap::App;
use failure::Error;
use futures::prelude::*;
use structopt::StructOpt;
use subito::{BranchArgs, CheckoutArgs, CloneArgs, CommitArgs, FetchArgs, FsckArgs, Head, InitArgs,
             LogArgs, PullArgs, PushArgs, RemoteArgs, ShowArgs, StageArgs, StatusArgs};

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
        .subcommand(BranchArgs::clap())
        .subcommand(CheckoutArgs::clap())
        .subcommand(CloneArgs::clap())
        .subcommand(CommitArgs::clap())
        .subcommand(FetchArgs::clap())
        .subcommand(FsckArgs::clap())
        .subcommand(LogArgs::clap())
        .subcommand(InitArgs::clap())
        .subcommand(PullArgs::clap())
        .subcommand(PushArgs::clap())
        .subcommand(RemoteArgs::clap())
        .subcommand(ShowArgs::clap())
        .subcommand(StatusArgs::clap());
    let matches = app.get_matches();

    match matches.subcommand() {
        ("branch", Some(sub_m)) => {
            let args = BranchArgs::from_clap(sub_m);
            search!(repository, repository.branch(args).blocking.wait())?
        }
        ("checkout", Some(sub_m)) => {
            let args = CheckoutArgs::from_clap(sub_m);
            search!(repository, repository.checkout(args).blocking.wait())?
        }
        ("clone", Some(sub_m)) => {
            subito::clone(CloneArgs::from_clap(sub_m)).blocking.wait()?;
            Ok(())
        }
        ("fetch", Some(sub_m)) => {
            let args = FetchArgs::from_clap(sub_m);
            search!(repository, repository.fetch(args).blocking.wait())?
        }
        ("fsck", Some(sub_m)) => search!(repository, {
            let args = FsckArgs::from_clap(sub_m);
            let errored = repository
                .fsck(args)
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
        })?,
        ("log", Some(sub_m)) => search!(repository, {
            let args = LogArgs::from_clap(sub_m);
            let commits = repository.log(args).entries.collect().wait()?;
            let mut buf = String::new();

            if !commits.is_empty() {
                for (commit_ref, commit) in commits {
                    buf.clear();
                    writeln!(&mut buf, "commit {}", commit_ref.as_inner())?;
                    let CommitAuthor { ref name, ref mbox } = *commit.as_author();
                    match (name, mbox) {
                        (&Some(ref n), &Some(ref m)) => writeln!(&mut buf, "author {} <{}>", n, m)?,
                        (&Some(ref n), &None) => writeln!(&mut buf, "author {}", n)?,
                        (&None, &Some(ref m)) => writeln!(&mut buf, "author <{}>", m)?,
                        (&None, &None) => {}
                    }
                    writeln!(&mut buf, "date {}", commit.as_timestamp())?;
                    if let Some(message) = commit.as_message() {
                        writeln!(&mut buf, "\t{}", message)?;
                    }
                    println!("{}", buf);
                }
            } else {
                println!("No commits yet.");
            }

            Ok(())
        })?,
        ("init", Some(sub_m)) => init!(InitArgs::from_clap(sub_m), _repository, Ok(()))?,
        ("push", Some(sub_m)) => {
            let args = PushArgs::from_clap(sub_m);
            search!(repository, repository.push(args).blocking.wait())?
        }
        ("pull", Some(sub_m)) => {
            let args = PullArgs::from_clap(sub_m);
            search!(repository, repository.pull(args).blocking.wait())?
        }
        ("stage", Some(sub_m)) => {
            let mut args = StageArgs::from_clap(sub_m);
            args.quiet = true;
            search!(repository, repository.stage(args).blocking.wait())?
        }
        ("unstage", Some(sub_m)) => {
            let mut args = StageArgs::from_clap(sub_m);
            args.quiet = true;
            args.previous = true;
            search!(repository, repository.stage(args).blocking.wait())?
        }
        ("commit", Some(sub_m)) => {
            let args = CommitArgs::from_clap(sub_m);
            search!(repository, repository.commit(args).blocking.wait())?
        }
        ("remote", Some(sub_m)) => {
            let args = RemoteArgs::from_clap(sub_m);
            search!(repository, repository.remote(args).blocking.wait())?
        }
        ("show", Some(sub_m)) => {
            let args = ShowArgs::from_clap(sub_m);
            search!(repository, repository.show(args).blocking.wait())?
        }
        ("status", Some(sub_m)) => {
            let args = StatusArgs::from_clap(sub_m);
            search!(repository, {
                let status = repository.status(args);
                let (head, cand) = status.head.join(status.candidate).wait()?;

                let head_display = match head {
                    Head::Empty => None,
                    Head::Detached(commit) => Some(format!("commit {}", &commit.as_inner()[..8])),
                    Head::Branch(branch) => Some(format!("branch {}", branch)),
                };
                let cand_display = cand.as_ref().map(|s| &s[..8]);

                match (head_display, cand_display) {
                    (Some(h), Some(c)) => println!("On {} with virtual workspace {}", h, c),
                    (Some(h), None) => println!("On {} without any virtual workspace", h),
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
            })?
        }
        (name, Some(_)) => unreachable!("Unhandled subcommand {}", name),
        (_, None) => {
            println!("{}", matches.usage());
            return Ok(());
        }
    }
}
