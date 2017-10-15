extern crate attaca;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate futures_cpupool;
extern crate globset;
extern crate histogram;
extern crate indicatif;
extern crate itertools;
extern crate memmap;

mod catalog;
mod errors;
mod index;
mod init;
mod remote;
mod stats;
mod test;
mod trace;
mod track;
mod untrack;
mod utils;

use std::env;
use std::ffi::OsString;

use clap::{App, ArgMatches};

use attaca::Repository;

use errors::*;


quick_main!(run);


fn command() -> App<'static, 'static> {
    App::new(crate_name!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .version(crate_version!())
        .subcommand(catalog::command())
        .subcommand(index::command())
        .subcommand(init::command())
        .subcommand(remote::command())
        .subcommand(stats::command())
        .subcommand(utils::command())
        .subcommand(test::command())
        .subcommand(track::command())
        .subcommand(untrack::command())
}


fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        // First match commands which don't need a loaded repository.
        ("init", Some(sub_m)) => init::go(sub_m),
        
        // Other commands need a repository to act on.
        other => {
            let mut repository = Repository::load(env::current_dir()?)?;

            let result = match other {
                ("catalog", Some(sub_m)) => catalog::go(&mut repository, sub_m),
                ("index", Some(sub_m)) => index::go(&mut repository, sub_m),
                ("remote", Some(sub_m)) => remote::go(&mut repository, sub_m),
                ("stats", Some(sub_m)) => stats::go(&mut repository, sub_m),
                ("test", Some(sub_m)) => test::go(&mut repository, sub_m),
                ("untrack", Some(sub_m)) => untrack::go(&mut repository, sub_m),
                ("utils", Some(sub_m)) => utils::go(&mut repository, sub_m),
                ("track", Some(sub_m)) => track::go(&mut repository, sub_m),
                _ => {
                    Err(Error::from_kind(ErrorKind::InvalidUsage))
                }
            };

            repository.cleanup()?;
            result
        }
    }
}


fn run() -> Result<()> {
    let matches = command().get_matches();
    let result = go(&matches);

    if let Err(Error(ErrorKind::InvalidUsage, _)) = result {
        eprintln!("Invalid usage:\n{}", matches.usage());
    }

    result
}


pub fn execute<I, T>(iterable: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    go(&command().get_matches_from(iterable))
}
