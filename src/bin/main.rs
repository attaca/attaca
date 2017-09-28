extern crate attaca;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate histogram;
extern crate indicatif;
extern crate itertools;
extern crate memmap;

mod errors;
mod init;
mod remote;
mod test;
mod trace;
mod utils;

use std::ffi::OsString;

use clap::{App, ArgMatches};

use errors::{ErrorKind, Result};


quick_main!(run);


fn command() -> App<'static, 'static> {
    App::new(crate_name!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .version(crate_version!())
        .subcommand(init::command())
        .subcommand(remote::command())
        .subcommand(test::command())
        .subcommand(utils::command())
}


fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("init", Some(sub_m)) => init::go(sub_m),
        ("remote", Some(sub_m)) => remote::go(sub_m),
        ("utils", Some(sub_m)) => utils::go(sub_m),
        ("test", Some(sub_m)) => test::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}


fn run() -> Result<()> {
    go(&command().get_matches())
}


pub fn execute<I, T>(iterable: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    go(&command().get_matches_from(iterable))
}
