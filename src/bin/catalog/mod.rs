use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;

mod clear;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("catalog")
        .about("Manipulate local and remote object store catalogs.")
        .subcommand(clear::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("clear", Some(sub_m)) => clear::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
