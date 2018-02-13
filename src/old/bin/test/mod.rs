use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;

mod chunk;
mod marshal;
mod suite;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("test")
        .about("Run internal commands and tests.")
        .subcommand(chunk::command())
        .subcommand(marshal::command())
        .subcommand(suite::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("chunk", Some(sub_m)) => chunk::go(repository, sub_m),
        ("marshal", Some(sub_m)) => marshal::go(repository, sub_m),
        ("suite", Some(sub_m)) => suite::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
