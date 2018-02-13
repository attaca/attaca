use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;

mod read;
mod refs;
mod stats;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("debug")
        .subcommand(read::command())
        .subcommand(refs::command())
        .subcommand(stats::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("read", Some(sub_m)) => read::go(repository, sub_m),
        ("refs", Some(sub_m)) => refs::go(repository, sub_m),
        ("stats", Some(sub_m)) => stats::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
