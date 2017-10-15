use clap::{App, ArgMatches, SubCommand};

use attaca::Repository;

use errors::*;

mod read;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("utils")
        .about(
            "Run utility commands on the repository, for inspection and debugging.",
        )
        .subcommand(read::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("read", Some(sub_m)) => read::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
