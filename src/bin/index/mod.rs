use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;

mod list;
mod update;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("index")
        .about("Commands for manipulation of the index.")
        .subcommand(list::command())
        .subcommand(update::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("list", Some(sub_m)) => list::go(repository, sub_m),
        ("update", Some(sub_m)) => update::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
