use clap::{App, ArgMatches, SubCommand};

use attaca::Repository;

use errors::*;

pub mod add;
pub mod list;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("remote")
        .about("Manipulate remote repositories.")
        .subcommand(add::command())
        .subcommand(list::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("add", Some(sub_m)) => add::go(repository, sub_m),
        ("list", Some(sub_m)) => list::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
