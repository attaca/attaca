use clap::{App, ArgMatches, SubCommand};

use attaca::Repository;

use errors::*;

mod file;
mod subtree;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("marshal")
        .about("Test marshalling of files or subtrees.")
        .subcommand(file::command())
        .subcommand(subtree::command())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("file", Some(sub_m)) => file::go(repository, sub_m),
        ("subtree", Some(sub_m)) => subtree::go(repository, sub_m),
        _ => {
            bail!(ErrorKind::InvalidUsage);
        }
    }
}
