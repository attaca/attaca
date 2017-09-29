use clap::{App, ArgMatches, SubCommand};

use errors::*;

mod file;
mod subtree;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("marshal")
        .about("Test marshalling of files or subtrees.")
        .subcommand(file::command())
        .subcommand(subtree::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("file", Some(sub_m)) => file::go(sub_m),
        ("subtree", Some(sub_m)) => subtree::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
