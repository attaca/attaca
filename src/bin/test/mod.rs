use clap::{App, SubCommand, ArgMatches};

use errors::*;

mod chunk;
mod marshal;
mod suite;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("test")
        .subcommand(chunk::command())
        .subcommand(marshal::command())
        .subcommand(suite::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("chunk", Some(sub_m)) => chunk::go(sub_m),
        ("marshal", Some(sub_m)) => marshal::go(sub_m),
        ("suite", Some(sub_m)) => suite::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
