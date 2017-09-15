use clap::{App, SubCommand, ArgMatches};

use errors::{ErrorKind, Result};

mod chunk;
mod marshal;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("test")
        .subcommand(chunk::command())
        .subcommand(marshal::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("chunk", Some(sub_m)) => chunk::go(sub_m),
        ("marshal", Some(sub_m)) => marshal::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
