use clap::{App, SubCommand, ArgMatches};

use errors::{ErrorKind, Result};

use chunk;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("test").subcommand(chunk::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("chunk", Some(sub_m)) => chunk::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
