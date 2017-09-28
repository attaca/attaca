use clap::{App, ArgMatches, SubCommand};

use errors::*;

mod read;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("utils")
        .about(
            "Run utility commands on the repository, for inspection and debugging.",
        )
        .subcommand(read::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("read", Some(sub_m)) => read::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
