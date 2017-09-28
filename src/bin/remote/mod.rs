use clap::{App, ArgMatches, SubCommand};

use errors::*;

mod add;
mod list;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("remote")
        .about("Manipulate remote repositories.")
        .subcommand(add::command())
        .subcommand(list::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("add", Some(sub_m)) => add::go(sub_m),
        ("list", Some(sub_m)) => list::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
