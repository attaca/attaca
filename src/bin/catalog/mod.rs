use clap::{App, SubCommand, ArgMatches};

use errors::*;

mod clear;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("catalog")
        .about("Manipulate local and remote object store catalogs.")
        .subcommand(clear::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("clear", Some(sub_m)) => clear::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
