use clap::{App, SubCommand, ArgMatches};

use errors::*;

mod clean;
mod list;
// mod update;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("index")
        .about("Commands for manipulation of the index.")
        .subcommand(clean::command())
        .subcommand(list::command())
//        .subcommand(update::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("clean", Some(sub_m)) => clean::go(sub_m),
        ("list", Some(sub_m)) => list::go(sub_m),
//         ("update", Some(sub_m)) => update::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
