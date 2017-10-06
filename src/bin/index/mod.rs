use clap::{App, SubCommand, ArgMatches};

use errors::*;

mod add;
mod clean;
mod commit;
mod list;
mod update;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("index")
        .about("Commands for manipulation of the index.")
        .subcommand(add::command())
        .subcommand(clean::command())
        .subcommand(commit::command())
        .subcommand(list::command())
        .subcommand(update::command())
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    match matches.subcommand() {
        ("add", Some(sub_m)) => add::go(sub_m),
        ("clean", Some(sub_m)) => clean::go(sub_m),
        ("commit", Some(sub_m)) => commit::go(sub_m),
        ("list", Some(sub_m)) => list::go(sub_m),
        ("update", Some(sub_m)) => update::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
