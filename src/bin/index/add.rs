use std::env;

use clap::{App, Arg, SubCommand, ArgMatches};

use errors::*;

use attaca::repository::Repository;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("add")
        .about("Add a file to the working set.")
        .arg(Arg::with_name("FILE").required(true).index(1).multiple(
            true,
        ))
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    let paths = matches.values_of("FILE").unwrap();
    let mut repository = Repository::load(env::current_dir()?)?;

    for path in paths {
        repository.index.add(&path)?;
    }

    Ok(())
}
