use std::env;

use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("list").about("List all entries in the index.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    println!("{:#?}", repository.index.iter().collect::<Vec<_>>());

    Ok(())
}
