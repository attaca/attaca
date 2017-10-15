use std::env;

use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("update").about("Update the index.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    repository.index.update()?;

    Ok(())
}
