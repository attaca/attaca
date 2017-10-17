use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;
use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("refs").about("Print all stored refs.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    println!("{:#?}", repository.refs);

    Ok(())
}
