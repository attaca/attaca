use std::env;

use clap::{App, SubCommand, ArgMatches};

use attaca::repository::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("list").about("List all entries in the index.")
}


pub fn go(_matches: &ArgMatches) -> Result<()> {
    let mut repository = Repository::find(env::current_dir()?)?;
    
    println!("{:#?}", repository.index.entries());
    
    Ok(())
}
