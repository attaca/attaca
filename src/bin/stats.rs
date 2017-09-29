use std::env;

use clap::{App, SubCommand, ArgMatches};

use attaca::repository::Repository;
use errors::*;


#[derive(Debug)]
struct Stats {
    object_count: usize,
}


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("stats").about("Print basic repository statistics.")
}


pub fn go(_matches: &ArgMatches) -> Result<()> {
    let wd = env::current_dir()?;
    let mut repository = Repository::find(wd)?;
    let catalog = repository.get_catalog(None)?;

    let stats = Stats { object_count: catalog.len() };

    println!("{:#?}", stats);

    Ok(())
}
