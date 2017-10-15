use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;
use errors::*;


#[derive(Debug)]
struct Stats {
    object_count: usize,
}


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("stats").about("Print basic repository statistics.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    let catalog = repository.catalogs.get(None)?;
    let stats = Stats { object_count: catalog.len() };

    println!("{:#?}", stats);

    Ok(())
}
