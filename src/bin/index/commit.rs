use std::env;

use clap::{App, SubCommand, ArgMatches};

use errors::*;

use attaca::repository::Repository;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("commit")
        .about("Commit all flagged changed files in the index.")
}


pub fn go(_matches: &ArgMatches) -> Result<()> {
    let mut repository = Repository::load(env::current_dir()?)?;

    for (path, _entry) in repository.index.added() {
        println!("Dummy: committing {}", path.display());
    }

    Ok(())
}
