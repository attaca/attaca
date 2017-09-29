use std::env;

use clap::{App, SubCommand, Arg, ArgMatches};

use attaca::repository::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("clear")
        .about("Clear object store catalogs.")
        .arg(Arg::with_name("REMOTE").index(1).help(
            "The remote for which the catalog will be cleared.",
        ))
        .arg(Arg::with_name("local").short("l").long("local").help(
            "Clear the local object store catalog instead of a remote catalog.",
        ))
        .arg(Arg::with_name("all").short("a").long("all").help(
            "Clear all object store catalogs.",
        ))
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    let wd = env::current_dir()?;
    let repository = Repository::find(&wd)?;

    if matches.is_present("all") {
        repository.clear_catalogs()?;
    } else if matches.is_present("local") {
        repository.get_catalog(None)?.clear()?;
    } else if let Some(remote) = matches.value_of("REMOTE") {
        repository.get_catalog(Some(remote.to_owned()))?.clear()?;
    } else {
        eprintln!("{}", matches.usage());
        bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
    }

    Ok(())
}
