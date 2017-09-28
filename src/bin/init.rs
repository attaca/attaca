use std::env;
use std::path::PathBuf;

use clap::{App, Arg, SubCommand, ArgMatches};

use errors::*;

use attaca::repository::Repository;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("init")
        .about("Initializes an attaca repository.")
        .arg(
            Arg::with_name("directory")
                .short("d")
                .long("directory")
                .takes_value(true)
                .help("Sets the root directory of the repository."),
        )
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    let wd = env::current_dir()?;
    let path = matches.value_of("directory").map(PathBuf::from).unwrap_or(
        wd,
    );

    let repository = Repository::init(path);

    println!("Initialized repository: {:?}", repository);

    Ok(())
}
