use clap::{App, SubCommand, Arg, ArgMatches};
use globset::{Glob, GlobSetBuilder};

use attaca::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("untrack")
        .arg(Arg::with_name("PATH")
             .help("Stop tracking changes to files.")
             .index(1)
             .multiple(true))
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let glob_set = if let Some(paths) = matches.values_of("PATH") {
        let mut builder = GlobSetBuilder::new();
        for path in paths {
            builder.add(Glob::new(path)?);
        }
        builder.build()?
    } else {
        bail!("No files!");
    };

    repository.index.untrack(&glob_set);

    Ok(())
}
