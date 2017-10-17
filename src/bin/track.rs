use clap::{App, SubCommand, Arg, ArgMatches};
use globset::{Glob, GlobSetBuilder};

use attaca::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("track")
        .help("Begin tracking changes to files.")
        .arg(Arg::with_name("PATH").index(1).multiple(true))
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let pattern = if let Some(paths) = matches.values_of("PATH") {
        let mut builder = GlobSetBuilder::new();
        for path in paths {
            builder.add(Glob::new(path)?);
        }
        builder.build()?
    } else {
        bail!("No files!");
    };

    repository.index.register(&pattern)?;
    repository
        .index
        .iter_mut()
        .filter(|&(path, _)| pattern.is_match(path))
        .for_each(|(_, entry)| entry.tracked = true);

    Ok(())
}
