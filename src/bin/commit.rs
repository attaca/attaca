use chrono::prelude::*;
use clap::{App, SubCommand, Arg, ArgMatches};
use futures::prelude::*;
use globset::{Glob, GlobSetBuilder};

use attaca::Repository;
use attaca::repository::Head;

use errors::*;
use trace::Progress;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("commit")
        .help("Commit a change to the local repository.")
        .arg(
            Arg::with_name("INCLUDE")
                .short("i")
                .long("inc")
                .takes_value(true)
                .multiple(true)
                .help(
                    "Zero or more patterns matching files to include in the commit.",
                ),
        )
        .arg(
            Arg::with_name("EXCLUDE")
                .short("e")
                .long("exc")
                .takes_value(true)
                .multiple(true)
                .help(
                    "Zero or more patterns matching files to exclude from the commit.",
                ),
        )
        .arg(Arg::with_name("MESSAGE").index(1).required(true).help(
            "The commit message.",
        ))
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let include = if let Some(paths) = matches.values_of("INCLUDE") {
        let mut builder = GlobSetBuilder::new();
        for path in paths {
            builder.add(Glob::new(path)?);
        }
        Some(builder.build()?)
    } else {
        None
    };

    let exclude = if let Some(paths) = matches.values_of("EXCLUDE") {
        let mut builder = GlobSetBuilder::new();
        for path in paths {
            builder.add(Glob::new(path)?);
        }
        Some(builder.build()?)
    } else {
        None
    };

    let message = matches.value_of("MESSAGE").unwrap().to_owned();

    repository.index.update()?;

    let commit_hash = {
        let ctx = repository.local(Progress::new(None))?;

        let head_hash = ctx.refs.head_as_hash();
        // Merges are unimplemented. So, the only possible parent is the head.
        let commit_hash = ctx.write_commit(
            include.as_ref(),
            exclude.as_ref(),
            head_hash.into_iter().collect(),
            message,
            Utc::now(),
        ).wait()?;

        ctx.close().wait()?;

        commit_hash
    };

    repository.refs.head = Head::Detached(commit_hash);
    repository.index.iter_mut().for_each(
        |(_, entry)| entry.added = false,
    );

    Ok(())
}
