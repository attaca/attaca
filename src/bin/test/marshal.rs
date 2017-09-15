use std::env;
use std::path::Path;

use clap::{App, Arg, SubCommand, ArgMatches};

use attaca::context::{Context, Files};
use attaca::marshal::ObjectHash;
use attaca::repository::Repository;
use attaca::trace::Trace;

use errors::Result;
use trace::ProgressTrace;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("marshal")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to marshal.")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("quiet")
                .help("Run quietly (no progress indicators.)")
                .short("q")
                .long("quiet"),
        )
}


fn marshal<T: Trace, P: AsRef<Path>>(trace: T, path: P) -> Result<ObjectHash> {
    let wd = env::current_dir()?;
    let repo = Repository::find(wd)?;
    let mut context = Context::with_trace(repo, trace);
    let mut files = Files::new();

    let chunked = context.chunk_file(&mut files, path)?;
    let (hash, marshalled) = context.marshal_file(chunked);
    context.write_marshalled(&marshalled)?;

    Ok(hash)
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    let path = matches.value_of("INPUT").unwrap();

    let hash = if matches.is_present("quiet") {
        marshal((), path)?
    } else {
        marshal(ProgressTrace::new(), path)?
    };

    println!("{}", hash);

    Ok(())
}
