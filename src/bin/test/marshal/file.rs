use std::path::Path;

use clap::{App, Arg, SubCommand, ArgMatches};
use futures::prelude::*;

use attaca::Repository;
use attaca::marshal::ObjectHash;
use attaca::trace::Trace;

use errors::Result;
use trace::Progress;


const HELP_STR: &'static str = r#"
Run the hashsplitter and marshaller on a single file, and then write all resulting objects to the local store.

Written objects can be found in .attaca/blobs. For some object with a given hash, say 0a1b2c3d..., the resulting path will be .attaca/blobs/0a/1b/2c3d... as blobs are stored in a directory structure like so: [0]/[1]/[2..32] where [n] represents the hex representation of the nth byte of the hash of the object.
"#;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("file")
        .about(
            "Chunk and marshal a single file, writing all resulting objects to disk.",
        )
        .after_help(HELP_STR)
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


fn marshal<T: Trace, P: AsRef<Path>>(
    repository: &mut Repository,
    trace: T,
    path: P,
) -> Result<ObjectHash> {
    let mut context = repository.local(trace)?;
    let chunk_stream = context.split_file(path);
    let hash_future = context.hash_file(chunk_stream);
    let write_future = context.close();

    let ((), hash) = write_future.join(hash_future).wait()?;

    Ok(hash)
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let path = matches.value_of("INPUT").unwrap();

    let hash = if matches.is_present("quiet") {
        marshal(repository, (), path)?
    } else {
        marshal(repository, Progress::new(None), path)?
    };

    println!("{}", hash);

    Ok(())
}
