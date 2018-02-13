use std::path::Path;

use clap::{App, Arg, SubCommand, ArgMatches};
use futures::prelude::*;
use futures::stream::FuturesUnordered;

use attaca::Repository;
use attaca::marshal::{ObjectHash, SubtreeEntry};
use attaca::trace::Trace;

use errors::*;
use trace::Progress;


const HELP_STR: &'static str = r#"
Run the hashsplitter and marshaller on all files in a directory, and then write all resulting objects to the local store.

Written objects can be found in .attaca/blobs. For some object with a given hash, say 0a1b2c3d..., the resulting path will be .attaca/blobs/0a/1b/2c3d... as blobs are stored in a directory structure like so: [0]/[1]/[2..32] where [n] represents the hex representation of the nth byte of the hash of the object.
"#;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("subtree")
        .about(
            "Recursively chunk and marshal all files in a directory, writing all resulting objects to disk.",
        )
        .after_help(HELP_STR)
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input directory to marshal.")
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
    let context = repository.local(trace)?;

    let mut entries = FuturesUnordered::new();
    let mut stack = Vec::new();

    let absolute_path = if path.as_ref().is_absolute() {
        path.as_ref().to_owned()
    } else {
        context.paths.base.join(path.as_ref())
    };

    stack.push(absolute_path.read_dir()?);

    while let Some(iter) = stack.pop() {
        for dir_entry_res in iter {
            let dir_entry = dir_entry_res?;
            let absolute_path = dir_entry.path();

            if absolute_path.symlink_metadata()?.is_dir() {
                stack.push(absolute_path.read_dir()?);
            } else {
                let size = absolute_path.metadata()?.len();
                let chunk_stream = context.split_file(&absolute_path);
                let relative_path = absolute_path
                    .strip_prefix(&context.paths.base)
                    .unwrap()
                    .to_owned();
                entries.push(context.write_file(chunk_stream).map(move |hash| {
                    (relative_path, SubtreeEntry::File(hash, size))
                }));
            }
        }
    }

    let hash_future = context.write_subtree(entries);
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
