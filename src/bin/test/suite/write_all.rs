use std::path::Path;

use clap::{App, SubCommand, Arg, ArgMatches};
use futures::prelude::*;

use attaca::Repository;
use attaca::trace::Trace;

use errors::*;
use trace::Progress;


const HELP_STR: &'static str = r#"
This test will first hashsplit and marshal a file; then, it will attempt to connect to a remote and
send all objects created in the process of marshalling. By default, it will display the progress
made in a progress bar; if this test is being used as part of a larger test suite, it may be a good
idea to run with with the --quiet option, which will suppress all output.
"#;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("write_all")
        .about("Chunk, marshal, and then send a file to a remote.")
        .after_help(HELP_STR)
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file.")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("quiet")
                .help("Don't display progress.")
                .short("q")
                .long("quiet"),
        )
}


fn run<T: Trace>(repository: &mut Repository, matches: &ArgMatches, trace: T) -> Result<()> {
    let path = matches.value_of("INPUT").unwrap();
    let mut context = repository.remote("_debug", trace)?;

    let chunk_stream = context.split_file(path);
    let hash_future = context.hash_file(chunk_stream);
    let write_future = context.close();

    write_future.join(hash_future).wait()?;

    Ok(())
}


pub fn go<P: AsRef<Path>>(
    repository: &mut Repository,
    conf_dir: P,
    matches: &ArgMatches,
) -> Result<()> {
    let conf = conf_dir.as_ref().join("ceph.conf");
    let keyring = conf_dir.as_ref().join("ceph.client.admin.keyring");

    // HACK: Ignore errors if adding the remote fails (it will fail if the remote's already
    // there.)
    let _ = ::remote::add::go(
        repository,
        &::remote::add::command().get_matches_from_safe(
            &[
                "add".as_ref(),
                "_debug".as_ref(),
                "--ceph".as_ref(),
                "--ceph-conf".as_ref(),
                conf.as_os_str(),
                "--ceph-keyring".as_ref(),
                keyring.as_os_str(),
            ],
        )?,
    );

    if matches.is_present("quiet") {
        run(repository, matches, ())
    } else {
        run(
            repository,
            matches,
            Progress::new(Some("_debug".to_string())),
        )
    }
}
