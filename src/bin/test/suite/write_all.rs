use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

use clap::{App, SubCommand, Arg, ArgMatches};
use futures::prelude::*;

use attaca::context::Context;
use attaca::repository::Repository;
use attaca::trace::Trace;

use errors::*;
use trace::ProgressTrace;


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
            Arg::with_name("repository")
                .help(
                    "Overrides the current directory as a target repository for this test.",
                )
                .long("repository")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("quiet")
                .help("Don't display progress.")
                .short("q")
                .long("quiet"),
        )
}


fn run<P: AsRef<Path>, T: Trace>(conf_dir: P, matches: &ArgMatches, trace: T) -> Result<()> {
    let path = matches.value_of("INPUT").unwrap();
    let conf = conf_dir.as_ref().join("ceph.conf");
    let keyring = conf_dir.as_ref().join("ceph.client.admin.keyring");

    let _ = ::execute(vec![
        "attaca",
        "remote",
        "add",
        "_debug",
        "--ceph",
        "--ceph-conf",
        &conf.display().to_string(),
        "--ceph-keyring",
        &keyring.display().to_string(),
    ]);

    let wd = matches
        .value_of("repository")
        .map(PathBuf::from)
        .unwrap_or_else(|| env::current_dir().unwrap());
    let repo = Repository::find(wd).chain_err(
        || "unable to find repository",
    )?;
    let mut context = Context::with_trace(repo, trace);
    let mut batch = context.with_batch();

    let chunked = batch.chunk_file(path).chain_err(|| "unable to chunk file")?;
    let hash_future = batch.marshal_file(chunked);

    let mut options = HashMap::new();
    options.insert("keyring".to_owned(), keyring.to_string_lossy().into_owned());

    let remote_ctx = context.with_remote("_debug").chain_err(
        || "unable to open remote context",
    )?;

    let batch_future = remote_ctx.write_batch(batch);

    batch_future.join(hash_future).wait()?;

    Ok(())
}


pub fn go<P: AsRef<Path>>(conf_dir: P, matches: &ArgMatches) -> Result<()> {
    if matches.is_present("quiet") {
        run(conf_dir, matches, ())
    } else {
        run(conf_dir, matches, ProgressTrace::new())
    }
}
