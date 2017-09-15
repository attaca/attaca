use std::env;
use std::ffi::CString;
use std::path::Path;

use clap::{App, SubCommand, Arg, ArgMatches};

use attaca::context::{Files, Context};
use attaca::repository::{Repository, RemoteCfg, RadosCfg, EtcdCfg};
use attaca::trace::Trace;

use errors::*;
use trace::ProgressTrace;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("write_all")
        .help("Chunk, marshal, and then send a file to a remote.")
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


fn run<P: AsRef<Path>, T: Trace>(conf_dir: P, matches: &ArgMatches, trace: T) -> Result<()> {
    let path = matches.value_of("INPUT").unwrap();
    let conf = conf_dir.as_ref().join("ceph.conf");
    let keyring = conf_dir.as_ref().join("ceph.client.admin.keyring");

    let wd = env::current_dir()?;
    let repo = Repository::find(wd).chain_err(
        || "unable to find repository",
    )?;
    let mut context = Context::with_trace(repo, trace);
    let mut files = Files::new();

    let chunked = context.chunk_file(&mut files, path).chain_err(
        || "unable to chunk file",
    )?;
    let (_hash, marshalled) = context.marshal_file(chunked);

    let mut remote_ctx = context
        .with_remote_from_cfg(
            None,
            RemoteCfg {
                object_store: RadosCfg {
                    conf,
                    pool: CString::new("rbd").unwrap(),
                    user: CString::new("admin").unwrap(),
                    keyring: Some(keyring),
                },

                ref_store: EtcdCfg { cluster: Vec::new() },
            },
        )
        .chain_err(|| "unable to open remote context")?;

    remote_ctx.write_marshalled(&marshalled).chain_err(
        || "unable to write marshalled file",
    )?;

    Ok(())
}


pub fn go<P: AsRef<Path>>(conf_dir: P, matches: &ArgMatches) -> Result<()> {
    if matches.is_present("quiet") {
        run(conf_dir, matches, ())
    } else {
        run(conf_dir, matches, ProgressTrace::new())
    }
}
