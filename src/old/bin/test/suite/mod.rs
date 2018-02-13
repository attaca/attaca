mod write_all;

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::os::unix::io::{AsRawFd, FromRawFd};

use clap::{App, SubCommand, Arg, ArgMatches};

use attaca::Repository;

use errors::*;


const HELP_STR: &'static str = r#"
Run a full test with a remote.

This will make calls to the outside environment and assumes you are running on a Unix-like system
with cat and git installed. As such there is no way it will currently work on Windows, at least not
without a significant amount of work.

This subcommand also relies on a set of scripts which it will automatically clone from a remote git
repository. These scripts are used to start, manage, and shutdown a makeshift RADOS cluster for
testing, when necessary.

TODO: Allow the user to pass the IP of a RADOS cluster to use for this test. Currently, we simply
use 127.0.0.1, which works with the test cluster scripts.
"#;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("suite")
        .about("Run a test which requires a remote repository.")
        .after_help(HELP_STR)
        .arg(Arg::with_name("fetch-tools")
             .long("fetch-tools")
             .takes_value(true)
             .min_values(0)
             .value_name("TOOL_PATH")
             .help("Download tools for running a containerized RADOS cluster and place them into a specified directory (requires git.)"))
        .arg(Arg::with_name("with-tools")
             .short("w")
             .long("with-tools")
             .takes_value(true)
             .value_name("TOOL_PATH")
             .conflicts_with("fetch-tools")
             .help("Specify a directory in which to find insta-RADOS scripts."))
        .arg(Arg::with_name("no-setup")
            .long("no-setup")
            .help("Do not run the 'start.sh' script, relying on an already running RADOS container."))
        .arg(Arg::with_name("no-takedown")
             .long("no-takedown")
             .help("Do not run the 'stop.sh' script, leaving a RADOS container running."))
        .subcommand(SubCommand::with_name("noop").about(
            "Test the test suite infrastructure. I.S.M.E.T.A.",
        ))
        .subcommand(write_all::command())
}


fn fetch_tools<P: AsRef<Path>>(ir_path: P) -> Result<()> {
    let status = Command::new("git")
        .args(
            &[
                "clone",
                "https://github.com/sdleffler/insta-rados",
                ir_path.as_ref().to_str().unwrap(),
            ],
        )
        .status()
        .chain_err(|| "error while running fetch command")?;

    if !status.success() {
        bail!("failure to fetch RADOS test cluster setup tools");
    }

    Ok(())
}


fn run_setup<P: AsRef<Path>>(ir_path: P) -> Result<()> {
    let cat = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let cat_stdin_fd = cat.stdin.as_ref().unwrap().as_raw_fd();

    let start_path = ir_path.as_ref().join("start.sh");

    let output = Command::new(&start_path)
        .stdout(unsafe { Stdio::from_raw_fd(cat_stdin_fd) })
        .stderr(unsafe { Stdio::from_raw_fd(cat_stdin_fd) })
        .output()
        .chain_err(|| {
            format!("unable to execute setup script '{}'", start_path.display())
        })?;

    let cat_output = cat.wait_with_output()?;

    if !output.status.success() {
        bail!(
            "failure to start RADOS test cluster: {}",
            String::from_utf8_lossy(&cat_output.stdout)
        );
    }

    Ok(())
}


fn run_takedown<P: AsRef<Path>>(ir_path: P) -> Result<()> {
    let cat = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let cat_stdin_fd = cat.stdin.as_ref().unwrap().as_raw_fd();

    let stop_path = ir_path.as_ref().join("stop.sh");

    let output = Command::new(&stop_path)
        .stdout(unsafe { Stdio::from_raw_fd(cat_stdin_fd) })
        .stderr(unsafe { Stdio::from_raw_fd(cat_stdin_fd) })
        .output()
        .chain_err(|| {
            format!(
                "unable to execute takedown script '{}'",
                stop_path.display()
            )
        })?;

    let cat_output = cat.wait_with_output()?;

    if !output.status.success() {
        bail!(
            "failure to takedown RADOS test cluster: {}",
            String::from_utf8_lossy(&cat_output.stdout)
        );
    }

    Ok(())
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    if let (subcmd, Some(sub_m)) = matches.subcommand() {
        let ir_path = matches
            .value_of("fetch-tools")
            .map(PathBuf::from)
            .or_else(|| matches.value_of("with-tools").map(PathBuf::from))
            .unwrap_or_else(|| env::current_dir().unwrap().join("insta-rados"));

        if matches.is_present("fetch-tools") {
            fetch_tools(&ir_path)?;
        }

        if !matches.is_present("no-setup") {
            println!("Running setup...");

            run_setup(&ir_path)?;

            println!("Setup finished.");
        }

        let ceph_path = ir_path.join("ceph");

        println!(
            "Running test suite: '{}', with ceph directory '{}'.",
            subcmd,
            ceph_path.display()
        );

        let result = match subcmd {
            "noop" => Ok(()),
            "write_all" => write_all::go(repository, &ceph_path, sub_m),
            _ => unreachable!("Invalid subcommand {}", subcmd),
        }.chain_err(|| format!("run test suite '{}'", subcmd));

        if !matches.is_present("no-takedown") {
            println!("Running takedown...");

            run_takedown(&ir_path)?;

            println!("Takedown finished.");
        }

        result
    } else {
        bail!(ErrorKind::InvalidUsage);
    }
}
