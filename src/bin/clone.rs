use std::fs::{self, OpenOptions};
use std::path::PathBuf;

use clap::{App, SubCommand, Arg, ArgMatches};
use memmap::{Mmap, Protection};
use futures::prelude::*;

use attaca::context::Context;
use attaca::marshal::{ObjectHash, Object, DataObject, SubtreeObject, SubtreeEntry};
use attaca::repository::Repository;
use attaca::store::ObjectStore;
use attaca::trace::Trace;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("clone")
        .help(
            "Clone a remote repository."
        )
        .arg(Arg::with_name("URL").index(1).required(true).help(
            "The URL of the remote repository."
        ))
}


// TODO: Tree diff in order to remove files.
pub fn go(matches: &ArgMatches) -> Result<()> {
    unimplemented!()
}
