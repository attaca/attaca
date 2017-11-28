use std::env;
use std::path::Path;

use failure::{self, Error};
use futures::prelude::*;

use attaca::object::{Object, SubtreeEntry};
use attaca::object_hash::ObjectHash;
use attaca::repository::Repository;
use attaca::workspace::Workspace;


pub fn init() -> Result<(), Error> {
    let path = env::current_dir()?;
    init_at(&path)
}

pub fn init_at<P: ?Sized + AsRef<Path>>(path: &P) -> Result<(), Error> {
    Workspace::init(path.as_ref().to_owned())
}

pub fn load() -> Result<Workspace, Error> {
    let path = env::current_dir()?;
    load_at(&path)
}

pub fn load_at<P: ?Sized + AsRef<Path>>(path: &P) -> Result<Workspace, Error> {
    Workspace::open(path.as_ref().to_owned())
}

#[derive(Debug)]
pub struct CheckoutOptions<'a> {
    force: bool,
    sub_path: Option<&'a Path>,
}

impl<'a> CheckoutOptions<'a> {
    pub fn new() -> Self {
        Self {
            force: false,
            sub_path: None,
        }
    }

    pub fn force(&mut self, force: bool) -> &mut Self {
        self.force = force;
        self
    }

    pub fn sub_path(&mut self, sub_path: Option<&'a Path>) -> &mut Self {
        self.sub_path = sub_path;
        self
    }
}

pub fn checkout<R: Repository>(
    commit_hash: ObjectHash,
    workspace: &mut Workspace,
    repository: &R,
    options: &CheckoutOptions,
) -> Result<(), Error> {
    let path = options.sub_path.unwrap_or(Path::new(""));

    let mut subtree_hash = match repository.read_object(commit_hash).wait()?.unwrap() {
        Object::Commit(commit_object) => commit_object.subtree,
        _ => return Err(failure::err_msg("Not a commit!")),
    };

    for component in path.iter() {
        subtree_hash = match repository.read_object(subtree_hash).wait()? {
            Some(Object::Subtree(subtree_object)) => match subtree_object.entries.get(component) {
                Some(&SubtreeEntry::Subtree(subtree_hash)) => subtree_hash,
                Some(_) => return Err(failure::err_msg("Not a subtree!")),
                None => return Err(failure::err_msg("No such subtree entry!")),
            },
            Some(_) => return Err(failure::err_msg("Not a subtree!")),
            None => return Err(failure::err_msg("No such subtree!")),
        };
    }

    let diff = workspace.diff_directory_to_subtree(path, repository, subtree_hash)?;

    if !options.force && diff.is_dirty() {
        Err(failure::err_msg(
            "Must use force to overwrite uncommitted changes!",
        ))
    } else {
        workspace.write_subtree(path, repository, subtree_hash)?;

        Ok(())
    }
}
