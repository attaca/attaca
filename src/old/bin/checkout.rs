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
    SubCommand::with_name("checkout")
        .help(
            "Checkout a file or working directory from a previous revision.",
        )
        .arg(Arg::with_name("COMMIT").index(1).required(true).help(
            "The commit hash to checkout.",
        ))
}


fn write_data_object<T: Trace, S: ObjectStore>(
    ctx: &Context<T, S>,
    path: PathBuf,
    object_hash: ObjectHash,
    size: u64,
) -> Result<()> {
    let mut parent = path.clone();
    assert!(parent.pop());
    fs::create_dir_all(parent)?;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(size)?;

    // PermissionError indicates mmap opened with wrong permissions.
    let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;

    let slice = unsafe { mmap.as_mut_slice() };
    let mut stack = vec![(0u64, object_hash)];
    while let Some((mut offset, object_hash)) = stack.pop() {
        match ctx.read_object(object_hash).wait()? {
            Object::Data(DataObject::Small(small_object)) => {
                slice[offset as usize..].copy_from_slice(&small_object.chunk);
            }
            Object::Data(DataObject::Large(large_object)) => {
                for (child_size, child_hash) in large_object.children {
                    stack.push((child_size, child_hash));
                    offset += child_size;
                }
            }
            _ => bail!("meep"),
        }
    }

    Ok(())
}


// TODO: Tree diff in order to remove files.
pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let commit_hash = matches.value_of("COMMIT").unwrap().parse()?;

    {
        let ctx = repository.local(())?;
        let commit = ctx.read_object(commit_hash).wait()?;
        let commit_object = match commit {
            Object::Commit(commit_object) => commit_object,
            _ => bail!(ErrorKind::NotACommit(commit_hash)),
        };

        let mut stack = vec![(PathBuf::new(), commit_object.subtree)];
        while let Some((path, object)) = stack.pop() {
            match ctx.read_object(object).wait()? {
                Object::Subtree(SubtreeObject { entries }) => {
                    for (component, entry) in entries {
                        let joined = path.join(component);

                        match entry {
                            SubtreeEntry::File(object_hash, size) => {
                                write_data_object(&ctx, joined, object_hash, size)
                                    .chain_err(|| "While trying to write file")?;
                            }
                            SubtreeEntry::Subtree(object_hash) => {
                                stack.push((joined, object_hash));
                            }
                        }
                    }
                }
                _ => bail!("Invalid subtree!"),
            }
        }

        ctx.close().wait()?;

        Ok(())
    }
}
