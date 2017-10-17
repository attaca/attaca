use clap::{App, SubCommand, Arg, ArgMatches};
use futures::prelude::*;

use attaca::marshal::{ObjectHash, Object, DataObject};
use attaca::Repository;
use attaca::Store;

use errors::*;


#[derive(Debug)]
pub enum Pretty {
    Small { size: u64 },
    Large { size: u64, children: usize },
    Subtree { entries: usize },
    Commit,
}


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("read")
        .about("Read an object from either the local or remote store.")
        .arg(Arg::with_name("OBJECT").index(1))
        .arg(
            Arg::with_name("remote")
                .takes_value(true)
                .short("r")
                .long("remote")
                .value_name("REMOTE"),
        )
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let object_hash = value_t!(matches.value_of("OBJECT"), ObjectHash)?;

    let object = match matches.value_of("remote") {
        Some(remote) => {
            repository
                .remote(remote, ())?
                .store()
                .read_object(object_hash)
                .wait()?
        }
        None => {
            repository
                .local(())?
                .store()
                .read_object(object_hash)
                .wait()?
        }
    };

    let pretty = match object {
        Object::Data(ref data_object) => {
            match *data_object {
                DataObject::Small(ref small_object) => Pretty::Small { size: small_object.size() },
                DataObject::Large(ref large_object) => Pretty::Large {
                    size: large_object.size(),
                    children: large_object.children.len(),
                },
            }
        }
        Object::Subtree(ref subtree_object) => Pretty::Subtree {
            entries: subtree_object.entries.len(),
        },
        Object::Commit(ref _commit_object) => Pretty::Commit,
    };

    println!("{:#?}", pretty);

    Ok(())
}
