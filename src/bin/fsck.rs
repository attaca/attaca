use std::collections::HashSet;
use std::fmt::Write;

use clap::{App, SubCommand, Arg, ArgMatches};
use futures::prelude::*;
use futures::stream;

use attaca::marshal::{self, Object, DataObject, SubtreeEntry};
use attaca::Repository;

use errors::*;


#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum Depth {
    Commit,
    Subtree,
    Data,
}


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("fsck")
        .about("Verify repository hashes.")
        .arg(
            Arg::with_name("depth")
                .help(
                    "Controls what objects' hashes are verified. Deeper \n\
                     objects will always have their parent objects' hashes verified.",
                )
                .next_line_help(true)
                .short("d")
                .long("depth")
                .possible_values(&["commit", "subtree", "data"])
                .default_value("commit"),
        )
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let depth = match matches.value_of("depth").unwrap() {
        "commit" => Depth::Commit,
        "subtree" => Depth::Subtree,
        "data" => Depth::Data,
        _ => panic!("clap verification failure!"),
    };

    let errors = {
        let ctx = repository.local(())?;

        let mut errors = Vec::new();
        let mut hashes = ctx.refs.head().into_iter().collect::<Vec<_>>();
        let mut visited = HashSet::new();

        while !hashes.is_empty() {
            let object_stream = {
                let next_hashes = hashes.drain(..).filter(|&hash| visited.insert(hash));
                stream::futures_unordered(next_hashes.map(|hash| {
                    ctx.read_object(hash).map(move |object| (hash, object))
                }))
            };

            object_stream
                .from_err::<Error>()
                .for_each(|(hash, object)| {
                    let real_hash = marshal::hash(&object);

                    if hash != real_hash {
                        errors.push(Error::from_kind(ErrorKind::FsckFailure(hash, real_hash)));
                    }

                    match object {
                        Object::Data(DataObject::Large(ref large_object))
                            if depth >= Depth::Data => {
                            hashes.extend(large_object.children.iter().map(|&(_, hash)| hash));
                        }
                        Object::Subtree(ref subtree_object) if depth >= Depth::Subtree => {
                            hashes.extend(subtree_object.entries.iter().filter_map(
                                |(_, entry)| match *entry {
                                    SubtreeEntry::File(hash, _) if depth >= Depth::Data => Some(
                                        hash,
                                    ),
                                    SubtreeEntry::Subtree(hash) => Some(hash),
                                    _ => None,
                                },
                            ));
                        }
                        Object::Commit(ref commit_object) if depth >= Depth::Commit => {
                            hashes.extend(commit_object.parents.iter().cloned());
                            if depth >= Depth::Subtree {
                                hashes.push(commit_object.subtree);
                            }
                        }
                        _ => {}
                    }

                    Ok(())
                })
                .wait()?;
        }

        ctx.close().wait()?;

        errors
    };

    if errors.is_empty() {
        println!("No errors detected!");
    } else {
        let mut buf = String::new();
        writeln!(buf, "Oh no! {} errors detected:", errors.len())?;

        for ek in errors {
            writeln!(buf, "\t{}", ek)?;
        }

        println!("{}", buf);
    }

    Ok(())
}
