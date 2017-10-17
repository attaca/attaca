use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;
use attaca::index::Cached;
use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("status").about("Show repository status, including tracked/added files.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    repository.index.update()?;

    let catalog = repository.catalogs.get(None)?;
    println!("{} local objects.", catalog.len());

    let mut added = Vec::new();
    let mut tracked = Vec::new();

    for (path, entry) in repository.index.iter() {
        if entry.tracked {
            tracked.push((path, entry));
        } else if entry.added {
            added.push((path, entry));
        }
    }

    added.sort_unstable_by_key(|&(path, _)| path);
    tracked.sort_unstable_by_key(|&(path, _)| path);

    println!("Tracked:");

    for (path, entry) in tracked {
        match entry.cached {
            Cached::Hashed(hashed) => {
                println!(
                    "\t[{:?}] {} Hashed({})",
                    entry.hygiene,
                    path.display(),
                    &hashed.to_string()[..6]
                )
            }
            Cached::Unhashed => println!("\t[{:?}] {} Unhashed", entry.hygiene, path.display()),
            Cached::Removed => println!("\t[{:?}] {} Removed", entry.hygiene, path.display()),
        }
    }

    println!("Added:");

    for (path, entry) in added {
        match entry.cached {
            Cached::Hashed(hashed) => {
                println!(
                    "\t[{:?}] {} Hashed({})",
                    entry.hygiene,
                    path.display(),
                    &hashed.to_string()[..6]
                )
            }
            Cached::Unhashed => println!("\t[{:?}] {} Unhashed", entry.hygiene, path.display()),
            Cached::Removed => println!("\t[{:?}] {} Removed", entry.hygiene, path.display()),
        }
    }


    Ok(())
}
