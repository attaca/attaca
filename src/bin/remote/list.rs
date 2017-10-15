use clap::{App, SubCommand, ArgMatches};

use attaca::Repository;

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("list").about("List all remotes of a repository.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    for (name, remote) in repository.config.remotes.iter() {
        if let Some(ref ceph_conf) = remote.object_store.conf_file.as_ref() {
            println!("{}: ceph.conf path `{}`", name, ceph_conf.display());
        } else if let Some(ref hosts) = remote.object_store.conf_options.get("mon_host") {
            println!("{}: from hosts `{}`", name, hosts);
        } else {
            println!("{}: no `mon_host` or ceph.conf entry", name);
        }
    }

    Ok(())
}
