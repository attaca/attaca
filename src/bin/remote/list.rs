use std::fmt::Write;

use clap::{App, SubCommand, ArgMatches};

use attaca::repository::{ObjectStoreCfg, Repository};

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("list").about("List all remotes of a repository.")
}


pub fn go(repository: &mut Repository, _matches: &ArgMatches) -> Result<()> {
    for (name, remote) in repository.config.remotes.iter() {
        match remote.object_store {
            ObjectStoreCfg::Ceph(ref ceph_cfg) => {
                if let Some(ref ceph_conf) = ceph_cfg.conf_file.as_ref() {
                    println!("{}: ceph.conf path `{}`", name, ceph_conf.display());
                } else if let Some(ref hosts) = ceph_cfg.conf_options.get("mon_host") {
                    println!("{}: from hosts `{}`", name, hosts);
                } else {
                    println!("{}: no `mon_host` or ceph.conf entry", name);
                }
            }
            ObjectStoreCfg::Ssh(ref ssh_cfg) => {
                let mut out = String::new();
                write!(
                    out,
                    "{}: {}@{}",
                    name,
                    ssh_cfg.username,
                    ssh_cfg.address.ip()
                )?;
                if ssh_cfg.address.port() != 22 {
                    write!(out, ":[{}]", ssh_cfg.address.port())?;
                }
                println!("{}", out);
            }
        }
    }

    Ok(())
}
