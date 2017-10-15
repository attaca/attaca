use std::collections::HashMap;
use std::path::PathBuf;

use clap::{App, SubCommand, Arg, ArgGroup, ArgMatches};
use itertools::Itertools;

use attaca::repository::{RemoteCfg, CephCfg, EtcdCfg, Repository};

use errors::*;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("add")
        .about("Add a remote to a repository.")
        .arg(
            Arg::with_name("NAME")
                .help("The short name of the repository.")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("etcd")
                .long("etcd")
                .requires("etcd-config")
                .help("Declare a repository using an etcd cluster as a ref store."),
        )
        .group(ArgGroup::with_name("ref-store").required(false))
        .arg(
            Arg::with_name("ceph")
                .long("ceph")
                .requires("ceph-config")
                .help(
                    "Declare a repository using a Ceph cluster as an object store.",
                ),
        )
        .group(
            ArgGroup::with_name("object-store")
                .args(&["ceph"])
                .required(true),
        )
        .arg(
            Arg::with_name("etcd-host")
                .long("etcd-host")
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .requires("etcd")
                .help("Supply an etcd cluster member to connect to."),
        )
        .group(
            ArgGroup::with_name("etcd-config")
                .requires("etcd")
                .args(&["etcd-host"])
                .required(false),
        )
        .arg(
            Arg::with_name("ceph-pool")
                .long("ceph-pool")
                .requires("ceph")
                .default_value("rbd")
                .help("The pool of the Ceph cluster to use for objects."),
        )
        .arg(
            Arg::with_name("ceph-user")
                .long("ceph-user")
                .requires("ceph")
                .default_value("admin")
                .help("The username to use when connecting to the Ceph cluster."),
        )
        .arg(
            Arg::with_name("ceph-mon-host")
                .long("ceph-mon-host")
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .requires("ceph")
                .help("Supply a Ceph monitor host IP to connect to."),
        )
        .arg(
            Arg::with_name("ceph-conf")
                .long("ceph-conf")
                .takes_value(true)
                .requires("ceph"),
        )
        .arg(
            Arg::with_name("ceph-keyring")
                .long("ceph-keyring")
                .takes_value(true)
                .requires("ceph"),
        )
        .group(ArgGroup::with_name("ceph-config").requires("ceph").args(
            &[
                "ceph-mon-host",
                "ceph-conf",
            ],
        ))
}


fn parse_ceph_object_store(matches: &ArgMatches) -> Result<CephCfg> {
    let pool = matches.value_of("ceph-pool").unwrap().to_owned();
    let user = matches.value_of("ceph-user").unwrap().to_owned();

    let mut conf_options = HashMap::new();

    if let Some(mut hosts) = matches.values_of("ceph-mon-host") {
        let value = hosts.join(",");
        conf_options.insert("mon_host".to_owned(), value);
    }

    if let Some(keyring) = matches.value_of("ceph-keyring") {
        conf_options.insert("keyring".to_owned(), keyring.to_owned());
    }

    let conf_file = matches.value_of("ceph-conf").map(PathBuf::from);

    Ok(CephCfg {
        conf_file,
        conf_options,
        pool,
        user,
    })
}


fn parse_object_store(matches: &ArgMatches) -> Result<CephCfg> {
    if matches.is_present("ceph") {
        parse_ceph_object_store(matches)
    } else {
        unreachable!("CLAP validation failure")
    }
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let name = matches.value_of("NAME").unwrap().to_owned();

    if repository.config.remotes.contains_key(&name) {
        bail!("Remote {} already exists!", name);
    }

    let object_store = parse_object_store(matches)?;

    repository.config.remotes.insert(
        name,
        RemoteCfg {
            object_store,
            ref_store: EtcdCfg::default(),
        },
    );

    // repository writes config on drop.

    Ok(())
}
