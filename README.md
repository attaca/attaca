# `attaca` - prototype for a distributed version control system for extremely large quantities of data

This tool is under development and not in working condition!

A working installation of Rust and Cargo are required to build/install. These
can be acquired through [rustup](https://www.rustup.rs/). Rust nightly is
needed to build this tool for the several nightly features: `offset_to`,
`proc_macro`, `conservative_impl_trait`, and `generators`. The latter three are
required for the `futures-await` crate. Rustup can be configured to override
the local Rust version for building this crate; installation and override will
look something like this:

```
curl https://sh.rustup.rs -sSf | sh
rustup install nightly
rustup override set nightly
```

Other dependencies are an installation of the librados-2 library, specifically
the development files. *Make sure to install Ceph Kraken or later!* These can
be installed with:

```
# Ubuntu (tested on 14.04 LTS)
sudo apt-add-repository "deb https://download.ceph.com/debian-luminous/ `lsb_release -sc` main"
sudo apt-get librados-dev

# Fedora (tested on Fedora 24)
dnf install librados2-devel
```

Testing requires an installation of Docker. Once Rust, Cargo, and other
dependencies are installed, `attaca` can be compiled and installed with:

```
cargo install
```

## Commands

```
attaca init                         # Initialize a repository in the current directory.
attaca remote add <NAME> --ceph --ceph-mon-host 127.0.0.1 --ceph-user admin --ceph-pool rbd
                                    # Add a new remote from bare Ceph options.
attaca remote add <NAME> --ceph --ceph-conf ./ceph.conf
                                    # Add a new remote from a ceph.conf file.
attaca remote list                  # List all remotes for the current repository.
attaca test chunk   <INPUT>         # Hashsplit a file and print chunk statistics.
attaca test marshal <INPUT>         # Split and marshal a file, and then write its chunks to disk in the local blob store.
attaca test suite noop              # Test the "suite" machinery - with no options this will result in spinning up and then shutting down a local RADOS cluster for testing.
attaca test suite write_all <INPUT> # Test hashsplitting, chunking, and then sending a file into a local RADOS cluster.
attaca utils read <HASH> [--dump]   # Get information about a specific object, and/or dump the whole object to stdout.
```

For more information, try running the above with `--help` or as `attaca help [SUBCOMMAND]`.
