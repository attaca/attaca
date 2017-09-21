# `attaca` - prototype for a distributed version control system for extremely large quantities of data

This tool is under development and not in working condition!

A working installation of Rust and Cargo are required to build/install. These
can be acquired through [rustup](https://www.rustup.rs/). Rust nightly is
needed to build this tool for the `offset_to` feature. Rustup can be configured
to override the local Rust version for building this crate; installation and
override will look something like this:

```
curl https://sh.rustup.rs -sSf | sh
rustup install nightly
rustup override set nightly
```

Other dependencies are an installation of the librados-2 library, specifically
the development files.  These can be installed with:

```
sudo apt-get librados-dev    # Ubuntu (tested on 14.04 LTS)
dnf install librados2-devel  # Fedora (tested on Fedora 24)
```

Testing requires an installation of Docker. Once Rust, Cargo, and other
dependencies are installed, `attaca` can be compiled and installed with:

```
cargo install
```

## Commands

```
attaca init                         # Initialize a repository in the current directory.
attaca test chunk   <INPUT>         # Hashsplit a file and print chunk statistics.
attaca test marshal <INPUT>         # Split and marshal a file, and then write its chunks to disk in the local blob store.
attaca test suite noop              # Test the "suite" machinery - with no options this will result in spinning up and then shutting down a local RADOS cluster for testing.
attaca test suite write_all <INPUT> # Test hashsplitting, chunking, and then sending a file into a local RADOS cluster.
```

For more information, try running the above with `--help` or as `attaca help [SUBCOMMAND]`.
