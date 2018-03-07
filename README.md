# Attaca/Subito: distributed version control for extremely large files and repositories

## What is this?

Attaca is a generic content addressable storage system with support for
upgradeable hashes and hashsplitting, providing both futureproofing and
improved sharing of stored data. On top of it is Subito, a Git-like distributed
version control system using Attaca as an object store.

## Why is this?

There are currently few utilities out there to version ridiculously large
files; for example, a data scientist might want to version their fifty- or one
hundred-gigabyte dataset alongside their scripts and analysis code. Most
solutions to this problem are thrown together haphazardly or hacked together
out of existing version control solutions which were never designed to handle
the load. Attaca - and Subito - are intended for data management of this scale,
in an efficient, robust, and ergonomic manner.

## How do I build it?

A working installation of Rust and Cargo are required to build/install. These
can be acquired through [rustup](https://www.rustup.rs/). Rust nightly is
needed to build this tool for the several nightly features: 

```rust
#![feature(proc_macro, conservative_impl_trait, generators, from_utf8_error_as_bytes)]
```

The first three are the main reason we use Rust nightly; they are necessary to
make use of the Rust `futures-await` crate, which provides procedural
macro-based async/await syntax. The latter is useful for extracting error data
from attempts to convert arbitrary bytes to UTF-8, and will probably be
stabilized very shortly, long before `proc_macro`, `conservative_impl_trait`,
and `generators`.

Rustup can be configured to override
the local Rust version for building this crate; installation and override will
look something like this:

```
# Currently tested nightly build is nightly-2018-02-14. First, grab the rustup
# Rust install management tool.
curl https://sh.rustup.rs -sSf | sh
rustup install nightly-2018-02-14

# Run this command inside the repository to tell rustup that all cargo commands
# run inside should be using our particular nightly.
rustup override set nightly-2018-02-14
```

Other dependencies are:

- librados-2 *N.B. make sure to install Ceph Kraken or later!*
- capnp
- libsnappy
- libleveldb

Installation might look something like this:

```
# Ubuntu (tested on 14.04 LTS)
sudo apt-add-repository "deb https://download.ceph.com/debian-luminous/ `lsb_release -sc` main"
sudo apt-get install librados-dev capnproto libleveldb-dev libsnappy-dev

# Fedora (tested on Fedora 24)
dnf install librados2-devel capnproto
```

Testing requires an installation of Docker. Once Rust, Cargo, and other
dependencies are installed, `attaca` can be compiled and installed with:

```
cargo install
```

## How do I use it?

Attaca/Subito are *not* currently recommended for use or anywhere near
feature-complete! However, they *do* have some functionality as of now.

Subito's command line interface looks quite Git-like, with a few major
differences. Here are some of the commands we currently support. Note that they
are without exception all missing some major functionality:

- `subito init` works at a bare minimum.
- `subito stage` and `subito unstage` act very similarly to `git add` and `git
  reset`. These are responsible for modifying the "candidate tree", also called
  the "virtual workspace". In Git this is the index, but with Subito, the index
  is itself a tree in the object store. Among other things, this opens up some
  interesting possibilities for undo-tracking.
- `subito commit` acts very similarly to `git commit`.
- `subito checkout` acts very similarly to `git checkout`.
- `subito fetch` is mainly exposed for debugging purposes, but it's there, and
  it fills the same niche as `git fetch`.
- `subito show` works for showing information about subtrees of refs pointing
  to commits.
- `subito fsck` works for checking integrity. Currently the only hash supported
  is SHA3-256, but more will come (but only the hashes supported by a given
  repository may be verified, because generating hashes on the fly instead of
  checking expected hashes would not help with verification.)
- `subito remote` allows adding and listing remotes.
- `subito branch` allows creating and listing branches.
- `subito status` shows information *only about the differences between the
  candidate tree and previous commit.* It will soon show information about
  changed local files, but likely not untracked files.

## What are all these crates?

| Crate name | Description |
| ---------- | ----------- |
| `attaca` | Core traits and generic functionality for Attaca stores. As of 3/6/17 this also contains VCS-specific functionality which will soon be removed and spliced into an `attaca-vcs` crate. |
| `attaca-leveldb` | Implements a small Attaca backend over a LevelDB database. |
| `attaca-rados` | Implements an Attaca backend over the RADOS API of a Ceph cluster. |
| `attaca-test` | Generic test batteries for Attaca backends. (highly WIP) |
| `subito` | A Git-like distributed version control system built on top of an Attaca store with some additional capabilities. |

## Why the names?

"Attaca" is a musical term used in classical music to indicate that a performer
should not pause or break in between movements of a given piece. "Subito" means
"suddenly". When written together as "attaca subito", the combined term
indicates that the start of the next movement is intended to come especially
surprisingly quickly.
