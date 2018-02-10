//! # `attaca` - distributed, resilient version control with a git-like interface

#![feature(proc_macro, conservative_impl_trait, generators, use_nested_groups, nll)]
#![recursion_limit = "256"]

#[cfg(not(target_pointer_width = "64"))]
compile_error!(
    "Must be compiled on a 64-bit architecture due to the use of memory-mapping for potentially extremely large files!"
);

#[cfg(test)]
extern crate rand;

extern crate chrono;
extern crate etcd;
#[macro_use]
extern crate failure;
extern crate futures_await as futures;
extern crate hex;
extern crate leb128;
extern crate memchr;
#[macro_use]
extern crate nom;
extern crate sha3;
extern crate typenum;

pub mod canonical;
pub mod digest;
pub mod object;
pub mod split;
pub mod store;
pub mod workspace;

use failure::Error;

pub use store::*;
pub use workspace::*;

/// Trait for types representing resources which must be "opened" for use from URLs; e.g. local
/// filesystem workspaces and remote servers/clusters.
pub trait Open: Sized {
    /// The valid URL schemes for this resource. `SCHEMES` is presented as a slice rather than a
    /// single string because one resource might be openable through multiple different URL
    /// schemes. For example, one might want to connect to a Ceph/RADOS cluster by reading a
    /// configuration from, say, nodes of an etcd cluster running on the same hardware; or, one
    /// might want to provide a path to a `ceph.conf` file local to their machine. `SCHEMES` as a
    /// list of valid schemes allows one to write:
    ///
    /// ```
    /// const SCHEMES: &'static [&'static str] = &["attaca+ceph", "attaca+ceph+local", "attaca+ceph+etcd"];
    /// ```
    const SCHEMES: &'static [&'static str];

    /// Attempt to open a connection to an instance of this type at the provided URL. This will
    /// usually not be called unless the URL scheme matches a member of `Self::SCHEMES`; however,
    /// it is possible that the scheme may not be checked beforehand, so it should not be assumed
    /// that it matches.  In the case of a scheme mismatch, parse error, or other error connecting,
    /// an `Error` should be returned rather than, say, a panic.
    fn open(s: &str) -> Result<Self, Error>;
}

/// Trait for `Open`-able resources which may be discoverable without any URL information; for
/// example, filesystem workspaces, which can often be discovered by recursively searching
/// directories.
pub trait Search: Open {
    /// Attempt to discover a local instance without any URL information. In the case that an
    /// instance is discovered but connecting results in an error, an `Error` should be returned;
    /// in the case that no instance is discovered, `Ok(None)` should be returned. If multiple
    /// ambiguous instances are discovered, they should either be resolved in a reasonable,
    /// type-defined fashion or result in an error (multiple satisfactory instances found.)
    ///
    /// For example, for a filesystem-based workspace type, `Open::search` might recursively search
    /// directories, checking each directory for repository files; if none are found in the current
    /// directory, the search moves to the parent directory. If no repositories are found after
    /// searching all the way to the root directory, `Ok(None)` would be returned; if a repository
    /// is found, but is malformed in some way, `Err(...)` would be returned; if multiple
    /// repositories are found, then the innermost might be opened (unless the repositories are
    /// configured in a nested manner, a la Git submodules.)
    fn search() -> Result<Option<Self>, Error>;
}
