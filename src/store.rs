use std::{hash::Hash, io::{BufRead, Write}};

use failure::Error;
use futures::prelude::*;

use Open;
use digest::Digest;

pub trait Store: Open + Clone + Send + Sync + Sized + 'static {
    type Handle: Handle;

    type HandleBuilder: HandleBuilder<Handle = Self::Handle>;
    fn handle_builder(&self) -> Self::HandleBuilder;

    type FutureLoadBranch: Future<Item = Option<Self::Handle>, Error = Error>;
    fn load_branch(&self, branch: String) -> Self::FutureLoadBranch;

    type FutureSwapBranch: Future<Item = (), Error = Error>;
    fn swap_branch(
        &self,
        branch: String,
        previous: Option<Self::Handle>,
        new: Self::Handle,
    ) -> Self::FutureSwapBranch;

    type FutureResolve: Future<Item = Option<Self::Handle>, Error = Error>;
    fn resolve<D: Digest>(&self, digest: &D) -> Self::FutureResolve
    where
        Self::Handle: HandleDigest<D>;
}

/// Trait for handles to objects in a `Store`.
///
/// Handle types must implement `HandleDigest` *for all* `D: Digest` (the only reason this is not a
/// bound is because it is currently impossible to express in Rust.) If a handle does not support
/// some specific `D`, that should be detected via comparison with the `Digest::NAME` and
/// `Digest::SIZE` constants, and then the incompatibility expressed via returning an error from
/// the `HandleDigest::digest` method.
///
/// There is also an additional contract on `PartialEq` impls for `Handle`. If two `Handle`s return
/// equal, then for any supported digest, the digests of the two handles should be equal (and vice
/// versa.) To restate, informally:
///
/// `if a == b then forall D: Digest, HandleDigest::<D>::digest(a) == HandleDigest::<D>::digest(b)`
///
/// There are no such contracts on `PartialOrd`, `Ord`, and `Hash`, except for those implied by
/// respecting the above contract on `PartialEq`.
pub trait Handle: Clone + Ord + Hash + Send + Sync + Sized + 'static {
    type Content: BufRead;
    type Refs: Iterator<Item = Self>;

    type FutureLoad: Future<Item = (Self::Content, Self::Refs), Error = Error>;
    fn load(&self) -> Self::FutureLoad;
}

pub trait HandleDigest<D: Digest>: Handle {
    type FutureDigest: Future<Item = D, Error = Error>;
    fn digest(&self) -> Self::FutureDigest;
}

pub trait HandleBuilder: Write {
    type Handle: Handle;

    fn add_reference(&mut self, handle: Self::Handle);

    type FutureHandle: Future<Item = Self::Handle, Error = Error>;
    fn finish(self) -> Self::FutureHandle;
}
