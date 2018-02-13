use std::{hash::Hash, io::{Read, Write}};

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

pub trait Handle: Clone + Ord + Hash + Send + Sync + Sized + 'static {
    type Content: Read;
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
