use std::ops::Range;

use failure::Error;

use object::{Commit, Object, ObjectRef, Tree};
use store::Handle;

// Suppose we allow paths to go *into* large objects.
// 1. Straightforward correspondence between conflicts and conflicting ranges of bytes.
//
// Suppose we do *not* allow paths to go into large objects.
// 1. Less general, but does not expose internal information (hashsplitting).
// 2. Straightforward correspondence between conflicts and conflicting data trees... *if* we
//    enforce that the "pending" iterator output at most one conflict per unique data tree.

pub struct Chunk<H: Handle> {
    root: Commit<H>,

    path: Vec<String>,

    range: Range<u64>,
    object: Object<H>,
}

/// Trait for merge algorithms.
pub trait Merge {
    type Handle: Handle;

    type Conflict: Iterator<Item = Chunk<Self::Handle>>;
    type Pending: Iterator<Item = Self::Conflict>;

    fn pending(&self) -> Self::Pending;
    fn resolve(&mut self, conflict: Self::Conflict);
    fn finish(self) -> Result<Tree<Self::Handle>, Error>;
}
