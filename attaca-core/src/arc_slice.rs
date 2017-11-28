//! # `arc_slice` - atomically reference-counted slices
//!
//! This module allows for slices of memory maps/memory-mapped files, owned vectors, and empty
//! slices to be atomatically reference-counted and shared without lifetime bounds.

use std::ops::Deref;
use std::sync::Arc;

use memmap::Mmap;
use owning_ref::ArcRef;


lazy_static! {
    /// Allocate an `ArcSlice` once to represent all empty slices, as constructing an `Arc` for a
    /// zero-sized type will still result in an allocation.
    static ref EMPTY: ArcSlice = Source::Empty.into_bytes();
}


/// The "erased" `Source` type. This exists to prevent manual construction of `ArcSlice`s - please
/// use `arc_slice::{empty, mapped, owned}` instead of constructing a `Source` manually.
#[derive(Debug)]
pub struct ErasedSource(Source);


/// The `Source` of an `ArcSlice` - either the constant, empty slice; a memory-mapped region; or an
/// owned byte vector.
#[derive(Debug)]
enum Source {
    Empty,
    Mapped(Mmap),
    Vector(Vec<u8>),
}


impl Source {
    fn into_bytes(self) -> ArcSlice {
        ArcRef::new(Arc::new(ErasedSource(self))).map(|source| &**source)
    }
}


/// An `ArcSlice` is composed of an `ArcRef`, which is an atomically reference-counted source
/// along with a derived reference to the byte slice contained in the source.
///
/// For more information about references which own or share the data they reference, see the
/// `owning_ref` crate.
pub type ArcSlice = ArcRef<ErasedSource, [u8]>;


/// Get an `ArcSlice` which dereferences to the empty slice.
pub fn empty() -> ArcSlice {
    EMPTY.clone()
}


/// Construct an `ArcSlice` which dereferences to the slice provided by a given memory-mapped
/// region.
pub fn mapped(mmap: Mmap) -> ArcSlice {
    Source::Mapped(mmap).into_bytes()
}


/// Construct an `ArcSlice` from an owned byte vector.
pub fn owned(vector: Vec<u8>) -> ArcSlice {
    Source::Vector(vector).into_bytes()
}


impl Deref for ErasedSource {
    type Target = [u8];

    /// Defer the dereference to the inner, hidden `Source` type.
    fn deref(&self) -> &[u8] {
        &self.0
    }
}


impl Deref for Source {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match *self {
            Source::Empty => &[],
            Source::Mapped(ref mmap) => &*mmap,
            Source::Vector(ref vec) => vec.as_slice(),
        }
    }
}
