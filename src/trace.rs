//! `trace` - traits and default implementations for "trace objects"
//!
//! A "trace object" here is essentially a bundle of callbacks, implemented as a type implementing
//! a given trait. This is useful for monitoring long-running operations and producing a nice-looking
//! user interface, for example to drive a progress bar.
//!
//! All `Trace` traits have a default implementation for `()` which does nothing, discarded all
//! passed-in information. This dummy implementation should be perfectly efficient, as any calls to
//! it can be optimized out.

use marshal::ObjectHash;
use repository::RemoteCfg;
use split::Chunk;


/// `SplitTrace` tracks the progress of hashsplitting a file.
pub trait SplitTrace: Send + Sized {
    fn on_chunk(&mut self, _offset: u64, _chunk: &Chunk) {}
}


impl SplitTrace for () {}


/// `MarshalTrace` tracks the process of marshalling objects.
pub trait MarshalTrace: Send + Sized {
    fn on_reserve(&mut self, _n: usize) {}
    fn on_register(&mut self, _object_hash: &ObjectHash, _cache_hit: bool) {}
}


impl MarshalTrace for () {}


/// The destination of marshalled objects being written.
pub enum WriteDestination<'a> {
    Local,
    Remote(&'a Option<String>, &'a RemoteCfg),
}


/// `WriteMarshalledTrace` tracks the process of writing marshalled objects to a local or remote
/// object store.
pub trait WriteMarshalledTrace: Send + Sized {
    fn on_write(&mut self, _object_hash: &ObjectHash) {}
}


impl WriteMarshalledTrace for () {}


/// `Trace` is the parent trace object; it is passed to a `Context` once created, and other trace
/// objects are intended to be created from a `Trace` type used as a factory.
pub trait Trace {
    type MarshalTrace: MarshalTrace;
    type SplitTrace: SplitTrace;
    type WriteMarshalledTrace: WriteMarshalledTrace;

    fn on_marshal(&mut self, chunks: usize) -> Self::MarshalTrace;
    fn on_split(&mut self, size: u64) -> Self::SplitTrace;
    fn on_write_marshalled(
        &mut self,
        objects: usize,
        destination: WriteDestination,
    ) -> Self::WriteMarshalledTrace;
}


impl Trace for () {
    type MarshalTrace = ();
    type SplitTrace = ();
    type WriteMarshalledTrace = ();

    fn on_marshal(&mut self, _chunks: usize) -> Self::MarshalTrace {
        ()
    }

    fn on_split(&mut self, _size: u64) -> Self::SplitTrace {
        ()
    }

    fn on_write_marshalled(
        &mut self,
        _objects: usize,
        _destination: WriteDestination,
    ) -> Self::WriteMarshalledTrace {
        ()
    }
}
