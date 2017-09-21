//! `trace` - traits and default implementations for "trace objects"
//!
//! A "trace object" here is essentially a bundle of callbacks, implemented as a type implementing
//! a given trait. This is useful for monitoring long-running operations and producing a nice-looking
//! user interface, for example to drive a progress bar.
//!
//! All `Trace` traits have a default implementation for `()` which does nothing, discarded all
//! passed-in information. This dummy implementation should be perfectly efficient, as any calls to
//! it can be optimized out.

use batch::Batch;
use marshal::ObjectHash;
use repository::RemoteCfg;


/// `SplitTrace` tracks the progress of hashsplitting a file.
pub trait SplitTrace: Send + Sized + 'static {
    fn on_chunk(&mut self, _offset: u64, _chunk: &[u8]) {}
}


impl SplitTrace for () {}


/// `MarshalTrace` tracks the process of marshalling objects.
pub trait MarshalTrace: Send + Sized + 'static {
    fn on_reserve(&mut self, _n: usize) {}
    fn on_hashed(&mut self, _object_hash: &ObjectHash) {}
}


impl MarshalTrace for () {}


/// The destination of marshalled objects being written.
pub enum WriteDestination<'a> {
    Local,
    Remote(&'a Option<String>, &'a RemoteCfg),
}


/// `WriteTrace` tracks the process of writing marshalled objects to a local or remote
/// object store.
pub trait WriteTrace: Send + Sized + 'static {
    fn on_begin(&mut self, _object_hash: &ObjectHash) {}
    fn on_complete(&mut self, _object_hash: &ObjectHash) {}
}


impl WriteTrace for () {}


pub trait BatchTrace: Send + Sized + 'static {
    type MarshalTrace: MarshalTrace;
    type SplitTrace: SplitTrace;

    fn on_marshal(&mut self, chunks: usize) -> Self::MarshalTrace;
    fn on_split(&mut self, size: u64) -> Self::SplitTrace;
}


impl BatchTrace for () {
    type MarshalTrace = ();
    type SplitTrace = ();

    fn on_marshal(&mut self, _chunks: usize) -> Self::MarshalTrace {
        ()
    }

    fn on_split(&mut self, _size: u64) -> Self::SplitTrace {
        ()
    }
}


/// `Trace` is the parent trace object; it is passed to a `Context` once created, and other trace
/// objects are intended to be created from a `Trace` type used as a factory.
pub trait Trace: Send + Sized + 'static {
    type BatchTrace: BatchTrace;
    type WriteTrace: WriteTrace;

    fn on_batch(&mut self) -> Self::BatchTrace;
    fn on_write(
        &mut self,
        batch: &Batch<Self::BatchTrace>,
        destination: WriteDestination,
    ) -> Self::WriteTrace;
}


impl Trace for () {
    type BatchTrace = ();
    type WriteTrace = ();

    fn on_batch(&mut self) -> Self::BatchTrace {
        ()
    }

    fn on_write(
        &mut self,
        _batch: &Batch<Self::BatchTrace>,
        _destination: WriteDestination,
    ) -> Self::WriteTrace {
        ()
    }
}
