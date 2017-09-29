//! # `trace` - traits and default implementations for "trace objects"
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


/// `SplitTrace` tracks the progress of hashsplitting a file.
pub trait SplitTrace: Send + Sized + 'static {
    /// Called when a chunk is split from a parent slice.
    fn on_chunk(&mut self, _offset: u64, _chunk: &[u8]) {}
}


impl SplitTrace for () {}


/// `MarshalTrace` tracks the process of marshalling objects.
pub trait MarshalTrace: Send + Sized + 'static {
    fn on_reserve(&mut self, _n: usize) {}

    /// Called when a slice has its SHA3-256 hash calculated, and before it is sent to a writer
    /// task.
    fn on_hashed(&mut self, _object_hash: &ObjectHash) {}
}


impl MarshalTrace for () {}


/// The destination of marshalled objects being written.
pub enum WriteDestination<'a> {
    /// The destination is the local blob store of the current repository - the local filesystem.
    Local,

    /// The destination is a remote, either named or unnamed. If named, the name is the same as
    /// given in the repository remote configuration.
    Remote(&'a str),
}


/// `WriteTrace` tracks the process of writing marshalled objects to a local or remote
/// object store.
pub trait WriteTrace: Send + Sized + 'static {
    /// Called when a write operation begins.
    fn on_begin(&mut self, _object_hash: &ObjectHash) {}

    /// Called when a write operation completes.
    fn on_complete(&mut self, _object_hash: &ObjectHash, _fresh: bool) {}
}


impl WriteTrace for () {}


/// `BatchTrace` tracks the occurrences of marshalling and splitting files passed to a `Batch`.
pub trait BatchTrace: Send + Sized + 'static {
    type MarshalTrace: MarshalTrace;
    type SplitTrace: SplitTrace;

    /// Called when we begin splitting a file.
    fn on_split(&mut self, size: u64) -> Self::SplitTrace;

    /// Called when we begin marshalling a fully split and tree-loaded file.
    fn on_marshal(&mut self, chunks: usize) -> Self::MarshalTrace;
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

    /// Called when a batch is created.
    fn on_batch(&mut self) -> Self::BatchTrace;

    /// Called when a write operation begins on a batch.
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
