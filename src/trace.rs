//! # `trace` - traits and default implementations for "trace objects"
//!
//! A "trace object" here is essentially a bundle of callbacks, implemented as a type implementing
//! a given trait. This is useful for monitoring long-running operations and producing a nice-looking
//! user interface, for example to drive a progress bar.
//!
//! All `Trace` traits have a default implementation for `()` which does nothing, discarded all
//! passed-in information. This dummy implementation should be perfectly efficient, as any calls to
//! it can be optimized out.

use marshal::ObjectHash;


/// `Trace` is the parent trace object; it is passed to a `Context` once created, and other trace
/// objects are intended to be created from a `Trace` type used as a factory.
pub trait Trace: Clone + Send + Sync + Sized + 'static {
    fn on_split_begin(&self, size: u64) {}

    fn on_split_chunk(&self, offset: u64, chunk: &[u8]) {}

    fn on_marshal_process(&self, object_hash: &ObjectHash) {}

    fn on_marshal_subtree(&self, count: u64, object_hash: &ObjectHash) {}
    
    fn on_write_object_start(&self, object_hash: &ObjectHash) {}

    fn on_write_object_finish(&self, object_hash: &ObjectHash, fresh: bool) {}

    fn on_close(&self) {}
}


impl Trace for () {}
