use marshal::{Object, ObjectHash};
use split::Chunk;


pub trait SplitTrace: Sized {
    fn on_chunk(&mut self, _offset: u64, _chunk: &Chunk) {}
}


impl SplitTrace for () {}


pub trait MarshalTrace: Sized {
    fn on_reserve(&mut self, _n: usize) {}
    fn on_register(&mut self, _object: &Object, _object_hash: &ObjectHash, _cache_hit: bool) {}
}


impl MarshalTrace for () {}


pub trait WriteMarshalledTrace: Sized {
    fn on_write(&mut self, _object_hash: &ObjectHash) {}
}


impl WriteMarshalledTrace for () {}


pub trait Trace {
    type MarshalTrace: MarshalTrace;
    type SplitTrace: SplitTrace;
    type WriteMarshalledTrace: WriteMarshalledTrace;

    fn on_marshal(&mut self, chunks: usize) -> Self::MarshalTrace;
    fn on_split(&mut self, size: u64) -> Self::SplitTrace;
    fn on_write_marshalled(&mut self, objects: usize) -> Self::WriteMarshalledTrace;
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

    fn on_write_marshalled(&mut self, _objects: usize) -> Self::WriteMarshalledTrace {
        ()
    }
}
