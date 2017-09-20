use std::ops::Deref;
use std::sync::Arc;

use memmap::Mmap;
use owning_ref::ArcRef;


#[derive(Debug)]
pub enum Source {
    Empty,
    Mapped(Mmap),
    Vector(Vec<u8>),
}


impl Source {
    pub fn into_bytes(self) -> ArcSlice {
        ArcRef::new(Arc::new(self)).map(|source| &**source)
    }
}


impl Deref for Source {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match *self {
            Source::Empty => &[],
            Source::Mapped(ref mmap) => unsafe { mmap.as_slice() },
            Source::Vector(ref vec) => vec.as_slice(),
        }
    }
}


pub type ArcSlice = ArcRef<Source, [u8]>;
