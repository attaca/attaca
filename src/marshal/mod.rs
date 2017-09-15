//! `marshal` - marshal chunked files and commits into a format which can be uploaded to the store.
//!
//! This includes several pieces of key functionality:
//! - Compute the hashes of chunks.
//! - Insert chunks into subtree/large-file nodes.
//! - Deduplicate chunks.
//!
//! Key types in `marshal` include:
//! - `Chunk`, the hashed chunk type. Any split file should result in a `Vec` or iterator of `Chunk`s.

pub mod object;
pub mod tree;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{self, Write};
use std::io::BufWriter;
use std::ops::Deref;
use std::path::PathBuf;

use bincode;
use digest_writer::{FixedOutput, Writer};
use generic_array::GenericArray;
use sha3::{Sha3_256, Digest};
use typenum::U32;

use trace::MarshalTrace;


pub use self::object::{Object, SmallObject, LargeObject, DataObject, SubtreeObject, CommitObject};


/// The SHA3-512 hash of a stored object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectHash(GenericArray<u8, U32>);


impl ObjectHash {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }


    #[inline]
    pub fn to_path(&self) -> PathBuf {
        let mut buf = String::with_capacity(32);

        write!(buf, "{:02x}/{:02x}/", self.0[0], self.0[1]).unwrap();

        for b in &self.0[2..] {
            write!(buf, "{:02x}", b).unwrap();
        }

        buf.into()
    }
}


impl fmt::Debug for ObjectHash {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Display>::fmt(self, f)
    }
}


impl fmt::Display for ObjectHash {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for &b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }

        Ok(())
    }
}


pub struct Marshaller<'fresh, T: MarshalTrace = ()> {
    trace: T,
    objects: HashMap<ObjectHash, Option<Object<'fresh>>>,
}


impl<'fresh, T: MarshalTrace> Marshaller<'fresh, T> {
    pub fn new() -> Self
    where
        T: Default,
    {
        Marshaller {
            trace: T::default(),
            objects: HashMap::new(),
        }
    }


    pub fn with_trace(trace: T) -> Self {
        Marshaller {
            trace,
            objects: HashMap::new(),
        }
    }


    pub fn reserve(&mut self, n: usize) {
        self.trace.on_reserve(n);
        self.objects.reserve(n);
    }


    pub fn register(&mut self, object: Object<'fresh>) -> ObjectHash {
        let mut dw = BufWriter::new(Writer::new(Sha3_256::new()));
        bincode::serialize_into(&mut dw, &object, bincode::Infinite).unwrap();

        let digest = match dw.into_inner() {
            Ok(writer) => writer.fixed_result(),
            Err(_) => unreachable!(),
        };

        let object_hash = ObjectHash(digest);
        let entry = self.objects.entry(object_hash);

        match entry {
            Entry::Occupied(_) => {
                self.trace.on_register(&object, &object_hash, true);
            }
            Entry::Vacant(vacant) => {
                self.trace.on_register(&object, &object_hash, false);
                vacant.insert(Some(object));
            }
        }

        object_hash
    }


    pub fn put<U>(&mut self, object: U) -> ObjectHash
    where
        U: Marshal<'fresh>,
    {
        object.marshal(self)
    }


    pub fn finish(self) -> Marshalled<'fresh> {
        Marshalled { objects: self.objects }
    }
}


pub struct Marshalled<'fresh> {
    objects: HashMap<ObjectHash, Option<Object<'fresh>>>,
}


impl<'fresh> Deref for Marshalled<'fresh> {
    type Target = HashMap<ObjectHash, Option<Object<'fresh>>>;

    fn deref(&self) -> &Self::Target {
        &self.objects
    }
}


pub trait Marshal<'fresh> {
    fn marshal<T: MarshalTrace>(self, marshaller: &mut Marshaller<'fresh, T>) -> ObjectHash;
}
