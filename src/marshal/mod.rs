//! `marshal` - marshal chunked files and commits into a format which can be uploaded to the store.
//!
//! Important pieces of functionality in this module include:
//!
//! * Compute the hashes of chunks.
//! * Insert chunks into subtree/large-file nodes.
//! * Deduplicate chunks.

pub mod object;
pub mod record;
pub mod tree;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{self, Write};
use std::io::BufWriter;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Mutex;

use bincode;
use digest_writer::{FixedOutput, Writer};
use futures::prelude::*;
use futures::future::{self, FutureResult};
use generic_array::GenericArray;
use sha3::{Sha3_256, Digest};
use typenum::U32;

use errors::*;
use trace::MarshalTrace;


pub use self::object::{Object, SmallObject, LargeObject, DataObject, SubtreeObject, CommitObject};
pub use self::record::{Record, DataRecord, MetaRecord, SmallRecord};


pub trait Marshal<'data, 'ctx>: Send + Sync + 'ctx
where
    'data: 'ctx,
{
    type Registered: Future<Item = ObjectHash, Error = Error> + Send + 'ctx;

    fn register<T: Into<Record<'data>>>(&'ctx self, object: T) -> Self::Registered;
}


/// The SHA3-256 hash of a stored object.
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


/// A `Marshaller` is responsible for managing the environment of the transformation of an
/// in-memory representation of the Git-like data structure to the encoded, serialized
/// representation.
pub struct Marshaller<'fresh, T: MarshalTrace = ()> {
    inner: Mutex<MarshallerInner<'fresh, T>>,
}


struct MarshallerInner<'fresh, T: MarshalTrace = ()> {
    trace: T,
    objects: HashMap<ObjectHash, Option<Object<'fresh>>>,
}


impl<'fresh, T: MarshalTrace> Marshaller<'fresh, T> {
    pub fn new() -> Self
    where
        T: Default,
    {
        Marshaller {
            inner: Mutex::new(MarshallerInner {
                trace: T::default(),
                objects: HashMap::new(),
            }),
        }
    }


    pub fn with_trace(trace: T) -> Self {
        Marshaller {
            inner: Mutex::new(MarshallerInner {
                trace,
                objects: HashMap::new(),
            }),
        }
    }


    pub fn reserve(&mut self, n: usize) {
        let mut inner = self.inner.lock().unwrap();

        inner.trace.on_reserve(n);
        inner.objects.reserve(n);
    }


    pub fn finish(self) -> Marshalled<'fresh> {
        Marshalled { objects: self.inner.into_inner().unwrap().objects }
    }
}


impl<'ctx, 'data: 'ctx, T: MarshalTrace + 'ctx> Marshal<'data, 'ctx> for Marshaller<'data, T> {
    type Registered = FutureResult<ObjectHash, Error>;

    fn register<R: Into<Record<'data>>>(&'ctx self, target: R) -> Self::Registered {
        let mut inner = self.inner.lock().unwrap();

        let (object_hash, object) = match target.into().to_deep() {
            Ok(object) => {
                let mut dw = BufWriter::new(Writer::new(Sha3_256::new()));
                bincode::serialize_into(&mut dw, &object, bincode::Infinite).unwrap();

                let digest = match dw.into_inner() {
                    Ok(writer) => writer.fixed_result(),
                    Err(_) => unreachable!(),
                };

                (ObjectHash(digest), Some(object))
            }

            Err(hash) => (hash, None),
        };

        let inner = &mut *inner;
        let entry = inner.objects.entry(object_hash);

        match entry {
            Entry::Occupied(_) => {
                inner.trace.on_register(&object_hash, true);
            }
            Entry::Vacant(vacant) => {
                inner.trace.on_register(&object_hash, false);
                vacant.insert(object);
            }
        }

        future::ok(object_hash)
    }
}


/// `Marshalled` is a frozen `Marshaller`, also no longer holding onto a trace object.
pub struct Marshalled<'fresh> {
    objects: HashMap<ObjectHash, Option<Object<'fresh>>>,
}


impl<'fresh> Deref for Marshalled<'fresh> {
    type Target = HashMap<ObjectHash, Option<Object<'fresh>>>;

    fn deref(&self) -> &Self::Target {
        &self.objects
    }
}
