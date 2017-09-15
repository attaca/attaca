use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::Path;

use bincode;
use chrono::{DateTime, Utc};

use errors::Result;
use marshal::{Marshal, Marshaller, ObjectHash};
use trace::MarshalTrace;


/// The marshaled, deserialized representation of a "small" object (composed of a single chunk.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SmallObject<'a> {
    #[serde(borrow)]
    pub chunk: Cow<'a, [u8]>,
}


impl Deref for SmallObjectHash {
    type Target = ObjectHash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl<'fresh> Marshal<'fresh> for &'fresh SmallObject<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        ctx.register(Object::Data(DataObject::Small(
            SmallObject { chunk: Cow::Borrowed(&self.chunk) },
        )))
    }
}


impl<'fresh> Marshal<'fresh> for SmallObject<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        ctx.register(Object::Data(DataObject::Small(self)))
    }
}


#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SmallObjectHash(ObjectHash);


/// The marshaled, deserialized representation of a "large" object (composed of smaller chunks.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LargeObject<'a> {
    pub size: u64,

    #[serde(borrow)]
    pub children: Cow<'a, [(u64, ObjectHash)]>,
}


impl Deref for LargeObjectHash {
    type Target = ObjectHash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl<'fresh> Marshal<'fresh> for &'fresh LargeObject<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        ctx.register(Object::Data(DataObject::Large(LargeObject {
            size: self.size,
            children: Cow::Borrowed(&self.children),
        })))
    }
}


impl<'fresh> Marshal<'fresh> for LargeObject<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        ctx.register(Object::Data(DataObject::Large(self)))
    }
}


#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LargeObjectHash(ObjectHash);


/// The marshaled, deserialized representation of a subtree.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubtreeObject<'a> {
    #[serde(borrow)]
    entries: BTreeMap<Cow<'a, Path>, ObjectHash>,
}


/// The marshaled, deserialized representation of a commit object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommitObject<'a> {
    /// The subtree the commit object references.
    subtree: ObjectHash,

    /// The parents of the commit.
    #[serde(borrow)]
    parents: Cow<'a, [ObjectHash]>,

    /// A commit message, provided by the user.
    #[serde(borrow)]
    message: Cow<'a, str>,

    /// The commit timestamp, denoting when the commit was made locally.
    timestamp: DateTime<Utc>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataObject<'a> {
    /// A "small" blob is a single chunk.
    Small(
        #[serde(borrow)]
        SmallObject<'a>
    ),

    /// A "large" blob is a blob consisting of multiple smaller blobs, stored as a tuple of size
    /// and content hash.
    Large(
        #[serde(borrow)]
        LargeObject<'a>
    ),
}


impl<'a> DataObject<'a> {
    pub fn is_empty(&self) -> bool {
        match *self {
            DataObject::Large(LargeObject {
                                  size: 0,
                                  ref children,
                              }) => children.len() == 0,
            _ => false,
        }
    }
}


impl Deref for DataObjectHash {
    type Target = ObjectHash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DataObjectHash(ObjectHash);


/// The marshaled, deserialized representation of an object in the distributed store.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Object<'a> {
    /// A "data object" or "blob" is a single file.
    Data(
        #[serde(borrow)]
        DataObject<'a>
    ),

    /// A subtree is a directory, consisting of a mapping of paths to blobs.
    Subtree(
        #[serde(borrow)]
        SubtreeObject<'a>
    ),

    /// A commit is a pointer to a subtree representing the current state of the repository, as
    /// well as a list of parent commits.
    Commit(
        #[serde(borrow)]
        CommitObject<'a>
    ),
}


impl<'a> AsRef<Object<'a>> for Object<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}


impl<'a> Object<'a> {
    pub fn from_bytes(slice: &'a [u8]) -> Result<Object<'a>> {
        bincode::deserialize(slice).map_err(Into::into)
    }


    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self, bincode::Infinite).map_err(Into::into)
    }
}
