//! `marshal` - marshal chunked files and commits into a format which can be uploaded to the store.
//!
//! This includes several pieces of key functionality:
//! - Compute the hashes of chunks.
//! - Insert chunks into subtree/large-file nodes.
//! - Deduplicate chunks.
//!
//! Key types in `marshal` include:
//! - `Chunk`, the hashed chunk type. Any split file should result in a `Vec` or iterator of `Chunk`s.


use std::borrow::Cow;
use std::fmt;
use std::collections::BTreeMap;
use std::path::Path;

use bincode;
use chrono::prelude::*;
use digest_writer::{FixedOutput, Writer};
use generic_array::GenericArray;
use sha3::{Sha3_256, Digest};
use typenum::U32;


pub type Chunk<'a> = Cow<'a, [u8]>;


/// The SHA3-512 hash of a stored object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectHash(GenericArray<u8, U32>);


impl ObjectHash {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
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


/// The marshaled, deserialized representation of a "small" object (composed of a single chunk.)
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SmallObject<'a> {
    #[serde(borrow)]
    chunk: Cow<'a, [u8]>,
}


/// The marshaled, deserialized representation of a "large" object (composed of smaller chunks.)
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LargeObject<'a> {
    depth: u8,
    size: u64,

    #[serde(borrow)]
    children: Cow<'a, [(ObjectHash, u64)]>,
}


/// The marshaled, deserialized representation of a subtree.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubtreeObject<'a> {
    #[serde(borrow)]
    entries: BTreeMap<Cow<'a, Path>, ObjectHash>,
}


/// The marshaled, deserialized representation of a commit object.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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


/// The marshaled, deserialized representation of an object in the distributed store.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Object<'a> {
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


impl<'a> Object<'a> {
    pub fn hash(&self) -> ObjectHash {
        let mut dw = Writer::new(Sha3_256::new());
        bincode::serialize_into(&mut dw, self, bincode::Infinite).unwrap();

        ObjectHash(dw.fixed_result())
    }
}
