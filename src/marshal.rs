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

use sha3::{Sha3_256, Digest};


#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkHash {
    val: [u8; 32],
}


impl ChunkHash {
    fn compute(slice: &[u8]) -> ChunkHash {
        let mut val = [0u8; 32];

        let digest = Sha3_256::digest(slice);
        val.copy_from_slice(digest.as_slice());

        ChunkHash { val }
    }


    pub fn as_slice(&self) -> &[u8] {
        &self.val
    }
}


#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Chunk<'a> {
    bytes: Cow<'a, [u8]>,
    hash: ChunkHash,
}


impl<'a> From<&'a [u8]> for Chunk<'a> {
    fn from(bytes: &'a [u8]) -> Chunk<'a> {
        Chunk {
            bytes: Cow::Borrowed(bytes),
            hash: ChunkHash::compute(bytes),
        }
    }
}


impl<'a> From<Vec<u8>> for Chunk<'a> {
    fn from(bytes: Vec<u8>) -> Chunk<'a> {
        let hash = ChunkHash::compute(bytes.as_slice());

        Chunk {
            bytes: Cow::Owned(bytes),
            hash,
        }
    }
}


impl<'a> Chunk<'a> {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}
