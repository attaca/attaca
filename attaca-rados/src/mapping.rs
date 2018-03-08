use std::{collections::HashMap, sync::RwLock};

use attaca::{digest::Sha3Digest, store::RawHandle};

#[derive(Debug)]
pub struct Mapping {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    digests: HashMap<RawHandle, Sha3Digest>,
    handles: HashMap<Sha3Digest, RawHandle>,
}

impl Inner {
    fn reserve(&mut self, digest: Sha3Digest) -> Result<RawHandle, RawHandle> {
        match self.handles.get(&digest).cloned() {
            Some(id) => Err(id),
            None => {
                let new_id = RawHandle(self.handles.len() as u64);
                self.handles.insert(digest, new_id);
                self.digests.insert(new_id, digest);
                Ok(new_id)
            }
        }
    }
}

impl Mapping {
    pub fn new() -> Mapping {
        Self {
            inner: RwLock::new(Inner::default()),
        }
    }

    /// Get or allocate a new ID in the mapping for a given digest.
    ///
    /// This function returns `Ok` if the ID is fresh and `Err` if it is not.
    pub fn reserve(&self, digest: Sha3Digest) -> Result<RawHandle, RawHandle> {
        self.inner.write().unwrap().reserve(digest)
    }

    /// Acquire a read lock and resolve IDs into digests.
    pub fn map_ids_to_digests<'a, I>(&'a self, iter: I) -> impl Iterator<Item = Sha3Digest> + 'a
    where
        I: IntoIterator<Item = RawHandle>,
        I::IntoIter: 'a,
    {
        let inner = self.inner.read().unwrap();
        iter.into_iter().map(move |id| inner.digests[&id])
    }

    /// Acquire a write lock and resolve digests to IDs, reserving new IDs for as of yet unseen
    /// digests.
    pub fn map_digests_to_ids<'a, I>(&'a self, iter: I) -> impl Iterator<Item = RawHandle> + 'a
    where
        I: IntoIterator<Item = Sha3Digest>,
        I::IntoIter: 'a,
    {
        let mut inner = self.inner.write().unwrap();
        iter.into_iter()
            .map(move |digest| inner.reserve(digest).unwrap_or_else(|e| e))
    }

    /// Get the digest corresponding to a given ID.
    ///
    /// The invariant that all IDs have corresponding digests is preserved, so we may simply index.
    pub fn digest(&self, id: RawHandle) -> Sha3Digest {
        self.inner.read().unwrap().digests[&id]
    }
}
