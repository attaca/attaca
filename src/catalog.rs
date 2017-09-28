use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::{Arc, Weak, Mutex};
use std::sync::atomic::{Ordering, AtomicUsize};
use std::thread;

use bincode;
use futures::prelude::*;
use futures::task::AtomicTask;
use qp_trie::{Entry, Trie};

use errors::*;
use marshal::ObjectHash;


const OK_NOT_READY: usize = 0;
const OK_READY: usize = 1;
const ERR: usize = 2;


#[derive(Debug)]
struct CatalogLockInner {
    task: AtomicTask,
    locked: AtomicUsize,
}


#[derive(Debug)]
#[must_use = "CatalogLock must be .release()'d on success!"]
pub struct CatalogLock {
    inner: Weak<CatalogLockInner>,
    hash: ObjectHash,
    catalog: Catalog,
}


impl CatalogLock {
    fn new(catalog: &Catalog, hash: ObjectHash) -> (CatalogLock, CatalogFuture) {
        let inner = Arc::new(CatalogLockInner {
            task: AtomicTask::new(),
            locked: AtomicUsize::new(OK_NOT_READY),
        });

        let lock = CatalogLock {
            inner: Arc::downgrade(&inner),
            hash,
            catalog: catalog.clone(),
        };
        let future = CatalogFuture { inner };

        (lock, future)
    }


    pub fn release(self) {}
}


impl Drop for CatalogLock {
    fn drop(&mut self) {
        if thread::panicking() {
            let inner = Weak::upgrade(&self.inner).unwrap();
            inner.locked.store(ERR, Ordering::SeqCst);
        } else {
            let inner = Weak::upgrade(&self.inner).unwrap();
            let previous_state = inner.locked.compare_and_swap(
                OK_NOT_READY,
                OK_READY,
                Ordering::SeqCst,
            );
            assert_eq!(previous_state, OK_NOT_READY);
            self.catalog.inner.lock().unwrap().objects.insert(
                self.hash,
                CatalogEntry::Finished,
            );
            inner.task.notify();
        }
    }
}


#[derive(Debug, Clone)]
pub struct CatalogFuture {
    inner: Arc<CatalogLockInner>,
}


impl Future for CatalogFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.locked.load(Ordering::SeqCst) {
            OK_NOT_READY => {
                self.inner.task.register();
                Ok(Async::NotReady)
            }
            OK_READY => Ok(Async::Ready(())),
            ERR => bail!(ErrorKind::CatalogPoisoned),
            _ => unreachable!("Invalid CatalogLock state!"),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogEntry {
    /// An unlocked catalog entry indicates that something has been pushed to a remote or local
    /// blob store successfully and may be pulled from it.
    ///
    /// FIXME: [a bincode bug](https://github.com/TyOverby/bincode/issues/184) means this variant
    /// cannot be named `Unlocked` or else through some asymmetrical oddities `bincode`'s
    /// serializer/deserializer will attempt to deserialize the `Unlocked` variant and cause
    /// everything to blow up.
    Finished,

    /// A "locked" catalog entry has a reference to the future in it, so that a task can wait until
    /// a catalog entry is done filling.
    ///
    /// Note: If serialized, serde will error; however, a `Locked` catalog entry should never be
    /// serialized as catalog locks hold references to the catalog and also replaced `Locked`
    /// entries with `Finished` when dropped.
    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    Locked(CatalogFuture),
}


impl Future for CatalogEntry {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            CatalogEntry::Locked(ref mut future) => future.poll(),
            CatalogEntry::Finished => Ok(Async::Ready(())),
        }
    }
}


#[derive(Debug, Clone)]
pub struct CatalogTrie {
    objects: Trie<ObjectHash, CatalogEntry>,
}


impl CatalogTrie {
    pub fn new() -> Self {
        Self { objects: Trie::new() }
    }


    pub fn insert(&mut self, hash: ObjectHash) {
        self.objects.insert(hash, CatalogEntry::Finished);
    }
}


#[derive(Debug)]
struct CatalogInner {
    catalog_path: PathBuf,
    objects: Trie<ObjectHash, CatalogEntry>,
}


#[derive(Debug, Clone)]
pub struct Catalog {
    inner: Arc<Mutex<CatalogInner>>,
}


impl Catalog {
    pub fn new(catalog_trie: CatalogTrie, catalog_path: PathBuf) -> Result<Catalog> {
        Ok(Catalog {
            inner: Arc::new(Mutex::new(CatalogInner {
                catalog_path,
                objects: catalog_trie.objects,
            })),
        })
    }


    pub fn load(catalog_path: PathBuf) -> Result<Catalog> {
        let objects = if catalog_path.is_file() {
            bincode::deserialize_from(
                &mut OpenOptions::new()
                    .read(true)
                    .open(&catalog_path)
                    .chain_err(|| ErrorKind::CatalogOpen(catalog_path.clone()))?,
                bincode::Infinite,
            ).chain_err(|| ErrorKind::CatalogDeserialize(catalog_path.clone()))?
        } else {
            Trie::new()
        };

        Ok(Catalog {
            inner: Arc::new(Mutex::new(CatalogInner {
                catalog_path,
                objects,
            })),
        })
    }


    pub fn try_lock(&self, hash: ObjectHash) -> StdResult<CatalogLock, CatalogEntry> {
        let mut inner_lock = self.inner.lock().unwrap();

        match inner_lock.objects.entry(hash) {
            Entry::Vacant(vacant) => {
                let (lock, future) = CatalogLock::new(self, hash);
                vacant.insert(CatalogEntry::Locked(future));
                Ok(lock)
            }
            Entry::Occupied(occupied) => Err(occupied.get().clone()),
        }
    }
}


impl Drop for CatalogInner {
    fn drop(&mut self) {
        let mut file = File::create(&self.catalog_path).unwrap();
        bincode::serialize_into(&mut file, &self.objects, bincode::Infinite).unwrap();
    }
}
