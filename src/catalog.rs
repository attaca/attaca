use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::iter;
use std::mem;
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
use repository::{Config, Paths};


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
            assert_eq!(previous_state, OK_NOT_READY, "Lock forcibly canceled!");
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


impl CatalogFuture {
    fn cancel_lock(&self) -> Result<()> {
        let prev_state = self.inner.locked.compare_and_swap(
            OK_NOT_READY,
            ERR,
            Ordering::SeqCst,
        );
        ensure!(prev_state != ERR, ErrorKind::CatalogPoisoned);

        Ok(())
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
    // We must *never* attempt to wait for a catalog entry future while inner is locked - this will
    // cause a deadlock!
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

    pub fn get(&self, hash: ObjectHash) -> Option<CatalogEntry> {
        self.inner.lock().unwrap().objects.get(&hash).map(|entry| {
            entry.clone()
        })
    }

    pub fn search<K: Borrow<[u8]>>(&self, bytes: K) -> Vec<ObjectHash> {
        self.inner
            .lock()
            .unwrap()
            .objects
            .iter_prefix(bytes.borrow())
            .map(|(&hash, _)| hash)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().objects.count()
    }

    /// Clear all registered hashes from the catalog. Clearing a `Catalog` will cancel any
    /// in-progress locks, causing any outstanding locks to panic when dropped and `CatalogFuture`s
    /// to return `Err`s.
    ///
    /// Needless to say it is not a good idea to call this while writing any objects.
    pub fn clear(&self) -> Result<()> {
        let mut inner_lock = self.inner.lock().unwrap();
        let objects = mem::replace(&mut inner_lock.objects, Trie::new());

        for (_, value) in objects {
            if let CatalogEntry::Locked(future) = value {
                // Using `clear` on a `Catalog` which is locked is Very Bad and will probably cause
                // everything to crash.
                future.cancel_lock()?;
            }
        }

        let _ = inner_lock;

        Ok(())
    }
}


impl Drop for CatalogInner {
    fn drop(&mut self) {
        let mut file = File::create(&self.catalog_path).unwrap();
        bincode::serialize_into(&mut file, &self.objects, bincode::Infinite).unwrap();
    }
}


#[derive(Debug)]
pub struct Registry {
    catalogs: HashMap<Option<String>, Option<Catalog>>,
    paths: Arc<Paths>,
}


impl Registry {
    pub fn new(config: &Config, paths: &Arc<Paths>) -> Self {
        Self {
            catalogs: config
                .remotes
                .keys()
                .map(|key| (Some(key.to_owned()), None))
                .chain(iter::once((None, None)))
                .collect(),
            paths: paths.clone(),
        }
    }

    pub fn get(&mut self, name_opt: Option<String>) -> Result<Catalog> {
        match self.catalogs.get(&name_opt) {
            Some(&Some(ref catalog)) => return Ok(catalog.clone()),
            Some(&None) => {}
            None => bail!(ErrorKind::CatalogNotFound(name_opt)),
        }

        let catalog_path = match name_opt {
            Some(ref name) => {
                Cow::Owned(self.paths.remote_catalogs.join(
                    format!("{}.catalog", name),
                ))
            }
            None => Cow::Borrowed(&self.paths.local_catalog),
        };
        let catalog = Catalog::load(catalog_path.clone().into_owned()).chain_err(
            || {
                ErrorKind::CatalogLoad(catalog_path.into_owned())
            },
        )?;
        self.catalogs.insert(name_opt, Some(catalog.clone()));

        Ok(catalog)
    }

    pub fn clear(&mut self) -> Result<()> {
        let catalog_names = self.catalogs.keys().cloned().collect::<Vec<_>>();

        for name in catalog_names {
            self.get(name)?.clear()?;
        }

        Ok(())
    }
}
