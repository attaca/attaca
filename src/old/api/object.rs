use std::{iter, mem, cell::Cell, collections::{BTreeMap, HashMap}, io::Read,
          sync::{Arc, RwLock, Weak, atomic::Ordering}};

use chashmap::CHashMap;
use failure::{self, Error};
use futures::{future::{self, Shared}, prelude::*, stream::FuturesUnordered, sync::oneshot};

use arc_slice::ArcSlice;
use api::atomic_weak::AtomicWeak;
use api::store::Store;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

#[derive(Debug)]
pub struct LargeChild {
    size: u64,
    object: Arc<Object>,
}

#[derive(Debug)]
pub enum DataContent {
    Small(Vec<u8>),
    Large(Vec<LargeChild>),
}

#[derive(Debug)]
pub enum SubtreeChild {
    File { size: u64, object: Arc<Object> },
    Directory(Arc<Object>),
}

#[derive(Debug)]
pub struct SubtreeContent {
    entries: BTreeMap<String, Arc<Object>>,
}

#[derive(Debug)]
pub struct CommitContent {
    subtree: Arc<Object>,
    parents: Vec<Arc<Object>>,
    message: String,
}

#[derive(Debug)]
pub enum Content {
    Data(DataContent),
    Subtree(SubtreeContent),
    Commit(CommitContent),
}

impl Content {
    pub fn dependencies<'a>(&'a self) -> Box<Iterator<Item = Arc<Object>> + 'a> {
        match *self {
            Content::Small(_) => Box::new(iter::empty()),
            Content::Large(ref cs) => Box::new(cs.iter().map(|ch| ch.object.clone())),
            Content::Subtree(ref ent) => Box::new(ent.values().map(|ch| match *ch {
                SubtreeChild::Directory(ref object) | SubtreeChild::File { ref object, .. } => {
                    object.clone()
                }
            })),
            Content::Commit(ref cmt) => {
                Box::new(iter::once(cmt.subtree.clone()).chain(cmt.parents.iter().cloned()))
            }
        }
    }
}

#[derive(Debug)]
pub struct Object {
    digest: Digest,
    content: AtomicWeak<Content>,
    cache: Weak<Cache>,
}

impl Object {
    pub fn content(this: &Arc<Self>) -> Box<Future<Item = Arc<Content>, Error = Error>> {
        match Weak::upgrade(&this.content.load(Ordering::Relaxed)) {
            Some(content) => Box::new(future::ok(content)),
            None => Cache::fix_miss(&Weak::upgrade(&this.cache).unwrap(), this),
        }
    }
}

/// An entry in the object cache indicates whether the object is:
///
/// 1. Currently not stored locally (`NotHere`)
/// 2. In the process of being fetched from a remote location (`NotReady`)
/// 3. Stored locally, optionally noted as having originated from some remote source (`Ready`)
///
/// The remote source stored in `Entry::Ready` is used in the case that too much local memory is
/// being used, and the cache must be cleared in order to make room for more data. If the entry's
/// store field is nonempty, then the object is assumed to be available from the store to re-fetch
/// in the case that the entry is cleared; otherwise, it is assumed to be unsafe to release the
/// entry without saving it locally first.
#[derive(Debug)]
pub enum Entry {
    NotHere(Arc<Store>),
    NotReady(Shared<oneshot::Receiver<Arc<Content>>>),
    Ready(Arc<Content>, Option<Arc<Store>>),
}

/// If an object is in the "objects" map, it may or may not weakly link to a member of the
/// "content" map; however, members of the "objects" map and members of the "content" map are
/// always in a one-to-one correspondence.
#[derive(Debug, Default)]
pub struct Cache {
    objects: CHashMap<Digest, Arc<Object>>,
    content: CHashMap<Digest, Entry>,
}

impl Cache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(this: &Arc<Cache>, digest: &Digest) -> Option<Arc<Object>> {
        this.objects.get(digest).map(|guard| (*guard).clone())
    }

    pub fn get_or_insert_remote(
        this: &Arc<Cache>,
        digest: &Digest,
        store: &Arc<Store>,
    ) -> Arc<Object> {
        if let Some(guard) = this.objects.get(digest) {
            return guard.clone();
        }

        // HACK: CHashMap has no entry API, so we have to use CHashMap::upsert to do things
        // atomically. First insert a lazily updated entry only if the entry does not already
        // exist; then, *if* the object entry isn't already there (due to a potential race between
        // `get_or_insert`s) create a new `Arc`'d object and insert it, assigning it to the
        // `result_object` binding, and otherwise create a new reference to the already created
        // object and make it our `result_object`. Either way, `result_object` is `Some`.
        let mut result_object = Cell::new(None);
        this.content
            .upsert(*digest, || Entry::NotHere(store.clone()), |_| {});
        this.objects.upsert(
            *digest,
            || {
                let object = Arc::new(Object {
                    digest: *digest,
                    content: AtomicWeak::new(Weak::new()),
                    cache: Arc::downgrade(this),
                });
                result_object.set(Some(object.clone()));
                object
            },
            |object| {
                result_object.set(Some(object.clone()));
            },
        );
        result_object.into_inner().unwrap()
    }

    pub fn get_or_insert_local(
        this: &Arc<Cache>,
        digest: &Digest,
        content: &Arc<Content>,
    ) -> Arc<Object> {
        if let Some(guard) = this.objects.get(digest) {
            return guard.clone();
        }

        let mut result_object = Cell::new(None);
        this.content
            .upsert(*digest, || Entry::Ready(content.clone(), None), |_| {});
        this.objects.upsert(
            *digest,
            || {
                let object = Arc::new(Object {
                    digest: *digest,
                    content: AtomicWeak::new(Arc::downgrade(content)),
                    cache: Arc::downgrade(this),
                });
                result_object.set(Some(object.clone()));
                object
            },
            |object| {
                result_object.set(Some(object.clone()));
            },
        );
        result_object.into_inner().unwrap()
    }

    fn fix_miss(
        this: &Arc<Cache>,
        object: &Arc<Object>,
    ) -> Box<Future<Item = Arc<Content>, Error = Error>> {
        match *this.content.get(&object.digest).unwrap() {
            Entry::NotHere(ref store) => {
                let this = this.clone();
                let object = object.clone();
                let (tx, rx) = oneshot::channel();
                this.content
                    .insert(object.digest, Entry::NotReady(rx.shared()));

                Box::new(store.get(&object.digest).map(move |ct| {
                    let content = Arc::new(ct);
                    tx.send(content.clone()).unwrap();
                    this.content
                        .insert(object.digest, Entry::Ready(content.clone(), None));
                    content
                }))
            }
            Entry::NotReady(ref shared) => Box::new(shared.clone().then(|res| match res {
                Ok(sc) => Ok((*sc).clone()),
                Err(_) => Err(failure::err_msg("Upstream error!")),
            })),
            Entry::Ready(ref content, _) => {
                object
                    .content
                    .store(Arc::downgrade(content), Ordering::Relaxed);
                Box::new(future::ok(content.clone()))
            }
        }
    }
}
