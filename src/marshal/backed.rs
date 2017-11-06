use std::borrow::Borrow;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

use futures::future;
use futures::prelude::*;

use errors::*;
use marshal::{ObjectHash, Object, Marshaller};
use marshal::tree::{Tree as RawTree, Blocked, Entry};
use store::Store;
use trace::Trace;


pub struct Tree<S: Store> {
    tree: RawTree,
    store: S,
}


impl<S: Store> From<Tree<S>> for RawTree {
    fn from(tree: Tree<S>) -> RawTree {
        tree.tree
    }
}


#[async]
fn bounce<I: IntoIterator + 'static, S: Store>(
    tree: RawTree,
    path: I,
    store: S,
) -> Result<(Entry<I::IntoIter>, S)>
where
    I::Item: Borrow<OsStr> + Into<OsString>,
{
    let mut entry_res = tree.entry(path);

    loop {
        match entry_res {
            Ok(entry) => return Ok((entry, store)),
            Err(blocked) => {
                let blocking_hash = blocked.object_hash();
                let entries = match await!(store.read_object(blocking_hash))? {
                    Object::Subtree(subtree_object) => subtree_object.entries,
                    _ => bail!("Expected a subtree!"),
                };

                entry_res = blocked.unblock(entries.into());
            }
        }
    }
}


pub enum TreeOp {
    Insert(PathBuf, ObjectHash),
    Remove(PathBuf),
}


impl TreeOp {
    pub fn path(&self) -> &Path {
        match *self {
            TreeOp::Insert(ref path, _) |
            TreeOp::Remove(ref path) => path,
        }
    }

    pub fn into_insert(self) -> Option<(PathBuf, ObjectHash)> {
        match self {
            TreeOp::Insert(path, object_hash) => Some((path, object_hash)),
            TreeOp::Remove(_) => None,
        }
    }
}


impl<S: Store> Tree<S> {
    pub fn new(store: S, root: ObjectHash) -> Self {
        Self {
            tree: RawTree::from(root),
            store,
        }
    }

    #[async]
    pub fn operate<I: IntoIterator<Item = TreeOp> + 'static>(mut self, ops: I) -> Result<Self> {
        let mut ops_vec = ops.into_iter().collect::<Vec<_>>();
        ops_vec.sort_unstable_by(|l, r| l.path().cmp(r.path()));
        for op in ops_vec {
            let path_vec = op.path().iter().map(OsStr::to_owned).collect::<Vec<_>>();
            match op {
                TreeOp::Insert(_, object_hash) => {
                    self = await!(self.insert(path_vec, object_hash))?
                }
                TreeOp::Remove(_) => self = await!(self.remove(path_vec))?,
            }
        }

        Ok(self)
    }

    #[async]
    pub fn insert<I: IntoIterator<Item = OsString> + 'static>(
        self,
        path: I,
        object_hash: ObjectHash,
    ) -> Result<Self> {
        let (entry, store) = await!(bounce(self.tree, path, self.store))?;
        let tree = match entry {
            Entry::Occupied(occupied) => occupied.replace(object_hash).into_inner(),
            Entry::Vacant(vacant) => vacant.insert(object_hash).into_inner(),
        };

        Ok(Self { tree, store })
    }

    #[async]
    pub fn remove<I: IntoIterator<Item = OsString> + 'static>(self, path: I) -> Result<Self> {
        let (entry, store) = await!(bounce(self.tree, path, self.store))?;
        let tree = match entry {
            Entry::Occupied(occupied) => occupied.remove().into_inner(),
            Entry::Vacant(vacant) => vacant.into_inner(),
        };

        Ok(Self { tree, store })
    }

    #[async]
    pub fn marshal<T: Trace>(self, marshaller: Marshaller<T>) -> Result<ObjectHash> {
        await!(self.tree.marshal(marshaller))
    }
}
