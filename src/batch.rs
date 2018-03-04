use std::{collections::HashMap, sync::Arc};

use failure::*;
use futures::prelude::*;
use im::List;

use object::{ObjectRef, TreeBuilder};
use path::ObjectPath;
use store::prelude::*;

#[derive(Debug, Clone)]
enum Node<B: Backend> {
    Add(ObjectRef<Handle<B>>),
    Delete,
    Branch(HashMap<Arc<String>, Node<B>>),
}

impl<B: Backend> Node<B> {
    fn new() -> Self {
        Node::Branch(HashMap::new())
    }

    fn singleton(key: Arc<String>, node: Self) -> Self {
        let mut hash_map = HashMap::new();
        hash_map.insert(key, node);
        Node::Branch(hash_map)
    }

    fn leaf(objref: Option<ObjectRef<Handle<B>>>) -> Self {
        match objref {
            Some(objref) => Node::Add(objref),
            None => Node::Delete,
        }
    }

    fn chain(path: List<String>, value: Option<ObjectRef<Handle<B>>>) -> Self {
        match path.uncons() {
            Some((head, tail)) => Self::singleton(head, Self::chain(tail, value)),
            None => Self::leaf(value),
        }
    }

    #[async]
    fn do_insert(
        head: Arc<String>,
        tail: List<String>,
        value: Option<ObjectRef<Handle<B>>>,
        mut map: HashMap<Arc<String>, Self>,
    ) -> Result<HashMap<Arc<String>, Self>, Error> {
        match map.remove(&head) {
            Some(entry) => {
                let node = await!(entry.insert(tail, value))?;
                map.insert(head, node);
            }
            None => {
                map.insert(head, Self::chain(tail, value));
            }
        };
        Ok(map)
    }

    #[async(boxed)]
    fn insert(
        self,
        path: List<String>,
        value: Option<ObjectRef<Handle<B>>>,
    ) -> Result<Self, Error> {
        match (path.uncons(), self) {
            (Some((head, tail)), Node::Add(ObjectRef::Tree(tree_ref))) => {
                let tree = await!(tree_ref.fetch()).context("Error fetching unloaded branch node")?;
                let mut map = tree.into_iter()
                    .map(|(k, v)| (Arc::new(k), Self::leaf(Some(v))))
                    .collect::<HashMap<_, _>>();
                let new_map = await!(Self::do_insert(head, tail, value, map))?;
                Ok(Node::Branch(new_map))
            }
            (Some((head, tail)), Node::Branch(mut map)) => Ok(Node::Branch(await!(
                Self::do_insert(head, tail, value, map)
            )?)),
            (Some((head, tail)), _) => Ok(Self::singleton(head, Self::chain(tail, value))),
            (None, _) => Ok(Node::leaf(value)),
        }
    }

    #[async]
    fn do_remove(
        head: Arc<String>,
        tail: List<String>,
        mut map: HashMap<Arc<String>, Self>,
    ) -> Result<Self, Error> {
        if tail.is_empty() {
            map.remove(&head);
        } else if let Some(entry) = map.remove(&head) {
            let node = await!(entry.remove(tail))?;
            map.insert(head, node);
        }

        Ok(Node::Branch(map))
    }

    #[allow(dead_code)]
    #[async(boxed)]
    fn remove(self, path: List<String>) -> Result<Self, Error> {
        match (path.uncons(), self) {
            (Some((head, tail)), Node::Add(ObjectRef::Tree(tree_ref))) => {
                let tree = await!(tree_ref.fetch())?;
                let mut map = tree.into_iter()
                    .map(|(k, v)| (Arc::new(k), Self::leaf(Some(v))))
                    .collect::<HashMap<_, _>>();
                await!(Self::do_remove(head, tail, map))
            }
            (Some((head, tail)), Node::Branch(map)) => await!(Self::do_remove(head, tail, map)),
            (Some(_), _) => bail!("No such object to remove!"),
            (None, _) => Ok(Self::new()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Operation<B: Backend> {
    Add(ObjectPath, ObjectRef<Handle<B>>),
    Delete(ObjectPath),
}

impl<B: Backend> Operation<B> {
    pub fn as_object_path(&self) -> &ObjectPath {
        match *self {
            Operation::Add(ref object_path, _) => object_path,
            Operation::Delete(ref object_path) => object_path,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Batch<B: Backend> {
    root: HashMap<Arc<String>, Node<B>>,
}

impl<B: Backend> Batch<B> {
    pub fn new() -> Self {
        Self {
            root: HashMap::new(),
        }
    }

    #[async]
    pub fn add(self, op: Operation<B>) -> Result<Self, Error> {
        let (path, value) = match op {
            Operation::Add(path, value) => (path, Some(value)),
            Operation::Delete(path) => (path, None),
        };
        let (head, tail) = path.inner
            .uncons()
            .ok_or_else(|| format_err!("Cannot replace or delete the root node!"))?;
        let root = await!(Node::do_insert(head, tail, value, self.root))
            .context("Error while inserting operation into batch trie")?;

        Ok(Self { root })
    }

    #[async]
    pub fn run(
        self,
        store: Store<B>,
        tree_builder: TreeBuilder<Handle<B>>,
    ) -> Result<TreeBuilder<Handle<B>>, Error> {
        Ok(await!(self.into_iter().run(store, tree_builder))?)
    }
}

impl<B: Backend> IntoIterator for Batch<B> {
    type IntoIter = BatchIter<B>;
    type Item = (Arc<String>, BatchedOp<B>);

    fn into_iter(self) -> Self::IntoIter {
        BatchIter(self.root.into_iter())
    }
}

pub enum BatchedOp<B: Backend> {
    Add(ObjectRef<Handle<B>>),
    Delete,
    Recurse(BatchIter<B>),
}

pub struct BatchIter<B: Backend>(<HashMap<Arc<String>, Node<B>> as IntoIterator>::IntoIter);

impl<B: Backend> Iterator for BatchIter<B> {
    type Item = (Arc<String>, BatchedOp<B>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(name, node)| match node {
            Node::Add(objref) => (name, BatchedOp::Add(objref)),
            Node::Delete => (name, BatchedOp::Delete),
            Node::Branch(map) => (name, BatchedOp::Recurse(BatchIter(map.into_iter()))),
        })
    }
}

impl<B: Backend> BatchIter<B> {
    // TODO: Optimize (recursions should be able to make requests concurrently.)
    #[async(boxed)]
    fn run(
        self,
        store: Store<B>,
        tree_builder: TreeBuilder<Handle<B>>,
    ) -> Result<TreeBuilder<Handle<B>>, Error> {
        for (name, batched_op) in self {
            match batched_op {
                BatchedOp::Add(objref) => {
                    tree_builder.insert(
                        Arc::try_unwrap(name).unwrap_or_else(|arcd| (*arcd).clone()),
                        objref,
                    );
                }
                BatchedOp::Delete => {
                    tree_builder.remove(name.as_ref());
                }
                BatchedOp::Recurse(batch_iter) => {
                    let child_builder = match tree_builder.remove(name.as_ref()) {
                        Some(ObjectRef::Tree(tree_ref)) => await!(tree_ref.fetch())?.diverge(),
                        _ => TreeBuilder::new(),
                    };

                    let child_built = await!(batch_iter.run(store.clone(), child_builder))?;

                    if !child_built.is_empty() {
                        let child_ref = await!(child_built.as_tree().send(&store))?;
                        tree_builder.insert(
                            Arc::try_unwrap(name).unwrap_or_else(|arcd| (*arcd).clone()),
                            ObjectRef::Tree(child_ref),
                        );
                    }
                }
            }
        }

        Ok(tree_builder)
    }
}
