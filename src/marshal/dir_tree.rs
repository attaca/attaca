use std::collections::HashMap;
use std::ffi::OsString;
use std::mem;
use std::path::PathBuf;

use futures::future::{self, Either, Loop};
use futures::prelude::*;
use sequence_trie::SequenceTrie;

use context::Store;
use errors::*;
use marshal::{ObjectHash, Object};


#[derive(Debug)]
pub enum Node {
    Branch(HashMap<OsString, Node>),
    Leaf(ObjectHash),
    Empty,
}


impl Node {
    pub fn entry<K, I, S, T, A, F>(
        mut self,
        mut path: I,
        store: S,
        func: F,
    ) -> Box<Future<Item = (Self, T), Error = Error> + Send>
    where
        K: Into<OsString> + 'static,
        I: Iterator<Item = K> + Send + 'static,
        S: Store,
        T: Send + 'static,
        A: IntoFuture<Item = T, Error = Error> + 'static,
        A::Future: Send,
        F: FnOnce(Option<&mut Node>) -> A + Send + 'static,
    {
        let async = {
            async_block! {
                let next = path.next().map(Into::into);

                match next {
                    Some(component) => {
                        match self {
                            Node::Branch(mut entries) => {
                                let node = entries
                                    .get_mut(&component)
                                    .map(|entry| mem::replace(entry, Node::Empty))
                                    .unwrap_or(Node::Empty);

                                await!(node.entry(path, store, func).map(move |(new_node, out)| {
                                    entries.insert(component, new_node);
                                    (Node::Branch(entries), out)
                                }))
                            }
                            Node::Leaf(hash) => {
                                let object = await!(store.read_object(hash))?;
                                if let Object::Subtree(subtree) = object {
                                    let mut entries: HashMap<
                                        _,
                                        _,
                                    > = subtree
                                        .entries
                                        .into_iter()
                                        .map(|(component, entry)| (component, Node::Leaf(entry)))
                                        .collect();

                                    let node = entries
                                        .get_mut(&component)
                                        .map(|entry| mem::replace(entry, Node::Empty))
                                        .unwrap_or(Node::Empty);

                                    await!(
                                        node.entry(path, store, func).map(move |(new_node, out)| {
                                            entries.insert(component, new_node);
                                            (Node::Branch(entries), out)
                                        })
                                    )
                                } else {
                                    Ok((self, await!(func(None).into_future())?))
                                }
                            }
                            Node::Empty => {
                                let (new_node, out) = await!(Node::Empty.entry(path, store, func))?;
                                let mut entries = HashMap::new();
                                entries.insert(component, new_node);
                                Ok((Node::Branch(entries), out))
                            }
                        }
                    }
                    None => {
                        let out = await!(func(Some(&mut self)).into_future())?;
                        Ok((self, out))
                    }
                }
            }
        };

        Box::new(async)
    }
}


#[derive(Debug)]
pub struct DirTree {
    pub(super) root: Node,
}


impl DirTree {
    pub fn delta<S: Store, I: IntoIterator<Item = (PathBuf, Option<ObjectHash>)> + 'static>(
        store: S,
        root_opt: Option<ObjectHash>,
        ops: I,
    ) -> Box<Future<Item = DirTree, Error = Error> + Send> {
        let mut op_trie = SequenceTrie::new();
        ops.into_iter().for_each(|(path, hash_opt)| {
            op_trie.insert(&path, hash_opt);
        });

        let dir_tree = root_opt.map(Node::Leaf).unwrap_or(Node::Empty);
        let children: Vec<_> = op_trie.into_inner().1.into_iter().collect();
        let result = future::loop_fn((dir_tree, children, Vec::new()), move |(dir_tree,
               mut stack,
               mut path)| {
            if let Some((component, subtrie)) = stack.pop() {
                path.push(component);
                let value_opt = subtrie.value().cloned();

                match value_opt {
                    Some(op) => {
                        // When removing or inserting a node, we can rely on the area beyond the
                        // node *never* being modified again. This is because we only
                        // insert/remove files. If this is changed in the future, then
                        // recursion will have to continue past this point.
                        //
                        // TODO: Better error reporting than `.unwrap()`. This is a
                        // concern: if `node` is `None`, then the only possible
                        // explanation is that an element of the deltas referred to a
                        // directory rather than a file. This shouldn't be possible
                        // anyways since we don't recurse further in this case.
                        let consumed_path = path.clone();
                        let consumed_store = store.clone();

                        let operate_future = dir_tree
                            .entry(consumed_path.into_iter(), consumed_store, move |node| {
                                if let Some(hash) = op {
                                    *node.unwrap() = Node::Leaf(hash);
                                } else {
                                    *node.unwrap() = Node::Empty;
                                }

                                Ok(())
                            }).map(|(dir_tree, ())| { 
                                assert!(path.pop().is_some()); 
                                Loop::Continue((dir_tree, stack, path)) 
                            });

                        Either::A(operate_future)
                    }

                    None => {
                        // If we don't remove this node, we need to extend the stack with all
                        // children.
                        stack.extend(subtrie.into_inner().1);
                        Either::B(future::ok(Loop::Continue((dir_tree, stack, path))))
                    }
                }
            } else {
                Either::B(future::ok(Loop::Break(DirTree { root: dir_tree })))
            }
        });

        Box::new(result)
    }
}
