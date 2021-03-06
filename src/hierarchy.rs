use std::{fmt, collections::HashMap, sync::Arc};

use failure::{Compat, Error, ResultExt};
use futures::{future::Shared, prelude::*};
use im::List;
use parking_lot::RwLock;

use object::{FutureTree, ObjectRef, Tree, TreeRef};
use path::ObjectPath;
use store::prelude::*;

#[derive(Debug)]
pub struct Hierarchy<B: Backend> {
    root: Option<Arc<RwLock<Node<B>>>>,
}

impl<B: Backend> Clone for Hierarchy<B> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl<B: Backend> From<TreeRef<Handle<B>>> for Hierarchy<B> {
    fn from(tree_ref: TreeRef<Handle<B>>) -> Self {
        Self {
            root: Some(Arc::new(RwLock::new(Node {
                objref: ObjectRef::Tree(tree_ref),
                state: NodeState::UnPolled,
            }))),
        }
    }
}

impl<B: Backend> Hierarchy<B> {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn get(
        &self,
        path: ObjectPath,
    ) -> impl Future<Item = Option<ObjectRef<Handle<B>>>, Error = Error> {
        let root = self.root.clone();

        async_block! {
            match root {
                Some(node) => Ok(await!(Node::get(node, path.inner))?),
                None => Ok(None),
            }
        }
    }
}

struct CompatFutureTree<B: Backend>(FutureTree<B>);

impl<B: Backend> fmt::Debug for CompatFutureTree<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("CompatFutureTree").field(&"OPAQUE").finish()
    }
}

impl<B: Backend> Future for CompatFutureTree<B> {
    type Item = Tree<Handle<B>>;
    type Error = Compat<Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().compat()
    }
}

#[derive(Debug, Clone)]
enum NodeState<B: Backend> {
    UnPolled,
    NotReady(Shared<CompatFutureTree<B>>),
    Ready(HashMap<Arc<String>, Arc<RwLock<Node<B>>>>),
}

impl<B: Backend> NodeState<B> {
    fn update_if_unpolled_and_extract_shared(
        &mut self,
        head: &Arc<String>,
        tree_ref: TreeRef<Handle<B>>,
    ) -> Option<Secondary<B>> {
        match *self {
            NodeState::UnPolled => {
                let shared = CompatFutureTree(tree_ref.fetch()).shared();
                *self = NodeState::NotReady(shared.clone());
                Some(Secondary::NotReady(shared))
            }
            NodeState::NotReady(ref shared) => Some(Secondary::NotReady(shared.clone())),
            NodeState::Ready(ref hash_map) => hash_map.get(head).cloned().map(Secondary::Ready),
        }
    }

    fn extract_shared(&self, head: &Arc<String>) -> Option<Secondary<B>> {
        match *self {
            NodeState::UnPolled => {
                unreachable!("Missed write opportunity, should no longer be UnPolled!")
            }
            NodeState::NotReady(ref shared) => Some(Secondary::NotReady(shared.clone())),
            NodeState::Ready(ref hash_map) => hash_map.get(head).cloned().map(Secondary::Ready),
        }
    }
}

enum Primary<B: Backend> {
    UnPolled(TreeRef<Handle<B>>),
    NotReady(Shared<CompatFutureTree<B>>),
    Ready(Arc<RwLock<Node<B>>>),
}

impl<B: Backend> Primary<B> {
    fn update_if_unpolled(
        self,
        this: &Arc<RwLock<Node<B>>>,
        head: &Arc<String>,
    ) -> Option<Secondary<B>> {
        match self {
            // If we're unpolled, attempt to acquire a write guard in order to poll the tree ref.
            Primary::UnPolled(tree_ref) => match this.try_write() {
                // We've acquired the write guard; create a shared future and update the node state
                // to `NotReady`.
                Some(mut write_guard) => write_guard
                    .state
                    .update_if_unpolled_and_extract_shared(head, tree_ref),

                // We've missed the write guard. Wait until the write-guarded task is done and then
                // clone the resulting shared future.
                None => this.read().state.extract_shared(head),
            },

            // If we're not unpolled, just keep going. Nothing to do here.
            Primary::NotReady(shared) => Some(Secondary::NotReady(shared)),
            Primary::Ready(node) => Some(Secondary::Ready(node)),
        }
    }
}

enum Secondary<B: Backend> {
    NotReady(Shared<CompatFutureTree<B>>),
    Ready(Arc<RwLock<Node<B>>>),
}

#[derive(Debug, Clone)]
struct Node<B: Backend> {
    objref: ObjectRef<Handle<B>>,
    state: NodeState<B>,
}

impl<B: Backend> Node<B> {
    #[async(boxed)]
    fn get(
        this: Arc<RwLock<Self>>,
        path: List<String>,
    ) -> Result<Option<ObjectRef<Handle<B>>>, Error> {
        match path.uncons() {
            None => Ok(Some(this.read().objref.clone())),
            Some((head, tail)) => {
                let primary = {
                    let read_guard = this.read();

                    match read_guard.state {
                        NodeState::UnPolled => match read_guard.objref {
                            ObjectRef::Tree(ref tree_ref) => Primary::UnPolled(tree_ref.clone()),
                            _ => return Ok(None),
                        },
                        NodeState::NotReady(ref shared_ref) => {
                            Primary::NotReady(shared_ref.clone())
                        }
                        NodeState::Ready(ref hash_map) => match hash_map.get(&head).cloned() {
                            Some(node) => Primary::Ready(node),
                            None => return Ok(None),
                        },
                    }
                };

                match primary.update_if_unpolled(&this, &head) {
                    // Poll the shared future to completion, then race to emplace the result.
                    Some(Secondary::NotReady(shared)) => {
                        let shared_tree = await!(shared)?;

                        let future_node = {
                            // See if we're the node who wins the replacement race.
                            let guard = match this.try_write() {
                                // We win - read out the fetched tree and set the state.
                                Some(mut guard) => {
                                    let hash_map = shared_tree
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                Arc::new(k.to_owned()),
                                                Arc::new(RwLock::new(Self {
                                                    objref: v.to_owned(),
                                                    state: NodeState::UnPolled,
                                                })),
                                            )
                                        })
                                        .collect();
                                    guard.state = NodeState::Ready(hash_map);
                                    guard.downgrade()
                                }
                                // We lose - wait for a read lock.
                                None => this.read(),
                            };

                            // The depended-upon node has been loaded; recurse.
                            match guard.state {
                                NodeState::Ready(ref hash_map) => hash_map
                                    .get(&head)
                                    .map(|node| Self::get(node.clone(), tail)),

                                // Impossible! We just polled everything to completion.
                                NodeState::UnPolled | NodeState::NotReady(_) => unreachable!(),
                            }
                        };

                        Ok(await!(future_node)?.and_then(|x| x))
                    }
                    Some(Secondary::Ready(node)) => Ok(await!(Self::get(node, tail))?),
                    None => Ok(None),
                }
            }
        }
    }
}
