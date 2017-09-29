use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::path::Path;

use futures::prelude::*;

use errors::*;
use marshal::{Hasher, ObjectHash, SubtreeObject, DataTree};
use trace::MarshalTrace;


#[derive(Debug)]
pub enum Node {
    Branch(Branch),
    Leaf(DataTree),
}


impl Node {
    fn total(&self) -> usize {
        match *self {
            Node::Branch(ref branch) => branch.total(),
            Node::Leaf(ref leaf) => leaf.total(),
        }
    }


    fn marshal<T: MarshalTrace>(
        self,
        hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        match self {
            Node::Branch(branch) => branch.marshal(hasher),
            Node::Leaf(data) => data.marshal(hasher),
        }
    }
}


impl From<DataTree> for Node {
    fn from(data_tree: DataTree) -> Node {
        Node::Leaf(data_tree)
    }
}


impl From<DirTree> for Node {
    fn from(dir_tree: DirTree) -> Node {
        Node::Branch(dir_tree.root)
    }
}


#[derive(Debug)]
pub struct Branch {
    entries: HashMap<OsString, Node>,
}


impl Branch {
    fn new() -> Branch {
        Branch { entries: HashMap::new() }
    }


    fn total(&self) -> usize {
        let n: usize = self.entries.iter().map(|(_, value)| value.total()).sum();
        n + 1
    }


    fn insert<T: Into<Node>>(&mut self, mut stack: Vec<&OsStr>, data: T) {
        let key = stack.pop().unwrap().to_owned();
        let value = if stack.is_empty() {
            data.into()
        } else {
            let mut branch = Branch { entries: HashMap::new() };
            branch.insert(stack, data);
            Node::Branch(branch)
        };

        self.entries.insert(key, value);
    }


    fn marshal<T: MarshalTrace>(
        self,
        mut hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let result = {
            async_block! {
                let mut entries = BTreeMap::new();

                for (key, value) in self.entries.into_iter() {
                    let object_hash = await!(value.marshal(hasher.clone()))?;
                    entries.insert(key.into(), object_hash);
                }

                await!(hasher.compute(SubtreeObject { entries }))
            }
        };

        Box::new(result)
    }
}


#[derive(Debug)]
pub struct DirTree {
    root: Branch,
}


impl DirTree {
    pub fn new() -> DirTree {
        DirTree { root: Branch::new() }
    }


    pub fn total(&self) -> usize {
        self.root.total()
    }


    pub fn insert<P: AsRef<Path>, T: Into<Node>>(&mut self, path: P, data: T) {
        let stack = path.as_ref().iter().rev().collect();
        self.root.insert(stack, data);
    }


    pub fn marshal<T: MarshalTrace>(
        self,
        hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        self.root.marshal(hasher)
    }
}
