pub mod decode;
pub mod encode;

use std::collections::BTreeMap;

use store::Handle;

#[derive(Debug, Clone)]
pub enum Object<H: Handle> {
    Small(SmallObject),
    Large(LargeObject<H>),
    Tree(TreeObject<H>),
    Commit(CommitObject<H>),
}

#[derive(Debug, Clone)]
pub struct SmallObject {
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectKind {
    Small,
    Large,
    Tree,
    Commit,
}

#[derive(Debug, Clone)]
pub struct LargeObject<H: Handle> {
    children: Vec<(u64, ObjectKind, H)>,
}

#[derive(Debug, Clone)]
pub struct TreeObject<H: Handle> {
    entries: BTreeMap<String, (ObjectKind, H)>,
}

#[derive(Debug, Clone)]
pub struct CommitObject<H: Handle> {
    subtree: H,
    parents: Vec<H>,
}
