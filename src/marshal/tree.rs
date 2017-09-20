use std::borrow::Cow;
use std::mem;

use futures::future;
use futures::prelude::*;
use futures::stream::{self, FuturesOrdered};

use errors::*;
use marshal::{Hasher, ObjectHash, LargeObject, SmallRecord};
use trace::MarshalTrace;


/// The branching factor of the data object B+ tree structure.
const BRANCH_FACTOR: usize = 1024;

const ERR_BRANCH_FACTOR_TOO_SMALL: &'static str = "BRANCH_FACTOR too small (less than 2?) freshly split node should always have space for one more child!";
const ERR_BYTE_OUTSIDE_OF_TREE: &'static str = "Node::search_bytes should only be called when we know the byte is in the tree!";

/// A `Leaf` node of a B+ tree contains a list of records.
#[derive(Clone, Debug)]
struct Leaf<'a> {
    size: u64,
    records: Vec<SmallRecord<'a>>,
}


impl<'a> From<Vec<SmallRecord<'a>>> for Leaf<'a> {
    fn from(records: Vec<SmallRecord<'a>>) -> Self {
        let size = records.iter().map(SmallRecord::size).sum();

        Leaf { size, records }
    }
}


impl<'a> Leaf<'a> {
    fn new() -> Self {
        Vec::new().into()
    }


    /// The size, in bytes, of the data the `Leaf` represents.
    fn size(&self) -> u64 {
        self.size
    }


    /// The total number of records in this leaf.
    fn count(&self) -> usize {
        self.records.len()
    }


    /// Append a record to a leaf; if the leaf cannot hold the record and must split, this function
    /// will return `Err` with the left-hand side of the split.
    fn append(&mut self, record: SmallRecord<'a>) -> ::std::result::Result<(), Leaf<'a>> {
        if self.records.len() < BRANCH_FACTOR {
            self.size += record.size();
            self.records.push(record);
            Ok(())
        } else {
            let right_records = self.records.split_off(BRANCH_FACTOR / 2);
            let mut right = Leaf::from(right_records);

            self.size -= right.size();

            right.append(record).expect(ERR_BRANCH_FACTOR_TOO_SMALL);

            Err(mem::replace(self, right))
        }
    }


    fn marshal<T: MarshalTrace>(
        self,
        mut hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + 'a> {
        // Being lazy here means that we hash about 3GB a pop, all sequential access.
        let result = future::lazy(move || {
            let mut futures = FuturesOrdered::new();

            let size = self.size();

            for child in self.records {
                let child_size = child.size();
                let mut hasher_captured = hasher.clone();

                let computed = hasher_captured.compute(child);
                let tupled = computed.map(move |child_hash| (child_size, child_hash));

                futures.push(tupled);
            }

            futures.collect().and_then(move |children| {
                hasher.compute(LargeObject {
                    size,
                    children: Cow::Owned(children),
                })
            })
        });

        Box::new(result)
    }
}


/// An internal node of a B+ tree.
#[derive(Clone, Debug)]
struct Internal<'a> {
    size: u64,
    count: usize,
    children: Vec<Node<'a>>,
}


impl<'a> From<Vec<Node<'a>>> for Internal<'a> {
    fn from(children: Vec<Node<'a>>) -> Self {
        let size = children.iter().map(Node::size).sum();
        let count = children.iter().map(Node::count).sum();

        Internal {
            size,
            count,
            children,
        }
    }
}


impl<'a> Internal<'a> {
    /// Construct an internal node containing a single child.
    fn singleton(elem: Node<'a>) -> Self {
        Internal {
            size: elem.size(),
            count: elem.count(),
            children: vec![elem],
        }
    }


    /// The size, in bytes, of the contiguous range of a file this `Internal` node represents.
    fn size(&self) -> u64 {
        self.size
    }


    /// The total number of records held in child nodes.
    fn count(&self) -> usize {
        self.count
    }


    /// Append a node; if we must split to accomodate the new node, the left-hand side of the split
    /// is returned as `Err`.
    fn append(&mut self, node: Node<'a>) -> ::std::result::Result<(), Self> {
        if self.children.len() < BRANCH_FACTOR {
            self.size += node.size();
            self.count += node.count();
            self.children.push(node);
            Ok(())
        } else {
            let right_children = self.children.split_off(BRANCH_FACTOR / 2);
            let mut right = Internal::from(right_children);

            self.size -= right.size;
            self.count -= right.count;

            right.append(node).expect(ERR_BRANCH_FACTOR_TOO_SMALL);

            Err(mem::replace(self, right))
        }
    }


    fn marshal<T: MarshalTrace>(
        self,
        mut hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + 'a> {
        let hasher_captured = hasher.clone();
        let children_future = stream::futures_ordered(self.children.into_iter().map(move |child| {
            let child_size = child.size();

            child.marshal(hasher_captured.clone()).map(
                move |child_hash| {
                    (child_size, child_hash)
                },
            )
        })).collect();

        let self_size = self.size;

        let result = children_future.and_then(move |children| {
            hasher.compute(LargeObject {
                size: self_size,
                children: Cow::Owned(children),
            })
        });

        Box::new(result)
    }
}


/// The general `Node` enum representing either an internal or leaf node in the B+ tree.
#[derive(Clone, Debug)]
enum Node<'a> {
    Internal(Internal<'a>),
    Leaf(Leaf<'a>),
}


impl<'a> Node<'a> {
    /// Convenience function to construct an internal node from a vec of child nodes.
    fn internal(children: Vec<Node<'a>>) -> Self {
        Node::Internal(Internal::from(children))
    }


    /// Get the size, in bytes, of the data this node represents.
    fn size(&self) -> u64 {
        match *self {
            Node::Internal(ref internal) => internal.size(),
            Node::Leaf(ref leaf) => leaf.size(),
        }
    }


    /// The number of records held in child nodes of this node.
    fn count(&self) -> usize {
        match *self {
            Node::Internal(ref internal) => internal.count(),
            Node::Leaf(ref leaf) => leaf.count(),
        }
    }


    /// Perform a "bulk-loading" operation, constructing a whole B+ tree from an iterator of
    /// records.
    // TODO: This could probably be made *much* simpler and *much* more efficient.
    fn load<I>(iterable: I) -> Self
    where
        I: IntoIterator<Item = SmallRecord<'a>>,
    {
        fn insert_into_spine<'a, 'b: 'a, I>(mut stack: I, node: Node<'b>) -> Option<Internal<'b>>
        where
            I: Iterator<Item = &'a mut Internal<'b>>,
        {
            let free = match stack.next() {
                Some(parent) => {
                    match parent.append(node) {
                        Ok(()) => return None,
                        Err(left) => left,
                    }
                }
                None => return Some(Internal::singleton(node)),
            };

            insert_into_spine(stack, Node::Internal(free))
        }

        let mut spine = Vec::new();
        let mut leaf = Leaf::new();

        for record in iterable.into_iter() {
            match leaf.append(record) {
                Ok(()) => {}
                Err(left) => {
                    match insert_into_spine(spine.iter_mut(), Node::Leaf(left)) {
                        Some(internal) => spine.push(internal),
                        None => {}
                    }
                }
            }
        }


        let mut stack = vec![Node::Leaf(leaf)];
        for mut node in spine {
            let mut overflow = None;

            for free in stack.drain(..) {
                match node.append(free) {
                    Ok(()) => {}
                    Err(left) => {
                        assert!(overflow.is_none());
                        overflow = Some(left);
                    }
                }
            }

            if let Some(free) = overflow {
                stack.push(Node::Internal(free));
            }

            stack.push(Node::Internal(node));
        }

        if stack.len() == 1 {
            stack.pop().unwrap()
        } else {
            Node::internal(stack)
        }
    }


    // TODO: While we will eventually  need this functionality, at the moment this implementation
    // of `push` does not correctly preserve record counts.
    //
    // fn push(&mut self, record: SmallRecord<'a>) -> Result<(), Self> {
    //     // Attempt to push this node into our local children (whether we are a leaf or an internal
    //     // node.) If `left` is not empty, we swap ourselves with it and return it as `Err`; it is
    //     // the left side of our new split.
    //     let left = match *self {
    //         Node::Internal(ref mut internal) => {
    //             match internal
    //                 .children
    //                 .last_mut()
    //                 .expect(ERR_INTERNAL_NONEMPTY)
    //                 .push(record) {
    //                 Ok(()) => return Ok(()),
    //                 Err(left) => left,
    //             }
    //         }

    //         Node::Leaf(ref mut leaf) => {
    //             match leaf.append(record) {
    //                 Ok(()) => return Ok(()),
    //                 Err(left) => Node::Leaf(left),
    //             }
    //         }
    //     };

    //     Err(mem::replace(self, left))
    // }


    /// Search the tree to find out what chunk a given byte lives in.
    fn search_bytes(&self, byte: u64) -> usize {
        match *self {
            Node::Internal(ref internal) => {
                let mut loc = 0;
                let mut count = 0;

                for node in internal.children.iter() {
                    if loc + node.size() > byte {
                        return node.search_bytes(byte - loc) + count;
                    }

                    loc += node.size();
                    count += node.count();
                }

                unreachable!(ERR_BYTE_OUTSIDE_OF_TREE);
            }

            Node::Leaf(ref leaf) => {
                let mut loc = 0;

                for (i, record) in leaf.records.iter().enumerate() {
                    if loc + record.size() > byte {
                        return i;
                    }

                    loc += record.size();
                }

                unreachable!(ERR_BYTE_OUTSIDE_OF_TREE);
            }
        }
    }


    /// Search the tree to find the `n`th chunk.
    fn search(&self, idx: usize) -> &SmallRecord<'a> {
        match *self {
            Node::Internal(ref internal) => {
                let mut loc = 0;

                for node in internal.children.iter() {
                    if loc + node.count() > idx {
                        return node.search(idx - loc);
                    }

                    loc += node.count();
                }

                unreachable!(ERR_BYTE_OUTSIDE_OF_TREE);
            }

            Node::Leaf(ref leaf) => &leaf.records[idx],
        }
    }


    fn marshal<T: MarshalTrace>(
        self,
        hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + 'a> {
        match self {
            Node::Internal(internal) => internal.marshal(hasher),
            Node::Leaf(leaf) => leaf.marshal(hasher),
        }
    }
}


/// A B+ tree used to marshal small objects into a B+ tree structure with large objects as nodes
/// and small objects as records.
#[derive(Clone, Debug)]
pub struct Tree<'a> {
    root: Node<'a>,
}


impl<'a> Tree<'a> {
    /// Bulk-load the tree.
    pub fn load<I: IntoIterator<Item = SmallRecord<'a>>>(iterable: I) -> Self {
        Tree { root: Node::load(iterable) }
    }


    /// The number of records in the tree.
    pub fn len(&self) -> usize {
        self.root.count()
    }


    /// The size of the file represented by the tree, in bytes.
    pub fn size(&self) -> u64 {
        self.root.size()
    }


    /// Search the tree for the `nth` chunk.
    pub fn search(&self, idx: usize) -> Option<&SmallRecord<'a>> {
        if idx < self.root.count() {
            Some(self.root.search(idx))
        } else {
            None
        }
    }


    /// Find the chunk that a given byte lives in.
    pub fn search_bytes(&self, byte: u64) -> Option<usize> {
        if byte < self.root.size() {
            Some(self.root.search_bytes(byte))
        } else {
            None
        }
    }


    // pub fn push(&mut self, record: SmallRecord<'a>) {
    //     if let Err(right) = self.root.push(record) {
    //         // Replace `left` with a dummy value.
    //         let left = mem::replace(&mut self.root, Node::internal(Vec::new()));

    //         // Fill in `left` with a fresh two-element root node.
    //         self.root = Node::internal(vec![left, right]);
    //     }
    // }


    pub fn marshal<T: MarshalTrace>(
        self,
        hasher: Hasher<T>,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + 'a> {
        self.root.marshal(hasher)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck::TestResult;

    quickcheck! {
        fn search_vs_bulk_load(chunks: Vec<Vec<u8>>) -> TestResult {
            for chunk in chunks.iter() {
                if chunk.len() == 0 {
                    return TestResult::discard();
                }
            }

            let mut pos = 0;
            let mut index = Vec::new();

            let tree = Tree::load(chunks.iter().map(|chunk| {
                let slice = chunk.as_ref();

                index.push((pos, slice));
                pos += chunk.len() as u64;

                SmallRecord::Deep(Cow::Borrowed(chunk))
            }));

            if tree.len() != chunks.len() || tree.size() != pos {
                return TestResult::failed();
            }

            for (i, (pos, chunk)) in index.into_iter().enumerate() {
                if tree.search_bytes(pos) != Some(i) ||
                    tree.search(i) != Some(&SmallRecord::Deep(Cow::Borrowed(chunk)))
                {
                    panic!("byte search: {}, search: {}, tree: {:?}", tree.search_bytes(pos) != Some(i), tree.search(i) != Some(&SmallRecord::Deep(Cow::Borrowed(chunk))), tree);

                    return TestResult::failed();
                }
            }

            TestResult::passed()
        }
    }


    #[test]
    fn load() {
        let chunk = SmallRecord::Deep(Cow::Borrowed(&[0][..]));

        let tree = Tree::load(vec![chunk; BRANCH_FACTOR + 1]);

        assert_eq!(tree.len(), BRANCH_FACTOR + 1, "{:?}", tree);
    }
}
