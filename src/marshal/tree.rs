use std::borrow::Cow;
use std::mem;

use marshal::{Marshal, Marshaller, ObjectHash, SmallObject, LargeObject};
use trace::MarshalTrace;


const BRANCH_FACTOR: usize = 1024;

const ERR_BRANCH_FACTOR_TOO_SMALL: &'static str = "BRANCH_FACTOR too small (less than 2?) freshly split node should always have space for one more child!";
const ERR_BYTE_OUTSIDE_OF_TREE: &'static str = "Node::search_bytes should only be called when we know the byte is in the tree!";
const ERR_INTERNAL_NONEMPTY: &'static str = "Internal nodes should never be empty!";


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Record<'a> {
    Deep(Cow<'a, [u8]>),
    Shallow(u64, ObjectHash),
}


impl<'a> Record<'a> {
    fn size(&self) -> u64 {
        match *self {
            Record::Deep(ref bytes) => bytes.len() as u64,
            Record::Shallow(sz, _) => sz,
        }
    }
}


#[derive(Clone, Debug)]
struct Leaf<'a> {
    size: u64,
    records: Vec<Record<'a>>,
}


impl<'fresh> Marshal<'fresh> for Leaf<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        let size = self.size();
        let children = self.records
            .into_iter()
            .map(|record| match record {
                Record::Deep(bytes) => (bytes.len() as u64, ctx.put(SmallObject { chunk: bytes })),
                Record::Shallow(sz, hash) => (sz, hash),
            })
            .collect();

        ctx.put(LargeObject { size, children })
    }
}


impl<'a> From<Vec<Record<'a>>> for Leaf<'a> {
    fn from(records: Vec<Record<'a>>) -> Self {
        let size = records.iter().map(Record::size).sum();

        Leaf { size, records }
    }
}


impl<'a> Leaf<'a> {
    fn new() -> Self {
        Vec::new().into()
    }


    fn size(&self) -> u64 {
        self.size
    }


    fn count(&self) -> usize {
        self.records.len()
    }


    fn append(&mut self, record: Record<'a>) -> Result<(), Leaf<'a>> {
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
}


#[derive(Clone, Debug)]
struct Internal<'a> {
    size: u64,
    count: usize,
    children: Vec<Node<'a>>,
}


impl<'fresh> Marshal<'fresh> for Internal<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        let children = self.children
            .into_iter()
            .map(|child| (child.size(), ctx.put(child)))
            .collect();

        ctx.put(LargeObject {
            size: self.size,
            children,
        })
    }
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
    fn singleton(elem: Node<'a>) -> Self {
        Internal {
            size: elem.size(),
            count: elem.count(),
            children: vec![elem],
        }
    }


    fn size(&self) -> u64 {
        self.size
    }


    fn count(&self) -> usize {
        self.count
    }


    fn append(&mut self, node: Node<'a>) -> Result<(), Self> {
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
}


#[derive(Clone, Debug)]
enum Node<'a> {
    Internal(Internal<'a>),
    Leaf(Leaf<'a>),
}


impl<'fresh> Marshal<'fresh> for Node<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        match self {
            Node::Internal(internal) => ctx.put(internal),
            Node::Leaf(leaf) => ctx.put(leaf),
        }
    }
}


impl<'a> Node<'a> {
    fn internal(children: Vec<Node<'a>>) -> Self {
        Node::Internal(Internal::from(children))
    }


    fn size(&self) -> u64 {
        match *self {
            Node::Internal(ref internal) => internal.size(),
            Node::Leaf(ref leaf) => leaf.size(),
        }
    }


    fn count(&self) -> usize {
        match *self {
            Node::Internal(ref internal) => internal.count(),
            Node::Leaf(ref leaf) => leaf.count(),
        }
    }


    fn load<I>(iterable: I) -> Self
    where
        I: IntoIterator<Item = Record<'a>>,
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

        Node::internal(stack)
    }


    fn push(&mut self, record: Record<'a>) -> Result<(), Self> {
        // Attempt to push this node into our local children (whether we are a leaf or an internal
        // node.) If `left` is not empty, we swap ourselves with it and return it as `Err`; it is
        // the left side of our new split.
        let left = match *self {
            Node::Internal(ref mut internal) => {
                match internal
                    .children
                    .last_mut()
                    .expect(ERR_INTERNAL_NONEMPTY)
                    .push(record) {
                    Ok(()) => return Ok(()),
                    Err(left) => left,
                }
            }

            Node::Leaf(ref mut leaf) => {
                match leaf.append(record) {
                    Ok(()) => return Ok(()),
                    Err(left) => Node::Leaf(left),
                }
            }
        };

        Err(mem::replace(self, left))
    }


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
    fn search(&self, idx: usize) -> &Record<'a> {
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
}


#[derive(Clone, Debug)]
pub struct Tree<'a> {
    root: Node<'a>,
}


impl<'fresh> Marshal<'fresh> for Tree<'fresh> {
    fn marshal<T: MarshalTrace>(self, ctx: &mut Marshaller<'fresh, T>) -> ObjectHash {
        ctx.reserve(self.len());
        ctx.put(self.root)
    }
}


impl<'a> Tree<'a> {
    pub fn load<I: IntoIterator<Item = Record<'a>>>(iterable: I) -> Self {
        Tree { root: Node::load(iterable) }
    }


    pub fn len(&self) -> usize {
        self.root.count()
    }


    pub fn size(&self) -> u64 {
        self.root.size()
    }


    pub fn search(&self, idx: usize) -> Option<&Record<'a>> {
        if idx < self.root.count() {
            Some(self.root.search(idx))
        } else {
            None
        }
    }


    pub fn search_bytes(&self, byte: u64) -> Option<usize> {
        if byte < self.root.size() {
            Some(self.root.search_bytes(byte))
        } else {
            None
        }
    }


    pub fn push(&mut self, record: Record<'a>) {
        if let Err(right) = self.root.push(record) {
            // Replace `left` with a dummy value.
            let left = mem::replace(&mut self.root, Node::internal(Vec::new()));

            // Fill in `left` with a fresh two-element root node.
            self.root = Node::internal(vec![left, right]);
        }
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

                Record::Deep(Cow::Borrowed(chunk))
            }));

            if tree.len() != chunks.len() || tree.size() != pos {
                return TestResult::failed();
            }

            for (i, (pos, chunk)) in index.into_iter().enumerate() {
                if tree.search_bytes(pos) != Some(i) ||
                    tree.search(i) != Some(&Record::Deep(Cow::Borrowed(chunk)))
                {
                    panic!("byte search: {}, search: {}, tree: {:?}", tree.search_bytes(pos) != Some(i), tree.search(i) != Some(&Record::Deep(Cow::Borrowed(chunk))), tree);

                    return TestResult::failed();
                }
            }

            TestResult::passed()
        }
    }


    #[test]
    fn load() {
        let chunk = Record::Deep(Cow::Borrowed(&[0][..]));

        let tree = Tree::load(vec![chunk; BRANCH_FACTOR + 1]);

        assert_eq!(tree.len(), BRANCH_FACTOR + 1, "{:?}", tree);
    }
}
