use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, BufWriter, Write};
use std::mem;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::Arc;

use bincode;
use digest_writer::{FixedOutput, Writer};
use futures::future::{self, Either, FutureResult};
use futures::prelude::*;
use futures::stream;
use futures::sync::mpsc::Sender;
use generic_array::GenericArray;
use sha3::{Sha3_256, Digest};
use typenum::consts;

use errors::*;
use marshal::{LargeObject, SubtreeObject, Record, SmallRecord, DirTree};
use marshal::dir_tree::Node;
use split::GenericSplitter;
use trace::Trace;


/// The SHA3-256 hash of an object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectHash(GenericArray<u8, consts::U32>);


impl ObjectHash {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }


    #[inline]
    pub fn to_path(&self) -> PathBuf {
        use std::fmt::Write;

        let mut buf = String::with_capacity(32);

        write!(buf, "{:02x}/{:02x}/", self.0[0], self.0[1]).unwrap();

        for b in &self.0[2..] {
            write!(buf, "{:02x}", b).unwrap();
        }

        buf.into()
    }
}


impl Borrow<[u8]> for ObjectHash {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}


impl Deref for ObjectHash {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl fmt::Debug for ObjectHash {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Display>::fmt(self, f)
    }
}


impl fmt::Display for ObjectHash {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for &b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }

        Ok(())
    }
}


impl FromStr for ObjectHash {
    type Err = Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        if s.len() != 64 {
            bail!(
                Error::from_kind(ErrorKind::InvalidHashLength(s.len()))
                    .chain_err(|| ErrorKind::InvalidHashString(s.to_owned()))
            );
        }

        let mut generic_array = GenericArray::map_slice(&[0; 32], |&x| x);
        for (i, byte) in generic_array.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..(i + 1) * 2], 16).chain_err(
                || {
                    ErrorKind::InvalidHashString(s.to_owned())
                },
            )?;
        }

        Ok(ObjectHash(generic_array))
    }
}


struct Fork<L: Write, R: Write> {
    left: L,
    right: R,
}


impl<L: Write, R: Write> Fork<L, R> {
    #[inline]
    fn new(left: L, right: R) -> Fork<L, R> {
        Fork { left, right }
    }
}


impl<L: Write, R: Write> Write for Fork<L, R> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.left.write_all(buf)?;
        self.right.write_all(buf)?;

        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.left.flush()?;
        self.right.flush()?;

        Ok(())
    }
}


#[derive(Debug)]
pub struct Hashed {
    hash: ObjectHash,
    bytes: Option<Vec<u8>>,
}


impl Hashed {
    pub fn as_hash(&self) -> &ObjectHash {
        &self.hash
    }


    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.bytes.as_ref().map(AsRef::as_ref)
    }


    pub fn into_components(self) -> (ObjectHash, Option<Vec<u8>>) {
        (self.hash, self.bytes)
    }
}


#[derive(Debug, Clone)]
pub struct Marshaller<T: Trace> {
    output: Sender<Hashed>,
    trace: T,
}


type LeafSplitter<A, B, C, D, E> = GenericSplitter<
    consts::U4,
    consts::U1,
    consts::U9,
    consts::U1,
    A,
    B,
    C,
    D,
    E,
>;


impl<T: Trace> Marshaller<T> {
    pub fn with_trace(output: Sender<Hashed>, trace: T) -> Self {
        Self {
            output,
            trace,
        }
    }

    pub fn process<R: Into<Record>>(
        &self,
        object: R,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let trace = self.trace.clone();
        let output = self.output.clone();
        let record = object.into();

        let async = {
            async_block! {
                let (hash, bytes) = match record.to_deep() {
                    Ok(object) => {
                        let raw_object = object.as_raw();
                        let size = bincode::serialized_size(&raw_object);

                        let mut buf = Vec::with_capacity(size as usize);
                        let mut digest_writer = Writer::new(Sha3_256::new());

                        bincode::serialize_into(
                            &mut Fork::new(&mut buf, BufWriter::new(&mut digest_writer)),
                            &raw_object,
                            bincode::Infinite,
                        ).expect("No actual I/O, impossible to have an I/O error.");

                        let digest = digest_writer.fixed_result();

                        (ObjectHash(digest), Some(buf))
                    }

                    Err(hash) => (hash, None),
                };

                trace.on_marshal_process(&hash);

                await!(output.send(Hashed { hash, bytes })).expect("Channel closed!");

                Ok(hash)
            }
        };


        Box::new(async)
    }

    pub fn process_chunks<S, C>(
        &self,
        stream: S,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send>
    where
        S: Stream<Item = C, Error = Error> + Send + 'static,
        C: Into<SmallRecord> + Send + 'static,
    {

        let marshaller = self.clone();
        let result = {
            async_block! {
                let record_marshaller = marshaller.clone();
                let records = stream.and_then(move |chunk| {
                    let small_record = chunk.into();
                    let size = small_record.size();
                    record_marshaller.process(small_record).map(
                        move |hash| (size, hash),
                    )
                });

                let mut leaves = await!(records.collect())?;

                while leaves.len() > 1024 {
                    let old_leaves = mem::replace(&mut leaves, Vec::new());
                    let splitter =
                        LeafSplitter::new(old_leaves.into_iter(), |(sz, hash)| (hash, (sz, hash)));

                    for (_, children) in splitter {
                        let size = children.iter().map(|&(sz, _)| sz).sum();
                        let object = LargeObject { size, children };
                        let object_hash = await!(marshaller.process(object))?;

                        leaves.push((size, object_hash));
                    }
                }

                let object_hash = if leaves.len() == 1 {
                    leaves.pop().unwrap().1
                } else {
                    let size = leaves.iter().map(|&(sz, _)| sz).sum();
                    let object = LargeObject {
                        size,
                        children: leaves,
                    };
                    await!(marshaller.process(object))?
                };

                Ok(object_hash)
            }
        };

        Box::new(result)
    }

    fn process_dir_tree_inner(
        &self,
        prefix: &Path,
        node: Node,
    ) -> Box<Future<Item = Option<ObjectHash>, Error = Error> + Send> {
        let async = match node {
            Node::Branch(entries) => {
                let marshaller = self.clone();
                let hash_futures = entries.into_iter().map(move |(component, entry)| {
                    marshaller
                        .process_dir_tree_inner(&prefix.join(&component), entry)
                        .map(|hash_opt| hash_opt.map(|hash| (component, hash)))
                });
                let hash_stream =
                    stream::futures_unordered(hash_futures).filter_map(|hash_opt| hash_opt);

                let marshaller = self.clone();
                let hash_future = hash_stream
                    .fold::<_, _, FutureResult<_, Error>>(
                        BTreeMap::new(),
                        |mut acc, (component, entry)| {
                            acc.insert(component, entry);
                            future::ok(acc)
                        },
                    )
                    .and_then(move |entries| marshaller.process(SubtreeObject { entries }))
                    .map(Some);

                Either::A(hash_future)
            }
            Node::Leaf(object_hash) => Either::B(future::ok(Some(object_hash))), 
            Node::Empty => Either::B(future::ok(None)),
        };

        Box::new(async)
    }

    pub fn process_dir_tree(
        &self,
        dir_tree: DirTree,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let marshaller = self.clone();
        let async = marshaller
            .process_dir_tree_inner(Path::new(""), dir_tree.root)
            .and_then(move |out| match out {
                Some(object_hash) => Either::A(future::ok(object_hash)),
                None => Either::B(marshaller.process(
                    SubtreeObject { entries: BTreeMap::new() },
                )),
            });

        Box::new(async)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use futures::stream;
    use futures::sync::mpsc;
    use futures_cpupool::CpuPool;
    use rand::{XorShiftRng, Rng, SeedableRng};
    use quickcheck::TestResult;

    use arc_slice;
    use marshal::SmallObject;

    quickcheck! {
        #[test]
        fn large_simple(chunks: Vec<Vec<u8>>) -> TestResult {
            let pool = CpuPool::new_num_cpus();
            let chunks = chunks.into_iter().map(arc_slice::owned).collect::<Vec<_>>();

            for chunk in chunks.iter() {
                if chunk.len() == 0 {
                    return TestResult::discard();
                }
            }

            let n = chunks.len();

            let (tx, rx) = mpsc::channel(64);
            let hasher = Marshaller::with_trace(tx, ());
            let marshal_future = pool.spawn(hasher.process_chunks(stream::iter_ok(chunks)));
            let joined = pool.spawn(rx.collect())
                .map_err(|_| Error::from_kind(ErrorKind::Absurd))
                .join(marshal_future);

            let (hashes, _marshal_success) = joined.wait().unwrap();

            if hashes.len() < n || hashes.len() > 2 * n {
                TestResult::failed()
            } else {
                TestResult::passed()
            }
        }
    }

    #[test]
    fn large_singleton() {
        const CHUNK_QUANTITY: usize = 1;
        const CHUNK_SIZE: usize = 64;

        let pool = CpuPool::new_num_cpus();
        let chunks = (0..CHUNK_QUANTITY)
            .map(|i| {
                arc_slice::owned((0..CHUNK_SIZE).map(|j| (i + j) as u8).collect())
            })
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel(64);
        let hasher = Marshaller::with_trace(tx, ());
        let marshal_future = pool.spawn(hasher.process_chunks(stream::iter_ok(chunks)));
        mem::drop(hasher);
        let joined = pool.spawn(rx.collect())
            .map_err(|_| Error::from_kind(ErrorKind::Absurd))
            .join(marshal_future);

        let (hashes, _marshal_success) = joined.wait().unwrap();

        assert_eq!(hashes.len(), 1);
    }

    #[test]
    fn large_hierarchical() {
        const CHUNK_QUANTITY: usize = 8192;
        const CHUNK_SIZE: usize = 64;

        let pool = CpuPool::new_num_cpus();
        let chunks = (0..CHUNK_QUANTITY)
            .map(|i| {
                arc_slice::owned(
                    XorShiftRng::from_seed([i as u32, 2, 3, 7])
                        .gen_iter()
                        .take(CHUNK_SIZE)
                        .collect(),
                )
            })
            .inspect(|chunk| assert!(chunk.len() > 0))
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel(64);
        let hasher = Marshaller::with_trace(tx, ());
        let marshal_future = pool.spawn(hasher.process_chunks(stream::iter_ok(chunks)));
        mem::drop(hasher);
        let joined = pool.spawn(rx.collect())
            .map_err(|_| Error::from_kind(ErrorKind::Absurd))
            .join(marshal_future);

        let (hashes, _marshal_success) = joined.wait().unwrap();

        assert_eq!(hashes.len(), 8220);
    }
}
