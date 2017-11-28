use std::borrow::Borrow;
use std::fmt;
use std::io::{self, BufWriter, Write};
use std::mem;
use std::ops::Deref;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::str::FromStr;

use bincode;
use digest::{FixedOutput, Input};
use failure::{Error, ResultExt};
use futures::prelude::*;
use futures::sync::mpsc::Sender;
use generic_array::GenericArray;
use sha3::{Digest, Sha3_256};
use typenum::consts;

use arc_slice::{self, ArcSlice};
use object::{LargeObject, Object, RawObject};
use object_hash::ObjectHash;
use record::{Record, SmallRecord};
use tree::Tree;
use split::GenericSplitter;


struct HashSink {
    digest: Sha3_256,
}


impl HashSink {
    #[inline]
    fn new() -> Self {
        Self {
            digest: Sha3_256::new(),
        }
    }

    #[inline]
    fn into_output(self) -> GenericArray<u8, consts::U32> {
        self.digest.fixed_result()
    }
}


impl Write for HashSink {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.digest.process(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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


pub fn serialize_into_and_hash<W: Write>(
    raw_object: &RawObject,
    writer: &mut W,
) -> Result<ObjectHash, Error> {
    let mut digest_writer = HashSink::new();

    bincode::serialize_into(
        &mut Fork::new(writer, BufWriter::new(&mut digest_writer)),
        &raw_object,
        bincode::Infinite,
    )?;

    Ok(ObjectHash(digest_writer.into_output()))
}


pub fn hash(object: &Object) -> ObjectHash {
    serialize_into_and_hash(&object.as_raw(), &mut io::sink())
        .expect("Sink should never error, Digest should never error!")
}


pub fn serialize_and_hash(object: &Object) -> Hashed {
    let raw_object = object.as_raw();
    let size = bincode::serialized_size(&raw_object);
    let mut bytes = Vec::with_capacity(size as usize);
    let hash = serialize_into_and_hash(&raw_object, &mut bytes)
        .expect("Vec should never error, Digest should never error!");

    Hashed {
        hash,
        bytes: arc_slice::owned(bytes),
    }
}


#[derive(Debug)]
pub struct Hashed {
    hash: ObjectHash,
    bytes: ArcSlice,
}


impl Hashed {
    pub fn as_hash(&self) -> &ObjectHash {
        &self.hash
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_components(self) -> (ObjectHash, ArcSlice) {
        (self.hash, self.bytes)
    }

    pub fn to_object(&self) -> Result<Object, Error> {
        Object::from_bytes(&self.bytes)
    }
}


#[derive(Debug, Clone)]
pub struct Hasher {
    output: Sender<Hashed>,
}


impl Hasher {
    pub fn new(output: Sender<Hashed>) -> Self {
        Self { output }
    }

    pub fn process<R: Into<Record>>(
        &self,
        object: R,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let output = self.output.clone();
        let record = object.into();

        let async = {
            async_block! {
                let hashed = match record.to_deep() {
                    Ok(object) => serialize_and_hash(&object),
                    Err(hash) => return Ok(hash),
                };
                let hash = *hashed.as_hash();
                await!(output.send(hashed)).expect("Channel closed!");
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
                    let old_leaves = mem::replace(&mut leaves, Vec::new()).into_iter();
                    let hash_thunk = |(sz, hash)| (hash, (sz, hash));
                    let splitter =
                        GenericSplitter::new(4, 1, 9, 1, old_leaves, hash_thunk);

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

    pub fn process_tree<U: Into<Tree>>(
        &self,
        tree: U,
    ) -> impl Future<Item = ObjectHash, Error = Error> + Send {
        tree.into().marshal(self.clone())
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use futures::stream;
    use futures::sync::mpsc;
    use futures_cpupool::CpuPool;
    use rand::{Rng, SeedableRng, XorShiftRng};
    use quickcheck::TestResult;

    use arc_slice;

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
            let hasher = Hasher::new(tx);
            let marshal_future = pool.spawn(hasher.process_chunks(stream::iter_ok(chunks)));
            mem::drop(hasher);
            let joined = pool.spawn(rx.collect())
                .map_err(|_| Error::from_kind(ErrorKind::Absurd))
                .join(marshal_future);

            let (hashes, _marshal_success) = joined.wait().unwrap();

            if hashes.len() < n || hashes.len() > 2 * n + 1 {
                TestResult::error(
                    format!(
                        "{} hashed objects produced from {} inputs",
                        hashes.len(),
                        n,
                    )
                )
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
        let hasher = Hasher::new(tx);
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
        let hasher = Hasher::new(tx);
        let marshal_future = pool.spawn(hasher.process_chunks(stream::iter_ok(chunks)));
        mem::drop(hasher);
        let joined = pool.spawn(rx.collect())
            .map_err(|_| Error::from_kind(ErrorKind::Absurd))
            .join(marshal_future);

        let (hashes, _marshal_success) = joined.wait().unwrap();

        assert_eq!(hashes.len(), 8220);
    }
}
