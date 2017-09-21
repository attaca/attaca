use std::fmt;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bincode;
use digest_writer::{FixedOutput, Writer};
use futures::prelude::*;
use futures::sync::mpsc::Sender;
use generic_array::GenericArray;
use sha3::{Sha3_256, Digest};
use typenum::U32;

use errors::*;
use marshal::Record;
use trace::MarshalTrace;


/// The SHA3-256 hash of an object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectHash(GenericArray<u8, U32>);


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


#[derive(Debug)]
pub struct Hasher<T: MarshalTrace> {
    output: Sender<Hashed>,
    trace: Arc<Mutex<T>>,
}


impl<T: MarshalTrace> Clone for Hasher<T> {
    fn clone(&self) -> Self {
        Hasher {
            output: self.output.clone(),
            trace: self.trace.clone(),
        }
    }
}


impl<T: MarshalTrace> Hasher<T> {
    pub fn with_trace(output: Sender<Hashed>, trace: T) -> Self {
        Self {
            output,
            trace: Arc::new(Mutex::new(trace)),
        }
    }


    pub fn compute<R: Into<Record>>(
        &mut self,
        object: R,
    ) -> Box<Future<Item = ObjectHash, Error = Error> + Send> {
        let trace = self.trace.clone();

        let (hash, bytes) = match object.into().to_deep() {
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

        trace.lock().unwrap().on_hashed(&hash);

        let result = self.output.clone().send(Hashed { hash, bytes }).then(
            move |result| match result {
                Ok(_) => Ok(hash),
                Err(_) => Err("Channel closed!".into()),
            },
        );

        Box::new(result)
    }
}
