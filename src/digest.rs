use std::{hash::Hash, io::{self, Write}};

mod crypto {
    extern crate digest;
    extern crate sha3;

    pub use self::digest::Digest;
    pub use self::sha3::Sha3_256;
}

#[derive(Debug)]
pub struct GenericDigestWriter<D: crypto::Digest>(D);

impl<D: crypto::Digest> Write for GenericDigestWriter<D> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.0.input(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

pub trait DigestWriter: Write {
    type Output: Digest;

    fn finish(self) -> Self::Output;
}

pub trait Digest: Clone + Ord + Hash + Sized + Send + Sync + 'static {
    const NAME: &'static str;
    const SIZE: usize;

    type Writer: DigestWriter<Output = Self>;
    fn writer() -> Self::Writer;

    fn digest(bytes: &[u8]) -> Self {
        let mut writer = Self::writer();
        writer.write_all(bytes).unwrap();
        writer.finish()
    }

    fn from_bytes(&[u8]) -> Self;
    fn as_bytes(&self) -> &[u8];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha3Digest([u8; 32]);

impl Digest for Sha3Digest {
    const NAME: &'static str = "SHA-3-256";
    const SIZE: usize = 32;

    type Writer = GenericDigestWriter<self::crypto::Sha3_256>;
    fn writer() -> Self::Writer {
        GenericDigestWriter(self::crypto::Sha3_256::default())
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut new = Sha3Digest([0; 32]);
        new.0.copy_from_slice(bytes);
        new
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl DigestWriter for GenericDigestWriter<crypto::Sha3_256> {
    type Output = Sha3Digest;

    fn finish(self) -> Self::Output {
        use self::crypto::Digest;

        let mut dg = Sha3Digest([0u8; 32]);
        dg.0.copy_from_slice(self.0.result().as_slice());
        dg
    }
}
