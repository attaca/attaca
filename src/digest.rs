use std::{hash::Hash, io::{self, Write}};

pub mod prelude {
    pub use super::{Digest, DigestSignature, DigestWriter, Id};
}

mod crypto {
    extern crate digest;
    extern crate sha3;

    pub use self::digest::Digest;
    pub use self::sha3::Sha3_256;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DigestSignature {
    pub name: &'static str,
    pub size: usize,
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

/// Trait for types which may be used to identify objects in a store.
///
/// This is not necessarily a digest. It could even be a randomly generated UUID. It is a given
/// store's responsibility that any two local IDs do not collide.
pub trait Id: AsRef<[u8]> + ToOwned + Ord + Hash + Send + Sync + 'static {
    fn from_bytes(bytes: &[u8]) -> Self::Owned;

    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl Id for [u8] {
    fn from_bytes(bytes: &[u8]) -> Self::Owned {
        bytes.to_owned()
    }
}

/// A digest is an identifier which is defined as the cryptographic digest of the canonical form of
/// a given object. Unlike a local identifier, a digest may be used to identify objects across two
/// different stores and regardless of backends (assuming the backends in question support the
/// digest in question.)
pub trait Digest: Clone + Id
where
    Self: ToOwned<Owned = Self>,
{
    const SIGNATURE: DigestSignature;

    type Writer: DigestWriter<Output = Self>;
    fn writer() -> Self::Writer;

    fn digest(bytes: &[u8]) -> Self {
        let mut writer = Self::writer();
        writer.write_all(bytes).unwrap();
        writer.finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha3Digest([u8; 32]);

impl AsRef<[u8]> for Sha3Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Id for Sha3Digest {
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut new = Sha3Digest([0; 32]);
        new.0.copy_from_slice(bytes);
        new
    }
}

impl Digest for Sha3Digest {
    const SIGNATURE: DigestSignature = DigestSignature {
        name: "SHA-3-256",
        size: 32,
    };

    type Writer = GenericDigestWriter<self::crypto::Sha3_256>;
    fn writer() -> Self::Writer {
        GenericDigestWriter(self::crypto::Sha3_256::default())
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
