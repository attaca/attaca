use std::io::{BufRead, BufReader, Read, Write};

use failure::*;
use leb128;
use memchr;

use digest::prelude::*;

const NUL: u8 = 0;

pub fn encode<W: Write, D: Digest>(w: &mut W, blob: &[u8], refs: &[D]) -> Result<(), Error> {
    let hash_name_bytes = D::SIGNATURE.name.as_bytes();
    ensure!(
        memchr::memchr(NUL, hash_name_bytes).is_none(),
        "Hash name contains a nul byte: {}"
    );

    w.write_all(&hash_name_bytes)?;
    w.write(&[NUL])?;
    leb128::write::unsigned(w, D::SIGNATURE.size as u64)?;
    leb128::write::unsigned(w, blob.len() as u64)?;
    leb128::write::unsigned(w, refs.len() as u64)?;
    leb128::write::unsigned(w, 0)?;
    w.write_all(D::digest(blob).as_bytes())?;

    for digest in refs {
        w.write_all(digest.as_bytes())?;
    }

    Ok(())
}

pub fn decode<R: Read>(r: &mut R) -> Result<PartialItem, Error> {
    let mut buf_reader = BufReader::new(r);

    let hash_name = {
        let mut hash_name_buf = Vec::new();
        buf_reader.read_until(NUL, &mut hash_name_buf)?;
        hash_name_buf.pop(); // Pop the NUL off.
        String::from_utf8(hash_name_buf).with_context(|e| {
            format_err!(
                "Hash name is invalid UTF-8: {}",
                String::from_utf8_lossy(e.as_bytes())
            )
        })?
    };

    let hash_size = leb128::read::unsigned(&mut buf_reader)? as usize;
    let blob_len = leb128::read::unsigned(&mut buf_reader)? as usize;
    let ref_count = leb128::read::unsigned(&mut buf_reader)? as usize;
    let _num_args = leb128::read::unsigned(&mut buf_reader)? as usize;

    let mut digests = Vec::new();
    buf_reader.read_to_end(&mut digests)?;

    Ok(PartialItem {
        hash_name,
        hash_size,
        blob_len,
        ref_count,
        digests,
    })
}

pub struct PartialItem {
    hash_name: String,
    hash_size: usize,
    blob_len: usize,
    ref_count: usize,
    digests: Vec<u8>,
}

impl PartialItem {
    pub fn hash_name(&self) -> &str {
        &self.hash_name
    }

    pub fn hash_size(&self) -> usize {
        self.hash_size
    }

    pub fn finish<D: Digest>(&self) -> Result<Item<D>, Error> {
        let DigestSignature { name, size } = D::SIGNATURE;

        ensure!(
            self.hash_name == name && self.hash_size == size,
            "Hash name/size mismatch! Decoded {}/{} vs. expected {}/{}",
            self.hash_name,
            self.hash_size,
            name,
            size,
        );

        let (blob_digest_bytes, ref_bytes) = self.digests.split_at(size);
        let blob_digest = D::from_bytes(blob_digest_bytes);
        assert!(ref_bytes.len() % size == 0 && ref_bytes.len() / size == self.ref_count);
        let refs = ref_bytes.chunks(size).map(D::from_bytes).collect();

        Ok(Item {
            blob_len: self.blob_len,
            blob_digest,
            refs,
        })
    }
}

pub struct Item<D: Digest> {
    pub blob_len: usize,
    pub blob_digest: D,
    pub refs: Vec<D>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    use digest::Sha3Digest;

    prop_compose! {
        fn arb_sha3()
                (bytes in prop::collection::vec(any::<u8>(), 32)) -> Sha3Digest {
            Sha3Digest::from_bytes(&bytes)
        }
    }

    proptest! {
        #[test]
        fn roundtrip_canonical_sha3(ref bytes in prop::collection::vec(any::<u8>(), 0..1024),
                                    ref digests in prop::collection::vec(arb_sha3(), 0..1024)) {
            let mut buf = Vec::new();
            encode::<_, Sha3Digest>(&mut buf, bytes, digests).unwrap();
            let partial = decode(&mut &buf[..]).unwrap();
            assert_eq!(partial.hash_name(), Sha3Digest::SIGNATURE.name);
            assert_eq!(partial.hash_size(), Sha3Digest::SIGNATURE.size);
            let item = partial.finish::<Sha3Digest>().unwrap();
            assert_eq!(item.blob_len, bytes.len());
            assert_eq!(&item.refs, digests);
        }
    }
}
