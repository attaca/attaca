use std::io::{BufRead, BufReader, Read, Write};

use failure::Error;
use leb128;
use memchr;

use digest::Digest;

pub fn encode<W: Write, D: Digest>(w: &mut W, blob: &[u8], refs: &[D]) -> Result<(), Error> {
    let hash_name_bytes = D::NAME.as_bytes();
    ensure!(
        memchr::memchr(b'\0', hash_name_bytes).is_none(),
        "Hash name contains a nul byte!"
    );

    w.write_all(&hash_name_bytes)?;
    w.write(&[b'\0'])?;
    leb128::write::unsigned(w, D::SIZE as u64)?;
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
        buf_reader.read_until(b'0', &mut hash_name_buf)?;
        String::from_utf8(hash_name_buf)?
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
        ensure!(
            self.hash_name == D::NAME && self.hash_size == D::SIZE,
            "Hash name/size mismatch! Decoded {}/{} vs. expected {}/{}",
            self.hash_name,
            self.hash_size,
            D::NAME,
            D::SIZE
        );

        let (blob_digest_bytes, ref_bytes) = self.digests.split_at(D::SIZE);
        let blob_digest = D::from_bytes(blob_digest_bytes);
        assert!(ref_bytes.len() % D::SIZE == 0 && ref_bytes.len() / D::SIZE == self.ref_count);
        let refs = ref_bytes.chunks(D::SIZE).map(D::from_bytes).collect();

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
