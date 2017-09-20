//! `split` - split extremely large files into consistently-sized deterministic chunks.
//!
//! Key functionality of this module includes:
//!
//! * Splitting large files into chunks, via memory-mapped files.
//! * Splitting streams of bytes into chunks, for use with lazily-downloaded files.


use std::hash::{Hash, Hasher};
use std::mem;

use seahash::SeaHasher;

use trace::SplitTrace;


/// A "splitter" over a slice, which functions as an iterator producing ~10 kb [citation needed]
/// chunks.
pub struct SliceSplitter<'a> {
    slice: &'a [u8],
}


impl<'a> SliceSplitter<'a> {
    pub fn new(slice: &'a [u8]) -> SliceSplitter<'a> {
        SliceSplitter { slice }
    }
}


/// The minimum chunk size. Chosen to reduce the number of extremely small (1-4 byte) chunks by
/// reducing the likelihood of finding chunks which instantly satisfy the chunking criteria (within
/// only a few bytes - e.g. a `0x01` byte followed by a whole bunch of zeroes, or a whole bunch of
/// zeroes with a `0x01` byte embedded inside.)
const SPLIT_MINIMUM: usize = 32;

// 1 << 12 = 4096
// 1 << 13 = 8192
// 1 << 14 = 16384
/// The bitmask of the power-of-two modulus we use to cap the rolling sum.
const SPLIT_MODULUS_MASK: u16 = (1 << 14) - 1;

/// We sum all the bytes in a window, mask them off, and then check whether or not the result is
/// equal to the `SPLIT` constant. If it is, this is a chunk split.
const SPLIT_CONSTANT: u16 = 1;

// 1 << 13 = 8192
/// The size of the window of bytes to "hash".
const SPLIT_WINDOW: usize = 1 << 13;


impl<'a> Iterator for SliceSplitter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.slice.len() == 0 {
            return None;
        }

        let mut acc = 0;

        for i in 0..self.slice.len() {
            if i >= SPLIT_WINDOW {
                acc -= self.slice[i - SPLIT_WINDOW] as u16;
            }

            acc += self.slice[i] as u16;
            acc &= SPLIT_MODULUS_MASK;

            if acc == SPLIT_CONSTANT && i >= SPLIT_MINIMUM {
                let (split, rest) = self.slice.split_at(i);

                self.slice = rest;

                return Some(split);
            }
        }

        return Some(mem::replace(&mut self.slice, &[]));
    }
}


/// A ring buffer used to implement the rolling rsync-style checksum.
pub struct Ring {
    buf: [u8; CHUNK_WINDOW],
    loc: usize,
}


impl Ring {
    fn new() -> Ring {
        Ring {
            buf: [0u8; CHUNK_WINDOW],
            loc: 0,
        }
    }


    /// Push a new byte into the ring buffer, ignoring the last byte.
    fn push(&mut self, byte: u8) {
        self.push_pop(byte);
    }


    /// Remove the last byte from the ring buffer, replacing it.
    fn push_pop(&mut self, byte: u8) -> u8 {
        let popped = mem::replace(&mut self.buf[self.loc], byte);
        self.loc = (self.loc + 1) % CHUNK_WINDOW;

        popped
    }
}


/// "Chunk" a slice, producing ~3 MB chunks.
pub struct SliceChunker<'a> {
    rest: &'a [u8],
    splitter: SliceSplitter<'a>,
}


impl<'a> SliceChunker<'a> {
    pub fn new(slice: &'a [u8]) -> SliceChunker<'a> {
        SliceChunker {
            rest: slice,
            splitter: SliceSplitter::new(slice),
        }
    }
}


const CHUNK_MINIMUM: usize = 32;
const CHUNK_MODULUS_MASK: u16 = (1 << 7) - 1;
const CHUNK_CONSTANT: u16 = 1;
const CHUNK_WINDOW: usize = 256;


fn seahash(slice: &[u8]) -> u8 {
    let mut hasher = SeaHasher::default();
    slice.hash(&mut hasher);
    hasher.finish() as u8
}


impl<'a> Iterator for SliceChunker<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.rest.len() == 0 {
            return None;
        }

        let mut ring = Ring::new();

        let mut offset = 0;
        let mut acc = 0;

        for (i, slice) in self.splitter.by_ref().enumerate() {
            let sig = seahash(slice);

            if i >= CHUNK_WINDOW {
                acc -= ring.push_pop(sig) as u16;
            } else {
                ring.push(sig);
            }

            acc += sig as u16;
            acc &= CHUNK_MODULUS_MASK;

            offset += slice.len();

            if acc == CHUNK_CONSTANT && i >= CHUNK_MINIMUM {
                let (split, rest) = self.rest.split_at(offset);

                self.rest = rest;

                return Some(split);
            }
        }

        return Some(mem::replace(&mut self.rest, &[]));
    }
}


pub struct Chunked<'batch>(Vec<&'batch [u8]>);


impl<'batch> Chunked<'batch> {
    pub fn to_vec(self) -> Vec<&'batch [u8]> {
        self.0
    }
}


pub fn chunk<'batch>(bytes: &'batch [u8]) -> Chunked<'batch> {
    chunk_with_trace(bytes, &mut ())
}


pub fn chunk_with_trace<'batch, T: SplitTrace>(
    bytes: &'batch [u8],
    trace: &mut T,
) -> Chunked<'batch> {
    let mut offset = 0u64;

    let slices = SliceChunker::new(bytes)
        .inspect(|chunk| {
            trace.on_chunk(offset, chunk);

            offset += chunk.len() as u64;
        })
        .collect();

    Chunked(slices)
}
