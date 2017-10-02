//! # `split` - split extremely large files into consistently-sized deterministic chunks.
//!
//! Key functionality of this module includes:
//!
//! * Splitting large files into chunks, via memory-mapped files.
//! * Splitting slices of bytes into chunks, for use with lazily-downloaded files.
//!
//! The main functions of this module are `arc_slice::chunk` and `arc_slice::chunk_with_trace`.


use std::cmp;
use std::hash::{Hash, Hasher};
use std::mem;

use seahash::SeaHasher;

use arc_slice::{self, ArcSlice};
use trace::SplitTrace;


/// A "splitter" over a slice, which functions as an iterator producing ~10 kb [citation needed]
/// chunks.
pub struct SliceSplitter {
    slice: ArcSlice,
}


impl SliceSplitter {
    pub fn new(slice: ArcSlice) -> SliceSplitter {
        SliceSplitter { slice }
    }
}


/// The minimum chunk size. Chosen to reduce the number of extremely small (1-4 byte) chunks by
/// reducing the likelihood of finding chunks which instantly satisfy the chunking criteria (within
/// only a few bytes - e.g. a `0x01` byte followed by a whole bunch of zeroes, or a whole bunch of
/// zeroes with a `0x01` byte embedded inside.)
const SPLIT_MINIMUM: usize = SPLIT_WINDOW;

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


impl Iterator for SliceSplitter {
    type Item = ArcSlice;

    fn next(&mut self) -> Option<ArcSlice> {
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
                let split = self.slice.clone().map(|slice| slice.split_at(i).0);
                let rest = self.slice.clone().map(|slice| slice.split_at(i).1);

                self.slice = rest;

                return Some(split);
            }
        }

        return Some(mem::replace(&mut self.slice, arc_slice::empty()));
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
pub struct SliceChunker {
    rest: ArcSlice,
    splitter: SliceSplitter,
}


impl SliceChunker {
    pub fn new(slice: ArcSlice) -> SliceChunker {
        SliceChunker {
            rest: slice.clone(),
            splitter: SliceSplitter::new(slice),
        }
    }
}


const CHUNK_MINIMUM: usize = 32;
const CHUNK_MODULUS_MASK: u16 = (1 << 7) - 1;
const CHUNK_CONSTANT: u16 = 1;
const CHUNK_WINDOW: usize = 4;


fn seahash(slice: &[u8]) -> u8 {
    let mut hasher = SeaHasher::default();
    slice.hash(&mut hasher);
    hasher.finish() as u8
}


impl Iterator for SliceChunker {
    type Item = ArcSlice;

    fn next(&mut self) -> Option<ArcSlice> {
        if self.rest.len() == 0 {
            return None;
        }

        let mut ring = Ring::new();

        let mut offset = 0;
        let mut acc = 0;

        for (i, slice) in self.splitter.by_ref().enumerate() {
            let sig = seahash(&slice[slice.len() - cmp::min(SPLIT_WINDOW, slice.len())..]);

            if i >= CHUNK_WINDOW {
                acc -= ring.push_pop(sig) as u16;
            } else {
                ring.push(sig);
            }

            acc += sig as u16;
            acc &= CHUNK_MODULUS_MASK;

            offset += slice.len();

            if acc == CHUNK_CONSTANT && i >= CHUNK_MINIMUM {
                let split = self.rest.clone().map(|slice| slice.split_at(offset).0);
                let rest = self.rest.clone().map(|slice| slice.split_at(offset).1);

                self.rest = rest;

                return Some(split);
            }
        }

        return Some(mem::replace(&mut self.rest, arc_slice::empty()));
    }
}


/// A chunked file, consisting of a `Vec` of `ArcSlice`s. We wrap this in a newtype for safety.
pub struct Chunked(Vec<ArcSlice>);


impl Chunked {
    /// Extract the inner `Vec`, destroying the `Chunked` object.
    pub fn to_vec(self) -> Vec<ArcSlice> {
        self.0
    }
}


/// As `chunk_with_trace`, but with the default no-op trace.
pub fn chunk(bytes: ArcSlice) -> Chunked {
    chunk_with_trace(bytes, &mut ())
}


/// Chunk a slice into roughly 3MB chunks.
pub fn chunk_with_trace<'batch, T: SplitTrace>(bytes: ArcSlice, trace: &mut T) -> Chunked {
    let mut offset = 0u64;

    let slices = SliceChunker::new(bytes)
        .inspect(|chunk| {
            trace.on_chunk(offset, chunk);

            offset += chunk.len() as u64;
        })
        .collect();

    Chunked(slices)
}
