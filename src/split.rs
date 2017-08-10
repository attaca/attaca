//! `split` - split extremely large files into consistently-sized deterministic chunks.
//!
//! Key functionality of this module includes:
//! - Splitting large files into chunks, via memory-mapped files.
//! - Splitting streams of bytes into chunks, for use with lazily-downloaded files.


use std::fs::File;
use std::hash::{Hash, Hasher};
use std::mem;

use histogram::Histogram;
use memmap::{Mmap, Protection};
use seahash::SeaHasher;

use errors::Result;
use marshal::Chunk;


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


    fn push(&mut self, byte: u8) {
        self.push_pop(byte);
    }


    fn push_pop(&mut self, byte: u8) -> u8 {
        let popped = mem::replace(&mut self.buf[self.loc], byte);
        self.loc = (self.loc + 1) % CHUNK_WINDOW;

        popped
    }
}


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
    type Item = Chunk<'a>;

    fn next(&mut self) -> Option<Chunk<'a>> {
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

                return Some(split.into());
            }
        }

        return Some(mem::replace(&mut self.rest, &[]).into());
    }
}


pub struct FileChunker {
    mmap: Mmap,
}


impl FileChunker {
    pub fn new(file: &File) -> Result<FileChunker> {
        let mmap = Mmap::open(file, Protection::Read)?;

        Ok(FileChunker { mmap })
    }


    pub fn iter(&self) -> SliceChunker {
        let slice = unsafe { self.mmap.as_slice() };
        SliceChunker::new(slice)
    }


    pub fn chunk_stats(&self) -> Histogram {
        let mut chunk_sizes = Histogram::new();
        let mut bytes_processed = 0u64;

        let slice = unsafe { self.mmap.as_slice() };

        for (i, chunk) in SliceChunker::new(slice).enumerate() {
            let chunk_size = chunk.len() as u64;
            bytes_processed += chunk_size;

            eprintln!("Chunk {} :: size {}, total MB: {} / {}", i, chunk_size, bytes_processed / 1_000_000, slice.len() as u64 / 1_000_000);

            chunk_sizes.increment(chunk_size).unwrap();
        }

        chunk_sizes
    }


    pub fn chunk(&self) -> ChunkedFile {
        let mut chunk_sizes = Histogram::new();
        let mut bytes_processed = 0u64;

        let slice = unsafe { self.mmap.as_slice() };
        let chunks = SliceChunker::new(slice)
            .enumerate()
            .inspect(|&(i, ref chunk)| {
                bytes_processed += chunk.len() as u64;

                eprintln!("Chunk {} :: size {}, total MB: {}", i, chunk.len(), bytes_processed / 1_000_000);

                chunk_sizes
                    .increment(chunk.len() as u64)
                    .unwrap();
            })
            .map(|(_, chunk)| chunk)
            .collect();

        ChunkedFile {
            slice,
            chunks,
            chunk_sizes,
        }
    }
}


pub struct ChunkedFile<'a> {
    slice: &'a [u8],
    chunks: Vec<Chunk<'a>>,

    chunk_sizes: Histogram,
}


impl<'a> ChunkedFile<'a> {
    pub fn as_slice(&self) -> &'a [u8] {
        self.slice
    }


    pub fn chunks(&self) -> &[Chunk<'a>] {
        &self.chunks
    }


    pub fn sizes(&self) -> &Histogram {
        &self.chunk_sizes
    }
}
