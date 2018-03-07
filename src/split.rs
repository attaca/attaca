use std::{io::{Read, Write}, ops::Range};

use failure::Error;

#[derive(Debug, Clone, Copy)]
pub struct Parameters {
    pub stride: usize,
    pub strides_per_window: usize,

    pub split_marker: u64,
    pub log2_modulus: u32,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            stride: 1,
            strides_per_window: 8192,

            split_marker: 1,
            log2_modulus: 14,
        }
    }
}

struct State<R: Read> {
    parameters: Parameters,
    buffer: Box<[u8]>,
    buffer_idx: usize,
    source: R,

    // Total strides read so far.
    total: usize,
}

impl<R: Read> State<R> {
    fn new(source: R, parameters: Parameters) -> Self {
        Self {
            parameters,
            buffer: vec![0; parameters.stride * (parameters.strides_per_window + 1)]
                .into_boxed_slice(),
            buffer_idx: 0,
            source,
            total: 0,
        }
    }

    #[inline]
    fn next_buffer(&mut self) {
        self.buffer_idx = (self.buffer_idx + 1) % (self.parameters.strides_per_window + 1);
    }

    #[inline]
    fn fill_buffer(&mut self) -> Result<(&[u8], &[u8]), Error> {
        let (old, new) = {
            let stride = self.parameters.stride;
            let strides_per_window = self.parameters.strides_per_window;

            let (new, old) = if self.buffer_idx == strides_per_window {
                let (old, new) = self.buffer.split_at_mut(strides_per_window * stride);
                (new, old)
            } else {
                self.buffer[self.buffer_idx * stride..][..(stride * 2)].split_at_mut(stride)
            };

            (&old[..stride], &mut new[..stride])
        };

        let mut n = 0;
        loop {
            let dn = self.source.read(&mut new[n..])?;
            if dn == 0 {
                break;
            }
            n += dn;
        }

        Ok((old, &new[..n]))
    }

    /// Write all bytes up to the next split marker into the "sink".
    fn find<W: Write>(mut self, sink: &mut W) -> Result<(Option<Self>, Range<usize>), Error> {
        let p = self.parameters;

        let mut chunk_strides = 0; // The number of strides we've read.
        let mut acc = 0u64;

        loop {
            let read_bytes = {
                let (old, new) = self.fill_buffer()?; // Fill the buffer with new bytes from the source.
                sink.write_all(new)?; // Write the newly filled buffer into the sink.

                for &b in new {
                    acc = acc.wrapping_add(b as u64); // Insert new values into the rolling hash.
                }

                if chunk_strides >= p.strides_per_window {
                    // If we've read enough bytes to fill the split window, we may begin removing old bytes
                    // from the rolling hash and checking for splits.
                    for &b in old {
                        acc = acc.wrapping_sub(b as u64);
                    }
                }

                new.len()
            };

            if read_bytes < p.stride {
                let pre_chunk_bytes = self.total * p.stride;
                let total_bytes = pre_chunk_bytes + chunk_strides * p.stride + read_bytes;

                return Ok((None, pre_chunk_bytes..total_bytes)); // If the buffer *isn't* full, we've hit EOF. Write and finish up.
            }

            chunk_strides += 1; // At this point, we've read a full stride.

            // Truncate the accumulator, treating it as modulo some power of two.
            acc &= (1 << p.log2_modulus) - 1;

            if chunk_strides >= p.strides_per_window && acc == p.split_marker {
                let pre_chunk_bytes = self.total * p.stride;
                let post_chunk_bytes = pre_chunk_bytes + chunk_strides * p.stride;

                self.total += chunk_strides;

                return Ok((Some(self), pre_chunk_bytes..post_chunk_bytes));
            }

            self.next_buffer();
        }
    }
}

/// A hashsplitter over a given bytestream.
pub struct Splitter<R: Read> {
    inner: Option<State<R>>,
}

impl<R: Read> Splitter<R> {
    /// Create a new splitter with a given byte stream and parameters.
    pub fn new(reader: R, params: Parameters) -> Self {
        Self {
            inner: Some(State::new(reader, params)),
        }
    }

    /// Find the next split marker in the byte stream.
    pub fn find<W: Write>(&mut self, sink: &mut W) -> Result<Option<Range<usize>>, Error> {
        match self.inner.take() {
            Some(state) => {
                let (new_state, split) = state.find(sink)?;
                self.inner = new_state;
                Ok(Some(split))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_1() {
        let data = vec![
            0, 1, 2, 3, 255, 5, 6, 7, 255, 255, 255, 11, 12, 13, 14, 15, 16, 17, 18, 19, 255, 21,
            22, 23, 24, 25, 26, 255, 28, 29, 30, 31,
        ];
        let mut data_slice = data.as_slice();
        let mut splitter = Splitter::new(
            &mut data_slice,
            Parameters {
                stride: 1,
                strides_per_window: 1,

                log2_modulus: 8,
                split_marker: 255,
            },
        );

        let mut split = Vec::new();
        assert_eq!(splitter.find(&mut split).unwrap(), Some(0..5));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(5..9));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(9..10));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(10..11));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(11..21));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(21..28));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(28..32));
        assert_eq!(splitter.find(&mut split).unwrap(), None);

        assert_eq!(data, split);
    }

    #[test]
    fn split_2() {
        let data = vec![
            0, 1, 2, 128, 127, 5, 6, 7, 248, 127, 128, 11, 12, 13, 14, 15, 16, 17, 18, 19, 236, 21,
            22, 23, 24, 25, 26, 255, 28, 29, 30, 31,
        ];
        let mut data_slice = data.as_slice();
        let mut splitter = Splitter::new(
            &mut data_slice,
            Parameters {
                stride: 1,
                strides_per_window: 2,

                log2_modulus: 8,
                split_marker: 255,
            },
        );

        let mut split = Vec::new();
        assert_eq!(splitter.find(&mut split).unwrap(), Some(0..5));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(5..9));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(9..11));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(11..21));
        assert_eq!(splitter.find(&mut split).unwrap(), Some(21..32));
        assert_eq!(splitter.find(&mut split).unwrap(), None);

        assert_eq!(data, split);
    }
}
