extern crate attaca;
#[macro_use]
extern crate clap;
extern crate histogram;
extern crate iter_read;
extern crate rand;

use std::io::{self, BufReader};

use attaca::split::{Parameters, Splitter};
use histogram::Histogram;
use iter_read::IterRead;
use rand::Rng;

fn main() {
    let mut rng = rand::thread_rng();

    let mut hs = Histogram::new();
    let mut sp = Splitter::new(
        BufReader::new(IterRead::<u8, _>::new(rng.gen_iter().take(1 << 25))),
        Parameters {
            // stride: 8,
            // strides_per_window: 1024,
            ..Parameters::default()
        },
    );

    let (mut n, mut t) = (0, 0);
    while let Some(range) = sp.find(&mut io::sink()).unwrap() {
        let range_len = range.end - range.start;
        hs.increment(range_len as u64).unwrap();
        n += 1;
        t += range_len;
    }

    println!("{} chunks from {} bytes.", n, t);

    println!(
        "Percentiles: p50: {} bytes p90: {} bytes p99: {} bytes p999: {} bytes",
        hs.percentile(50.0).unwrap(),
        hs.percentile(90.0).unwrap(),
        hs.percentile(99.0).unwrap(),
        hs.percentile(99.9).unwrap(),
    );

    println!(
        "Chunk size (bytes): Min: {} Avg: {} Max: {} StdDev: {}",
        hs.minimum().unwrap(),
        hs.mean().unwrap(),
        hs.maximum().unwrap(),
        hs.stddev().unwrap(),
    );
}
