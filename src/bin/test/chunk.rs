use clap::{App, Arg, SubCommand, ArgMatches};
use histogram::Histogram;
use memmap::{Mmap, Protection};

use attaca::split;
use attaca::trace::SplitTrace;

use errors::Result;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("chunk")
        .about("Dry-run the hashsplitter on a single file.")
        .after_help("Chunk a file and list statistics for the resulting chunks. This is a \"dry-run\" and will not write any files to disk.")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to chunk.")
                .required(true)
                .index(1),
        )
}


#[derive(Default)]
struct TraceSplit {
    processed: u64,
    total: u64,
    stats: Histogram,
}


impl SplitTrace for TraceSplit {
    fn on_chunk(&mut self, _offset: u64, chunk: &[u8]) {
        eprintln!(
            "Chunk {:09} :: size {:08}, total MB: {:010}",
            self.processed,
            chunk.len(),
            self.total / 1_000_000
        );

        self.processed += 1;
        self.total += chunk.len() as u64;
        self.stats.increment(chunk.len() as u64).unwrap();
    }
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    let mmap = Mmap::open_path(matches.value_of("INPUT").unwrap(), Protection::Read)?;
    let mut trace = TraceSplit::default();
    split::chunk_with_trace(unsafe { mmap.as_slice() }, &mut trace);

    let stats = trace.stats;

    println!(
        "(Sizes) Min: {} Avg: {} Max: {} StdDev: {}",
        stats.minimum().unwrap(),
        stats.mean().unwrap(),
        stats.maximum().unwrap(),
        stats.stddev().unwrap(),
    );

    Ok(())
}
