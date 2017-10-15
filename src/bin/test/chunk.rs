use std::sync::{Arc, Mutex};

use clap::{App, Arg, SubCommand, ArgMatches};
use futures::prelude::*;
use histogram::Histogram;

use attaca::Repository;
use attaca::trace::Trace;

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


#[derive(Debug, Default)]
struct TraceSplitInner {
    processed: u64,
    total: u64,
    stats: Histogram,
}


#[derive(Debug, Clone, Default)]
struct TraceSplit {
    inner: Arc<Mutex<TraceSplitInner>>,
}


impl Trace for TraceSplit {
    fn on_split_chunk(&self, _offset: u64, chunk: &[u8]) {
        let mut inner = self.inner.lock().unwrap();

        eprintln!(
            "Chunk {:09} :: size {:08}, total MB: {:010}",
            inner.processed,
            chunk.len(),
            inner.total / 1_000_000
        );

        inner.processed += 1;
        inner.total += chunk.len() as u64;
        inner.stats.increment(chunk.len() as u64).unwrap();
    }
}


pub fn go(repository: &mut Repository, matches: &ArgMatches) -> Result<()> {
    let trace = TraceSplit::default();
    let mut ctx = repository.local(trace.clone())?;

    ctx.split_file(matches.value_of("INPUT").unwrap())
        .for_each(|_| Ok(()))
        .join(ctx.close())
        .wait()?;

    let stats = &trace.inner.lock().unwrap().stats;

    println!(
        "(Sizes) Min: {} Avg: {} Max: {} StdDev: {}",
        stats.minimum().unwrap(),
        stats.mean().unwrap(),
        stats.maximum().unwrap(),
        stats.stddev().unwrap(),
    );

    Ok(())
}
