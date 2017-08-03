use std::fs::File;

use clap::{App, Arg, SubCommand, ArgMatches};

use attaca::split::FileChunker;

use errors::Result;


pub fn command() -> App<'static, 'static> {
    SubCommand::with_name("chunk").arg(
        Arg::with_name("INPUT")
            .help("Sets the input file to chunk.")
            .required(true)
            .index(1),
    )
}


pub fn go(matches: &ArgMatches) -> Result<()> {
    let chunker = {
        let file = File::open(matches.value_of("INPUT").unwrap())?;

        FileChunker::new(&file)?
    };

    let stats = chunker.chunk_stats();

    println!(
        "(Sizes) Min: {} Avg: {} Max: {} StdDev: {}",
        stats.minimum().unwrap(),
        stats.mean().unwrap(),
        stats.maximum().unwrap(),
        stats.stddev().unwrap(),
    );

    Ok(())
}
