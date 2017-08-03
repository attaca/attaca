extern crate attaca;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;


mod chunk;
mod errors;
mod test;


use clap::App;

use errors::{ErrorKind, Result};


quick_main!(run);


fn run() -> Result<()> {
    let matches = App::new(crate_name!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .version(crate_version!())
        .subcommand(test::command())
        .get_matches();

    match matches.subcommand() {
        ("test", Some(sub_m)) => test::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        },
    }
}
