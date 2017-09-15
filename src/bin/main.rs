extern crate attaca;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate histogram;
extern crate indicatif;


mod errors;
mod init;
mod test;
mod trace;


use clap::App;

use errors::{ErrorKind, Result};


quick_main!(run);


fn run() -> Result<()> {
    let matches = App::new(crate_name!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .version(crate_version!())
        .subcommand(init::command())
        .subcommand(test::command())
        .get_matches();

    match matches.subcommand() {
        ("init", Some(sub_m)) => init::go(sub_m),
        ("test", Some(sub_m)) => test::go(sub_m),
        _ => {
            eprintln!("{}", matches.usage());
            bail!(ErrorKind::InvalidUsage(format!("{:?}", matches)));
        }
    }
}
