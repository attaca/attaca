use failure::*;
use futures::prelude::*;

use Repository;
use quantified::{QuantifiedOutput, QuantifiedRef};

#[derive(Debug, StructOpt, Builder)]
#[structopt(name = "fsck")]
pub struct FsckArgs {
    #[structopt(short = "d", long = "depth",
                raw(possible_values = r#"&["commit", "tree", "large", "small"]"#))]
    pub depth: String,
}
