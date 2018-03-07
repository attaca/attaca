use std::{fmt, str::FromStr};

use failure::*;
use regex::Regex;

#[derive(Debug, Clone)]
pub enum LocalRef {
    Branch(String),
}

impl fmt::Display for LocalRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LocalRef::Branch(ref branch) => f.pad(branch),
        }
    }
}

impl FromStr for LocalRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(\w)+$").unwrap();
        }
        ensure!(RE.is_match(s), "unable to parse {} as localref!", s);
        Ok(LocalRef::Branch(String::from(s)))
    }
}

#[derive(Debug, Clone)]
pub enum Ref {
    Local(LocalRef),
    Remote(String, LocalRef),
    Head,
}

impl fmt::Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Ref::Local(ref local) => local.fmt(f),
            Ref::Remote(ref remote, ref local) => write!(f, "{}/{}", remote, local),
            Ref::Head => f.pad("HEAD"),
        }
    }
}

impl FromStr for Ref {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "HEAD" {
            Ok(Ref::Head)
        } else {
            lazy_static! {
                static ref RE: Regex = Regex::new(r"^(?:(?P<r>\w+)/)?(?P<b>\w+)$").unwrap();
            }

            let cap = RE.captures(s)
                .ok_or_else(|| format_err!("could not parse {} as a ref!", s))?;
            let local_ref = cap.name("b").unwrap().as_str().parse()?;
            match cap.name("r") {
                Some(remote) => Ok(Ref::Remote(String::from(remote.as_str()), local_ref)),
                None => Ok(Ref::Local(local_ref)),
            }
        }
    }
}
