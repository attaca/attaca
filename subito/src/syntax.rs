use std::{fmt, borrow::Cow, ops::Deref, str::FromStr, sync::Arc};

use failure::*;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Name(Arc<String>);

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl FromStr for Name {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(Cow::Borrowed(s))
    }
}

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Name {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn parse(s: Cow<str>) -> Result<Self, Error> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(\w)+$").unwrap();
        }
        ensure!(RE.is_match(&s), "'{}' is not a valid name!", s);
        Ok(Name(Arc::from(s.into_owned())))
    }

    pub fn from_string(s: String) -> Result<Self, Error> {
        Self::parse(Cow::Owned(s))
    }

    pub fn into_string(self) -> String {
        Arc::try_unwrap(self.0).unwrap_or_else(|e| (*e).clone())
    }
}

#[derive(Debug, Clone)]
pub enum Ref {
    Local(Name),
    Remote(Name, Name),
    Head,
}

impl fmt::Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Ref::Local(ref name) => name.fmt(f),
            Ref::Remote(ref remote, ref name) => write!(f, "{}/{}", remote, name),
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
            let name = cap.name("b").unwrap().as_str().parse()?;
            match cap.name("r") {
                Some(remote) => Ok(Ref::Remote(remote.as_str().parse()?, name)),
                None => Ok(Ref::Local(name)),
            }
        }
    }
}
