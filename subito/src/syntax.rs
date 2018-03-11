use std::{fmt, borrow::Cow, ops::Deref, str::FromStr, sync::Arc};

use failure::*;
use regex::{Regex, RegexSet};

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
pub struct RemoteRef {
    pub remote: Name,
    pub branch: Name,
}

impl fmt::Display for RemoteRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.remote, self.branch)
    }
}

impl FromStr for RemoteRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(\w+)/(\w+)$").unwrap();
        }

        let cap = RE.captures(s)
            .ok_or_else(|| format_err!("could not parse {} as a ref!", s))?;
        Ok(Self {
            remote: cap[1].parse()?,
            branch: cap[2].parse()?,
        })
    }
}

impl RemoteRef {
    pub fn new(remote: Name, branch: Name) -> Self {
        Self { remote, branch }
    }
}

#[derive(Debug, Clone)]
pub enum BranchRef {
    Local(Name),
    Remote(RemoteRef),
}

impl fmt::Display for BranchRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BranchRef::Local(ref name) => name.fmt(f),
            BranchRef::Remote(ref remote_ref) => remote_ref.fmt(f),
        }
    }
}

impl FromStr for BranchRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE_SET: RegexSet = RegexSet::new(&[
                r"^\w+$",
                r"^\w+/\w+$",
            ]).unwrap();
        }

        let matches = RE_SET.matches(s);
        if matches.matched(0) {
            Ok(BranchRef::Local(s.parse()?))
        } else if matches.matched(1) {
            Ok(BranchRef::Remote(s.parse()?))
        } else {
            bail!("Unable to parse branch ref as local branch or remote ref");
        }
    }
}

#[derive(Debug, Clone)]
pub enum Ref {
    Head,
    Branch(BranchRef),
}

impl fmt::Display for Ref {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Ref::Head => f.pad("HEAD"),
            Ref::Branch(ref branch_ref) => branch_ref.fmt(f),
        }
    }
}

impl FromStr for Ref {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "HEAD" {
            Ok(Ref::Head)
        } else {
            let branch_ref = s.parse()
                .map_err(|e| format_err!("Unable to parse as HEAD, or as branch ref ({})", e))?;
            Ok(Ref::Branch(branch_ref))
        }
    }
}
