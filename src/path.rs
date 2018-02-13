use std::{fmt, io::Write, path::{Path, PathBuf}};

use failure::Error;
use im::List;

#[derive(Clone)]
pub struct ObjectPath {
    inner: List<String>,
}

impl fmt::Debug for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list().entries(self.inner.iter()).finish()
    }
}

impl ObjectPath {
    pub fn new() -> Self {
        Self { inner: List::new() }
    }

    pub fn from_path(path: &Path) -> Result<Self, Error> {
        let inner_opt = path.iter()
            .map(|os_str| os_str.to_str().map(str::to_owned))
            .collect::<Option<List<String>>>();

        match inner_opt {
            Some(inner) => Ok(Self { inner }),
            None => bail!("Invalid unicode in path!"),
        }
    }

    pub fn to_path(&self) -> PathBuf {
        // Do this imperatively because `List::iter` returns the irritating item type of
        // `Arc<String>` instead of `&String`.
        let mut path_buf = PathBuf::new();
        for s in &self.inner {
            path_buf.push(s.as_ref());
        }
        path_buf
    }

    pub fn encode<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        for s in &self.inner {
            let bytes = s.as_bytes();
            write!(w, "{}:", bytes.len())?;
            w.write_all(bytes)?;
            write!(w, ",")?;
        }

        Ok(())
    }
}
