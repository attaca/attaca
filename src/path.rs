use std::{fmt, io::Write, ops::Add, path::{Path, PathBuf}};

use failure::Error;
use im::{List, shared::Shared};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectPath {
    pub(crate) inner: List<String>,
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

    pub fn depth(&self) -> usize {
        self.inner.len()
    }

    pub fn push_front<S: Shared<String>>(&self, component: S) -> Self {
        Self {
            inner: self.inner.push_front(component),
        }
    }

    pub fn push_back<S: Shared<String>>(&self, component: S) -> Self {
        Self {
            inner: self.inner.push_back(component),
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: &P) -> Result<Self, Error> {
        let inner_opt = path.as_ref()
            .iter()
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

macro_rules! add_impl {
    ([$($quant:tt)*] $rhs:ty , $lhs:ty) => {
        impl $($quant)* Add<$rhs> for $lhs {
            type Output = ObjectPath;

            fn add(self, rhs: $rhs) -> Self::Output {
                ObjectPath {
                    inner: self.inner.append(&rhs.inner),
                }
            }
        }
    };
}

add_impl!([] ObjectPath, ObjectPath);
add_impl!([<'a>] ObjectPath, &'a ObjectPath);
add_impl!([<'a>] &'a ObjectPath, ObjectPath);
add_impl!([<'a>] &'a ObjectPath, &'a ObjectPath);
