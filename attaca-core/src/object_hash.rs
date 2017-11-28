use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;

use failure::{Context, ResultExt};
use generic_array::GenericArray;
use typenum::consts;


/// The SHA3-256 hash of an object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectHash(pub GenericArray<u8, consts::U32>);


impl ObjectHash {
    #[inline]
    pub fn zero() -> Self {
        ObjectHash(GenericArray::clone_from_slice(&[0; 32]))
    }


    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }


    #[inline]
    pub fn to_path(&self) -> PathBuf {
        use std::fmt::Write;

        let mut buf = String::with_capacity(32);

        write!(buf, "{:02x}/{:02x}/", self.0[0], self.0[1]).unwrap();

        for b in &self.0[2..] {
            write!(buf, "{:02x}", b).unwrap();
        }

        buf.into()
    }
}


impl Borrow<[u8]> for ObjectHash {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}


impl Deref for ObjectHash {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl fmt::Debug for ObjectHash {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Display>::fmt(self, f)
    }
}


impl fmt::Display for ObjectHash {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for &b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }

        Ok(())
    }
}


pub type ObjectHashParseError = Context<ObjectHashParseErrorKind>;


#[derive(Debug, Fail)]
pub enum ObjectHashParseErrorKind {
    #[fail(display = "hash string should be 64 bytes (32 hex digits) but was {} bytes", _0)]
    InvalidLength(usize),

    #[fail(display = "string contains invalid hexadecimal: {}", _0)] InvalidHex(String),
}


impl FromStr for ObjectHash {
    type Err = ObjectHashParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::ObjectHashParseErrorKind as ErrorKind;

        if s.len() != 64 {
            return Err(Context::new(ErrorKind::InvalidLength(s.len())));
        }

        let mut generic_array = GenericArray::map_slice(&[0; 32], |&x| x);
        for (i, byte) in generic_array.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..(i + 1) * 2], 16)
                .with_context(|_| ErrorKind::InvalidHex(s.to_owned()))?;
        }

        Ok(ObjectHash(generic_array))
    }
}
