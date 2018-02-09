use std::{mem, borrow::Cow, io::{self, Read, Write}, iter::FromIterator};

use failure::Error;
use smallvec::SmallVec;

use b10::b10_digits;

#[derive(Debug)]
pub enum Component<'a> {
    Byterope(Byterope<'a>),
    Netstring(Netstring<'a>),
}

impl<'a> From<&'a [u8]> for Component<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Component::Byterope(Byterope(Byterope_::Lift(Cow::Borrowed(bytes))))
    }
}

impl<'a> From<Vec<u8>> for Component<'a> {
    fn from(bytes: Vec<u8>) -> Self {
        Component::Byterope(Byterope(Byterope_::Lift(Cow::Owned(bytes))))
    }
}

impl<'a> From<Cow<'a, [u8]>> for Component<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        Component::Byterope(Byterope(Byterope_::Lift(bytes)))
    }
}

impl<'a> From<Byterope<'a>> for Component<'a> {
    fn from(br: Byterope<'a>) -> Self {
        Component::Byterope(br)
    }
}

impl<'a> From<Netstring<'a>> for Component<'a> {
    fn from(ns: Netstring<'a>) -> Self {
        Component::Netstring(ns)
    }
}

impl<'a> Component<'a> {
    pub fn len(&self) -> usize {
        use self::Component::*;

        match *self {
            Byterope(ref br) => br.len(),
            Netstring(ref ns) => ns.len(),
        }
    }

    pub fn encode<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        use self::Component::*;

        match *self {
            Byterope(ref br) => br.encode(w),
            Netstring(ref ns) => ns.encode(w),
        }
    }
}

#[derive(Debug)]
pub struct Netstring<'a>(Byterope<'a>);

impl<'a> From<&'a [u8]> for Netstring<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Netstring(Byterope::from(bytes))
    }
}

impl<'a, T: 'a> FromIterator<T> for Netstring<'a>
where
    T: Into<Component<'a>>,
{
    fn from_iter<I>(iterable: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let mut ns = Netstring::new();
        for c in iterable {
            ns.push(c);
        }
        ns
    }
}

impl<'a> Netstring<'a> {
    pub fn new() -> Self {
        Netstring(Byterope::new())
    }

    pub fn len(&self) -> usize {
        let pl_sz = self.0.len();
        pl_sz + 2 + b10_digits(pl_sz as u64) as usize
    }

    pub fn push<T: Into<Component<'a>>>(&mut self, component: T) {
        self.0.push(component);
    }

    pub fn encode<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        write!(w, "{}:", self.0.len())?;
        self.0.encode(w)?;
        write!(w, ",")?;
        Ok(())
    }
}

#[derive(Debug)]
enum Byterope_<'a> {
    Lift(Cow<'a, [u8]>),
    Concat(usize, Vec<Component<'a>>),
}

#[derive(Debug)]
pub struct Byterope<'a>(Byterope_<'a>);

impl<'a> From<&'a [u8]> for Byterope<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Byterope(Byterope_::Lift(Cow::Borrowed(bytes)))
    }
}

impl<'a> From<Vec<u8>> for Byterope<'a> {
    fn from(bytes: Vec<u8>) -> Self {
        Byterope(Byterope_::Lift(Cow::Owned(bytes)))
    }
}

impl<'a> From<Cow<'a, [u8]>> for Byterope<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        Byterope(Byterope_::Lift(bytes))
    }
}

impl<'a> Byterope<'a> {
    pub fn new() -> Self {
        Byterope(Byterope_::Concat(0, Vec::new()))
    }

    pub fn len(&self) -> usize {
        use self::Byterope_::*;

        match self.0 {
            Lift(ref buf) => buf.len(),
            Concat(sz, _) => sz,
        }
    }

    pub fn push<T: Into<Component<'a>>>(&mut self, component: T) {
        use self::{Byterope_::*, Component::*};

        let r = component.into();
        let curr = mem::replace(&mut self.0, Lift(Cow::Borrowed(&[])));
        self.0 = match curr {
            Concat(mut sz, mut components) => {
                sz += r.len();
                components.push(r);
                Concat(sz, components)
            }
            Lift(l) => {
                let sz = l.len() + r.len();
                Concat(sz, vec![l.into(), r])
            }
        };
    }

    pub fn encode<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        use self::Byterope_::*;

        match self.0 {
            Lift(ref buf) => w.write_all(buf)?,
            Concat(_, ref cmps) => cmps.iter()
                .map(|cmp| cmp.encode(w))
                .collect::<Result<_, _>>()?,
        }

        Ok(())
    }
}

pub fn decode<R: Read, W: Write>(r: &mut R, w: &mut W) -> Result<(), Error> {
    let mut len = 0;
    for br in r.by_ref().bytes() {
        let b = br?;
        if b >= b'0' && b <= b'9' {
            len = (len * 10) + (b - b'0') as usize;
        } else if b == b':' {
            break;
        } else {
            bail!(
                "Malformed netstring: expected ':' after length, found byte '{:x}'",
                b
            );
        }
    }
    io::copy(&mut r.by_ref().take(len as u64), w)?;
    match r.by_ref().bytes().next() {
        Some(Ok(b',')) => Ok(()),
        _ => bail!("Malformed netstring: expected ',' after {} bytes.", len),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn encode_hello_world() {
        let mut buf = Vec::new();
        Netstring::from(&b"hello world!"[..])
            .encode(&mut buf)
            .unwrap();
        assert_eq!(&buf, b"12:hello world!,");
    }

    #[test]
    fn decode_hello_world() {
        let mut buf = Vec::new();
        decode(&mut &b"12:hello world!,asdf"[..], &mut buf).unwrap();
        assert_eq!(&buf, b"hello world!");
    }

    #[test]
    fn encode_nested() {
        let mut ns = Netstring::new();
        ns.push(Netstring::from(&b"hello"[..]));
        ns.push(Netstring::from(&b" "[..]));
        ns.push({
            let mut ns = Netstring::new();
            ns.push(&b"wo"[..]);
            ns.push(&b"rld"[..]);
            ns.push({
                let mut ns = Netstring::new();
                ns.push(Netstring::from(&b""[..]));
                ns
            });
            ns.push(Netstring::from(&b"!"[..]));
            ns
        });
        let mut buf = Vec::new();
        ns.encode(&mut buf).unwrap();
        println!("{}", String::from_utf8_lossy(&buf));
        assert_eq!(&buf[..], &b"31:5:hello,1: ,15:world3:0:,,1:!,,,"[..]);
    }

    quickcheck! {
        fn roundtrip(bytes: Vec<u8>) -> bool {
            let mut enc = Vec::new();
            Netstring::from(&bytes[..]).encode(&mut enc).unwrap();
            let mut dec = Vec::new();
            decode(&mut &enc[..], &mut dec).unwrap();

            bytes == dec
        }

        fn roundtrip_multiple(strings: Vec<String>) -> bool {
            let mut ns = strings.iter().map(|s| Netstring::from(s.as_bytes())).collect::<Vec<_>>();
            let mut enc = Vec::new();
            for n in ns {
                n.encode(&mut enc).unwrap();
            }

            let mut enc_reader = Cursor::new(enc);
            strings.iter().all(|b| {
                let mut dec = Vec::new();
                decode(&mut enc_reader, &mut dec).unwrap();

                b.as_bytes() == &dec[..]
            })
        }
    }
}
