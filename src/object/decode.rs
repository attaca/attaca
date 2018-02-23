use std::{usize, collections::BTreeMap, io::{BufRead, Read}, str::{self, FromStr}};

use failure::{self, Error};
use nom::{digit, rest, IResult};
use ntriple::{self, Object, Predicate, Subject};

use object::{Commit, CommitAuthor, CommitBuilder, CommitRef, Large, LargeRef, ObjectKind,
             ObjectRef, Small, SmallRef, Tree, TreeRef,
             metadata::{ATTACA_COMMIT_MESSAGE, FOAF_MBOX, FOAF_NAME}};
use store::Handle;

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(
  parse_u64<u64>,
  map_res!(
    map_res!(
      digit,
      str::from_utf8
    ),
    u64::from_str
  )
);

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(
  parse_u8<u8>,
  map_res!(
    map_res!(
      digit,
      str::from_utf8
    ),
    u8::from_str
  )
);

#[cfg_attr(rustfmt, rustfmt_skip)]
macro_rules! netstring {
  ($i:expr, $submac:ident!( $($args:tt)* )) => (
    do_parse!($i,
      length: parse_u64 >>
      tag!(b":") >>
      value: flat_map!(take!(length), $submac!($($args)*)) >>
      tag!(b",") >>
      (value)
    )
  );
  ($i:expr, $f:expr) => (netstring!($i, call!($f)));
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(
  object_kind<ObjectKind>,
  alt_complete!(
      value!(ObjectKind::Small, tag!(b"small"))
    | value!(ObjectKind::Large, tag!(b"large"))
    | value!(ObjectKind::Tree, tag!(b"tree"))
    | value!(ObjectKind::Commit, tag!(b"commit"))
  )
);

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(
  handle<usize>,
  map_res!(
    map_res!(
      digit,
      str::from_utf8
    ),
    usize::from_str
  )
);

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(
  tagged_handle<(ObjectKind, usize)>,
  separated_pair!(
    object_kind,
    tag!(b" "),
    handle
  )
);

pub fn small<H: Handle>(mut content: H::Content) -> Result<Small, Error> {
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    Ok(Small { data })
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(large_entry<(u64, u64, usize)>,
  do_parse!(
    start: parse_u64 >>
    tag!(b" ") >>
    end: parse_u64 >>
    tag!(b" ") >>
    hd: handle >>
    tag!(b"\n") >>
    (start, end, hd)
  )
);

pub fn large<H: Handle>(
    mut content: H::Content,
    refs_iter: H::Refs,
    size: u64,
    depth: u8,
) -> Result<Large<H>, Error> {
    assert!(depth > 0);

    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    let refs = refs_iter.collect::<Vec<_>>();

    let ir: IResult<_, _> = terminated!(
        data.as_slice(),
        fold_many0!(
            large_entry,
            Ok(BTreeMap::new()),
            |acc_res: Result<BTreeMap<_, _>, Error>, (start, end, handle_idx)| {
                let mut acc = acc_res?;
                let reference = refs.get(handle_idx)
                    .cloned()
                    .ok_or_else(|| failure::err_msg("Bad handle index!"))?;
                assert!(end > start);
                let child_size = end - start;
                let child_depth = depth - 1;
                let objref = if child_depth == 0 {
                    assert!(size <= usize::MAX as u64, "Unable to keep Small in memory!");
                    ObjectRef::Small(SmallRef::new(child_size, reference))
                } else {
                    ObjectRef::Large(LargeRef::new(child_size, child_depth - 1, reference))
                };
                acc.insert(start, (end, objref));
                Ok(acc)
            }
        ),
        eof!()
    );

    Ok(Large {
        size,
        depth,
        entries: ir.to_result()??,
    })
}

#[derive(Debug)]
enum TreeEntry {
    Tree,
    Data(u64, u8),
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(tree_entry<(&str, usize, TreeEntry)>,
  terminated!(
    netstring!(
      do_parse!(
        hd: handle >>
        entry: dbg_dmp!(alt_complete!(
          do_parse!(
            tag!(b" data ") >>
            size: parse_u64 >>
            tag!(b" ") >>
            depth: parse_u8 >>
            tag!(b" ") >>
            (TreeEntry::Data(size, depth))
          ) |
          do_parse!(
            tag!(b" tree ") >>
            (TreeEntry::Tree)
          )
        )) >>
        name: map_res!(rest, str::from_utf8) >>
        (name, hd, entry)
      )
    ),
    tag!(b"\n")
  )
);

pub fn tree<H: Handle>(mut content: H::Content, refs_iter: H::Refs) -> Result<Tree<H>, Error> {
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    let refs = refs_iter.collect::<Vec<_>>();

    let ir: IResult<_, _> = terminated!(
        data.as_slice(),
        fold_many0!(
            tree_entry,
            Ok(BTreeMap::new()),
            |acc_res: Result<BTreeMap<_, _>, Error>, (name, hd, entry)| {
                let mut acc = acc_res?;
                let reference = refs.get(hd)
                    .cloned()
                    .ok_or_else(|| failure::err_msg("Bad handle index!"))?;
                acc.insert(
                    String::from(name),
                    match entry {
                        TreeEntry::Data(sz, 0) => ObjectRef::Small(SmallRef::new(sz, reference)),
                        TreeEntry::Data(sz, d) => ObjectRef::Large(LargeRef::new(sz, d, reference)),
                        TreeEntry::Tree => ObjectRef::Tree(TreeRef::new(reference)),
                    },
                );
                Ok(acc)
            }
        ),
        eof!()
    );

    Ok(Tree {
        entries: ir.to_result()??,
    })
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(commit_header<(usize, usize)>,
  do_parse!(
    n_parents: handle >>
    tag!(b" ") >>
    n_meta: handle >>
    tag!(b"\n") >>
    (n_parents, n_meta)
  )
);

// TODO: Robust RDF formatting - current breaks for non-ASCII strings:
// https://github.com/sdleffler/attaca/issues/25
pub fn commit<H: Handle>(
    mut content: H::Content,
    mut refs_iter: H::Refs,
) -> Result<Commit<H>, Error> {
    let mut data = Vec::new();
    content.read_until(b'\n', &mut data)?;
    let (n_parents, n_meta) = commit_header(data.as_slice()).to_result()?;

    let mut commit_builder = CommitBuilder::new();

    commit_builder.subtree(TreeRef::new(refs_iter
        .next()
        .ok_or_else(|| format_err!("Malformed commit: no subtree handle!"))?));
    commit_builder.parents(refs_iter.by_ref().take(n_parents).map(CommitRef::new));

    // We don't have anything to do with these metadata refs yet, but the encoding has them, so we
    // should at least consume them to be sure they're there.
    let _meta_handles = refs_iter.by_ref().take(n_meta).for_each(|_| ());

    let mut meta_string = String::new();
    content.read_to_string(&mut meta_string)?;

    let mut author = CommitAuthor::new();

    for line in meta_string.lines() {
        let triple = match ntriple::parser::triple_line(line)? {
            Some(triple) => triple,
            None => continue,
        };

        let name = match triple.subject {
            Subject::BNode(name) => name,

            // No-op if we don't recognize it.
            _ => continue,
        };

        match name.as_str() {
            "this" => {
                let Predicate::IriRef(iri) = triple.predicate;

                let object = match triple.object {
                    Object::Lit(literal) => literal.data,
                    _ => bail!("Malformed commit metadata: expected commit message to have a literal object"),
                };

                match iri.as_str() {
                    ATTACA_COMMIT_MESSAGE => commit_builder.message(object),
                    _ => bail!(
                        "Malformed commit metadata: invalid commit predicate <{}>",
                        iri
                    ),
                };
            }
            "author" => {
                let Predicate::IriRef(iri) = triple.predicate;

                let object = match triple.object {
                    Object::Lit(literal) => literal.data,
                    _ => bail!(
                        "Malformed commit metadata: expected author subject to be a literal object"
                    ),
                };

                match iri.as_str() {
                    FOAF_MBOX => author.mbox = Some(object),
                    FOAF_NAME => author.name = Some(object),
                    _ => bail!(
                        "Malformed commit metadata: invalid author predicate <{}>",
                        iri
                    ),
                }
            }

            // No-op if unrecognized subject.
            _ => {}
        }
    }

    commit_builder.author(author);
    Ok(commit_builder.into_commit()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_netstring(ref bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
            let mut buf = Vec::new();
            write!(&mut buf, "{}:", bytes.len()).unwrap();
            buf.write_all(&bytes).unwrap();
            write!(&mut buf, ",").unwrap();
            let battered_bytes = netstring!(buf.as_slice(), rest).to_full_result().unwrap();
            assert_eq!(battered_bytes, bytes.as_slice());
        }
    }
}
