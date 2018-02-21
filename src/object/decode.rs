use std::{usize, collections::BTreeMap, io::{BufRead, Read}, str::{self, FromStr}};

use failure::{self, Error};
use nom::{digit, rest, IResult};

use object::{Commit, CommitRef, Large, LargeRef, ObjectKind, ObjectRef, Small, SmallRef, Tree,
             TreeRef, metadata::Metadata};
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
    length_value!($i,
      parse_u64,
      delimited!(tag!(b":"), $submac!($($args)*), tag!(b","))
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

    let ir: IResult<_, _> = fold_many0!(
        data.as_slice(),
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
    );

    Ok(Large {
        size,
        depth,
        entries: ir.to_result()??,
    })
}

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
        entry: alt!(
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
        ) >>
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

    let ir: IResult<_, _> = fold_many0!(
        data.as_slice(),
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

pub fn commit<H: Handle>(
    mut content: H::Content,
    mut refs_iter: H::Refs,
) -> Result<Commit<H>, Error> {
    let mut data = Vec::new();
    content.read_until(b'\n', &mut data)?;
    let (n_parents, n_meta) = commit_header(data.as_slice()).to_result()?;

    let subtree = TreeRef::new(refs_iter
        .next()
        .ok_or_else(|| format_err!("Malformed commit: no subtree handle!"))?);
    let parents = refs_iter
        .by_ref()
        .take(n_parents)
        .map(CommitRef::new)
        .collect();
    let meta_handles = refs_iter.by_ref().take(n_meta).collect();

    let mut meta_string = String::new();
    content.read_to_string(&mut meta_string)?;

    Ok(Commit {
        subtree,
        parents,
        metadata: Metadata::new(meta_string, meta_handles),
    })
}
