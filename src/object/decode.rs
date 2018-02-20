use std::{collections::BTreeMap, io::{BufRead, Read}, str::{self, FromStr}};

use failure::{self, Error};
use nom::{digit, rest, IResult};

use object::{Commit, CommitRef, Large, LargeRef, ObjectKind, ObjectRef, Small, SmallRef, Tree,
             TreeRef, metadata::Metadata};
use store::Handle;

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(
  byte_offset<u64>,
  map_res!(
    map_res!(
      digit,
      str::from_utf8
    ),
    u64::from_str
  )
);

#[cfg_attr(rustfmt, rustfmt_skip)]
macro_rules! netstring {
  ($i:expr, $submac:ident!( $($args:tt)* )) => (
    length_value!($i,
      byte_offset,
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
named!(large_entry<(u64, u64, ObjectKind, usize)>,
  do_parse!(
    start: byte_offset >>
    tag!(b" ") >>
    end: byte_offset >>
    tag!(b" ") >>
    th: tagged_handle >>
    tag!(b"\n") >>
    (start, end, th.0, th.1)
  )
);

pub fn large<H: Handle>(mut content: H::Content, refs_iter: H::Refs) -> Result<Large<H>, Error> {
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    let refs = refs_iter.collect::<Vec<_>>();

    let ir: IResult<_, _> = fold_many0!(
        data.as_slice(),
        large_entry,
        Ok(BTreeMap::new()),
        |acc_res: Result<BTreeMap<_, _>, _>, (start, end, kind, handle_idx)| {
            let mut acc = acc_res?;
            let reference = refs.get(handle_idx)
                .cloned()
                .ok_or_else(|| failure::err_msg("Bad handle index!"))?;
            let objref = match kind {
                ObjectKind::Small => ObjectRef::Small(SmallRef::from_handle(reference)),
                ObjectKind::Large => ObjectRef::Large(LargeRef::from_handle(reference)),
                other => bail!("Malformed tree: object kind {:?} not supported!", other),
            };
            acc.insert(start, (end, objref));
            Ok(acc)
        }
    );

    Ok(Large {
        entries: ir.to_result()??,
    })
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(tree_entry<(ObjectKind, usize, &str)>,
  netstring!(
    do_parse!(
      th: tagged_handle >>
      tag!(b" ") >>
      name: map_res!(rest, str::from_utf8) >>
      tag!(b"\n") >>
      (th.0, th.1, name)
    )
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
        |acc_res: Result<BTreeMap<_, _>, Error>,
         (kind, handle_idx, name): (ObjectKind, usize, &str)| {
            let mut acc = acc_res?;
            let reference = refs.get(handle_idx)
                .cloned()
                .ok_or_else(|| failure::err_msg("Bad handle index!"))?;
            acc.insert(
                name.to_owned(),
                match kind {
                    ObjectKind::Small => ObjectRef::Small(SmallRef::from_handle(reference)),
                    ObjectKind::Large => ObjectRef::Large(LargeRef::from_handle(reference)),
                    ObjectKind::Tree => ObjectRef::Tree(TreeRef::from_handle(reference)),
                    other => bail!("Malformed tree: object kind {:?} not supported!", other),
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

    let subtree = TreeRef::from_handle(refs_iter
        .next()
        .ok_or_else(|| format_err!("Malformed commit: no subtree handle!"))?);
    let parents = refs_iter
        .by_ref()
        .take(n_parents)
        .map(CommitRef::from_handle)
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
