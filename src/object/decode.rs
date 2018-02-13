use std::{collections::BTreeMap, io::Read, str::{self, FromStr}};

use failure::{self, Error};
use nom::{digit, rest, IResult};

use object::{Large, ObjectKind, ObjectRef, Small, Tree};
use store::Handle;

#[cfg_attr(rustfmt, rustfmt_skip)]
macro_rules! netstring {
  ($i:expr, $submac:ident!( $($args:tt)* )) => (
    length_value!($i,
      map_res!(
        map_res!(
          digit,
          str::from_utf8
        ),
        u64::from_str
      ),
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
  tagged_handle<(ObjectKind, usize)>,
  separated_pair!(
    object_kind,
    tag!(b" "),
    map_res!(
      map_res!(
        digit,
        str::from_utf8
      ),
      usize::from_str
    )
  )
);

pub fn small<H: Handle>(mut content: H::Content) -> Result<Small, Error> {
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    Ok(Small { data })
}

#[cfg_attr(rustfmt, rustfmt_skip)]
named!(large_entry<(u64, ObjectKind, usize)>,
  do_parse!(
    size: map_res!(
      map_res!(
        digit,
        str::from_utf8
      ),
      u64::from_str
    ) >>
    tag!(b" ") >>
    th: tagged_handle >>
    tag!(b"\n") >>
    (size, th.0, th.1)
  )
);

pub fn large<H: Handle>(mut content: H::Content, refs_iter: H::Refs) -> Result<Large<H>, Error> {
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    let refs = refs_iter.collect::<Vec<_>>();

    let ir: IResult<_, _> = fold_many0!(
        data.as_slice(),
        large_entry,
        Ok(Vec::new()),
        |acc_res: Result<Vec<_>, Error>, (sz, kind, handle_idx): (u64, ObjectKind, usize)| {
            let mut acc = acc_res?;
            let reference = refs.get(handle_idx)
                .cloned()
                .ok_or_else(|| failure::err_msg("Bad handle index!"))?;
            acc.push((
                sz,
                match kind {
                    ObjectKind::Small => ObjectRef::Small(reference),
                    ObjectKind::Large => ObjectRef::Large(reference),
                    other => bail!("Malformed tree: object kind {:?} not supported!", other),
                },
            ));
            Ok(acc)
        }
    );

    Ok(Large {
        children: ir.to_result()??,
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
                    ObjectKind::Small => ObjectRef::Small(reference),
                    ObjectKind::Large => ObjectRef::Large(reference),
                    ObjectKind::Tree => ObjectRef::Tree(reference),
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
