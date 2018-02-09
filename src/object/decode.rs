use std::{collections::BTreeMap, io::Read, str::{self, FromStr}};

use failure::{self, Error};
use futures::prelude::*;
use nom::{digit, rest, IResult};

use object::{LargeObject, ObjectKind, SmallObject, TreeObject};
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

#[async]
pub fn small<H: Handle>(handle: H) -> Result<SmallObject, Error> {
    let (mut content, _) = await!(handle.load())?;
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    Ok(SmallObject { data })
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

#[async]
pub fn large<H: Handle>(handle: H) -> Result<LargeObject<H>, Error> {
    let (mut content, refs_iter) = await!(handle.load())?;
    let mut data = Vec::new();
    content.read_to_end(&mut data)?;
    let refs = refs_iter.collect::<Vec<_>>();

    #[cfg_attr(rustfmt, rustfmt_skip)]
    let ir: IResult<_, _> = fold_many0!(
        data.as_slice(),
        large_entry,
        Ok(Vec::new()),
        |acc_res: Result<Vec<_>, Error>, (sz, kind, handle_idx): (u64, ObjectKind, usize)| {
            let mut acc = acc_res?;
            let reference = refs.get(handle_idx)
                .cloned()
                .ok_or_else(|| failure::err_msg("Bad handle index!"))?;
            acc.push((sz, kind, reference));
            Ok(acc)
        }
    );

    Ok(LargeObject {
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

#[async]
pub fn tree<H: Handle>(handle: H) -> Result<TreeObject<H>, Error> {
    let (mut content, refs_iter) = await!(handle.load())?;
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
            acc.insert(name.to_owned(), (kind, reference));
            Ok(acc)
        }
    );

    Ok(TreeObject {
        entries: ir.to_result()??,
    })
}
