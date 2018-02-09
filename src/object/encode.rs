use std::{collections::HashMap, io::Write};

use failure::Error;

use object::{LargeObject, ObjectKind, SmallObject, TreeObject};
use store::{Handle, HandleBuilder};

pub fn small<H, HB>(builder: &mut HB, object: SmallObject) -> Result<(), Error>
where
    H: Handle,
    HB: HandleBuilder<H>,
{
    builder.write_all(&object.data)?;
    Ok(())
}

pub fn large<H, HB>(builder: &mut HB, object: LargeObject<H>) -> Result<(), Error>
where
    H: Handle,
    HB: HandleBuilder<H>,
{
    let mut handles = HashMap::new();

    for &(size, kind, ref handle) in &object.children {
        let ks = match kind {
            ObjectKind::Small => "small",
            ObjectKind::Large => "large",
            _ => bail!("Bad large object: child with bad kind (not small or large)"),
        };

        let id = match handles.get(handle) {
            Some(&id) => id,
            None => {
                let new_id = handles.len();
                handles.insert(handle, new_id);
                builder.add_reference(handle.clone());
                new_id
            }
        };

        write!(builder, "{} {} {}\n", size, ks, id)?;
    }

    Ok(())
}

pub fn tree<H, HB>(builder: &mut HB, object: TreeObject<H>) -> Result<(), Error>
where
    H: Handle,
    HB: HandleBuilder<H>,
{
    let mut handles = HashMap::new();

    for (name, &(kind, ref handle)) in &object.entries {
        let ks = match kind {
            ObjectKind::Small => "small",
            ObjectKind::Large => "large",
            _ => bail!("Bad large object: child with bad kind (not small or large)"),
        };

        let id = match handles.get(handle) {
            Some(&id) => id,
            None => {
                let new_id = handles.len();
                handles.insert(handle, new_id);
                builder.add_reference(handle.clone());
                new_id
            }
        };

        let mut buf = Vec::new();
        write!(&mut buf, "{} {} {}\n", ks, id, name)?;

        write!(builder, "{}:", buf.len())?;
        builder.write_all(&buf)?;
        write!(builder, ",")?;
    }

    Ok(())
}
