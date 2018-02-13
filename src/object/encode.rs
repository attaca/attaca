use std::{collections::HashMap, io::Write};

use failure::Error;

use object::{Large, ObjectRef, Small, Tree};
use store::{Handle, HandleBuilder};

pub fn small<HB>(builder: &mut HB, object: &Small) -> Result<(), Error>
where
    HB: HandleBuilder,
{
    builder.write_all(&object.data)?;
    Ok(())
}

pub fn large<HB>(builder: &mut HB, object: &Large<HB::Handle>) -> Result<(), Error>
where
    HB: HandleBuilder,
{
    let mut handles = HashMap::new();

    for &(size, ref reference) in &object.children {
        let (ks, handle) = match *reference {
            ObjectRef::Small(ref handle) => ("small", handle.clone()),
            ObjectRef::Large(ref handle) => ("large", handle.clone()),
            _ => bail!("Bad large object: child with bad kind (not small or large)"),
        };

        let id = match handles.get(&handle) {
            Some(&id) => id,
            None => {
                let new_id = handles.len();
                handles.insert(handle.clone(), new_id);
                builder.add_reference(handle);
                new_id
            }
        };

        write!(builder, "{} {} {}\n", size, ks, id)?;
    }

    Ok(())
}

pub fn tree<HB>(builder: &mut HB, object: &Tree<HB::Handle>) -> Result<(), Error>
where
    HB: HandleBuilder,
{
    let mut handles = HashMap::new();

    for (name, reference) in &object.entries {
        let (ks, handle) = match *reference {
            ObjectRef::Small(ref handle) => ("small", handle.clone()),
            ObjectRef::Large(ref handle) => ("large", handle.clone()),
            ObjectRef::Tree(ref handle) => ("tree", handle.clone()),
            _ => bail!("Bad tree object: child with bad kind (not small, large or tree)"),
        };

        let id = match handles.get(&handle) {
            Some(&id) => id,
            None => {
                let new_id = handles.len();
                handles.insert(handle.clone(), new_id);
                builder.add_reference(handle);
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
