use std::{collections::HashMap, io::Write};

use failure::Error;

use object::{Commit, Large, ObjectRef, Small, Tree};
use store::HandleBuilder;

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

    for (&start, &(end, ref reference)) in &object.entries {
        let (ks, handle) = match *reference {
            ObjectRef::Small(ref small) => ("small", small.as_handle().clone()),
            ObjectRef::Large(ref large) => ("large", large.as_handle().clone()),
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

        write!(builder, "{} {} {} {}\n", start, end, ks, id)?;
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
            ObjectRef::Small(ref small) => ("small", small.as_handle()),
            ObjectRef::Large(ref large) => ("large", large.as_handle()),
            ObjectRef::Tree(ref tree) => ("tree", tree.as_handle()),
            _ => bail!("Bad tree object: child with bad kind (not small, large or tree)"),
        };

        let id = match handles.get(&handle) {
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

pub fn commit<HB>(builder: &mut HB, object: &Commit<HB::Handle>) -> Result<(), Error>
where
    HB: HandleBuilder,
{
    builder.add_reference(object.subtree.as_handle().clone());
    for parent in &object.parents {
        builder.add_reference(parent.as_handle().clone());
    }
    for handle in object.metadata.as_handles() {
        builder.add_reference(handle.clone());
    }

    write!(
        builder,
        "{} {}\n",
        object.parents.len(),
        object.metadata.as_handles().len()
    )?;

    builder.write_all(object.metadata.as_str().as_bytes())?;

    Ok(())
}
