use std::{usize, collections::HashMap, io::Write};

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
        assert!(end > start);
        let sz = end - start;

        let handle = match *reference {
            ObjectRef::Small(ref small) => {
                assert!(object.depth == 1 && sz <= usize::MAX as u64 && sz == small.size());
                small.as_inner().clone()
            }
            ObjectRef::Large(ref large) => {
                assert!(object.depth > 1 && object.depth == large.depth + 1 && sz == large.size());
                large.as_inner().clone()
            }
            _ => unreachable!("Bad large object!"),
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

        write!(builder, "{} {} {}\n", start, end, id)?;
    }

    Ok(())
}

pub fn tree<HB>(builder: &mut HB, object: &Tree<HB::Handle>) -> Result<(), Error>
where
    HB: HandleBuilder,
{
    let mut handles = HashMap::new();

    for (name, reference) in &object.entries {
        let handle = reference.as_inner();
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
        write!(&mut buf, "{} ", id)?;

        match *reference {
            ObjectRef::Small(ref small) => write!(&mut buf, "data {} {}", small.size(), 0)?,
            ObjectRef::Large(ref large) => {
                write!(&mut buf, "data {} {}", large.size(), large.depth())?
            }
            ObjectRef::Tree(_) => write!(&mut buf, "tree")?,
            _ => bail!("Bad tree object: child with bad kind (not small, large or tree)"),
        };

        write!(&mut buf, " {}", name)?;

        write!(builder, "{}:", buf.len())?;
        builder.write_all(&buf)?;
        write!(builder, ",\n")?;
    }

    Ok(())
}

pub fn commit<HB>(builder: &mut HB, object: &Commit<HB::Handle>) -> Result<(), Error>
where
    HB: HandleBuilder,
{
    builder.add_reference(object.subtree.as_inner().clone());
    for parent in &object.parents {
        builder.add_reference(parent.as_inner().clone());
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
