use std::{ascii, usize, collections::{BTreeSet, HashMap}, io::Write};

use failure::Error;

use object::{Commit, Large, ObjectRef, Small, Tree,
             metadata::{ATTACA_COMMIT_MESSAGE, ATTACA_COMMIT_TIMESTAMP, FOAF_MBOX, FOAF_NAME}};
use store::prelude::*;

pub fn small<B: Backend>(builder: &mut Builder<B>, object: &Small) -> Result<(), Error> {
    builder.write_all(&object.data)?;
    Ok(())
}

pub fn large<B: Backend>(builder: &mut Builder<B>, object: &Large<Handle<B>>) -> Result<(), Error> {
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
                builder.push(handle);
                new_id
            }
        };

        write!(builder, "{} {} {}\n", start, end, id)?;
    }

    Ok(())
}

pub fn tree<B: Backend>(builder: &mut Builder<B>, object: &Tree<Handle<B>>) -> Result<(), Error> {
    let mut handles = HashMap::new();

    println!("Encoding tree...");

    for (name, reference) in &object.entries {
        let handle = reference.as_inner();
        let id = match handles.get(handle) {
            Some(&id) => id,
            None => {
                let new_id = handles.len();
                handles.insert(handle, new_id);
                builder.push(handle.clone());
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

        println!("{}", String::from_utf8_lossy(&buf));
    }

    Ok(())
}

// TODO: Robust RDF formatting/parsing - current breaks for non-ASCII strings:
// https://github.com/sdleffler/attaca/issues/25
pub fn commit<B: Backend>(
    builder: &mut Builder<B>,
    object: &Commit<Handle<B>>,
) -> Result<(), Error> {
    fn rdf_literal(s: &str) -> Vec<u8> {
        s.as_bytes()
            .iter()
            .cloned()
            .flat_map(ascii::escape_default)
            .collect::<Vec<_>>()
    }

    builder.push(object.subtree.as_inner().clone());
    for parent in &object.parents {
        builder.push(parent.as_inner().clone());
    }

    // The `0` is for metadata refs; N-triples metadata does not yet use refs (since it's just
    // author/message metadata) but it might eventually.
    write!(builder, "{} {}\n", object.parents.len(), 0)?;

    let mut ntriples = BTreeSet::new();

    if let Some(name) = object.author.name.as_ref() {
        let mut buf = Vec::new();
        write!(&mut buf, "_:author <{}> \"", FOAF_NAME)?;
        buf.write_all(&rdf_literal(name))?;
        write!(&mut buf, "\" .\n")?;
        ntriples.insert(buf);
    }

    if let Some(mbox) = object.author.mbox.as_ref() {
        let mut buf = Vec::new();
        write!(&mut buf, "_:author <{}> \"", FOAF_MBOX)?;
        buf.write_all(&rdf_literal(mbox))?;
        write!(&mut buf, "\" .\n")?;
        ntriples.insert(buf);
    }

    if let Some(message) = object.as_message() {
        let mut buf = Vec::new();
        write!(&mut buf, "_:this <{}> \"", ATTACA_COMMIT_MESSAGE)?;
        buf.write_all(&rdf_literal(message))?;
        write!(&mut buf, "\" .\n")?;
        ntriples.insert(buf);
    }

    {
        let mut buf = Vec::new();
        write!(&mut buf, "_:this <{}> \"", ATTACA_COMMIT_TIMESTAMP)?;
        buf.write_all(&rdf_literal(&object.as_timestamp().to_rfc2822()))?;
        write!(&mut buf, "\" .\n")?;
        ntriples.insert(buf);
    }

    for triple in ntriples {
        builder.write_all(&triple)?;
    }

    Ok(())
}
