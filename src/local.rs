//! `local` - operate on the locally stored files and blobs of a given repository.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::mem;
use std::rc::Rc;

use bincode;
use memmap::{Mmap, Protection};

use errors::*;
use marshal::{ObjectHash, Object};
use repository::Repository;


/// The type of a locally stored object.
#[derive(Debug, Clone)]
pub struct LocalObject {
    mmap: Rc<Mmap>,
    object: Object<'static>,
}


impl<'obj> AsRef<Object<'obj>> for LocalObject {
    fn as_ref(&self) -> &Object<'obj> {
        &self.object
    }
}


/// The type of a local object store.
// TODO: Store `Weak` references to `Mmap`s in `Local` so that we cut down on the number of file
// descriptors that our process owns.
#[derive(Debug)]
pub struct Local {
    repository: Rc<Repository>,

    objects: RefCell<HashMap<ObjectHash, LocalObject>>,
}


impl Local {
    pub fn new(repository: Rc<Repository>) -> Local {
        Local {
            repository,
            objects: RefCell::new(HashMap::new()),
        }
    }


    /// Write an object to the file system. This will open and then close a file.
    pub fn write_object(&self, object_hash: &ObjectHash, object: &Object) -> Result<()> {
        if self.objects.borrow().contains_key(object_hash) {
            return Ok(());
        }

        let path = self.repository.blob_path.join(object_hash.to_path());

        if !path.is_file() {
            let dir_path = {
                let mut dp = path.clone();
                dp.pop();
                dp
            };

            fs::create_dir_all(dir_path)?;

            let mut file = BufWriter::new(File::create(path)?);
            bincode::serialize_into(&mut file, object, bincode::Infinite)?;
        }

        Ok(())
    }


    /// Load an object from the file system. This will open a file if the object has not already
    /// been loaded.
    pub fn read_object<'local>(&'local self, object_hash: &ObjectHash) -> Result<LocalObject> {
        if let Some(local) = self.objects.borrow().get(object_hash) {
            return Ok(local.clone());
        }

        let path = self.repository.blob_path.join(object_hash.to_path());
        let mmap = Mmap::open_path(path, Protection::Read)?;
        let object =
            Object::from_bytes(unsafe { mem::transmute::<&[u8], &[u8]>(mmap.as_slice()) })?;

        let local = LocalObject {
            mmap: Rc::new(mmap),
            object,
        };

        self.objects.borrow_mut().insert(
            *object_hash,
            local.clone(),
        );

        return Ok(local);
    }
}
