use std::{sync::{Arc, RwLock}};

use attaca::ObjectPath;
use leveldb::database::Database;
use smallvec::SmallVec;

use db::Key;

pub struct ChangeSet {
    db: Arc<RwLock<Database<Key>>>,
}

impl From<Arc<RwLock<Database<Key>>>> for ChangeSet {
    fn from(db: Arc<RwLock<Database<Key>>>) -> Self {
        Self { db }
    }
}

impl ChangeSet {
    pub fn add(&mut self, path: &ObjectPath) -> Result<(), Error> {
        let key = Key::changeset(path);
        db.read().put(WriteOptions::new(), &key, unimplemented!())?;
        Ok(())
    }
}
