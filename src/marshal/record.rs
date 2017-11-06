//! `record` - in-memory representations of objects which can be "deep" or "shallow".
//!
//! A "deep" record contains data which can be turned directly into an `Object`.
//!
//! A "shallow" record contains simply an object's hash and possibly metadata about the object.
//!
//! `Record`s are intended to be suitable for manipulation of objects' structures before and during
//! marshalling.

use arc_slice::ArcSlice;
use marshal::{ObjectHash, Object, SmallObject, LargeObject, DataObject, SubtreeObject,
              CommitObject};


/// A `Record` may either hold data (records representing small or large objects) or metadata
/// (records of subtrees or commits.) Data records contain additional information about the size of
/// the data contained in the objects they represent, while metadata records do not.
#[derive(Debug, Clone)]
pub enum Record {
    Data(DataRecord),
    Meta(MetaRecord),
}


impl From<ObjectHash> for Record {
    fn from(object_hash: ObjectHash) -> Self {
        Record::Meta(object_hash.into())
    }
}


impl From<SmallObject> for Record {
    fn from(small_object: SmallObject) -> Self {
        Record::Data(small_object.into())
    }
}


impl From<LargeObject> for Record {
    fn from(large_object: LargeObject) -> Self {
        Record::Data(large_object.into())
    }
}


impl From<SubtreeObject> for Record {
    fn from(subtree_object: SubtreeObject) -> Self {
        Record::Meta(subtree_object.into())
    }
}


impl From<CommitObject> for Record {
    fn from(commit_object: CommitObject) -> Self {
        Record::Meta(commit_object.into())
    }
}


impl From<SmallRecord> for Record {
    fn from(small_record: SmallRecord) -> Self {
        Record::Data(small_record.into())
    }
}


impl From<DataRecord> for Record {
    fn from(data_record: DataRecord) -> Self {
        Record::Data(data_record)
    }
}


impl From<MetaRecord> for Record {
    fn from(meta_record: MetaRecord) -> Self {
        Record::Meta(meta_record)
    }
}


impl Record {
    pub fn to_deep(self) -> Result<Object, ObjectHash> {
        match self {
            Record::Data(data) => data.to_deep().map(Object::Data),
            Record::Meta(meta) => meta.to_deep(),
        }
    }
}


/// A `SmallRecord` is a specialized record which holds information about a small object. This is
/// useful for representing things such as leaves of the tree formed by the large-object
/// small-object hierarchy.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum SmallRecord {
    Shallow(u64, ObjectHash),
    Deep(SmallObject),
}


impl From<ArcSlice> for SmallRecord {
    fn from(arc_slice: ArcSlice) -> Self {
        SmallRecord::Deep(SmallObject { chunk: arc_slice })
    }
}


impl SmallRecord {
    pub fn size(&self) -> u64 {
        match *self {
            SmallRecord::Shallow(sz, _) => sz,
            SmallRecord::Deep(ref small_object) => small_object.size(),
        }
    }
}


/// A `DataRecord` represents either a small or large object.
#[derive(Debug, Clone)]
pub enum DataRecord {
    Shallow(u64, ObjectHash),
    Deep(DataObject),
}


impl From<SmallRecord> for DataRecord {
    fn from(small: SmallRecord) -> Self {
        match small {
            SmallRecord::Shallow(sz, hash) => DataRecord::Shallow(sz, hash),
            SmallRecord::Deep(small_object) => DataRecord::Deep(DataObject::Small(small_object)),
        }
    }
}


impl From<SmallObject> for DataRecord {
    fn from(small_object: SmallObject) -> Self {
        DataRecord::Deep(DataObject::Small(small_object))
    }
}


impl From<LargeObject> for DataRecord {
    fn from(large_object: LargeObject) -> Self {
        DataRecord::Deep(DataObject::Large(large_object))
    }
}


impl From<DataObject> for DataRecord {
    fn from(data_object: DataObject) -> Self {
        DataRecord::Deep(data_object)
    }
}


impl DataRecord {
    pub fn size(&self) -> u64 {
        match *self {
            DataRecord::Shallow(sz, _) => sz,
            DataRecord::Deep(ref data) => data.size(),
        }
    }


    pub fn to_deep(self) -> Result<DataObject, ObjectHash> {
        match self {
            DataRecord::Shallow(_, hash) => Err(hash),
            DataRecord::Deep(data) => Ok(data),
        }
    }
}


/// A `MetaRecord` represents either a subtree object or a commit object.
#[derive(Debug, Clone)]
pub enum MetaRecord {
    Shallow(ObjectHash),

    Subtree(SubtreeObject),
    Commit(CommitObject),
}


impl From<ObjectHash> for MetaRecord {
    fn from(object_hash: ObjectHash) -> MetaRecord {
        MetaRecord::Shallow(object_hash)
    }
}


impl From<SubtreeObject> for MetaRecord {
    fn from(subtree_object: SubtreeObject) -> MetaRecord {
        MetaRecord::Subtree(subtree_object)
    }
}


impl From<CommitObject> for MetaRecord {
    fn from(commit_object: CommitObject) -> MetaRecord {
        MetaRecord::Commit(commit_object)
    }
}


impl MetaRecord {
    pub fn to_deep(self) -> Result<Object, ObjectHash> {
        match self {
            MetaRecord::Shallow(hash) => Err(hash),
            MetaRecord::Subtree(subtree) => Ok(Object::Subtree(subtree)),
            MetaRecord::Commit(commit) => Ok(Object::Commit(commit)),
        }
    }
}
