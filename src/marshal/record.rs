//! `record` - in-memory representations of objects which can be "deep" or "shallow".
//!
//! A "deep" record contains data which can be turned directly into an `Object`.
//!
//! A "shallow" record contains simply an object's hash and possibly metadata about the object.
//!
//! `Record`s are intended to be suitable for manipulation of objects' structures before and during
//! marshalling.

use marshal::{ObjectHash, Object, SmallObject, LargeObject, DataObject, SubtreeObject,
              CommitObject};


/// A `Record` may either hold data (records representing small or large objects) or metadata
/// (records of subtrees or commits.) Data records contain additional information about the size of
/// the data contained in the objects they represent, while metadata records do not.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Record<'a> {
    Data(DataRecord<'a>),
    Meta(MetaRecord<'a>),
}


impl<'a> From<SmallObject<'a>> for Record<'a> {
    fn from(small_object: SmallObject<'a>) -> Self {
        Record::Data(small_object.into())
    }
}


impl<'a> From<LargeObject<'a>> for Record<'a> {
    fn from(large_object: LargeObject<'a>) -> Self {
        Record::Data(large_object.into())
    }
}


impl<'a> From<SmallRecord<'a>> for Record<'a> {
    fn from(small_record: SmallRecord<'a>) -> Self {
        Record::Data(small_record.into())
    }
}


impl<'a> From<DataRecord<'a>> for Record<'a> {
    fn from(data_record: DataRecord<'a>) -> Self {
        Record::Data(data_record)
    }
}


impl<'a> From<MetaRecord<'a>> for Record<'a> {
    fn from(meta_record: MetaRecord<'a>) -> Self {
        Record::Meta(meta_record)
    }
}


impl<'a> Record<'a> {
    pub fn to_deep(self) -> Result<Object<'a>, ObjectHash> {
        match self {
            Record::Data(data) => data.to_deep().map(Object::Data),
            Record::Meta(meta) => meta.to_deep(),
        }
    }
}


/// A `SmallRecord` is a specialized record which holds information about a small object. This is
/// useful for representing things such as leaves of the tree formed by the large-object
/// small-object hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SmallRecord<'a> {
    Shallow(u64, ObjectHash),
    Deep(SmallObject<'a>),
}


impl<'a> SmallRecord<'a> {
    pub fn size(&self) -> u64 {
        match *self {
            SmallRecord::Shallow(sz, _) => sz,
            SmallRecord::Deep(ref small_object) => small_object.size(),
        }
    }
}


/// A `DataRecord` represents either a small or large object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataRecord<'a> {
    Shallow(u64, ObjectHash),
    Deep(DataObject<'a>),
}


impl<'a> From<SmallRecord<'a>> for DataRecord<'a> {
    fn from(small: SmallRecord<'a>) -> Self {
        match small {
            SmallRecord::Shallow(sz, hash) => DataRecord::Shallow(sz, hash),
            SmallRecord::Deep(small_object) => DataRecord::Deep(DataObject::Small(small_object)),
        }
    }
}


impl<'a> From<SmallObject<'a>> for DataRecord<'a> {
    fn from(small_object: SmallObject<'a>) -> Self {
        DataRecord::Deep(DataObject::Small(small_object))
    }
}


impl<'a> From<LargeObject<'a>> for DataRecord<'a> {
    fn from(large_object: LargeObject<'a>) -> Self {
        DataRecord::Deep(DataObject::Large(large_object))
    }
}


impl<'a> From<DataObject<'a>> for DataRecord<'a> {
    fn from(data_object: DataObject<'a>) -> Self {
        DataRecord::Deep(data_object)
    }
}


impl<'a> DataRecord<'a> {
    pub fn size(&self) -> u64 {
        match *self {
            DataRecord::Shallow(sz, _) => sz,
            DataRecord::Deep(ref data) => data.size(),
        }
    }


    pub fn to_deep(self) -> Result<DataObject<'a>, ObjectHash> {
        match self {
            DataRecord::Shallow(_, hash) => Err(hash),
            DataRecord::Deep(data) => Ok(data),
        }
    }
}


/// A `MetaRecord` represents either a subtree object or a commit object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaRecord<'a> {
    Shallow(ObjectHash),

    Subtree(SubtreeObject<'a>),
    Commit(CommitObject<'a>),
}


impl<'a> From<ObjectHash> for MetaRecord<'a> {
    fn from(object_hash: ObjectHash) -> MetaRecord<'a> {
        MetaRecord::Shallow(object_hash)
    }
}


impl<'a> From<SubtreeObject<'a>> for MetaRecord<'a> {
    fn from(subtree_object: SubtreeObject<'a>) -> MetaRecord<'a> {
        MetaRecord::Subtree(subtree_object)
    }
}


impl<'a> From<CommitObject<'a>> for MetaRecord<'a> {
    fn from(commit_object: CommitObject<'a>) -> MetaRecord<'a> {
        MetaRecord::Commit(commit_object)
    }
}


impl<'a> MetaRecord<'a> {
    pub fn to_deep(self) -> Result<Object<'a>, ObjectHash> {
        match self {
            MetaRecord::Shallow(hash) => Err(hash),
            MetaRecord::Subtree(subtree) => Ok(Object::Subtree(subtree)),
            MetaRecord::Commit(commit) => Ok(Object::Commit(commit)),
        }
    }
}
