//! `marshal` - marshal chunked files and commits into a format which can be uploaded to the store.
//!
//! Important pieces of functionality in this module include:
//!
//! * Compute the hashes of chunks.
//! * Insert chunks into subtree/large-file nodes.
//! * Deduplicate chunks.

//pub mod data_tree;
pub mod dir_tree;
pub mod marshaller;
pub mod object;
pub mod record;


pub use self::dir_tree::DirTree;
pub use self::marshaller::{ObjectHash, Marshaller, Hashed};
pub use self::object::{Object, SmallObject, LargeObject, DataObject, SubtreeObject, CommitObject};
pub use self::record::{Record, DataRecord, MetaRecord, SmallRecord};
