use failure::Error;
use futures::prelude::*;

use arc_slice::ArcSlice;
use hasher::Hashed;
use object::Object;
use object_hash::ObjectHash;

mod ceph;
mod empty;
mod fs;
// mod ssh;

pub use self::ceph::{Ceph, CephCfg};
pub use self::empty::Empty;
pub use self::fs::{FileSystem, FileSystemCfg};


pub trait ObjectStore: Send + Sync + Clone + 'static {
    type ReadHashed: Future<Item = Option<ArcSlice>, Error = Error> + Send;
    type WriteHashed: Future<Item = bool, Error = Error> + Send;

    fn read_hashed(&self, object_hash: ObjectHash) -> Self::ReadHashed;
    fn write_hashed(&self, object_hash: ObjectHash, bytes: ArcSlice) -> Self::WriteHashed;
}


pub trait MetadataStore: Send + Sync + Clone + 'static {
    fn compare_and_swap_branch(
        &self,
        branch: String,
        prev_hash: Option<ObjectHash>,
        new_hash: ObjectHash,
    ) -> Result<Option<ObjectHash>, Error>;
    fn get_branch(&self, branch: String) -> Result<Option<ObjectHash>, Error>;
}


// #[derive(Clone)]
// pub struct Compose<O: ObjectStore, M: MetadataStore> {
//     objects: O,
//     metadata: M,
// }
//
//
// impl<O: ObjectStore, M: MetadataStore> Repository for Compose<O, M> {
//     type ReadHashed = O::ReadHashed;
//     type WriteHashed = O::WriteHashed;
//
//     fn read_object(&self, object_hash: ObjectHash) -> Self::ReadHashed {
//         self.objects.read_object(object_hash)
//     }
//
//     fn write_object(&self, hashed: Hashed) -> Self::WriteHashed {
//         self.objects.write_object(hashed)
//     }
//
//     fn compare_and_swap_branch(
//         &self,
//         branch: String,
//         prev_hash: Option<ObjectHash>,
//         new_hash: ObjectHash,
//     ) -> Result<Option<ObjectHash>, Error> {
//         self.metadata
//             .compare_and_swap_branch(branch, prev_hash, new_hash)
//     }
//
//     fn get_branch(&self, branch: String) -> Result<Option<ObjectHash>, Error> {
//         self.metadata.get_branch(branch)
//     }
// }


pub trait Repository: Send + Sync + Clone + 'static {
    type ReadHashed: Future<Item = Option<ArcSlice>, Error = Error> + Send;
    type WriteHashed: Future<Item = bool, Error = Error> + Send;

    fn read_hashed(&self, object_hash: ObjectHash) -> Self::ReadHashed;
    fn write_hashed(&self, object_hash: ObjectHash, bytes: ArcSlice) -> Self::WriteHashed;

    fn read_object(&self, object_hash: ObjectHash) -> ReadObject<<Self as Repository>::ReadHashed> {
        ReadObject {
            future: self.read_hashed(object_hash),
        }
    }

    fn compare_and_swap_branch(
        &self,
        branch: String,
        prev_hash: Option<ObjectHash>,
        new_hash: ObjectHash,
    ) -> Result<Option<ObjectHash>, Error>;
    fn get_branch(&self, branch: String) -> Result<Option<ObjectHash>, Error>;
}


pub struct ReadObject<F: Future<Item = Option<ArcSlice>, Error = Error>> {
    future: F,
}


impl<F: Future<Item = Option<ArcSlice>, Error = Error>> Future for ReadObject<F> {
    type Item = Option<Object>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.future.poll() {
            Ok(Async::Ready(Some(arc_slice))) => {
                Object::from_bytes(&arc_slice).map(Some).map(Async::Ready)
            }
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepositoryCfg {
    Ceph(CephCfg),
    Empty,
    FileSystem(FileSystemCfg),
}


impl RepositoryCfg {
    pub fn open(self) -> Result<AnyRepository, Error> {
        match self {
            RepositoryCfg::Ceph(ceph_cfg) => Ceph::open(ceph_cfg).map(AnyRepository::Ceph),
            RepositoryCfg::Empty => Ok(AnyRepository::Empty(Empty)),
            RepositoryCfg::FileSystem(fs_cfg) => {
                FileSystem::open(fs_cfg).map(AnyRepository::FileSystem)
            }
        }
    }
}


#[derive(Clone)]
pub enum AnyRepository {
    Ceph(Ceph),
    Empty(Empty),
    FileSystem(FileSystem),
}
