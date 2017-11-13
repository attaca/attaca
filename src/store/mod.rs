use futures::prelude::*;

use errors::*;
use marshal::{ObjectHash, Hashed, Object};

mod ceph;
mod empty;
mod local;

pub use self::ceph::Ceph;
pub use self::empty::Empty;
pub use self::local::Local;


pub trait RefStore: Send + Sync + Clone + 'static {
    type CompareAndSwap: Future<Item = ObjectHash, Error = Error> + Send;
    type Get: Future<Item = ObjectHash, Error = Error> + Send;

    fn compare_and_swap(&self, branch: String, prev_hash: ObjectHash, new_hash: ObjectHash) -> Self::CompareAndSwap;
    fn get(&self, branch: String) -> Self::Get;
}


pub trait ObjectStore: Send + Sync + Clone + 'static {
    type Read: Future<Item = Object, Error = Error> + Send;
    type Write: Future<Item = bool, Error = Error> + Send;

    fn read_object(&self, object_hash: ObjectHash) -> Self::Read;
    fn write_object(&self, hashed: Hashed) -> Self::Write;
}


pub enum RemoteRead {
    Ceph(<Ceph as ObjectStore>::Read),
}


impl Future for RemoteRead {
    type Item = Object;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            RemoteRead::Ceph(ref mut ceph) => ceph.poll(),
        }
    }
}


pub enum RemoteWrite {
    Ceph(<Ceph as ObjectStore>::Write),
}


impl Future for RemoteWrite {
    type Item = bool;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            RemoteWrite::Ceph(ref mut ceph) => ceph.poll(),
        }
    }
}


#[derive(Clone)]
pub enum Remote {
    Ceph(Ceph),
}


impl ObjectStore for Remote {
    type Read = RemoteRead;
    type Write = RemoteWrite;

    fn read_object(&self, object_hash: ObjectHash) -> Self::Read {
        match *self {
            Remote::Ceph(ref ceph) => RemoteRead::Ceph(ceph.read_object(object_hash)),
        }
    }

    fn write_object(&self, hashed: Hashed) -> Self::Write {
        match *self {
            Remote::Ceph(ref ceph) => RemoteWrite::Ceph(ceph.write_object(hashed)),
        }
    }
}
