use std::collections::hash_map::{Entry, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, TryLockError};

use failure::{self, Error};
use futures::future::Shared;
use futures::prelude::*;
use futures::sync::oneshot;

use arc_slice::ArcSlice;
use object_hash::ObjectHash;


#[derive(Debug)]
enum Location {
    Locked(Shared<oneshot::Receiver<ArcSlice>>),
    Unlocked(ArcSlice),
}


#[derive(Clone)]
pub struct LockMap {
    inner: Arc<Mutex<HashMap<ObjectHash, Location>>>,
}


impl fmt::Debug for LockMap {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.inner.try_lock() {
            Ok(guard) => guard.fmt(fmt),
            Err(TryLockError::Poisoned(err)) => {
                write!(fmt, "{{ Poisoned({:?}) }}", &**err.get_ref())
            }
            Err(TryLockError::WouldBlock) => write!(fmt, "{{ <locked> }}"),
        }
    }
}


impl LockMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn contains_key(&self, key: ObjectHash) -> bool {
        self.inner.lock().unwrap().contains_key(&key)
    }

    pub fn get(&self, key: ObjectHash) -> Option<Value> {
        self.inner
            .lock()
            .unwrap()
            .get(&key)
            .map(|location| match *location {
                Location::Locked(ref shared) => Value::Waiting(shared.clone()),
                Location::Unlocked(ref object) => Value::Done(Some(object.clone())),
            })
    }

    pub fn get_or_lock(&self, key: ObjectHash) -> Result<Value, Lock> {
        match self.inner.lock().unwrap().entry(key) {
            Entry::Occupied(occupied) => match occupied.get() {
                &Location::Locked(ref shared) => Ok(Value::Waiting(shared.clone())),
                &Location::Unlocked(ref object) => Ok(Value::Done(Some(object.clone()))),
            },
            Entry::Vacant(vacant) => {
                let (tx, rx) = oneshot::channel();
                vacant.insert(Location::Locked(rx.shared()));

                Err(Lock {
                    map: self.clone(),
                    key,
                    tx,
                })
            }
        }
    }
}


pub enum Value {
    Waiting(Shared<oneshot::Receiver<ArcSlice>>),
    Done(Option<ArcSlice>),
}


impl Future for Value {
    type Item = ArcSlice;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let object = match *self {
            Value::Waiting(ref mut shared) => {
                match shared.poll().map_err(|shared_err| (&*shared_err).clone())? {
                    Async::Ready(object) => (&*object).clone(),
                    Async::NotReady => return Ok(Async::NotReady),
                }
            }
            Value::Done(ref mut object_opt) => object_opt.take().unwrap(),
        };
        *self = Value::Done(None);

        Ok(Async::Ready(object))
    }
}


pub struct Lock {
    map: LockMap,
    key: ObjectHash,
    tx: oneshot::Sender<ArcSlice>,
}


impl Lock {
    pub fn fill(self, object: ArcSlice) -> Result<(), Error> {
        self.map
            .inner
            .lock()
            .unwrap()
            .insert(self.key, Location::Unlocked(object.clone()));

        self.tx
            .send(object)
            .map_err(|_| failure::err_msg("LockMap dropped!"))
    }
}
