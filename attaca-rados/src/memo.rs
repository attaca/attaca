use std::{fmt, cell::Cell, hash::Hash, sync::{Arc, Weak}};

use chashmap::CHashMap;
use failure::{Compat, Error, Fail};
use futures::{future::{Shared, SharedError}, prelude::*};

pub struct FutureWaiting<K: Hash + Eq, F: Future>
where
    F::Item: Clone,
{
    key: K,
    weak_map: Weak<CHashMap<K, FutureValue<K, F>>>,
    future: F,
}

impl<K: Hash + Eq + fmt::Debug, F: Future + fmt::Debug> fmt::Debug for FutureWaiting<K, F>
where
    F::Item: Clone + fmt::Debug,
    F::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FutureWaiting")
            .field("key", &self.key)
            .field("weak_map", &self.weak_map)
            .field("future", &self.future)
            .finish()
    }
}

impl<K: Hash + Eq, F: Future> Future for FutureWaiting<K, F>
where
    F::Item: Clone,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let item = match self.future.poll()? {
            Async::Ready(item) => item,
            Async::NotReady => return Ok(Async::NotReady),
        };

        if let Some(map) = Weak::upgrade(&self.weak_map) {
            *map.get_mut(&self.key).unwrap() = FutureValue::Done(item.clone());
        }

        Ok(Async::Ready(item))
    }
}

pub enum FutureValue<K: Hash + Eq, F: Future>
where
    F::Item: Clone,
{
    Waiting(Shared<FutureWaiting<K, F>>),
    Done(F::Item),
}

impl<K: Hash + Eq + fmt::Debug, F: Future + fmt::Debug> fmt::Debug for FutureValue<K, F>
where
    F::Item: Clone + fmt::Debug,
    F::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FutureValue::Waiting(ref shared) => f.debug_tuple("Waiting").field(shared).finish(),
            FutureValue::Done(ref item) => f.debug_tuple("Done").field(item).finish(),
        }
    }
}

impl<K: Hash + Eq, F: Future> Clone for FutureValue<K, F>
where
    F::Item: Clone,
{
    fn clone(&self) -> Self {
        match *self {
            FutureValue::Waiting(ref shared) => FutureValue::Waiting(shared.clone()),
            FutureValue::Done(ref item) => FutureValue::Done(item.clone()),
        }
    }
}

impl<K: Hash + Eq, F: Future> Future for FutureValue<K, F>
where
    F::Item: Clone,
{
    type Item = F::Item;
    type Error = SharedError<F::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FutureValue::Waiting(ref mut shared) => {
                let item = match shared.poll()? {
                    Async::Ready(item) => (*item).clone(),
                    Async::NotReady => return Ok(Async::NotReady),
                };

                *self = FutureValue::Done(item.clone());
                Ok(Async::Ready(item))
            }
            FutureValue::Done(ref item) => Ok(Async::Ready(item.clone())),
        }
    }
}

pub struct FutureMap<K: Hash + Eq, F: Future>
where
    F::Item: Clone,
{
    inner: Arc<CHashMap<K, FutureValue<K, F>>>,
}

impl<K: Hash + Eq + fmt::Debug, F: Future + fmt::Debug> fmt::Debug for FutureMap<K, F>
where
    F::Item: Clone + fmt::Debug,
    F::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FutureMap")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<K: Hash + Eq, F: Future> Clone for FutureMap<K, F>
where
    F::Item: Clone,
    F::Error: fmt::Debug,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K: Hash + Eq + Copy, F: Future> FutureMap<K, F>
where
    F::Item: Clone,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CHashMap::new()),
        }
    }

    pub fn get_or_insert<G: FnOnce() -> F>(&self, key: K, thunk: G) -> FutureValue<K, F> {
        let output = Cell::new(None);

        self.inner.upsert(
            key,
            || {
                let fv = FutureValue::Waiting(
                    FutureWaiting {
                        key,
                        weak_map: Arc::downgrade(&self.inner),
                        future: thunk(),
                    }.shared(),
                );

                output.set(Some(fv.clone()));
                fv
            },
            |fv| {
                output.set(Some(fv.clone()));
            },
        );

        output.into_inner().unwrap()
    }
}
