use std::{fmt, sync::Arc};

use failure::Error;
use futures::prelude::*;

pub struct TestResult {
    name: Arc<String>,
    result: Result<(), Error>,
}

impl fmt::Display for TestResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.result {
            Ok(()) => f.debug_tuple("Pass").finish(),
            Err(ref err) => f.debug_tuple("Fail").field(&self.name).field(err).finish(),
        }
    }
}

impl TestResult {
    pub fn is_failure(&self) -> bool {
        self.result.is_err()
    }
}

pub struct Test<'a> {
    name: Arc<String>,
    inner: Box<Future<Item = (), Error = Error> + 'a>,
}

impl<'a> Future for Test<'a> {
    type Item = TestResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = match self.inner.poll() {
            Ok(Async::Ready(ok)) => Ok(ok),
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(err) => Err(err),
        };

        Ok(Async::Ready(TestResult {
            name: self.name.clone(),
            result,
        }))
    }
}

impl<'a> Test<'a> {
    pub fn new<S, F>(name: S, future: F) -> Self
    where
        S: Into<String>,
        F: Future<Item = (), Error = Error> + 'a,
    {
        Self {
            name: Arc::new(name.into()),
            inner: Box::new(future),
        }
    }
}
