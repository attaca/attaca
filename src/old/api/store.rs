use std::{fmt::Debug, sync::Arc};

use failure::Error;
use futures::prelude::*;

use api::object::{Content, Digest};

pub trait Store: Debug {
    fn get(&self, digest: &Digest) -> Box<Future<Item = Content, Error = Error>>;
    fn put(&self, content: &Arc<Content>) -> Box<Future<Item = (), Error = Error>>;
}
