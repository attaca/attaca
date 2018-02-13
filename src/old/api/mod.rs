mod atomic_weak;
mod context;
mod marshal;
mod object;
mod store;

// use std::collections::BTreeMap;
// use std::{mem, sync::Arc};
// 
// use failure::Error;
// use futures::{prelude::*, stream::FuturesUnordered};
// 
// #[derive(Debug, Clone)]
// pub struct Digest;
// 
// #[derive(Debug, Clone)]
// pub struct Object;
// 
// #[derive(Debug, Clone)]
// pub enum ObjectContent {
//     Data(DataContent),
//     Subtree(SubtreeContent),
//     Commit(CommitContent),
// }
// 
// #[derive(Debug, Clone)]
// pub struct DataContent;
// 
// impl DataContent {
//     pub fn size(&self) -> u64 {
//         unimplemented!()
//     }
// }
// 
// #[derive(Debug, Clone)]
// pub struct SubtreeContent {
//     pub entries: BTreeMap<String, SubtreeChild>,
// }
// 
// #[derive(Debug, Clone)]
// pub enum SubtreeChild {
//     File { size: u64, object: Object },
//     Directory { object: Object },
// }
// 
// #[derive(Debug, Clone)]
// pub struct CommitContent {
//     pub subtree: Object,
//     pub parents: Vec<Object>,
// }
// 
// #[derive(Debug, Clone)]
// pub struct Context;
// 
// #[derive(Debug, Clone)]
// pub struct Repository;
// 
// #[derive(Debug, Clone)]
// pub struct Workspace;
// 
// #[derive(Debug)]
// pub struct ObjectFuture;
// 
// impl Context {
//     pub fn new() -> Self {
//         unimplemented!()
//     }
// 
//     pub fn connect(&self, url: &str) -> Repository {
//         unimplemented!()
//     }
// 
//     pub fn open(&self, url: &str) -> Workspace {
//         unimplemented!()
//     }
// 
//     pub fn data<R: Read>(&self, reader: R) -> ObjectFuture {
//         unimplemented!()
//     }
// 
//     pub fn subtree(&self, content: SubtreeContent) -> ObjectFuture {
//         unimplemented!()
//     }
// 
//     pub fn commit(&self, content: CommitContent) -> ObjectFuture {
//         unimplemented!()
//     }
// }
// 
// impl AsRef<Repository> for Workspace {
//     fn as_ref(&self) -> &Repository {
//         unimplemented!()
//     }
// }
// 
// #[derive(Debug)]
// pub struct PutFuture;
// 
// impl Future for PutFuture {
//     type Item = ();
//     type Error = Error;
// 
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         unimplemented!()
//     }
// }
// 
// #[derive(Debug)]
// pub struct GetFuture;
// 
// impl Future for GetFuture {
//     type Item = Object;
//     type Error = Error;
// 
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         unimplemented!()
//     }
// }
// 
// #[derive(Debug)]
// pub struct ResolveFuture;
// 
// impl Future for ResolveFuture {
//     type Item = Object;
//     type Error = Error;
// 
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         unimplemented!()
//     }
// }
// 
// impl Repository {
//     pub fn put(&self, tag: &str, object: &Object) -> PutFuture {
//         unimplemented!()
//     }
// 
//     pub fn get(&self, tag: &str) -> GetFuture {
//         unimplemented!()
//     }
// 
//     pub fn resolve(&self, hash: &Digest) -> ResolveFuture {
//         unimplemented!()
//     }
// }
// 
// // pub enum PushFuture<S0: Store, S1: Store> {
// //     Transferring {
// //         queue: FuturesUnordered<<S0 as ObjectStore>::Read>,
// //
// //         source: S0,
// //
// //         target: S1,
// //         target_branch: String,
// //     },
// //     Updating(<S1 as RefStore>::CompareAndSwap),
// //     Invalid,
// // }
// //
// // impl<S0: Store, S1: Store> Future for PushFuture<S0, S1> {
// //     type Item = ();
// //     type Error = Error;
// //
// //     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
// //         match mem::replace(self, PushFuture::Invalid) {
// //             PushFuture::Transferring { queue, source, target, target_branch } => {
// //                 match queue.poll()? {
// //                     Ok(Async::Ready(Some(object))) => {
// //                         match object {
// //
// //                         }
// //                     }
// //                     Err(error) => return Err(error),
// //                 }
// //             }
// //         }
// //     }
// // }
