use std::{cmp::Ordering, collections::{BTreeMap, BTreeSet}, path::{Path, PathBuf}, sync::Arc};

use attaca::{Handle, HandleDigest, Store,
             batch::{Batch as ObjectBatch, Operation as ObjectOperation}, digest::Digest,
             hierarchy::Hierarchy, object::{ObjectRef, TreeBuilder, TreeRef}, path::ObjectPath};
use failure::Error;
use futures::{stream, future::Either, prelude::*};
use itertools::Itertools;
use sequence_trie::SequenceTrie;

use {Repository, State};

#[derive(Debug, Clone, Copy)]
pub enum OpKind {
    Stage,
    Unstage,
}

#[derive(Debug, Clone)]
pub struct BatchOp {
    path: PathBuf,
    op: OpKind,
}

#[derive(Debug, Clone)]
pub struct Batch {
    ops: Vec<BatchOp>,
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn process<'r, P: AsRef<Path>>(
        &'r self,
        path: &P,
    ) -> impl Future<Item = Option<ObjectRef<S::Handle>>, Error = Error> {
        async_block! {
            Ok(unimplemented!())
        }
    }

    fn process_operation<'r>(
        &'r self,
        hierarchy: Hierarchy<S::Handle>,
        batch_op: BatchOp,
    ) -> impl Future<Item = ObjectOperation<S::Handle>, Error = Error> {
        let BatchOp { path, op } = batch_op;
        let future_res = ObjectPath::from_path(&path).map(|object_path| {
            let future = match op {
                OpKind::Unstage => Either::A(hierarchy.get(object_path.clone())),
                OpKind::Stage => Either::B(self.process(&path)),
            };
            future.map(|objref_opt| (object_path, objref_opt))
        });
        async_block! {
            let (object_path, objref_opt) = await!(future_res?)?;
            let operation = match objref_opt {
                Some(objref) => ObjectOperation::Add(object_path, objref),
                None => ObjectOperation::Delete(object_path),
            };
            Ok(operation)
        }
    }

    pub fn stage<'r>(&'r mut self, batch: Batch) -> impl Future<Item = (), Error = Error> + 'r {
        async_block! {
            let state = self.get_state()?;
            let hierarchy = match state.head {
                Some(head_ref) => Hierarchy::from(await!(head_ref.fetch())?.as_subtree().clone()),
                None => Hierarchy::new(),
            };
            let queue = stream::futures_ordered(
                batch
                    .ops
                    .into_iter()
                    .map(|batch_op| self.process_operation(hierarchy.clone(), batch_op)),
            );
            let batch: ObjectBatch<S::Handle> =
                await!(queue.fold(ObjectBatch::new(), |batch, op| batch.add(op)))?;
            await!(self.stage_objects(batch))?;

            Ok(())
        }
    }

    pub fn stage_objects<'r>(
        &'r mut self,
        batch: ObjectBatch<S::Handle>,
    ) -> impl Future<Item = (), Error = Error> + 'r {
        async_block! {
            let state = self.get_state()?;
            let tree_builder = match state.candidate.clone() {
                Some(candidate_ref) => await!(candidate_ref.fetch())?.diverge(),
                None => TreeBuilder::new(),
            };

            let new_candidate_built = await!(batch.run(self.store.clone(), tree_builder))?;
            let candidate = if new_candidate_built.is_empty() && state.head.is_none() {
                None
            } else {
                Some(await!(new_candidate_built.as_tree().send(&self.store))?)
            };

            self.set_state(&State { candidate, ..state })?;

            Ok(())
        }
    }
}
