use attaca::{Handle, HandleDigest, Store, digest::Digest, object::{ObjectRef, TreeRef},
             path::ObjectPath};
use failure::*;
use futures::{future, prelude::*, stream::FuturesUnordered};
use itertools::{EitherOrBoth, Itertools};

use {Repository, State};

#[derive(Debug, Clone)]
pub enum Change {
    Added(ObjectPath),
    Modified(ObjectPath),
    Removed(ObjectPath),
}

#[async_stream(item = self::Change)]
fn compare_subtrees<H: Handle>(
    head_ref: TreeRef<H>,
    candidate_ref: TreeRef<H>,
) -> Result<(), Error> {
    let mut queue = FuturesUnordered::new();
    queue.push(future::ok(ObjectPath::new()).join3(head_ref.fetch(), candidate_ref.fetch()));

    // This is a `while let` and not a `#[async] for` because it is necessary to push new elements
    // into the queue.
    while let (Some((path, head_st, candidate_st)), new_queue) =
        await!(queue.into_future().map_err(|(err, _)| err))?
    {
        queue = new_queue;

        let merged = Itertools::merge_join_by(
            head_st.into_iter(),
            candidate_st.into_iter(),
            |head, cand| head.0.cmp(&cand.0),
        );

        for either_or_both in merged {
            // Three cases:
            // 1. Only the HEAD contains the path.
            // 2. Only the candidate contains the path.
            // 3. Both HEAD and the candidate contain the path.
            match either_or_both {
                EitherOrBoth::Left((name, _)) => {
                    stream_yield!(Change::Removed(path.push_back(name)));
                }
                EitherOrBoth::Right((name, cand_ref)) => {
                    stream_yield!(Change::Added(path.push_back(name)));
                }
                EitherOrBoth::Both((name, head_ref), (_, cand_ref)) => {
                    let child_path = path.push_back(name);
                    // Four cases:
                    // 1. HEAD entry is a subtree and candidate entry is a subtree.
                    //    In this case there is not yet any obvious modification,
                    //    and we recurse down both sides, adding the trees to the
                    //    queue.
                    // 2. HEAD entry is a subtree and candidate entry is a file. In
                    //    this case, the candidate entry is added, while the HEAD
                    //    entry is added.
                    // 3. HEAD entry is a file and candidate entry is a subtree. In
                    //    this case, the HEAD entry is removed, and the candidate
                    //    entry is added.
                    match (head_ref, cand_ref) {
                        // Early exit on two commits: it is impossible for the HEAD subtree and/or
                        // candidate subtree to be commits.
                        (ObjectRef::Commit(_), _) | (_, ObjectRef::Commit(_)) => unreachable!(),

                        (ObjectRef::Tree(head_tree), ObjectRef::Tree(cand_tree)) => {
                            queue.push(
                                future::ok(child_path).join3(head_tree.fetch(), cand_tree.fetch()),
                            );
                        }
                        (head_not_tree, ObjectRef::Tree(cand_tree)) => {
                            let tree_future = cand_tree.fetch();
                            stream_yield!(Change::Removed(child_path.clone()));

                            for (name, _) in await!(tree_future)?.into_iter() {
                                stream_yield!(Change::Added(child_path.push_back(name)));
                            }
                        }
                        (ObjectRef::Tree(head_tree), cand_not_tree) => {
                            let tree_future = head_tree.fetch();
                            stream_yield!(Change::Added(child_path.clone()));

                            for (name, _) in await!(tree_future)?.into_iter() {
                                stream_yield!(Change::Removed(child_path.push_back(name)));
                            }
                        }
                        (head_not_tree, cand_not_tree) => {
                            if head_not_tree != cand_not_tree {
                                stream_yield!(Change::Modified(child_path));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

impl<S: Store, D: Digest> Repository<S, D>
where
    S::Handle: HandleDigest<D>,
{
    pub fn staged_changes(&self) -> impl Stream<Item = Change, Error = Error> {
        let state_res = self.get_state();

        async_stream_block! {
            let state = state_res?;
            // We have four possible cases:
            // 1. Both HEAD and the candidate tree are `Some`. In this case we take the
            //    union of all paths in both the HEAD tree and candidate tree; then, we look at
            //    whether or not the HEAD tree contains an element and the candidate tree does not
            //    (in which case it is a candidate for removal), or if the candidate tree contains
            //    an element and the HEAD tree does not (in which case it is newly inserted), and
            //    lastly if both the HEAD and candidate trees contain the element (in which case
            //    the element is possibly modified or simply unchanged.)
            // 2. Both HEAD and the candidate tree are `None`. In this case there are no changes
            //    staged for commit.
            // 3. HEAD is `Some` but the candidate tree is `None`. This case is absurd.
            // 4. The candidate tree is `Some` but HEAD is `None`. In this case, everything in the
            //    candidate tree is newly added.
            match (state.head, state.candidate) {
                (Some(head_ref), Some(candidate_ref)) => {
                    let head_subtree = await!(head_ref.fetch())?.as_subtree().clone();

                    #[async]
                    for change in compare_subtrees(head_subtree, candidate_ref) {
                        stream_yield!(change);
                    }

                    Ok(())
                }
                (None, Some(candidate_ref)) => {
                    let candidate_tree = await!(candidate_ref.fetch())?;

                    for (name, _) in candidate_tree.into_iter() {
                        stream_yield!(Change::Added(ObjectPath::new().push_front(name)));
                    }

                    Ok(())
                }
                (None, None) => Ok(()),
                (Some(_), None) => bail!("None tree with left HEAD!"),
            }
        }
    }
}
