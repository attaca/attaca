use std::io::{self, Read};

use attaca::{HandleDigest, Store, digest::Digest,
             object::{self, LargeBuilder, ObjectRef, SmallBuilder}, split::{Parameters, Splitter}};
use failure::Error;
use futures::{prelude::*, stream::{self, FuturesOrdered}};

pub fn share_and_resolve<S, R, D>(store: S, reader: R) -> impl Future<Item = (), Error = Error>
where
    S: Store,
    R: Read + 'static,
    D: Digest,
    S::Handle: HandleDigest<D>,
{
    async_block! {
        let reference = await!(object::share(reader, store.clone()))?;
        let digest = await!(reference.as_handle().digest())?;
        let resolved = await!(store.resolve(&digest))?;

        ensure!(
            resolved.as_ref() == Some(reference.as_handle()),
            "Test failure: share'd object's digest did not resolve to the same reference!"
        );
        Ok(())
    }
}

pub fn send_and_fetch_small<S: Store, R: Read + 'static>(
    store: S,
    mut reader: R,
) -> impl Future<Item = (), Error = Error> {
    async_block! {
        let mut small_builder = SmallBuilder::new();
        io::copy(&mut reader, &mut small_builder)?;
        let small_ref = await!(small_builder.as_small().send(&store))?;
        let fetched_small = await!(small_ref.fetch())?;
        ensure!(small_builder.as_small() == &fetched_small, "Test failure: sent and fetched small objects differ!");
        Ok(())
    }
}

pub fn send_and_fetch_large<S: Store, R: Read + 'static>(
    store: S,
    reader: R,
) -> impl Future<Item = (), Error = Error> {
    async_block! {
        let mut splitter = Splitter::new(reader, Parameters::default());

        let mut small_builder = SmallBuilder::new();
        let mut smalls = Vec::new();
        let mut chunks = FuturesOrdered::new();
        while let Some(range) = splitter.find(&mut small_builder)? {
            chunks.push(small_builder.as_small().send(&store));
            smalls.push((
                (range.start as u64..range.end as u64),
                small_builder.as_small().clone(),
            ));
            small_builder.clear();
        }

        let mut large_builder = LargeBuilder::new(1);
        let mut small_refs = Vec::new();
        #[async]
        for small_ref in chunks {
            large_builder.push(ObjectRef::Small(small_ref.clone()));
            small_refs.push(small_ref);
        }
        let large_ref = await!(large_builder.as_large().send(&store))?;

        let fetched_large = await!(large_ref.fetch())?;

        ensure!(
            large_builder.as_large() == &fetched_large,
            "Test failure: sent and fetched large objects differ!"
        );
        ensure!(
            fetched_large.len() == smalls.len(),
            "Test failure: fetched large object had {} objects but {} were made!"
        );

        let fetched_smalls = await!(
            stream::futures_ordered(
                fetched_large
                    .into_iter()
                    .map(|(range, objref)| match objref {
                        ObjectRef::Small(small_ref) => Ok((range, small_ref)),
                        _ => bail!(
                            "Test failure: fetched large object contained a non-small object!"
                        ),
                    })
                    .map(
                        |res| res.into_future().and_then(|(range, small_ref)| small_ref
                            .fetch()
                            .map(|small| (range, small)))
                    )
            ).collect()
        )?;

        ensure!(
            smalls == fetched_smalls,
            "Test failure: fetched children did not match sent children!"
        );

        Ok(())
    }
}
