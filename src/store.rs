use std::{fmt, hash::Hash, io::{self, BufRead, Read, Write}};

use failure::Error;
use futures::{stream, prelude::*, sync::mpsc};

use Open;
use canonical;
use digest::{Digest, DigestWriter};
use hex::ToHex;

const COPY_BUFFER_SIZE: usize = 16;
const FSCK_BUFFER_SIZE: usize = 16;
const FSCK_CHANNEL_SIZE: usize = 16;

pub trait Store: Open + Clone + Send + Sync + Sized + 'static {
    type Handle: Handle;

    type HandleBuilder: HandleBuilder<Handle = Self::Handle>;
    fn handle_builder(&self) -> Self::HandleBuilder;

    type FutureLoadBranch: Future<Item = Option<Self::Handle>, Error = Error>;
    fn load_branch(&self, branch: String) -> Self::FutureLoadBranch;

    type FutureSwapBranch: Future<Item = (), Error = Error>;
    fn swap_branch(
        &self,
        branch: String,
        previous: Option<Self::Handle>,
        new: Option<Self::Handle>,
    ) -> Self::FutureSwapBranch;

    type FutureResolve: Future<Item = Option<Self::Handle>, Error = Error>;
    fn resolve<D: Digest>(&self, digest: &D) -> Self::FutureResolve
    where
        Self::Handle: HandleDigest<D>;
}

/// Trait for handles to objects in a `Store`.
///
/// Handle types must implement `HandleDigest` *for all* `D: Digest` (the only reason this is not a
/// bound is because it is currently impossible to express in Rust.) If a handle does not support
/// some specific `D`, that should be detected via comparison with the `Digest::NAME` and
/// `Digest::SIZE` constants, and then the incompatibility expressed via returning an error from
/// the `HandleDigest::digest` method.
///
/// There is also an additional contract on `PartialEq` impls for `Handle`. If two `Handle`s return
/// equal, then for any supported digest, the digests of the two handles should be equal (and vice
/// versa.) To restate, informally:
///
/// `if a == b then forall D: Digest, HandleDigest::<D>::digest(a) == HandleDigest::<D>::digest(b)`
///
/// There are no such contracts on `PartialOrd`, `Ord`, and `Hash`, except for those implied by
/// respecting the above contract on `PartialEq`.
pub trait Handle: Clone + Ord + Hash + Send + Sync + Sized + 'static {
    type Content: BufRead;
    type Refs: Iterator<Item = Self>;

    type FutureLoad: Future<Item = (Self::Content, Self::Refs), Error = Error>;
    fn load(&self) -> Self::FutureLoad;
}

pub trait HandleDigest<D: Digest>: Handle {
    type FutureDigest: Future<Item = D, Error = Error>;
    fn digest(&self) -> Self::FutureDigest;
}

pub trait HandleBuilder: Write {
    type Handle: Handle;

    fn add_reference(&mut self, handle: Self::Handle);

    type FutureHandle: Future<Item = Self::Handle, Error = Error>;
    fn finish(self) -> Self::FutureHandle;
}

#[async(boxed)]
pub fn copy<H: Handle, S: Store>(root: H, target: S) -> Result<S::Handle, Error> {
    let (mut content, refs) = await!(root.load())?;
    let mut handle_builder = target.handle_builder();

    // TODO: ICE (unresolved type in dtorck) if we stream::iter_ok the map without collecting
    // first.
    let ref_futures = refs.map(|r| copy(r, target.clone())).collect::<Vec<_>>();
    let new_refs = stream::iter_ok(ref_futures).buffered(COPY_BUFFER_SIZE);
    #[async]
    for reference in new_refs {
        handle_builder.add_reference(reference);
    }

    io::copy(&mut content, &mut handle_builder)?;

    Ok(await!(handle_builder.finish())?)
}

#[derive(Debug, Clone, Copy, Fail)]
pub struct FsckError<D: Digest> {
    pub received: D,
    pub calculated: D,
}

impl<D: Digest> fmt::Display for FsckError<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Fsck error: digest mismatch: calculated ")?;
        self.calculated.as_bytes().write_hex(f)?;
        write!(f, " but received ")?;
        self.received.as_bytes().write_hex(f)?;
        write!(f, " from store")?;

        Ok(())
    }
}

// This is *not* a stream because we want to both put out a stream through it *and* return the
// calculated digest.
#[async(boxed)]
fn do_fsck<D, H>(root: H, tx: mpsc::Sender<FsckError<D>>) -> Result<D, Error>
where
    D: Digest,
    H: HandleDigest<D>,
{
    let ((mut content, refs), store_digest) = await!(root.load().join(root.digest()))?;

    let digests = {
        let digest_futures = refs.map(|r| do_fsck(r, tx.clone())).collect::<Vec<_>>();
        let future_digests = stream::iter_ok(digest_futures)
            .buffered(FSCK_BUFFER_SIZE)
            .collect();
        await!(future_digests)?
    };
    let mut content_buf = Vec::new();
    content.read_to_end(&mut content_buf)?;

    let mut writer = D::writer();
    canonical::encode(&mut writer, &mut content_buf, &digests)?;
    let checked_digest = writer.finish();

    if store_digest != checked_digest {
        await!(tx.send(FsckError {
            received: store_digest,
            calculated: checked_digest.clone(),
        }))?;
    }

    Ok(checked_digest)
}

#[async_stream(item = FsckError<D>)]
pub fn fsck<D, H>(root: H) -> Result<(), Error>
where
    D: Digest,
    H: HandleDigest<D>,
{
    let (tx, rx) = mpsc::channel(FSCK_CHANNEL_SIZE);
    let blocking = do_fsck(root, tx).into_stream();

    // Use `select` here because we want to drive the channel to completion alongside the
    // "blocking" future which is producing the stream.
    #[async]
    for item in rx.map(Some)
        .map_err(|_| unreachable!())
        .select(blocking.map(|_| None))
    {
        match item {
            Some(error) => stream_yield!(error),
            None => return Ok(()),
        }
    }

    Ok(())
}
