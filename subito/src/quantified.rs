use attaca::{Handle, HandleDigest, Store, digest::Digest};
use failure::Error;

use Repository;

pub trait QuantifiedOutput<'r> {
    type Output;
}

pub trait Quantified: for<'x> QuantifiedOutput<'x> {
    fn apply<'r, S, D>(
        self,
        Repository<S, D>,
    ) -> Result<<Self as QuantifiedOutput<'r>>::Output, Error>
    where
        S: Store,
        D: Digest,
        S::Handle: HandleDigest<D>;
}

pub trait QuantifiedRefMut: for<'x> QuantifiedOutput<'x> {
    fn apply_mut<'r, S, D>(
        self,
        &'r mut Repository<S, D>,
    ) -> Result<<Self as QuantifiedOutput<'r>>::Output, Error>
    where
        S: Store,
        D: Digest,
        S::Handle: HandleDigest<D>;
}

pub trait QuantifiedRef: for<'x> QuantifiedOutput<'x> {
    fn apply_ref<'r, S, D>(
        self,
        &'r Repository<S, D>,
    ) -> Result<<Self as QuantifiedOutput<'r>>::Output, Error>
    where
        S: Store,
        D: Digest,
        S::Handle: HandleDigest<D>;
}
