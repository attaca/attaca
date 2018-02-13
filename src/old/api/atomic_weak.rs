use std::{mem, marker::PhantomData, sync::{Weak, atomic::{AtomicPtr, Ordering}}};

#[derive(Debug)]
pub struct AtomicWeak<T> {
    phantom: PhantomData<T>,
    ptr: AtomicPtr<T>,
}

impl<T> Drop for AtomicWeak<T> {
    fn drop(&mut self) {
        mem::drop(unsafe { mem::transmute::<*mut T, Weak<T>>(*self.ptr.get_mut()) });
    }
}

impl<T> AtomicWeak<T> {
    pub fn new(weak: Weak<T>) -> Self {
        let ptr = AtomicPtr::new(unsafe { mem::transmute::<Weak<T>, *mut T>(weak) });
        Self {
            phantom: PhantomData,
            ptr,
        }
    }

    pub fn load(&self, ordering: Ordering) -> Weak<T> {
        let ptr = self.ptr.load(ordering);
        let original = unsafe { mem::transmute::<*mut T, Weak<T>>(ptr) };
        let cloned = original.clone();
        mem::forget(original);
        cloned
    }

    pub fn store(&self, weak: Weak<T>, ordering: Ordering) {
        mem::drop(self.swap(weak, ordering));
    }

    pub fn swap(&self, weak: Weak<T>, ordering: Ordering) -> Weak<T> {
        let ptr = unsafe { mem::transmute::<Weak<T>, *mut T>(weak) };
        let old_ptr = self.ptr.swap(ptr, ordering);
        unsafe { mem::transmute::<*mut T, Weak<T>>(old_ptr) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn correct_drop() {
        let arc = Arc::new(42);
        let atomic_weak = AtomicWeak::new(Arc::downgrade(&arc));

        assert_eq!(Arc::strong_count(&arc), 1);
        assert_eq!(Arc::weak_count(&arc), 1);

        mem::drop(atomic_weak);

        assert_eq!(Arc::strong_count(&arc), 1);
        assert_eq!(Arc::weak_count(&arc), 0);
    }

    #[test]
    fn correct_load() {
        let arc = Arc::new(42);
        let atomic_weak = AtomicWeak::new(Arc::downgrade(&arc));

        assert_eq!(Arc::strong_count(&arc), 1);
        assert_eq!(Arc::weak_count(&arc), 1);

        let weak = atomic_weak.load(Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&arc), 1);
        assert_eq!(Arc::weak_count(&arc), 2);
    }

    #[test]
    fn correct_store() {
        let arc_a = Arc::new(42);
        let arc_b = Arc::new(43);
        let atomic_weak = AtomicWeak::new(Arc::downgrade(&arc_a));

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 0);

        atomic_weak.store(Arc::downgrade(&arc_b), Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 0);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 1);
    }

    #[test]
    fn correct_swap() {
        let arc_a = Arc::new(42);
        let arc_b = Arc::new(43);
        let atomic_weak = AtomicWeak::new(Arc::downgrade(&arc_a));

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 0);

        let weak_a = atomic_weak.swap(Arc::downgrade(&arc_b), Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 1);
    }

    #[test]
    fn correct_swap_and_load() {
        let arc_a = Arc::new(42);
        let arc_b = Arc::new(43);
        let atomic_weak = AtomicWeak::new(Arc::downgrade(&arc_a));

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 0);

        let weak_a = atomic_weak.swap(Arc::downgrade(&arc_b), Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 1);

        let weak_b = atomic_weak.load(Ordering::Relaxed);

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 1);
        assert_eq!(Arc::weak_count(&arc_b), 2);

        let strong_b = Weak::upgrade(&weak_b).unwrap();

        assert_eq!(Arc::strong_count(&arc_a), 1);
        assert_eq!(Arc::weak_count(&arc_a), 1);
        assert_eq!(Arc::strong_count(&arc_b), 2);
        assert_eq!(Arc::weak_count(&arc_b), 2);

        assert_eq!(*strong_b, 43);
    }
}
