//! Fixed-size pool whose mutation is protected by a caller-owned exclusive lock.
use super::{Error, Mapping, PoolItem, Result};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

const BUCKETS: usize = 128;

/// Type-specific link sentinel and allocation initialization for LockedPool.
///
/// # Safety
/// `LINK_NONE` must not name an index the pool can issue. `on_alloc` must leave
/// the Rust value initialized and must not panic.
pub unsafe trait LockedItem: PoolItem {
    /// Terminator for this item's free-list and deleted-list links.
    const LINK_NONE: u32 = u32::MAX;

    /// Called on every allocation, before the slot is returned to its owner.
    ///
    /// # Safety
    /// `slot` is a fully initialized, exclusively owned item.
    unsafe fn on_alloc(_slot: *mut Self) {}
}

struct Deleted {
    head: u32,
    tail: u32,
}

impl Deleted {
    fn new(none: u32) -> Self {
        Self { head: none, tail: none }
    }
}

struct Bucket {
    free: u32,
    deleted: Deleted,
}

impl Bucket {
    fn new(none: u32) -> Self {
        Self { free: none, deleted: Deleted::new(none) }
    }
}

/// Fixed-size pool with 128 free lists and one deleted list per bucket.
///
/// The pool does not lock. All calls that change pool state (`alloc`,
/// `retire_index`, and `reclaim`) require a mutable borrow, so callers must make
/// them while holding the exclusive lock that protects their surrounding
/// state. Readers may inspect live items under a shared lock; they must not
/// access the pool's inline link or list metadata.
///
/// `T` supplies slot size, alignment, fresh-slot initialization, a link
/// sentinel, and per-allocation cleanup. The pool never destroys slots.
/// Callers must reset each item before return and destroy live `T` values
/// before pool teardown. `LINK` must be the offset of a `u32` field in `T`.
pub struct LockedPool<T: LockedItem, const ALIGN: usize, const LINK: usize> {
    mapping: Mapping,
    max_blocks: u32,
    next: u32,
    buckets: Box<[Bucket]>,
    _slot: PhantomData<fn() -> T>,
}

// SAFETY: mutations require an exclusive borrow, and the mapping address stays
// fixed when the pool moves. Raw pointer dereferences remain the caller's unsafe
// responsibility and must obey the caller-owned lock.
unsafe impl<T: LockedItem + Send, const A: usize, const L: usize> Send for LockedPool<T, A, L> {}
unsafe impl<T: LockedItem + Send + Sync, const A: usize, const L: usize> Sync
    for LockedPool<T, A, L>
{
}

impl<T: LockedItem, const A: usize, const L: usize> LockedPool<T, A, L> {
    pub fn new(max_blocks: u32, initial_blocks: u32) -> Result<Self> {
        if !A.is_power_of_two()
            || size_of::<T>() == 0
            || size_of::<T>() % A != 0
            || align_of::<T>() > A
            || L % align_of::<u32>() != 0
            || L.checked_add(size_of::<u32>()).is_none_or(|end| end > size_of::<T>())
        {
            return Err(Error::new("slot size, alignment, and inline link offset are invalid"));
        }
        if max_blocks == 0 {
            return Err(Error::new("pool needs max_blocks >= 1"));
        }
        if T::LINK_NONE < max_blocks {
            return Err(Error::new("pool link sentinel overlaps its index range"));
        }
        if initial_blocks > max_blocks {
            return Err(Error::new("pool needs initial_blocks <= max_blocks"));
        }
        let initial_blocks = initial_blocks.max(1);
        let bytes = size_of::<T>()
            .checked_mul(max_blocks as usize)
            .ok_or_else(|| Error::new("block pool is too large"))?;
        let mapping = Mapping::new(bytes, A)?;
        let mut pool = Self {
            mapping,
            max_blocks,
            next: initial_blocks,
            buckets: (0..BUCKETS).map(|_| Bucket::new(T::LINK_NONE)).collect(),
            _slot: PhantomData,
        };
        for index in 0..initial_blocks {
            // SAFETY: each index is new writable storage, initialized once.
            unsafe { T::init(pool.slot(index)) };
            pool.init_link(index);
            let next = index as u64 + BUCKETS as u64;
            pool.set_link(
                index,
                if next < initial_blocks as u64 { next as u32 } else { T::LINK_NONE },
            );
        }
        for index in 0..initial_blocks.min(BUCKETS as u32) {
            pool.buckets[index as usize].free = index;
        }
        Ok(pool)
    }

    #[inline]
    fn link_ptr(&self, index: u32) -> *mut u32 {
        // SAFETY: caller uses an issued index in the reserved mapping.
        unsafe { self.mapping.base.as_ptr().add(index as usize * size_of::<T>() + L).cast::<u32>() }
    }

    #[inline]
    fn link(&mut self, index: u32) -> u32 {
        // SAFETY: all issued links are initialized; callers hold exclusive access.
        unsafe { self.link_ptr(index).read() }
    }

    #[inline]
    fn set_link(&mut self, index: u32, next: u32) {
        // SAFETY: callers hold exclusive access and the link field is writable.
        unsafe { self.link_ptr(index).write(next) };
    }

    #[inline]
    fn init_link(&mut self, index: u32) {
        self.set_link(index, T::LINK_NONE);
    }

    #[inline]
    fn slot(&self, index: u32) -> *mut T {
        // SAFETY: index is in the reserved mapping and slots use T's stride.
        unsafe { self.mapping.base.as_ptr().add(index as usize * size_of::<T>()).cast::<T>() }
    }

    /// Number of initialized indexes, including free slots.
    pub fn issued_blocks(&self) -> u32 {
        self.next
    }

    pub const fn block_bytes(&self) -> usize {
        size_of::<T>()
    }

    /// Stable pointer to slot zero, initialized during construction.
    pub fn base_ptr(&self) -> NonNull<T> {
        NonNull::new(self.slot(0)).expect("pool mapping is non-null")
    }

    /// Search all 128 free lists in a randomized stride, then issue one new
    /// index if none has a block. Does not reclaim deleted blocks.
    pub fn alloc(&mut self) -> Result<u32> {
        let random = random::thread_u64();
        let mask = BUCKETS - 1;
        let first = ((random as usize) ^ mask) & mask;
        let delta =
            ((((random >> BUCKETS.trailing_zeros()) as usize) & (BUCKETS / 2 - 1)) << 1) | 1;
        for step in 0..BUCKETS {
            let choice = first.wrapping_add(step * delta) & mask;
            if let Some(index) = self.pop(choice) {
                self.prepare_alloc(index);
                return Ok(index);
            }
        }
        let index = self.issue_fresh()?;
        self.prepare_alloc(index);
        Ok(index)
    }

    fn prepare_alloc(&mut self, index: u32) {
        // SAFETY: this index is initialized, allocated, and exclusively owned.
        unsafe { T::on_alloc(self.slot(index)) };
        self.set_link(index, T::LINK_NONE);
    }

    fn pop(&mut self, bucket: usize) -> Option<u32> {
        let head = self.buckets[bucket].free;
        if head == T::LINK_NONE {
            return None;
        }
        let next = self.link(head);
        self.buckets[bucket].free = next;
        Some(head)
    }

    fn issue_fresh(&mut self) -> Result<u32> {
        if self.next == self.max_blocks {
            return Err(Error::new("block pool exhausted; some blocks may await reclamation"));
        }
        let index = self.next;
        self.next += 1;
        // SAFETY: this is the next never-before-issued slot.
        unsafe { T::init(self.slot(index)) };
        self.init_link(index);
        Ok(index)
    }

    /// Return a pool index whose ownership is tracked by the caller.
    ///
    /// # Safety
    /// `index` must name one currently allocated block owned by the caller, and
    /// that block must not be returned again. Call this under the same
    /// exclusive lock used for `alloc`; `reclaim` must remain excluded until
    /// the operation is complete.
    pub unsafe fn retire_index(&mut self, index: u32) {
        assert!(index < self.next, "retired pool index was never issued");
        let bucket = index as usize & (BUCKETS - 1);
        let head = self.buckets[bucket].deleted.head;
        self.set_link(index, head);
        if head == T::LINK_NONE {
            self.buckets[bucket].deleted.tail = index;
        }
        self.buckets[bucket].deleted.head = index;
    }

    /// Move every bucket's deleted list onto its free list.
    ///
    /// The caller must hold its global lock exclusively. No pool operation may
    /// run concurrently with this method.
    pub fn reclaim(&mut self) {
        for bucket in 0..BUCKETS {
            let head = self.buckets[bucket].deleted.head;
            if head == T::LINK_NONE {
                continue;
            }
            let tail = self.buckets[bucket].deleted.tail;
            let free = self.buckets[bucket].free;
            self.set_link(tail, free);
            self.buckets[bucket].free = head;
            self.buckets[bucket].deleted = Deleted::new(T::LINK_NONE);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::mem::MaybeUninit;

    #[repr(C)]
    struct TestItem {
        link: u32,
        flags: u8,
        bytes: [MaybeUninit<u8>; 8],
    }

    const VALID: u8 = 8;

    // SAFETY: all fields are initialized before the pool exposes this item.
    unsafe impl PoolItem for TestItem {
        unsafe fn init(slot: *mut Self) {
            // SAFETY: the pool provides fresh, aligned TestItem storage.
            unsafe {
                slot.write(Self { link: u32::MAX, flags: 0, bytes: [MaybeUninit::uninit(); 8] })
            };
        }
    }

    // SAFETY: the chosen sentinel is outside each test pool's issued range.
    unsafe impl LockedItem for TestItem {
        const LINK_NONE: u32 = u32::MAX - 1;

        unsafe fn on_alloc(slot: *mut Self) {
            // SAFETY: the pool provides this initialized slot exclusively.
            unsafe { (*slot).flags = 0 };
        }
    }

    type TestPool = LockedPool<TestItem, 4, { std::mem::offset_of!(TestItem, link) }>;

    #[test]
    fn reclaim_only() {
        let mut pool = TestPool::new(2, 1).unwrap();
        let first = pool.alloc().unwrap();
        assert_eq!(first, 0);
        assert_eq!(unsafe { (*pool.slot(first)).flags }, 0);
        assert_eq!(pool.link(first), <TestItem as LockedItem>::LINK_NONE);
        unsafe { (*pool.slot(first)).flags = VALID };
        // Deleted slots remain unavailable until the exclusive reclaim pass.
        unsafe { pool.retire_index(first) };
        let second = pool.alloc().unwrap();
        assert_eq!(second, 1);
        unsafe { pool.retire_index(second) };
        assert!(pool.alloc().is_err());
        assert_eq!(pool.issued_blocks(), 2);

        pool.reclaim();
        let blocks = [pool.alloc().unwrap(), pool.alloc().unwrap()];
        assert_eq!(blocks.iter().copied().collect::<HashSet<_>>(), HashSet::from([0, 1]));
        for index in blocks {
            assert_eq!(unsafe { (*pool.slot(index)).flags }, 0);
            assert_eq!(pool.link(index), <TestItem as LockedItem>::LINK_NONE);
        }
        assert_eq!(pool.issued_blocks(), 2);
    }

    #[test]
    fn reclaim_keeps_tail() {
        let mut pool = TestPool::new(257, 257).unwrap();
        let first = pool.pop(0).unwrap();
        assert_eq!(first, 0);
        assert_eq!(pool.buckets[0].free, 128);
        unsafe { pool.retire_index(first) };

        pool.reclaim();
        assert_eq!(pool.pop(0), Some(0));
        assert_eq!(pool.pop(0), Some(128));
        assert_eq!(pool.pop(0), Some(256));
        assert_eq!(pool.pop(0), None);
    }

    #[test]
    fn allocation_requires_exclusive_borrow() {
        let mut pool = TestPool::new(2, 0).unwrap();
        let first = pool.alloc().unwrap();
        assert_eq!(first, 0);
        unsafe { pool.retire_index(first) };
        pool.reclaim();
        assert_eq!(pool.alloc().unwrap(), 0);
    }
}
