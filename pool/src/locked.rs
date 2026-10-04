//! Fixed-size pool whose reclamation is protected by a caller-owned lock.
use super::{Error, Mapping, NONE, PoolItem, Result};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};

const BUCKETS: usize = 128;

struct Deleted {
    head: AtomicU32,
    tail: AtomicU32,
}

impl Deleted {
    fn new() -> Self {
        Self { head: AtomicU32::new(NONE), tail: AtomicU32::new(NONE) }
    }
}

#[repr(align(64))]
struct Bucket {
    free: AtomicU32,
    deleted: Deleted,
}

impl Bucket {
    fn new() -> Self {
        Self { free: AtomicU32::new(NONE), deleted: Deleted::new() }
    }
}

/// Fixed-size pool with 128 free lists and one deleted list per bucket.
///
/// The pool does not lock. Call `alloc` and drop returned blocks while holding
/// the caller's shared or exclusive lock, and call `reclaim` while holding that
/// lock exclusively. That lock must exclude every operation on this pool while
/// reclamation runs. Reclamation is the only operation that moves blocks from
/// deleted lists to free lists. `T` supplies slot size, alignment, and fresh-slot
/// initialization; this pool never resets or destroys slots. Callers must reset
/// each item before returning it and destroy live `T` values before pool teardown.
/// `LINK` must be the offset of a `MaybeUninit<AtomicU32>` field in `T`.
pub struct LockedPool<T: PoolItem, const ALIGN: usize, const LINK: usize> {
    mapping: Mapping,
    max_blocks: u32,
    next: AtomicU32,
    buckets: Box<[Bucket]>,
    _slot: PhantomData<fn() -> T>,
}

// SAFETY: list operations use atomics; the caller's lock excludes operations
// from reclamation. Mapping addresses stay fixed when this pool moves.
unsafe impl<T: PoolItem + Send, const A: usize, const L: usize> Send for LockedPool<T, A, L> {}
unsafe impl<T: PoolItem + Send + Sync, const A: usize, const L: usize> Sync
    for LockedPool<T, A, L>
{
}

impl<T: PoolItem, const A: usize, const L: usize> LockedPool<T, A, L> {
    pub fn new(max_blocks: u32, initial_blocks: u32) -> Result<Self> {
        if !A.is_power_of_two()
            || A < align_of::<AtomicU32>()
            || size_of::<T>() == 0
            || size_of::<T>() % A != 0
            || align_of::<T>() > A
            || size_of::<T>() % align_of::<AtomicU32>() != 0
            || L % align_of::<AtomicU32>() != 0
            || L.checked_add(size_of::<AtomicU32>()).is_none_or(|end| end > size_of::<T>())
        {
            return Err(Error::new("slot size, alignment, and inline link offset are invalid"));
        }
        if max_blocks == 0 {
            return Err(Error::new("pool needs max_blocks >= 1"));
        }
        if initial_blocks > max_blocks {
            return Err(Error::new("pool needs initial_blocks <= max_blocks"));
        }
        let initial_blocks = initial_blocks.max(1);
        let bytes = size_of::<T>()
            .checked_mul(max_blocks as usize)
            .ok_or_else(|| Error::new("block pool is too large"))?;
        let mapping = Mapping::new(bytes, A)?;
        let pool = Self {
            mapping,
            max_blocks,
            next: AtomicU32::new(initial_blocks),
            buckets: (0..BUCKETS).map(|_| Bucket::new()).collect(),
            _slot: PhantomData,
        };
        for index in 0..initial_blocks {
            // SAFETY: each index is new writable storage, initialized once.
            unsafe { T::init(pool.slot(index)) };
            // SAFETY: each index has one stable link initialized before publication.
            unsafe { pool.init_link(index) };
            let next = index as u64 + BUCKETS as u64;
            pool.link(index)
                .store(if next < initial_blocks as u64 { next as u32 } else { NONE }, SeqCst);
        }
        for index in 0..initial_blocks.min(BUCKETS as u32) {
            pool.buckets[index as usize].free.store(index, SeqCst);
        }
        Ok(pool)
    }

    #[inline]
    fn link(&self, index: u32) -> &AtomicU32 {
        // SAFETY: internal callers use initialized pool indexes.
        unsafe { &*self.link_ptr(index) }
    }

    #[inline]
    fn link_ptr(&self, index: u32) -> *mut AtomicU32 {
        // SAFETY: internal callers use indexes within the reserved mapping.
        unsafe {
            self.mapping.base.as_ptr().add(index as usize * size_of::<T>() + L).cast::<AtomicU32>()
        }
    }

    #[inline]
    unsafe fn init_link(&self, index: u32) {
        // SAFETY: called once for fresh storage before the slot is published.
        unsafe { self.link_ptr(index).write(AtomicU32::new(NONE)) };
    }

    #[inline]
    fn slot(&self, index: u32) -> *mut T {
        // SAFETY: index is in the reserved mapping and slots use T's stride.
        unsafe { self.mapping.base.as_ptr().add(index as usize * size_of::<T>()).cast::<T>() }
    }

    /// Number of initialized indexes, including free slots.
    pub fn issued_blocks(&self) -> u32 {
        self.next.load(SeqCst)
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
    pub fn alloc(&self) -> Result<LockedBlock<'_, T, A, L>> {
        let random = random::thread_u64();
        let mask = BUCKETS - 1;
        let first = ((random as usize) ^ mask) & mask;
        let delta =
            ((((random >> BUCKETS.trailing_zeros()) as usize) & (BUCKETS / 2 - 1)) << 1) | 1;
        for step in 0..BUCKETS {
            let choice = first.wrapping_add(step * delta) & mask;
            if let Some(index) = self.pop(&self.buckets[choice]) {
                return Ok(LockedBlock { pool: self, index });
            }
        }
        self.issue_fresh()
    }

    fn pop(&self, bucket: &Bucket) -> Option<u32> {
        let mut head = bucket.free.load(SeqCst);
        while head != NONE {
            let next = self.link(head).load(SeqCst);
            match bucket.free.compare_exchange_weak(head, next, SeqCst, SeqCst) {
                Ok(_) => return Some(head),
                Err(actual) => head = actual,
            }
        }
        None
    }

    fn issue_fresh(&self) -> Result<LockedBlock<'_, T, A, L>> {
        let index = self
            .next
            .fetch_update(SeqCst, SeqCst, |next| (next < self.max_blocks).then(|| next + 1))
            .map_err(|_| Error::new("block pool exhausted; some blocks may await reclamation"))?;
        // SAFETY: fetch_update granted this caller one new writable index.
        unsafe { T::init(self.slot(index)) };
        // SAFETY: each fresh index has its link initialized before use.
        unsafe { self.init_link(index) };
        Ok(LockedBlock { pool: self, index })
    }

    fn retire(&self, index: u32) {
        let bucket = &self.buckets[index as usize & (BUCKETS - 1)];
        let mut head = bucket.deleted.head.load(SeqCst);
        loop {
            self.link(index).store(head, SeqCst);
            match bucket.deleted.head.compare_exchange_weak(head, index, SeqCst, SeqCst) {
                Ok(_) => {
                    if head == NONE {
                        bucket.deleted.tail.store(index, SeqCst);
                    }
                    return;
                }
                Err(actual) => head = actual,
            }
        }
    }

    /// Return a pool index whose ownership is tracked by the caller instead of a block handle.
    ///
    /// # Safety
    /// `index` must name one currently allocated block owned by the caller, and that block
    /// must not be returned again. Call this under the same caller-owned lock used for
    /// `alloc`; `reclaim` must remain excluded until the operation is complete.
    pub unsafe fn retire_index(&self, index: u32) {
        assert!(index < self.issued_blocks(), "retired pool index was never issued");
        self.retire(index);
    }

    /// Move every bucket's deleted list onto its free list.
    ///
    /// The caller must hold its global lock exclusively. No pool operation may
    /// run concurrently with this method.
    pub fn reclaim(&self) {
        for bucket in self.buckets.iter() {
            let head = bucket.deleted.head.swap(NONE, SeqCst);
            let tail = bucket.deleted.tail.swap(NONE, SeqCst);
            if head == NONE {
                debug_assert_eq!(tail, NONE);
                continue;
            }
            assert_ne!(tail, NONE, "deleted list has no tail");
            let free = bucket.free.load(SeqCst);
            self.link(tail).store(free, SeqCst);
            bucket.free.store(head, SeqCst);
        }
    }
}

/// Ownership of one pool index. Its destructor pushes the index onto the
/// deleted list without reclaiming it. The caller must drop it while holding
/// the shared or exclusive lock that also protects calls to `LockedPool::alloc`.
pub struct LockedBlock<'a, T: PoolItem, const ALIGN: usize, const LINK: usize> {
    pool: &'a LockedPool<T, ALIGN, LINK>,
    index: u32,
}

impl<T: PoolItem, const A: usize, const L: usize> LockedBlock<'_, T, A, L> {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn as_ptr(&self) -> *mut u8 {
        // SAFETY: index identifies a block in the reserved mapping.
        unsafe { self.pool.mapping.base.as_ptr().add(self.index as usize * size_of::<T>()) }
    }

    pub fn link(&self) -> &AtomicU32 {
        self.pool.link(self.index)
    }
}

impl<T: PoolItem, const A: usize, const L: usize> Drop for LockedBlock<'_, T, A, L> {
    fn drop(&mut self) {
        self.pool.retire(self.index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::mem::MaybeUninit;
    use std::sync::Barrier;
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::SeqCst};
    use std::thread;
    use std::time::Duration;

    #[repr(C)]
    struct TestItem {
        link: MaybeUninit<AtomicU32>,
        bytes: [MaybeUninit<u8>; 12],
    }

    // SAFETY: every field is MaybeUninit, so a fresh slot is a valid TestItem.
    unsafe impl PoolItem for TestItem {}

    type TestPool = LockedPool<TestItem, 4, { std::mem::offset_of!(TestItem, link) }>;

    #[test]
    fn reclaim_only() {
        let pool = TestPool::new(2, 1).unwrap();
        let lock = RwLock::new(());
        let first = {
            let _read = lock.read().unwrap();
            pool.alloc().unwrap()
        };
        assert_eq!(first.index(), 0);
        {
            let _read = lock.read().unwrap();
            drop(first);
        }
        let second = {
            let _read = lock.read().unwrap();
            pool.alloc().unwrap()
        };
        assert_eq!(second.index(), 1);
        {
            let _read = lock.read().unwrap();
            drop(second);
            assert!(pool.alloc().is_err());
        }
        assert_eq!(pool.issued_blocks(), 2);
        {
            let _write = lock.write().unwrap();
            pool.reclaim();
        }
        let _read = lock.read().unwrap();
        let blocks: Vec<_> = (0..2).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(
            blocks.iter().map(LockedBlock::index).collect::<HashSet<_>>(),
            HashSet::from([0, 1])
        );
        assert_eq!(pool.issued_blocks(), 2);
        drop(blocks);
    }

    #[test]
    fn reclaim_keeps_tail() {
        let pool = TestPool::new(257, 257).unwrap();
        let lock = RwLock::new(());
        {
            let _read = lock.read().unwrap();
            let index = pool.pop(&pool.buckets[0]).unwrap();
            assert_eq!(index, 0);
            drop(LockedBlock { pool: &pool, index });
        }
        assert_eq!(pool.buckets[0].free.load(SeqCst), 128);
        {
            let _write = lock.write().unwrap();
            pool.reclaim();
        }
        assert_eq!(pool.buckets[0].free.load(SeqCst), 0);
        let _read = lock.read().unwrap();
        let indexes = (0..3).map(|_| pool.pop(&pool.buckets[0]).unwrap()).collect::<Vec<_>>();
        assert_eq!(indexes, [0, 128, 256]);
    }

    #[test]
    fn link_reuse() {
        let pool = TestPool::new(3, 1).unwrap();
        let lock = RwLock::new(());
        let first = {
            let _read = lock.read().unwrap();
            pool.alloc().unwrap()
        };
        assert_eq!(first.index(), 0);
        {
            let _read = lock.read().unwrap();
            first.link().store(0xfeed, SeqCst);
            drop(first);
        }
        let second = {
            let _read = lock.read().unwrap();
            pool.alloc().unwrap()
        };
        assert_eq!(second.index(), 1);
        {
            let _read = lock.read().unwrap();
            drop(second);
        }
        {
            let _write = lock.write().unwrap();
            pool.reclaim();
        }
        let _read = lock.read().unwrap();
        let blocks: Vec<_> = (0..2).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(
            blocks.iter().map(LockedBlock::index).collect::<HashSet<_>>(),
            HashSet::from([0, 1])
        );
        assert_eq!(pool.issued_blocks(), 2);
        drop(blocks);
    }

    #[test]
    fn concurrent_reuse() {
        const THREADS: usize = 8;
        const OPS: usize = 1200;
        const MAX: usize = 16_384;
        let pool = TestPool::new(MAX as u32, 128).unwrap();
        let lock = RwLock::new(());
        let active: Vec<_> = (0..MAX).map(|_| AtomicBool::new(false)).collect();
        let start = Barrier::new(THREADS + 1);

        thread::scope(|scope| {
            let pool = &pool;
            let lock = &lock;
            let active = &active;
            let start = &start;
            for id in 0..THREADS {
                let pool = pool;
                let lock = lock;
                let start = start;
                let active = active;
                scope.spawn(move || {
                    let mut held = Vec::new();
                    let mut random = id as u64 + 1;
                    start.wait();
                    for _ in 0..OPS {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        let allocate = held.is_empty() || (held.len() < 4 && random & 1 == 0);
                        let _read = lock.read().unwrap();
                        if allocate {
                            let block = pool.alloc().unwrap();
                            assert!(
                                active[block.index() as usize]
                                    .compare_exchange(false, true, SeqCst, SeqCst)
                                    .is_ok()
                            );
                            held.push(block);
                        } else {
                            let at = random as usize % held.len();
                            let block = held.swap_remove(at);
                            assert!(active[block.index() as usize].swap(false, SeqCst));
                            drop(block);
                        }
                    }
                    let _read = lock.read().unwrap();
                    for block in held {
                        assert!(active[block.index() as usize].swap(false, SeqCst));
                        drop(block);
                    }
                });
            }
            let pool = pool;
            let lock = lock;
            let start = start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..200 {
                    {
                        let _write = lock.write().unwrap();
                        pool.reclaim();
                    }
                    thread::sleep(Duration::from_micros(50));
                }
            });
        });

        {
            let _write = lock.write().unwrap();
            pool.reclaim();
        }
        assert!(active.iter().all(|used| !used.load(SeqCst)));
        let issued = pool.issued_blocks() as usize;
        let _read = lock.read().unwrap();
        let blocks: Vec<_> = (0..issued).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(pool.issued_blocks() as usize, issued);
        assert_eq!(blocks.iter().map(LockedBlock::index).collect::<HashSet<_>>().len(), issued);
        drop(blocks);
    }
}
