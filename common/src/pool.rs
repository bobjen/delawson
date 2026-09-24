//! Fixed-size pools with two retirement lists and statically selected link storage.
use crate::{Error, Result};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::SeqCst};
use std::sync::{OnceLock, RwLock, TryLockError, TryLockResult};

#[cfg(test)]
mod concurrency;

macro_rules! pool_hit {
    ($pool:ident, $case:ident) => {
        #[cfg(test)]
        $pool.probe.hit(concurrency::Case::$case);
    };
}

pub const DEFAULT_BYTES: usize = 4096;
pub const DEFAULT_ALIGN: usize = 4096;
pub const DEFAULT_MAX: u32 = 1 << 20;
pub const DEFAULT_INITIAL: u32 = 16;
const NONE: u32 = u32::MAX;
pub type GlobalPool = Pool<DEFAULT_ALIGN, DEFAULT_BYTES, ExternalLinks>;
static GLOBAL: OnceLock<GlobalPool> = OnceLock::new();

pub(crate) fn default_max() -> u32 {
    DEFAULT_MAX
}
pub(crate) fn default_initial() -> u32 {
    DEFAULT_INITIAL
}

/// Initialize from process parameters before allocating any blocks.
pub fn initialize(max_blocks: u32, initial_blocks: u32) -> Result<()> {
    if let Some(pool) = GLOBAL.get() {
        return pool.same_limits(max_blocks, initial_blocks);
    }
    let pool = GlobalPool::new(max_blocks, initial_blocks)?;
    // A concurrent initializer may win; its parameters must agree.
    let _ = GLOBAL.set(pool);
    GLOBAL.get().unwrap().same_limits(max_blocks, initial_blocks)
}

/// Retain this reference to avoid checking initialization on every allocation.
pub fn global() -> &'static GlobalPool {
    GLOBAL.get().expect("initialize the block pool before use")
}

struct Mapping {
    raw: NonNull<u8>,
    base: NonNull<u8>,
    len: usize,
}

impl Mapping {
    fn new(bytes: usize, align: usize) -> Result<Self> {
        let len = bytes
            .checked_add(align - 1)
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or_else(|| Error::new("block pool mapping is too large"))?;
        // Anonymous pages are supplied on demand, not allocated for the whole range.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(Error::new(format!(
                "reserve block pool: {}",
                std::io::Error::last_os_error()
            )));
        }
        let Some(raw) = NonNull::new(raw.cast::<u8>()) else {
            // Rust references cannot represent a mapping at address zero.
            unsafe { libc::munmap(raw, len) };
            return Err(Error::new("block pool mapped at address zero"));
        };
        let offset = raw.as_ptr().align_offset(align);
        // SAFETY: len includes align - 1 bytes of alignment slack.
        let base = unsafe { NonNull::new_unchecked(raw.as_ptr().add(offset)) };
        Ok(Self { raw, base, len })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this owns the original mapping, including alignment slack.
        unsafe { libc::munmap(self.raw.as_ptr().cast(), self.len) };
    }
}

/// Supplies stable atomic links, separately mapped or inside each item.
///
/// # Safety
/// Implementations must keep each initialized link at a distinct, aligned address
/// for the pool's lifetime. `link` must always return that same AtomicU32. Links
/// must not overlap other mutable storage exposed through safe APIs. Neither
/// initialization nor lookup may panic for valid arguments.
pub unsafe trait Links: Send + Sync + Sized {
    /// # Safety
    /// `base` describes max_blocks * block_bytes writable bytes, kept mapped
    /// until after this Links value is dropped. Each block has the pool alignment.
    unsafe fn new(base: NonNull<u8>, block_bytes: usize, max_blocks: u32) -> Result<Self>;

    /// # Safety
    /// Called exactly once per index, before that index is published or allocated.
    unsafe fn init(&self, index: u32);

    /// # Safety
    /// The index must have been initialized and be less than max_blocks.
    unsafe fn link(&self, index: u32) -> &AtomicU32;
}

pub struct ExternalLinks {
    mapping: Mapping,
}

// SAFETY: only atomic accesses are made to the mapping after initialization.
unsafe impl Send for ExternalLinks {}
unsafe impl Sync for ExternalLinks {}
unsafe impl Links for ExternalLinks {
    unsafe fn new(_: NonNull<u8>, _: usize, max_blocks: u32) -> Result<Self> {
        let bytes = (max_blocks as usize)
            .checked_mul(4)
            .ok_or_else(|| Error::new("link mapping is too large"))?;
        Ok(Self { mapping: Mapping::new(bytes, 4)? })
    }

    unsafe fn init(&self, index: u32) {
        // SAFETY: the caller exclusively initializes this reserved index once.
        unsafe {
            self.mapping
                .base
                .as_ptr()
                .cast::<AtomicU32>()
                .add(index as usize)
                .write(AtomicU32::new(NONE))
        };
    }

    #[inline]
    unsafe fn link(&self, index: u32) -> &AtomicU32 {
        // SAFETY: the caller guarantees an initialized index in this mapping.
        unsafe { &*self.mapping.base.as_ptr().cast::<AtomicU32>().add(index as usize) }
    }
}

/// OFFSET can be supplied with offset_of!(Item, field). The field remains atomic
/// while allocated, but its value may then have an application-specific meaning.
pub struct InlineLinks<const OFFSET: usize> {
    base: NonNull<u8>,
    stride: usize,
}

// SAFETY: this exposes only atomic references to the live pool mapping.
unsafe impl<const O: usize> Send for InlineLinks<O> {}
unsafe impl<const O: usize> Sync for InlineLinks<O> {}
unsafe impl<const O: usize> Links for InlineLinks<O> {
    unsafe fn new(base: NonNull<u8>, block_bytes: usize, _: u32) -> Result<Self> {
        if O.checked_add(4).is_none_or(|end| end > block_bytes)
            || O % 4 != 0
            || block_bytes % 4 != 0
            || base.as_ptr().align_offset(4) != 0
        {
            return Err(Error::new("inline link must fit an item and be 4-byte aligned"));
        }
        Ok(Self { base, stride: block_bytes })
    }

    unsafe fn init(&self, index: u32) {
        // SAFETY: this is the first use of the index, with space checked in new.
        unsafe {
            self.base
                .as_ptr()
                .add(index as usize * self.stride + O)
                .cast::<AtomicU32>()
                .write(AtomicU32::new(NONE))
        };
    }

    #[inline]
    unsafe fn link(&self, index: u32) -> &AtomicU32 {
        // SAFETY: the caller guarantees an initialized index in the live mapping.
        unsafe { &*self.base.as_ptr().add(index as usize * self.stride + O).cast::<AtomicU32>() }
    }
}

struct Deleted {
    gate: RwLock<()>,
    head: AtomicU32,
    tail: AtomicU32,
}

impl Deleted {
    fn new() -> Self {
        Self { gate: RwLock::new(()), head: AtomicU32::new(NONE), tail: AtomicU32::new(NONE) }
    }
}

fn try_guard<T>(result: TryLockResult<T>) -> Option<T> {
    match result {
        Ok(guard) => Some(guard),
        Err(TryLockError::Poisoned(error)) => Some(error.into_inner()),
        Err(TryLockError::WouldBlock) => None,
    }
}

pub struct Pool<const ALIGN: usize, const BYTES: usize, L: Links = ExternalLinks> {
    // Links must be dropped before the mapping they may refer to.
    links: L,
    mapping: Mapping,
    max_blocks: u32,
    initial_blocks: u32,
    next: AtomicU32,
    free: AtomicU32,
    deleted: [Deleted; 2],
    current: AtomicUsize,
    #[cfg(test)]
    probe: concurrency::Probe,
}

// SAFETY: pool metadata is synchronized; each live block has one owner. Mapping
// addresses stay fixed when the pool moves. L provides its own Send/Sync contract.
unsafe impl<const A: usize, const B: usize, L: Links> Send for Pool<A, B, L> {}
unsafe impl<const A: usize, const B: usize, L: Links> Sync for Pool<A, B, L> {}

impl<const A: usize, const B: usize, L: Links> Pool<A, B, L> {
    pub fn new(max_blocks: u32, initial_blocks: u32) -> Result<Self> {
        if !A.is_power_of_two() || B == 0 || B % A != 0 {
            return Err(Error::new(
                "block size must be a positive multiple of power-of-two alignment",
            ));
        }
        if max_blocks == 0 || initial_blocks > max_blocks {
            return Err(Error::new(
                "pool needs 0 <= initial_blocks <= max_blocks and max_blocks > 0",
            ));
        }
        let bytes = B
            .checked_mul(max_blocks as usize)
            .ok_or_else(|| Error::new("block pool is too large"))?;
        let mapping = Mapping::new(bytes, A)?;
        // SAFETY: mapping remains alive until after links, including on drop.
        let links = unsafe { L::new(mapping.base, B, max_blocks)? };
        let pool = Self {
            links,
            mapping,
            max_blocks,
            initial_blocks,
            next: AtomicU32::new(initial_blocks),
            free: AtomicU32::new(NONE),
            deleted: [Deleted::new(), Deleted::new()],
            current: AtomicUsize::new(0),
            #[cfg(test)]
            probe: concurrency::Probe::default(),
        };
        for index in 0..initial_blocks {
            // SAFETY: initialization is single-threaded and each index is new.
            unsafe { pool.links.init(index) };
            pool.link(index)
                .store(if index + 1 == initial_blocks { NONE } else { index + 1 }, SeqCst);
        }
        if initial_blocks != 0 {
            pool.free.store(0, SeqCst);
        }
        Ok(pool)
    }

    fn same_limits(&self, max_blocks: u32, initial_blocks: u32) -> Result<()> {
        if self.max_blocks != max_blocks || self.initial_blocks != initial_blocks {
            return Err(Error::new("block pool already initialized with different limits"));
        }
        Ok(())
    }

    #[inline]
    fn link(&self, index: u32) -> &AtomicU32 {
        #[cfg(test)]
        if index >= self.next.load(SeqCst) || index >= self.max_blocks {
            self.probe.abort();
            panic!("pool link index {index} outside issued range");
        }
        // SAFETY: internal callers use initialized, owned or published indexes.
        unsafe { self.links.link(index) }
    }

    pub const fn block_bytes(&self) -> usize {
        B
    }

    fn pop(&self) -> Option<u32> {
        loop {
            #[cfg(test)]
            if self.probe.cancelled() {
                return None;
            }
            let current = self.current.load(SeqCst);
            let Some(_guard) = try_guard(self.deleted[current].gate.try_read()) else {
                std::hint::spin_loop();
                continue;
            };
            if self.current.load(SeqCst) != current {
                continue;
            }
            let mut head = self.free.load(SeqCst);
            while head != NONE {
                #[cfg(test)]
                if self.probe.cancelled() {
                    return None;
                }
                let next = self.link(head).load(SeqCst);
                match self.free.compare_exchange_weak(head, next, SeqCst, SeqCst) {
                    Ok(_) => {
                        pool_hit!(self, Reused);
                        return Some(head);
                    }
                    Err(actual) => {
                        pool_hit!(self, PopRetry);
                        head = actual;
                    }
                }
                // A stale link may contain an application value: never dereference
                // it unless the head CAS succeeded. Retirement prevents head ABA.
            }
            return None;
        }
    }

    /// Returns an error at capacity if no block is currently safe to reuse.
    /// Retired blocks may become available after concurrent operations finish.
    pub fn alloc(&self) -> Result<Block<'_, A, B, L>> {
        if let Some(index) = self.pop() {
            return Ok(Block { pool: self, index });
        }
        // Two turns can make both retired lists available, including a return to
        // the initially active list. Neither turn waits for exclusive access.
        for _ in 0..2 {
            self.recycle();
            if let Some(index) = self.pop() {
                return Ok(Block { pool: self, index });
            }
        }
        #[cfg(test)]
        if self.probe.cancelled() {
            return Err(Error::new("pool test cancelled"));
        }
        let index = self
            .next
            .fetch_update(SeqCst, SeqCst, |next| (next < self.max_blocks).then(|| next + 1))
            .map_err(|_| {
                pool_hit!(self, Exhausted);
                Error::new("block pool exhausted; some blocks may await reclamation")
            })?;
        // SAFETY: fetch_update granted this caller a unique new index.
        unsafe { self.links.init(index) };
        pool_hit!(self, Fresh);
        Ok(Block { pool: self, index })
    }

    fn retire(&self, index: u32) {
        loop {
            #[cfg(test)]
            if self.probe.cancelled() {
                return;
            }
            let current = self.current.load(SeqCst);
            let list = &self.deleted[current];
            let Some(_guard) = try_guard(list.gate.try_read()) else {
                std::hint::spin_loop();
                continue;
            };
            if self.current.load(SeqCst) != current {
                continue;
            }
            let mut head = list.head.load(SeqCst);
            loop {
                #[cfg(test)]
                if self.probe.cancelled() {
                    return;
                }
                self.link(index).store(head, SeqCst);
                match list.head.compare_exchange_weak(head, index, SeqCst, SeqCst) {
                    Ok(_) => {
                        if head == NONE {
                            list.tail.store(index, SeqCst);
                            pool_hit!(self, RetiredEmpty);
                        } else {
                            pool_hit!(self, RetiredNonempty);
                        }
                        return;
                    }
                    Err(actual) => {
                        pool_hit!(self, RetireRetry);
                        head = actual;
                    }
                }
            }
        }
    }

    fn recycle(&self) {
        #[cfg(test)]
        if self.probe.cancelled() {
            return;
        }
        let current = self.current.load(SeqCst);
        let inactive = 1 - current;
        let list = &self.deleted[inactive];
        let Some(_guard) = try_guard(list.gate.try_write()) else {
            pool_hit!(self, RecycleBusy);
            return;
        };
        if self.current.load(SeqCst) != current {
            return;
        }
        // Any other switch to this list also needs its exclusive lock, so the
        // selector stays current until we change it below. Its previous users
        // have drained, including readers of items retired into the current list.
        let head = list.head.swap(NONE, SeqCst);
        let tail = list.tail.swap(NONE, SeqCst);
        if head != NONE {
            let mut free = self.free.load(SeqCst);
            loop {
                #[cfg(test)]
                if self.probe.cancelled() {
                    return;
                }
                self.link(tail).store(free, SeqCst);
                match self.free.compare_exchange_weak(free, head, SeqCst, SeqCst) {
                    Ok(_) => {
                        if free == NONE {
                            pool_hit!(self, SpliceEmpty);
                        } else {
                            pool_hit!(self, SpliceNonempty);
                        }
                        break;
                    }
                    Err(actual) => {
                        pool_hit!(self, SpliceRetry);
                        free = actual;
                    }
                }
            }
        }
        if head == NONE {
            pool_hit!(self, RecycleEmpty);
        }
        // Keep the exclusive lock through the switch; releasing it first would
        // allow a delayed recycler to switch after an intervening full rotation.
        self.current.store(inactive, SeqCst);
    }
}

/// Ownership of one index; the borrow keeps its pool and mappings alive.
/// Inline-link callers must not overwrite the atomic link with non-atomic writes,
/// or create an exclusive reference spanning it while stale pool readers exist.
pub struct Block<'a, const ALIGN: usize, const BYTES: usize, L: Links = ExternalLinks> {
    pool: &'a Pool<ALIGN, BYTES, L>,
    index: u32,
}

impl<const A: usize, const B: usize, L: Links> Block<'_, A, B, L> {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn as_ptr(&self) -> *mut u8 {
        // SAFETY: index identifies a block in the reserved mapping.
        unsafe { self.pool.mapping.base.as_ptr().add(self.index as usize * B) }
    }

    /// May be reused as an application value until this block is returned.
    pub fn link(&self) -> &AtomicU32 {
        self.pool.link(self.index)
    }
}

impl<const A: usize, const B: usize> Block<'_, A, B, ExternalLinks> {
    pub fn as_slice(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: the external link never overlaps these exclusively owned bytes.
        unsafe { std::slice::from_raw_parts(self.as_ptr().cast(), B) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: the handle uniquely owns the payload; pool readers only use links.
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr().cast(), B) }
    }
}

impl<const A: usize, const B: usize, L: Links> Drop for Block<'_, A, B, L> {
    fn drop(&mut self) {
        self.pool.retire(self.index);
    }
}

pub fn free<const A: usize, const B: usize, L: Links>(block: Block<'_, A, B, L>) {
    drop(block);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn aligned_contiguous_and_reused() {
        let pool = Pool::<65536, 65536>::new(4, 2).unwrap();
        let mut blocks: Vec<_> = (0..4).map(|_| pool.alloc().unwrap()).collect();
        let base = blocks[0].as_ptr() as usize;
        for (index, block) in blocks.iter_mut().enumerate() {
            assert_eq!(block.index(), index as u32);
            assert_eq!(block.as_ptr() as usize, base + index * 65536);
            assert_eq!(block.as_ptr() as usize % 65536, 0);
            block.as_mut_slice().fill(MaybeUninit::new(index as u8));
        }
        assert!(pool.alloc().is_err());
        std::thread::scope(|scope| {
            scope.spawn(move || drop(blocks));
        });
        let blocks: Vec<_> = (0..4).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(blocks.iter().map(Block::index).collect::<HashSet<_>>().len(), 4);
        for block in &blocks {
            // SAFETY: initialized above and never overwritten by the free list.
            assert!(
                block.as_slice().iter().all(|b| unsafe { b.assume_init() } == block.index() as u8)
            );
        }
        assert_eq!(pool.next.load(SeqCst), 4);
    }

    #[test]
    fn retired_tail_preserves_existing_free_chain() {
        let pool = Pool::<4, 16>::new(4, 4).unwrap();
        let first = pool.alloc().unwrap();
        let second = pool.alloc().unwrap();
        drop(first);
        drop(second);
        assert_eq!(pool.deleted[0].head.load(SeqCst), 1);
        assert_eq!(pool.deleted[0].tail.load(SeqCst), 0);
        pool.recycle();
        pool.recycle();
        assert_eq!(pool.free.load(SeqCst), 1);
        assert_eq!(pool.link(1).load(SeqCst), 0);
        assert_eq!(pool.link(0).load(SeqCst), 2);
        assert_eq!(pool.link(2).load(SeqCst), 3);
        assert_eq!(pool.link(3).load(SeqCst), NONE);
        let blocks: Vec<_> = (0..4).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(blocks.iter().map(Block::index).collect::<Vec<_>>(), [1, 0, 2, 3]);
    }

    #[test]
    fn stalled_reader_delays_reuse_but_not_growth_or_retirement() {
        let pool = Pool::<4, 16, InlineLinks<4>>::new(3, 1).unwrap();
        let old = pool.current.load(SeqCst);
        let guard = pool.deleted[old].gate.read().unwrap();
        let stale_head = pool.free.load(SeqCst);
        let first = pool.alloc().unwrap();
        first.link().store(0xdeadbeef, SeqCst);
        let stale_next = pool.link(stale_head).load(SeqCst);
        assert_eq!(stale_next, 0xdeadbeef);
        assert!(pool.free.compare_exchange(stale_head, stale_next, SeqCst, SeqCst).is_err());
        drop(first);
        let second = pool.alloc().unwrap();
        assert_eq!(second.index(), 1);
        assert_ne!(pool.current.load(SeqCst), old);
        drop(second);
        let third = pool.alloc().unwrap();
        assert_eq!(third.index(), 2);
        assert_eq!(pool.current.load(SeqCst), 1 - old);
        assert!(pool.alloc().is_err());
        drop(guard);
        let reused: Vec<_> = (0..2).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(reused.iter().map(Block::index).collect::<HashSet<_>>(), HashSet::from([0, 1]));
    }

    #[test]
    fn older_group_protects_items_retired_in_new_group() {
        let pool = Pool::<4, 16>::new(2, 1).unwrap();
        let old = pool.current.load(SeqCst);
        let guard = pool.deleted[old].gate.read().unwrap();
        let stale_head = pool.free.load(SeqCst);
        pool.recycle();
        assert_eq!(pool.current.load(SeqCst), 1 - old);
        let block = pool.alloc().unwrap();
        assert_eq!(block.index(), stale_head);
        drop(block); // Retires into the NEW group, with an OLD reader outstanding.
        let fresh = pool.alloc().unwrap();
        assert_ne!(fresh.index(), stale_head);
        assert!(pool.alloc().is_err());
        drop(guard);
        assert_eq!(pool.alloc().unwrap().index(), stale_head);
    }

    #[test]
    fn validates_geometry_and_limits() {
        assert!(Pool::<0, 4>::new(1, 0).is_err());
        assert!(Pool::<3, 12>::new(1, 0).is_err());
        assert!(Pool::<4, 6>::new(1, 0).is_err());
        assert!(Pool::<4, 0>::new(1, 0).is_err());
        assert!(Pool::<4, 4>::new(0, 0).is_err());
        assert!(Pool::<4, 4>::new(1, 2).is_err());
        assert!(Pool::<1, { usize::MAX }>::new(2, 0).is_err());
        assert!(Pool::<4, 4, InlineLinks<4>>::new(1, 0).is_err());
        assert!(Pool::<4, 8, InlineLinks<1>>::new(1, 0).is_err());
        let pool = Pool::<4, 4>::new(1, 0).unwrap();
        assert_eq!(pool.alloc().unwrap().index(), 0);
        assert_eq!(pool.alloc().unwrap().index(), 0);
    }

    #[test]
    fn global_limits_cannot_change() {
        initialize(DEFAULT_MAX, DEFAULT_INITIAL).unwrap();
        initialize(DEFAULT_MAX, DEFAULT_INITIAL).unwrap();
        assert!(initialize(DEFAULT_MAX + 1, DEFAULT_INITIAL).is_err());
        assert!(initialize(DEFAULT_MAX, DEFAULT_INITIAL + 1).is_err());
        free(global().alloc().unwrap());
    }
}
