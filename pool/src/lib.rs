//! Fixed-size pools with two retirement lists and statically selected link storage.
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicUsize, Ordering::SeqCst};

mod segmented;
pub use segmented::*;

#[cfg(test)]
mod concurrency;

macro_rules! pool_hit {
    ($pool:ident, $case:ident) => {
        #[cfg(test)]
        $pool.probe.hit(concurrency::Case::$case);
    };
}

pub const DEFAULT_BUCKETS: usize = 256;
pub const DEFAULT_CHOICES: usize = 4;
const NONE: u32 = u32::MAX;

#[derive(Debug)]
pub struct Error(String);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Initialization hook for fresh slots. Byte arrays intentionally do nothing.
/// The pool calls this once when it first assigns raw storage to a slot; it does
/// not reset items when their blocks are returned or reused.
///
/// # Safety
/// `init` must be safe to call once on a fresh slot and must prepare any typed
/// state that callers expect to read. It must not panic.
pub unsafe trait PoolItem: Sized {
    /// # Safety
    /// `slot` is aligned writable storage for one fresh `Self` value.
    unsafe fn init(_slot: *mut Self) {}
}

unsafe impl<const N: usize> PoolItem for [u8; N] {}

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
        // Reserve address space now, while leaving physical backing to first touch.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
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

struct ListLock(AtomicI64);

impl ListLock {
    fn new() -> Self {
        Self(AtomicI64::new(0))
    }

    #[inline]
    fn try_read(&self) -> Option<ReadGuard<'_>> {
        // Check and increment atomically; outstanding readers are thread-bounded.
        let mut count = self.0.load(SeqCst);
        loop {
            if count < 0 {
                return None;
            }
            match self.0.compare_exchange_weak(count, count.wrapping_add(1), SeqCst, SeqCst) {
                Ok(_) => return Some(ReadGuard(self)),
                Err(actual) => count = actual,
            }
        }
    }

    #[inline]
    fn try_write(&self) -> Option<WriteGuard<'_>> {
        self.0.compare_exchange(0, i64::MIN, SeqCst, SeqCst).ok().map(|_| WriteGuard(self))
    }
}

struct ReadGuard<'a>(&'a ListLock);

impl Drop for ReadGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, SeqCst);
    }
}

struct WriteGuard<'a>(&'a ListLock);

impl Drop for WriteGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        // Rejected readers never modify the writer's counter.
        self.0.0.store(0, SeqCst);
    }
}

struct Deleted {
    gate: ListLock,
    head: AtomicU32,
    tail: AtomicU32,
}

impl Deleted {
    fn new() -> Self {
        Self { gate: ListLock::new(), head: AtomicU32::new(NONE), tail: AtomicU32::new(NONE) }
    }
}

// Keep independent buckets off each other's cache lines.
#[repr(align(64))]
struct Bucket {
    free: AtomicU32,
    deleted: [Deleted; 2],
    current: AtomicUsize,
}

impl Bucket {
    fn new() -> Self {
        Self {
            free: AtomicU32::new(NONE),
            deleted: [Deleted::new(), Deleted::new()],
            current: AtomicUsize::new(0),
        }
    }
}

/// Fixed-size slots laid out as `T`, aligned to at least `ALIGN`.
///
/// `T` supplies the slot size, alignment, and fresh-slot initializer. The pool
/// never resets or destroys slots; callers must reset item state before return
/// and drop every initialized `T` before the pool does. `L` supplies the atomic
/// free-list link.
pub struct Pool<T: PoolItem, const ALIGN: usize, L: Links = ExternalLinks> {
    // Links must be dropped before the mapping they may refer to.
    links: L,
    mapping: Mapping,
    max_blocks: u32,
    next: AtomicU32,
    buckets: Box<[Bucket]>,
    _slot: PhantomData<fn() -> T>,
    #[cfg(test)]
    probe: concurrency::Probe,
}

// SAFETY: pool metadata is synchronized; each live block has one owner. Mapping
// addresses stay fixed when the pool moves. L provides its own Send/Sync contract.
unsafe impl<T: PoolItem + Send, const A: usize, L: Links> Send for Pool<T, A, L> {}
unsafe impl<T: PoolItem + Send + Sync, const A: usize, L: Links> Sync for Pool<T, A, L> {}

impl<T: PoolItem, const A: usize, L: Links> Pool<T, A, L> {
    pub fn new(max_blocks: u32, initial_blocks: u32) -> Result<Self> {
        Self::with_params(max_blocks, initial_blocks, DEFAULT_BUCKETS)
    }

    /// Each index permanently belongs to index & (bucket_count - 1).
    /// Call alloc_from_bucket with a caller-generated random value to spread work.
    pub fn with_buckets(max_blocks: u32, initial_blocks: u32, bucket_count: usize) -> Result<Self> {
        Self::with_params(max_blocks, initial_blocks, bucket_count)
    }

    fn with_params(max_blocks: u32, initial_blocks: u32, bucket_count: usize) -> Result<Self> {
        if !bucket_count.is_power_of_two() || bucket_count > u32::MAX as usize {
            return Err(Error::new("bucket count must be a representable power of two"));
        }
        let bytes = size_of::<T>();
        if !A.is_power_of_two() || bytes == 0 || bytes % A != 0 || align_of::<T>() > A {
            return Err(Error::new(
                "slot size must be a positive multiple of power-of-two alignment at least as large as the slot alignment",
            ));
        }
        if max_blocks == 0 || initial_blocks > max_blocks {
            return Err(Error::new("pool needs 0 <= initial_blocks <= max_blocks"));
        }
        let total_bytes = bytes
            .checked_mul(max_blocks as usize)
            .ok_or_else(|| Error::new("block pool is too large"))?;
        let mapping = Mapping::new(total_bytes, A)?;
        // SAFETY: mapping remains alive until after links, including on drop.
        let links = unsafe { L::new(mapping.base, bytes, max_blocks)? };
        let pool = Self {
            links,
            mapping,
            max_blocks,
            next: AtomicU32::new(initial_blocks),
            buckets: (0..bucket_count).map(|_| Bucket::new()).collect(),
            _slot: PhantomData,
            #[cfg(test)]
            probe: concurrency::Probe::default(),
        };
        for index in 0..initial_blocks {
            // SAFETY: each index is new writable storage, initialized before publication.
            unsafe { T::init(pool.slot(index)) };
            // SAFETY: each index is initialized once before publication.
            unsafe { pool.links.init(index) };
            let next = index as u64 + bucket_count as u64;
            pool.link(index)
                .store(if next < initial_blocks as u64 { next as u32 } else { NONE }, SeqCst);
        }
        for index in 0..initial_blocks.min(bucket_count as u32) {
            pool.buckets[index as usize].free.store(index, SeqCst);
        }
        Ok(pool)
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

    #[inline]
    fn slot(&self, index: u32) -> *mut T {
        // SAFETY: index is in the reserved mapping and slots are aligned by stride.
        unsafe { self.mapping.base.as_ptr().add(index as usize * size_of::<T>()).cast::<T>() }
    }

    /// Number of initialized indexes, including staged free slots and initial blocks.
    /// During concurrent growth, some indexes may still be initializing.
    pub fn issued_blocks(&self) -> u32 {
        self.next.load(SeqCst)
    }

    pub const fn block_bytes(&self) -> usize {
        size_of::<T>()
    }

    fn pop(&self, bucket: &Bucket) -> Option<u32> {
        loop {
            // This is only an empty hint: never dereference a link until protected
            // by the shared guard, and reload the head after taking that guard.
            if bucket.free.load(SeqCst) == NONE {
                return None;
            }
            #[cfg(test)]
            if self.probe.cancelled() {
                return None;
            }
            let current = bucket.current.load(SeqCst);
            let Some(_guard) = bucket.deleted[current].gate.try_read() else {
                std::hint::spin_loop();
                continue;
            };
            if bucket.current.load(SeqCst) != current {
                continue;
            }
            let mut head = bucket.free.load(SeqCst);
            while head != NONE {
                #[cfg(test)]
                if self.probe.cancelled() {
                    return None;
                }
                let next = self.link(head).load(SeqCst);
                match bucket.free.compare_exchange_weak(head, next, SeqCst, SeqCst) {
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

    /// Tries up to DEFAULT_CHOICES buckets, reclaiming at each before issuing a fresh index.
    /// Bucket selection uses this thread's RNG; no shared RNG is used.
    /// Returns can remain temporarily unavailable while protected by readers.
    pub fn alloc(&self) -> Result<Block<'_, T, A, L>> {
        let random = random::thread_u64();
        let count = self.buckets.len();
        let mask = count - 1;
        let first = ((random as usize) ^ mask) & mask;
        let delta = if count == 1 {
            1
        } else {
            ((((random >> count.trailing_zeros()) as usize) & (count / 2 - 1)) << 1) | 1
        };
        self.alloc_reclaiming_each(
            (0..DEFAULT_CHOICES.min(count)).map(|step| (first + step * delta) & mask),
        )
    }

    /// Grows by one fresh index without reclaiming retired slots.
    ///
    /// Use this for arenas whose indexes remain stable and are never retired.
    #[inline]
    pub fn alloc_fresh(&self) -> Result<Block<'_, T, A, L>> {
        self.issue_fresh()
    }

    /// Tries one bucket, selected by the low bits, then issues a fresh index.
    /// A fresh index may belong to a different bucket; returns always use its index.
    /// Other buckets are not searched, even at capacity.
    pub fn alloc_from_bucket(&self, choice: usize) -> Result<Block<'_, T, A, L>> {
        self.alloc_from_buckets(std::iter::once(choice))
    }

    /// Tries caller-selected buckets in order before issuing a fresh index.
    /// Earlier buckets only try their free lists; only the last choice attempts
    /// reclamation. Choices are consumed lazily until a free block is found.
    pub fn alloc_from_buckets(
        &self,
        choices: impl IntoIterator<Item = usize>,
    ) -> Result<Block<'_, T, A, L>> {
        self.alloc_choices::<false>(choices)
    }

    /// Tries each free list and its two reclamation turns before the next choice.
    pub fn alloc_reclaiming_each(
        &self,
        choices: impl IntoIterator<Item = usize>,
    ) -> Result<Block<'_, T, A, L>> {
        self.alloc_choices::<true>(choices)
    }

    fn reclaim_pop(&self, bucket: &Bucket) -> Option<u32> {
        // Two turns can make both retired lists available. Neither waits for
        // exclusive access.
        for _ in 0..2 {
            self.recycle(bucket);
            if let Some(index) = self.pop(bucket) {
                return Some(index);
            }
        }
        None
    }

    fn alloc_choices<const EACH: bool>(
        &self,
        choices: impl IntoIterator<Item = usize>,
    ) -> Result<Block<'_, T, A, L>> {
        let mut last = None;
        for choice in choices {
            let bucket = &self.buckets[choice & (self.buckets.len() - 1)];
            if let Some(index) = self.pop(bucket) {
                return Ok(Block { pool: self, index });
            }
            if EACH {
                if let Some(index) = self.reclaim_pop(bucket) {
                    return Ok(Block { pool: self, index });
                }
            }
            last = Some(bucket);
        }
        if !EACH {
            if let Some(bucket) = last {
                if let Some(index) = self.reclaim_pop(bucket) {
                    return Ok(Block { pool: self, index });
                }
            }
        }
        self.issue_fresh()
    }

    fn issue_fresh(&self) -> Result<Block<'_, T, A, L>> {
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
        // SAFETY: fetch_update granted this caller one new writable index.
        unsafe { T::init(self.slot(index)) };
        // SAFETY: each fresh index has its atomic link initialized before use.
        unsafe { self.links.init(index) };
        pool_hit!(self, Fresh);
        Ok(Block { pool: self, index })
    }

    fn retire(&self, index: u32) {
        let bucket = &self.buckets[index as usize & (self.buckets.len() - 1)];
        loop {
            #[cfg(test)]
            if self.probe.cancelled() {
                return;
            }
            let current = bucket.current.load(SeqCst);
            let list = &bucket.deleted[current];
            let Some(_guard) = list.gate.try_read() else {
                std::hint::spin_loop();
                continue;
            };
            if bucket.current.load(SeqCst) != current {
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

    fn recycle(&self, bucket: &Bucket) {
        #[cfg(test)]
        if self.probe.cancelled() {
            return;
        }
        let current = bucket.current.load(SeqCst);
        let inactive = 1 - current;
        let list = &bucket.deleted[inactive];
        let Some(_guard) = list.gate.try_write() else {
            pool_hit!(self, RecycleBusy);
            return;
        };
        if bucket.current.load(SeqCst) != current {
            return;
        }
        // Any other switch to this list also needs its exclusive lock, so the
        // selector stays current until we change it below. Its previous users
        // have drained, including readers of items retired into the current list.
        let head = list.head.swap(NONE, SeqCst);
        let tail = list.tail.swap(NONE, SeqCst);
        if head != NONE {
            let mut free = bucket.free.load(SeqCst);
            loop {
                #[cfg(test)]
                if self.probe.cancelled() {
                    return;
                }
                self.link(tail).store(free, SeqCst);
                match bucket.free.compare_exchange_weak(free, head, SeqCst, SeqCst) {
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
        bucket.current.store(inactive, SeqCst);
    }
}

/// Ownership of one index; the borrow keeps its pool and mappings alive.
/// Inline-link callers must not overwrite the atomic link with non-atomic writes,
/// or create an exclusive reference spanning it while stale pool readers exist.
pub struct Block<'a, T: PoolItem, const ALIGN: usize, L: Links = ExternalLinks> {
    pool: &'a Pool<T, ALIGN, L>,
    index: u32,
}

impl<T: PoolItem, const A: usize, L: Links> Block<'_, T, A, L> {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn as_ptr(&self) -> *mut u8 {
        // SAFETY: index identifies a block in the reserved mapping.
        unsafe { self.pool.mapping.base.as_ptr().add(self.index as usize * size_of::<T>()) }
    }

    /// May be reused as an application value until this block is returned.
    pub fn link(&self) -> &AtomicU32 {
        self.pool.link(self.index)
    }
}

impl<T: PoolItem, const A: usize> Block<'_, T, A, ExternalLinks> {
    pub fn as_slice(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: the external link never overlaps these exclusively owned bytes.
        unsafe { std::slice::from_raw_parts(self.as_ptr().cast(), size_of::<T>()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: the handle uniquely owns the payload; pool readers only use links.
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr().cast(), size_of::<T>()) }
    }
}

impl<T: PoolItem, const A: usize, L: Links> Drop for Block<'_, T, A, L> {
    fn drop(&mut self) {
        self.pool.retire(self.index);
    }
}

pub fn free<T: PoolItem, const A: usize, L: Links>(block: Block<'_, T, A, L>) {
    drop(block);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    type Pool16 = Pool<[u8; 16], 4, ExternalLinks>;
    type Pool4 = Pool<[u8; 4], 4, ExternalLinks>;
    type InlinePool16 = Pool<[u8; 16], 4, InlineLinks<4>>;

    #[test]
    fn default_allocator_uses_configured_choices_and_grows() {
        let pool = Pool16::new(DEFAULT_CHOICES as u32 + 4, 0).unwrap();
        assert_eq!(pool.buckets.len(), DEFAULT_BUCKETS);
        let held: Vec<_> = (0..DEFAULT_CHOICES).map(|_| pool.alloc().unwrap()).collect();
        assert_eq!(
            held.iter().map(Block::index).collect::<Vec<_>>(),
            (0..DEFAULT_CHOICES as u32).collect::<Vec<_>>()
        );
        drop(held);
        assert!(pool.alloc().is_ok());
        assert!(pool.issued_blocks() >= DEFAULT_CHOICES as u32);
    }

    #[test]
    fn fresh_only() {
        let pool = Pool16::with_buckets(2, 1, 1).unwrap();
        drop(pool.alloc_from_bucket(0).unwrap());
        assert_eq!(pool.alloc_fresh().unwrap().index(), 1);
        assert!(pool.alloc_fresh().is_err());
        assert_eq!(pool.issued_blocks(), 2);

        let pool = Pool16::new(2, 0).unwrap();
        drop(pool.alloc_fresh().unwrap());
        assert_eq!(pool.issued_blocks(), 1);
    }

    #[test]
    fn growth_issues_one_slot() {
        let pool = Pool16::with_buckets(6, 0, 4).unwrap();
        let mut blocks = Vec::new();
        for index in 0..6 {
            let block = pool.alloc_from_bucket(index).unwrap();
            assert_eq!(block.index(), index as u32);
            assert_eq!(pool.issued_blocks(), index as u32 + 1);
            blocks.push(block);
        }
        assert!(pool.alloc_from_bucket(0).is_err());
        drop(blocks);

        let pool = Pool16::new(4, 0).unwrap();
        let blocks: Vec<_> = (0..4).map(|_| pool.alloc_fresh().unwrap()).collect();
        assert_eq!(blocks.iter().map(Block::index).collect::<Vec<_>>(), [0, 1, 2, 3]);
        assert_eq!(pool.issued_blocks(), 4);
        assert!(pool.alloc_fresh().is_err());
    }

    #[test]
    fn shared_and_exclusive_counts() {
        let gate = ListLock::new();
        let first = gate.try_read().unwrap();
        let second = gate.try_read().unwrap();
        assert_eq!(gate.0.load(SeqCst), 2);
        assert!(gate.try_write().is_none());
        drop(first);
        assert_eq!(gate.0.load(SeqCst), 1);
        drop(second);
        let writer = gate.try_write().unwrap();
        assert_eq!(gate.0.load(SeqCst), i64::MIN);
        assert!(gate.try_read().is_none());
        assert_eq!(gate.0.load(SeqCst), i64::MIN);
        assert!(gate.try_write().is_none());
        drop(writer);
        assert_eq!(gate.0.load(SeqCst), 0);
        drop(gate.try_read().unwrap());
        assert_eq!(gate.0.load(SeqCst), 0);
    }

    #[test]
    fn stale_reader_cannot_increment_exclusive_lock() {
        let gate = ListLock::new();
        let observed = gate.0.load(SeqCst);
        let writer = gate.try_write().unwrap();
        // A writer arrived between the reader's load and conditional increment.
        assert_eq!(
            gate.0.compare_exchange(observed, observed.wrapping_add(1), SeqCst, SeqCst),
            Err(i64::MIN)
        );
        assert_eq!(gate.0.load(SeqCst), i64::MIN);
        drop(writer);
        assert_eq!(gate.0.load(SeqCst), 0);
        drop(gate.try_read().unwrap());
        assert_eq!(gate.0.load(SeqCst), 0);
    }

    #[test]
    fn aligned_contiguous_and_reused() {
        let pool = Pool::<[u8; 65536], 65536>::with_buckets(4, 2, 1).unwrap();
        let mut blocks: Vec<_> = (0..4).map(|_| pool.alloc_from_bucket(0).unwrap()).collect();
        let base = blocks[0].as_ptr() as usize;
        for (index, block) in blocks.iter_mut().enumerate() {
            assert_eq!(block.index(), index as u32);
            assert_eq!(block.as_ptr() as usize, base + index * 65536);
            assert_eq!(block.as_ptr() as usize % 65536, 0);
            block.as_mut_slice().fill(MaybeUninit::new(index as u8));
        }
        assert!(pool.alloc_from_bucket(0).is_err());
        std::thread::scope(|scope| {
            scope.spawn(move || drop(blocks));
        });
        let blocks: Vec<_> = (0..4).map(|_| pool.alloc_from_bucket(0).unwrap()).collect();
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
        let pool = Pool16::with_buckets(4, 4, 1).unwrap();
        let first = pool.alloc_from_bucket(0).unwrap();
        let second = pool.alloc_from_bucket(0).unwrap();
        drop(first);
        drop(second);
        assert_eq!(pool.buckets[0].deleted[0].head.load(SeqCst), 1);
        assert_eq!(pool.buckets[0].deleted[0].tail.load(SeqCst), 0);
        pool.recycle(&pool.buckets[0]);
        pool.recycle(&pool.buckets[0]);
        assert_eq!(pool.buckets[0].free.load(SeqCst), 1);
        assert_eq!(pool.link(1).load(SeqCst), 0);
        assert_eq!(pool.link(0).load(SeqCst), 2);
        assert_eq!(pool.link(2).load(SeqCst), 3);
        assert_eq!(pool.link(3).load(SeqCst), NONE);
        let blocks: Vec<_> = (0..4).map(|_| pool.alloc_from_bucket(0).unwrap()).collect();
        assert_eq!(blocks.iter().map(Block::index).collect::<Vec<_>>(), [1, 0, 2, 3]);
    }

    #[test]
    fn stalled_reader_delays_reuse_but_not_growth_or_retirement() {
        let pool = InlinePool16::with_buckets(3, 1, 1).unwrap();
        let old = pool.buckets[0].current.load(SeqCst);
        let guard = pool.buckets[0].deleted[old].gate.try_read().unwrap();
        let stale_head = pool.buckets[0].free.load(SeqCst);
        let first = pool.alloc_from_bucket(0).unwrap();
        first.link().store(0xdeadbeef, SeqCst);
        let stale_next = pool.link(stale_head).load(SeqCst);
        assert_eq!(stale_next, 0xdeadbeef);
        assert!(
            pool.buckets[0].free.compare_exchange(stale_head, stale_next, SeqCst, SeqCst).is_err()
        );
        drop(first);
        let second = pool.alloc_from_bucket(0).unwrap();
        assert_eq!(second.index(), 1);
        assert_ne!(pool.buckets[0].current.load(SeqCst), old);
        drop(second);
        let third = pool.alloc_from_bucket(0).unwrap();
        assert_eq!(third.index(), 2);
        assert_eq!(pool.buckets[0].current.load(SeqCst), 1 - old);
        assert!(pool.alloc_from_bucket(0).is_err());
        drop(guard);
        let reused: Vec<_> = (0..2).map(|_| pool.alloc_from_bucket(0).unwrap()).collect();
        assert_eq!(reused.iter().map(Block::index).collect::<HashSet<_>>(), HashSet::from([0, 1]));
    }

    #[test]
    fn older_group_protects_items_retired_in_new_group() {
        let pool = Pool16::with_buckets(2, 1, 1).unwrap();
        let old = pool.buckets[0].current.load(SeqCst);
        let guard = pool.buckets[0].deleted[old].gate.try_read().unwrap();
        let stale_head = pool.buckets[0].free.load(SeqCst);
        pool.recycle(&pool.buckets[0]);
        assert_eq!(pool.buckets[0].current.load(SeqCst), 1 - old);
        let block = pool.alloc_from_bucket(0).unwrap();
        assert_eq!(block.index(), stale_head);
        drop(block); // Retires into the NEW group, with an OLD reader outstanding.
        let fresh = pool.alloc_from_bucket(0).unwrap();
        assert_ne!(fresh.index(), stale_head);
        assert!(pool.alloc_from_bucket(0).is_err());
        drop(guard);
        assert_eq!(pool.alloc_from_bucket(0).unwrap().index(), stale_head);
    }

    #[test]
    fn bucket_returns_preserve_identity_and_grace() {
        let pool = Pool16::with_buckets(64, 64, 32).unwrap();
        let bucket = &pool.buckets[7];
        let guard = bucket.deleted[0].gate.try_read().unwrap();
        let block = pool.alloc_from_bucket(7).unwrap();
        assert_eq!(block.index(), 7);
        drop(block);
        let other = pool.alloc_from_bucket(7).unwrap();
        assert_eq!(other.index(), 39);
        drop(other);
        assert!(pool.alloc_from_bucket(7).is_err());
        // Bucket 8 can still recycle despite the stalled participant in bucket 7.
        for _ in 0..4 {
            drop(pool.alloc_from_bucket(8).unwrap());
        }
        drop(guard);
        let reused = pool.alloc_from_bucket(7).unwrap();
        assert_eq!(reused.index() & 31, 7);
        assert_eq!(pool.issued_blocks(), 64);
    }

    #[test]
    fn second_bucket_avoids_growth_and_choices_are_lazy() {
        let pool = Pool16::with_buckets(64, 1, 32).unwrap();
        let mut draws = 0;
        let choices = [31, 0, 1].into_iter().inspect(|_| draws += 1);
        let block = pool.alloc_from_buckets(choices).unwrap();
        assert_eq!(draws, 2);
        assert_eq!(block.index(), 0);
        assert_eq!(pool.issued_blocks(), 1);
        drop(block);
        assert_eq!(pool.alloc_from_buckets([31, 0]).unwrap().index(), 0);
        assert_eq!(pool.issued_blocks(), 1);
    }

    #[test]
    fn first_empty_bucket_is_not_reclaimed() {
        let pool = Pool16::with_buckets(64, 2, 32).unwrap();
        drop(pool.alloc_from_bucket(0).unwrap());
        let first = &pool.buckets[0];
        let current = first.current.load(SeqCst);
        let head = first.deleted[current].head.load(SeqCst);
        assert_eq!(head, 0);
        let _writer = first.deleted[current].gate.try_write().unwrap();
        // No lock or rotation of the first bucket, even though it has a retiree.
        let block = pool.alloc_from_buckets([0, 1]).unwrap();
        assert_eq!(block.index(), 1);
        assert_eq!(first.current.load(SeqCst), current);
        assert_eq!(first.deleted[current].head.load(SeqCst), head);
        assert_eq!(pool.issued_blocks(), 2);
        // If the second bucket is empty, grow instead of reclaiming the first.
        assert_eq!(pool.alloc_from_buckets([0, 2]).unwrap().index(), 2);
        assert_eq!(first.current.load(SeqCst), current);
        assert_eq!(first.deleted[current].head.load(SeqCst), head);
    }

    #[test]
    fn reclaim_each_reuses_first_retiree_before_next_choice() {
        let pool = Pool16::with_buckets(32, 1, 32).unwrap();
        drop(pool.alloc_from_bucket(0).unwrap());
        let mut tried = 0;
        let block =
            pool.alloc_reclaiming_each([0, 1, 2, 3].into_iter().inspect(|_| tried += 1)).unwrap();
        assert_eq!(block.index(), 0);
        assert_eq!(tried, 1);
        assert_eq!(pool.issued_blocks(), 1);
    }

    #[test]
    fn empty_pop_does_not_take_shared_lock() {
        let pool = Pool16::with_buckets(1, 0, 1).unwrap();
        let bucket = &pool.buckets[0];
        let _writer = bucket.deleted[0].gate.try_write().unwrap();
        assert_eq!(pool.pop(bucket), None);
    }

    #[test]
    fn fresh_index_returns_to_its_own_bucket() {
        let pool = Pool16::with_buckets(1, 0, 32).unwrap();
        let block = pool.alloc_from_bucket(31).unwrap();
        assert_eq!(block.index(), 0);
        drop(block);
        assert!(pool.alloc_from_bucket(31).is_err());
        assert_eq!(pool.alloc_from_bucket(0).unwrap().index(), 0);
        assert_eq!(pool.issued_blocks(), 1);
        assert!(Pool16::with_buckets(1, 0, 0).is_err());
        assert!(Pool16::with_buckets(1, 0, 3).is_err());
    }

    #[test]
    fn validates_geometry_and_limits() {
        assert!(Pool::<[u8; 4], 0>::with_buckets(1, 0, 1).is_err());
        assert!(Pool::<[u8; 12], 3>::with_buckets(1, 0, 1).is_err());
        assert!(Pool::<[u8; 6], 4>::with_buckets(1, 0, 1).is_err());
        assert!(Pool::<[u8; 0], 4>::with_buckets(1, 0, 1).is_err());
        assert!(Pool4::with_buckets(0, 0, 1).is_err());
        assert!(Pool4::with_buckets(1, 2, 1).is_err());
        assert!(Pool::<[u8; 1usize << 40], 1>::with_buckets(u32::MAX, 0, 1).is_err());
        assert!(Pool::<[u8; 4], 4, InlineLinks<4>>::with_buckets(1, 0, 1).is_err());
        assert!(Pool::<[u8; 8], 4, InlineLinks<1>>::with_buckets(1, 0, 1).is_err());
        #[allow(dead_code)]
        #[repr(align(8))]
        struct Aligned([u8; 8]);
        // SAFETY: this test only checks the alignment rejection path.
        unsafe impl PoolItem for Aligned {}
        assert!(Pool::<Aligned, 4>::with_buckets(1, 0, 1).is_err());
        let pool = Pool4::with_buckets(1, 0, 1).unwrap();
        assert_eq!(pool.alloc_from_bucket(0).unwrap().index(), 0);
        assert_eq!(pool.alloc_from_bucket(0).unwrap().index(), 0);
    }
}
