//! Fixed-size pool backed by an atomic bitmap.
use super::{Error, Mapping, PoolItem, Result};
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, size_of};
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering, Ordering::SeqCst};

const BITS: usize = 64;
const INTERLEAVE_BITS: usize = 4096;
const INTERLEAVE_WORDS: usize = INTERLEAVE_BITS / BITS;
const PROBE_LIMIT: usize = 2;
// 64 * ln(2^64) ≈ 2839.1, rounded up to 2840 probes.
// At 1/64 free words, the miss chance is < 2^-64; at 1/128, about 2^-32.
const DEFAULT_RANDOM_PROBES: usize = 2840;
const REFILL_BATCH: u32 = 64;
const REFILL_BUSY: u32 = 1 << 31;
const REFILL_MASK: u32 = !REFILL_BUSY;

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(super) enum Race {
    ClaimLost,
    FreeRace,
    DrainRace,
    EmptyScan,
    FreshRace,
    ScheduleRace,
    RefillBusy,
    RefillCASLoss,
    RefillDone,
}

#[cfg(test)]
impl Race {
    pub(super) const ALL: [Self; 9] = [
        Self::ClaimLost,
        Self::FreeRace,
        Self::DrainRace,
        Self::EmptyScan,
        Self::FreshRace,
        Self::ScheduleRace,
        Self::RefillBusy,
        Self::RefillCASLoss,
        Self::RefillDone,
    ];

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::ClaimLost => "claim CAS lost",
            Self::FreeRace => "free raced with claim",
            Self::DrainRace => "candidate drained",
            Self::EmptyScan => "probe scan empty",
            Self::FreshRace => "concurrent fresh issue",
            Self::ScheduleRace => "refill schedule race",
            Self::RefillBusy => "refill already busy",
            Self::RefillCASLoss => "refill claim CAS lost",
            Self::RefillDone => "refill published blocks",
        }
    }
}

#[cfg(test)]
thread_local! {
    static RACE_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn clear_hits() {
    RACE_HITS.with(|hits| hits.set(0));
}

#[cfg(test)]
pub(super) fn take_hits() -> u64 {
    RACE_HITS.with(|hits| hits.replace(0))
}

#[cfg(test)]
#[inline]
fn record_race(race: Race) {
    let flag = 1u64 << race as usize;
    RACE_HITS.with(|hits| {
        let seen = hits.get();
        if seen & flag == 0 {
            hits.set(seen | flag);
        }
    });
}

#[cfg(test)]
macro_rules! race_hit {
    ($race:ident) => {
        record_race(Race::$race);
    };
}

/// Spreads each 4096-index group over 64 bitmap words.
#[inline]
fn bit_at(index: usize) -> (usize, u32) {
    let group = index / INTERLEAVE_BITS;
    let within = index % INTERLEAVE_BITS;
    (group * INTERLEAVE_WORDS + within % BITS, (within / BITS) as u32)
}

#[inline]
fn word_count(slots: usize) -> usize {
    let groups = slots / INTERLEAVE_BITS;
    let rem = slots % INTERLEAVE_BITS;
    groups * INTERLEAVE_WORDS + rem.min(INTERLEAVE_WORDS)
}

#[inline]
fn random_word(words: usize, random: u64) -> usize {
    ((random as u128 * words as u128) >> 64) as usize
}

/// A fixed-size pool whose free slots are represented by one bit per index.
/// Bit 1 means free; zero means allocated or not yet issued. Each 4096-index
/// group is transposed across 64 bitmap words.
pub struct Pool<T: PoolItem, const ALIGN: usize> {
    pub(super) mapping: Mapping,
    bits: Mapping,
    max_blocks: u32,
    /// Reserved prefix; growth may advance this before new slots are initialized.
    next: AtomicU32,
    refill: AtomicU32,
    growth: Mutex<()>,
    random_probes: usize,
    _slot: PhantomData<fn() -> T>,
}

// SAFETY: every allocated slot has one owner; bitmap updates are atomic.
unsafe impl<T: PoolItem + Send, const A: usize> Send for Pool<T, A> {}
unsafe impl<T: PoolItem + Send + Sync, const A: usize> Sync for Pool<T, A> {}

impl<T: PoolItem, const A: usize> Pool<T, A> {
    pub fn new(max_blocks: u32, initial_blocks: u32) -> Result<Self> {
        Self::with_random_probes(max_blocks, initial_blocks, DEFAULT_RANDOM_PROBES)
    }

    /// Create a pool with a custom random-probe limit. Probe words may repeat.
    /// A zero `initial_blocks` value is treated as one.
    pub fn with_random_probes(
        max_blocks: u32,
        initial_blocks: u32,
        random_probes: usize,
    ) -> Result<Self> {
        if !A.is_power_of_two()
            || size_of::<T>() == 0
            || size_of::<T>() % A != 0
            || align_of::<T>() > A
        {
            return Err(Error::new(
                "slot size must be a positive multiple of power-of-two alignment at least as large as the slot alignment",
            ));
        }
        if max_blocks == 0 {
            return Err(Error::new("pool needs max_blocks >= 1"));
        }
        if initial_blocks > max_blocks {
            return Err(Error::new("pool needs initial_blocks <= max_blocks"));
        }
        let initial_blocks = initial_blocks.max(1);
        if random_probes == 0 {
            return Err(Error::new("pool needs at least one random bitmap probe"));
        }
        let total_bytes = size_of::<T>()
            .checked_mul(max_blocks as usize)
            .ok_or_else(|| Error::new("block pool is too large"))?;
        let mapping = Mapping::new(total_bytes, A)?;
        let slots = (max_blocks as usize)
            .checked_next_power_of_two()
            .ok_or_else(|| Error::new("bitmap capacity is too large"))?;
        let bytes = word_count(slots)
            .checked_mul(size_of::<u64>())
            .ok_or_else(|| Error::new("bitmap is too large"))?;
        let bits = Mapping::new(bytes, align_of::<AtomicU64>())?;
        let pool = Self {
            mapping,
            bits,
            max_blocks,
            next: AtomicU32::new(initial_blocks),
            refill: AtomicU32::new(0),
            growth: Mutex::new(()),
            random_probes,
            _slot: PhantomData,
        };
        for index in 0..initial_blocks {
            // SAFETY: this is a fresh slot in the data mapping.
            unsafe { T::init(pool.slot(index)) };
            pool.set_free(index);
        }
        Ok(pool)
    }

    #[inline]
    fn slot(&self, index: u32) -> *mut T {
        // SAFETY: index is within the reserved mapping and slots use size_of::<T>() stride.
        unsafe { self.mapping.base.as_ptr().add(index as usize * size_of::<T>()).cast::<T>() }
    }

    #[inline]
    fn word(&self, index: usize) -> &AtomicU64 {
        // MAP_ANONYMOUS gives zeroed memory. AtomicU64 has u64's size and bit validity;
        // the mapping is aligned and remains live for the pool's lifetime.
        let ptr = unsafe { self.bits.base.as_ptr().cast::<u64>().add(index) };
        // SAFETY: ptr is aligned, points into the live zeroed bitmap mapping, and every
        // access to the location uses this atomic view.
        unsafe { AtomicU64::from_ptr(ptr) }
    }

    #[inline]
    fn set_free(&self, index: u32) {
        let (word, bit) = bit_at(index as usize);
        self.word(word).fetch_or(1u64 << bit, SeqCst);
    }

    fn set_free_range(&self, start: u32, count: u32) {
        for index in start..start + count {
            self.set_free(index);
        }
    }

    fn schedule_refill(&self, count: u32) -> bool {
        if count == 0 {
            return false;
        }
        let count = count.min(REFILL_MASK);
        match self.refill.compare_exchange(0, count, SeqCst, SeqCst) {
            Ok(_) => true,
            Err(_) => {
                #[cfg(test)]
                race_hit!(ScheduleRace);
                false
            }
        }
    }

    /// Pays at most one fixed-size part of the reserve target. The busy bit
    /// keeps a concurrent full scan from scheduling the same reserve twice.
    fn refill(&self, len: Option<&AtomicUsize>) {
        let state = self.refill.load(SeqCst);
        if state == 0 {
            return;
        }
        if state & REFILL_BUSY != 0 {
            #[cfg(test)]
            race_hit!(RefillBusy);
            return;
        }
        let pending = state & REFILL_MASK;
        let count = pending.min(REFILL_BATCH);
        if self
            .refill
            .compare_exchange(state, REFILL_BUSY | (pending - count), SeqCst, SeqCst)
            .is_err()
        {
            #[cfg(test)]
            race_hit!(RefillCASLoss);
            return;
        }
        let _growth = self.growth.lock().expect("pool growth lock");

        let start = loop {
            let next = self.next.load(SeqCst);
            let count = count.min(self.max_blocks.saturating_sub(next));
            if count == 0 {
                break None;
            }
            if self.next.compare_exchange(next, next + count, SeqCst, SeqCst).is_ok() {
                break Some((next, count));
            }
        };

        if let Some((start, count)) = start {
            for index in start..start + count {
                // SAFETY: this caller reserved a unique fresh index range.
                unsafe { T::init(self.slot(index)) };
            }
            if let Some(len) = len {
                len.fetch_max((start + count) as usize, Ordering::Release);
            }
            self.set_free_range(start, count);
            #[cfg(test)]
            race_hit!(RefillDone);
        }
        self.refill.fetch_and(REFILL_MASK, SeqCst);
    }

    #[inline]
    fn claim(&self, word: usize, bit: u32) -> u32 {
        let group = word / INTERLEAVE_WORDS;
        let within = word % INTERLEAVE_WORDS;
        (group * INTERLEAVE_BITS + bit as usize * BITS + within) as u32
    }

    #[inline]
    fn take_probe(&self, at: usize) -> u64 {
        self.word(at).load(SeqCst)
    }

    #[inline]
    fn take_word_from(&self, at: usize, mut free: u64) -> Option<u32> {
        let word = self.word(at);
        while free != 0 {
            let bit = free.trailing_zeros();
            match word.compare_exchange_weak(free, free & !(1u64 << bit), SeqCst, SeqCst) {
                Ok(_) => return Some(self.claim(at, bit)),
                Err(actual) => {
                    if actual != free {
                        #[cfg(test)]
                        race_hit!(ClaimLost);
                        if actual & !free != 0 {
                            #[cfg(test)]
                            race_hit!(FreeRace);
                        }
                    }
                    free = actual;
                }
            }
        }
        #[cfg(test)]
        race_hit!(DrainRace);
        None
    }

    fn take_index(&self, index: u32) -> bool {
        let (at, bit) = bit_at(index as usize);
        let mask = 1u64 << bit;
        let word = self.word(at);
        let mut free = word.load(SeqCst);
        loop {
            if free & mask == 0 {
                return false;
            }
            match word.compare_exchange_weak(free, free & !mask, SeqCst, SeqCst) {
                Ok(_) => return true,
                Err(actual) => free = actual,
            }
        }
    }

    /// Number of indexes reserved so far, including indexes being initialized.
    /// Concurrent growth can reserve indexes before publishing them as free.
    pub fn issued_blocks(&self) -> u32 {
        self.next.load(SeqCst)
    }

    /// Stable pointer to slot zero, initialized during construction.
    pub fn base_ptr(&self) -> NonNull<T> {
        self.mapping.base.cast()
    }

    /// Whether an issued index is currently available for allocation.
    pub fn is_free(&self, index: u32) -> Result<bool> {
        if index >= self.max_blocks || index >= self.issued_blocks() {
            return Err(Error::new("block pool index is outside the issued range"));
        }
        let (word, bit) = bit_at(index as usize);
        Ok(self.word(word).load(SeqCst) & (1u64 << bit) != 0)
    }

    /// Release a claimed index after its `Block` handle has been forgotten.
    /// Exclusive access prevents releasing an index while a live handle can
    /// still refer to it.
    pub fn release(&mut self, index: u32) -> Result<()> {
        if index >= self.max_blocks || index >= self.issued_blocks() {
            return Err(Error::new("block pool index is outside the issued range"));
        }
        let (word, bit) = bit_at(index as usize);
        let mask = 1u64 << bit;
        if self.word(word).fetch_or(mask, SeqCst) & mask != 0 {
            return Err(Error::new("block pool index is already free"));
        }
        Ok(())
    }

    /// Reserve one exact index. If `index` is beyond the issued range, initialize
    /// through it and leave the intervening indexes free. Requires exclusive pool
    /// access; a previously allocated or out-of-capacity index fails.
    pub fn reserve(&mut self, index: u32) -> Result<Block<'_, T, A>> {
        self.reserve_inner(index, None)
    }

    /// Claim an already initialized slot without extending the issued prefix.
    /// Callers must ensure no other owner can use the claimed slot.
    pub fn reserve_issued(&self, index: u32) -> Result<Block<'_, T, A>> {
        if index >= self.issued_blocks() {
            return Err(Error::new("block pool index is outside the issued range"));
        }
        if !self.take_index(index) {
            return Err(Error::new("block pool index is already allocated"));
        }
        Ok(Block { pool: self, index })
    }

    /// Reserve one exact index, publishing newly initialized slots to `len`
    /// before exposing any gaps as free in the bitmap.
    pub fn reserve_with_len(&mut self, index: u32, len: &AtomicUsize) -> Result<Block<'_, T, A>> {
        self.reserve_inner(index, Some(len))
    }

    fn reserve_inner(&mut self, index: u32, len: Option<&AtomicUsize>) -> Result<Block<'_, T, A>> {
        if index >= self.max_blocks {
            return Err(Error::new("block pool index is out of range"));
        }

        let issued = self.next.load(SeqCst);
        if index < issued {
            if !self.take_index(index) {
                return Err(Error::new("block pool index is already allocated"));
            }
        } else {
            for slot in issued..=index {
                // SAFETY: exclusive access reserves each previously unissued slot.
                unsafe { T::init(self.slot(slot)) };
            }
            self.next.store(index + 1, SeqCst);
            if let Some(len) = len {
                len.fetch_max(index as usize + 1, Ordering::Release);
            }
            self.set_free_range(issued, index - issued);
        }

        Ok(Block { pool: self, index })
    }

    pub const fn block_bytes(&self) -> usize {
        size_of::<T>()
    }

    /// Find the index of an issued block from its starting address.
    ///
    /// The pointer must point to the first byte of a block in this pool. This
    /// checks the pool's mapping, block alignment, and issued range; it does not
    /// establish ownership or prove that the block is currently allocated.
    pub fn index_of(&self, ptr: *const u8) -> Result<u32> {
        let base = self.mapping.base.as_ptr() as usize;
        let address = ptr as usize;
        let offset = address
            .checked_sub(base)
            .ok_or_else(|| Error::new("pointer is below the block pool mapping"))?;
        let block_bytes = size_of::<T>();
        if offset % block_bytes != 0 {
            return Err(Error::new("pointer is not at a block boundary"));
        }
        let index = offset / block_bytes;
        if index >= self.max_blocks as usize || index >= self.issued_blocks() as usize {
            return Err(Error::new("pointer is outside the issued block pool"));
        }
        Ok(index as u32)
    }

    /// Searches the bitmap with the configured probe policy, then grows if needed.
    #[inline]
    pub fn alloc(&self) -> Result<Block<'_, T, A>> {
        self.alloc_inner(None)
    }

    /// Allocate while publishing any newly initialized slots to `len` before
    /// making spare slots available in the bitmap.
    pub fn alloc_with_len(&self, len: &AtomicUsize) -> Result<Block<'_, T, A>> {
        self.alloc_inner(Some(len))
    }

    fn alloc_inner(&self, len: Option<&AtomicUsize>) -> Result<Block<'_, T, A>> {
        let random = || random::thread_u64();
        let mut issued;
        loop {
            issued = self.next.load(SeqCst);
            let words = word_count(issued as usize);
            let mut probes = 1;
            let mut at = random_word(words, random());
            let mut free = self.take_probe(at);

            while free == 0 && probes < self.random_probes {
                at = random_word(words, random());
                probes += 1;
                free = self.take_probe(at);
            }

            if free == 0 {
                break;
            }

            if probes == 1 && free.count_ones() == 1 {
                let other_at = random_word(words, random());
                probes += 1;
                let other_free = self.take_probe(other_at);
                if other_free.count_ones() > 1 {
                    at = other_at;
                    free = other_free;
                }
            }

            if let Some(index) = self.take_word_from(at, free) {
                if probes > PROBE_LIMIT {
                    self.refill(len);
                }
                return Ok(Block { pool: self, index });
            }
            // The selected word was emptied since the probe. Start a fresh
            // sequence with the current issued bitmap length.
        }

        #[cfg(test)]
        race_hit!(EmptyScan);

        let block = self.alloc_fresh_inner(len)?;
        let issued_after = self.next.load(SeqCst);
        #[cfg(test)]
        if issued_after != issued + 1 {
            race_hit!(FreshRace);
        }
        let scheduled =
            issued_after == issued + 1 && self.schedule_refill(issued / (2 * BITS as u32));
        if !scheduled {
            // A failed probe search allocates one fresh block for its caller. If
            // another call already scheduled reserve work, help pay it here.
            self.refill(len);
        }
        Ok(block)
    }

    /// Grows by one fresh index without searching or reusing free indexes.
    pub fn alloc_fresh(&self) -> Result<Block<'_, T, A>> {
        self.alloc_fresh_inner(None)
    }

    fn alloc_fresh_inner(&self, len: Option<&AtomicUsize>) -> Result<Block<'_, T, A>> {
        let _growth = self.growth.lock().expect("pool growth lock");
        let index = self
            .next
            .fetch_update(SeqCst, SeqCst, |next| (next < self.max_blocks).then(|| next + 1))
            .map_err(|_| Error::new("block pool exhausted"))?;
        // SAFETY: fetch_update granted this caller a unique fresh index.
        unsafe { T::init(self.slot(index)) };
        if let Some(len) = len {
            len.fetch_max(index as usize + 1, Ordering::Release);
        }
        Ok(Block { pool: self, index })
    }

    pub(super) fn retire(&self, index: u32) {
        self.set_free(index);
    }
}

/// Ownership of one bitmap-pool index; dropping it makes the index available.
pub struct Block<'a, T: PoolItem, const ALIGN: usize> {
    pool: &'a Pool<T, ALIGN>,
    index: u32,
}

impl<T: PoolItem, const A: usize> Block<'_, T, A> {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn as_ptr(&self) -> *mut u8 {
        // SAFETY: index identifies a slot in the reserved mapping.
        unsafe { self.pool.mapping.base.as_ptr().add(self.index as usize * size_of::<T>()) }
    }

    pub fn as_slice(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: the block uniquely owns its slot until dropped.
        unsafe { std::slice::from_raw_parts(self.as_ptr().cast(), size_of::<T>()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: the block uniquely owns its slot until dropped.
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr().cast(), size_of::<T>()) }
    }
}

impl<T: PoolItem, const A: usize> Drop for Block<'_, T, A> {
    fn drop(&mut self) {
        self.pool.retire(self.index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, OnceLock};

    #[allow(dead_code)]
    struct Gated([u8; 8]);

    static INIT_COUNT: AtomicUsize = AtomicUsize::new(0);
    static INIT_PAUSE: OnceLock<(Barrier, Barrier)> = OnceLock::new();

    unsafe impl PoolItem for Gated {
        unsafe fn init(slot: *mut Self) {
            // SAFETY: the pool provides aligned, fresh storage for Gated.
            unsafe { slot.write(Self([0; 8])) };
            if INIT_COUNT.fetch_add(1, Ordering::Relaxed) == 1 {
                let (entered, resume) =
                    INIT_PAUSE.get_or_init(|| (Barrier::new(2), Barrier::new(2)));
                entered.wait();
                resume.wait();
            }
        }
    }

    #[test]
    fn extent_len_can_lag_reserved_slots_until_init_finishes() {
        INIT_COUNT.store(0, Ordering::Relaxed);
        let (entered, resume) = INIT_PAUSE.get_or_init(|| (Barrier::new(2), Barrier::new(2)));
        let pool = Pool::<Gated, 8>::new(8, 1).unwrap();
        let len = AtomicUsize::new(1);
        assert!(pool.schedule_refill(2));

        std::thread::scope(|scope| {
            let grow = scope.spawn(|| pool.refill(Some(&len)));
            entered.wait();
            let reserved = pool.issued_blocks();
            let initialized = len.load(Ordering::Acquire);
            let first_free = pool.is_free(1).unwrap();
            let second_free = pool.is_free(2).unwrap();
            resume.wait();
            grow.join().unwrap();

            assert_eq!(reserved, 3);
            assert_eq!(initialized, 1);
            assert!(!first_free);
            assert!(!second_free);
        });

        assert_eq!(len.load(Ordering::Acquire), 3);
        assert!(pool.is_free(1).unwrap());
        assert!(pool.is_free(2).unwrap());
    }

    #[test]
    fn interleaved_bits() {
        for i in 0..64 {
            assert_eq!(bit_at(i), (i, 0));
        }
        for i in 0..64 {
            assert_eq!(bit_at(i + 64), (i, 1));
        }
        assert_eq!(bit_at(4095), (63, 63));
        assert_eq!(bit_at(4096), (64, 0));
        let pool = Pool::<[u8; 64], 64>::new(1, 0).unwrap();
        for i in 0..8192 {
            let (word, bit) = bit_at(i);
            assert_eq!(pool.claim(word, bit), i as u32);
        }
        assert_eq!(word_count(64), 64);
        assert_eq!(word_count(65), 64);
        assert_eq!(word_count(4096), 64);
        assert_eq!(word_count(4097), 65);
    }

    #[test]
    fn random_word_maps_values_into_the_full_range() {
        for words in 1..=257 {
            assert_eq!(random_word(words, 0), 0);
            assert_eq!(random_word(words, u64::MAX), words - 1);
        }
    }

    #[test]
    fn failed_random_probes_schedule_one_over_128_spare() {
        let pool = Pool::<[u8; 64], 64>::with_random_probes(256, 0, 6000).unwrap();
        let mut held = vec![pool.alloc().unwrap()];
        held.extend((1..128).map(|_| pool.alloc_fresh().unwrap()));

        let first = pool.alloc().unwrap();
        assert_eq!(pool.index_of(first.as_ptr()).unwrap(), 128);
        assert_eq!(pool.issued_blocks(), 129);

        let second = pool.alloc().unwrap();
        assert_eq!(pool.index_of(second.as_ptr()).unwrap(), 129);
        assert_eq!(pool.issued_blocks(), 131);

        let a = pool.alloc().unwrap();
        let b = pool.alloc().unwrap();
        let mut indexes = [pool.index_of(a.as_ptr()).unwrap(), pool.index_of(b.as_ptr()).unwrap()];
        indexes.sort_unstable();
        assert_eq!(indexes, [130, 131]);
        drop((held, first, second, a, b));
    }

    #[test]
    fn random_probe_reuses_a_free_block_without_growth() {
        let pool = Pool::<[u8; 64], 64>::with_random_probes(256, 128, 1).unwrap();
        let block = pool.alloc().unwrap();
        assert!(pool.index_of(block.as_ptr()).unwrap() < 128);
        assert_eq!(pool.issued_blocks(), 128);
    }

    #[test]
    fn reserve_matches_a_block_from_another_pool() {
        let source = Pool::<[u8; 64], 64>::new(8, 0).unwrap();
        let mut target = Pool::<[u8; 64], 64>::new(8, 0).unwrap();
        let source_block = source.alloc().unwrap();
        let index = source_block.index();

        let reserved = target.reserve(index).unwrap();
        assert_eq!(reserved.index(), index);
        std::mem::forget(reserved);
        assert!(target.reserve(index).is_err());
    }

    #[test]
    fn release_once() {
        let mut pool = Pool::<[u8; 64], 64>::new(8, 0).unwrap();
        let block = pool.alloc().unwrap();
        let index = block.index();
        std::mem::forget(block);

        assert!(!pool.is_free(index).unwrap());
        pool.release(index).unwrap();
        assert!(pool.is_free(index).unwrap());
        assert!(pool.release(index).is_err());
        let reused = pool.reserve(index).unwrap();
        assert_eq!(reused.index(), index);
    }

    #[test]
    fn reserve_grows_through_a_block_from_another_pool() {
        let source = Pool::<[u8; 64], 64>::new(16, 0).unwrap();
        let mut source_blocks = vec![source.alloc().unwrap()];
        source_blocks.extend((1..8).map(|_| source.alloc_fresh().unwrap()));
        let index = source_blocks.last().unwrap().index();
        assert_eq!(index, 7);

        let mut target = Pool::<[u8; 64], 64>::new(16, 0).unwrap();
        let reserved = target.reserve(index).unwrap();
        assert_eq!(reserved.index(), index);
        std::mem::forget(reserved);
        assert_eq!(target.issued_blocks(), index + 1);
        assert!(target.reserve(16).is_err());

        let held: Vec<_> = (0..index).map(|_| target.alloc().unwrap()).collect();
        let mut found: Vec<_> = held.iter().map(|block| block.index()).collect();
        found.sort_unstable();
        assert_eq!(found, (0..index).collect::<Vec<_>>());
    }

    #[test]
    fn reuse_all() {
        let pool = Pool::<[u8; 64], 64>::new(130, 0).unwrap();
        let mut first = vec![pool.alloc().unwrap()];
        first.extend((1..130).map(|_| pool.alloc_fresh().unwrap()));
        assert!(pool.alloc_fresh().is_err());
        drop(first);
        let mut seen = vec![false; 130];
        let held: Vec<_> = (0..130)
            .map(|_| {
                let block = pool.alloc().unwrap();
                let index = pool.index_of(block.as_ptr()).unwrap() as usize;
                assert!(!seen[index]);
                seen[index] = true;
                block
            })
            .collect();
        assert!(pool.alloc().is_err());
        assert!(seen.into_iter().all(|found| found));
        drop(held);
        assert_eq!(pool.issued_blocks(), 130);
    }

    #[test]
    fn rejects_zero_random_probes() {
        assert!(Pool::<[u8; 64], 64>::with_random_probes(1, 0, 0).is_err());
    }

    #[test]
    fn zero_initial_blocks_means_one() {
        let pool = Pool::<[u8; 64], 64>::new(3, 0).unwrap();
        assert_eq!(pool.issued_blocks(), 1);
        let block = pool.alloc().unwrap();
        assert_eq!(pool.index_of(block.as_ptr()).unwrap(), 0);
        assert_eq!(pool.issued_blocks(), 1);
    }

    #[test]
    fn index_of_checks_pool_and_slot_boundaries() {
        let pool = Pool::<[u8; 64], 64>::new(4, 0).unwrap();
        let base = pool.mapping.base.as_ptr();
        assert_eq!(pool.index_of(base).unwrap(), 0);
        assert!(pool.index_of(base.wrapping_add(1)).is_err());
        assert!(pool.index_of(base.wrapping_add(64)).is_err());
        assert!(pool.index_of(base.wrapping_add(4 * 64)).is_err());
        assert!(pool.index_of((base as usize - 1) as *const u8).is_err());
        let other = Pool::<[u8; 64], 64>::new(4, 0).unwrap();
        let other_block = other.alloc().unwrap();
        assert!(pool.index_of(other_block.as_ptr()).is_err());
    }

    #[test]
    fn zero_capacity_is_rejected() {
        assert!(Pool::<[u8; 64], 64>::new(0, 0).is_err());
    }

    #[test]
    fn repeated_empty_probes_do_not_schedule_unbounded_growth() {
        const INITIAL: u32 = 4096;
        let pool = Pool::<[u8; 64], 64>::new(8192, 0).unwrap();
        let mut held = vec![pool.alloc().unwrap()];
        held.extend((1..INITIAL).map(|_| pool.alloc_fresh().unwrap()));

        // Empty probes give the caller one fresh block and schedule reserve.
        held.push(pool.alloc().unwrap());
        assert_eq!(pool.issued_blocks(), INITIAL + 1);

        // A later empty probe search gets its own block and helps pay that reserve.
        held.push(pool.alloc().unwrap());
        let issued = pool.issued_blocks();
        assert_eq!(issued, INITIAL + 2 + INITIAL / (2 * BITS as u32));

        // The reserve is now paid. Repeated sparse probes may be long, but
        // cannot create more capacity unless the available blocks are used up.
        for _ in 0..20_000 {
            drop(pool.alloc().unwrap());
        }
        assert_eq!(pool.issued_blocks(), issued);
        drop(held);
    }

    #[test]
    fn concurrent_exhaustion_schedules_one_reserve() {
        const INITIAL: u32 = 4096;
        const THREADS: usize = 8;
        let pool = Pool::<[u8; 64], 64>::new(8192, 0).unwrap();
        let mut held = vec![pool.alloc().unwrap()];
        held.extend((1..INITIAL).map(|_| pool.alloc_fresh().unwrap()));
        let start = std::sync::Barrier::new(THREADS + 1);

        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..THREADS {
                let start = &start;
                let pool = &pool;
                workers.push(scope.spawn(move || {
                    start.wait();
                    pool.alloc().unwrap()
                }));
            }
            start.wait();
            held.extend(workers.into_iter().map(|worker| worker.join().unwrap()));
        });

        // One reserve is scheduled for this exhaustion. Concurrent demand can
        // add user-owned slots, but cannot each schedule another full reserve.
        assert!(
            pool.issued_blocks() <= INITIAL + THREADS as u32 + INITIAL / (2 * BITS as u32),
            "issued {} blocks",
            pool.issued_blocks()
        );
        drop(held);
    }
}
