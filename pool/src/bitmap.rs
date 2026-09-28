//! Fixed-size pool backed by an atomic bitmap.
use super::{Error, Mapping, PoolItem, Result};
use std::marker::PhantomData;
use std::mem::{MaybeUninit, align_of, size_of};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::SeqCst};

const BITS: usize = 64;
const INTERLEAVE_BITS: usize = 4096;
const INTERLEAVE_WORDS: usize = INTERLEAVE_BITS / BITS;
const PROBE_LIMIT: usize = 4;
const DEFAULT_STRIDE_LIMIT: usize = 6000;
const DEFAULT_RANDOM_PROBES: usize = 6000;
const REFILL_BATCH: u32 = 64;
const REFILL_BUSY: u32 = 1 << 31;
const REFILL_MASK: u32 = !REFILL_BUSY;

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

struct ProbeSeq {
    words: usize,
    random_probes: usize,
    used: usize,
    scan: usize,
    mask: usize,
    base: usize,
    delta: usize,
    step: usize,
    strided: bool,
}

impl ProbeSeq {
    fn new(
        words: usize,
        stride_limit: usize,
        random_probes: usize,
        random: &mut impl FnMut() -> u64,
    ) -> Self {
        let strided = words <= stride_limit;
        if strided {
            let scan = words.next_power_of_two();
            let mask = scan - 1;
            let value = random();
            let base = value as usize & mask;
            // An odd delta visits every padded word before repeating.
            let delta = (((value >> (usize::BITS / 2)) as usize) & mask) | 1;
            Self { words, random_probes, used: 0, scan, mask, base, delta, step: 0, strided }
        } else {
            Self {
                words,
                random_probes,
                used: 0,
                scan: 0,
                mask: 0,
                base: 0,
                delta: 0,
                step: 0,
                strided,
            }
        }
    }

    fn next(&mut self, random: &mut impl FnMut() -> u64) -> Option<usize> {
        if self.strided {
            if self.used == self.words {
                return None;
            }
            while self.step < self.scan {
                let at = self.base.wrapping_add(self.step.wrapping_mul(self.delta)) & self.mask;
                self.step += 1;
                if at < self.words {
                    self.used += 1;
                    return Some(at);
                }
            }
            None
        } else {
            if self.used == self.random_probes {
                return None;
            }
            self.used += 1;
            Some(random_word(self.words, random()))
        }
    }
}

/// A fixed-size pool whose free slots are represented by one bit per index.
/// Bit 1 means free; zero means allocated or not yet issued. Each 4096-index
/// group is transposed across 64 bitmap words.
pub struct Pool<T: PoolItem, const ALIGN: usize> {
    pub(super) mapping: Mapping,
    bits: Mapping,
    max_blocks: u32,
    next: AtomicU32,
    refill: AtomicU32,
    refill_gen: AtomicU32,
    stride_limit: usize,
    random_probes: usize,
    _slot: PhantomData<fn() -> T>,
}

// SAFETY: every allocated slot has one owner; bitmap updates are atomic.
unsafe impl<T: PoolItem + Send, const A: usize> Send for Pool<T, A> {}
unsafe impl<T: PoolItem + Send + Sync, const A: usize> Sync for Pool<T, A> {}

impl<T: PoolItem, const A: usize> Pool<T, A> {
    pub fn new(max_blocks: u32, initial_blocks: u32) -> Result<Self> {
        Self::with_probe_limits(
            max_blocks,
            initial_blocks,
            DEFAULT_STRIDE_LIMIT,
            DEFAULT_RANDOM_PROBES,
        )
    }

    /// Create a pool with custom limits for stride and random probing.
    /// Pools with at most `stride_limit` bitmap words are exhaustively scanned
    /// with a stride. Larger pools draw up to `random_probes` independent random
    /// words, allowing repeats.
    pub fn with_probe_limits(
        max_blocks: u32,
        initial_blocks: u32,
        stride_limit: usize,
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
        if max_blocks == 0 || initial_blocks > max_blocks {
            return Err(Error::new("pool needs 0 <= initial_blocks <= max_blocks"));
        }
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
            refill_gen: AtomicU32::new(0),
            stride_limit,
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
        self.refill.compare_exchange(0, count, SeqCst, SeqCst).is_ok()
    }

    /// Pays at most one fixed-size part of the reserve target. The busy bit
    /// keeps a concurrent full scan from scheduling the same reserve twice.
    fn refill(&self) {
        let state = self.refill.load(SeqCst);
        if state == 0 || state & REFILL_BUSY != 0 {
            return;
        }
        let pending = state & REFILL_MASK;
        let count = pending.min(REFILL_BATCH);
        if self
            .refill
            .compare_exchange(state, REFILL_BUSY | (pending - count), SeqCst, SeqCst)
            .is_err()
        {
            return;
        }

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
            self.set_free_range(start, count);
            self.refill_gen.fetch_add(1, SeqCst);
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
    fn take_word(&self, at: usize) -> Option<u32> {
        let word = self.word(at);
        self.take_word_from(at, word.load(SeqCst))
    }

    #[inline]
    fn take_word_from(&self, at: usize, mut free: u64) -> Option<u32> {
        let word = self.word(at);
        while free != 0 {
            let bit = free.trailing_zeros();
            match word.compare_exchange_weak(free, free & !(1u64 << bit), SeqCst, SeqCst) {
                Ok(_) => return Some(self.claim(at, bit)),
                Err(actual) => free = actual,
            }
        }
        None
    }

    /// Number of indexes initialized so far, including currently free indexes.
    /// Concurrent growth may have reserved indexes that are still initializing.
    pub fn issued_blocks(&self) -> u32 {
        self.next.load(SeqCst)
    }

    pub const fn block_bytes(&self) -> usize {
        size_of::<T>()
    }

    /// Searches the bitmap with the configured probe policy, then grows if needed.
    #[inline]
    pub fn alloc(&self) -> Result<Block<'_, T, A>> {
        let issued = self.next.load(SeqCst) as usize;
        let words = word_count(issued);
        let refill_gen = (words > PROBE_LIMIT).then(|| self.refill_gen.load(SeqCst));
        if words != 0 {
            let mut random = || random::thread_u64();
            let mut probes =
                ProbeSeq::new(words, self.stride_limit, self.random_probes, &mut random);
            if let Some(first_at) = probes.next(&mut random) {
                let first_free = self.word(first_at).load(SeqCst);

                if first_free != 0 {
                    let second = if first_free.count_ones() == 1 {
                        probes.next(&mut random).map(|at| (at, self.word(at).load(SeqCst)))
                    } else {
                        None
                    };
                    let found = if let Some((second_at, second_free)) = second {
                        let (fuller_at, fuller_free, other_at, other_free) =
                            if first_free.count_ones() >= second_free.count_ones() {
                                (first_at, first_free, second_at, second_free)
                            } else {
                                (second_at, second_free, first_at, first_free)
                            };
                        self.take_word_from(fuller_at, fuller_free)
                            .or_else(|| self.take_word_from(other_at, other_free))
                    } else {
                        self.take_word_from(first_at, first_free)
                    };
                    if let Some(index) = found {
                        if probes.used > PROBE_LIMIT {
                            self.refill();
                        }
                        return Ok(Block { pool: self, index });
                    }
                }

                while let Some(at) = probes.next(&mut random) {
                    if let Some(index) = self.take_word(at) {
                        if probes.used > PROBE_LIMIT {
                            self.refill();
                        }
                        return Ok(Block { pool: self, index });
                    }
                }
            }
        }
        let block = self.alloc_fresh()?;
        let issued = self.next.load(SeqCst);
        let scheduled = refill_gen.is_none_or(|seen| self.refill_gen.load(SeqCst) == seen)
            && self.schedule_refill(issued.saturating_sub(1) / BITS as u32);
        if !scheduled {
            // A failed full scan allocates one fresh block for its caller. If
            // another call already scheduled reserve work, help pay it here.
            self.refill();
        }
        Ok(block)
    }

    /// Grows by one fresh index without searching or reusing free indexes.
    pub fn alloc_fresh(&self) -> Result<Block<'_, T, A>> {
        let index = self
            .next
            .fetch_update(SeqCst, SeqCst, |next| (next < self.max_blocks).then(|| next + 1))
            .map_err(|_| Error::new("block pool exhausted"))?;
        // SAFETY: fetch_update granted this caller a unique fresh index.
        unsafe { T::init(self.slot(index)) };
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
    use std::sync::atomic::{AtomicU32, Ordering};

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
    fn short_probe_sequence_visits_every_word_once() {
        let calls = std::cell::Cell::new(0);
        let mut random = || {
            calls.set(calls.get() + 1);
            17
        };
        let mut probes = ProbeSeq::new(37, 37, 6000, &mut random);
        let mut seen = vec![false; 37];
        while let Some(word) = probes.next(&mut random) {
            assert!(!seen[word]);
            seen[word] = true;
        }
        assert!(seen.into_iter().all(|hit| hit));
        assert_eq!(calls.get(), 1, "only the base is random for a stride scan");
    }

    #[test]
    fn long_probe_sequence_draws_independent_samples_with_repeats() {
        let samples = [u64::MAX, u64::MAX];
        let mut calls = 0;
        let mut random = || {
            let sample = samples[calls];
            calls += 1;
            sample
        };
        let mut probes = ProbeSeq::new(8, 0, 2, &mut random);
        assert_eq!(probes.next(&mut random), Some(7));
        assert_eq!(probes.next(&mut random), Some(7));
        assert_eq!(probes.next(&mut random), None);
        assert_eq!(calls, 2, "each random probe draws a fresh value");
    }

    #[test]
    fn failed_probe_scan_schedules_one_over_64_spare() {
        let pool = Pool::<[u8; 64], 64>::with_probe_limits(256, 0, 64, 6000).unwrap();
        let held: Vec<_> = (0..128).map(|_| pool.alloc_fresh().unwrap()).collect();

        let first = pool.alloc().unwrap();
        assert_eq!(first.index(), 128);
        assert_eq!(pool.issued_blocks(), 129);

        let second = pool.alloc().unwrap();
        assert_eq!(second.index(), 129);
        assert_eq!(pool.issued_blocks(), 132);

        let a = pool.alloc().unwrap();
        let b = pool.alloc().unwrap();
        let mut indexes = [a.index(), b.index()];
        indexes.sort_unstable();
        assert_eq!(indexes, [130, 131]);
        drop((held, first, second, a, b));
    }

    #[test]
    fn custom_probe_limit_selects_random_mode_above_threshold() {
        let pool = Pool::<[u8; 64], 64>::with_probe_limits(256, 128, 1, 1).unwrap();
        let block = pool.alloc().unwrap();
        assert!(block.index() < 128);
        assert_eq!(pool.issued_blocks(), 128);
    }

    #[test]
    fn reuse_all() {
        let pool = Pool::<[u8; 64], 64>::new(130, 0).unwrap();
        let first: Vec<_> = (0..130).map(|_| pool.alloc_fresh().unwrap()).collect();
        assert!(pool.alloc_fresh().is_err());
        drop(first);
        let mut seen = vec![false; 130];
        let held: Vec<_> = (0..130)
            .map(|_| {
                let block = pool.alloc().unwrap();
                let index = block.index() as usize;
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
        assert!(Pool::<[u8; 64], 64>::with_probe_limits(1, 0, 0, 0).is_err());
    }

    #[test]
    fn slow_scans_do_not_schedule_unbounded_growth() {
        const INITIAL: u32 = 4096;
        let pool = Pool::<[u8; 64], 64>::new(8192, 0).unwrap();
        let mut held: Vec<_> = (0..INITIAL).map(|_| pool.alloc_fresh().unwrap()).collect();

        // An empty scan gives its caller one fresh block and schedules reserve.
        held.push(pool.alloc().unwrap());
        assert_eq!(pool.issued_blocks(), INITIAL + 1);

        // A later empty scan gets its own block and helps pay that reserve.
        held.push(pool.alloc().unwrap());
        let issued = pool.issued_blocks();
        assert_eq!(issued, INITIAL + 2 + INITIAL / 64);

        // The reserve is now paid. Repeated sparse scans may be long, but
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
        let mut held: Vec<_> = (0..INITIAL).map(|_| pool.alloc_fresh().unwrap()).collect();
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
            pool.issued_blocks() <= INITIAL + THREADS as u32 + INITIAL / 64,
            "issued {} blocks",
            pool.issued_blocks()
        );
        drop(held);
    }

    #[test]
    fn concurrent_reuse() {
        const COUNT: usize = 64;
        let pool = Pool::<[u8; 64], 64>::new(COUNT as u32, 0).unwrap();
        let owners: [AtomicU32; COUNT] = std::array::from_fn(|_| AtomicU32::new(0));
        std::thread::scope(|scope| {
            for id in 1..=8u32 {
                let pool = &pool;
                let owners = &owners;
                scope.spawn(move || {
                    for _ in 0..20_000 {
                        match pool.alloc() {
                            Ok(block) => {
                                let owner = &owners[block.index() as usize];
                                assert_eq!(
                                    owner.compare_exchange(
                                        0,
                                        id,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst
                                    ),
                                    Ok(0)
                                );
                                assert_eq!(owner.swap(0, Ordering::SeqCst), id);
                                drop(block);
                            }
                            Err(_) => std::thread::yield_now(),
                        }
                    }
                });
            }
        });
        let mut held = Vec::new();
        while let Ok(block) = pool.alloc() {
            held.push(block);
        }
        assert_eq!(held.len(), pool.issued_blocks() as usize);
        drop(held);
    }
}
