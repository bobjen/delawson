//! Linear-hash bucket heads stored in a segmented buffer.
//!
//! The directory starts at its configured size and grows one bucket at a time,
//! keeping at least one bucket per item. It shrinks above two buckets per live
//! item. The byte buffer allocates a 64 KiB leaf only at boundaries.
//! Items and their chain links remain owned by the caller.
use super::{CHUNK_BYTES, ChunkPool, LockedItem, LockedPool, SegmentedBuffer};
use crate::{Error, Result};
use std::ops::Index;
use std::ptr::NonNull;

const MIN_BUCKETS: u64 = 1;
const HEAD_BYTES: usize = std::mem::size_of::<u32>();
static EMPTY_HEADS: [u8; CHUNK_BYTES] = [0xff; CHUNK_BYTES];

fn append_empty(heads: &mut SegmentedBuffer<'_>, buckets: u64) -> Result<()> {
    let mut bytes = buckets
        .checked_mul(HEAD_BYTES as u64)
        .ok_or_else(|| Error::new("linear-hash directory size overflow"))?;
    while bytes != 0 {
        let count = bytes.min(CHUNK_BYTES as u64) as usize;
        heads.append(&EMPTY_HEADS[..count])?;
        bytes -= count as u64;
    }
    Ok(())
}

fn bucket_shape(buckets: u64) -> (u32, u64) {
    let level = u64::BITS - 1 - buckets.leading_zeros();
    (level, buckets - (1u64 << level))
}

/// Bucket directory used internally by `LinearPool`.
struct LinearHash<'a> {
    heads: SegmentedBuffer<'a>,
    buckets: u64,
    level: u32,
    split: u64,
    max_buckets: u64,
    initial_buckets: u64,
}

impl<'a> LinearHash<'a> {
    fn with_buckets(pool: &'a ChunkPool, max_buckets: u64, initial_buckets: u64) -> Result<Self> {
        let max_buckets = max_buckets.max(MIN_BUCKETS);
        let initial_buckets = initial_buckets.max(MIN_BUCKETS);
        if initial_buckets > max_buckets {
            return Err(Error::new("initial linear-hash size exceeds its item limit"));
        }
        let initial_bytes = initial_buckets * HEAD_BYTES as u64;
        let retained = initial_bytes.div_ceil(CHUNK_BYTES as u64).max(1);
        let retained = u32::try_from(retained)
            .map_err(|_| Error::new("linear-hash directory is too large"))?;
        let mut heads = SegmentedBuffer::with_retained_leaves(pool, retained);
        append_empty(&mut heads, initial_buckets)?;
        let (level, split) = bucket_shape(initial_buckets);
        Ok(Self { heads, buckets: initial_buckets, level, split, max_buckets, initial_buckets })
    }

    /// Bucket for an integer key, using its low bits.
    #[inline]
    fn bucket(&self, key: u64) -> u64 {
        let low = key & ((1u64 << self.level) - 1);
        if low < self.split { key & ((1u64 << (self.level + 1)) - 1) } else { low }
    }

    fn bucket_count(&self) -> u64 {
        self.buckets
    }

    fn head(&self, bucket: u64) -> Result<u32> {
        if bucket >= self.buckets {
            return Err(Error::new("linear-hash bucket is out of range"));
        }
        let mut bytes = [0; HEAD_BYTES];
        self.heads.read_at(bucket * HEAD_BYTES as u64, &mut bytes)?;
        Ok(u32::from_ne_bytes(bytes))
    }

    fn set_head(&mut self, bucket: u64, index: u32) -> Result<()> {
        if bucket >= self.buckets {
            return Err(Error::new("linear-hash bucket is out of range"));
        }
        self.heads.write_at(bucket * HEAD_BYTES as u64, &index.to_ne_bytes())
    }

    /// Split one bucket, letting the caller repartition its chain by bit.
    /// The callback returns the new heads for the old and new buckets.
    fn grow_one(
        &mut self,
        mut split_chain: impl FnMut(u64, u64, u64, u32) -> (u32, u32),
    ) -> Result<()> {
        if self.buckets >= self.max_buckets {
            return Err(Error::new("linear-hash directory reached its item limit"));
        }
        let old = self.split;
        let new = self.buckets;
        let bit = 1u64 << self.level;
        let head = self.head(old)?;
        self.heads.append(&[0xff; HEAD_BYTES])?;
        let (old_head, new_head) = split_chain(old, new, bit, head);
        self.buckets += 1;
        self.set_head(old, old_head).expect("split bucket remains in range");
        self.set_head(new, new_head).expect("new bucket was appended");
        self.split += 1;
        if self.split == 1u64 << self.level {
            self.level += 1;
            self.split = 0;
        }
        Ok(())
    }

    /// Merge the latest split bucket into its partner, then trim the logical
    /// byte range. Whole 64 KiB leaves are released only at boundaries.
    fn shrink_one(&mut self, mut merge_chain: impl FnMut(u64, u64, u32, u32) -> u32) -> Result<()> {
        if self.buckets <= MIN_BUCKETS {
            return Err(Error::new("linear-hash directory is at its minimum size"));
        }
        if self.split == 0 {
            self.level -= 1;
            self.split = (1u64 << self.level) - 1;
        } else {
            self.split -= 1;
        }
        let removed = self.buckets - 1;
        let partner = self.split;
        let partner_head = self.head(partner)?;
        let removed_head = self.head(removed)?;
        let head = merge_chain(partner, removed, partner_head, removed_head);
        self.set_head(partner, head)?;
        self.buckets -= 1;
        self.heads.truncate(self.buckets * HEAD_BYTES as u64)
    }

    /// Return to the configured directory size and clear its bucket heads.
    fn reset(&mut self) -> Result<()> {
        self.heads.reset();
        append_empty(&mut self.heads, self.initial_buckets)?;
        self.buckets = self.initial_buckets;
        (self.level, self.split) = bucket_shape(self.initial_buckets);
        Ok(())
    }
}

/// Behavior `LinearPool` needs from its fixed-size entries.
pub trait LinearEntry: LockedItem {
    type Key: Copy + Ord;

    fn key(&self) -> Self::Key;
    /// Return the same stable hash for every value with this key.
    fn hash(key: &Self::Key) -> u64;
    fn next(&self) -> u32;
    fn set_next(&mut self, next: u32);

    /// Initialize a new slot with `value` under `key`.
    /// After this call, `key()` must return `key`; the pool restores the link.
    fn init(&mut self, key: Self::Key, value: Self);

    /// Drop per-entry resources and return this initialized slot to its blank state.
    fn clear(&mut self);
}

/// Fixed-size entries indexed by a compact, incrementally resized hash table.
///
/// All methods that mutate the pool or table require exclusive access. Callers
/// protect shared reads and mutations with their service-wide lock. Adds grow
/// the directory as needed; removes shrink it above two buckets per live item.
pub struct LinearPool<'a, T: LinearEntry, const ALIGN: usize, const LINK: usize> {
    slots: LockedPool<T, ALIGN, LINK>,
    base: NonNull<T>,
    hash: LinearHash<'a>,
    len: u64,
}

// SAFETY: slots are stable and all access is serialized by the caller's lock.
unsafe impl<T: LinearEntry + Send, const A: usize, const L: usize> Send
    for LinearPool<'_, T, A, L>
{
}
unsafe impl<T: LinearEntry + Send + Sync, const A: usize, const L: usize> Sync
    for LinearPool<'_, T, A, L>
{
}

impl<'a, T: LinearEntry, const A: usize, const L: usize> LinearPool<'a, T, A, L> {
    /// Create a pool that can hold max_items entries, initially using one bucket.
    pub fn new(chunks: &'a ChunkPool, max_items: u32) -> Result<Self> {
        Self::with_buckets(chunks, max_items, 0)
    }

    /// Create a pool with an explicit initial bucket count; zero means one.
    pub fn with_buckets(
        chunks: &'a ChunkPool,
        max_items: u32,
        initial_buckets: u32,
    ) -> Result<Self> {
        let max_items = max_items.max(1);
        let max_buckets = u64::from(max_items);
        let hash = LinearHash::with_buckets(chunks, max_buckets, u64::from(initial_buckets))?;
        let slots = LockedPool::new(max_items, 0)?;
        let base = slots.base_ptr();
        Ok(Self { slots, base, hash, len: 0 })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn slot(&self, index: u32) -> &T {
        // SAFETY: issued slots remain initialized at stable addresses.
        unsafe { &*self.base.as_ptr().add(index as usize) }
    }

    fn slot_mut(&mut self, index: u32) -> &mut T {
        // SAFETY: exclusive table access protects the claimed slot.
        unsafe { &mut *self.base.as_ptr().add(index as usize) }
    }

    fn seek(&self, key: &T::Key, hash: u64) -> (u64, u32, u32) {
        let bucket = self.hash.bucket(hash);
        let mut previous = T::LINK_NONE;
        let mut index = self.hash.head(bucket).expect("linear hash bucket exists");
        while index != T::LINK_NONE && self.slot(index).key() < *key {
            previous = index;
            index = self.slot(index).next();
        }
        (bucket, previous, index)
    }

    pub fn get(&self, key: &T::Key) -> Option<&T> {
        let (_, _, index) = self.seek(key, T::hash(key));
        (index != T::LINK_NONE && self.slot(index).key() == *key).then(|| self.slot(index))
    }

    pub fn get_mut(&mut self, key: &T::Key) -> Option<&mut T> {
        let (_, _, index) = self.seek(key, T::hash(key));
        if index == T::LINK_NONE || self.slot(index).key() != *key {
            return None;
        }
        Some(self.slot_mut(index))
    }

    pub fn contains_key(&self, key: &T::Key) -> bool {
        self.get(key).is_some()
    }

    /// Return the existing entry, or initialize and insert one if it is absent.
    /// The factory is statically dispatched and called only for a missing key.
    #[inline]
    pub fn find_or_create(
        &mut self,
        key: T::Key,
        create: impl FnOnce(T::Key) -> T,
    ) -> Result<(&mut T, bool)> {
        let hash = T::hash(&key);
        let (bucket, previous, next) = self.seek(&key, hash);
        if next != T::LINK_NONE && self.slot(next).key() == key {
            return Ok((self.slot_mut(next), false));
        }

        let final_len =
            self.len.checked_add(1).ok_or_else(|| Error::new("linear pool item count overflow"))?;
        if final_len > self.hash.max_buckets {
            return Err(Error::new("linear-hash directory reached its item limit"));
        }
        let grows = final_len > self.hash.bucket_count();
        let value = create(key);
        let index = self.slots.alloc()?;
        self.slot_mut(index).init(key, value);
        debug_assert!(self.slot(index).key() == key);
        self.slot_mut(index).set_next(next);
        if previous == T::LINK_NONE {
            self.hash.set_head(bucket, index).expect("linear-hash bucket exists");
        } else {
            self.slot_mut(previous).set_next(index);
        }
        if grows {
            // Split after linking so the new item is repartitioned with its
            // original chain. A failed append leaves the old table intact.
            if let Err(error) = self.grow() {
                if previous == T::LINK_NONE {
                    self.hash.set_head(bucket, next).expect("rollback bucket remains in range");
                } else {
                    self.slot_mut(previous).set_next(next);
                }
                self.slot_mut(index).clear();
                // SAFETY: this unique entry was unlinked above.
                unsafe { self.slots.retire_index(index) };
                return Err(error);
            }
        }
        self.len = final_len;
        Ok((self.slot_mut(index), true))
    }

    pub fn remove(&mut self, key: &T::Key) -> bool {
        let (bucket, previous, index) = self.seek(key, T::hash(key));
        if index == T::LINK_NONE || self.slot(index).key() != *key {
            return false;
        }
        let next = self.slot(index).next();
        if previous == T::LINK_NONE {
            self.hash.set_head(bucket, next).expect("linear hash bucket exists");
        } else {
            self.slot_mut(previous).set_next(next);
        }
        self.slot_mut(index).clear();
        // SAFETY: this unique live entry was removed from its chain above.
        unsafe { self.slots.retire_index(index) };
        self.len -= 1;
        while self.hash.bucket_count() > MIN_BUCKETS && self.hash.bucket_count() > self.len * 2 {
            self.shrink();
        }
        true
    }

    pub fn clear(&mut self) {
        for bucket in 0..self.hash.bucket_count() {
            let mut index = self.hash.head(bucket).expect("linear hash bucket exists");
            while index != T::LINK_NONE {
                let next = self.slot(index).next();
                self.slot_mut(index).clear();
                // SAFETY: this unique live entry is removed by clearing the table.
                unsafe { self.slots.retire_index(index) };
                index = next;
            }
        }
        self.len = 0;
        self.hash.reset().expect("reset linear hash directory");
        self.slots.reclaim();
    }

    /// Reclaim retired slots while holding the caller's exclusive service lock.
    pub fn reclaim(&mut self) {
        self.slots.reclaim();
    }

    fn grow(&mut self) -> Result<()> {
        let base = self.base;
        self.hash.grow_one(|_, _, bit, head| split_chain::<T>(base, head, bit))
    }

    fn shrink(&mut self) {
        let base = self.base;
        self.hash
            .shrink_one(|_, _, keep, add| merge_chain::<T>(base, keep, add))
            .expect("linear pool shrinks by one valid bucket");
    }
}

impl<T: LinearEntry, const A: usize, const L: usize> Drop for LinearPool<'_, T, A, L> {
    fn drop(&mut self) {
        for index in 0..self.slots.issued_blocks() {
            // PoolItem::init initialized each issued slot, including retired slots.
            unsafe { std::ptr::drop_in_place(self.base.as_ptr().add(index as usize)) };
        }
    }
}

impl<T: LinearEntry, const A: usize, const L: usize> Index<&T::Key> for LinearPool<'_, T, A, L> {
    type Output = T;

    fn index(&self, key: &T::Key) -> &Self::Output {
        self.get(key).expect("linear pool key is present")
    }
}

fn split_chain<T: LinearEntry>(base: NonNull<T>, head: u32, bit: u64) -> (u32, u32) {
    let mut keep_head = T::LINK_NONE;
    let mut keep_tail = T::LINK_NONE;
    let mut move_head = T::LINK_NONE;
    let mut move_tail = T::LINK_NONE;
    let mut index = head;
    while index != T::LINK_NONE {
        // SAFETY: all linked slots are live and the table is exclusively borrowed.
        let ptr = unsafe { base.as_ptr().add(index as usize) };
        let next = unsafe { (*ptr).next() };
        let move_to_new = unsafe { (T::hash(&(*ptr).key()) & bit) != 0 };
        let (list_head, list_tail) = if move_to_new {
            (&mut move_head, &mut move_tail)
        } else {
            (&mut keep_head, &mut keep_tail)
        };
        if *list_head == T::LINK_NONE {
            *list_head = index;
        } else {
            let tail = unsafe { base.as_ptr().add(*list_tail as usize) };
            // SAFETY: tail is a live item in this output list.
            unsafe { (*tail).set_next(index) };
        }
        *list_tail = index;
        index = next;
    }
    if keep_tail != T::LINK_NONE {
        // SAFETY: keep_tail is the last live item in the kept output chain.
        unsafe { (*base.as_ptr().add(keep_tail as usize)).set_next(T::LINK_NONE) };
    }
    if move_tail != T::LINK_NONE {
        // SAFETY: move_tail is the last live item in the moved output chain.
        unsafe { (*base.as_ptr().add(move_tail as usize)).set_next(T::LINK_NONE) };
    }
    (keep_head, move_head)
}

fn merge_chain<T: LinearEntry>(base: NonNull<T>, mut left: u32, mut right: u32) -> u32 {
    let mut head = T::LINK_NONE;
    let mut tail = T::LINK_NONE;
    while left != T::LINK_NONE && right != T::LINK_NONE {
        // SAFETY: both chains contain live items during the exclusive merge.
        let left_ptr = unsafe { base.as_ptr().add(left as usize) };
        let right_ptr = unsafe { base.as_ptr().add(right as usize) };
        let take_left = unsafe { (*left_ptr).key() <= (*right_ptr).key() };
        let (take, next) = if take_left {
            (left, unsafe { (*left_ptr).next() })
        } else {
            (right, unsafe { (*right_ptr).next() })
        };
        if head == T::LINK_NONE {
            head = take;
        } else {
            // SAFETY: tail is the last output item and is live.
            unsafe { (*base.as_ptr().add(tail as usize)).set_next(take) };
        }
        tail = take;
        if take_left {
            left = next;
        } else {
            right = next;
        }
    }
    let rest = if left != T::LINK_NONE { left } else { right };
    if tail == T::LINK_NONE {
        return rest;
    }
    // SAFETY: tail is the output suffix and `rest` is the remaining sorted chain.
    unsafe { (*base.as_ptr().add(tail as usize)).set_next(rest) };
    head
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grow_shrink() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::with_buckets(&pool, 256, 0).unwrap();
        assert_eq!(hash.bucket_count(), 1);

        // Every key initially maps to the single bucket.
        assert_eq!(hash.bucket(0x41), 0);
        assert_eq!(hash.bucket(0x82), 0);
        assert_eq!(hash.bucket(0x1_0000_0003), 0);
        assert_eq!(hash.head(0).unwrap(), u32::MAX);

        for expected in 2..=256 {
            hash.grow_one(|_, _, _, head| (head, u32::MAX)).unwrap();
            assert_eq!(hash.bucket_count(), expected);
        }
        assert_eq!(hash.bucket(64), 64);
        assert_eq!(hash.bucket(128), 128);
        assert_eq!(hash.bucket(192), 192);

        for expected in (1..256).rev() {
            hash.shrink_one(|_, _, keep, _| keep).unwrap();
            assert_eq!(hash.bucket_count(), expected);
        }
        assert_eq!(hash.bucket(192), 0);
        assert!(hash.shrink_one(|_, _, keep, _| keep).is_err());
    }

    #[test]
    fn capacity_reset() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::with_buckets(&pool, 65, 64).unwrap();
        assert_eq!(hash.bucket_count(), 64);
        hash.grow_one(|_, _, _, head| (head, u32::MAX)).unwrap();
        assert_eq!(hash.bucket_count(), 65);
        assert!(hash.grow_one(|_, _, _, head| (head, u32::MAX)).is_err());
        hash.reset().unwrap();
        assert_eq!(hash.bucket_count(), 64);
        assert_eq!(hash.head(0).unwrap(), u32::MAX);
    }

    #[repr(C)]
    struct Entry {
        next: u32,
        key: u32,
        valid: bool,
    }

    unsafe impl super::super::PoolItem for Entry {
        unsafe fn init(slot: *mut Self) {
            // SAFETY: the pool provides aligned, fresh storage for one Entry.
            unsafe { slot.write(Self { next: u32::MAX, key: 0, valid: false }) };
        }
    }

    unsafe impl LockedItem for Entry {
        const LINK_NONE: u32 = u32::MAX;

        unsafe fn on_alloc(slot: *mut Self) {
            // SAFETY: this slot is initialized and exclusively allocated.
            unsafe {
                (*slot).next = Self::LINK_NONE;
                (*slot).valid = false;
            }
        }
    }

    impl LinearEntry for Entry {
        type Key = u32;

        fn key(&self) -> Self::Key {
            self.key
        }

        fn hash(key: &Self::Key) -> u64 {
            u64::from(*key)
        }

        fn next(&self) -> u32 {
            self.next
        }

        fn set_next(&mut self, next: u32) {
            self.next = next;
        }

        fn init(&mut self, key: Self::Key, value: Self) {
            self.key = key;
            self.valid = value.valid;
        }

        fn clear(&mut self) {
            self.next = Self::LINK_NONE;
            self.key = 0;
            self.valid = false;
        }
    }

    fn entry(key: u32) -> Entry {
        Entry { next: Entry::LINK_NONE, key, valid: true }
    }

    fn add(table: &mut LinearPool<'_, Entry, 4, 0>, key: u32, value: Entry) {
        let (_, inserted) = table.find_or_create(key, |_| value).unwrap();
        assert!(inserted);
    }

    fn assert_sorted(table: &LinearPool<'_, Entry, 4, 0>) {
        for bucket in 0..table.hash.bucket_count() {
            let mut previous = None;
            let mut index = table.hash.head(bucket).unwrap();
            while index != Entry::LINK_NONE {
                let key = table.slot(index).key;
                assert!(previous.is_none_or(|prior| prior < key));
                previous = Some(key);
                index = table.slot(index).next;
            }
        }
    }

    #[test]
    fn entries_resize_and_reuse() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, 256).unwrap();
        for key in (0..129).map(|n| n * 64 + 7).rev() {
            add(&mut table, key, entry(key));
            assert_sorted(&table);
        }
        assert_eq!(table.len(), 129);
        for key in 0..129 {
            assert_eq!(table.get(&(key * 64 + 7)).unwrap().key, key * 64 + 7);
        }

        for key in (0..129).step_by(2) {
            assert!(table.remove(&(key * 64 + 7)));
            assert_sorted(&table);
        }
        for key in (1..129).step_by(2) {
            assert!(table.remove(&(key * 64 + 7)));
        }
        assert!(table.is_empty());
        assert_eq!(table.hash.bucket_count(), MIN_BUCKETS);

        table.reclaim();
        add(&mut table, 17, entry(17));
        assert!(table.get(&17).unwrap().valid);
        table.clear();
        assert!(table.is_empty());
        assert_eq!(table.hash.bucket_count(), MIN_BUCKETS);
    }

    #[test]
    fn capacity_is_exact() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, 65).unwrap();
        assert_eq!(table.hash.bucket_count(), 1);
        for key in 0..65 {
            add(&mut table, key, entry(key));
        }
        assert!(table.find_or_create(65, entry).is_err());
        assert_eq!(table.len(), 65);
        assert_eq!(table.hash.bucket_count(), 65);
        assert!(table.get(&64).unwrap().valid);
    }

    #[test]
    fn failed_growth_rolls_back_insert() {
        let chunks = ChunkPool::new(1, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::with_buckets(&chunks, 16_385, 16_384).unwrap();
        for key in 0..16_384 {
            add(&mut table, key, entry(key));
        }

        assert!(table.find_or_create(16_384, entry).is_err());
        assert_eq!(table.len(), 16_384);
        assert_eq!(table.hash.bucket_count(), 16_384);
        assert!(table.get(&16_383).unwrap().valid);
        assert!(table.get(&16_384).is_none());
        assert_sorted(&table);
    }

    #[test]
    fn find_or_create_keeps_the_existing_entry() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, 8).unwrap();
        let (first, inserted) = table.find_or_create(7, entry).unwrap();
        assert!(inserted);
        first.valid = false;

        let (existing, inserted) = table.find_or_create(7, |_| panic!("factory ran")).unwrap();
        assert!(!inserted);
        assert!(!existing.valid);
        assert_eq!(table.len(), 1);
        assert!(!table.get(&7).unwrap().valid);
    }

    #[test]
    fn grows_at_one_bucket_per_item_and_shrinks_above_two() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, 256).unwrap();
        for key in 0..129 {
            add(&mut table, key, entry(key));
        }
        assert_eq!(table.len(), 129);
        assert_eq!(table.hash.bucket_count(), 129);

        for key in 0..64 {
            assert!(table.remove(&key));
        }
        assert_eq!(table.len(), 65);
        assert_eq!(table.hash.bucket_count(), 129);

        assert!(table.remove(&64));
        assert_eq!(table.len(), 64);
        assert_eq!(table.hash.bucket_count(), 128);
        assert_sorted(&table);
    }

    #[test]
    fn keeps_configured_bucket_count_after_remove_and_clear() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::with_buckets(&chunks, 128, 100).unwrap();
        assert_eq!(table.hash.bucket_count(), 100);

        for key in 0..100 {
            add(&mut table, key, entry(key));
        }
        assert_eq!(table.hash.bucket_count(), 100);
        for key in 0..100 {
            assert!(table.remove(&key));
        }
        assert_eq!(table.hash.bucket_count(), MIN_BUCKETS);

        table.clear();
        assert_eq!(table.hash.bucket_count(), 100);
        for bucket in 0..100 {
            assert_eq!(table.hash.head(bucket).unwrap(), Entry::LINK_NONE);
        }
    }
}
