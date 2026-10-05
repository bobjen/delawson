//! Linear-hash bucket heads stored in a segmented buffer.
//!
//! The directory starts at at least 64 buckets and grows or shrinks in batches
//! of 64. It grows when items exceed buckets and shrinks above two buckets per
//! live item. The byte buffer allocates a 64 KiB leaf only at boundaries.
//! Items and their chain links remain owned by the caller.
use super::{CHUNK_BYTES, ChunkPool, LockedItem, LockedPool, SegmentedBuffer};
use crate::{Error, Result};
use std::ops::Index;
use std::ptr::NonNull;

const MIN_BUCKETS: u64 = 64;
const BUCKET_BATCH: u64 = 64;
const HEAD_BYTES: usize = std::mem::size_of::<u32>();
const HEADS_PER_LEAF: u64 = (CHUNK_BYTES / HEAD_BYTES) as u64;

fn append_empty(heads: &mut SegmentedBuffer<'_>, buckets: u64) -> Result<()> {
    let bytes = buckets
        .checked_mul(HEAD_BYTES as u64)
        .ok_or_else(|| Error::new("linear-hash directory size overflow"))?;
    heads.append_fill(0xff, bytes)
}

fn head_ptr_at(heads: &mut SegmentedBuffer<'_>, bucket: u64) -> Result<*mut u32> {
    let at = bucket * HEAD_BYTES as u64;
    let mut chunks = heads.chunks_mut_range(at..at + HEAD_BYTES as u64)?;
    let bytes =
        chunks.next().ok_or_else(|| Error::new("linear-hash bucket head is not allocated"))?;
    if bytes.len() != HEAD_BYTES {
        return Err(Error::new("linear-hash bucket head crosses a leaf"));
    }
    let ptr = bytes.as_mut_ptr().cast::<u32>();
    if (ptr as usize) % std::mem::align_of::<u32>() != 0 {
        return Err(Error::new("linear-hash bucket head is unaligned"));
    }
    Ok(ptr)
}

fn bucket_shape(buckets: u64) -> (u32, u64) {
    let level = u64::BITS - 1 - buckets.leading_zeros();
    (level, buckets - (1u64 << level))
}

fn head_cap(buckets: u64, max_buckets: u64) -> u64 {
    buckets.div_ceil(HEADS_PER_LEAF).saturating_mul(HEADS_PER_LEAF).min(max_buckets)
}

fn batch_cap(buckets: u64) -> u64 {
    buckets.div_ceil(BUCKET_BATCH).saturating_mul(BUCKET_BATCH)
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
        let initial_buckets = batch_cap(initial_buckets.max(MIN_BUCKETS));
        if initial_buckets > max_buckets {
            return Err(Error::new("initial linear-hash size exceeds its item limit"));
        }
        let init_cap = head_cap(initial_buckets, max_buckets);
        let initial_bytes = init_cap * HEAD_BYTES as u64;
        let retained = initial_bytes.div_ceil(CHUNK_BYTES as u64).max(1);
        let retained = u32::try_from(retained)
            .map_err(|_| Error::new("linear-hash directory is too large"))?;
        let mut heads = SegmentedBuffer::with_retained_leaves(pool, retained);
        append_empty(&mut heads, init_cap)?;
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

    fn head_capacity(&self) -> u64 {
        self.heads.end() / HEAD_BYTES as u64
    }

    /// Mutable pointer to an initialized head slot, including reserved slots.
    fn head_ptr(&mut self, bucket: u64) -> Result<*mut u32> {
        if bucket >= self.head_capacity() {
            return Err(Error::new("linear-hash bucket head is not allocated"));
        }
        head_ptr_at(&mut self.heads, bucket)
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

    /// Split 64 buckets, letting the caller repartition each chain by bit.
    /// The callback rewrites each old and new bucket pair in place.
    fn grow_batch(
        &mut self,
        mut split_chain: impl FnMut(&mut SegmentedBuffer<'a>, u64, u64, u64) -> Result<()>,
    ) -> Result<()> {
        let old_buckets = self.buckets;
        let new_buckets = old_buckets + BUCKET_BATCH;
        if new_buckets > self.max_buckets {
            return Err(Error::new("linear-hash directory reached its item limit"));
        }
        let old_split = self.split;
        let old_level = self.level;
        let new_cap = head_cap(new_buckets, self.max_buckets);
        let old_cap = self.head_capacity();
        if new_cap > old_cap {
            append_empty(&mut self.heads, new_cap - old_cap)?;
        }

        let bit = 1u64 << old_level;
        for offset in 0..BUCKET_BATCH {
            let old = old_split + offset;
            let new = old_buckets + offset;
            split_chain(&mut self.heads, old, new, bit)?;
        }

        self.buckets = new_buckets;
        self.split = old_split + BUCKET_BATCH;
        if self.split == 1u64 << old_level {
            self.level += 1;
            self.split = 0;
        }
        Ok(())
    }

    /// Merge 64 buckets into their partners, then trim directory capacity.
    fn shrink_batch(
        &mut self,
        mut merge_chain: impl FnMut(u64, u64, u32, u32) -> u32,
    ) -> Result<()> {
        if self.buckets < MIN_BUCKETS + BUCKET_BATCH {
            return Err(Error::new("linear-hash directory is at its minimum size"));
        }
        let old_buckets = self.buckets;
        let old_split = self.split;
        let (base, new_split) = if old_split == 0 {
            self.level -= 1;
            ((1u64 << self.level) - 1, (1u64 << self.level) - BUCKET_BATCH)
        } else {
            (old_split - 1, old_split - BUCKET_BATCH)
        };
        for offset in 0..BUCKET_BATCH {
            let partner = base - offset;
            let removed = old_buckets - 1 - offset;
            let partner_head = self.head(partner).expect("merge partner remains in range");
            let removed_head = self.head(removed).expect("removed bucket remains in range");
            let head = merge_chain(partner, removed, partner_head, removed_head);
            if head != partner_head {
                self.set_head(partner, head).expect("merge partner remains in range");
            }
            if removed_head != u32::MAX {
                self.set_head(removed, u32::MAX).expect("removed bucket remains in range");
            }
        }

        self.buckets = old_buckets - BUCKET_BATCH;
        self.split = new_split;
        let new_cap = head_cap(self.buckets, self.max_buckets);
        if new_cap < self.head_capacity() {
            self.heads.truncate(new_cap * HEAD_BYTES as u64)?;
        }
        Ok(())
    }

    /// Return to the configured directory size and clear its bucket heads.
    fn reset(&mut self) -> Result<()> {
        self.heads.reset();
        append_empty(&mut self.heads, head_cap(self.initial_buckets, self.max_buckets))?;
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
    /// Return the address of this entry's link field.
    ///
    /// # Safety
    /// The pointer must be aligned, writable, and point to the field returned
    /// by `next()` and changed by `set_next()` for this live entry.
    unsafe fn next_ptr(&mut self) -> *mut u32;

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
    max_items: u32,
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
    /// Create a pool that can hold max_items entries, initially using 64 buckets.
    pub fn new(chunks: &'a ChunkPool, max_items: u32) -> Result<Self> {
        Self::with_buckets(chunks, max_items, 0)
    }

    /// Create a pool with an explicit initial bucket count; zero means 64.
    pub fn with_buckets(
        chunks: &'a ChunkPool,
        max_items: u32,
        initial_buckets: u32,
    ) -> Result<Self> {
        assert_eq!(T::LINK_NONE, u32::MAX, "LinearPool needs LINK_NONE = u32::MAX");
        let max_items = max_items.max(1);
        let max_buckets = batch_cap(u64::from(max_items)).max(MIN_BUCKETS);
        let hash = LinearHash::with_buckets(chunks, max_buckets, u64::from(initial_buckets))?;
        let slots = LockedPool::new(max_items, 0)?;
        let base = slots.base_ptr();
        Ok(Self { slots, base, hash, len: 0, max_items })
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

    fn seek_link(&mut self, key: &T::Key, bucket: u64) -> Result<(*mut u32, u32)> {
        let mut link = self.hash.head_ptr(bucket)?;
        loop {
            // SAFETY: `link` points to a bucket head or a live entry link.
            let index = unsafe { *link };
            if index == T::LINK_NONE {
                return Ok((link, index));
            }
            // SAFETY: linked indexes name initialized, live slots.
            let item = unsafe { self.base.as_ptr().add(index as usize) };
            // SAFETY: the entry is live and exclusively protected by the caller.
            let found = unsafe { (*item).key() };
            if found >= *key {
                return Ok((link, index));
            }
            // SAFETY: this live entry exposes its mutable link field.
            link = unsafe { (*item).next_ptr() };
        }
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
        let bucket = self.hash.bucket(hash);
        let (link, next) = self.seek_link(&key, bucket)?;
        if next != T::LINK_NONE && self.slot(next).key() == key {
            return Ok((self.slot_mut(next), false));
        }

        let final_len =
            self.len.checked_add(1).ok_or_else(|| Error::new("linear pool item count overflow"))?;
        if final_len > u64::from(self.max_items) {
            return Err(Error::new("linear pool reached its item limit"));
        }
        let grows = final_len > self.hash.bucket_count();
        let value = create(key);
        let index = self.slots.alloc()?;
        self.slot_mut(index).init(key, value);
        debug_assert!(self.slot(index).key() == key);
        // SAFETY: `next_ptr` is the initialized slot's link; `link` points to
        // the insertion location found before allocation. Slots and heads are stable.
        unsafe {
            *self.slot_mut(index).next_ptr() = next;
            *link = index;
        }
        if grows {
            // Split after linking so the new item is repartitioned with its
            // original chain. A failed append leaves the old table intact.
            if let Err(error) = self.grow() {
                let (rollback, found) =
                    self.seek_link(&key, bucket).expect("rollback bucket remains in range");
                debug_assert_eq!(found, index);
                // SAFETY: rollback points at this entry in its current chain.
                unsafe { *rollback = next };
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
        while self.hash.bucket_count() >= MIN_BUCKETS + BUCKET_BATCH
            && self.hash.bucket_count() > self.len * 2
        {
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
        self.hash.grow_batch(|heads, old, new, bit| split_chain::<T>(heads, base, old, new, bit))
    }

    fn shrink(&mut self) {
        let base = self.base;
        self.hash
            .shrink_batch(|_, _, keep, add| merge_chain::<T>(base, keep, add))
            .expect("linear pool shrinks by one valid batch");
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

fn split_chain<T: LinearEntry>(
    heads: &mut SegmentedBuffer<'_>,
    base: NonNull<T>,
    old_bucket: u64,
    new_bucket: u64,
    bit: u64,
) -> Result<()> {
    let mut link = head_ptr_at(heads, old_bucket)?;
    let mut keep_tail = T::LINK_NONE;
    let mut index;
    let first_next;
    loop {
        // SAFETY: link points at the old bucket head or a live entry link.
        index = unsafe { *link };
        if index == T::LINK_NONE {
            return Ok(());
        }
        // SAFETY: all linked slots are live and the table is exclusively borrowed.
        let ptr = unsafe { base.as_ptr().add(index as usize) };
        let move_to_new = unsafe { (T::hash(&(*ptr).key()) & bit) != 0 };
        if move_to_new {
            // SAFETY: ptr names a live slot; next_ptr addresses its link field.
            let next = unsafe { (*ptr).next_ptr() };
            // SAFETY: next points at this slot's initialized link.
            first_next = unsafe { *next };
            break;
        }
        keep_tail = index;
        // SAFETY: ptr names a live slot; next_ptr addresses its link field.
        link = unsafe { (*ptr).next_ptr() };
    }

    let mut keep_head = T::LINK_NONE;
    let mut move_head = index;
    let mut move_tail = index;
    let mut previous = index;
    index = first_next;
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
        } else if *list_tail != previous {
            let tail = unsafe { base.as_ptr().add(*list_tail as usize) };
            // SAFETY: tail is a live item in this output list.
            unsafe { (*tail).set_next(index) };
        }
        *list_tail = index;
        previous = index;
        index = next;
    }
    if keep_tail != T::LINK_NONE && keep_tail != previous {
        // SAFETY: keep_tail is the last live item in the kept output chain.
        unsafe { (*base.as_ptr().add(keep_tail as usize)).set_next(T::LINK_NONE) };
    }
    if move_tail != T::LINK_NONE && move_tail != previous {
        // SAFETY: move_tail is the last live item in the moved output chain.
        unsafe { (*base.as_ptr().add(move_tail as usize)).set_next(T::LINK_NONE) };
    }
    // `link` still points at the old-chain link that led to the first moved
    // item. That link may be the bucket head or the previous kept item's next.
    // SAFETY: link remains a valid link slot throughout the in-place split.
    if unsafe { *link } != keep_head {
        unsafe { *link = keep_head };
    }
    if move_head != T::LINK_NONE {
        // New heads are initialized to NONE as part of directory growth.
        let new_link = head_ptr_at(heads, new_bucket)?;
        // SAFETY: new_link addresses the initialized new bucket head.
        unsafe { *new_link = move_head };
    }
    Ok(())
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
        assert_eq!(hash.bucket_count(), 64);

        // Keys initially map across the minimum 64 buckets.
        assert_eq!(hash.bucket(0x41), 1);
        assert_eq!(hash.bucket(0x82), 2);
        assert_eq!(hash.bucket(0x1_0000_0003), 3);
        assert_eq!(hash.head(0).unwrap(), u32::MAX);

        for expected in [128, 192, 256] {
            hash.grow_batch(|_, _, _, _| Ok(())).unwrap();
            assert_eq!(hash.bucket_count(), expected);
        }
        assert_eq!(hash.bucket(64), 64);
        assert_eq!(hash.bucket(128), 128);
        assert_eq!(hash.bucket(192), 192);

        for expected in [192, 128, 64] {
            hash.shrink_batch(|_, _, keep, _| keep).unwrap();
            assert_eq!(hash.bucket_count(), expected);
        }
        assert_eq!(hash.bucket(192), 0);
        assert!(hash.shrink_batch(|_, _, keep, _| keep).is_err());
    }

    #[test]
    fn capacity_reset() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::with_buckets(&pool, 128, 64).unwrap();
        assert_eq!(hash.bucket_count(), 64);
        hash.grow_batch(|_, _, _, _| Ok(())).unwrap();
        assert_eq!(hash.bucket_count(), 128);
        assert!(hash.grow_batch(|_, _, _, _| Ok(())).is_err());
        hash.reset().unwrap();
        assert_eq!(hash.bucket_count(), 64);
        assert_eq!(hash.head(0).unwrap(), u32::MAX);
    }

    fn raw_head(hash: &LinearHash<'_>, bucket: u64) -> u32 {
        let mut bytes = [0; HEAD_BYTES];
        hash.heads.read_at(bucket * HEAD_BYTES as u64, &mut bytes).unwrap();
        u32::from_ne_bytes(bytes)
    }

    #[test]
    fn head_reuse() {
        let pool = ChunkPool::new(4, 1).unwrap();
        let max = HEADS_PER_LEAF + BUCKET_BATCH;
        let mut hash = LinearHash::with_buckets(&pool, max, HEADS_PER_LEAF).unwrap();
        assert_eq!(hash.head_capacity(), HEADS_PER_LEAF);
        assert_eq!(raw_head(&hash, HEADS_PER_LEAF - 1), u32::MAX);

        hash.set_head(0, 17).unwrap();
        hash.grow_batch(|heads, _, new, _| {
            heads.write_at(new * HEAD_BYTES as u64, &23u32.to_ne_bytes())
        })
        .unwrap();
        assert_eq!(hash.bucket_count(), max);
        assert_eq!(hash.head(HEADS_PER_LEAF).unwrap(), 23);

        hash.shrink_batch(|_, _, keep, _| keep).unwrap();
        assert_eq!(hash.head_capacity(), HEADS_PER_LEAF);
        hash.grow_batch(|_, _, _, _| Ok(())).unwrap();
        assert_eq!(hash.head(HEADS_PER_LEAF).unwrap(), u32::MAX);
    }

    #[repr(C)]
    struct Entry {
        next: u32,
        key: u32,
        valid: bool,
        next_writes: u32,
    }

    unsafe impl super::super::PoolItem for Entry {
        unsafe fn init(slot: *mut Self) {
            // SAFETY: the pool provides aligned, fresh storage for one Entry.
            unsafe { slot.write(Self { next: u32::MAX, key: 0, valid: false, next_writes: 0 }) };
        }
    }

    unsafe impl LockedItem for Entry {
        const LINK_NONE: u32 = u32::MAX;

        unsafe fn on_alloc(slot: *mut Self) {
            // SAFETY: this slot is initialized and exclusively allocated.
            unsafe {
                (*slot).next = Self::LINK_NONE;
                (*slot).valid = false;
                (*slot).next_writes = 0;
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
            self.next_writes += 1;
        }

        unsafe fn next_ptr(&mut self) -> *mut u32 {
            std::ptr::addr_of_mut!(self.next)
        }

        fn init(&mut self, key: Self::Key, value: Self) {
            self.key = key;
            self.valid = value.valid;
            self.next_writes = 0;
        }

        fn clear(&mut self) {
            self.next = Self::LINK_NONE;
            self.key = 0;
            self.valid = false;
            self.next_writes = 0;
        }
    }

    fn entry(key: u32) -> Entry {
        Entry { next: Entry::LINK_NONE, key, valid: true, next_writes: 0 }
    }

    fn chain(keys: &[u32]) -> Vec<Entry> {
        let mut entries = keys.iter().copied().map(entry).collect::<Vec<_>>();
        for index in 0..entries.len().saturating_sub(1) {
            entries[index].next = (index + 1) as u32;
        }
        entries
    }

    fn chain_keys(entries: &[Entry], mut index: u32) -> Vec<u32> {
        let mut keys = Vec::new();
        while index != Entry::LINK_NONE {
            keys.push(entries[index as usize].key);
            index = entries[index as usize].next;
        }
        keys
    }

    fn split_for_test(keys: &[u32], bit: u64) -> (Vec<Entry>, u32, u32) {
        let pool = ChunkPool::new(1, 1).unwrap();
        let mut heads = SegmentedBuffer::new(&pool);
        append_empty(&mut heads, 128).unwrap();
        let mut entries = chain(keys);
        let base = NonNull::new(entries.as_mut_ptr()).unwrap();
        heads.write_at(0, &0u32.to_ne_bytes()).unwrap();
        split_chain::<Entry>(&mut heads, base, 0, 64, bit).unwrap();

        let read_head = |bucket| {
            let mut bytes = [0; HEAD_BYTES];
            heads.read_at(bucket * HEAD_BYTES as u64, &mut bytes).unwrap();
            u32::from_ne_bytes(bytes)
        };
        (entries, read_head(0), read_head(64))
    }

    #[test]
    fn split_writes() {
        let (entries, keep, moved) = split_for_test(&[1, 2, 3], 8);
        assert_eq!(chain_keys(&entries, keep), [1, 2, 3]);
        assert_eq!(moved, Entry::LINK_NONE);
        assert!(entries.iter().all(|entry| entry.next_writes == 0));

        let (entries, keep, moved) = split_for_test(&[8, 24, 40], 8);
        assert_eq!(keep, Entry::LINK_NONE);
        assert_eq!(chain_keys(&entries, moved), [8, 24, 40]);
        assert!(entries.iter().all(|entry| entry.next_writes == 0));

        let (entries, keep, moved) = split_for_test(&[8, 1, 24, 2], 8);
        assert_eq!(chain_keys(&entries, keep), [1, 2]);
        assert_eq!(chain_keys(&entries, moved), [8, 24]);
        assert_eq!(entries.iter().map(|entry| entry.next_writes).collect::<Vec<_>>(), [1, 1, 1, 0]);
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
        assert_eq!(table.hash.bucket_count(), 64);
        for key in 0..65 {
            add(&mut table, key, entry(key));
        }
        assert!(table.find_or_create(65, entry).is_err());
        assert_eq!(table.len(), 65);
        assert_eq!(table.hash.bucket_count(), 128);
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
    fn grows_and_shrinks_in_batches() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, 256).unwrap();
        for key in 0..129 {
            add(&mut table, key, entry(key));
        }
        assert_eq!(table.len(), 129);
        assert_eq!(table.hash.bucket_count(), 192);

        for key in 0..64 {
            assert!(table.remove(&key));
        }
        assert_eq!(table.len(), 65);
        assert_eq!(table.hash.bucket_count(), 128);

        assert!(table.remove(&64));
        assert_eq!(table.len(), 64);
        assert_eq!(table.hash.bucket_count(), 128);
        assert!(table.remove(&65));
        assert_eq!(table.len(), 63);
        assert_eq!(table.hash.bucket_count(), 64);
        assert_sorted(&table);
    }

    #[test]
    fn keeps_configured_bucket_count_after_remove_and_clear() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let mut table = LinearPool::<Entry, 4, 0>::with_buckets(&chunks, 128, 100).unwrap();
        assert_eq!(table.hash.bucket_count(), 128);

        for key in 0..100 {
            add(&mut table, key, entry(key));
        }
        assert_eq!(table.hash.bucket_count(), 128);
        for key in 0..100 {
            assert!(table.remove(&key));
        }
        assert_eq!(table.hash.bucket_count(), MIN_BUCKETS);

        table.clear();
        assert_eq!(table.hash.bucket_count(), 128);
        for bucket in 0..128 {
            assert_eq!(table.hash.head(bucket).unwrap(), Entry::LINK_NONE);
        }
    }
}
