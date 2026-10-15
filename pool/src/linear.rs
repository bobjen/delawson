//! Linear-hash bucket heads stored in a contiguous virtual array.
//!
//! The directory starts at at least 256 buckets and grows or shrinks in batches
//! of 256. It grows above two items per bucket and shrinks when buckets outnumber
//! items. Pool blocks are mapped into the directory's reserved virtual range
//! as it grows. Items and their chain links remain owned by the caller.
use super::{Block, CHUNK_BYTES, ChunkPool, LockedItem, LockedPool, Mapping};
use crate::{Error, Result};
use std::ops::Index;
use std::ptr::NonNull;

const BUCKET_GROUP: u64 = 256;
const HEAD_BYTES: usize = std::mem::size_of::<u32>();
const HEADS_PER_BLOCK: u64 = (CHUNK_BYTES / HEAD_BYTES) as u64;

type HeadBlock<'a> = Block<'a, [u8; CHUNK_BYTES], CHUNK_BYTES>;

/// A contiguous bucket-head range backed by 64 KiB blocks from `ChunkPool`.
/// Each pool block remains owned while its pages are aliased into the reserved
/// virtual range. Field order ensures the alias mapping is removed before the
/// block handles return those pages to the pool.
struct VirtualHeads<'a> {
    pool: &'a ChunkPool,
    mapping: Mapping,
    blocks: Vec<HeadBlock<'a>>,
    capacity: u64,
    max_buckets: u64,
}

impl<'a> VirtualHeads<'a> {
    fn new(pool: &'a ChunkPool, max_buckets: u64, initial: u64) -> Result<Self> {
        let block_count = max_buckets.div_ceil(HEADS_PER_BLOCK).max(1);
        let bytes = block_count
            .checked_mul(CHUNK_BYTES as u64)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| Error::new("linear-hash virtual array is too large"))?;
        let mapping = Mapping::new(bytes, CHUNK_BYTES)?;
        let mut heads = Self { pool, mapping, blocks: Vec::new(), capacity: 0, max_buckets };
        heads.extend_to(initial)?;
        Ok(heads)
    }

    #[inline]
    fn base(&self) -> *mut u32 {
        self.mapping.base.as_ptr().cast()
    }

    fn capacity(&self) -> u64 {
        self.capacity
    }

    fn extend_to(&mut self, buckets: u64) -> Result<()> {
        if buckets > self.max_buckets {
            return Err(Error::new("linear-hash virtual array capacity exceeded"));
        }
        let needed = buckets.div_ceil(HEADS_PER_BLOCK).max(1) as usize;
        while self.blocks.len() < needed {
            self.blocks
                .try_reserve(1)
                .map_err(|e| Error::new(format!("linear-hash block list: {e}")))?;
            let block = self.pool.alloc()?;
            // New directory slots are NONE, including unused heads in this block.
            unsafe { block.as_ptr().write_bytes(0xff, CHUNK_BYTES) };
            let offset = self.blocks.len() * CHUNK_BYTES;
            let dest = unsafe { self.mapping.base.as_ptr().add(offset) };
            map_block(block.as_ptr(), dest)?;
            self.blocks.push(block);
            self.capacity = (self.blocks.len() as u64 * HEADS_PER_BLOCK).min(self.max_buckets);
        }
        if buckets > self.capacity {
            // A shrink can leave a partial final block mapped. Clear its newly
            // exposed suffix before making those bucket heads addressable again.
            let count = usize::try_from(buckets - self.capacity)
                .map_err(|_| Error::new("linear-hash capacity is too large"))?;
            unsafe {
                std::ptr::write_bytes(
                    self.base().add(self.capacity as usize).cast::<u8>(),
                    0xff,
                    count * HEAD_BYTES,
                )
            };
            self.capacity = buckets;
        }
        Ok(())
    }

    fn truncate(&mut self, buckets: u64) -> Result<()> {
        if buckets > self.capacity {
            return Err(Error::new("linear-hash truncate exceeds capacity"));
        }
        let needed = buckets.div_ceil(HEADS_PER_BLOCK).max(1) as usize;
        let retained_capacity = (needed as u64 * HEADS_PER_BLOCK).min(self.max_buckets);
        if buckets < retained_capacity {
            let count = usize::try_from(retained_capacity - buckets)
                .map_err(|_| Error::new("linear-hash capacity is too large"))?;
            unsafe {
                std::ptr::write_bytes(
                    self.base().add(buckets as usize).cast::<u8>(),
                    0xff,
                    count * HEAD_BYTES,
                )
            };
        }
        while self.blocks.len() > needed {
            let offset = (self.blocks.len() - 1) * CHUNK_BYTES;
            let dest = unsafe { self.mapping.base.as_ptr().add(offset) };
            reserve_block(dest)?;
            self.blocks.pop();
            self.capacity = (self.blocks.len() as u64 * HEADS_PER_BLOCK).min(self.max_buckets);
        }
        self.capacity = buckets;
        Ok(())
    }

    fn reset(&mut self, initial: u64) -> Result<()> {
        self.truncate(initial)?;
        let bytes = usize::try_from(initial)
            .ok()
            .and_then(|count| count.checked_mul(HEAD_BYTES))
            .ok_or_else(|| Error::new("linear-hash reset size is too large"))?;
        unsafe { std::ptr::write_bytes(self.base().cast::<u8>(), 0xff, bytes) };
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn map_block(source: *mut u8, dest: *mut u8) -> Result<()> {
    let mapped = unsafe {
        libc::mremap(
            source.cast(),
            0,
            CHUNK_BYTES,
            libc::MREMAP_MAYMOVE | libc::MREMAP_FIXED,
            dest.cast::<libc::c_void>(),
        )
    };
    if mapped == libc::MAP_FAILED {
        Err(Error::new(format!("map linear-hash pool block: {}", std::io::Error::last_os_error())))
    } else {
        debug_assert_eq!(mapped, dest.cast());
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
fn map_block(_source: *mut u8, _dest: *mut u8) -> Result<()> {
    Err(Error::new("linear-hash virtual arrays require Linux"))
}

#[cfg(target_os = "linux")]
fn reserve_block(dest: *mut u8) -> Result<()> {
    let reserved = unsafe {
        libc::mmap(
            dest.cast(),
            CHUNK_BYTES,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED,
            -1,
            0,
        )
    };
    if reserved == libc::MAP_FAILED {
        Err(Error::new(format!(
            "release linear-hash pool block: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        debug_assert_eq!(reserved, dest.cast());
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
fn reserve_block(_dest: *mut u8) -> Result<()> {
    Err(Error::new("linear-hash virtual arrays require Linux"))
}

fn bucket_shape(buckets: u64) -> (u32, u64) {
    let level = u64::BITS - 1 - buckets.leading_zeros();
    (level, buckets - (1u64 << level))
}

fn head_cap(buckets: u64, max_buckets: u64) -> u64 {
    buckets.div_ceil(HEADS_PER_BLOCK).saturating_mul(HEADS_PER_BLOCK).min(max_buckets)
}

fn batch_cap(buckets: u64) -> u64 {
    buckets.div_ceil(BUCKET_GROUP).saturating_mul(BUCKET_GROUP)
}

/// Bucket directory used internally by `LinearPool`.
struct LinearHash<'a> {
    heads: VirtualHeads<'a>,
    buckets: u64,
    level: u32,
    split: u64,
    max_buckets: u64,
    initial_buckets: u64,
}

impl<'a> LinearHash<'a> {
    fn with_buckets(pool: &'a ChunkPool, max_buckets: u64, initial_buckets: u64) -> Result<Self> {
        let max_buckets = max_buckets.max(BUCKET_GROUP);
        let initial_buckets = batch_cap(initial_buckets.max(BUCKET_GROUP));
        if initial_buckets > max_buckets {
            return Err(Error::new("initial linear-hash size exceeds its item limit"));
        }
        let init_cap = head_cap(initial_buckets, max_buckets);
        let heads = VirtualHeads::new(pool, max_buckets, init_cap)?;
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
        self.heads.capacity()
    }

    /// Mutable pointer to an initialized head slot, including reserved slots.
    #[inline]
    fn head_ptr(&mut self, bucket: u64) -> *mut u32 {
        debug_assert!(bucket < self.head_capacity());
        unsafe { self.heads.base().add(bucket as usize) }
    }

    #[inline]
    fn head(&self, bucket: u64) -> u32 {
        debug_assert!(bucket < self.buckets);
        unsafe { *self.heads.base().add(bucket as usize) }
    }

    #[inline]
    fn set_head(&mut self, bucket: u64, index: u32) {
        debug_assert!(bucket < self.buckets);
        unsafe { self.heads.base().add(bucket as usize).write(index) };
    }

    /// Split one bucket group, letting the caller repartition each chain by bit.
    /// The callback rewrites each old and new bucket pair in place.
    fn grow_batch(&mut self, mut split_chain: impl FnMut(*mut u32, u64, u64, u64)) -> Result<()> {
        let old_buckets = self.buckets;
        let new_buckets = old_buckets + BUCKET_GROUP;
        if new_buckets > self.max_buckets {
            return Err(Error::new("linear-hash directory reached its item limit"));
        }
        let old_split = self.split;
        let old_level = self.level;
        let new_cap = head_cap(new_buckets, self.max_buckets);
        let old_cap = self.head_capacity();
        if new_cap > old_cap {
            self.heads.extend_to(new_cap)?;
        }

        let bit = 1u64 << old_level;
        let heads = self.heads.base();
        for offset in 0..BUCKET_GROUP {
            let old = old_split + offset;
            let new = old_buckets + offset;
            split_chain(heads, old, new, bit);
        }

        self.buckets = new_buckets;
        self.split = old_split + BUCKET_GROUP;
        if self.split == 1u64 << old_level {
            self.level += 1;
            self.split = 0;
        }
        Ok(())
    }

    /// Merge one bucket group into its partner buckets, then trim directory capacity.
    fn shrink_batch(
        &mut self,
        mut merge_chain: impl FnMut(u64, u64, u32, u32) -> u32,
    ) -> Result<()> {
        if self.buckets < BUCKET_GROUP * 2 {
            return Err(Error::new("linear-hash directory is at its minimum size"));
        }
        let old_buckets = self.buckets;
        let old_split = self.split;
        let (base, new_split) = if old_split == 0 {
            self.level -= 1;
            ((1u64 << self.level) - 1, (1u64 << self.level) - BUCKET_GROUP)
        } else {
            (old_split - 1, old_split - BUCKET_GROUP)
        };
        for offset in 0..BUCKET_GROUP {
            let partner = base - offset;
            let removed = old_buckets - 1 - offset;
            let partner_head = self.head(partner);
            let removed_head = self.head(removed);
            let head = merge_chain(partner, removed, partner_head, removed_head);
            if head != partner_head {
                self.set_head(partner, head);
            }
            if removed_head != u32::MAX {
                self.set_head(removed, u32::MAX);
            }
        }

        self.buckets = old_buckets - BUCKET_GROUP;
        self.split = new_split;
        let new_cap = head_cap(self.buckets, self.max_buckets);
        if new_cap < self.head_capacity() {
            self.heads.truncate(new_cap)?;
        }
        Ok(())
    }

    /// Return to the configured directory size and clear its bucket heads.
    fn reset(&mut self) -> Result<()> {
        let init_cap = head_cap(self.initial_buckets, self.max_buckets);
        self.heads.reset(init_cap)?;
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
    /// Create a pool that can hold max_items entries, initially using 256 buckets.
    pub fn new(chunks: &'a ChunkPool, max_items: u32) -> Result<Self> {
        Self::with_buckets(chunks, max_items, 0)
    }

    /// Create a pool with an explicit initial bucket count; zero means 256.
    pub fn with_buckets(
        chunks: &'a ChunkPool,
        max_items: u32,
        initial_buckets: u32,
    ) -> Result<Self> {
        assert_eq!(T::LINK_NONE, u32::MAX, "LinearPool needs LINK_NONE = u32::MAX");
        let max_items = max_items.max(1);
        let max_buckets = batch_cap(u64::from(max_items)).max(BUCKET_GROUP);
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
        let mut link = self.hash.head_ptr(bucket);
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
        let mut index = self.hash.head(bucket);
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
        let grows = final_len > self.hash.bucket_count() * 2;
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
            self.hash.set_head(bucket, next);
        } else {
            self.slot_mut(previous).set_next(next);
        }
        self.slot_mut(index).clear();
        // SAFETY: this unique live entry was removed from its chain above.
        unsafe { self.slots.retire_index(index) };
        self.len -= 1;
        while self.hash.bucket_count() >= BUCKET_GROUP * 2 && self.hash.bucket_count() > self.len {
            self.shrink();
        }
        true
    }

    pub fn clear(&mut self) {
        for bucket in 0..self.hash.bucket_count() {
            let mut index = self.hash.head(bucket);
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
    heads: *mut u32,
    base: NonNull<T>,
    old_bucket: u64,
    new_bucket: u64,
    bit: u64,
) {
    let mut oldp = unsafe { heads.add(old_bucket as usize) }; // walks old bucket
    let mut index; // index of current item
    let mut ptr; // pointer to current item
    loop {
        // SAFETY: oldp points at the old bucket head or a live entry link.
        index = unsafe { *oldp };
        if index == T::LINK_NONE {
            return; // all items stayed in the old list, nothing changes
        }
        // SAFETY: all linked slots are live and the table is exclusively borrowed.
        ptr = unsafe { base.as_ptr().add(index as usize) };
        if unsafe { (T::hash(&(*ptr).key()) & bit) != 0 } {
            break; // need to move ptr (base[index]) to new list
        }
        // SAFETY: ptr names a live slot; next_ptr addresses its link field.
        oldp = unsafe { (*ptr).next_ptr() }; // oldp now points past index
    }
    // index==*oldp, ptr==base[index], *ptr needs to move to new bucket

    let mut newp = unsafe { heads.add(new_bucket as usize) }; // tail of new bucket
    let mut moving = true; // the current item needs to move to the new bucket
    let mut old_move = false; // the previous item did not need to move
    let mut nextp = unsafe { (*ptr).next_ptr() };
    let mut next = unsafe { *nextp };
    while next != T::LINK_NONE {
        if moving {
            if moving != old_move {
                unsafe { *newp = index }; // move to new bucket
            }
            newp = nextp;
        } else {
            if moving != old_move {
                unsafe { *oldp = index };
            }
            oldp = nextp;
        }

        ptr = unsafe { base.as_ptr().add(next as usize) };
        old_move = moving;
        moving = unsafe { (T::hash(&(*ptr).key()) & bit) != 0 };
        index = next;
        nextp = unsafe { (*ptr).next_ptr() };
        next = unsafe { *nextp };
    }
    // Postcondition: index and ptr identify the last item; next is NONE;
    // moving and old_move classify this item and its predecessor. oldp and
    // newp point at the output tails' links, or an empty bucket head. The
    // final branch attaches the item if needed, then terminates the other list.

    if moving {
        if moving != old_move {
            unsafe { *newp = index };
        }
        unsafe { *oldp = T::LINK_NONE };
    } else {
        if moving != old_move {
            unsafe { *oldp = index };
        }
        unsafe { *newp = T::LINK_NONE };
    }
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
    use std::time::Instant;

    #[test]
    fn grow_shrink() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let group = BUCKET_GROUP;
        let mut hash = LinearHash::with_buckets(&pool, group * 4, 0).unwrap();
        assert_eq!(hash.bucket_count(), group);

        // Keys initially map across the minimum bucket group.
        assert_eq!(hash.bucket(group + 1), 1);
        assert_eq!(hash.bucket(group * 2 + 2), 2);
        assert_eq!(hash.bucket(group * 3 + 3), 3);
        assert_eq!(hash.head(0), u32::MAX);

        for expected in [group * 2, group * 3, group * 4] {
            hash.grow_batch(|_, _, _, _| {}).unwrap();
            assert_eq!(hash.bucket_count(), expected);
        }
        assert_eq!(hash.bucket(group), group);
        assert_eq!(hash.bucket(group * 2), group * 2);
        assert_eq!(hash.bucket(group * 3), group * 3);

        for expected in [group * 3, group * 2, group] {
            hash.shrink_batch(|_, _, keep, _| keep).unwrap();
            assert_eq!(hash.bucket_count(), expected);
        }
        assert_eq!(hash.bucket(group), 0);
        assert!(hash.shrink_batch(|_, _, keep, _| keep).is_err());
    }

    #[test]
    fn capacity_reset() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::with_buckets(&pool, BUCKET_GROUP * 5, BUCKET_GROUP * 4).unwrap();
        assert_eq!(hash.bucket_count(), BUCKET_GROUP * 4);
        hash.grow_batch(|_, _, _, _| {}).unwrap();
        assert_eq!(hash.bucket_count(), BUCKET_GROUP * 5);
        assert!(hash.grow_batch(|_, _, _, _| {}).is_err());
        hash.reset().unwrap();
        assert_eq!(hash.bucket_count(), BUCKET_GROUP * 4);
        assert_eq!(hash.head(0), u32::MAX);
    }

    #[test]
    fn head_reuse() {
        let pool = ChunkPool::new(4, 1).unwrap();
        let max = HEADS_PER_BLOCK + BUCKET_GROUP;
        let mut hash = LinearHash::with_buckets(&pool, max, HEADS_PER_BLOCK).unwrap();
        assert_eq!(hash.head_capacity(), HEADS_PER_BLOCK);
        assert_eq!(hash.head(HEADS_PER_BLOCK - 1), u32::MAX);

        hash.set_head(0, 17);
        hash.grow_batch(|heads, _, new, _| unsafe { heads.add(new as usize).write(23) }).unwrap();
        assert_eq!(hash.bucket_count(), max);
        assert_eq!(hash.head(HEADS_PER_BLOCK), 23);

        hash.shrink_batch(|_, _, keep, _| keep).unwrap();
        assert_eq!(hash.head_capacity(), HEADS_PER_BLOCK);
        hash.grow_batch(|_, _, _, _| {}).unwrap();
        assert_eq!(hash.head(HEADS_PER_BLOCK), u32::MAX);
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

        unsafe fn next_ptr(&mut self) -> *mut u32 {
            std::ptr::addr_of_mut!(self.next)
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
            assert!(keys.len() < entries.len(), "cycle in test chain");
            assert!((index as usize) < entries.len(), "invalid index in test chain");
            keys.push(entries[index as usize].key);
            index = entries[index as usize].next;
        }
        keys
    }

    fn split_for_test(keys: &[u32], bit: u64) -> (Vec<Entry>, u32, u32) {
        let pool = ChunkPool::new(1, 1).unwrap();
        let heads = VirtualHeads::new(&pool, 128, 128).unwrap();
        let mut entries = chain(keys);
        let base = NonNull::new(entries.as_mut_ptr()).unwrap();
        let first = if entries.is_empty() { Entry::LINK_NONE } else { 0 };
        unsafe { heads.base().write(first) };
        split_chain::<Entry>(heads.base(), base, 0, 64, bit);
        (entries, unsafe { heads.base().read() }, unsafe { heads.base().add(64).read() })
    }

    #[test]
    fn split_chains() {
        let (entries, keep, moved) = split_for_test(&[1, 2, 3], 8);
        assert_eq!(chain_keys(&entries, keep), [1, 2, 3]);
        assert_eq!(moved, Entry::LINK_NONE);

        let (entries, keep, moved) = split_for_test(&[8, 24, 40], 8);
        assert_eq!(keep, Entry::LINK_NONE);
        assert_eq!(chain_keys(&entries, moved), [8, 24, 40]);

        let (entries, keep, moved) = split_for_test(&[8, 1, 24, 2], 8);
        assert_eq!(chain_keys(&entries, keep), [1, 2]);
        assert_eq!(chain_keys(&entries, moved), [8, 24]);

        let (entries, keep, moved) = split_for_test(&[1, 8, 2, 24], 8);
        assert_eq!(chain_keys(&entries, keep), [1, 2]);
        assert_eq!(chain_keys(&entries, moved), [8, 24]);
    }

    #[test]
    fn split_all_patterns_up_to_four() {
        let bit = 8u32;
        let mut cases = 0;
        for len in 0..=4 {
            for pattern in 0..(1u32 << len) {
                let mut keys = Vec::with_capacity(len);
                let mut expected_keep = Vec::new();
                let mut expected_move = Vec::new();
                for at in 0..len {
                    let low = at as u32;
                    if pattern & (1 << at) == 0 {
                        keys.push(low);
                        expected_keep.push(low);
                    } else {
                        let key = bit | low;
                        keys.push(key);
                        expected_move.push(key);
                    }
                }

                let (entries, keep, moved) = split_for_test(&keys, u64::from(bit));
                assert_eq!(
                    chain_keys(&entries, keep),
                    expected_keep,
                    "len={len}, pattern={pattern:#x}"
                );
                assert_eq!(
                    chain_keys(&entries, moved),
                    expected_move,
                    "len={len}, pattern={pattern:#x}"
                );
                cases += 1;
            }
        }
        assert_eq!(cases, 31);
    }

    #[test]
    #[ignore = "manual linear-hash growth timing"]
    fn time_growth_batch() {
        const TRIALS: usize = 10_000;
        let group = BUCKET_GROUP;
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::with_buckets(&pool, group * 2, group).unwrap();
        let mut entries = (0..group as u32)
            .map(|bucket| {
                let key = if bucket & 1 == 0 { bucket + group as u32 } else { bucket };
                Entry { next: Entry::LINK_NONE, key, valid: true }
            })
            .collect::<Vec<_>>();
        let base = NonNull::new(entries.as_mut_ptr()).unwrap();
        let mut samples = Vec::with_capacity(TRIALS);

        for _ in 0..TRIALS {
            hash.reset().unwrap();
            for (bucket, entry) in entries.iter_mut().enumerate() {
                entry.next = Entry::LINK_NONE;
                hash.set_head(bucket as u64, bucket as u32);
            }

            let start = Instant::now();
            hash.grow_batch(|heads, old, new, bit| {
                split_chain::<Entry>(heads, base, old, new, bit)
            })
            .unwrap();
            samples.push(start.elapsed().as_nanos());
            std::hint::black_box(hash.bucket_count());
        }

        samples.sort_unstable();
        let total: u128 = samples.iter().sum();
        println!(
            "linear_growth buckets={group} trials={TRIALS} moved_per_batch={} mean_ns={:.1} median_ns={} p95_ns={} max_ns={}",
            group / 2,
            total as f64 / TRIALS as f64,
            samples[TRIALS / 2],
            samples[TRIALS * 95 / 100],
            samples[TRIALS - 1],
        );
    }

    fn add(table: &mut LinearPool<'_, Entry, 4, 0>, key: u32, value: Entry) {
        let (_, inserted) = table.find_or_create(key, |_| value).unwrap();
        assert!(inserted);
    }

    fn assert_sorted(table: &LinearPool<'_, Entry, 4, 0>) {
        for bucket in 0..table.hash.bucket_count() {
            let mut previous = None;
            let mut index = table.hash.head(bucket);
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
        let count = (BUCKET_GROUP * 2 + 1) as u32;
        let stride = BUCKET_GROUP as u32;
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, count).unwrap();
        for key in (0..count).map(|n| n * stride + 7).rev() {
            add(&mut table, key, entry(key));
            assert_sorted(&table);
        }
        assert_eq!(table.len(), u64::from(count));
        for key in 0..count {
            assert_eq!(table.get(&(key * stride + 7)).unwrap().key, key * stride + 7);
        }

        for key in (0..count).step_by(2) {
            assert!(table.remove(&(key * stride + 7)));
            assert_sorted(&table);
        }
        for key in (1..count).step_by(2) {
            assert!(table.remove(&(key * stride + 7)));
        }
        assert!(table.is_empty());
        assert_eq!(table.hash.bucket_count(), BUCKET_GROUP);

        table.reclaim();
        add(&mut table, 17, entry(17));
        assert!(table.get(&17).unwrap().valid);
        table.clear();
        assert!(table.is_empty());
        assert_eq!(table.hash.bucket_count(), BUCKET_GROUP);
    }

    #[test]
    fn capacity_is_exact() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let max_items = (BUCKET_GROUP * 4 + 1) as u32;
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, max_items).unwrap();
        assert_eq!(table.hash.bucket_count(), BUCKET_GROUP);
        for key in 0..max_items {
            add(&mut table, key, entry(key));
        }
        assert!(table.find_or_create(max_items, entry).is_err());
        assert_eq!(table.len(), u64::from(max_items));
        assert_eq!(table.hash.bucket_count(), BUCKET_GROUP * 3);
        assert!(table.get(&(max_items - 1)).unwrap().valid);
    }

    #[test]
    fn failed_growth_rolls_back_insert() {
        let chunks = ChunkPool::new(1, 1).unwrap();
        let initial_buckets = HEADS_PER_BLOCK as u32;
        let full_len = initial_buckets * 2;
        let mut table =
            LinearPool::<Entry, 4, 0>::with_buckets(&chunks, full_len + 1, initial_buckets)
                .unwrap();
        for key in 0..full_len {
            add(&mut table, key, entry(key));
        }

        assert!(table.find_or_create(full_len, entry).is_err());
        assert_eq!(table.len(), u64::from(full_len));
        assert_eq!(table.hash.bucket_count(), u64::from(initial_buckets));
        assert!(table.get(&(full_len - 1)).unwrap().valid);
        assert!(table.get(&full_len).is_none());
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
        let group = BUCKET_GROUP as u32;
        let count = group * 2 + 1;
        let mut table = LinearPool::<Entry, 4, 0>::new(&chunks, count).unwrap();
        for key in 0..count {
            add(&mut table, key, entry(key));
        }
        assert_eq!(table.len(), u64::from(count));
        assert_eq!(table.hash.bucket_count(), u64::from(group * 2));

        for key in 0..group {
            assert!(table.remove(&key));
        }
        assert_eq!(table.len(), u64::from(group + 1));
        assert_eq!(table.hash.bucket_count(), u64::from(group));

        assert!(table.remove(&group));
        assert_eq!(table.len(), u64::from(group));
        assert_eq!(table.hash.bucket_count(), u64::from(group));
        assert!(table.remove(&(group + 1)));
        assert_eq!(table.len(), u64::from(group - 1));
        assert_eq!(table.hash.bucket_count(), u64::from(group));
        assert_sorted(&table);
    }

    #[test]
    fn keeps_configured_bucket_count_after_remove_and_clear() {
        let chunks = ChunkPool::new(8, 1).unwrap();
        let initial_items = BUCKET_GROUP * 6;
        let initial_buckets = batch_cap(initial_items + BUCKET_GROUP / 2);
        let mut table = LinearPool::<Entry, 4, 0>::with_buckets(
            &chunks,
            initial_buckets as u32,
            (initial_items + BUCKET_GROUP / 2) as u32,
        )
        .unwrap();
        assert_eq!(table.hash.bucket_count(), initial_buckets);

        for key in 0..initial_items as u32 {
            add(&mut table, key, entry(key));
        }
        assert_eq!(table.hash.bucket_count(), initial_buckets);
        for key in 0..initial_items as u32 {
            assert!(table.remove(&key));
        }
        assert_eq!(table.hash.bucket_count(), BUCKET_GROUP);

        table.clear();
        assert_eq!(table.hash.bucket_count(), initial_buckets);
        for bucket in 0..initial_buckets {
            assert_eq!(table.hash.head(bucket), Entry::LINK_NONE);
        }
    }
}
