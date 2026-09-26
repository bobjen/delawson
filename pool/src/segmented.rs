//! A packed byte range backed by a radix tree of pool indexes.
//!
//! Leaves and index nodes use CHUNK_BYTES for size and alignment. Index nodes
//! hold CHUNK_BYTES / size_of::<u32>() native-endian pool indexes; NONE marks an absent child. These are
//! in-memory indexes, not a serialized disk format. Depth zero is a leaf.
//! The derived maximum depth covers the entire u64 byte-offset space (using u128 internally
//! for tree spans). The pool's capacity independently limits resident storage.
#[path = "alias.rs"]
mod alias;
#[path = "stream.rs"]
mod stream;
pub use stream::{SegmentedReader, SegmentedWriter};

use crate::{Block, Error, NONE, Pool, Result};

pub const CHUNK_BYTES: usize = 64 * 1024;
pub const INDEX_ENTRIES: usize = CHUNK_BYTES / size_of::<u32>();
const CHUNK_BITS: u32 = CHUNK_BYTES.trailing_zeros();
const INDEX_BITS: u32 = INDEX_ENTRIES.trailing_zeros();
pub const MAX_DEPTH: u8 = (64 - CHUNK_BITS).div_ceil(INDEX_BITS) as u8;
pub type ChunkPool = Pool<CHUNK_BYTES, CHUNK_BYTES>;

/// One initialized byte range, `start()..end()`, with no holes.
///
/// Appending grows the tree; discarding a prefix returns whole unused leaves
/// and index nodes to the pool. Offsets are absolute and do not change when a
/// prefix is discarded. By default an empty buffer retains its end offset but
/// no blocks. An optional bounded prefix of leaves can remain allocated for reuse;
/// retained capacity is never exposed as initialized data.
/// The exclusive end must fit in u64, so byte u64::MAX cannot be stored.
///
/// Mutation requires exclusive access. Borrowed slices prevent mutation or
/// recycling while used by a synchronous caller. Asynchronous I/O callers must
/// additionally keep the buffer alive and its submitted bytes unchanged until
/// completion. This type does not submit I/O or initialize disk padding.
pub struct SegmentedBuffer<'a> {
    pool: &'a ChunkPool,
    alias: Option<Box<alias::AliasMapping>>,
    root: u32,
    // Last initialized leaf, or retained leaf zero after reset, or NONE.
    // At nonzero block boundaries this still names the previous leaf; append
    // must allocate/find the next one. Pruning invalidates it before recycling.
    tail: u32,
    depth: u8,
    // Absolute leaf number of the beginning of the root's address range.
    base: u64,
    start: u64,
    end: u64,
    retain_leaves: u32,
    retained: u32,
    // Exclusive upper bound of allocated leaf numbers, including reusable capacity.
    allocated_end: u64,
}

impl<'a> SegmentedBuffer<'a> {
    pub fn new(pool: &'a ChunkPool) -> Self {
        Self::with_start(pool, 0)
    }

    /// Start an empty rolling buffer at an absolute stream offset.
    pub fn with_start(pool: &'a ChunkPool, offset: u64) -> Self {
        Self {
            pool,
            alias: None,
            root: NONE,
            tail: NONE,
            depth: 0,
            base: 0,
            start: offset,
            end: offset,
            retain_leaves: 0,
            retained: 0,
            allocated_end: 0,
        }
    }

    /// Retain at most this many zero-based prefix leaves across resets. Allocates
    /// lazily; extra index nodes needed to address the retained leaves remain too.
    pub fn with_retained_leaves(pool: &'a ChunkPool, leaves: u32) -> Self {
        let mut buffer = Self::new(pool);
        buffer.retain_leaves = leaves;
        buffer
    }

    /// Start a new zero-based byte range, retaining only the configured capacity.
    pub fn reset(&mut self) {
        self.start = 0;
        self.end = 0;
        if self.retained == 1 && self.depth == 0 && self.base == 0 {
            // The only allocation is the retained first leaf: nothing to prune,
            // and no index nodes or mappings to update. Old bytes stay invisible.
            self.tail = self.root;
            return;
        }
        self.prune();
        if self.retained != 0 {
            self.tail = self.leaf(0);
        }
    }

    pub fn start(&self) -> u64 {
        self.start
    }
    pub fn end(&self) -> u64 {
        self.end
    }
    pub fn len(&self) -> u64 {
        self.end - self.start
    }
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
    pub fn depth(&self) -> Option<u8> {
        (self.root != NONE).then_some(self.depth)
    }

    // Spans are measured in leaves. The maximum depth covers all u64 byte offsets.
    fn span(depth: u8) -> u128 {
        1u128 << (INDEX_BITS * depth as u32)
    }

    fn ptr(&self, index: u32) -> *mut u8 {
        // SAFETY: private callers use indexes exclusively owned by this tree.
        unsafe { self.pool.mapping.base.as_ptr().add(index as usize * CHUNK_BYTES) }
    }

    fn child(&self, node: u32, slot: usize) -> u32 {
        // SAFETY: node is an initialized, aligned index block; slot < INDEX_ENTRIES.
        unsafe { self.ptr(node).cast::<u32>().add(slot).read() }
    }

    fn set_child(&mut self, node: u32, slot: usize, child: u32) {
        // SAFETY: exclusive tree access; node and slot are valid index storage.
        unsafe { self.ptr(node).cast::<u32>().add(slot).write(child) }
    }

    fn index_block(
        &self,
        random: &mut impl FnMut() -> u64,
    ) -> Result<Block<'a, CHUNK_BYTES, CHUNK_BYTES>> {
        let block = self.pool.alloc(random())?;
        // SAFETY: this new block is exclusively owned, and all bits set is NONE.
        unsafe { block.as_ptr().write_bytes(0xff, CHUNK_BYTES) };
        Ok(block)
    }

    fn take(block: Block<'a, CHUNK_BYTES, CHUNK_BYTES>) -> u32 {
        let index = block.index();
        // Ownership moves into the tree; release() returns it exactly once.
        std::mem::forget(block);
        index
    }

    fn release(&self, index: u32) {
        self.pool.retire(index);
    }

    // Add a missing path. New nodes remain RAII-owned until their child is
    // successfully installed, so allocation errors or a panicking RNG cannot
    // leak a detached subtree. Existing nodes change only after child success.
    fn insert(
        &mut self,
        node: u32,
        depth: u8,
        relative: u128,
        random: &mut impl FnMut() -> u64,
    ) -> Result<u32> {
        if depth == 0 {
            return if node == NONE {
                Ok(Self::take(self.pool.alloc(random())?))
            } else {
                Ok(node)
            };
        }
        let owned = if node == NONE { Some(self.index_block(random)?) } else { None };
        let node = owned.as_ref().map_or(node, Block::index);
        let span = Self::span(depth - 1);
        let slot = (relative / span) as usize;
        let child = self.insert(self.child(node, slot), depth - 1, relative % span, random)?;
        self.set_child(node, slot, child);
        Ok(owned.map_or(node, Self::take))
    }

    fn ensure_leaf(&mut self, leaf: u64, random: &mut impl FnMut() -> u64) -> Result<u32> {
        if self.root == NONE {
            self.base = leaf;
            self.depth = 0;
        }
        while leaf as u128 >= self.base as u128 + Self::span(self.depth) {
            let depth = self.depth + 1;
            debug_assert!(depth <= MAX_DEPTH);
            let span = Self::span(depth);
            let base = (self.base as u128 / span * span) as u64;
            let parent = self.index_block(random)?;
            let slot = ((self.base - base) as u128 / Self::span(self.depth)) as usize;
            self.set_child(parent.index(), slot, self.root);
            self.root = Self::take(parent);
            self.base = base;
            self.depth = depth;
        }
        self.root = self.insert(self.root, self.depth, (leaf - self.base) as u128, random)?;
        self.allocated_end = self.allocated_end.max(leaf + 1);
        if leaf < self.retain_leaves as u64 {
            self.retained = self.retained.max(leaf as u32 + 1);
        }
        Ok(self.leaf(leaf))
    }

    fn leaf(&self, leaf: u64) -> u32 {
        let mut node = self.root;
        let mut relative = (leaf - self.base) as u128;
        for depth in (1..=self.depth).rev() {
            let span = Self::span(depth - 1);
            node = self.child(node, (relative / span) as usize);
            relative %= span;
        }
        debug_assert_ne!(node, NONE);
        node
    }

    /// Append initialized bytes. Calls the caller's RNG once per pool allocation.
    /// On allocation failure the original byte range and contents are preserved.
    /// No leaf bytes outside the appended range are cleared or exposed.
    #[inline]
    pub fn append(&mut self, bytes: &[u8], random: impl FnMut() -> u64) -> Result<()> {
        self.end
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::new("segmented buffer offset overflow"))?;
        if bytes.is_empty() {
            return Ok(());
        }
        let within = self.end as usize % CHUNK_BYTES;
        if self.tail != NONE
            && (within != 0 || self.end == 0)
            && bytes.len() <= CHUNK_BYTES - within
        {
            // SAFETY: tail owns the leaf containing end - 1, or retained leaf zero
            // after reset. Nonzero boundaries are excluded to avoid overwriting
            // the previous leaf. This write stays in the cached leaf, and the
            // exclusive borrow prevents source aliasing. Overflow was checked above.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    self.ptr(self.tail).add(within),
                    bytes.len(),
                );
            }
            self.end += bytes.len() as u64;
            return Ok(());
        }
        self.append_slow(bytes, random)
    }

    fn append_slow(&mut self, bytes: &[u8], mut random: impl FnMut() -> u64) -> Result<()> {
        #[cfg(test)]
        tests::APPEND_SLOW_CALLS.with(|count| count.set(count.get() + 1));
        let old_end = self.end;
        self.tail = NONE;
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let index = match self.ensure_leaf(self.end / CHUNK_BYTES as u64, &mut random) {
                Ok(index) => index,
                Err(error) => {
                    self.end = old_end;
                    self.prune();
                    return Err(error);
                }
            };
            let offset = self.end as usize % CHUNK_BYTES;
            let count = remaining.len().min(CHUNK_BYTES - offset);
            // SAFETY: exclusively owned leaf, bounded destination. The source
            // cannot alias this mutable buffer through the safe API.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    remaining.as_ptr(),
                    self.ptr(index).add(offset),
                    count,
                )
            };
            self.end += count as u64;
            self.tail = index;
            remaining = &remaining[count..];
        }
        Ok(())
    }

    /// Raw spare space in the currently initialized tail leaf. Does not allocate.
    /// The pointer retains the provenance of the whole owned leaf. It remains
    /// valid until a buffer mutation that can release that leaf. Bytes written
    /// here are not readable through this buffer until committed below.
    pub fn spare_tail_ptr(&mut self) -> (*mut u8, usize) {
        let within = self.end as usize % CHUNK_BYTES;
        if self.is_empty() || within == 0 {
            return (std::ptr::NonNull::<u8>::dangling().as_ptr(), 0);
        }
        let index = self.leaf(self.end / CHUNK_BYTES as u64);
        let spare = ((CHUNK_BYTES - within) as u64).min(u64::MAX - self.end) as usize;
        // SAFETY: within is inside this owned leaf. No reference to spare bytes
        // is constructed, since they may still be uninitialized.
        (unsafe { self.ptr(index).add(within) }, spare)
    }

    /// Make bytes written into the current spare tail part of the live range.
    ///
    /// # Safety
    /// The caller must have initialized the next `count` bytes of the spare tail.
    /// No borrowed view of those bytes may remain live across this mutation.
    pub unsafe fn advance_initialized_tail(&mut self, count: usize) {
        let (_, spare) = self.spare_tail_ptr();
        assert!(count <= spare, "initialized bytes exceed spare tail");
        self.end += count as u64;
    }

    /// Borrow the initialized suffix of the leaf containing offset, clipped to
    /// the live range. Returns None for an offset outside start..end.
    pub fn chunk(&self, offset: u64) -> Option<&[u8]> {
        if offset < self.start || offset >= self.end {
            return None;
        }
        let index = self.leaf(offset / CHUNK_BYTES as u64);
        let within = offset as usize % CHUNK_BYTES;
        let count = (self.end - offset).min((CHUNK_BYTES - within) as u64) as usize;
        // SAFETY: only appended, initialized bytes are exposed. The shared
        // borrow prevents mutation or retirement of this leaf.
        Some(unsafe { std::slice::from_raw_parts(self.ptr(index).add(within), count) })
    }

    pub fn chunks(&self) -> Chunks<'_, 'a> {
        Chunks { buffer: self, offset: self.start }
    }

    fn check_range(&self, offset: u64, count: usize) -> Result<()> {
        if offset < self.start || offset.checked_add(count as u64).is_none_or(|end| end > self.end)
        {
            return Err(Error::new("range outside segmented buffer"));
        }
        Ok(())
    }

    /// Copy an entire requested range, including across leaf boundaries.
    /// Invalid ranges leave the destination unchanged.
    pub fn read_at(&self, mut offset: u64, mut destination: &mut [u8]) -> Result<()> {
        self.check_range(offset, destination.len())?;
        while !destination.is_empty() {
            let chunk = self.chunk(offset).unwrap();
            let count = chunk.len().min(destination.len());
            destination[..count].copy_from_slice(&chunk[..count]);
            destination = &mut destination[count..];
            offset += count as u64;
        }
        Ok(())
    }

    /// Overwrite an existing range without extending it or allocating blocks.
    pub fn write_at(&mut self, mut offset: u64, mut bytes: &[u8]) -> Result<()> {
        self.check_range(offset, bytes.len())?;
        while !bytes.is_empty() {
            let index = self.leaf(offset / CHUNK_BYTES as u64);
            let within = offset as usize % CHUNK_BYTES;
            let count = bytes.len().min(CHUNK_BYTES - within);
            // SAFETY: exclusive access to this live range, with no source alias.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr(index).add(within), count)
            };
            bytes = &bytes[count..];
            offset += count as u64;
        }
        Ok(())
    }

    /// Discard bytes before offset. A partially consumed first leaf remains
    /// allocated; fully consumed leaves and empty index nodes are returned.
    pub fn discard_before(&mut self, offset: u64) -> Result<()> {
        self.check_range(offset, 0)?;
        let old_leaf = self.start / CHUNK_BYTES as u64;
        self.start = offset;
        if self.is_empty() || offset / CHUNK_BYTES as u64 != old_leaf {
            self.prune();
        }
        Ok(())
    }

    /// Return the pool shared by this buffer's nodes.
    pub fn pool(&self) -> &'a ChunkPool {
        self.pool
    }

    /// Release bytes at and after an absolute offset within the live range.
    pub fn truncate(&mut self, end: u64) -> Result<()> {
        self.check_range(end, 0)?;
        self.end = end;
        self.prune();
        Ok(())
    }

    /// Borrow disjoint initialized leaf slices for a writable range.
    pub fn chunks_mut_range(&mut self, range: std::ops::Range<u64>) -> Result<ChunksMut<'_, 'a>> {
        if range.start < self.start || range.end > self.end || range.start > range.end {
            return Err(Error::new("range outside segmented buffer"));
        }
        Ok(ChunksMut { buffer: self, offset: range.start, end: range.end })
    }

    /// Clear the live range, retaining the absolute end and any configured prefix capacity.
    pub fn clear(&mut self) {
        self.start = self.end;
        self.prune();
    }

    fn release_tree(&self, node: u32, depth: u8) {
        if node == NONE {
            return;
        }
        if depth != 0 {
            for slot in 0..INDEX_ENTRIES {
                self.release_tree(self.child(node, slot), depth - 1);
            }
        }
        self.release(node);
    }

    fn retain(&mut self, node: u32, depth: u8, base: u128, first: u128, end: u128) -> u32 {
        if node == NONE {
            return NONE;
        }
        let limit = (base + Self::span(depth)).min(self.allocated_end as u128);
        let cached = self.retained as u128;
        if (limit <= first || base >= end) && base >= cached {
            self.release_tree(node, depth);
            return NONE;
        }
        if (first <= base && limit <= end) || limit <= cached {
            return node;
        }
        debug_assert_ne!(depth, 0);
        let span = Self::span(depth - 1);
        for slot in 0..INDEX_ENTRIES {
            let child = self.child(node, slot);
            let retained = self.retain(child, depth - 1, base + slot as u128 * span, first, end);
            if retained != child {
                self.set_child(node, slot, retained);
            }
        }
        node
    }

    fn prune(&mut self) {
        #[cfg(test)]
        tests::PRUNE_CALLS.with(|count| count.set(count.get() + 1));
        self.tail = NONE;
        if self.is_empty() && self.retained == 0 {
            drop(self.alias.take());
            self.release_tree(self.root, self.depth);
            self.root = NONE;
            self.depth = 0;
            self.base = 0;
            self.allocated_end = 0;
            return;
        }
        let end = (self.end as u128).div_ceil(CHUNK_BYTES as u128);
        let first = if self.is_empty() { end } else { self.start as u128 / CHUNK_BYTES as u128 };
        self.root = self.retain(self.root, self.depth, self.base as u128, first, end);
        let first = if self.retained != 0 { 0 } else { first };
        let end = if self.is_empty() { 0 } else { end }.max(self.retained as u128);
        self.allocated_end = end as u64;
        // Collapse single-child roots; a rolling window need not retain the
        // ancestors of discarded portions of its address space.
        while self.depth > 0 {
            let span = Self::span(self.depth - 1);
            let first_slot = (first - self.base as u128) / span;
            let last_slot = (end - 1 - self.base as u128) / span;
            if first_slot != last_slot {
                break;
            }
            let root = self.root;
            self.root = self.child(root, first_slot as usize);
            self.base = (self.base as u128 + first_slot * span) as u64;
            self.depth -= 1;
            self.release(root);
        }
    }
}

impl Drop for SegmentedBuffer<'_> {
    fn drop(&mut self) {
        drop(self.alias.take());
        self.release_tree(self.root, self.depth);
    }
}

pub struct Chunks<'b, 'p> {
    buffer: &'b SegmentedBuffer<'p>,
    offset: u64,
}

impl<'b> Iterator for Chunks<'b, '_> {
    type Item = &'b [u8];
    fn next(&mut self) -> Option<Self::Item> {
        let chunk = self.buffer.chunk(self.offset)?;
        self.offset += chunk.len() as u64;
        Some(chunk)
    }
}
impl std::iter::FusedIterator for Chunks<'_, '_> {}

/// Mutable initialized slices, each belonging to a distinct leaf.
pub struct ChunksMut<'b, 'p> {
    buffer: &'b mut SegmentedBuffer<'p>,
    offset: u64,
    end: u64,
}
impl<'b> Iterator for ChunksMut<'b, '_> {
    type Item = &'b mut [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.end {
            return None;
        }
        let index = self.buffer.leaf(self.offset / CHUNK_BYTES as u64);
        let within = self.offset as usize % CHUNK_BYTES;
        let count = (self.end - self.offset).min((CHUNK_BYTES - within) as u64) as usize;
        self.offset += count as u64;
        // SAFETY: the iterator exclusively borrows the tree for 'b and yields
        // each leaf at most once. All bytes in its validated range are initialized.
        Some(unsafe { std::slice::from_raw_parts_mut(self.buffer.ptr(index).add(within), count) })
    }
}
impl std::iter::FusedIterator for ChunksMut<'_, '_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn pool(max: u32) -> ChunkPool {
        ChunkPool::with_buckets(max, 0, 1).unwrap()
    }

    fn audit(buffer: &SegmentedBuffer<'_>) -> usize {
        fn walk(
            b: &SegmentedBuffer<'_>,
            node: u32,
            depth: u8,
            base: u128,
            seen: &mut HashSet<u32>,
        ) {
            if node == NONE {
                return;
            }
            assert!(seen.insert(node), "duplicate tree ownership");
            assert!(node < b.pool.issued_blocks());
            assert_eq!(b.ptr(node) as usize % CHUNK_BYTES, 0);
            if depth == 0 {
                assert!(
                    base < b.retained as u128
                        || (!b.is_empty()
                            && base >= b.start as u128 / CHUNK_BYTES as u128
                            && base < (b.end as u128).div_ceil(CHUNK_BYTES as u128))
                );
            } else {
                let span = SegmentedBuffer::span(depth - 1);
                let mut children = 0;
                for slot in 0..INDEX_ENTRIES {
                    let child = b.child(node, slot);
                    children += usize::from(child != NONE);
                    walk(b, child, depth - 1, base + slot as u128 * span, seen);
                }
                assert_ne!(children, 0, "empty index node retained");
            }
        }
        if buffer.tail != NONE {
            if buffer.end == 0 {
                assert!(buffer.retained > 0);
                assert_eq!(buffer.tail, buffer.leaf(0));
            } else {
                assert_eq!(buffer.tail, buffer.leaf((buffer.end - 1) / CHUNK_BYTES as u64));
            }
        }
        let mut seen = HashSet::new();
        walk(buffer, buffer.root, buffer.depth, buffer.base as u128, &mut seen);
        assert_eq!(buffer.chunks().map(|c| c.len() as u64).sum::<u64>(), buffer.len());
        seen.len()
    }

    fn all_returned(pool: &ChunkPool, max: usize) {
        let mut held = Vec::new();
        while let Ok(block) = pool.alloc(0) {
            held.push(block);
        }
        assert_eq!(held.len(), max, "lost pool blocks");
        assert_eq!(held.iter().map(Block::index).collect::<HashSet<_>>().len(), max);
    }

    thread_local! {
        pub(super) static APPEND_SLOW_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        pub(super) static PRUNE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    #[test]
    fn retained_first_leaf_reset_and_append_skip_slow_paths() {
        let pool = pool(8);
        let mut b = SegmentedBuffer::with_retained_leaves(&pool, 1);
        b.append(&vec![7; CHUNK_BYTES * 2], || 0).unwrap();
        let first = b.leaf(0);
        // A large buffer still needs its general cleanup, retaining only leaf zero.
        b.reset();
        assert_eq!(audit(&b), 1);
        assert_eq!(b.tail, first);
        let calls = APPEND_SLOW_CALLS.get();
        let prunes = PRUNE_CALLS.get();
        for _ in 0..3 {
            b.reset();
            b.reset(); // already empty
            assert_eq!(PRUNE_CALLS.get(), prunes);
            assert!(b.chunk(0).is_none());
            b.append(&vec![3; CHUNK_BYTES], || panic!("retained allocation")).unwrap();
            assert_eq!(APPEND_SLOW_CALLS.get(), calls);
            assert_eq!(b.chunk(0), Some(vec![3; CHUNK_BYTES].as_slice()));
            assert_eq!(b.tail, first);
        }
        // A full leaf's tail must never be mistaken for the NEXT leaf.
        b.append(&[9], || 0).unwrap();
        assert_eq!(b.chunk(0), Some(vec![3; CHUNK_BYTES].as_slice()));
        assert_eq!(b.chunk(CHUNK_BYTES as u64), Some([9].as_slice()));
        audit(&b);
        drop(b);
        all_returned(&pool, 8);
    }

    #[test]
    fn bounded_retention_reuses_leaves_without_exposing_old_bytes() {
        for keep in [0, 1, 2, 4] {
            let pool = pool(32);
            let mut b = SegmentedBuffer::with_retained_leaves(&pool, keep);
            b.append(&vec![0xa5; CHUNK_BYTES * 6], || 0).unwrap();
            let indexes: Vec<_> = (0..keep).map(|i| b.leaf(i as u64)).collect();
            b.discard_before((CHUNK_BYTES * 4) as u64).unwrap();
            assert_eq!(audit(&b), keep as usize + 2 + 1);
            b.reset();
            assert_eq!(b.len(), 0);
            assert!(b.chunk(0).is_none());
            assert_eq!(audit(&b), keep as usize + usize::from(keep > 1));
            for i in 0..keep {
                b.append(&vec![i as u8; CHUNK_BYTES], || panic!("retained leaf allocated"))
                    .unwrap();
                assert_eq!(b.leaf(i as u64), indexes[i as usize]);
            }
            b.discard_before(b.end()).unwrap();
            b.reset();
            assert_eq!(audit(&b), keep as usize + usize::from(keep > 1));
            b.append(b"new", || 0).unwrap();
            b.truncate(0).unwrap();
            b.append(b"ok", || {
                assert_eq!(keep, 0, "retained leaf allocated");
                0
            })
            .unwrap();
            assert_eq!(b.chunk(0), Some(b"ok".as_slice()));
            drop(b);
            all_returned(&pool, 32);
        }
    }

    #[test]
    fn packed_append_read_overwrite_and_bounds() {
        let pool = pool(16);
        let mut b = SegmentedBuffer::new(&pool);
        b.append(&[], || panic!("empty append allocated")).unwrap();
        assert_eq!(b.depth(), None);
        let mut expected: Vec<u8> = (0..CHUNK_BYTES * 3 + 17).map(|i| (i % 251) as u8).collect();
        for part in expected.chunks(37) {
            b.append(part, || 0).unwrap();
        }
        assert_eq!(audit(&b), 5); // four leaves and one index
        assert_eq!(b.depth(), Some(1));
        for chunk in b.chunks() {
            assert_eq!(chunk.as_ptr() as usize % CHUNK_BYTES, 0);
        }
        b.write_at((CHUNK_BYTES - 3) as u64, &[255; 19]).unwrap();
        expected[CHUNK_BYTES - 3..CHUNK_BYTES + 16].fill(255);
        let mut actual = vec![0; expected.len()];
        b.read_at(0, &mut actual).unwrap();
        assert_eq!(actual, expected);
        let mut small = [42; 8];
        assert!(b.read_at(b.end() - 1, &mut small).is_err());
        assert_eq!(small, [42; 8]);
        assert!(b.write_at(u64::MAX, &[1, 2]).is_err());
        assert!(b.discard_before(b.end() + 1).is_err());
        assert!(b.chunk(b.end()).is_none());
        b.read_at(b.end(), &mut []).unwrap();
        b.discard_before((CHUNK_BYTES + 1) as u64).unwrap();
        assert_eq!(b.chunks().flatten().copied().collect::<Vec<_>>(), expected[CHUNK_BYTES + 1..]);
        assert!(b.read_at(CHUNK_BYTES as u64, &mut []).is_err());
        audit(&b);
        drop(b);
        all_returned(&pool, 16);
    }

    #[test]
    fn grows_to_depth_two_and_reclaims_index_nodes() {
        // Exercise both sides of an index boundary without allocating an
        // entire index node's worth of data (quadratic in block size).
        let pool = pool(16);
        let boundary = (CHUNK_BYTES * INDEX_ENTRIES) as u64;
        let start = boundary - 2 * CHUNK_BYTES as u64;
        let mut b = SegmentedBuffer::with_start(&pool, start);
        let bytes = vec![73; CHUNK_BYTES * 3];
        b.append(&bytes, || 0).unwrap();
        assert_eq!(b.depth(), Some(2));
        assert_eq!(audit(&b), 6); // three leaves, two lower indexes, one root
        b.discard_before(boundary).unwrap();
        assert_eq!(b.depth(), Some(0));
        assert_eq!(audit(&b), 1);
        assert_eq!(b.chunk(boundary).unwrap(), &[73; CHUNK_BYTES]);
        let end = start + bytes.len() as u64;
        b.clear();
        assert!(b.is_empty());
        assert_eq!(audit(&b), 0);
        assert_eq!(b.start(), end);
        b.append(&[12], || 0).unwrap();
        assert_eq!(b.chunk(end), Some(&[12][..]));
        drop(b);
        all_returned(&pool, 16);
    }

    #[test]
    fn crosses_every_tree_depth_without_allocating_the_address_space() {
        let pool = pool(32);
        for depth in 1..=MAX_DEPTH {
            let boundary = 1u64 << (CHUNK_BITS + INDEX_BITS * (depth - 1) as u32);
            let mut b = SegmentedBuffer::with_start(&pool, boundary - 1);
            b.append(&[23, 45], || 0).unwrap();
            assert_eq!(b.depth(), Some(depth));
            assert_eq!(b.chunks().flatten().copied().collect::<Vec<_>>(), [23, 45]);
            assert_eq!(audit(&b), 1 + 2 * depth as usize);
            b.discard_before(boundary).unwrap();
            assert_eq!(b.depth(), Some(0));
            assert_eq!(audit(&b), 1);
            assert_eq!(b.chunk(boundary), Some(&[45][..]));
        }
        let mut b = SegmentedBuffer::with_start(&pool, u64::MAX - 3);
        b.append(&[1, 2, 3], || 0).unwrap();
        assert!(b.append(&[4], || panic!("overflow allocated")).is_err());
        assert_eq!(b.end(), u64::MAX);
        assert_eq!(b.chunks().flatten().copied().collect::<Vec<_>>(), [1, 2, 3]);
        b.clear();
        b.append(&[], || 0).unwrap();
        assert!(b.append(&[4], || 0).is_err());
        drop(b);
        all_returned(&pool, 32);
    }

    #[test]
    fn allocation_failure_rolls_back_and_returns_every_block() {
        // Cross the maximum-depth boundary, failing at each intermediate allocation.
        for max in 1..=1 + 2 * MAX_DEPTH as u32 {
            let pool = pool(max);
            let mut b = SegmentedBuffer::with_start(&pool, (1u64 << 62) - 2);
            b.append(&[77], || 0).unwrap();
            let result = b.append(&[88, 99], || 0);
            if max < 1 + 2 * MAX_DEPTH as u32 {
                assert!(result.is_err());
                assert_eq!(b.len(), 1);
                assert_eq!(b.depth(), Some(0));
                assert_eq!(audit(&b), 1);
                assert_eq!(b.chunk(b.start()), Some(&[77][..]));
            } else {
                result.unwrap();
                audit(&b);
            }
            drop(b);
            all_returned(&pool, max as usize);
        }
        let pool = pool(2);
        let mut b = SegmentedBuffer::new(&pool);
        assert!(b.append(&[5; CHUNK_BYTES * 2], || 0).is_err());
        assert!(b.is_empty());
        assert_eq!(audit(&b), 0);
        drop(b);
        all_returned(&pool, 2);
    }

    #[test]
    fn rng_panic_does_not_lose_detached_nodes() {
        for allowed in 0..1 + 2 * MAX_DEPTH as usize {
            let pool = pool(32);
            let mut b = SegmentedBuffer::with_start(&pool, (1u64 << 62) - 1);
            b.append(&[3], || 0).unwrap();
            let mut calls = 0;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = b.append(&[7], || {
                    assert!(calls < allowed, "injected RNG panic");
                    calls += 1;
                    0
                });
            }));
            drop(b);
            all_returned(&pool, 32);
        }
    }

    #[test]
    fn rolling_buffer_matches_a_contiguous_model() {
        let pool = pool(128);
        let mut b =
            SegmentedBuffer::with_start(&pool, ((CHUNK_BYTES * INDEX_ENTRIES) as u64) - 17000);
        let mut model = Vec::new();
        let mut state = 0x123456789abcdefu64;
        let mut random = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for iteration in 0..1500 {
            let count = (random() % 9000) as usize;
            let input = vec![iteration as u8; count];
            if iteration % 3 == 0 {
                use std::io::Write;
                b.writer(&mut random).write_all(&input).unwrap();
            } else {
                b.append(&input, &mut random).unwrap();
            }
            model.extend_from_slice(&input);
            if iteration % 5 == 0 {
                let keep = random() as usize % (model.len() + 1);
                b.truncate(b.start() + keep as u64).unwrap();
                model.truncate(keep);
            }
            if iteration % 17 == 0 {
                b.clear();
                model.clear();
            }
            if !model.is_empty() {
                let offset = random() as usize % model.len();
                let count = (model.len() - offset).min(19);
                b.write_at(b.start() + offset as u64, &[241; 19][..count]).unwrap();
                model[offset..offset + count].fill(241);
            }
            let discard = random() as usize % (model.len() + 1);
            b.discard_before(b.start() + discard as u64).unwrap();
            model.drain(..discard);
            assert_eq!(b.chunks().flatten().copied().collect::<Vec<_>>(), model);
            audit(&b);
        }
        drop(b);
        all_returned(&pool, 128);
    }
}
