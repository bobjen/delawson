//! Per-EN extent index used by SN for sync.
//!
//! Each EN's root is inline in its SN-side row. The root starts as a leaf, so
//! small trees need no node-pool allocation. At greater heights it is an inline
//! branch. Height zero means leaf; branches and the root terminate their child
//! arrays with `NONE` instead of storing child counts. Leaf items use raw u32
//! slots. Scans retain their path through the branches instead of linking
//! leaves.

use common::ids::ExtentId;
use common::{Error, Result};
use pool::{LockedItem, LockedPool, PoolItem};
use std::mem::{MaybeUninit, align_of, offset_of, size_of};

const NONE: u32 = u32::MAX;
// The tuning knob: each u32 slot contributes four bytes to a pooled node.
const NODE_SLOTS_LOG2: usize = 5;
const NODE_SLOTS: usize = 1 << NODE_SLOTS_LOG2;
const NODE_BYTES: usize = NODE_SLOTS * size_of::<u32>();
const NODE_LEAF_CAPACITY: usize = NODE_SLOTS;
const ROOT_LEAF_CAPACITY: usize = NODE_SLOTS;
const KEY_SCRATCH: usize = ROOT_LEAF_CAPACITY + 2;
const MERGE_KEY_SCRATCH: usize = NODE_LEAF_CAPACITY * 2 + 2;
// Branch nodes hold sorted child minima and links in matching sparse slots.
// Stored key-first: the separator keys occupy the first half of the node and
// child indexes the second, so a search probes the keys contiguously.
const BRANCH_SLOTS: usize = NODE_SLOTS / 2;
const BRANCH_CAPACITY: usize = BRANCH_SLOTS;
const BRANCH_KEYS_SCRATCH: usize = BRANCH_SLOTS + 1;
const BRANCH_CHILDREN_SCRATCH: usize = BRANCH_SLOTS + 1;
const CURSOR_PATH: usize = u8::MAX as usize + 1;

const fn node_alignment(bytes: usize) -> usize {
    let mut alignment = 64;
    while alignment > 4 && bytes % alignment != 0 {
        alignment /= 2;
    }
    alignment
}

const NODE_ALIGNMENT: usize = node_alignment(NODE_BYTES);
const _: () = assert!(NODE_SLOTS >= 8);
const _: () = assert!(BRANCH_CAPACITY > 0);
const _: () = assert!(NODE_BYTES % NODE_ALIGNMENT == 0);

type BranchKeys = [u32; BRANCH_KEYS_SCRATCH];
type BranchChildren = [u32; BRANCH_CHILDREN_SCRATCH];
type KeyScratch = [u32; KEY_SCRATCH];
type Nodes = LockedPool<Node, NODE_ALIGNMENT, { offset_of!(Node, words) }>;

#[repr(C)]
#[derive(Clone, Copy)]
struct Node {
    words: [u32; NODE_SLOTS],
}

const _: [(); NODE_BYTES] = [(); size_of::<Node>()];
const _: () = assert!(align_of::<Node>() <= NODE_ALIGNMENT);

impl Node {
    fn blank() -> Self {
        Self { words: [NONE; NODE_SLOTS] }
    }

    fn branch(keys: &[u32], children: &[u32]) -> Self {
        debug_assert_eq!(keys.len(), BRANCH_SLOTS);
        debug_assert_eq!(children.len(), BRANCH_SLOTS);
        let mut node = Self::blank();
        write_branch(&mut node.words, keys, children);
        node
    }

    fn leaf(keys: &[u32]) -> Self {
        debug_assert!(!keys.is_empty());
        debug_assert!(keys.len() <= NODE_LEAF_CAPACITY);
        let mut node = Self::blank();
        node.words[..keys.len()].copy_from_slice(keys);
        node
    }

    fn leaf_parts(self, keys: &mut KeyScratch) -> usize {
        let mut count = 0;
        for &key in &self.words {
            if key != NONE {
                keys[count] = key;
                count += 1;
            }
        }
        debug_assert!(keys[..count].windows(2).all(|pair| pair[0] < pair[1]));
        count
    }

    fn branch_parts(self) -> (BranchKeys, BranchChildren, usize) {
        branch_parts(&self.words)
    }
}

// SAFETY: a fresh pool slot is initialized as a complete empty node.
unsafe impl PoolItem for Node {
    unsafe fn init(slot: *mut Self) {
        // SAFETY: LockedPool supplies aligned, fresh storage for Node.
        unsafe { slot.write(Self::blank()) };
    }
}

// SAFETY: word zero is available for LockedPool's link while the node is free.
unsafe impl LockedItem for Node {
    const LINK_NONE: u32 = NONE;
}

/// Shared node storage for all per-EN extent trees in one SN.
pub(super) struct ExtentTreePool {
    slots: Nodes,
}

impl ExtentTreePool {
    pub(super) fn new(max_nodes: u32) -> Result<Self> {
        let slots = Nodes::new(max_nodes, 0)
            .map_err(|error| Error::new(format!("create EN extent-tree pool: {error}")))?;
        Ok(Self { slots })
    }

    /// Reclaim nodes retired by tree deletion. Call under the owning X lock.
    pub(super) fn reclaim(&mut self) {
        self.slots.reclaim();
    }

    fn ptr(&self, index: u32) -> *mut Node {
        // SAFETY: callers pass an issued pool index; the mapping is stationary.
        unsafe { self.slots.base_ptr().as_ptr().add(index as usize) }
    }

    fn read(&self, index: u32) -> Node {
        // SAFETY: the tree references only live nodes, and callers hold the
        // appropriate owner lock while reading or changing them.
        unsafe { self.ptr(index).read() }
    }

    fn words_ptr(&self, ptr: *mut Node) -> &[u32; NODE_SLOTS] {
        // SAFETY: cursor pointers come from this pool's stable mapping and
        // remain live while the caller holds the lock protecting the tree.
        unsafe { &(*ptr).words }
    }

    fn words_mut_ptr(&mut self, ptr: *mut Node) -> &mut [u32; NODE_SLOTS] {
        // SAFETY: cursor pointers come from this pool's stable mapping and
        // remain live while the caller holds the tree's exclusive lock.
        unsafe { &mut (*ptr).words }
    }

    fn read_word(&self, ptr: *mut Node, slot: usize) -> u32 {
        debug_assert!(slot < NODE_SLOTS);
        // SAFETY: cursor pointers come from this pool's stable mapping and
        // remain live while the caller holds the lock protecting the tree.
        unsafe { (*ptr).words[slot] }
    }

    fn write(&mut self, index: u32, node: Node) {
        // SAFETY: the tree owns this live node exclusively during mutation.
        unsafe { self.ptr(index).write(node) };
    }

    fn write_ptr(&mut self, ptr: *mut Node, node: Node) {
        // SAFETY: cursor pointers come from this pool's stable mapping and
        // remain live while the caller holds the tree's exclusive lock.
        unsafe { ptr.write(node) };
    }

    fn index_of(&self, ptr: *mut Node) -> u32 {
        self.slots.index_of(ptr.cast()).expect("cursor node pointer must belong to this pool")
    }

    /// Reserve all nodes an insertion can need before it mutates the tree.
    /// This keeps pool exhaustion from leaving a partially split path.
    fn reserve_nodes(&mut self, count: usize) -> Result<Vec<u32>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let indexes = self
            .slots
            .alloc_many(count)
            .map_err(|error| Error::new(format!("allocate EN extent-tree nodes: {error}")))?;
        for &index in &indexes {
            self.write(index, Node::blank());
        }
        Ok(indexes)
    }

    fn retire(&mut self, index: u32) {
        // SAFETY: callers detach this node from its tree before retiring it.
        unsafe { self.slots.retire_index(index) };
    }
}

/// One EN's ordered ExtentId index, embedded in that EN's row in the SN table.
/// The inline root is deliberately not allocated from the node pool.
pub(super) struct EnExtentTree {
    height: u8,
    len: u32,
    _reserved: [u8; 8],
    root_words: [u32; NODE_SLOTS],
}

impl Default for EnExtentTree {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
enum DeleteResult {
    NotFound,
    /// The subtree's new minimum, or NONE if the subtree became empty.
    Removed {
        minimum: u32,
        merge_candidate: bool,
    },
}

#[derive(Clone, Copy)]
struct CursorPath {
    node: *mut Node,
    slot: u16,
}

/// A short-lived search position. Its path is valid only until the tree is
/// modified; the caller is responsible for keeping it under the appropriate
/// global lock. It deliberately does not borrow or lock the tree.
pub(super) struct Cursor {
    key: u32,
    height: u8,
    // Entries are indexed by subtree height: path[0] is the leaf, each higher
    // index is its parent, and path[height] is the inline root branch.
    path: [MaybeUninit<CursorPath>; CURSOR_PATH],
    found: bool,
}

impl Cursor {
    pub(super) fn new(key: u32) -> Self {
        Self { key, height: 0, path: [MaybeUninit::uninit(); CURSOR_PATH], found: false }
    }

    fn find_sparse_leaf(&mut self, node: *mut Node, words: &[u32; NODE_SLOTS]) {
        let Some((first, last)) = leaf_bounds(words) else {
            self.set_path(0, node, NODE_SLOTS);
            self.found = false;
            return;
        };
        let (found, next) = sparse_leaf_find(words, self.key, first, last);
        self.set_path(0, node, found.or(next).unwrap_or(last + 1));
        self.found = found.is_some();
    }

    fn leaf(&self) -> CursorPath {
        self.path(0)
    }

    fn set_path(&mut self, level: usize, node: *mut Node, slot: usize) {
        self.path[level].write(CursorPath { node, slot: slot as u16 });
    }

    fn path(&self, level: usize) -> CursorPath {
        // SAFETY: find() initializes every path entry from 0..=height before
        // use; next_leaf() only reads entries in that range.
        unsafe { *self.path[level].assume_init_ref() }
    }

    fn insert_reserve_count(
        &self,
        tree: &EnExtentTree,
        pool: &ExtentTreePool,
        leaf_full: bool,
    ) -> Result<usize> {
        if self.height == 0 {
            return Ok(if leaf_full { 2 } else { 0 });
        }
        if !leaf_full {
            return Ok(0);
        }

        let mut needed = 1; // one new right leaf
        let mut split_carries = true;
        for level in 1..self.height as usize {
            if split_carries && !branch_has_hole_words(pool.words_ptr(self.path(level).node)) {
                needed += 1; // full pooled branch splits and carries upward
            } else {
                split_carries = false;
                break;
            }
        }
        if split_carries && !tree.root_has_branch_hole() {
            if self.height == u8::MAX {
                return Err(Error::new("EN extent-tree height overflow"));
            }
            needed += 2; // the inline root becomes two pooled branches
        }
        Ok(needed)
    }

    fn next_leaf(&mut self, tree: &EnExtentTree, pool: &ExtentTreePool) -> bool {
        let height = self.height as usize;
        if height == 0 {
            return false;
        }

        for level in 1..=height {
            let path = self.path(level);
            let words = if level == height { &tree.root_words } else { pool.words_ptr(path.node) };
            let Some(slot) = next_branch_slot_words(words, path.slot as usize + 1) else {
                continue;
            };
            self.set_path(level, path.node, slot);
            let mut next = words[BRANCH_SLOTS + slot];
            for child_level in (0..level).rev() {
                let node_ptr = pool.ptr(next);
                if child_level == 0 {
                    self.set_path(0, node_ptr, 0);
                    break;
                }
                let words = pool.words_ptr(node_ptr);
                let child_slot = first_branch_slot_words(words).expect("nonempty branch");
                self.set_path(child_level, node_ptr, child_slot);
                next = words[BRANCH_SLOTS + child_slot];
            }
            self.found = false;
            return true;
        }
        false
    }
}

impl EnExtentTree {
    pub(super) fn new() -> Self {
        Self { height: 0, len: 0, _reserved: [0; 8], root_words: [NONE; NODE_SLOTS] }
    }

    pub(super) fn len(&self) -> u32 {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Find a key and retain its path, indexed by subtree height, in `cursor`.
    /// The cursor becomes stale if a caller mutates the tree.
    pub(super) fn find(&self, pool: &ExtentTreePool, key: u32, cursor: &mut Cursor) {
        cursor.key = key;
        cursor.height = self.height;
        if self.height == 0 {
            cursor.find_sparse_leaf(std::ptr::null_mut(), &self.root_words);
            return;
        }

        let mut slot = branch_child(&self.root_words[..BRANCH_SLOTS], key);
        cursor.set_path(self.height as usize, std::ptr::null_mut(), slot);
        let mut index = self.root_words[BRANCH_SLOTS + slot];

        for root_depth in 1..self.height as usize {
            let node_ptr = pool.ptr(index);
            let words = pool.words_ptr(node_ptr);
            slot = branch_child(&words[..BRANCH_SLOTS], key);
            cursor.set_path(self.height as usize - root_depth, node_ptr, slot);
            index = words[BRANCH_SLOTS + slot];
        }

        let leaf_ptr = pool.ptr(index);
        cursor.find_sparse_leaf(leaf_ptr, pool.words_ptr(leaf_ptr));
    }

    pub(super) fn contains(&self, pool: &ExtentTreePool, id: ExtentId) -> bool {
        let key = id.get();
        if key == NONE || self.len == 0 {
            return false;
        }
        let mut cursor = Cursor::new(key);
        self.find(pool, key, &mut cursor);
        cursor.found
    }

    /// Insert an ExtentId. Returns false if it was already present.
    pub(super) fn insert(&mut self, pool: &mut ExtentTreePool, id: ExtentId) -> Result<bool> {
        let key = id.get();
        if key == NONE {
            return Err(Error::new("ExtentId::NONE cannot be indexed"));
        }
        if self.len == u32::MAX {
            return Err(Error::new("EN extent index is full"));
        }

        let mut cursor = Cursor::new(key);
        self.find(pool, key, &mut cursor);
        self.insert_with_cursor(pool, key, &cursor)
    }

    pub(super) fn insert_with_cursor(
        &mut self,
        pool: &mut ExtentTreePool,
        key: u32,
        cursor: &Cursor,
    ) -> Result<bool> {
        debug_assert_eq!(cursor.key, key);
        if cursor.found {
            return Ok(false);
        }
        if self.len == u32::MAX {
            return Err(Error::new("EN extent index is full"));
        }
        let leaf = cursor.leaf();
        let leaf_words =
            if self.height == 0 { &self.root_words } else { pool.words_ptr(leaf.node) };
        let leaf_hole = nearest_leaf_hole(leaf_words, leaf.slot as usize);
        let leaf_full = leaf_hole.is_none();
        let reserve_count = cursor.insert_reserve_count(self, pool, leaf_full)?;
        let mut reserved = pool.reserve_nodes(reserve_count)?;

        if self.height == 0 {
            if let Some(hole) = leaf_hole {
                insert_sparse_leaf(&mut self.root_words, key, leaf.slot as usize, hole);
            } else {
                // A leaf only needs a dense scratch array when it is full and
                // must split. A full leaf has no holes, so its physical order
                // is already the sorted key order.
                let mut keys = [0; KEY_SCRATCH];
                let count = self.root_leaf_parts(&mut keys);
                insert_key(&mut keys, count, leaf.slot as usize, key);
                let split = choose_leaf_split(&keys[..count + 1], NODE_LEAF_CAPACITY);
                let left_index = reserved.pop().expect("root leaf split was pre-reserved");
                let right_index = reserved.pop().expect("root leaf split was pre-reserved");
                pool.write(left_index, Node::leaf(&keys[..split]));
                pool.write(right_index, Node::leaf(&keys[split..count + 1]));
                let (root_keys, root_children) =
                    centered_two_branch_pairs(keys[0], left_index, keys[split], right_index);
                self.write_root_branch(
                    1,
                    &root_keys[..BRANCH_SLOTS],
                    &root_children[..BRANCH_SLOTS],
                );
            }
            debug_assert!(reserved.is_empty());
            self.len += 1;
            return Ok(true);
        }

        let root_path = cursor.path(self.height as usize);
        let child_at = root_path.slot as usize;
        let first_child = first_branch_slot_words(&self.root_words).expect("nonempty root branch");
        if child_at == first_child && key < self.root_words[child_at] {
            self.root_words[child_at] = key;
        }
        if let Some((separator, right)) =
            insert_subtree(pool, self.height - 1, 1, key, &cursor, leaf_hole, &mut reserved)
        {
            if !insert_branch_entry_words(&mut self.root_words, child_at, separator, right) {
                let (dense_keys, dense_children, dense_count) =
                    dense_insert_branch_words(&self.root_words, child_at, separator, right);
                let split_at = dense_count / 2;
                let left_index = reserved.pop().expect("root branch split was pre-reserved");
                let right_index = reserved.pop().expect("root branch split was pre-reserved");
                let (left_keys, left_children) =
                    spread_split_branch(&dense_keys[..split_at], &dense_children[..split_at]);
                let (right_keys, right_children) = spread_split_branch(
                    &dense_keys[split_at..dense_count],
                    &dense_children[split_at..dense_count],
                );
                let left_min = dense_keys[0];
                let right_min = dense_keys[split_at];
                pool.write(
                    left_index,
                    Node::branch(&left_keys[..BRANCH_SLOTS], &left_children[..BRANCH_SLOTS]),
                );
                pool.write(
                    right_index,
                    Node::branch(&right_keys[..BRANCH_SLOTS], &right_children[..BRANCH_SLOTS]),
                );
                let new_height = self.height + 1;
                let (new_keys, new_children) =
                    centered_two_branch_pairs(left_min, left_index, right_min, right_index);
                self.write_root_branch(
                    new_height,
                    &new_keys[..BRANCH_SLOTS],
                    &new_children[..BRANCH_SLOTS],
                );
            }
        }
        debug_assert!(reserved.is_empty());
        self.len += 1;
        Ok(true)
    }

    /// Remove an ExtentId. Returns false if it was not present.
    pub(super) fn remove(&mut self, pool: &mut ExtentTreePool, id: ExtentId) -> Result<bool> {
        let key = id.get();
        if key == NONE || self.len == 0 {
            return Ok(false);
        }
        if self.height == 0 {
            let Some((first, last)) = leaf_bounds(&self.root_words) else {
                return Ok(false);
            };
            let (found, _) = sparse_leaf_find(&self.root_words, key, first, last);
            let Some(slot) = found else {
                return Ok(false);
            };
            match remove_leaf_word(&mut self.root_words, slot, key, None) {
                DeleteResult::NotFound => return Ok(false),
                DeleteResult::Removed { .. } => {
                    self.len -= 1;
                    if self.len == 0 {
                        self.reset_root();
                    }
                    return Ok(true);
                }
            }
        }

        // A standalone remove can search and mutate in one recursive descent.
        // Callers that already have a cursor can use remove_with_cursor().
        let child_at = branch_child(&self.root_words[..BRANCH_SLOTS], key);
        let child = self.root_words[BRANCH_SLOTS + child_at];
        let (minimum, merge_candidate) = match delete_subtree_direct(
            pool,
            child,
            self.height - 1,
            key,
            self.root_words[child_at],
        ) {
            DeleteResult::NotFound => return Ok(false),
            DeleteResult::Removed { minimum, merge_candidate } => (minimum, merge_candidate),
        };
        self.finish_remove(pool, child_at, child, minimum, merge_candidate)
    }

    pub(super) fn remove_with_cursor(
        &mut self,
        pool: &mut ExtentTreePool,
        key: u32,
        cursor: &Cursor,
    ) -> Result<bool> {
        debug_assert_eq!(cursor.key, key);
        if !cursor.found {
            return Ok(false);
        }
        if self.height == 0 {
            match remove_leaf_word(&mut self.root_words, cursor.leaf().slot as usize, key, None) {
                DeleteResult::NotFound => return Ok(false),
                DeleteResult::Removed { .. } => {
                    self.len -= 1;
                    if self.len == 0 {
                        self.reset_root();
                    }
                    return Ok(true);
                }
            }
        }

        let root_path = cursor.path(self.height as usize);
        let child_at = root_path.slot as usize;
        let child = self.root_words[BRANCH_SLOTS + child_at];
        let (minimum, merge_candidate) =
            match delete_subtree(pool, self.height - 1, 1, key, &cursor, self.root_words[child_at])
            {
                DeleteResult::NotFound => return Ok(false),
                DeleteResult::Removed { minimum, merge_candidate } => (minimum, merge_candidate),
            };

        self.finish_remove(pool, child_at, child, minimum, merge_candidate)
    }

    fn finish_remove(
        &mut self,
        pool: &mut ExtentTreePool,
        child_at: usize,
        child: u32,
        minimum: u32,
        merge_candidate: bool,
    ) -> Result<bool> {
        if minimum == NONE {
            remove_branch_child_words(&mut self.root_words, child_at);
            pool.retire(child);
            if first_branch_slot_words(&self.root_words).is_none() {
                self.reset_root();
            }
        } else {
            self.root_words[child_at] = minimum;
            if merge_candidate {
                let (mut root_keys, mut root_children) = branch_values(&self.root_words);
                let removed_slot = if self.height == 1 {
                    merge_leaf_children(pool, &mut root_keys, &mut root_children, child_at)
                } else {
                    merge_branch_children(pool, &mut root_keys, &mut root_children, child_at)
                };
                if removed_slot.is_some() {
                    write_branch(
                        &mut self.root_words,
                        &root_keys[..BRANCH_SLOTS],
                        &root_children[..BRANCH_SLOTS],
                    );
                }
            }
        }

        self.len -= 1;
        if self.len == 0 {
            self.reset_root();
        } else {
            self.collapse_root(pool);
        }
        Ok(true)
    }

    /// Visit ExtentIds in increasing order. Return false from `visit` to stop.
    pub(super) fn for_each(&self, pool: &ExtentTreePool, mut visit: impl FnMut(ExtentId) -> bool) {
        if self.len == 0 {
            return;
        }
        let mut cursor = Cursor::new(0);
        self.find(pool, 0, &mut cursor);
        loop {
            let mut leaf = cursor.leaf();
            while (leaf.slot as usize) < NODE_SLOTS {
                let slot = leaf.slot as usize;
                let key = if cursor.height == 0 {
                    self.root_words[slot]
                } else {
                    pool.read_word(leaf.node, slot)
                };
                leaf.slot += 1;
                cursor.set_path(0, leaf.node, leaf.slot as usize);
                if key == NONE {
                    continue;
                }
                if !visit(ExtentId::new(key)) {
                    return;
                }
            }
            if !cursor.next_leaf(self, pool) {
                break;
            }
        }
    }

    fn root_leaf_parts(&self, keys: &mut KeyScratch) -> usize {
        debug_assert!(self.height == 0);
        let mut count = 0;
        for &key in &self.root_words {
            if key != NONE {
                keys[count] = key;
                count += 1;
            }
        }
        debug_assert!(keys[..count].windows(2).all(|pair| pair[0] < pair[1]));
        count
    }

    fn root_branch_parts(&self) -> (BranchKeys, BranchChildren, usize) {
        debug_assert!(self.height > 0);
        branch_parts(&self.root_words)
    }

    fn root_has_branch_hole(&self) -> bool {
        debug_assert!(self.height > 0);
        branch_has_hole_words(&self.root_words)
    }

    fn write_root_branch(&mut self, height: u8, keys: &[u32], children: &[u32]) {
        debug_assert!(height > 0);
        debug_assert_eq!(keys.len(), BRANCH_SLOTS);
        debug_assert_eq!(children.len(), BRANCH_SLOTS);
        debug_assert!(children.iter().any(|&child| child != NONE));
        self.height = height;
        self._reserved = [0; 8];
        self.root_words = [NONE; NODE_SLOTS];
        write_branch(&mut self.root_words, keys, children);
    }

    fn reset_root(&mut self) {
        self.height = 0;
        self._reserved = [0; 8];
        self.root_words = [NONE; NODE_SLOTS];
    }

    fn collapse_root(&mut self, pool: &mut ExtentTreePool) {
        while self.height > 0 {
            let Some(child_at) = first_branch_slot_words(&self.root_words) else {
                self.reset_root();
                return;
            };
            if next_branch_slot_words(&self.root_words, child_at + 1).is_some() {
                return;
            }
            let child = self.root_words[BRANCH_SLOTS + child_at];
            let node = pool.read(child);
            if self.height == 1 {
                self.height = 0;
                self._reserved = [0; 8];
                self.root_words = node.words;
            } else {
                self.height -= 1;
                self._reserved = [0; 8];
                self.root_words = node.words;
            }
            pool.retire(child);
        }
    }
}

fn insert_subtree(
    pool: &mut ExtentTreePool,
    height: u8,
    path_level: usize,
    key: u32,
    cursor: &Cursor,
    leaf_hole: Option<usize>,
    reserved: &mut Vec<u32>,
) -> Option<(u32, u32)> {
    if height == 0 {
        let leaf = cursor.leaf();
        debug_assert!(!leaf.node.is_null());
        if let Some(hole) = leaf_hole {
            insert_sparse_leaf(pool.words_mut_ptr(leaf.node), key, leaf.slot as usize, hole);
            return None;
        }

        // With no holes, every physical slot is occupied and the leaf is
        // already dense. Materialize keys only for the split itself.
        let words = pool.words_ptr(leaf.node);
        let mut keys = [0; KEY_SCRATCH];
        keys[..NODE_SLOTS].copy_from_slice(words);
        let count = NODE_SLOTS;
        insert_key(&mut keys, count, leaf.slot as usize, key);
        let split = choose_leaf_split(&keys[..count + 1], NODE_LEAF_CAPACITY);
        let right_index = reserved.pop().expect("leaf split was pre-reserved");
        pool.write(right_index, Node::leaf(&keys[split..count + 1]));
        pool.write_ptr(leaf.node, Node::leaf(&keys[..split]));
        return Some((keys[split], right_index));
    }

    let path_index = cursor.height as usize - path_level;
    let path = cursor.path(path_index);
    debug_assert!(!path.node.is_null());
    let words = pool.words_ptr(path.node);
    let child_at = path.slot as usize;
    let first_child = first_branch_slot_words(words).expect("nonempty branch");
    let min_changed = child_at == first_child && key < words[child_at];
    if let Some((separator, right)) =
        insert_subtree(pool, height - 1, path_level + 1, key, cursor, leaf_hole, reserved)
    {
        if min_changed {
            pool.words_mut_ptr(path.node)[child_at] = key;
        }
        if insert_branch_entry_words(pool.words_mut_ptr(path.node), child_at, separator, right) {
            return None;
        }

        let (dense_keys, dense_children, dense_count) =
            dense_insert_branch_words(pool.words_ptr(path.node), child_at, separator, right);
        let split_at = dense_count / 2;
        let right_index = reserved.pop().expect("branch split was pre-reserved");
        let promoted = dense_keys[split_at];
        let (left_keys, left_children) =
            spread_split_branch(&dense_keys[..split_at], &dense_children[..split_at]);
        let (right_keys, right_children) = spread_split_branch(
            &dense_keys[split_at..dense_count],
            &dense_children[split_at..dense_count],
        );
        pool.write(
            right_index,
            Node::branch(&right_keys[..BRANCH_SLOTS], &right_children[..BRANCH_SLOTS]),
        );
        write_branch(
            pool.words_mut_ptr(path.node),
            &left_keys[..BRANCH_SLOTS],
            &left_children[..BRANCH_SLOTS],
        );
        return Some((promoted, right_index));
    }
    if min_changed {
        pool.words_mut_ptr(path.node)[child_at] = key;
    }
    None
}

/// Delete while searching in the same recursive descent. Standalone removes
/// use this because they do not already have a cursor to reuse.
fn delete_subtree_direct(
    pool: &mut ExtentTreePool,
    index: u32,
    height: u8,
    key: u32,
    current_min: u32,
) -> DeleteResult {
    let node_ptr = pool.ptr(index);
    if height == 0 {
        let words = pool.words_ptr(node_ptr);
        let Some((first, last)) = leaf_bounds(words) else {
            return DeleteResult::NotFound;
        };
        let (found, _) = sparse_leaf_find(words, key, first, last);
        let Some(slot) = found else {
            return DeleteResult::NotFound;
        };
        return remove_leaf_word(pool.words_mut_ptr(node_ptr), slot, key, Some(current_min));
    }

    let words = pool.words_ptr(node_ptr);
    let child_at = branch_child(&words[..BRANCH_SLOTS], key);
    let child_index = words[BRANCH_SLOTS + child_at];
    let child_min = words[child_at];
    let (child_minimum, merge_candidate) =
        match delete_subtree_direct(pool, child_index, height - 1, key, child_min) {
            DeleteResult::NotFound => return DeleteResult::NotFound,
            DeleteResult::Removed { minimum, merge_candidate } => (minimum, merge_candidate),
        };
    finish_branch_delete(
        pool,
        node_ptr,
        child_index,
        height,
        child_at,
        child_minimum,
        merge_candidate,
    )
}

fn delete_subtree(
    pool: &mut ExtentTreePool,
    height: u8,
    path_level: usize,
    key: u32,
    cursor: &Cursor,
    current_min: u32,
) -> DeleteResult {
    if height == 0 {
        let leaf = cursor.leaf();
        debug_assert!(!leaf.node.is_null());
        debug_assert!(cursor.found);
        let slot = leaf.slot as usize;
        debug_assert!(slot < NODE_SLOTS);
        return remove_leaf_word(pool.words_mut_ptr(leaf.node), slot, key, Some(current_min));
    }

    let path_index = cursor.height as usize - path_level;
    let path = cursor.path(path_index);
    debug_assert!(!path.node.is_null());
    let words = pool.words_ptr(path.node);
    let child_at = path.slot as usize;
    let child_index = pool.index_of(cursor.path(path_index - 1).node);
    let child_min = words[child_at];
    let (child_minimum, merge_candidate) =
        match delete_subtree(pool, height - 1, path_level + 1, key, cursor, child_min) {
            DeleteResult::NotFound => return DeleteResult::NotFound,
            DeleteResult::Removed { minimum, merge_candidate } => (minimum, merge_candidate),
        };
    finish_branch_delete(
        pool,
        path.node,
        child_index,
        height,
        child_at,
        child_minimum,
        merge_candidate,
    )
}

fn finish_branch_delete(
    pool: &mut ExtentTreePool,
    node_ptr: *mut Node,
    child_index: u32,
    height: u8,
    child_at: usize,
    child_minimum: u32,
    merge_candidate: bool,
) -> DeleteResult {
    if child_minimum == NONE {
        let (minimum, merge_candidate) = {
            let words = pool.words_mut_ptr(node_ptr);
            remove_branch_child_words(words, child_at);
            match first_branch_slot_words(words) {
                Some(first_child) => {
                    (words[first_child], branch_slot_has_empty_neighbors_words(words, child_at))
                }
                None => (NONE, false),
            }
        };
        pool.retire(child_index);
        return DeleteResult::Removed { minimum, merge_candidate };
    }

    pool.words_mut_ptr(node_ptr)[child_at] = child_minimum;
    let branch_merge_candidate = if merge_candidate {
        let (mut keys, mut children) = branch_values(pool.words_ptr(node_ptr));
        let removed_slot = if height == 1 {
            merge_leaf_children(pool, &mut keys, &mut children, child_at)
        } else {
            merge_branch_children(pool, &mut keys, &mut children, child_at)
        };
        if let Some(slot) = removed_slot {
            let branch_merge_candidate = branch_slot_has_empty_neighbors(&children, slot);
            write_branch(
                pool.words_mut_ptr(node_ptr),
                &keys[..BRANCH_SLOTS],
                &children[..BRANCH_SLOTS],
            );
            branch_merge_candidate
        } else {
            false
        }
    } else {
        false
    };

    let words = pool.words_ptr(node_ptr);
    let first_child = first_branch_slot_words(words).expect("nonempty branch after deletion");
    DeleteResult::Removed { minimum: words[first_child], merge_candidate: branch_merge_candidate }
}

/// Merge the selected leaf with an adjacent leaf when the combined IDs fit
/// after recomputing their common prefix. The right node is retired on success.
fn merge_leaf_children(
    pool: &mut ExtentTreePool,
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    child_at: usize,
) -> Option<usize> {
    let leaf_count = pool.read(children[child_at]).words.iter().filter(|&&key| key != NONE).count();
    if leaf_count > NODE_LEAF_CAPACITY / 2 {
        return None;
    }

    if let Some(left_at) = previous_branch_slot(children, child_at) {
        let left = children[left_at];
        let right = children[child_at];
        if merge_leaf_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, child_at);
            return Some(child_at);
        }
    }
    if let Some(right_at) = next_branch_slot(children, child_at + 1) {
        let left = children[child_at];
        let right = children[right_at];
        if merge_leaf_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, right_at);
            return Some(right_at);
        }
    }
    None
}

/// Merge the selected branch with an adjacent branch when their child entries
/// fit in one node. The right node is retired on success.
fn merge_branch_children(
    pool: &mut ExtentTreePool,
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    child_at: usize,
) -> Option<usize> {
    let branch_count = pool.read(children[child_at]).branch_parts().2;
    if branch_count > BRANCH_CAPACITY / 2 {
        return None;
    }

    if let Some(left_at) = previous_branch_slot(children, child_at) {
        let left = children[left_at];
        let right = children[child_at];
        if merge_branch_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, child_at);
            return Some(child_at);
        }
    }
    if let Some(right_at) = next_branch_slot(children, child_at + 1) {
        let left = children[child_at];
        let right = children[right_at];
        if merge_branch_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, right_at);
            return Some(right_at);
        }
    }
    None
}

fn merge_leaf_nodes(pool: &mut ExtentTreePool, left_index: u32, right_index: u32) -> bool {
    let left = pool.read(left_index);
    let right = pool.read(right_index);
    let mut left_keys = [0; KEY_SCRATCH];
    let mut right_keys = [0; KEY_SCRATCH];
    let left_count = left.leaf_parts(&mut left_keys);
    let right_count = right.leaf_parts(&mut right_keys);
    debug_assert!(left_count > 0 && right_count > 0);
    debug_assert!(left_keys[left_count - 1] < right_keys[0]);

    let count = left_count + right_count;
    let mut merged = [0; MERGE_KEY_SCRATCH];
    merged[..left_count].copy_from_slice(&left_keys[..left_count]);
    merged[left_count..count].copy_from_slice(&right_keys[..right_count]);
    if !can_store_leaf(&merged[..count], NODE_LEAF_CAPACITY) {
        return false;
    }
    pool.write(left_index, Node::leaf(&merged[..count]));
    true
}

fn merge_branch_nodes(pool: &mut ExtentTreePool, left_index: u32, right_index: u32) -> bool {
    let left = pool.read(left_index);
    let right = pool.read(right_index);
    let (left_keys, left_children, left_count) = left.branch_parts();
    let (right_keys, right_children, right_count) = right.branch_parts();
    debug_assert!(left_count > 0 && right_count > 0);
    if left_count == 0 || right_count == 0 {
        return false;
    }
    let count = left_count + right_count;
    if count > BRANCH_CAPACITY {
        return false;
    }

    let mut dense_keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut dense_children = [NONE; BRANCH_CHILDREN_SCRATCH];
    let mut out = 0;
    for (keys, children) in [(&left_keys, &left_children), (&right_keys, &right_children)] {
        for slot in 0..BRANCH_SLOTS {
            if children[slot] != NONE {
                dense_keys[out] = keys[slot];
                dense_children[out] = children[slot];
                out += 1;
            }
        }
    }
    debug_assert_eq!(out, count);
    debug_assert!(dense_keys[..count].windows(2).all(|pair| pair[0] < pair[1]));

    let mut merged_keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut merged_children = [NONE; BRANCH_CHILDREN_SCRATCH];
    for i in 0..count {
        let slot = i * BRANCH_SLOTS / count;
        merged_keys[slot] = dense_keys[i];
        merged_children[slot] = dense_children[i];
    }
    pool.write(
        left_index,
        Node::branch(&merged_keys[..BRANCH_SLOTS], &merged_children[..BRANCH_SLOTS]),
    );
    true
}

fn branch_slot_has_empty_neighbors(children: &BranchChildren, slot: usize) -> bool {
    slot > 0
        && slot + 1 < BRANCH_SLOTS
        && children[slot] == NONE
        && children[slot - 1] == NONE
        && children[slot + 1] == NONE
}

fn branch_values(words: &[u32; NODE_SLOTS]) -> (BranchKeys, BranchChildren) {
    let mut keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut children = [NONE; BRANCH_CHILDREN_SCRATCH];
    keys[..BRANCH_SLOTS].copy_from_slice(&words[..BRANCH_SLOTS]);
    children[..BRANCH_SLOTS].copy_from_slice(&words[BRANCH_SLOTS..]);
    debug_assert!(
        keys[..BRANCH_SLOTS]
            .iter()
            .zip(&children[..BRANCH_SLOTS])
            .all(|(key, child)| (*key == NONE) == (*child == NONE))
    );
    (keys, children)
}

fn branch_count(children: &BranchChildren) -> usize {
    children[..BRANCH_SLOTS].iter().filter(|&&child| child != NONE).count()
}

fn branch_has_hole_words(words: &[u32; NODE_SLOTS]) -> bool {
    words[BRANCH_SLOTS..].contains(&NONE)
}

fn first_branch_slot_words(words: &[u32; NODE_SLOTS]) -> Option<usize> {
    (0..BRANCH_SLOTS).find(|&slot| words[BRANCH_SLOTS + slot] != NONE)
}

fn next_branch_slot_words(words: &[u32; NODE_SLOTS], from: usize) -> Option<usize> {
    (from..BRANCH_SLOTS).find(|&slot| words[BRANCH_SLOTS + slot] != NONE)
}

fn remove_branch_child_words(words: &mut [u32; NODE_SLOTS], child_at: usize) {
    debug_assert!(child_at < BRANCH_SLOTS);
    debug_assert_ne!(words[BRANCH_SLOTS + child_at], NONE);
    words[child_at] = NONE;
    words[BRANCH_SLOTS + child_at] = NONE;
}

fn branch_slot_has_empty_neighbors_words(words: &[u32; NODE_SLOTS], slot: usize) -> bool {
    slot > 0
        && slot + 1 < BRANCH_SLOTS
        && words[BRANCH_SLOTS + slot] == NONE
        && words[BRANCH_SLOTS + slot - 1] == NONE
        && words[BRANCH_SLOTS + slot + 1] == NONE
}

/// Insert a child immediately after `child_at`, editing the branch in place.
/// Returning false means the branch has no hole and the caller must split it.
fn insert_branch_entry_words(
    words: &mut [u32; NODE_SLOTS],
    child_at: usize,
    separator: u32,
    right: u32,
) -> bool {
    let children = &words[BRANCH_SLOTS..];
    debug_assert!(children[child_at] != NONE);
    debug_assert!(separator > words[child_at]);

    let next = next_branch_slot_words(words, child_at + 1).unwrap_or(BRANCH_SLOTS);
    let gap_start = child_at + 1;
    if gap_start < next {
        let at = gap_start + (next - gap_start) / 2;
        words[at] = separator;
        words[BRANCH_SLOTS + at] = right;
        return true;
    }

    let hole = if child_at & 1 == 1 {
        (0..child_at).rev().find(|&slot| words[BRANCH_SLOTS + slot] == NONE).or_else(|| {
            (child_at + 1..BRANCH_SLOTS).find(|&slot| words[BRANCH_SLOTS + slot] == NONE)
        })
    } else {
        (child_at + 1..BRANCH_SLOTS)
            .find(|&slot| words[BRANCH_SLOTS + slot] == NONE)
            .or_else(|| (0..child_at).rev().find(|&slot| words[BRANCH_SLOTS + slot] == NONE))
    };
    let Some(hole) = hole else {
        return false;
    };

    if hole < child_at {
        for src in hole + 1..=child_at {
            words[src - 1] = words[src];
            words[BRANCH_SLOTS + src - 1] = words[BRANCH_SLOTS + src];
        }
        words[child_at] = separator;
        words[BRANCH_SLOTS + child_at] = right;
    } else {
        for src in (child_at + 1..hole).rev() {
            words[src + 1] = words[src];
            words[BRANCH_SLOTS + src + 1] = words[BRANCH_SLOTS + src];
        }
        words[child_at + 1] = separator;
        words[BRANCH_SLOTS + child_at + 1] = right;
    }
    true
}

fn dense_insert_branch_words(
    words: &[u32; NODE_SLOTS],
    after_slot: usize,
    separator: u32,
    right: u32,
) -> (BranchKeys, BranchChildren, usize) {
    let mut dense_keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut dense_children = [NONE; BRANCH_CHILDREN_SCRATCH];
    let mut out = 0;
    for slot in 0..BRANCH_SLOTS {
        let child = words[BRANCH_SLOTS + slot];
        if child == NONE {
            continue;
        }
        dense_keys[out] = words[slot];
        dense_children[out] = child;
        out += 1;
        if slot == after_slot {
            dense_keys[out] = separator;
            dense_children[out] = right;
            out += 1;
        }
    }
    debug_assert_eq!(out, BRANCH_SLOTS + 1);
    (dense_keys, dense_children, out)
}

fn branch_parts(words: &[u32; NODE_SLOTS]) -> (BranchKeys, BranchChildren, usize) {
    let (keys, children) = branch_values(words);
    let count = branch_count(&children);
    (keys, children, count)
}

fn write_branch(words: &mut [u32; NODE_SLOTS], keys: &[u32], children: &[u32]) {
    debug_assert_eq!(keys.len(), BRANCH_SLOTS);
    debug_assert_eq!(children.len(), BRANCH_SLOTS);
    *words = [NONE; NODE_SLOTS];
    debug_assert!(keys.iter().zip(children).all(|(key, child)| (*key == NONE) == (*child == NONE)));
    words[..BRANCH_SLOTS].copy_from_slice(keys);
    words[BRANCH_SLOTS..].copy_from_slice(children);
}

fn root_branch_parts(tree: &EnExtentTree) -> (BranchKeys, BranchChildren, usize) {
    debug_assert!(tree.height > 0);
    branch_parts(&tree.root_words)
}

fn branch_child(keys: &[u32], key: u32) -> usize {
    debug_assert_eq!(keys.len(), BRANCH_SLOTS);
    let first = keys.iter().position(|&separator| separator != NONE).expect("nonempty branch");
    let mut low = first;
    let mut high = BRANCH_SLOTS;
    let mut previous = None;

    while low < high {
        let middle = low + (high - low) / 2;
        let at = if keys[middle] != NONE {
            middle
        } else if let Some(next) = (middle + 1..high).find(|&slot| keys[slot] != NONE) {
            next
        } else {
            let Some(previous) = (low..middle).rev().find(|&slot| keys[slot] != NONE) else {
                break;
            };
            previous
        };

        if key < keys[at] {
            high = at;
        } else {
            previous = Some(at);
            low = at + 1;
        }
    }

    previous.unwrap_or(first)
}

fn spread_split_branch(keys: &[u32], children: &[u32]) -> (BranchKeys, BranchChildren) {
    debug_assert_eq!(keys.len(), children.len());
    debug_assert!(keys.len() == BRANCH_SLOTS / 2 || keys.len() == BRANCH_SLOTS / 2 + 1);
    let mut sparse_keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut sparse_children = [NONE; BRANCH_CHILDREN_SCRATCH];
    let evenly_spaced_count = BRANCH_SLOTS / 2;
    for i in 0..keys.len() {
        let slot = if i < evenly_spaced_count { 2 * i } else { BRANCH_SLOTS - (keys.len() - i) };
        sparse_keys[slot] = keys[i];
        sparse_children[slot] = children[i];
    }
    (sparse_keys, sparse_children)
}

fn centered_two_branch_pairs(
    left_min: u32,
    left: u32,
    right_min: u32,
    right: u32,
) -> (BranchKeys, BranchChildren) {
    let mut keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut children = [NONE; BRANCH_CHILDREN_SCRATCH];
    let first = BRANCH_SLOTS / 2 - 1;
    keys[first] = left_min;
    children[first] = left;
    keys[first + 1] = right_min;
    children[first + 1] = right;
    (keys, children)
}

fn remove_branch_child(keys: &mut BranchKeys, children: &mut BranchChildren, child_at: usize) {
    debug_assert!(child_at < BRANCH_SLOTS);
    debug_assert_ne!(children[child_at], NONE);
    keys[child_at] = NONE;
    children[child_at] = NONE;
}

fn first_branch_slot(children: &BranchChildren) -> Option<usize> {
    (0..BRANCH_SLOTS).find(|&slot| children[slot] != NONE)
}

fn previous_branch_slot(children: &BranchChildren, before: usize) -> Option<usize> {
    (0..before.min(BRANCH_SLOTS)).rev().find(|&slot| children[slot] != NONE)
}

fn next_branch_slot(children: &BranchChildren, from: usize) -> Option<usize> {
    (from..BRANCH_SLOTS).find(|&slot| children[slot] != NONE)
}

fn can_store_leaf(keys: &[u32], capacity: usize) -> bool {
    keys.len() <= capacity
}

fn choose_leaf_split(keys: &[u32], capacity: usize) -> usize {
    debug_assert!(keys.len() >= 2);
    let mut best: Option<(usize, usize, usize)> = None;
    for split in 1..keys.len() {
        let left = &keys[..split];
        let right = &keys[split..];
        if left.len() > capacity || right.len() > capacity {
            continue;
        }
        let score = (left.len().max(right.len()), left.len().abs_diff(right.len()), split);
        if best.is_none_or(|old| score < old) {
            best = Some(score);
        }
    }
    best.map(|(_, _, split)| split).unwrap_or_else(|| {
        debug_assert!(false, "no capacity-respecting split for raw leaf");
        keys.len() / 2
    })
}

fn insert_key(keys: &mut KeyScratch, count: usize, at: usize, key: u32) {
    debug_assert!(count + 1 < KEY_SCRATCH);
    for i in (at..count).rev() {
        keys[i + 1] = keys[i];
    }
    keys[at] = key;
}

fn leaf_bounds(words: &[u32; NODE_SLOTS]) -> Option<(usize, usize)> {
    let mut first = NODE_SLOTS;
    let mut last = 0;
    for (slot, &key) in words.iter().enumerate() {
        if key != NONE {
            if first == NODE_SLOTS {
                first = slot;
            }
            last = slot;
        }
    }
    (first != NODE_SLOTS).then_some((first, last))
}

fn remove_leaf_word(
    words: &mut [u32; NODE_SLOTS],
    slot: usize,
    key: u32,
    current_min: Option<u32>,
) -> DeleteResult {
    if slot >= NODE_SLOTS || words[slot] != key {
        return DeleteResult::NotFound;
    }

    words[slot] = NONE;
    let minimum = match current_min {
        Some(minimum) if key == minimum => {
            words[slot + 1..].iter().copied().find(|&value| value != NONE).unwrap_or(NONE)
        }
        Some(minimum) => minimum,
        None => NONE,
    };
    if minimum == NONE {
        return DeleteResult::Removed { minimum, merge_candidate: false };
    }

    // Derive the adjacent-hole condition at deletion time. It is only needed
    // for the merge heuristic, so it does not belong in the cursor.
    let leaf_prev = slot.checked_sub(1).map(|prev| words[prev]);
    let leaf_next = (slot + 1 < NODE_SLOTS).then(|| words[slot + 1]);
    let merge_candidate = matches!((leaf_prev, leaf_next), (Some(NONE), Some(NONE)));
    DeleteResult::Removed { minimum, merge_candidate }
}

fn nearest_leaf_key(
    words: &[u32; NODE_SLOTS],
    low: usize,
    high: usize,
    middle: usize,
) -> Option<usize> {
    debug_assert!(low <= middle && middle < high);
    for distance in 0..high - low {
        let right = middle + distance;
        if right < high && words[right] != NONE {
            return Some(right);
        }
        if distance != 0 {
            if let Some(left) = middle.checked_sub(distance).filter(|&left| left >= low) {
                if words[left] != NONE {
                    return Some(left);
                }
            }
        }
    }
    None
}

fn sparse_leaf_find(
    words: &[u32; NODE_SLOTS],
    key: u32,
    first: usize,
    last: usize,
) -> (Option<usize>, Option<usize>) {
    let mut low = first;
    let mut high = last + 1;
    let mut next = None;
    while low < high {
        let middle = low + (high - low) / 2;
        let Some(at) = nearest_leaf_key(words, low, high, middle) else {
            break;
        };
        match words[at].cmp(&key) {
            std::cmp::Ordering::Less => low = at + 1,
            std::cmp::Ordering::Equal => return (Some(at), Some(at)),
            std::cmp::Ordering::Greater => {
                next = Some(at);
                high = at;
            }
        }
    }
    (None, next)
}

fn nearest_leaf_hole(words: &[u32; NODE_SLOTS], target: usize) -> Option<usize> {
    debug_assert!(target <= NODE_SLOTS);
    let target = target.min(NODE_SLOTS - 1);
    for distance in 0..NODE_SLOTS {
        let right = target + distance;
        if right < NODE_SLOTS && words[right] == NONE {
            return Some(right);
        }
        if distance != 0 {
            if let Some(left) = target.checked_sub(distance) {
                if words[left] == NONE {
                    return Some(left);
                }
            }
        }
    }
    None
}

fn insert_sparse_leaf(words: &mut [u32; NODE_SLOTS], key: u32, anchor: usize, mut hole: usize) {
    debug_assert_eq!(words[hole], NONE);
    debug_assert!(anchor <= NODE_SLOTS);
    if hole < anchor {
        for from in hole + 1..anchor {
            if words[from] != NONE {
                words[hole] = words[from];
                words[from] = NONE;
                hole = from;
            }
        }
    } else if hole > anchor {
        for from in (anchor..hole).rev() {
            if words[from] != NONE {
                words[hole] = words[from];
                words[from] = NONE;
                hole = from;
            }
        }
    }

    words[hole] = key;
}

#[cfg(test)]
mod tests {
    use super::*;
    use permute::Permute;
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};

    const TUNE_DEFAULT_ENTRIES: u32 = 1 << 13;
    const TUNE_DEFAULT_CHURN_PAIRS: u64 = 1 << 20;
    const TUNE_SEED: u64 = 0x51a7_ee42_9330_600d;
    const TUNE_PERMUTE_RANGE: u64 = 1u64 << 32;

    fn tuning_entries() -> u32 {
        std::env::var("EXTENT_TREE_ENTRIES")
            .map(|value| value.parse().expect("EXTENT_TREE_ENTRIES must be a u32"))
            .unwrap_or(TUNE_DEFAULT_ENTRIES)
    }

    fn tuning_churn_pairs() -> u64 {
        std::env::var("EXTENT_TREE_CHURN_PAIRS")
            .map(|value| value.parse().expect("EXTENT_TREE_CHURN_PAIRS must be a u64"))
            .unwrap_or(TUNE_DEFAULT_CHURN_PAIRS)
    }

    #[derive(Default)]
    struct TreeUsage {
        leaves: usize,
        branches: usize,
        leaf_id_bytes: usize,
        occupied_slots: usize,
    }

    fn usage_node(pool: &ExtentTreePool, index: u32, height: u8, usage: &mut TreeUsage) {
        let node = pool.read(index);
        if height == 0 {
            let mut keys = [0; KEY_SCRATCH];
            let count = node.leaf_parts(&mut keys);
            usage.leaves += 1;
            usage.leaf_id_bytes += count * size_of::<u32>();
            usage.occupied_slots += count;
            return;
        }
        let (_, children, count) = node.branch_parts();
        usage.branches += 1;
        usage.occupied_slots += count * 2;
        for &child in &children[..BRANCH_SLOTS] {
            if child != NONE {
                usage_node(pool, child, height - 1, usage);
            }
        }
    }

    fn tree_usage(tree: &EnExtentTree, pool: &ExtentTreePool) -> TreeUsage {
        let mut usage = TreeUsage::default();
        if tree.height == 0 {
            let mut keys = [0; KEY_SCRATCH];
            let count = tree.root_leaf_parts(&mut keys);
            if count != 0 {
                usage.leaves = 1;
                usage.leaf_id_bytes = count * size_of::<u32>();
                usage.occupied_slots = count;
            }
        } else {
            let (_, children, count) = tree.root_branch_parts();
            usage.occupied_slots += count * 2;
            for &child in &children[..BRANCH_SLOTS] {
                if child != NONE {
                    usage_node(pool, child, tree.height - 1, &mut usage);
                }
            }
        }
        usage
    }

    fn next_permuted_id(permute: &mut Permute) -> u32 {
        loop {
            let value = permute.value() as u32;
            permute.next_value();
            if value != NONE {
                return value;
            }
        }
    }

    fn report_timing(
        phase: &str,
        tree: &EnExtentTree,
        pool: &ExtentTreePool,
        elapsed: Duration,
        operations: u64,
        max_nodes: u32,
    ) {
        let usage = tree_usage(tree, pool);
        let live_nodes = if tree.height == 0 {
            0 // The leaf root is inline in EnExtentTree, not a pooled node.
        } else {
            usage.leaves + usage.branches
        };
        let live_node_bytes = live_nodes * NODE_BYTES;
        let issued_bytes = pool.slots.issued_blocks() as usize * NODE_BYTES;
        let reserved_bytes = max_nodes as usize * NODE_BYTES;
        let seconds = elapsed.as_secs_f64();
        println!(
            "extent_tree_tune phase={phase} elapsed_s={seconds:.3} operations={operations} operations_per_s={:.0} entries={} height={} leaves={} branches={} live_node_bytes={} tree_struct_bytes={} leaf_id_bytes={} occupied_node_slots={} pool_issued_nodes={} pool_issued_bytes={} pool_reserved_bytes={}",
            if seconds == 0.0 { 0.0 } else { operations as f64 / seconds },
            tree.len,
            tree.height,
            usage.leaves,
            usage.branches,
            live_node_bytes,
            size_of::<EnExtentTree>(),
            usage.leaf_id_bytes,
            usage.occupied_slots,
            pool.slots.issued_blocks(),
            issued_bytes,
            reserved_bytes,
        );
    }

    /// Manual release-mode timing harness for the configured fill level. This is
    /// intentionally ignored in the ordinary correctness-test run.
    #[test]
    #[ignore = "manual timing harness; run in release mode with --ignored --nocapture"]
    fn tune_fill_churn_delete() {
        let entries = tuning_entries();
        let churn_pairs = tuning_churn_pairs().min(u32::MAX as u64 - entries as u64);
        // Allow two nodes per NODE_SLOTS entries for branch-node overhead.
        let max_nodes = ((entries as u64) * 4).div_ceil(NODE_SLOTS as u64).max(1) as u32;
        let mut pool = ExtentTreePool::new(max_nodes).unwrap();
        let mut tree = EnExtentTree::new();

        // Both permutations are initialized before any timed phase. They use
        // the same cycle: one yields new IDs, the other yields active IDs to
        // remove. After churn, its remaining suffix names every live ID once.
        let mut old = Permute::new(TUNE_PERMUTE_RANGE, TUNE_SEED);
        let mut new = Permute::new(TUNE_PERMUTE_RANGE, TUNE_SEED);
        println!(
            "extent_tree_tune entries={} churn_pairs={} permute_range={} reserved_id_skipped=true node_slots={} node_bytes={}",
            entries, churn_pairs, TUNE_PERMUTE_RANGE, NODE_SLOTS, NODE_BYTES,
        );

        let start = Instant::now();
        for _ in 0..entries {
            let value = next_permuted_id(&mut old);
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        let elapsed = start.elapsed();
        assert_eq!(tree.len(), entries);
        report_timing("fill", &tree, &pool, elapsed, entries as u64, max_nodes);

        let start = Instant::now();
        for pair in 0..churn_pairs {
            if entries == 0 {
                let inserted = next_permuted_id(&mut old);
                assert!(tree.insert(&mut pool, ExtentId::new(inserted)).unwrap());
                let removed = next_permuted_id(&mut new);
                assert!(tree.remove(&mut pool, ExtentId::new(removed)).unwrap());
            } else {
                let removed = next_permuted_id(&mut new);
                assert!(tree.remove(&mut pool, ExtentId::new(removed)).unwrap());
                let inserted = next_permuted_id(&mut old);
                assert!(tree.insert(&mut pool, ExtentId::new(inserted)).unwrap());
            }

            if pair % 1024 == 1023 {
                pool.reclaim();
            }
        }
        pool.reclaim();
        let elapsed = start.elapsed();
        report_timing("churn", &tree, &pool, elapsed, churn_pairs * 2, max_nodes);
        println!(
            "extent_tree_tune churn_pairs={} pairs_per_s={:.0}",
            churn_pairs,
            churn_pairs as f64 / elapsed.as_secs_f64()
        );

        let start = Instant::now();
        for operation in 0..entries {
            let value = next_permuted_id(&mut new);
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
            if operation % 1024 == 1023 {
                pool.reclaim();
            }
        }
        pool.reclaim();
        let elapsed = start.elapsed();
        assert!(tree.is_empty());
        report_timing("delete_all", &tree, &pool, elapsed, entries as u64, max_nodes);
    }

    fn collect(tree: &EnExtentTree, pool: &ExtentTreePool) -> Vec<u32> {
        let mut keys = Vec::with_capacity(tree.len() as usize);
        tree.for_each(pool, |id| {
            keys.push(id.get());
            true
        });
        keys
    }

    fn permutation(n: u32, seed: u64) -> Vec<u32> {
        let mut values: Vec<u32> = (0..n).collect();
        let mut state = seed;
        for i in (1..values.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.swap(i, (state as usize) % (i + 1));
        }
        values
    }

    fn pseudorandom_ids(n: u32) -> Vec<u32> {
        (0..n)
            .map(|value| value.wrapping_mul(0x9e37_79b1).rotate_left(11))
            .filter(|&value| value != NONE)
            .collect()
    }

    #[test]
    fn inline_root_holds_root_capacity_raw_ids_then_rebalances() {
        let mut pool = ExtentTreePool::new(256).unwrap();
        let initial_nodes = pool.slots.issued_blocks();
        let mut tree = EnExtentTree::new();
        for value in 0..ROOT_LEAF_CAPACITY as u32 {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(pool.slots.issued_blocks(), initial_nodes);
        assert_eq!(collect(&tree, &pool), (0..ROOT_LEAF_CAPACITY as u32).collect::<Vec<_>>());

        assert!(tree.insert(&mut pool, ExtentId::new(u32::MAX - 1)).unwrap());
        assert_eq!(tree.height, 1);
        assert_eq!(pool.slots.issued_blocks(), initial_nodes + 1);
        let mut expected: Vec<u32> = (0..ROOT_LEAF_CAPACITY as u32).collect();
        expected.push(u32::MAX - 1);
        assert_eq!(collect(&tree, &pool), expected);

        assert!(tree.remove(&mut pool, ExtentId::new(u32::MAX - 1)).unwrap());
        assert_eq!(tree.height, 1);
        assert_eq!(collect(&tree, &pool), (0..ROOT_LEAF_CAPACITY as u32).collect::<Vec<_>>());
        for value in ROOT_LEAF_CAPACITY as u32 / 2..ROOT_LEAF_CAPACITY as u32 {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(collect(&tree, &pool), (0..ROOT_LEAF_CAPACITY as u32 / 2).collect::<Vec<_>>());
        for value in 1..ROOT_LEAF_CAPACITY as u32 / 2 {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(collect(&tree, &pool), vec![0]);
        assert!(tree.remove(&mut pool, ExtentId::new(0)).unwrap());
        assert!(tree.is_empty());
        pool.reclaim();
    }

    #[test]
    fn leaf_merge_obeys_raw_slot_capacity() {
        let mut pool = ExtentTreePool::new(64).unwrap();
        let mut tree = EnExtentTree::new();
        let mut values: Vec<u32> =
            (0..NODE_LEAF_CAPACITY + 2).map(|value| value as u32 * 0x0100_0000).collect();
        values.push((NODE_LEAF_CAPACITY as u32 + 2) * 0x0100_0000);
        for &value in values.iter().rev() {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 1);

        let trigger_slot = NODE_LEAF_CAPACITY / 2;
        // A merge triggers only after the slots neighboring the deleted slot are empty.
        assert!(tree.remove(&mut pool, ExtentId::new(values[trigger_slot - 1])).unwrap());
        assert_eq!(tree.height, 1);
        assert!(tree.remove(&mut pool, ExtentId::new(values[trigger_slot + 1])).unwrap());
        assert_eq!(tree.height, 1);

        assert!(tree.remove(&mut pool, ExtentId::new(values[trigger_slot])).unwrap());
        assert_eq!(tree.height, 0);
        let mut root_keys = [0; KEY_SCRATCH];
        assert_eq!(tree.root_leaf_parts(&mut root_keys), NODE_LEAF_CAPACITY);
        let expected: Vec<u32> = values
            .iter()
            .copied()
            .filter(|&value| {
                value != values[trigger_slot - 1]
                    && value != values[trigger_slot]
                    && value != values[trigger_slot + 1]
            })
            .collect();
        assert_eq!(collect(&tree, &pool), expected);
    }

    #[test]
    fn pooled_leaf_empty_marker_is_reached_by_descending_deletes() {
        let mut pool = ExtentTreePool::new(64).unwrap();
        let mut tree = EnExtentTree::new();
        for value in 0..ROOT_LEAF_CAPACITY as u32 + 1 {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 1);

        let mut cursor = Cursor::new(ROOT_LEAF_CAPACITY as u32);
        tree.find(&pool, ROOT_LEAF_CAPACITY as u32, &mut cursor);
        let right_leaf = pool.index_of(cursor.leaf().node);
        let mut leaf_keys = [0; KEY_SCRATCH];
        let count = pool.read(right_leaf).leaf_parts(&mut leaf_keys);
        assert!(count > 0);
        assert_eq!(leaf_keys[count - 1], ROOT_LEAF_CAPACITY as u32);

        // Removing the leaf from highest to lowest leaves its minimum until
        // the last removal, when the all-NONE slots mark the leaf empty.
        for &value in leaf_keys[..count].iter().rev() {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(collect(&tree, &pool), (0..ROOT_LEAF_CAPACITY as u32 / 2).collect::<Vec<_>>());
        assert_eq!(tree.height, 0);
        pool.reclaim();
    }

    fn tree_until_height_two(pool: &mut ExtentTreePool) -> EnExtentTree {
        let mut tree = EnExtentTree::new();
        let max_entries = NODE_SLOTS * BRANCH_CAPACITY * 8;
        for value in 0..max_entries as u32 {
            assert!(tree.insert(pool, ExtentId::new(value)).unwrap());
            if tree.height == 2 {
                return tree;
            }
        }
        panic!("inserting ordered IDs did not grow the tree to height two");
    }

    fn delete_leaf_highest_to_lowest(
        tree: &mut EnExtentTree,
        pool: &mut ExtentTreePool,
        leaf: u32,
    ) -> Vec<u32> {
        let mut keys = [0; KEY_SCRATCH];
        let count = pool.read(leaf).leaf_parts(&mut keys);
        let removed = keys[..count].to_vec();
        for &key in removed.iter().rev() {
            assert!(tree.remove(pool, ExtentId::new(key)).unwrap());
        }
        removed
    }

    #[test]
    fn branch_merge_uses_empty_neighbor_rule_and_reduces_root_height() {
        let mut pool = ExtentTreePool::new(4096).unwrap();
        let mut tree = tree_until_height_two(&mut pool);
        let before = collect(&tree, &pool);

        let (_, root_children, root_count) = tree.root_branch_parts();
        assert_eq!(root_count, 2);
        let left_branch = root_children[first_branch_slot(&root_children).unwrap()];
        let (_, children, child_count) = pool.read(left_branch).branch_parts();
        assert_eq!(child_count, BRANCH_SLOTS / 2);
        let trigger_slot = BRANCH_SLOTS / 2;
        assert_eq!(children[trigger_slot - 1], NONE);
        assert_ne!(children[trigger_slot], NONE);
        assert_eq!(children[trigger_slot + 1], NONE);

        let removed = delete_leaf_highest_to_lowest(&mut tree, &mut pool, children[trigger_slot]);
        assert_eq!(tree.height, 1);
        let removed: BTreeSet<u32> = removed.into_iter().collect();
        let expected: Vec<u32> = before.into_iter().filter(|key| !removed.contains(key)).collect();
        assert_eq!(collect(&tree, &pool), expected);
        pool.reclaim();
    }

    #[test]
    fn branch_merge_does_not_trigger_at_an_edge_slot() {
        let mut pool = ExtentTreePool::new(4096).unwrap();
        let mut tree = tree_until_height_two(&mut pool);
        let before = collect(&tree, &pool);

        let (_, root_children, root_count) = tree.root_branch_parts();
        assert_eq!(root_count, 2);
        let left_branch = root_children[first_branch_slot(&root_children).unwrap()];
        let (_, children, child_count) = pool.read(left_branch).branch_parts();
        assert_eq!(child_count, BRANCH_SLOTS / 2);
        assert_ne!(children[0], NONE);
        assert_eq!(children[1], NONE);

        let removed = delete_leaf_highest_to_lowest(&mut tree, &mut pool, children[0]);
        assert_eq!(tree.height, 2);
        let removed: BTreeSet<u32> = removed.into_iter().collect();
        let expected: Vec<u32> = before.into_iter().filter(|key| !removed.contains(key)).collect();
        assert_eq!(collect(&tree, &pool), expected);
        pool.reclaim();
    }

    #[test]
    fn inserts_and_deletes_preserve_order_for_scrambled_and_wide_keys() {
        for values in [permutation(4000, 0x1234), pseudorandom_ids(2500)] {
            let mut pool = ExtentTreePool::new(4096).unwrap();
            let mut tree = EnExtentTree::new();
            let mut expected = BTreeSet::new();
            for &value in &values {
                assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
                expected.insert(value);
            }
            assert!(tree.height >= 2);
            assert!(!tree.insert(&mut pool, ExtentId::new(values[values.len() / 2])).unwrap());
            assert_eq!(collect(&tree, &pool), expected.iter().copied().collect::<Vec<_>>());
            for &value in values.iter().step_by(71) {
                assert!(tree.contains(&pool, ExtentId::new(value)));
            }

            let mut remove_order = values.clone();
            remove_order.reverse();
            for (i, value) in remove_order.into_iter().enumerate() {
                assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
                expected.remove(&value);
                assert_eq!(tree.len() as usize, expected.len());
                if i % 113 == 0 || expected.is_empty() {
                    assert_eq!(collect(&tree, &pool), expected.iter().copied().collect::<Vec<_>>());
                }
            }
            assert!(tree.is_empty());
            assert!(!tree.remove(&mut pool, ExtentId::new(values[0])).unwrap());
            pool.reclaim();
        }
    }

    #[test]
    fn rejects_reserved_extent_id() {
        let mut pool = ExtentTreePool::new(8).unwrap();
        let mut tree = EnExtentTree::new();
        assert!(tree.insert(&mut pool, ExtentId::NONE).is_err());
        assert!(!tree.contains(&pool, ExtentId::NONE));
        assert!(!tree.remove(&mut pool, ExtentId::NONE).unwrap());
    }
}
