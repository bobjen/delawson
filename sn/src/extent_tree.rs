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
// Branch nodes hold sorted child minima and links in matching sparse slots.
// Stored key-first: the separator keys occupy the first half of the node and
// child indexes the second, so a search probes the keys contiguously.
const BRANCH_SLOTS: usize = NODE_SLOTS / 2;
const BRANCH_CAPACITY: usize = BRANCH_SLOTS;
const BRANCH_KEYS_SCRATCH: usize = BRANCH_SLOTS + 1;
const BRANCH_CHILDREN_SCRATCH: usize = BRANCH_SLOTS + 1;
// Height is stored as u8. The cursor path includes both the leaf at level 0
// and the root branch at level `height`, so it needs 256 entries.
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
type LeafWords = [u32; NODE_LEAF_CAPACITY];
type KeyScratch = [u32; KEY_SCRATCH];
type Nodes = LockedPool<Node, NODE_ALIGNMENT, { offset_of!(Node, link) }>;

#[repr(C)]
#[derive(Clone, Copy)]
union Node {
    // LockedPool uses this word while the node is on a free or deleted list.
    link: u32,
    // A live node uses the whole array; every word is written before it is read.
    words: [u32; NODE_SLOTS],
}

const _: [(); NODE_BYTES] = [(); size_of::<Node>()];
const _: () = assert!(align_of::<Node>() <= NODE_ALIGNMENT);

impl Node {
    fn free_slot() -> Self {
        Self { link: NONE }
    }

    fn leaf(words: &LeafWords) -> Self {
        Self { words: *words }
    }

    fn words(&self) -> &[u32; NODE_SLOTS] {
        // SAFETY: live nodes have their words field initialized before use.
        unsafe { &self.words }
    }

    fn words_mut(&mut self) -> &mut [u32; NODE_SLOTS] {
        // SAFETY: callers hold the exclusive lock and the node is live.
        unsafe { &mut self.words }
    }

    fn leaf_parts(&self, keys: &mut KeyScratch) -> usize {
        let mut count = 0;
        for &key in self.words() {
            if key != NONE {
                keys[count] = key;
                count += 1;
            }
        }
        debug_assert!(keys[..count].windows(2).all(|pair| pair[0] < pair[1]));
        count
    }

    fn branch_parts(&self) -> (BranchKeys, BranchChildren, usize) {
        branch_parts(self.words())
    }
}

// SAFETY: a fresh pool slot is initialized with its free-list link. Live node
// words are written in full by insertion before any word is read.
unsafe impl PoolItem for Node {
    unsafe fn init(slot: *mut Self) {
        // SAFETY: LockedPool supplies aligned, fresh storage for Node.
        unsafe { slot.write(Self::free_slot()) };
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
        // remain live while the caller holds the lock protecting the tree;
        // their complete words array was written before they entered the tree.
        unsafe { (*ptr).words() }
    }

    fn words_mut_ptr(&mut self, ptr: *mut Node) -> &mut [u32; NODE_SLOTS] {
        // SAFETY: cursor pointers come from this pool's stable mapping and
        // remain live while the caller holds the tree's exclusive lock; the
        // node was fully initialized before it entered the tree.
        unsafe { (*ptr).words_mut() }
    }

    fn read_word(&self, ptr: *mut Node, slot: usize) -> u32 {
        debug_assert!(slot < NODE_SLOTS);
        // SAFETY: cursor pointers come from this pool's stable mapping and
        // remain live while the caller holds the lock protecting the tree;
        // the selected live node's complete words array is initialized.
        unsafe { (*ptr).words()[slot] }
    }

    fn write(&mut self, index: u32, node: Node) {
        // SAFETY: the tree owns this live node exclusively during mutation.
        unsafe { self.ptr(index).write(node) };
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
        // The insertion path writes every reserved node in full before
        // linking it into the live tree.
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
    root: Node,
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
        if split_carries && !branch_has_hole_words(tree.root.words()) {
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
            let words = if level == height { tree.root.words() } else { pool.words_ptr(path.node) };
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
        Self { height: 0, len: 0, _reserved: [0; 8], root: Node::leaf(&[NONE; NODE_SLOTS]) }
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
            cursor.find_sparse_leaf(std::ptr::null_mut(), self.root.words());
            return;
        }

        let mut slot = branch_child(&self.root.words()[..BRANCH_SLOTS], key);
        cursor.set_path(self.height as usize, std::ptr::null_mut(), slot);
        let mut index = self.root.words()[BRANCH_SLOTS + slot];

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
        self.insert_with_cursor(pool, key, &mut cursor)
    }

    pub(super) fn insert_with_cursor(
        &mut self,
        pool: &mut ExtentTreePool,
        key: u32,
        cursor: &mut Cursor,
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
            if self.height == 0 { self.root.words() } else { pool.words_ptr(leaf.node) };
        let leaf_hole = nearest_hole(leaf_words, leaf.slot as usize);
        let leaf_full = leaf_hole.is_none();
        let reserve_count = cursor.insert_reserve_count(self, pool, leaf_full)?;
        let mut reserved = pool.reserve_nodes(reserve_count)?;

        if self.height == 0 {
            if let Some(hole) = leaf_hole {
                insert_sparse_item(self.root.words_mut(), key, leaf.slot as usize, hole);
                debug_assert!(reserved.is_empty());
                self.len += 1;
                return Ok(true);
            }

            // Move a full inline root leaf into the pool and make it the sole
            // child of a height-one root. The normal subtree split path will
            // retain this node as the left leaf and allocate its right sibling.
            let root_leaf_index = reserved.pop().expect("root leaf promotion was pre-reserved");
            let root_minimum = self.root.words()[0];
            pool.write(root_leaf_index, self.root);

            let root_child_at = BRANCH_SLOTS / 2 - 1;
            self.height = 1;
            self._reserved = [0; 8];
            let root_words = self.root.words_mut();
            root_words.fill(NONE);
            root_words[root_child_at] = root_minimum;
            root_words[BRANCH_SLOTS + root_child_at] = root_leaf_index;

            cursor.height = 1;
            cursor.set_path(0, pool.ptr(root_leaf_index), leaf.slot as usize);
            cursor.set_path(1, std::ptr::null_mut(), root_child_at);
        }

        let root_path = cursor.path(self.height as usize);
        let child_at = root_path.slot as usize;
        let first_child = first_branch_slot_words(self.root.words()).expect("nonempty root branch");
        if child_at == first_child && key < self.root.words()[child_at] {
            self.root.words_mut()[child_at] = key;
        }
        if let Some((separator, right)) =
            insert_subtree(pool, self.height - 1, 1, key, &cursor, leaf_hole, &mut reserved)
        {
            if !insert_branch_entry_words(self.root.words_mut(), child_at, separator, right) {
                let left_index = reserved.pop().expect("root branch split was pre-reserved");
                let right_index = reserved.pop().expect("root branch split was pre-reserved");
                pool.write(left_index, self.root);
                pool.write(right_index, Node::leaf(&[NONE; NODE_SLOTS]));
                let (left_min, right_min) = split_branch_nodes(
                    pool,
                    left_index,
                    right_index,
                    child_at + 1,
                    separator,
                    right,
                );
                let root_child_at = BRANCH_SLOTS / 2 - 1;
                let root_words = self.root.words_mut();
                root_words.fill(NONE);
                root_words[root_child_at] = left_min;
                root_words[BRANCH_SLOTS + root_child_at] = left_index;
                root_words[root_child_at + 1] = right_min;
                root_words[BRANCH_SLOTS + root_child_at + 1] = right_index;
                self.height += 1;
                self._reserved = [0; 8];
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
            let Some((first, last)) = leaf_bounds(self.root.words()) else {
                return Ok(false);
            };
            let (found, _) = sparse_leaf_find(self.root.words(), key, first, last);
            let Some(slot) = found else {
                return Ok(false);
            };
            match remove_leaf_word(self.root.words_mut(), slot, key, None) {
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
        let child_at = branch_child(&self.root.words()[..BRANCH_SLOTS], key);
        let child = self.root.words()[BRANCH_SLOTS + child_at];
        let (minimum, merge_candidate) = match delete_subtree_direct(
            pool,
            child,
            self.height - 1,
            key,
            self.root.words()[child_at],
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
            match remove_leaf_word(self.root.words_mut(), cursor.leaf().slot as usize, key, None) {
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
        let child = self.root.words()[BRANCH_SLOTS + child_at];
        let (minimum, merge_candidate) = match delete_subtree(
            pool,
            self.height - 1,
            1,
            key,
            &cursor,
            self.root.words()[child_at],
        ) {
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
            remove_branch_child_words(self.root.words_mut(), child_at);
            pool.retire(child);
            if first_branch_slot_words(self.root.words()).is_none() {
                self.reset_root();
            }
        } else {
            self.root.words_mut()[child_at] = minimum;
            if merge_candidate {
                let root_ptr = std::ptr::addr_of_mut!(self.root);
                let _ = if self.height == 1 {
                    merge_leaf_children(pool, root_ptr, child_at)
                } else {
                    merge_branch_children(pool, root_ptr, child_at)
                };
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
                    self.root.words()[slot]
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

    fn reset_root(&mut self) {
        self.height = 0;
        self._reserved = [0; 8];
        self.root = Node::leaf(&[NONE; NODE_SLOTS]);
    }

    fn collapse_root(&mut self, pool: &mut ExtentTreePool) {
        while self.height > 0 {
            let Some(child_at) = first_branch_slot_words(self.root.words()) else {
                self.reset_root();
                return;
            };
            if next_branch_slot_words(self.root.words(), child_at + 1).is_some() {
                return;
            }
            let child = self.root.words()[BRANCH_SLOTS + child_at];
            let node = pool.read(child);
            if self.height == 1 {
                self.height = 0;
            } else {
                self.height -= 1;
            }
            self._reserved = [0; 8];
            // SAFETY: this child is a live tree node and all of its words are initialized.
            self.root = node;
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
            insert_sparse_item(pool.words_mut_ptr(leaf.node), key, leaf.slot as usize, hole);
            return None;
        }

        // With no holes, the leaf is dense. Split it in place while moving
        // the inserted key directly into its final sparse slot.
        let split = choose_leaf_split(NODE_SLOTS + 1, NODE_LEAF_CAPACITY);
        let right_index = reserved.pop().expect("leaf split was pre-reserved");
        pool.write(right_index, Node::leaf(&[NONE; NODE_SLOTS]));
        let separator =
            split_leaf_node(pool, leaf.node, right_index, leaf.slot as usize, key, split);
        return Some((separator, right_index));
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

        let right_index = reserved.pop().expect("branch split was pre-reserved");
        pool.write(right_index, Node::leaf(&[NONE; NODE_SLOTS]));
        let left_index = pool.index_of(path.node);
        let (_, promoted) =
            split_branch_nodes(pool, left_index, right_index, child_at + 1, separator, right);
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
        let removed_slot = if height == 1 {
            merge_leaf_children(pool, node_ptr, child_at)
        } else {
            merge_branch_children(pool, node_ptr, child_at)
        };
        if let Some(slot) = removed_slot {
            branch_slot_has_empty_neighbors_words(pool.words_ptr(node_ptr), slot)
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
/// in one node. The right node is retired on success.
///
/// `parent_ptr` points to this tree's live parent branch. The caller holds the
/// tree's exclusive lock; child nodes are distinct from that parent node.
fn merge_leaf_children(
    pool: &mut ExtentTreePool,
    parent_ptr: *mut Node,
    child_at: usize,
) -> Option<usize> {
    merge_children(
        pool,
        parent_ptr,
        child_at,
        NODE_LEAF_CAPACITY,
        leaf_node_count,
        merge_leaf_nodes,
    )
}

/// Merge the selected branch with an adjacent branch when their child entries
/// fit in one node. The right node is retired on success. `parent_ptr` has the
/// same ownership and locking requirements as in `merge_leaf_children`.
fn merge_branch_children(
    pool: &mut ExtentTreePool,
    parent_ptr: *mut Node,
    child_at: usize,
) -> Option<usize> {
    merge_children(
        pool,
        parent_ptr,
        child_at,
        BRANCH_CAPACITY,
        branch_node_count,
        merge_branch_nodes,
    )
}

fn leaf_node_count(pool: &ExtentTreePool, index: u32) -> usize {
    count_non_none(pool.words_ptr(pool.ptr(index)))
}

fn branch_node_count(pool: &ExtentTreePool, index: u32) -> usize {
    count_non_none(&pool.words_ptr(pool.ptr(index))[BRANCH_SLOTS..])
}

#[inline]
fn merge_children<C, M>(
    pool: &mut ExtentTreePool,
    parent_ptr: *mut Node,
    child_at: usize,
    capacity: usize,
    count_node: C,
    merge_nodes: M,
) -> Option<usize>
where
    C: Fn(&ExtentTreePool, u32) -> usize,
    M: Fn(&mut ExtentTreePool, u32, u32, usize, usize) -> bool,
{
    let current = unsafe { (*parent_ptr).words()[BRANCH_SLOTS + child_at] };
    debug_assert_ne!(current, NONE);
    let current_count = count_node(pool, current);
    if current_count > capacity / 2 {
        return None;
    }

    let next_at = unsafe { next_branch_slot_words((*parent_ptr).words(), child_at + 1) };
    if let Some(next_at) = next_at {
        let next = unsafe { (*parent_ptr).words()[BRANCH_SLOTS + next_at] };
        let next_count = count_node(pool, next);
        if current_count + next_count <= capacity
            && merge_nodes(pool, current, next, current_count, next_count)
        {
            pool.retire(next);
            unsafe { remove_branch_child_words((*parent_ptr).words_mut(), next_at) };
            return Some(next_at);
        }
    }

    let previous_at = unsafe { previous_branch_slot_words((*parent_ptr).words(), child_at) };
    if let Some(previous_at) = previous_at {
        let previous = unsafe { (*parent_ptr).words()[BRANCH_SLOTS + previous_at] };
        let previous_count = count_node(pool, previous);
        if previous_count + current_count <= capacity
            && merge_nodes(pool, previous, current, previous_count, current_count)
        {
            pool.retire(current);
            unsafe { remove_branch_child_words((*parent_ptr).words_mut(), child_at) };
            return Some(child_at);
        }
    }
    None
}

fn merge_leaf_nodes(
    pool: &mut ExtentTreePool,
    left_index: u32,
    right_index: u32,
    left_count: usize,
    right_count: usize,
) -> bool {
    if left_count == 0 || right_count == 0 || left_count + right_count > NODE_LEAF_CAPACITY {
        return false;
    }

    debug_assert_ne!(left_index, right_index);
    let count = left_count + right_count;
    let left_ptr = pool.ptr(left_index);
    let right_ptr = pool.ptr(right_index);
    // SAFETY: the indexes identify two distinct live nodes in the stable pool.
    // The caller holds the tree's exclusive lock, and the right node is only
    // read while the left node is rewritten in place.
    let (left_words, right_words) = unsafe { (&mut (*left_ptr).words, &(*right_ptr).words) };
    debug_assert_eq!(count_non_none(left_words), left_count);
    debug_assert_eq!(count_non_none(right_words), right_count);
    let _merged_count = merge_items_spread_b(left_words, right_words, left_count, right_count);
    debug_assert_eq!(_merged_count, count);
    true
}

fn count_non_none(words: &[u32]) -> usize {
    words.iter().filter(|&&key| key != NONE).count()
}

fn merge_branch_nodes(
    pool: &mut ExtentTreePool,
    left_index: u32,
    right_index: u32,
    left_count: usize,
    right_count: usize,
) -> bool {
    debug_assert_ne!(left_index, right_index);
    let left_ptr = pool.ptr(left_index);
    let right_ptr = pool.ptr(right_index);
    // SAFETY: the indexes refer to distinct live nodes and the caller owns the
    // tree's exclusive lock while merging them.
    let (left_words, right_words) = unsafe { (&mut (*left_ptr).words, &(*right_ptr).words) };
    debug_assert!(left_count > 0 && right_count > 0);
    if left_count == 0 || right_count == 0 || left_count + right_count > BRANCH_CAPACITY {
        return false;
    }

    let (left_keys, left_children) = left_words.split_at_mut(BRANCH_SLOTS);
    let (right_keys, right_children) = right_words.split_at(BRANCH_SLOTS);

    debug_assert_eq!(count_non_none(left_keys), left_count);
    debug_assert_eq!(count_non_none(right_keys), right_count);
    debug_assert_eq!(count_non_none(left_children), left_count);
    debug_assert_eq!(count_non_none(right_children), right_count);
    let count = left_count + right_count;
    let _merged_key_count = merge_items_spread_b(left_keys, right_keys, left_count, right_count);
    let _merged_child_count =
        merge_items_spread_b(left_children, right_children, left_count, right_count);
    debug_assert_eq!(_merged_key_count, count);
    debug_assert_eq!(_merged_child_count, count);
    true
}

/// Pack A at the start of its array, then spread B through the remaining slots.
/// The same routine handles leaf keys, branch keys, and child links.
fn merge_items_spread_b(a: &mut [u32], b: &[u32], a_count: usize, b_count: usize) -> usize {
    assert_eq!(a.len(), b.len());
    let mut out = 0;
    for source in 0..a.len() {
        let item = a[source];
        if item == NONE {
            continue;
        }
        if source != out {
            a[out] = item;
            a[source] = NONE;
        }
        out += 1;
    }
    debug_assert_eq!(out, a_count);
    let available = a.len() - a_count;
    let holes = available - b_count;
    let intervals = b_count + 1;
    a[a_count..].fill(NONE);
    let mut b_at = 0;
    for &item in b {
        if item != NONE {
            let spaces_before = ((b_at + 1) * holes + intervals / 2) / intervals;
            let target = a_count + b_at + spaces_before;
            debug_assert_eq!(a[target], NONE);
            a[target] = item;
            b_at += 1;
        }
    }
    debug_assert_eq!(b_at, b_count);
    a_count + b_count
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

fn branch_has_hole_words(words: &[u32; NODE_SLOTS]) -> bool {
    words[BRANCH_SLOTS..].contains(&NONE)
}

fn first_branch_slot_words(words: &[u32; NODE_SLOTS]) -> Option<usize> {
    (0..BRANCH_SLOTS).find(|&slot| words[BRANCH_SLOTS + slot] != NONE)
}

fn next_branch_slot_words(words: &[u32; NODE_SLOTS], from: usize) -> Option<usize> {
    (from..BRANCH_SLOTS).find(|&slot| words[BRANCH_SLOTS + slot] != NONE)
}

fn previous_branch_slot_words(words: &[u32; NODE_SLOTS], before: usize) -> Option<usize> {
    (0..before.min(BRANCH_SLOTS)).rev().find(|&slot| words[BRANCH_SLOTS + slot] != NONE)
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
    debug_assert_ne!(words[BRANCH_SLOTS + child_at], NONE);
    debug_assert!(separator > words[child_at]);

    let (keys, children) = words.split_at_mut(BRANCH_SLOTS);
    let anchor = child_at + 1;
    let hole = nearest_hole(children, anchor);
    let Some(hole) = hole else {
        return false;
    };

    // The caller updates the left subtree's separator first when its minimum
    // changed. Separators and child links have matching occupancy, so move
    // them separately through the same hole to keep each pair aligned.
    insert_sparse_item(keys, separator, anchor, hole);
    insert_sparse_item(children, right, anchor, hole);
    true
}

/// Split a full leaf in place. The original array is dense because a split is
/// only needed when it has no hole. `right_words` is fresh initialized storage.
fn split_leaf_node(
    pool: &mut ExtentTreePool,
    left_ptr: *mut Node,
    right_index: u32,
    insert_at: usize,
    key: u32,
    left_count: usize,
) -> u32 {
    debug_assert_ne!(pool.index_of(left_ptr), right_index);
    let right_ptr = pool.ptr(right_index);
    // SAFETY: both pointers identify distinct live nodes, the right node was
    // initialized to NONE, and the caller owns the tree's exclusive lock.
    let (left_words, right_words) = unsafe { (&mut (*left_ptr).words, &mut (*right_ptr).words) };
    debug_assert!(left_words.iter().all(|&word| word != NONE));
    let separator = value_with_insert(left_words, insert_at, key, left_count).0;
    split_sparse_items(left_words, right_words, insert_at, key, left_count, leaf_slot_for);
    separator
}

/// Split a full branch in place, using the same layout routine for its keys
/// and child indexes. Returns the minima of the left and right results.
fn split_branch_nodes(
    pool: &mut ExtentTreePool,
    left_index: u32,
    right_index: u32,
    insert_at: usize,
    separator: u32,
    right_child: u32,
) -> (u32, u32) {
    debug_assert_ne!(left_index, right_index);
    let total = BRANCH_SLOTS + 1;
    let left_count = total / 2;
    let left_ptr = pool.ptr(left_index);
    let right_ptr = pool.ptr(right_index);
    // SAFETY: both indexes name distinct live nodes, right was initialized to
    // NONE, and this caller owns the tree's exclusive lock.
    let (left_words, right_words) = unsafe { (&mut (*left_ptr).words, &mut (*right_ptr).words) };
    let (left_keys, left_children) = left_words.split_at_mut(BRANCH_SLOTS);
    let (right_keys, right_children) = right_words.split_at_mut(BRANCH_SLOTS);
    debug_assert!(left_children.iter().all(|&word| word != NONE));
    let left_min = value_with_insert(left_keys, insert_at, separator, 0).0;
    let right_min = value_with_insert(left_keys, insert_at, separator, left_count).0;
    split_sparse_items(
        left_keys,
        right_keys,
        insert_at,
        separator,
        left_count,
        branch_split_slot_for,
    );
    split_sparse_items(
        left_children,
        right_children,
        insert_at,
        right_child,
        left_count,
        branch_split_slot_for,
    );
    (left_min, right_min)
}

/// Split a dense input plus one virtual insertion into sparse left and right
/// outputs without materializing an intermediate packed array. The right side
/// is filled backwards first; then A is walked backwards into its final slots.
#[inline]
fn split_sparse_items<F>(
    a: &mut [u32],
    b: &mut [u32],
    insert_at: usize,
    item: u32,
    left_count: usize,
    slot_for: F,
) where
    F: Fn(usize, usize) -> usize + Copy,
{
    assert_eq!(a.len(), b.len());
    let total = a.len() + 1;
    let right_count = total - left_count;
    debug_assert!(insert_at <= a.len());
    debug_assert!(left_count > 0 && right_count > 0);
    debug_assert!(a.iter().all(|&value| value != NONE));

    b.fill(NONE);
    for sequence_at in (left_count..total).rev() {
        let (value, source) = value_with_insert(a, insert_at, item, sequence_at);
        let target = slot_for(sequence_at - left_count, right_count);
        b[target] = value;
        if let Some(source) = source {
            a[source] = NONE;
        }
    }

    for sequence_at in (0..left_count).rev() {
        let (value, source) = value_with_insert(a, insert_at, item, sequence_at);
        let target = slot_for(sequence_at, left_count);
        debug_assert!(source.is_none_or(|source| target >= source));
        a[target] = value;
        if let Some(source) = source {
            if source != target {
                a[source] = NONE;
            }
        }
    }
}

fn value_with_insert(
    words: &[u32],
    insert_at: usize,
    item: u32,
    sequence_at: usize,
) -> (u32, Option<usize>) {
    if sequence_at < insert_at {
        (words[sequence_at], Some(sequence_at))
    } else if sequence_at == insert_at {
        (item, None)
    } else {
        (words[sequence_at - 1], Some(sequence_at - 1))
    }
}

fn branch_split_slot_for(index: usize, count: usize) -> usize {
    debug_assert!(count == BRANCH_SLOTS / 2 || count == BRANCH_SLOTS / 2 + 1);
    let evenly_spaced_count = BRANCH_SLOTS / 2;
    if index < evenly_spaced_count { 2 * index } else { BRANCH_SLOTS - (count - index) }
}

fn branch_parts(words: &[u32; NODE_SLOTS]) -> (BranchKeys, BranchChildren, usize) {
    let (keys, children) = branch_values(words);
    let count = count_non_none(&words[BRANCH_SLOTS..]);
    (keys, children, count)
}

fn branch_child(keys: &[u32], key: u32) -> usize {
    debug_assert_eq!(keys.len(), BRANCH_SLOTS);
    let first = keys.iter().position(|&separator| separator != NONE).expect("nonempty branch");
    let mut low = first;
    let mut high = BRANCH_SLOTS;
    let mut previous = None;

    while low < high {
        let middle = low + (high - low) / 2;
        let Some(at) = nearest_key(keys, low, high, middle) else {
            break;
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

fn first_branch_slot(children: &BranchChildren) -> Option<usize> {
    (0..BRANCH_SLOTS).find(|&slot| children[slot] != NONE)
}

fn previous_branch_slot(children: &BranchChildren, before: usize) -> Option<usize> {
    (0..before.min(BRANCH_SLOTS)).rev().find(|&slot| children[slot] != NONE)
}

fn next_branch_slot(children: &BranchChildren, from: usize) -> Option<usize> {
    (from..BRANCH_SLOTS).find(|&slot| children[slot] != NONE)
}

fn leaf_slot_for(key_at: usize, count: usize) -> usize {
    debug_assert!(count > 0 && count <= NODE_LEAF_CAPACITY);
    debug_assert!(key_at < count);
    if count == 1 {
        NODE_LEAF_CAPACITY / 2
    } else {
        let intervals = count - 1;
        (key_at * (NODE_LEAF_CAPACITY - 1) + intervals / 2) / intervals
    }
}

fn choose_leaf_split(total_count: usize, capacity: usize) -> usize {
    debug_assert!(total_count >= 2);
    let mut best: Option<(usize, usize, usize)> = None;
    for split in 1..total_count {
        let right_count = total_count - split;
        if split > capacity || right_count > capacity {
            continue;
        }
        let score = (split.max(right_count), split.abs_diff(right_count), split);
        if best.is_none_or(|old| score < old) {
            best = Some(score);
        }
    }
    best.map(|(_, _, split)| split).unwrap_or_else(|| {
        debug_assert!(false, "no capacity-respecting split for raw leaf");
        total_count / 2
    })
}

fn leaf_bounds(words: &[u32; NODE_SLOTS]) -> Option<(usize, usize)> {
    let first = (0..NODE_SLOTS).find(|&slot| words[slot] != NONE)?;
    let last = (first..NODE_SLOTS).rev().find(|&slot| words[slot] != NONE)?;
    Some((first, last))
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

fn nearest_key(words: &[u32], low: usize, high: usize, middle: usize) -> Option<usize> {
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
        let Some(at) = nearest_key(words, low, high, middle) else {
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

fn nearest_hole(words: &[u32], target: usize) -> Option<usize> {
    let len = words.len();
    if len == 0 {
        return None;
    }
    debug_assert!(target <= len);
    let target = target.min(len - 1);
    if words[target] == NONE {
        return Some(target);
    }

    let mut right = target + 1;
    // A valid slice's byte size fits in isize, so its indexes fit as well.
    let mut left = target as isize - 1;
    loop {
        if right == len {
            while left >= 0 {
                let at = left as usize;
                if words[at] == NONE {
                    return Some(at);
                }
                left -= 1;
            }
            return None;
        }
        if words[right] == NONE {
            return Some(right);
        }
        right += 1;

        if left < 0 {
            for at in right..len {
                if words[at] == NONE {
                    return Some(at);
                }
            }
            return None;
        }
        let at = left as usize;
        if words[at] == NONE {
            return Some(at);
        }
        left -= 1;
    }
}

fn insert_sparse_item(words: &mut [u32], item: u32, anchor: usize, mut hole: usize) {
    debug_assert_eq!(words[hole], NONE);
    debug_assert!(anchor <= words.len());

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

    words[hole] = item;
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
            let count = count_non_none(node.words());
            usage.leaves += 1;
            usage.leaf_id_bytes += count * size_of::<u32>();
            usage.occupied_slots += count;
            return;
        }
        let words = node.words();
        let count = count_non_none(&words[BRANCH_SLOTS..]);
        usage.branches += 1;
        usage.occupied_slots += count * 2;
        for &child in &words[BRANCH_SLOTS..] {
            if child != NONE {
                usage_node(pool, child, height - 1, usage);
            }
        }
    }

    fn tree_usage(tree: &EnExtentTree, pool: &ExtentTreePool) -> TreeUsage {
        let mut usage = TreeUsage::default();
        if tree.height == 0 {
            let count = count_non_none(tree.root.words());
            if count != 0 {
                usage.leaves = 1;
                usage.leaf_id_bytes = count * size_of::<u32>();
                usage.occupied_slots = count;
            }
        } else {
            let words = tree.root.words();
            let count = count_non_none(&words[BRANCH_SLOTS..]);
            usage.occupied_slots += count * 2;
            for &child in &words[BRANCH_SLOTS..] {
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
        assert_eq!(count_non_none(tree.root.words()), NODE_LEAF_CAPACITY);
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
    fn merge_packs_a_then_spreads_b() {
        let mut a = [10, NONE, NONE, 20];
        let b = [NONE, 30, NONE, NONE];
        assert_eq!(merge_items_spread_b(&mut a, &b, 2, 1), 3);
        assert_eq!(a, [10, 20, NONE, 30]);
    }

    #[test]
    fn split_moves_inserted_items_in_place_at_every_position() {
        let original = [10u32, 20, 30, 40];
        let capacity = original.len();
        for insert_at in 0..=original.len() {
            let inserted = insert_at as u32 * 10 + 5;
            let mut expected = original.to_vec();
            expected.insert(insert_at, inserted);
            let mut a = original;
            let mut b = [NONE; 4];

            split_sparse_items(&mut a, &mut b, insert_at, inserted, 2, |index, count| {
                index * (capacity - 1) / (count - 1)
            });

            let left: Vec<_> = a.into_iter().filter(|&item| item != NONE).collect();
            let right: Vec<_> = b.into_iter().filter(|&item| item != NONE).collect();
            assert_eq!(left, expected[..2]);
            assert_eq!(right, expected[2..]);
        }
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

    fn tree_until_height_two(pool: &mut ExtentTreePool, stride: u32) -> EnExtentTree {
        let mut tree = EnExtentTree::new();
        let max_entries = NODE_SLOTS * BRANCH_CAPACITY * 8;
        for value in 0..max_entries as u32 {
            assert!(tree.insert(pool, ExtentId::new(value * stride)).unwrap());
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
        let mut tree = tree_until_height_two(&mut pool, 1);
        let before = collect(&tree, &pool);

        let (_, root_children, root_count) = tree.root.branch_parts();
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
        // Leave gaps between IDs so the next leaf can be filled without
        // splitting. While this leaf still has an item, a full neighbor makes
        // their combined count exceed capacity. Removing its final item yields
        // NONE and removes its child slot directly, without a coalesce attempt.
        let mut tree = tree_until_height_two(&mut pool, 2);

        let (_, root_children, root_count) = tree.root.branch_parts();
        assert_eq!(root_count, 2);
        let left_branch = root_children[first_branch_slot(&root_children).unwrap()];
        let (_, children, child_count) = pool.read(left_branch).branch_parts();
        assert_eq!(child_count, BRANCH_SLOTS / 2);
        assert_ne!(children[0], NONE);
        assert_eq!(children[1], NONE);

        let next_at = next_branch_slot(&children, 1).expect("edge leaf has a right neighbor");
        let next_leaf = children[next_at];
        let mut next_keys = [0; KEY_SCRATCH];
        let next_count = pool.read(next_leaf).leaf_parts(&mut next_keys);
        assert_eq!(next_count, NODE_LEAF_CAPACITY / 2);
        for &key in &next_keys[..next_count] {
            assert_eq!(key % 2, 0);
            assert!(tree.insert(&mut pool, ExtentId::new(key + 1)).unwrap());
        }
        assert_eq!(count_non_none(pool.words_ptr(pool.ptr(next_leaf))), NODE_LEAF_CAPACITY);
        let before = collect(&tree, &pool);

        let removed = delete_leaf_highest_to_lowest(&mut tree, &mut pool, children[0]);
        assert_eq!(tree.height, 2);
        let (_, root_children, root_count) = tree.root.branch_parts();
        assert_eq!(root_count, 2);
        let left_branch_after = root_children[first_branch_slot(&root_children).unwrap()];
        assert_eq!(left_branch_after, left_branch);
        let (_, children_after, child_count_after) = pool.read(left_branch_after).branch_parts();
        assert_eq!(child_count_after, child_count - 1);
        assert_eq!(children_after[0], NONE);
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
