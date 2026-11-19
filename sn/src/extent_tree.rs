//! Per-EN extent index used by SN for sync.
//!
//! Each EN's root is inline in its SN-side row. The root starts as a leaf, so
//! small trees need no node-pool allocation. At greater heights it is an inline
//! branch. Height zero means leaf; branches and the root terminate their child
//! arrays with `NONE` instead of storing child counts. Leaf items use raw u32
//! slots.

use common::ids::ExtentId;
use common::{Error, Result};
use pool::{LockedItem, LockedPool, PoolItem};
use std::mem::{align_of, offset_of, size_of};

const NONE: u32 = u32::MAX;
// The tuning knob: each u32 slot contributes four bytes to a pooled node.
const NODE_SLOTS: usize = 32;
const NODE_BYTES: usize = NODE_SLOTS * size_of::<u32>();
const LEAF_LINK_SLOT: usize = NODE_SLOTS - 1;
const NODE_LEAF_CAPACITY: usize = LEAF_LINK_SLOT;
const ROOT_LEAF_CAPACITY: usize = NODE_SLOTS;
const KEY_SCRATCH: usize = ROOT_LEAF_CAPACITY + 2;
const MERGE_KEY_SCRATCH: usize = NODE_LEAF_CAPACITY * 2 + 2;
// Branch nodes hold sorted (child minimum, child link) pairs in sparse slots.
const BRANCH_SLOTS: usize = NODE_SLOTS / 2;
const BRANCH_CAPACITY: usize = BRANCH_SLOTS;
const BRANCH_KEYS_SCRATCH: usize = BRANCH_SLOTS + 1;
const BRANCH_CHILDREN_SCRATCH: usize = BRANCH_SLOTS + 1;

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

    fn leaf(keys: &[u32], next: u32) -> Self {
        debug_assert!(!keys.is_empty());
        debug_assert!(keys.len() <= NODE_LEAF_CAPACITY);
        let mut node = Self::blank();
        node.words[..keys.len()].copy_from_slice(keys);
        node.words[LEAF_LINK_SLOT] = next;
        node
    }

    fn leaf_parts(self, keys: &mut KeyScratch) -> (usize, u32) {
        let mut count = 0;
        for &key in &self.words[..LEAF_LINK_SLOT] {
            if key != NONE {
                keys[count] = key;
                count += 1;
            }
        }
        debug_assert!(keys[..count].windows(2).all(|pair| pair[0] < pair[1]));
        (count, self.words[LEAF_LINK_SLOT])
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

    fn write(&mut self, index: u32, node: Node) {
        // SAFETY: the tree owns this live node exclusively during mutation.
        unsafe { self.ptr(index).write(node) };
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
        empty_leaf_next: Option<u32>,
        merge_candidate: bool,
    },
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

    pub(super) fn contains(&self, pool: &ExtentTreePool, id: ExtentId) -> bool {
        let key = id.get();
        if key == NONE || self.len == 0 {
            return false;
        }
        if self.height == 0 {
            let mut keys = [0; KEY_SCRATCH];
            let count = self.root_leaf_parts(&mut keys);
            return keys[..count].binary_search(&key).is_ok();
        }
        let leaf = self.find_leaf(pool, key);
        let mut keys = [0; KEY_SCRATCH];
        let (count, _) = pool.read(leaf).leaf_parts(&mut keys);
        keys[..count].binary_search(&key).is_ok()
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

        let Some(reserve_count) = self.plan_insert(pool, key)? else {
            return Ok(false);
        };
        let mut reserved = pool.reserve_nodes(reserve_count)?;

        if self.len == 0 {
            self.set_root_leaf(&[key]);
            self.len = 1;
            return Ok(true);
        }

        if self.height == 0 {
            let mut keys = [0; KEY_SCRATCH];
            let count = self.root_leaf_parts(&mut keys);
            let at = lower_bound(&keys[..count], key);
            insert_key(&mut keys, count, at, key);
            if can_store_leaf(&keys[..count + 1], ROOT_LEAF_CAPACITY) {
                self.set_root_leaf(&keys[..count + 1]);
            } else {
                let split = choose_leaf_split(&keys[..count + 1], NODE_LEAF_CAPACITY);
                let left_index = reserved.pop().expect("root leaf split was pre-reserved");
                let right_index = reserved.pop().expect("root leaf split was pre-reserved");
                pool.write(left_index, Node::leaf(&keys[..split], right_index));
                pool.write(right_index, Node::leaf(&keys[split..count + 1], NONE));
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

        let (mut root_keys, mut root_children, mut root_count) = self.root_branch_parts();
        let child_at = branch_child(&root_keys, &root_children, root_count, key);
        let first_child = first_branch_slot(&root_children).expect("nonempty root branch");
        let min_changed = child_at == first_child && key < root_keys[child_at];
        if min_changed {
            root_keys[child_at] = key;
        }
        if let Some((separator, right)) =
            insert_subtree(pool, root_children[child_at], self.height - 1, key, &mut reserved)
        {
            if insert_branch_entry(
                &mut root_keys,
                &mut root_children,
                &mut root_count,
                child_at,
                separator,
                right,
            ) {
                self.write_root_branch(
                    self.height,
                    &root_keys[..BRANCH_SLOTS],
                    &root_children[..BRANCH_SLOTS],
                );
            } else {
                let (dense_keys, dense_children, dense_count) = dense_insert_branch(
                    &root_keys,
                    &root_children,
                    root_count,
                    child_at,
                    separator,
                    right,
                );
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
        } else if min_changed {
            self.write_root_branch(
                self.height,
                &root_keys[..BRANCH_SLOTS],
                &root_children[..BRANCH_SLOTS],
            );
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
            let mut keys = [0; KEY_SCRATCH];
            let count = self.root_leaf_parts(&mut keys);
            let at = lower_bound(&keys[..count], key);
            if at == count || keys[at] != key {
                return Ok(false);
            }
            remove_key(&mut keys, count, at);
            self.set_root_leaf(&keys[..count - 1]);
            self.len -= 1;
            return Ok(true);
        }

        let (mut root_keys, mut root_children, mut root_count) = self.root_branch_parts();
        let child_at = branch_child(&root_keys, &root_children, root_count, key);
        let child = root_children[child_at];
        let (minimum, empty_leaf_next, merge_candidate) =
            match delete_subtree(pool, child, self.height - 1, key) {
                DeleteResult::NotFound => return Ok(false),
                DeleteResult::Removed { minimum, empty_leaf_next, merge_candidate } => {
                    (minimum, empty_leaf_next, merge_candidate)
                }
            };

        if let (Some(next), Some(previous)) =
            (empty_leaf_next, previous_branch_slot(&root_children, child_at))
        {
            let previous_leaf = rightmost_leaf(pool, root_children[previous], self.height - 1);
            set_leaf_next(pool, previous_leaf, next);
        }

        if minimum == NONE {
            remove_branch_child(&mut root_keys, &mut root_children, &mut root_count, child_at);
            pool.retire(child);
            if first_branch_slot(&root_children).is_none() {
                self.reset_root();
            } else {
                self.write_root_branch(
                    self.height,
                    &root_keys[..BRANCH_SLOTS],
                    &root_children[..BRANCH_SLOTS],
                );
            }
        } else {
            root_keys[child_at] = minimum;
            if self.height == 1 && root_count > 1 && merge_candidate {
                let _ = merge_leaf_children(
                    pool,
                    &mut root_keys,
                    &mut root_children,
                    &mut root_count,
                    child_at,
                );
            } else if self.height > 1 && root_count > 1 && merge_candidate {
                let _ = merge_branch_children(
                    pool,
                    &mut root_keys,
                    &mut root_children,
                    &mut root_count,
                    child_at,
                );
            }
            self.write_root_branch(
                self.height,
                &root_keys[..BRANCH_SLOTS],
                &root_children[..BRANCH_SLOTS],
            );
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
        if self.height == 0 {
            let mut keys = [0; KEY_SCRATCH];
            let count = self.root_leaf_parts(&mut keys);
            for &key in &keys[..count] {
                if !visit(ExtentId::new(key)) {
                    return;
                }
            }
            return;
        }

        let mut index = self.leftmost_leaf(pool);
        while index != NONE {
            let node = pool.read(index);
            let mut keys = [0; KEY_SCRATCH];
            let (count, next) = node.leaf_parts(&mut keys);
            for &key in &keys[..count] {
                if !visit(ExtentId::new(key)) {
                    return;
                }
            }
            index = next;
        }
    }

    fn plan_insert(&self, pool: &ExtentTreePool, key: u32) -> Result<Option<usize>> {
        if self.height == 0 {
            let mut keys = [0; KEY_SCRATCH];
            let count = self.root_leaf_parts(&mut keys);
            let at = lower_bound(&keys[..count], key);
            if at < count && keys[at] == key {
                return Ok(None);
            }
            insert_key(&mut keys, count, at, key);
            return Ok(Some(if can_store_leaf(&keys[..count + 1], ROOT_LEAF_CAPACITY) {
                0
            } else {
                2
            }));
        }

        let (root_keys, root_children, root_count) = self.root_branch_parts();
        let mut child_at = branch_child(&root_keys, &root_children, root_count, key);
        let mut index = root_children[child_at];
        let mut level = self.height;
        let mut pooled_branch_counts = Vec::new();
        while level > 1 {
            let node = pool.read(index);
            let (keys, children, count) = node.branch_parts();
            pooled_branch_counts.push(count);
            child_at = branch_child(&keys, &children, count, key);
            index = children[child_at];
            level -= 1;
        }

        let node = pool.read(index);
        let mut keys = [0; KEY_SCRATCH];
        let (count, _) = node.leaf_parts(&mut keys);
        let at = lower_bound(&keys[..count], key);
        if at < count && keys[at] == key {
            return Ok(None);
        }
        insert_key(&mut keys, count, at, key);
        if can_store_leaf(&keys[..count + 1], NODE_LEAF_CAPACITY) {
            return Ok(Some(0));
        }

        let mut needed = 1; // one new right leaf
        let mut split_carries = true;
        for branch_count in pooled_branch_counts.into_iter().rev() {
            if split_carries && branch_count == BRANCH_CAPACITY {
                needed += 1; // full pooled branch splits and carries upward
            } else {
                split_carries = false;
                break;
            }
        }
        if split_carries && root_count == BRANCH_CAPACITY {
            if self.height == u8::MAX {
                return Err(Error::new("EN extent-tree height overflow"));
            }
            needed += 2; // the inline root becomes two pooled branches
        }
        Ok(Some(needed))
    }

    fn root_leaf_parts(&self, keys: &mut KeyScratch) -> usize {
        debug_assert!(self.height == 0);
        let count = self.root_words.iter().position(|&key| key == NONE).unwrap_or(NODE_SLOTS);
        debug_assert!(self.root_words[count..].iter().all(|&key| key == NONE));
        keys[..count].copy_from_slice(&self.root_words[..count]);
        count
    }

    fn set_root_leaf(&mut self, keys: &[u32]) {
        debug_assert!(keys.len() <= ROOT_LEAF_CAPACITY);
        self.height = 0;
        self._reserved = [0; 8];
        self.root_words = [NONE; NODE_SLOTS];
        self.root_words[..keys.len()].copy_from_slice(keys);
    }

    fn root_branch_parts(&self) -> (BranchKeys, BranchChildren, usize) {
        debug_assert!(self.height > 0);
        branch_parts(&self.root_words)
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

    fn find_leaf(&self, pool: &ExtentTreePool, key: u32) -> u32 {
        let (root_keys, root_children, root_count) = self.root_branch_parts();
        let mut index = root_children[branch_child(&root_keys, &root_children, root_count, key)];
        let mut level = self.height - 1;
        while level > 0 {
            let node = pool.read(index);
            let (keys, children, count) = node.branch_parts();
            index = children[branch_child(&keys, &children, count, key)];
            level -= 1;
        }
        index
    }

    fn leftmost_leaf(&self, pool: &ExtentTreePool) -> u32 {
        let (_, children, _) = self.root_branch_parts();
        let mut index = children[first_branch_slot(&children).expect("nonempty root branch")];
        let mut level = self.height - 1;
        while level > 0 {
            let (_, children, _) = pool.read(index).branch_parts();
            index = children[first_branch_slot(&children).expect("nonempty branch")];
            level -= 1;
        }
        index
    }

    fn collapse_root(&mut self, pool: &mut ExtentTreePool) {
        while self.height > 0 {
            let (keys, children, count) = self.root_branch_parts();
            if count > 1 {
                return;
            }
            if count == 0 {
                self.reset_root();
                return;
            }
            let child_at = first_branch_slot(&children).expect("single-child root");
            let child = children[child_at];
            let node = pool.read(child);
            if self.height == 1 {
                let mut child_keys = [0; KEY_SCRATCH];
                let (child_count, _) = node.leaf_parts(&mut child_keys);
                self.set_root_leaf(&child_keys[..child_count]);
            } else {
                self.height -= 1;
                self._reserved = [0; 8];
                self.root_words = node.words;
            }
            pool.retire(child);
            let _ = keys;
        }
    }
}

fn insert_subtree(
    pool: &mut ExtentTreePool,
    index: u32,
    height: u8,
    key: u32,
    reserved: &mut Vec<u32>,
) -> Option<(u32, u32)> {
    let node = pool.read(index);
    if height == 0 {
        let mut keys = [0; KEY_SCRATCH];
        let (count, next) = node.leaf_parts(&mut keys);
        let at = lower_bound(&keys[..count], key);
        debug_assert!(at == count || keys[at] != key);
        insert_key(&mut keys, count, at, key);
        if can_store_leaf(&keys[..count + 1], NODE_LEAF_CAPACITY) {
            pool.write(index, Node::leaf(&keys[..count + 1], next));
            return None;
        }
        let split = choose_leaf_split(&keys[..count + 1], NODE_LEAF_CAPACITY);
        let right_index = reserved.pop().expect("leaf split was pre-reserved");
        pool.write(right_index, Node::leaf(&keys[split..count + 1], next));
        pool.write(index, Node::leaf(&keys[..split], right_index));
        return Some((keys[split], right_index));
    }

    let (mut keys, mut children, mut count) = node.branch_parts();
    let child_at = branch_child(&keys, &children, count, key);
    let first_child = first_branch_slot(&children).expect("nonempty branch");
    let min_changed = child_at == first_child && key < keys[child_at];
    if min_changed {
        keys[child_at] = key;
    }
    if let Some((separator, right)) =
        insert_subtree(pool, children[child_at], height - 1, key, reserved)
    {
        if insert_branch_entry(&mut keys, &mut children, &mut count, child_at, separator, right) {
            pool.write(index, Node::branch(&keys[..BRANCH_SLOTS], &children[..BRANCH_SLOTS]));
            return None;
        }

        let (dense_keys, dense_children, dense_count) =
            dense_insert_branch(&keys, &children, count, child_at, separator, right);
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
        pool.write(index, Node::branch(&left_keys[..BRANCH_SLOTS], &left_children[..BRANCH_SLOTS]));
        return Some((promoted, right_index));
    }
    if min_changed {
        pool.write(index, Node::branch(&keys[..BRANCH_SLOTS], &children[..BRANCH_SLOTS]));
    }
    None
}

fn delete_subtree(pool: &mut ExtentTreePool, index: u32, height: u8, key: u32) -> DeleteResult {
    let node = pool.read(index);
    if height == 0 {
        let mut keys = [0; KEY_SCRATCH];
        let (count, next) = node.leaf_parts(&mut keys);
        let at = lower_bound(&keys[..count], key);
        if at == count || keys[at] != key {
            return DeleteResult::NotFound;
        }
        let slot = node.words[..LEAF_LINK_SLOT]
            .iter()
            .position(|&value| value == key)
            .expect("found leaf key has a physical slot");
        let mut updated = node;
        updated.words[slot] = NONE;
        let minimum = updated.words[..LEAF_LINK_SLOT]
            .iter()
            .copied()
            .find(|&value| value != NONE)
            .unwrap_or(NONE);
        if minimum == NONE {
            return DeleteResult::Removed {
                minimum: NONE,
                empty_leaf_next: Some(next),
                merge_candidate: false,
            };
        }
        let merge_candidate = slot > 0
            && slot + 1 < LEAF_LINK_SLOT
            && updated.words[slot - 1] == NONE
            && updated.words[slot + 1] == NONE;
        pool.write(index, updated);
        return DeleteResult::Removed { minimum, empty_leaf_next: None, merge_candidate };
    }

    let (mut keys, mut children, mut count) = node.branch_parts();
    let child_at = branch_child(&keys, &children, count, key);
    let child_index = children[child_at];
    let (child_minimum, mut empty_leaf_next, merge_candidate) =
        match delete_subtree(pool, child_index, height - 1, key) {
            DeleteResult::NotFound => return DeleteResult::NotFound,
            DeleteResult::Removed { minimum, empty_leaf_next, merge_candidate } => {
                (minimum, empty_leaf_next, merge_candidate)
            }
        };

    if let (Some(next), Some(previous)) =
        (empty_leaf_next, previous_branch_slot(&children, child_at))
    {
        let previous_leaf = rightmost_leaf(pool, children[previous], height - 1);
        set_leaf_next(pool, previous_leaf, next);
        empty_leaf_next = None;
    }

    if child_minimum == NONE {
        remove_branch_child(&mut keys, &mut children, &mut count, child_at);
        pool.retire(child_index);
        let Some(first_child) = first_branch_slot(&children) else {
            return DeleteResult::Removed {
                minimum: NONE,
                empty_leaf_next,
                merge_candidate: false,
            };
        };
        let minimum = keys[first_child];
        let merge_candidate = branch_slot_has_empty_neighbors(&children, child_at);
        pool.write(index, Node::branch(&keys[..BRANCH_SLOTS], &children[..BRANCH_SLOTS]));
        return DeleteResult::Removed { minimum, empty_leaf_next, merge_candidate };
    }

    keys[child_at] = child_minimum;
    let branch_merge_candidate = if count > 1 && merge_candidate {
        let removed_slot = if height == 1 {
            merge_leaf_children(pool, &mut keys, &mut children, &mut count, child_at)
        } else {
            merge_branch_children(pool, &mut keys, &mut children, &mut count, child_at)
        };
        removed_slot.is_some_and(|slot| branch_slot_has_empty_neighbors(&children, slot))
    } else {
        false
    };
    let first_child = first_branch_slot(&children).expect("nonempty branch after deletion");
    let minimum = keys[first_child];
    pool.write(index, Node::branch(&keys[..BRANCH_SLOTS], &children[..BRANCH_SLOTS]));
    DeleteResult::Removed { minimum, empty_leaf_next, merge_candidate: branch_merge_candidate }
}

fn set_leaf_next(pool: &mut ExtentTreePool, index: u32, next: u32) {
    let mut leaf = pool.read(index);
    debug_assert!(leaf.words[..LEAF_LINK_SLOT].iter().any(|&value| value != NONE));
    leaf.words[LEAF_LINK_SLOT] = next;
    pool.write(index, leaf);
}

/// Merge the selected leaf with an adjacent leaf when the combined IDs fit
/// after recomputing their common prefix. The right node is retired on success.
fn merge_leaf_children(
    pool: &mut ExtentTreePool,
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    parent_count: &mut usize,
    child_at: usize,
) -> Option<usize> {
    if let Some(left_at) = previous_branch_slot(children, child_at) {
        let left = children[left_at];
        let right = children[child_at];
        if merge_leaf_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, parent_count, child_at);
            return Some(child_at);
        }
    }
    if let Some(right_at) = next_branch_slot(children, child_at + 1) {
        let left = children[child_at];
        let right = children[right_at];
        if merge_leaf_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, parent_count, right_at);
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
    parent_count: &mut usize,
    child_at: usize,
) -> Option<usize> {
    if let Some(left_at) = previous_branch_slot(children, child_at) {
        let left = children[left_at];
        let right = children[child_at];
        if merge_branch_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, parent_count, child_at);
            return Some(child_at);
        }
    }
    if let Some(right_at) = next_branch_slot(children, child_at + 1) {
        let left = children[child_at];
        let right = children[right_at];
        if merge_branch_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, parent_count, right_at);
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
    let (left_count, _) = left.leaf_parts(&mut left_keys);
    let (right_count, right_next) = right.leaf_parts(&mut right_keys);
    debug_assert!(left_count > 0 && right_count > 0);
    debug_assert!(left_keys[left_count - 1] < right_keys[0]);

    let count = left_count + right_count;
    let mut merged = [0; MERGE_KEY_SCRATCH];
    merged[..left_count].copy_from_slice(&left_keys[..left_count]);
    merged[left_count..count].copy_from_slice(&right_keys[..right_count]);
    if !can_store_leaf(&merged[..count], NODE_LEAF_CAPACITY) {
        return false;
    }
    pool.write(left_index, Node::leaf(&merged[..count], right_next));
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

fn branch_parts(words: &[u32; NODE_SLOTS]) -> (BranchKeys, BranchChildren, usize) {
    let mut keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut children = [NONE; BRANCH_CHILDREN_SCRATCH];
    let mut count = 0;
    for slot in 0..BRANCH_SLOTS {
        let key = words[2 * slot];
        let child = words[2 * slot + 1];
        debug_assert_eq!(key == NONE, child == NONE);
        keys[slot] = key;
        children[slot] = child;
        if child != NONE {
            count += 1;
        }
    }
    (keys, children, count)
}

fn write_branch(words: &mut [u32; NODE_SLOTS], keys: &[u32], children: &[u32]) {
    debug_assert_eq!(keys.len(), BRANCH_SLOTS);
    debug_assert_eq!(children.len(), BRANCH_SLOTS);
    *words = [NONE; NODE_SLOTS];
    for slot in 0..BRANCH_SLOTS {
        debug_assert_eq!(keys[slot] == NONE, children[slot] == NONE);
        words[2 * slot] = keys[slot];
        words[2 * slot + 1] = children[slot];
    }
}

fn root_branch_parts(tree: &EnExtentTree) -> (BranchKeys, BranchChildren, usize) {
    debug_assert!(tree.height > 0);
    branch_parts(&tree.root_words)
}

fn branch_child(keys: &BranchKeys, children: &BranchChildren, count: usize, key: u32) -> usize {
    debug_assert!(count > 0);
    let mut previous = None;
    for slot in 0..BRANCH_SLOTS {
        if children[slot] == NONE {
            debug_assert_eq!(keys[slot], NONE);
            continue;
        }
        debug_assert_ne!(keys[slot], NONE);
        if key < keys[slot] {
            return previous.unwrap_or(slot);
        }
        previous = Some(slot);
    }
    previous.expect("nonempty branch")
}

fn insert_branch_entry(
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    count: &mut usize,
    child_at: usize,
    separator: u32,
    right: u32,
) -> bool {
    debug_assert!(children[child_at] != NONE);
    debug_assert!(separator > keys[child_at]);
    if *count == BRANCH_CAPACITY {
        return false;
    }

    let next = next_branch_slot(children, child_at + 1).unwrap_or(BRANCH_SLOTS);
    let gap_start = child_at + 1;
    if gap_start < next {
        let gap_len = next - gap_start;
        let at = gap_start + gap_len / 2;
        keys[at] = separator;
        children[at] = right;
        *count += 1;
        return true;
    }

    let left_hole = (0..child_at).rev().find(|&slot| children[slot] == NONE);
    let right_hole = (child_at + 1..BRANCH_SLOTS).find(|&slot| children[slot] == NONE);
    match (left_hole, right_hole) {
        (None, None) => false,
        (Some(hole), None) => {
            for src in hole + 1..=child_at {
                keys[src - 1] = keys[src];
                children[src - 1] = children[src];
            }
            keys[child_at] = separator;
            children[child_at] = right;
            *count += 1;
            true
        }
        (None, Some(hole)) => {
            for src in (child_at + 1..hole).rev() {
                keys[src + 1] = keys[src];
                children[src + 1] = children[src];
            }
            keys[child_at + 1] = separator;
            children[child_at + 1] = right;
            *count += 1;
            true
        }
        (Some(left), Some(right_hole)) => {
            let left_distance = child_at - left;
            let right_distance = right_hole - (child_at + 1);
            if left_distance < right_distance {
                for src in left + 1..=child_at {
                    keys[src - 1] = keys[src];
                    children[src - 1] = children[src];
                }
                keys[child_at] = separator;
                children[child_at] = right;
            } else {
                for src in (child_at + 1..right_hole).rev() {
                    keys[src + 1] = keys[src];
                    children[src + 1] = children[src];
                }
                keys[child_at + 1] = separator;
                children[child_at + 1] = right;
            }
            *count += 1;
            true
        }
    }
}

fn dense_insert_branch(
    keys: &BranchKeys,
    children: &BranchChildren,
    count: usize,
    after_slot: usize,
    separator: u32,
    right: u32,
) -> (BranchKeys, BranchChildren, usize) {
    let mut dense_keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut dense_children = [NONE; BRANCH_CHILDREN_SCRATCH];
    let mut out = 0;
    for slot in 0..BRANCH_SLOTS {
        if children[slot] == NONE {
            continue;
        }
        dense_keys[out] = keys[slot];
        dense_children[out] = children[slot];
        out += 1;
        if slot == after_slot {
            dense_keys[out] = separator;
            dense_children[out] = right;
            out += 1;
        }
    }
    debug_assert_eq!(out, count + 1);
    (dense_keys, dense_children, out)
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

fn remove_branch_child(
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    count: &mut usize,
    child_at: usize,
) {
    debug_assert!(*count > 0);
    debug_assert!(child_at < BRANCH_SLOTS);
    debug_assert_ne!(children[child_at], NONE);
    keys[child_at] = NONE;
    children[child_at] = NONE;
    *count -= 1;
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

fn last_branch_slot(children: &BranchChildren) -> Option<usize> {
    (0..BRANCH_SLOTS).rev().find(|&slot| children[slot] != NONE)
}

fn rightmost_leaf(pool: &ExtentTreePool, mut index: u32, mut height: u8) -> u32 {
    while height > 0 {
        let node = pool.read(index);
        let (_, children, _) = node.branch_parts();
        index = children[last_branch_slot(&children).expect("nonempty branch")];
        height -= 1;
    }
    index
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

fn remove_key(keys: &mut KeyScratch, count: usize, at: usize) {
    for i in at..count - 1 {
        keys[i] = keys[i + 1];
    }
}

fn lower_bound(keys: &[u32], key: u32) -> usize {
    keys.binary_search(&key).unwrap_or_else(|at| at)
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
            let (count, _) = node.leaf_parts(&mut keys);
            usage.leaves += 1;
            usage.leaf_id_bytes += count * size_of::<u32>();
            usage.occupied_slots += 1 + count;
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

        let right_leaf = tree.find_leaf(&pool, ROOT_LEAF_CAPACITY as u32);
        let mut leaf_keys = [0; KEY_SCRATCH];
        let (count, _) = pool.read(right_leaf).leaf_parts(&mut leaf_keys);
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
        let (count, _) = pool.read(leaf).leaf_parts(&mut keys);
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
