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
// The tuning knob: sixteen u32 slots initially make a 64-byte pooled node.
const NODE_SLOTS: usize = 16;
const NODE_BYTES: usize = NODE_SLOTS * size_of::<u32>();
const NODE_LEAF_CAPACITY: usize = NODE_SLOTS - 2;
const ROOT_LEAF_CAPACITY: usize = NODE_SLOTS;
const KEY_SCRATCH: usize = ROOT_LEAF_CAPACITY + 2;
const MERGE_KEY_SCRATCH: usize = NODE_LEAF_CAPACITY * 2 + 2;
// Branch nodes store one (child minimum, child link) pair per child. The first
// minimum is stored too; a NONE child link terminates a nonfull node.
const BRANCH_MAX: usize = NODE_SLOTS / 2 - 1;
const BRANCH_KEYS_SCRATCH: usize = BRANCH_MAX + 2;
const BRANCH_CHILDREN_SCRATCH: usize = BRANCH_MAX + 3;

const fn node_alignment(bytes: usize) -> usize {
    let mut alignment = 64;
    while alignment > 4 && bytes % alignment != 0 {
        alignment /= 2;
    }
    alignment
}

const NODE_ALIGNMENT: usize = node_alignment(NODE_BYTES);
const _: () = assert!(NODE_SLOTS >= 8);
const _: () = assert!(BRANCH_MAX > 0);
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

    fn branch(first_min: u32, keys: &[u32], children: &[u32]) -> Self {
        debug_assert!(keys.len() <= BRANCH_MAX);
        debug_assert_eq!(children.len(), keys.len() + 1);
        debug_assert_ne!(first_min, NONE);
        let mut node = Self::blank();
        write_branch(&mut node.words, first_min, keys, children);
        node
    }

    fn leaf(keys: &[u32], next: u32) -> Self {
        debug_assert!(!keys.is_empty());
        debug_assert!(keys.len() <= NODE_LEAF_CAPACITY);
        let mut node = Self::blank();
        node.words[0] = keys.len() as u32;
        node.words[1] = next;
        node.words[2..2 + keys.len()].copy_from_slice(keys);
        node
    }

    fn leaf_parts(self, keys: &mut KeyScratch) -> (usize, u32) {
        let count = self.words[0] as usize;
        debug_assert!(count <= NODE_LEAF_CAPACITY);
        keys[..count].copy_from_slice(&self.words[2..2 + count]);
        (count, self.words[1])
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
    root_leaf_count: u16,
    _reserved: [u8; 6],
    root_words: [u32; NODE_SLOTS],
}

impl Default for EnExtentTree {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
struct DeleteResult {
    removed: bool,
    empty: bool,
    min_changed: Option<u32>,
    empty_leaf_next: Option<u32>,
}

impl EnExtentTree {
    pub(super) fn new() -> Self {
        Self {
            height: 0,
            len: 0,
            root_leaf_count: 0,
            _reserved: [0; 6],
            root_words: [0; NODE_SLOTS],
        }
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
            self.root_leaf_parts(&mut keys);
            return keys[..self.root_leaf_count as usize].binary_search(&key).is_ok();
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
                let root_keys = [keys[split]];
                let root_children = [left_index, right_index];
                self.write_root_branch(keys[0], 1, &root_keys, &root_children);
            }
            debug_assert!(reserved.is_empty());
            self.len += 1;
            return Ok(true);
        }

        let (mut root_keys, mut root_children, mut root_count) = self.root_branch_parts();
        let child_at = branch_child(&root_keys, root_count, key);
        let mut root_first_min = self.root_words[0];
        if child_at == 0 && key < root_first_min {
            root_first_min = key;
            self.root_words[0] = key;
        }
        if let Some((separator, right)) =
            insert_subtree(pool, root_children[child_at], self.height - 1, key, &mut reserved)
        {
            insert_branch_entry(
                &mut root_keys,
                &mut root_children,
                &mut root_count,
                child_at,
                separator,
                right,
            );
            if root_count <= BRANCH_MAX {
                self.write_root_branch(
                    root_first_min,
                    self.height,
                    &root_keys[..root_count],
                    &root_children[..=root_count],
                );
            } else {
                let split_at = root_count / 2;
                let left_index = reserved.pop().expect("root branch split was pre-reserved");
                let right_index = reserved.pop().expect("root branch split was pre-reserved");
                pool.write(
                    left_index,
                    Node::branch(
                        root_first_min,
                        &root_keys[..split_at],
                        &root_children[..=split_at],
                    ),
                );
                pool.write(
                    right_index,
                    Node::branch(
                        root_keys[split_at],
                        &root_keys[split_at + 1..root_count],
                        &root_children[split_at + 1..=root_count],
                    ),
                );
                let new_height = self.height + 1;
                let new_keys = [root_keys[split_at]];
                let new_children = [left_index, right_index];
                self.write_root_branch(root_first_min, new_height, &new_keys, &new_children);
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

        // A predecessor is needed only if this is the leaf's last key. Find it
        // before mutation so removing branches cannot obscure the old leaf path.
        let target = self.find_leaf(pool, key);
        let target_node = pool.read(target);
        let mut target_keys = [0; KEY_SCRATCH];
        let (target_count, _) = target_node.leaf_parts(&mut target_keys);
        let target_at = lower_bound(&target_keys[..target_count], key);
        if target_at == target_count || target_keys[target_at] != key {
            return Ok(false);
        }
        let predecessor = if target_count == 1 { self.predecessor_leaf(pool, key) } else { None };

        let (mut root_keys, mut root_children, mut root_count) = self.root_branch_parts();
        let child_at = branch_child(&root_keys, root_count, key);
        let mut root_first_min = self.root_words[0];
        let child = root_children[child_at];
        let result = delete_subtree(pool, child, self.height - 1, key);
        debug_assert!(result.removed);

        // Relink the leaf chain before a merge can retire either neighbor.
        if let (Some(previous), Some(next)) = (predecessor, result.empty_leaf_next) {
            let mut node = pool.read(previous);
            node.words[1] = next;
            pool.write(previous, node);
        }

        if result.empty {
            pool.retire(child);
            if root_count == 0 {
                self.reset_root();
            } else {
                if child_at == 0 {
                    root_first_min = root_keys[0];
                }
                remove_branch_child(&mut root_keys, &mut root_children, &mut root_count, child_at);
            }
        } else {
            if let Some(minimum) = result.min_changed {
                if child_at == 0 {
                    root_first_min = minimum;
                } else {
                    root_keys[child_at - 1] = minimum;
                }
            }
        }

        if self.height == 1 && root_count > 0 {
            let merge_at = child_at.min(root_count);
            merge_leaf_children(
                pool,
                &mut root_keys,
                &mut root_children,
                &mut root_count,
                merge_at,
            );
        }
        if self.height > 0 {
            self.write_root_branch(
                root_first_min,
                self.height,
                &root_keys[..root_count],
                &root_children[..=root_count],
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
        let mut child_at = branch_child(&root_keys, root_count, key);
        let mut index = root_children[child_at];
        let mut level = self.height;
        let mut pooled_branch_counts = Vec::new();
        while level > 1 {
            let node = pool.read(index);
            let (keys, children, count) = node.branch_parts();
            pooled_branch_counts.push(count);
            child_at = branch_child(&keys, count, key);
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
            if split_carries && branch_count == BRANCH_MAX {
                needed += 1; // full pooled branch splits and carries upward
            } else {
                split_carries = false;
                break;
            }
        }
        if split_carries && root_count == BRANCH_MAX {
            if self.height == u8::MAX {
                return Err(Error::new("EN extent-tree height overflow"));
            }
            needed += 2; // the inline root becomes two pooled branches
        }
        Ok(Some(needed))
    }

    fn root_leaf_parts(&self, keys: &mut KeyScratch) -> usize {
        let count = self.root_leaf_count as usize;
        debug_assert!(self.height == 0);
        debug_assert!(count <= ROOT_LEAF_CAPACITY);
        keys[..count].copy_from_slice(&self.root_words[..count]);
        count
    }

    fn set_root_leaf(&mut self, keys: &[u32]) {
        debug_assert!(keys.len() <= ROOT_LEAF_CAPACITY);
        self.height = 0;
        self.root_words = [0; NODE_SLOTS];
        self.root_leaf_count = keys.len() as u16;
        self.root_words[..keys.len()].copy_from_slice(keys);
    }

    fn root_branch_parts(&self) -> (BranchKeys, BranchChildren, usize) {
        debug_assert!(self.height > 0);
        branch_parts(&self.root_words)
    }

    fn write_root_branch(&mut self, first_min: u32, height: u8, keys: &[u32], children: &[u32]) {
        debug_assert!(height > 0);
        debug_assert!(keys.len() <= BRANCH_MAX);
        debug_assert_eq!(children.len(), keys.len() + 1);
        self.height = height;
        self.root_leaf_count = 0;
        self._reserved = [0; 6];
        self.root_words = [NONE; NODE_SLOTS];
        write_branch(&mut self.root_words, first_min, keys, children);
    }

    fn reset_root(&mut self) {
        self.height = 0;
        self.root_leaf_count = 0;
        self._reserved = [0; 6];
        self.root_words = [0; NODE_SLOTS];
    }

    fn find_leaf(&self, pool: &ExtentTreePool, key: u32) -> u32 {
        let (root_keys, root_children, root_count) = self.root_branch_parts();
        let mut index = root_children[branch_child(&root_keys, root_count, key)];
        let mut level = self.height - 1;
        while level > 0 {
            let node = pool.read(index);
            let (keys, children, count) = node.branch_parts();
            index = children[branch_child(&keys, count, key)];
            level -= 1;
        }
        index
    }

    fn leftmost_leaf(&self, pool: &ExtentTreePool) -> u32 {
        let (_, children, _) = self.root_branch_parts();
        let mut index = children[0];
        let mut level = self.height - 1;
        while level > 0 {
            let (_, children, _) = pool.read(index).branch_parts();
            index = children[0];
            level -= 1;
        }
        index
    }

    fn predecessor_leaf(&self, pool: &ExtentTreePool, key: u32) -> Option<u32> {
        if self.height == 0 {
            return None;
        }
        let (keys, children, count) = self.root_branch_parts();
        let mut child_at = branch_child(&keys, count, key);
        let mut predecessor = if child_at == 0 {
            None
        } else {
            Some(rightmost_leaf(pool, children[child_at - 1], self.height - 1))
        };
        let mut index = children[child_at];
        let mut level = self.height - 1;
        while level > 0 {
            let node = pool.read(index);
            let (keys, children, count) = node.branch_parts();
            child_at = branch_child(&keys, count, key);
            if child_at > 0 {
                predecessor = Some(rightmost_leaf(pool, children[child_at - 1], level - 1));
            }
            index = children[child_at];
            level -= 1;
        }
        predecessor
    }

    fn collapse_root(&mut self, pool: &mut ExtentTreePool) {
        while self.height > 0 {
            let (keys, children, count) = self.root_branch_parts();
            if count != 0 {
                return;
            }
            let child = children[0];
            let node = pool.read(child);
            if self.height == 1 {
                let mut child_keys = [0; KEY_SCRATCH];
                let (child_count, _) = node.leaf_parts(&mut child_keys);
                self.set_root_leaf(&child_keys[..child_count]);
            } else {
                self.height -= 1;
                self.root_leaf_count = 0;
                self._reserved = [0; 6];
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
    let mut node = pool.read(index);
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
    let mut first_min = node.words[0];
    let child_at = branch_child(&keys, count, key);
    if child_at == 0 && key < node.words[0] {
        first_min = key;
        node.words[0] = first_min;
        pool.write(index, node);
    }
    if let Some((separator, right)) =
        insert_subtree(pool, children[child_at], height - 1, key, reserved)
    {
        insert_branch_entry(&mut keys, &mut children, &mut count, child_at, separator, right);
        if count <= BRANCH_MAX {
            pool.write(index, Node::branch(first_min, &keys[..count], &children[..=count]));
            return None;
        }
        let split_at = count / 2;
        let right_index = reserved.pop().expect("branch split was pre-reserved");
        let promoted = keys[split_at];
        pool.write(
            right_index,
            Node::branch(promoted, &keys[split_at + 1..count], &children[split_at + 1..=count]),
        );
        pool.write(index, Node::branch(first_min, &keys[..split_at], &children[..=split_at]));
        return Some((promoted, right_index));
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
            return DeleteResult {
                removed: false,
                empty: false,
                min_changed: None,
                empty_leaf_next: None,
            };
        }
        remove_key(&mut keys, count, at);
        if count == 1 {
            return DeleteResult {
                removed: true,
                empty: true,
                min_changed: None,
                empty_leaf_next: Some(next),
            };
        }
        let new_count = count - 1;
        let min_changed = if at == 0 { Some(keys[0]) } else { None };
        pool.write(index, Node::leaf(&keys[..new_count], next));
        return DeleteResult { removed: true, empty: false, min_changed, empty_leaf_next: None };
    }

    let (mut keys, mut children, mut count) = node.branch_parts();
    let mut first_min = node.words[0];
    let child_at = branch_child(&keys, count, key);
    let child = delete_subtree(pool, children[child_at], height - 1, key);
    if !child.removed {
        return child;
    }
    if child.empty {
        pool.retire(children[child_at]);
        if count == 0 {
            return DeleteResult {
                removed: true,
                empty: true,
                min_changed: None,
                empty_leaf_next: child.empty_leaf_next,
            };
        }
        if child_at == 0 {
            first_min = keys[0];
        }
        remove_branch_child(&mut keys, &mut children, &mut count, child_at);
        let min_changed = if child_at == 0 { Some(first_min) } else { None };
        pool.write(index, Node::branch(first_min, &keys[..count], &children[..=count]));
        return DeleteResult {
            removed: true,
            empty: false,
            min_changed,
            empty_leaf_next: child.empty_leaf_next,
        };
    }

    let mut min_changed = if child_at == 0 {
        if let Some(minimum) = child.min_changed {
            first_min = minimum;
        }
        child.min_changed
    } else {
        if let Some(minimum) = child.min_changed {
            keys[child_at - 1] = minimum;
        }
        None
    };
    if height == 1 && count > 0 {
        if merge_leaf_children(pool, &mut keys, &mut children, &mut count, child_at)
            == Some(LeafMerge::Left)
        {
            min_changed = None;
        }
    }
    pool.write(index, Node::branch(first_min, &keys[..count], &children[..=count]));
    DeleteResult {
        removed: true,
        empty: false,
        min_changed,
        empty_leaf_next: child.empty_leaf_next,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LeafMerge {
    Left,
    Right,
}

/// Merge the selected leaf with an adjacent leaf when the combined IDs fit
/// after recomputing their common prefix. The right node is retired on success.
fn merge_leaf_children(
    pool: &mut ExtentTreePool,
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    parent_count: &mut usize,
    child_at: usize,
) -> Option<LeafMerge> {
    if child_at > 0 {
        let left = children[child_at - 1];
        let right = children[child_at];
        if merge_leaf_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, parent_count, child_at);
            return Some(LeafMerge::Left);
        }
    }
    if child_at < *parent_count {
        let left = children[child_at];
        let right = children[child_at + 1];
        if merge_leaf_nodes(pool, left, right) {
            pool.retire(right);
            remove_branch_child(keys, children, parent_count, child_at + 1);
            return Some(LeafMerge::Right);
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

fn branch_parts(words: &[u32; NODE_SLOTS]) -> (BranchKeys, BranchChildren, usize) {
    let mut keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut children = [NONE; BRANCH_CHILDREN_SCRATCH];
    if words[1] == NONE {
        return (keys, children, 0);
    }
    debug_assert_ne!(words[0], NONE);
    children[0] = words[1];
    let mut count = 0;
    for child_at in 1..NODE_SLOTS / 2 {
        let child_word = 2 * child_at + 1;
        if words[child_word] == NONE {
            break;
        }
        keys[count] = words[2 * child_at];
        children[count + 1] = words[child_word];
        count += 1;
    }
    (keys, children, count)
}

fn write_branch(words: &mut [u32; NODE_SLOTS], first_min: u32, keys: &[u32], children: &[u32]) {
    debug_assert!(keys.len() <= BRANCH_MAX);
    debug_assert_eq!(children.len(), keys.len() + 1);
    debug_assert_ne!(first_min, NONE);
    *words = [NONE; NODE_SLOTS];
    if children.is_empty() {
        return;
    }
    words[0] = first_min;
    words[1] = children[0];
    for (i, &key) in keys.iter().enumerate() {
        let child_at = i + 1;
        words[2 * child_at] = key;
        words[2 * child_at + 1] = children[child_at];
    }
}

fn root_branch_parts(tree: &EnExtentTree) -> (BranchKeys, BranchChildren, usize) {
    debug_assert!(tree.height > 0);
    branch_parts(&tree.root_words)
}

fn branch_child(keys: &BranchKeys, count: usize, key: u32) -> usize {
    let mut at = 0;
    while at < count && key >= keys[at] {
        at += 1;
    }
    at
}

fn insert_branch_entry(
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    count: &mut usize,
    child_at: usize,
    separator: u32,
    right: u32,
) {
    for i in (child_at..*count).rev() {
        keys[i + 1] = keys[i];
    }
    keys[child_at] = separator;
    for i in (child_at + 1..=*count).rev() {
        children[i + 1] = children[i];
    }
    children[child_at + 1] = right;
    *count += 1;
}

fn remove_branch_child(
    keys: &mut BranchKeys,
    children: &mut BranchChildren,
    count: &mut usize,
    child_at: usize,
) {
    debug_assert!(*count > 0);
    debug_assert!(child_at <= *count);
    if child_at == 0 {
        for i in 0..*count - 1 {
            keys[i] = keys[i + 1];
        }
    } else {
        let key_at = child_at - 1;
        for i in key_at..*count - 1 {
            keys[i] = keys[i + 1];
        }
    }
    for i in child_at..*count {
        children[i] = children[i + 1];
    }
    *count -= 1;
}

fn rightmost_leaf(pool: &ExtentTreePool, mut index: u32, mut height: u8) -> u32 {
    while height > 0 {
        let node = pool.read(index);
        let (_, children, count) = node.branch_parts();
        index = children[count];
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
    const TUNE_MIN_POOL_NODES: u32 = 1 << 16;
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
            let count = node.words[0] as usize;
            usage.leaves += 1;
            usage.leaf_id_bytes += count * size_of::<u32>();
            usage.occupied_slots += 2 + count;
            return;
        }
        let (_, children, count) = node.branch_parts();
        usage.branches += 1;
        usage.occupied_slots += count * 2 + 2; // child terminator included
        for &child in &children[..=count] {
            usage_node(pool, child, height - 1, usage);
        }
    }

    fn tree_usage(tree: &EnExtentTree, pool: &ExtentTreePool) -> TreeUsage {
        let mut usage = TreeUsage::default();
        if tree.height == 0 {
            if tree.root_leaf_count != 0 {
                usage.leaves = 1;
                usage.leaf_id_bytes = tree.root_leaf_count as usize * size_of::<u32>();
                usage.occupied_slots = tree.root_leaf_count as usize;
            }
        } else {
            let (_, children, count) = tree.root_branch_parts();
            usage.occupied_slots += count * 2 + 2;
            for &child in &children[..=count] {
                usage_node(pool, child, tree.height - 1, &mut usage);
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
        let max_nodes = ((entries as u64).div_ceil(4) as u32).max(TUNE_MIN_POOL_NODES);
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
    fn inline_root_holds_sixteen_raw_ids_then_rebalances() {
        let mut pool = ExtentTreePool::new(256).unwrap();
        let initial_nodes = pool.slots.issued_blocks();
        let mut tree = EnExtentTree::new();
        for value in 0..16 {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(pool.slots.issued_blocks(), initial_nodes);
        assert_eq!(collect(&tree, &pool), (0..16).collect::<Vec<_>>());

        assert!(tree.insert(&mut pool, ExtentId::new(u32::MAX - 1)).unwrap());
        assert_eq!(tree.height, 1);
        assert_eq!(pool.slots.issued_blocks(), initial_nodes + 1);
        let mut expected: Vec<u32> = (0..16).collect();
        expected.push(u32::MAX - 1);
        assert_eq!(collect(&tree, &pool), expected);

        assert!(tree.remove(&mut pool, ExtentId::new(u32::MAX - 1)).unwrap());
        assert_eq!(tree.height, 1);
        assert_eq!(collect(&tree, &pool), (0..16).collect::<Vec<_>>());
        for value in 1..3 {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(tree.root_leaf_count, 14);
        for value in 3..16 {
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
        let mut values: Vec<u32> = (0..16).map(|value| value << 28).collect();
        values.push(0xf100_0000);
        for &value in &values {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 1);

        // Fifteen values need two leaves; after deleting one more, fourteen
        // fit in one raw leaf and the root collapses back to an inline leaf.
        for &value in &values[..2] {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 1);

        assert!(tree.remove(&mut pool, ExtentId::new(values[2])).unwrap());
        assert_eq!(tree.height, 0);
        assert_eq!(tree.root_leaf_count, 14);
        assert_eq!(collect(&tree, &pool), values[3..].to_vec());
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
