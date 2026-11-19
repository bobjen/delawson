//! Per-EN extent index used by SN for sync.
//!
//! Each EN's root is inline in its SN-side row. The root starts as a packed
//! leaf, so small trees need no node-pool allocation. At greater heights it is
//! an inline branch. Height zero means leaf; branches and the root terminate
//! their child arrays with `NONE` instead of storing child counts. Leaf items
//! share their high-order prefix and store only packed low-order bits.

use common::ids::ExtentId;
use common::{Error, Result};
use pool::{LockedItem, LockedPool, PoolItem};
use std::mem::{align_of, offset_of, size_of};

const NONE: u32 = u32::MAX;
// The tuning knob: sixteen u32 slots initially make a 64-byte pooled node.
const NODE_SLOTS: usize = 16;
const NODE_BYTES: usize = NODE_SLOTS * size_of::<u32>();
const NODE_LEAF_BITS: usize = (NODE_SLOTS - 3) * 32;
const ROOT_LEAF_BITS: usize = NODE_SLOTS * 32;
const KEY_SCRATCH: usize = ROOT_LEAF_BITS + 2;
const MERGE_KEY_SCRATCH: usize = NODE_LEAF_BITS * 2 + 2;
// Branch words alternate child slots and separator slots. The next child slot
// is the NONE terminator, leaving the final slot unused at this initial size.
const BRANCH_MAX: usize = (NODE_SLOTS - 4) / 2;
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

    fn branch(keys: &[u32], children: &[u32]) -> Self {
        debug_assert!(keys.len() <= BRANCH_MAX);
        debug_assert_eq!(children.len(), keys.len() + 1);
        let mut node = Self::blank();
        write_branch(&mut node.words, keys, children);
        node
    }

    fn leaf(keys: &[u32], next: u32) -> Self {
        debug_assert!(!keys.is_empty());
        debug_assert!(can_pack(keys, NODE_LEAF_BITS));
        let (bits, prefix) = leaf_encoding(keys);
        let mut node = Self::blank();
        node.words[0] = leaf_header(keys.len(), bits);
        node.words[1] = next;
        node.words[2] = prefix;
        node.words[3..].fill(0);
        pack_keys(keys, bits, &mut node.words[3..]);
        node
    }

    fn leaf_parts(self, keys: &mut KeyScratch) -> (usize, u8, u32, u32) {
        let count = (self.words[0] & 0xffff) as usize;
        let bits = ((self.words[0] >> 16) & 0x3f) as u8;
        let prefix = self.words[2];
        debug_assert!(count <= NODE_LEAF_BITS);
        debug_assert!(count * bits as usize <= NODE_LEAF_BITS);
        unpack_keys(&self.words[3..], count, bits, prefix, keys);
        (count, bits, prefix, self.words[1])
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
    root_leaf_bits: u8,
    _reserved: u8,
    root_leaf_prefix: u32,
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
            root_leaf_bits: 0,
            _reserved: 0,
            root_leaf_prefix: 0,
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
        let (count, _, _, _) = pool.read(leaf).leaf_parts(&mut keys);
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
            if can_pack(&keys[..count + 1], ROOT_LEAF_BITS) {
                self.set_root_leaf(&keys[..count + 1]);
            } else {
                let split = choose_leaf_split(&keys[..count + 1], NODE_LEAF_BITS);
                let left_index = reserved.pop().expect("root leaf split was pre-reserved");
                let right_index = reserved.pop().expect("root leaf split was pre-reserved");
                pool.write(left_index, Node::leaf(&keys[..split], right_index));
                pool.write(right_index, Node::leaf(&keys[split..count + 1], NONE));
                let root_keys = [keys[split]];
                let root_children = [left_index, right_index];
                self.write_root_branch(1, &root_keys, &root_children);
            }
            debug_assert!(reserved.is_empty());
            self.len += 1;
            return Ok(true);
        }

        let (mut root_keys, mut root_children, mut root_count) = self.root_branch_parts();
        let child_at = branch_child(&root_keys, root_count, key);
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
                    Node::branch(&root_keys[..split_at], &root_children[..=split_at]),
                );
                pool.write(
                    right_index,
                    Node::branch(
                        &root_keys[split_at + 1..root_count],
                        &root_children[split_at + 1..=root_count],
                    ),
                );
                let new_height = self.height + 1;
                let new_keys = [root_keys[split_at]];
                let new_children = [left_index, right_index];
                self.write_root_branch(new_height, &new_keys, &new_children);
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
        let (target_count, _, _, _) = target_node.leaf_parts(&mut target_keys);
        let target_at = lower_bound(&target_keys[..target_count], key);
        if target_at == target_count || target_keys[target_at] != key {
            return Ok(false);
        }
        let predecessor = if target_count == 1 { self.predecessor_leaf(pool, key) } else { None };

        let (mut root_keys, mut root_children, mut root_count) = self.root_branch_parts();
        let child_at = branch_child(&root_keys, root_count, key);
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
                remove_branch_child(&mut root_keys, &mut root_children, &mut root_count, child_at);
                if child_at == 0 {
                    root_keys[0] = subtree_min(pool, root_children[0], self.height - 1);
                }
            }
        } else if child_at > 0 {
            if let Some(minimum) = result.min_changed {
                root_keys[child_at - 1] = minimum;
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
            let (count, _, _, next) = node.leaf_parts(&mut keys);
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
            return Ok(Some(if can_pack(&keys[..count + 1], ROOT_LEAF_BITS) { 0 } else { 2 }));
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
        let (count, _, _, _) = node.leaf_parts(&mut keys);
        let at = lower_bound(&keys[..count], key);
        if at < count && keys[at] == key {
            return Ok(None);
        }
        insert_key(&mut keys, count, at, key);
        if can_pack(&keys[..count + 1], NODE_LEAF_BITS) {
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
        debug_assert!(count * self.root_leaf_bits as usize <= ROOT_LEAF_BITS);
        unpack_keys(&self.root_words, count, self.root_leaf_bits, self.root_leaf_prefix, keys);
        count
    }

    fn set_root_leaf(&mut self, keys: &[u32]) {
        debug_assert!(can_pack(keys, ROOT_LEAF_BITS));
        self.height = 0;
        self.root_words = [0; NODE_SLOTS];
        if keys.is_empty() {
            self.root_leaf_count = 0;
            self.root_leaf_bits = 0;
            self.root_leaf_prefix = 0;
            return;
        }
        let (bits, prefix) = leaf_encoding(keys);
        self.root_leaf_count = keys.len() as u16;
        self.root_leaf_bits = bits;
        self.root_leaf_prefix = prefix;
        pack_keys(keys, bits, &mut self.root_words);
    }

    fn root_branch_parts(&self) -> (BranchKeys, BranchChildren, usize) {
        debug_assert!(self.height > 0);
        branch_parts(&self.root_words)
    }

    fn write_root_branch(&mut self, height: u8, keys: &[u32], children: &[u32]) {
        debug_assert!(height > 0);
        debug_assert!(keys.len() <= BRANCH_MAX);
        debug_assert_eq!(children.len(), keys.len() + 1);
        self.height = height;
        self.root_leaf_count = 0;
        self.root_leaf_bits = 0;
        self.root_leaf_prefix = 0;
        self.root_words = [NONE; NODE_SLOTS];
        write_branch(&mut self.root_words, keys, children);
    }

    fn reset_root(&mut self) {
        self.height = 0;
        self.root_leaf_count = 0;
        self.root_leaf_bits = 0;
        self.root_leaf_prefix = 0;
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
            index = pool.read(index).words[0];
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
                let (child_count, _, _, _) = node.leaf_parts(&mut child_keys);
                self.set_root_leaf(&child_keys[..child_count]);
            } else {
                self.height -= 1;
                self.root_leaf_count = 0;
                self.root_leaf_bits = 0;
                self.root_leaf_prefix = 0;
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
        let (count, _, _, next) = node.leaf_parts(&mut keys);
        let at = lower_bound(&keys[..count], key);
        debug_assert!(at == count || keys[at] != key);
        insert_key(&mut keys, count, at, key);
        if can_pack(&keys[..count + 1], NODE_LEAF_BITS) {
            pool.write(index, Node::leaf(&keys[..count + 1], next));
            return None;
        }
        let split = choose_leaf_split(&keys[..count + 1], NODE_LEAF_BITS);
        let right_index = reserved.pop().expect("leaf split was pre-reserved");
        pool.write(right_index, Node::leaf(&keys[split..count + 1], next));
        pool.write(index, Node::leaf(&keys[..split], right_index));
        return Some((keys[split], right_index));
    }

    let (mut keys, mut children, mut count) = node.branch_parts();
    let child_at = branch_child(&keys, count, key);
    if let Some((separator, right)) =
        insert_subtree(pool, children[child_at], height - 1, key, reserved)
    {
        insert_branch_entry(&mut keys, &mut children, &mut count, child_at, separator, right);
        if count <= BRANCH_MAX {
            pool.write(index, Node::branch(&keys[..count], &children[..=count]));
            return None;
        }
        let split_at = count / 2;
        let right_index = reserved.pop().expect("branch split was pre-reserved");
        let promoted = keys[split_at];
        pool.write(
            right_index,
            Node::branch(&keys[split_at + 1..count], &children[split_at + 1..=count]),
        );
        pool.write(index, Node::branch(&keys[..split_at], &children[..=split_at]));
        return Some((promoted, right_index));
    }
    None
}

fn delete_subtree(pool: &mut ExtentTreePool, index: u32, height: u8, key: u32) -> DeleteResult {
    let node = pool.read(index);
    if height == 0 {
        let mut keys = [0; KEY_SCRATCH];
        let (count, _, _, next) = node.leaf_parts(&mut keys);
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
        remove_branch_child(&mut keys, &mut children, &mut count, child_at);
        let min_changed =
            if child_at == 0 { Some(subtree_min(pool, children[0], height - 1)) } else { None };
        pool.write(index, Node::branch(&keys[..count], &children[..=count]));
        return DeleteResult {
            removed: true,
            empty: false,
            min_changed,
            empty_leaf_next: child.empty_leaf_next,
        };
    }

    let mut min_changed = if child_at == 0 {
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
    pool.write(index, Node::branch(&keys[..count], &children[..=count]));
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
    let (left_count, _, _, _) = left.leaf_parts(&mut left_keys);
    let (right_count, _, _, right_next) = right.leaf_parts(&mut right_keys);
    debug_assert!(left_count > 0 && right_count > 0);
    debug_assert!(left_keys[left_count - 1] < right_keys[0]);

    let count = left_count + right_count;
    let mut merged = [0; MERGE_KEY_SCRATCH];
    merged[..left_count].copy_from_slice(&left_keys[..left_count]);
    merged[left_count..count].copy_from_slice(&right_keys[..right_count]);
    if !can_pack(&merged[..count], NODE_LEAF_BITS) {
        return false;
    }
    pool.write(left_index, Node::leaf(&merged[..count], right_next));
    true
}

fn branch_parts(words: &[u32; NODE_SLOTS]) -> (BranchKeys, BranchChildren, usize) {
    let mut keys = [NONE; BRANCH_KEYS_SCRATCH];
    let mut children = [NONE; BRANCH_CHILDREN_SCRATCH];
    if words[0] == NONE {
        return (keys, children, 0);
    }
    children[0] = words[0];
    let mut count = 0;
    loop {
        let child_word = 2 * (count + 1);
        if child_word >= NODE_SLOTS || words[child_word] == NONE {
            break;
        }
        debug_assert!(count < BRANCH_KEYS_SCRATCH);
        keys[count] = words[2 * count + 1];
        children[count + 1] = words[child_word];
        count += 1;
    }
    (keys, children, count)
}

fn write_branch(words: &mut [u32; NODE_SLOTS], keys: &[u32], children: &[u32]) {
    debug_assert!(keys.len() <= BRANCH_MAX);
    debug_assert_eq!(children.len(), keys.len() + 1);
    *words = [NONE; NODE_SLOTS];
    if children.is_empty() {
        return;
    }
    words[0] = children[0];
    for (i, &key) in keys.iter().enumerate() {
        words[2 * i + 1] = key;
        words[2 * i + 2] = children[i + 1];
    }
    // The next even word remains NONE and terminates the child list.
}

fn root_branch_parts(tree: &EnExtentTree) -> (BranchKeys, BranchChildren, usize) {
    debug_assert!(tree.height > 0);
    branch_parts(&tree.root_words)
}

fn write_root_branch(tree: &mut EnExtentTree, height: u8, keys: &[u32], children: &[u32]) {
    debug_assert!(height > 0);
    tree.height = height;
    tree.root_leaf_count = 0;
    tree.root_leaf_bits = 0;
    tree.root_leaf_prefix = 0;
    write_branch(&mut tree.root_words, keys, children);
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

fn subtree_min(pool: &ExtentTreePool, mut index: u32, mut height: u8) -> u32 {
    while height > 0 {
        index = pool.read(index).words[0];
        height -= 1;
    }
    let node = pool.read(index);
    let mut keys = [0; KEY_SCRATCH];
    let (count, _, _, _) = node.leaf_parts(&mut keys);
    debug_assert!(count > 0);
    keys[0]
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

fn leaf_header(count: usize, bits: u8) -> u32 {
    debug_assert!(count <= u16::MAX as usize);
    count as u32 | ((bits as u32) << 16)
}

fn leaf_encoding(keys: &[u32]) -> (u8, u32) {
    if keys.is_empty() {
        return (0, 0);
    }
    let bits = (u32::BITS - (keys[0] ^ keys[keys.len() - 1]).leading_zeros()) as u8;
    let prefix = keys[0] & !low_mask(bits);
    (bits, prefix)
}

fn encoded_bits(keys: &[u32]) -> usize {
    let (bits, _) = leaf_encoding(keys);
    bits as usize * keys.len()
}

fn can_pack(keys: &[u32], capacity_bits: usize) -> bool {
    keys.len() <= u16::MAX as usize && encoded_bits(keys) <= capacity_bits
}

fn choose_leaf_split(keys: &[u32], capacity_bits: usize) -> usize {
    debug_assert!(keys.len() >= 2);
    let mut best: Option<(usize, usize, usize)> = None;
    for split in 1..keys.len() {
        let left = &keys[..split];
        let right = &keys[split..];
        let left_bits = encoded_bits(left);
        let right_bits = encoded_bits(right);
        if left_bits > capacity_bits || right_bits > capacity_bits {
            continue;
        }
        let score = (left_bits.max(right_bits), left.len().abs_diff(right.len()), split);
        if best.is_none_or(|old| score < old) {
            best = Some(score);
        }
    }
    best.map(|(_, _, split)| split).unwrap_or_else(|| {
        // Inserting one key into a previously representable leaf always leaves
        // a representable split: isolate the new key or split the old set.
        debug_assert!(false, "no representable split for packed leaf");
        keys.len() / 2
    })
}

fn low_mask(bits: u8) -> u32 {
    match bits {
        0 => 0,
        32.. => u32::MAX,
        _ => (1u32 << bits) - 1,
    }
}

fn pack_keys(keys: &[u32], bits: u8, payload: &mut [u32]) {
    payload.fill(0);
    if bits == 0 {
        return;
    }
    let mask = low_mask(bits);
    for (i, &key) in keys.iter().enumerate() {
        let bit_at = i * bits as usize;
        let word_at = bit_at / 32;
        let shift = bit_at % 32;
        let value = (key & mask) as u64;
        payload[word_at] |= (value << shift) as u32;
        if shift + bits as usize > 32 {
            payload[word_at + 1] |= (value >> (32 - shift)) as u32;
        }
    }
}

fn unpack_keys(payload: &[u32], count: usize, bits: u8, prefix: u32, output: &mut [u32]) {
    debug_assert!(count <= output.len());
    if bits == 0 {
        output[..count].fill(prefix);
        return;
    }
    let mask = low_mask(bits);
    for (i, slot) in output[..count].iter_mut().enumerate() {
        let bit_at = i * bits as usize;
        let word_at = bit_at / 32;
        let shift = bit_at % 32;
        let mut value = (payload[word_at] as u64) >> shift;
        if shift + bits as usize > 32 {
            value |= (payload[word_at + 1] as u64) << (32 - shift);
        }
        *slot = prefix | (value as u32 & mask);
    }
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
    use std::collections::BTreeSet;

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
    fn inline_root_packs_shared_prefix_then_splits_and_collapses() {
        let mut pool = ExtentTreePool::new(256).unwrap();
        let initial_nodes = pool.slots.issued_blocks();
        let mut tree = EnExtentTree::new();
        for value in 0..60 {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(pool.slots.issued_blocks(), initial_nodes);
        assert_eq!(collect(&tree, &pool), (0..60).collect::<Vec<_>>());

        assert!(tree.insert(&mut pool, ExtentId::new(u32::MAX - 1)).unwrap());
        assert_eq!(tree.height, 1);
        assert_eq!(pool.slots.issued_blocks(), initial_nodes + 1);
        let mut expected: Vec<u32> = (0..60).collect();
        expected.push(u32::MAX - 1);
        assert_eq!(collect(&tree, &pool), expected);

        assert!(tree.remove(&mut pool, ExtentId::new(u32::MAX - 1)).unwrap());
        assert_eq!(tree.height, 0);
        assert_eq!(tree.root_leaf_bits, 6);
        for value in 1..60 {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 0);
        assert_eq!(collect(&tree, &pool), vec![0]);
        assert!(tree.remove(&mut pool, ExtentId::new(0)).unwrap());
        assert!(tree.is_empty());
        pool.reclaim();
    }

    #[test]
    fn leaf_merge_recomputes_prefix_and_checks_capacity() {
        let mut pool = ExtentTreePool::new(64).unwrap();
        let mut tree = EnExtentTree::new();
        let mut values: Vec<u32> = (0..16).map(|value| value << 28).collect();
        values.push(0xf100_0000);
        for &value in &values {
            assert!(tree.insert(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 1);

        // The two leaves each fit at 32 bits per ID, but their combined
        // high-bit range still needs 32 bits per ID and does not fit yet.
        for &value in &values[..3] {
            assert!(tree.remove(&mut pool, ExtentId::new(value)).unwrap());
        }
        assert_eq!(tree.height, 1);

        // After another deletion, the combined 13 IDs fit exactly in one node
        // (13 * 32 bits); the prefix is recomputed across both former leaves.
        assert!(tree.remove(&mut pool, ExtentId::new(values[3])).unwrap());
        assert_eq!(tree.height, 0);
        assert_eq!(tree.root_leaf_bits, 32);
        assert_eq!(collect(&tree, &pool), values[4..].to_vec());
    }

    #[test]
    fn inserts_and_deletes_preserve_order_for_packed_and_wide_keys() {
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
