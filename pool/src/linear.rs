//! Linear-hash bucket heads stored in a segmented buffer.
//!
//! The directory grows or shrinks 64 buckets at a time. The byte buffer itself
//! allocates another 64 KiB leaf only when the directory crosses that boundary.
//! Items and their chain links remain owned by the caller.
use super::{ChunkPool, SegmentedBuffer};
use crate::{Error, Result};

const GROUP: u64 = 64;
const MIN_BUCKETS: u64 = GROUP;
const HEAD_BYTES: usize = std::mem::size_of::<u32>();

/// A linear-hash directory of caller-owned chain heads.
///
/// The caller supplies the hash key when looking up a bucket and follows the
/// item's link to compare full keys. Keys convertible to u64 are supported,
/// including u16, u32, and u64 identifiers.
pub struct LinearHash<'a> {
    heads: SegmentedBuffer<'a>,
    buckets: u64,
    level: u32,
    split: u64,
    max_buckets: u64,
}

impl<'a> LinearHash<'a> {
    /// Make a directory able to index at least max_items items. The minimum
    /// directory is 64 buckets, even when the item pool is smaller.
    pub fn new(pool: &'a ChunkPool, max_items: u32) -> Result<Self> {
        let max_items = u64::from(max_items).max(MIN_BUCKETS);
        let max_buckets = (max_items + GROUP - 1) / GROUP * GROUP;
        let mut heads = SegmentedBuffer::with_retained_leaves(pool, 1);
        heads.append(&[0xff; (MIN_BUCKETS as usize) * HEAD_BYTES])?;
        Ok(Self { heads, buckets: MIN_BUCKETS, level: 6, split: 0, max_buckets })
    }

    /// Bucket for an integer key, using its low bits.
    #[inline]
    pub fn bucket<K: Into<u64>>(&self, key: K) -> u64 {
        let key = key.into();
        let low = key & ((1u64 << self.level) - 1);
        if low < self.split { key & ((1u64 << (self.level + 1)) - 1) } else { low }
    }

    pub fn bucket_count(&self) -> u64 {
        self.buckets
    }

    pub fn head(&self, bucket: u64) -> Result<u32> {
        if bucket >= self.buckets {
            return Err(Error::new("linear-hash bucket is out of range"));
        }
        let mut bytes = [0; HEAD_BYTES];
        self.heads.read_at(bucket * HEAD_BYTES as u64, &mut bytes)?;
        Ok(u32::from_ne_bytes(bytes))
    }

    pub fn set_head(&mut self, bucket: u64, index: u32) -> Result<()> {
        if bucket >= self.buckets {
            return Err(Error::new("linear-hash bucket is out of range"));
        }
        self.heads.write_at(bucket * HEAD_BYTES as u64, &index.to_ne_bytes())
    }

    /// Split 64 buckets, letting the caller repartition each old chain by bit.
    /// The callback returns the new heads for the old and new buckets.
    pub fn grow_by_64(
        &mut self,
        mut split_chain: impl FnMut(u64, u64, u64, u32) -> (u32, u32),
    ) -> Result<()> {
        if self.buckets + GROUP > self.max_buckets {
            return Err(Error::new("linear-hash directory reached its item limit"));
        }
        self.heads.append(&[0xff; (GROUP as usize) * HEAD_BYTES])?;
        for _ in 0..GROUP {
            let old = self.split;
            let new = self.buckets;
            let bit = 1u64 << self.level;
            let head = self.head(old)?;
            let (old_head, new_head) = split_chain(old, new, bit, head);
            self.buckets += 1;
            self.set_head(old, old_head)?;
            self.set_head(new, new_head)?;
            self.split += 1;
            if self.split == 1u64 << self.level {
                self.level += 1;
                self.split = 0;
            }
        }
        Ok(())
    }

    /// Merge the latest 64 split buckets into their partners, then trim the
    /// logical byte range. Whole 64 KiB leaves are released only at boundaries.
    pub fn shrink_by_64(
        &mut self,
        mut merge_chain: impl FnMut(u64, u64, u32, u32) -> u32,
    ) -> Result<()> {
        if self.buckets < MIN_BUCKETS + GROUP {
            return Err(Error::new("linear-hash directory is at its minimum size"));
        }
        for _ in 0..GROUP {
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
        }
        self.heads.truncate(self.buckets * HEAD_BYTES as u64)
    }

    /// Return to the initial directory while retaining its first leaf.
    pub fn reset(&mut self) -> Result<()> {
        self.heads.reset();
        self.heads.append(&[0xff; (MIN_BUCKETS as usize) * HEAD_BYTES])?;
        self.buckets = MIN_BUCKETS;
        self.level = 6;
        self.split = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grow_shrink() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::new(&pool, 200).unwrap();
        assert_eq!(hash.bucket_count(), 64);

        // The low six bits select the initial buckets. The API accepts the
        // common integer widths used for identifiers.
        assert_eq!(hash.bucket(0x41u16), 1);
        assert_eq!(hash.bucket(0x82u32), 2);
        assert_eq!(hash.bucket(0x1_0000_0003u64), 3);
        assert_eq!(hash.head(63).unwrap(), u32::MAX);

        hash.grow_by_64(|_, _, _, head| (head, u32::MAX)).unwrap();
        assert_eq!(hash.bucket_count(), 128);
        assert_eq!(hash.bucket(64u32), 64);
        assert_eq!(hash.bucket(5u32), 5);

        hash.grow_by_64(|_, _, _, head| (head, u32::MAX)).unwrap();
        assert_eq!(hash.bucket_count(), 192);
        assert_eq!(hash.bucket(128u32), 128);

        hash.grow_by_64(|_, _, _, head| (head, u32::MAX)).unwrap();
        assert_eq!(hash.bucket_count(), 256);
        assert_eq!(hash.bucket(192u32), 192);

        hash.shrink_by_64(|_, _, keep, _| keep).unwrap();
        assert_eq!(hash.bucket_count(), 192);
        assert_eq!(hash.bucket(192u32), 64);
        hash.shrink_by_64(|_, _, keep, _| keep).unwrap();
        assert_eq!(hash.bucket_count(), 128);
        hash.shrink_by_64(|_, _, keep, _| keep).unwrap();
        assert_eq!(hash.bucket_count(), 64);
        assert_eq!(hash.bucket(192u32), 0);
        assert!(hash.shrink_by_64(|_, _, keep, _| keep).is_err());
    }

    #[test]
    fn capacity_reset() {
        let pool = ChunkPool::new(8, 1).unwrap();
        let mut hash = LinearHash::new(&pool, 65).unwrap();
        hash.grow_by_64(|_, _, _, head| (head, u32::MAX)).unwrap();
        assert_eq!(hash.bucket_count(), 128);
        assert!(hash.grow_by_64(|_, _, _, head| (head, u32::MAX)).is_err());
        hash.reset().unwrap();
        assert_eq!(hash.bucket_count(), 64);
        assert_eq!(hash.head(0).unwrap(), u32::MAX);
    }
}
