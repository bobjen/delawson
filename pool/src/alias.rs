//! Cached contiguous aliases of shared pool leaves, exposed only in scoped callbacks.
use super::{CHUNK_BYTES, SegmentedBuffer};
use crate::{Error, Result};
use std::{ops::Range, ptr::NonNull};

/// Owned by exactly one buffer. Cached slots are checked against its live leaves
/// before exposure; dormant aliases never expose recycled blocks to callers.
pub(super) struct AliasMapping {
    base: Option<NonNull<u8>>,
    reserved: usize,
    origin: Option<u64>,
    mapped: Vec<u32>,
    #[cfg(test)]
    map_calls: usize,
    #[cfg(test)]
    fail_after: Option<usize>,
}

// SAFETY: mappings have no thread affinity; access requires &mut self.
unsafe impl Send for AliasMapping {}
// SAFETY: shared access exposes no alias bytes or mapping mutation.
unsafe impl Sync for AliasMapping {}

impl AliasMapping {
    fn new() -> Self {
        Self {
            base: None,
            reserved: 0,
            origin: None,
            mapped: Vec::new(),
            #[cfg(test)]
            map_calls: 0,
            #[cfg(test)]
            fail_after: None,
        }
    }

    pub fn supported() -> bool {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        cfg!(target_os = "linux") && page > 0 && CHUNK_BYTES % page as usize == 0
    }

    fn reserve(&mut self, needed: usize) -> Result<NonNull<u8>> {
        if needed <= self.reserved {
            if let Some(base) = self.base {
                return Ok(base);
            }
        }
        let len = needed
            .max(64 * 1024 * 1024)
            .checked_next_power_of_two()
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or_else(|| Error::new("virtual alias too large"))?;
        self.release();
        // SAFETY: creates an owned, inaccessible virtual address reservation.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(os_error("reserve virtual alias"));
        }
        let Some(base) = NonNull::new(raw.cast()) else {
            unsafe {
                libc::munmap(raw, len);
            }
            return Err(Error::new("virtual alias mapped at zero"));
        };
        self.reserved = len;
        self.base = Some(base);
        Ok(base)
    }

    pub fn with_read<T>(
        &mut self,
        buffer: &SegmentedBuffer<'_>,
        range: Range<u64>,
        f: impl FnOnce(&[u8]) -> T,
    ) -> Result<T> {
        let (base, offset, len) = self.map(buffer, range)?;
        // SAFETY: initialized range; buffer stays shared-borrowed. The callback
        // cannot return a reference borrowing this temporary slice.
        let bytes = unsafe { std::slice::from_raw_parts(base.as_ptr().add(offset), len) };
        Ok(f(bytes))
    }

    pub fn with_write<T>(
        &mut self,
        buffer: &mut SegmentedBuffer<'_>,
        range: Range<u64>,
        f: impl FnOnce(&mut [u8]) -> T,
    ) -> Result<T> {
        let (base, offset, len) = self.map(buffer, range)?;
        // SAFETY: distinct leaves and initialized range. The exclusive buffer
        // borrow prevents access through its original mapping during this call.
        let bytes = unsafe { std::slice::from_raw_parts_mut(base.as_ptr().add(offset), len) };
        Ok(f(bytes))
    }

    fn map(
        &mut self,
        buffer: &SegmentedBuffer<'_>,
        range: Range<u64>,
    ) -> Result<(NonNull<u8>, usize, usize)> {
        if range.start > range.end || range.start < buffer.start() || range.end > buffer.end() {
            return Err(Error::new("virtual alias range outside buffer"));
        }
        let len = usize::try_from(range.end - range.start)
            .map_err(|_| Error::new("virtual alias range too large"))?;
        if !Self::supported() {
            return Err(Error::new("unsupported virtual alias range or page size"));
        }
        let origin = buffer.start() / CHUNK_BYTES as u64;
        let offset = usize::try_from(range.start - origin * CHUNK_BYTES as u64)
            .map_err(|_| Error::new("virtual alias offset too large"))?;
        let needed =
            offset.checked_add(len).ok_or_else(|| Error::new("virtual alias range too large"))?;
        let base = self.reserve(needed)?;
        if self.origin != Some(origin) {
            self.mapped.clear();
            self.origin = Some(origin);
        }
        let pages = if len == 0 { 0 } else { needed.div_ceil(CHUNK_BYTES) };
        if pages > self.mapped.len() {
            self.mapped
                .try_reserve(pages - self.mapped.len())
                .map_err(|e| Error::new(format!("alias cache allocation: {e}")))?;
            self.mapped.resize(pages, crate::NONE);
        }
        let first = origin;
        let mut page = offset / CHUNK_BYTES;
        while page < pages {
            let index = buffer.leaf(first + page as u64);
            if self.mapped[page] == index {
                page += 1;
                continue;
            }
            let mut count = 1;
            // Coalesce adjacent changed slots, leaving matching aliases alone.
            while page + count < pages
                && buffer.leaf(first + (page + count) as u64) == index + count as u32
                && self.mapped[page + count] != index + count as u32
            {
                count += 1;
            }
            // Invalidate before a syscall can fail. A failed mapping discards
            // the whole reservation, so no uncertain mapping is reused.
            self.mapped[page..page + count].fill(crate::NONE);
            #[cfg(test)]
            {
                if self.fail_after == Some(self.map_calls) {
                    self.release();
                    return Err(Error::new("injected alias mapping failure"));
                }
                self.map_calls += 1;
            }
            #[cfg(target_os = "linux")]
            let raw = unsafe {
                // old_size=0 duplicates shared pages, preserving the original
                // mapping. FIXED replaces only our own reserved destination.
                libc::mremap(
                    buffer.ptr(index).cast(),
                    0,
                    count * CHUNK_BYTES,
                    libc::MREMAP_MAYMOVE | libc::MREMAP_FIXED,
                    base.as_ptr().add(page * CHUNK_BYTES).cast::<libc::c_void>(),
                )
            };
            #[cfg(not(target_os = "linux"))]
            let raw = libc::MAP_FAILED;
            if raw == libc::MAP_FAILED {
                let error = os_error("alias pool leaves");
                self.release();
                return Err(error);
            }
            for n in 0..count {
                self.mapped[page + n] = index + n as u32;
            }
            page += count;
        }
        Ok((base, offset, len))
    }
}

fn os_error(operation: &str) -> Error {
    Error::new(format!("{operation}: {}", std::io::Error::last_os_error()))
}
impl AliasMapping {
    fn release(&mut self) {
        if let Some(base) = self.base.take() {
            // SAFETY: only this buffer owns the complete reserved range;
            // no callback slice is live when releasing it.
            unsafe {
                libc::munmap(base.as_ptr().cast(), self.reserved);
            }
        }
        self.reserved = 0;
        self.origin = None;
        self.mapped.clear();
    }
}
impl Drop for AliasMapping {
    fn drop(&mut self) {
        self.release();
    }
}

impl SegmentedBuffer<'_> {
    pub fn alias_supported() -> bool {
        AliasMapping::supported()
    }

    /// Borrow a contiguous alias of initialized bytes. The reservation and cached
    /// mappings belong to this buffer and survive calls, resets, and moves.
    /// Exclusive access also prevents recycling or modifying leaves during use.
    /// Reserves 64MiB lazily, growing geometrically for larger views. Only the
    /// requested leaves are mapped; this allocates no additional payload blocks.
    pub fn with_contiguous_read<T>(
        &mut self,
        range: Range<u64>,
        f: impl FnOnce(&[u8]) -> T,
    ) -> Result<T> {
        let mut alias = self.alias.take().unwrap_or_else(|| Box::new(AliasMapping::new()));
        let result = alias.with_read(self, range, f);
        self.alias = Some(alias);
        result
    }

    /// Mutably borrow a contiguous alias. No callback reference can escape.
    /// If the callback panics, the reservation is dropped before unwinding.
    pub fn with_contiguous_write<T>(
        &mut self,
        range: Range<u64>,
        f: impl FnOnce(&mut [u8]) -> T,
    ) -> Result<T> {
        let mut alias = self.alias.take().unwrap_or_else(|| Box::new(AliasMapping::new()));
        let result = alias.with_write(self, range, f);
        self.alias = Some(alias);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChunkPool;

    #[test]
    fn mappings_follow_buffers_and_reuse_retained_leaves() {
        if !SegmentedBuffer::alias_supported() {
            return;
        }
        let pool = ChunkPool::new(((4u64 << 30) / CHUNK_BYTES as u64) as u32, 0).unwrap();
        let mut a = SegmentedBuffer::with_retained_leaves(&pool, 4);
        let mut b = SegmentedBuffer::with_retained_leaves(&pool, 4);
        for _ in 0..4 {
            a.append(&[7; CHUNK_BYTES]).unwrap();
            b.append(&[9; CHUNK_BYTES]).unwrap();
        }
        let end = a.end();
        a.with_contiguous_read(3..end, |s| assert!(s.iter().all(|&x| x == 7))).unwrap();
        b.with_contiguous_read(3..end, |s| assert!(s.iter().all(|&x| x == 9))).unwrap();
        let base = a.alias.as_ref().unwrap().base;
        let calls = a.alias.as_ref().unwrap().map_calls;
        assert_ne!(base, b.alias.as_ref().unwrap().base);
        std::mem::swap(&mut a, &mut b);
        b.with_contiguous_write(CHUNK_BYTES as u64 + 3..end, |s| s.fill(11)).unwrap();
        assert_eq!(b.chunk(CHUNK_BYTES as u64 + 3).unwrap()[0], 11);
        b.reset();
        b.append(&vec![13; end as usize]).unwrap();
        b.with_contiguous_read(0..end, |s| assert!(s.iter().all(|&x| x == 13))).unwrap();
        assert_eq!(b.alias.as_ref().unwrap().base, base);
        assert_eq!(b.alias.as_ref().unwrap().map_calls, calls);
        // Growing maps only the new suffix; the prefix keeps its virtual address.
        b.append(&[15; CHUNK_BYTES]).unwrap();
        b.with_contiguous_read(0..b.end(), |_| ()).unwrap();
        assert_eq!(b.alias.as_ref().unwrap().map_calls, calls + 1);
        assert_eq!(b.alias.as_ref().unwrap().base, base);
        // Replace a non-retained suffix while its old alias is still cached.
        b.reset();
        b.append(&vec![17; end as usize]).unwrap();
        b.append(&[19; CHUNK_BYTES]).unwrap();
        b.with_contiguous_read(0..b.end(), |s| {
            assert!(s[..end as usize].iter().all(|&x| x == 17));
            assert!(s[end as usize..].iter().all(|&x| x == 19));
        })
        .unwrap();
    }

    #[test]
    fn rolling_ranges_errors_and_panics() {
        if !SegmentedBuffer::alias_supported() {
            return;
        }
        let pool = ChunkPool::new(((4u64 << 30) / CHUNK_BYTES as u64) as u32, 0).unwrap();
        let start = CHUNK_BYTES as u64 - 3;
        let mut b = SegmentedBuffer::with_start(&pool, start);
        let mut other = SegmentedBuffer::new(&pool);
        for n in 1..=4 {
            b.append(&[n; CHUNK_BYTES]).unwrap();
            other.append(&[99; CHUNK_BYTES]).unwrap();
        }
        let end = b.end();
        let mut expected = vec![0; b.len() as usize];
        b.read_at(start, &mut expected).unwrap();
        b.with_contiguous_read(start..end, |s| {
            assert_eq!(s, expected);
            assert_eq!(s.as_ptr() as usize % 4096, start as usize % 4096);
        })
        .unwrap();
        b.discard_before(start + CHUNK_BYTES as u64).unwrap();
        b.with_contiguous_read(b.start()..end, |s| assert_eq!(s, &expected[CHUNK_BYTES..]))
            .unwrap();
        assert!(b.with_contiguous_read(0..1, |_| ()).is_err());
        b.with_contiguous_read(end..end, |s| assert!(s.is_empty())).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            b.with_contiguous_write(b.start()..end, |s| {
                s[0] = 42;
                panic!("test unwind");
            })
            .unwrap();
        }));
        assert!(result.is_err());
        assert!(b.alias.is_none());
        b.with_contiguous_read(b.start()..end, |s| assert_eq!(s[0], 42)).unwrap();
        let calls = b.alias.as_ref().unwrap().map_calls;
        let result = b.with_contiguous_read(b.start()..end, |_| Err::<(), ()>(()));
        assert_eq!(result.unwrap(), Err(()));
        assert_eq!(b.alias.as_ref().unwrap().map_calls, calls);
        // Reservation growth discards old mappings but not their source bytes.
        b.alias.as_mut().unwrap().reserve(65 * 1024 * 1024).unwrap();
        b.with_contiguous_read(b.start()..end, |s| assert_eq!(s[0], 42)).unwrap();
        b.alias.as_mut().unwrap().release();
        let alias = b.alias.as_mut().unwrap();
        alias.fail_after = Some(alias.map_calls + 1);
        assert!(
            b.with_contiguous_read(b.start()..end, |_| panic!("must not expose failed mapping"))
                .is_err()
        );
        assert!(b.alias.as_ref().unwrap().base.is_none());
        b.alias.as_mut().unwrap().fail_after = None;
        b.with_contiguous_read(b.start()..end, |s| assert_eq!(s[0], 42)).unwrap();
    }
}
