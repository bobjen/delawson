//! Scoped contiguous aliases of shared pool leaves.
use super::{CHUNK_BYTES, SegmentedBuffer};
use crate::{Error, Result};
use std::{ops::Range, ptr::NonNull};

/// Persistent address reservation, populated only for a scoped callback.
/// Source blocks remain owned by their buffer. No payload bytes are copied.
pub struct VirtualScratch {
    base: Option<NonNull<u8>>,
    capacity: usize,
    reserved: usize,
}

// SAFETY: mappings have no thread affinity; access requires &mut self.
unsafe impl Send for VirtualScratch {}

impl VirtualScratch {
    /// Reserve lazily. One extra block accommodates a range starting mid-leaf.
    pub fn new(capacity: usize) -> Self {
        Self { base: None, capacity, reserved: 0 }
    }

    pub fn supported() -> bool {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        cfg!(target_os = "linux") && page > 0 && CHUNK_BYTES % page as usize == 0
    }

    fn reserve(&mut self) -> Result<NonNull<u8>> {
        if let Some(base) = self.base {
            return Ok(base);
        }
        let len = self
            .capacity
            .checked_add(CHUNK_BYTES * 2 - 1)
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or_else(|| Error::new("virtual scratch too large"))?
            / CHUNK_BYTES
            * CHUNK_BYTES;
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
            return Err(os_error("reserve virtual scratch"));
        }
        let Some(base) = NonNull::new(raw.cast()) else {
            unsafe {
                libc::munmap(raw, len);
            }
            return Err(Error::new("virtual scratch mapped at zero"));
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
        let (guard, offset, len) = self.map(buffer, range)?;
        // SAFETY: initialized range; buffer stays shared-borrowed. The callback
        // cannot return a reference borrowing this temporary slice.
        let bytes = unsafe { std::slice::from_raw_parts(guard.base().add(offset), len) };
        Ok(f(bytes))
    }

    pub fn with_write<T>(
        &mut self,
        buffer: &mut SegmentedBuffer<'_>,
        range: Range<u64>,
        f: impl FnOnce(&mut [u8]) -> T,
    ) -> Result<T> {
        let (guard, offset, len) = self.map(buffer, range)?;
        // SAFETY: distinct leaves and initialized range. The exclusive buffer
        // borrow prevents access through its original mapping during this call.
        let bytes = unsafe { std::slice::from_raw_parts_mut(guard.base().add(offset), len) };
        Ok(f(bytes))
    }

    fn map(
        &mut self,
        buffer: &SegmentedBuffer<'_>,
        range: Range<u64>,
    ) -> Result<(Mapped<'_>, usize, usize)> {
        if range.start > range.end || range.start < buffer.start() || range.end > buffer.end() {
            return Err(Error::new("virtual scratch range outside buffer"));
        }
        let len = usize::try_from(range.end - range.start)
            .map_err(|_| Error::new("virtual scratch range too large"))?;
        if len > self.capacity || !Self::supported() {
            return Err(Error::new("unsupported virtual scratch range or page size"));
        }
        let base = self.reserve()?;
        let offset = (range.start % CHUNK_BYTES as u64) as usize;
        let pages = if len == 0 { 0 } else { (offset + len).div_ceil(CHUNK_BYTES) };
        let guard = Mapped { scratch: self, used: pages * CHUNK_BYTES };
        let first = range.start / CHUNK_BYTES as u64;
        let mut page = 0;
        while page < pages {
            let index = buffer.leaf(first + page as u64);
            let mut count = 1;
            // Alias adjacent physical pool slots with a single syscall.
            while page + count < pages
                && buffer.leaf(first + (page + count) as u64) == index + count as u32
            {
                count += 1;
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
                return Err(os_error("alias pool leaves"));
            }
            page += count;
        }
        Ok((guard, offset, len))
    }
}

fn os_error(operation: &str) -> Error {
    Error::new(format!("{operation}: {}", std::io::Error::last_os_error()))
}
struct Mapped<'a> {
    scratch: &'a mut VirtualScratch,
    used: usize,
}
impl Mapped<'_> {
    fn base(&self) -> *mut u8 {
        self.scratch.base.unwrap().as_ptr()
    }
}
impl Drop for Mapped<'_> {
    fn drop(&mut self) {
        if self.used == 0 {
            return;
        }
        // Remove aliases on success, error, or panic; retain the reservation.
        let raw = unsafe {
            libc::mmap(
                self.base().cast(),
                self.used,
                libc::PROT_NONE,
                libc::MAP_FIXED | libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            // Discard the reservation if replacement fails. No stale alias can
            // be exposed on a later call, which must reserve a fresh window.
            if let Some(base) = self.scratch.base.take() {
                unsafe {
                    libc::munmap(base.as_ptr().cast(), self.scratch.reserved);
                }
            }
        }
    }
}
impl Drop for VirtualScratch {
    fn drop(&mut self) {
        if let Some(base) = self.base {
            unsafe {
                libc::munmap(base.as_ptr().cast(), self.reserved);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChunkPool;
    use common::random::Rng;

    #[test]
    fn aliases_scattered_leaves_at_the_original_offset() {
        if !VirtualScratch::supported() {
            return;
        }
        let pool = ChunkPool::new(((4u64 << 30) / CHUNK_BYTES as u64) as u32, 0).unwrap();
        let mut rng = Rng::from_seed(42);
        let mut buffer = SegmentedBuffer::with_start(&pool, (CHUNK_BYTES - 3) as u64);
        let mut other = SegmentedBuffer::new(&pool);
        let mut expected = Vec::new();
        // Interleave allocations to force nonconsecutive backing indexes.
        for n in 0..5u8 {
            let bytes = vec![n + 1; CHUNK_BYTES];
            buffer.append(&bytes, || rng.next_u64()).unwrap();
            expected.extend(bytes);
            other.append(&[99; CHUNK_BYTES], || rng.next_u64()).unwrap();
        }
        let mut scratch = VirtualScratch::new(expected.len());
        let start = buffer.start() + 1;
        let end = buffer.end() - 2;
        scratch
            .with_read(&buffer, start..end, |bytes| {
                let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
                assert_eq!(bytes.as_ptr() as usize % page, (CHUNK_BYTES - 2) % page);
                assert_eq!(bytes, &expected[1..expected.len() - 2]);
            })
            .unwrap();
        let reservation = scratch.base;
        scratch
            .with_write(&mut buffer, start..end, |bytes| {
                for b in bytes {
                    *b ^= 0x55;
                }
            })
            .unwrap();
        let last = expected.len() - 2;
        for b in &mut expected[1..last] {
            *b ^= 0x55;
        }
        let mut actual = vec![0; expected.len()];
        buffer.read_at(buffer.start(), &mut actual).unwrap();
        assert_eq!(actual, expected); // includes untouched prefix and suffix
        assert_eq!(scratch.base, reservation);
        // Change the original mapping, then read again through the same scratch.
        buffer.write_at(start, b"test").unwrap();
        scratch.with_read(&buffer, start..start + 4, |b| assert_eq!(b, b"test")).unwrap();
        scratch.with_read(&other, 0..other.end(), |b| assert!(b.iter().all(|&v| v == 99))).unwrap();
        buffer.discard_before(start + CHUNK_BYTES as u64).unwrap();
        scratch
            .with_read(&buffer, buffer.start()..buffer.end(), |b| {
                assert_eq!(b.len() as u64, buffer.len());
            })
            .unwrap();
        scratch.with_read(&buffer, buffer.end()..buffer.end(), |b| assert!(b.is_empty())).unwrap();
        assert!(scratch.with_read(&buffer, 0..1, |_| ()).is_err());
        assert!(
            VirtualScratch::new(1)
                .with_read(&buffer, buffer.start()..buffer.end(), |_| ())
                .is_err()
        );
    }

    #[test]
    fn aliases_are_cleaned_up_on_panic_and_error() {
        if !VirtualScratch::supported() {
            return;
        }
        let pool = ChunkPool::new(((4u64 << 30) / CHUNK_BYTES as u64) as u32, 0).unwrap();
        let mut rng = Rng::from_seed(42);
        let mut buffer = SegmentedBuffer::new(&pool);
        buffer.append(&[7; CHUNK_BYTES * 2], || rng.next_u64()).unwrap();
        let mut scratch = VirtualScratch::new(CHUNK_BYTES * 2);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scratch
                .with_write(&mut buffer, 3..(CHUNK_BYTES + 1904) as u64, |b| {
                    b[0] = 42;
                    panic!("exercise alias guard");
                })
                .unwrap();
        }));
        assert!(result.is_err());
        let reservation = scratch.base;
        let result: Result<std::result::Result<(), ()>> =
            scratch.with_read(&buffer, 3..(CHUNK_BYTES + 1904) as u64, |b| {
                assert_eq!(b[0], 42);
                Err(())
            });
        assert_eq!(result.unwrap(), Err(()));
        assert_eq!(scratch.base, reservation);
        // The source can be released and another source aliased safely afterward.
        drop(buffer);
        let mut replacement = SegmentedBuffer::new(&pool);
        replacement.append(&[11; CHUNK_BYTES * 2], || rng.next_u64()).unwrap();
        scratch
            .with_read(&replacement, 3..(CHUNK_BYTES + 1904) as u64, |b| {
                assert!(b.iter().all(|&v| v == 11))
            })
            .unwrap();
    }
}
