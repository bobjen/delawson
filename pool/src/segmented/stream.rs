//! Borrowing stream cursors. No extra buffering or virtual dispatch is required.
//!
//! ```
//! use pool::{ChunkPool, SegmentedBuffer};
//! use std::io::{Read, Write};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = ChunkPool::new(32, 0)?;
//! let start = pool::segmented::CHUNK_BYTES as u64 - 3;
//! let mut buffer = SegmentedBuffer::with_start(&pool, start);
//! {
//!     // Supply the calling thread's RNG in production.
//!     let mut writer = buffer.writer(|| 0);
//!     writer.write_u32_le(5)?; // crosses a leaf boundary
//!     writer.write_all(b"hello")?;
//! }
//! let mut reader = buffer.reader_range(start..start + 9)?;
//! assert_eq!(reader.read_u32_le()?, 5);
//! let mut text = [0; 5];
//! reader.read_exact(&mut text)?;
//! assert_eq!(&text, b"hello");
//! assert_eq!(reader.remaining(), 0);
//! # Ok(())
//! # }
//! ```
use super::{CHUNK_BYTES, SegmentedBuffer};
use crate::NONE;
use std::io::{self, BufRead, Read, Write};
use std::ops::Range;

/// A reader over a bounded, immutable range of a segmented buffer.
///
/// The current slice is clipped to both the leaf boundary and the range end.
/// `fill_buf` borrows bytes; `consume` advances within that slice without freeing
/// storage. A subsequent `fill_buf` selects the next leaf. No disk I/O occurs.
/// Drop the reader before discarding consumed bytes from the underlying buffer.
pub struct SegmentedReader<'b, 'p> {
    buffer: &'b SegmentedBuffer<'p>,
    current: &'b [u8],
    position: u64,
    end: u64,
}

impl<'b, 'p> SegmentedReader<'b, 'p> {
    pub fn new(buffer: &'b SegmentedBuffer<'p>) -> Self {
        let mut reader = Self { buffer, current: &[], position: buffer.start(), end: buffer.end() };
        reader.refill();
        reader
    }

    /// Offsets are absolute. The range must be entirely within the live buffer.
    pub fn with_range(buffer: &'b SegmentedBuffer<'p>, range: Range<u64>) -> io::Result<Self> {
        if range.start < buffer.start() || range.end > buffer.end() || range.start > range.end {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "reader range outside buffer"));
        }
        let mut reader = Self { buffer, current: &[], position: range.start, end: range.end };
        reader.refill();
        Ok(reader)
    }

    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn remaining(&self) -> u64 {
        self.end - self.position
    }

    fn refill(&mut self) {
        if self.position == self.end {
            self.current = &[];
            return;
        }
        let chunk = self.buffer.chunk(self.position).unwrap();
        let count = (chunk.len() as u64).min(self.remaining()) as usize;
        self.current = &chunk[..count];
    }

    /// Read fixed-width bytes without a heap allocation. Within a leaf, this
    /// is a slice-length check and a copy into the returned value; crossing a
    /// leaf uses the slow path. Short input leaves the cursor unchanged.
    #[inline]
    pub fn read_array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        if self.current.len() >= N {
            let value = self.current[..N].try_into().unwrap();
            self.consume(N);
            Ok(value)
        } else {
            self.read_array_slow()
        }
    }

    #[cold]
    fn read_array_slow<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut value = [0; N];
        self.read_exact(&mut value)?;
        Ok(value)
    }

    #[inline]
    pub fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.read_array::<1>()?[0])
    }
    #[inline]
    pub fn read_u16_le(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }
    #[inline]
    pub fn read_u32_le(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }
    #[inline]
    pub fn read_u64_le(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }
}

impl BufRead for SegmentedReader<'_, '_> {
    #[inline]
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.current.is_empty() {
            self.refill();
        }
        Ok(self.current)
    }

    /// amount must not exceed the unread slice returned by fill_buf().
    #[inline]
    fn consume(&mut self, amount: usize) {
        assert!(amount <= self.current.len(), "consume exceeds current buffer");
        self.current = &self.current[amount..];
        self.position += amount as u64;
    }
}

impl Read for SegmentedReader<'_, '_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let current = self.fill_buf()?;
        let count = current.len().min(out.len());
        out[..count].copy_from_slice(&current[..count]);
        self.consume(count);
        Ok(count)
    }

    /// For this fully resident reader, short input leaves both the cursor and
    /// destination unchanged. read() itself may return only one leaf's bytes.
    fn read_exact(&mut self, mut out: &mut [u8]) -> io::Result<()> {
        if out.len() as u64 > self.remaining() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segmented input ends mid-value",
            ));
        }
        while !out.is_empty() {
            let count = self.read(out)?;
            out = &mut out[count..];
        }
        Ok(())
    }
}

/// An append-only writer, borrowing its buffer and a caller-provided RNG.
///
/// Small writes use a cached tail index without walking the tree or calling the
/// RNG. Boundary-crossing writes use SegmentedBuffer::append and its rollback
/// on allocation failure. Each write either appends all its bytes or returns an
/// error without extending the logical range; a sequence of writes is not a
/// transaction. Unused leaf bytes remain uninitialized. flush() is a no-op: all
/// bytes are already in memory, and this writer provides no disk durability.
pub struct SegmentedWriter<'b, 'p, F> {
    buffer: &'b mut SegmentedBuffer<'p>,
    random: F,
    tail: u32,
    spare: usize,
}

impl<'b, 'p, F: FnMut() -> u64> SegmentedWriter<'b, 'p, F> {
    pub fn new(buffer: &'b mut SegmentedBuffer<'p>, random: F) -> Self {
        let mut writer = Self { buffer, random, tail: NONE, spare: 0 };
        writer.refresh();
        writer
    }

    pub fn position(&self) -> u64 {
        self.buffer.end()
    }

    /// Return the buffer, ending the writer's exclusive borrow.
    pub fn into_inner(self) -> &'b mut SegmentedBuffer<'p> {
        self.buffer
    }

    fn refresh(&mut self) {
        let within = self.buffer.end() as usize % CHUNK_BYTES;
        if self.buffer.end() == 0 && self.buffer.tail != NONE {
            // reset leaves an owned first leaf ready even though no bytes are live.
            self.tail = self.buffer.tail;
            self.spare = CHUNK_BYTES;
        } else if self.buffer.is_empty() || within == 0 {
            self.tail = NONE;
            self.spare = 0;
        } else {
            self.tail = self.buffer.leaf(self.buffer.end() / CHUNK_BYTES as u64);
            self.spare = ((CHUNK_BYTES - within) as u64).min(u64::MAX - self.buffer.end()) as usize;
        }
    }

    #[cold]
    fn write_slow(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.buffer.end().checked_add(bytes.len() as u64).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segmented output offset overflow",
            ));
        }
        // Invalidate the cache before invoking user code, including the RNG.
        // If that unwinds after a partial append, no stale tail can be reused.
        self.tail = NONE;
        self.spare = 0;
        let result = self.buffer.append(bytes, &mut self.random);
        self.refresh();
        result.map_err(io::Error::other)?;
        Ok(bytes.len())
    }

    #[inline]
    pub fn write_u8(&mut self, value: u8) -> io::Result<()> {
        self.write_all(&[value])
    }
    #[inline]
    pub fn write_u16_le(&mut self, value: u16) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }
    #[inline]
    pub fn write_u32_le(&mut self, value: u32) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }
    #[inline]
    pub fn write_u64_le(&mut self, value: u64) -> io::Result<()> {
        self.write_all(&value.to_le_bytes())
    }
}

impl<F: FnMut() -> u64> Write for SegmentedWriter<'_, '_, F> {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if bytes.len() > self.spare {
            return self.write_slow(bytes);
        }
        let within = self.buffer.end() as usize % CHUNK_BYTES;
        // SAFETY: the writer exclusively borrows its buffer. The cached tail
        // is owned by that buffer and cannot be freed or changed elsewhere.
        // spare bounds both the leaf and the representable end offset. No safe
        // caller can supply a source slice aliasing this mutable buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.buffer.ptr(self.tail).add(within),
                bytes.len(),
            );
        }
        self.buffer.end += bytes.len() as u64;
        self.spare -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'p> SegmentedBuffer<'p> {
    pub fn reader(&self) -> SegmentedReader<'_, 'p> {
        SegmentedReader::new(self)
    }
    pub fn reader_range(&self, range: Range<u64>) -> io::Result<SegmentedReader<'_, 'p>> {
        SegmentedReader::with_range(self, range)
    }
    pub fn writer<F: FnMut() -> u64>(&mut self, random: F) -> SegmentedWriter<'_, 'p, F> {
        SegmentedWriter::new(self, random)
    }
}

// Well below Linux IOV_MAX. Each syscall transfers at most 256KiB of leaves.
const IO_SLICES: usize = 64;

impl SegmentedBuffer<'_> {
    /// Send initialized leaves directly, using scalar I/O for a single leaf.
    /// Retry interrupted and short writes.
    pub fn write_to(&self, output: &mut impl Write) -> io::Result<()> {
        let mut at = self.start();
        while at < self.end() {
            let mut next = at;
            let result = if self.end() - at <= (CHUNK_BYTES - at as usize % CHUNK_BYTES) as u64 {
                next = self.end();
                output.write(self.chunk(at).expect("live output leaf"))
            } else {
                let mut slices: [io::IoSlice<'_>; IO_SLICES] =
                    std::array::from_fn(|_| io::IoSlice::new(&[]));
                let mut count = 0;
                while next < self.end() && count < IO_SLICES {
                    let chunk = self.chunk(next).unwrap();
                    slices[count] = io::IoSlice::new(chunk);
                    next += chunk.len() as u64;
                    count += 1;
                }
                output.write_vectored(&slices[..count])
            };
            match result {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "segmented write made no progress",
                    ));
                }
                Ok(n) if n as u64 <= next - at => at += n as u64,
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "writer exceeded supplied buffers",
                    ));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Append exactly count bytes, using scalar reads for a single leaf. The safe
    /// Read trait requires initialized destinations, so newly exposed bytes are
    /// zeroed before I/O. On EOF, I/O error, or allocation failure, roll back the
    /// logical append. Bytes already consumed from the input cannot be restored.
    pub fn read_exact_from(
        &mut self,
        input: &mut impl Read,
        count: usize,
        mut random: impl FnMut() -> u64,
    ) -> io::Result<()> {
        let start = self.end();
        let end = start.checked_add(count as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "receive offset overflow")
        })?;
        let result = (|| {
            let mut at = start;
            while at < end {
                let window_end = end.min(at.saturating_add(((IO_SLICES - 1) * CHUNK_BYTES) as u64));
                while self.end() < window_end {
                    let n = (window_end - self.end()).min(CHUNK_BYTES as u64) as usize;
                    self.append(&[0; CHUNK_BYTES][..n], &mut random).map_err(io::Error::other)?;
                }
                while at < window_end {
                    let result =
                        if window_end - at <= (CHUNK_BYTES - at as usize % CHUNK_BYTES) as u64 {
                            let mut chunks =
                                self.chunks_mut_range(at..window_end).map_err(io::Error::other)?;
                            input.read(chunks.next().expect("live input leaf"))
                        } else {
                            let mut slices: [io::IoSliceMut<'_>; IO_SLICES] =
                                std::array::from_fn(|_| io::IoSliceMut::new(&mut []));
                            let mut count = 0;
                            for chunk in
                                self.chunks_mut_range(at..window_end).map_err(io::Error::other)?
                            {
                                slices[count] = io::IoSliceMut::new(chunk);
                                count += 1;
                            }
                            input.read_vectored(&mut slices[..count])
                        };
                    match result {
                        Ok(0) => {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "segmented input ended early",
                            ));
                        }
                        Ok(n) if n as u64 <= window_end - at => at += n as u64,
                        Ok(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "reader exceeded supplied buffers",
                            ));
                        }
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.truncate(start).map_err(io::Error::other)?;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChunkPool;
    use std::cell::Cell;

    fn pool(count: u32) -> ChunkPool {
        ChunkPool::with_buckets(count, 0, 1).unwrap()
    }

    #[test]
    fn integer_fields_at_every_leaf_split() {
        let pool = pool(16);
        for tail in 0..=8 {
            let start = CHUNK_BYTES as u64 - tail;
            let mut buffer = SegmentedBuffer::with_start(&pool, start);
            {
                let mut writer = buffer.writer(|| 0);
                writer.write_u64_le(0x0123456789abcdef).unwrap();
                writer.write_u32_le(0x89abcdef).unwrap();
                writer.write_u16_le(0x1234).unwrap();
                writer.write_u8(0x56).unwrap();
                assert_eq!(writer.position(), start + 15);
                writer.flush().unwrap();
            }
            let mut reader = buffer.reader();
            assert_eq!(reader.read_u64_le().unwrap(), 0x0123456789abcdef);
            assert_eq!(reader.read_u32_le().unwrap(), 0x89abcdef);
            assert_eq!(reader.read_u16_le().unwrap(), 0x1234);
            assert_eq!(reader.read_u8().unwrap(), 0x56);
            assert_eq!(reader.position(), start + 15);
            assert_eq!(reader.remaining(), 0);
            assert!(reader.fill_buf().unwrap().is_empty());
            assert_eq!(reader.read_u8().unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        }
        // Explicitly exercise every internal split for the shorter fields too.
        for width in [2, 4] {
            for tail in 1..width {
                let mut buffer = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - tail);
                let mut writer = buffer.writer(|| 0);
                if width == 2 {
                    writer.write_u16_le(0xabcd).unwrap();
                } else {
                    writer.write_u32_le(0xabcdef01).unwrap();
                }
                let mut reader = writer.into_inner().reader();
                if width == 2 {
                    assert_eq!(reader.read_u16_le().unwrap(), 0xabcd);
                } else {
                    assert_eq!(reader.read_u32_le().unwrap(), 0xabcdef01);
                }
            }
        }
    }

    #[test]
    fn fill_and_consume_borrow_one_leaf_at_a_time() {
        let pool = pool(8);
        let mut buffer = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 3);
        buffer.append(&[1, 2, 3, 4, 5], || 0).unwrap();
        let first_ptr = buffer.chunk(CHUNK_BYTES as u64 - 3).unwrap().as_ptr();
        let next_ptr = buffer.chunk(CHUNK_BYTES as u64).unwrap().as_ptr();
        let mut reader = buffer.reader();
        assert_eq!(reader.fill_buf().unwrap(), [1, 2, 3]);
        assert_eq!(reader.fill_buf().unwrap().as_ptr(), first_ptr);
        reader.consume(2);
        assert_eq!(reader.fill_buf().unwrap(), [3]);
        reader.consume(1);
        assert_eq!(reader.position(), CHUNK_BYTES as u64);
        assert_eq!(reader.fill_buf().unwrap().as_ptr(), next_ptr);
        assert_eq!(reader.fill_buf().unwrap(), [4, 5]);
        reader.consume(2);
        assert!(reader.fill_buf().unwrap().is_empty());
        reader.consume(0);
        assert_eq!(reader.read(&mut [0; 8]).unwrap(), 0);
        assert_eq!(buffer.len(), 5); // consumption does not discard storage
    }

    #[test]
    #[should_panic(expected = "consume exceeds current buffer")]
    fn consume_cannot_cross_the_exposed_slice() {
        let pool = pool(8);
        let mut buffer = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 3);
        buffer.append(&[1; 8], || 0).unwrap();
        let mut reader = buffer.reader();
        assert_eq!(reader.fill_buf().unwrap().len(), 3);
        reader.consume(4);
    }

    #[test]
    fn logical_end_is_enforced_before_the_physical_end() {
        let pool = pool(8);
        let mut buffer = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 3);
        buffer.append(&[1, 2, 3, 4, 5, 6, 7, 8], || 0).unwrap();
        for length in 0..4 {
            let mut reader = buffer
                .reader_range(CHUNK_BYTES as u64 - 3..CHUNK_BYTES as u64 - 3 + length)
                .unwrap();
            let mut output = [42; 4];
            assert_eq!(
                reader.read_exact(&mut output).unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            assert_eq!(output, [42; 4]);
            assert_eq!(reader.read_u32_le().unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(reader.position(), CHUNK_BYTES as u64 - 3);
            assert_eq!(reader.remaining(), length);
        }
        let mut reader =
            buffer.reader_range(CHUNK_BYTES as u64 - 3..CHUNK_BYTES as u64 + 1).unwrap();
        assert_eq!(reader.read_u32_le().unwrap(), 0x04030201);
        assert!(reader.fill_buf().unwrap().is_empty());
        assert_eq!(reader.read_u8().unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        let mut same_leaf =
            buffer.reader_range(CHUNK_BYTES as u64..CHUNK_BYTES as u64 + 2).unwrap();
        assert_eq!(same_leaf.fill_buf().unwrap(), [4, 5]);
        assert!(buffer.reader_range(CHUNK_BYTES as u64 - 4..CHUNK_BYTES as u64 + 1).is_err());
        assert!(buffer.reader_range(CHUNK_BYTES as u64 - 1..CHUNK_BYTES as u64 - 2).is_err());
        assert!(buffer.reader_range(CHUNK_BYTES as u64 - 3..CHUNK_BYTES as u64 + 6).is_err());
    }

    #[test]
    fn writer_starts_directly_in_the_retained_leaf() {
        let pool = pool(8);
        let mut buffer = SegmentedBuffer::with_retained_leaves(&pool, 1);
        buffer.append(b"old bytes", || 0).unwrap();
        buffer.reset();
        let first = buffer.tail;
        {
            let mut writer = buffer.writer(|| panic!("retained allocation"));
            assert_eq!(writer.tail, first);
            assert_eq!(writer.spare, CHUNK_BYTES);
            writer.write_all(&vec![5; CHUNK_BYTES]).unwrap();
            assert_eq!(writer.spare, 0);
        }
        // Reopening at a full-leaf boundary must not reuse the previous leaf.
        buffer.writer(|| 0).write_u8(9).unwrap();
        assert_eq!(buffer.chunk(0), Some(vec![5; CHUNK_BYTES].as_slice()));
        assert_eq!(buffer.chunk(CHUNK_BYTES as u64), Some([9].as_slice()));
    }

    #[test]
    fn small_writes_reuse_the_tail_without_allocating() {
        let pool = pool(8);
        let calls = Cell::new(0);
        let mut buffer = SegmentedBuffer::new(&pool);
        let mut writer = buffer.writer(|| {
            calls.set(calls.get() + 1);
            0
        });
        writer.write_all(&[]).unwrap();
        assert_eq!(calls.get(), 0);
        for _ in 0..CHUNK_BYTES / 4 {
            writer.write_u32_le(0x12345678).unwrap();
        }
        assert_eq!(calls.get(), 1);
        writer.write_u8(9).unwrap();
        assert_eq!(calls.get(), 3); // one index node and a second leaf
        let buffer = writer.into_inner();
        let mut reader = buffer.reader();
        for _ in 0..CHUNK_BYTES / 4 {
            assert_eq!(reader.read_u32_le().unwrap(), 0x12345678);
        }
        assert_eq!(reader.read_u8().unwrap(), 9);
    }

    #[test]
    fn write_error_restores_the_cached_tail() {
        let pool = pool(2); // can't own two leaves plus an index node
        let mut buffer = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 3);
        let mut writer = buffer.writer(|| 0);
        writer.write_u8(11).unwrap();
        assert!(writer.write_u32_le(0x12345678).is_err());
        assert_eq!(writer.position(), CHUNK_BYTES as u64 - 2);
        writer.write_u16_le(0x2233).unwrap();
        assert_eq!(writer.position(), CHUNK_BYTES as u64);
        assert!(writer.write_u8(44).is_err());
        let buffer = writer.into_inner();
        assert_eq!(buffer.chunks().flatten().copied().collect::<Vec<_>>(), [11, 0x33, 0x22]);
        buffer.clear();
        let mut writer = buffer.writer(|| 0);
        writer.write_u32_le(0xabcdef01).unwrap();
        assert_eq!(writer.into_inner().reader().read_u32_le().unwrap(), 0xabcdef01);
    }

    #[test]
    fn reader_writer_handle_the_end_of_u64_offsets() {
        let pool = pool(4);
        let mut buffer = SegmentedBuffer::with_start(&pool, u64::MAX - 4);
        let mut writer = buffer.writer(|| 0);
        writer.write_u16_le(0x1234).unwrap();
        writer.write_u16_le(0xabcd).unwrap(); // cached path ends at u64::MAX
        assert_eq!(writer.position(), u64::MAX);
        assert_eq!(writer.write_u8(1).unwrap_err().kind(), io::ErrorKind::InvalidInput);
        writer.write_all(&[]).unwrap();
        let buffer = writer.into_inner();
        let mut reader = buffer.reader();
        assert_eq!(reader.read_u32_le().unwrap(), 0xabcd1234);
        assert_eq!(reader.position(), u64::MAX);
        assert_eq!(reader.read_array::<0>().unwrap(), []);
        assert_eq!(reader.read_u8().unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn generic_io_copy_and_reopened_rolling_cursors() {
        let pool = pool(32);
        let mut source = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 5);
        let bytes: Vec<_> = (0..17000).map(|n| (n % 251) as u8).collect();
        source.writer(|| 0).write_all(&bytes).unwrap();
        let mut destination = SegmentedBuffer::new(&pool);
        let count = io::copy(&mut source.reader(), &mut destination.writer(|| 0)).unwrap();
        assert_eq!(count, bytes.len() as u64);
        let mut output = Vec::new();
        destination.reader().read_to_end(&mut output).unwrap();
        assert_eq!(output, bytes);
        source.discard_before(CHUNK_BYTES as u64 - 5 + 9000).unwrap();
        source.writer(|| 0).write_all(&[99; 7]).unwrap();
        let mut output = Vec::new();
        source.reader().read_to_end(&mut output).unwrap();
        let mut expected = bytes[9000..].to_vec();
        expected.extend([99; 7]);
        assert_eq!(output, expected);
    }

    #[test]
    fn writer_cache_survives_caught_rng_panic() {
        let pool = pool(16);
        let mut buffer = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 3);
        let fail = Cell::new(false);
        let mut writer = buffer.writer(|| {
            assert!(!fail.get(), "injected RNG failure");
            0
        });
        writer.write_u8(11).unwrap();
        fail.set(true);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = writer.write_all(&[22; 8]);
            }))
            .is_err()
        );
        // The resident append copied the first two bytes before requesting
        // another node. Panics (unlike returned errors) need not roll back.
        assert_eq!(writer.position(), CHUNK_BYTES as u64);
        fail.set(false);
        writer.write_u16_le(0x4433).unwrap();
        let buffer = writer.into_inner();
        assert_eq!(
            buffer.chunks().flatten().copied().collect::<Vec<_>>(),
            [11, 22, 22, 0x33, 0x44]
        );
    }

    #[test]
    fn scalar_encoding_matches_existing_structure_format() {
        use common::wire::{Arg, Op, StructReader, StructWriter};
        let mut old = StructWriter::new();
        old.number_u8(7);
        old.nested(Op::Range, |w| {
            w.number_u16(0x1234);
            w.number_u32(0xabcdef01);
            w.number_u64(0x0123456789abcdef);
            w.text("crossing a block");
        });
        let Arg::Struct(_, expected, depth) = old.finish(Op::Range) else { unreachable!() };
        let pool = pool(8);
        for start in 4088..CHUNK_BYTES as u64 + 1 {
            let mut buffer = SegmentedBuffer::with_start(&pool, start);
            let mut writer = buffer.writer(|| 0);
            writer.write_u8(7).unwrap();
            writer.write_u16_le(Op::Range.code()).unwrap();
            writer.write_u16_le(0x1234).unwrap();
            writer.write_u32_le(0xabcdef01).unwrap();
            writer.write_u64_le(0x0123456789abcdef).unwrap();
            writer.write_u32_le(16).unwrap();
            writer.write_all(b"crossing a block").unwrap();
            let buffer = writer.into_inner();
            let actual = buffer.chunks().flatten().copied().collect::<Vec<_>>();
            assert_eq!(actual, expected.to_vec());
            let mut old_reader = StructReader::new(&actual, depth);
            assert_eq!(old_reader.number_u8().unwrap(), 7);
            old_reader
                .nested(Op::Range, "range", |r| {
                    assert_eq!(r.number_u16()?, 0x1234);
                    assert_eq!(r.number_u32()?, 0xabcdef01);
                    assert_eq!(r.number_u64()?, 0x0123456789abcdef);
                    assert_eq!(r.text()?, "crossing a block");
                    Ok(())
                })
                .unwrap();
            old_reader.done().unwrap();
            let mut reader = buffer.reader();
            assert_eq!(reader.read_u8().unwrap(), 7);
            assert_eq!(reader.read_u16_le().unwrap(), Op::Range.code());
            assert_eq!(reader.read_u16_le().unwrap(), 0x1234);
            assert_eq!(reader.read_u32_le().unwrap(), 0xabcdef01);
            assert_eq!(reader.read_u64_le().unwrap(), 0x0123456789abcdef);
            let length = reader.read_u32_le().unwrap();
            assert_eq!(length, 16);
            assert_eq!(&reader.read_array::<16>().unwrap(), b"crossing a block");
            assert_eq!(reader.remaining(), 0);
        }
    }
}

#[cfg(test)]
mod vectored_tests {
    use super::*;
    use crate::ChunkPool;

    struct Transport {
        bytes: Vec<u8>,
        at: usize,
        calls: usize,
        largest_vector: usize,
        scalar_reads: usize,
        scalar_writes: usize,
    }
    impl Transport {
        fn new(bytes: Vec<u8>) -> Self {
            Self { bytes, at: 0, calls: 0, largest_vector: 0, scalar_reads: 0, scalar_writes: 0 }
        }
        fn step(&mut self, vectors: usize) -> io::Result<()> {
            self.calls += 1;
            self.largest_vector = self.largest_vector.max(vectors);
            if self.calls % 3 == 1 {
                return Err(io::ErrorKind::Interrupted.into());
            }
            Ok(())
        }
    }
    impl Write for Transport {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.scalar_writes += 1;
            self.write_vectored(&[io::IoSlice::new(bytes)])
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn write_vectored(&mut self, slices: &[io::IoSlice<'_>]) -> io::Result<usize> {
            self.step(slices.len())?;
            let mut n = 0;
            for slice in slices {
                let count = slice.len().min(5003 - n);
                self.bytes.extend_from_slice(&slice[..count]);
                n += count;
                if n == 5003 {
                    break;
                }
            }
            Ok(n)
        }
    }
    impl Read for Transport {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.scalar_reads += 1;
            self.read_vectored(&mut [io::IoSliceMut::new(bytes)])
        }
        fn read_vectored(&mut self, slices: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
            self.step(slices.len())?;
            let mut n = 0;
            for slice in slices {
                let count = slice.len().min(5003 - n).min(self.bytes.len() - self.at);
                slice[..count].copy_from_slice(&self.bytes[self.at..self.at + count]);
                self.at += count;
                n += count;
                if n == 5003 || self.at == self.bytes.len() {
                    break;
                }
            }
            Ok(n)
        }
    }

    #[test]
    fn single_leaf_io_uses_scalar_calls_with_interrupts_and_short_transfers() {
        let pool = ChunkPool::with_buckets(8, 0, 1).unwrap();
        let mut source = SegmentedBuffer::with_start(&pool, 17);
        let bytes: Vec<_> = (0..16001).map(|n| (n % 251) as u8).collect();
        source.append(&bytes, || 0).unwrap();
        let mut transport = Transport::new(Vec::new());
        source.write_to(&mut transport).unwrap();
        assert_eq!(transport.bytes, bytes);
        assert_eq!(transport.scalar_writes, transport.calls);
        let writes = transport.calls;
        let mut dest = SegmentedBuffer::with_start(&pool, 31);
        dest.read_exact_from(&mut transport, bytes.len(), || 0).unwrap();
        assert_eq!(transport.scalar_reads, transport.calls - writes);
        assert!(transport.scalar_reads > 3);
        assert_eq!(dest.chunk(31), Some(bytes.as_slice()));
        let before = dest.len();
        assert_eq!(
            dest.read_exact_from(&mut transport, 1, || 0).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(dest.len(), before);
    }

    #[test]
    fn vectored_io_handles_short_calls_interrupts_and_multiple_windows() {
        let pool = ChunkPool::with_buckets(200, 0, 1).unwrap();
        let mut source = SegmentedBuffer::with_start(&pool, CHUNK_BYTES as u64 - 3);
        let bytes: Vec<u8> = (0..CHUNK_BYTES * (IO_SLICES + 9)).map(|n| (n % 251) as u8).collect();
        source.append(&bytes, || 0).unwrap();
        let mut transport = Transport::new(Vec::new());
        source.write_to(&mut transport).unwrap();
        assert_eq!(transport.bytes, bytes);
        assert_eq!(transport.largest_vector, IO_SLICES);
        let mut dest = SegmentedBuffer::with_start(&pool, 17);
        dest.append(&[9; 5], || 0).unwrap();
        dest.read_exact_from(&mut transport, bytes.len(), || 0).unwrap();
        let mut actual = vec![0; bytes.len()];
        dest.read_at(22, &mut actual).unwrap();
        assert_eq!(actual, bytes);
        assert_eq!(dest.reader().read_array::<5>().unwrap(), [9; 5]);
        assert!(transport.calls > 100);
    }

    #[test]
    fn failed_receive_rolls_back_and_mutable_slices_stay_within_the_range() {
        let pool = ChunkPool::with_buckets(8, 0, 1).unwrap();
        let mut dest = SegmentedBuffer::new(&pool);
        dest.append(&[7; CHUNK_BYTES + 904], || 0).unwrap();
        let mut input = Transport::new(vec![8; 100]);
        assert_eq!(
            dest.read_exact_from(&mut input, CHUNK_BYTES * 2, || 0).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(dest.len(), (CHUNK_BYTES + 904) as u64);
        assert!(dest.chunks().all(|c| c.iter().all(|&b| b == 7)));
        let mut chunks: Vec<_> = dest
            .chunks_mut_range(CHUNK_BYTES as u64 - 3..CHUNK_BYTES as u64 + 4)
            .unwrap()
            .collect();
        assert_eq!(chunks.len(), 2);
        chunks[0].fill(1);
        chunks[1].fill(2);
        let mut result = [0; 9];
        dest.read_at(CHUNK_BYTES as u64 - 4, &mut result).unwrap();
        assert_eq!(result, [7, 1, 1, 1, 2, 2, 2, 2, 7]);
        assert!(
            dest.chunks_mut_range((CHUNK_BYTES + 904) as u64..(CHUNK_BYTES + 905) as u64).is_err()
        );
        dest.truncate(CHUNK_BYTES as u64 - 1).unwrap();
        assert_eq!(dest.depth(), Some(0));
    }

    #[test]
    fn receive_allocation_failure_preserves_existing_bytes() {
        let pool = ChunkPool::with_buckets(1, 0, 1).unwrap();
        let mut dest = SegmentedBuffer::new(&pool);
        dest.append(&[7; 3], || 0).unwrap();
        let mut input = Transport::new(vec![8; CHUNK_BYTES * 2]);
        assert!(dest.read_exact_from(&mut input, CHUNK_BYTES * 2, || 0).is_err());
        assert_eq!(dest.len(), 3);
        assert_eq!(dest.reader().read_array::<3>().unwrap(), [7; 3]);
        assert_eq!(dest.depth(), Some(0));
    }
}
