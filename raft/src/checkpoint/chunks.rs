//! Parallel positional I/O for table chunks in large checkpoints.
use common::{Error, Result};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Location and logical coverage of one independently encoded table chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkPos {
    pub start: u64,
    /// Decoded, uncompressed chunk length.
    pub plain: u64,
    /// Bytes stored in the checkpoint. Smaller than `plain` when LZ4 helped.
    pub stored: u64,
    pub first: u64,
    pub span: u64,
    pub items: u64,
}
crate::wire_struct!(ChunkPos, CkChunkLoc, {start,plain,stored,first,span,items});

/// Encode chunks on a bounded worker set, reserve each file range after encoding,
/// then write the bytes positionally. Large bucket ranges split until they fit
/// max_bytes; a single bucket remains indivisible.
pub fn write_chunks(
    file: &File,
    start: u64,
    workers: usize,
    kind: &'static str,
    ranges: &[(u64, u64)],
    max_bytes: usize,
    max_items: u64,
    encode: impl Fn(u64, u64) -> Result<(u64, Vec<u8>)> + Sync,
) -> Result<(Vec<ChunkPos>, u64)> {
    if workers == 0 || max_bytes == 0 || max_items == 0 {
        return Err(Error::new("invalid checkpoint chunk worker settings"));
    }
    let next_range = AtomicUsize::new(0);
    let next_offset = AtomicU64::new(start);
    let pending_count = AtomicUsize::new(ranges.len());
    let positions = Mutex::new(Vec::new());
    let worker_count = workers.min(ranges.len());
    if worker_count != 0 {
        std::thread::scope(|scope| -> Result<()> {
            let mut threads = Vec::with_capacity(worker_count);
            let next_range = &next_range;
            let pending_count = &pending_count;
            let next_offset = &next_offset;
            let positions = &positions;
            let ranges = ranges;
            let encode = &encode;
            let file = file;
            for worker in 0..worker_count {
                threads.push(scope.spawn(move || -> Result<()> {
                    loop {
                        let at = next_range.fetch_add(1, Ordering::Relaxed);
                        if at >= ranges.len() {
                            break;
                        }
                        let mut todo = vec![ranges[at]];
                        while let Some((first, span)) = todo.pop() {
                            let began = Instant::now();
                            let (items, bytes) = encode(first, span)?;
                            if items == 0 {
                                let left = decrement(pending_count);
                                common::logi!(
                                    "Checkpoint chunk empty",
                                    "kind {},worker {},first {},span {},pending {}",
                                    kind,
                                    worker,
                                    first,
                                    span,
                                    left
                                );
                                continue;
                            }
                            if (bytes.len() > max_bytes || items > max_items) && span > 1 {
                                let left = span / 2;
                                let queued = pending_count.fetch_add(1, Ordering::Relaxed) + 1;
                                common::logi!(
                                    "Checkpoint chunk split",
                                    "kind {},worker {},first {},span {},pending {}",
                                    kind,
                                    worker,
                                    first,
                                    span,
                                    queued
                                );
                                todo.push((first + left, span - left));
                                todo.push((first, left));
                                continue;
                            }
                            let plain = u64::try_from(bytes.len())
                                .map_err(|_| Error::new("checkpoint chunk is too large"))?;
                            let squeezed = lz4_flex::compress(&bytes);
                            let stored_bytes = if squeezed.len() < bytes.len() {
                                squeezed
                            } else {
                                bytes
                            };
                            let stored = u64::try_from(stored_bytes.len())
                                .map_err(|_| Error::new("checkpoint chunk is too large"))?;
                            let saved = plain - stored;
                            let offset = next_offset
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                                    old.checked_add(stored)
                                })
                                .map_err(|_| Error::new("checkpoint file offset overflow"))?;
                            common::logi!(
                                "Checkpoint chunk ready",
                                "kind {},worker {},first {},span {},items {},plain {},stored {},saved {},offset {},pending {}",
                                kind,
                                worker,
                                first,
                                span,
                                items,
                                plain,
                                stored,
                                saved,
                                offset,
                                pending_count.load(Ordering::Relaxed)
                            );
                            if let Err(error) = write_at_all(file, &stored_bytes, offset) {
                                common::logi!(
                                    "Checkpoint chunk write failed",
                                    "kind {},worker {},first {},span {},offset {},us {},error {}",
                                    kind,
                                    worker,
                                    first,
                                    span,
                                    offset,
                                    began.elapsed().as_micros(),
                                    error
                                );
                                return Err(error);
                            }
                            let left = decrement(pending_count);
                            common::logi!(
                                "Checkpoint chunk written",
                                "kind {},worker {},first {},span {},items {},plain {},stored {},saved {},offset {},us {},pending {}",
                                kind,
                                worker,
                                first,
                                span,
                                items,
                                plain,
                                stored,
                                saved,
                                offset,
                                began.elapsed().as_micros(),
                                left
                            );
                            positions
                                .lock()
                                .map_err(|_| Error::new("checkpoint chunk queue poisoned"))?
                                .push(ChunkPos { start: offset, plain, stored, first, span, items });
                        }
                    }
                    Ok(())
                }));
            }
            for thread in threads {
                thread.join().map_err(|_| Error::new("checkpoint chunk writer panicked"))??;
            }
            Ok(())
        })?;
    }
    let mut positions =
        positions.into_inner().map_err(|_| Error::new("checkpoint chunk queue poisoned"))?;
    positions.sort_unstable_by_key(|chunk| chunk.first);
    let (plain, stored) = positions.iter().try_fold((0u64, 0u64), |(plain, stored), chunk| {
        let plain = plain
            .checked_add(chunk.plain)
            .ok_or_else(|| Error::new("checkpoint plain byte count overflow"))?;
        let stored = stored
            .checked_add(chunk.stored)
            .ok_or_else(|| Error::new("checkpoint stored byte count overflow"))?;
        Ok::<_, Error>((plain, stored))
    })?;
    let saved = plain - stored;
    let percent = if plain == 0 { 0.0 } else { saved as f64 * 100.0 / plain as f64 };
    common::logi!(
        "Checkpoint chunks compressed",
        "kind {},chunks {},plain {},stored {},saved {},percent {:.1}",
        kind,
        positions.len(),
        plain,
        stored,
        saved,
        percent
    );
    Ok((positions, next_offset.load(Ordering::Relaxed)))
}

/// Read and process independent chunks concurrently using positional I/O.
pub fn read_chunks(
    file: &File,
    chunks: &[ChunkPos],
    workers: usize,
    kind: &'static str,
    process: impl Fn(ChunkPos, Vec<u8>) -> Result<()> + Sync,
) -> Result<()> {
    if workers == 0 {
        return Err(Error::new("invalid checkpoint chunk worker settings"));
    }
    let next = AtomicUsize::new(0);
    let worker_count = workers.min(chunks.len());
    if worker_count == 0 {
        return Ok(());
    }
    std::thread::scope(|scope| -> Result<()> {
        let mut threads = Vec::with_capacity(worker_count);
        let next = &next;
        let file = file;
        let chunks = chunks;
        let process = &process;
        for worker in 0..worker_count {
            threads.push(scope.spawn(move || -> Result<()> {
                loop {
                    let at = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&chunk) = chunks.get(at) else { break };
                    let began = Instant::now();
                    let queued = chunks.len().saturating_sub(at + 1);
                    common::logi!(
                        "Checkpoint chunk read",
                        "kind {},worker {},first {},span {},items {},plain {},stored {},pending {}",
                        kind,
                        worker,
                        chunk.first,
                        chunk.span,
                        chunk.items,
                        chunk.plain,
                        chunk.stored,
                        queued
                    );
                    let stored = usize::try_from(chunk.stored)
                        .ok()
                        .filter(|stored| *stored <= isize::MAX as usize)
                        .ok_or_else(|| Error::new("checkpoint chunk is too large"))?;
                    let plain = usize::try_from(chunk.plain)
                        .ok()
                        .filter(|plain| *plain <= isize::MAX as usize)
                        .ok_or_else(|| Error::new("checkpoint chunk is too large"))?;
                    if stored == 0 || stored > plain {
                        return Err(Error::new("invalid checkpoint chunk lengths"));
                    }
                    let mut stored_bytes = Vec::new();
                    stored_bytes
                        .try_reserve_exact(stored)
                        .map_err(|_| Error::new("checkpoint chunk allocation failed"))?;
                    stored_bytes.resize(stored, 0);
                    let result = read_at_exact(file, &mut stored_bytes, chunk.start).and_then(|()| {
                        if stored == plain {
                            process(chunk, stored_bytes)
                        } else {
                            let bytes = lz4_flex::decompress(&stored_bytes, plain).map_err(|e| {
                                Error::new(format!("checkpoint chunk decompression failed: {e}"))
                            })?;
                            if bytes.len() != plain {
                                return Err(Error::new("checkpoint chunk expanded to wrong length"));
                            }
                            process(chunk, bytes)
                        }
                    });
                    if let Err(error) = result {
                        common::logi!(
                            "Checkpoint chunk read failed",
                            "kind {},worker {},first {},span {},us {},error {}",
                            kind,
                            worker,
                            chunk.first,
                            chunk.span,
                            began.elapsed().as_micros(),
                            error
                        );
                        return Err(error);
                    }
                    common::logi!(
                        "Checkpoint chunk loaded",
                        "kind {},worker {},first {},span {},items {},plain {},stored {},us {},pending {}",
                        kind,
                        worker,
                        chunk.first,
                        chunk.span,
                        chunk.items,
                        chunk.plain,
                        chunk.stored,
                        began.elapsed().as_micros(),
                        queued
                    );
                }
                Ok(())
            }));
        }
        for thread in threads {
            thread.join().map_err(|_| Error::new("checkpoint chunk reader panicked"))??;
        }
        Ok(())
    })
}

fn decrement(pending: &AtomicUsize) -> usize {
    pending.fetch_sub(1, Ordering::Relaxed) - 1
}

fn write_at_all(file: &File, mut bytes: &[u8], mut offset: u64) -> Result<()> {
    while !bytes.is_empty() {
        let count = file.write_at(bytes, offset)?;
        if count == 0 {
            return Err(Error::new("checkpoint positional write made no progress"));
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| Error::new("checkpoint write offset overflow"))?;
        bytes = &bytes[count..];
    }
    Ok(())
}

fn read_at_exact(file: &File, mut bytes: &mut [u8], mut offset: u64) -> Result<()> {
    while !bytes.is_empty() {
        let count = file.read_at(bytes, offset)?;
        if count == 0 {
            return Err(Error::new("truncated checkpoint chunk"));
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| Error::new("checkpoint read offset overflow"))?;
        bytes = &mut bytes[count..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_compressed_and_raw_chunks() -> Result<()> {
        let file = tempfile::tempfile()?;
        let repeated = vec![0x5a; 4096];
        let mut state = 0x8d26_7c31u32;
        let mut random = Vec::with_capacity(4096);
        for _ in 0..4096 {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            random.push(state as u8);
        }
        let expected = [repeated, random];
        let ranges = [(0, 1), (1, 1)];
        let (chunks, _) = write_chunks(&file, 0, 2, "test", &ranges, 8192, 1, |first, _| {
            let bytes = expected[first as usize].clone();
            Ok((1, bytes))
        })?;

        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].stored < chunks[0].plain);
        assert_eq!(chunks[1].stored, chunks[1].plain);

        let actual = Mutex::new(Vec::new());
        read_chunks(&file, &chunks, 2, "test", |chunk, bytes| {
            actual.lock().unwrap().push((chunk.first, bytes));
            Ok(())
        })?;
        let mut actual = actual.into_inner().unwrap();
        actual.sort_unstable_by_key(|(first, _)| *first);
        assert_eq!(actual[0].1, expected[0]);
        assert_eq!(actual[1].1, expected[1]);
        Ok(())
    }
}
