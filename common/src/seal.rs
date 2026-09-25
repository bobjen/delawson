//! Sealing a batch: compress, then encrypt, then send.
//!
//! The unit is the batch, not the message. Everything a connection sends in one
//! write is one sealed segment, so the cost of a nonce, a tag and a compression
//! header is paid per write rather than per message — and a batch of small
//! messages is compressible in a way that any one of them is not.
//!
//! Compress before encrypt, for the reason the storage pipeline has: ciphertext
//! does not compress.

use crate::config::{Compress, Messaging};
use crate::{Context, Error, Result};
use aegis::aegis128x2::Aegis128X2;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bytes a sealed segment adds: the length, the counter, and the tag.
pub const HEADER: usize = 12;
pub const TAG: usize = 16;
pub const OVERHEAD: usize = HEADER + TAG;

/// Bytes offered to the sealer and bytes it produced, process-wide, so the
/// compression a run actually achieved is observable rather than assumed.
static PLAIN_BYTES: AtomicU64 = AtomicU64::new(0);
static WIRE_BYTES: AtomicU64 = AtomicU64::new(0);

pub fn totals() -> (u64, u64) {
    (PLAIN_BYTES.load(Ordering::Relaxed), WIRE_BYTES.load(Ordering::Relaxed))
}

/// First byte of the plaintext, saying what was done to the rest.
const PLAIN: u8 = 0;
const LZ4: u8 = 1;

/// One direction of one connection. The nonce is the connection's prefix, the
/// direction, and a counter, so no two segments under one key share one.
/// A sample is only worth taking when it is small against the batch it stands
/// for. At an eighth, a wasted sample costs an eighth of the attempt it was
/// meant to avoid, and a batch smaller than that is cheap to compress outright.
const SAMPLE_SHARE: usize = 8;

/// The sample is taken in this many pieces, spread evenly from the first byte to
/// the last. A batch is whatever happened to be pending, so it holds a mixture —
/// a prefix would judge the whole of it by whichever kind of message arrived
/// first.
const SAMPLE_PIECES: usize = 4;

pub struct Seal {
    key: Option<[u8; 16]>,
    compress: Compress,
    compress_min: usize,
    compress_sample: usize,
    /// Reused by the sampler, so judging a batch allocates nothing.
    sample_buf: Vec<u8>,
    /// Fallback for platforms without compatible shared-page aliases.
    compression_input: Vec<u8>,
    prefix: [u8; 8],
    dir: u8,
    counter: u64,
}

impl Seal {
    pub fn new(config: &Messaging, prefix: [u8; 8], dir: u8) -> Result<Seal> {
        let key = if config.encrypt { Some(config.key()?) } else { None };
        Ok(Seal {
            key,
            compress: config.compress,
            compress_min: config.compress_min,
            compress_sample: config.compress_sample,
            sample_buf: Vec::with_capacity(config.compress_sample),
            compression_input: Vec::new(),
            prefix,
            dir,
            counter: 0,
        })
    }

    fn nonce(&self, counter: u64) -> [u8; 16] {
        let mut nonce = [0u8; 16];
        nonce[..8].copy_from_slice(&self.prefix);
        nonce[8] = self.dir;
        nonce[9..].copy_from_slice(&counter.to_le_bytes()[..7]);
        nonce
    }

    /// Seal `plain` into `out` as one segment. `plain` is left usable as scratch.
    pub fn seal(&mut self, plain: &mut [u8], out: &mut Vec<u8>) -> Result<()> {
        self.seal_to(plain, out)
    }

    /// Write the unchanged segment format to a contiguous or segmented sink.
    pub fn seal_to(&mut self, plain: &[u8], out: &mut impl Write) -> Result<()> {
        let counter = self.counter;
        self.counter += 1;

        let attempt = self.compress == Compress::Lz4
            && plain.len() >= self.compress_min
            && self.worth_trying(plain);

        let mut body = vec![PLAIN];
        // Even past the sampler, only if it actually helped.
        let squeezed = attempt.then(|| lz4_flex::compress(plain));
        match squeezed {
            Some(squeezed) if squeezed.len() < plain.len() => {
                body[0] = LZ4;
                body.extend((plain.len() as u32).to_le_bytes());
                body.extend(squeezed);
            }
            _ => body.extend(plain.iter()),
        }

        // The header is written before the body is sealed, because it is the
        // associated data: the length and counter are authenticated even though
        // they travel in the clear.
        let length = body.len() + if self.key.is_some() { TAG } else { 0 };
        PLAIN_BYTES.fetch_add(plain.len() as u64, Ordering::Relaxed);
        WIRE_BYTES.fetch_add((length + HEADER) as u64, Ordering::Relaxed);
        let mut header = [0; HEADER];
        header[..4].copy_from_slice(&(length as u32).to_le_bytes());
        header[4..].copy_from_slice(&counter.to_le_bytes());
        out.write_all(&header)?;

        match &self.key {
            Some(key) => {
                let cipher = Aegis128X2::<TAG>::new(key, &self.nonce(counter));
                let tag = cipher.encrypt_in_place(&mut body, &header);
                out.write_all(&body)?;
                out.write_all(&tag)?;
            }
            None => out.write_all(&body)?,
        }
        Ok(())
    }

    /// Seal a bounded range directly into pool leaves, preserving the wire format.
    /// LZ4 borrows single-leaf input and writes directly when its output bound fits.
    pub fn seal_segmented(
        &mut self,
        plain: &mut pool::SegmentedBuffer<'_>,
        range: std::ops::Range<u64>,
        out: &mut pool::SegmentedBuffer<'_>,
        mut random: impl FnMut() -> u64,
    ) -> Result<()> {
        // Validate before arithmetic or examining chunks; batches can have a rolling start.
        plain.reader_range(range.clone())?;
        let size = usize::try_from(range.end - range.start)
            .map_err(|_| Error::new("segment length does not fit"))?;
        if size > crate::wire::MAX_SEGMENT_BYTES + 4 {
            return Err(Error::new("plaintext segment exceeds maximum size"));
        }
        let attempt = self.compress == Compress::Lz4
            && size >= self.compress_min
            && self.worth_trying_segmented(plain, range.start, size)?;
        let counter = self.counter;
        self.counter += 1; // A failed write must not reuse this nonce.
        let start = out.end();
        let result = (|| -> Result<()> {
            let mut header = [0; HEADER];
            header[4..].copy_from_slice(&counter.to_le_bytes());
            out.append(&header, &mut random).context("sealed segment append")?;
            let body_start = out.end();
            let mut compressed = false;
            if attempt {
                out.append(&[LZ4], &mut random).context("sealed segment append")?;
                out.append(&(size as u32).to_le_bytes(), &mut random)
                    .context("sealed segment append")?;
                // Input and output select direct-leaf or virtual alias independently.
                let mut compress = |input: &[u8]| compress_into_pool(input, out, &mut random);
                let chunk = plain.chunk(range.start).unwrap_or(&[]);
                let written = if chunk.len() >= size {
                    compress(&chunk[..size])?
                } else if pool::SegmentedBuffer::alias_supported() {
                    plain
                        .with_contiguous_read(range.clone(), compress)
                        .context("compression input alias")??
                } else {
                    self.compression_input.resize(size, 0);
                    plain
                        .read_at(range.start, &mut self.compression_input)
                        .context("compression input")?;
                    compress(&self.compression_input)?
                };
                compressed = written < size;
                if !compressed {
                    out.truncate(body_start).context("compression discarded")?;
                }
            }
            if !compressed {
                out.append(&[PLAIN], &mut random).context("sealed segment append")?;
                let mut at = range.start;
                while at < range.end {
                    let chunk = plain.chunk(at).expect("validated plaintext range");
                    let n = chunk.len().min((range.end - at) as usize);
                    out.append(&chunk[..n], &mut random).context("sealed segment append")?;
                    at += n as u64;
                }
            }
            let length = out.end() - body_start + if self.key.is_some() { TAG as u64 } else { 0 };
            header[..4].copy_from_slice(&(length as u32).to_le_bytes());
            out.write_at(start, &header).context("sealed segment header")?;
            if let Some(key) = &self.key {
                let cipher = Aegis128X2::<TAG>::new(key, &self.nonce(counter));
                let body_end = out.end();
                let body_len = (body_end - body_start) as usize;
                let tag = if out.chunk(body_start).expect("nonempty sealed body").len() == body_len
                {
                    // No mapping syscall for a body wholly within one leaf.
                    let mut chunks = out
                        .chunks_mut_range(body_start..body_end)
                        .context("encryption output leaf")?;
                    cipher.encrypt_in_place(chunks.next().expect("nonempty sealed body"), &header)
                } else if pool::SegmentedBuffer::alias_supported() {
                    // Compression's callback borrow has ended. Reuse its cached
                    // aliases where the encryption body occupies the same leaves.
                    out.with_contiguous_write(body_start..body_end, |body| {
                        cipher.encrypt_in_place(body, &header)
                    })
                    .context("encryption output alias")?
                } else {
                    let mut encryptor = cipher.encryptor(&header);
                    for chunk in out
                        .chunks_mut_range(body_start..body_end)
                        .context("encryption output leaves")?
                    {
                        encryptor.update_in_place(chunk);
                    }
                    encryptor.finalize()
                };
                out.append(&tag, &mut random).context("sealed segment append")?;
            }
            Ok(())
        })();
        if result.is_err() {
            out.truncate(start).context("sealed segment rollback")?;
        }
        result?;
        PLAIN_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        WIRE_BYTES.fetch_add(out.end() - start, Ordering::Relaxed);
        Ok(())
    }

    fn worth_trying_segmented(
        &mut self,
        plain: &pool::SegmentedBuffer<'_>,
        start: u64,
        size: usize,
    ) -> Result<bool> {
        let sample = self.compress_sample;
        if sample == 0 || size < sample.saturating_mul(SAMPLE_SHARE) {
            return Ok(true);
        }
        let piece = sample / SAMPLE_PIECES;
        let stride = (size - piece) / (SAMPLE_PIECES - 1);
        self.sample_buf.resize(piece * SAMPLE_PIECES, 0);
        for n in 0..SAMPLE_PIECES {
            plain
                .read_at(
                    start + (n * stride) as u64,
                    &mut self.sample_buf[n * piece..(n + 1) * piece],
                )
                .context("compression sample")?;
        }
        Ok(lz4_flex::compress(&self.sample_buf).len() < self.sample_buf.len())
    }

    /// Whether to attempt the whole batch, judged by a few pieces of it taken
    /// from end to end. Finding out the expensive way costs a third of
    /// throughput on traffic that never compresses.
    ///
    /// ⚠️ Pieces are concatenated, which hides any redundancy that spans the gaps
    /// between them. That makes the sampler pessimistic, never optimistic.
    fn worth_trying(&mut self, plain: &[u8]) -> bool {
        let sample = self.compress_sample;
        if sample == 0 || plain.len() < sample * SAMPLE_SHARE {
            return true;
        }
        let piece = sample / SAMPLE_PIECES;
        let stride = (plain.len() - piece) / (SAMPLE_PIECES - 1);
        self.sample_buf.clear();
        for n in 0..SAMPLE_PIECES {
            let at = n * stride;
            self.sample_buf.extend_from_slice(&plain[at..at + piece]);
        }
        lz4_flex::compress(&self.sample_buf).len() < self.sample_buf.len()
    }

    /// Open a segment whose header has already been read, leaving the messages
    /// in `out`.
    pub fn open(&mut self, header: &[u8; HEADER], sealed: &[u8], out: &mut Vec<u8>) -> Result<()> {
        self.open_to(header, sealed, out)
    }

    /// Authenticate/decompress before exposing messages to the output sink.
    pub fn open_to(
        &mut self,
        header: &[u8; HEADER],
        sealed: &[u8],
        out: &mut impl Write,
    ) -> Result<()> {
        self.open_with(header, sealed, |input, expanded| {
            if let Some(size) = expanded {
                let loose = lz4_flex::decompress(input, size)
                    .map_err(|e| Error::new(format!("segment did not expand: {e}")))?;
                out.write_all(&loose)?;
            } else {
                out.write_all(input)?;
            }
            Ok(())
        })
    }

    /// Authenticate first, then expand directly into a leaf when the result fits.
    pub fn open_segmented(
        &mut self,
        header: &[u8; HEADER],
        sealed: &[u8],
        out: &mut pool::SegmentedBuffer<'_>,
        mut random: impl FnMut() -> u64,
    ) -> Result<()> {
        let start = out.end();
        let result = self.open_with(header, sealed, |input, expanded| {
            match expanded {
                Some(size) if size > 0 && fits_tail(out, size) => {
                    let dest = append_contiguous_space(out, size, &mut random)?;
                    let written = lz4_flex::block::decompress_into(input, dest)
                        .map_err(|e| Error::new(format!("segment did not expand: {e}")))?;
                    out.truncate(start + written as u64).context("expanded segment truncate")?;
                }
                Some(size) if size > 0 && pool::SegmentedBuffer::alias_supported() => {
                    append_zeroes(out, size, &mut random)?;
                    let written = out
                        .with_contiguous_write(start..start + size as u64, |dest| {
                            lz4_flex::block::decompress_into(input, dest)
                        })
                        .context("expansion output alias")?
                        .map_err(|e| Error::new(format!("segment did not expand: {e}")))?;
                    out.truncate(start + written as u64).context("expanded segment truncate")?;
                }
                Some(size) => {
                    let loose = lz4_flex::decompress(input, size)
                        .map_err(|e| Error::new(format!("segment did not expand: {e}")))?;
                    out.append(&loose, &mut random).context("expanded segment append")?;
                }
                None => out.append(input, &mut random).context("opened segment append")?,
            }
            Ok(())
        });
        if result.is_err() {
            out.truncate(start).context("opened segment rollback")?;
        }
        result
    }

    fn open_with(
        &mut self,
        header: &[u8; HEADER],
        sealed: &[u8],
        output: impl FnOnce(&[u8], Option<usize>) -> Result<()>,
    ) -> Result<()> {
        let counter = u64::from_le_bytes(header[4..12].try_into().expect("counter"));
        if counter != self.counter {
            return Err(Error::new(format!(
                "segment {counter} arrived where {} was expected",
                self.counter
            )));
        }
        self.counter += 1;

        let body = match &self.key {
            Some(key) => {
                if sealed.len() < TAG {
                    return Err(Error::new("a sealed segment shorter than its tag"));
                }
                let (cipher_text, tag) = sealed.split_at(sealed.len() - TAG);
                let tag: [u8; TAG] = tag.try_into().expect("tag");
                let cipher = Aegis128X2::<TAG>::new(key, &self.nonce(counter));
                std::borrow::Cow::Owned(
                    cipher
                        .decrypt(cipher_text, &tag, header)
                        .map_err(|e| Error::new(format!("segment {counter} did not open: {e}")))?,
                )
            }
            None => std::borrow::Cow::Borrowed(sealed),
        };

        match body.split_first() {
            Some((&PLAIN, messages)) => {
                if messages.len() > crate::wire::MAX_SEGMENT_BYTES + 4 {
                    return Err(Error::new("opened segment exceeds maximum size"));
                }
                output(messages, None)?;
            }
            Some((&LZ4, rest)) => {
                if rest.len() < 4 {
                    return Err(Error::new("a compressed segment with no size"));
                }
                let size = u32::from_le_bytes(rest[..4].try_into().expect("size")) as usize;
                if size > crate::wire::MAX_SEGMENT_BYTES + 4 {
                    return Err(Error::new("expanded segment exceeds maximum size"));
                }
                output(&rest[4..], Some(size))?;
            }
            Some((&other, _)) => {
                return Err(Error::new(format!("segment {counter} has form {other}")));
            }
            None => return Err(Error::new("an empty segment")),
        }
        Ok(())
    }
}

fn append_zeroes(
    out: &mut pool::SegmentedBuffer<'_>,
    mut size: usize,
    mut random: impl FnMut() -> u64,
) -> Result<()> {
    let zeros = [0; pool::segmented::CHUNK_BYTES];
    while size != 0 {
        let n = size.min(zeros.len());
        out.append(&zeros[..n], &mut random).context("LZ4 output space")?;
        size -= n;
    }
    Ok(())
}

fn compress_into_pool(
    input: &[u8],
    out: &mut pool::SegmentedBuffer<'_>,
    mut random: impl FnMut() -> u64,
) -> Result<usize> {
    let start = out.end();
    let bound = lz4_flex::block::get_maximum_output_size(input.len());
    let written = if fits_tail(out, bound) {
        let dest = append_contiguous_space(out, bound, &mut random)?;
        lz4_flex::block::compress_into(input, dest)
            .map_err(|e| Error::new(format!("segment did not compress: {e}")))?
    } else if pool::SegmentedBuffer::alias_supported() {
        append_zeroes(out, bound, &mut random)?;
        out.with_contiguous_write(start..start + bound as u64, |dest| {
            lz4_flex::block::compress_into(input, dest)
        })
        .context("compression output alias")?
        .map_err(|e| Error::new(format!("segment did not compress: {e}")))?
    } else {
        let bytes = lz4_flex::compress(input);
        out.append(&bytes, &mut random).context("compressed segment append")?;
        bytes.len()
    };
    out.truncate(start + written as u64).context("compression output truncate")?;
    Ok(written)
}

// LZ4 requires initialized slices. Initialize only the bounded destination in the
// current leaf; callers truncate its unused suffix after compression/expansion.
fn fits_tail(out: &pool::SegmentedBuffer<'_>, size: usize) -> bool {
    size <= pool::segmented::CHUNK_BYTES
        - (out.end() % pool::segmented::CHUNK_BYTES as u64) as usize
}

fn append_contiguous_space<'a>(
    out: &'a mut pool::SegmentedBuffer<'_>,
    size: usize,
    random: impl FnMut() -> u64,
) -> Result<&'a mut [u8]> {
    assert!(size > 0 && fits_tail(out, size));
    let start = out.end();
    out.append(&[0; pool::segmented::CHUNK_BYTES][..size], random).context("LZ4 output space")?;
    Ok(out
        .chunks_mut_range(start..out.end())
        .context("LZ4 output leaf")?
        .next()
        .expect("nonempty single-leaf output"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compress;

    fn messaging(encrypt: bool, compress: Compress) -> Messaging {
        Messaging {
            authentication: None,
            expected_peer: None,
            ring_identities: Default::default(),
            local_ring: None,
            ignore_ring_identity: false,
            send: crate::config::SendMode::Direct,
            encrypt,
            compress,
            compress_min: 64,
            buffer_pool: Default::default(),
            compress_sample: 4096,
            peer_conns: 2,
            key: "000102030405060708090a0b0c0d0e0f".to_string(),
        }
    }

    /// Seal on one side, open on the other, as the two ends of a connection do.
    fn pair(config: &Messaging) -> (Seal, Seal) {
        let prefix = [7u8; 8];
        (Seal::new(config, prefix, 0).unwrap(), Seal::new(config, prefix, 0).unwrap())
    }

    fn round_trip(config: Messaging, message: &[u8]) {
        let (mut sender, mut receiver) = pair(&config);
        let mut wire = Vec::new();
        for _ in 0..3 {
            sender.seal(&mut message.to_vec(), &mut wire).unwrap();
        }
        let mut at = 0;
        for _ in 0..3 {
            let header: [u8; HEADER] = wire[at..at + HEADER].try_into().unwrap();
            let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            let mut out = Vec::new();
            receiver.open(&header, &wire[at + HEADER..at + HEADER + length], &mut out).unwrap();
            assert_eq!(out, message);
            at += HEADER + length;
        }
        assert_eq!(at, wire.len());
    }

    #[test]
    fn segmented_encryption_matches_one_shot_at_leaf_boundaries() {
        // This tests encryption compatibility, not pool capacity. Reserve ample
        // virtual space; physical pages are supplied only as blocks are used.
        let pool =
            pool::ChunkPool::new(((4u64 << 30) / pool::segmented::CHUNK_BYTES as u64) as u32, 0)
                .unwrap();
        let mut allocation_rng = crate::random::Rng::from_seed(42);
        for encrypted in [false, true] {
            for compression in [Compress::None, Compress::Lz4] {
                for repetitive in [false, true] {
                    let config = messaging(encrypted, compression);
                    let (mut reference, _) = pair(&config);
                    let (mut sender, mut receiver) = pair(&config);
                    for len in [
                        0,
                        1,
                        31,
                        32,
                        33,
                        pool::segmented::CHUNK_BYTES - 1,
                        pool::segmented::CHUNK_BYTES,
                        pool::segmented::CHUNK_BYTES + 1,
                        pool::segmented::CHUNK_BYTES * 3 + 17,
                        35000,
                    ] {
                        let mut rng = crate::random::Rng::from_seed(17);
                        let mut data: Vec<u8> = (0..len)
                            .map(|_| if repetitive { 7 } else { rng.next_u64() as u8 })
                            .collect();
                        let mut input = pool::SegmentedBuffer::with_start(
                            &pool,
                            pool::segmented::CHUNK_BYTES as u64 - 3,
                        );
                        input.append(b"skip", || allocation_rng.next_u64()).unwrap();
                        let start = input.end();
                        input.append(&data, || allocation_rng.next_u64()).unwrap();
                        let end = input.end();
                        input.append(b"ignore", || allocation_rng.next_u64()).unwrap();
                        let mut output = pool::SegmentedBuffer::with_start(
                            &pool,
                            pool::segmented::CHUNK_BYTES as u64 - 6,
                        );
                        output.append(b"keep", || allocation_rng.next_u64()).unwrap();
                        let output_start = output.end();
                        sender
                            .seal_segmented(&mut input, start..end, &mut output, || {
                                allocation_rng.next_u64()
                            })
                            .unwrap();
                        let mut actual = vec![0; (output.end() - output_start) as usize];
                        output.read_at(output_start, &mut actual).unwrap();
                        let mut expected = Vec::new();
                        reference.seal(&mut data, &mut expected).unwrap();
                        assert_eq!(actual, expected, "length {len}, encrypted {encrypted}");
                        let mut opened = Vec::new();
                        receiver
                            .open(
                                actual[..HEADER].try_into().unwrap(),
                                &actual[HEADER..],
                                &mut opened,
                            )
                            .unwrap();
                        assert_eq!(opened, data);
                        if compression == Compress::None {
                            assert_eq!(sender.compression_input.capacity(), 0);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn lz4_input_and_output_fast_paths_are_independent() {
        let pool =
            pool::ChunkPool::new(((4u64 << 30) / pool::segmented::CHUNK_BYTES as u64) as u32, 0)
                .unwrap();
        let mut rng = crate::random::Rng::from_seed(42);
        for encrypted in [false, true] {
            for repetitive in [false, true] {
                for input_start in [0, pool::segmented::CHUNK_BYTES as u64 - 6] {
                    // The compression destination begins after the 12-byte header
                    // and 5-byte LZ4 prefix. A 96-byte tail cannot hold its bound.
                    for output_start in [0, pool::segmented::CHUNK_BYTES as u64 - 96] {
                        for opened_start in [0, pool::segmented::CHUNK_BYTES as u64 - 96] {
                            let config = messaging(encrypted, Compress::Lz4);
                            let (mut sender, mut receiver) = pair(&config);
                            let (mut reference, _) = pair(&config);
                            let mut data = vec![7; 1024];
                            if !repetitive {
                                crate::random::Rng::from_seed(17).fill_bytes(&mut data);
                            }
                            let mut input = pool::SegmentedBuffer::with_start(&pool, input_start);
                            input.append(&data, || rng.next_u64()).unwrap();
                            let mut output = pool::SegmentedBuffer::with_start(&pool, output_start);
                            output.append(b"keep", || rng.next_u64()).unwrap();
                            let start = output.end();
                            let input_range = input.start()..input.end();
                            sender
                                .seal_segmented(&mut input, input_range, &mut output, || {
                                    rng.next_u64()
                                })
                                .unwrap();
                            assert_eq!(
                                sender.compression_input.is_empty(),
                                input_start == 0 || pool::SegmentedBuffer::alias_supported()
                            );
                            let mut actual = vec![0; output.len() as usize];
                            output.read_at(output_start, &mut actual).unwrap();
                            assert_eq!(&actual[..4], b"keep");
                            let mut expected = Vec::new();
                            reference.seal(&mut data, &mut expected).unwrap();
                            assert_eq!(&actual[4..], expected);
                            assert_eq!(output.end() - start, expected.len() as u64);
                            if !encrypted {
                                assert_eq!(expected[HEADER], if repetitive { LZ4 } else { PLAIN });
                            }
                            let mut opened = pool::SegmentedBuffer::with_start(&pool, opened_start);
                            opened.append(b"keep", || rng.next_u64()).unwrap();
                            receiver
                                .open_segmented(
                                    expected[..HEADER].try_into().unwrap(),
                                    &expected[HEADER..],
                                    &mut opened,
                                    || rng.next_u64(),
                                )
                                .unwrap();
                            let mut actual = vec![0; opened.len() as usize];
                            opened.read_at(opened_start, &mut actual).unwrap();
                            assert_eq!(&actual[..4], b"keep");
                            assert_eq!(&actual[4..], data);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn segmented_open_rolls_back_failed_expansion_and_authentication() {
        let pool =
            pool::ChunkPool::new(((4u64 << 30) / pool::segmented::CHUNK_BYTES as u64) as u32, 0)
                .unwrap();
        let mut rng = crate::random::Rng::from_seed(42);
        for encrypted in [false, true] {
            for output_start in [0, pool::segmented::CHUNK_BYTES as u64 - 6] {
                let config = messaging(encrypted, Compress::Lz4);
                let (mut sender, mut receiver) = pair(&config);
                let mut wire = Vec::new();
                sender.seal(&mut vec![7; 1024], &mut wire).unwrap();
                if encrypted {
                    *wire.last_mut().unwrap() ^= 1; // Authentication must precede expansion.
                } else {
                    wire.truncate(HEADER + 5); // Missing raw LZ4 block.
                    wire[..4].copy_from_slice(&5u32.to_le_bytes());
                }
                let mut opened = pool::SegmentedBuffer::with_start(&pool, output_start);
                opened.append(b"keep", || rng.next_u64()).unwrap();
                assert!(
                    receiver
                        .open_segmented(
                            wire[..HEADER].try_into().unwrap(),
                            &wire[HEADER..],
                            &mut opened,
                            || rng.next_u64()
                        )
                        .is_err()
                );
                assert_eq!(opened.len(), 4);
                let mut actual = [0; 4];
                opened.read_at(output_start, &mut actual).unwrap();
                assert_eq!(&actual, b"keep");
            }
        }
    }

    #[test]
    fn virtual_scratch_handles_maximum_segment_with_unaligned_ends() {
        if !pool::SegmentedBuffer::alias_supported() {
            return;
        }
        let pool =
            pool::ChunkPool::new(((4u64 << 30) / pool::segmented::CHUNK_BYTES as u64) as u32, 0)
                .unwrap();
        let mut rng = crate::random::Rng::from_seed(42);
        let size = crate::wire::MAX_SEGMENT_BYTES + 4;
        let mut input =
            pool::SegmentedBuffer::with_start(&pool, pool::segmented::CHUNK_BYTES as u64 - 1);
        let chunk = [7; pool::segmented::CHUNK_BYTES];
        for _ in 0..size / chunk.len() {
            input.append(&chunk, || rng.next_u64()).unwrap();
        }
        input.append(&chunk[..size % chunk.len()], || rng.next_u64()).unwrap();
        let (mut sender, mut receiver) = pair(&messaging(false, Compress::Lz4));
        let mut output =
            pool::SegmentedBuffer::with_start(&pool, pool::segmented::CHUNK_BYTES as u64 - 2);
        let input_range = input.start()..input.end();
        sender.seal_segmented(&mut input, input_range, &mut output, || rng.next_u64()).unwrap();
        assert!(sender.compression_input.is_empty());
        let mut sealed = vec![0; output.len() as usize];
        output.read_at(output.start(), &mut sealed).unwrap();
        assert_eq!(sealed[HEADER], LZ4);
        let mut opened =
            pool::SegmentedBuffer::with_start(&pool, pool::segmented::CHUNK_BYTES as u64 - 3);
        receiver
            .open_segmented(
                sealed[..HEADER].try_into().unwrap(),
                &sealed[HEADER..],
                &mut opened,
                || rng.next_u64(),
            )
            .unwrap();
        assert_eq!(opened.len(), size as u64);
        let mut at = opened.start();
        while at < opened.end() {
            let bytes = opened.chunk(at).unwrap();
            assert!(bytes.iter().all(|&b| b == 7));
            at += bytes.len() as u64;
        }
    }

    #[test]
    fn rejected_compression_sample_never_flattens_input() {
        let pool = pool::ChunkPool::new(128, 0).unwrap();
        let mut allocation_rng = crate::random::Rng::from_seed(42);
        let (mut sender, _) = pair(&messaging(true, Compress::Lz4));
        let mut rng = crate::random::Rng::from_seed(29);
        let bytes: Vec<u8> = (0..40000).map(|_| rng.next_u64() as u8).collect();
        let mut input = pool::SegmentedBuffer::new(&pool);
        input.append(&bytes, || allocation_rng.next_u64()).unwrap();
        let mut out = pool::SegmentedBuffer::new(&pool);
        let input_end = input.end();
        sender
            .seal_segmented(&mut input, 0..input_end, &mut out, || allocation_rng.next_u64())
            .unwrap();
        assert_eq!(sender.compression_input.capacity(), 0);
    }

    #[test]
    fn encryption_pool_exhaustion_rolls_back_output_without_reusing_counter() {
        let source_pool = pool::ChunkPool::new(4, 0).unwrap();
        let output_pool = pool::ChunkPool::new(1, 0).unwrap();
        let mut input = pool::SegmentedBuffer::new(&source_pool);
        input.append(&vec![1; pool::segmented::CHUNK_BYTES], || 0).unwrap();
        let mut out = pool::SegmentedBuffer::new(&output_pool);
        out.append(b"prefix", || 0).unwrap();
        let (mut sender, _) = pair(&messaging(true, Compress::None));
        assert!(
            sender
                .seal_segmented(&mut input, 0..pool::segmented::CHUNK_BYTES as u64, &mut out, || 0)
                .is_err()
        );
        let mut prefix = [0; 6];
        out.read_at(0, &mut prefix).unwrap();
        assert_eq!(&prefix, b"prefix");
        assert_eq!(out.end(), 6);
        assert_eq!(sender.counter, 1);
        sender.seal_segmented(&mut input, 0..10, &mut out, || 0).unwrap();
        let mut header = [0; HEADER];
        out.read_at(6, &mut header).unwrap();
        assert_eq!(u64::from_le_bytes(header[4..].try_into().unwrap()), 1);
    }

    #[test]
    fn oversized_expansion_is_rejected_before_allocation() {
        let (_, mut receiver) = pair(&messaging(false, Compress::Lz4));
        let mut body = vec![LZ4];
        body.extend(((crate::wire::MAX_SEGMENT_BYTES + 5) as u32).to_le_bytes());
        let mut header = [0; HEADER];
        header[..4].copy_from_slice(&(body.len() as u32).to_le_bytes());
        let mut out = Vec::new();
        assert!(
            receiver
                .open(&header, &body, &mut out)
                .unwrap_err()
                .to_string()
                .contains("maximum size")
        );
        assert!(out.is_empty());
    }

    #[test]
    fn sealed_round_trip() {
        round_trip(messaging(true, Compress::None), b"one two three");
    }

    #[test]
    fn compressed_round_trip() {
        round_trip(messaging(true, Compress::Lz4), &b"the same words over and over ".repeat(40));
    }

    #[test]
    fn bare_round_trip() {
        round_trip(messaging(false, Compress::None), b"in the clear");
    }

    #[test]
    fn incompressible_stays_plain() {
        let config = messaging(false, Compress::Lz4);
        let mut noise = vec![0u8; 4096];
        crate::random::Rng::from_seed(1).fill_bytes(&mut noise);
        let (mut sender, _) = pair(&config);
        let mut wire = Vec::new();
        sender.seal(&mut noise.clone(), &mut wire).unwrap();
        assert_eq!(wire[HEADER], PLAIN, "random bytes must not be stored larger");
    }

    #[test]
    fn a_tampered_segment_will_not_open() {
        let config = messaging(true, Compress::None);
        let (mut sender, mut receiver) = pair(&config);
        let mut wire = Vec::new();
        sender.seal(&mut b"honest".to_vec(), &mut wire).unwrap();
        let last = wire.len() - 1;
        wire[last] ^= 1;
        let header: [u8; HEADER] = wire[..HEADER].try_into().unwrap();
        assert!(receiver.open(&header, &wire[HEADER..], &mut Vec::new()).is_err());
    }

    #[test]
    fn a_segment_out_of_order_will_not_open() {
        let config = messaging(true, Compress::None);
        let (mut sender, mut receiver) = pair(&config);
        let mut first = Vec::new();
        let mut second = Vec::new();
        sender.seal(&mut b"one".to_vec(), &mut first).unwrap();
        sender.seal(&mut b"two".to_vec(), &mut second).unwrap();
        let header: [u8; HEADER] = second[..HEADER].try_into().unwrap();
        assert!(receiver.open(&header, &second[HEADER..], &mut Vec::new()).is_err());
    }
}

#[cfg(test)]
mod ratio {
    use crate::random::Rng;

    /// A batch of replies with random bodies must not compress. If it does, a
    /// measurement of compression is measuring the padding instead of the wire.
    #[test]
    fn random_bodies_do_not_compress() {
        let mut block = vec![0u8; 8 << 20];
        Rng::new().fill_bytes(&mut block);
        for size in [1024usize, 16384] {
            let mut batch = Vec::new();
            for slot in 0..64 {
                let at = slot * size;
                batch.extend((size as u32).to_le_bytes());
                batch.extend_from_slice(&block[at..at + size]);
            }
            let squeezed = lz4_flex::compress(&batch).len();
            println!(
                "size {size}: {} -> {squeezed} ({:.2}x)",
                batch.len(),
                batch.len() as f64 / squeezed as f64
            );
            assert!(squeezed as f64 > batch.len() as f64 * 0.98, "random should not shrink");
        }
    }
}

#[cfg(test)]
mod sampling {
    use super::*;
    use crate::config::{Compress, SendMode};
    use crate::random::Rng;

    fn config(sample: usize) -> Messaging {
        Messaging {
            authentication: None,
            expected_peer: None,
            ring_identities: Default::default(),
            local_ring: None,
            ignore_ring_identity: false,
            send: SendMode::Direct,
            encrypt: false,
            compress: Compress::Lz4,
            compress_min: 64,
            buffer_pool: Default::default(),
            compress_sample: sample,
            peer_conns: 2,
            key: "000102030405060708090a0b0c0d0e0f".to_string(),
        }
    }

    fn sealed_size(config: &Messaging, plain: &[u8]) -> usize {
        let mut out = Vec::new();
        Seal::new(config, [1u8; 8], 0).unwrap().seal(&mut plain.to_vec(), &mut out).unwrap();
        out.len()
    }

    fn noise(bytes: usize, seed: u64) -> Vec<u8> {
        let mut noise = vec![0u8; bytes];
        Rng::from_seed(seed).fill_bytes(&mut noise);
        noise
    }

    /// Nothing to find either way, so the sizes match; what the sample saves is
    /// work, which the wire cannot show.
    #[test]
    fn noise_is_left_alone() {
        let noise = noise(1 << 20, 9);
        assert_eq!(sealed_size(&config(4096), &noise), sealed_size(&config(0), &noise));
    }

    #[test]
    fn repetition_gets_through() {
        let dull = b"over and over ".repeat(1 << 16);
        let sampled = sealed_size(&config(4096), &dull);
        assert!(sampled < dull.len() / 50, "{sampled} of {}", dull.len());
        assert_eq!(sampled, sealed_size(&config(0), &dull));
    }

    /// Below eight samples' worth there is nothing to save, so it is compressed
    /// outright rather than judged.
    #[test]
    fn a_small_batch_is_not_judged() {
        let dull = b"over and over ".repeat(1024);
        assert!(dull.len() < 4096 * SAMPLE_SHARE);
        assert!(sealed_size(&config(4096), &dull) < dull.len() / 50);
    }

    /// The reason the sample is spread rather than a prefix: a batch holds
    /// whatever was pending, and the compressible part may be anywhere in it.
    #[test]
    fn compressible_at_either_end_is_found() {
        let dull = b"over and over ".repeat(1 << 14);
        for (head, tail) in [(noise(1 << 18, 3), dull.clone()), (dull.clone(), noise(1 << 18, 4))] {
            let mut batch = head;
            batch.extend(tail);
            assert_eq!(
                sealed_size(&config(4096), &batch),
                sealed_size(&config(0), &batch),
                "a spread sample should reach it wherever it is"
            );
        }
    }
}
