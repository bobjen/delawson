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
use crate::{Error, Result};
use aegis::aegis128x2::Aegis128X2;
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
    pub fn seal(&mut self, plain: &mut Vec<u8>, out: &mut Vec<u8>) -> Result<()> {
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
        let start = out.len();
        out.extend((length as u32).to_le_bytes());
        out.extend(counter.to_le_bytes());
        let header: [u8; HEADER] = out[start..start + HEADER].try_into().expect("header");

        match &self.key {
            Some(key) => {
                let cipher = Aegis128X2::<TAG>::new(key, &self.nonce(counter));
                let (sealed, tag) = cipher.encrypt(&body, &header);
                out.extend(sealed);
                out.extend(tag);
            }
            None => out.extend(&body),
        }
        Ok(())
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
                cipher
                    .decrypt(cipher_text, &tag, header)
                    .map_err(|e| Error::new(format!("segment {counter} did not open: {e}")))?
            }
            None => sealed.to_vec(),
        };

        match body.split_first() {
            Some((&PLAIN, messages)) => out.extend_from_slice(messages),
            Some((&LZ4, rest)) => {
                if rest.len() < 4 {
                    return Err(Error::new("a compressed segment with no size"));
                }
                let size = u32::from_le_bytes(rest[..4].try_into().expect("size")) as usize;
                let loose = lz4_flex::decompress(&rest[4..], size)
                    .map_err(|e| Error::new(format!("segment {counter} did not expand: {e}")))?;
                out.extend_from_slice(&loose);
            }
            Some((&other, _)) => {
                return Err(Error::new(format!("segment {counter} has form {other}")));
            }
            None => return Err(Error::new("an empty segment")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compress;

    fn messaging(encrypt: bool, compress: Compress) -> Messaging {
        Messaging {
            send: crate::config::SendMode::Direct,
            encrypt,
            compress,
            compress_min: 64,
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
        crate::random::Rng::from_seed(1).fill(&mut noise);
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
        Rng::new().fill(&mut block);
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
            send: SendMode::Direct,
            encrypt: false,
            compress: Compress::Lz4,
            compress_min: 64,
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
        Rng::from_seed(seed).fill(&mut noise);
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
