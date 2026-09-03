//! Compressing an append block, which the client does and nothing else undoes.
//!
//! A block is compressed by the writer and stays that way: an FE passes it along,
//! an EN stores it and hands it back, and the reader that decompresses it is the
//! same client that compressed it. So compression is not a stage in a pipeline of
//! machines — it is a representation the data is in from the moment it leaves the
//! writer's buffer until it reaches a reader's.
//!
//! **Not in place.** The original buffer survives compression so that it can be
//! checked against the CRC afterwards, which is what catches a corruption that
//! happened before or during the compression itself. Compressing over the top of
//! the evidence would make that check impossible.
//!
//! ⚠️ **A compressed block is opaque below the client.** Nothing between the writer
//! and the reader can see records in it, count them, or find a boundary — so what a
//! block *contains* has to travel beside it as numbers the writer supplies: how many
//! logical bytes, how many records it completed, and whether it left one open.

use crate::config::Compress;
use crate::framing::Framing;
use crate::offset::{ECoff, ELoff, ERoff};
use crate::wire::{StructReader, StructWriter};
use crate::{Error, Result};

/// The version of the extent on-disk layout.
pub const EXTENT_VERSION: u32 = 1;

/// The fixed overhead of an append block on disk:
/// Plaintext Holds (12 bytes) + AEGIS-128X2 tag (16 bytes) + Encrypted Header (16 bytes: CRC u64 + Framing u32/u32).
pub const APPEND_OVERHEAD: usize = 44;

/// What a block holds, as the writer knows it and as everything below has to be
/// told. None of it is recoverable from the compressed bytes without decompressing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Holds {
    /// The actual physical length of the append block on disk:
    /// `APPEND_OVERHEAD` + the length of the compressed user data.
    pub compressed: ECoff,
    /// Bytes before compression, in canonical logical numbering (`body + records * 4`).
    pub logical: ELoff,
    /// Records this block completed, which is what numbers them.
    pub records: ERoff,
}

impl Holds {
    /// On the wire, as three arguments. Written and read here so that no two
    /// processes count positions independently — the mistake §17.13.3 records.
    pub fn args(&self) -> Vec<crate::wire::Arg> {
        use crate::wire::Arg;
        vec![
            Arg::Number(self.compressed.number()),
            Arg::Number(self.logical.number()),
            Arg::Number(self.records.number()),
        ]
    }

    /// And back, returning whatever followed it.
    pub fn of_args(args: &[crate::wire::Arg]) -> Result<(Holds, &[crate::wire::Arg])> {
        use crate::wire::Arg;
        let [Arg::Number(compressed), Arg::Number(logical), Arg::Number(records), rest @ ..] = args
        else {
            return Err(Error::new("a block does not say what it holds"));
        };
        Ok((
            Holds {
                compressed: ECoff::of_number(*compressed)?,
                logical: ELoff::of_number(*logical)?,
                records: ERoff::of_number(*records)?,
            },
            rest,
        ))
    }

    pub fn write(&self, payload: &mut StructWriter) {
        payload.tag(crate::wire::Op::AppendBlock);
        payload.number_u64(self.compressed.number());
        payload.number_u64(self.logical.number());
        payload.number_u64(self.records.number());
    }

    pub fn read(payload: &mut StructReader<'_>) -> Result<Holds> {
        payload.tag(crate::wire::Op::AppendBlock, "block holds")?;
        Ok(Holds {
            compressed: ECoff::of_number(payload.number_u64()?)?,
            logical: ELoff::of_number(payload.number_u64()?)?,
            records: ERoff::of_number(payload.number_u64()?)?,
        })
    }

    /// Measure a framed block, given the length of the compressed user data.
    /// The physical compressed size on disk is the user data length plus `APPEND_OVERHEAD`.
    pub fn of(
        framing: Framing,
        open: &[u8],
        block: &[u8],
        user_compressed_len: usize,
    ) -> Result<Holds> {
        let (records, _) = framing.advance(open, block)?;
        let logical = ELoff::of_number(framing.canonical_size(open.len(), block)?)?;
        let records = ERoff::of_number(records)?;
        let compressed = ECoff::of_len(user_compressed_len + APPEND_OVERHEAD)?;
        Ok(Holds { compressed, logical, records })
    }
}

/// Compress a block, leaving the original alone. `None` means it was not worth it —
/// the caller stores what it had.
pub fn squeeze(how: Compress, block: &[u8]) -> Option<Vec<u8>> {
    match how {
        Compress::None => None,
        // Bigger is a real outcome on incompressible bytes, and paying for it twice
        // — once in space and once in the decompression — is worse than not trying.
        Compress::Lz4 => {
            let squeezed = lz4_flex::compress(block);
            (squeezed.len() < block.len()).then_some(squeezed)
        }
    }
}

/// Undo it, given how many uncompressed bytes to expect. The length comes from the
/// block's own record of itself rather than from the bytes, since a caller that
/// has lost it cannot ask them.
pub fn expand(how: Compress, stored: &[u8], expected: usize) -> Result<Vec<u8>> {
    if stored.len() == expected {
        // Stored as it stands, because compressing it did not help.
        return Ok(stored.to_vec());
    }
    match how {
        Compress::None => Err(Error::new(format!(
            "{} stored bytes for {expected} expected, with nothing to have compressed them",
            stored.len()
        ))),
        Compress::Lz4 => {
            let block = lz4_flex::decompress(stored, expected)
                .map_err(|e| Error::new(format!("a block would not decompress: {e}")))?;
            // The codec takes the length as a bound rather than a promise, so a
            // block that came out a different size is checked here — a wrong length
            // is either a corrupted one or a lying one, and both are worth saying.
            if block.len() != expected {
                return Err(Error::new(format!(
                    "a block said {expected} bytes and decompressed to {}",
                    block.len()
                )));
            }
            Ok(block)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(count: usize) -> Vec<u8> {
        (0..count).flat_map(|n| format!("record {n}, padded out a little\n").into_bytes()).collect()
    }

    #[test]
    fn a_block_comes_back_as_it_went_in() {
        let block = lines(50);
        let stored = squeeze(Compress::Lz4, &block).expect("that should compress");
        assert!(stored.len() < block.len());
        assert_eq!(expand(Compress::Lz4, &stored, block.len()).unwrap(), block);
    }

    /// The original is untouched, which is what lets the writer check it against its
    /// CRC afterwards and catch a corruption that happened on the way through.
    #[test]
    fn compressing_leaves_the_original_to_be_checked() {
        let block = lines(20);
        let before = crate::crc::of(&block);
        let stored = squeeze(Compress::Lz4, &block).expect("compressible");
        assert_eq!(crate::crc::of(&block), before, "the buffer is still there to check");
        assert_ne!(crate::crc::of(&stored), before, "and the stored form is other bytes");
    }

    /// Incompressible bytes are stored as they are rather than larger.
    #[test]
    fn what_does_not_compress_is_left_alone() {
        let mut noise = vec![0u8; 4096];
        crate::random::Rng::new().fill_bytes(&mut noise);
        assert!(squeeze(Compress::Lz4, &noise).is_none());
        // And a block stored uncompressed reads back without a codec.
        assert_eq!(expand(Compress::Lz4, &noise, noise.len()).unwrap(), noise);
        assert_eq!(expand(Compress::None, &noise, noise.len()).unwrap(), noise);
    }

    #[test]
    fn nothing_compresses_when_nothing_is_asked_to() {
        assert!(squeeze(Compress::None, &lines(50)).is_none());
    }

    /// What a block holds is measured by the writer, because below it the bytes are
    /// opaque: the records, the logical size, and the compressed size.
    #[test]
    fn a_writer_measures_what_below_cannot_see() {
        let block = lines(3);
        let holds = Holds::of(Framing::Lf, b"", &block, block.len()).unwrap();
        assert_eq!(holds.records, ERoff::new(3));
        // Lf overhead is 1. body is lines(3).len() - 3.
        // canonical = (len - 3) + 3 * 4 = len + 9.
        assert_eq!(holds.logical, ELoff::of_number((block.len() + 9) as u64).unwrap());
        assert_eq!(holds.compressed, ECoff::of_len(block.len() + APPEND_OVERHEAD).unwrap());

        // A block that leaves a record open closes nothing.
        let partial = Holds::of(Framing::Lf, b"", b"half a rec", 10).unwrap();
        assert_eq!(partial.records, ERoff::ZERO);

        // And one that closes a record then starts another is still refused here,
        // where the framing is still visible.
        assert!(Holds::of(Framing::Lf, b"", b"one\ntw", 6).is_err());
    }

    /// A logical length that does not match what was stored, for a block nothing
    /// compressed, is a claim the bytes contradict.
    #[test]
    fn a_wrong_logical_length_is_caught() {
        let block = lines(10);
        let stored = squeeze(Compress::Lz4, &block).expect("compressible");
        assert!(expand(Compress::Lz4, &stored, 7).is_err(), "too small to be right");
        assert!(expand(Compress::Lz4, &stored, 1 << 20).is_err(), "too large to be right");
    }
}
