//! CRC-64/NVME, which is the user's guarantee that the bytes stored are the bytes
//! the user's buffer held.
//!
//! Encryption already detects a changed byte on the wire or on a disk, and that is
//! not the same promise: it covers a byte range the system had already accepted. A
//! CRC computed by the *caller*, on the caller's buffer, before the caller's own
//! validation, covers everything between the caller's memory and what was stored.
//!
//! **The value is over logical bytes in a stated row format** — not the compressed
//! or encrypted form, which are the system's representations of the data rather than
//! the data. So it survives every representation change the pipeline makes, and a
//! stored CRC is only interpretable alongside the framing it was computed over.
//!
//! **Initialization builds tables, so it happens once per process.** That is true of
//! the fast implementation more than of this one, and it is why nothing here asks a
//! caller to hold a CRC object: there is one per process, built on first use.
//!
//! The implementation is `crc64-nvme`, a port of the Cosmos 64-bit CRC authorized by
//! Microsoft, bitwise compatible with the CRC Azure uses, at about 16 GB/s
//! single-threaded. It is a dependency rather than a copy — and one we own, so where
//! it needs work (its fast path is x86 carryless multiply, so an ARM branch wants
//! doing properly on an ARM machine) that is our work rather than somebody's to wait
//! for.

use crc64_nvme::Crc;

/// The one per process. **Initialization builds big tables**, so it happens once, on
/// first use, and nothing asks a caller to hold one.
fn crc() -> &'static Crc {
    static CRC: std::sync::OnceLock<Crc> = std::sync::OnceLock::new();
    CRC.get_or_init(Crc::new)
}

/// The CRC of a buffer, which is what a caller computes and passes to an append.
pub fn of(bytes: &[u8]) -> u64 {
    crc().compute(bytes, 0)
}

/// Carry on from a previous value, for a buffer that arrives in pieces — which is
/// what lets a writer checksum as it fills one. Start from zero: `continuing(0, b)`
/// is `of(b)`.
pub fn continuing(so_far: u64, bytes: &[u8]) -> u64 {
    crc().compute(bytes, so_far)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The catalogue's check value: the CRC of the nine bytes "123456789". If this
    /// matches, the polynomial and the reflection are right — and the values agree
    /// with Azure's, with Cosmos's, and with the SIMD implementation this stands in
    /// for.
    #[test]
    fn the_catalogue_check_value() {
        assert_eq!(of(b"123456789"), 0xae8b_1486_0a79_9888);
    }

    #[test]
    fn an_empty_buffer_has_a_crc_and_it_is_zero() {
        assert_eq!(of(b""), 0);
    }

    /// A single flipped bit anywhere has to change the value — which is the whole
    /// point, and the property a mistyped polynomial would quietly lose.
    #[test]
    fn one_flipped_bit_changes_it() {
        let record = b"the bytes a caller meant to store, framed and ready";
        let clean = of(record);
        for at in 0..record.len() {
            for bit in 0..8 {
                let mut bent = record.to_vec();
                bent[at] ^= 1 << bit;
                assert_ne!(of(&bent), clean, "byte {at} bit {bit} went unnoticed");
            }
        }
    }

    /// Length is part of what is covered, so trailing zeros are not free.
    #[test]
    fn a_longer_buffer_of_the_same_bytes_differs() {
        assert_ne!(of(b"record"), of(b"record\0"));
        assert_ne!(of(b""), of(b"\0"));
    }

    /// A buffer arriving in pieces gives the same answer as one that did not, which
    /// is what lets a writer checksum as it fills a buffer.
    #[test]
    fn piecewise_is_the_same_as_all_at_once() {
        let whole = b"one\ntwo\nthree\nfour\n";
        let mut crc = 0;
        for piece in [&whole[..4], &whole[4..8], &whole[8..]] {
            crc = continuing(crc, piece);
        }
        assert_eq!(crc, of(whole));
        assert_eq!(continuing(0, whole), of(whole), "nothing so far is zero");
    }

    /// Two different framings of the same records are different bytes, so they have
    /// different CRCs — which is why a stored CRC is worthless without the row format
    /// it was computed over.
    #[test]
    fn framing_is_part_of_what_is_covered() {
        assert_ne!(of(b"one\ntwo\n"), of(b"one\r\ntwo\r\n"));
        assert_ne!(of(b"one\0two\0"), of(b"one\ntwo\n"));
    }
}
