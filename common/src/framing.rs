//! Where one record ends and the next begins.
//!
//! An object declares its framing when it is created, and appends are stored in
//! the format the writer gave them. Five of them, differing only in what marks a
//! boundary and what that costs per record:
//!
//! | Framing | Boundary | Overhead |
//! |---|---|---|
//! | fixed | every record the same declared width | 0 |
//! | nul | a terminating `0x00` | 1 |
//! | lf | a terminating `\n` | 1 |
//! | crlf | a terminating `\r\n` | 2 |
//! | length | a four-byte length before each record | 4 |
//!
//! **The boundary bytes are part of the logical length**, so a 100-byte record in
//! `crlf` occupies 102 logical bytes and the cumulative totals include the framing.
//!
//! A record's *content* is what a reader gets back, without the terminator or the
//! length that found it. Framing is how the records happen to be laid out, not a
//! property of the data — a reader may ask for a different one than the writer
//! used, which is why the overhead per record is a constant worth naming.

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// Every record the declared width, so a boundary is arithmetic.
    Fixed(u32),
    Nul,
    Lf,
    CrLf,
    /// A four-byte little-endian length, then that many bytes.
    Length,
}

impl Framing {
    /// The name used on a command line and in a configuration. `fixed` carries its
    /// width, since a width is part of the framing rather than a separate knob.
    pub fn parse(text: &str) -> Result<Framing> {
        if let Some(width) = text.strip_prefix("fixed:") {
            let width: u32 = width
                .parse()
                .map_err(|_| Error::new(format!("fixed framing wants a width, not {width:?}")))?;
            if width == 0 {
                return Err(Error::new("fixed framing with a width of zero frames nothing"));
            }
            return Ok(Framing::Fixed(width));
        }
        Ok(match text {
            "nul" => Framing::Nul,
            "lf" => Framing::Lf,
            "crlf" => Framing::CrLf,
            "length" => Framing::Length,
            _ => {
                return Err(Error::new(format!(
                    "{text:?} is not a framing: nul, lf, crlf, length or fixed:WIDTH"
                )));
            }
        })
    }

    /// Bytes a record costs beyond its content.
    pub fn overhead(self) -> u32 {
        match self {
            Framing::Fixed(_) => 0,
            Framing::Nul | Framing::Lf => 1,
            Framing::CrLf => 2,
            Framing::Length => 4,
        }
    }

    /// Two numbers, so a framing can travel with a request. A width of zero for
    /// the framings that have no width.
    pub fn wire(self) -> (u64, u64) {
        match self {
            Framing::Fixed(width) => (0, width as u64),
            Framing::Nul => (1, 0),
            Framing::Lf => (2, 0),
            Framing::CrLf => (3, 0),
            Framing::Length => (4, 0),
        }
    }

    pub fn of_wire(kind: u64, width: u64) -> Result<Framing> {
        Ok(match kind {
            0 => Framing::Fixed(
                u32::try_from(width)
                    .ok()
                    .filter(|width| *width > 0)
                    .ok_or_else(|| Error::new(format!("{width} is not a record width")))?,
            ),
            1 => Framing::Nul,
            2 => Framing::Lf,
            3 => Framing::CrLf,
            4 => Framing::Length,
            _ => return Err(Error::new(format!("unknown framing {kind}"))),
        })
    }

    /// The records in a block, each as its content without the boundary. Stops at
    /// the last whole record: what is left over is a fragment, which `whole` is for.
    pub fn records<'a>(self, block: &'a [u8]) -> Records<'a> {
        Records { framing: self, block, at: 0 }
    }

    /// How many whole records are in a block, and how many bytes they took. The
    /// remainder is a fragment of a record that has not all arrived.
    pub fn measure(self, block: &[u8]) -> (u64, usize) {
        let mut records = self.records(block);
        let mut count = 0;
        while records.next().is_some() {
            count += 1;
        }
        (count, records.at)
    }

    /// Every byte accounted for, or an error saying how much was left over. What an
    /// extent must be, and what the client's own appends always are.
    pub fn whole(self, block: &[u8]) -> Result<u64> {
        let (count, used) = self.measure(block);
        if used != block.len() {
            return Err(Error::new(format!(
                "{} bytes end {} bytes into a record",
                block.len(),
                block.len() - used
            )));
        }
        Ok(count)
    }

    /// What a block does to a stream of records: how many it completes, and the
    /// bytes of a record still open at the end. `open` is what a previous block
    /// left.
    ///
    /// **A block ends on a record boundary or holds no boundary at all.** A record
    /// may fail to end in its block only if it began at the start of that block or
    /// in an earlier one — so never several records and half of another. That is
    /// what makes a block indivisible: it can go wholly into this extent or wholly
    /// into a fresh one, and sealing never has to cut a record in half.
    pub fn advance(self, open: &[u8], block: &[u8]) -> Result<(u64, Vec<u8>)> {
        let joined = if open.is_empty() { Vec::new() } else { [open, block].concat() };
        let bytes = if open.is_empty() { block } else { &joined };
        let (records, used) = self.measure(bytes);
        let carry = bytes[used..].to_vec();
        if !carry.is_empty() && records > 0 {
            return Err(Error::new(format!(
                "an append closing {records} records may not end {} bytes into another",
                carry.len()
            )));
        }
        Ok((records, carry))
    }

    /// The size this block would occupy in `length` framing, which is the
    /// canonical logical space.
    pub fn canonical_size(self, open_len: usize, block: &[u8]) -> Result<u64> {
        // Use dummy open bytes to find record boundaries.
        let (records, _) = self.advance(&vec![0u8; open_len], block)?;
        let overhead = self.overhead() as usize;

        let body_len = if records > 0 {
            block.len() - (records as usize * overhead)
        } else {
            match self {
                Framing::Fixed(_) => block.len(),
                Framing::Nul | Framing::Lf | Framing::CrLf => block.len(),
                Framing::Length => {
                    if open_len >= 4 {
                        block.len()
                    } else {
                        block.len().saturating_sub(4 - open_len)
                    }
                }
            }
        };
        Ok(body_len as u64 + records * 4)
    }
}

impl std::fmt::Display for Framing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Framing::Fixed(width) => write!(f, "fixed:{width}"),
            Framing::Nul => f.write_str("nul"),
            Framing::Lf => f.write_str("lf"),
            Framing::CrLf => f.write_str("crlf"),
            Framing::Length => f.write_str("length"),
        }
    }
}

/// Walks the records of a block. `at` is how far the whole records reached, which
/// is what says whether anything was left over.
pub struct Records<'a> {
    framing: Framing,
    block: &'a [u8],
    at: usize,
}

impl<'a> Iterator for Records<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let left = self.block.get(self.at..)?;
        if left.is_empty() {
            return None;
        }
        let (content, used) = match self.framing {
            Framing::Fixed(width) => {
                let width = width as usize;
                (left.get(..width)?, width)
            }
            Framing::Nul | Framing::Lf => {
                let mark = if self.framing == Framing::Nul { 0 } else { b'\n' };
                let end = left.iter().position(|byte| *byte == mark)?;
                (&left[..end], end + 1)
            }
            Framing::CrLf => {
                let end = left.windows(2).position(|pair| pair == b"\r\n")?;
                (&left[..end], end + 2)
            }
            Framing::Length => {
                let length = left.get(..4)?;
                let length = u32::from_le_bytes(length.try_into().expect("four bytes")) as usize;
                (left.get(4..4 + length)?, 4 + length)
            }
        };
        self.at += used;
        Some(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The same three records in each framing, so the shapes can be compared.
    fn three(framing: Framing) -> Vec<u8> {
        match framing {
            Framing::Fixed(_) => b"aaabbbccc".to_vec(),
            Framing::Nul => b"one\0two\0three\0".to_vec(),
            Framing::Lf => b"one\ntwo\nthree\n".to_vec(),
            Framing::CrLf => b"one\r\ntwo\r\nthree\r\n".to_vec(),
            Framing::Length => {
                let mut block = Vec::new();
                for word in ["one", "two", "three"] {
                    block.extend((word.len() as u32).to_le_bytes());
                    block.extend(word.as_bytes());
                }
                block
            }
        }
    }

    fn every() -> [Framing; 5] {
        [Framing::Fixed(3), Framing::Nul, Framing::Lf, Framing::CrLf, Framing::Length]
    }

    #[test]
    fn every_framing_finds_its_three_records() {
        for framing in every() {
            let block = three(framing);
            assert_eq!(framing.whole(&block).unwrap(), 3, "{framing}");
            let records: Vec<&[u8]> = framing.records(&block).collect();
            assert_eq!(records.len(), 3, "{framing}");
            // Fixed-width records are padded rather than words, so only their
            // count is comparable.
            if framing != Framing::Fixed(3) {
                assert_eq!(records[0], b"one", "{framing}");
                assert_eq!(records[2], b"three", "{framing}");
            }
        }
    }

    /// The content comes back without the boundary that found it, and the boundary
    /// is still part of what the block cost.
    #[test]
    fn content_excludes_the_boundary_but_the_length_includes_it() {
        let block = three(Framing::CrLf);
        let records: Vec<&[u8]> = Framing::CrLf.records(&block).collect();
        let content: usize = records.iter().map(|record| record.len()).sum();
        assert_eq!(content, 11, "one, two and three");
        assert_eq!(block.len(), content + 3 * Framing::CrLf.overhead() as usize);
    }

    /// A whole extent must account for every byte, and this is what says so.
    #[test]
    fn a_span_ending_mid_record_is_not_whole() {
        for (framing, block) in [
            (Framing::Lf, b"one\ntwo".to_vec()),
            (Framing::Nul, b"one\0two".to_vec()),
            (Framing::CrLf, b"one\r\ntwo\r".to_vec()),
            (Framing::Fixed(3), b"aaabb".to_vec()),
            (Framing::Length, vec![3, 0, 0, 0, b'o', b'n']),
        ] {
            let error = framing.whole(&block).expect_err(&format!("{framing} should refuse"));
            assert!(error.to_string().contains("end"), "{framing}: {error}");
        }
    }

    /// And the whole records before the fragment are still countable, which is what
    /// supporting fragments will need.
    #[test]
    fn a_fragment_leaves_the_records_before_it_intact() {
        let (count, used) = Framing::Lf.measure(b"one\ntwo\nthr");
        assert_eq!((count, used), (2, 8));
    }

    #[test]
    fn an_empty_block_holds_no_records() {
        for framing in every() {
            assert_eq!(framing.whole(&[]).unwrap(), 0, "{framing}");
        }
    }

    /// A record may be empty, and it is still a record: two terminators in a row
    /// are two records, not one.
    #[test]
    fn an_empty_record_is_still_a_record() {
        assert_eq!(Framing::Lf.whole(b"\n\n").unwrap(), 2);
        assert_eq!(Framing::Length.whole(&[0, 0, 0, 0]).unwrap(), 1);
        let empty: Vec<&[u8]> = Framing::Lf.records(b"\n\n").collect();
        assert_eq!(empty, [b"", b""]);
    }

    /// Binary content is what length and fixed framing are for: a terminator inside
    /// a record would end it early, which is why the others are not general.
    #[test]
    fn length_framing_carries_bytes_a_terminator_would_cut() {
        let mut block = Vec::new();
        block.extend(4u32.to_le_bytes());
        block.extend([0u8, b'\n', b'\r', 0xff]);
        assert_eq!(Framing::Length.whole(&block).unwrap(), 1);
        let records: Vec<&[u8]> = Framing::Length.records(&block).collect();
        assert_eq!(records[0], [0u8, b'\n', b'\r', 0xff]);
    }

    /// A record may run past the end of its block only if it started there, or
    /// earlier. So a block is all whole records, or all one record's bytes.
    #[test]
    fn a_block_ends_on_a_boundary_or_holds_no_boundary() {
        let lf = Framing::Lf;
        // Whole records: nothing left open.
        assert_eq!(lf.advance(b"", b"one\ntwo\n").unwrap(), (2, Vec::new()));
        // A record that starts at the beginning of the block and does not end.
        assert_eq!(lf.advance(b"", b"thr").unwrap(), (0, b"thr".to_vec()));
        // Continuing it, and finishing it, closes exactly one record.
        assert_eq!(lf.advance(b"thr", b"ee\n").unwrap(), (1, Vec::new()));
        // Continuing it without finishing it is still legal.
        assert_eq!(lf.advance(b"thr", b"ee").unwrap(), (0, b"three".to_vec()));
        // Records and then half of another is not.
        assert!(lf.advance(b"", b"one\ntwo\nthr").is_err());
        // Nor is finishing an open record and then starting one that does not end.
        assert!(lf.advance(b"thr", b"ee\nfou").is_err());
    }

    /// The rule holds for the framings with no terminator too, where a partial
    /// record is found by arithmetic rather than by a missing byte.
    #[test]
    fn a_fixed_width_record_can_be_partial_as_well() {
        let fixed = Framing::Fixed(4);
        assert_eq!(fixed.advance(b"", b"aaaa").unwrap(), (1, Vec::new()));
        assert_eq!(fixed.advance(b"", b"bb").unwrap(), (0, b"bb".to_vec()));
        assert_eq!(fixed.advance(b"bb", b"bb").unwrap(), (1, Vec::new()));
        assert!(fixed.advance(b"", b"aaaabb").is_err());

        let length = Framing::Length;
        let mut half = 8u32.to_le_bytes().to_vec();
        half.extend(b"four");
        assert_eq!(length.advance(b"", &half).unwrap(), (0, half.clone()));
        assert_eq!(length.advance(&half, b"more").unwrap(), (1, Vec::new()));
    }

    #[test]
    fn a_framing_survives_the_wire_and_the_command_line() {
        for framing in every() {
            let (kind, width) = framing.wire();
            assert_eq!(Framing::of_wire(kind, width).unwrap(), framing);
            assert_eq!(Framing::parse(&framing.to_string()).unwrap(), framing);
        }
    }

    #[test]
    fn a_framing_that_frames_nothing_is_refused() {
        assert!(Framing::parse("fixed:0").is_err());
        assert!(Framing::parse("fixed:").is_err());
        assert!(Framing::parse("fixed").is_err());
        assert!(Framing::parse("lines").is_err());
        assert!(Framing::of_wire(9, 0).is_err());
        assert!(Framing::of_wire(0, 0).is_err());
    }
}
