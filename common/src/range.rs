//! What a range read asks for, and what the answer says it gave.
//!
//! ⚠️ **This exists for §17.13.3's reason**: a request with a start, a budget and
//! three measures in the answer is an argument order neither side could test alone.
//! Two processes each counting positions in a list pass their own unit tests while
//! disagreeing with each other, so the shape is built and read here.
//!
//! The design is §9.1.3.4 for the start and §9.1.3.6 for the budget. Two rules from
//! there are load-bearing and easy to lose:
//!
//! - **A request names one measure; the answer reports all three.** A caller cannot
//!   know the compression ratio in advance, so a request bounding two of them would
//!   routinely satisfy neither.
//! - **One block always comes back, even past the budget.** A budget below the first
//!   block's size must not return nothing, or a caller with a small buffer retries
//!   for ever — back-pressure that is really a livelock.

use crate::offset::{COff, LOff};
use crate::wire::Arg;
use crate::{Error, Result};

/// Where a range starts.
///
/// **Every read names the object regardless** (§9.1.3.4) — the ACL is the object's
/// and no reverse edge recovers it — so this is a position *within* that object and
/// never a substitute for naming it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// An object-local record number: the durable form, good for ever.
    Record(u64),
    /// That many logical bytes past that record's own start, which may land **inside**
    /// the record. The only way to address a record's interior, which a store for very
    /// large objects needs (§9.1.3.4).
    RecordThen(u64, u64),
    /// That many logical bytes into the object, snapping **forward** to the next
    /// record boundary.
    ///
    /// ⚠️ **The snap happens in two places and the division matters.** The store
    /// snaps to a *block*, because a block is compressed and it cannot see records
    /// inside one (§9.1). The client snaps to a *record*, discarding the partial
    /// leading one. That is what keeps §9.1.3.4's partition property: the record snap
    /// is a deterministic function of the offset and the bytes, and every caller sees
    /// both identically, so adjacent callers agree on the boundary without talking.
    Logical(u64),
}

impl Start {
    /// The record this start is at or after, which is what locates the extent.
    pub fn record(self) -> Option<u64> {
        match self {
            Start::Record(record) | Start::RecordThen(record, _) => Some(record),
            Start::Logical(_) => None,
        }
    }

    fn code(self) -> u64 {
        match self {
            Start::Record(_) => 0,
            Start::RecordThen(..) => 1,
            Start::Logical(_) => 2,
        }
    }
}

/// How much a range may return, in one of three spaces (§9.1.3.6).
///
/// All three are free to enforce, which is why there is no reason to offer one: the
/// counts are already exact, the stored bytes are what an EN hands back, and the
/// logical size travels beside every block precisely because nothing below the
/// client can recompute it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// A count. Bounds nothing physical, and is what a caller with uniform records
    /// finds easiest to reason about.
    Records(u64),
    /// Bounds the caller's **decompressed buffer**, and the default: a buffer is the
    /// thing a caller is certain about.
    Logical(u64),
    /// Bounds the **network and the disk read** — what §12.3 bills and §5.12.1 makes
    /// scarce. For a caller sizing a transfer or estimating a bill.
    Compressed(u64),
}

impl Budget {
    fn code(self) -> u64 {
        match self {
            Budget::Records(_) => 0,
            Budget::Logical(_) => 1,
            Budget::Compressed(_) => 2,
        }
    }

    fn amount(self) -> u64 {
        match self {
            Budget::Records(n) | Budget::Logical(n) | Budget::Compressed(n) => n,
        }
    }

    /// Whether what has been gathered so far has met it. Asked **after** a block is
    /// taken, never before, which is what makes the first block unconditional.
    pub fn met(self, so_far: &Covered) -> bool {
        match self {
            Budget::Records(n) => so_far.records >= n,
            Budget::Logical(n) => so_far.logical >= n,
            Budget::Compressed(n) => so_far.compressed >= n,
        }
    }
}

/// A start and a budget: the whole of what a range read asks for, beside the object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start: Start,
    pub budget: Budget,
}

impl Range {
    /// The default budget, in logical bytes, for a caller that named none.
    pub const DEFAULT_LOGICAL: u64 = 1 << 20;

    pub fn from(record: u64) -> Range {
        Range { start: Start::Record(record), budget: Budget::Logical(Range::DEFAULT_LOGICAL) }
    }

    /// Four arguments: what kind of start, its two numbers, then the budget as a kind
    /// and an amount. Fixed width whichever variant it is, so a reader never has to
    /// know the kind before it can find what follows.
    pub fn args(&self) -> Vec<Arg> {
        let (first, second) = match self.start {
            Start::Record(record) => (record, 0),
            Start::RecordThen(record, offset) => (record, offset),
            Start::Logical(offset) => (offset, 0),
        };
        vec![
            Arg::Number(self.start.code()),
            Arg::Number(first),
            Arg::Number(second),
            Arg::Number(self.budget.code()),
            Arg::Number(self.budget.amount()),
        ]
    }

    /// And back, returning whatever followed it.
    pub fn of_args(args: &[Arg]) -> Result<(Range, &[Arg])> {
        let [
            Arg::Number(kind),
            Arg::Number(first),
            Arg::Number(second),
            Arg::Number(measure),
            Arg::Number(amount),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("a range takes a start and a budget"));
        };
        let start = match kind {
            0 => Start::Record(*first),
            1 => Start::RecordThen(*first, *second),
            2 => Start::Logical(*first),
            other => return Err(Error::new(format!("no range start of kind {other}"))),
        };
        let budget = match measure {
            0 => Budget::Records(*amount),
            1 => Budget::Logical(*amount),
            2 => Budget::Compressed(*amount),
            other => return Err(Error::new(format!("no budget measured in kind {other}"))),
        };
        Ok((Range { start, budget }, rest))
    }
}

/// What an answer actually covered — **all three measures, whichever one was asked
/// for** (§9.1.3.6) — and where to carry on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Covered {
    pub records: u64,
    pub logical: u64,
    pub compressed: u64,
    /// The record to ask for next, and the reason the whole scheme works: the
    /// continuation is a **record number the system computed**, never an offset the
    /// caller has to derive from where the bytes stopped (§9.1.3).
    pub next: u64,
    /// **Whether there is more where this came from**, which is deliberately about
    /// the tier that answered rather than about the object.
    ///
    /// ⚠️ An EN sets it from its own extent, because an extent is all it knows
    /// about; the FE is the only party that can turn that into a statement about the
    /// object, since it is the one that knows there are further extents. A caller
    /// loops while this is set.
    pub more: bool,
}

impl Covered {
    /// Count a block in. Records are the writer's count, so this is arithmetic on
    /// numbers that arrived rather than anything read out of the bytes.
    pub fn took(&mut self, records: u64, logical: LOff, compressed: COff) {
        self.records += records;
        self.logical += logical.number();
        self.compressed += compressed.number();
    }

    pub fn args(&self) -> Vec<Arg> {
        vec![
            Arg::Number(self.records),
            Arg::Number(self.logical),
            Arg::Number(self.compressed),
            Arg::Number(self.next),
            Arg::Flag(self.more),
        ]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Covered, &[Arg])> {
        let [
            Arg::Number(records),
            Arg::Number(logical),
            Arg::Number(compressed),
            Arg::Number(next),
            Arg::Flag(more),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("an answer does not say what it covered"));
        };
        Ok((
            Covered {
                records: *records,
                logical: *logical,
                compressed: *compressed,
                next: *next,
                more: *more,
            },
            rest,
        ))
    }
}

/// One block on its way back, and everything needed to make sense of it.
///
/// ⚠️ **The bytes are useless without the other four**, which is why they travel as
/// one shape: a block arrives compressed, so its logical size cannot be recovered
/// from it, and its records cannot be found without the framing and the number the
/// writer assigned (§8.1.4.1, §17.16.1). A reader decompresses to `logical`, checks
/// `crc`, parses by the object's framing, and numbers what it finds from
/// `first_record`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Piece {
    /// Bytes before compression, which is what to decompress to.
    pub logical: u64,
    /// The number of the first record this block **completed**, in whatever numbering
    /// the tier that answered counts in — an EN's is extent-local, and the FE is what
    /// converts it (§17.16.2).
    pub first_record: u64,
    /// Records it completed. **Zero is ordinary**: a block wholly inside one record
    /// completes nothing (§8.2.4), and it is still part of the answer because its
    /// bytes are part of that record.
    pub records: u64,
    pub crc: Option<u64>,
    pub bytes: Vec<u8>,
}

impl Piece {
    pub fn args(&self) -> Vec<Arg> {
        vec![
            Arg::Number(self.logical),
            Arg::Number(self.first_record),
            Arg::Number(self.records),
            Arg::Flag(self.crc.is_some()),
            Arg::Number(self.crc.unwrap_or(0)),
            Arg::Bytes(self.bytes.clone()),
        ]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Piece, &[Arg])> {
        let [
            Arg::Number(logical),
            Arg::Number(first_record),
            Arg::Number(records),
            Arg::Flag(has_crc),
            Arg::Number(crc),
            Arg::Bytes(bytes),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("a block does not have the shape of one"));
        };
        Ok((
            Piece {
                logical: *logical,
                first_record: *first_record,
                records: *records,
                crc: has_crc.then_some(*crc),
                bytes: bytes.clone(),
            },
            rest,
        ))
    }

    /// Every block in an answer, in order.
    pub fn all_of_args(mut args: &[Arg]) -> Result<Vec<Piece>> {
        let mut pieces = Vec::new();
        while !args.is_empty() {
            let (piece, rest) = Piece::of_args(args)?;
            pieces.push(piece);
            args = rest;
        }
        Ok(pieces)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant survives the wire, which is the only thing this file is for.
    #[test]
    fn a_range_survives_the_wire() {
        let each = [
            Range { start: Start::Record(7), budget: Budget::Records(3) },
            Range { start: Start::RecordThen(7, 4096), budget: Budget::Logical(1 << 20) },
            Range { start: Start::Logical(1 << 30), budget: Budget::Compressed(4096) },
        ];
        for range in each {
            let args = range.args();
            let (back, rest) = Range::of_args(&args).expect("a range");
            assert_eq!(back, range);
            assert!(rest.is_empty());
        }
    }

    /// The width does not depend on the variant, so a reader finds what follows
    /// without having to interpret the kind first.
    #[test]
    fn a_range_is_the_same_width_whichever_kind_it_is() {
        let mut args = Range { start: Start::Logical(9), budget: Budget::Records(1) }.args();
        args.push(Arg::Text("and something after it".into()));
        let (range, rest) = Range::of_args(&args).expect("a range");
        assert_eq!(range.start, Start::Logical(9));
        assert_eq!(rest.len(), 1, "and the rest is whoever's else");
    }

    /// A kind nothing defines is a bad request, not a default.
    #[test]
    fn an_unknown_kind_is_refused() {
        let bad = |kind, measure| {
            let args = vec![
                Arg::Number(kind),
                Arg::Number(0),
                Arg::Number(0),
                Arg::Number(measure),
                Arg::Number(1),
            ];
            Range::of_args(&args).is_err()
        };
        assert!(bad(9, 0), "no such start");
        assert!(bad(0, 9), "no such measure");
        assert!(!bad(0, 0));
        assert!(Range::of_args(&[Arg::Number(0)]).is_err(), "and a short one");
    }

    /// A budget is met by the measure it names and by nothing else. This is the
    /// arithmetic that decides when to stop, so it is worth a test of its own.
    #[test]
    fn a_budget_is_met_only_in_its_own_space() {
        let so_far = Covered { records: 5, logical: 500, compressed: 50, next: 5, more: true };
        assert!(Budget::Records(5).met(&so_far));
        assert!(!Budget::Records(6).met(&so_far));
        assert!(Budget::Logical(500).met(&so_far));
        assert!(!Budget::Logical(501).met(&so_far));
        assert!(Budget::Compressed(50).met(&so_far));
        // The one that would be a bug: 500 logical bytes do not meet a budget of
        // 500 compressed bytes just because the numbers match.
        assert!(!Budget::Compressed(500).met(&so_far));
    }

    /// A start says which record locates the extent, and a bare offset says it
    /// cannot — which is why §9.1.3.4 needs a prefix sum for that one.
    #[test]
    fn only_a_record_start_locates_an_extent_by_itself() {
        assert_eq!(Start::Record(7).record(), Some(7));
        assert_eq!(Start::RecordThen(7, 99).record(), Some(7));
        assert_eq!(Start::Logical(99).record(), None);
    }

    #[test]
    fn what_an_answer_covered_survives_the_wire() {
        let covered = Covered { records: 3, logical: 300, compressed: 42, next: 10, more: true };
        let args = covered.args();
        let (back, rest) = Covered::of_args(&args).expect("an answer");
        assert_eq!(back, covered);
        assert!(rest.is_empty());
    }

    /// Blocks are counted in as they arrive, in all three spaces at once — the
    /// caller named one and is told all of them.
    #[test]
    fn a_block_is_counted_in_all_three_spaces() {
        let mut covered = Covered::default();
        covered.took(2, LOff::new(200), COff::new(30));
        covered.took(1, LOff::new(100), COff::new(20));
        assert_eq!((covered.records, covered.logical, covered.compressed), (3, 300, 50));
    }

    fn piece(first_record: u64, records: u64, bytes: &[u8]) -> Piece {
        Piece {
            logical: bytes.len() as u64,
            first_record,
            records,
            crc: Some(7),
            bytes: bytes.to_vec(),
        }
    }

    /// A run of blocks reads back as the same run, which is the whole reason the
    /// shape is here and not counted out at both ends.
    #[test]
    fn a_run_of_blocks_survives_the_wire() {
        let sent = vec![piece(0, 2, b"one\ntwo\n"), piece(2, 0, b"half a "), piece(2, 1, b"rec\n")];
        let args: Vec<Arg> = sent.iter().flat_map(|piece| piece.args()).collect();
        assert_eq!(Piece::all_of_args(&args).expect("blocks"), sent);
        // And no blocks is a run of none rather than an error: a budget can be met
        // by an answer that carries only counts.
        assert!(Piece::all_of_args(&[]).expect("none").is_empty());
    }

    /// A block with no CRC keeps saying so, because the flag is what a reader
    /// decides by — a zero is a CRC that happens to be zero.
    #[test]
    fn a_block_without_a_crc_is_not_one_with_a_zero() {
        let bare = Piece { crc: None, ..piece(0, 1, b"x\n") };
        let args = bare.args();
        let (back, _) = Piece::of_args(&args).expect("a block");
        assert_eq!(back.crc, None);
        let zero = Piece { crc: Some(0), ..piece(0, 1, b"x\n") };
        let args = zero.args();
        assert_eq!(Piece::of_args(&args).expect("a block").0.crc, Some(0));
    }

    /// Half a shape is an error rather than a block with defaults, and a trailing
    /// fragment does not quietly disappear.
    #[test]
    fn a_partial_block_is_refused() {
        let whole = piece(0, 1, b"x\n");
        let args = whole.args();
        assert!(Piece::of_args(&args[..5]).is_err(), "no bytes");
        let mut ragged = args.clone();
        ragged.push(Arg::Number(1));
        assert!(Piece::all_of_args(&ragged).is_err(), "one whole block and a fragment");
    }
}
