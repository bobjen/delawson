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

use crate::ids::{ObjectId, RangeId};
use crate::info::{Expecting, ObjectName};
use crate::offset::{ECoff, ELoff};
use crate::placement::Expansion;
use crate::wire::Arg;
use crate::wire::{Op, StructReader, StructWriter, structure};
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

/// The beginning of the object, which is where a caller that names no start means.
impl Default for Start {
    fn default() -> Start {
        Start::Record(0)
    }
}

impl Start {
    /// The record this start is at or after, which is what locates the extent.
    pub fn record(self) -> Option<u64> {
        match self {
            Start::Record(record) | Start::RecordThen(record, _) => Some(record),
            Start::Logical(_) => None,
        }
    }

    fn code(self) -> u8 {
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
    fn code(self) -> u8 {
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
        let mut payload = StructWriter::new();
        payload.number_u8(self.start.code() as u8);
        payload.number_u64(first);
        payload.number_u64(second);
        payload.number_u8(self.budget.code() as u8);
        payload.number_u64(self.budget.amount());
        vec![payload.finish(Op::Range)]
    }

    /// Write this range as a nested value in a command-owned payload.
    pub fn write_nested(&self, payload: &mut StructWriter) {
        let (first, second) = match self.start {
            Start::Record(record) => (record, 0),
            Start::RecordThen(record, offset) => (record, offset),
            Start::Logical(offset) => (offset, 0),
        };
        payload.nested(Op::Range, |payload| {
            payload.number_u8(self.start.code() as u8);
            payload.number_u64(first);
            payload.number_u64(second);
            payload.number_u8(self.budget.code() as u8);
            payload.number_u64(self.budget.amount());
        });
    }

    pub fn read_nested(payload: &mut StructReader<'_>) -> Result<Range> {
        payload.nested(Op::Range, "a range", |payload| {
            let kind = payload.number_u8()?;
            let first = payload.number_u64()?;
            let second = payload.number_u64()?;
            let measure = payload.number_u8()?;
            let amount = payload.number_u64()?;
            let start = match kind {
                0 => Start::Record(first),
                1 => Start::RecordThen(first, second),
                2 => Start::Logical(first),
                other => return Err(Error::new(format!("no range start of kind {other}"))),
            };
            let budget = match measure {
                0 => Budget::Records(amount),
                1 => Budget::Logical(amount),
                2 => Budget::Compressed(amount),
                other => return Err(Error::new(format!("no budget measured in kind {other}"))),
            };
            Ok(Range { start, budget })
        })
    }

    /// And back, returning whatever followed it.
    pub fn of_args(args: &[Arg]) -> Result<(Range, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::Range, "a range")?;
        let kind = payload.number_u8()?;
        let first = payload.number_u64()?;
        let second = payload.number_u64()?;
        let measure = payload.number_u8()?;
        let amount = payload.number_u64()?;
        payload.done()?;
        let start = match kind {
            0 => Start::Record(first),
            1 => Start::RecordThen(first, second),
            2 => Start::Logical(first),
            other => return Err(Error::new(format!("no range start of kind {other}"))),
        };
        let budget = match measure {
            0 => Budget::Records(amount),
            1 => Budget::Logical(amount),
            2 => Budget::Compressed(amount),
            other => return Err(Error::new(format!("no budget measured in kind {other}"))),
        };
        Ok((Range { start, budget }, rest))
    }
}

/// What an extent came to when it filled: the three measures the writing tier knows
/// (§9.1.3.6).
///
/// ⚠️ **A shape rather than three numbers in a row, deliberately** (#94). §8.1.6 exists
/// because a logical length and a compressed one are easy to confuse in append code, and a
/// seal report carrying both as bare arguments is exactly that situation — the wrong order
/// is a silent mis-billing rather than a compile error. §17.13.3's rule applies: the shape
/// lives here so no two processes count argument positions independently.
///
/// **There is no physical length here, and there is not one stored anywhere** (Asserted,
/// Bob, 2026-08-20). Physical bytes are what billing reads, and they are *calculated* from
/// the compressed length and the storage class — never measured and never kept. A stored
/// copy would be a fourth number that drifts the moment a storage class changes, and the
/// class is what the expansion factor belongs to. See §17.25.3.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Filled {
    pub records: u64,
    /// ⚠️ **An `ELoff`, and the next field an `ECoff`** — the type discipline #94 asked for, and
    /// the reason this is a shape at all. It also costs nothing: both spaces are 32-bit, so
    /// all three measures together are the 16 bytes the two of them used to take, and
    /// `Sealed` did not grow by a byte on §14.1's 1.7 × 10^9 extents. Storing them as `u64`
    /// would have been **13.6 GB of an SN's memory** for range that neither space has.
    pub logical: ELoff,
    pub compressed: ECoff,
}

impl Filled {
    pub fn args(&self) -> Vec<Arg> {
        let mut payload = StructWriter::new();
        payload.number_u64(self.records);
        payload.number_u64(self.logical.number());
        payload.number_u64(self.compressed.number());
        vec![payload.finish(Op::FilledResp)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Filled, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::FilledResp, "a seal report")?;
        let filled = Filled {
            records: payload.number_u64()?,
            logical: ELoff::of_number(payload.number_u64()?)?,
            compressed: ECoff::of_number(payload.number_u64()?)?,
        };
        payload.done()?;
        Ok((filled, rest))
    }

    /// Whether the extent took nothing at all — a block that would not fit an extent even
    /// empty, rather than an extent worth replacing.
    pub fn empty(&self) -> bool {
        self.records == 0 && self.compressed == ECoff::ZERO
    }

    /// Bytes the user is charged for, which is what billing reads.
    ///
    /// ⚠️ **Calculated, never stored.** `expansion` is the storage class's ratio — 4/1 for
    /// four copies, 4/3 or 7/3 or 6/5 for erasure coding — so this was never a multiply, and
    /// `Expansion` is where that lives (§17.25.3).
    ///
    /// ⚠️ **`expansion` is the class's intended ratio, never a count of the instances that
    /// happen to exist** (Asserted, Bob, 2026-08-20). An extent meant to have four copies
    /// bills as four whether three or five are up right now, and the reason is not only
    /// fairness: it means **billing runs off a snapshot of SN metadata alone**, while ENs
    /// are offline and without asking any of them. Reading the live instance list would put
    /// every EN on the billing path and make the bill depend on the repair queue
    /// (`BILLING-CHARGES-THE-CLASS-NOT-THE-COPIES`).
    pub fn physical(&self, expansion: Expansion) -> u64 {
        expansion.bytes(self.compressed.number())
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
    pub fn took(&mut self, records: u64, logical: ELoff, compressed: ECoff) {
        self.records += records;
        self.logical += logical.number();
        self.compressed += compressed.number();
    }

    pub fn args(&self) -> Vec<Arg> {
        let mut payload = StructWriter::new();
        payload.number_u64(self.records);
        payload.number_u64(self.logical);
        payload.number_u64(self.compressed);
        payload.number_u64(self.next);
        payload.flag(self.more);
        vec![payload.finish(Op::CoveredResp)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Covered, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::CoveredResp, "an answer coverage")?;
        let covered = Covered {
            records: payload.number_u64()?,
            logical: payload.number_u64()?,
            compressed: payload.number_u64()?,
            next: payload.number_u64()?,
            more: payload.flag()?,
        };
        payload.done()?;
        Ok((covered, rest))
    }
}

/// Which bits mean what in a [`Want`]'s flag field.
///
/// ⚠️ **A bit rather than an argument, and that is the whole point** (Asserted, Bob, 2026-08-19).
/// Each of these would otherwise be a required boolean argument, and §17.13.3 records what a
/// growing argument list costs: adding one **changes the shape**, so every reader shifts by a
/// position and two processes disagree about which field is which. Adding a *bit* changes nothing —
/// arity is fixed, old readers see a value they do not recognize in a field they already skip, and
/// the encoding cost stays one number however many flags exist.
///
/// That last point is not decoration at `object-info`'s rates (§17.5g measures 263k/s unbatched and
/// 5.1M/s batched): six booleans would be six `Arg`s per request, and a batch of a thousand carries
/// the request once and the names a thousand times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum Detail {
    /// The object's streams — ids, their SNs, and where each one's records start.
    Streams = 1 << 0,
    /// The extents of the streams returned. Implies [`Detail::Streams`], since an extent's record
    /// numbers mean nothing without the stream that contains it.
    Extents = 1 << 1,
    /// Which ENs hold each extent, primary first. Implies [`Detail::Extents`], and it is the field
    /// that makes a hint a route rather than a description (§8.1.1a-10).
    Instances = 1 << 2,
    /// The capability proving each extent belongs to this object (§9.1.3.5). Implies
    /// [`Detail::Extents`], and costs a MAC per entry, so it is opt-in for a reason.
    Capabilities = 1 << 3,
    /// The exact counts — streams per object, extents per stream, records per stream (§9.1.3.2).
    /// What lets a caller compute every remaining request by arithmetic.
    Counts = 1 << 4,
    /// The capability flags of the object and its streams (§4.1.1).
    Flags = 1 << 5,
}

impl Detail {
    pub const ALL: &'static [Detail] = &[
        Detail::Streams,
        Detail::Extents,
        Detail::Instances,
        Detail::Capabilities,
        Detail::Counts,
        Detail::Flags,
    ];

    pub const fn bit(self) -> u64 {
        self as u64
    }

    /// What asking for this also requires. A caller that asks for instances without extents is not
    /// making a mistake worth refusing — it is asking for something that only makes sense with the
    /// extents, so the extents come too (§9.1.3.11).
    fn implies(self) -> u64 {
        match self {
            Detail::Streams | Detail::Counts | Detail::Flags => 0,
            Detail::Extents => Detail::Streams.bit(),
            Detail::Instances | Detail::Capabilities => {
                Detail::Extents.bit() | Detail::Streams.bit()
            }
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Detail::Streams => "streams",
            Detail::Extents => "extents",
            Detail::Instances => "instances",
            Detail::Capabilities => "capabilities",
            Detail::Counts => "counts",
            Detail::Flags => "flags",
        }
    }
}

/// How much an `object-info` or `batch-object-info` wants back.
///
/// **The zero value is today's answer** (§9.1.3.11), which is what makes the field safe to add: a
/// caller that names nothing gets the object and its first stream, exactly as before, and pays
/// exactly what it paid before. That is §13.2.5.1's rule — a new thing defaults to the old
/// behaviour — arriving in a request rather than in a configuration file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Want {
    /// The [`Detail`] bits, normalized so that implications hold.
    pub detail: u64,
    /// The most entries — objects plus streams plus extents — this answer may carry. Zero means
    /// the configured default (§9.1.3.7), and a value above it is capped rather than refused.
    pub limit: u64,
    /// Which record to describe from, for an object too large to describe at once.
    pub from: Start,
    /// What object the caller believes the path points at, or nothing if it has no opinion
    /// (§17.36).
    pub expect: Expecting,
}

impl Want {
    /// Arguments a want takes on the wire, so a caller sizing or slicing a request does not
    /// count them itself (§17.13.3).
    pub const ARGS: usize = 4 + Expecting::ARGS;

    /// Nothing beyond the object and its first stream: what every caller asked for before this
    /// field existed.
    pub fn plain() -> Want {
        Want { detail: 0, limit: 0, from: Start::Record(0), expect: Expecting::whatever() }
    }

    pub fn with(mut self, detail: Detail) -> Want {
        self.detail |= detail.bit() | detail.implies();
        self
    }

    pub fn wants(&self, detail: Detail) -> bool {
        self.detail & detail.bit() != 0
    }

    /// Every bit asked for that this build does not answer, by name, so a refusal can say which.
    pub fn beyond(&self, built: u64) -> Vec<&'static str> {
        Detail::ALL
            .iter()
            .filter(|detail| self.wants(**detail) && built & detail.bit() == 0)
            .map(|detail| detail.as_str())
            .collect()
    }

    /// The flag field, the limit, the start as a kind and a number, and what object the
    /// caller believes it is addressing.
    ///
    /// ⚠️ **The expectation rides here rather than as a group of its own** (§17.36.1). A
    /// second optional group after the path would be told from the first only by counting
    /// arguments, and this document is full of what happens then (§17.19, §17.12). One
    /// optional group stays one optional group, and a caller with no want and no opinion
    /// sends neither — so the plain lookup is byte-for-byte what §17.5g measured.
    pub fn args(&self) -> Vec<Arg> {
        let (kind, at) = match self.from {
            Start::Record(record) => (0, record),
            Start::RecordThen(record, _) => (1, record),
            Start::Logical(offset) => (2, offset),
        };
        let mut args = vec![
            Arg::Number(self.detail),
            Arg::Number(self.limit),
            Arg::Number(kind),
            Arg::Number(at),
        ];
        args.extend(self.expect.args());
        args
    }

    /// Write the want fields into a command-owned payload without per-field wire tags.
    pub fn write(&self, payload: &mut StructWriter) {
        let (kind, at) = match self.from {
            Start::Record(record) => (0, record),
            Start::RecordThen(record, _) => (1, record),
            Start::Logical(offset) => (2, offset),
        };
        payload.number_u64(self.detail);
        payload.number_u64(self.limit);
        payload.number_u8(kind as u8);
        payload.number_u64(at);
        payload.number_u32(self.expect.0.range.get());
        payload.number_u32(self.expect.0.object.get());
        payload.number_u64(self.expect.0.created.number());
    }

    pub fn read(payload: &mut StructReader<'_>) -> Result<Want> {
        let detail = payload.number_u64()?;
        let limit = payload.number_u64()?;
        let kind = payload.number_u8()?;
        let at = payload.number_u64()?;
        let expect = Expecting::of(ObjectName {
            range: RangeId::of_number(payload.number_u32()? as u64)?,
            object: ObjectId::of_number(payload.number_u32()? as u64)?,
            created: crate::time::TimeSec::new(payload.number_u64()?),
        });
        let from = match kind {
            0 => Start::Record(at),
            1 => Start::RecordThen(at, 0),
            2 => Start::Logical(at),
            other => return Err(Error::new(format!("no range start of kind {other}"))),
        };
        let mut want = Want { detail, limit, from, expect };
        for one in Detail::ALL {
            if want.wants(*one) {
                want.detail |= one.implies();
            }
        }
        Ok(want)
    }

    /// And back. ⚠️ **A bit nothing here defines is not an error**, which is the forward
    /// compatibility the bitfield exists for: a newer caller may ask for something this build has
    /// never heard of, and `beyond` is what turns that into an honest refusal naming the rest.
    pub fn of_args(args: &[Arg]) -> Result<(Want, &[Arg])> {
        let [
            Arg::Number(detail),
            Arg::Number(limit),
            Arg::Number(kind),
            Arg::Number(at),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("a want takes a detail field, a limit and a start"));
        };
        let (expect, rest) = Expecting::of_args(rest)?;
        let from = match kind {
            0 => Start::Record(*at),
            1 => Start::RecordThen(*at, 0),
            2 => Start::Logical(*at),
            other => return Err(Error::new(format!("no range start of kind {other}"))),
        };
        // Normalized on the way in, so nothing downstream has to remember the implications.
        let mut want = Want { detail: 0, limit: *limit, from, expect };
        want.detail = *detail;
        for one in Detail::ALL {
            if want.wants(*one) {
                want.detail |= one.implies();
            }
        }
        Ok((want, rest))
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
    pub crc: u64,
    pub bytes: Vec<u8>,
}

impl Piece {
    pub fn args(&self) -> Vec<Arg> {
        let mut payload = StructWriter::new();
        payload.number_u64(self.logical);
        payload.number_u64(self.first_record);
        payload.number_u64(self.records);
        payload.number_u64(self.crc);
        payload.bytes(&self.bytes);
        vec![payload.finish(Op::PieceResp)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Piece, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::PieceResp, "a piece")?;
        let piece = Piece {
            logical: payload.number_u64()?,
            first_record: payload.number_u64()?,
            records: payload.number_u64()?,
            crc: payload.number_u64()?,
            bytes: payload.bytes()?,
        };
        payload.done()?;
        Ok((piece, rest))
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
            let mut payload = StructWriter::new();
            payload.number_u8(kind as u8);
            payload.number_u64(0);
            payload.number_u64(0);
            payload.number_u8(measure as u8);
            payload.number_u64(1);
            let args = vec![payload.finish(Op::Range)];
            Range::of_args(&args).is_err()
        };
        assert!(bad(9, 0), "no such start");
        assert!(bad(0, 9), "no such measure");
        assert!(!bad(0, 0));
        let mut payload = StructWriter::new();
        payload.number_u64(0);
        assert!(Range::of_args(&[payload.finish(Op::Range)]).is_err(), "and a short one");
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

    /// The zero value is what every caller asked for before the field existed, which is what makes
    /// the field safe to add at all.
    #[test]
    fn wanting_nothing_is_the_old_answer() {
        let plain = Want::plain();
        assert_eq!(plain.detail, 0);
        for detail in Detail::ALL {
            assert!(!plain.wants(*detail), "{}", detail.as_str());
        }
        // And it survives the wire as itself.
        let args = plain.args();
        let (back, rest) = Want::of_args(&args).expect("a want");
        assert_eq!(back, plain);
        assert!(rest.is_empty());
    }

    /// Asking for something that only makes sense with its container brings the container. A
    /// caller wanting instances is not making a mistake worth refusing.
    #[test]
    fn a_detail_brings_what_it_needs() {
        for asked in [Detail::Instances, Detail::Capabilities] {
            let want = Want::plain().with(asked);
            assert!(want.wants(asked));
            assert!(want.wants(Detail::Extents), "{} needs extents", asked.as_str());
            assert!(want.wants(Detail::Streams), "{} needs streams", asked.as_str());
        }
        // Extents bring streams; counts and flags stand alone.
        assert!(Want::plain().with(Detail::Extents).wants(Detail::Streams));
        assert!(!Want::plain().with(Detail::Counts).wants(Detail::Streams));
        assert!(!Want::plain().with(Detail::Flags).wants(Detail::Extents));
    }

    /// ⚠️ Normalization happens on the way in too, so a caller that set a bit without its
    /// implications does not produce an answer nobody can interpret.
    #[test]
    fn implications_are_restored_off_the_wire() {
        let raw = Want { detail: Detail::Instances.bit(), ..Want::plain() }.args();
        let (want, _) = Want::of_args(&raw).expect("a want");
        assert!(want.wants(Detail::Extents) && want.wants(Detail::Streams));
    }

    /// A bit this build does not know is not an error — that is the forward compatibility the
    /// field exists for — and `beyond` is what turns it into a refusal that says which.
    #[test]
    fn an_unknown_bit_is_reported_rather_than_refused() {
        // ⚠️ Built through the shape, so a field joining a want is a compile error here rather
        // than a raw list that silently stops parsing (§17.36.1 added the expectation).
        let raw = Want { detail: 1 << 40, ..Want::plain() }.args();
        let (want, _) = Want::of_args(&raw).expect("an unknown bit parses");
        assert_eq!(want.beyond(0), Vec::<&str>::new(), "nothing named, so nothing to report");

        // And what it does report is only what was asked for and is not built.
        let asked = Want::plain().with(Detail::Capabilities).with(Detail::Counts);
        let built = Detail::Streams.bit() | Detail::Extents.bit();
        assert_eq!(asked.beyond(built), vec!["capabilities", "counts"]);
        assert!(asked.beyond(u64::MAX).is_empty(), "everything built, nothing beyond");
    }

    /// The limit and the start survive the wire, since they are what an object too large to
    /// describe at once is paged with.
    #[test]
    fn a_limit_and_a_start_survive_the_wire() {
        let want = Want {
            detail: Detail::Extents.bit(),
            limit: 500,
            from: Start::Record(9_000),
            ..Want::plain()
        };
        let args = want.args();
        let (back, _) = Want::of_args(&args).expect("a want");
        assert_eq!(back.limit, 500);
        assert_eq!(back.from, Start::Record(9_000));
        assert!(Want::of_args(&args[..3]).is_err(), "and a short one is refused");
    }

    /// Blocks are counted in as they arrive, in all three spaces at once — the
    /// caller named one and is told all of them.
    #[test]
    fn a_block_is_counted_in_all_three_spaces() {
        let mut covered = Covered::default();
        covered.took(2, ELoff::new(200), ECoff::new(30));
        covered.took(1, ELoff::new(100), ECoff::new(20));
        assert_eq!((covered.records, covered.logical, covered.compressed), (3, 300, 50));
    }

    fn piece(first_record: u64, records: u64, bytes: &[u8]) -> Piece {
        Piece { logical: bytes.len() as u64, first_record, records, crc: 7, bytes: bytes.to_vec() }
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

    /// Half a shape is an error rather than a block with defaults, and a trailing
    /// fragment does not quietly disappear.
    #[test]
    fn a_partial_block_is_refused() {
        let whole = piece(0, 1, b"x\n");
        let args = whole.args();
        assert!(Piece::of_args(&args[..0]).is_err(), "no payload");
        let mut ragged = args.clone();
        ragged.push(Arg::Number(1));
        assert!(Piece::all_of_args(&ragged).is_err(), "one whole block and a fragment");
    }
}
