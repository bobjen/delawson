//! What an object is, as `object-info` and `create` answer it.
//!
//! ⚠️ **This is here because four places were counting the same positions
//! independently, and two of them were wrong** — §17.13.3's mistake, found by
//! §17.5g's benchmark measuring nothing. The DN built the row, the FE read it to
//! find a stream, `laws` read it to print it, and the benchmark chopped the answer
//! into fixed-width pieces to count how many objects were found. That last one
//! assumed **five** fields; the row had grown to **eight** when §17.12 added the
//! stream's first record. So it counted every object twice and nobody noticed,
//! because the number it produced was plausible.
//!
//! One shape, built and read here, makes that a compile error instead.

use crate::framing::Framing;
use crate::ids::{DiskId, DnId, EnId, ExtentId, ObjectId, RangeId, SnId, StreamId};
use crate::offset::{ECoff, ELoff, ERoff, OCoff, OEoff, OLoff, ORoff, SCoff, SLoff, SRoff};
use crate::placement::Expansion;
use crate::time::TimeSec;
use crate::wire::Arg;
use crate::wire::{Op, StructReader, StructWriter, checked_vec, structure};
use crate::{Error, Result};

/// An object as the DN describes it, at one of its streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Described {
    /// The object's own name, `(RangeId, ObjectId, CreateSec)`.
    ///
    /// ⚠️ **The path is not this** (Asserted, Bob, 2026-08-20): a path *points at* a name, and a
    /// user may repoint it as often as they like — so the path recycles freely while every
    /// incarnation it ever pointed at keeps a distinct name of its own (§17.35).
    pub object: ObjectName,
    /// The DN currently managing the range.
    pub dn: DnId,
    /// The stream this answer is about, **by its whole name**: the last one for an append,
    /// or the one holding a record for a read.
    ///
    /// ⚠️ **The name and not the id** (#109), because everything the FE does next — open an
    /// extent, seal one, locate a record — keys a stream at the SN by
    /// `(RangeId, StreamId, CreateSec)`, and the DN is the only tier holding all three.
    pub stream: StreamName,
    pub sn: SnId,
    pub keyed: bool,
    pub framing: Framing,
    /// The object-local number of that stream's first record, which is what makes
    /// an object's numbering continuous across the streams it is made of.
    pub first_record: ORoff,
}

impl Described {
    /// Fields per row on the wire. Public because a caller sizing a buffer wants it,
    /// and derived from the shape rather than written down twice.
    /// A described row is one self-contained argument on the wire.
    pub const ARGS: usize = 1;
    /// A row meaning "there is no such object", for a batch answer that must line up
    /// with the names it was asked about. A zero id is what marks it.
    ///
    /// ⚠️ §9.1.3.7 will retire this: once an answer may be short, lining up by
    /// position stops working and the answer has to say how many names it covered.
    pub fn missing() -> Vec<Arg> {
        vec![Self::missing_arg()]
    }

    pub fn missing_arg() -> Arg {
        let mut payload = StructWriter::with_capacity(7 * 8 + 3 * 4 + 2);
        payload.number_u64(0);
        payload.number_u32(ObjectId::NONE.get());
        payload.number_u64(0);
        payload.number_u64(0);
        payload.number_u64(0);
        payload.number_u32(0);
        payload.number_u64(0);
        payload.number_u64(0);
        payload.flag(false);
        payload.number_u8(0);
        payload.number_u32(0);
        payload.number_u64(0);
        payload.finish(Op::DescribedResp)
    }

    pub fn args(&self) -> Vec<Arg> {
        vec![self.arg()]
    }

    pub fn arg(&self) -> Arg {
        let (kind, width) = self.framing.wire();
        let mut payload = StructWriter::with_capacity(7 * 8 + 3 * 4 + 2);
        payload.range_id(self.object.range);
        payload.object_id(self.object.object);
        payload.time_sec(self.object.created);
        payload.dn_id(self.dn);
        payload.range_id(self.stream.range);
        payload.stream_id(self.stream.stream);
        payload.time_sec(self.stream.created);
        payload.sn_id(self.sn);
        payload.flag(self.keyed);
        payload.number_u8(kind);
        payload.number_u32(width);
        payload.oroff(self.first_record);
        payload.finish(Op::DescribedResp)
    }

    /// And back, returning whatever followed it. `None` for a row marked missing,
    /// since a zero id is not an object.
    pub fn of_args(args: &[Arg]) -> Result<(Option<Described>, &[Arg])> {
        {
            let (mut payload, rest) = structure(args, Op::DescribedResp, "a described row")?;
            let object = ObjectName {
                range: payload.range_id()?,
                object: payload.object_id()?,
                created: payload.time_sec()?,
            };
            let dn = payload.dn_id()?;
            let stream = StreamName {
                range: payload.range_id()?,
                stream: payload.stream_id()?,
                created: payload.time_sec()?,
            };
            let sn = payload.sn_id()?;
            let keyed = payload.flag()?;
            let kind = payload.number_u8()?;
            let width = payload.number_u32()?;
            let first_record = payload.oroff()?;
            payload.done()?;
            if object.object.is_none() {
                return Ok((None, rest));
            }
            return Ok((
                Some(Described {
                    object,
                    dn,
                    stream,
                    sn,
                    keyed,
                    framing: Framing::of_wire(kind, width)?,
                    first_record,
                }),
                rest,
            ));
        }
    }

    /// Every row in an answer, in the order asked about. `None` where the name was
    /// not there.
    pub fn all_of_args(mut args: &[Arg]) -> Result<Vec<Option<Described>>> {
        let mut rows = Vec::with_capacity(args.len() / Described::ARGS);
        while !args.is_empty() {
            let (row, rest) = Described::of_args(args)?;
            rows.push(row);
            args = rest;
        }
        Ok(rows)
    }

    /// The argument slices for a prefix of rows, whether they are old flat rows
    /// or new self-contained values.
    pub fn some_of_args<'a>(mut args: &'a [Arg], count: u64) -> Result<Vec<&'a [Arg]>> {
        let mut rows = checked_vec(count, args.len())?;
        for _ in 0..count {
            let whole = args;
            let (_, rest) = Described::of_args(args)?;
            rows.push(&whole[..whole.len() - rest.len()]);
            args = rest;
        }
        Ok(rows)
    }
}

/// **No value given**, for the counts in an answer that may or may not know them.
///
/// At the top of the range rather than at zero, because zero is a count a stream can
/// legitimately have — §8.1.6.3's rule, and #90 is what putting it at zero costs.
pub const UNKNOWN: u64 = u64::MAX;

/// What an answer covered and what it did not tell you (§9.1.3.2).
///
/// **Complete in counts even when partial in detail**, which is what lets a caller
/// compute every remaining request by arithmetic instead of discovering them one at a
/// time. A count of `UNKNOWN` means *nobody asked* rather than *none*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    /// Streams the object has, whether or not they are all described here.
    pub streams: StreamId,
    /// Stream rows that follow.
    pub given: StreamId,
    /// Extents across the streams described, or `UNKNOWN` when no SN was asked.
    pub extents: OEoff,
    /// Entries this answer spent, against the limit it was given (§9.1.3.7).
    pub entries: u64,
    /// The object-local record to ask from next, or `UNKNOWN` when nothing follows.
    pub next: ORoff,
    /// Aggregate object record count, when counts were requested.
    pub records: ORoff,
    /// Aggregate logical length, when counts were requested and known.
    pub logical: OLoff,
    /// Aggregate compressed length, when counts were requested and known.
    pub compressed: OCoff,
}

impl Shape {
    /// What an answer carrying no detail says: one object, nothing else asked about.
    pub fn plain() -> Shape {
        Shape {
            streams: StreamId::FIRST,
            given: StreamId::FIRST,
            extents: OEoff::NONE,
            entries: 1,
            next: ORoff::NONE,
            records: ORoff::NONE,
            logical: OLoff::NONE,
            compressed: OCoff::NONE,
        }
    }

    pub fn more(&self) -> bool {
        self.next != ORoff::NONE
    }

    pub fn args(&self) -> Vec<Arg> {
        vec![self.arg()]
    }

    pub fn arg(&self) -> Arg {
        let mut payload = StructWriter::with_capacity(8 * 8);
        // Preserve the existing 64-bit wire fields and UNKNOWN spelling.
        payload.number_u64(if self.streams.is_none() { UNKNOWN } else { self.streams.number() });
        payload.number_u64(self.given.number());
        payload.oeoff(self.extents);
        payload.number_u64(self.entries);
        payload.number_u64(self.next.number());
        payload.number_u64(self.records.number());
        payload.number_u64(self.logical.number());
        payload.number_u64(self.compressed.number());
        payload.finish(Op::ShapeResp)
    }

    pub fn of_args(args: &[Arg]) -> Result<(Shape, &[Arg])> {
        {
            let (mut payload, rest) = structure(args, Op::ShapeResp, "a shape")?;
            let shape = Shape {
                streams: match payload.number_u64()? {
                    UNKNOWN => StreamId::NONE,
                    count => StreamId::of_number(count)?,
                },
                given: StreamId::of_number(payload.number_u64()?)?,
                extents: payload.oeoff()?,
                entries: payload.number_u64()?,
                next: ORoff::of_number(payload.number_u64()?)?,
                records: ORoff::of_number(payload.number_u64()?)?,
                logical: OLoff::of_number(payload.number_u64()?)?,
                compressed: OCoff::of_number(payload.number_u64()?)?,
            };
            payload.done()?;
            return Ok((shape, rest));
        }
    }
}

/// A name that holds for ever: **who mints it, which one, and when**.
///
/// ⚠️ **Three parts, because the id alone was never the name** (Asserted, Bob, 2026-08-20). An id
/// has only to be unique among the things one minter makes in one second, which is what lets it be
/// 32 bits — and what makes recycling one safe, once #110's gate exists
/// (`A-NAME-IS-NEVER-REMINTED-IN-ITS-OWN-SECOND`).
///
/// **Sixteen bytes, like a GUID, and §17.34 measures the difference**: a list of these compresses
/// where a list of GUIDs cannot, and they carry fields a GUID would need stored beside it.
macro_rules! name {
    ($name:ident, $minter:ident, $minted:ty, $what:ident, $what_ty:ty, $mints:literal) => {
        #[doc = concat!("What names one of ", $mints, " for ever.")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name {
            pub $minter: $minted,
            pub $what: $what_ty,
            pub created: TimeSec,
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    concat!($mints, " {} of ", stringify!($minter), " {} made in second {}"),
                    self.$what, self.$minter, self.created
                )
            }
        }
    };
}

// ⚠️ **Scoped to the range and not to the DN**, both of them, because a range outlives the DN
// serving it. `routing::range_of_dn` stands in until ranges exist. And ⚠️ **`range` is not `sn`**:
// the range is *whose* a thing is, the SN is *where* it lives.
//
// ⚠️ **It is the *minting* range and it is not durable.** #75 makes the range a hint and the key
// the name, because a **split renames a range** — so this component says who handed the id out,
// which is unique for ever, and not where the thing lives now. A name identifies; it does not
// locate (§17.38.1).
name!(ObjectName, range, RangeId, object, ObjectId, "an object");
name!(StreamName, range, RangeId, stream, StreamId, "a stream");

/// What a caller believes it is addressing, when it has an opinion.
///
/// ⚠️ **Only where the answer does not already carry the name.** `object-info` returns an
/// object's name, so a caller can compare for itself and needs nothing here. A **read** answers
/// with blocks and a **append** with a receipt — neither says which object it was about, so a
/// caller holding a reference across a replacement is answered about a different object and
/// cannot tell (§17.36).
///
/// **Fixed width and always present**, three numbers after the path, with a zero object id
/// meaning *no opinion* — which is the sentinel `Described` already uses for a name that is not
/// there, and it avoids an optional group that a parser has to guess the presence of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Expecting(pub ObjectName);

impl Expecting {
    /// No opinion: whatever the path points at now.
    pub fn whatever() -> Expecting {
        Expecting(ObjectName::default())
    }

    pub fn of(name: ObjectName) -> Expecting {
        Expecting(name)
    }

    pub fn asked(&self) -> bool {
        self.0.object.is_some()
    }

    pub fn write(&self, payload: &mut StructWriter) {
        payload.nested(Op::Expecting, |payload| {
            payload.range_id(self.0.range);
            payload.object_id(self.0.object);
            payload.time_sec(self.0.created);
        });
    }

    pub fn read(payload: &mut StructReader<'_>) -> Result<Expecting> {
        payload.nested(Op::Expecting, "an expectation", |payload| {
            Ok(Expecting(ObjectName {
                range: payload.range_id()?,
                object: payload.object_id()?,
                created: payload.time_sec()?,
            }))
        })
    }

    /// Whether `found` is the object the caller meant. ⚠️ **True when nothing was asked**, so
    /// the default stays "whatever the path points at now" — which is what a path means, and
    /// what almost every caller wants.
    pub fn holds(&self, found: ObjectName) -> bool {
        !self.asked() || self.0 == found
    }
}

/// One object a create asks for: a name, a flavour, and a framing.
///
/// ⚠️ **Here rather than in `dn`, on §17.13.3's rule.** A batch create has to be *divided* by
/// the FE before any DN sees it (#98), which means the FE cuts the request into objects — and
/// two processes counting argument positions independently is how §17.19 went wrong. One
/// shape, one `ARGS`, one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wanted {
    pub name: String,
    pub keyed: bool,
    pub framing: Framing,
}

impl Wanted {
    /// Write one create record into a command-owned payload.
    pub fn write(&self, payload: &mut StructWriter) {
        let (kind, width) = self.framing.wire();
        payload.text(&self.name);
        payload.flag(self.keyed);
        payload.number_u8(kind);
        payload.number_u32(width);
    }

    pub fn read(payload: &mut StructReader<'_>) -> Result<Wanted> {
        Ok(Wanted {
            name: payload.text()?,
            keyed: payload.flag()?,
            framing: Framing::of_wire(payload.number_u8()?, payload.number_u32()?)?,
        })
    }
}

/// What a stream has come to: the sizes a user asks for and none of the addressing.
///
/// ⚠️ **Sums, so `u64` — unlike `Filled`, whose measures are one extent's and fit 32 bits.**
/// A stream is many extents, so its logical total passes 4 GB in the second one.
///
/// **Exact for a sealed stream and as-of for a growing one** (Asserted, Bob, 2026-08-20).
/// That asymmetry is deliberate: a sealed stream never changes, so one refresh is correct
/// for ever, while a growing one is only ever a point in time. `records` is the exception —
/// it stays `UNKNOWN` while a stream grows rather than going stale, because §4.5's exact
/// count is what addressing runs on and a *stale record count is a wrong read*, where a
/// stale byte count is only a slightly wrong listing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub extents: ExtentId,
    pub logical: SLoff,
    pub compressed: SCoff,
}

impl Totals {
    /// Nothing known yet: a stream nobody has asked an SN about.
    pub fn unknown() -> Totals {
        Totals { extents: ExtentId::NONE, logical: SLoff::NONE, compressed: SCoff::NONE }
    }

    /// Bytes the user is charged for. ⚠️ See `Filled::physical`: `expansion` is the storage
    /// class's ratio, never a count of the copies that happen to exist.
    pub fn physical(&self, expansion: Expansion) -> u64 {
        match self.compressed {
            SCoff::NONE => UNKNOWN,
            compressed => expansion.bytes(compressed.number()),
        }
    }
}

/// One of an object's streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRow {
    pub stream: StreamId,
    pub sn: SnId,
    /// The object-local number of this stream's first record.
    pub first_record: ORoff,
    /// Records in it once sealed, and **`UNKNOWN` while it is the one being appended
    /// to** — how far that one reaches is not the DN's to say (§6.3).
    pub records: SRoff,
    /// The second the stream was made. The DN's own, so always exact.
    pub created: TimeSec,
    /// What it has come to. `extents` is here rather than in the row proper because the
    /// three travel together and mean the same thing about staleness.
    pub totals: Totals,
    /// Extent rows that follow this one. ⚠️ **The answer is interleaved** — a stream then
    /// its extents, repeated — so a reader never attributes extents to a stream by
    /// position, which is §17.19's defect.
    pub given: ExtentId,
}

impl StreamRow {
    pub fn args(&self) -> Vec<Arg> {
        vec![self.arg()]
    }

    pub fn arg(&self) -> Arg {
        let mut payload = StructWriter::with_capacity(7 * 8 + 2 * 4);
        payload.stream_id(self.stream);
        payload.sn_id(self.sn);
        payload.oroff(self.first_record);
        payload.sroff(self.records);
        payload.time_sec(self.created);
        payload.extent_id(self.totals.extents);
        payload.sloff(self.totals.logical);
        payload.scoff(self.totals.compressed);
        payload.number_u64(self.given.number());
        payload.finish(Op::StreamRowResp)
    }

    pub fn of_args(args: &[Arg]) -> Result<(StreamRow, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::StreamRowResp, "a stream row")?;
        let row = StreamRow {
            stream: payload.stream_id()?,
            sn: payload.sn_id()?,
            first_record: payload.oroff()?,
            records: payload.sroff()?,
            created: payload.time_sec()?,
            totals: Totals {
                extents: payload.extent_id()?,
                logical: payload.sloff()?,
                compressed: payload.scoff()?,
            },
            given: ExtentId::of_number(payload.number_u64()?)?,
        };
        payload.done()?;
        Ok((row, rest))
    }

    /// `given` rows, and whatever followed them.
    pub fn some_of_args(args: &[Arg], given: u64) -> Result<(Vec<StreamRow>, &[Arg])> {
        let mut rows = checked_vec(given, args.len())?;
        let mut rest = args;
        for _ in 0..given {
            let (row, after) = StreamRow::of_args(rest)?;
            rows.push(row);
            rest = after;
        }
        Ok((rows, rest))
    }
}

/// One extent of a stream, and everything a caller needs to build a hint from it
/// (§8.1.1a-10, #61).
///
/// ⚠️ **There is no `sealed` flag, deliberately.** Whether an extent is sealed is
/// exactly whether its record count is known — an open extent reaches further with
/// every append, so the SN cannot say — and carrying the same fact twice invites the
/// two copies to disagree. §18.1's rule: do not make a dimension out of something
/// another dimension already determines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentRow {
    /// The SN that created it, which with the id and the second is its whole name
    /// (§4.6.3) — and the second is what makes a stale hint fail loudly.
    pub sn: SnId,
    pub id: ExtentId,
    pub created: TimeSec,
    /// Its first record within the stream, at every tier. Add the parent
    /// StreamRow.first_record only when an object-relative position is needed.
    pub first_record: SRoff,
    /// Records in it, or `UNKNOWN` while it is the one being appended to — which is
    /// also what says it is not sealed.
    pub records: ERoff,
    /// Logical bytes, or `UNKNOWN` while it is open.
    ///
    /// ⚠️ **All three of §9.1.3.6's measures are here on purpose.** A caller budgeting
    /// in logical bytes cannot plan from a compressed length, and one that wants to
    /// bound a range *inside* an extent needs that extent's base in the space it means
    /// to bound (§9.1.3.5.4). Two of three would silently make one of the three
    /// budgets unplannable.
    pub logical: ELoff,
    /// Compressed bytes, or `UNKNOWN` while it is open.
    pub length: ECoff,
    /// The disks holding its instances, primary first, each as `(EnId, DiskId)`.
    pub disks: Vec<(EnId, DiskId)>,
}

impl ExtentRow {
    pub fn sealed(&self) -> bool {
        self.records != ERoff::NONE
    }

    pub fn args(&self) -> Vec<Arg> {
        let mut payload = StructWriter::new();
        payload.sn_id(self.sn);
        payload.extent_id(self.id);
        payload.time_sec(self.created);
        payload.sroff(self.first_record);
        payload.eroff(self.records);
        payload.eloff(self.logical);
        payload.ecoff(self.length);
        payload.number_u32(self.disks.len() as u32);
        for (en, disk) in &self.disks {
            payload.en_id(*en);
            payload.disk_id(*disk);
        }
        vec![payload.finish(Op::ExtentRowResp)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(ExtentRow, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::ExtentRowResp, "an extent row")?;
        let sn = payload.sn_id()?;
        let id = payload.extent_id()?;
        let created = payload.time_sec()?;
        let first_record = payload.sroff()?;
        let records = payload.eroff()?;
        let logical = payload.eloff()?;
        let length = payload.ecoff()?;
        let count = payload.number_u32()?;
        let mut disks = payload.checked_vec(count)?;
        for _ in 0..count {
            disks.push((payload.en_id()?, payload.disk_id()?));
        }
        payload.done()?;
        Ok((ExtentRow { sn, id, created, first_record, records, logical, length, disks }, rest))
    }

    pub fn some_of_args(args: &[Arg], given: u64) -> Result<(Vec<ExtentRow>, &[Arg])> {
        let mut rows = checked_vec(given, args.len())?;
        let mut rest = args;
        for _ in 0..given {
            let (row, after) = ExtentRow::of_args(rest)?;
            rows.push(row);
            rest = after;
        }
        Ok((rows, rest))
    }
}

/// Trim a detailed object answer in record order, keeping counts independent of row limits.
/// Both DN and FE use this final boundary check after collecting independently bounded replies.
pub fn limit_object_info(args: &[Arg], limit: u64) -> Result<Vec<Arg>> {
    if limit == 0 {
        return Err(Error::new("an object summary needs at least one entry"));
    }
    let (mut shape, rest) = Shape::of_args(args)?;
    let (described, mut rest) = Described::of_args(rest)?;
    let mut body = Vec::new();
    let mut entries = 1;
    let mut streams = 0;
    let mut stopped = false;
    for _ in 0..shape.given.number() {
        let (mut stream, after) = StreamRow::of_args(rest)?;
        let (extents, after) = ExtentRow::some_of_args(after, stream.given.number())?;
        rest = after;
        if stopped {
            continue;
        }
        if entries == limit {
            shape.next = shape.next.min(stream.first_record);
            stopped = true;
            continue;
        }
        entries += 1;
        streams += 1;
        let take = extents.len().min((limit - entries) as usize);
        if let Some(omitted) = extents.get(take) {
            let next = stream.first_record.plus(ORoff::from_stream(omitted.first_record))?;
            shape.next = shape.next.min(next);
            stopped = true;
        }
        stream.given = ExtentId::of_number(take as u64)?;
        body.push(stream.arg());
        for extent in extents.iter().take(take) {
            body.extend(extent.args());
        }
        entries += take as u64;
    }
    if !rest.is_empty() {
        return Err(Error::new("object summary has trailing arguments"));
    }
    shape.given = StreamId::of_number(streams)?;
    shape.entries = entries;
    let mut out = shape.args();
    out.push(described.map_or_else(Described::missing_arg, |row| row.arg()));
    out.extend(body);
    Ok(out)
}

/// What a batch answer covered, and where to carry on.
///
/// ⚠️ **This replaces lining up by position** (§9.1.3.7). The old answer padded a missing
/// name with a zero-id row so that the *n*th row was the *n*th name; once an answer may
/// stop early, position is not enough, and a caller reading it the old way would
/// silently attribute one object's metadata to a different name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batch {
    /// Names the request held.
    pub asked: u64,
    /// Names described. **Always a prefix of the request**, so what is missing is a
    /// suffix and the caller need only slice its own list.
    pub given: u64,
    /// Entries the whole answer spent, against the limit (§9.1.3.7).
    pub entries: u64,
    /// Index of the first name not described, or `UNKNOWN` when all were.
    ///
    /// **An index rather than a name**, because the caller already holds the list and an
    /// index is what it needs to slice it — and because a name would have to be repeated
    /// in an answer that is trying to be smaller.
    pub next: u64,
}

impl Batch {
    pub fn args(&self) -> Vec<Arg> {
        let mut payload = StructWriter::with_capacity(4 * 8);
        payload.number_u64(self.asked);
        payload.number_u64(self.given);
        payload.number_u64(self.entries);
        payload.number_u64(self.next);
        vec![payload.finish(Op::BatchResp)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Batch, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::BatchResp, "a batch")?;
        let batch = Batch {
            asked: payload.number_u64()?,
            given: payload.number_u64()?,
            entries: payload.number_u64()?,
            next: payload.number_u64()?,
        };
        payload.done()?;
        Ok((batch, rest))
    }

    pub fn more(&self) -> bool {
        self.next != UNKNOWN
    }

    /// One name's arguments at a time, from the body of a levelled batch answer.
    ///
    /// ⚠️ **Slices rather than parses.** A batch that came from several DNs has to be put
    /// back together in the caller's order (§9.1.3.12), and re-encoding every row to do that
    /// would mean the merge and the DN could disagree about the wire — §17.19's defect,
    /// which cost an afternoon. Handing back the untouched arguments cannot.
    ///
    /// Each name is a `Shape`, an object row, then the stream rows the shape says, each
    /// with the extents *its* row says. Only for the levelled form: a batch asked for no
    /// detail at all answers with bare object rows and `Described::all_of_args` reads those.
    pub fn pieces(mut args: &[Arg], count: u64) -> Result<Vec<&[Arg]>> {
        let mut out = checked_vec(count, args.len())?;
        for _ in 0..count {
            let whole = args;
            let (shape, mut rest) = Shape::of_args(args)?;
            let (_, after) = Described::of_args(rest)?;
            rest = after;
            for _ in 0..shape.given.number() {
                let (row, after) = StreamRow::of_args(rest)?;
                rest = after;
                for _ in 0..row.given.number() {
                    rest = ExtentRow::of_args(rest)?.1;
                }
            }
            out.push(&whole[..whole.len() - rest.len()]);
            args = rest;
        }
        Ok(out)
    }
}

/// Several streams to describe in one ask, which is what makes `batch-object-info`
/// batch all the way down.
///
/// ⚠️ **One ask per stream would defeat the point.** §1.4.0 measures nine tenths of an
/// unbatched request's cost as messaging, and a batch of a thousand objects can hold a
/// thousand streams — so asking per stream would put the DN's fan-out exactly where
/// §1.4.0 says the cost is. The limit is **shared across the whole ask**, because
/// §9.1.3.7's limit is one number over the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// Entries the whole answer may carry, over every stream in it.
    pub limit: u64,
    /// Whether extent rows should include their `(EnId, DiskId)` instance lists.
    pub instances: bool,
    /// `(name, first stream-local record to describe from)`, in order.
    ///
    /// ⚠️ **A whole name per stream, not an id against one shared range.** A batch may
    /// name streams of different ranges — and once ranges split and move (#75) it routinely
    /// will — so the range travels with each entry rather than once at the front.
    pub at: Vec<(StreamName, SRoff)>,
}

impl Listing {
    pub fn write_payload(&self, payload: &mut StructWriter) {
        payload.number_u64(self.limit);
        payload.flag(self.instances);
        payload.number_u32(self.at.len() as u32);
        for (name, from) in &self.at {
            payload.number_u64(name.range.get());
            payload.number_u32(name.stream.get());
            payload.number_u64(name.created.number());
            payload.sroff(*from);
        }
    }

    pub fn read_payload(payload: &mut StructReader<'_>) -> Result<Listing> {
        let limit = payload.number_u64()?;
        let instances = payload.flag()?;
        let count = payload.number_u32()?;
        let mut at = payload.checked_vec(count)?;
        for _ in 0..count {
            at.push((
                StreamName {
                    range: RangeId::new(payload.number_u64()?),
                    stream: StreamId::new(payload.number_u32()?),
                    created: TimeSec::new(payload.number_u64()?),
                },
                payload.sroff()?,
            ));
        }
        Ok(Listing { limit, instances, at })
    }
}

/// What an SN says about a run of one stream's extents: complete in counts, partial
/// in detail (§9.1.3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentRun {
    /// Which stream this run is about. ⚠️ **Named rather than positional**, so a batch
    /// answer says what it is answering instead of relying on the order matching the
    /// question — §17.19's defect was exactly that assumption.
    pub stream: StreamId,
    /// What the stream has come to, which the SN is the authority for and the DN caches
    /// (§17.25.1). Includes the extent count, whether or not the rows are all here.
    pub totals: Totals,
    /// Rows that follow.
    pub given: ExtentId,
    /// The **stream-local** record to ask from next, or `UNKNOWN` when nothing
    /// follows.
    pub next: SRoff,
}

impl ExtentRun {
    pub fn args(&self) -> Vec<Arg> {
        let mut payload = StructWriter::new();
        payload.number_u32(self.stream.get());
        payload.number_u64(self.totals.extents.number());
        payload.number_u64(self.totals.logical.number());
        payload.number_u64(self.totals.compressed.number());
        payload.number_u64(self.given.number());
        payload.number_u64(self.next.number());
        vec![payload.finish(Op::ExtentRunResp)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(ExtentRun, &[Arg])> {
        let (mut payload, rest) = structure(args, Op::ExtentRunResp, "a run of extents")?;
        let stream = payload.number_u32()?;
        let totals = Totals {
            extents: ExtentId::of_number(payload.number_u64()?)?,
            logical: SLoff::of_number(payload.number_u64()?)?,
            compressed: SCoff::of_number(payload.number_u64()?)?,
        };
        let given = ExtentId::of_number(payload.number_u64()?)?;
        let next = SRoff::of_number(payload.number_u64()?)?;
        payload.done()?;
        Ok((ExtentRun { stream: StreamId::new(stream), totals, given, next }, rest))
    }

    /// Every run in a batch answer, each followed by its own rows.
    pub fn all_of_args(mut args: &[Arg]) -> Result<Vec<(ExtentRun, Vec<ExtentRow>)>> {
        let mut runs = Vec::new();
        while !args.is_empty() {
            let (run, rest) = ExtentRun::of_args(args)?;
            let (rows, rest) = ExtentRow::some_of_args(rest, run.given.number())?;
            runs.push((run, rows));
            args = rest;
        }
        Ok(runs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_stream_counts_keep_the_existing_wire_sentinel() {
        let shape = Shape { streams: StreamId::NONE, ..Shape::plain() };
        let args = shape.args();
        let (mut payload, _) = structure(&args, Op::ShapeResp, "shape").unwrap();
        assert_eq!(payload.number_u64().unwrap(), UNKNOWN);
        assert_eq!(Shape::of_args(&args).unwrap().0, shape);
    }

    fn one(id: u32) -> Described {
        let id = ObjectId::new(id);
        Described {
            object: ObjectName {
                range: RangeId::new(2),
                object: id,
                created: TimeSec::new(1_000_000),
            },
            dn: DnId::new(1),
            stream: StreamName {
                range: RangeId::new(2),
                stream: StreamId::new(17),
                created: TimeSec::new(1_000_000),
            },
            sn: SnId::new(3),
            keyed: false,
            framing: Framing::Lf,
            first_record: ORoff::new(400),
        }
    }

    #[test]
    fn an_object_survives_the_wire() {
        let args = one(9).args();
        assert_eq!(args.len(), Described::ARGS);
        let (back, rest) = Described::of_args(&args).expect("a row");
        assert_eq!(back, Some(one(9)));
        assert!(rest.is_empty());
    }

    /// ⚠️ **The bug this file exists for.** A batch answer holds one row per name,
    /// and counting them by chopping the args into fixed-width pieces of the wrong
    /// width gives a plausible wrong number — 8 fields read 5 at a time is two
    /// chunks per object, so every object was counted twice.
    #[test]
    fn rows_are_counted_by_parsing_not_by_chopping() {
        let mut args = Vec::new();
        for id in [1, 2, 3] {
            args.extend(one(id).args());
        }
        let rows = Described::all_of_args(&args).expect("rows");
        assert_eq!(rows.len(), 3, "three objects, not six");
        assert_eq!(rows.iter().filter(|row| row.is_some()).count(), 3);
        // The old arithmetic was wrong whenever the row changed. The self-contained form
        // makes the logical row width one and leaves its payload to the row parser.
        assert_eq!(args.len(), 3 * Described::ARGS);
        assert_eq!(args.len() / Described::ARGS, 3);
    }

    /// A missing name still occupies a row, so a batch answer lines up with the
    /// question — and it reads back as absent rather than as an object with id 0.
    #[test]
    fn a_missing_name_is_a_row_that_is_not_an_object() {
        let mut args = one(1).args();
        args.extend(Described::missing());
        args.extend(one(2).args());
        let rows = Described::all_of_args(&args).expect("rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1], None, "the gap");
        assert_eq!(rows.iter().flatten().count(), 2, "two objects found of three asked");
    }

    #[test]
    fn a_short_or_ragged_answer_is_refused() {
        let args = one(1).args();
        assert!(Described::of_args(&args[..0]).is_err());
        let mut ragged = args.clone();
        ragged.push(StructWriter::new().finish(Op::NumberResp));
        assert!(Described::all_of_args(&ragged).is_err(), "a row and a fragment");
    }

    /// A framing the DN could not have meant is not an object either.
    #[test]
    fn an_impossible_framing_is_refused() {
        let mut args = one(1).args();
        let payload = args[0].bytes_mut();
        let mut writer = crate::wire::MessageBytes::from(std::mem::take(payload));
        writer.write_at(57, &[99]).unwrap();
        *payload = writer.into();
        assert!(Described::of_args(&args).is_err());
    }

    fn stream(stream: u32, first_record: u64, records: u64) -> StreamRow {
        let stream = StreamId::new(stream);
        StreamRow {
            stream,
            sn: SnId::new(0),
            first_record: ORoff::new(first_record),
            records: SRoff::new(records),
            created: TimeSec::new(1_000_000),
            totals: Totals::unknown(),
            given: ExtentId::FIRST,
        }
    }

    /// The header says how many rows follow, so a reader never guesses — and the
    /// rows come back as themselves.
    #[test]
    fn a_shape_and_its_streams_survive_the_wire() {
        let shape = Shape {
            streams: StreamId::new(3),
            given: StreamId::new(2),
            extents: OEoff::new(40),
            entries: 3,
            next: ORoff::new(700),
            records: ORoff::NONE,
            logical: OLoff::NONE,
            compressed: OCoff::NONE,
        };
        let rows = vec![stream(1, 0, 300), stream(2, 300, 400)];
        let mut args = shape.args();
        for row in &rows {
            args.extend(row.args());
        }
        let (back, rest) = Shape::of_args(&args).expect("a shape");
        assert_eq!(back, shape);
        let (given, rest) = StreamRow::some_of_args(rest, back.given.number()).expect("streams");
        assert_eq!(given, rows);
        assert!(rest.is_empty());
    }

    /// ⚠️ `UNKNOWN` is *nobody asked*, not *none*, and it sits at the top of the
    /// range because zero is a count a stream can legitimately have (§8.1.6.3).
    #[test]
    fn unknown_is_not_zero() {
        assert_ne!(UNKNOWN, 0);
        let open = stream(9, 0, UNKNOWN);
        assert_eq!(open.records, SRoff::NONE, "the stream being appended to");
        let empty = stream(9, 0, 0);
        assert_eq!(
            empty.records,
            SRoff::ZERO,
            "and a sealed stream holding nothing is not the same"
        );
        assert_ne!(open, empty);
        // A plain answer knows its own entry count and nothing else.
        let plain = Shape::plain();
        assert_eq!((plain.entries, plain.extents), (1, OEoff::NONE));
        assert!(!plain.more(), "nothing follows a plain answer");
    }

    /// The header's count is what bounds the read, so a truncated answer is refused
    /// rather than read as fewer rows.
    #[test]
    fn a_shape_promising_more_rows_than_it_carries_is_refused() {
        let shape = Shape {
            streams: StreamId::new(9),
            given: StreamId::new(3),
            extents: OEoff::NONE,
            entries: 4,
            next: ORoff::NONE,
            records: ORoff::NONE,
            logical: OLoff::NONE,
            compressed: OCoff::NONE,
        };
        let mut args = shape.args();
        args.extend(stream(1, 0, 10).args());
        let (back, rest) = Shape::of_args(&args).expect("a shape");
        assert!(
            StreamRow::some_of_args(rest, back.given.number()).is_err(),
            "three promised, one given"
        );
    }

    fn extent(id: u64, first_record: u64, records: u64) -> ExtentRow {
        ExtentRow {
            sn: SnId::new(2),
            id: ExtentId::of_number(id).expect("an extent"),
            created: TimeSec::new(1_000_000),
            first_record: SRoff::new(first_record),
            records: ERoff::new(records as u32),
            logical: if records == UNKNOWN {
                ELoff::NONE
            } else {
                ELoff::new(records as u32 * 100)
            },
            length: if records == UNKNOWN { ECoff::NONE } else { ECoff::new(records as u32 * 10) },
            disks: vec![
                (EnId::new(3), DiskId::new(0)),
                (EnId::new(1), DiskId::new(0)),
                (EnId::new(0), DiskId::new(0)),
                (EnId::new(2), DiskId::new(0)),
            ],
        }
    }

    /// A run of extents, with the instance list length-prefixed so a sealed extent
    /// may later carry a different number than an open one.
    #[test]
    fn a_run_of_extents_survives_the_wire() {
        let totals = Totals {
            extents: ExtentId::new(5),
            logical: SLoff::new(900),
            compressed: SCoff::new(300),
        };
        let run = ExtentRun {
            stream: StreamId::new(7),
            totals,
            given: ExtentId::new(2),
            next: SRoff::new(90),
        };
        let rows = vec![extent(1, 0, 40), extent(2, 40, 50)];
        let mut args = run.args();
        for row in &rows {
            args.extend(row.args());
        }
        let (back, rest) = ExtentRun::of_args(&args).expect("a run");
        assert_eq!(back, run);
        let (given, rest) = ExtentRow::some_of_args(rest, back.given.number()).expect("extents");
        assert_eq!(given, rows);
        assert!(rest.is_empty());
        assert_eq!(
            given[0].disks,
            vec![
                (EnId::new(3), DiskId::new(0)),
                (EnId::new(1), DiskId::new(0)),
                (EnId::new(0), DiskId::new(0)),
                (EnId::new(2), DiskId::new(0)),
            ],
            "primary first, in order"
        );
    }

    /// Instance lists of different lengths in one answer, which is what erasure
    /// coding will produce once a sealed extent has fragments instead of copies.
    #[test]
    fn extents_may_carry_different_numbers_of_instances() {
        let mut wide = extent(1, 0, 40);
        wide.disks = (0..9).map(|i| (EnId::new(i), DiskId::new(0))).collect();
        let narrow = ExtentRow { disks: vec![(EnId::new(9), DiskId::new(0))], ..extent(2, 40, 50) };
        let mut args = Vec::new();
        args.extend(wide.args());
        args.extend(narrow.args());
        let (rows, rest) = ExtentRow::some_of_args(&args, 2).expect("extents");
        assert_eq!(rows[0].disks.len(), 9);
        assert_eq!(rows[1].disks.len(), 1);
        assert!(rest.is_empty());
    }

    /// ⚠️ Sealed is not a field: it is whether the record count is known. Carrying it
    /// twice would let the two copies disagree.
    #[test]
    fn sealed_is_whether_the_count_is_known() {
        assert!(extent(1, 0, 40).sealed());
        assert!(!extent(2, 40, UNKNOWN).sealed(), "the one being appended to");
        assert!(extent(3, 90, 0).sealed(), "and a sealed extent holding nothing is sealed");
    }

    /// A row claiming more instances than it carries is refused rather than read
    /// short, which is the length-prefix equivalent of a ragged answer.
    #[test]
    fn an_extent_lying_about_its_instances_is_refused() {
        let mut args = extent(1, 0, 40).args();
        let payload = args[0].bytes_mut();
        let mut writer = crate::wire::MessageBytes::from(std::mem::take(payload));
        writer.write_at(40, &99u32.to_le_bytes()).unwrap();
        *payload = writer.into();
        assert!(ExtentRow::of_args(&args).is_err());
        assert!(ExtentRow::of_args(&args[..0]).is_err(), "and a short row too");
    }

    /// And the counts survive being complete while the detail is partial, which is
    /// the whole point of §9.1.3.2.
    #[test]
    fn an_answer_can_be_complete_in_counts_and_partial_in_detail() {
        let shape = Shape {
            streams: StreamId::new(1000),
            given: StreamId::new(2),
            extents: OEoff::NONE,
            entries: 3,
            next: ORoff::new(512),
            records: ORoff::NONE,
            logical: OLoff::NONE,
            compressed: OCoff::NONE,
        };
        let args = shape.args();
        let (back, _) = Shape::of_args(&args).expect("a shape");
        assert_eq!(back.streams, StreamId::new(1000), "how many there are");
        assert_eq!(back.given, StreamId::new(2), "how many are described");
        assert!(back.more(), "and where to carry on");
        assert_eq!(back.next, ORoff::new(512));
    }

    /// ⚠️ **The rule #111 turns on**, tested where it lives rather than through the tier that
    /// calls it. A caller with no opinion gets whatever the path points at, which is what a
    /// path means; a caller with one gets an error rather than a different object.
    #[test]
    fn an_expectation_holds_only_for_the_object_it_names() {
        let mine = ObjectName {
            range: RangeId::new(2),
            object: ObjectId::new(7),
            created: TimeSec::new(1_000_000),
        };
        assert!(Expecting::whatever().holds(mine), "no opinion, so anything");
        assert!(!Expecting::whatever().asked());
        assert!(Expecting::of(mine).holds(mine));

        // ⚠️ Every component, because any one of them differing is a different object — and
        // the second is the one a replacement inside a second will *not* differ in (§17.35.2).
        for other in [
            ObjectName { range: RangeId::new(3), ..mine },
            ObjectName { object: ObjectId::new(8), ..mine },
            ObjectName { created: TimeSec::new(1_000_001), ..mine },
        ] {
            assert!(!Expecting::of(mine).holds(other), "{other} is not {mine}");
        }
    }

    /// ⚠️ **`NONE` is "no opinion", and zero is a real object** — the sentinel moved to the top
    /// of the range so that slot 0 of an arena is usable (§17.37.2). A test with zero meaning
    /// nothing is exactly what would have gone on passing while an object numbered zero became
    /// unaddressable.
    #[test]
    fn an_unset_object_is_no_opinion_and_zero_is_not() {
        let unset =
            ObjectName { range: RangeId::new(4), object: ObjectId::NONE, created: TimeSec::new(9) };
        assert!(!Expecting::of(unset).asked(), "a range and a second alone name nothing");
        let first =
            ObjectName { range: RangeId::new(0), object: ObjectId::FIRST, created: TimeSec::EPOCH };
        assert!(Expecting::of(unset).holds(first), "no opinion holds of anything");
        assert!(Expecting::of(first).asked(), "and object zero is an object");
        assert!(!Expecting::of(first).holds(unset));
    }
}
