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
use crate::placement::Expansion;
use crate::time::TimeSec;
use crate::wire::Arg;
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
    /// The stream this answer is about, **by its whole name**: the last one for an append,
    /// or the one holding a record for a read.
    ///
    /// ⚠️ **The name and not the id** (#109), because everything the FE does next — open an
    /// extent, seal one, locate a record — keys a stream at the SN by
    /// `(RangeId, StreamId, CreateSec)`, and the DN is the only tier holding all three.
    pub stream: StreamName,
    pub sn: u32,
    pub keyed: bool,
    pub framing: Framing,
    /// The object-local number of that stream's first record, which is what makes
    /// an object's numbering continuous across the streams it is made of.
    pub first_record: u64,
}

impl Described {
    /// Fields per row on the wire. Public because a caller sizing a buffer wants it,
    /// and **derived from the shape rather than written down twice**.
    pub const ARGS: usize = 5 + ObjectName::ARGS + StreamName::ARGS;

    /// A row meaning "there is no such object", for a batch answer that must line up
    /// with the names it was asked about. A zero id is what marks it.
    ///
    /// ⚠️ §9.1.3.7 will retire this: once an answer may be short, lining up by
    /// position stops working and the answer has to say how many names it covered.
    pub fn missing() -> Vec<Arg> {
        let mut args = ObjectName::default().args();
        args.extend(StreamName::default().args());
        args.extend([
            Arg::Number(0),
            Arg::Flag(false),
            Arg::Number(0),
            Arg::Number(0),
            Arg::Number(0),
        ]);
        args
    }

    pub fn args(&self) -> Vec<Arg> {
        let (kind, width) = self.framing.wire();
        let mut args = self.object.args();
        args.extend(self.stream.args());
        args.extend([
            Arg::Number(self.sn as u64),
            Arg::Flag(self.keyed),
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(self.first_record),
        ]);
        args
    }

    /// And back, returning whatever followed it. `None` for a row marked missing,
    /// since a zero id is not an object.
    pub fn of_args(args: &[Arg]) -> Result<(Option<Described>, &[Arg])> {
        let (object, after) = ObjectName::of_args(args)?;
        let (stream, after) = StreamName::of_args(after)?;
        let [
            Arg::Number(sn),
            Arg::Flag(keyed),
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(first_record),
            rest @ ..,
        ] = after
        else {
            return Err(Error::new("that answer did not describe an object"));
        };
        // ⚠️ A zero *object id* is what marks a missing name, not a zero range: range 0 is a real
        // range, and an id is minted from one so it is never zero for a thing that exists.
        if object.object == 0 {
            return Ok((None, rest));
        }
        Ok((
            Some(Described {
                object,
                stream,
                sn: *sn as u32,
                keyed: *keyed,
                framing: Framing::of_wire(*kind, *width)?,
                first_record: *first_record,
            }),
            rest,
        ))
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
    pub streams: u64,
    /// Stream rows that follow.
    pub given: u64,
    /// Extents across the streams described, or `UNKNOWN` when no SN was asked.
    pub extents: u64,
    /// Entries this answer spent, against the limit it was given (§9.1.3.7).
    pub entries: u64,
    /// The object-local record to ask from next, or `UNKNOWN` when nothing follows.
    pub next: u64,
}

impl Shape {
    /// What an answer carrying no detail says: one object, nothing else asked about.
    pub fn plain() -> Shape {
        Shape { streams: 0, given: 0, extents: UNKNOWN, entries: 1, next: UNKNOWN }
    }

    pub fn more(&self) -> bool {
        self.next != UNKNOWN
    }

    pub fn args(&self) -> Vec<Arg> {
        vec![
            Arg::Number(self.streams),
            Arg::Number(self.given),
            Arg::Number(self.extents),
            Arg::Number(self.entries),
            Arg::Number(self.next),
        ]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Shape, &[Arg])> {
        let [
            Arg::Number(streams),
            Arg::Number(given),
            Arg::Number(extents),
            Arg::Number(entries),
            Arg::Number(next),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("that answer did not say what shape it was"));
        };
        Ok((
            Shape {
                streams: *streams,
                given: *given,
                extents: *extents,
                entries: *entries,
                next: *next,
            },
            rest,
        ))
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
    ($name:ident, $minter:ident, $what:ident, $mints:literal) => {
        #[doc = concat!("What names one of ", $mints, " for ever.")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name {
            pub $minter: u32,
            pub $what: u32,
            pub created: TimeSec,
        }

        impl $name {
            pub const ARGS: usize = 3;

            pub fn args(&self) -> Vec<Arg> {
                vec![
                    Arg::Number(self.$minter as u64),
                    Arg::Number(self.$what as u64),
                    Arg::Number(self.created.number()),
                ]
            }

            pub fn of_args(args: &[Arg]) -> Result<($name, &[Arg])> {
                let [Arg::Number(minter), Arg::Number(id), Arg::Number(created), rest @ ..] = args
                else {
                    return Err(Error::new(concat!(
                        $mints,
                        " is named by a minter, an id and a second"
                    )));
                };
                let name = $name {
                    $minter: *minter as u32,
                    $what: *id as u32,
                    created: TimeSec::new(*created),
                };
                Ok((name, rest))
            }
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

// ⚠️ **Scoped to the range and not to the DN**, both of them, because #75 makes the range the
// durable key precisely so it can move between DNs. `routing::range_of_dn` stands in until ranges
// exist. And ⚠️ **`range` is not `sn`**: the range is *whose* a thing is, the SN is *where* it lives.
name!(ObjectName, range, object, "an object");
name!(StreamName, range, stream, "a stream");

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
    /// Arguments per object, which is what a batch is cut into.
    pub const ARGS: usize = 4;

    pub fn args(&self) -> Vec<Arg> {
        let (kind, width) = self.framing.wire();
        vec![
            Arg::Text(self.name.clone()),
            Arg::Flag(self.keyed),
            Arg::Number(kind),
            Arg::Number(width),
        ]
    }

    pub fn of_args(args: &[Arg]) -> Result<Wanted> {
        match args {
            [Arg::Text(name), Arg::Flag(keyed), Arg::Number(kind), Arg::Number(width)] => {
                Ok(Wanted {
                    name: name.clone(),
                    keyed: *keyed,
                    framing: Framing::of_wire(*kind, *width)?,
                })
            }
            _ => Err(Error::new("a create takes a name, a flavour and a framing")),
        }
    }

    /// Every object in a batch. ⚠️ **A trailing part-object is an error, not a truncation**:
    /// a batch that does not divide is a batch somebody built wrong, and creating the whole
    /// ones would make a caller's list and the objects that exist disagree silently.
    pub fn all_of_args(args: &[Arg]) -> Result<Vec<Wanted>> {
        if !args.len().is_multiple_of(Wanted::ARGS) {
            return Err(Error::new(format!(
                "a batch create of {} arguments does not divide into objects of {}",
                args.len(),
                Wanted::ARGS
            )));
        }
        args.chunks(Wanted::ARGS).map(Wanted::of_args).collect()
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
    pub extents: u64,
    pub logical: u64,
    pub compressed: u64,
}

impl Totals {
    pub const ARGS: usize = 3;

    /// Nothing known yet: a stream nobody has asked an SN about.
    pub fn unknown() -> Totals {
        Totals { extents: UNKNOWN, logical: UNKNOWN, compressed: UNKNOWN }
    }

    pub fn args(&self) -> Vec<Arg> {
        vec![Arg::Number(self.extents), Arg::Number(self.logical), Arg::Number(self.compressed)]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Totals, &[Arg])> {
        let [Arg::Number(extents), Arg::Number(logical), Arg::Number(compressed), rest @ ..] = args
        else {
            return Err(Error::new("that answer's totals are not totals"));
        };
        Ok((Totals { extents: *extents, logical: *logical, compressed: *compressed }, rest))
    }

    /// Bytes the user is charged for. ⚠️ See `Filled::physical`: `expansion` is the storage
    /// class's ratio, never a count of the copies that happen to exist.
    pub fn physical(&self, expansion: Expansion) -> u64 {
        match self.compressed {
            UNKNOWN => UNKNOWN,
            compressed => expansion.bytes(compressed),
        }
    }
}

/// One of an object's streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRow {
    pub stream: u64,
    pub sn: u32,
    /// The object-local number of this stream's first record.
    pub first_record: u64,
    /// Records in it once sealed, and **`UNKNOWN` while it is the one being appended
    /// to** — how far that one reaches is not the DN's to say (§6.3).
    pub records: u64,
    /// The second the stream was made. The DN's own, so always exact.
    pub created: TimeSec,
    /// What it has come to. `extents` is here rather than in the row proper because the
    /// three travel together and mean the same thing about staleness.
    pub totals: Totals,
    /// Extent rows that follow this one. ⚠️ **The answer is interleaved** — a stream then
    /// its extents, repeated — so a reader never attributes extents to a stream by
    /// position, which is §17.19's defect.
    pub given: u64,
}

impl StreamRow {
    pub const ARGS: usize = 5 + Totals::ARGS;

    pub fn args(&self) -> Vec<Arg> {
        let mut args = vec![
            Arg::Number(self.stream),
            Arg::Number(self.sn as u64),
            Arg::Number(self.first_record),
            Arg::Number(self.records),
            Arg::Number(self.created.number()),
        ];
        args.extend(self.totals.args());
        args.push(Arg::Number(self.given));
        args
    }

    pub fn of_args(args: &[Arg]) -> Result<(StreamRow, &[Arg])> {
        let [
            Arg::Number(stream),
            Arg::Number(sn),
            Arg::Number(first_record),
            Arg::Number(records),
            Arg::Number(created),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("that answer's streams are not streams"));
        };
        let (totals, rest) = Totals::of_args(rest)?;
        let [Arg::Number(given), rest @ ..] = rest else {
            return Err(Error::new("that stream did not say how many extents follow"));
        };
        Ok((
            StreamRow {
                stream: *stream,
                sn: *sn as u32,
                first_record: *first_record,
                records: *records,
                created: TimeSec::new(*created),
                totals,
                given: *given,
            },
            rest,
        ))
    }

    /// `given` rows, and whatever followed them.
    pub fn some_of_args(args: &[Arg], given: u64) -> Result<(Vec<StreamRow>, &[Arg])> {
        let mut rows = Vec::with_capacity(given as usize);
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
    pub sn: u32,
    pub id: u64,
    pub created: TimeSec,
    /// Its first record. **Stream-local from the SN, object-local once the FE has
    /// converted it** (§17.16.2 is what happens when that is skipped).
    pub first_record: u64,
    /// Records in it, or `UNKNOWN` while it is the one being appended to — which is
    /// also what says it is not sealed.
    pub records: u64,
    /// Logical bytes, or `UNKNOWN` while it is open.
    ///
    /// ⚠️ **All three of §9.1.3.6's measures are here on purpose.** A caller budgeting
    /// in logical bytes cannot plan from a compressed length, and one that wants to
    /// bound a range *inside* an extent needs that extent's base in the space it means
    /// to bound (§9.1.3.5.4). Two of three would silently make one of the three
    /// budgets unplannable.
    pub logical: u64,
    /// Compressed bytes, or `UNKNOWN` while it is open.
    pub length: u64,
    /// The ENs holding it, **primary first**, which is what makes a hint a route
    /// rather than a description. Length-prefixed on the wire because erasure coding
    /// will give a sealed extent a different number of them than an open one has
    /// (§4.3.2).
    /// The **disks** holding its instances, primary first. ⚠️ Disks rather than ENs — see
    /// `placement::en_of` for why, and for the lookup that turns one into a process.
    pub disks: Vec<u32>,
}

impl ExtentRow {
    pub fn sealed(&self) -> bool {
        self.records != UNKNOWN
    }

    pub fn args(&self) -> Vec<Arg> {
        let mut args = vec![
            Arg::Number(self.sn as u64),
            Arg::Number(self.id),
            Arg::Number(self.created.number()),
            Arg::Number(self.first_record),
            Arg::Number(self.records),
            Arg::Number(self.logical),
            Arg::Number(self.length),
            Arg::Number(self.disks.len() as u64),
        ];
        args.extend(self.disks.iter().map(|disk| Arg::Number(*disk as u64)));
        args
    }

    pub fn of_args(args: &[Arg]) -> Result<(ExtentRow, &[Arg])> {
        let [
            Arg::Number(sn),
            Arg::Number(id),
            Arg::Number(created),
            Arg::Number(first_record),
            Arg::Number(records),
            Arg::Number(logical),
            Arg::Number(length),
            Arg::Number(count),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("that answer's extents are not extents"));
        };
        let count = *count as usize;
        if rest.len() < count {
            return Err(Error::new(format!(
                "an extent says it has {count} instances and carries {}",
                rest.len()
            )));
        }
        let (given, rest) = rest.split_at(count);
        let mut disks = Vec::with_capacity(count);
        for one in given {
            match one {
                Arg::Number(disk) => disks.push(*disk as u32),
                _ => return Err(Error::new("an instance list is numbers")),
            }
        }
        Ok((
            ExtentRow {
                sn: *sn as u32,
                id: *id,
                created: TimeSec::new(*created),
                first_record: *first_record,
                records: *records,
                logical: *logical,
                length: *length,
                disks,
            },
            rest,
        ))
    }

    pub fn some_of_args(args: &[Arg], given: u64) -> Result<(Vec<ExtentRow>, &[Arg])> {
        let mut rows = Vec::with_capacity(given as usize);
        let mut rest = args;
        for _ in 0..given {
            let (row, after) = ExtentRow::of_args(rest)?;
            rows.push(row);
            rest = after;
        }
        Ok((rows, rest))
    }
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
        vec![
            Arg::Number(self.asked),
            Arg::Number(self.given),
            Arg::Number(self.entries),
            Arg::Number(self.next),
        ]
    }

    pub fn of_args(args: &[Arg]) -> Result<(Batch, &[Arg])> {
        let [
            Arg::Number(asked),
            Arg::Number(given),
            Arg::Number(entries),
            Arg::Number(next),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("that batch answer did not say what it covered"));
        };
        Ok((Batch { asked: *asked, given: *given, entries: *entries, next: *next }, rest))
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
        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let whole = args;
            let (shape, mut rest) = Shape::of_args(args)?;
            if rest.len() < Described::ARGS {
                return Err(Error::new("a batch answer stopped inside an object row"));
            }
            rest = &rest[Described::ARGS..];
            for _ in 0..shape.given {
                let (row, after) = StreamRow::of_args(rest)?;
                rest = after;
                for _ in 0..row.given {
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
    /// `(name, first stream-local record to describe from)`, in order.
    ///
    /// ⚠️ **A whole name per stream, not an id against one shared range.** A batch may
    /// name streams of different ranges — and once ranges split and move (#75) it routinely
    /// will — so the range travels with each entry rather than once at the front.
    pub at: Vec<(StreamName, u64)>,
}

impl Listing {
    pub fn args(&self) -> Vec<Arg> {
        let mut args = vec![Arg::Number(self.limit), Arg::Number(self.at.len() as u64)];
        for (name, from) in &self.at {
            args.extend(name.args());
            args.push(Arg::Number(*from));
        }
        args
    }

    pub fn of_args(args: &[Arg]) -> Result<Listing> {
        let [Arg::Number(limit), Arg::Number(count), rest @ ..] = args else {
            return Err(Error::new("list-extents takes a limit and a run of streams"));
        };
        let count = *count as usize;
        let each = StreamName::ARGS + 1;
        if rest.len() != count * each {
            return Err(Error::new(format!(
                "list-extents says it names {count} streams and carries {}",
                rest.len() / each
            )));
        }
        let mut at = Vec::with_capacity(count);
        let mut rest = rest;
        for _ in 0..count {
            let (name, after) = StreamName::of_args(rest)?;
            let [Arg::Number(from), after @ ..] = after else {
                return Err(Error::new("a stream is named, then the record to start from"));
            };
            at.push((name, *from));
            rest = after;
        }
        Ok(Listing { limit: *limit, at })
    }
}

/// What an SN says about a run of one stream's extents: complete in counts, partial
/// in detail (§9.1.3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentRun {
    /// Which stream this run is about. ⚠️ **Named rather than positional**, so a batch
    /// answer says what it is answering instead of relying on the order matching the
    /// question — §17.19's defect was exactly that assumption.
    pub stream: u64,
    /// What the stream has come to, which the SN is the authority for and the DN caches
    /// (§17.25.1). Includes the extent count, whether or not the rows are all here.
    pub totals: Totals,
    /// Rows that follow.
    pub given: u64,
    /// The **stream-local** record to ask from next, or `UNKNOWN` when nothing
    /// follows.
    pub next: u64,
}

impl ExtentRun {
    pub fn args(&self) -> Vec<Arg> {
        let mut args = vec![Arg::Number(self.stream)];
        args.extend(self.totals.args());
        args.push(Arg::Number(self.given));
        args.push(Arg::Number(self.next));
        args
    }

    pub fn of_args(args: &[Arg]) -> Result<(ExtentRun, &[Arg])> {
        let [Arg::Number(stream), rest @ ..] = args else {
            return Err(Error::new("that run of extents does not say which stream"));
        };
        let (totals, rest) = Totals::of_args(rest)?;
        let [Arg::Number(given), Arg::Number(next), rest @ ..] = rest else {
            return Err(Error::new("that answer did not say what run of extents it was"));
        };
        Ok((ExtentRun { stream: *stream, totals, given: *given, next: *next }, rest))
    }

    /// Every run in a batch answer, each followed by its own rows.
    pub fn all_of_args(mut args: &[Arg]) -> Result<Vec<(ExtentRun, Vec<ExtentRow>)>> {
        let mut runs = Vec::new();
        while !args.is_empty() {
            let (run, rest) = ExtentRun::of_args(args)?;
            let (rows, rest) = ExtentRow::some_of_args(rest, run.given)?;
            runs.push((run, rows));
            args = rest;
        }
        Ok(runs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(id: u32) -> Described {
        Described {
            object: ObjectName { range: 2, object: id, created: TimeSec::new(1_000_000) },
            stream: StreamName { range: 2, stream: 17, created: TimeSec::new(1_000_000) },
            sn: 3,
            keyed: false,
            framing: Framing::Lf,
            first_record: 400,
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
        // What the old arithmetic would have said, kept as the thing not to do. ⚠️ It said
        // four when the row was eight fields and says six now the row is ten — the number is
        // not the point, being wrong in a different way each time the shape moves is.
        assert_eq!(args.len(), 3 * Described::ARGS);
        assert_ne!(args.len() / 5, 3, "chunks of five find something other than three");
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
        assert!(Described::of_args(&args[..7]).is_err());
        let mut ragged = args.clone();
        ragged.push(Arg::Number(1));
        assert!(Described::all_of_args(&ragged).is_err(), "a row and a fragment");
    }

    /// A framing the DN could not have meant is not an object either.
    #[test]
    fn an_impossible_framing_is_refused() {
        let mut args = one(1).args();
        // ⚠️ Counted back from the end rather than forward from the start: the framing is the
        // last three fields but one, and this test broke when the stream id at the front grew
        // into a whole name (#109). A test that hard-codes a position is a test that has to be
        // edited every time the shape does — which is the same complaint the *next* test makes.
        args[Described::ARGS - 3] = Arg::Number(99);
        assert!(Described::of_args(&args).is_err());
    }

    fn stream(stream: u64, first_record: u64, records: u64) -> StreamRow {
        StreamRow {
            stream,
            sn: 0,
            first_record,
            records,
            created: TimeSec::new(1_000_000),
            totals: Totals::unknown(),
            given: 0,
        }
    }

    /// The header says how many rows follow, so a reader never guesses — and the
    /// rows come back as themselves.
    #[test]
    fn a_shape_and_its_streams_survive_the_wire() {
        let shape = Shape { streams: 3, given: 2, extents: 40, entries: 3, next: 700 };
        let rows = vec![stream(1, 0, 300), stream(2, 300, 400)];
        let mut args = shape.args();
        for row in &rows {
            args.extend(row.args());
        }
        let (back, rest) = Shape::of_args(&args).expect("a shape");
        assert_eq!(back, shape);
        let (given, rest) = StreamRow::some_of_args(rest, back.given).expect("streams");
        assert_eq!(given, rows);
        assert!(rest.is_empty());
    }

    /// ⚠️ `UNKNOWN` is *nobody asked*, not *none*, and it sits at the top of the
    /// range because zero is a count a stream can legitimately have (§8.1.6.3).
    #[test]
    fn unknown_is_not_zero() {
        assert_ne!(UNKNOWN, 0);
        let open = stream(9, 0, UNKNOWN);
        assert_eq!(open.records, UNKNOWN, "the stream being appended to");
        let empty = stream(9, 0, 0);
        assert_eq!(empty.records, 0, "and a sealed stream holding nothing is not the same");
        assert_ne!(open, empty);
        // A plain answer knows its own entry count and nothing else.
        let plain = Shape::plain();
        assert_eq!((plain.entries, plain.extents), (1, UNKNOWN));
        assert!(!plain.more(), "nothing follows a plain answer");
    }

    /// The header's count is what bounds the read, so a truncated answer is refused
    /// rather than read as fewer rows.
    #[test]
    fn a_shape_promising_more_rows_than_it_carries_is_refused() {
        let shape = Shape { streams: 9, given: 3, extents: UNKNOWN, entries: 4, next: UNKNOWN };
        let mut args = shape.args();
        args.extend(stream(1, 0, 10).args());
        let (back, rest) = Shape::of_args(&args).expect("a shape");
        assert!(StreamRow::some_of_args(rest, back.given).is_err(), "three promised, one given");
    }

    fn extent(id: u64, first_record: u64, records: u64) -> ExtentRow {
        ExtentRow {
            sn: 2,
            id,
            created: TimeSec::new(1_000_000),
            first_record,
            records,
            logical: if records == UNKNOWN { UNKNOWN } else { records * 100 },
            length: if records == UNKNOWN { UNKNOWN } else { records * 10 },
            disks: vec![3, 1, 0, 2],
        }
    }

    /// A run of extents, with the instance list length-prefixed so a sealed extent
    /// may later carry a different number than an open one.
    #[test]
    fn a_run_of_extents_survives_the_wire() {
        let totals = Totals { extents: 5, logical: 900, compressed: 300 };
        let run = ExtentRun { stream: 7, totals, given: 2, next: 90 };
        let rows = vec![extent(1, 0, 40), extent(2, 40, 50)];
        let mut args = run.args();
        for row in &rows {
            args.extend(row.args());
        }
        let (back, rest) = ExtentRun::of_args(&args).expect("a run");
        assert_eq!(back, run);
        let (given, rest) = ExtentRow::some_of_args(rest, back.given).expect("extents");
        assert_eq!(given, rows);
        assert!(rest.is_empty());
        assert_eq!(given[0].disks, vec![3, 1, 0, 2], "primary first, in order");
    }

    /// Instance lists of different lengths in one answer, which is what erasure
    /// coding will produce once a sealed extent has fragments instead of copies.
    #[test]
    fn extents_may_carry_different_numbers_of_instances() {
        let mut wide = extent(1, 0, 40);
        wide.disks = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
        let narrow = ExtentRow { disks: vec![9], ..extent(2, 40, 50) };
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
        args[7] = Arg::Number(99);
        assert!(ExtentRow::of_args(&args).is_err());
        assert!(ExtentRow::of_args(&args[..7]).is_err(), "and a short row too");
    }

    /// And the counts survive being complete while the detail is partial, which is
    /// the whole point of §9.1.3.2.
    #[test]
    fn an_answer_can_be_complete_in_counts_and_partial_in_detail() {
        let shape = Shape { streams: 1000, given: 2, extents: UNKNOWN, entries: 3, next: 512 };
        let args = shape.args();
        let (back, _) = Shape::of_args(&args).expect("a shape");
        assert_eq!(back.streams, 1000, "how many there are");
        assert_eq!(back.given, 2, "how many are described");
        assert!(back.more(), "and where to carry on");
        assert_eq!(back.next, 512);
    }
}
