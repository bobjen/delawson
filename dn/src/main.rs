//! DN, the Directory Node: objects by name, and the streams they are made of.
//!
//! An object is indexed by name here, and it grows a stream at a time on the SNs
//! that record them — no inlining of small objects and no coalescing of several
//! objects into one stream, both of which come later and both of which would hide
//! what a create costs. DN owns the `StreamId`; the SN it picks records
//! `(RangeId, StreamId)`.
//!
//! ⚠️ **The hash table sharded by name is a stand-in for a B-tree** with a read and
//! a write lock per node, walked by locking a parent and then a child. Nothing is
//! deleted yet either, so nothing here copes with reuse.
//!
//! A create waits on an SN, and waits without holding a worker: the names are
//! taken, the streams are asked for, and the client's `Reply` travels with that
//! request to be answered when the SN says the streams exist.

use common::config::{DNConfig, ServiceKind};
use common::framing::Framing;
use common::ids::{DnId, ObjectId, RangeId, SnId, StreamId};
use common::info::{
    Batch, Described, ExtentRow, ExtentRun, Listing, ObjectName, Shape, StreamName, StreamRow,
    Totals, UNKNOWN, Wanted,
};
use common::logw;
use common::offset::ORoff;
use common::range::{Detail, Want};
use common::routing;
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SERVES: &[Op] = &[
    Op::CreateObject,
    Op::BatchCreate,
    Op::ObjectInfo,
    Op::BatchInfo,
    Op::LocateStream,
    Op::ExtendObject,
    Op::MakeDirectory,
    Op::RemoveDirectory,
    Op::List,
    Op::Stat,
    Op::DeleteObject,
    Op::UndeleteObject,
    Op::SetExpiry,
    Op::CloneObject,
    Op::ConcatObjects,
    Op::Count,
    Op::TimeRange,
];

const SHARDS: usize = 64;

/// Which `Detail` bits this build actually answers.
///
/// **Streams and counts locally; extents and instances by asking the SNs** (§17.22). Capabilities
/// need the key of #82 and the stream flags of §4.1.1 do not exist — so a caller asking for those is
/// told which by name rather than handed an answer that quietly omits what it asked for.
const ANSWERS: u64 =
    Detail::Streams.bit() | Detail::Counts.bit() | Detail::Extents.bit() | Detail::Instances.bit();

/// And what a *batch* answers, which is less.
///
/// ⚠️ **Extents are missing on purpose rather than by oversight.** One object's extents cost one ask per
/// SN (§17.22); a batch of a thousand names would want them grouped across *every* object at once, which
/// is the same gather over a wider set. Until that exists a batch says so by name, because an answer
/// that quietly dropped the level asked for is the failure §17's rule exists to prevent.
const BATCH_ANSWERS: u64 = Detail::Streams.bit() | Detail::Counts.bit();

/// What a lookup wants, and a refusal naming the parts of it this build cannot supply.
///
/// **A request with no want field is `Want::plain()`**, which is exactly today's answer — so every
/// caller that predates the field keeps working and keeps costing what it cost (§9.1.3.11).
fn wanted(args: &[Arg], answers: u64) -> Result<Want> {
    let want = match args {
        [] => Want::plain(),
        _ => Want::of_args(args)?.0,
    };
    let beyond = want.beyond(answers);
    if !beyond.is_empty() {
        return Err(Error::new(format!("object-info cannot yet return {}", beyond.join(", "))));
    }
    Ok(want)
}

/// The answer for a lookup: the plain row when a record was named or no detail was
/// asked for, and the levelled answer otherwise.
///
/// ⚠️ **`locate-stream` always gets the plain row.** It asks *which stream holds this
/// record*, an FE's question on the read path, and it must keep the shape `Located::of`
/// reads — so detail is an `object-info` feature and not a lookup feature.
fn describe_at(
    object: &Object,
    range: RangeId,
    dn: DnId,
    record: Option<u64>,
    want: &Want,
    limit: u64,
) -> Option<Answering> {
    match record {
        Some(_) => object.args(range, dn, record).map(Answering::Plain),
        None => match detailed(object, range, dn, want, limit) {
            Some(levelled) => Some(Answering::Levelled(levelled)),
            // No detail asked for, so the row that predates the field, byte for byte.
            None => object.args(range, dn, None).map(Answering::Plain),
        },
    }
}

/// Which shape a lookup's answer takes, and whether SNs have to be asked first.
enum Answering {
    Plain(Vec<Arg>),
    Levelled(Levelled),
}

/// What an object says about itself at the level asked for.
///
/// ⚠️ **The counts are exact even when the detail is not** (§9.1.3.2), which is what lets a caller
/// compute the requests it has not made yet instead of discovering them one answer at a time. The
/// stream count is the DN's own, so it costs nothing to include and is always right.
fn detailed(
    object: &Object,
    range: RangeId,
    dn: DnId,
    want: &Want,
    limit: u64,
) -> Option<Levelled> {
    let args = object.args(range, dn, None)?;
    if want.detail == 0 {
        return None;
    }

    let total = object.streams.len() as u64;
    let from = want.from.record().unwrap_or(0);
    // Streams are in record order, so the first to describe is the last one starting
    // at or before `from` — the same rule a read uses to pick a stream.
    let first =
        object.streams_from().filter(|(at, _)| at.number() <= from).count().saturating_sub(1);

    // One entry for the object, then one per stream, against whichever cap is smaller.
    let cap = match want.limit {
        0 => limit,
        asked => asked.min(limit),
    };
    let room = cap.saturating_sub(1);
    let mut rows = Vec::new();
    if want.wants(Detail::Streams) {
        for (at, held) in object.streams_from().skip(first).take(room as usize) {
            rows.push(StreamRow {
                stream: held.id,
                sn: held.sn,
                first_record: at.number(),
                // ⚠️ UNKNOWN, not zero: how far the stream being appended to reaches is
                // not the DN's to say (§6.3), and a sealed stream may hold nothing.
                records: held.records,
                created: held.created,
                // What the DN last learned. Exact once the stream is sealed, a point in
                // time before that, and UNKNOWN until any SN has been asked (§17.25.1).
                totals: held.totals(),
                given: 0,
            });
        }
    }

    // Where to carry on: the first record of the stream after the last one described,
    // and nothing when the last stream described is the object's last.
    let after = first + rows.len();
    let next = object.streams_from().nth(after).map_or(UNKNOWN, |(at, _)| at.number());
    let shape = Shape {
        streams: if want.wants(Detail::Counts) { total } else { UNKNOWN },
        given: rows.len() as u64,
        extents: UNKNOWN,
        entries: 1 + rows.len() as u64,
        // ⚠️ **Wherever a stream count was given, not only where stream rows were.** A
        // summary — counts and no rows — is what a batch asks the DNs it is not leading
        // (§9.1.3.12), and one that says `nothing more` while holding undescribed streams
        // contradicts the batch header that sent the caller back to it. So this says where a
        // stream listing *would* resume, which is true of a summary and of a truncation
        // alike (§17.24.2).
        next: if want.wants(Detail::Counts) || want.wants(Detail::Streams) {
            next
        } else {
            UNKNOWN
        },
    };

    // The shape first, then the object, then its streams — so a reader knows how many
    // rows follow before it starts reading them.
    Some(Levelled { shape, head: args, rows })
}

/// What a lookup came to before any SN was asked.
struct Levelled {
    shape: Shape,
    head: Vec<Arg>,
    rows: Vec<StreamRow>,
}

impl Levelled {
    /// Flattened, with each stream's extents following its row.
    ///
    /// ⚠️ **Interleaved rather than two blocks**, so the answer is self-describing: a reader takes a
    /// stream, reads the extents it says follow, and takes the next. Two blocks would need the reader
    /// to attribute extents to streams by position, which is §17.19's defect.
    fn args(self, mut extents: HashMap<StreamId, (ExtentRun, Vec<ExtentRow>)>) -> Vec<Arg> {
        let mut out = self.shape.args();
        out.extend(self.head);
        for mut row in self.rows {
            match extents.remove(&row.stream) {
                Some((run, rows)) => {
                    // Straight from the SN, which is more recent than anything cached.
                    row.totals = run.totals;
                    row.given = rows.len() as u64;
                    out.extend(row.args());
                    for extent in rows {
                        out.extend(extent.args());
                    }
                }
                None => {
                    row.given = 0;
                    out.extend(row.args());
                }
            }
        }
        out
    }
}

struct Object {
    /// ⚠️ **`u32`, minted per range**, so an object's name is `(RangeId, ObjectId, CreateSec)` —
    /// the same triple as a stream's and an extent's (§17.35.2). **A path is not part of it**: a
    /// path points at a name and may be repointed as often as a user likes, which is exactly what
    /// a creation second cannot pin and does not have to.
    id: ObjectId,
    /// Where one record ends and the next begins, declared here and never
    /// changed: everything that reads or writes this object's records needs it.
    framing: Framing,
    /// The streams this object is made of, in order. Empty while the name is taken
    /// and the first stream is not named yet, which is what stops two creates of
    /// one name from making two streams.
    ///
    /// A stream fills, so an object grows more of them; the last is the one appends
    /// go to and the record ranges say which holds what.
    streams: Vec<Stream>,
    /// An extension is being asked of an SN, so anything else that wants one waits
    /// rather than asking for a second.
    extending: bool,
    /// Requests waiting for this object to have a stream they can use: the first
    /// stream while it is being made, or the next one while it is being asked for.
    ///
    /// ⚠️ **They live here, under the shard's lock, and not in a table of their
    /// own.** Deciding to wait and then waiting have to be one step: with a lock
    /// released in between, the work can finish and wake nobody, and the waiter is
    /// left holding a `Reply` that nothing will ever answer.
    waiting: Vec<Reply>,
    keyed: bool,
    created: TimeSec,
}

/// What an `extend-object` request turned out to be: the one doing the work, one
/// waiting behind it, or one that can be answered on the spot.
enum Next {
    Ours(u64),
    Wait,
    Say(Response),
}

/// One of an object's streams, and what it has come to.
///
/// ⚠️ **No first record here**, for the reason `sn::Extent` has none (§17.29.1) one tier up:
/// §11's lumping puts many objects in one stream, so a stream's position *in an object* is the
/// object's property and not the stream's. It is recovered by summing the `records` of the
/// streams before it, and only the last stream of an object can be unsealed — so every term of
/// that sum is known (§17.33.1).
///
/// ⚠️ **The DN's largest structure, so every field here is paid 10^12 times** in §13.1.3's
/// worst case. Adding the sizes took it from **40 to 56 bytes**, and four new fields cost
/// the space of two only because `records` stopped being an `Option<u64>` — a sentinel in a
/// space that already reserves one is 8 bytes where the discriminant was 8 plus padding
/// (`EVERY-SPACE-RESERVES-A-SENTINEL`, §8.1.6.3). The layout is exact: six `u64` and two
/// `u32`, no padding.
#[derive(Clone, Copy)]
struct Stream {
    /// ⚠️ **`u32`, and safe for the reason an extent id is** (§17.29.2): a stable reference
    /// to a stream is `(RangeId, StreamId, CreateSec)` (Asserted, Bob, 2026-08-20), so the
    /// id alone was never the name. It has only to be unique among the streams **one range
    /// makes in one second**, which 4.3 × 10^9 is not close to.
    ///
    /// ⚠️ **Scoped to the range, not to the DN.** #75 makes the range the durable key
    /// precisely because a range moves between DNs — so a DN-scoped id would stop meaning
    /// what it meant the first time its range moved. `dn` stands in for `RangeId` until
    /// ranges exist (#109).
    id: StreamId,
    /// Records in it once sealed, and **`UNKNOWN` while it is the one being appended to**.
    ///
    /// ⚠️ **This one does not go stale, unlike the sizes below.** §4.5's exact record count
    /// is what addressing runs on, so a stale record count is a *wrong read*, while a stale
    /// byte count is a slightly wrong listing (§17.25.1).
    records: u64,
    /// The second this stream was handed out. The DN's own, so always exact — which is what
    /// a directory listing wants, and ⚠️ **which turns out to be part of the stream's name**
    /// rather than decoration: it is the third component of `(RangeId, StreamId, CreateSec)`,
    /// and it is what makes a 32-bit id safe.
    created: TimeSec,
    /// Bytes the writer wrote and bytes stored, summed over the stream's extents.
    ///
    /// ⚠️ **Exact for a sealed stream, as-of for a growing one, `UNKNOWN` until an SN has
    /// been asked** (Asserted, Bob, 2026-08-20). A sealed stream cannot change, so one
    /// refresh is right for ever; a growing one is only ever a point in time, and that is
    /// good enough for the listing these exist for.
    logical: u64,
    compressed: u64,
    sn: SnId,
    /// Extents in it. `UNKNOWN` is not representable in 32 bits alongside a real count, so
    /// this uses `u32::MAX` and the accessor is what reads it (§8.1.6.2).
    extents: u32,
}

impl Stream {
    /// A stream just handed out: nothing has come to anything yet.
    fn new(id: StreamId, sn: SnId, created: TimeSec) -> Stream {
        Stream {
            id,
            records: UNKNOWN,
            created,
            logical: UNKNOWN,
            compressed: UNKNOWN,
            sn,
            extents: u32::MAX,
        }
    }

    /// What this stream has come to, in the shape the wire carries.
    fn totals(&self) -> Totals {
        Totals {
            extents: match self.extents {
                u32::MAX => UNKNOWN,
                extents => extents as u64,
            },
            logical: self.logical,
            compressed: self.compressed,
        }
    }

    /// What an SN said the stream has come to. ⚠️ **Only ever moves forward**: an answer
    /// that crossed a newer one in flight must not walk the numbers back, and a sealed
    /// stream's totals are final whatever arrives afterwards.
    fn learned(&mut self, totals: Totals) {
        if totals.extents != UNKNOWN {
            let seen = totals.extents.min(u32::MAX as u64 - 1) as u32;
            if self.extents == u32::MAX || seen > self.extents {
                self.extents = seen;
            }
        }
        for (mine, seen) in
            [(&mut self.logical, totals.logical), (&mut self.compressed, totals.compressed)]
        {
            if seen != UNKNOWN && (*mine == UNKNOWN || seen > *mine) {
                *mine = seen;
            }
        }
    }
}

impl Object {
    /// Each stream with the object-local number of its first record — the running total of
    /// the records in the streams before it.
    ///
    /// ⚠️ **Only the last stream of an object can be unsealed**, so every term of the sum
    /// before the last is a known count and the walk never meets an `UNKNOWN` it needs. The
    /// last one's own count is unknown, which is why it is the last (§17.33.1).
    fn streams_from(&self) -> impl Iterator<Item = (ORoff, &Stream)> {
        self.streams.iter().scan(ORoff::ZERO, |at, stream| {
            let first = *at;
            if stream.records != UNKNOWN {
                *at = ORoff::new(at.number() + stream.records);
            }
            Some((first, stream))
        })
    }

    /// Nothing here can be appended to or read yet: the first stream is being made,
    /// or the next one is being asked for. Waiting is better than being told there is
    /// no stream, which is true for a moment and useless to hear.
    fn between_streams(&self) -> bool {
        self.streams.is_empty() || self.extending
    }

    /// What this object is, at one of its streams: the last for an append, or the one
    /// holding a record for a read.
    fn args(&self, range: RangeId, dn: DnId, record: Option<u64>) -> Option<Vec<Arg>> {
        // ⚠️ The position comes back *with* the stream, because the stream does not carry
        // it (§17.33.1) — the same shape as `sn::Placed`, and for the same reason.
        let (first, held) = match record {
            None => self.last()?,
            Some(record) => self.holding(ORoff::new(record))?,
        };
        Some(
            Described {
                object: self.named(range),
                dn,
                stream: StreamName { range, stream: held.id, created: held.created },
                sn: held.sn,
                keyed: self.keyed,
                framing: self.framing,
                first_record: first.number(),
            }
            .args(),
        )
    }

    /// This object's name, which is `(RangeId, ObjectId, CreateSec)` and not its path — a path
    /// points at this and may be repointed (§17.35.2).
    fn named(&self, range: RangeId) -> ObjectName {
        ObjectName { range, object: self.id, created: self.created }
    }

    /// Records the object holds, which is also where its next stream would start.
    ///
    /// ⚠️ **`UNKNOWN` while the last stream is open**, because how far that one reaches is
    /// not the DN's to say (§6.3) — and a caller adding to `UNKNOWN` would get `u64::MAX`
    /// plus a count, which §17.22.1 is the story of.
    fn records(&self) -> u64 {
        match self.streams_from().last() {
            Some((_, last)) if last.records == UNKNOWN => UNKNOWN,
            Some((at, last)) => at.number() + last.records,
            None => 0,
        }
    }

    /// The stream appends go to, which is the last one, and where it starts.
    fn last(&self) -> Option<(ORoff, Stream)> {
        self.streams_from().last().map(|(at, held)| (at, *held))
    }

    /// The stream holding an object-local record number: the sealed one whose range
    /// covers it, or the last, whose reach is not known here.
    fn holding(&self, record: ORoff) -> Option<(ORoff, Stream)> {
        for (at, held) in self.streams_from() {
            match held.records {
                // The stream still being appended to: anything past its start is
                // routed to it, and the SN and EN say whether it is really there.
                UNKNOWN => return Some((at, *held)),
                records if record.number() < at.number() + records => return Some((at, *held)),
                _ => continue,
            }
        }
        None
    }
}

/// One SN's answer has arrived. The one that takes the count to zero assembles and replies.
///
/// ⚠️ **Refusals are kept and the whole answer fails**, rather than being dropped so that the answer
/// silently lacks extents the caller asked for. §17's rule: a well-formed request for something that
/// did not work says so.
fn arrived(dn: &Arc<DN>, gather: &Arc<Mutex<Gather>>, answer: Response) {
    let ready = {
        let mut held = gather.lock().expect("gather");
        held.left -= 1;
        if answer.status != Status::Ok {
            held.failed.get_or_insert(answer);
        } else {
            match ExtentRun::all_of_args(&answer.args) {
                Ok(runs) => {
                    for (run, rows) in runs {
                        held.runs.insert(run.stream, (run, rows));
                    }
                }
                Err(e) => {
                    held.failed.get_or_insert(Response::error(Status::Internal, e.to_string()));
                }
            }
        }
        held.left == 0
    };
    if !ready {
        return;
    }

    let mut held = gather.lock().expect("gather");
    let Some(reply) = held.reply.take() else { return };
    if let Some(failed) = held.failed.take() {
        return reply.send(failed);
    }
    // ⚠️ The rows come back stream-local and the caller counts from the start of the object, so the
    // DN converts — it is the tier that holds each stream's first record (§17.16.2 is what happens
    // when a conversion like this is skipped).
    let mut runs = std::mem::take(&mut held.runs);
    // ⚠️ **Write back what the SNs said before answering.** This is the only path by which a
    // stream's cached sizes ever stop being UNKNOWN (§17.25.1), so skipping it would leave
    // every listing blank while the numbers sat in a response on their way out the door.
    // A sealed stream's totals are final, so once written they are right for ever; a growing
    // one's are a point in time, which is what `Stream::learned` refusing to go backwards is
    // for — two answers can cross in flight.
    dn.remember(&held.name, &runs);
    let mut extents = 0;
    let mut given = 0;
    let mut converted: HashMap<StreamId, (ExtentRun, Vec<ExtentRow>)> = HashMap::new();
    for row in &held.rows {
        let Some((run, rows)) = runs.remove(&row.stream) else { continue };
        extents += if run.totals.extents == UNKNOWN { 0 } else { run.totals.extents };
        given += rows.len() as u64;
        // Stream-local to object-local, by adding where this stream starts in the object.
        let moved = rows
            .into_iter()
            .map(|extent| ExtentRow {
                first_record: extent.first_record + row.first_record,
                ..extent
            })
            .collect();
        converted.insert(row.stream, (run, moved));
    }
    // ⚠️ **Where to carry on is now the extent level's business.** A stream whose extents were cut
    // short is the place to resume, and it comes before any stream that was not described at all — so
    // the first such stream in record order wins, and only if none was cut does the stream-level
    // continuation stand. Without this an answer that truncated extents reported `nothing more`.
    let cut = held.rows.iter().find_map(|row| {
        let (run, rows) = converted.get(&row.stream)?;
        let short = run.totals.extents != UNKNOWN && (rows.len() as u64) < run.totals.extents;
        // ⚠️ `then`, not `then_some`: `then_some` evaluates its argument **eagerly**, so it
        // computed `UNKNOWN + first_record` — `u64::MAX` plus a record number — and
        // overflowed. It reads like a guard and is not one (§17.22.1).
        (short && run.next != UNKNOWN).then(|| run.next + row.first_record)
    });
    let next = cut.unwrap_or(held.shape.next);
    let levelled = Levelled {
        shape: Shape { extents, entries: held.shape.entries + given, next, ..held.shape },
        head: std::mem::take(&mut held.head),
        rows: std::mem::take(&mut held.rows),
    };
    reply.send(Response { id: 0, status: Status::Ok, args: levelled.args(converted) });
}

/// Several asks outstanding at once, answered when the last of them returns.
///
/// ⚠️ **The DN had parking but not gathering.** A create waits for *one* SN; describing an object's
/// extents waits for **one per SN its streams live on**, and the answer is the combination. So this is
/// a count and a place to put pieces, and the last arrival does the work — no thread waits, which is
/// the same discipline §17.10 uses for the single-ask case.
struct Gather {
    /// Answers still to come. The one that takes it to zero assembles and replies.
    left: usize,
    /// What each SN said, keyed by stream so nothing depends on the order they return in.
    runs: HashMap<StreamId, (ExtentRun, Vec<ExtentRow>)>,
    /// Set when any SN refused, so the whole answer fails rather than silently omitting extents
    /// the caller asked for.
    failed: Option<Response>,
    reply: Option<Reply>,
    /// What the object looked like when the asks went out, so the answer is built from one
    /// consistent view rather than re-read after the streams may have changed.
    head: Vec<Arg>,
    rows: Vec<StreamRow>,
    shape: Shape,
    /// The object the asks were about, so what the SNs said can be written back to the
    /// streams' cached sizes (§17.25.1). Only the name: the object itself must not be held
    /// across a round trip, since it can change while the SNs are answering.
    name: String,
}

struct DN {
    config: DNConfig,
    /// This DN's own identity.
    dn: DnId,
    /// The RangeId managed by this DN (#109, `routing::range_of_dn`).
    mine: RangeId,
    shards: Vec<Mutex<HashMap<Box<str>, Object>>>,
    /// How many requests are waiting for an object to have a stream, for `status`.
    parked: AtomicU64,
    objects: AtomicU64,
    /// Streams handed to objects after their first, which is what a stream sealing
    /// costs.
    streams: AtomicU64,
    next_object: AtomicU64,
    next_stream: AtomicU64,
}

fn shard_of(name: &str) -> usize {
    // A cheap spread over the name: the tail varies most in a generated name,
    // and a hash of the whole string costs more than this decides.
    let bytes = name.as_bytes();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes.iter().rev().take(8).chain(bytes.first()) {
        hash = (hash ^ *byte as u64).wrapping_mul(0x100_0000_01b3);
    }
    hash as usize % SHARDS
}

impl Service for DN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    /// A create is answered once its streams exist, which is another process's
    /// business; a lookup is answered here.
    fn defers(&self, op: Op) -> bool {
        // A lookup can park behind a create, so its answer need not come from the
        // thread that read it either.
        matches!(
            op,
            Op::CreateObject
                | Op::BatchCreate
                | Op::ObjectInfo
                | Op::BatchInfo
                | Op::LocateStream
                | Op::ExtendObject
        )
    }

    /// A single lookup is a hash and a read, which is not worth a queue hop. Taking
    /// names for a create or looking up a batch of names is, since a batch of a
    /// thousand takes a thousand locks.
    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::ObjectInfo
                | Op::LocateStream
                | Op::ExtendObject
                | Op::Ping
                | Op::Status
                | Op::Shutdown
                | Op::Sites
        )
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        match request.op {
            Op::CreateObject => self.create(process, &request, reply),
            Op::BatchCreate => self.create_many(process, &request, reply),
            Op::ObjectInfo => self.info(process, &request, None, reply),
            Op::LocateStream => match request.args.as_slice() {
                [_, Arg::Number(record), ..] => self.info(process, &request, Some(*record), reply),
                _ => reply.send(Response::error(
                    Status::BadRequest,
                    "locate-stream takes a name and a record",
                )),
            },
            Op::ExtendObject => self.extend(process, &request, reply),
            Op::BatchInfo => reply.send(self.info_many(&request)),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("objects {}", self.objects.load(Ordering::Relaxed)),
            format!("extra_streams {}", self.streams.load(Ordering::Relaxed)),
            format!("waiting_for_a_stream {}", self.parked.load(Ordering::Relaxed)),
            format!("self_hosted {}", self.config.self_hosted),
        ]
    }
}

impl DN {
    /// `(name, keyed, framing kind, framing width)`, and one stream to go with it.
    fn create(self: &Arc<Self>, process: &Arc<Process>, request: &Request, reply: Reply) {
        // Only the first few arguments are the object; a create may carry more —
        // media class, an expiry — that nothing here reads yet.
        let head = request.args.get(..Wanted::ARGS).unwrap_or_default();
        let wanted = match Wanted::of_args(head) {
            Ok(wanted) => wanted,
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        let name = wanted.name.clone();
        self.record(process, vec![wanted], reply, move |dn, made| {
            if made == 0 {
                return Response::error(Status::BadRequest, format!("{name} exists already"));
            }
            // The object it just made, in the same shape object-info answers, so an
            // append that had to create one needs no second lookup to go on with.
            match dn.look(&name) {
                Some(args) => Response { id: 0, status: Status::Ok, args },
                None => Response::error(Status::Internal, format!("{name} went missing")),
            }
        });
    }

    /// A run of `(name, keyed)` pairs, which costs one stream request and one
    /// answer however many objects it holds.
    fn create_many(self: &Arc<Self>, process: &Arc<Process>, request: &Request, reply: Reply) {
        let wanted = match Wanted::all_of_args(&request.args) {
            Ok(wanted) => wanted,
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        self.record(process, wanted, reply, |_, made| Response {
            id: 0,
            status: Status::Ok,
            args: vec![Arg::Number(made)],
        });
    }

    /// Take the names first, then name a stream for each one taken, then fill
    /// them in. In that order because a create that loses a race must not leave a
    /// stream behind — nothing deletes streams yet, so a leak here is permanent.
    ///
    /// The filling in happens when the SN answers, on whichever thread that
    /// arrives, so nothing here waits.
    fn record(
        self: &Arc<Self>,
        process: &Arc<Process>,
        wanted: Vec<Wanted>,
        mut reply: Reply,
        say: impl FnOnce(&DN, u64) -> Response + Send + 'static,
    ) {
        let created = TimeSec::now();

        // Whoever inserts the name owns the object; anyone else finds it there.
        let mut mine = Vec::with_capacity(wanted.len());
        for one in &wanted {
            let mut shard = self.shards[shard_of(&one.name)].lock().expect("objects");
            if !shard.contains_key(one.name.as_str()) {
                let id = ObjectId::new(self.next_object.fetch_add(1, Ordering::Relaxed) as u32);
                shard.insert(
                    Box::from(one.name.as_str()),
                    Object {
                        id,
                        framing: one.framing,
                        streams: Vec::new(),
                        extending: false,
                        waiting: Vec::new(),
                        keyed: one.keyed,
                        created,
                    },
                );
                mine.push(one.name.clone());
            }
        }
        if mine.is_empty() {
            return reply.send(say(self, 0));
        }

        // The first name as well as the range: the name matches this line with
        // FE's, the range matches it with the SN's.
        let named_first = mine[0].clone();
        let count = mine.len() as u64;
        let first = self.next_stream.fetch_add(count, Ordering::Relaxed);
        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(e) => {
                let failed = self.give_back(&mine, Status::Internal, e.to_string());
                self.wake(&mine, Some(&failed));
                return reply.send(failed);
            }
        };
        let ask = Request::new(Op::CreateStream)
            .number(self.mine.number())
            .number(first)
            .number(count)
            .number(created.number());

        let dn = Arc::clone(self);
        let sn_number =
            SnId::new(process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0));
        let named = sn.clone();
        let asked = process.ask(&sn, &ask, move |answer| {
            if answer.status != Status::Ok {
                let why = format!("{named} would not make {count} streams");
                let failed = dn.give_back(&mine, answer.status, why);
                dn.wake(&mine, Some(&failed));
                return reply.send(failed);
            }
            for (n, name) in mine.iter().enumerate() {
                let mut shard = dn.shards[shard_of(name)].lock().expect("objects");
                if let Some(object) = shard.get_mut(name.as_str()) {
                    let made = object.created;
                    object.streams.push(Stream::new(
                        StreamId::new((first + n as u64) as u32),
                        sn_number,
                        made,
                    ));
                }
            }
            dn.objects.fetch_add(count, Ordering::Relaxed);
            dn.wake(&mine, None);
            // The stream range is how this request is matched with the SN's line
            // for it: SN does not know names, and should not.
            reply
                .about(format_args!("{named_first} count {count} streams from {first} on {named}"));
            reply.send(say(&dn, count));
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {sn},{e}");
        }
    }

    /// `(name, sealed stream, records)`: that stream is full, so give the object
    /// another one carrying on from where it stopped.
    ///
    /// **One request does the work.** Several appends find the stream full at once,
    /// so the first to arrive asks an SN and the rest park behind it — and because
    /// asking an SN is a round trip, they park rather than hold a worker.
    fn extend(self: &Arc<Self>, process: &Arc<Process>, request: &Request, reply: Reply) {
        let (name, sealed, records) = match request.args.as_slice() {
            [Arg::Text(name), Arg::Number(sealed), Arg::Number(records)] => {
                (name.clone(), *sealed, *records)
            }
            _ => {
                return reply.send(Response::error(
                    Status::BadRequest,
                    "extend-object takes a name, the sealed stream and its record count",
                ));
            }
        };

        // ⚠️ One lock, and everything decided inside it. An earlier version released
        // the lock before parking — so the extension could finish in the gap and wake
        // nobody — and called a method that takes the same lock again, which is a
        // deadlock rather than a slow path.
        let mut carry = Some(reply);
        let next = {
            let mut shard = self.shards[shard_of(&name)].lock().expect("objects");
            match shard.get_mut(name.as_str()) {
                None => Next::Say(Response::error(Status::NotFound, format!("no object {name}"))),
                Some(object) => {
                    // Whoever names the stream that filled is the one that seals it in
                    // the list; the rest are late and have nothing to record.
                    let ours = object.streams.last().is_some_and(|last| last.id.number() == sealed);
                    if let Some(last) = object.streams.last_mut().filter(|_| ours) {
                        last.records = records;
                    }
                    if object.extending {
                        object.waiting.push(carry.take().expect("a reply"));
                        self.parked.fetch_add(1, Ordering::Relaxed);
                        Next::Wait
                    } else if ours {
                        object.extending = true;
                        // The sealed stream's count was just recorded above, so the object's
                        // total is known and is where the next stream starts.
                        Next::Ours(object.records())
                    } else {
                        // Somebody else extended past it already, so whatever is last
                        // now is the answer.
                        match object.args(self.mine, self.dn, None) {
                            Some(args) => Next::Say(Response { id: 0, status: Status::Ok, args }),
                            None => Next::Say(Response::error(
                                Status::Internal,
                                format!("{name} has no stream"),
                            )),
                        }
                    }
                }
            }
        };

        match (next, carry) {
            (Next::Wait, _) => {}
            (Next::Say(answer), Some(reply)) => reply.send(answer),
            (Next::Ours(first_record), Some(reply)) => {
                self.next_stream_for(process, name, first_record, reply)
            }
            (_, None) => {}
        }
    }

    /// Name a stream, have an SN record it, and add it to the object.
    fn next_stream_for(
        self: &Arc<Self>,
        process: &Arc<Process>,
        name: String,
        first_record: u64,
        mut reply: Reply,
    ) {
        let created = TimeSec::now();
        let stream = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(e) => return reply.send(self.gave_up(&name, Status::Internal, e.to_string())),
        };
        let sn_number =
            SnId::new(process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0));
        let ask = Request::new(Op::CreateStream)
            .number(self.mine.number())
            .number(stream)
            .number(1)
            .number(created.number());

        let dn = Arc::clone(self);
        let named = sn.clone();
        reply.about(format_args!("{name} stream {stream} on {named} from record {first_record}"));
        let asked = process.ask(&sn, &ask, move |answer| {
            if answer.status != Status::Ok {
                let why = format!("{named} would not make a stream");
                let failed = dn.gave_up(&name, answer.status, why);
                dn.wake(std::slice::from_ref(&name), Some(&failed));
                return reply.send(failed);
            }
            {
                let mut shard = dn.shards[shard_of(&name)].lock().expect("objects");
                if let Some(object) = shard.get_mut(name.as_str()) {
                    object.streams.push(Stream::new(
                        StreamId::new(stream as u32),
                        sn_number,
                        TimeSec::now(),
                    ));
                    object.extending = false;
                }
            }
            dn.streams.fetch_add(1, Ordering::Relaxed);
            let answer = match dn.describe(&name, None) {
                Some(args) => Response { id: 0, status: Status::Ok, args },
                None => Response::error(Status::Internal, format!("{name} has no stream")),
            };
            dn.wake(std::slice::from_ref(&name), None);
            reply.send(answer);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {sn},{e}");
        }
    }

    /// An extension that could not be made: stop claiming one is in flight, or
    /// everything waiting behind it waits for ever.
    fn gave_up(&self, name: &str, status: Status, why: String) -> Response {
        if let Some(object) = self.shards[shard_of(name)].lock().expect("objects").get_mut(name) {
            object.extending = false;
        }
        Response::error(status, why)
    }

    /// Release names taken for objects that could not be given a stream.
    fn give_back(&self, names: &[String], status: Status, why: String) -> Response {
        for name in names {
            self.shards[shard_of(name)].lock().expect("objects").remove(name.as_str());
        }
        Response::error(status, why)
    }

    fn info(
        self: &Arc<Self>,
        process: &Arc<Process>,
        request: &Request,
        record: Option<u64>,
        reply: Reply,
    ) {
        let (name, rest) = match request.args.as_slice() {
            [Arg::Text(name), rest @ ..] => (name.as_str(), rest),
            _ => {
                return reply.send(Response::error(Status::BadRequest, "object-info takes a name"));
            }
        };
        // `locate-stream` puts a record number where a want field would be, and it asks for
        // nothing beyond the stream holding it, so it is not read here.
        let rest = if record.is_some() { &rest[1..] } else { rest };
        let want = match wanted(rest, ANSWERS) {
            Ok(want) => want,
            Err(e) => return reply.send(Response::error(Status::Unimplemented, e.to_string())),
        };
        let limit = self.config.info_entries;
        // Under one lock: describe, or park. Deciding to wait and then waiting must be
        // one step, or the work can finish in between and wake nobody.
        let mut carry = Some(reply);
        let described = {
            let mut shard = self.shards[shard_of(name)].lock().expect("objects");
            match shard.get_mut(name) {
                None => Some(Err(Response::error(Status::NotFound, format!("no object {name}")))),
                // ⚠️ **The check goes here and nowhere else**, because the DN owns the path
                // table and is the only tier that can see a path point somewhere new. A read
                // answers with blocks and an append with a receipt; neither says which object
                // it was about, so without this a caller is answered about a different one and
                // cannot tell (§17.36).
                Some(object) if !want.expect.holds(object.named(self.mine)) => {
                    Some(Err(Response::error(
                        Status::Replaced,
                        format!("{name} is {} and not {}", object.named(self.mine), want.expect.0),
                    )))
                }
                Some(object) => match describe_at(object, self.mine, self.dn, record, &want, limit)
                {
                    Some(answering) => Some(Ok(answering)),
                    None if object.between_streams() => {
                        object.waiting.push(carry.take().expect("a reply"));
                        self.parked.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    None => {
                        Some(Err(Response::error(Status::NotFound, format!("no object {name}"))))
                    }
                },
            }
        };
        let (Some(described), Some(reply)) = (described, carry) else { return };
        match described {
            Err(refused) => reply.send(refused),
            Ok(Answering::Plain(args)) => reply.send(Response { id: 0, status: Status::Ok, args }),
            // Extents were asked for, so the SNs holding those streams have to be asked.
            Ok(Answering::Levelled(levelled)) if want.wants(Detail::Extents) => {
                self.spread(process, name, levelled, limit, reply)
            }
            Ok(Answering::Levelled(levelled)) => reply.send(Response {
                id: 0,
                status: Status::Ok,
                args: levelled.args(HashMap::new()),
            }),
        }
    }

    /// Ask every SN that holds one of these streams, once each, and answer when the last
    /// one has replied.
    ///
    /// ⚠️ **One ask per SN, not one per stream** (§1.4.0): several of an object's streams may live on
    /// one SN, and a `batch-object-info` over a thousand names could name a thousand streams. Grouping
    /// first is what keeps a fan-out from becoming §1.4.0's unbatched case, where nine tenths of the
    /// cost is messaging.
    fn spread(
        self: &Arc<Self>,
        process: &Arc<Process>,
        name: &str,
        levelled: Levelled,
        limit: u64,
        reply: Reply,
    ) {
        // Group by SN, keeping each stream's record to start from.
        // ⚠️ The whole name per stream, not the id: the SN keys streams by
        // `(RangeId, StreamId, CreateSec)` (#109), and the DN is what holds all three.
        // The DN's own range, which it already holds — see `DN::mine`.
        let range = self.mine;
        let mut by_sn: HashMap<SnId, Vec<(StreamName, u64)>> = HashMap::new();
        for row in &levelled.rows {
            let name = StreamName { range, stream: row.stream, created: row.created };
            by_sn.entry(row.sn).or_default().push((name, 0));
        }
        if by_sn.is_empty() {
            return reply.send(Response {
                id: 0,
                status: Status::Ok,
                args: levelled.args(HashMap::new()),
            });
        }

        // ⚠️ **The limit is one number over the whole answer** (§9.1.3.7), so what goes to the SNs is
        // what the object and its stream rows did *not* spend. Passing the whole limit again would let
        // an answer carry twice what was asked for — which it did, until a live run showed six entries
        // against a cap of three.
        let room = limit.saturating_sub(levelled.shape.entries);
        let gather = Arc::new(Mutex::new(Gather {
            left: by_sn.len(),
            runs: HashMap::new(),
            failed: None,
            reply: Some(reply),
            head: levelled.head,
            rows: levelled.rows,
            shape: levelled.shape,
            name: name.to_string(),
        }));

        let dn = Arc::clone(self);
        for (sn, at) in by_sn {
            let identity = format!("SN{sn}");
            let ask = {
                let mut request = Request::new(Op::ListExtents);
                request.args = Listing { limit: room, at }.args();
                request
            };
            let waiting = Arc::clone(&gather);
            let named = identity.clone();
            let mine = Arc::clone(&dn);
            if let Err(e) =
                process.ask(&identity, &ask, move |answer| arrived(&mine, &waiting, answer))
            {
                logw!("could not reach an SN", "sn {named},{e}");
                arrived(
                    self,
                    &gather,
                    Response::error(Status::Internal, format!("could not reach {named}")),
                );
            }
        }
    }

    /// What the SNs said a stream has come to, kept against the next question about it
    /// (§17.25.1). Takes the name's shard lock, so nothing may hold it.
    fn remember(&self, name: &str, runs: &HashMap<StreamId, (ExtentRun, Vec<ExtentRow>)>) {
        if runs.is_empty() {
            return;
        }
        let mut shard = self.shards[shard_of(name)].lock().expect("objects");
        // Gone while the SNs answered: a deleted object is not a lost update worth caring
        // about, since the streams it named are going away too.
        let Some(object) = shard.get_mut(name) else { return };
        for held in &mut object.streams {
            if let Some((run, _)) = runs.get(&held.id) {
                held.learned(run.totals);
            }
        }
    }

    /// Answer everything parked on these names, now that their streams exist. A name
    /// whose create failed is answered too, with whatever the failure was, so nothing
    /// waits for an answer that is never coming.
    fn wake(&self, names: &[String], failed: Option<&Response>) {
        for name in names {
            let (waiters, args) = {
                let mut shard = self.shards[shard_of(name)].lock().expect("objects");
                match shard.get_mut(name.as_str()) {
                    Some(object) => {
                        (std::mem::take(&mut object.waiting), object.args(self.mine, self.dn, None))
                    }
                    None => (Vec::new(), None),
                }
            };
            self.parked.fetch_sub(waiters.len() as u64, Ordering::Relaxed);
            for reply in waiters {
                reply.send(match (failed, &args) {
                    (None, Some(args)) => {
                        Response { id: 0, status: Status::Ok, args: args.clone() }
                    }
                    (Some(failed), _) => failed.clone(),
                    (None, None) => {
                        Response::error(Status::Internal, format!("{name} was made and then lost"))
                    }
                });
            }
        }
    }

    /// One answer for many names, in the order asked, up to the entry limit.
    ///
    /// **A name that is not there still occupies a row**, with a zero id, so that the rows a caller
    /// gets line up with the names it asked about — but the answer no longer relies on that alone:
    /// it leads with a `Batch` header saying **how many names it covered** (§9.1.3.7).
    ///
    /// ⚠️ **It stops at a name boundary, always.** §9.1.3.7 permits stopping mid-object only when one
    /// object alone exceeds the limit, and at the levels a batch answers today an object is one entry —
    /// so that case cannot arise and every answer is a **prefix** of the request. Which is what makes
    /// the continuation an index: the caller slices its own list and asks again.
    fn info_many(&self, request: &Request) -> Response {
        // The names, then one want field for all of them — a batch asks the same question about
        // every name, so carrying it per name would repeat it a thousand times (§9.1.3.11).
        let names = request.args.iter().position(|arg| !matches!(arg, Arg::Text(_)));
        let (names, rest) = request.args.split_at(names.unwrap_or(request.args.len()));
        let want = match wanted(rest, BATCH_ANSWERS) {
            Ok(want) => want,
            Err(e) => return Response::error(Status::Unimplemented, e.to_string()),
        };
        let limit = match want.limit {
            0 => self.config.info_entries,
            asked => asked.min(self.config.info_entries),
        };

        let mut args = Vec::with_capacity(names.len() * Described::ARGS);
        let mut spent = 0;
        let mut given = 0;
        // Whether the last name given was described completely. Only the last one can be partial:
        // an earlier one stopping short would have spent the whole limit.
        let mut whole = true;
        for asked in names {
            let name = match asked {
                Arg::Text(name) => name.as_str(),
                _ => {
                    return Response::error(Status::BadRequest, "batch-object-info takes names");
                }
            };
            // What this name will cost, before committing to it: an object, plus its streams if
            // they were asked for. ⚠️ Measured first so the answer never overruns the limit — the
            // §17.22.2 bug was spending a cap and then spending it again.
            let piece = self.piece(name, &want, limit.saturating_sub(spent));
            let cost = piece.as_ref().map_or(1, |piece| piece.entries);
            if given > 0 && spent + cost > limit {
                break;
            }
            match piece {
                Some(mut piece) => {
                    spent += piece.entries;
                    whole = piece.whole;
                    args.append(&mut piece.args);
                }
                None => {
                    spent += 1;
                    whole = true;
                    // ⚠️ A shape for a name that is not there too, whenever the rest of the answer
                    // has shapes. Without it a levelled batch holding one miss cannot be read at
                    // all: every other name begins with a shape, so a reader counting arguments
                    // walks off by five and attributes rows to the wrong name (§17.24.1).
                    if want.detail != 0 {
                        args.extend(Shape::plain().args());
                    }
                    args.extend(Described::missing());
                }
            }
            given += 1;
        }

        // Where to resume. ⚠️ A name given only in part leaves the answer incomplete even when
        // every name was reached, so the continuation is *that* name rather than the one after it —
        // §17.22.2's bug was a truncated answer claiming nothing followed (§17.23.4).
        let next = match (given as usize == names.len(), whole) {
            (true, true) => UNKNOWN,
            (_, true) => given,
            (_, false) => given - 1,
        };
        let covered = Batch { asked: names.len() as u64, given, entries: spent, next };
        let mut out = covered.args();
        out.append(&mut args);
        Response { id: 0, status: Status::Ok, args: out }
    }

    /// One name's worth of a batch answer, and what it costs in entries.
    ///
    /// ⚠️ **Takes the name's shard lock** — see `look`.
    fn piece(&self, name: &str, want: &Want, room: u64) -> Option<Piece> {
        let shard = self.shards[shard_of(name)].lock().expect("objects");
        let object = shard.get(name)?;
        if want.detail == 0 {
            return object.args(self.mine, self.dn, None).map(|args| Piece {
                entries: 1,
                whole: true,
                args,
            });
        }
        // Room is what the limit has left, and an object costs at least its own entry, so a name
        // reached with no room to spare still gets its row and no streams.
        let levelled = detailed(object, self.mine, self.dn, want, room.max(1))?;
        let (entries, whole) = (levelled.shape.entries, !levelled.shape.more());
        Some(Piece { entries, whole, args: levelled.args(HashMap::new()) })
    }

    /// What an object is: id, stream, the SN holding it, the second it was made,
    /// its flavour, and its framing as a kind and a width.
    ///
    /// ⚠️ **Takes the name's shard lock**, so a caller already holding one deadlocks
    /// against itself. Anything working under a shard lock builds its own answer
    /// from the object it already has.
    fn look(&self, name: &str) -> Option<Vec<Arg>> {
        self.describe(name, None)
    }

    /// ⚠️ **Takes the name's shard lock** — see `look`.
    fn describe(&self, name: &str, record: Option<u64>) -> Option<Vec<Arg>> {
        self.shards[shard_of(name)]
            .lock()
            .expect("objects")
            .get(name)?
            .args(self.mine, self.dn, record)
    }
}

impl DN {
    fn new(config: DNConfig, dn: DnId, mine: RangeId) -> DN {
        DN {
            config,
            dn,
            mine,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            parked: AtomicU64::new(0),
            objects: AtomicU64::new(0),
            streams: AtomicU64::new(0),
            // ⚠️ From zero, because the sentinel is at the top of the range now: slot 0 of an
            // arena is a real object, and starting at one would waste it (§17.37.2).
            next_object: AtomicU64::new(0),
            next_stream: AtomicU64::new(0),
        }
    }
}

/// One name's worth of a batch answer: what it cost, whether it is all of that
/// name, and the arguments themselves.
struct Piece {
    entries: u64,
    whole: bool,
    args: Vec<Arg>,
}

fn main() {
    let result = server::main(|process| {
        let dn = DnId::new(process.entry.number());
        Ok(DN::new(DNConfig::load(&process.config.service_config)?, dn, routing::range_of_dn(dn)))
    });
    if let Err(e) = result {
        eprintln!("dn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    /// ⚠️ **The property a path cannot supply and a name must** (§17.35.2). A user may replace an
    /// object many times a second, so two incarnations at one path share a creation second — and
    /// they must still be told apart. The id is what does it, minted from a counter rather than
    /// read from a clock.
    #[test]
    fn two_objects_at_one_path_in_one_second_have_different_names() {
        let dn = Arc::new(DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0)));
        let made = TimeSec::new(1_000_000);
        let name_of = |dn: &DN| {
            let shard = dn.shards[shard_of("/p")].lock().unwrap();
            let object = shard.get("/p").expect("an object");
            ObjectName { range: dn.mine, object: object.id, created: object.created }
        };
        let put = |dn: &DN| {
            dn.shards[shard_of("/p")].lock().unwrap().insert(
                Box::from("/p"),
                Object {
                    id: ObjectId::new(dn.next_object.fetch_add(1, Ordering::Relaxed) as u32),
                    framing: Framing::Lf,
                    streams: Vec::new(),
                    extending: false,
                    waiting: Vec::new(),
                    keyed: false,
                    created: made,
                },
            );
        };

        put(&dn);
        let first = name_of(&dn);
        // The path is repointed — deleted and recreated — inside the same second.
        put(&dn);
        let second = name_of(&dn);

        assert_eq!(first.created, second.created, "the same second, which is the hard case");
        assert_ne!(first.object, second.object, "and still different names");
        assert_ne!(first, second, "so a reference to the first cannot resolve to the second");
    }

    /// A stream in a test object. `records` is `None` for the one still being appended to,
    /// which is what the sentinel means on the real thing.
    ///
    /// ⚠️ **No first record to give it**: where a stream starts in an object is the object's
    /// to work out by summing what came before (§17.33.1), so a test builds the list and the
    /// positions follow.
    fn held(id: u32, sn: u32, records: Option<u64>) -> Stream {
        Stream {
            records: records.unwrap_or(UNKNOWN),
            ..Stream::new(StreamId::new(id), SnId::new(sn), TimeSec::new(1_000_000))
        }
    }

    use std::mem::size_of;

    /// What one stream costs an object's list here. As with the SN (§4.7 again),
    /// the design budgets a **4 byte** node-local reference and this inlines the
    /// record, so an object's stream list is ten times its budget.
    #[test]
    fn a_stream_costs_an_object_more_than_a_reference() {
        let each = size_of::<Stream>();
        assert_eq!(each, 48, "if this moved, §4.7 and issue #9 want to hear about it");
        // ⚠️ **40, then 56, now 48.** The sizes went in (§17.25.1) — a creation second, an
        // extent count, a logical length and a compressed one, for the space of two, because
        // `records` stopped being an `Option<u64>` in the same change. Then the stream's
        // position came out again (§17.33.1), because a stream shared between objects has no
        // single one. Net: four fields gained and eight bytes, for a stream that now says
        // what it holds rather than where somebody put it.
        // ⚠️ Four `u64` and three `u32` is 44, and the alignment rounds it to 48 — so the
        // id narrowing to `u32` (§17.33.2) bought nothing here, and would buy 8 bytes the
        // moment a fourth `u32` joins it. Worth knowing before anyone claims it as a saving.
        assert_eq!(4 * size_of::<u64>() + 3 * size_of::<u32>(), 44, "and 4 bytes of padding");
        for bits in [24u32, 27, 32] {
            println!(
                "2^{bits} streams in one object = {:.0} GB",
                each as f64 * 2f64.powi(bits as i32) / 1e9
            );
        }
    }

    use super::*;
    use common::info::{Shape, StreamRow, UNKNOWN};
    use common::range::Start;
    use common::testing;

    /// An object as `record` leaves it before the SN has answered: the name is
    /// taken and the stream is not named.
    fn half_made(dn: &DN, name: &str) {
        dn.shards[shard_of(name)].lock().unwrap().insert(
            Box::from(name),
            Object {
                id: ObjectId::new(1),
                framing: Framing::Lf,
                streams: Vec::new(),
                extending: false,
                waiting: Vec::new(),
                keyed: false,
                created: TimeSec::new(1_000_000),
            },
        );
    }

    /// The stream is what everything below the DN needs, so an object without one
    /// is not something to describe — the name is taken, and that is a different
    /// answer from "no object".
    #[test]
    fn an_object_being_made_does_not_look_like_a_finished_one() {
        let dn = DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0));
        half_made(&dn, "/half");
        assert!(dn.look("/half").is_none(), "nothing to say about it yet");
        let waits = |name: &str| {
            dn.shards[shard_of(name)]
                .lock()
                .unwrap()
                .get(name)
                .is_some_and(|object| object.between_streams())
        };
        assert!(waits("/half"), "but the name is taken, so a lookup waits");
        assert!(!waits("/nothing"), "unlike a name nobody has");

        // Once the stream is named it describes normally.
        dn.shards[shard_of("/half")]
            .lock()
            .unwrap()
            .get_mut("/half")
            .unwrap()
            .streams
            .push(held(7, 0, None));
        let args = dn.look("/half").expect("an object");
        // ⚠️ Read through the shape rather than by index: the stream is a whole name now
        // (#109), so its id is no longer argument one, and an index would have gone on
        // asserting about whatever moved into that slot.
        let (described, _) = Described::of_args(&args).expect("a row");
        assert_eq!(described.expect("an object").stream.stream, StreamId::new(7), "its stream");
        assert!(!waits("/half"), "and it is no longer half made");
    }

    /// A name is taken before its stream is asked for, so two creates of one name
    /// ask for one stream. That is the rule that stops a create leaving a stream
    /// behind when it loses the race.
    #[test]
    fn one_name_is_taken_once() {
        let dn = DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0));
        let mine = |dn: &DN| {
            let mut taken = Vec::new();
            let name = "/same".to_string();
            let mut shard = dn.shards[shard_of(&name)].lock().unwrap();
            if !shard.contains_key(name.as_str()) {
                shard.insert(
                    Box::from(name.as_str()),
                    Object {
                        id: ObjectId::new(dn.next_object.fetch_add(1, Ordering::Relaxed) as u32),
                        framing: Framing::Lf,
                        streams: Vec::new(),
                        extending: false,
                        waiting: Vec::new(),
                        keyed: false,
                        created: TimeSec::new(1_000_000),
                    },
                );
                taken.push(name);
            }
            taken
        };
        assert_eq!(mine(&dn).len(), 1, "the first one takes it");
        assert!(mine(&dn).is_empty(), "the second finds it there");
    }

    /// An object grows streams, and the record ranges are what make its numbering
    /// continuous across them. This is the arithmetic the DN answers reads with.
    #[test]
    fn an_objects_streams_carry_on_from_one_another() {
        let mut object = Object {
            id: ObjectId::new(1),
            framing: Framing::Lf,
            streams: Vec::new(),
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: TimeSec::new(1_000_000),
        };
        // Two sealed streams and one still open.
        object.streams.push(held(1, 0, Some(10)));
        object.streams.push(held(2, 0, Some(5)));
        object.streams.push(held(3, 0, None));

        assert_eq!(object.holding(ORoff::new(0)).unwrap().1.id, StreamId::new(1));
        assert_eq!(object.holding(ORoff::new(9)).unwrap().1.id, StreamId::new(1));
        assert_eq!(object.holding(ORoff::new(10)).unwrap().1.id, StreamId::new(2));
        assert_eq!(object.holding(ORoff::new(14)).unwrap().1.id, StreamId::new(2));
        assert_eq!(object.holding(ORoff::new(15)).unwrap().1.id, StreamId::new(3));
        // Past everything sealed goes to the open stream, whose reach is not known
        // here — the SN and EN say whether the record is really there.
        assert_eq!(object.holding(ORoff::new(1000)).unwrap().1.id, StreamId::new(3));
        // And an append always goes to the last.
        assert_eq!(object.last().unwrap().1.id, StreamId::new(3));
        assert!(!object.between_streams());
    }

    /// An object with three streams, two sealed and one still growing.
    fn grown() -> Object {
        let mut object = Object {
            id: ObjectId::new(1),
            framing: Framing::Lf,
            streams: Vec::new(),
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: TimeSec::new(1_000_000),
        };
        object.streams.push(held(1, 0, Some(100)));
        object.streams.push(held(2, 1, Some(50)));
        object.streams.push(held(3, 2, None));
        object
    }

    /// What a caller sees, with no SN asked — so every stream row says `given` zero.
    fn flat(levelled: Levelled) -> Vec<Arg> {
        levelled.args(HashMap::new())
    }

    fn read_back(args: &[Arg]) -> (Shape, Vec<StreamRow>) {
        let (shape, rest) = Shape::of_args(args).expect("a shape");
        let (_, rest) = Described::of_args(rest).expect("an object");
        let (rows, rest) = StreamRow::some_of_args(rest, shape.given).expect("streams");
        assert!(rest.is_empty(), "and nothing left over");
        (shape, rows)
    }

    /// Wanting nothing asks for no levels at all, so the caller gets the row that
    /// predates the field — which is what keeps every existing caller and §17.5g's
    /// baseline.
    #[test]
    fn wanting_nothing_answers_the_bare_row() {
        assert!(
            detailed(&grown(), RangeId::new(0), DnId::new(0), &Want::plain(), 100).is_none(),
            "nothing levelled to build"
        );
        let object = grown();
        match describe_at(&object, RangeId::new(0), DnId::new(0), None, &Want::plain(), 100)
            .expect("an answer")
        {
            Answering::Plain(args) => {
                assert_eq!(args.len(), Described::ARGS);
                assert!(Described::of_args(&args).expect("a row").0.is_some());
            }
            Answering::Levelled(_) => panic!("no detail was asked for"),
        }
    }

    /// The counts are exact even when the detail is not, which is what lets a caller
    /// compute the requests it has not made yet (§9.1.3.2).
    #[test]
    fn the_counts_are_exact_while_the_detail_is_capped() {
        let want = Want::plain().with(Detail::Streams).with(Detail::Counts);
        // Two entries: one for the object, so room for one stream.
        let args = flat(
            detailed(&grown(), RangeId::new(0), DnId::new(0), &Want { limit: 2, ..want }, 100)
                .expect("an answer"),
        );
        let (shape, rows) = read_back(&args);
        assert_eq!(shape.streams, 3, "how many there are");
        assert_eq!((shape.given, rows.len()), (1, 1), "how many are described");
        assert_eq!(shape.entries, 2, "the object and one stream");
        assert_eq!(shape.next, 100, "and where to carry on");
        assert!(shape.more());
    }

    /// ⚠️ The continuation is a record number, so resuming from it describes the next
    /// stream and not the same one again — §9.1.3's whole scheme in one assertion.
    #[test]
    fn resuming_from_the_continuation_makes_progress() {
        let want = Want::plain().with(Detail::Streams).with(Detail::Counts);
        let object = grown();
        let mut at = 0;
        let mut seen = Vec::new();
        for _ in 0..8 {
            let args = flat(
                detailed(
                    &object,
                    RangeId::new(0),
                    DnId::new(0),
                    &Want { limit: 2, from: Start::Record(at), ..want },
                    100,
                )
                .expect("an answer"),
            );
            let (shape, rows) = read_back(&args);
            seen.extend(rows.iter().map(|row| row.stream));
            if !shape.more() {
                break;
            }
            assert!(shape.next > at, "a continuation that does not advance is a livelock");
            at = shape.next;
        }
        assert_eq!(
            seen,
            vec![StreamId::new(1), StreamId::new(2), StreamId::new(3)],
            "every stream, once, in order"
        );
    }

    /// The configured cap and the request's cap are both honoured, whichever is
    /// smaller — a request may ask for less and may not ask for more.
    #[test]
    fn the_smaller_of_the_two_caps_wins() {
        let want = Want::plain().with(Detail::Streams);
        let given = |asked, configured| {
            let args = flat(
                detailed(
                    &grown(),
                    RangeId::new(0),
                    DnId::new(0),
                    &Want { limit: asked, ..want },
                    configured,
                )
                .expect("an answer"),
            );
            read_back(&args).0.given
        };
        assert_eq!(given(0, 3), 2, "no request cap, so the configured one: object plus two");
        assert_eq!(given(2, 100), 1, "the request asks for less");
        assert_eq!(given(100, 2), 1, "and may not ask for more");
    }

    /// ⚠️ A stream still being appended to reports UNKNOWN rather than zero, because
    /// how far it reaches is not the DN's to say — and a sealed stream holding
    /// nothing would be a real zero (§8.1.6.3, #90).
    #[test]
    fn the_growing_stream_says_unknown_and_not_zero() {
        let want = Want::plain().with(Detail::Streams);
        let args = flat(
            detailed(&grown(), RangeId::new(0), DnId::new(0), &Want { limit: 100, ..want }, 100)
                .expect("an answer"),
        );
        let (_, rows) = read_back(&args);
        assert_eq!(rows[0].records, 100);
        assert_eq!(rows[2].records, UNKNOWN, "the one being appended to");
        assert_ne!(rows[2].records, 0, "which is not a count of none");
    }

    /// Counts without streams is a legitimate question: how big is this object,
    /// without the rows. ⚠️ And it still says where the rows would start — see §17.24.2
    /// for why saying `nothing more` there was wrong.
    #[test]
    fn counts_alone_bring_no_rows_but_say_where_rows_start() {
        let want = Want::plain().with(Detail::Counts);
        let args =
            flat(detailed(&grown(), RangeId::new(0), DnId::new(0), &want, 100).expect("an answer"));
        let (shape, rows) = read_back(&args);
        assert_eq!((shape.streams, shape.given, shape.entries), (3, 0, 1));
        assert!(rows.is_empty());
        assert!(shape.more(), "three streams it did not describe is not nothing more");
        assert_eq!(shape.next, 0, "and a listing would start at the first of them");
    }

    /// A DN holding `count` objects named `/n0`, `/n1`, and so on.
    fn with_objects(count: u64) -> Arc<DN> {
        let dn = Arc::new(DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0)));
        for n in 0..count {
            let name = format!("/n{n}");
            dn.shards[shard_of(&name)].lock().unwrap().insert(
                Box::from(name.as_str()),
                Object {
                    id: ObjectId::new(n as u32 + 1),
                    framing: Framing::Lf,
                    streams: vec![held(n as u32 + 1, 0, None)],
                    extending: false,
                    waiting: Vec::new(),
                    keyed: false,
                    created: TimeSec::new(1_000_000),
                },
            );
        }
        dn
    }

    fn batch_of(dn: &Arc<DN>, names: &[&str], want: Option<Want>) -> Response {
        let mut request = Request::new(Op::BatchInfo);
        for name in names {
            request = request.text(*name);
        }
        if let Some(want) = want {
            request.args.extend(want.args());
        }
        dn.info_many(&request)
    }

    /// The limit stops a batch at a **name boundary**, and the answer says how many names it
    /// covered rather than leaving a caller to count rows against its question.
    #[test]
    fn a_batch_stops_at_a_name_and_says_where() {
        // The test config allows 8 entries, so 8 of ten names at one entry each.
        let dn = with_objects(10);
        let names: Vec<String> = (0..10).map(|n| format!("/n{n}")).collect();
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
        let answer = batch_of(&dn, &borrowed, None);
        assert_eq!(answer.status, Status::Ok);

        let (covered, rest) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!(covered.asked, 10);
        assert_eq!(covered.given, 8, "the configured limit, spent one per object");
        assert_eq!(covered.entries, 8);
        assert_eq!(covered.next, 8, "so the caller asks about its own names from index 8");
        assert!(covered.more());
        let rows = Described::all_of_args(rest).expect("rows");
        assert_eq!(rows.len(), 8, "and the rows match what it said");
        assert!(rows.iter().all(Option::is_some));
    }

    /// A batch that fits says so, and its continuation is UNKNOWN rather than one past the end.
    #[test]
    fn a_batch_that_fits_says_there_is_no_more() {
        let dn = with_objects(3);
        let answer = batch_of(&dn, &["/n0", "/n1", "/n2"], None);
        let (covered, rest) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!((covered.asked, covered.given, covered.next), (3, 3, UNKNOWN));
        assert!(!covered.more());
        assert_eq!(Described::all_of_args(rest).expect("rows").len(), 3);
    }

    /// A name nobody took still occupies a row, so rows line up with names — and it costs an
    /// entry, since the answer carried it.
    #[test]
    fn a_missing_name_costs_a_row_and_an_entry() {
        let dn = with_objects(2);
        let answer = batch_of(&dn, &["/n0", "/nothing", "/n1"], None);
        let (covered, rest) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!((covered.given, covered.entries), (3, 3));
        let rows = Described::all_of_args(rest).expect("rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1], None, "the gap, in its place");
        assert_eq!(rows.iter().flatten().count(), 2);
    }

    /// Asking for streams makes each name cost more than one entry, so fewer names fit — which
    /// is the limit being one number over the whole answer rather than a count of objects.
    #[test]
    fn detail_makes_a_name_cost_more_so_fewer_fit() {
        let dn = with_objects(10);
        let names: Vec<String> = (0..10).map(|n| format!("/n{n}")).collect();
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
        let want = Want::plain().with(Detail::Streams).with(Detail::Counts);
        let answer = batch_of(&dn, &borrowed, Some(want));
        let (covered, _) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!(covered.given, 4, "an object plus its one stream, so two entries each");
        assert_eq!(covered.entries, 8, "and the limit is what bounds it");
        assert!(covered.more());
    }

    /// ⚠️ At least one name is always described, even when it alone exceeds the limit — or a
    /// caller with a small cap makes no progress and retries for ever (§9.1.3.6's rule, on the
    /// other level).
    #[test]
    fn one_name_always_comes_back() {
        let dn = with_objects(3);
        let want = Want { limit: 1, ..Want::plain().with(Detail::Streams) };
        let answer = batch_of(&dn, &["/n0", "/n1", "/n2"], Some(want));
        let (covered, _) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!(covered.given, 1, "one name, whatever it cost");
        // ⚠️ Not 1. That name got its object row and none of its streams, so the caller has to come
        // back to *it* — resuming at name 1 would drop the rest of name 0 without saying so.
        assert_eq!(covered.next, 0, "and the caller carries on from the name still owed to it");
    }

    /// ⚠️ A levelled batch holding a name that is not there is still readable: every name begins
    /// with a shape, present or not, so nothing is attributed to the wrong name (§17.24.1).
    #[test]
    fn a_missing_name_in_a_levelled_batch_still_has_a_shape() {
        let dn = with_objects(2);
        let want = Want { limit: 100, ..Want::plain().with(Detail::Streams) };
        let answer = batch_of(&dn, &["/n0", "/gone", "/n1"], Some(want));
        let (covered, rows) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!(covered.given, 3, "a miss is answered, not skipped");
        let pieces = Batch::pieces(rows, covered.given).expect("one slice per name");
        assert_eq!(pieces.len(), 3);
        // The middle one is the miss: a shape saying nothing, then a row saying nothing.
        let (shape, rest) = Shape::of_args(pieces[1]).expect("a shape");
        assert_eq!((shape.streams, shape.given, shape.entries), (0, 0, 1));
        assert!(Described::of_args(rest).expect("a row").0.is_none(), "the name is not there");
        // And the ones either side are real, which is what proves nothing slid by five.
        for present in [pieces[0], pieces[2]] {
            let (_, rest) = Shape::of_args(present).expect("a shape");
            assert!(Described::of_args(rest).expect("a row").0.is_some());
        }
    }

    /// A batch that reached every name and described each one whole says nothing follows, and that
    /// is the only case that may say so (§17.23.4).
    #[test]
    fn a_finished_batch_is_the_only_one_that_says_nothing_follows() {
        let dn = with_objects(2);
        let want = Want { limit: 100, ..Want::plain().with(Detail::Streams) };
        let answer = batch_of(&dn, &["/n0", "/n1"], Some(want));
        let (covered, _) = Batch::of_args(&answer.args).expect("a batch header");
        assert_eq!((covered.asked, covered.given), (2, 2));
        assert_eq!(covered.next, UNKNOWN, "every name, every stream");
    }

    /// A batch cannot fan out for extents yet, and says which level it cannot give rather than
    /// answering without it.
    #[test]
    fn a_batch_refuses_extents_by_name() {
        let dn = with_objects(1);
        let want = Want::plain().with(Detail::Extents);
        let answer = batch_of(&dn, &["/n0"], Some(want));
        assert_eq!(answer.status, Status::Unimplemented);
        let said = answer.message().unwrap_or_default();
        assert!(said.contains("extents"), "{said}");
        assert!(!said.contains("streams"), "streams it can do: {said}");
    }

    /// A create carries a name, a flavour and a framing, and anything after those is
    /// something this tier does not read yet rather than a malformed request.
    #[test]
    fn a_create_reads_its_first_four_arguments() {
        let (kind, width) = Framing::CrLf.wire();
        let args =
            vec![Arg::Text("/x".into()), Arg::Flag(true), Arg::Number(kind), Arg::Number(width)];
        let wanted = Wanted::of_args(&args).expect("an object");
        assert_eq!((wanted.name.as_str(), wanted.keyed), ("/x", true));
        assert_eq!(wanted.framing, Framing::CrLf);

        // A media class after the framing is not this tier's business, and the head
        // of the request still parses.
        let mut more = args.clone();
        more.push(Arg::Text("ssd".into()));
        assert!(Wanted::of_args(&more[..Wanted::ARGS]).is_ok());
        // But a framing that means nothing is a bad request.
        let bad = vec![Arg::Text("/x".into()), Arg::Flag(false), Arg::Number(9), Arg::Number(0)];
        assert!(Wanted::of_args(&bad).is_err());
    }
}
