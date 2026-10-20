//! DN, the Directory Node: objects by name, and the streams they are made of.
//!
//! An object is indexed by name here, and it grows a stream at a time on the SNs
//! that record them — no inlining of small objects and no coalescing of several
//! objects into one stream, both of which come later and both of which would hide
//! what a create costs. DN owns the `StreamId`; the SN it picks records
//! `(RangeId, StreamId)`.
//!
//! ⚠️ **The hash table by name is a stand-in for a B-tree** with a read and a
//! write lock. Stream slots use StreamId as their stable index and are reclaimed
//! by the Raft exclusive-lock pass after their last reference is deleted.
//!
//! A create waits on an SN, and waits without holding a worker: the names are
//! taken, the streams are asked for, and the client's `Reply` travels with that
//! request to be answered when the SN says the streams exist.

use common::config::{DNConfig, ServiceKind};
use common::deletion::{
    Delete, Extras, Flags, SharedExtras, Target, bad, forward, read_soft_deleted,
    with_read_soft_deleted,
};
use common::framing::Framing;
use common::ids::{DnId, ExtentId, ObjectId, RangeId, SnId, StreamId};
use common::info::{
    AppendFlags, Batch, Described, Expecting, ExtentRow, ExtentRun, Listing, ObjectName, Shape,
    StreamName, StreamRow, Totals, UNKNOWN, Wanted,
};
use common::logw;
use common::offset::{OCoff, OEoff, OLoff, ORoff, SCoff, SLoff, SRoff};
use common::range::{Detail, Want};
use common::routing;
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::{Arg, Op, Req, Resp, Status, StructWriter, structure};
use raft::codec::Wire;
use raft::runtime::{Host, Settings as RaftSettings};
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
#[cfg(test)]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const SERVES: &[Op] = &[
    Op::Raft,
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
    Op::SoftDeleteObject,
    Op::UndeleteObject,
    Op::SetExpiry,
    Op::CloneObject,
    Op::ConcatObjects,
    Op::SealObject,
    Op::Count,
    Op::TimeRange,
];

/// A single open stream can grow to just under 2^62 records. Do not create one
/// above this point, so the object's record offset remains representable even
/// though the stream itself does not need to know the object's total.
const OPEN_STREAM_RECORD_LIMIT: u64 = 3 * (1u64 << 62);
const CREATE_REJECT_KEEP: u64 = 1 << 16;

type ExtentReply = (ExtentRun, Vec<ExtentRow>);

/// One original stream-row request. The index is a reply slot, not a stream identity.
#[derive(Debug, Clone)]
struct ExtentAsk {
    row: usize,
    name: StreamName,
    from: SRoff,
}

const BATCH_ANSWERS: u64 = Detail::Streams.bit() | Detail::Counts.bit();

/// Which `Detail` bits this build actually answers.
///
/// **Streams and counts locally; extents and instances by asking the SNs** (§17.22). Capabilities
/// need the key of #82 and the stream flags of §4.1.1 do not exist — so a caller asking for those is
/// told which by name rather than handed an answer that quietly omits what it asked for.
/// The answer for a lookup: the plain row when a record was named or no detail was
/// asked for, and the levelled answer otherwise.
///
/// ⚠️ **`locate-stream` always gets the plain row.** It asks *which stream holds this
/// record*, an FE's question on the read path, and it must keep the shape `Located::of`
/// reads — so detail is an `object-info` feature and not a lookup feature.
fn describe_at(
    object: &Object,
    table: &[Stream],
    range: RangeId,
    dn: DnId,
    record: Option<ORoff>,
    want: &Want,
    limit: u64,
) -> Option<Answering> {
    match record {
        Some(_) => object.args(table, range, dn, record).map(Answering::Plain),
        None => match detailed(object, table, range, dn, want, limit) {
            Some(levelled) => Some(Answering::Levelled(levelled)),
            // No detail asked for, so the row that predates the field, byte for byte.
            None => object.args(table, range, dn, None).map(Answering::Plain),
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
    table: &[Stream],
    range: RangeId,
    dn: DnId,
    want: &Want,
    limit: u64,
) -> Option<Levelled> {
    let args = object.args(table, range, dn, None)?;
    if want.detail == 0 {
        return None;
    }

    let total = StreamId::of_number(object.streams.len() as u64).expect("object stream count fits");
    let from = want.from.object_record().unwrap_or(ORoff::ZERO);
    // Streams are in record order, so the first to describe is the last one starting
    // at or before `from` — the same rule a read uses to pick a stream.
    let first = object.streams_from(table).filter(|(at, _)| *at <= from).count().saturating_sub(1);

    // One entry for the object, then one per stream, against whichever cap is smaller.
    let cap = match want.limit {
        0 => limit,
        asked => asked.min(limit),
    };
    let room = cap.saturating_sub(1);
    let mut rows = Vec::new();
    let counts_only = want.wants(Detail::Counts) && !want.wants(Detail::Streams);
    if want.wants(Detail::Streams) {
        let streams = object.streams_from(table).skip(first);
        let streams = streams.take(room as usize);
        for (at, held) in streams {
            rows.push(StreamRow {
                stream: held.id,
                sn: held.sn,
                first_record: at,
                // ⚠️ UNKNOWN, not zero: how far the stream being appended to reaches is
                // not the DN's to say (§6.3), and a sealed stream may hold nothing.
                records: held.records,
                created: held.created,
                // What the DN last learned. Exact once the stream is sealed, a point in
                // time before that, and UNKNOWN until any SN has been asked (§17.25.1).
                totals: visible_totals(object.framing, held.totals(), held.records),
                given: ExtentId::FIRST,
            });
        }
    }

    // Where to carry on: the first record of the stream after the last one described,
    // and nothing when the last stream described is the object's last.
    let after = first + rows.len();
    let next = object.streams_from(table).nth(after).map_or(ORoff::NONE, |(at, _)| at);
    let shape = Shape {
        streams: if want.wants(Detail::Counts) { total } else { StreamId::NONE },
        given: StreamId::of_number(rows.len() as u64).expect("stream row count fits"),
        extents: if want.wants(Detail::Counts) {
            object.cached_extents(table)
        } else {
            OEoff::NONE
        },
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
            ORoff::NONE
        },
        records: if want.wants(Detail::Counts) {
            object.cached_records(table)
        } else {
            ORoff::NONE
        },
        logical: if want.wants(Detail::Counts) { object.logical(table) } else { OLoff::NONE },
        compressed: if want.wants(Detail::Counts) { object.compressed(table) } else { OCoff::NONE },
    };

    // The shape first, then the object, then its streams — so a reader knows how many
    // rows follow before it starts reading them.
    Some(Levelled {
        shape,
        head: args,
        rows,
        hide_rows: counts_only,
        instances: want.wants(Detail::Instances),
    })
}

/// What a lookup came to before any SN was asked.
struct Levelled {
    shape: Shape,
    head: Vec<Arg>,
    rows: Vec<StreamRow>,
    hide_rows: bool,
    instances: bool,
}

impl Levelled {
    /// The DN-only answer, before any extent rows have been requested.
    fn args(self) -> Vec<Arg> {
        let rows = if self.hide_rows { 0 } else { self.rows.len() };
        let mut out = Vec::with_capacity(1 + self.head.len() + rows);
        out.push(self.shape.arg());
        out.extend(self.head);
        if self.hide_rows {
            return out;
        }
        for mut row in self.rows {
            row.given = ExtentId::FIRST;
            out.push(row.arg());
        }
        out
    }
}

struct Object {
    deletion: Flags,
    /// Permanently skip the soft-delete interval when this object is deleted.
    purge_only: bool,
    /// ⚠️ **`u32`, minted per range**, so an object's name is `(RangeId, ObjectId, CreateSec)` —
    /// the same triple as a stream's and an extent's (§17.35.2). **A path is not part of it**: a
    /// path points at a name and may be repointed as often as a user likes, which is exactly what
    /// a creation second cannot pin and does not have to.
    id: ObjectId,
    /// Where one record ends and the next begins, declared here and never
    /// changed: everything that reads or writes this object's records needs it.
    framing: Framing,
    /// The streams this object is made of, in order. The first stream definition
    /// is committed with the object; SN creation follows outside the DN Raft action.
    ///
    /// A stream fills, so an object grows more of them; the last is the one appends
    /// go to and the record ranges say which holds what.
    streams: Vec<StreamId>,
    /// An extension is being asked of an SN, so anything else that wants one waits
    /// rather than asking for a second.
    extending: bool,
    /// Requests waiting for this object to have a stream they can use: the first
    /// stream while it is being made, or the next one while it is being asked for.
    ///
    /// ⚠️ **They live here, under the object-table lock, and not in a table of their
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
    Ours(ORoff),
    Wait,
    Say(Resp),
}

/// One of an object's streams, and what it has come to.
///
/// ⚠️ **No first record here**, for the reason `sn::Extent` has none (§17.29.1) one tier up:
/// §11's lumping puts many objects in one stream, so a stream's position *in an object* is the
/// object's property and not the stream's. It is recovered by summing the `records` of the
/// streams before it, and only the last stream of an object can be unsealed — so every term of
/// that sum is known (§17.33.1).
///
/// Stored once per range, rather than once per object reference. The reference count
/// is atomic; ordinary metadata is protected by the range's stream table lock.
#[repr(C)]
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
    /// This word also carries the free-list link while the slot is not live.
    id: StreamId,
    deletion: Flags,
    /// Records in it once sealed, and **`SRoff::NONE` while it is the one being appended to**.
    ///
    /// ⚠️ **This one does not go stale, unlike the sizes below.** §4.5's exact record count
    /// is what addressing runs on, so a stale record count is a *wrong read*, while a stale
    /// byte count is a slightly wrong listing (§17.25.1).
    records: SRoff,
    /// The second this stream was handed out. The DN's own, so always exact — which is what
    /// a directory listing wants, and ⚠️ **which turns out to be part of the stream's name**
    /// rather than decoration: it is the third component of `(RangeId, StreamId, CreateSec)`,
    /// and it is what makes a 32-bit id safe.
    created: TimeSec,
    /// Bytes the writer wrote and bytes stored, summed over the stream's extents.
    ///
    /// ⚠️ **Exact for a sealed stream, as-of for a growing one, `SLoff::NONE` / `SCoff::NONE` until an SN has
    /// been asked** (Asserted, Bob, 2026-08-20). A sealed stream cannot change, so one
    /// refresh is right for ever; a growing one is only ever a point in time, and that is
    /// good enough for the listing these exist for.
    logical: SLoff,
    compressed: SCoff,
    sn: SnId,
    /// Extents in it, or `ExtentId::NONE` if unknown (§8.1.6.2).
    extents: ExtentId,
}

/// Metadata copied only while planning a concat, never an owning reference.
#[derive(Clone, Copy)]
struct StreamSnapshot {
    id: StreamId,
    sn: SnId,
    created: TimeSec,
    extents: ExtentId,
    logical: SLoff,
    compressed: SCoff,
}

impl Stream {
    fn blank() -> Self {
        Self {
            id: StreamId::NONE,
            deletion: Flags::vacant(),
            records: SRoff::NONE,
            created: TimeSec::EPOCH,
            logical: SLoff::NONE,
            compressed: SCoff::NONE,
            sn: SnId::NONE,
            extents: ExtentId::NONE,
        }
    }

    fn snapshot(&self) -> StreamSnapshot {
        StreamSnapshot {
            id: self.id,
            sn: self.sn,
            created: self.created,
            extents: self.extents,
            logical: self.logical,
            compressed: self.compressed,
        }
    }
    /// A stream just handed out: nothing has come to anything yet.
    fn new(id: StreamId, sn: SnId, created: TimeSec) -> Stream {
        Stream {
            id,
            deletion: Flags::default(),
            records: SRoff::NONE,
            created,
            logical: SLoff::NONE,
            compressed: SCoff::NONE,
            sn,
            extents: ExtentId::NONE,
        }
    }

    /// What this stream has come to, in the shape the wire carries.
    fn totals(&self) -> Totals {
        Totals { extents: self.extents, logical: self.logical, compressed: self.compressed }
    }

    /// What an SN said the stream has come to. ⚠️ **Only ever moves forward**: an answer
    /// that crossed a newer one in flight must not walk the numbers back, and a sealed
    /// stream's totals are final whatever arrives afterwards.
    fn learned(&mut self, totals: Totals) {
        if totals.extents != ExtentId::NONE
            && let seen = totals.extents
            && (self.extents.is_none() || seen.get() > self.extents.get())
        {
            self.extents = seen;
        }
        if totals.logical != SLoff::NONE
            && let seen = totals.logical
            && (self.logical.is_none() || seen > self.logical)
        {
            self.logical = seen;
        }
        if totals.compressed != SCoff::NONE
            && let seen = totals.compressed
            && (self.compressed.is_none() || seen > self.compressed)
        {
            self.compressed = seen;
        }
    }
}

// SAFETY: init gives fresh pool storage a fully initialized vacant Stream.
unsafe impl pool::PoolItem for Stream {
    unsafe fn init(slot: *mut Self) {
        // SAFETY: the pool provides aligned, fresh storage for one Stream.
        unsafe { slot.write(Stream::blank()) };
    }
}

// SAFETY: `id` is the pool link while a slot is free and the StreamId while live;
// allocation resets it before the slot is handed back to DN.
unsafe impl pool::LockedItem for Stream {
    const LINK_NONE: u32 = StreamId::NONE.get();

    unsafe fn on_alloc(slot: *mut Self) {
        // SAFETY: the pool provides an initialized slot with exclusive ownership.
        // The caller replaces the complete Stream before publishing this slot.
        unsafe {
            (*slot).id = StreamId::NONE;
            (*slot).deletion = Flags::default();
        }
    }
}

type StreamPool = pool::LockedPool<
    Stream,
    { std::mem::align_of::<Stream>() },
    { std::mem::offset_of!(Stream, id) },
>;

/// Per-range stream slots. A StreamId is the slot index on every Raft member;
/// only a vacant or reclaimed slot can be reserved for a create action.
struct StreamTable {
    pool: StreamPool,
    /// High-water index visible to StreamId lookups. The pool itself starts
    /// with one free slot even while this table is empty.
    len: usize,
}

impl StreamTable {
    fn new() -> pool::Result<Self> {
        Ok(Self {
            // StreamId::NONE is the sentinel, so all lower u32 indexes are valid.
            pool: StreamPool::new(StreamId::NONE.get(), 0)?,
            len: 0,
        })
    }

    fn can_reserve(&self, id: StreamId) -> bool {
        !id.is_none() && self.pool.can_reserve_index(id.get())
    }

    fn reserve(&mut self, id: StreamId) -> pool::Result<()> {
        self.pool.reserve_index(id.get())?;
        self.len = self.len.max(id.get() as usize + 1);
        Ok(())
    }

    fn install(&mut self, stream: Stream) -> pool::Result<()> {
        let id = stream.id;
        self.reserve(id)?;
        self[id.get() as usize] = stream;
        Ok(())
    }

    fn retire(&mut self, id: StreamId) {
        let index = id.get() as usize;
        assert!(index < self.len, "retired DN stream index was never issued");
        assert!(self[index].deletion.retired(), "retired DN stream is still live");
        // SAFETY: the final reference to this stream has been reclaimed, and
        // callers hold the stream-table write lock. The global lock prevents
        // the exclusive reclamation pass from racing this insertion.
        unsafe { self.pool.retire_index(id.get()) };
    }

    fn reclaim(&mut self) {
        self.pool.reclaim();
    }

    fn reset(&mut self) {
        *self = Self::new().expect("rebuilding DN stream pool");
    }
}

impl Deref for StreamTable {
    type Target = [Stream];

    fn deref(&self) -> &Self::Target {
        // SAFETY: the pool's stable mapping has initialized every slot in this
        // published prefix, and len never exceeds its issued prefix.
        unsafe { std::slice::from_raw_parts(self.pool.base_ptr().as_ptr(), self.len) }
    }
}

impl DerefMut for StreamTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: callers have exclusive access to StreamTable and len covers
        // only initialized slots in the stable pool mapping.
        unsafe { std::slice::from_raw_parts_mut(self.pool.base_ptr().as_ptr(), self.len) }
    }
}

impl Drop for StreamTable {
    fn drop(&mut self) {
        for index in 0..self.pool.issued_blocks() {
            // PoolItem::init initialized every issued slot, including free ones.
            unsafe { std::ptr::drop_in_place(self.pool.base_ptr().as_ptr().add(index as usize)) };
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
    fn streams_from<'a>(
        &'a self,
        table: &'a [Stream],
    ) -> impl Iterator<Item = (ORoff, &'a Stream)> {
        self.streams.iter().map(|id| &table[id.get() as usize]).scan(ORoff::ZERO, |at, stream| {
            let first = *at;
            if stream.records.is_some() {
                *at = at
                    .plus(ORoff::from_stream(stream.records))
                    .expect("object record position fits");
            }
            Some((first, stream))
        })
    }

    /// Nothing here can be appended to or read yet: it has no stream, or the next
    /// one is being asked for. Waiting is better than a transient missing-stream answer.
    fn between_streams(&self) -> bool {
        self.streams.is_empty() || self.extending
    }

    /// What this object is, at one of its streams: the last for an append, or the one
    /// holding a record for a read.
    fn args(
        &self,
        table: &[Stream],
        range: RangeId,
        dn: DnId,
        record: Option<ORoff>,
    ) -> Option<Vec<Arg>> {
        // ⚠️ The position comes back *with* the stream, because the stream does not carry
        // it (§17.33.1) — the same shape as `sn::Placed`, and for the same reason.
        let (first, held) = match record {
            None => self.last(table)?,
            Some(record) => self.holding(table, record)?,
        };
        Some(
            Described {
                object: self.named(range),
                dn,
                stream: StreamName { range, stream: held.id, created: held.created },
                sn: held.sn,
                keyed: self.keyed,
                framing: self.framing,
                first_record: first,
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
    /// ⚠️ **`ORoff::NONE` while the last stream is open**, because how far that one reaches is
    /// not the DN's to say (§6.3).
    fn records(&self, table: &[Stream]) -> ORoff {
        match self.streams_from(table).last() {
            Some((_, last)) if last.records.is_none() => ORoff::NONE,
            Some((at, last)) => {
                at.plus(ORoff::from_stream(last.records)).expect("object record position fits")
            }
            None => ORoff::ZERO,
        }
    }

    fn cached_records(&self, table: &[Stream]) -> ORoff {
        self.streams.iter().map(|id| &table[id.get() as usize]).fold(
            ORoff::ZERO,
            |total, stream| {
                if stream.records.is_none() || total == ORoff::MAX {
                    total
                } else {
                    total.plus(ORoff::from_stream(stream.records)).unwrap_or(ORoff::MAX)
                }
            },
        )
    }

    fn cached_extents(&self, table: &[Stream]) -> OEoff {
        self.streams.iter().map(|id| &table[id.get() as usize]).fold(
            OEoff::ZERO,
            |total, stream| {
                if stream.extents.is_none() {
                    total
                } else {
                    total.plus(OEoff::from_stream(stream.extents)).unwrap_or(OEoff::MAX)
                }
            },
        )
    }

    fn logical(&self, table: &[Stream]) -> OLoff {
        // Sum in the visible space, before any canonical total can overflow.
        // An unrepresentable byte count does not restrict the object's records.
        let mut total = OLoff::ZERO;
        for stream in self.streams.iter().map(|id| &table[id.get() as usize]) {
            if stream.logical.is_none() {
                continue;
            }
            let logical = visible_totals(self.framing, stream.totals(), stream.records).logical;
            if logical.is_none() {
                return OLoff::NONE;
            }
            let Ok(next) = total.plus(OLoff::from_stream(logical)) else {
                return OLoff::MAX;
            };
            total = next;
        }
        total
    }

    fn compressed(&self, table: &[Stream]) -> OCoff {
        self.streams.iter().map(|id| &table[id.get() as usize]).fold(
            OCoff::ZERO,
            |total, stream| {
                if stream.compressed.is_none() || total == OCoff::MAX {
                    total
                } else {
                    total.plus(OCoff::from_stream(stream.compressed)).unwrap_or(OCoff::MAX)
                }
            },
        )
    }

    /// The stream appends go to, which is the last one, and where it starts.
    fn last<'a>(&'a self, table: &'a [Stream]) -> Option<(ORoff, &'a Stream)> {
        self.streams_from(table).last()
    }

    /// The stream holding an object-local record number: the sealed one whose range
    /// covers it, or the last, whose reach is not known here.
    fn holding<'a>(&'a self, table: &'a [Stream], record: ORoff) -> Option<(ORoff, &'a Stream)> {
        for (at, held) in self.streams_from(table) {
            if held.records.is_none()
                || record < at.plus(ORoff::from_stream(held.records)).expect("record fits")
            {
                return Some((at, held));
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
fn arrived(dn: &Arc<DN>, gather: &Arc<Mutex<Gather>>, asked: &[ExtentAsk], answer: Resp) {
    let answer = pending_stream_answer(answer, asked);
    let ready = {
        let mut held = gather.lock().expect("gather");
        held.left -= 1;
        if answer.status != Status::Ok {
            held.failed.get_or_insert(answer);
        } else {
            if let Err(e) = receive_extents(&mut held.runs, asked, answer.args()) {
                held.failed.get_or_insert(Resp::error(Status::Internal, e.to_string()));
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
    let runs = std::mem::take(&mut held.runs);
    // ⚠️ **Write back what the SNs said before answering.** This is the only path by which a
    // stream's cached sizes ever stop being UNKNOWN (§17.25.1), so skipping it would leave
    // every listing blank while the numbers sat in a response on their way out the door.
    // A sealed stream's totals are final, so once written they are right for ever; a growing
    // one's are a point in time, which is what `Stream::learned` refusing to go backwards is
    // for — two answers can cross in flight.
    dn.remember(&held.name, &runs);
    let levelled = Levelled {
        shape: held.shape.clone(),
        head: std::mem::take(&mut held.head),
        rows: std::mem::take(&mut held.rows),
        hide_rows: held.hide_rows,
        instances: held.instances,
    };
    reply.send(Resp::from_args(Status::Ok, merge_object_info(levelled, &runs, held.limit)).id(0));
}

/// Group by destination only. Keep every occurrence, its start, and its input order.
fn extent_asks(range: RangeId, rows: &[StreamRow], from: ORoff) -> HashMap<SnId, Vec<ExtentAsk>> {
    let mut groups: HashMap<SnId, Vec<ExtentAsk>> = HashMap::new();
    for (at, row) in rows.iter().enumerate() {
        groups.entry(row.sn).or_default().push(ExtentAsk {
            row: at,
            name: StreamName { range, stream: row.stream, created: row.created },
            from: stream_start(from, row.first_record),
        });
    }
    groups
}

fn ask_extents(asked: &[ExtentAsk], limit: u64, instances: bool) -> Req {
    let mut payload = StructWriter::new();
    Listing { limit, instances, at: asked.iter().map(|ask| (ask.name, ask.from)).collect() }
        .write_payload(&mut payload);
    Req::new(Op::ListExtents).structure(payload)
}

/// SN replies in request order; restore each reply to its original row slot.
fn receive_extents(
    slots: &mut [Option<ExtentReply>],
    asked: &[ExtentAsk],
    args: &[Arg],
) -> common::Result<()> {
    let replies = ExtentRun::all_of_args(args)?;
    if replies.len() != asked.len() {
        return Err(common::Error::new(
            "SN returned a different number of stream runs than requested",
        ));
    }
    for (ask, (run, _)) in asked.iter().zip(&replies) {
        if run.stream != ask.name.stream || ask.row >= slots.len() {
            return Err(common::Error::new("SN extent response does not match its request"));
        }
    }
    for (ask, reply) in asked.iter().zip(replies) {
        slots[ask.row] = Some(reply);
    }
    Ok(())
}

/// A detailed read consults both DN and SN. If DN names a stream that SN has
/// not created, report the cross-service mismatch as pending; DN-only reads do
/// not pass through this path and continue to return the declared stream.
fn pending_stream_answer(answer: Resp, asked: &[ExtentAsk]) -> Resp {
    if answer.status != Status::NotFound {
        return answer;
    }
    let Some(message) = answer.message() else {
        return answer;
    };
    let Some(missing) = message.strip_prefix("no ") else {
        return answer;
    };
    let Some(ask) = asked.iter().find(|ask| ask.name.to_string() == missing) else {
        return answer;
    };
    Resp::error(
        Status::NotFound,
        format!("pending: DN declares {}, but SN has not created it", ask.name),
    )
}

/// Convert an object position to the stream's space; later streams start at zero.
fn stream_start(from: ORoff, base: ORoff) -> SRoff {
    SRoff::new(from.since(base).number())
}

/// Translate payload totals to the object framing; caches remain canonical.
fn visible_totals(framing: Framing, mut totals: Totals, records: SRoff) -> Totals {
    if framing.overhead() != 0 {
        totals.logical = if totals.logical.is_none() || records.is_none() {
            SLoff::NONE
        } else {
            framing
                .visible_length(totals.logical.number(), records.number())
                .map(|n| SLoff::new(n.min(SLoff::MAX.number())))
                .unwrap_or(SLoff::MAX)
        };
    }
    totals
}

/// Assemble each reference independently: sharing a stream does not consume its summary.
fn merge_object_info(mut held: Levelled, runs: &[Option<ExtentReply>], limit: u64) -> Vec<Arg> {
    let framing =
        Described::of_args(&held.head).ok().and_then(|(row, _)| row).map(|row| row.framing);
    let mut body = Vec::new();
    let mut extents = OEoff::ZERO;
    let mut given = 0;
    assert_eq!(held.rows.len(), runs.len());
    for (row, reply) in held.rows.iter_mut().zip(runs) {
        if let Some((run, rows)) = reply {
            row.totals =
                visible_totals(framing.unwrap_or(Framing::Length), run.totals, row.records);

            row.given = ExtentId::of_number(rows.len() as u64).expect("extent row count fits");
            if run.next.is_some() {
                held.shape.next = held.shape.next.min(
                    row.first_record
                        .plus(ORoff::from_stream(run.next))
                        .expect("object position fits"),
                );
            }
            body.push(row.arg());
            for extent in rows {
                let mut extent = extent.clone();
                if let Some(framing) = framing.filter(|f| f.overhead() != 0) {
                    extent.logical = if extent.logical.is_none() || extent.records.is_none() {
                        common::offset::ELoff::NONE
                    } else {
                        framing
                            .visible_length(extent.logical.number(), extent.records.number())
                            .map(|n| {
                                common::offset::ELoff::new(
                                    n.min(common::offset::ELoff::MAX.number()) as u32,
                                )
                            })
                            .unwrap_or(common::offset::ELoff::MAX)
                    };
                }
                body.extend(extent.args());
            }
            given += rows.len() as u64;
        } else {
            row.given = ExtentId::FIRST;
            body.push(row.arg());
        }
        // Counts come from the SN when available, never from the bounded row vector.
        extents = if extents.is_none() || row.totals.extents.is_none() {
            OEoff::NONE
        } else {
            extents.plus(OEoff::from_stream(row.totals.extents)).expect("object count fits")
        };
    }
    if !held.rows.is_empty() {
        held.shape.extents = extents;
    }
    held.shape.entries = 1 + held.rows.len() as u64 + given;
    let mut args = held.shape.args();
    args.extend(held.head);
    args.extend(body);
    common::info::limit_object_info(&args, limit).expect("assembled object summary")
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
    /// One reply slot per original stream row, independent of SN arrival order.
    runs: Vec<Option<ExtentReply>>,
    /// Set when any SN refused, so the whole answer fails rather than silently omitting extents
    /// the caller asked for.
    failed: Option<Resp>,
    reply: Option<Reply>,
    /// What the object looked like when the asks went out, so the answer is built from one
    /// consistent view rather than re-read after the streams may have changed.
    head: Vec<Arg>,
    rows: Vec<StreamRow>,
    shape: Shape,
    hide_rows: bool,
    instances: bool,
    /// The object the asks were about, so what the SNs said can be written back to the
    /// streams' cached sizes (§17.25.1). Only the name: the object itself must not be held
    /// across a round trip, since it can change while the SNs are answering.
    name: String,
    limit: u64,
}

struct BatchCountGather {
    left: usize,
    reply: Option<Reply>,
    base: Vec<Arg>,
    runs: Vec<Option<ExtentReply>>,
    failed: Option<Resp>,
}

struct DNState {
    stream_table: Arc<RwLock<StreamTable>>,
}

impl raft::execution::Application for DNState {
    fn reset(&mut self) {
        self.stream_table.write().expect("streams").reset();
    }

    fn reclaim(&mut self) {
        self.stream_table.write().expect("streams").reclaim();
    }
}

struct DN {
    common: raft::runtime::Runtime<DNState>,
    config: DNConfig,
    gc_stop: AtomicBool,
    gc_thread: Mutex<Option<JoinHandle<()>>>,
    deleting: Mutex<()>,
    retired_objects: Mutex<std::collections::HashSet<String>>,
    create_rejects: Mutex<CreateRejects>,
    /// This DN's own identity.
    dn: DnId,
    /// The RangeId managed by this DN (#109, `routing::range_of_dn`).
    mine: RangeId,
    object_table: RwLock<HashMap<Box<str>, Object>>,
    /// How many requests are waiting for an object to have a stream, for `status`.
    parked: AtomicU64,
    objects: AtomicU64,
    /// Streams handed to objects after their first, which is what a stream sealing
    /// costs.
    streams: AtomicU64,
    next_object: AtomicU64,
    next_stream: AtomicU64,
    /// The current DN serves one range (`mine`). StreamId indexes this range's table.
    /// Lock the object table before this table; never hold either across a network call.
    stream_table: Arc<RwLock<StreamTable>>,
    // Sparse, range-scoped metadata. Lock owner before extras, never the reverse.
    object_extras: Mutex<Extras<(ObjectId, TimeSec)>>,
    stream_extras: Mutex<SharedExtras<(StreamId, TimeSec)>>,
    #[cfg(test)]
    _test_dir: Option<tempfile::TempDir>,
}

struct CreateReject {
    status: Status,
    message: String,
}

#[derive(Default)]
struct CreateRejects {
    by_id: HashMap<ObjectId, (u64, CreateReject)>,
    order: VecDeque<(u64, ObjectId)>,
}

impl CreateRejects {
    fn record(&mut self, index: u64, id: ObjectId, reject: Option<CreateReject>) {
        let oldest = index.saturating_sub(CREATE_REJECT_KEEP);
        while self.order.front().is_some_and(|(at, _)| *at < oldest) {
            let (at, id) = self.order.pop_front().unwrap();
            if self.by_id.get(&id).is_some_and(|(stored, _)| *stored == at) {
                self.by_id.remove(&id);
            }
        }
        if let Some(reject) = reject {
            self.by_id.insert(id, (index, reject));
            self.order.push_back((index, id));
        } else {
            self.by_id.remove(&id);
        }
    }

    fn take(&mut self, id: ObjectId) -> Option<Resp> {
        self.by_id.remove(&id).map(|(_, reject)| Resp::error(reject.status, reject.message))
    }
}

impl Deref for DN {
    type Target = raft::runtime::Runtime<DNState>;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl Service for DN {
    fn start(self: &Arc<Self>) -> common::Result<()> {
        self.start_gc()?;
        if let Err(error) = self.start_raft() {
            let _ = self.stop_gc();
            return Err(error);
        }
        Ok(())
    }

    fn stop(&self, _process: &Process) -> common::Result<()> {
        self.stop_gc()?;
        self.stop_raft()
    }

    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    /// A create is answered once its streams exist, which is another process's
    /// business; a lookup is answered here.
    fn defers(&self, op: Op) -> bool {
        if op == Op::Raft {
            return true;
        }
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
                | Op::ConcatObjects
                | Op::SealObject
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

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        if request.op() == Op::Raft {
            return reply.send(self.raft_rpc(process, &request));
        }
        if matches!(request.op(), Op::CreateObject | Op::BatchCreate) {
            if let Some(answer) = self.forward(&request) {
                return reply.send(answer);
            }
            return reply.send(self.create_request(process, &request));
        }
        let _state = self.global_lock.read();
        match request.op() {
            Op::SoftDeleteObject | Op::DeleteObject => {
                reply.send(self.delete_object(process, &request))
            }
            Op::ObjectInfo => self.info(process, &request, None, reply),
            Op::LocateStream => {
                let (mut payload, rest) =
                    match structure(&request.args(), Op::LocateStream, "a locate-stream request") {
                        Ok(value) => value,
                        Err(e) => {
                            return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                        }
                    };
                let path = match payload.text() {
                    Ok(path) => path,
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                };
                let record = match payload.number_u64().and_then(ORoff::of_number) {
                    Ok(record) => record,
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                };
                let expect = match Expecting::read(&mut payload) {
                    Ok(expect) => expect,
                    Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
                };
                let include = match read_soft_deleted(&mut payload, rest) {
                    Ok(value) => value,
                    Err(error) => return reply.send(bad(error)),
                };
                let mut payload = StructWriter::new();
                payload.text(&path);
                payload.flag(expect.asked());
                if expect.asked() {
                    Want { expect, ..Want::plain() }.write(&mut payload);
                }
                let request =
                    with_read_soft_deleted(Req::new(Op::ObjectInfo).structure(payload), include);
                self.info(process, &request, Some(record), reply)
            }
            Op::ExtendObject => self.extend(process, &request, reply),
            Op::ConcatObjects => self.concat_objects(process, &request, reply),
            Op::SealObject => self.seal_object(&request, reply),
            Op::BatchInfo => self.begin_info_many(process, &request, reply),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("objects {}", self.objects.load(Ordering::Relaxed)),
            format!("extra_streams {}", self.streams.load(Ordering::Relaxed)),
            format!("waiting_for_a_stream {}", self.parked.load(Ordering::Relaxed)),
            format!("self_hosted {}", self.config.self_hosted),
            self.raft_status(),
        ]
    }
}

impl DN {
    fn start_gc(self: &Arc<Self>) -> common::Result<()> {
        let mut slot = self.gc_thread.lock().expect("GC thread");
        if slot.is_some() {
            return Err(common::Error::new("DN GC thread is already running"));
        }
        self.gc_stop.store(false, Ordering::Release);
        let dn = Arc::downgrade(self);
        let worker = thread::Builder::new()
            .name("dn-gc".into())
            .spawn(move || {
                loop {
                    let Some(dn) = dn.upgrade() else { break };
                    if dn.gc_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let guard = dn.global_lock.read();
                    thread::sleep(Duration::from_millis(10));
                    drop(guard);
                    if dn.gc_stop.load(Ordering::Acquire) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            })
            .map_err(|error| common::Error::new(format!("starting DN GC thread: {error}")))?;
        *slot = Some(worker);
        Ok(())
    }

    fn stop_gc(&self) -> common::Result<()> {
        self.gc_stop.store(true, Ordering::Release);
        let worker = self.gc_thread.lock().expect("GC thread").take();
        if let Some(worker) = worker {
            worker.thread().unpark();
            worker.join().map_err(|_| common::Error::new("DN GC thread panicked"))?;
        }
        Ok(())
    }

    fn delete_object(&self, process: &Process, request: &Req) -> Resp {
        match self.deleting_object(process, request) {
            Ok(()) => Resp::ok(),
            Err(error) => error,
        }
    }

    fn deleting_object(&self, process: &Process, request: &Req) -> Result<(), Resp> {
        let deletion =
            Delete::of_with_limit(request, process.cluster.max_soft_delete_retention_secs)
                .map_err(bad)?;
        let Target::Object(path) = &deletion.target else {
            return Err(bad("expected an object"));
        };
        let _serial = self.deleting.lock().expect("deletion");
        let now = TimeSec::now();
        let maximum = process.cluster.max_soft_delete_retention_secs;
        let (ids, deletion) = {
            let mut objects = self.object_table.write().expect("objects");
            let Some(object) = objects.get_mut(path.as_str()) else {
                return if self.retired_objects.lock().unwrap().contains(path) {
                    Ok(())
                } else {
                    Err(Resp::error(Status::NotFound, "no object"))
                };
            };
            if object.extending || object.streams.is_empty() {
                return Err(bad("object extension is in progress; retry deletion"));
            }
            // `purge_only` is an object's persistent deletion policy. On its first
            // delete, perform the soft and hard transitions together and ignore the
            // caller's ordinary retention window.
            let deletion = deletion_policy(deletion.clone(), object.purge_only);
            let key = (object.id, object.created);
            let mut extras = self.object_extras.lock().expect("object extras");
            let first_hard = deletion.hard && !object.deletion.deleted();
            let first = extras
                .transition(
                    key,
                    &mut object.deletion,
                    now,
                    deletion.retention,
                    deletion.hard,
                    maximum,
                )
                .map_err(bad)?;
            let interval = extras.remaining(&key, now).map_err(bad)?;
            drop(extras);
            let mut table = self.stream_table.write().expect("streams");
            let mut stream_extras = self.stream_extras.lock().expect("stream extras");
            for id in &object.streams {
                let stream = &mut table[id.get() as usize];
                let key = (stream.id, stream.created);
                if first {
                    stream_extras
                        .release_soft(
                            key,
                            &mut stream.deletion,
                            now,
                            deletion.retention.unwrap_or(interval),
                            maximum,
                        )
                        .map_err(bad)?;
                }
                if let Some(interval) = deletion.retention {
                    stream_extras
                        .observe_retention(key, &mut stream.deletion, interval, maximum)
                        .map_err(bad)?;
                }
                if first_hard {
                    stream_extras.release_hard(&key, &mut stream.deletion).map_err(bad)?;
                }
            }
            (object.streams.clone(), deletion)
        };
        // Each occurrence released a reference above; fan out once per distinct child.
        let mut seen = std::collections::HashSet::new();
        for id in ids {
            if !seen.insert(id) {
                continue;
            }
            let child = {
                let mut table = self.stream_table.write().expect("streams");
                let stream = &mut table[id.get() as usize];
                if !stream.deletion.soft_deleted() || stream.deletion.retired() {
                    continue;
                }
                let extras = self.stream_extras.lock().expect("stream extras");
                let key = (stream.id, stream.created);
                let hard = deletion.hard && stream.deletion.deleted();
                if hard {
                    extras.delete(&key, &mut stream.deletion, TimeSec::now()).map_err(bad)?;
                }
                (
                    stream.sn,
                    StreamName { range: self.mine, stream: stream.id, created: stream.created },
                    extras.remaining(&key, TimeSec::now()).map_err(bad)?,
                    hard,
                )
            };
            // Replaying soft deletion repairs an interrupted fanout before hard deletion.
            forward(
                process,
                &format!("SN{}", child.0),
                Delete { target: Target::Stream(child.1), retention: Some(child.2), hard: false },
            )?;
            if child.3 {
                forward(
                    process,
                    &format!("SN{}", child.0),
                    Delete { target: Target::Stream(child.1), retention: None, hard: true },
                )?;
                let mut table = self.stream_table.write().unwrap();
                let reclaimed = {
                    let stream = &mut table[id.get() as usize];
                    let key = (stream.id, stream.created);
                    self.stream_extras.lock().unwrap().reclaim(
                        &key,
                        &mut stream.deletion,
                        TimeSec::now(),
                    )
                };
                assert!(reclaimed);
                table.retire(id);
            }
        }
        if deletion.hard {
            let mut objects = self.object_table.write().unwrap();
            let object = objects.get_mut(path.as_str()).expect("deleting object");
            let key = (object.id, object.created);
            assert!(self.object_extras.lock().unwrap().reclaim(
                &key,
                &mut object.deletion,
                TimeSec::now()
            ));
            objects.remove(path.as_str());
            self.retired_objects.lock().unwrap().insert(path.clone());
            self.objects.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn seal_object(&self, request: &Req, reply: Reply) {
        let (mut payload, rest) =
            match structure(&request.args(), Op::SealObject, "a seal-object request") {
                Ok(value) => value,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
        let name = match payload.text() {
            Ok(name) => name,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        let stream = match payload.stream_id() {
            Ok(stream) => stream,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        let records = match payload.sroff() {
            Ok(records) => records,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        if !rest.is_empty() || payload.done().is_err() {
            return reply
                .send(Resp::error(Status::BadRequest, "seal-object has trailing arguments"));
        }
        let mut objects = self.object_table.write().expect("objects");
        let Some(object) = objects.get_mut(name.as_str()) else {
            return reply.send(Resp::error(Status::NotFound, format!("no object {name}")));
        };
        if !object.deletion.readable(false) {
            return reply.send(bad("object is deleted"));
        }
        let mut table = self.stream_table.write().expect("streams");
        let Some(last) = object.streams.last().map(|id| &mut table[id.get() as usize]) else {
            return reply.send(Resp::error(Status::BadRequest, "object has no stream"));
        };
        if last.id != stream {
            return reply
                .send(Resp::error(Status::BadRequest, "seal-object named a non-tail stream"));
        }
        if last.records.is_none() {
            last.records = records;
        } else if last.records != records {
            return reply
                .send(Resp::error(Status::BadRequest, "seal-object record count disagrees"));
        }
        drop(table);
        match object.args(&self.stream_table.read().expect("streams"), self.mine, self.dn, None) {
            Some(args) => reply.send(Resp::from_args(Status::Ok, args).id(0)),
            None => reply.send(Resp::error(Status::Internal, "sealed object has no description")),
        }
    }
    /// Make a new object from sealed source objects. Source stream rows are copied
    /// by reference; the new object gets one fresh, empty stream at the end so it
    /// is immediately appendable but is not sealed.
    fn concat_objects(self: &Arc<Self>, process: &Arc<Process>, request: &Req, reply: Reply) {
        let _serial = self.deleting.lock().expect("deletion");
        let retry = request.clone();
        let (mut payload, rest) =
            match structure(&request.args(), Op::ConcatObjects, "a concat-objects request") {
                Ok(value) => value,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
        let destination = match payload.text() {
            Ok(name) => name,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        let count = match payload.number_u64() {
            Ok(count) => count,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        if count < 2 {
            return reply.send(Resp::error(
                Status::BadRequest,
                "concat requires at least two source objects",
            ));
        }
        if count > self.config.max_concat_sources {
            return reply.send(Resp::error(
                Status::BadRequest,
                format!(
                    "concat has {count} sources, above the configured limit of {}",
                    self.config.max_concat_sources
                ),
            ));
        }
        let count = match usize::try_from(count) {
            Ok(count) => count,
            Err(_) => {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "concat source count does not fit this machine",
                ));
            }
        };
        let mut sources = match payload.checked_vec(count) {
            Ok(sources) => sources,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        for _ in 0..count {
            match payload.text() {
                Ok(name) => sources.push(name),
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            }
        }
        if let Err(e) = payload.done() {
            return reply.send(Resp::error(Status::BadRequest, e.to_string()));
        }
        if !rest.is_empty() {
            return reply.send(Resp::error(
                Status::BadRequest,
                "a concat-objects request has trailing arguments",
            ));
        }

        let mut copied = Vec::new();
        let mut records = ORoff::ZERO;
        let mut framing = None;
        let mut keyed = None;
        let mut single_streams = [None; 2];
        let mut refresh = Vec::new();
        let mut refreshed = Vec::new();
        for (source_number, source) in sources.iter().enumerate() {
            if source == &destination {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "concat destination must differ from its sources",
                ));
            }
            let objects = self.object_table.read().expect("objects");
            let Some(object) = objects.get(source.as_str()) else {
                return reply.send(Resp::error(Status::NotFound, format!("no object {source}")));
            };
            if !object.deletion.readable(false) {
                return reply.send(bad("object is deleted"));
            }
            let table = self.stream_table.read().expect("streams");
            if object.streams.is_empty()
                || object.streams.iter().any(|id| table[id.get() as usize].records.is_none())
            {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    format!("cannot concatenate open object {source}"),
                ));
            }
            if let Some(expected) = framing {
                if expected != object.framing {
                    return reply.send(Resp::error(
                        Status::BadRequest,
                        "concat sources have different framing",
                    ));
                }
            } else {
                framing = Some(object.framing);
            }
            if let Some(expected) = keyed {
                if expected != object.keyed {
                    return reply.send(Resp::error(
                        Status::BadRequest,
                        "concat sources have different flavors",
                    ));
                }
            } else {
                keyed = Some(object.keyed);
            }
            if source_number < single_streams.len() && object.streams.len() == 1 {
                single_streams[source_number] =
                    Some(table[object.streams[0].get() as usize].snapshot());
            }
            for id in &object.streams {
                let stream = &table[id.get() as usize];
                if stream.records.is_some()
                    && (stream.extents.is_none()
                        || stream.logical.is_none()
                        || stream.compressed.is_none())
                    && !refreshed.contains(&(source.clone(), stream.id))
                {
                    refreshed.push((source.clone(), stream.id));
                    refresh.push((
                        source.clone(),
                        stream.sn,
                        StreamName { range: self.mine, stream: stream.id, created: stream.created },
                    ));
                }
            }
            let stream_count = copied.len().checked_add(object.streams.len());
            if stream_count.map_or(true, |count| count as u64 > u32::MAX as u64) {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "concat would exceed the stream-reference limit",
                ));
            }
            records = match records.plus(object.records(&table)) {
                Ok(records) => records,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
            copied.extend(object.streams.iter().copied());
        }
        if !refresh.is_empty() {
            return self.refresh_concat(process, retry, reply, refresh, 0);
        }
        if records.number() >= OPEN_STREAM_RECORD_LIMIT {
            return reply.send(Resp::error(
                Status::BadRequest,
                "concat would exceed the object record limit",
            ));
        }

        // Two one-stream objects on one SN can be represented by one new SN stream.
        // This keeps the DN metadata bounded while the SN owns the growing extent list.
        let optimized = if sources.len() == 2 {
            match (single_streams[0], single_streams[1]) {
                (Some(left), Some(right))
                    if left.sn == right.sn
                        && left.extents.is_some()
                        && right.extents.is_some()
                        && left.logical.is_some()
                        && right.logical.is_some()
                        && left.compressed.is_some()
                        && right.compressed.is_some() =>
                {
                    let extents = left.extents.number().checked_add(right.extents.number());
                    let logical = left.logical.plus(right.logical).ok();
                    let compressed = left.compressed.plus(right.compressed).ok();
                    match (extents, logical, compressed) {
                        (Some(extents), Some(logical), Some(compressed))
                            if extents < process.cluster.extents.per_stream as u64
                                && extents <= ExtentId::MAX.number() =>
                        {
                            Some((left, right, ExtentId::new(extents as u32), logical, compressed))
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        if let Some((left, right, extents, logical, compressed)) = optimized {
            let sn = format!("SN{}", left.sn);
            let sn_number = left.sn;
            let stream = match self.reserve_stream() {
                Ok(id) => id,
                Err(e) => return reply.send(Resp::error(Status::Internal, e.to_string())),
            };
            let created = TimeSec::now();
            {
                let mut objects = self.object_table.write().expect("objects");
                if objects.contains_key(destination.as_str()) {
                    return reply.send(Resp::error(
                        Status::BadRequest,
                        format!("{destination} exists already"),
                    ));
                }
                objects.insert(
                    destination.clone().into_boxed_str(),
                    Object {
                        deletion: Flags::default(),
                        purge_only: false,
                        id: ObjectId::new(self.next_object.fetch_add(1, Ordering::Relaxed) as u32),
                        framing: framing.expect("at least two sources"),
                        streams: Vec::new(),
                        extending: true,
                        waiting: vec![reply],
                        keyed: keyed.expect("at least two sources"),
                        created,
                    },
                );
            }

            let destination_stream = StreamName { range: self.mine, stream, created };
            let left_stream =
                StreamName { range: self.mine, stream: left.id, created: left.created };
            let right_stream =
                StreamName { range: self.mine, stream: right.id, created: right.created };
            let mut payload = StructWriter::new();
            for name in [destination_stream, left_stream, right_stream] {
                payload.range_id(name.range);
                payload.stream_id(name.stream);
                payload.time_sec(name.created);
            }
            payload.flag(false);
            payload.flag(false);
            let ask = Req::new(Op::ConcatStreams).structure(payload);
            let dn = Arc::clone(self);
            let destination_name = destination.clone();
            let named = sn.clone();
            if let Err(e) = process.ask(&sn, &ask, move |answer| {
                let _state = dn.global_lock.read();
                if answer.status != Status::Ok {
                    return dn.fail_concat(
                        &destination_name,
                        Resp::error(
                            answer.status,
                            format!("{named} would not concatenate streams"),
                        ),
                    );
                }
                dn.finish_concat_stream(
                    &destination_name,
                    stream,
                    sn_number,
                    extents,
                    logical,
                    compressed,
                );
            }) {
                self.fail_concat(&destination, Resp::error(Status::Internal, e.to_string()));
            }
            return;
        }

        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(e) => return reply.send(Resp::error(Status::Internal, e.to_string())),
        };
        let sn_number =
            SnId::new(process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0));
        let stream = match self.reserve_stream() {
            Ok(id) => id,
            Err(e) => return reply.send(Resp::error(Status::Internal, e.to_string())),
        };
        let created = TimeSec::now();
        {
            let mut objects = self.object_table.write().expect("objects");
            if objects.contains_key(destination.as_str()) {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    format!("{destination} exists already"),
                ));
            }
            if let Err(e) = self.retain_streams(&copied) {
                return reply.send(Resp::error(Status::BadRequest, e.to_string()));
            }
            let object = Object {
                deletion: Flags::default(),
                purge_only: false,
                id: ObjectId::new(self.next_object.fetch_add(1, Ordering::Relaxed) as u32),
                framing: framing.expect("at least two sources"),
                streams: copied,
                extending: true,
                waiting: vec![reply],
                keyed: keyed.expect("at least two sources"),
                created,
            };
            objects.insert(destination.clone().into_boxed_str(), object);
        }

        let stream_name = StreamName { range: self.mine, stream, created };
        let ask = create_streams(std::slice::from_ref(&stream_name));
        let dn = Arc::clone(self);
        let named = sn.clone();
        let destination_name = destination.clone();
        if let Err(e) = process.ask(&sn, &ask, move |answer| {
            let _state = dn.global_lock.read();
            if answer.status != Status::Ok {
                return dn.fail_concat(
                    &destination_name,
                    Resp::error(answer.status, format!("{named} would not make concat stream")),
                );
            }
            dn.finish_concat(&destination_name, stream, sn_number);
        }) {
            self.fail_concat(&destination, Resp::error(Status::Internal, e.to_string()));
        }
    }

    /// Refresh sealed-stream totals before concat arithmetic. A zero-row SN listing carries
    /// the complete totals without transferring the extent list itself.
    fn refresh_concat(
        self: &Arc<Self>,
        process: &Arc<Process>,
        request: Req,
        reply: Reply,
        refresh: Vec<(String, SnId, StreamName)>,
        at: usize,
    ) {
        let Some((object, sn, stream)) = refresh.get(at).cloned() else {
            return self.concat_objects(process, &request, reply);
        };
        let mut payload = StructWriter::new();
        common::info::Listing { limit: 0, instances: true, at: vec![(stream, SRoff::ZERO)] }
            .write_payload(&mut payload);
        let ask = Req::new(Op::ListExtents).structure(payload);
        let dn = Arc::clone(self);
        let identity = format!("SN{sn}");
        let onward = Arc::clone(process);
        let next = Arc::clone(&onward);
        let named = identity.clone();
        let asked = onward.ask(&identity, &ask, move |answer| {
            let _state = dn.global_lock.read();
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let runs = match ExtentRun::all_of_args(answer.args()) {
                Ok(mut runs) if runs.len() == 1 && runs[0].0.stream == stream.stream => {
                    let (run, rows) = runs.pop().expect("one run");
                    vec![Some((run, rows))]
                }
                _ => {
                    return reply.send(Resp::error(
                        Status::Internal,
                        format!("{named} returned no totals for {stream}"),
                    ));
                }
            };
            dn.remember(&object, &runs);
            dn.refresh_concat(&next, request, reply, refresh, at + 1);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {identity},{e}");
        }
    }

    /// Complete the asynchronous stream creation behind an object concat.
    fn finish_concat(&self, name: &str, stream: StreamId, sn: SnId) {
        let mut objects = self.object_table.write().expect("objects");
        let Some(object) = objects.get_mut(name) else { return };
        object.streams.push(self.install_stream(Stream::new(stream, sn, object.created)));
        object.extending = false;
        self.objects.fetch_add(1, Ordering::Relaxed);
        drop(objects);
        self.wake(&[name.to_string()], None);
    }

    /// Complete an object concat whose two source streams were combined by their SN.
    fn finish_concat_stream(
        &self,
        name: &str,
        stream: StreamId,
        sn: SnId,
        extents: ExtentId,
        logical: SLoff,
        compressed: SCoff,
    ) {
        let mut objects = self.object_table.write().expect("objects");
        let Some(object) = objects.get_mut(name) else { return };
        object.streams.push(self.install_stream(Stream {
            id: stream,
            deletion: Flags::default(),
            records: SRoff::NONE,
            created: object.created,
            logical,
            compressed,
            sn,
            extents,
        }));
        object.extending = false;
        self.objects.fetch_add(1, Ordering::Relaxed);
        drop(objects);
        self.wake(&[name.to_string()], None);
    }

    /// Remove a concat object that could not obtain its final stream and answer all
    /// readers that were parked behind its creation.
    fn fail_concat(&self, name: &str, failure: Resp) {
        let waiters = self
            .object_table
            .write()
            .expect("objects")
            .remove(name)
            .map(|object| {
                self.release_streams(&object.streams);
                object.waiting
            })
            .unwrap_or_default();
        for waiter in waiters {
            waiter.send(failure.clone());
        }
    }

    /// `(name, keyed, framing kind, framing width)`, and one stream to go with it.
    fn create_request(&self, process: &Process, request: &Req) -> Resp {
        match request.op() {
            Op::CreateObject => self.create_one(process, request),
            Op::BatchCreate => self.create_many(process, request),
            _ => Resp::error(Status::BadRequest, "not a create request"),
        }
    }

    fn create_one(&self, process: &Process, request: &Req) -> Resp {
        // Only the first few arguments are the object; a create may carry more —
        // media class, an expiry — that nothing here reads yet.
        let (mut payload, rest) =
            match structure(&request.args(), Op::CreateObject, "a create-object request") {
                Ok(value) => value,
                Err(e) => {
                    return Resp::error(Status::BadRequest, e.to_string());
                }
            };
        let wanted = match Wanted::read(&mut payload) {
            Ok(wanted) => wanted,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        // These fields are currently accepted for the CLI's create options but are not
        // interpreted by the DN yet. Keep them inside the command payload so the parser
        // still validates the complete value.
        if let Err(e) = payload.text() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        let has_expiry = match payload.flag() {
            Ok(has_expiry) => has_expiry,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if has_expiry {
            if let Err(e) = payload.number_u64() {
                return Resp::error(Status::BadRequest, e.to_string());
            }
        }
        let flags = match AppendFlags::read(&mut payload) {
            Ok(flags) => flags,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        if !rest.is_empty() {
            return Resp::error(
                Status::BadRequest,
                "a create-object request has trailing arguments",
            );
        }
        if let Err(answer) = validate_create_names(std::slice::from_ref(&wanted)) {
            return answer;
        }
        let name = wanted.name.clone();
        self.record(process, vec![wanted], flags, move |dn, _made| {
            // The object it just made, in the same shape object-info answers, so an
            // append that had to create one needs no second lookup to go on with. An
            // accepted name collision returns the existing row here as well.
            match dn.look(&name) {
                Some(args) => Resp::from_args(Status::Ok, args).id(0),
                None => Resp::error(Status::Internal, format!("{name} went missing")),
            }
        })
    }

    /// A run of `(name, keyed)` pairs, which costs one stream request and one
    /// answer however many objects it holds.
    fn create_many(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::BatchCreate, "a batch-create request") {
                Ok(value) => value,
                Err(e) => {
                    return Resp::error(Status::BadRequest, e.to_string());
                }
            };
        let count = match payload.number_u64() {
            Ok(count) => count as usize,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let mut wanted = match payload.checked_vec(count) {
            Ok(wanted) => wanted,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        for _ in 0..count {
            match Wanted::read(&mut payload) {
                Ok(one) => wanted.push(one),
                Err(e) => {
                    return Resp::error(Status::BadRequest, e.to_string());
                }
            }
        }
        let flags = match AppendFlags::read(&mut payload) {
            Ok(flags) => flags,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        if !rest.is_empty() {
            return Resp::error(
                Status::BadRequest,
                "a batch-create request has trailing arguments",
            );
        }
        if let Err(answer) = validate_create_names(&wanted) {
            return answer;
        }
        self.record(process, wanted, flags, |_, made| Resp::number(Status::Ok, made))
    }

    /// Commit DN's object and stream declarations, then create the SN streams.
    /// The SN call is outside the global lock; if it fails, the DN declaration
    /// remains and reads that also consult SN can report the mismatch as pending.
    fn record(
        &self,
        process: &Process,
        wanted: Vec<Wanted>,
        flags: AppendFlags,
        say: impl FnOnce(&DN, u64) -> Resp,
    ) -> Resp {
        if wanted.is_empty() {
            return say(self, 0);
        }
        let created = TimeSec::now();
        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        let sn_number =
            SnId::new(process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0));
        let first_object =
            match claim_ids(&self.next_object, wanted.len(), ObjectId::NONE.number(), "object") {
                Ok(first) => first,
                Err(error) => return Resp::error(Status::Internal, error.to_string()),
            };
        let streams = match self.allocate_stream_ids(wanted.len()) {
            Ok(streams) => streams,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        let rows: Vec<_> = wanted
            .iter()
            .zip(streams)
            .enumerate()
            .map(|(offset, (one, stream))| DnCreateRow {
                name: one.name.clone(),
                id: ObjectId::new((first_object + offset as u64) as u32),
                keyed: one.keyed,
                framing: one.framing,
                created,
                stream,
                sn: sn_number,
            })
            .collect();
        let action = match dn_action(&DnCreateCmd { rows: rows.clone(), flags }) {
            Ok(action) => action,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        let ticket = match self.queue_client(action) {
            Ok(ticket) => ticket,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        let index = match ticket.wait() {
            Ok(index) => index,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        if let Err(error) = self.wait_applied(index) {
            logw!("DN create action did not finish", "error {error}");
            return Resp::error(Status::Internal, error.to_string());
        }
        if let Some(answer) = self
            .create_rejects
            .lock()
            .expect("create rejects")
            .take(ObjectId::new(first_object as u32))
        {
            return answer;
        }

        // Read the rows actually applied: accepted collisions reuse the existing
        // object's streams, while new names use the rows from this command.
        let (by_sn, made) = match self.create_targets(&wanted, &rows) {
            Ok(targets) => targets,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        for (sn_id, names) in by_sn {
            let Some(sn) = process
                .cluster
                .services
                .iter()
                .find(|entry| entry.kind == ServiceKind::Sn && entry.number() == sn_id.number())
                .map(|entry| entry.identity.as_str())
            else {
                return Resp::error(Status::Internal, format!("unknown SN {sn_id}"));
            };
            let answer = match process.call(sn, &create_streams(&names)) {
                Ok(answer) => answer,
                Err(error) => return Resp::error(Status::Internal, error.to_string()),
            };
            if answer.status != Status::Ok {
                return answer;
            }
        }
        let _state = self.global_lock.read();
        say(self, made)
    }

    fn create_targets(
        &self,
        wanted: &[Wanted],
        planned: &[DnCreateRow],
    ) -> common::Result<(HashMap<SnId, Vec<StreamName>>, u64)> {
        let _state = self.global_lock.read();
        let objects = self.object_table.read().expect("objects");
        let streams = self.stream_table.read().expect("streams");
        let planned: HashMap<_, _> =
            planned.iter().map(|row| (row.name.as_str(), row.id)).collect();
        let mut by_sn = HashMap::<SnId, Vec<StreamName>>::new();
        let mut made = 0;
        for one in wanted {
            let object = objects
                .get(one.name.as_str())
                .ok_or_else(|| common::Error::new(format!("{} vanished after create", one.name)))?;
            if planned.get(one.name.as_str()) == Some(&object.id) {
                made += 1;
            }
            if object.deletion.soft_deleted() || object.deletion.deleted() {
                continue;
            }
            for id in &object.streams {
                let stream = streams
                    .get(id.get() as usize)
                    .filter(|stream| stream.id == *id)
                    .ok_or_else(|| common::Error::new(format!("stream {id} is missing in DN")))?;
                by_sn.entry(stream.sn).or_default().push(StreamName {
                    range: self.mine,
                    stream: stream.id,
                    created: stream.created,
                });
            }
        }
        Ok((by_sn, made))
    }

    /// `(name, sealed stream, records)`: that stream is full, so give the object
    /// another one carrying on from where it stopped.
    ///
    /// **One request does the work.** Several appends find the stream full at once,
    /// so the first to arrive asks an SN and the rest park behind it — and because
    /// asking an SN is a round trip, they park rather than hold a worker.
    fn extend(self: &Arc<Self>, process: &Arc<Process>, request: &Req, reply: Reply) {
        let (name, sealed, records) = match extension(request) {
            Ok(extension) => extension,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };

        // ⚠️ One lock, and everything decided inside it. An earlier version released
        // the lock before parking — so the extension could finish in the gap and wake
        // nobody — and called a method that takes the same lock again, which is a
        // deadlock rather than a slow path.
        let mut carry = Some(reply);
        let next = {
            let mut objects = self.object_table.write().expect("objects");
            match objects.get_mut(name.as_str()) {
                None => Next::Say(Resp::error(Status::NotFound, format!("no object {name}"))),
                Some(object) if !object.deletion.readable(false) => {
                    Next::Say(bad("object is deleted"))
                }
                Some(object) => {
                    // Whoever names the stream that filled is the one that seals it in
                    // the list; the rest are late and have nothing to record.
                    let ours = object.streams.last().is_some_and(|last| *last == sealed);
                    if let Some(last) = object.streams.last_mut().filter(|_| ours) {
                        self.stream_table.write().expect("streams")[last.get() as usize].records =
                            records;
                    }
                    if object.extending {
                        object.waiting.push(carry.take().expect("a reply"));
                        self.parked.fetch_add(1, Ordering::Relaxed);
                        Next::Wait
                    } else if ours {
                        // The sealed stream's count was just recorded above, so the object's
                        // total is known and is where the next stream starts. Reserve enough
                        // headroom for the one open stream that is about to be added.
                        let first_record =
                            object.records(&self.stream_table.read().expect("streams"));
                        if first_record.number() >= OPEN_STREAM_RECORD_LIMIT {
                            Next::Say(Resp::error(
                                Status::BadRequest,
                                "append would exceed the object record limit",
                            ))
                        } else {
                            object.extending = true;
                            Next::Ours(first_record)
                        }
                    } else {
                        // Somebody else extended past it already, so whatever is last
                        // now is the answer.
                        match object.args(
                            &self.stream_table.read().expect("streams"),
                            self.mine,
                            self.dn,
                            None,
                        ) {
                            Some(args) => Next::Say(Resp::from_args(Status::Ok, args).id(0)),
                            None => Next::Say(Resp::error(
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
        first_record: ORoff,
        mut reply: Reply,
    ) {
        let created = TimeSec::now();
        let stream = match self.reserve_stream() {
            Ok(id) => id,
            Err(e) => return reply.send(self.gave_up(&name, Status::Internal, e.to_string())),
        };
        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(e) => return reply.send(self.gave_up(&name, Status::Internal, e.to_string())),
        };
        let sn_number =
            SnId::new(process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0));
        let stream_name = StreamName { range: self.mine, stream, created };
        let ask = create_streams(std::slice::from_ref(&stream_name));

        let dn = Arc::clone(self);
        let named = sn.clone();
        reply.about(format_args!("{name} stream {stream} on {named} from record {first_record}"));
        let asked = process.ask(&sn, &ask, move |answer| {
            let _state = dn.global_lock.read();
            if answer.status != Status::Ok {
                let why = format!("{named} would not make a stream");
                let failed = dn.gave_up(&name, answer.status, why);
                dn.wake(std::slice::from_ref(&name), Some(&failed));
                return reply.send(failed);
            }
            {
                let mut objects = dn.object_table.write().expect("objects");
                if let Some(object) = objects.get_mut(name.as_str()) {
                    object.streams.push(dn.install_stream(Stream::new(stream, sn_number, created)));
                    object.extending = false;
                }
            }
            dn.streams.fetch_add(1, Ordering::Relaxed);
            let answer = match dn.describe(&name, None) {
                Some(args) => Resp::from_args(Status::Ok, args).id(0),
                None => Resp::error(Status::Internal, format!("{name} has no stream")),
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
    fn gave_up(&self, name: &str, status: Status, why: String) -> Resp {
        if let Some(object) = self.object_table.write().expect("objects").get_mut(name) {
            object.extending = false;
        }
        Resp::error(status, why)
    }

    /// Release names taken for objects that could not be given a stream.
    fn give_back(&self, names: &[String], status: Status, why: String) -> Resp {
        for name in names {
            if let Some(object) = self.object_table.write().expect("objects").remove(name.as_str())
            {
                self.release_streams(&object.streams);
            }
        }
        Resp::error(status, why)
    }

    fn info(
        self: &Arc<Self>,
        process: &Arc<Process>,
        request: &Req,
        record: Option<ORoff>,
        reply: Reply,
    ) {
        let (name, want, include) = if request.args().first().is_some_and(|arg| !arg.is_group()) {
            let (mut payload, rest) =
                match structure(&request.args(), Op::ObjectInfo, "an object-info request") {
                    Ok(value) => value,
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                };
            let name = match payload.text() {
                Ok(name) => name,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
            let has_want = match payload.flag() {
                Ok(value) => value,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
            let want = if has_want {
                match Want::read(&mut payload) {
                    Ok(want) => want,
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                }
            } else {
                Want::plain()
            };
            let include = match read_soft_deleted(&mut payload, rest) {
                Ok(value) => value,
                Err(error) => return reply.send(bad(error)),
            };
            (name, want, include)
        } else {
            return reply.send(Resp::error(
                Status::BadRequest,
                "object-info requires a structured payload",
            ));
        };
        let name = name.as_str();
        let limit = self.config.info_entries;
        // Most lookups only read the object table. If it is between streams, take
        // the write lock and recheck before parking, so a completion cannot wake
        // nobody in the gap.
        let mut carry = Some(reply);
        let describe = |object: &Object| -> Option<Result<Answering, Resp>> {
            // ⚠️ **The check goes here and nowhere else**, because the DN owns the path
            // table and is the only tier that can see a path point somewhere new. A read
            // answers with blocks and an append with a receipt; neither says which object
            // it was about, so without this a caller is answered about a different one and
            // cannot tell (§17.36).
            if !want.expect.holds(object.named(self.mine)) {
                return Some(Err(Resp::error(
                    Status::Replaced,
                    format!("{name} is {} and not {}", object.named(self.mine), want.expect.0),
                )));
            }
            if object.deletion.soft_deleted() && !object.deletion.deleted() && !include {
                return Some(Err(bad(
                    "object is soft-deleted; reading requires read-soft-deleted",
                )));
            }
            if object.deletion.deleted()
                || object.streams.iter().any(|id| {
                    self.stream_table.read().unwrap()[id.get() as usize].deletion.deleted()
                })
            {
                return Some(Err(Resp::error(Status::NotFound, "object is deleted")));
            }
            match describe_at(
                object,
                &self.stream_table.read().expect("streams"),
                self.mine,
                self.dn,
                record,
                &want,
                limit,
            ) {
                Some(answering) => Some(Ok(answering)),
                None if object.between_streams() => None,
                None => Some(Err(Resp::error(Status::NotFound, format!("no object {name}")))),
            }
        };
        let mut described = match self.object_table.read().expect("objects").get(name) {
            Some(object) => describe(object),
            None => Some(Err(Resp::error(Status::NotFound, format!("no object {name}")))),
        };
        if described.is_none() {
            let mut objects = self.object_table.write().expect("objects");
            described = match objects.get_mut(name) {
                Some(object) => match describe(object) {
                    None => {
                        object.waiting.push(carry.take().expect("a reply"));
                        self.parked.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    answered => answered,
                },
                None => Some(Err(Resp::error(Status::NotFound, format!("no object {name}")))),
            };
        }
        let (Some(described), Some(reply)) = (described, carry) else { return };
        match described {
            Err(refused) => reply.send(refused),
            Ok(Answering::Plain(args)) => reply.send(Resp::from_args(Status::Ok, args).id(0)),
            // Extents were asked for, so the SNs holding those streams have to be asked.
            Ok(Answering::Levelled(levelled)) if want.wants(Detail::Extents) => self.spread(
                process,
                name,
                levelled,
                want.from.object_record().expect("validated record start"),
                limit.min(if want.limit == 0 { limit } else { want.limit }),
                include,
                reply,
            ),
            Ok(Answering::Levelled(levelled)) => {
                reply.send(Resp::from_args(Status::Ok, levelled.args()).id(0))
            }
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
        from: ORoff,
        limit: u64,
        include: bool,
        reply: Reply,
    ) {
        // Group by SN, keeping each stream's record to start from.
        // ⚠️ The whole name per stream, not the id: the SN keys streams by
        // `(RangeId, StreamId, CreateSec)` (#109), and the DN is what holds all three.
        // The DN's own range, which it already holds — see `DN::mine`.
        let by_sn = extent_asks(self.mine, &levelled.rows, from);
        if by_sn.is_empty() {
            return reply.send(Resp::from_args(Status::Ok, levelled.args()).id(0));
        }

        // Each SN can use the allowance; final assembly trims the combined rows.
        let room = limit; // Overfetch per SN; trim the combined answer, not stale estimates.
        let gather = Arc::new(Mutex::new(Gather {
            left: by_sn.len(),
            runs: vec![None; levelled.rows.len()],
            failed: None,
            reply: Some(reply),
            head: levelled.head,
            rows: levelled.rows,
            shape: levelled.shape,
            hide_rows: levelled.hide_rows,
            instances: levelled.instances,
            name: name.to_string(),
            limit,
        }));

        let dn = Arc::clone(self);
        for (sn, at) in by_sn {
            let identity = format!("SN{sn}");
            let ask = with_read_soft_deleted(ask_extents(&at, room, levelled.instances), include);
            let waiting = Arc::clone(&gather);
            let named = identity.clone();
            let mine = Arc::clone(&dn);
            if let Err(e) = process.ask(&identity, &ask, move |answer| {
                let _state = mine.global_lock.read();
                arrived(&mine, &waiting, &at, answer)
            }) {
                logw!("could not reach an SN", "sn {named},{e}");
                arrived(
                    self,
                    &gather,
                    &[],
                    Resp::error(Status::Internal, format!("could not reach {named}")),
                );
            }
        }
    }

    /// What the SNs said a stream has come to, kept against the next question about it
    /// (§17.25.1). Takes the object-table lock, so nothing may hold it.
    fn remember(&self, name: &str, runs: &[Option<ExtentReply>]) {
        if runs.is_empty() {
            return;
        }
        let objects = self.object_table.read().expect("objects");
        // Gone while the SNs answered: a deleted object is not a lost update worth caring
        // about, since the streams it named are going away too.
        let Some(object) = objects.get(name) else { return };
        let mut table = self.stream_table.write().expect("streams");
        for id in &object.streams {
            let held = &mut table[id.get() as usize];
            if let Some((run, _)) = runs.iter().flatten().find(|(run, _)| run.stream == held.id) {
                held.learned(run.totals);
            }
        }
    }

    /// Answer requests parked while an object's stream list was being extended.
    fn wake(&self, names: &[String], failed: Option<&Resp>) {
        for name in names {
            let (waiters, args) = {
                let mut objects = self.object_table.write().expect("objects");
                match objects.get_mut(name.as_str()) {
                    Some(object) => (
                        std::mem::take(&mut object.waiting),
                        object.args(
                            &self.stream_table.read().expect("streams"),
                            self.mine,
                            self.dn,
                            None,
                        ),
                    ),
                    None => (Vec::new(), None),
                }
            };
            self.parked.fetch_sub(waiters.len() as u64, Ordering::Relaxed);
            for reply in waiters {
                reply.send(match (failed, &args) {
                    (None, Some(args)) => Resp::from_args(Status::Ok, args.clone()).id(0),
                    (Some(failed), _) => failed.clone(),
                    (None, None) => {
                        Resp::error(Status::Internal, format!("{name} was made and then lost"))
                    }
                });
            }
        }
    }

    /// Parse one batch-info request.
    fn read_batch_info(request: &Req) -> Result<(Vec<String>, Want), Resp> {
        let (mut payload, rest) =
            structure(&request.args(), Op::BatchInfo, "a batch-object-info request")
                .map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?;
        let count =
            payload.number_u64().map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?
                as usize;
        let mut names = payload
            .checked_vec(count)
            .map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?;
        for _ in 0..count {
            names.push(payload.text().map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?);
        }
        let has_want =
            payload.flag().map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?;
        let want = if has_want {
            Want::read(&mut payload).map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?
        } else {
            Want::plain()
        };
        payload.done().map_err(|e| Resp::error(Status::BadRequest, e.to_string()))?;
        if !rest.is_empty() {
            return Err(Resp::error(
                Status::BadRequest,
                "a batch-object-info request has trailing arguments",
            ));
        }
        Ok((names, want))
    }

    /// Start a rich batch-info scatter.  The replies are intentionally not interpreted yet.
    fn begin_info_many(self: &Arc<Self>, process: &Arc<Process>, request: &Req, reply: Reply) {
        let (names, want) = match Self::read_batch_info(request) {
            Ok(value) => value,
            Err(answer) => return reply.send(answer),
        };
        let unsupported =
            want.beyond(BATCH_ANSWERS | Detail::Extents.bit() | Detail::Instances.bit());
        if !unsupported.is_empty() {
            return reply.send(Resp::error(
                Status::Unimplemented,
                format!("object-info cannot yet return {}", unsupported.join(", ")),
            ));
        }
        let beyond = want.beyond(BATCH_ANSWERS);
        if beyond.is_empty() {
            return reply.send(self.info_many(request));
        }

        let limit = if want.limit == 0 {
            self.config.info_entries
        } else {
            want.limit.min(self.config.info_entries)
        };
        let from = want.from.object_record().expect("validated record start");
        // First construct the object and stream rows. The SN responses will fill in the
        // authoritative extent runs and totals without making this fan-out hold DN locks.
        let mut base_payload = StructWriter::new();
        base_payload.number_u64(names.len() as u64);
        for name in &names {
            base_payload.text(name);
        }
        base_payload.flag(true);
        Want { detail: want.detail & BATCH_ANSWERS, ..want }.write(&mut base_payload);
        let base = self.info_many(&Req::new(Op::BatchInfo).structure(base_payload));
        if base.status != Status::Ok {
            return reply.send(base);
        }
        let rows = match batch_stream_rows(base.args()) {
            Ok(rows) => rows,
            Err(e) => return reply.send(Resp::error(Status::Internal, e.to_string())),
        };
        let by_sn = extent_asks(self.mine, &rows, from);
        if by_sn.is_empty() {
            return reply.send(base);
        }
        let gather = Arc::new(Mutex::new(BatchCountGather {
            left: by_sn.len(),
            reply: Some(reply),
            base: base.into_args(),
            runs: vec![None; rows.len()],
            failed: None,
        }));
        for (sn, at) in by_sn {
            let identity = format!("SN{sn}");
            let ask = ask_extents(&at, limit, want.wants(Detail::Instances));
            let waiting = Arc::clone(&gather);
            if let Err(e) = process.ask(&identity, &ask, move |answer| {
                batch_arrived(&waiting, &at, answer, limit);
            }) {
                logw!("could not reach an SN", "sn {identity},{e}");
                batch_arrived(
                    &gather,
                    &[],
                    Resp::error(Status::Internal, format!("could not reach {identity}")),
                    limit,
                );
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
    fn info_many(&self, request: &Req) -> Resp {
        // The names, then one want field for all of them — a batch asks the same question about
        // every name, so carrying it per name would repeat it a thousand times (§9.1.3.11).
        let (names, want) = match Self::read_batch_info(request) {
            Ok(value) => value,
            Err(answer) => return answer,
        };
        let beyond = want.beyond(BATCH_ANSWERS);
        if !beyond.is_empty() {
            return Resp::error(
                Status::Unimplemented,
                format!("object-info cannot yet return {}", beyond.join(", ")),
            );
        }
        let limit = match want.limit {
            0 => self.config.info_entries,
            asked => asked.min(self.config.info_entries),
        };

        // A rich object normally contributes a shape, an object and one stream.
        // This is only a capacity estimate; multi-stream objects still grow normally.
        let per_name = 1 + usize::from(want.detail != 0) + usize::from(want.wants(Detail::Streams));
        let names_in_limit = names.len().min(usize::try_from(limit).unwrap_or(usize::MAX));
        let mut args = Vec::with_capacity(names_in_limit * per_name);
        let mut spent = 0;
        let mut given = 0;
        // Whether the last name given was described completely. Only the last one can be partial:
        // an earlier one stopping short would have spent the whole limit.
        let mut whole = true;
        for name in &names {
            let name = name.as_str();
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
                        args.push(Shape::plain().arg());
                    }
                    args.push(Described::missing_arg());
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
        Resp::from_args(Status::Ok, out).id(0)
    }

    /// One name's worth of a batch answer, and what it costs in entries.
    ///
    /// ⚠️ **Takes the object-table read lock** — see `look`.
    fn piece(&self, name: &str, want: &Want, room: u64) -> Option<Piece> {
        let objects = self.object_table.read().expect("objects");
        let object = objects.get(name)?;
        if !object.deletion.readable(false) {
            return None;
        }
        if want.detail == 0 {
            return object
                .args(&self.stream_table.read().expect("streams"), self.mine, self.dn, None)
                .map(|args| Piece { entries: 1, whole: true, args });
        }
        // Room is what the limit has left, and an object costs at least its own entry, so a name
        // reached with no room to spare still gets its row and no streams.
        let levelled = detailed(
            object,
            &self.stream_table.read().expect("streams"),
            self.mine,
            self.dn,
            want,
            room.max(1),
        )?;
        let (entries, whole) = (levelled.shape.entries, !levelled.shape.more());
        Some(Piece { entries, whole, args: levelled.args() })
    }

    /// What an object is: id, stream, the SN holding it, the second it was made,
    /// its flavour, and its framing as a kind and a width.
    ///
    /// ⚠️ **Takes the object-table read lock**, so a caller already holding it
    /// deadlocks against itself. Anything working under a write lock builds its answer
    /// from the object it already has.
    fn look(&self, name: &str) -> Option<Vec<Arg>> {
        self.describe(name, None)
    }

    /// ⚠️ **Takes the object-table read lock** — see `look`.
    fn describe(&self, name: &str, record: Option<ORoff>) -> Option<Vec<Arg>> {
        self.object_table.read().expect("objects").get(name)?.args(
            &self.stream_table.read().expect("streams"),
            self.mine,
            self.dn,
            record,
        )
    }
}

/// Apply an object's persistent deletion policy to a later delete request.
fn deletion_policy(deletion: Delete, purge_only: bool) -> Delete {
    if purge_only {
        Delete {
            target: deletion.target,
            retention: Some(common::time::TimeIntSec64::MIN),
            hard: true,
        }
    } else {
        deletion
    }
}

/// Duplicate names are a malformed command, independent of the current object table.
/// Presence policies are decided by the replicated apply while it holds the state lock.
fn validate_create_names(wanted: &[Wanted]) -> Result<(), Resp> {
    let mut names = std::collections::HashSet::with_capacity(wanted.len());
    for one in wanted {
        if !names.insert(one.name.as_str()) {
            return Err(bad("create request repeats an object name"));
        }
    }
    Ok(())
}

impl DN {
    /// Declare objects and their initial streams together in DN's committed state.
    fn apply_create(&self, cmd: DnCreateCmd) -> common::Result<Option<CreateReject>> {
        #[derive(Clone, Copy)]
        enum Choice {
            New,
            Same,
            Keep,
        }

        if cmd.rows.is_empty() {
            return Err(common::Error::new("empty DN create action"));
        }

        let mut names = std::collections::HashSet::new();
        let mut object_ids = std::collections::HashSet::new();
        let mut stream_ids = std::collections::HashSet::new();
        let mut choices = Vec::with_capacity(cmd.rows.len());
        let mut add_objects = 0usize;
        let objects = self.object_table.read().expect("objects");
        for row in &cmd.rows {
            if row.id.is_none() || row.stream.is_none() || row.sn.is_none() {
                return Err(common::Error::new("DN create action has a sentinel id"));
            }
            if !names.insert(row.name.as_str())
                || !object_ids.insert(row.id)
                || !stream_ids.insert(row.stream)
            {
                return Err(common::Error::new("DN create action repeats an identity"));
            }
            let choice = match objects.get(row.name.as_str()) {
                None => {
                    if cmd.flags.require_exists() {
                        return Ok(Some(CreateReject {
                            status: Status::NotFound,
                            message: format!("required object {} does not exist", row.name),
                        }));
                    }
                    add_objects += 1;
                    Choice::New
                }
                Some(object)
                    if object.id == row.id
                        && object.created == row.created
                        && object.keyed == row.keyed
                        && object.purge_only == cmd.flags.purge_only()
                        && object.framing == row.framing
                        && object.streams.as_slice() == [row.stream] =>
                {
                    Choice::Same
                }
                Some(_object) if cmd.flags.require_new() => {
                    return Ok(Some(CreateReject {
                        status: Status::BadRequest,
                        message: format!("object {} already exists", row.name),
                    }));
                }
                Some(object) if cmd.flags.overwrite() && object.deletion.soft_deleted() => {
                    Choice::New
                }
                Some(_) if !cmd.flags.overwrite() => Choice::Keep,
                Some(_) => {
                    return Ok(Some(CreateReject {
                        status: Status::BadRequest,
                        message: format!("could not overwrite object {}", row.name),
                    }));
                }
            };
            choices.push(choice);
        }
        drop(objects);

        self.object_table
            .write()
            .expect("objects")
            .try_reserve(add_objects)
            .map_err(|error| common::Error::new(format!("reserve DN objects: {error}")))?;
        {
            let table = self.stream_table.read().expect("streams");
            for (row, choice) in cmd.rows.iter().zip(&choices) {
                if matches!(choice, Choice::Keep) {
                    continue;
                }
                let slot = row.stream.get() as usize;
                if slot < table.len() {
                    let held = &table[slot];
                    let matches =
                        held.id == row.stream && held.created == row.created && held.sn == row.sn;
                    let conflict = if matches!(choice, Choice::Same) {
                        !matches
                    } else {
                        !held.deletion.retired() && !matches
                    };
                    if conflict {
                        return Err(common::Error::new(format!(
                            "DN create action conflicts with stream {}",
                            row.stream
                        )));
                    }
                } else if matches!(choice, Choice::Same) {
                    return Err(common::Error::new("completed object has no stream slot"));
                }
                let same_live = slot < table.len()
                    && table[slot].id == row.stream
                    && table[slot].created == row.created
                    && table[slot].sn == row.sn
                    && !table[slot].deletion.retired();
                if matches!(choice, Choice::New) && !same_live && !table.can_reserve(row.stream) {
                    return Err(common::Error::new(format!(
                        "DN create action cannot reserve stream {}",
                        row.stream
                    )));
                }
            }
        }

        let mut added = 0u64;
        let mut objects = self.object_table.write().expect("objects");
        let mut table = self.stream_table.write().expect("streams");
        for (row, choice) in cmd.rows.into_iter().zip(choices) {
            if !matches!(choice, Choice::New) {
                continue;
            }
            let slot = row.stream.get() as usize;
            let same_live = slot < table.len()
                && table[slot].id == row.stream
                && table[slot].created == row.created
                && table[slot].sn == row.sn
                && !table[slot].deletion.retired();
            if !same_live {
                let mut stream = Stream::new(row.stream, row.sn, row.created);
                stream.deletion = Flags::live();
                table.install(stream).expect("prechecked DN stream slot");
            }
            objects.insert(
                row.name.clone().into_boxed_str(),
                Object {
                    deletion: Flags::live(),
                    purge_only: cmd.flags.purge_only(),
                    id: row.id,
                    framing: row.framing,
                    streams: vec![row.stream],
                    extending: false,
                    waiting: Vec::new(),
                    keyed: row.keyed,
                    created: row.created,
                },
            );
            self.retired_objects.lock().expect("retired objects").remove(row.name.as_str());
            self.next_object.fetch_max(row.id.get() as u64 + 1, Ordering::Relaxed);
            self.next_stream.fetch_max(row.stream.get() as u64 + 1, Ordering::Relaxed);
            added += 1;
        }
        self.objects.fetch_add(added, Ordering::Relaxed);
        self.streams.fetch_add(added, Ordering::Relaxed);
        Ok(None)
    }

    fn reserve_stream(&self) -> common::Result<StreamId> {
        self.reserve_streams(1)?
            .into_iter()
            .next()
            .ok_or_else(|| common::Error::new("no stream was reserved"))
    }

    fn allocate_stream_ids(&self, count: usize) -> common::Result<Vec<StreamId>> {
        self.reserve_streams(count)
    }

    fn reserve_streams(&self, count: usize) -> common::Result<Vec<StreamId>> {
        let first = claim_ids(&self.next_stream, count, StreamId::NONE.number(), "stream")?;
        let end = first
            .checked_add(count as u64)
            .ok_or_else(|| common::Error::new("stream table size overflow"))?;
        let mut ids = Vec::new();
        ids.try_reserve(count).map_err(|e| common::Error::new(e.to_string()))?;
        for n in first..end {
            ids.push(StreamId::new(n as u32));
        }
        Ok(ids)
    }

    fn install_stream(&self, stream: Stream) -> StreamId {
        let id = stream.id;
        let mut table = self.stream_table.write().expect("streams");
        assert!(table.can_reserve(id), "stream index is not available");
        table.install(stream).expect("prechecked DN stream index");
        id
    }

    fn retain_streams(&self, ids: &[StreamId]) -> common::Result<()> {
        let mut table = self.stream_table.write().expect("streams");
        let mut extras = self.stream_extras.lock().expect("stream extras");
        for (i, id) in ids.iter().enumerate() {
            let stream = &mut table[id.get() as usize];
            if let Err(error) = extras.retain((stream.id, stream.created), &mut stream.deletion) {
                for prior in &ids[..i] {
                    let stream = &mut table[prior.get() as usize];
                    extras
                        .release(&(stream.id, stream.created), &mut stream.deletion)
                        .expect("retained reference");
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn release_streams(&self, ids: &[StreamId]) {
        let mut table = self.stream_table.write().expect("streams");
        let mut extras = self.stream_extras.lock().expect("stream extras");
        for id in ids {
            let stream = &mut table[id.get() as usize];
            extras
                .release(&(stream.id, stream.created), &mut stream.deletion)
                .expect("owned reference");
        }
    }

    fn with_runtime(
        config: DNConfig,
        dn: DnId,
        mine: RangeId,
        path: &std::path::Path,
        runtime: RaftSettings,
    ) -> common::Result<DN> {
        runtime.validate(&config.raft)?;
        let mut initial = raft::State::initial(
            runtime.ring.clone(),
            if runtime.identities.is_empty() {
                Default::default()
            } else {
                runtime.members.clone()
            },
            runtime.identities.clone(),
        )?;
        initial.ignore_ring_identity = runtime.ignore_ring_identity;
        let journal = raft::journal::Journal::open(path, initial, 3, &runtime.name)?;
        let id =
            journal.state.membership().slot(&journal.identity.current).unwrap_or(raft::UNASSIGNED);
        let mut node = raft::Node::new_with_log2(id, journal, config.raft.log2_entries)?;
        node.merge_history_scn()?;
        node.identity = Some(node.journal.identity.current.clone());
        node.lease = raft::lease::Lease::configured(&config.raft);
        if node.state().membership().members().len() == 1
            && node.state().membership().members().contains(&id)
        {
            node.campaign()?;
        }
        let options = raft::runtime::Options {
            raft: config.raft.clone(),
            snapshot_min_log_bytes: u64::MAX,
            snapshot_interval_secs: u64::MAX,
            allow_stale_reads: config.knobs.allow_stale_reads,
        };
        let stream_table = Arc::new(RwLock::new(
            StreamTable::new()
                .map_err(|error| common::Error::new(format!("create DN stream pool: {error}")))?,
        ));
        let dn_node = DN {
            common: raft::runtime::Runtime::with_state(
                runtime,
                node,
                options,
                DNState { stream_table: Arc::clone(&stream_table) },
            ),
            config,
            gc_stop: AtomicBool::new(true),
            gc_thread: Mutex::new(None),
            deleting: Mutex::new(()),
            retired_objects: Mutex::new(std::collections::HashSet::new()),
            create_rejects: Mutex::new(CreateRejects::default()),
            dn,
            mine,
            object_table: RwLock::new(HashMap::new()),
            parked: AtomicU64::new(0),
            objects: AtomicU64::new(0),
            streams: AtomicU64::new(0),
            // ⚠️ From zero, because the sentinel is at the top of the range now: slot 0 of an
            // arena is a real object, and starting at one would waste it (§17.37.2).
            next_object: AtomicU64::new(0),
            next_stream: AtomicU64::new(0),
            stream_table,
            object_extras: Mutex::new(Extras::default()),
            stream_extras: Mutex::new(Extras::default()),
            #[cfg(test)]
            _test_dir: None,
        };
        dn_node.ready.store(true, Ordering::SeqCst);
        let commit = dn_node.on_raft(|host, node| {
            host.refresh_application(node)?;
            Ok(node.state().commit)
        })?;
        while dn_node.applied() < commit {
            dn_node.execute_pending()?;
        }
        Ok(dn_node)
    }

    #[cfg(test)]
    fn new(config: DNConfig, dn: DnId, mine: RangeId) -> DN {
        let settings = RaftSettings::testing(&config.raft);
        let directory = tempfile::tempdir().expect("create DN test directory");
        raft::journal::provision_test_founder(
            directory.path(),
            &settings.ring,
            &settings.name,
            settings.identities[&0].identity.clone(),
        )
        .expect("provision DN test journal");
        let mut dn = Self::with_runtime(config, dn, mine, directory.path(), settings)
            .expect("construct DN test Raft runtime");
        dn._test_dir = Some(directory);
        dn
    }
}

/// Stream rows in the already selected batch prefix, in original request order.
fn batch_stream_rows(args: &[Arg]) -> common::Result<Vec<StreamRow>> {
    let (batch, rest) = Batch::of_args(args)?;
    let mut all = Vec::new();
    for piece in Batch::pieces(rest, batch.given)? {
        let (shape, rest) = Shape::of_args(piece)?;
        let (_, rest) = Described::of_args(rest)?;
        let (rows, rest) = StreamRow::some_of_args(rest, shape.given.number())?;
        if !rest.is_empty() {
            return Err(common::Error::new("batch base has trailing rows"));
        }
        all.extend(rows);
    }
    Ok(all)
}

fn batch_arrived(
    gather: &Arc<Mutex<BatchCountGather>>,
    asked: &[ExtentAsk],
    answer: Resp,
    limit: u64,
) {
    let answer = pending_stream_answer(answer, asked);
    let mut held = gather.lock().expect("batch-info gather");
    if answer.status != Status::Ok {
        held.failed.get_or_insert(answer);
    } else if let Err(e) = receive_extents(&mut held.runs, asked, answer.args()) {
        held.failed.get_or_insert(Resp::error(Status::Internal, e.to_string()));
    }
    held.left -= 1;
    if held.left != 0 {
        return;
    }
    let failed = held.failed.take();
    let base = std::mem::take(&mut held.base);
    let runs = std::mem::take(&mut held.runs);
    let reply = held.reply.take().expect("the last response owns the reply");
    drop(held);
    reply.send(failed.unwrap_or_else(|| merge_batch_info(base, &runs, limit)));
}

/// Merge SN extent runs into the batch body, retaining stream-relative positions.
/// Each parent stream row carries its object-relative base once.
fn merge_batch_info(args: Vec<Arg>, runs: &[Option<ExtentReply>], limit: u64) -> Resp {
    let merge = || -> common::Result<Vec<Arg>> {
        let (mut batch, rest) = Batch::of_args(&args)?;
        let pieces = Batch::pieces(rest, batch.given)?;
        let mut body = Vec::new();
        let mut entries = 0;
        let mut given = 0;
        let mut at = 0;
        for (i, piece) in pieces.iter().enumerate() {
            if entries == limit {
                batch.next = i as u64;
                break;
            }
            let (shape, rest) = Shape::of_args(piece)?;
            let (described, rest) = Described::of_args(rest)?;
            let (rows, rest) = StreamRow::some_of_args(rest, shape.given.number())?;
            if !rest.is_empty() {
                return Err(common::Error::new("batch base has trailing rows"));
            }
            let end = at + rows.len();
            let replies =
                runs.get(at..end).ok_or_else(|| common::Error::new("missing stream replies"))?;
            at = end;
            let merged = merge_object_info(
                Levelled {
                    shape,
                    head: described.map_or_else(Described::missing, |row| row.args()),
                    rows,
                    hide_rows: false,
                    instances: false,
                },
                replies,
                limit - entries,
            );
            let (shape, _) = Shape::of_args(&merged)?;
            entries += shape.entries;
            given += 1;
            body.extend(merged);
            if shape.more() {
                batch.next = i as u64;
                break;
            }
        }
        batch.given = given;
        batch.entries = entries;
        let mut out = batch.args();
        out.extend(body);
        Ok(out)
    };
    match merge() {
        Ok(args) => Resp::from_args(Status::Ok, args).id(0),
        Err(e) => Resp::error(Status::Internal, e.to_string()),
    }
}

/// One name's worth of a batch answer: what it cost, whether it is all of that
/// name, and the arguments themselves.
struct Piece {
    entries: u64,
    whole: bool,
    args: Vec<Arg>,
}

/// One object and its initial stream. A batch create carries a vector of these in
/// one Raft action, so stream ids and object rows become visible together.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DnCreateRow {
    name: String,
    id: ObjectId,
    keyed: bool,
    framing: Framing,
    created: TimeSec,
    stream: StreamId,
    sn: SnId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DnCreateCmd {
    rows: Vec<DnCreateRow>,
    flags: AppendFlags,
}

impl Wire for DnCreateCmd {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::DnCreate, |w| {
            w.number_u64(self.rows.len() as u64);
            for row in &self.rows {
                w.nested(Op::RaftPair, |w| {
                    w.text(&row.name);
                    w.number_u32(row.id.get());
                    w.flag(row.keyed);
                    let (kind, width) = row.framing.wire();
                    w.number_u8(kind);
                    w.number_u32(width);
                    w.time_sec(row.created);
                    w.stream_id(row.stream);
                    w.sn_id(row.sn);
                });
            }
            self.flags.write(w);
        });
    }

    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        r.nested(Op::DnCreate, "DN create action", |r| {
            let count = r.number_u64()?;
            let mut rows = common::checked_vec(count, r.remaining())?;
            for _ in 0..count {
                rows.push(r.nested(Op::RaftPair, "DN object and stream", |r| {
                    Ok(DnCreateRow {
                        name: r.text()?,
                        id: ObjectId::new(r.number_u32()?),
                        keyed: r.flag()?,
                        framing: Framing::of_wire(r.number_u8()?, r.number_u32()?)?,
                        created: r.time_sec()?,
                        stream: r.stream_id()?,
                        sn: r.sn_id()?,
                    })
                })?);
            }
            let flags = AppendFlags::read(r)?;
            Ok(DnCreateCmd { rows, flags })
        })
    }
}

fn dn_action(cmd: &DnCreateCmd) -> common::Result<raft::Action> {
    Ok(raft::Action::new(Op::DnCreate, raft::codec::encode(cmd)?))
}

fn claim_ids(next: &AtomicU64, count: usize, limit: u64, kind: &str) -> common::Result<u64> {
    let count =
        u64::try_from(count).map_err(|_| common::Error::new(format!("too many {kind}s")))?;
    let mut first = next.load(Ordering::Relaxed);
    loop {
        let end = first
            .checked_add(count)
            .filter(|end| *end <= limit)
            .ok_or_else(|| common::Error::new(format!("{kind} table is full")))?;
        match next.compare_exchange_weak(first, end, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Ok(first),
            Err(actual) => first = actual,
        }
    }
}

/// Decode the rollover request using the same ID and record widths as its writer.
fn create_streams(names: &[StreamName]) -> Req {
    let mut payload = StructWriter::new();
    payload.number_u64(names.len() as u64);
    for name in names {
        payload.range_id(name.range);
        payload.stream_id(name.stream);
        payload.time_sec(name.created);
    }
    Req::new(Op::CreateStream).structure(payload)
}

fn extension(request: &Req) -> common::Result<(String, StreamId, SRoff)> {
    let (mut payload, rest) =
        structure(&request.args(), Op::ExtendObject, "an extend-object request")?;
    if !rest.is_empty() {
        return Err(common::Error::new("an extend-object request has trailing arguments"));
    }
    let name = payload.text()?;
    let sealed = payload.stream_id()?;
    let records = payload.sroff()?;
    payload.done()?;
    Ok((name, sealed, records))
}

impl Host for DN {
    type Application = DNState;
    type Done = ();

    fn apply_client(
        &self,
        _state: &mut DNState,
        context: raft::execution::Context<'_>,
        action: &raft::Action,
        _done: &mut Vec<Self::Done>,
    ) -> common::Result<()> {
        match action.op {
            Op::DnCreate => {
                let cmd: DnCreateCmd = raft::codec::decode(&action.fields)?;
                let id = cmd
                    .rows
                    .first()
                    .ok_or_else(|| common::Error::new("empty DN create action"))?
                    .id;
                let reject = self.apply_create(cmd)?;
                self.create_rejects
                    .lock()
                    .map_err(|_| common::Error::new("create rejects poisoned"))?
                    .record(context.index, id, reject);
                Ok(())
            }
            _ => Err(common::Error::new(format!("DN does not apply {}", action.op))),
        }
    }

    fn reset_metadata(&self, state: &mut DNState) {
        raft::execution::Application::reset(state);
        self.object_table.write().expect("objects").clear();
        *self.object_extras.lock().expect("object extras") = Extras::default();
        *self.stream_extras.lock().expect("stream extras") = SharedExtras::default();
        self.retired_objects.lock().expect("retired objects").clear();
        *self.create_rejects.lock().expect("create rejects") = CreateRejects::default();
        self.next_object.store(0, Ordering::Relaxed);
        self.next_stream.store(0, Ordering::Relaxed);
        self.objects.store(0, Ordering::Relaxed);
        self.streams.store(0, Ordering::Relaxed);
        self.parked.store(0, Ordering::Relaxed);
    }

    fn prepare_primary(&self, _node: &mut raft::Node) -> common::Result<()> {
        Ok(())
    }

    fn checkpoint_actions(
        &self,
        state: &raft::State,
        _application: &Self::Application,
    ) -> common::Result<Vec<raft::Entry>> {
        Ok(state.capture().application)
    }

    fn peer_message(&self) -> Option<raft::Action> {
        None
    }

    fn receive_client(&self, _process: &Process, _action: raft::Action) -> common::Result<Resp> {
        Err(common::Error::new("DN has no Raft client actions yet"))
    }

    fn dispatch_forwarded(&self, process: &Process, request: &Req) -> Resp {
        self.create_request(process, request)
    }

    fn forwardable(&self, op: Op) -> bool {
        matches!(op, Op::CreateObject | Op::BatchCreate)
    }

    fn registered_endpoint(&self, _name: &str) -> Option<std::net::SocketAddr> {
        None
    }
}

fn main() {
    let result = server::main(|process| {
        let config = DNConfig::load(&process.config.service_config)?;
        let runtime = RaftSettings::for_process(process, &config.raft)?;
        let dn = DnId::new(process.entry.number());
        DN::with_runtime(
            config,
            dn,
            routing::range_of_dn(dn),
            &process.config.data_dir.join("raft"),
            runtime,
        )
    });
    if let Err(e) = result {
        eprintln!("dn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn purge_only_is_applied_when_a_later_delete_arrives() {
        let requested = Delete {
            target: Target::Object("/x".into()),
            retention: Some(common::time::TimeIntSec64::new(30)),
            hard: false,
        };
        assert_eq!(deletion_policy(requested.clone(), false), requested);
        assert_eq!(
            deletion_policy(requested, true),
            Delete {
                target: Target::Object("/x".into()),
                retention: Some(common::time::TimeIntSec64::MIN),
                hard: true,
            }
        );
    }

    #[test]
    fn create_batch_is_one_replayable_action() {
        let cmd = DnCreateCmd {
            rows: vec![
                DnCreateRow {
                    name: "/one".into(),
                    id: ObjectId::new(3),
                    keyed: false,
                    framing: Framing::Bytes,
                    created: TimeSec::new(500),
                    stream: StreamId::new(4),
                    sn: SnId::new(2),
                },
                DnCreateRow {
                    name: "/two".into(),
                    id: ObjectId::new(5),
                    keyed: true,
                    framing: Framing::Fixed(12),
                    created: TimeSec::new(500),
                    stream: StreamId::new(6),
                    sn: SnId::new(2),
                },
            ],
            flags: AppendFlags::PURGE_ONLY,
        };
        let action = dn_action(&cmd).unwrap();
        assert_eq!(action.op, Op::DnCreate);
        let read: DnCreateCmd = raft::codec::decode(&action.fields).unwrap();
        assert_eq!(read, cmd);

        let dn = DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST);
        assert!(dn.apply_create(read.clone()).unwrap().is_none());
        assert!(dn.apply_create(read).unwrap().is_none());
        assert_eq!(dn.objects.load(Ordering::Relaxed), 2);
        assert!(dn.object_table.read().unwrap()["/one"].purge_only);
        assert!(dn.object_table.read().unwrap()["/two"].purge_only);
        let table = dn.stream_table.read().unwrap();
        assert_eq!(table.len(), 7);
        assert_eq!(table[4].id, StreamId::new(4));
        assert_eq!(table[4].sn, SnId::new(2));
        assert_eq!(table[6].id, StreamId::new(6));
        drop(table);
        for (name, id, stream) in [("/one", 3, 4), ("/two", 5, 6)] {
            let objects = dn.object_table.read().unwrap();
            let object = objects.get(name).unwrap();
            assert_eq!(object.id, ObjectId::new(id));
            assert_eq!(object.streams, [StreamId::new(stream)]);
        }
    }

    #[test]
    fn create_policy_rejection_rolls_back_the_whole_batch() {
        let dn = DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST);
        let existing = DnCreateRow {
            name: "/same".into(),
            id: ObjectId::new(0),
            keyed: false,
            framing: Framing::Bytes,
            created: TimeSec::new(500),
            stream: StreamId::new(0),
            sn: SnId::new(1),
        };
        let initial = DnCreateCmd { rows: vec![existing.clone()], flags: AppendFlags::default() };
        assert!(dn.apply_create(initial).unwrap().is_none());

        let require_new = DnCreateCmd {
            rows: vec![
                DnCreateRow { id: ObjectId::new(1), stream: StreamId::new(1), ..existing.clone() },
                DnCreateRow {
                    name: "/new".into(),
                    id: ObjectId::new(2),
                    keyed: false,
                    framing: Framing::Bytes,
                    created: TimeSec::new(500),
                    stream: StreamId::new(2),
                    sn: SnId::new(1),
                },
            ],
            flags: AppendFlags::REQUIRE_NEW,
        };
        let reject = dn.apply_create(require_new).unwrap().unwrap();
        assert_eq!(reject.status, Status::BadRequest);
        assert_eq!(dn.objects.load(Ordering::Relaxed), 1);
        assert!(!dn.object_table.read().unwrap().contains_key("/new"));
        assert_eq!(dn.stream_table.read().unwrap().len(), 1);

        let require_exists = DnCreateCmd {
            rows: vec![
                DnCreateRow { id: ObjectId::new(3), stream: StreamId::new(3), ..existing },
                DnCreateRow {
                    name: "/missing".into(),
                    id: ObjectId::new(4),
                    keyed: false,
                    framing: Framing::Bytes,
                    created: TimeSec::new(500),
                    stream: StreamId::new(4),
                    sn: SnId::new(1),
                },
            ],
            flags: AppendFlags::REQUIRE_EXISTS,
        };
        let reject = dn.apply_create(require_exists).unwrap().unwrap();
        assert_eq!(reject.status, Status::NotFound);
        assert_eq!(dn.objects.load(Ordering::Relaxed), 1);
        assert!(!dn.object_table.read().unwrap().contains_key("/missing"));
        assert_eq!(dn.stream_table.read().unwrap().len(), 1);
    }

    #[test]
    fn a_rejected_create_action_reports_failure_without_stopping_apply() {
        let dn = DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST);
        let existing = DnCreateRow {
            name: "/same".into(),
            id: ObjectId::new(0),
            keyed: false,
            framing: Framing::Bytes,
            created: TimeSec::new(500),
            stream: StreamId::new(0),
            sn: SnId::new(1),
        };
        assert!(
            dn.apply_create(DnCreateCmd {
                rows: vec![existing.clone()],
                flags: AppendFlags::default()
            })
            .unwrap()
            .is_none()
        );

        let cmd = DnCreateCmd {
            rows: vec![DnCreateRow { id: ObjectId::new(1), stream: StreamId::new(1), ..existing }],
            flags: AppendFlags::REQUIRE_NEW,
        };
        let id = cmd.rows[0].id;
        let action = dn_action(&cmd).unwrap();
        let mut done = Vec::new();
        let mut state = DNState { stream_table: Arc::clone(&dn.stream_table) };
        dn.apply_client(
            &mut state,
            raft::execution::Context { ring: "DN", index: 7, term: 1 },
            &action,
            &mut done,
        )
        .unwrap();
        let answer = dn.create_rejects.lock().unwrap().take(id).unwrap();
        assert_eq!(answer.status, Status::BadRequest);
        assert_eq!(dn.objects.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn single_member_raft_applies_batch_create_as_one_action() {
        let dn =
            std::sync::Arc::new(DN::new(common::testing::dn_config(), DnId::FIRST, RangeId::FIRST));
        dn.start_raft().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut ready = false;
        while std::time::Instant::now() < deadline {
            let (role, commit) =
                dn.on_raft(|_, node| Ok((node.role, node.state().commit))).unwrap();
            if role == raft::Role::Leader && commit != 0 && dn.applied() >= commit {
                ready = true;
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        let action = dn_action(&DnCreateCmd {
            rows: vec![
                DnCreateRow {
                    name: "/raft/one".into(),
                    id: ObjectId::new(0),
                    keyed: false,
                    framing: Framing::Bytes,
                    created: TimeSec::new(800),
                    stream: StreamId::new(0),
                    sn: SnId::new(1),
                },
                DnCreateRow {
                    name: "/raft/two".into(),
                    id: ObjectId::new(1),
                    keyed: true,
                    framing: Framing::Lf,
                    created: TimeSec::new(800),
                    stream: StreamId::new(1),
                    sn: SnId::new(1),
                },
            ],
            flags: AppendFlags::default(),
        })
        .unwrap();
        let index = dn.queue_client(action).unwrap().wait().unwrap();
        dn.wait_applied(index).unwrap();
        assert_eq!(dn.objects.load(Ordering::Relaxed), 2);
        assert!(dn.look("/raft/one").is_some());
        assert!(dn.look("/raft/two").is_some());
        let status = dn.raft_status();
        dn.stop_raft().unwrap();
        assert!(ready, "single-member DN Raft did not apply its no-op entry: {status}");
    }

    fn counts(dn: &DN, stream: &Stream) -> (u32, u32) {
        dn.stream_extras.lock().unwrap().counts(&(stream.id, stream.created), stream.deletion)
    }

    #[test]
    fn objects_larger_than_byte_offset_space_still_have_record_access() {
        let mut object = grown();
        object.framing = Framing::Bytes;
        object.streams = vec![StreamId::new(0), StreamId::new(1)];
        let table: Vec<_> = (0..2)
            .map(|n| Stream { logical: SLoff::new((1u64 << 63) - 4), ..held(n, 0, Some(1)) })
            .collect();
        // Payload length fits even though length-prefixed framing would overflow.
        assert_eq!(object.logical(&table).number(), u64::MAX - 7);
        let table: Vec<_> = table
            .into_iter()
            .map(|s| Stream { logical: SLoff::new((1u64 << 63) + 10), ..s })
            .collect();
        assert_eq!(object.logical(&table), OLoff::MAX);
        assert_eq!(object.records(&table), ORoff::new(2));
        assert_eq!(object.holding(&table, ORoff::new(1)).unwrap().1.id, StreamId::new(1));
    }

    #[test]
    fn framing_overflow_saturates_instead_of_becoming_unknown() {
        let totals =
            Totals { extents: ExtentId::new(1), logical: SLoff::MAX, compressed: SCoff::ZERO };
        for framing in [Framing::Lf, Framing::Length] {
            assert_eq!(visible_totals(framing, totals, SRoff::new(1)).logical, SLoff::MAX);
        }
        assert_eq!(visible_totals(Framing::Bytes, totals, SRoff::NONE).logical, SLoff::MAX);
    }

    #[test]
    fn bytes_summary_counts_empty_records_without_exposing_prefixes() {
        let totals = Totals {
            extents: ExtentId::new(1),
            logical: SLoff::new(3),
            compressed: SCoff::new(99),
        };
        let visible = visible_totals(Framing::Bytes, totals, SRoff::new(4));
        assert_eq!(visible.logical, SLoff::new(3));
        assert_eq!(visible.compressed, totals.compressed);
        assert_eq!(visible_totals(Framing::Bytes, totals, SRoff::NONE).logical, SLoff::new(3));
    }

    #[test]
    fn deletion_retry_releases_each_counter_once_and_keeps_the_name_until_success() {
        use common::time::TimeIntSec64;
        let dn = DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST);
        let stream = held(0, 0, Some(1));
        install(&dn, stream);
        dn.retain_streams(&[StreamId::FIRST]).unwrap();
        let mut object = grown();
        object.streams = vec![StreamId::FIRST, StreamId::FIRST];
        dn.object_table.write().unwrap().insert("/retry".into(), object);
        dn.objects.store(1, Ordering::Relaxed);
        let process = testing::process();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        process.assign_peer("SN0".into(), listener.local_addr().unwrap());
        let seen = Arc::new(AtomicU32::new(0));
        let count = Arc::clone(&seen);
        let peer = std::thread::spawn(move || {
            let mut workers = Vec::new();
            for _ in 0..testing::messaging().peer_conns.max(1) {
                let socket = listener.accept().unwrap().0;
                socket.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
                let count = Arc::clone(&count);
                workers.push(std::thread::spawn(move || {
                    let (mut reader, mut writer) =
                        common::accept(socket, &testing::messaging()).unwrap();
                    for _ in 0..2 {
                        let request = reader.request().unwrap();
                        let n = count.fetch_add(1, Ordering::Relaxed);
                        assert_eq!(
                            request.op(),
                            if n == 3 { Op::DeleteStream } else { Op::SoftDeleteStream }
                        );
                        let mut response = if n == 0 {
                            Resp::error(Status::Internal, "injected failure")
                        } else {
                            Resp::ok()
                        };
                        response.id = request.id;
                        let mut bytes = writer.new_buffer();
                        common::put_response(&mut bytes, &response).unwrap();
                        writer.send(&mut bytes).unwrap();
                    }
                }));
            }
            for worker in workers {
                worker.join().unwrap();
            }
        });
        let soft = Delete {
            target: Target::Object("/retry".into()),
            retention: Some(TimeIntSec64::MIN),
            hard: false,
        }
        .request()
        .unwrap();
        assert_eq!(dn.delete_object(&process, &soft).status, Status::Internal);
        {
            let table = dn.stream_table.read().unwrap();
            assert_eq!(counts(&dn, &table[0]).0, 0);
            assert_eq!(counts(&dn, &table[0]).1, 2);
        }
        assert!(dn.object_table.read().unwrap().contains_key("/retry"));
        assert_eq!(dn.delete_object(&process, &soft).status, Status::Ok);
        assert_eq!(counts(&dn, &dn.stream_table.read().unwrap()[0]).1, 2);
        let hard = Delete { target: Target::Object("/retry".into()), retention: None, hard: true }
            .request()
            .unwrap();
        assert_eq!(dn.delete_object(&process, &hard).status, Status::Ok);
        assert_eq!(dn.delete_object(&process, &hard).status, Status::Ok);
        assert!(!dn.object_table.read().unwrap().contains_key("/retry"));
        let table = dn.stream_table.read().unwrap();
        assert_eq!(counts(&dn, &table[0]).1, 0);
        assert!(table[0].deletion.retired());
        peer.join().unwrap();
        assert_eq!(seen.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn live_and_retained_stream_counters_are_independent() {
        use common::time::TimeIntSec64;
        let mut stream = held(0, 0, Some(1));
        let key = (stream.id, stream.created);
        let mut extras = SharedExtras::default();
        assert_eq!(extras.counts(&key, stream.deletion), (1, 1));
        assert!(!stream.deletion.extra());
        extras.retain(key, &mut stream.deletion).unwrap();
        assert!(stream.deletion.extra());
        assert!(
            !extras
                .release_soft(
                    key,
                    &mut stream.deletion,
                    TimeSec::new(10),
                    TimeIntSec64::MIN,
                    TimeIntSec64::new(30)
                )
                .unwrap()
        );
        assert!(!extras.release_hard(&key, &mut stream.deletion).unwrap());
        assert!(!stream.deletion.extra());
        assert!(extras.get(&key).is_none());
        assert_eq!(extras.counts(&key, stream.deletion), (1, 1));
        assert!(
            extras
                .release_soft(
                    key,
                    &mut stream.deletion,
                    TimeSec::new(10),
                    TimeIntSec64::MIN,
                    TimeIntSec64::new(30)
                )
                .unwrap()
        );
        assert_eq!(extras.counts(&key, stream.deletion), (0, 1));
        assert!(extras.retain(key, &mut stream.deletion).is_err());
        assert!(extras.release_hard(&key, &mut stream.deletion).unwrap());
        assert_eq!(extras.counts(&key, stream.deletion), (0, 0));
    }
    #[test]
    fn shared_stream_metadata_and_reference_rollback() {
        let dn = DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST);
        let id = dn.reserve_stream().unwrap();
        assert_eq!(id, StreamId::FIRST);
        dn.install_stream(held(id.get(), 0, Some(12)));
        dn.retain_streams(&[id, id]).unwrap();
        {
            let mut table = dn.stream_table.write().unwrap();
            assert_eq!(table.len(), 1);
            assert_eq!(counts(&dn, &table[0]).1, 3);
            table[0].learned(Totals {
                extents: ExtentId::new(2),
                logical: SLoff::new(100),
                compressed: SCoff::new(80),
            });
        }
        let mut first = grown();
        first.streams = vec![id];
        let mut second = grown();
        second.streams = vec![id, id];
        let table = dn.stream_table.read().unwrap();
        assert_eq!(first.logical(&table), OLoff::new(112));
        assert_eq!(second.logical(&table), OLoff::new(224));
        drop(table);
        // A failed concat gives back every repeated reference it acquired.
        dn.object_table.write().unwrap().insert(Box::from("/failed"), second);
        dn.fail_concat("/failed", Resp::error(Status::Internal, "test"));
        assert_eq!(counts(&dn, &dn.stream_table.read().unwrap()[0]).1, 1);
    }

    #[test]
    fn stream_reference_counts_are_atomic_and_checked() {
        let stream = common::deletion::ReferenceCounts::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let stream = &stream;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        stream.retain().unwrap();
                        assert!(!stream.release().unwrap());
                    }
                });
            }
        });
        assert_eq!(stream.hard.load(Ordering::Relaxed), 1);
        assert!(stream.release().unwrap());
        assert!(stream.release().is_err());
        stream.hard.store(u32::MAX, Ordering::Relaxed);
        assert!(stream.retain().is_err());
        assert_eq!(stream.hard.load(Ordering::Relaxed), u32::MAX);

        let dn = DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST);
        dn.reserve_streams(2).unwrap();
        dn.install_stream(held(0, 0, None));
        dn.install_stream(held(1, 0, None));
        dn.retain_streams(&[StreamId::new(1)]).unwrap();
        dn.stream_extras
            .lock()
            .unwrap()
            .get(&(StreamId::new(1), TimeSec::new(1_000_000)))
            .unwrap()
            .references
            .hard
            .store(u32::MAX, Ordering::Relaxed);
        assert!(dn.retain_streams(&[StreamId::new(0), StreamId::new(1)]).is_err());
        assert_eq!(counts(&dn, &dn.stream_table.read().unwrap()[0]).1, 1);
    }
    #[test]
    fn object_summaries_sum_streams_in_object_spaces() {
        let table: Vec<Stream> = (0..2)
            .map(|n| Stream {
                extents: ExtentId::MAX,
                logical: SLoff::new((1 << 32) + 17),
                compressed: SCoff::new((1 << 32) + 3),
                ..held(n, 0, Some(1))
            })
            .collect();
        let object = Object {
            deletion: Flags::default(),
            purge_only: false,
            id: ObjectId::FIRST,
            framing: Framing::Lf,
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: TimeSec::new(100),
            streams: vec![StreamId::new(0), StreamId::new(1)],
        };
        let extents: OEoff = object.cached_extents(&table);
        let logical: OLoff = object.logical(&table);
        let compressed: OCoff = object.compressed(&table);
        assert_eq!(extents, OEoff::new(2 * ExtentId::MAX.number()));
        assert_eq!(logical, OLoff::new(2 * ((1 << 32) + 18)));
        assert_eq!(compressed, OCoff::new(2 * ((1 << 32) + 3)));
        let described = detailed(
            &object,
            &table,
            RangeId::FIRST,
            DnId::FIRST,
            &Want::plain().with(Detail::Counts),
            100,
        )
        .unwrap();
        let (shape, _) = Shape::of_args(&described.shape.args()).unwrap();
        assert_eq!(shape.streams, StreamId::new(2));
        assert_eq!(
            (shape.extents, shape.logical, shape.compressed),
            (extents, logical, compressed)
        );
    }

    #[test]
    fn rollover_reads_a_32_bit_stream_id_and_a_64_bit_record_count() {
        let stream = StreamId::new(0x1234_5678);
        let records = SRoff::new((1 << 32) + 20);
        let mut payload = StructWriter::new();
        payload.text("/x");
        payload.stream_id(stream);
        payload.sroff(records);
        let request = Req::new(Op::ExtendObject).structure(payload);
        assert_eq!(extension(&request).unwrap(), ("/x".to_string(), stream, records));
    }

    #[test]
    fn locate_stream_refuses_a_replaced_object() {
        let dn = Arc::new(DN::new(testing::dn_config(), DnId::FIRST, RangeId::FIRST));
        dn.object_table.write().unwrap().insert(
            Box::from("/x"),
            Object {
                deletion: Flags::default(),
                purge_only: false,
                id: ObjectId::new(2),
                framing: Framing::Lf,
                streams: vec![install(&dn, held(0, 0, None))],
                extending: false,
                waiting: Vec::new(),
                keyed: false,
                created: TimeSec::new(100),
            },
        );
        let mut payload = StructWriter::new();
        payload.text("/x");
        payload.oroff(ORoff::ZERO);
        Expecting::of(ObjectName {
            range: RangeId::FIRST,
            object: ObjectId::new(1),
            created: TimeSec::new(100),
        })
        .write(&mut payload);
        let (wire, mut reader) = testing::link();
        dn.handle(
            &testing::process(),
            Req::new(Op::LocateStream).structure(payload),
            Reply::new_test(Arc::new(common::server::Out::direct(wire))),
        );
        assert_eq!(reader.response().unwrap().status, Status::Replaced);
    }

    /// ⚠️ **The property a path cannot supply and a name must** (§17.35.2). A user may replace an
    /// object many times a second, so two incarnations at one path share a creation second — and
    /// they must still be told apart. The id is what does it, minted from a counter rather than
    /// read from a clock.
    #[test]
    fn two_objects_at_one_path_in_one_second_have_different_names() {
        let dn = Arc::new(DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0)));
        let made = TimeSec::new(1_000_000);
        let name_of = |dn: &DN| {
            let objects = dn.object_table.read().unwrap();
            let object = objects.get("/p").expect("an object");
            ObjectName { range: dn.mine, object: object.id, created: object.created }
        };
        let put = |dn: &DN| {
            dn.object_table.write().unwrap().insert(
                Box::from("/p"),
                Object {
                    deletion: Flags::default(),
                    purge_only: false,
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
    fn held(id: u32, sn: u64, records: Option<u64>) -> Stream {
        Stream {
            records: records.map(SRoff::new).unwrap_or(SRoff::NONE),
            ..Stream::new(StreamId::new(id), SnId::new(sn), TimeSec::new(1_000_000))
        }
    }

    fn install(dn: &DN, stream: Stream) -> StreamId {
        let id = stream.id;
        let len = dn.stream_table.read().unwrap().len();
        if len <= id.get() as usize {
            dn.reserve_streams(id.get() as usize + 1 - len).unwrap();
        }
        dn.install_stream(stream)
    }

    fn grown_streams() -> Vec<Stream> {
        vec![held(0, 0, Some(0)), held(1, 0, Some(100)), held(2, 1, Some(50)), held(3, 2, None)]
    }

    use std::mem::size_of;

    #[test]
    fn stream_references_and_counts_are_four_bytes() {
        assert_eq!(size_of::<StreamId>(), 4);
        assert_eq!(size_of::<Stream>(), 56);
        assert_eq!(size_of::<AtomicU32>(), 4);
    }

    #[test]
    fn stream_pool_uses_stream_id_as_slot_and_reclaims_after_gc() {
        let mut table = StreamTable::new().unwrap();
        let id = StreamId::new(5);
        table.install(Stream::new(id, SnId::new(2), TimeSec::new(10))).unwrap();
        assert_eq!(table.len(), 6);
        assert_eq!(table[id.get() as usize].id, id);
        assert!(table[0].deletion.retired(), "skipped IDs start vacant");

        table[id.get() as usize].deletion = Flags::vacant();
        table.retire(id);
        assert!(!table.can_reserve(id), "deleted slots wait for the global GC pass");
        table.reclaim();
        assert!(table.can_reserve(id));
        table.install(Stream::new(id, SnId::new(3), TimeSec::new(11))).unwrap();
        assert_eq!(table[id.get() as usize].sn, SnId::new(3));
        assert_eq!(table[id.get() as usize].created, TimeSec::new(11));
    }

    use super::*;
    use common::info::{Shape, StreamRow, UNKNOWN};
    use common::range::Start;
    use common::testing;

    /// A temporary object whose stream list has not been attached yet, as can
    /// occur while a concat destination is being built.
    fn half_made(dn: &DN, name: &str) {
        dn.object_table.write().unwrap().insert(
            Box::from(name),
            Object {
                deletion: Flags::default(),
                purge_only: false,
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

    /// An object without a DN stream reference is incomplete and cannot be described.
    #[test]
    fn object_without_stream_is_incomplete_in_dn_only_reads() {
        let dn = DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0));
        half_made(&dn, "/half");
        assert!(dn.look("/half").is_none(), "nothing to say about it yet");
        let waits = |name: &str| {
            dn.object_table.read().unwrap().get(name).is_some_and(|object| object.between_streams())
        };
        assert!(waits("/half"), "but the name is taken, so a lookup waits");
        assert!(!waits("/nothing"), "unlike a name nobody has");

        // Once the stream is named it describes normally.
        dn.object_table
            .write()
            .unwrap()
            .get_mut("/half")
            .unwrap()
            .streams
            .push(install(&dn, held(7, 0, None)));
        let args = dn.look("/half").expect("an object");
        // ⚠️ Read through the shape rather than by index: the stream is a whole name now
        // (#109), so its id is no longer argument one, and an index would have gone on
        // asserting about whatever moved into that slot.
        let (described, _) = Described::of_args(&args).expect("a row");
        assert_eq!(described.expect("an object").stream.stream, StreamId::new(7), "its stream");
        assert!(!waits("/half"), "and it is no longer half made");
    }

    /// Accepted collisions keep and reuse the already-declared stream identity.
    #[test]
    fn accepted_create_collision_reuses_existing_stream() {
        let dn = DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0));
        let original = DnCreateRow {
            name: "/same".into(),
            id: ObjectId::new(0),
            keyed: false,
            framing: Framing::Lf,
            created: TimeSec::new(500),
            stream: StreamId::new(0),
            sn: SnId::new(1),
        };
        assert!(
            dn.apply_create(DnCreateCmd {
                rows: vec![original.clone()],
                flags: AppendFlags::default(),
            })
            .unwrap()
            .is_none()
        );

        let candidate = DnCreateRow {
            id: ObjectId::new(1),
            stream: StreamId::new(1),
            created: TimeSec::new(501),
            ..original.clone()
        };
        assert!(
            dn.apply_create(DnCreateCmd {
                rows: vec![candidate.clone()],
                flags: AppendFlags::default(),
            })
            .unwrap()
            .is_none()
        );
        assert_eq!(dn.objects.load(Ordering::Relaxed), 1);
        assert_eq!(dn.stream_table.read().unwrap().len(), 1);
        assert_eq!(dn.object_table.read().unwrap()["/same"].id, original.id);

        let wanted = [Wanted { name: "/same".into(), keyed: false, framing: Framing::Lf }];
        let (by_sn, made) = dn.create_targets(&wanted, &[candidate]).unwrap();
        assert_eq!(made, 0);
        assert_eq!(
            by_sn[&SnId::new(1)],
            [StreamName { range: dn.mine, stream: original.stream, created: original.created }]
        );
    }

    /// An object grows streams, and the record ranges are what make its numbering
    /// continuous across them. This is the arithmetic the DN answers reads with.
    #[test]
    fn an_objects_streams_carry_on_from_one_another() {
        let table =
            vec![held(0, 0, Some(0)), held(1, 0, Some(10)), held(2, 0, Some(5)), held(3, 0, None)];
        let mut object = Object {
            deletion: Flags::default(),
            purge_only: false,
            id: ObjectId::new(1),
            framing: Framing::Lf,
            streams: Vec::new(),
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: TimeSec::new(1_000_000),
        };
        // Two sealed streams and one still open.
        object.streams.push(StreamId::new(1));
        object.streams.push(StreamId::new(2));
        object.streams.push(StreamId::new(3));

        assert_eq!(object.holding(&table, ORoff::new(0)).unwrap().1.id, StreamId::new(1));
        assert_eq!(object.holding(&table, ORoff::new(9)).unwrap().1.id, StreamId::new(1));
        assert_eq!(object.holding(&table, ORoff::new(10)).unwrap().1.id, StreamId::new(2));
        assert_eq!(object.holding(&table, ORoff::new(14)).unwrap().1.id, StreamId::new(2));
        assert_eq!(object.holding(&table, ORoff::new(15)).unwrap().1.id, StreamId::new(3));
        // Past everything sealed goes to the open stream, whose reach is not known
        // here — the SN and EN say whether the record is really there.
        assert_eq!(object.holding(&table, ORoff::new(1000)).unwrap().1.id, StreamId::new(3));
        // And an append always goes to the last.
        assert_eq!(object.last(&table).unwrap().1.id, StreamId::new(3));
        assert!(!object.between_streams());
    }

    /// An object with three streams, two sealed and one still growing.
    fn grown() -> Object {
        let mut object = Object {
            deletion: Flags::default(),
            purge_only: false,
            id: ObjectId::new(1),
            framing: Framing::Lf,
            streams: Vec::new(),
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: TimeSec::new(1_000_000),
        };
        object.streams.push(StreamId::new(1));
        object.streams.push(StreamId::new(2));
        object.streams.push(StreamId::new(3));
        object
    }

    /// What a caller sees, with no SN asked — so every stream row says `given` zero.
    fn flat(levelled: Levelled) -> Vec<Arg> {
        levelled.args()
    }

    fn read_back(args: &[Arg]) -> (Shape, Vec<StreamRow>) {
        let (shape, rest) = Shape::of_args(args).expect("a shape");
        let (_, rest) = Described::of_args(rest).expect("an object");
        let (rows, rest) = StreamRow::some_of_args(rest, shape.given.number()).expect("streams");
        assert!(rest.is_empty(), "and nothing left over");
        (shape, rows)
    }

    fn scoped_summary(base: ORoff) -> (Levelled, ExtentRun, ExtentRow) {
        let want = Want::plain().with(Detail::Streams).with(Detail::Counts);
        let mut summary =
            detailed(&grown(), &grown_streams(), RangeId::new(0), DnId::new(0), &want, 100)
                .unwrap();
        summary.rows.truncate(1);
        summary.rows[0].first_record = base;
        summary.shape.given = StreamId::new(1);
        summary.shape.entries = 2;
        let run = ExtentRun {
            stream: summary.rows[0].stream,
            totals: common::info::Totals {
                extents: ExtentId::new(2),
                logical: SLoff::new(100),
                compressed: SCoff::new(80),
            },
            given: ExtentId::new(1),
            next: SRoff::new((1 << 32) + 20),
        };
        let extent = ExtentRow {
            sn: summary.rows[0].sn,
            id: ExtentId::new(3),
            created: TimeSec::new(100),
            first_record: SRoff::new((1 << 32) + 7),
            records: common::offset::ERoff::new(13),
            logical: common::offset::ELoff::new(100),
            length: common::offset::ECoff::new(80),
            disks: vec![],
        };
        (summary, run, extent)
    }

    #[test]
    fn listing_positions_preserve_each_occurrence_and_order() {
        let base = ORoff::new(1 << 40);
        let (summary, _, _) = scoped_summary(base);
        let mut rows = vec![summary.rows[0].clone(); 4];
        rows[1].first_record = base.plus(ORoff::new(43)).unwrap();
        rows[2].sn = SnId::new(99);
        let from = base.plus(ORoff::new(50)).unwrap();
        let groups = extent_asks(RangeId::FIRST, &rows, from);
        let asked = &groups[&rows[0].sn];
        assert_eq!(asked.iter().map(|ask| ask.row).collect::<Vec<_>>(), vec![0, 1, 3]);
        assert_eq!(
            asked.iter().map(|ask| ask.from).collect::<Vec<_>>(),
            vec![SRoff::new(50), SRoff::new(7), SRoff::new(50)]
        );
        assert_eq!(groups[&SnId::new(99)][0].row, 2);
        assert_eq!(stream_start(ORoff::ZERO, base), SRoff::ZERO);
        let request = ask_extents(asked, 3, true);
        let (mut reader, _) = structure(&request.args(), Op::ListExtents, "listing").unwrap();
        let listing = Listing::read_payload(&mut reader).unwrap();
        reader.done().unwrap();
        assert_eq!(listing.limit, 3);
        assert!(listing.instances);
        assert_eq!(listing.at.len(), 3, "identical requests are not combined");
        assert_eq!(listing.at[0], listing.at[2]);
        let mut encoded = common::StructWriter::new();
        listing.write_payload(&mut encoded);
        let args = [encoded.finish(Op::ListExtents)];
        let (mut reader, _) = structure(&args, Op::ListExtents, "listing").unwrap();
        assert_eq!(Listing::read_payload(&mut reader).unwrap(), listing);
        reader.done().unwrap();
    }

    fn summary_contents(args: &[Arg]) -> (Shape, Vec<(StreamRow, Vec<ExtentRow>)>) {
        let (shape, rest) = Shape::of_args(args).unwrap();
        let (_, mut rest) = Described::of_args(rest).unwrap();
        let mut rows = Vec::new();
        for _ in 0..shape.given.number() {
            let (row, after) = StreamRow::of_args(rest).unwrap();
            let (extents, after) = ExtentRow::some_of_args(after, row.given.number()).unwrap();
            rows.push((row, extents));
            rest = after;
        }
        assert!(rest.is_empty());
        (shape, rows)
    }

    #[test]
    fn replies_fill_original_slots_without_unifying_repeated_streams() {
        let base = ORoff::new(1 << 40);
        let (summary, run, extent) = scoped_summary(base);
        let mut rows = vec![summary.rows[0].clone(); 3];
        rows[1].sn = SnId::new(99);
        rows[2].first_record = base.plus(ORoff::new(40)).unwrap();
        let groups = extent_asks(RangeId::FIRST, &rows, base.plus(ORoff::new(50)).unwrap());
        let mut slots = vec![None; 3];
        let answer = |starts: &[u64]| {
            let mut args = Vec::new();
            for &start in starts {
                args.extend(run.args());
                args.extend(ExtentRow { first_record: SRoff::new(start), ..extent.clone() }.args());
            }
            args
        };
        // The second SN finishes first. The first and third requests have the same stream ID.
        receive_extents(&mut slots, &groups[&SnId::new(99)], &answer(&[50])).unwrap();
        assert!(slots[0].is_none() && slots[2].is_none());
        receive_extents(&mut slots, &groups[&rows[0].sn], &answer(&[50, 10])).unwrap();
        let starts: Vec<_> =
            slots.iter().map(|slot| slot.as_ref().unwrap().1[0].first_record).collect();
        assert_eq!(starts, vec![SRoff::new(50), SRoff::new(50), SRoff::new(10)]);
        assert!(
            receive_extents(&mut slots, &groups[&rows[0].sn], &answer(&[50])).is_err(),
            "missing replies must not silently reuse another occurrence"
        );
    }

    #[test]
    fn detailed_reads_report_an_sn_stream_mismatch_as_pending() {
        let name = StreamName {
            range: RangeId::new(4),
            stream: StreamId::new(7),
            created: TimeSec::new(500),
        };
        let asked = [ExtentAsk { row: 0, name, from: SRoff::ZERO }];
        let answer =
            pending_stream_answer(Resp::error(Status::NotFound, format!("no {name}")), &asked);
        assert_eq!(answer.status, Status::NotFound);
        let expected = format!("pending: DN declares {name}, but SN has not created it");
        assert_eq!(answer.message(), Some(expected));

        let deleted = Resp::error(Status::NotFound, "stream is deleted");
        assert_eq!(pending_stream_answer(deleted.clone(), &asked), deleted);
    }

    #[test]
    fn combined_sn_responses_are_trimmed_to_one_object_budget() {
        let base = ORoff::new(1 << 40);
        let (mut summary, mut run, extent) = scoped_summary(base);
        run.next = SRoff::NONE;
        let mut second = summary.rows[0].clone();
        second.sn = SnId::new(99);
        second.stream = StreamId::new(99);
        second.first_record = base.plus(ORoff::new(1 << 35)).unwrap();
        let next = second.first_record;
        let other = ExtentRun { stream: second.stream, ..run.clone() };
        summary.rows.push(second);
        summary.shape.given = StreamId::new(2);
        summary.shape.entries = 3;
        let extents = vec![ExtentRow { first_record: SRoff::ZERO, ..extent.clone() }, extent];
        let replies = vec![Some((run, extents.clone())), Some((other, extents))];
        let args = merge_object_info(summary, &replies, 4);
        let (shape, rows) = summary_contents(&args);
        assert_eq!(shape.entries, 4);
        assert_eq!(shape.extents, OEoff::new(4), "retain SN counts, not the two returned rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.len(), 2);
        assert_eq!(shape.next, next);
    }

    #[test]
    fn shared_streams_are_reported_for_every_reference_and_trimmed_after_collection() {
        let base = ORoff::new(1 << 40);
        let (mut summary, mut run, extent) = scoped_summary(base);
        run.next = SRoff::NONE;
        run.totals.extents = ExtentId::new(1);
        let mut second = summary.rows[0].clone();
        second.first_record = base.plus(ORoff::new(1 << 35)).unwrap();
        summary.rows.push(second);
        summary.shape.given = StreamId::new(2);
        summary.shape.entries = 3;
        let runs = vec![Some((run, vec![extent.clone()]))];
        let runs = vec![runs[0].clone(), runs[0].clone()];
        let full = merge_object_info(summary, &runs, 100);
        let (shape, rows) = summary_contents(&full);
        assert_eq!(shape.extents, OEoff::new(2));
        assert_eq!(shape.entries, 5);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, vec![visible_extent(&extent)]);
        assert_eq!(rows[1].1, vec![visible_extent(&extent)]);
        let trimmed = common::info::limit_object_info(&full, 4).unwrap();
        let (shape, rows) = summary_contents(&trimmed);
        assert_eq!(shape.entries, 4);
        assert_eq!(shape.extents, OEoff::new(2), "counts survive trimming");
        assert_eq!(rows[1].1.len(), 0);
        assert_eq!(
            shape.next,
            rows[1].0.first_record.plus(ORoff::from_stream(extent.first_record)).unwrap()
        );
    }

    #[test]
    fn resumed_summary_counts_all_extents_but_reports_only_the_requested_suffix() {
        let base = ORoff::new(1 << 40);
        let (summary, mut run, extent) = scoped_summary(base);
        run.next = SRoff::NONE;
        let from = base.plus(ORoff::from_stream(extent.first_record)).unwrap();
        let groups = extent_asks(RangeId::FIRST, &summary.rows, from);
        assert_eq!(groups[&summary.rows[0].sn][0].from, extent.first_record);
        let runs = vec![Some((run, vec![extent.clone()]))];
        let args = merge_object_info(summary, &runs, 3);
        let (shape, rows) = summary_contents(&args);
        assert_eq!(shape.extents, OEoff::new(2));
        assert_eq!(shape.entries, 3);
        assert_eq!(rows[0].1, vec![visible_extent(&extent)]);
        assert_eq!(shape.next, ORoff::NONE);
    }

    #[test]
    fn batch_repeats_shared_summaries_and_uses_sn_counts_even_without_room_for_extents() {
        let base = ORoff::new(1 << 40);
        let (summary, mut run, extent) = scoped_summary(base);
        run.next = SRoff::NONE;
        run.totals.extents = ExtentId::new(1);
        let piece = flat(summary);
        let mut args = Batch { asked: 2, given: 2, entries: 4, next: UNKNOWN }.args();
        args.extend(piece.clone());
        args.extend(piece);
        let runs = vec![Some((run, vec![extent.clone()]))];
        let runs = vec![runs[0].clone(), runs[0].clone()];
        let response = merge_batch_info(args.clone(), &runs, 6);
        assert_eq!(response.status, Status::Ok);
        let (batch, rest) = Batch::of_args(response.args()).unwrap();
        assert_eq!((batch.given, batch.entries, batch.next), (2, 6, UNKNOWN));
        for piece in Batch::pieces(rest, batch.given).unwrap() {
            let (shape, rows) = summary_contents(piece);
            assert_eq!(shape.extents, OEoff::new(1));
            assert_eq!(rows[0].1, vec![visible_extent(&extent)]);
        }
        let response = merge_batch_info(args, &runs, 2);
        let (batch, rest) = Batch::of_args(response.args()).unwrap();
        assert_eq!((batch.given, batch.entries, batch.next), (1, 2, 0));
        let (shape, rows) = summary_contents(rest);
        assert_eq!(shape.extents, OEoff::new(1));
        assert!(rows[0].1.is_empty());
        assert_eq!(shape.next, base.plus(ORoff::from_stream(extent.first_record)).unwrap());
    }

    fn visible_extent(extent: &ExtentRow) -> ExtentRow {
        ExtentRow {
            logical: common::offset::ELoff::of_number(
                extent.logical.number() + extent.records.number(),
            )
            .unwrap(),
            ..extent.clone()
        }
    }

    fn assert_scoped_summary(args: &[Arg], base: ORoff, expected: &ExtentRow) -> Shape {
        let (shape, rest) = Shape::of_args(args).unwrap();
        let (_, rest) = Described::of_args(rest).unwrap();
        let (stream, rest) = StreamRow::of_args(rest).unwrap();
        let (extent, rest) = ExtentRow::of_args(rest).unwrap();
        assert!(rest.is_empty());
        assert_eq!(stream.first_record, base);
        assert_eq!(
            extent,
            visible_extent(expected),
            "DN preserves extent coordinates and translates the size to LF framing"
        );
        shape
    }

    #[test]
    fn object_summary_keeps_extent_positions_stream_local_but_rebases_continuation() {
        let base = ORoff::new(1 << 40);
        let (summary, run, extent) = scoped_summary(base);
        let next = base.plus(ORoff::from_stream(run.next)).unwrap();
        let runs = vec![Some((run, vec![extent.clone()]))];
        let args = merge_object_info(summary, &runs, 100);
        let shape = assert_scoped_summary(&args, base, &extent);
        assert_eq!(shape.next, next, "object continuation still includes the stream base");
    }

    #[test]
    fn batch_summary_keeps_extent_positions_stream_local_for_each_object() {
        let mut body = Vec::new();
        let mut runs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..2 {
            let base = ORoff::new((1 << 40) + i * 1000);
            let (mut summary, mut run, extent) = scoped_summary(base);
            run.totals.extents = ExtentId::new(1);
            run.next = SRoff::NONE;
            run.stream = StreamId::new(i as u32);
            summary.rows[0].stream = run.stream;
            body.extend(flat(summary));
            runs.push(Some((run, vec![extent.clone()])));
            expected.push((base, extent));
        }
        let mut args = Batch { asked: 2, given: 2, entries: 4, next: UNKNOWN }.args();
        args.extend(body);
        let response = merge_batch_info(args, &runs, 100);
        assert_eq!(response.status, Status::Ok);
        let (batch, rest) = Batch::of_args(response.args()).unwrap();
        let pieces = Batch::pieces(rest, batch.given).unwrap();
        assert_eq!(pieces.len(), expected.len());
        for (piece, (base, extent)) in pieces.iter().zip(expected) {
            assert_scoped_summary(piece, base, &extent);
        }
    }

    /// Wanting nothing asks for no levels at all, so the caller gets the row that
    /// predates the field — which is what keeps every existing caller and §17.5g's
    /// baseline.
    #[test]
    fn wanting_nothing_answers_the_bare_row() {
        assert!(
            detailed(
                &grown(),
                &grown_streams(),
                RangeId::new(0),
                DnId::new(0),
                &Want::plain(),
                100
            )
            .is_none(),
            "nothing levelled to build"
        );
        let object = grown();
        match describe_at(
            &object,
            &grown_streams(),
            RangeId::new(0),
            DnId::new(0),
            None,
            &Want::plain(),
            100,
        )
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
            detailed(
                &grown(),
                &grown_streams(),
                RangeId::new(0),
                DnId::new(0),
                &Want { limit: 2, ..want },
                100,
            )
            .expect("an answer"),
        );
        let (shape, rows) = read_back(&args);
        assert_eq!(shape.streams, StreamId::new(3), "how many there are");
        assert_eq!((shape.given, rows.len()), (StreamId::new(1), 1), "how many are described");
        assert_eq!(shape.entries, 2, "the object and one stream");
        assert_eq!(shape.next, ORoff::new(100), "and where to carry on");
        assert!(shape.more());
    }

    /// ⚠️ The continuation is a record number, so resuming from it describes the next
    /// stream and not the same one again — §9.1.3's whole scheme in one assertion.
    #[test]
    fn resuming_from_the_continuation_makes_progress() {
        let want = Want::plain().with(Detail::Streams).with(Detail::Counts);
        let object = grown();
        let mut at = ORoff::ZERO;
        let mut seen = Vec::new();
        for _ in 0..8 {
            let args = flat(
                detailed(
                    &object,
                    &grown_streams(),
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
                    &grown_streams(),
                    RangeId::new(0),
                    DnId::new(0),
                    &Want { limit: asked, ..want },
                    configured,
                )
                .expect("an answer"),
            );
            read_back(&args).0.given.number()
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
            detailed(
                &grown(),
                &grown_streams(),
                RangeId::new(0),
                DnId::new(0),
                &Want { limit: 100, ..want },
                100,
            )
            .expect("an answer"),
        );
        let (_, rows) = read_back(&args);
        assert_eq!(rows[0].records, SRoff::new(100));
        assert_eq!(rows[2].records, SRoff::NONE, "the one being appended to");
        assert_ne!(rows[2].records, SRoff::ZERO, "which is not a count of none");
    }

    /// Counts without streams is a legitimate question: how big is this object,
    /// without the rows. ⚠️ And it still says where the rows would start — see §17.24.2
    /// for why saying `nothing more` there was wrong.
    #[test]
    fn counts_alone_bring_no_rows_but_say_where_rows_start() {
        let want = Want::plain().with(Detail::Counts);
        let args = flat(
            detailed(&grown(), &grown_streams(), RangeId::new(0), DnId::new(0), &want, 100)
                .expect("an answer"),
        );
        let (shape, rows) = read_back(&args);
        assert_eq!(
            (shape.streams, shape.given, shape.entries),
            (StreamId::new(3), StreamId::FIRST, 1)
        );
        assert!(rows.is_empty());
        assert!(shape.more(), "three streams it did not describe is not nothing more");
        assert_eq!(shape.next, ORoff::ZERO, "and a listing would start at the first of them");
    }

    /// A DN holding `count` objects named `/n0`, `/n1`, and so on.
    fn with_objects(count: u64) -> Arc<DN> {
        let dn = Arc::new(DN::new(testing::dn_config(), DnId::new(0), RangeId::new(0)));
        for n in 0..count {
            let name = format!("/n{n}");
            dn.object_table.write().unwrap().insert(
                Box::from(name.as_str()),
                Object {
                    deletion: Flags::default(),
                    purge_only: false,
                    id: ObjectId::new(n as u32 + 1),
                    framing: Framing::Lf,
                    streams: vec![install(&dn, held(n as u32 + 1, 0, None))],
                    extending: false,
                    waiting: Vec::new(),
                    keyed: false,
                    created: TimeSec::new(1_000_000),
                },
            );
        }
        dn
    }

    fn batch_of(dn: &Arc<DN>, names: &[&str], want: Option<Want>) -> Resp {
        let mut payload = StructWriter::new();
        payload.number_u64(names.len() as u64);
        for name in names {
            payload.text(name);
        }
        if let Some(want) = want {
            payload.flag(true);
            want.write(&mut payload);
        } else {
            payload.flag(false);
        }
        let request = Req::new(Op::BatchInfo).structure(payload);
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

        let (covered, rest) = Batch::of_args(answer.args()).expect("a batch header");
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
        let (covered, rest) = Batch::of_args(answer.args()).expect("a batch header");
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
        let (covered, rest) = Batch::of_args(answer.args()).expect("a batch header");
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
        let (covered, _) = Batch::of_args(answer.args()).expect("a batch header");
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
        let (covered, _) = Batch::of_args(answer.args()).expect("a batch header");
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
        let (covered, rows) = Batch::of_args(answer.args()).expect("a batch header");
        assert_eq!(covered.given, 3, "a miss is answered, not skipped");
        let pieces = Batch::pieces(rows, covered.given).expect("one slice per name");
        assert_eq!(pieces.len(), 3);
        // The middle one is the miss: a shape saying nothing, then a row saying nothing.
        let (shape, rest) = Shape::of_args(pieces[1]).expect("a shape");
        assert_eq!(
            (shape.streams, shape.given, shape.entries),
            (StreamId::FIRST, StreamId::FIRST, 1)
        );
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
        let (covered, _) = Batch::of_args(answer.args()).expect("a batch header");
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
        let wanted = Wanted { name: "/x".into(), keyed: true, framing: Framing::CrLf };
        let mut payload = StructWriter::new();
        wanted.write(&mut payload);
        let request = Req::new(Op::CreateObject).structure(payload);
        let (mut reader, rest) = structure(&request.args(), Op::CreateObject, "a create").unwrap();
        assert!(rest.is_empty());
        let wanted = Wanted::read(&mut reader).expect("an object");
        assert_eq!((wanted.name.as_str(), wanted.keyed), ("/x", true));
        assert_eq!(wanted.framing, Framing::CrLf);

        let mut bad = StructWriter::new();
        bad.text("/x");
        bad.flag(false);
        bad.number_u8(kind + 100);
        bad.number_u32(width);
        let request = Req::new(Op::CreateObject).structure(bad);
        let (mut reader, rest) = structure(&request.args(), Op::CreateObject, "a create").unwrap();
        assert!(rest.is_empty());
        assert!(Wanted::read(&mut reader).is_err());
    }

    #[test]
    fn stream_create_request_lists_full_nonconsecutive_names() {
        let names = [
            StreamName {
                range: RangeId::new(7),
                stream: StreamId::new(2),
                created: TimeSec::new(11),
            },
            StreamName {
                range: RangeId::new(7),
                stream: StreamId::new(19),
                created: TimeSec::new(11),
            },
        ];
        let request = create_streams(&names);
        let (mut payload, rest) =
            structure(&request.args(), Op::CreateStream, "stream names").unwrap();
        assert!(rest.is_empty());
        assert_eq!(payload.number_u64().unwrap(), 2);
        let got = (0..2)
            .map(|_| StreamName {
                range: payload.range_id().unwrap(),
                stream: payload.stream_id().unwrap(),
                created: payload.time_sec().unwrap(),
            })
            .collect::<Vec<_>>();
        payload.done().unwrap();
        assert_eq!(got, names);
    }
}
