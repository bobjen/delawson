//! DN, the Directory Node: objects by name, and the streams they are made of.
//!
//! An object is indexed by name here, and it grows a stream at a time on the SNs
//! that record them — no inlining of small objects and no coalescing of several
//! objects into one stream, both of which come later and both of which would hide
//! what a create costs. DN owns the `StreamId`; the SN it picks records
//! `(DnId, StreamId)`.
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
use common::info::{Batch, Described, ExtentRow, ExtentRun, Listing, Shape, StreamRow, UNKNOWN};
use common::logw;
use common::range::{Detail, Want};
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
fn describe_at(object: &Object, record: Option<u64>, want: &Want, limit: u64) -> Option<Answering> {
    match record {
        Some(_) => object.args(record).map(Answering::Plain),
        None => match detailed(object, want, limit) {
            Some(levelled) => Some(Answering::Levelled(levelled)),
            // No detail asked for, so the row that predates the field, byte for byte.
            None => object.args(None).map(Answering::Plain),
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
fn detailed(object: &Object, want: &Want, limit: u64) -> Option<Levelled> {
    let args = object.args(None)?;
    if want.detail == 0 {
        return None;
    }

    let total = object.streams.len() as u64;
    let from = want.from.record().unwrap_or(0);
    // Streams are in record order, so the first to describe is the last one starting
    // at or before `from` — the same rule a read uses to pick a stream.
    let first = object.streams.iter().rposition(|held| held.first_record <= from).unwrap_or(0);

    // One entry for the object, then one per stream, against whichever cap is smaller.
    let cap = match want.limit {
        0 => limit,
        asked => asked.min(limit),
    };
    let room = cap.saturating_sub(1);
    let mut rows = Vec::new();
    if want.wants(Detail::Streams) {
        for held in object.streams.iter().skip(first).take(room as usize) {
            rows.push(StreamRow {
                stream: held.id,
                sn: held.sn,
                first_record: held.first_record,
                // ⚠️ UNKNOWN, not zero: how far the stream being appended to reaches is
                // not the DN's to say (§6.3), and a sealed stream may hold nothing.
                records: held.records.unwrap_or(UNKNOWN),
                extents: UNKNOWN,
                given: 0,
            });
        }
    }

    // Where to carry on: the first record of the stream after the last one described,
    // and nothing when the last stream described is the object's last.
    let after = first + rows.len();
    let next = object.streams.get(after).map_or(UNKNOWN, |held| held.first_record);
    let shape = Shape {
        streams: if want.wants(Detail::Counts) { total } else { UNKNOWN },
        given: rows.len() as u64,
        extents: UNKNOWN,
        entries: 1 + rows.len() as u64,
        next: if want.wants(Detail::Streams) { next } else { UNKNOWN },
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
    fn args(self, mut extents: HashMap<u64, (ExtentRun, Vec<ExtentRow>)>) -> Vec<Arg> {
        let mut out = self.shape.args();
        out.extend(self.head);
        for mut row in self.rows {
            match extents.remove(&row.stream) {
                Some((run, rows)) => {
                    row.extents = run.extents;
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
    id: u64,
    /// Where one record ends and the next begins, declared here and never
    /// changed: everything that reads or writes this object's records needs it.
    framing: Framing,
    /// The streams this object is made of, in order. Empty while the name is taken
    /// and the first stream is not named yet, which is what stops two creates of
    /// one name from making two streams.
    ///
    /// A stream fills, so an object grows more of them; the last is the one appends
    /// go to and the record ranges say which holds what.
    streams: Vec<Held>,
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
    created: u64,
}

/// What an `extend-object` request turned out to be: the one doing the work, one
/// waiting behind it, or one that can be answered on the spot.
enum Next {
    Ours(u64),
    Wait,
    Say(Response),
}

/// One of an object's streams, and where its records start.
#[derive(Clone, Copy)]
struct Held {
    id: u64,
    sn: u32,
    /// The object-local number of this stream's first record. A stream numbers its
    /// own records from zero; this is what makes an object's numbering continuous
    /// across the streams it is made of.
    first_record: u64,
    /// Records in it, once it is sealed. None while it is the one being appended to,
    /// because how far it reaches is not known here.
    records: Option<u64>,
}

impl Object {
    /// Nothing here can be appended to or read yet: the first stream is being made,
    /// or the next one is being asked for. Waiting is better than being told there is
    /// no stream, which is true for a moment and useless to hear.
    fn between_streams(&self) -> bool {
        self.streams.is_empty() || self.extending
    }

    /// What this object is, at one of its streams: the last for an append, or the one
    /// holding a record for a read.
    fn args(&self, record: Option<u64>) -> Option<Vec<Arg>> {
        let held = match record {
            None => self.last()?,
            Some(record) => self.holding(record)?,
        };
        Some(
            Described {
                id: self.id,
                stream: held.id,
                sn: held.sn,
                created: self.created,
                keyed: self.keyed,
                framing: self.framing,
                first_record: held.first_record,
            }
            .args(),
        )
    }

    /// The stream appends go to, which is the last one.
    fn last(&self) -> Option<Held> {
        self.streams.last().copied()
    }

    /// The stream holding an object-local record number: the sealed one whose range
    /// covers it, or the last, whose reach is not known here.
    fn holding(&self, record: u64) -> Option<Held> {
        for held in &self.streams {
            match held.records {
                Some(records) if record < held.first_record + records => return Some(*held),
                Some(_) => continue,
                // The stream still being appended to: anything past its start is
                // routed to it, and the SN and EN say whether it is really there.
                None => return Some(*held),
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
fn arrived(gather: &Arc<Mutex<Gather>>, answer: Response) {
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
    let mut extents = 0;
    let mut given = 0;
    let mut converted: HashMap<u64, (ExtentRun, Vec<ExtentRow>)> = HashMap::new();
    for row in &held.rows {
        let Some((run, rows)) = runs.remove(&row.stream) else { continue };
        extents += if run.extents == UNKNOWN { 0 } else { run.extents };
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
        let short = run.extents != UNKNOWN && (rows.len() as u64) < run.extents;
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
    runs: HashMap<u64, (ExtentRun, Vec<ExtentRow>)>,
    /// Set when any SN refused, so the whole answer fails rather than silently omitting extents
    /// the caller asked for.
    failed: Option<Response>,
    reply: Option<Reply>,
    /// What the object looked like when the asks went out, so the answer is built from one
    /// consistent view rather than re-read after the streams may have changed.
    head: Vec<Arg>,
    rows: Vec<StreamRow>,
    shape: Shape,
}

/// One object a create asks for: a name, a flavour, and a framing.
struct Wanted {
    name: String,
    keyed: bool,
    framing: Framing,
}

impl Wanted {
    /// Arguments per object, which is what a batch is cut into.
    const ARGS: usize = 4;

    fn of(args: &[Arg]) -> Result<Wanted> {
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
}

struct DN {
    config: DNConfig,
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

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
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
                | Op::LocateStream
                | Op::ExtendObject
        )
    }

    /// A lookup is a hash and a read, which is not worth a queue hop. Taking
    /// names for a create is, since a batch of a thousand takes a thousand locks.
    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::ObjectInfo
                | Op::BatchInfo
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
        let wanted = match Wanted::of(head) {
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
        let mut wanted = Vec::with_capacity(request.args.len() / Wanted::ARGS);
        for one in request.args.chunks(Wanted::ARGS) {
            match Wanted::of(one) {
                Ok(one) => wanted.push(one),
                Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
            }
        }
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
        let created = now();

        // Whoever inserts the name owns the object; anyone else finds it there.
        let mut mine = Vec::with_capacity(wanted.len());
        for one in &wanted {
            let mut shard = self.shards[shard_of(&one.name)].lock().expect("objects");
            if !shard.contains_key(one.name.as_str()) {
                let id = self.next_object.fetch_add(1, Ordering::Relaxed);
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
            .number(process.entry.number() as u64)
            .number(first)
            .number(count)
            .number(created);

        let dn = Arc::clone(self);
        let sn_number = process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0);
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
                    object.streams.push(Held {
                        id: first + n as u64,
                        sn: sn_number,
                        first_record: 0,
                        records: None,
                    });
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
                    let ours = object.streams.last().is_some_and(|last| last.id == sealed);
                    if let Some(last) = object.streams.last_mut().filter(|_| ours) {
                        last.records = Some(records);
                    }
                    if object.extending {
                        object.waiting.push(carry.take().expect("a reply"));
                        self.parked.fetch_add(1, Ordering::Relaxed);
                        Next::Wait
                    } else if ours {
                        object.extending = true;
                        let from = object.streams.last().map_or(0, |last| last.first_record);
                        Next::Ours(from + records)
                    } else {
                        // Somebody else extended past it already, so whatever is last
                        // now is the answer.
                        match object.args(None) {
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
        let created = now();
        let stream = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(e) => return reply.send(self.gave_up(&name, Status::Internal, e.to_string())),
        };
        let sn_number = process.cluster.service(&sn).map(|entry| entry.number()).unwrap_or(0);
        let ask = Request::new(Op::CreateStream)
            .number(process.entry.number() as u64)
            .number(stream)
            .number(1)
            .number(created);

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
                    object.streams.push(Held {
                        id: stream,
                        sn: sn_number,
                        first_record,
                        records: None,
                    });
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
                Some(object) => match describe_at(object, record, &want, limit) {
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
                self.spread(process, levelled, limit, reply)
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
        levelled: Levelled,
        limit: u64,
        reply: Reply,
    ) {
        // Group by SN, keeping each stream's record to start from.
        let mut by_sn: HashMap<u32, Vec<(u64, u64)>> = HashMap::new();
        for row in &levelled.rows {
            by_sn.entry(row.sn).or_default().push((row.stream, 0));
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
        }));

        for (sn, at) in by_sn {
            let identity = format!("SN{sn}");
            let ask = {
                let mut request = Request::new(Op::ListExtents);
                request.args = Listing { dn: process.entry.number(), limit: room, at }.args();
                request
            };
            let waiting = Arc::clone(&gather);
            let named = identity.clone();
            if let Err(e) = process.ask(&identity, &ask, move |answer| arrived(&waiting, answer)) {
                logw!("could not reach an SN", "sn {named},{e}");
                arrived(
                    &gather,
                    Response::error(Status::Internal, format!("could not reach {named}")),
                );
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
                    Some(object) => (std::mem::take(&mut object.waiting), object.args(None)),
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
            let cost = piece.as_ref().map_or(1, |(entries, _)| *entries);
            if given > 0 && spent + cost > limit {
                break;
            }
            match piece {
                Some((entries, mut of)) => {
                    spent += entries;
                    args.append(&mut of);
                }
                None => {
                    spent += 1;
                    args.extend(Described::missing());
                }
            }
            given += 1;
        }

        let covered = Batch {
            asked: names.len() as u64,
            given,
            entries: spent,
            next: if given as usize == names.len() { UNKNOWN } else { given },
        };
        let mut out = covered.args();
        out.append(&mut args);
        Response { id: 0, status: Status::Ok, args: out }
    }

    /// One name's worth of a batch answer, and what it costs in entries.
    ///
    /// ⚠️ **Takes the name's shard lock** — see `look`.
    fn piece(&self, name: &str, want: &Want, room: u64) -> Option<(u64, Vec<Arg>)> {
        let shard = self.shards[shard_of(name)].lock().expect("objects");
        let object = shard.get(name)?;
        if want.detail == 0 {
            return object.args(None).map(|args| (1, args));
        }
        // Room is what the limit has left, and an object costs at least its own entry, so a name
        // reached with no room to spare still gets its row and no streams.
        let levelled = detailed(object, want, room.max(1))?;
        let entries = levelled.shape.entries;
        Some((entries, levelled.args(HashMap::new())))
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
        self.shards[shard_of(name)].lock().expect("objects").get(name)?.args(record)
    }
}

impl DN {
    fn new(config: DNConfig) -> DN {
        DN {
            config,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            parked: AtomicU64::new(0),
            objects: AtomicU64::new(0),
            streams: AtomicU64::new(0),
            next_object: AtomicU64::new(1),
            next_stream: AtomicU64::new(1),
        }
    }
}

fn main() {
    let result =
        server::main(|process| Ok(DN::new(DNConfig::load(&process.config.service_config)?)));
    if let Err(e) = result {
        eprintln!("dn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    /// What one stream costs an object's list here. As with the SN (§4.7 again),
    /// the design budgets a **4 byte** node-local reference and this inlines the
    /// record, so an object's stream list is ten times its budget.
    #[test]
    fn a_stream_costs_an_object_more_than_a_reference() {
        let each = size_of::<Held>();
        assert_eq!(each, 40, "if this moved, §4.7 and issue #9 want to hear about it");
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
                id: 1,
                framing: Framing::Lf,
                streams: Vec::new(),
                extending: false,
                waiting: Vec::new(),
                keyed: false,
                created: 1_000_000,
            },
        );
    }

    /// The stream is what everything below the DN needs, so an object without one
    /// is not something to describe — the name is taken, and that is a different
    /// answer from "no object".
    #[test]
    fn an_object_being_made_does_not_look_like_a_finished_one() {
        let dn = DN::new(testing::dn_config());
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
        dn.shards[shard_of("/half")].lock().unwrap().get_mut("/half").unwrap().streams.push(Held {
            id: 7,
            sn: 0,
            first_record: 0,
            records: None,
        });
        let args = dn.look("/half").expect("an object");
        assert_eq!(args[1], Arg::Number(7), "its stream");
        assert!(!waits("/half"), "and it is no longer half made");
    }

    /// A name is taken before its stream is asked for, so two creates of one name
    /// ask for one stream. That is the rule that stops a create leaving a stream
    /// behind when it loses the race.
    #[test]
    fn one_name_is_taken_once() {
        let dn = DN::new(testing::dn_config());
        let mine = |dn: &DN| {
            let mut taken = Vec::new();
            let name = "/same".to_string();
            let mut shard = dn.shards[shard_of(&name)].lock().unwrap();
            if !shard.contains_key(name.as_str()) {
                shard.insert(
                    Box::from(name.as_str()),
                    Object {
                        id: dn.next_object.fetch_add(1, Ordering::Relaxed),
                        framing: Framing::Lf,
                        streams: Vec::new(),
                        extending: false,
                        waiting: Vec::new(),
                        keyed: false,
                        created: 1_000_000,
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
            id: 1,
            framing: Framing::Lf,
            streams: Vec::new(),
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: 1_000_000,
        };
        // Two sealed streams and one still open.
        object.streams.push(Held { id: 1, sn: 0, first_record: 0, records: Some(10) });
        object.streams.push(Held { id: 2, sn: 0, first_record: 10, records: Some(5) });
        object.streams.push(Held { id: 3, sn: 0, first_record: 15, records: None });

        assert_eq!(object.holding(0).unwrap().id, 1);
        assert_eq!(object.holding(9).unwrap().id, 1);
        assert_eq!(object.holding(10).unwrap().id, 2);
        assert_eq!(object.holding(14).unwrap().id, 2);
        assert_eq!(object.holding(15).unwrap().id, 3);
        // Past everything sealed goes to the open stream, whose reach is not known
        // here — the SN and EN say whether the record is really there.
        assert_eq!(object.holding(1000).unwrap().id, 3);
        // And an append always goes to the last.
        assert_eq!(object.last().unwrap().id, 3);
        assert!(!object.between_streams());
    }

    /// An object with three streams, two sealed and one still growing.
    fn grown() -> Object {
        let mut object = Object {
            id: 1,
            framing: Framing::Lf,
            streams: Vec::new(),
            extending: false,
            waiting: Vec::new(),
            keyed: false,
            created: 1_000_000,
        };
        object.streams.push(Held { id: 1, sn: 0, first_record: 0, records: Some(100) });
        object.streams.push(Held { id: 2, sn: 1, first_record: 100, records: Some(50) });
        object.streams.push(Held { id: 3, sn: 2, first_record: 150, records: None });
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
        assert!(detailed(&grown(), &Want::plain(), 100).is_none(), "nothing levelled to build");
        let object = grown();
        match describe_at(&object, None, &Want::plain(), 100).expect("an answer") {
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
        let args = flat(detailed(&grown(), &Want { limit: 2, ..want }, 100).expect("an answer"));
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
                detailed(&object, &Want { limit: 2, from: Start::Record(at), ..want }, 100)
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
        assert_eq!(seen, vec![1, 2, 3], "every stream, once, in order");
    }

    /// The configured cap and the request's cap are both honoured, whichever is
    /// smaller — a request may ask for less and may not ask for more.
    #[test]
    fn the_smaller_of_the_two_caps_wins() {
        let want = Want::plain().with(Detail::Streams);
        let given = |asked, configured| {
            let args = flat(
                detailed(&grown(), &Want { limit: asked, ..want }, configured).expect("an answer"),
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
        let args = flat(detailed(&grown(), &Want { limit: 100, ..want }, 100).expect("an answer"));
        let (_, rows) = read_back(&args);
        assert_eq!(rows[0].records, 100);
        assert_eq!(rows[2].records, UNKNOWN, "the one being appended to");
        assert_ne!(rows[2].records, 0, "which is not a count of none");
    }

    /// Counts without streams is a legitimate question: how big is this object,
    /// without the rows.
    #[test]
    fn counts_alone_bring_no_rows() {
        let want = Want::plain().with(Detail::Counts);
        let args = flat(detailed(&grown(), &want, 100).expect("an answer"));
        let (shape, rows) = read_back(&args);
        assert_eq!((shape.streams, shape.given, shape.entries), (3, 0, 1));
        assert!(rows.is_empty());
        assert!(!shape.more(), "nothing was asked to be described, so nothing follows");
    }

    /// A DN holding `count` objects named `/n0`, `/n1`, and so on.
    fn with_objects(count: u64) -> Arc<DN> {
        let dn = Arc::new(DN::new(testing::dn_config()));
        for n in 0..count {
            let name = format!("/n{n}");
            dn.shards[shard_of(&name)].lock().unwrap().insert(
                Box::from(name.as_str()),
                Object {
                    id: n + 1,
                    framing: Framing::Lf,
                    streams: vec![Held { id: n + 1, sn: 0, first_record: 0, records: None }],
                    extending: false,
                    waiting: Vec::new(),
                    keyed: false,
                    created: 1_000_000,
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
        assert_eq!(covered.next, 1, "and the caller carries on from the next");
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
        let wanted = Wanted::of(&args).expect("an object");
        assert_eq!((wanted.name.as_str(), wanted.keyed), ("/x", true));
        assert_eq!(wanted.framing, Framing::CrLf);

        // A media class after the framing is not this tier's business, and the head
        // of the request still parses.
        let mut more = args.clone();
        more.push(Arg::Text("ssd".into()));
        assert!(Wanted::of(&more[..Wanted::ARGS]).is_ok());
        // But a framing that means nothing is a bad request.
        let bad = vec![Arg::Text("/x".into()), Arg::Flag(false), Arg::Number(9), Arg::Number(0)];
        assert!(Wanted::of(&bad).is_err());
    }
}
