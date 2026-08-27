//! SN, the Stream Node: the extents of a stream and the instances of an extent.
//!
//! A stream is named by the range that owns it, so SN looks one up by
//! `(RangeId, StreamId)` and allocates neither. An extent is named here: this SN, an
//! id it hands out, and the second it was created in, which is what selects the
//! key it is encrypted under.
//!
//! **The SN names an extent and chooses its instances, and then gets out of the
//! way.** An append goes from the FE to the primary EN and is not seen here, so
//! this is asked once per extent rather than once per write.
//!
//! State is in memory and sharded by stream id, so creating streams from every
//! core does not queue on one lock. Nothing is deleted yet, so nothing here has
//! to cope with reuse — an index that has to survive churn is a later problem.

use common::config::{SNConfig, ServiceKind};
use common::ids::{DiskId, EnId, ExtentId, RangeId, SnDiskId, SnId, StreamId};
use common::info::{ExtentRow, ExtentRun, Listing, StreamName, Totals, UNKNOWN};
use common::offset::{COff, ERoff, LOff, SRoff};
use common::range::Filled;
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SERVES: &[Op] =
    &[Op::CreateStream, Op::OpenExtent, Op::LocateRecord, Op::SealExtent, Op::ListExtents];

/// Locks over the stream table. Enough that sixteen cores rarely collide,
/// few enough to walk when something wants a total.
const SHARDS: usize = 64;

struct Stream {
    /// When it was made. Nothing reads it yet; whether a stream needs a creation
    /// timestamp of its own at all is an open question.
    #[allow(dead_code)]
    created: TimeSec,
    /// The SN that created this stream's extents, which is part of their name
    /// (§4.6.3) and what §7.3.3 requires key derivation to use — **the creating SN,
    /// never the serving one**, because a reassigned stream keeps its old extents'
    /// keys.
    ///
    /// ⚠️ **Held per stream rather than per extent, on §4.1.2's rule.** The two differ
    /// only after a reassignment, which does not exist yet, so per-extent storage
    /// would spend 4 bytes on every one of §14.1's 1.7 × 10^9 extent records — **6.8 GB
    /// of an SN's memory** — for a fact that is almost always the stream's own. When
    /// reassignment arrives, the exceptions want an override out of line, not a field
    /// inline on the rule.
    creator: SnId,
    /// This stream's sealed extents, oldest first, each with the record count it
    /// came to. In record order, because a stream only ever appends — which is what
    /// makes finding the extent holding a record a search rather than a scan.
    sealed: Vec<Sealed>,
    /// The extent appends are landing in, once anything has asked for one.
    open: Option<Extent>,
    /// Records in the sealed extents, which is where the open extent's numbering
    /// starts from. **Stream-local**, which is the space above the extent's and below
    /// the object's — see `offset` for why all three are separate types.
    sealed_records: SRoff,
    /// Sealed: no more appends and no more concatenates, so the object this belongs
    /// to needs another stream for anything further.
    ///
    /// That is what sealed *means*; filling up is only why it happens here. An
    /// object is append-only, so only its last stream may take an append — every
    /// stream before it is sealed because appending to the middle of an object would
    /// renumber everything after it.
    sealed_stream: bool,
}

impl Stream {
    /// Each sealed extent with the stream-local number of its first record — the running
    /// total of everything before it.
    ///
    /// ⚠️ **This is the cost of not storing a position** (§17.29.1): finding an extent is
    /// a scan with a running sum rather than a search over stored keys. It was already a
    /// scan, and a stream holds a handful of extents in the expected case (§14.1.1's
    /// 128 MB against 32 MB extents), so nothing here got slower. A stream with millions
    /// of extents would want a sparse checkpoint every *k* extents, which is one `u64`
    /// per *k* rather than per extent (#107).
    fn sealed_from(&self) -> impl Iterator<Item = (SRoff, &Sealed)> {
        self.sealed.iter().scan(SRoff::ZERO, |at, sealed| {
            let first = *at;
            *at = SRoff::new(at.number() + sealed.records().number());
            Some((first, sealed))
        })
    }
}

/// An extent that is closed, and what it came to — **all three measures** (§9.1.3.6).
///
/// ⚠️ The logical length was missing here until #94, so `list-extents` answered
/// `logical: UNKNOWN` for every extent and one of §9.1.3.6's three budgets could not be
/// planned at all. The number was already flowing past on the write path; the seal report
/// simply did not carry it.
#[derive(Clone)]
struct Sealed {
    extent: Extent,
    /// The two lengths. ⚠️ The record count is **not** here: it belongs to the extent
    /// (§17.29.1), and `Filled` is the message the EN sends rather than the shape this
    /// keeps.
    logical: LOff,
    compressed: COff,
}

impl Sealed {
    /// ⚠️ **A count of records in an extent, widened into the stream's space** — which is
    /// a conversion and not a cast, since the two spaces are only equal for a stream of one
    /// extent. Written once, here, so the callers that sum it cannot get it wrong.
    fn records(&self) -> SRoff {
        SRoff::new(self.extent.records.number())
    }

    /// Whether a stream-local record number falls in this extent, given where the
    /// extent starts — which the *stream* knows and the extent does not.
    fn holds(&self, first: SRoff, record: SRoff) -> bool {
        record >= first && record.number() < first.number() + self.records().number()
    }
}

/// Where an extent is, under the name this SN gave it, and what it holds.
///
/// ⚠️ **No first record here** (Asserted, Bob, 2026-08-20), and the reason is not that it
/// was redundant: *"you can concatenate the same extent to multiple streams"*. One extent
/// has **as many first-records as there are streams referencing it**, so the field had no
/// single correct value to hold — it recorded whichever stream happened to create it and
/// was wrong for every other (§17.29.1).
#[derive(Clone, Debug)]
struct Extent {
    /// ⚠️ **`u32`, and safe because the id was never the name.** An extent is
    /// `(SN, id, second)` (§4.6.3), so an id only has to be unique among the extents one
    /// SN makes in one second — which 4.3 × 10^9 is not close to (§17.29.2).
    id: ExtentId,
    /// Records it holds, and **zero until it is sealed**, because until then the count
    /// grows with every append. `ERoff`, so it cannot be handed to anything counting a
    /// stream's or an object's records — and 32-bit because an extent seals at 32 MB
    /// compressed, so even one-byte records do not reach 4.3 × 10^9.
    records: ERoff,
    /// The second it was made in, which selects its key (§7.3.3).
    created: TimeSec,
    /// The **disks** holding its instances as indexes into the SN's disk table, primary first.
    disks: Vec<SnDiskId>,
}

/// An extent **as one stream holds it**: the extent, and where its records start in that
/// stream.
///
/// ⚠️ **The pair exists because the position is not the extent's** (Asserted, Bob,
/// 2026-08-20): *"you can concatenate the same extent to multiple streams"*, so one extent
/// has as many first-records as there are streams referencing it, and none of them is its
/// own (§17.29.1). Anything answering a question about an extent *in a stream* answers with
/// this; anything about the extent itself answers with `Extent`.
#[derive(Clone, Debug)]
struct Placed {
    extent: Extent,
    first: SRoff,
}

impl Extent {
    /// The id, the second, the first record, how many records it holds, then the disks
    /// with the primary first as `(EnId, DiskId)` pairs.
    ///
    /// ⚠️ **The caller supplies `first`**, because the extent does not know it. That is
    /// the whole of §17.29.1 in one signature: position is the stream's to say.
    fn args(&self, first: SRoff, disk_table: &[(EnId, DiskId)]) -> Vec<Arg> {
        let mut args = vec![
            Arg::Number(self.id.number()),
            Arg::Number(self.created.number()),
            Arg::Number(first.number()),
            Arg::Number(self.records.number()),
            Arg::Number(self.disks.len() as u64),
        ];
        for sn_disk in &self.disks {
            let (en, disk) = disk_table[sn_disk.get() as usize];
            args.push(Arg::Number(en.number()));
            args.push(Arg::Number(disk.number()));
        }
        args
    }
}

/// A stream that will hold no more extents, and how many records it came to. Not a
/// failure: whoever asked needs another stream, and the DN is what makes those.
#[derive(Debug)]
struct Full {
    records: u64,
    /// Set when the stream could not be extended for an ordinary reason instead.
    why: Option<Error>,
}

impl Full {
    fn records(records: u64) -> Full {
        Full { records, why: None }
    }
}

impl From<Error> for Full {
    fn from(why: Error) -> Full {
        Full { records: 0, why: Some(why) }
    }
}

/// Hands out extent names and chooses instances for them.
struct Naming {
    next: AtomicU64,
    /// The latest second used in a name, so one cannot go backwards.
    latest: AtomicU64,
    turn: AtomicU64,
}

impl Naming {
    fn new() -> Naming {
        Naming { next: AtomicU64::new(1), latest: AtomicU64::new(0), turn: AtomicU64::new(0) }
    }

    /// An id and the second to name an extent with. A second earlier than one
    /// already used is refused rather than quietly moved forward: the second is
    /// what selects the key, so reaching back for an earlier one asks for a key
    /// that may since have been retired.
    fn name(&self, second: TimeSec) -> Result<(ExtentId, TimeSec)> {
        // The atomic holds seconds; the check that they only move forward is `TimeSec`'s,
        // so the message is the same wherever a creation second is taken (§17.32).
        let latest = TimeSec::new(self.latest.fetch_max(second.number(), Ordering::Relaxed));
        let second = second.not_before(latest)?;
        Ok((ExtentId::new(self.next.fetch_add(1, Ordering::Relaxed) as u32), second))
    }

    /// Instances for one extent, taken in turn from the disks there are.
    fn disks(&self, total_disks: usize, want: usize) -> Result<Vec<SnDiskId>> {
        if total_disks < want {
            return Err(Error::new(format!(
                "an extent wants {want} instances, and there are only {total_disks} disks",
            )));
        }
        let turn = self.turn.fetch_add(want as u64, Ordering::Relaxed) as usize;
        Ok((0..want).map(|n| SnDiskId::new(((turn + n) % total_disks) as u32)).collect())
    }
}

struct SN {
    config: SNConfig,
    /// This SN's own number, which every extent it creates records as its creator.
    ///
    /// ⚠️ **Held here rather than threaded through**, because §8.1.6.1 flags `close`'s
    /// parameter list as the worst run of same-typed arguments in the code and passing
    /// this down would have made it one longer. An SN's identity is a property of the
    /// SN, not an argument to its methods.
    mine: SnId,
    /// All disks across all ENs that this SN can see.
    disks: Vec<(EnId, DiskId)>,
    /// ⚠️ **Keyed by the whole name**, `(RangeId, StreamId, CreateSec)`, and not by the id
    /// alone (#109). That is what makes the id safe at 32 bits and what makes recycling one
    /// safe at all: a re-minted id in a later second is a different key, so the old stream is
    /// not overwritten and a stale reference to it does not silently find the new one
    /// (`A-NAME-IS-NEVER-REMINTED-IN-ITS-OWN-SECOND`).
    shards: Vec<Mutex<HashMap<StreamName, Stream>>>,
    naming: Naming,
    streams: AtomicU64,
    extents: AtomicU64,
    sealed: AtomicU64,
    sealed_streams: AtomicU64,
}

fn shard_of(name: StreamName) -> usize {
    // Stream ids from one range are consecutive, so mixing the low bits spreads a
    // batch of them across every shard rather than filling one. ⚠️ The second is *not*
    // mixed in: streams made together share it, and hashing it would undo the spreading
    // the id gives — and two streams differing only in the second are a recycled id, which
    // is rare by construction (#110).
    (name.stream.number().wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ name.range.number()) as usize
        % SHARDS
}

impl Service for SN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, mut reply: Reply) {
        // The stream range, which is what a DN's line for the same request says.
        if let [Arg::Number(range), Arg::Number(first), Arg::Number(count), ..] =
            request.args.as_slice()
        {
            reply.about(format_args!("count {count} from {first} for range {range}"));
        }
        reply.send(self.answer(process, &request));
    }

    fn answer(&self, process: &Process, request: &Request) -> Response {
        match request.op {
            Op::CreateStream => self.create(request),
            Op::OpenExtent => self.open_extent(process, request),
            Op::LocateRecord => self.locate(request),
            Op::ListExtents => self.list(request),
            Op::SealExtent => self.seal(process, request),
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("streams {}", self.streams.load(Ordering::Relaxed)),
            format!("extents {}", self.extents.load(Ordering::Relaxed)),
            format!("sealed_extents {}", self.sealed.load(Ordering::Relaxed)),
            format!("sealed_streams {}", self.sealed_streams.load(Ordering::Relaxed)),
            format!("stream_share {}", self.config.stream_share),
        ]
    }
}

impl SN {
    fn new(config: SNConfig, mine: SnId, disks: Vec<(EnId, DiskId)>) -> SN {
        SN {
            config,
            mine,
            disks,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            naming: Naming::new(),
            streams: AtomicU64::new(0),
            extents: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            sealed_streams: AtomicU64::new(0),
        }
    }

    /// `(RangeId, first StreamId, count, creation second)`. A DN creating many
    /// objects at once names a run of streams in one request, and they share the second —
    /// so it is the consecutive ids that keep the names apart (#110).
    fn create(&self, request: &Request) -> Response {
        let (range, first, count, created) = match request.args.as_slice() {
            [Arg::Number(range), Arg::Number(first), Arg::Number(count), Arg::Number(created)] => {
                match (RangeId::of_number(*range), StreamId::of_number(*first)) {
                    (Ok(range), Ok(first)) => (range, first, *count, TimeSec::new(*created)),
                    _ => {
                        return Response::error(
                            Status::BadRequest,
                            "create-stream takes a RangeId, a first StreamId, a count and a second",
                        );
                    }
                }
            }
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "create-stream takes a RangeId, a first StreamId, a count and a second",
                );
            }
        };

        for n in 0..count {
            // Every stream of the run shares the second, which is what makes the ids have to
            // differ — and they do, being consecutive (#110).
            let Ok(stream) = StreamId::of_number(first.number() + n) else {
                return Response::error(
                    Status::BadRequest,
                    format!("{} is not a stream", first.number() + n),
                );
            };
            let name = StreamName { range, stream, created };
            let mut shard = self.shards[shard_of(name)].lock().expect("streams");
            shard.insert(
                name,
                Stream {
                    created,
                    creator: self.mine,
                    sealed: Vec::new(),
                    open: None,
                    sealed_records: SRoff::ZERO,
                    sealed_stream: false,
                },
            );
        }
        self.streams.fetch_add(count, Ordering::Relaxed);
        Response::ok()
    }

    /// A stream's name, and back comes the extent to append to. The ENs are taken from the
    /// roster, and the second from the clock.
    fn open_extent(&self, process: &Process, request: &Request) -> Response {
        let name = match StreamName::of_args(&request.args) {
            Ok((name, _)) => name,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        match self.open(name, TimeSec::now(), process.cluster.extents.instances) {
            Ok(placed) => Response {
                id: 0,
                status: Status::Ok,
                args: placed.extent.args(placed.first, &self.disks),
            },
            Err(Full { records, why: None }) => {
                Response { id: 0, status: Status::Sealed, args: vec![Arg::Number(records)] }
            }
            Err(Full { why: Some(why), .. }) => Response::error(Status::Internal, why.to_string()),
        }
    }

    /// `(RangeId, StreamId, record)`, and back comes the extent whose numbering
    /// starts at or before that record. Whether the record is actually there is
    /// the EN's to say: an SN keeps its byte and record totals lazily, so it
    /// routes a read and does not bound it.
    fn locate(&self, request: &Request) -> Response {
        let (name, record) = match request.args.as_slice() {
            [args @ .., Arg::Number(record)] => match StreamName::of_args(args) {
                Ok((name, [])) => (name, SRoff::new(*record)),
                _ => {
                    return Response::error(
                        Status::BadRequest,
                        "locate-record takes a stream's name and a record",
                    );
                }
            },
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "locate-record takes a stream's name and a record",
                );
            }
        };
        let shard = self.shards[shard_of(name)].lock().expect("streams");
        let Some(held) = shard.get(&name) else {
            return Response::error(Status::NotFound, format!("no {name}"));
        };
        // The sealed extents are in record order, so this is a walk over a sorted run,
        // accumulating each extent's start as it goes (§17.29.1).
        if let Some((first, sealed)) = held.sealed_from().find(|(first, s)| s.holds(*first, record))
        {
            return Response {
                id: 0,
                status: Status::Ok,
                args: sealed.extent.args(first, &self.disks),
            };
        }
        match &held.open {
            // How far the open extent reaches is not known here — an SN keeps its
            // record totals lazily — so anything at or past its first record is
            // routed to it and the EN says whether it is there.
            Some(extent) if record >= held.sealed_records => Response {
                id: 0,
                status: Status::Ok,
                args: extent.args(held.sealed_records, &self.disks),
            },
            Some(_) | None => Response::error(
                Status::NotFound,
                format!("{name} has no extent holding record {record}"),
            ),
        }
    }

    /// `list-extents`: runs of extents for **several** streams in one ask (§9.1.3.7).
    ///
    /// **Complete in counts and partial in detail** (§9.1.3.2): each stream's extent
    /// total is this SN's own, so it costs nothing and is always right, while the rows
    /// stop at the limit and `next` says where to carry on.
    ///
    /// ⚠️ **The limit is shared across the whole ask**, spent on the streams in the
    /// order given. A stream reached after it runs out is still answered — with
    /// `given` zero and `next` where it was asked to start — so a caller learns the
    /// count and where to resume rather than being left to infer that it was skipped.
    ///
    /// ⚠️ **Record numbers here are stream-local**, which is what an SN knows. The DN
    /// converts them to the object's numbering, because it is the tier that holds each
    /// stream's first record (§17.22).
    fn list(&self, request: &Request) -> Response {
        let asked = match Listing::of_args(&request.args) {
            Ok(asked) => asked,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        let mut args = Vec::new();
        let mut room = asked.limit;
        for (name, from) in &asked.at {
            let shard = self.shards[shard_of(*name)].lock().expect("streams");
            let Some(held) = shard.get(name) else {
                return Response::error(Status::NotFound, format!("no {name}"));
            };
            let (run, rows) = self.run_of(held, name.stream, *from, room);
            room = room.saturating_sub(rows.len() as u64);
            args.extend(run.args());
            for row in rows {
                args.extend(row.args());
            }
        }
        Response { id: 0, status: Status::Ok, args }
    }

    /// One stream's run, up to `room` rows.
    fn run_of(
        &self,
        held: &Stream,
        stream: StreamId,
        from: u64,
        room: u64,
    ) -> (ExtentRun, Vec<ExtentRow>) {
        // The sealed extents are in record order, so the first to describe is the last
        // one starting at or before `from` — the same rule locate uses to pick one.
        let first = if from == 0 {
            0
        } else {
            held.sealed_from().filter(|(at, _)| at.number() <= from).count().saturating_sub(1)
        };
        let total = held.sealed.len() as u64 + u64::from(held.open.is_some());

        let mut rows: Vec<ExtentRow> = held
            .sealed_from()
            .skip(first)
            .take(room as usize)
            .map(|(at, sealed)| ExtentRow {
                sn: held.creator,
                id: sealed.extent.id,
                created: sealed.extent.created,
                first_record: at.number(),
                records: sealed.records().number(),
                logical: sealed.logical.number(),
                length: sealed.compressed.number(),
                disks: sealed
                    .extent
                    .disks
                    .iter()
                    .map(|sn_disk| self.disks[sn_disk.get() as usize])
                    .collect(),
            })
            .collect();

        // Then the one still open, if there is room and everything sealed is described.
        // ⚠️ Its counts are UNKNOWN rather than zero: how far it reaches is not knowable
        // here, and a sealed extent holding nothing would be a real zero (§8.1.6.3).
        let sealed_seen = first + rows.len();
        if (rows.len() as u64) < room
            && sealed_seen == held.sealed.len()
            && let Some(open) = &held.open
        {
            rows.push(ExtentRow {
                sn: held.creator,
                id: open.id,
                created: open.created,
                first_record: held.sealed_records.number(),
                records: UNKNOWN,
                logical: UNKNOWN,
                length: UNKNOWN,
                disks: open
                    .disks
                    .iter()
                    .map(|sn_disk| self.disks[sn_disk.get() as usize])
                    .collect(),
            });
        }

        // Where to carry on: the first record of the next extent not described.
        let after = first + rows.len();
        let next = match held.sealed.get(after) {
            Some(_) => held.sealed_from().nth(after).map_or(UNKNOWN, |(at, _)| at.number()),
            None if after < total as usize => {
                held.open.as_ref().map_or(UNKNOWN, |_| held.sealed_records.number())
            }
            None => UNKNOWN,
        };
        // What the stream has come to, which only the SN can say: it owns the extent list.
        //
        // ⚠️ **Over the sealed extents, and the open one contributes nothing.** How far the
        // open extent reaches is not knowable here (§6.3), and adding a zero for it would
        // make the total read as complete when it is a stream short of its tail. So a
        // growing stream's totals are *behind*, which is exactly the staleness the DN's
        // cache is allowed (§17.25.1) — and a sealed stream has no open extent, so its
        // totals are exact.
        let mut totals = Totals { extents: total, logical: 0, compressed: 0 };
        for sealed in &held.sealed {
            totals.logical += sealed.logical.number();
            totals.compressed += sealed.compressed.number();
        }
        (ExtentRun { stream, totals, given: rows.len() as u64, next }, rows)
    }

    /// A stream's name, the sealed extent and what it came to: record it and open the next
    /// one, which is one round trip rather than two because whoever saw the seal wants both.
    fn seal(&self, process: &Process, request: &Request) -> Response {
        let (name, id, came_to): (StreamName, ExtentId, Filled) =
            match StreamName::of_args(&request.args) {
                Ok((name, [Arg::Number(id), rest @ ..])) => match Filled::of_args(rest) {
                    Ok((came_to, [])) => match ExtentId::of_number(*id) {
                        Ok(id) => (name, id, came_to),
                        Err(e) => return Response::error(Status::BadRequest, e.to_string()),
                    },
                    _ => {
                        return Response::error(
                            Status::BadRequest,
                            "seal-extent takes a stream's name, an extent and what it came to",
                        );
                    }
                },
                _ => {
                    return Response::error(
                        Status::BadRequest,
                        "seal-extent takes a RangeId, a StreamId, an extent and what it came to",
                    );
                }
            };
        match self.close(
            name,
            id,
            came_to,
            TimeSec::now(),
            process.cluster.extents.instances,
            process.cluster.extents.per_stream,
        ) {
            Ok(placed) => Response {
                id: 0,
                status: Status::Ok,
                args: placed.extent.args(placed.first, &self.disks),
            },
            // The stream is full, which is the DN's business: it says what the
            // stream came to so the object's next stream can carry on from there.
            Err(Full { records, why: None }) => {
                Response { id: 0, status: Status::Sealed, args: vec![Arg::Number(records)] }
            }
            Err(Full { why: Some(why), .. }) => Response::error(Status::Internal, why.to_string()),
        }
    }

    /// Seal the stream's open extent and name the next one, both under the stream's
    /// lock, so a request that finds the extent already sealed gets the extent that
    /// replaced it rather than an error.
    #[allow(clippy::too_many_arguments)]
    fn close(
        &self,
        name: StreamName,
        id: ExtentId,
        came_to: Filled,
        second: TimeSec,
        instances: usize,
        per_stream: usize,
    ) -> std::result::Result<Placed, Full> {
        let mut shard = self.shards[shard_of(name)].lock().expect("streams");
        let Some(held) = shard.get_mut(&name) else {
            return Err(Full::from(Error::new(format!("no {name}"))));
        };
        // Only whoever names the open extent seals it. Two appends can find the same
        // extent full at once, and the second one has nothing left to do.
        if held.open.as_ref().is_some_and(|open| open.id == id) {
            let extent = held.open.take().expect("an open extent");
            // ⚠️ Accumulated rather than computed from a stored base, which is the same
            // change one level down: the running total is the stream's (§17.29.1).
            held.sealed_records = SRoff::new(held.sealed_records.number() + came_to.records);
            let extent = Extent { records: ERoff::of_number(came_to.records)?, ..extent };
            held.sealed.push(Sealed {
                extent,
                logical: came_to.logical,
                compressed: came_to.compressed,
            });
            self.sealed.fetch_add(1, Ordering::Relaxed);
            if held.sealed.len() >= per_stream {
                held.sealed_stream = true;
                self.sealed_streams.fetch_add(1, Ordering::Relaxed);
            }
        }
        if held.sealed_stream {
            return Err(Full::records(held.sealed_records.number()));
        }
        self.appendable(held, second, instances).map_err(Full::from)
    }

    /// The stream's open extent, naming one if it has none.
    fn open(
        &self,
        name: StreamName,
        second: TimeSec,
        instances: usize,
    ) -> std::result::Result<Placed, Full> {
        let mut shard = self.shards[shard_of(name)].lock().expect("streams");
        let Some(held) = shard.get_mut(&name) else {
            return Err(Full::from(Error::new(format!("no {name}"))));
        };
        if held.sealed_stream {
            return Err(Full::records(held.sealed_records.number()));
        }
        self.appendable(held, second, instances).map_err(Full::from)
    }

    /// The extent this stream can be appended to, named here if there is not one.
    ///
    /// **The stream's lock is held across the naming, so several requests for a new
    /// extent produce one extent**: whoever gets the lock does the work and the rest
    /// queue behind it and are handed the same answer. That is what stops a burst of
    /// appends to a fresh stream — or to one that has just sealed — from opening an
    /// extent apiece.
    ///
    /// ⚠️ The queue is the mutex only because the work is a counter and an instance
    /// list. When opening an extent has to be recorded durably, this becomes a wait
    /// on somebody else's round trip, and blocking a worker per waiter would be the
    /// wrong shape — the waiters must attach to the one in flight instead.
    fn appendable(&self, held: &mut Stream, second: TimeSec, instances: usize) -> Result<Placed> {
        // Where the open extent starts is the stream's running total, whether the extent
        // was already there or is being made now.
        let first = held.sealed_records;
        if let Some(extent) = &held.open {
            return Ok(Placed { extent: extent.clone(), first });
        }
        let (id, created) = self.naming.name(second)?;
        let extent = Extent {
            id,
            created,
            records: ERoff::ZERO,
            disks: self.naming.disks(self.disks.len(), instances)?,
        };
        held.open = Some(extent.clone());
        self.extents.fetch_add(1, Ordering::Relaxed);
        Ok(Placed { extent, first })
    }
}

fn main() {
    let result = server::main(|process| {
        let disks: Vec<(EnId, DiskId)> = process
            .cluster
            .services_of_kind(ServiceKind::En)
            .map(|en| (EnId::new(en.number()), DiskId::FIRST))
            .collect();
        Ok(SN::new(
            SNConfig::load(&process.config.service_config)?,
            SnId::new(process.entry.number()),
            disks,
        ))
    });
    if let Err(e) = result {
        eprintln!("sn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    /// What an extent came to, for a test that does not care about the difference between
    /// a logical length and a compressed one. Anything testing #94's distinction says both.
    fn came(records: u64, compressed: u64) -> Filled {
        Filled {
            records,
            logical: LOff::new(compressed as u32),
            compressed: COff::new(compressed as u32),
        }
    }

    use std::mem::size_of;

    /// What one extent costs a stream's list here, which is what decides how many a
    /// stream can hold before a box runs out of RAM. §4.7 budgets a **4 byte**
    /// node-local reference for this list, with the ~100 byte record itself living
    /// once in a table the streams point into.
    ///
    /// ⚠️ **This list inlines the whole record instead**, so it costs twenty times
    /// the budget. That is a property of the placeholder and not of the design
    /// (§14.6), and it is the thing that decides what a large-stream test can
    /// actually reach today.
    #[test]
    fn an_extent_costs_a_stream_more_than_a_reference() {
        // Plus a second allocation per extent for the instance list, which the
        // allocator rounds up and adds a header to — so the true cost is larger
        // again, and 2^n of them are 2^n allocations.
        let each = size_of::<Sealed>() + 4 * size_of::<SnDiskId>();
        assert_eq!(each, 64, "if this moved, §4.7 and issue #9 want to hear about it");
        // ⚠️ **80 until 2026-08-20** (§17.29): a `u32` id, a `u32` record count in place of a
        // `u64` one, and no stored first record — 16 bytes off the SN's largest structure,
        // which is **27 GB** at §14.1's 1.7 × 10^9 extents per SN.
        assert_eq!(
            size_of::<Sealed>(),
            2 * size_of::<u32>()
                + size_of::<u64>()
                + size_of::<Vec<SnDiskId>>()
                + 2 * size_of::<u32>(),
            "and no padding: id, records, created, the disk list, and the two lengths"
        );
        for bits in [24u32, 27, 32] {
            println!(
                "2^{bits} extents in one stream = {:.0} GB",
                each as f64 * 2f64.powi(bits as i32) / 1e9
            );
        }
        // Against the design's reference width, the same list is a different object.
        assert_eq!(each / 4, 16, "sixteen times what §4.7 budgets for a reference");
    }

    use super::*;
    use common::offset::{COff, LOff};
    use common::testing;

    /// Extents enough that a stream does not seal in the tests that are not about
    /// a stream sealing.
    const ROOMY: usize = 1000;

    /// A stream, so there is something to open an extent on.
    /// The name every test in here uses, so the second is one thing rather than sprinkled.
    const MADE: TimeSec = TimeSec::new(1_000_000);

    fn named(range: u32, stream: u32) -> StreamName {
        StreamName { range: RangeId::new(range), stream: StreamId::new(stream), created: MADE }
    }

    fn default_disks() -> Vec<(EnId, DiskId)> {
        (0..4).map(|i| (EnId::new(i), DiskId::new(0))).collect()
    }

    fn sn_with_stream(range: u32, stream: u32) -> SN {
        let sn = SN::new(testing::sn_config(), SnId::new(0), default_disks());
        let made = sn.create(
            &Request::new(Op::CreateStream)
                .number(range as u64)
                .number(stream as u64)
                .number(1)
                .number(1_000_000),
        );
        assert_eq!(made.status, Status::Ok);
        sn
    }

    #[test]
    fn a_stream_opens_one_extent_however_often_it_is_asked() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let again = sn.open(named(0, 7), TimeSec::new(1_000_001), want).unwrap();
        assert_eq!(first.extent.id, again.extent.id, "the second ask should find the open extent");
        assert_eq!(first.extent.created, again.extent.created);
        assert_eq!(sn.extents.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn an_extent_on_a_stream_that_does_not_exist_is_refused() {
        let sn = SN::new(testing::sn_config(), SnId::new(0), default_disks());
        assert!(sn.open(named(0, 7), TimeSec::new(1_000_000), 4).is_err());
    }

    /// Sealing records what the extent came to and names the next one, whose
    /// numbering starts where the last one stopped.
    #[test]
    fn sealing_starts_the_next_extent_where_the_last_one_stopped() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        // ⚠️ Asserted on the *stream*, because an extent no longer carries where it starts
        // (§17.29.1) — which is the point: the running total is the stream's, and testing it
        // there tests the thing that is actually maintained.
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        assert_eq!(sn.starts_at(0, 7), SRoff::new(0));

        // Twelve records in it, and 400 bytes.
        let second = sn
            .close(
                named(0, 7),
                first.extent.id,
                came(12, 400),
                TimeSec::new(1_000_001),
                want,
                ROOMY,
            )
            .unwrap();
        assert_ne!(second.extent.id, first.extent.id, "a fresh extent");
        assert_eq!(sn.starts_at(0, 7), SRoff::new(12), "numbering carries on");

        let third = sn
            .close(named(0, 7), second.extent.id, came(5, 90), TimeSec::new(1_000_002), want, ROOMY)
            .unwrap();
        assert_eq!(sn.starts_at(0, 7), SRoff::new(17));
        assert_eq!(third.extent.records, ERoff::ZERO, "and the fresh one holds nothing yet");
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 2);

        // And the positions the stream hands out are the running sum of what came before.
        assert_eq!(sn.firsts(0, 7), vec![SRoff::ZERO, SRoff::new(12)], "one per sealed extent");
    }

    /// Where the open extent starts, for a test.
    impl SN {
        fn starts_at(&self, range: u32, stream: u32) -> SRoff {
            self.shards[shard_of(named(range, stream))].lock().unwrap()[&named(range, stream)]
                .sealed_records
        }

        /// The stream-local first record of every sealed extent, recomputed by summing.
        fn firsts(&self, range: u32, stream: u32) -> Vec<SRoff> {
            self.shards[shard_of(named(range, stream))].lock().unwrap()[&named(range, stream)]
                .sealed_from()
                .map(|(at, _)| at)
                .collect()
        }
    }

    /// ⚠️ **The spot the `args(0)` bug was at** (§17.29.5). `open-extent` answers with the
    /// extent *and where it starts in this stream*, and the second one is easy to get wrong
    /// by passing a literal: the same argument used to mean "how many records" and now means
    /// "from which record", so `0` stayed correct-looking and became wrong. Nothing at this
    /// tier caught it, because `open` itself was right — the mistake was in the answer.
    #[test]
    fn the_open_extent_answers_with_where_it_starts_in_the_stream() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        assert_eq!(first.first, SRoff::ZERO, "the first extent starts the stream");

        // Seal it with twelve records, so the next one does not start at zero.
        sn.close(named(0, 7), first.extent.id, came(12, 400), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        let next = sn.open(named(0, 7), TimeSec::new(1_000_002), want).unwrap();
        assert_eq!(next.first, SRoff::new(12), "and the next carries on from what came before");

        // And the answer on the wire says so, which is the argument that was wrong: third
        // field is the first record, fourth is the count.
        let answer = next.extent.args(next.first, &sn.disks);
        let [_, _, Arg::Number(at), Arg::Number(records), ..] = answer.as_slice() else {
            panic!("an extent answers with an id, a second, a first record and a count")
        };
        assert_eq!((*at, *records), (12, 0), "starts at 12, holds nothing yet");
    }

    /// An extent already sealed by somebody else does not fail: the caller wanted
    /// somewhere to append, and the extent that replaced it is the answer. This is
    /// the race the stream's lock exists to make harmless.
    #[test]
    fn sealing_an_extent_that_is_already_gone_gives_the_one_that_replaced_it() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(4, 100), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        // A request that saw the seal a moment later, naming the extent that is now
        // sealed, and one naming an extent this stream never had.
        assert_eq!(
            sn.close(
                named(0, 7),
                first.extent.id,
                came(4, 100),
                TimeSec::new(1_000_002),
                want,
                ROOMY
            )
            .unwrap()
            .extent
            .id,
            second.extent.id
        );
        assert_eq!(
            sn.close(
                named(0, 7),
                ExtentId::new(999),
                came(0, 0),
                TimeSec::new(1_000_003),
                want,
                ROOMY
            )
            .unwrap()
            .extent
            .id,
            second.extent.id
        );
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 1, "sealed once");
        assert_eq!(sn.extents.load(Ordering::Relaxed), 2, "two extents, not four");
    }

    /// Two appends can find the same extent full at once, so sealing it twice must
    /// not open two extents. The second one gets whatever is open now.
    #[test]
    fn sealing_the_same_extent_twice_opens_one_extent() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(4, 100), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        let repeat = sn
            .close(named(0, 7), first.extent.id, came(4, 100), TimeSec::new(1_000_002), want, ROOMY)
            .unwrap();
        assert_eq!(repeat.extent.id, second.extent.id, "the extent that is open now");
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 1, "sealed once");
    }

    /// A stream seals when it has all the extents it is allowed, and then it will
    /// take no more — which is the object's problem, not this stream's.
    #[test]
    fn a_stream_seals_once_it_has_its_extents() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        // Room for two extents, so the second seal fills the stream.
        let two = 2;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(10, 100), TimeSec::new(1_000_001), want, two)
            .unwrap();
        assert_eq!(sn.starts_at(0, 7), SRoff::new(10));

        let full = sn
            .close(named(0, 7), second.extent.id, came(7, 70), TimeSec::new(1_000_002), want, two)
            .expect_err("the stream should be full");
        assert!(full.why.is_none(), "full is not a failure");
        assert_eq!(full.records, 17, "what the whole stream came to");
        assert_eq!(sn.sealed_streams.load(Ordering::Relaxed), 1);

        // And it stays sealed: nothing opens another extent on it.
        let again = sn.open(named(0, 7), TimeSec::new(1_000_003), want).expect_err("sealed");
        assert_eq!(again.records, 17);
        assert!(again.why.is_none());
    }

    /// A record in a sealed extent is found there; one past everything sealed goes to
    /// the open extent, because how far that reaches is the EN's to say.
    #[test]
    fn a_record_is_found_in_the_extent_holding_it() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(3, 60), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        let third = sn
            .close(named(0, 7), second.extent.id, came(2, 40), TimeSec::new(1_000_002), want, ROOMY)
            .unwrap();

        let holds = |record: u64| {
            // The whole name, built from the shape rather than three numbers in a row (#109).
            let mut ask = Request::new(Op::LocateRecord);
            ask.args.extend(named(0, 7).args());
            ask.args.push(Arg::Number(record));
            let answer = sn.locate(&ask);
            assert_eq!(answer.status, Status::Ok, "record {record}");
            match answer.args.first() {
                Some(Arg::Number(id)) => *id,
                _ => panic!("no extent"),
            }
        };
        // Records 0 to 2 are in the first extent, 3 and 4 in the second, and
        // anything from 5 on is routed to the one still open.
        assert_eq!(holds(0), first.extent.id.number());
        assert_eq!(holds(2), first.extent.id.number());
        assert_eq!(holds(3), second.extent.id.number());
        assert_eq!(holds(4), second.extent.id.number());
        assert_eq!(holds(5), third.extent.id.number());
        assert_eq!(holds(500), third.extent.id.number(), "the EN says whether it is really there");
    }

    /// A stream with no extent at all has nowhere to look.
    #[test]
    fn a_record_in_a_stream_with_no_extent_is_not_found() {
        let sn = sn_with_stream(0, 7);
        let answer = sn.locate(&{
            let mut a = Request::new(Op::LocateRecord);
            a.args.extend(named(0, 7).args());
            a.args.push(Arg::Number(0));
            a
        });
        assert_eq!(answer.status, Status::NotFound);
    }

    /// The second names the key, so an id must never carry one that has already
    /// been passed.
    #[test]
    fn a_second_may_not_go_backwards() {
        let naming = Naming::new();
        assert!(naming.name(TimeSec::new(1_000_000)).is_ok());
        assert!(naming.name(TimeSec::new(1_000_005)).is_ok());
        assert!(naming.name(TimeSec::new(999_999)).is_err());
        // And the refusal did not lose the position it had reached.
        assert!(naming.name(TimeSec::new(1_000_005)).is_ok());
    }

    #[test]
    fn every_extent_gets_a_name_of_its_own() {
        let naming = Naming::new();
        let (first, _) = naming.name(TimeSec::new(1_000_000)).unwrap();
        let (second, _) = naming.name(TimeSec::new(1_000_000)).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn instances_are_four_distinct_disks_taken_in_turn() {
        let naming = Naming::new();
        for _ in 0..20 {
            let picked = naming.disks(7, testing::extents().instances).unwrap();
            assert_eq!(picked.len(), testing::extents().instances);
            let mut sorted = picked.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), testing::extents().instances, "{picked:?} repeats a disk");
        }
    }

    /// Fewer ENs than an extent has instances cannot be papered over by repeating
    /// one: four instances on three machines is three failure domains.
    #[test]
    fn too_few_ens_is_an_error_not_a_repeat() {
        let naming = Naming::new();
        assert!(naming.disks(3, testing::extents().instances).is_err());
    }

    /// The four ENs in the default roster are exactly what an extent needs.
    #[test]
    fn the_test_roster_has_an_extent_worth_of_ens() {
        let cluster = testing::cluster();
        let ens: Vec<(EnId, DiskId)> = cluster
            .services_of_kind(ServiceKind::En)
            .map(|en| (EnId::new(en.number()), DiskId::FIRST))
            .collect();
        assert_eq!(ens.len(), testing::extents().instances);
        assert!(Naming::new().disks(ens.len(), testing::extents().instances).is_ok());
    }
}
