//! SN, the Stream Node: the extents of a stream and the instances of an extent.
//!
//! A stream is named by the DN that owns it, so SN looks one up by
//! `(DnId, StreamId)` and allocates neither. An extent is named here: this SN, an
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
use common::info::{ExtentRow, ExtentRun, UNKNOWN};
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SERVES: &[Op] =
    &[Op::CreateStream, Op::OpenExtent, Op::LocateRecord, Op::SealExtent, Op::ListExtents];

/// Locks over the stream table. Enough that sixteen cores rarely collide,
/// few enough to walk when something wants a total.
const SHARDS: usize = 64;

struct Stream {
    /// Seconds since the epoch. Nothing reads it yet; whether a stream needs a
    /// creation timestamp of its own at all is an open question.
    #[allow(dead_code)]
    created: u64,
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
    creator: u32,
    /// This stream's sealed extents, oldest first, each with the record count it
    /// came to. In record order, because a stream only ever appends — which is what
    /// makes finding the extent holding a record a search rather than a scan.
    sealed: Vec<Sealed>,
    /// The extent appends are landing in, once anything has asked for one.
    open: Option<Extent>,
    /// Records in the sealed extents, which is where the open extent's numbering
    /// starts from.
    sealed_records: u64,
    /// Sealed: no more appends and no more concatenates, so the object this belongs
    /// to needs another stream for anything further.
    ///
    /// That is what sealed *means*; filling up is only why it happens here. An
    /// object is append-only, so only its last stream may take an append — every
    /// stream before it is sealed because appending to the middle of an object would
    /// renumber everything after it.
    sealed_stream: bool,
}

/// An extent that is closed, and what it came to.
#[derive(Clone)]
struct Sealed {
    extent: Extent,
    records: u64,
    /// Compressed bytes. Nothing reads it yet; billing and repair scheduling will.
    #[allow(dead_code)]
    length: u64,
}

impl Sealed {
    /// Whether a stream-local record number falls in this extent.
    fn holds(&self, record: u64) -> bool {
        record >= self.extent.first_record && record < self.extent.first_record + self.records
    }
}

/// Where an extent is, under the name this SN gave it.
#[derive(Clone, Debug)]
struct Extent {
    id: u64,
    /// The second it was made in, which selects its key.
    created: u64,
    /// The stream-local number of this extent's first record. An extent numbers
    /// its own records from zero, and this is what turns one of those into the
    /// stream-local number that is a durable reference.
    first_record: u64,
    /// The ENs holding its instances. The first is the primary, which is the one
    /// an append is sent to.
    instances: Vec<u32>,
}

impl Extent {
    /// The id, the second, the first record, how many records it holds, then the
    /// instances with the primary first, which is the order an FE needs them in.
    ///
    /// **The record count is zero unless the extent is sealed**, because until then it
    /// grows with every append. A count is what lets whoever asked remember where
    /// those records live and never ask again — so it is worth sending even to a
    /// reader that did not write them.
    fn args(&self, records: u64) -> Vec<Arg> {
        let mut args = vec![
            Arg::Number(self.id),
            Arg::Number(self.created),
            Arg::Number(self.first_record),
            Arg::Number(records),
        ];
        args.extend(self.instances.iter().map(|en| Arg::Number(*en as u64)));
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
    fn name(&self, second: u64) -> Result<(u64, u64)> {
        let latest = self.latest.fetch_max(second, Ordering::Relaxed);
        if second < latest {
            return Err(Error::new(format!("the clock went back to {second} from {latest}")));
        }
        Ok((self.next.fetch_add(1, Ordering::Relaxed), second))
    }

    /// Instances for one extent, taken in turn from the ENs there are. Round
    /// robin is not placement — it is blind to fullness and to failure domains —
    /// but it is honest striping until there is a solver to ask.
    fn instances(&self, ens: &[u32], want: usize) -> Result<Vec<u32>> {
        if ens.len() < want {
            return Err(Error::new(format!(
                "an extent wants {want} instances, and there are {} ENs",
                ens.len()
            )));
        }
        let turn = self.turn.fetch_add(want as u64, Ordering::Relaxed) as usize;
        Ok((0..want).map(|n| ens[(turn + n) % ens.len()]).collect())
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
    mine: u32,
    shards: Vec<Mutex<HashMap<(u32, u64), Stream>>>,
    naming: Naming,
    streams: AtomicU64,
    extents: AtomicU64,
    sealed: AtomicU64,
    sealed_streams: AtomicU64,
}

fn shard_of(dn: u32, stream: u64) -> usize {
    // Stream ids from one DN are consecutive, so mixing the low bits spreads a
    // batch of them across every shard rather than filling one.
    (stream.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (dn as u64)) as usize % SHARDS
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
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
        if let [Arg::Number(dn), Arg::Number(first), Arg::Number(count), ..] =
            request.args.as_slice()
        {
            reply.about(format_args!("count {count} from {first} for dn {dn}"));
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
    fn new(config: SNConfig, mine: u32) -> SN {
        SN {
            config,
            mine,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            naming: Naming::new(),
            streams: AtomicU64::new(0),
            extents: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            sealed_streams: AtomicU64::new(0),
        }
    }

    /// `(DnId, first StreamId, count, creation second)`. A DN creating many
    /// objects at once names a run of streams in one request.
    fn create(&self, request: &Request) -> Response {
        let (dn, first, count, created) = match request.args.as_slice() {
            [Arg::Number(dn), Arg::Number(first), Arg::Number(count), Arg::Number(created)] => {
                (*dn as u32, *first, *count, *created)
            }
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "create-stream takes a DnId, a first StreamId, a count and a second",
                );
            }
        };

        for n in 0..count {
            let id = first + n;
            let mut shard = self.shards[shard_of(dn, id)].lock().expect("streams");
            shard.insert(
                (dn, id),
                Stream {
                    created,
                    creator: self.mine,
                    sealed: Vec::new(),
                    open: None,
                    sealed_records: 0,
                    sealed_stream: false,
                },
            );
        }
        self.streams.fetch_add(count, Ordering::Relaxed);
        Response::ok()
    }

    /// `(DnId, StreamId)`, and back comes the extent to append to. The ENs are
    /// taken from the roster, and the second from the clock.
    fn open_extent(&self, process: &Process, request: &Request) -> Response {
        let (dn, stream) = match request.args.as_slice() {
            [Arg::Number(dn), Arg::Number(stream), ..] => (*dn as u32, *stream),
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "open-extent takes a DnId and a StreamId",
                );
            }
        };
        let ens: Vec<u32> =
            process.cluster.services_of_kind(ServiceKind::En).map(|en| en.number()).collect();
        match self.open(dn, stream, now(), &ens, process.cluster.extents.instances) {
            Ok(extent) => Response { id: 0, status: Status::Ok, args: extent.args(0) },
            Err(Full { records, why: None }) => {
                Response { id: 0, status: Status::Sealed, args: vec![Arg::Number(records)] }
            }
            Err(Full { why: Some(why), .. }) => Response::error(Status::Internal, why.to_string()),
        }
    }

    /// `(DnId, StreamId, record)`, and back comes the extent whose numbering
    /// starts at or before that record. Whether the record is actually there is
    /// the EN's to say: an SN keeps its byte and record totals lazily, so it
    /// routes a read and does not bound it.
    fn locate(&self, request: &Request) -> Response {
        let (dn, stream, record) = match request.args.as_slice() {
            [Arg::Number(dn), Arg::Number(stream), Arg::Number(record), ..] => {
                (*dn as u32, *stream, *record)
            }
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "locate-record takes a DnId, a StreamId and a record",
                );
            }
        };
        let shard = self.shards[shard_of(dn, stream)].lock().expect("streams");
        let Some(held) = shard.get(&(dn, stream)) else {
            return Response::error(Status::NotFound, format!("no stream {stream} from dn {dn}"));
        };
        // The sealed extents are in record order, so this is a search over a sorted
        // run; a scan is what it costs while a stream has few extents.
        if let Some(sealed) = held.sealed.iter().find(|sealed| sealed.holds(record)) {
            return Response {
                id: 0,
                status: Status::Ok,
                args: sealed.extent.args(sealed.records),
            };
        }
        match &held.open {
            // How far the open extent reaches is not known here — an SN keeps its
            // record totals lazily — so anything at or past its first record is
            // routed to it and the EN says whether it is there.
            Some(extent) if record >= extent.first_record => {
                Response { id: 0, status: Status::Ok, args: extent.args(0) }
            }
            Some(_) | None => Response::error(
                Status::NotFound,
                format!("stream {stream} has no extent holding record {record}"),
            ),
        }
    }

    /// `(DnId, StreamId, from record, limit)`: a run of this stream's extents, in
    /// record order, starting at the one holding `from`.
    ///
    /// **Complete in counts and partial in detail** (§9.1.3.2): the extent total is
    /// this SN's own, so it costs nothing and is always right, while the rows stop at
    /// the limit and `next` says where to carry on.
    ///
    /// ⚠️ **Record numbers here are stream-local**, which is what the SN knows. The FE
    /// converts them to the caller's numbering, because it is the only party that knows
    /// all three spaces (§17.16.2).
    fn list(&self, request: &Request) -> Response {
        let (dn, stream, from, limit) = match request.args.as_slice() {
            [Arg::Number(dn), Arg::Number(stream), Arg::Number(from), Arg::Number(limit)] => {
                (*dn as u32, *stream, *from, *limit)
            }
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "list-extents takes a DnId, a StreamId, a record and a limit",
                );
            }
        };
        let shard = self.shards[shard_of(dn, stream)].lock().expect("streams");
        let Some(held) = shard.get(&(dn, stream)) else {
            return Response::error(Status::NotFound, format!("no stream {stream} from dn {dn}"));
        };

        // The sealed extents are in record order, so the first to describe is the last
        // one starting at or before `from` — the same rule locate uses to pick one.
        let first = held
            .sealed
            .iter()
            .rposition(|sealed| sealed.extent.first_record <= from)
            .map_or(0, |at| if from == 0 { 0 } else { at });
        let total = held.sealed.len() as u64 + u64::from(held.open.is_some());

        let mut rows: Vec<ExtentRow> = held
            .sealed
            .iter()
            .skip(first)
            .take(limit as usize)
            .map(|sealed| ExtentRow {
                sn: held.creator,
                id: sealed.extent.id,
                created: sealed.extent.created,
                first_record: sealed.extent.first_record,
                records: sealed.records,
                length: sealed.length,
                instances: sealed.extent.instances.clone(),
            })
            .collect();

        // Then the one still open, if there is room and it starts at or after `from`.
        // ⚠️ Its records and length are UNKNOWN rather than zero: how far it reaches is
        // not knowable here, and a sealed extent holding nothing would be a real zero
        // (§8.1.6.3).
        let sealed_seen = first + rows.len();
        if rows.len() < limit as usize
            && sealed_seen == held.sealed.len()
            && let Some(open) = &held.open
        {
            rows.push(ExtentRow {
                sn: held.creator,
                id: open.id,
                created: open.created,
                first_record: open.first_record,
                records: UNKNOWN,
                length: UNKNOWN,
                instances: open.instances.clone(),
            });
        }

        // Where to carry on: the first record of the next extent not described.
        let after = first + rows.len();
        let next = match held.sealed.get(after) {
            Some(sealed) => sealed.extent.first_record,
            None if after < total as usize => {
                held.open.as_ref().map_or(UNKNOWN, |open| open.first_record)
            }
            None => UNKNOWN,
        };

        let mut args = ExtentRun { extents: total, given: rows.len() as u64, next }.args();
        for row in rows {
            args.extend(row.args());
        }
        Response { id: 0, status: Status::Ok, args }
    }

    /// `(DnId, StreamId, sealed extent, records, length)`: record what an extent
    /// came to and open the next one, which is one round trip rather than two
    /// because whoever saw the seal wants both.
    fn seal(&self, process: &Process, request: &Request) -> Response {
        let (dn, stream, id, records, length) = match request.args.as_slice() {
            [
                Arg::Number(dn),
                Arg::Number(stream),
                Arg::Number(id),
                Arg::Number(records),
                Arg::Number(length),
            ] => (*dn as u32, *stream, *id, *records, *length),
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "seal-extent takes a DnId, a StreamId, an extent, a record count and a length",
                );
            }
        };
        let ens: Vec<u32> =
            process.cluster.services_of_kind(ServiceKind::En).map(|en| en.number()).collect();
        match self.close(
            dn,
            stream,
            id,
            records,
            length,
            now(),
            &ens,
            process.cluster.extents.instances,
            process.cluster.extents.per_stream,
        ) {
            Ok(extent) => Response { id: 0, status: Status::Ok, args: extent.args(0) },
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
        dn: u32,
        stream: u64,
        id: u64,
        records: u64,
        length: u64,
        second: u64,
        ens: &[u32],
        instances: usize,
        per_stream: usize,
    ) -> std::result::Result<Extent, Full> {
        let mut shard = self.shards[shard_of(dn, stream)].lock().expect("streams");
        let Some(held) = shard.get_mut(&(dn, stream)) else {
            return Err(Full::from(Error::new(format!("no stream {stream} from dn {dn}"))));
        };
        // Only whoever names the open extent seals it. Two appends can find the same
        // extent full at once, and the second one has nothing left to do.
        if held.open.as_ref().is_some_and(|open| open.id == id) {
            let extent = held.open.take().expect("an open extent");
            held.sealed_records = extent.first_record + records;
            held.sealed.push(Sealed { extent, records, length });
            self.sealed.fetch_add(1, Ordering::Relaxed);
            if held.sealed.len() >= per_stream {
                held.sealed_stream = true;
                self.sealed_streams.fetch_add(1, Ordering::Relaxed);
            }
        }
        if held.sealed_stream {
            return Err(Full::records(held.sealed_records));
        }
        self.appendable(held, second, ens, instances).map_err(Full::from)
    }

    /// The stream's open extent, naming one if it has none.
    fn open(
        &self,
        dn: u32,
        stream: u64,
        second: u64,
        ens: &[u32],
        instances: usize,
    ) -> std::result::Result<Extent, Full> {
        let mut shard = self.shards[shard_of(dn, stream)].lock().expect("streams");
        let Some(held) = shard.get_mut(&(dn, stream)) else {
            return Err(Full::from(Error::new(format!("no stream {stream} from dn {dn}"))));
        };
        if held.sealed_stream {
            return Err(Full::records(held.sealed_records));
        }
        self.appendable(held, second, ens, instances).map_err(Full::from)
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
    fn appendable(
        &self,
        held: &mut Stream,
        second: u64,
        ens: &[u32],
        instances: usize,
    ) -> Result<Extent> {
        if let Some(extent) = &held.open {
            return Ok(extent.clone());
        }
        let (id, created) = self.naming.name(second)?;
        let extent = Extent {
            id,
            created,
            first_record: held.sealed_records,
            instances: self.naming.instances(ens, instances)?,
        };
        held.open = Some(extent.clone());
        self.extents.fetch_add(1, Ordering::Relaxed);
        Ok(extent)
    }
}

fn main() {
    let result = server::main(|process| {
        Ok(SN::new(SNConfig::load(&process.config.service_config)?, process.entry.number()))
    });
    if let Err(e) = result {
        eprintln!("sn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
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
        let each = size_of::<Sealed>() + 4 * size_of::<u32>();
        assert_eq!(each, 80, "if this moved, §4.7 and issue #9 want to hear about it");
        for bits in [24u32, 27, 32] {
            println!(
                "2^{bits} extents in one stream = {:.0} GB",
                each as f64 * 2f64.powi(bits as i32) / 1e9
            );
        }
        // Against the design's reference width, the same list is a different object.
        assert_eq!(each / 4, 20, "twenty times what §4.7 budgets for a reference");
    }

    use super::*;
    use common::testing;

    /// Extents enough that a stream does not seal in the tests that are not about
    /// a stream sealing.
    const ROOMY: usize = 1000;

    /// A stream, so there is something to open an extent on.
    fn sn_with_stream(dn: u32, stream: u64) -> SN {
        let sn = SN::new(testing::sn_config(), 0);
        let made = sn.create(
            &Request::new(Op::CreateStream)
                .number(dn as u64)
                .number(stream)
                .number(1)
                .number(1_000_000),
        );
        assert_eq!(made.status, Status::Ok);
        sn
    }

    #[test]
    fn a_stream_opens_one_extent_however_often_it_is_asked() {
        let sn = sn_with_stream(0, 7);
        let ens = [0, 1, 2, 3];
        let want = testing::extents().instances;
        let first = sn.open(0, 7, 1_000_000, &ens, want).unwrap();
        let again = sn.open(0, 7, 1_000_001, &ens, want).unwrap();
        assert_eq!(first.id, again.id, "the second ask should find the open extent");
        assert_eq!(first.created, again.created);
        assert_eq!(sn.extents.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn an_extent_on_a_stream_that_does_not_exist_is_refused() {
        let sn = SN::new(testing::sn_config(), 0);
        assert!(sn.open(0, 7, 1_000_000, &[0, 1, 2, 3], 4).is_err());
    }

    /// Sealing records what the extent came to and names the next one, whose
    /// numbering starts where the last one stopped.
    #[test]
    fn sealing_starts_the_next_extent_where_the_last_one_stopped() {
        let sn = sn_with_stream(0, 7);
        let ens = [0, 1, 2, 3];
        let want = testing::extents().instances;
        let first = sn.open(0, 7, 1_000_000, &ens, want).unwrap();
        assert_eq!(first.first_record, 0);

        // Twelve records in it, and 400 bytes.
        let second = sn.close(0, 7, first.id, 12, 400, 1_000_001, &ens, want, ROOMY).unwrap();
        assert_ne!(second.id, first.id, "a fresh extent");
        assert_eq!(second.first_record, 12, "numbering carries on");

        let third = sn.close(0, 7, second.id, 5, 90, 1_000_002, &ens, want, ROOMY).unwrap();
        assert_eq!(third.first_record, 17);
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 2);
    }

    /// An extent already sealed by somebody else does not fail: the caller wanted
    /// somewhere to append, and the extent that replaced it is the answer. This is
    /// the race the stream's lock exists to make harmless.
    #[test]
    fn sealing_an_extent_that_is_already_gone_gives_the_one_that_replaced_it() {
        let sn = sn_with_stream(0, 7);
        let ens = [0, 1, 2, 3];
        let want = testing::extents().instances;
        let first = sn.open(0, 7, 1_000_000, &ens, want).unwrap();
        let second = sn.close(0, 7, first.id, 4, 100, 1_000_001, &ens, want, ROOMY).unwrap();
        // A request that saw the seal a moment later, naming the extent that is now
        // sealed, and one naming an extent this stream never had.
        assert_eq!(
            sn.close(0, 7, first.id, 4, 100, 1_000_002, &ens, want, ROOMY).unwrap().id,
            second.id
        );
        assert_eq!(sn.close(0, 7, 999, 0, 0, 1_000_003, &ens, want, ROOMY).unwrap().id, second.id);
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 1, "sealed once");
        assert_eq!(sn.extents.load(Ordering::Relaxed), 2, "two extents, not four");
    }

    /// Two appends can find the same extent full at once, so sealing it twice must
    /// not open two extents. The second one gets whatever is open now.
    #[test]
    fn sealing_the_same_extent_twice_opens_one_extent() {
        let sn = sn_with_stream(0, 7);
        let ens = [0, 1, 2, 3];
        let want = testing::extents().instances;
        let first = sn.open(0, 7, 1_000_000, &ens, want).unwrap();
        let second = sn.close(0, 7, first.id, 4, 100, 1_000_001, &ens, want, ROOMY).unwrap();
        let repeat = sn.close(0, 7, first.id, 4, 100, 1_000_002, &ens, want, ROOMY).unwrap();
        assert_eq!(repeat.id, second.id, "the extent that is open now");
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 1, "sealed once");
    }

    /// A stream seals when it has all the extents it is allowed, and then it will
    /// take no more — which is the object's problem, not this stream's.
    #[test]
    fn a_stream_seals_once_it_has_its_extents() {
        let sn = sn_with_stream(0, 7);
        let ens = [0, 1, 2, 3];
        let want = testing::extents().instances;
        // Room for two extents, so the second seal fills the stream.
        let two = 2;
        let first = sn.open(0, 7, 1_000_000, &ens, want).unwrap();
        let second = sn.close(0, 7, first.id, 10, 100, 1_000_001, &ens, want, two).unwrap();
        assert_eq!(second.first_record, 10);

        let full = sn
            .close(0, 7, second.id, 7, 70, 1_000_002, &ens, want, two)
            .expect_err("the stream should be full");
        assert!(full.why.is_none(), "full is not a failure");
        assert_eq!(full.records, 17, "what the whole stream came to");
        assert_eq!(sn.sealed_streams.load(Ordering::Relaxed), 1);

        // And it stays sealed: nothing opens another extent on it.
        let again = sn.open(0, 7, 1_000_003, &ens, want).expect_err("sealed");
        assert_eq!(again.records, 17);
        assert!(again.why.is_none());
    }

    /// A record in a sealed extent is found there; one past everything sealed goes to
    /// the open extent, because how far that reaches is the EN's to say.
    #[test]
    fn a_record_is_found_in_the_extent_holding_it() {
        let sn = sn_with_stream(0, 7);
        let ens = [0, 1, 2, 3];
        let want = testing::extents().instances;
        let first = sn.open(0, 7, 1_000_000, &ens, want).unwrap();
        let second = sn.close(0, 7, first.id, 3, 60, 1_000_001, &ens, want, ROOMY).unwrap();
        let third = sn.close(0, 7, second.id, 2, 40, 1_000_002, &ens, want, ROOMY).unwrap();

        let holds = |record: u64| {
            let answer =
                sn.locate(&Request::new(Op::LocateRecord).number(0).number(7).number(record));
            assert_eq!(answer.status, Status::Ok, "record {record}");
            match answer.args.first() {
                Some(Arg::Number(id)) => *id,
                _ => panic!("no extent"),
            }
        };
        // Records 0 to 2 are in the first extent, 3 and 4 in the second, and
        // anything from 5 on is routed to the one still open.
        assert_eq!(holds(0), first.id);
        assert_eq!(holds(2), first.id);
        assert_eq!(holds(3), second.id);
        assert_eq!(holds(4), second.id);
        assert_eq!(holds(5), third.id);
        assert_eq!(holds(500), third.id, "the EN says whether it is really there");
    }

    /// A stream with no extent at all has nowhere to look.
    #[test]
    fn a_record_in_a_stream_with_no_extent_is_not_found() {
        let sn = sn_with_stream(0, 7);
        let answer = sn.locate(&Request::new(Op::LocateRecord).number(0).number(7).number(0));
        assert_eq!(answer.status, Status::NotFound);
    }

    /// The second names the key, so an id must never carry one that has already
    /// been passed.
    #[test]
    fn a_second_may_not_go_backwards() {
        let naming = Naming::new();
        assert!(naming.name(1_000_000).is_ok());
        assert!(naming.name(1_000_005).is_ok());
        assert!(naming.name(999_999).is_err());
        // And the refusal did not lose the position it had reached.
        assert!(naming.name(1_000_005).is_ok());
    }

    #[test]
    fn every_extent_gets_a_name_of_its_own() {
        let naming = Naming::new();
        let (first, _) = naming.name(1_000_000).unwrap();
        let (second, _) = naming.name(1_000_000).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn instances_are_four_distinct_ens_taken_in_turn() {
        let naming = Naming::new();
        let ens = [0, 1, 2, 3, 4, 5, 6];
        for _ in 0..20 {
            let picked = naming.instances(&ens, testing::extents().instances).unwrap();
            assert_eq!(picked.len(), testing::extents().instances);
            let mut sorted = picked.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), testing::extents().instances, "{picked:?} repeats an EN");
        }
    }

    /// Fewer ENs than an extent has instances cannot be papered over by repeating
    /// one: four instances on three machines is three failure domains.
    #[test]
    fn too_few_ens_is_an_error_not_a_repeat() {
        let naming = Naming::new();
        assert!(naming.instances(&[0, 1, 2], testing::extents().instances).is_err());
    }

    /// The four ENs in the default roster are exactly what an extent needs.
    #[test]
    fn the_test_roster_has_an_extent_worth_of_ens() {
        let cluster = testing::cluster();
        let ens: Vec<u32> =
            cluster.services_of_kind(ServiceKind::En).map(|en| en.number()).collect();
        assert_eq!(ens.len(), testing::extents().instances);
        assert!(Naming::new().instances(&ens, testing::extents().instances).is_ok());
    }
}
