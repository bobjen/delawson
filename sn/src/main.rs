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
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SERVES: &[Op] = &[Op::CreateStream, Op::OpenExtent, Op::LocateRecord];

/// Locks over the stream table. Enough that sixteen cores rarely collide,
/// few enough to walk when something wants a total.
const SHARDS: usize = 64;

struct Stream {
    /// Seconds since the epoch. Nothing reads it yet; whether a stream needs a
    /// creation timestamp of its own at all is an open question.
    #[allow(dead_code)]
    created: u64,
    /// The extent appends to this stream are landing in, once anything has asked
    /// for one. A stream that has only been created has none.
    open: Option<Extent>,
    /// Records in this stream's sealed extents, which is where the open extent's
    /// numbering starts from. Nothing seals yet, so it is always zero — and when
    /// sealing arrives this is what has to be right.
    sealed_records: u64,
}

/// Where an extent is, under the name this SN gave it.
#[derive(Clone)]
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
    /// The id, the second, the first record, then the instances with the primary
    /// first, which is the order an FE needs them in.
    fn args(&self) -> Vec<Arg> {
        let mut args =
            vec![Arg::Number(self.id), Arg::Number(self.created), Arg::Number(self.first_record)];
        args.extend(self.instances.iter().map(|en| Arg::Number(*en as u64)));
        args
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
    shards: Vec<Mutex<HashMap<(u32, u64), Stream>>>,
    naming: Naming,
    streams: AtomicU64,
    extents: AtomicU64,
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
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("streams {}", self.streams.load(Ordering::Relaxed)),
            format!("extents {}", self.extents.load(Ordering::Relaxed)),
            format!("stream_share {}", self.config.stream_share),
        ]
    }
}

impl SN {
    fn new(config: SNConfig) -> SN {
        SN {
            config,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            naming: Naming::new(),
            streams: AtomicU64::new(0),
            extents: AtomicU64::new(0),
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
            shard.insert((dn, id), Stream { created, open: None, sealed_records: 0 });
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
            Ok(extent) => Response { id: 0, status: Status::Ok, args: extent.args() },
            Err(e) => Response::error(Status::Internal, e.to_string()),
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
        match &held.open {
            Some(extent) if record >= extent.first_record => {
                Response { id: 0, status: Status::Ok, args: extent.args() }
            }
            // Before the open extent means a sealed one, and nothing seals yet, so
            // there is nowhere else for it to be.
            Some(_) | None => Response::error(
                Status::NotFound,
                format!("stream {stream} has no extent holding record {record}"),
            ),
        }
    }

    /// The stream's open extent, naming one if it has none. Holding the shard
    /// across the naming is what stops two appends to a fresh stream from opening
    /// two extents for it.
    fn open(
        &self,
        dn: u32,
        stream: u64,
        second: u64,
        ens: &[u32],
        instances: usize,
    ) -> Result<Extent> {
        let mut shard = self.shards[shard_of(dn, stream)].lock().expect("streams");
        let Some(record) = shard.get_mut(&(dn, stream)) else {
            return Err(Error::new(format!("no stream {stream} from dn {dn}")));
        };
        if let Some(extent) = &record.open {
            return Ok(extent.clone());
        }
        let (id, created) = self.naming.name(second)?;
        let extent = Extent {
            id,
            created,
            first_record: record.sealed_records,
            instances: self.naming.instances(ens, instances)?,
        };
        record.open = Some(extent.clone());
        self.extents.fetch_add(1, Ordering::Relaxed);
        Ok(extent)
    }
}

fn main() {
    let result =
        server::main(|process| Ok(SN::new(SNConfig::load(&process.config.service_config)?)));
    if let Err(e) = result {
        eprintln!("sn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testing;

    /// A stream, so there is something to open an extent on.
    fn sn_with_stream(dn: u32, stream: u64) -> SN {
        let sn = SN::new(testing::sn_config());
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
        let sn = SN::new(testing::sn_config());
        assert!(sn.open(0, 7, 1_000_000, &[0, 1, 2, 3], 4).is_err());
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
