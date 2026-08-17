//! SN, the Stream Node: the extents of a stream and the instances of an extent.
//!
//! A stream is named by the DN that owns it, so SN looks one up by
//! `(DnId, StreamId)` and allocates neither. Extents do not exist yet; a stream
//! here is a record saying it was created and by whom.
//!
//! State is in memory and sharded by stream id, so creating streams from every
//! core does not queue on one lock. Nothing is deleted yet, so nothing here has
//! to cope with reuse — an index that has to survive churn is a later problem.

use common::config::SNConfig;
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SERVES: &[Op] = &[Op::CreateStream];

/// Locks over the stream table. Enough that sixteen cores rarely collide,
/// few enough to walk when something wants a total.
const SHARDS: usize = 64;

struct Stream {
    /// Seconds since the epoch. Nothing reads it yet; an extent created in this
    /// stream will, since an extent id carries the second it was made.
    #[allow(dead_code)]
    created: u64,
}

struct SN {
    config: SNConfig,
    shards: Vec<Mutex<HashMap<(u32, u64), Stream>>>,
    streams: AtomicU64,
}

fn shard_of(dn: u32, stream: u64) -> usize {
    // Stream ids from one DN are consecutive, so mixing the low bits spreads a
    // batch of them across every shard rather than filling one.
    (stream.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (dn as u64)) as usize % SHARDS
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
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("streams {}", self.streams.load(Ordering::Relaxed)),
            format!("stream_share {}", self.config.stream_share),
        ]
    }
}

impl SN {
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
            shard.insert((dn, id), Stream { created });
        }
        self.streams.fetch_add(count, Ordering::Relaxed);
        Response::ok()
    }
}

fn main() {
    let result = server::main(|process| {
        let config = SNConfig::load(&process.config.service_config)?;
        Ok(SN {
            config,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            streams: AtomicU64::new(0),
        })
    });
    if let Err(e) = result {
        eprintln!("sn: {e}");
        std::process::exit(1);
    }
}
