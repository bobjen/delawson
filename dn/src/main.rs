//! DN, the Directory Node: objects by name, and the streams they are made of.
//!
//! An object is indexed by name here, and every object gets one stream on an SN —
//! no inlining of small objects and no coalescing of several objects into one
//! stream, both of which come later and both of which would hide what a create
//! costs. DN owns the `StreamId`; the SN it picks records `(DnId, StreamId)`.
//!
//! State is in memory and sharded by name. Nothing is deleted yet, so nothing
//! here copes with reuse; an index that has to survive churn is a later problem.
//!
//! A create waits on an SN, and waits without holding a worker: the names are
//! taken, the streams are asked for, and the client's `Reply` travels with that
//! request to be answered when the SN says the streams exist.

use common::config::{DNConfig, ServiceKind};
use common::logw;
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SERVES: &[Op] = &[
    Op::CreateObject,
    Op::BatchCreate,
    Op::ObjectInfo,
    Op::BatchInfo,
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

struct Object {
    id: u64,
    /// The one stream this object is made of, on the SN that recorded it. Zero
    /// while the name is taken but the stream is not yet named, which is what
    /// stops two creates of one name from making two streams.
    stream: u64,
    sn: u32,
    keyed: bool,
    created: u64,
}

struct DN {
    config: DNConfig,
    shards: Vec<Mutex<HashMap<Box<str>, Object>>>,
    objects: AtomicU64,
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
        matches!(op, Op::CreateObject | Op::BatchCreate)
    }

    /// A lookup is a hash and a read, which is not worth a queue hop. Taking
    /// names for a create is, since a batch of a thousand takes a thousand locks.
    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::ObjectInfo | Op::BatchInfo | Op::Ping | Op::Status | Op::Shutdown | Op::Histogram
        )
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        match request.op {
            Op::CreateObject => self.create(process, &request, reply),
            Op::BatchCreate => self.create_many(process, &request, reply),
            Op::ObjectInfo => reply.send(self.info(&request)),
            Op::BatchInfo => reply.send(self.info_many(&request)),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("objects {}", self.objects.load(Ordering::Relaxed)),
            format!("self_hosted {}", self.config.self_hosted),
        ]
    }
}

impl DN {
    /// `(name, keyed)`, and one stream to go with it.
    fn create(self: &Arc<Self>, process: &Arc<Process>, request: &Request, reply: Reply) {
        let (name, keyed) = match request.args.as_slice() {
            [Arg::Text(name), Arg::Flag(keyed), ..] => (name.clone(), *keyed),
            _ => {
                return reply.send(Response::error(
                    Status::BadRequest,
                    "create takes a name and a flavour",
                ));
            }
        };
        let existed = format!("{name} exists already");
        self.record(process, vec![(name, keyed)], reply, move |made| {
            if made == 0 { Response::error(Status::BadRequest, existed) } else { Response::ok() }
        });
    }

    /// A run of `(name, keyed)` pairs, which costs one stream request and one
    /// answer however many objects it holds.
    fn create_many(self: &Arc<Self>, process: &Arc<Process>, request: &Request, reply: Reply) {
        let mut wanted = Vec::with_capacity(request.args.len() / 2);
        for pair in request.args.chunks(2) {
            match pair {
                [Arg::Text(name), Arg::Flag(keyed)] => wanted.push((name.clone(), *keyed)),
                _ => {
                    return reply.send(Response::error(
                        Status::BadRequest,
                        "batch-create takes name and flavour pairs",
                    ));
                }
            }
        }
        self.record(process, wanted, reply, |made| Response {
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
        wanted: Vec<(String, bool)>,
        reply: Reply,
        say: impl FnOnce(u64) -> Response + Send + 'static,
    ) {
        let created = now();

        // Whoever inserts the name owns the object; anyone else finds it there.
        let mut mine = Vec::with_capacity(wanted.len());
        for (name, keyed) in &wanted {
            let mut shard = self.shards[shard_of(name)].lock().expect("objects");
            if !shard.contains_key(name.as_str()) {
                let id = self.next_object.fetch_add(1, Ordering::Relaxed);
                shard.insert(
                    Box::from(name.as_str()),
                    Object { id, stream: 0, sn: 0, keyed: *keyed, created },
                );
                mine.push(name.clone());
            }
        }
        if mine.is_empty() {
            return reply.send(say(0));
        }

        let count = mine.len() as u64;
        let first = self.next_stream.fetch_add(count, Ordering::Relaxed);
        let sn = match process.pick(ServiceKind::Sn) {
            Ok(sn) => sn.to_string(),
            Err(e) => {
                return reply.send(self.give_back(&mine, Status::Internal, e.to_string()));
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
                return reply.send(dn.give_back(&mine, answer.status, why));
            }
            for (n, name) in mine.iter().enumerate() {
                let mut shard = dn.shards[shard_of(name)].lock().expect("objects");
                if let Some(object) = shard.get_mut(name.as_str()) {
                    object.stream = first + n as u64;
                    object.sn = sn_number;
                }
            }
            dn.objects.fetch_add(count, Ordering::Relaxed);
            reply.send(say(count));
        });
        if let Err(e) = asked {
            logw!("could not reach {sn}: {e}");
        }
    }

    /// Release names taken for objects that could not be given a stream.
    fn give_back(&self, names: &[String], status: Status, why: String) -> Response {
        for name in names {
            self.shards[shard_of(name)].lock().expect("objects").remove(name.as_str());
        }
        Response::error(status, why)
    }

    fn info(&self, request: &Request) -> Response {
        let name = match request.args.first() {
            Some(Arg::Text(name)) => name.as_str(),
            _ => return Response::error(Status::BadRequest, "object-info takes a name"),
        };
        match self.look(name) {
            Some(args) => Response { id: 0, status: Status::Ok, args },
            None => Response::error(Status::NotFound, format!("no object {name}")),
        }
    }

    /// One answer for many names, in the order asked. A name that is not there
    /// gets a zero id, so the answer lines up with the question without saying
    /// which was missing twice.
    fn info_many(&self, request: &Request) -> Response {
        let mut args = Vec::with_capacity(request.args.len() * 4);
        for asked in &request.args {
            let name = match asked {
                Arg::Text(name) => name.as_str(),
                _ => {
                    return Response::error(Status::BadRequest, "batch-object-info takes names");
                }
            };
            match self.look(name) {
                Some(found) => args.extend(found),
                None => args.extend([
                    Arg::Number(0),
                    Arg::Number(0),
                    Arg::Number(0),
                    Arg::Number(0),
                    Arg::Flag(false),
                ]),
            }
        }
        Response { id: 0, status: Status::Ok, args }
    }

    /// What an object is, as five values: id, stream, the SN holding it, the
    /// second it was made, and its flavour.
    fn look(&self, name: &str) -> Option<Vec<Arg>> {
        let shard = self.shards[shard_of(name)].lock().expect("objects");
        let object = shard.get(name)?;
        Some(vec![
            Arg::Number(object.id),
            Arg::Number(object.stream),
            Arg::Number(object.sn as u64),
            Arg::Number(object.created),
            Arg::Flag(object.keyed),
        ])
    }
}

fn main() {
    let result = server::main(|process| {
        let config = DNConfig::load(&process.config.service_config)?;
        Ok(DN {
            config,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            objects: AtomicU64::new(0),
            next_object: AtomicU64::new(1),
            next_stream: AtomicU64::new(1),
        })
    });
    if let Err(e) = result {
        eprintln!("dn: {e}");
        std::process::exit(1);
    }
}
