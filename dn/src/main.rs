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
use common::framing::Framing;
use common::logw;
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
    /// Where one record ends and the next begins, declared here and never
    /// changed: everything that reads or writes this object's records needs it.
    framing: Framing,
    /// The one stream this object is made of, on the SN that recorded it. Zero
    /// while the name is taken but the stream is not yet named, which is what
    /// stops two creates of one name from making two streams.
    stream: u64,
    sn: u32,
    keyed: bool,
    created: u64,
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

/// Requests parked until an object's stream exists, keyed by name.
type Waiting = HashMap<Box<str>, Vec<Reply>>;

struct DN {
    config: DNConfig,
    shards: Vec<Mutex<HashMap<Box<str>, Object>>>,
    /// Lookups that arrived while an object was half made: the name is taken and
    /// its stream is not named yet. Whoever took the name is asking an SN for the
    /// stream, and these are handed the answer when it arrives rather than being
    /// told the object has no stream — which is true for a moment and useless to
    /// hear.
    waiting: Mutex<Waiting>,
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
        // A lookup can park behind a create, so its answer need not come from the
        // thread that read it either.
        matches!(op, Op::CreateObject | Op::BatchCreate | Op::ObjectInfo)
    }

    /// A lookup is a hash and a read, which is not worth a queue hop. Taking
    /// names for a create is, since a batch of a thousand takes a thousand locks.
    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::ObjectInfo | Op::BatchInfo | Op::Ping | Op::Status | Op::Shutdown | Op::Sites
        )
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        match request.op {
            Op::CreateObject => self.create(process, &request, reply),
            Op::BatchCreate => self.create_many(process, &request, reply),
            Op::ObjectInfo => self.info(&request, reply),
            Op::BatchInfo => reply.send(self.info_many(&request)),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("objects {}", self.objects.load(Ordering::Relaxed)),
            format!("waiting_for_a_stream {}", self.waiting.lock().expect("waiting").len()),
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
                        stream: 0,
                        sn: 0,
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
                    object.stream = first + n as u64;
                    object.sn = sn_number;
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

    /// Release names taken for objects that could not be given a stream.
    fn give_back(&self, names: &[String], status: Status, why: String) -> Response {
        for name in names {
            self.shards[shard_of(name)].lock().expect("objects").remove(name.as_str());
        }
        Response::error(status, why)
    }

    fn info(&self, request: &Request, reply: Reply) {
        let name = match request.args.first() {
            Some(Arg::Text(name)) => name.as_str(),
            _ => {
                return reply.send(Response::error(Status::BadRequest, "object-info takes a name"));
            }
        };
        match self.look(name) {
            Some(args) => reply.send(Response { id: 0, status: Status::Ok, args }),
            None if self.taken(name) => {
                // Half made: park behind whoever is naming its stream.
                self.waiting
                    .lock()
                    .expect("waiting")
                    .entry(Box::from(name))
                    .or_default()
                    .push(reply);
            }
            None => reply.send(Response::error(Status::NotFound, format!("no object {name}"))),
        }
    }

    /// Whether the name is taken by an object whose stream is not named yet.
    fn taken(&self, name: &str) -> bool {
        let shard = self.shards[shard_of(name)].lock().expect("objects");
        shard.get(name).is_some_and(|object| object.stream == 0)
    }

    /// Answer everything parked on these names, now that their streams exist. A name
    /// whose create failed is answered too, with whatever the failure was, so nothing
    /// waits for an answer that is never coming.
    fn wake(&self, names: &[String], failed: Option<&Response>) {
        for name in names {
            let parked = self.waiting.lock().expect("waiting").remove(name.as_str());
            for reply in parked.unwrap_or_default() {
                match (failed, self.look(name)) {
                    (None, Some(args)) => reply.send(Response { id: 0, status: Status::Ok, args }),
                    (failed, _) => reply.send(match failed {
                        Some(answer) => answer.clone(),
                        None => Response::error(
                            Status::Internal,
                            format!("{name} was made and then lost"),
                        ),
                    }),
                }
            }
        }
    }

    /// One answer for many names, in the order asked. A name that is not there
    /// gets a zero id, so the answer lines up with the question without saying
    /// which was missing twice.
    fn info_many(&self, request: &Request) -> Response {
        let mut args = Vec::with_capacity(request.args.len() * 7);
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
                    Arg::Number(0),
                    Arg::Number(0),
                ]),
            }
        }
        Response { id: 0, status: Status::Ok, args }
    }

    /// What an object is: id, stream, the SN holding it, the second it was made,
    /// its flavour, and its framing as a kind and a width.
    fn look(&self, name: &str) -> Option<Vec<Arg>> {
        let shard = self.shards[shard_of(name)].lock().expect("objects");
        let object = shard.get(name)?;
        // A name with no stream is an object that is still being made, and saying so
        // is worse than useless — whoever asked would have to guess how long to wait.
        if object.stream == 0 {
            return None;
        }
        let (kind, width) = object.framing.wire();
        Some(vec![
            Arg::Number(object.id),
            Arg::Number(object.stream),
            Arg::Number(object.sn as u64),
            Arg::Number(object.created),
            Arg::Flag(object.keyed),
            Arg::Number(kind),
            Arg::Number(width),
        ])
    }
}

impl DN {
    fn new(config: DNConfig) -> DN {
        DN {
            config,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            waiting: Mutex::new(HashMap::new()),
            objects: AtomicU64::new(0),
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
    use super::*;
    use common::testing;

    /// An object as `record` leaves it before the SN has answered: the name is
    /// taken and the stream is not named.
    fn half_made(dn: &DN, name: &str) {
        dn.shards[shard_of(name)].lock().unwrap().insert(
            Box::from(name),
            Object {
                id: 1,
                framing: Framing::Lf,
                stream: 0,
                sn: 0,
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
        assert!(dn.taken("/half"), "but the name is taken");
        assert!(!dn.taken("/nothing"), "unlike a name nobody has");

        // Once the stream is named it describes normally.
        dn.shards[shard_of("/half")].lock().unwrap().get_mut("/half").unwrap().stream = 7;
        let args = dn.look("/half").expect("an object");
        assert_eq!(args[1], Arg::Number(7), "its stream");
        assert!(!dn.taken("/half"), "and it is no longer half made");
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
                        stream: 0,
                        sn: 0,
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
