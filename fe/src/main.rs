//! FE, the Front End: authorization, and coordinating a client's request across
//! the tiers.
//!
//! Authorization does not exist yet, so what FE does today is forward and
//! coordinate. A create or a lookup goes to a DN, and the DN goes on to an SN. An
//! append takes three hops: the DN says which stream the object is, the SN says
//! which extent that stream is appending to and where its instances are, and the
//! primary instance takes the bytes.
//!
//! **A forward holds nothing while it waits.** The request is handed on with the
//! client's `Reply` attached to the answer, and the read loop goes back to reading.
//! What is in flight is a table entry per request, not a thread, which is the
//! difference between a forwarding tier that costs a forward and one that costs a
//! thread.
//!
//! **The extent a stream is appending to is remembered, so the SN is asked once per
//! extent and not once per append.** The SN places instances and then gets out of
//! the write path, which it cannot do if every write consults it. Losing what is
//! remembered costs one ask and nothing else, so an FE holding it is still an FE
//! that holds no state.

use common::block::Holds;
use common::config::{FEConfig, ServiceKind};
use common::framing::Framing;
use common::logw;
use common::server::{self, Process, Reply, Service};
use common::wire::{AppendBlock, Arg, Op, Request, Response, Status};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const SERVES: &[Op] = &[
    Op::MakeDirectory,
    Op::RemoveDirectory,
    Op::List,
    Op::Stat,
    Op::CreateObject,
    Op::BatchCreate,
    Op::ObjectInfo,
    Op::BatchInfo,
    Op::DeleteObject,
    Op::UndeleteObject,
    Op::SetExpiry,
    Op::CloneObject,
    Op::ConcatObjects,
    Op::AppendRecord,
    Op::InsertRecord,
    Op::UpdateRecord,
    Op::DeleteRecord,
    Op::ReadRecord,
    Op::Scan,
    Op::Count,
    Op::TimeRange,
];

/// What a DN can answer on its own. The rest of `SERVES` is here so that a client
/// sending it is told it is unbuilt rather than misdirected.
const FORWARDED: &[Op] = &[Op::CreateObject, Op::BatchCreate, Op::ObjectInfo, Op::BatchInfo];

/// How many sealed extents an append will step over before giving up. More than one
/// because other writers are filling extents at the same time; bounded because
/// stepping over them for ever would be a livelock rather than progress.
const TRIES: u32 = 8;

/// What FE coordinates rather than forwards.
const COORDINATED: &[Op] = &[Op::AppendRecord, Op::ReadRecord];

/// An extent, as the SN described it.
#[derive(Clone)]
struct Open {
    sn: u32,
    id: u64,
    created: u64,
    /// The stream-local number of this extent's first record, which is what turns an
    /// extent-local number into a durable reference.
    first_record: u64,
    /// Records in it, or zero for the extent still being appended to — whose count
    /// changes with every append and is therefore not a thing to remember.
    records: u64,
    /// The instances, primary first.
    instances: Vec<u32>,
}

impl Open {
    /// The id, the second, the first record, then the instances, which is the order
    /// the SN answers in.
    fn of(sn: u32, args: &[Arg]) -> Option<Open> {
        let [
            Arg::Number(id),
            Arg::Number(created),
            Arg::Number(first_record),
            Arg::Number(records),
            instances @ ..,
        ] = args
        else {
            return None;
        };
        let instances: Vec<u32> = instances
            .iter()
            .filter_map(|arg| match arg {
                Arg::Number(en) => Some(*en as u32),
                _ => None,
            })
            .collect();
        (!instances.is_empty()).then_some(Open {
            sn,
            id: *id,
            created: *created,
            first_record: *first_record,
            records: *records,
            instances,
        })
    }

    /// Whether a stream-local record is in this extent. False for an extent whose
    /// count is not known, which is the one still being appended to.
    fn holds(&self, record: u64) -> bool {
        self.records > 0 && record >= self.first_record && record < self.first_record + self.records
    }
}

/// An append waiting for an extent to append to.
struct Wanting {
    path: String,
    located: Located,
    body: Vec<u8>,
    /// What the writer said the block holds: logical size, records, whether one is
    /// left open, and the CRC. Carried, never invented and never recomputed — below
    /// the client the bytes are compressed and none of it could be worked out again.
    holds: Holds,
    tries: u32,
    reply: Reply,
}

/// What the FE remembers about one stream. Nothing here is state in the sense that
/// matters: losing all of it costs asks, not correctness.
#[derive(Default)]
struct Stream {
    /// The extent appends are landing in.
    open: Option<Open>,
    /// Extents that have sealed, so a read knows where a record lives without asking
    /// the SN. Learned two ways: by being the one to report a seal, and from a read
    /// the SN answered — since an SN naming a sealed extent says how much it holds.
    sealed: Vec<Open>,
    /// **Set means a request for the next extent is in flight**, and the appends in
    /// it are waiting for that one answer rather than each asking for themselves.
    getting: Option<Vec<Wanting>>,
}

/// What an append should do about finding somewhere to put itself.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    /// Use the extent that is open.
    Use,
    /// Ask the SN for the next one; nobody else is asking.
    Ask,
    /// Somebody is already asking, so join them.
    Join,
}

impl Stream {
    /// Decide what an append should do, and record what the decision implies.
    ///
    /// `sealed` is `(extent, records)` when the caller has just been told that extent
    /// filled up. Remembering it here rather than forgetting it is what lets a read of
    /// those records skip the SN afterwards.
    fn plan(&mut self, sealed: Option<(u64, u64)>) -> Plan {
        let stale = sealed.map(|(id, _)| id);
        if self.open.is_some() && self.open.as_ref().map(|open| open.id) != stale {
            // Either nothing has filled, or somebody has already replaced what did.
            return Plan::Use;
        }
        if let Some((id, records)) = sealed
            && let Some(mut open) = self.open.take().filter(|open| open.id == id)
        {
            open.records = records;
            self.remember(open);
        }
        match &self.getting {
            Some(_) => Plan::Join,
            None => {
                self.getting = Some(Vec::new());
                Plan::Ask
            }
        }
    }

    /// The sealed extent holding a stream-local record, if this FE knows of it.
    fn holding(&self, record: u64) -> Option<Open> {
        self.sealed.iter().find(|extent| extent.holds(record)).cloned()
    }

    /// Keep a sealed extent, unless it is already kept. Two appends and any number of
    /// reads can each learn the same one.
    fn remember(&mut self, extent: Open) {
        if extent.records > 0 && !self.sealed.iter().any(|kept| kept.id == extent.id) {
            self.sealed.push(extent);
        }
    }
}

struct FE {
    config: FEConfig,
    /// Per `(DnId, StreamId)`.
    streams: Mutex<HashMap<(u32, u64), Stream>>,
}

impl Service for FE {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    /// Everything FE does is answered by somebody else, so no answer comes from
    /// the thread that read the request.
    fn defers(&self, op: Op) -> bool {
        FORWARDED.contains(&op) || COORDINATED.contains(&op)
    }

    /// Issuing the first hop is a write, not work, so it happens on the read loop.
    fn inline(&self, op: Op) -> bool {
        self.defers(op) || matches!(op, Op::Ping | Op::Status | Op::Shutdown | Op::Sites)
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, mut reply: Reply) {
        match request.op {
            Op::AppendRecord => return self.append(process, &request, reply),
            Op::ReadRecord => return self.read(process, &request, reply),
            _ => {}
        }
        if !FORWARDED.contains(&request.op) {
            reply.send(server::unbuilt(process, SERVES, &request));
            return;
        }
        let dn = match process.pick(ServiceKind::Dn) {
            Ok(dn) => dn.to_string(),
            Err(e) => return reply.send(Response::error(Status::Internal, e.to_string())),
        };
        // Which DN it went to belongs on the line, since that is the hop a
        // live-site argument is about.
        reply.about(format_args!("{},dn {dn}", first(&request)));
        // The answer is whatever the DN says, since FE adds nothing to it yet.
        if let Err(e) = process.ask(&dn, &request, move |answer| reply.send(answer)) {
            // The reply went into the closure, so this needs its own.
            logw!("could not reach a DN", "dn {dn},{e}");
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        let streams = self.streams.lock().expect("streams");
        let sealed: usize = streams.values().map(|stream| stream.sealed.len()).sum();
        let waiting: usize =
            streams.values().filter_map(|stream| stream.getting.as_ref().map(Vec::len)).sum();
        vec![
            format!("stateless {}", self.config.stateless),
            format!("open_extents {}", streams.len()),
            format!("known_sealed_extents {sealed}"),
            format!("appends_waiting_for_an_extent {waiting}"),
        ]
    }
}

impl FE {
    fn new(config: FEConfig) -> FE {
        FE { config, streams: Mutex::new(HashMap::new()) }
    }

    /// What a DN says an object is. Every coordinated operation starts here,
    /// because a path is not something the tiers below the DN know about.
    fn resolve(
        self: &Arc<Self>,
        process: &Arc<Process>,
        path: &str,
        make: Option<Framing>,
        reply: Reply,
        then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
    ) {
        self.resolving(process, path, None, make, true, reply, then)
    }

    /// The stream holding a record, rather than the one being appended to. Same
    /// shape of answer, different question — an object is several streams and the
    /// DN is what knows which.
    fn resolve_record(
        self: &Arc<Self>,
        process: &Arc<Process>,
        path: &str,
        record: u64,
        reply: Reply,
        then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
    ) {
        self.resolving(process, path, Some(record), None, false, reply, then)
    }

    /// `again` is the one retry a lost race gets: two appends to a name that does not
    /// exist both try to make it, and the one that loses is told the name is taken.
    /// That is the right answer to a create and the wrong answer to an append, so the
    /// append looks the object up again — by then it is there, and a lookup waits for
    /// its stream (`Op::ObjectInfo` parks at the DN).
    #[allow(clippy::too_many_arguments)]
    fn resolving(
        self: &Arc<Self>,
        process: &Arc<Process>,
        path: &str,
        record: Option<u64>,
        make: Option<Framing>,
        again: bool,
        reply: Reply,
        then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
    ) {
        let dn = match process.pick(ServiceKind::Dn) {
            Ok(dn) => dn.to_string(),
            Err(e) => return reply.send(Response::error(Status::Internal, e.to_string())),
        };
        // The DN that answers is the one that owns the name, so its number is half
        // of the stream's name.
        let owner = process.cluster.service(&dn).map(|entry| entry.number()).unwrap_or(0);

        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let path = path.to_string();
        let ask = match record {
            None => Request::new(Op::ObjectInfo).text(&path),
            Some(record) => Request::new(Op::LocateStream).text(&path).number(record),
        };
        let named = dn.clone();
        let asked = process.ask(&dn, &ask, move |answer| {
            // An append may make the object it is appending to, which is one more
            // hop and only on the first append.
            if let (Status::NotFound, Some(framing)) = (answer.status, make) {
                return make_object(
                    &fe, &onward, &named, owner, &path, framing, again, reply, then,
                );
            }
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(located) = Located::of(owner, &answer.args) else {
                return reply.send(Response::error(
                    Status::Internal,
                    "that answer did not describe an object",
                ));
            };
            // A create takes the name first and names the stream afterwards, so a
            // zero here is an object caught between the two.
            if located.stream == 0 {
                return reply
                    .send(Response::error(Status::Internal, format!("{path} has no stream yet")));
            }
            then(&fe, &onward, located, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach a DN", "dn {dn},{e}");
        }
    }

    /// An append: resolve the name — making the object if it is not there — find the
    /// extent, and hand the bytes to its primary.
    fn append(self: &Arc<Self>, process: &Arc<Process>, request: &Request, mut reply: Reply) {
        let (path, framing, holds, body) = match request.args.as_slice() {
            [Arg::Text(path), Arg::Number(kind), Arg::Number(width), rest @ ..] => {
                let framing = match Framing::of_wire(*kind, *width) {
                    Ok(framing) => framing,
                    Err(e) => {
                        return reply.send(Response::error(Status::BadRequest, e.to_string()));
                    }
                };
                match Holds::of_args(rest) {
                    Ok((holds, [Arg::Bytes(body)])) => (path.clone(), framing, holds, body.clone()),
                    _ => {
                        return reply.send(Response::error(
                            Status::BadRequest,
                            "append takes a name, a framing, what the block holds, and bytes",
                        ));
                    }
                }
            }
            _ => {
                return reply.send(Response::error(
                    Status::BadRequest,
                    "append takes a name, a framing, what the block holds, and bytes",
                ));
            }
        };
        reply.about(format_args!("{path},bytes {},logical {}", body.len(), holds.logical));
        let named = path.clone();
        self.resolve(process, &path, Some(framing), reply, move |fe, process, located, reply| {
            // The object's own framing wins, and a writer that disagrees about it is
            // writing records the object cannot hold.
            if located.framing != framing {
                return reply.send(Response::error(
                    Status::BadRequest,
                    format!(
                        "that object is framed {} because it was made that way, not {framing}",
                        located.framing
                    ),
                ));
            }
            let wanting = Wanting { path: named, located, body, holds, tries: TRIES, reply };
            fe.somewhere(process, wanting, None);
        });
    }

    /// Find this append somewhere to go, and go there.
    ///
    /// **One request per stream asks the SN; the rest wait for its answer.** Every
    /// append in flight when an extent fills is told the extent is sealed, and each of
    /// them wants the same next extent — so without this they each ask, and the SN's
    /// request rate climbs towards the append rate while the number of extents it
    /// creates stays the same.
    ///
    /// `sealed` is set by whoever saw the extent fill: it carries what that extent came
    /// to, which the SN needs and which is also what makes the extent rememberable for
    /// reads.
    fn somewhere(
        self: &Arc<Self>,
        process: &Arc<Process>,
        wanting: Wanting,
        sealed: Option<(u64, u64, u64)>,
    ) {
        let key = (wanting.located.dn, wanting.located.stream);
        // ⚠️ Deciding and waiting under one lock. With the lock released in between,
        // the answer can arrive in the gap and wake nobody.
        let mut carry = Some(wanting);
        let (plan, open) = {
            let mut streams = self.streams.lock().expect("streams");
            let stream = streams.entry(key).or_default();
            let plan = stream.plan(sealed.map(|(id, records, _)| (id, records)));
            if plan == Plan::Join {
                stream
                    .getting
                    .as_mut()
                    .expect("a request in flight")
                    .push(carry.take().expect("an append"));
            }
            (plan, stream.open.clone())
        };

        let Some(wanting) = carry else { return };
        match (plan, open) {
            (Plan::Join, _) => {}
            (Plan::Use, Some(open)) => self.to_primary(process, wanting, open),
            // Nothing to use after all, which only a race can produce; asking is
            // always safe.
            (Plan::Use, None) | (Plan::Ask, _) => {
                let ask = wanting_extent(key, sealed);
                let identity = format!("SN{}", wanting.located.sn);
                let fe = Arc::clone(self);
                let onward = Arc::clone(process);
                let named = identity.clone();
                let asked = process.ask(&identity, &ask, move |answer| {
                    fe.arrived(&onward, key, wanting, answer);
                });
                if let Err(e) = asked {
                    logw!("could not reach an SN", "sn {named},{e}");
                    // Nothing is in flight any more, and whoever is waiting has to
                    // be told rather than left holding a reply.
                    self.give_up(key, Status::Internal, "could not reach an SN");
                }
            }
        }
    }

    /// The SN answered about the next extent. Whoever asked, and everyone who queued
    /// behind them, get the same answer.
    fn arrived(
        self: &Arc<Self>,
        process: &Arc<Process>,
        key: (u32, u64),
        wanting: Wanting,
        answer: Response,
    ) {
        let extent = match answer.status {
            Status::Ok => Open::of(wanting.located.sn, &answer.args),
            _ => None,
        };
        let waiting = {
            let mut streams = self.streams.lock().expect("streams");
            let stream = streams.entry(key).or_default();
            if let Some(extent) = &extent {
                stream.open = Some(extent.clone());
            }
            stream.getting.take().unwrap_or_default()
        };

        // Whoever was waiting wanted exactly what this answer says, so they are
        // resumed the same way — including the awkward ones, since a full stream or a
        // refusal is the same news for all of them.
        for waiting in waiting {
            self.resume(process, waiting, &answer, extent.clone());
        }
        self.resume(process, wanting, &answer, extent);
    }

    fn resume(
        self: &Arc<Self>,
        process: &Arc<Process>,
        wanting: Wanting,
        answer: &Response,
        extent: Option<Open>,
    ) {
        match (answer.status, extent) {
            (Status::Ok, Some(extent)) => self.to_primary(process, wanting, extent),
            (Status::Ok, None) => {
                wanting.reply.send(Response::error(Status::Internal, "that answer named no extent"))
            }
            // The stream is full: the object needs another, which is the DN's to give.
            (Status::Sealed, _) => self.after_stream(process, wanting, answer.clone()),
            _ => wanting.reply.send(answer.clone()),
        }
    }

    /// Nothing is in flight, and nothing may be left waiting for it.
    fn give_up(&self, key: (u32, u64), status: Status, why: &str) {
        let waiting = {
            let mut streams = self.streams.lock().expect("streams");
            streams.get_mut(&key).and_then(|stream| stream.getting.take())
        };
        for wanting in waiting.unwrap_or_default() {
            wanting.reply.send(Response::error(status, why));
        }
    }

    /// The bytes, to the primary instance. The framing and the instance list travel
    /// with them: the primary is what counts the records and what broadcasts to the
    /// others, and the SN is not in this path to be asked again.
    fn to_primary(self: &Arc<Self>, process: &Arc<Process>, wanting: Wanting, open: Open) {
        let Wanting { path, located, body, holds, tries, reply } = wanting;
        let identity = format!("EN{}", open.instances[0]);
        // One shape, built and read in `common`, so neither side counts positions.
        let ask = AppendBlock {
            sn: open.sn,
            extent: open.id,
            created: open.created,
            holds,
            instances: open.instances.clone(),
            offset: None,
            bytes: body.clone(),
        }
        .request(Op::AppendExtent);
        let named = identity.clone();
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status == Status::Sealed {
                let wanting = Wanting { path, located, body, holds, tries, reply };
                return fe.after_seal(&onward, wanting, open, answer);
            }
            reply.send(receipt(&located, &open, answer));
        });
        if let Err(e) = asked {
            logw!("could not reach an EN", "en {named},{e}");
        }
    }

    /// The extent filled up. Tell the SN what it came to and take the next one — which
    /// is the same operation as opening the first, so it collapses the same way.
    fn after_seal(
        self: &Arc<Self>,
        process: &Arc<Process>,
        mut wanting: Wanting,
        open: Open,
        answer: Response,
    ) {
        let [Arg::Number(records), Arg::Number(length)] = answer.args.as_slice() else {
            return wanting
                .reply
                .send(Response::error(Status::Internal, "a seal said nothing about the extent"));
        };
        // An extent that sealed with nothing in it is one this block could not fit
        // even empty, which is the block's fault and not worth another extent.
        if *records == 0 && *length == 0 {
            return wanting.reply.send(Response::error(
                Status::BadRequest,
                format!("{} bytes will not fit an extent of its own", wanting.body.len()),
            ));
        }
        if wanting.tries == 0 {
            return wanting.reply.send(Response::error(
                Status::Internal,
                "kept finding a sealed extent, which should not go on this long",
            ));
        }
        wanting.tries -= 1;
        self.somewhere(process, wanting, Some((open.id, *records, *length)));
    }

    /// The stream is full. The DN gives the object another, carrying on from where
    /// this one stopped, and the append starts again there.
    fn after_stream(self: &Arc<Self>, process: &Arc<Process>, wanting: Wanting, answer: Response) {
        let [Arg::Number(records)] = answer.args.as_slice() else {
            return wanting.reply.send(Response::error(
                Status::Internal,
                "a full stream said nothing about itself",
            ));
        };
        let Wanting { path, located, body, holds, tries, reply } = wanting;
        // Nothing may hand out an extent of a stream that will take no more, and what
        // is remembered about it is no use to an append either.
        self.streams.lock().expect("streams").remove(&(located.dn, located.stream));

        let dn = format!("DN{}", located.dn);
        let ask =
            Request::new(Op::ExtendObject).text(&path).number(located.stream).number(*records);
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let named = dn.clone();
        let asked = process.ask(&dn, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(next) = Located::of(located.dn, &answer.args) else {
                return reply
                    .send(Response::error(Status::Internal, "that extension named no stream"));
            };
            if next.stream == located.stream {
                return reply.send(Response::error(
                    Status::Internal,
                    format!("{path} was extended and is still on stream {}", next.stream),
                ));
            }
            let wanting = Wanting { path, located: next, body, holds, tries, reply };
            fe.somewhere(&onward, wanting, None);
        });
        if let Err(e) = asked {
            logw!("could not reach a DN", "dn {named},{e}");
        }
    }

    /// A read: resolve the name, find the extent holding the record, read it from an
    /// instance.
    ///
    /// ⚠️ **Three numbering spaces meet on this path.** A caller counts records from
    /// the start of the object, a stream counts from its own first, and an extent
    /// counts from its own. The FE is the only party that knows all three, so every
    /// conversion happens here — a bug in one of them shows up as a read that works
    /// until the first seal and then does not.
    fn read(self: &Arc<Self>, process: &Arc<Process>, request: &Request, mut reply: Reply) {
        let (path, record) = match request.args.as_slice() {
            [Arg::Text(path), Arg::Number(record)] => (path.clone(), *record),
            // A key rather than a record number is a keyed read, which needs the
            // key ordering nothing has built.
            [Arg::Text(_), Arg::Text(_)] => {
                return reply.send(Response::error(
                    Status::Unimplemented,
                    "reading by key is not built yet",
                ));
            }
            _ => {
                return reply
                    .send(Response::error(Status::BadRequest, "read takes a name and a record"));
            }
        };
        reply.about(format_args!("{path},record {record}"));
        self.resolve_record(process, &path, record, reply, move |fe, process, located, reply| {
            // The DN answered with the stream holding it, so the number the stream
            // knows it by is what is left after that stream's first.
            let within = record.saturating_sub(located.first_record);
            fe.locate(process, located, within, reply);
        });
    }

    /// Which extent holds a record.
    ///
    /// **A sealed extent's record range never changes**, so one this FE has seen seal
    /// is one it never has to ask about again — which is what keeps a read rate from
    /// becoming an SN request rate. Anything else is the SN's to answer: the extent
    /// still being appended to reaches further with every append, and how far is not
    /// something to remember.
    fn locate(
        self: &Arc<Self>,
        process: &Arc<Process>,
        located: Located,
        record: u64,
        reply: Reply,
    ) {
        let known = self
            .streams
            .lock()
            .expect("streams")
            .get(&(located.dn, located.stream))
            .and_then(|stream| stream.holding(record));
        if let Some(open) = known {
            return from_instance(process, &located, &open, record, reply);
        }

        let identity = format!("SN{}", located.sn);
        let ask = Request::new(Op::LocateRecord)
            .number(located.dn as u64)
            .number(located.stream)
            .number(record);
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let named = identity.clone();
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(open) = Open::of(located.sn, &answer.args) else {
                return reply
                    .send(Response::error(Status::Internal, "that answer named no extent"));
            };
            // A sealed extent the SN just named is one to remember: its record range
            // is fixed, so no read of it need ask again — including on an FE that
            // never wrote to it.
            fe.streams
                .lock()
                .expect("streams")
                .entry((located.dn, located.stream))
                .or_default()
                .remember(open.clone());
            from_instance(&onward, &located, &open, record, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {named},{e}");
        }
    }
}

/// What to ask the SN for the next extent: open one, or report the one that filled
/// and get its replacement in the same round trip.
fn wanting_extent(key: (u32, u64), sealed: Option<(u64, u64, u64)>) -> Request {
    let (dn, stream) = key;
    match sealed {
        None => Request::new(Op::OpenExtent).number(dn as u64).number(stream),
        Some((id, records, length)) => Request::new(Op::SealExtent)
            .number(dn as u64)
            .number(stream)
            .number(id)
            .number(records)
            .number(length),
    }
}

#[allow(clippy::too_many_arguments)]
fn make_object(
    fe: &Arc<FE>,
    process: &Arc<Process>,
    dn: &str,
    owner: u32,
    path: &str,
    framing: Framing,
    again: bool,
    reply: Reply,
    then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
) {
    let (kind, width) = framing.wire();
    // Unkeyed: a record number is what an append gets back, and a keyed object is
    // addressed by key instead.
    let ask = Request::new(Op::CreateObject).text(path).flag(false).number(kind).number(width);
    let fe = Arc::clone(fe);
    let onward = Arc::clone(process);
    let named = dn.to_string();
    let path = path.to_string();
    let asked = process.ask(dn, &ask, move |answer| {
        // Somebody else made it first. For a create that is the answer; for an
        // append it is only a reason to look again.
        if answer.status == Status::BadRequest && again {
            return fe.resolving(&onward, &path, None, Some(framing), false, reply, then);
        }
        if answer.status != Status::Ok {
            return reply.send(answer);
        }
        let Some(located) = Located::of(owner, &answer.args) else {
            return reply.send(Response::error(Status::Internal, "that create made no object"));
        };
        if located.stream == 0 {
            return reply.send(Response::error(Status::Internal, "that object has no stream"));
        }
        then(&fe, &onward, located, reply);
    });
    if let Err(e) = asked {
        logw!("could not reach a DN", "dn {named},{e}");
    }
}

/// Read a record from an instance of the extent holding it.
///
/// ⚠️ **The primary, not any instance.** A sealed extent is identical everywhere and
/// could be read from whichever instance is nearest or least busy. An unsealed one
/// differs, but not in its bytes: below the durable length every instance has
/// everything, and holes exist only above it — on the primary too. What a secondary
/// may have is a **stale durable length**, so it would answer against the wrong
/// watermark and refuse something that is durable. The primary is the one that knows
/// what the watermark is. Until sealing exists, every extent is unsealed.
fn from_instance(
    process: &Arc<Process>,
    located: &Located,
    open: &Open,
    record: u64,
    reply: Reply,
) {
    let identity = format!("EN{}", open.instances[0]);
    // The record number the extent knows is its own, counted from its first — and no
    // framing goes with it, because the EN will not be looking inside.
    let within = record.saturating_sub(open.first_record);
    // ⚠️ And the answer comes back in the extent's numbering, which is not the
    // caller's. Three numbering spaces meet here — object, stream, extent — and the
    // FE is the only party that knows all three, so it is the one that converts.
    let base = located.first_record + open.first_record;
    let ask = Request::new(Op::ReadExtent)
        .number(open.sn as u64)
        .number(open.id)
        .number(open.created)
        .number(within);
    let named = identity.clone();
    // The block comes back whole and is passed on whole: the reader decompresses it,
    // checks the CRC against what it wrote, and takes the record it wanted. Doing
    // that here would mean holding plaintext and spending CPU for no reason.
    let asked = process.ask(&identity, &ask, move |answer| {
        reply.send(in_the_callers_numbering(answer, base));
    });
    if let Err(e) = asked {
        logw!("could not reach an EN", "en {named},{e}");
    }
}

/// Put a block's first record into the numbering the caller asked in. An EN counts
/// from the start of its extent; a caller counts from the start of the object.
fn in_the_callers_numbering(mut answer: Response, base: u64) -> Response {
    if answer.status == Status::Ok
        && let Some(Arg::Number(first)) = answer.args.get_mut(1)
    {
        *first += base;
    }
    answer
}

/// What an append tells the writer. The durable reference comes first — a stream
/// and a record number, which are good for ever — and then the extent and offset,
/// which are a hint: they are correct until something rewrites that extent, and a
/// rewritten extent has a different name, so a stale hint fails loudly.
fn receipt(located: &Located, open: &Open, answer: Response) -> Response {
    if answer.status != Status::Ok {
        return answer;
    }
    let [
        _sn,
        Arg::Number(extent),
        Arg::Number(created),
        Arg::Number(offset),
        Arg::Number(length),
        Arg::Number(first),
        Arg::Number(records),
    ] = answer.args.as_slice()
    else {
        return Response::error(Status::Internal, "that append answered with no receipt");
    };
    // The stream-local record number, and then the object-local one, which is the
    // address a reader gives back.
    let in_stream = open.first_record + first;
    Response {
        id: 0,
        status: Status::Ok,
        args: vec![
            Arg::Number(located.stream),
            Arg::Number(in_stream),
            Arg::Number(located.first_record + in_stream),
            Arg::Number(*records),
            Arg::Number(*extent),
            Arg::Number(*created),
            Arg::Number(*offset),
            Arg::Number(*length),
        ],
    }
}

/// What a DN says an object is: which stream holds it, which SN holds that, and how
/// its records are framed.
#[derive(Clone, Copy)]
struct Located {
    dn: u32,
    stream: u64,
    sn: u32,
    framing: Framing,
    /// The object-local number of this stream's first record, which is what turns a
    /// record number the user gave into one the stream knows.
    first_record: u64,
}

impl Located {
    fn of(dn: u32, args: &[Arg]) -> Option<Located> {
        let [
            _id,
            Arg::Number(stream),
            Arg::Number(sn),
            _created,
            _keyed,
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(first_record),
        ] = args
        else {
            return None;
        };
        Some(Located {
            dn,
            stream: *stream,
            sn: *sn as u32,
            framing: Framing::of_wire(*kind, *width).ok()?,
            first_record: *first_record,
        })
    }
}

/// The request's first argument, which for anything about an object is the name.
fn first(request: &Request) -> &str {
    match request.args.first() {
        Some(Arg::Text(name)) => name.as_str(),
        _ => "",
    }
}

fn main() {
    let result =
        server::main(|process| Ok(FE::new(FEConfig::load(&process.config.service_config)?)));
    if let Err(e) = result {
        eprintln!("fe: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testing;

    /// The SN answers with the id, the second, the first record, then the instances.
    #[test]
    fn an_open_extent_is_read_out_of_the_sns_answer() {
        let args = vec![
            Arg::Number(7),
            Arg::Number(1_000_000),
            Arg::Number(40),
            Arg::Number(90),
            Arg::Number(2),
            Arg::Number(3),
            Arg::Number(0),
            Arg::Number(1),
        ];
        let open = Open::of(0, &args).expect("an extent");
        assert_eq!((open.sn, open.id, open.created, open.first_record), (0, 7, 1_000_000, 40));
        assert_eq!(open.records, 90, "sealed, so its range is known");
        assert_eq!(open.instances, [2, 3, 0, 1]);
        // The primary is the first, which is the one an append is sent to.
        assert_eq!(open.instances[0], 2);
    }

    /// An extent with nowhere to put it is not an extent.
    #[test]
    fn an_extent_with_no_instances_is_refused() {
        assert!(
            Open::of(0, &[Arg::Number(7), Arg::Number(1_000_000), Arg::Number(0), Arg::Number(0)])
                .is_none()
        );
        assert!(Open::of(0, &[Arg::Number(7), Arg::Number(1_000_000)]).is_none());
        assert!(Open::of(0, &[]).is_none());
    }

    /// The DN's answer says which stream, which SN, and how the records are framed.
    #[test]
    fn an_object_is_read_out_of_the_dns_answer() {
        let (kind, width) = Framing::CrLf.wire();
        let args = vec![
            Arg::Number(9),
            Arg::Number(17),
            Arg::Number(0),
            Arg::Number(1_000_000),
            Arg::Flag(false),
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(400),
        ];
        let located = Located::of(3, &args).expect("an object");
        assert_eq!((located.dn, located.stream, located.sn), (3, 17, 0));
        assert_eq!(located.framing, Framing::CrLf);
        assert_eq!(located.first_record, 400, "where this stream's records start");
        // A framing the DN could not have meant is not an object either.
        assert!(Located::of(3, &args[..5]).is_none());
    }

    /// The receipt leads with the durable reference and follows with the hint, and
    /// the record number it reports is stream-local rather than extent-local.
    #[test]
    fn a_receipt_leads_with_the_durable_reference() {
        let located = Located { dn: 0, stream: 17, sn: 0, framing: Framing::Lf, first_record: 0 };
        let open = Open {
            sn: 0,
            id: 7,
            created: 1_000_000,
            first_record: 40,
            records: 0,
            instances: vec![2, 3],
        };
        let from_en = Response {
            id: 0,
            status: Status::Ok,
            args: vec![
                Arg::Number(0),
                Arg::Number(7),
                Arg::Number(1_000_000),
                Arg::Number(512),
                Arg::Number(30),
                Arg::Number(2),
                Arg::Number(1),
            ],
        };
        let receipt = receipt(&located, &open, from_en);
        assert_eq!(receipt.status, Status::Ok);
        assert_eq!(
            receipt.args,
            vec![
                Arg::Number(17),
                // 40 records in the extents before this one, plus its record 2.
                Arg::Number(42),
                // And the same again object-locally, since this stream starts at 0.
                Arg::Number(42),
                Arg::Number(1),
                Arg::Number(7),
                Arg::Number(1_000_000),
                Arg::Number(512),
                Arg::Number(30),
            ]
        );
    }

    /// A refusal from the EN is passed through as it stands: FE has nothing to add
    /// to it and must not turn it into a receipt.
    #[test]
    fn a_failed_append_is_not_dressed_up_as_a_receipt() {
        let located = Located { dn: 0, stream: 17, sn: 0, framing: Framing::Lf, first_record: 0 };
        let open = Open {
            sn: 0,
            id: 7,
            created: 1_000_000,
            first_record: 0,
            records: 0,
            instances: vec![2],
        };
        let refused = Response::error(Status::Internal, "two instances would not apply it");
        assert_eq!(receipt(&located, &open, refused.clone()), refused);
    }

    fn extent(id: u64, first_record: u64) -> Open {
        Open {
            sn: 0,
            id,
            created: 1_000_000,
            first_record,
            records: 0,
            instances: vec![2, 3, 0, 1],
        }
    }

    /// One append asks the SN and the rest join it. Without this, every append in
    /// flight when an extent fills asks for the same next extent.
    #[test]
    fn one_append_asks_for_the_next_extent_and_the_rest_join() {
        let mut stream = Stream::default();
        // Nothing known yet: somebody has to ask.
        assert_eq!(stream.plan(None), Plan::Ask);
        // Everyone arriving while that is in flight joins it.
        assert_eq!(stream.plan(None), Plan::Join);
        assert_eq!(stream.plan(None), Plan::Join);

        // The answer arrives.
        stream.open = Some(extent(1, 0));
        stream.getting = None;
        assert_eq!(stream.plan(None), Plan::Use);
    }

    /// The extent filled: the first append to say so does the asking, and the ones
    /// that saw the same extent fill join rather than asking again.
    #[test]
    fn appends_that_all_saw_the_same_extent_fill_ask_once() {
        let mut stream = Stream { open: Some(extent(1, 0)), ..Stream::default() };
        assert_eq!(stream.plan(Some((1, 90))), Plan::Ask, "the first to notice");
        assert_eq!(stream.plan(Some((1, 90))), Plan::Join, "the rest of them");
        assert_eq!(stream.plan(Some((1, 90))), Plan::Join);
        // And it was remembered on the way past, which is what a read wants.
        assert_eq!(stream.sealed.len(), 1);
        assert_eq!(stream.sealed[0].records, 90);
    }

    /// An append that hears about an extent already replaced uses the replacement
    /// rather than asking for a third one.
    #[test]
    fn an_append_late_to_a_seal_uses_what_replaced_it() {
        let mut stream = Stream { open: Some(extent(2, 90)), ..Stream::default() };
        assert_eq!(stream.plan(Some((1, 90))), Plan::Use);
        assert!(stream.sealed.is_empty(), "extent 1 was not this FE's to remember");
    }

    /// A sealed extent's record range never changes, so a read of it never has to ask
    /// the SN again. The extent still open is not rememberable — it reaches further
    /// with every append.
    #[test]
    fn a_read_finds_a_record_in_a_remembered_extent() {
        let mut stream = Stream { open: Some(extent(1, 0)), ..Stream::default() };
        stream.plan(Some((1, 90)));
        stream.open = Some(extent(2, 90));
        stream.plan(Some((2, 80)));
        stream.open = Some(extent(3, 170));

        assert_eq!(stream.holding(0).unwrap().id, 1);
        assert_eq!(stream.holding(89).unwrap().id, 1);
        assert_eq!(stream.holding(90).unwrap().id, 2);
        assert_eq!(stream.holding(169).unwrap().id, 2);
        // In the extent still open, so the SN has to be asked.
        assert!(stream.holding(170).is_none());
        assert!(stream.holding(5000).is_none());
    }

    /// An FE that never wrote an extent can still learn where its records are, because
    /// an SN naming a sealed extent says how much it holds. Otherwise a read-only front
    /// end asks about every read for ever.
    #[test]
    fn a_read_teaches_an_fe_that_never_wrote() {
        let mut stream = Stream::default();
        assert!(stream.holding(5).is_none(), "nothing known yet");

        // What a locate answers for a sealed extent: 90 records from 0.
        let mut sealed = extent(1, 0);
        sealed.records = 90;
        stream.remember(sealed);
        assert_eq!(stream.holding(5).unwrap().id, 1);
        assert_eq!(stream.holding(89).unwrap().id, 1);
        assert!(stream.holding(90).is_none());

        // Learning it twice keeps one copy, since appends and reads both teach it.
        stream.remember(stream.holding(5).unwrap());
        assert_eq!(stream.sealed.len(), 1);

        // And the extent still open says zero records, which is not rememberable.
        stream.remember(extent(2, 90));
        assert_eq!(stream.sealed.len(), 1, "an open extent is not a fact to keep");
    }

    /// Both are answered by somebody else, so neither may be answered on the read
    /// loop that read it.
    #[test]
    fn everything_fe_does_waits_on_another_process() {
        let fe = FE::new(testing::fe_config());
        assert!(fe.defers(Op::AppendRecord));
        assert!(fe.defers(Op::ReadRecord));
        assert!(fe.defers(Op::CreateObject));
        assert!(!fe.defers(Op::Ping));
        // And issuing the first hop is cheap enough to do on the read loop.
        assert!(fe.inline(Op::AppendRecord));
    }

    /// An unbuilt operation is still one FE is the right place to send.
    #[test]
    fn the_unbuilt_record_operations_are_still_ours() {
        assert!(SERVES.contains(&Op::Scan));
        assert!(!COORDINATED.contains(&Op::Scan));
        assert!(!FORWARDED.contains(&Op::Scan));
    }
}
