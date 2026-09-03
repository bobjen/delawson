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

use common::Result;
use common::block::Holds;
use common::config::{FEConfig, ServiceKind};
use common::framing::Framing;
use common::ids::{DiskId, DnId, EnId, ExtentId, SnId};
use common::info::{Batch, Described, Expecting, Shape, StreamName, UNKNOWN, Wanted};
use common::logw;
use common::offset::{ERoff, ORoff, SRoff};
use common::range::{Budget, Covered, Detail, Filled, Piece, Range, Start, Want};
use common::routing;
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::wire::{AppendBlock, Arg, Op, Req, Resp, Status, StructWriter, structure};
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
    sn: SnId,
    id: ExtentId,
    created: TimeSec,
    /// The stream-local number of this extent's first record, which is what turns an
    /// extent-local number into a durable reference.
    first_record: SRoff,
    /// Records in it, or zero for the extent still being appended to — whose count
    /// changes with every append and is therefore not a thing to remember.
    records: ERoff,
    /// The framing used for records in this extent.
    _framing: Framing,
    /// The **disks** holding its instances, primary first, each as `(EnId, DiskId)`.
    disks: Vec<(EnId, DiskId)>,
}

impl Open {
    /// The id, the second, the first record, how many records it holds, the count of instances,
    /// then the (en, disk) pairs, which is the order the SN answers in.
    fn of(sn: SnId, framing: Framing, args: &[Arg]) -> Option<Open> {
        let (mut payload, rest) = structure(args, Op::ExtentResp, "an extent response").ok()?;
        if !rest.is_empty() {
            return None;
        }
        let id = payload.number().ok()?;
        let created = payload.number().ok()?;
        let first_record = payload.number().ok()?;
        let records = payload.number().ok()?;
        let count = usize::try_from(payload.number().ok()?).ok()?;
        let mut disks = Vec::with_capacity(count);
        for _ in 0..count {
            disks.push((
                EnId::of_number(payload.number().ok()?).ok()?,
                DiskId::of_number(payload.number().ok()?).ok()?,
            ));
        }
        payload.done().ok()?;
        (!disks.is_empty()).then_some(Open {
            sn,
            id: ExtentId::of_number(id).ok()?,
            created: TimeSec::new(created),
            first_record: SRoff::of_number(first_record).ok()?,
            records: ERoff::of_number(records).ok()?,
            _framing: framing,
            disks,
        })
    }

    /// Whether a stream-local record is in this extent. False for an extent whose
    /// count is not known, which is the one still being appended to.
    fn holds(&self, record: SRoff) -> bool {
        self.records > ERoff::ZERO
            && record >= self.first_record
            && record.number() < self.first_record.number() + self.records.number()
    }
}

/// An append waiting for an extent to append to.
struct Wanting {
    path: String,
    located: Located,
    framing: Framing,
    crc: u64,
    body: Vec<u8>,
    /// What the writer said the block holds: compressed size, logical size, records.
    /// Carried, never invented and never recomputed.
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
    fn plan(&mut self, sealed: Option<(ExtentId, ERoff)>) -> Plan {
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
    fn holding(&self, record: SRoff) -> Option<Open> {
        self.sealed.iter().find(|extent| extent.holds(record)).cloned()
    }

    /// Keep a sealed extent, unless it is already kept. Two appends and any number of
    /// reads can each learn the same one.
    fn remember(&mut self, extent: Open) {
        if extent.records > ERoff::ZERO && !self.sealed.iter().any(|kept| kept.id == extent.id) {
            self.sealed.push(extent);
        }
    }
}

struct FE {
    config: FEConfig,
    /// Per `(RangeId, StreamId)`.
    /// Keyed by the stream's whole name (#109), which is also what the SN keys by — so a
    /// recycled id in a later second cannot find a cached open extent belonging to the old
    /// stream (`A-NAME-IS-NEVER-REMINTED-IN-ITS-OWN-SECOND`).
    streams: Mutex<HashMap<StreamName, Stream>>,
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

    /// Only control operations are answered on the read loop. Everything that
    /// touches another process runs on worker threads so the read loop never stalls.
    fn inline(&self, op: Op) -> bool {
        matches!(op, Op::Ping | Op::Status | Op::Shutdown | Op::Sites)
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        match request.op {
            Op::AppendRecord => return self.append(process, &request, reply),
            Op::ReadRecord => return self.read(process, &request, reply),
            _ => {}
        }
        if !FORWARDED.contains(&request.op) {
            reply.send(server::unbuilt(process, SERVES, &request));
            return;
        }
        match request.op {
            Op::BatchInfo => self.batch(process, &request, reply),
            Op::BatchCreate => self.create(process, &request, reply),
            _ => self.onward(process, &request, reply),
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
        expect: Expecting,
        make: Option<Framing>,
        reply: Reply,
        then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
    ) {
        self.resolving(process, path, None, expect, make, true, reply, then)
    }

    /// Look up the specific stream holding an object-local record number (`ORoff`),
    /// rather than the stream currently being appended to. Same shape of answer,
    /// different question — an object is several streams and the DN is what knows which.
    fn resolve_record(
        self: &Arc<Self>,
        process: &Arc<Process>,
        path: &str,
        record: ORoff,
        expect: Expecting,
        reply: Reply,
        then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
    ) {
        self.resolving(process, path, Some(record), expect, None, false, reply, then)
    }

    /// Resolve an object path at the owning Directory Node (DN).
    ///
    /// Determines which DN owns the path and asks it for object and stream location:
    /// - If `record` is `None` (for appends), asks `Op::ObjectInfo` to locate the object's
    ///   tail stream currently taking appends. If `make` is `Some(framing)` and the object
    ///   does not exist yet, creates it.
    /// - If `record` is `Some(record)` (for reads), asks `Op::LocateStream` for the specific
    ///   stream holding that object-local record number (`ORoff`).
    ///
    /// Once the DN describes the stream (`Located`), invokes `then`. `again` provides a single
    /// retry if an initial create raced with another writer and lost.
    #[allow(clippy::too_many_arguments)]
    fn resolving(
        self: &Arc<Self>,
        process: &Arc<Process>,
        path: &str,
        record: Option<ORoff>,
        expect: Expecting,
        make: Option<Framing>,
        again: bool,
        reply: Reply,
        then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
    ) {
        // ⚠️ **By name, not in turn** (`A-NAME-HAS-ONE-OWNER`). This was `pick` until #106:
        // every append to one object resolved through a different DN, so each DN made its
        // own object under that name with its own stream, and the SN saw one stream key per
        // DN. Appends reported success and landed in three unrelated streams (§17.30).
        let dn = match process.owner(ServiceKind::Dn, path) {
            Ok(dn) => dn.to_string(),
            Err(e) => return reply.send(Resp::error(Status::Internal, e.to_string())),
        };

        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let path = path.to_string();
        // ⚠️ The expectation goes to the **DN**, because the DN owns the path table and is the
        // only tier that can see the path point somewhere new (§17.36).
        let ask = match record {
            None => {
                let mut payload = StructWriter::new();
                payload.text(&path);
                payload.flag(expect.asked());
                if expect.asked() {
                    Want { expect, ..Want::plain() }.write(&mut payload);
                }
                Req::new(Op::ObjectInfo).structure(payload)
            }
            Some(record) => {
                let mut payload = StructWriter::new();
                payload.text(&path);
                payload.number(record.number());
                Req::new(Op::LocateStream).structure(payload)
            }
        };
        let named = dn.clone();
        let asked = process.ask(&dn, &ask, move |answer| {
            // An append may make the object it is appending to, which is one more
            // hop and only on the first append.
            if let (Status::NotFound, Some(framing)) = (answer.status, make) {
                return make_object(&fe, &onward, &named, &path, framing, again, reply, then);
            }
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(located) = Located::of(&answer.args) else {
                return reply
                    .send(Resp::error(Status::Internal, "that answer did not describe an object"));
            };
            // A create takes the name first and names the stream afterwards, so a
            // zero here is an object caught between the two.
            if located.stream.stream.is_none() {
                return reply
                    .send(Resp::error(Status::Internal, format!("{path} has no stream yet")));
            }
            then(&fe, &onward, located, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach a DN", "dn {dn},{e}");
        }
    }

    /// An append: resolve the name — making the object if it is not there — find the
    /// extent, and hand the bytes to its primary.
    fn append(self: &Arc<Self>, process: &Arc<Process>, request: &Req, mut reply: Reply) {
        let (path, expect, framing, crc, holds, body) = {
            let (mut payload, rest) =
                match structure(&request.args, Op::AppendRecord, "an append request") {
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
            let expect = match Expecting::read(&mut payload) {
                Ok(expect) => expect,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let kind = match payload.number() {
                Ok(kind) => kind,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let width = match payload.number() {
                Ok(width) => width,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let framing = match Framing::of_wire(kind, width) {
                Ok(framing) => framing,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let crc = match payload.number() {
                Ok(crc) => crc,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let holds = match Holds::read(&mut payload) {
                Ok(holds) => holds,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let body = match payload.bytes() {
                Ok(body) => body,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            if let Err(e) = payload.done() {
                return reply.send(Resp::error(Status::BadRequest, e.to_string()));
            }
            if !rest.is_empty() {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "an append request has trailing arguments",
                ));
            }
            (path, expect, framing, crc, holds, body)
        };
        reply.about(format_args!("{path},bytes {},logical {}", body.len(), holds.logical));
        let named = path.clone();
        self.resolve(
            process,
            &path,
            expect,
            Some(framing),
            reply,
            move |fe, process, located, reply| {
                // The object's own framing wins, and a writer that disagrees about it is
                // writing records the object cannot hold.
                if located.framing != framing {
                    return reply.send(Resp::error(
                        Status::BadRequest,
                        format!(
                            "that object is framed {} because it was made that way, not {framing}",
                            located.framing
                        ),
                    ));
                }
                let wanting = Wanting {
                    path: named,
                    located,
                    framing,
                    crc,
                    body,
                    holds,
                    tries: TRIES,
                    reply,
                };
                fe.somewhere(process, wanting, None);
            },
        );
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
        sealed: Option<(ExtentId, Filled)>,
    ) {
        let key = wanting.located.stream;
        // ⚠️ Deciding and waiting under one lock. With the lock released in between,
        // the answer can arrive in the gap and wake nobody.
        let mut carry = Some(wanting);
        let (plan, open) = {
            let mut streams = self.streams.lock().expect("streams");
            let stream = streams.entry(key).or_default();
            let plan = stream.plan(sealed.map(|(id, came_to)| {
                (id, ERoff::of_number(came_to.records).unwrap_or(ERoff::ZERO))
            }));
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
        key: StreamName,
        wanting: Wanting,
        answer: Resp,
    ) {
        let extent = match answer.status {
            Status::Ok => Open::of(wanting.located.sn, wanting.located.framing, &answer.args),
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
        answer: &Resp,
        extent: Option<Open>,
    ) {
        match (answer.status, extent) {
            (Status::Ok, Some(extent)) => self.to_primary(process, wanting, extent),
            (Status::Ok, None) => {
                wanting.reply.send(Resp::error(Status::Internal, "that answer named no extent"))
            }
            // The stream is full: the object needs another, which is the DN's to give.
            (Status::Sealed, _) => self.after_stream(process, wanting, answer.clone()),
            _ => wanting.reply.send(answer.clone()),
        }
    }

    /// Nothing is in flight, and nothing may be left waiting for it.
    fn give_up(&self, key: StreamName, status: Status, why: &str) {
        let waiting = {
            let mut streams = self.streams.lock().expect("streams");
            streams.get_mut(&key).and_then(|stream| stream.getting.take())
        };
        for wanting in waiting.unwrap_or_default() {
            wanting.reply.send(Resp::error(status, why));
        }
    }

    /// The bytes, to the primary instance. The framing and the instance list travel
    /// with them: the primary is what counts the records and what broadcasts to the
    /// others, and the SN is not in this path to be asked again.
    fn to_primary(self: &Arc<Self>, process: &Arc<Process>, wanting: Wanting, open: Open) {
        let Wanting { path, located, framing, crc, body, holds, tries, reply } = wanting;
        let identity = format!("EN{}", open.disks[0].0);
        // One shape, built and read in `common`, so neither side counts positions.
        let ask = AppendBlock {
            sn: open.sn,
            extent: open.id,
            created: open.created,
            framing,
            crc,
            holds,
            logical_offset: None,
            instances: open.disks.clone(),
            bytes: body.clone(),
        }
        .request(open.disks[0].0, open.disks[0].1, Op::AppendExtent);
        let named = identity.clone();
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status == Status::Sealed {
                let wanting = Wanting { path, located, framing, crc, body, holds, tries, reply };
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
        answer: Resp,
    ) {
        let Ok((came_to, [])) = Filled::of_args(&answer.args) else {
            return wanting
                .reply
                .send(Resp::error(Status::Internal, "a seal said nothing about the extent"));
        };
        // An extent that sealed with nothing in it is one this block could not fit
        // even empty, which is the block's fault and not worth another extent.
        if came_to.empty() {
            return wanting.reply.send(Resp::error(
                Status::BadRequest,
                format!("{} bytes will not fit an extent of its own", wanting.body.len()),
            ));
        }
        if wanting.tries == 0 {
            return wanting.reply.send(Resp::error(
                Status::Internal,
                "kept finding a sealed extent, which should not go on this long",
            ));
        }
        wanting.tries -= 1;
        self.somewhere(process, wanting, Some((open.id, came_to)));
    }

    /// The stream is full. The DN gives the object another, carrying on from where
    /// this one stopped, and the append starts again there.
    fn after_stream(self: &Arc<Self>, process: &Arc<Process>, wanting: Wanting, answer: Resp) {
        let (mut payload, rest) = match structure(&answer.args, Op::NumberResp, "a sealed response")
        {
            Ok(value) => value,
            Err(_) => {
                return wanting.reply.send(Resp::error(
                    Status::Internal,
                    "a full stream said nothing about itself",
                ));
            }
        };
        let records = match payload.number().and_then(|records| payload.done().map(|()| records)) {
            Ok(records) if rest.is_empty() => records,
            _ => {
                return wanting.reply.send(Resp::error(
                    Status::Internal,
                    "a full stream said nothing about itself",
                ));
            }
        };
        let Wanting { path, located, framing, crc, body, holds, tries, reply } = wanting;
        // Nothing may hand out an extent of a stream that will take no more, and what
        // is remembered about it is no use to an append either.
        self.streams.lock().expect("streams").remove(&located.stream);

        let dn = format!("DN{}", located.dn);
        let mut payload = StructWriter::new();
        payload.text(&path);
        payload.number(located.stream.stream.number());
        payload.number(records);
        let ask = Req::new(Op::ExtendObject).structure(payload);
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let named = dn.clone();
        let asked = process.ask(&dn, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(next) = Located::of(&answer.args) else {
                return reply.send(Resp::error(Status::Internal, "that extension named no stream"));
            };
            if next.stream == located.stream {
                return reply.send(Resp::error(
                    Status::Internal,
                    format!("{path} was extended and is still on stream {}", next.stream),
                ));
            }
            let wanting = Wanting { path, located: next, framing, crc, body, holds, tries, reply };
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
    fn read(self: &Arc<Self>, process: &Arc<Process>, request: &Req, mut reply: Reply) {
        let (path, expect, range) = {
            let (mut payload, rest) =
                match structure(&request.args, Op::ReadRecord, "a read request") {
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
            let expect = match Expecting::read(&mut payload) {
                Ok(expect) => expect,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            let selector = match payload.number() {
                Ok(selector) => selector,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            if selector == 1 {
                return reply
                    .send(Resp::error(Status::Unimplemented, "reading by key is not built yet"));
            }
            if selector != 0 {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    format!("unknown read selector {selector}"),
                ));
            }
            let range = match Range::read_nested(&mut payload) {
                Ok(range) => range,
                Err(e) => {
                    return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                }
            };
            if let Err(e) = payload.done() {
                return reply.send(Resp::error(Status::BadRequest, e.to_string()));
            }
            if !rest.is_empty() {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "a read request has trailing arguments",
                ));
            }
            (path, expect, range)
        };
        // The start has to be a record for now: the other two forms need a prefix sum
        // of logical lengths, which §4.5 keeps lazily and nothing here can ask for
        // (§9.1.3.4). A well-formed request for something unbuilt.
        let Some(record) = range.start.record() else {
            return reply.send(Resp::error(
                Status::Unimplemented,
                "starting from a logical offset needs a prefix sum nothing keeps yet",
            ));
        };
        reply.about(format_args!("{path},record {record},{:?}", range.budget));
        let record = ORoff::new(record);
        self.resolve_record(
            process,
            &path,
            record,
            expect,
            reply,
            move |fe, process, located, reply| {
                // The DN answered with the stream holding it, so the number the stream
                // knows it by is what is left after that stream's first.
                let at = in_stream(record, located.first_record);
                fe.locate(process, located, at, range.budget, reply);
            },
        );
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
        record: SRoff,
        budget: Budget,
        reply: Reply,
    ) {
        let known = self
            .streams
            .lock()
            .expect("streams")
            .get(&located.stream)
            .and_then(|stream| stream.holding(record));
        if let Some(open) = known {
            return from_instance(process, &located, &open, record, budget, reply);
        }

        let identity = format!("SN{}", located.sn);
        let mut payload = StructWriter::new();
        payload.number(located.stream.range.number());
        payload.number(located.stream.stream.number());
        payload.number(located.stream.created.number());
        payload.number(record.number());
        let ask = Req::new(Op::LocateRecord).structure(payload);
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let named = identity.clone();
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(open) = Open::of(located.sn, located.framing, &answer.args) else {
                return reply.send(Resp::error(Status::Internal, "that answer named no extent"));
            };
            // A sealed extent the SN just named is one to remember: its record range
            // is fixed, so no read of it need ask again — including on an FE that
            // never wrote to it.
            fe.streams
                .lock()
                .expect("streams")
                .entry(located.stream)
                .or_default()
                .remember(open.clone());
            from_instance(&onward, &located, &open, record, budget, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {named},{e}");
        }
    }
}

/// What to ask the SN for the next extent: open one, or report the one that filled
/// and get its replacement in the same round trip.
fn wanting_extent(key: StreamName, sealed: Option<(ExtentId, Filled)>) -> Req {
    if sealed.is_none() {
        let mut payload = StructWriter::new();
        payload.number(key.range.number());
        payload.number(key.stream.number());
        payload.number(key.created.number());
        return Req::new(Op::OpenExtent).structure(payload);
    }
    let mut request = Req::new(match sealed {
        None => Op::OpenExtent,
        Some(_) => Op::SealExtent,
    });
    if let Some((id, came_to)) = sealed {
        let mut payload = StructWriter::new();
        payload.number(key.range.number());
        payload.number(key.stream.number());
        payload.number(key.created.number());
        payload.number(id.number());
        payload.number(came_to.records);
        payload.number(came_to.logical.number());
        payload.number(came_to.compressed.number());
        return request.structure(payload);
    }
    request.args.extend(key.args());
    request
}

#[allow(clippy::too_many_arguments)]
fn make_object(
    fe: &Arc<FE>,
    process: &Arc<Process>,
    dn: &str,
    path: &str,
    framing: Framing,
    again: bool,
    reply: Reply,
    then: impl FnOnce(&Arc<FE>, &Arc<Process>, Located, Reply) + Send + 'static,
) {
    let (kind, width) = framing.wire();
    // Unkeyed: a record number is what an append gets back, and a keyed object is
    // addressed by key instead.
    let mut payload = StructWriter::new();
    payload.text(path);
    payload.flag(false);
    payload.number(kind);
    payload.number(width);
    payload.text("");
    payload.flag(false);
    let ask = Req::new(Op::CreateObject).structure(payload);
    let fe = Arc::clone(fe);
    let onward = Arc::clone(process);
    let named = dn.to_string();
    let path = path.to_string();
    let asked = process.ask(dn, &ask, move |answer| {
        // Somebody else made it first. For a create that is the answer; for an
        // append it is only a reason to look again.
        if answer.status == Status::BadRequest && again {
            // ⚠️ No expectation on the retry: this is the losing side of a create race looking
            // the object up again, and what it wants is whatever won, not what it guessed.
            return fe.resolving(
                &onward,
                &path,
                None,
                Expecting::whatever(),
                Some(framing),
                false,
                reply,
                then,
            );
        }
        if answer.status != Status::Ok {
            return reply.send(answer);
        }
        let Some(located) = Located::of(&answer.args) else {
            return reply.send(Resp::error(Status::Internal, "that create made no object"));
        };
        if located.stream.stream.is_none() {
            return reply.send(Resp::error(Status::Internal, "that object has no stream"));
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
    in_stream: SRoff,
    budget: Budget,
    reply: Reply,
) {
    let identity = format!("EN{}", open.disks[0].0);
    // The record number the extent knows is its own, counted from its first — and no
    // framing goes with it, because the EN will not be looking inside.
    let at = in_extent(in_stream, open.first_record);
    // ⚠️ And the answer comes back in the extent's numbering, which is not the
    // caller's. Three numbering spaces meet here — object, stream, extent — and the
    // FE is the only party that knows all three, so it is the one that converts.
    let base = origin(located.first_record, open.first_record);
    // A sealed extent has something after it; the one still open is the object's
    // tail, so what the EN says about its own blocks is the whole answer.
    let sealed = open.records > ERoff::ZERO;
    let mut payload = StructWriter::new();
    payload.number(open.disks[0].0.number());
    payload.number(open.disks[0].1.number());
    payload.number(open.sn.number());
    payload.number(open.id.number());
    payload.number(open.created.number());
    Range { start: Start::Record(at.number()), budget }.write_nested(&mut payload);
    let ask = Req::new(Op::ReadExtent).structure(payload);
    let named = identity.clone();
    // The blocks come back whole and are passed on whole: the reader decompresses
    // them, checks the CRC against what it wrote, and takes the records it wanted.
    // Doing that here would mean holding plaintext and spending CPU for no reason.
    let asked = process.ask(&identity, &ask, move |answer| {
        reply.send(in_the_callers_numbering(answer, base, sealed));
    });
    if let Err(e) = asked {
        logw!("could not reach an EN", "en {named},{e}");
    }
}

/// A record number one space inward: an object's into its stream's, or a stream's into
/// its extent's. Both are the same subtraction, of that container's first record.
/// A caller's object-local record number in the numbering its stream knows it by.
fn in_stream(record: ORoff, stream_first: ORoff) -> SRoff {
    SRoff::new(record.since(stream_first).number())
}

/// And a stream-local record number in the extent's own numbering.
///
/// ⚠️ **Two functions, because these are two conversions.** One `within` served both until
/// the record spaces became types (§17.32.1) — object-to-stream at one call site and
/// stream-to-extent at the other, told apart only by which caller you were reading. That is
/// §17.16.2's bug written as a helper: the arithmetic is identical, the meaning is not, and
/// nothing but the types objects to using the wrong one.
///
/// The narrowing is the space change made visible: a stream's record space is 64-bit and an
/// extent's is 32-bit, so a difference that does not fit is a record that is not in this
/// extent. Clamped rather than refused, because the caller is about to ask the EN and the EN
/// is the one that can say it is not there.
fn in_extent(record: SRoff, extent_first: SRoff) -> ERoff {
    ERoff::of_number(record.since(extent_first).number()).unwrap_or(ERoff::MAX)
}

/// And what puts an extent's answer back into the caller's numbering — the two origins
/// crossed on the way in, added together.
///
/// ⚠️ **`within` twice and `origin` are only correct as a set.** Both first-records are
/// zero until an object outgrows its first extent, so a version that drops either one
/// passes everything that never leaves that extent and then fails most reads.
fn origin(stream_first: ORoff, extent_first: SRoff) -> ORoff {
    // ⚠️ **Two different spaces added, which is exactly why they are two types.** The first
    // is where this stream starts in the object; the second is where this extent starts in
    // the stream. Their sum is object-local, and neither addend is.
    ORoff::new(stream_first.number() + extent_first.number())
}

/// Put every record number in an answer into the numbering the caller asked in. An
/// EN counts from the start of its extent; a caller counts from the start of the
/// object.
///
/// ⚠️ **This parses and rebuilds rather than patching a position.** An earlier
/// version added `base` to argument index 1, which was the block's first record — and
/// when the answer grew a header saying what it covered, index 1 became a byte count
/// and the patch silently corrupted it instead. A shape read through `common` cannot
/// go wrong that way, which is §17.13.3 again.
///
/// `sealed` says whether the extent this came from has anything after it, which the
/// EN cannot know: it sets `more` from its own blocks, and only the FE knows there are
/// further extents. Overstating it costs one round trip that answers "no such
/// record"; understating it would lose data.
fn in_the_callers_numbering(answer: Resp, base: ORoff, sealed: bool) -> Resp {
    if answer.status != Status::Ok {
        return answer;
    }
    let Ok((mut covered, rest)) = Covered::of_args(&answer.args) else {
        return Resp::error(Status::Internal, "that answer did not say what it covered");
    };
    let Ok(pieces) = Piece::all_of_args(rest) else {
        return Resp::error(Status::Internal, "that answer's blocks are not blocks");
    };
    covered.next += base.number();
    covered.more = covered.more || sealed;
    let mut args = covered.args();
    for piece in pieces {
        args.extend(Piece { first_record: piece.first_record + base.number(), ..piece }.args());
    }
    Resp { id: 0, status: Status::Ok, args }
}

/// What an append tells the writer. The durable reference comes first — a stream
/// and a record number, which are good for ever — and then the extent and offset,
/// which are a hint: they are correct until something rewrites that extent, and a
/// rewritten extent has a different name, so a stale hint fails loudly.
fn receipt(located: &Located, open: &Open, answer: Resp) -> Resp {
    if answer.status != Status::Ok {
        return answer;
    }
    let (mut payload, rest) = match structure(&answer.args, Op::ReceiptResp, "an append receipt") {
        Ok(value) => value,
        Err(_) => {
            return Resp::error(Status::Internal, "that append answered with no receipt");
        }
    };
    if !rest.is_empty() {
        return Resp::error(Status::Internal, "that append answered with no receipt");
    }
    let _sn = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    let extent = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    let created = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    let offset = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    let length = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    let first = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    let records = match payload.number() {
        Ok(value) => value,
        Err(_) => return Resp::error(Status::Internal, "that append answered with no receipt"),
    };
    if payload.done().is_err() {
        return Resp::error(Status::Internal, "that append answered with no receipt");
    }
    // The stream-local record number, and then the object-local one, which is the
    // address a reader gives back.
    let in_stream = open.first_record.number() + first;
    Resp {
        id: 0,
        status: Status::Ok,
        args: {
            let mut payload = StructWriter::new();
            for value in [
                located.stream.stream.number(),
                in_stream,
                located.first_record.number() + in_stream,
                records,
                extent,
                created,
                offset,
                length,
            ] {
                payload.number(value);
            }
            vec![payload.finish(Op::ReceiptResp)]
        },
    }
}

/// What a DN says an object is: which stream holds it, which SN holds that, and how
/// its records are framed.
#[derive(Clone, Copy)]
struct Located {
    dn: DnId,
    /// The stream's whole name, which is what the SN keys by (#109).
    stream: StreamName,
    sn: SnId,
    framing: Framing,
    /// The object-local number of this stream's first record, which is what turns a
    /// record number the user gave into one the stream knows.
    first_record: ORoff,
}

impl Located {
    fn of(args: &[Arg]) -> Option<Located> {
        let (described, rest) = Described::of_args(args).ok()?;
        let described = described?;
        rest.is_empty().then_some(Located {
            dn: described.dn,
            stream: described.stream,
            sn: described.sn,
            framing: described.framing,
            first_record: ORoff::of_number(described.first_record).ok()?,
        })
    }
}

/// One name's place in a batch answer being put back together (§9.1.3.12).
#[derive(Clone)]
struct Slot {
    /// The arguments the owning DN produced for this name, untouched. Copied rather than
    /// re-encoded: a merge that re-encodes can disagree with the DN about the wire, which
    /// is §17.19's defect.
    args: Vec<Arg>,
    /// Whether the name was described to the detail asked for. A summary is a *valid*
    /// answer for one name — its shape says no streams follow and where to resume — but it
    /// is not a finished one, and the continuation points at the first unfinished name.
    whole: bool,
    /// What describing this name in full would cost in entries: one for the object and one
    /// per stream. **Exact**, because the stream count is the DN's own and it reports it
    /// whether or not it described the streams (§9.1.3.2).
    full: u64,
    /// What it cost as answered.
    spent: u64,
}

/// One request to one DN, and which of the caller's names it is about.
struct Ask {
    dn: String,
    /// Indices into the caller's list, in the order they go on the wire — so the Nth name
    /// of the answer is `at[N]`, and nothing has to match names up by string.
    at: Vec<usize>,
    want: Want,
}

/// A batch being assembled from several DNs.
///
/// ⚠️ **Every estimate here is allowed to be wrong.** The DN's limit is the authority, a
/// DN may answer shorter than asked, and #75 will make ownership move. What keeps that from
/// being a correctness problem is that assembly walks the caller's order and **stops at the
/// first name it does not have**, so a bad estimate costs a round trip and never an answer
/// attributed to the wrong name (`A-BATCH-ANSWER-IS-A-PREFIX`).
struct Spread {
    /// Answers still to come in this round. The one that takes it to zero moves on.
    left: usize,
    asked: usize,
    want: Want,
    limit: u64,
    got: Vec<Option<Slot>>,
    /// Set when a DN refused, so the whole batch fails rather than silently dropping the
    /// names that DN owned — which would look like those objects not existing.
    failed: Option<Resp>,
    reply: Option<Reply>,
    /// Set once the second round has gone out, so it goes out once.
    filled: bool,
    /// Which DN owns each name, and their names, so a second round can be addressed
    /// without recomputing the routing the first round already did.
    owners: Vec<usize>,
    dns: Vec<String>,
    names: Vec<String>,
}

impl Spread {
    /// Entries the answer as it stands would spend.
    fn spent(&self) -> u64 {
        self.got.iter().flatten().map(|slot| slot.spent).sum()
    }

    /// The first name not described in full: where the caller has to come back to. Names
    /// after it are still worth sending — their counts are what let the caller size its own
    /// next requests (§9.1.3.12) — but they are not what it asked for.
    fn unfinished(&self) -> Option<usize> {
        (0..self.got.len()).find(|i| self.got[*i].as_ref().is_none_or(|slot| !slot.whole))
    }

    /// What one DN said about the names it was asked about, filed under the caller's
    /// indices. A name already described in full is not overwritten by a lesser answer.
    fn take(&mut self, ask: &Ask, answer: Resp) {
        if answer.status != Status::Ok {
            // ⚠️ Not "those names do not exist". A DN that refused is a DN whose names are
            // unknown, and answering as though they were absent is a wrong answer rather
            // than a partial one.
            if self.failed.is_none() {
                self.failed = Some(answer);
            }
            return;
        }
        let (covered, rows) = match Batch::of_args(&answer.args) {
            Ok(read) => read,
            Err(e) => return self.give_up(e.to_string()),
        };
        let pieces = match self.want.detail {
            0 => match Described::some_of_args(rows, covered.given) {
                Ok(pieces) => pieces,
                Err(e) => return self.give_up(e.to_string()),
            },
            _ => match Batch::pieces(rows, covered.given) {
                Ok(pieces) => pieces,
                Err(e) => return self.give_up(e.to_string()),
            },
        };
        for (n, piece) in pieces.iter().enumerate() {
            let Some(at) = ask.at.get(n) else {
                return self.give_up("a DN described more names than it was asked about".into());
            };
            let slot = match self.slot_of(piece, ask) {
                Ok(slot) => slot,
                Err(e) => return self.give_up(e.to_string()),
            };
            let better = match &self.got[*at] {
                None => true,
                Some(old) => !old.whole && (slot.whole || slot.spent > old.spent),
            };
            if better {
                self.got[*at] = Some(slot);
            }
        }
    }

    fn give_up(&mut self, why: String) {
        if self.failed.is_none() {
            self.failed = Some(Resp::error(Status::Internal, why));
        }
    }

    /// One name's arguments, read only as far as is needed to know what it cost and
    /// whether it is finished. The rest is carried through untouched.
    fn slot_of(&self, piece: &[Arg], ask: &Ask) -> Result<Slot> {
        let args = piece.to_vec();
        if self.want.detail == 0 {
            return Ok(Slot { args, whole: true, full: 1, spent: 1 });
        }
        let (shape, _) = Shape::of_args(piece)?;
        // Whether this ask covered everything the caller wanted. A summary did not — but a
        // summary of an object with no streams is still all there is to say about it, which
        // is why this is not simply `shape.more()`.
        let covered = ask.want.detail & self.want.detail == self.want.detail;
        let whole = if covered { !shape.more() } else { shape.streams == 0 };
        let full = match shape.streams {
            UNKNOWN => shape.entries,
            streams => 1 + streams,
        };
        Ok(Slot { args, whole, full, spent: shape.entries })
    }

    /// The second round: spend whatever the first round left over on the names that are
    /// still summaries, in the caller's order (§9.1.3.12).
    ///
    /// ⚠️ **A contiguous run from the first unfinished name, not the cheapest names going.**
    /// The continuation points at the first unfinished name, so a caller that has to come
    /// back for name 5 will ask again from 5 — and detail fetched for name 9 in the meantime
    /// is work nobody will read.
    fn refill(&mut self) -> Option<Vec<Ask>> {
        if self.filled || self.failed.is_some() {
            return None;
        }
        self.filled = true;
        let start = self.unfinished()?;
        // A name no DN produced is a DN that truncated, and there is no more room to be had.
        self.got[start].as_ref()?;
        let slack = self.limit.saturating_sub(self.spent());
        let (mut extra, mut upgrade) = (0u64, Vec::new());
        for at in start..self.got.len() {
            let Some(slot) = &self.got[at] else { break };
            if slot.whole {
                continue;
            }
            let add = slot.full.saturating_sub(slot.spent);
            if extra + add > slack {
                break;
            }
            extra += add;
            upgrade.push(at);
        }
        if upgrade.is_empty() {
            return None;
        }
        let mut asks: Vec<Ask> = Vec::new();
        for at in upgrade {
            let dn = self.dns[self.owners[at]].clone();
            let cost = self.got[at].as_ref().map_or(1, |slot| slot.full);
            match asks.iter_mut().find(|ask| ask.dn == dn) {
                Some(ask) => {
                    ask.at.push(at);
                    ask.want.limit += cost;
                }
                None => {
                    asks.push(Ask { dn, at: vec![at], want: Want { limit: cost, ..self.want } })
                }
            }
        }
        self.left = asks.len();
        Some(asks)
    }

    /// The header and body of the answer, in the caller's order, stopping at the first name
    /// no DN produced.
    fn answer(&mut self) -> Resp {
        if let Some(failed) = self.failed.take() {
            return failed;
        }
        let mut args = Vec::new();
        let mut given = 0;
        for slot in &self.got {
            match slot {
                Some(slot) => {
                    args.extend(slot.args.iter().cloned());
                    given += 1;
                }
                None => break,
            }
        }
        let next = match self.unfinished() {
            Some(at) if (at as u64) < given => at as u64,
            _ if given < self.asked as u64 => given,
            _ => UNKNOWN,
        };
        let covered = Batch { asked: self.asked as u64, given, entries: self.spent(), next };
        let mut out = covered.args();
        out.append(&mut args);
        Resp { id: 0, status: Status::Ok, args: out }
    }
}

impl FE {
    /// A batch of lookups whose names may belong to different DNs (§9.1.3.12).
    ///
    /// **Round one is one round trip**: the DN owning the earliest names is asked for
    /// everything, and every other DN for a per-object summary — one entry per name, no
    /// stream rows, no SN hop. What the summaries will spend is therefore known *before*
    /// any of them is asked, because a name costs at least its own row
    /// (`A-NAME-COSTS-AT-LEAST-ONE-ENTRY`), so the detailed ask can be given the limit
    /// minus that count with no measuring round trip at all.
    ///
    /// **Round two spends the slack**, if the first DN left any, on the summaries nearest
    /// the front — sized from the exact stream counts the summaries reported.
    fn batch(self: &Arc<Self>, process: &Arc<Process>, request: &Req, mut reply: Reply) {
        let (names, want) = {
            let (mut payload, rest) =
                match structure(&request.args, Op::BatchInfo, "a batch-object-info request") {
                    Ok(value) => value,
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                };
            let count = match payload.number() {
                Ok(count) => count as usize,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
            let mut names = Vec::with_capacity(count);
            for _ in 0..count {
                match payload.text() {
                    Ok(name) => names.push(name),
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                }
            }
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
            if let Err(e) = payload.done() {
                return reply.send(Resp::error(Status::BadRequest, e.to_string()));
            }
            if !rest.is_empty() {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "a batch-object-info request has trailing arguments",
                ));
            }
            (names, want)
        };
        let dns: Vec<String> =
            process.all_of(ServiceKind::Dn).iter().map(|dn| dn.to_string()).collect();
        // Nothing to divide: one DN owns everything, or there is one name. The request goes
        // through untouched, which is also the path every measurement so far was taken on.
        if dns.len() < 2 || names.len() < 2 {
            return self.onward(process, request, reply);
        }

        let limit = match want.limit {
            0 => self.config.info_entries,
            asked => asked.min(self.config.info_entries),
        };
        // ⚠️ Names past the limit are not asked about at all. Each one would cost an entry
        // there is not, so asking would be work whose answer must be thrown away — and the
        // header still reports the whole count asked for, so the caller learns to come back.
        let keep = (limit as usize).min(names.len());
        let owners: Vec<usize> =
            names[..keep].iter().map(|name| routing::owner_of(name, dns.len())).collect();
        let asks = plan(&dns, &owners, want, limit);

        reply.about(format_args!("{},dns {}", first(request), asks.len()));
        let spread = Arc::new(Mutex::new(Spread {
            left: asks.len(),
            asked: names.len(),
            want,
            limit,
            got: vec![None; keep],
            failed: None,
            reply: Some(reply),
            filled: want.detail == 0,
            owners,
            dns,
            names,
        }));
        send(self, process, &spread, asks);
    }

    /// A batch create whose names may belong to different DNs (#98).
    ///
    /// **Simpler than the lookup fan-out**, and worth saying why: a create has no entry limit
    /// to divide, no detail levels, and its answer is a **count** rather than a row per name
    /// — so there is nothing to budget, nothing to size, and nothing to put back in order.
    /// The counts add.
    ///
    /// ⚠️ **The answer being a count is also its weakness.** A caller learns how many objects
    /// were newly made and not *which*, so a partial failure names nothing. That was already
    /// true of one DN; spreading over several makes it worse, which is why a DN that refuses
    /// fails the whole batch rather than being quietly subtracted (§17.31.1).
    fn create(self: &Arc<Self>, process: &Arc<Process>, request: &Req, mut reply: Reply) {
        let wanted = {
            let (mut payload, rest) =
                match structure(&request.args, Op::BatchCreate, "a batch-create request") {
                    Ok(value) => value,
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                };
            let count = match payload.number() {
                Ok(count) => count as usize,
                Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
            };
            let mut wanted = Vec::with_capacity(count);
            for _ in 0..count {
                match Wanted::read(&mut payload) {
                    Ok(one) => wanted.push(one),
                    Err(e) => {
                        return reply.send(Resp::error(Status::BadRequest, e.to_string()));
                    }
                }
            }
            if let Err(e) = payload.done() {
                return reply.send(Resp::error(Status::BadRequest, e.to_string()));
            }
            if !rest.is_empty() {
                return reply.send(Resp::error(
                    Status::BadRequest,
                    "a batch-create request has trailing arguments",
                ));
            }
            wanted
        };
        let dns: Vec<String> =
            process.all_of(ServiceKind::Dn).iter().map(|dn| dn.to_string()).collect();
        if dns.len() < 2 || wanted.len() < 2 {
            return self.onward(process, request, reply);
        }

        let owners: Vec<usize> =
            wanted.iter().map(|one| routing::owner_of(&one.name, dns.len())).collect();
        let groups = by_owner(&owners);
        reply.about(format_args!("{},dns {}", first(request), groups.len()));
        let making = Arc::new(Mutex::new(Making {
            left: groups.len(),
            made: 0,
            failed: None,
            reply: Some(reply),
        }));

        for (dn, at) in groups {
            let mut payload = StructWriter::new();
            payload.number(at.len() as u64);
            for one in at.iter().map(|at| &wanted[*at]) {
                one.write(&mut payload);
            }
            let ask = Req::new(Op::BatchCreate).structure(payload);
            let identity = dns[dn].clone();
            let waiting = Arc::clone(&making);
            let named = identity.clone();
            if let Err(e) = process.ask(&identity, &ask, move |answer| made(&waiting, answer)) {
                logw!("could not reach a DN", "dn {named},{e}");
                made(&making, Resp::error(Status::Internal, format!("could not reach {named}")));
            }
        }
    }

    /// Forward a request whole to the DN that should answer it.
    fn onward(&self, process: &Arc<Process>, request: &Req, mut reply: Reply) {
        let dn = match self.destination(process, request) {
            Ok(dn) => dn.to_string(),
            Err(e) => return reply.send(Resp::error(Status::Internal, e.to_string())),
        };
        // Which DN it went to belongs on the line, since that is the hop a live-site
        // argument is about.
        reply.about(format_args!("{},dn {dn}", first(request)));
        if let Err(e) = process.ask(&dn, request, move |answer| reply.send(answer)) {
            // The reply went into the closure, so this needs its own.
            logw!("could not reach a DN", "dn {dn},{e}");
        }
    }

    /// Which DN answers for a request.
    ///
    /// ⚠️ **By name, not in turn.** A name has an owner, and taking DNs in turn means a
    /// create lands on one machine and the lookup that follows it asks another — which with
    /// one DN is invisible and with two loses half the objects.
    fn destination<'a>(&self, process: &'a Process, request: &Req) -> Result<&'a str> {
        match request.op {
            Op::CreateObject | Op::ObjectInfo | Op::BatchInfo => {
                process.owner(ServiceKind::Dn, &first(request))
            }
            // A batch create reaching here is one DN's worth: `FE::create` has already
            // grouped by owner, or there was nothing to group (#98). Either way the name to
            // route by is the first one.
            Op::BatchCreate => process.owner(ServiceKind::Dn, &first(request)),
            _ => process.pick(ServiceKind::Dn),
        }
    }
}

/// A batch create being counted up from several DNs.
struct Making {
    left: usize,
    made: u64,
    /// ⚠️ Set when any DN refused, so the whole batch fails. A short count with no way to
    /// say which names it is short of is not a partial answer, it is an unusable one.
    failed: Option<Resp>,
    reply: Option<Reply>,
}

/// One DN's count. The last one in answers.
fn made(making: &Arc<Mutex<Making>>, answer: Resp) {
    let mut held = making.lock().expect("making");
    let number = structure(&answer.args, Op::NumberResp, "a number response").and_then(
        |(mut payload, rest)| {
            if !rest.is_empty() {
                return Err(common::Error::new("a number response has trailing arguments"));
            }
            let number = payload.number()?;
            payload.done()?;
            Ok(number)
        },
    );
    match (answer.status, number) {
        (Status::Ok, Ok(made)) => held.made += made,
        (Status::Ok, _) => {
            held.failed.get_or_insert(Resp::error(
                Status::Internal,
                "a batch create answered without saying how many it made",
            ));
        }
        _ => {
            held.failed.get_or_insert(answer);
        }
    }
    held.left -= 1;
    if held.left > 0 {
        return;
    }
    let Some(reply) = held.reply.take() else { return };
    match held.failed.take() {
        Some(failed) => reply.send(failed),
        None => reply.send(Resp::number(Status::Ok, held.made)),
    }
}

/// Group name indices by the DN that owns them, in the order the DNs first appear — so the
/// DN owning the earliest name comes first, which is the one that gets the detailed ask
/// (§9.1.3.12).
fn by_owner(owners: &[usize]) -> Vec<(usize, Vec<usize>)> {
    let mut out: Vec<(usize, Vec<usize>)> = Vec::new();
    for (at, dn) in owners.iter().enumerate() {
        match out.iter_mut().find(|(owner, _)| owner == dn) {
            Some((_, at_dn)) => at_dn.push(at),
            None => out.push((*dn, vec![at])),
        }
    }
    out
}

/// Round one: who is asked what (§9.1.3.12).
///
/// The DN owning the earliest name is asked for everything the caller wanted, and every
/// other DN for a per-object summary. **What the summaries will spend is known before any
/// of them is asked** — a name costs at least its own row and a summary costs exactly that
/// (`A-NAME-COSTS-AT-LEAST-ONE-ENTRY`) — so the detailed ask gets the limit minus that
/// count without a measuring round trip.
fn plan(dns: &[String], owners: &[usize], want: Want, limit: u64) -> Vec<Ask> {
    let groups = by_owner(owners);
    let lead = groups[0].0;
    let elsewhere = owners.iter().filter(|dn| **dn != lead).count() as u64;
    groups
        .iter()
        .map(|(dn, at)| {
            let want = if want.detail == 0 {
                // No detail wanted, so every name costs exactly one entry and there is
                // nothing to estimate: each DN is asked for its own share, and they add up
                // to a count that already fits.
                Want { limit: at.len() as u64, ..want }
            } else if *dn == lead {
                // At least one name, however little room the others left it
                // (`ONE-BLOCK-ALWAYS-RETURNS`, one tier up).
                Want { limit: limit.saturating_sub(elsewhere).max(1), ..want }
            } else {
                // Counts, and not streams: the object row and the exact number of streams
                // it has. One entry, and no SN hop.
                Want {
                    detail: Detail::Counts.bit(),
                    limit: at.len() as u64,
                    from: want.from,
                    expect: want.expect,
                }
            };
            Ask { dn: dns[*dn].clone(), at: at.clone(), want }
        })
        .collect()
}

/// Send a round of asks, and answer when the last one is back.
fn send(fe: &Arc<FE>, process: &Arc<Process>, spread: &Arc<Mutex<Spread>>, asks: Vec<Ask>) {
    let names: Vec<String> = {
        let held = spread.lock().expect("spread");
        held.names.clone()
    };
    for ask in asks {
        let mut payload = StructWriter::new();
        payload.number(ask.at.len() as u64);
        for at in &ask.at {
            payload.text(&names[*at]);
        }
        payload.flag(true);
        ask.want.write(&mut payload);
        let request = Req::new(Op::BatchInfo).structure(payload);
        let ask = Arc::new(ask);
        let (mine, onward, waiting) = (Arc::clone(fe), Arc::clone(process), Arc::clone(spread));
        let named = ask.dn.clone();
        let carried = Arc::clone(&ask);
        let sent = process.ask(&ask.dn, &request, move |answer| {
            arrived(&mine, &onward, &waiting, &carried, answer)
        });
        if let Err(e) = sent {
            logw!("could not reach a DN", "dn {named},{e}");
            let refused = Resp::error(Status::Internal, format!("could not reach {named}"));
            arrived(fe, process, spread, &ask, refused);
        }
    }
}

/// One DN's answer. The last one in either starts the second round or replies.
fn arrived(
    fe: &Arc<FE>,
    process: &Arc<Process>,
    spread: &Arc<Mutex<Spread>>,
    ask: &Ask,
    answer: Resp,
) {
    let next = {
        let mut held = spread.lock().expect("spread");
        held.take(ask, answer);
        held.left -= 1;
        if held.left > 0 {
            return;
        }
        // ⚠️ Both decided under the one lock, because `refill` sets `left` for the round it
        // is asking for. Deciding to send and then sending must not be two steps here for
        // the same reason waiting must not be at the DN: in between, everything can finish.
        match held.refill() {
            Some(asks) => Some(asks),
            None => {
                let answer = held.answer();
                if let Some(reply) = held.reply.take() {
                    reply.send(answer);
                }
                None
            }
        }
    };
    if let Some(asks) = next {
        send(fe, process, spread, asks);
    }
}

/// The request's first argument, which for anything about an object is the name.
fn first(request: &Req) -> String {
    match request.args.first() {
        Some(Arg::Text(name)) => name.clone(),
        Some(Arg::Struct(Op::ObjectInfo, _bytes)) => {
            structure(&request.args, Op::ObjectInfo, "an object-info request")
                .and_then(|(mut payload, _)| payload.text())
                .unwrap_or_default()
        }
        Some(Arg::Struct(Op::BatchInfo, _bytes)) => {
            structure(&request.args, Op::BatchInfo, "a batch-object-info request")
                .and_then(|(mut payload, _)| {
                    let _ = payload.number()?;
                    payload.text()
                })
                .unwrap_or_default()
        }
        Some(Arg::Struct(Op::CreateObject, _bytes)) => {
            structure(&request.args, Op::CreateObject, "a create-object request")
                .and_then(|(mut payload, _)| payload.text())
                .unwrap_or_default()
        }
        Some(Arg::Struct(Op::BatchCreate, _bytes)) => {
            structure(&request.args, Op::BatchCreate, "a batch-create request")
                .and_then(|(mut payload, _)| {
                    let _ = payload.number()?;
                    payload.text()
                })
                .unwrap_or_default()
        }
        _ => String::new(),
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
    use common::ids::{ObjectId, RangeId, StreamId};
    use common::info::{ObjectName, StreamRow, Totals};

    fn dns(count: usize) -> Vec<String> {
        (0..count).map(|n| format!("DN{n}")).collect()
    }

    /// The DN owning the earliest name comes first, because it is the one that gets the
    /// detailed ask — a prefix in the caller's order is not a prefix in any DN's order.
    #[test]
    fn the_dn_owning_the_first_name_leads() {
        let groups = by_owner(&[2, 0, 2, 1, 0]);
        assert_eq!(groups[0], (2, vec![0, 2]), "DN2 owns name 0, so DN2 leads");
        assert_eq!(groups[1], (0, vec![1, 4]));
        assert_eq!(groups[2], (1, vec![3]));
    }

    /// Round one, on the numbers §9.1.3.12 argues: the lead is given the limit less one
    /// entry for every name held elsewhere, and everyone else is asked for counts only.
    #[test]
    fn the_lead_is_given_the_limit_less_what_the_others_will_spend() {
        let want = Want::plain().with(Detail::Streams);
        // Six names: three on the lead, three elsewhere. Limit 12.
        let asks = plan(&dns(3), &[0, 1, 2, 0, 1, 0], want, 12);
        assert_eq!(asks.len(), 3);
        assert_eq!(asks[0].dn, "DN0");
        assert_eq!(asks[0].want.limit, 9, "12 less one apiece for the three elsewhere");
        assert_eq!(asks[0].want.detail, want.detail, "and everything that was asked for");
        for other in &asks[1..] {
            assert_eq!(other.want.detail, Detail::Counts.bit(), "counts, and no rows");
            assert!(!other.want.wants(Detail::Streams), "so no SN is asked anything");
            assert_eq!(other.want.limit, other.at.len() as u64, "one entry per name");
        }
    }

    /// ⚠️ The lead always gets room for one name, however many the others reserved. A
    /// limit that can answer nothing cannot make progress (`ONE-BLOCK-ALWAYS-RETURNS`).
    #[test]
    fn the_lead_always_gets_room_for_one() {
        let want = Want::plain().with(Detail::Streams);
        let asks = plan(&dns(2), &[0, 1, 1, 1, 1], want, 3);
        assert_eq!(asks[0].want.limit, 1);
    }

    /// With no detail wanted every name costs exactly one entry, so there is nothing to
    /// estimate and nobody is singled out.
    #[test]
    fn no_detail_needs_no_reservation() {
        let asks = plan(&dns(2), &[0, 1, 0, 1], Want::plain(), 10);
        for ask in &asks {
            assert_eq!(ask.want.detail, 0);
            assert_eq!(ask.want.limit, 2);
        }
    }

    /// A `Spread` holding `n` names, none answered yet.
    fn spreading(n: usize, owners: Vec<usize>, want: Want, limit: u64) -> Spread {
        Spread {
            left: 1,
            asked: n,
            want,
            limit,
            got: vec![None; n],
            failed: None,
            reply: None,
            filled: false,
            owners,
            dns: dns(2),
            names: (0..n).map(|i| format!("/n{i}")).collect(),
        }
    }

    /// One name's worth of a levelled answer, as a DN would send it.
    fn described(streams: u64, given: u64, next: u64) -> Vec<Arg> {
        let shape = Shape { streams, given, extents: UNKNOWN, entries: 1 + given, next };
        let mut args = shape.args();
        args.extend(
            Described {
                object: ObjectName {
                    range: RangeId::new(0),
                    object: ObjectId::new(1),
                    created: TimeSec::EPOCH,
                },
                dn: DnId::new(0),
                stream: StreamName {
                    range: RangeId::new(0),
                    stream: StreamId::new(1),
                    created: TimeSec::new(1_000_000),
                },
                sn: SnId::new(0),
                keyed: false,
                framing: Framing::Lf,
                first_record: 0,
            }
            .args(),
        );
        for _ in 0..given {
            args.extend(
                StreamRow {
                    stream: StreamId::new(1),
                    sn: SnId::new(0),
                    first_record: 0,
                    records: 3,
                    created: TimeSec::new(1_000_000),
                    totals: Totals::unknown(),
                    given: 0,
                }
                .args(),
            );
        }
        args
    }

    fn answer_of(pieces: &[Vec<Arg>], entries: u64) -> Resp {
        let covered = Batch {
            asked: pieces.len() as u64,
            given: pieces.len() as u64,
            entries,
            next: UNKNOWN,
        };
        let mut args = covered.args();
        for piece in pieces {
            args.extend(piece.iter().cloned());
        }
        Resp { id: 0, status: Status::Ok, args }
    }

    /// A summary is a valid answer for a name and not a finished one, so the continuation
    /// points at it — even though every name asked about came back (§17.23.4).
    #[test]
    fn a_summary_is_not_a_finished_name() {
        let want = Want::plain().with(Detail::Streams);
        let mut spread = spreading(2, vec![0, 1], want, 10);
        let lead = Ask { dn: "DN0".into(), at: vec![0], want };
        spread.take(&lead, answer_of(&[described(1, 1, UNKNOWN)], 2));
        let other = Ask {
            dn: "DN1".into(),
            at: vec![1],
            want: Want { detail: Detail::Counts.bit(), ..want },
        };
        spread.take(&other, answer_of(&[described(1, 0, 0)], 1));
        assert_eq!(spread.unfinished(), Some(1));
        let answer = spread.answer();
        let (covered, _) = Batch::of_args(&answer.args).expect("a header");
        assert_eq!((covered.given, covered.entries), (2, 3));
        assert_eq!(covered.next, 1, "come back for the one described only in counts");
    }

    /// Round two upgrades a contiguous run from the first unfinished name, and stops when
    /// the slack runs out rather than skipping ahead to a cheaper name further on.
    #[test]
    fn round_two_fills_from_the_front_and_stops() {
        let want = Want::plain().with(Detail::Streams);
        let mut spread = spreading(4, vec![0, 1, 1, 1], want, 8);
        let lead = Ask { dn: "DN0".into(), at: vec![0], want };
        spread.take(&lead, answer_of(&[described(1, 1, UNKNOWN)], 2));
        // Three summaries, each of an object with three streams: full cost 4 apiece.
        let summary = Ask {
            dn: "DN1".into(),
            at: vec![1, 2, 3],
            want: Want { detail: Detail::Counts.bit(), ..want },
        };
        let one = described(3, 0, 0);
        spread.take(&summary, answer_of(&[one.clone(), one.clone(), one], 3));
        // Spent 5 of 8, so 3 to spend and an upgrade costs 3: exactly one of them.
        let asks = spread.refill().expect("a second round");
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].at, vec![1], "the first unfinished name, and no further");
        assert_eq!(asks[0].want.limit, 4, "sized from the count the summary reported");
        assert_eq!(asks[0].want.detail, want.detail, "and this time in full");
    }

    /// ⚠️ The one property that makes every estimate here safe: assembly walks the caller's
    /// order and stops at the first name nobody produced, so a short answer is a prefix and
    /// never a hole (`A-BATCH-ANSWER-IS-A-PREFIX`).
    #[test]
    fn a_hole_ends_the_answer() {
        let want = Want::plain().with(Detail::Streams);
        let mut spread = spreading(3, vec![0, 1, 0], want, 10);
        // The lead answers about names 0 and 2; nobody answers about name 1.
        let lead = Ask { dn: "DN0".into(), at: vec![0, 2], want };
        spread.take(&lead, answer_of(&[described(1, 1, UNKNOWN), described(1, 1, UNKNOWN)], 4));
        let answer = spread.answer();
        let (covered, _) = Batch::of_args(&answer.args).expect("a header");
        assert_eq!(covered.given, 1, "name 2 is held back rather than passed off as name 1");
        assert_eq!(covered.next, 1);
    }

    /// A DN that refused is not a DN whose objects are absent.
    #[test]
    fn a_refusal_is_not_an_absence() {
        let want = Want::plain().with(Detail::Streams);
        let mut spread = spreading(2, vec![0, 1], want, 10);
        let lead = Ask { dn: "DN0".into(), at: vec![0], want };
        spread.take(&lead, answer_of(&[described(1, 1, UNKNOWN)], 2));
        let other = Ask { dn: "DN1".into(), at: vec![1], want };
        spread.take(&other, Resp::error(Status::Internal, "DN1 is down"));
        assert_eq!(spread.answer().status, Status::Internal);
    }

    /// A caller's record number goes in through two origins and an answer comes back
    /// out through their sum, so the two directions have to agree.
    ///
    /// ⚠️ This is the bug that survived until the first seal: both origins are zero
    /// while an object fits one extent, so dropping either one is invisible until it
    /// does not.
    #[test]
    fn a_record_number_survives_the_trip_through_three_spaces() {
        use super::{in_extent, in_stream, in_the_callers_numbering, origin};
        use common::wire::{Resp, Status};

        // ⚠️ Each step names the space it lands in, which is what stops the two
        // conversions being the same helper called twice (§17.32.1).
        let stream_first = ORoff::new(700);
        let extent_first = SRoff::new(40);
        for asked in [740, 741, 800, 12_345] {
            let at_stream: SRoff = in_stream(ORoff::new(asked), stream_first);
            let at_extent: ERoff = in_extent(at_stream, extent_first);
            let back: ORoff = origin(stream_first, extent_first);
            assert_eq!(at_extent.number() + back.number(), asked, "record {asked}");
        }

        // And **every** record number in the answer is rewritten, while the three
        // byte-and-count measures are left alone.
        //
        // ⚠️ An earlier version of this test asserted that argument index 1 was
        // patched, which is what the code did. When the answer grew a header saying
        // what it covered, index 1 became a byte count — so the test would have gone
        // on passing while the code corrupted a measure and reported record numbers
        // unconverted. Reading the shape is what makes the test about the meaning.
        let covered = Covered { records: 3, logical: 9, compressed: 9, next: 3, more: false };
        let piece = |first_record| Piece {
            logical: 3,
            first_record,
            records: 1,
            crc: 0,
            bytes: b"a\n".to_vec(),
        };
        let mut args = covered.args();
        for first_record in [0, 1, 2] {
            args.extend(piece(first_record).args());
        }
        let told = in_the_callers_numbering(
            Resp { id: 0, status: Status::Ok, args },
            origin(stream_first, extent_first),
            true,
        );

        let (back, rest) = Covered::of_args(&told.args).expect("what it covered");
        assert_eq!(back.next, 743, "the continuation, made the caller's");
        assert_eq!((back.records, back.logical, back.compressed), (3, 9, 9), "measures untouched");
        assert!(back.more, "and a sealed extent has something after it");
        let pieces = Piece::all_of_args(rest).expect("blocks");
        let numbers: Vec<u64> = pieces.iter().map(|piece| piece.first_record).collect();
        assert_eq!(numbers, [740, 741, 742], "every block, not just the first");
        assert!(pieces.iter().all(|piece| piece.logical == 3), "and not their sizes");
    }

    /// A read of the extent still open is the object's tail, so what the EN says
    /// about its own blocks is the whole answer. A sealed extent has something after
    /// it, which only the FE knows.
    #[test]
    fn only_the_fe_can_say_whether_the_object_has_more() {
        let covered = Covered { records: 1, logical: 2, compressed: 2, next: 1, more: false };
        let answer = || Resp { id: 0, status: Status::Ok, args: covered.args() };

        let tail = in_the_callers_numbering(answer(), ORoff::new(0), false);
        assert!(!Covered::of_args(&tail.args).unwrap().0.more, "the open extent ends the object");
        let sealed = in_the_callers_numbering(answer(), ORoff::new(0), true);
        assert!(Covered::of_args(&sealed.args).unwrap().0.more, "a sealed one does not");
    }

    /// A refusal is passed through untouched: there are no record numbers in it to
    /// convert, and dressing it up as an answer would be worse than useless.
    #[test]
    fn a_refusal_is_not_converted() {
        let refused = Resp::error(Status::NotFound, "no such record");
        assert_eq!(in_the_callers_numbering(refused.clone(), ORoff::new(700), true), refused);
    }

    use common::testing;

    /// The SN answers with the id, the second, the first record, how many records, the count, then the instances.
    #[test]
    fn an_open_extent_is_read_out_of_the_sns_answer() {
        let mut payload = StructWriter::new();
        for value in [7, 1_000_000, 40, 90, 4, 2, 0, 3, 0, 0, 0, 1, 0] {
            payload.number(value);
        }
        let response =
            Resp { id: 0, status: Status::Ok, args: vec![payload.finish(Op::ExtentResp)] };
        let open = Open::of(SnId::new(0), Framing::Lf, &response.args).expect("an extent");
        assert_eq!(
            (open.sn, open.id, open.created, open.first_record),
            (SnId::new(0), ExtentId::new(7), TimeSec::new(1_000_000), SRoff::new(40))
        );
        assert_eq!(open.records, ERoff::new(90), "sealed, so its range is known");
        assert_eq!(
            open.disks,
            [
                (EnId::new(2), DiskId::new(0)),
                (EnId::new(3), DiskId::new(0)),
                (EnId::new(0), DiskId::new(0)),
                (EnId::new(1), DiskId::new(0)),
            ]
        );
        // The primary is the first, which is the one an append is sent to.
        assert_eq!(open.disks[0], (EnId::new(2), DiskId::new(0)));
    }

    /// An extent with nowhere to put it is not an extent.
    #[test]
    fn an_extent_with_no_instances_is_refused() {
        assert!(
            Open::of(
                SnId::new(0),
                Framing::Lf,
                &[Arg::Number(7), Arg::Number(1_000_000), Arg::Number(0), Arg::Number(0)]
            )
            .is_none()
        );
        assert!(
            Open::of(SnId::new(0), Framing::Lf, &[Arg::Number(7), Arg::Number(1_000_000)])
                .is_none()
        );
        assert!(Open::of(SnId::new(0), Framing::Lf, &[]).is_none());
    }

    /// The DN's answer says which stream, which SN, and how the records are framed.
    #[test]
    fn an_object_is_read_out_of_the_dns_answer() {
        // ⚠️ Built through the shape rather than as a list of numbers, so that a field
        // growing — as the stream id did into a whole name, #109 — is a compile error here
        // instead of a row that parses into the wrong things.
        let args = Described {
            object: ObjectName {
                range: RangeId::new(3),
                object: ObjectId::new(9),
                created: TimeSec::new(1_000_000),
            },
            dn: DnId::new(3),
            stream: StreamName {
                range: RangeId::new(3),
                stream: StreamId::new(17),
                created: TimeSec::new(1_000_000),
            },
            sn: SnId::new(0),
            keyed: false,
            framing: Framing::CrLf,
            first_record: 400,
        }
        .args();
        let located = Located::of(&args).expect("an object");
        assert_eq!(
            (located.dn, located.stream.stream, located.sn),
            (DnId::new(3), StreamId::new(17), SnId::new(0))
        );
        assert_eq!(located.framing, Framing::CrLf);
        assert_eq!(located.first_record, ORoff::new(400), "where this stream's records start");
        // A framing the DN could not have meant is not an object either.
        assert!(Located::of(&args[..0]).is_none());
    }

    /// The receipt leads with the durable reference and follows with the hint, and
    /// the record number it reports is stream-local rather than extent-local.
    #[test]
    fn a_receipt_leads_with_the_durable_reference() {
        let located = Located {
            dn: DnId::new(0),
            stream: StreamName {
                range: RangeId::new(0),
                stream: StreamId::new(17),
                created: TimeSec::new(1_000_000),
            },
            sn: SnId::new(0),
            framing: Framing::Lf,
            first_record: ORoff::ZERO,
        };
        let open = Open {
            sn: SnId::new(0),
            id: ExtentId::new(7),
            created: TimeSec::new(1_000_000),
            first_record: SRoff::new(40),
            records: ERoff::ZERO,
            _framing: Framing::Lf,
            disks: vec![(EnId::new(2), DiskId::new(0)), (EnId::new(3), DiskId::new(0))],
        };
        let mut from_en_payload = StructWriter::new();
        for value in [0, 7, 1_000_000, 512, 30, 2, 1] {
            from_en_payload.number(value);
        }
        let from_en =
            Resp { id: 0, status: Status::Ok, args: vec![from_en_payload.finish(Op::ReceiptResp)] };
        let receipt = receipt(&located, &open, from_en);
        assert_eq!(receipt.status, Status::Ok);
        let (mut payload, rest) = structure(&receipt.args, Op::ReceiptResp, "a receipt").unwrap();
        assert!(rest.is_empty());
        let values: Vec<_> = (0..8).map(|_| payload.number().unwrap()).collect();
        assert_eq!(values, [17, 42, 42, 1, 7, 1_000_000, 512, 30]);
        payload.done().unwrap();
    }

    /// A refusal from the EN is passed through as it stands: FE has nothing to add
    /// to it and must not turn it into a receipt.
    #[test]
    fn a_failed_append_is_not_dressed_up_as_a_receipt() {
        let located = Located {
            dn: DnId::new(0),
            stream: StreamName {
                range: RangeId::new(0),
                stream: StreamId::new(17),
                created: TimeSec::new(1_000_000),
            },
            sn: SnId::new(0),
            framing: Framing::Lf,
            first_record: ORoff::ZERO,
        };
        let open = Open {
            sn: SnId::new(0),
            id: ExtentId::new(7),
            created: TimeSec::new(1_000_000),
            first_record: SRoff::ZERO,
            records: ERoff::ZERO,
            _framing: Framing::Lf,
            disks: vec![(EnId::new(2), DiskId::new(0))],
        };
        let refused = Resp::error(Status::Internal, "two instances would not apply it");
        assert_eq!(receipt(&located, &open, refused.clone()), refused);
    }

    fn extent(id: u32, first_record: u64) -> Open {
        Open {
            sn: SnId::new(0),
            id: ExtentId::new(id),
            created: TimeSec::new(1_000_000),
            first_record: SRoff::new(first_record),
            records: ERoff::ZERO,
            _framing: Framing::Lf,
            disks: vec![
                (EnId::new(2), DiskId::new(0)),
                (EnId::new(3), DiskId::new(0)),
                (EnId::new(0), DiskId::new(0)),
                (EnId::new(1), DiskId::new(0)),
            ],
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
        assert_eq!(
            stream.plan(Some((ExtentId::new(1), ERoff::new(90)))),
            Plan::Ask,
            "the first to notice"
        );
        assert_eq!(
            stream.plan(Some((ExtentId::new(1), ERoff::new(90)))),
            Plan::Join,
            "the rest of them"
        );
        assert_eq!(stream.plan(Some((ExtentId::new(1), ERoff::new(90)))), Plan::Join);
        // And it was remembered on the way past, which is what a read wants.
        assert_eq!(stream.sealed.len(), 1);
        assert_eq!(stream.sealed[0].records, ERoff::new(90));
    }

    /// An append that hears about an extent already replaced uses the replacement
    /// rather than asking for a third one.
    #[test]
    fn an_append_late_to_a_seal_uses_what_replaced_it() {
        let mut stream = Stream { open: Some(extent(2, 90)), ..Stream::default() };
        assert_eq!(stream.plan(Some((ExtentId::new(1), ERoff::new(90)))), Plan::Use);
        assert!(stream.sealed.is_empty(), "extent 1 was not this FE's to remember");
    }

    /// A sealed extent's record range never changes, so a read of it never has to ask
    /// the SN again. The extent still open is not rememberable — it reaches further
    /// with every append.
    #[test]
    fn a_read_finds_a_record_in_a_remembered_extent() {
        let mut stream = Stream { open: Some(extent(1, 0)), ..Stream::default() };
        stream.plan(Some((ExtentId::new(1), ERoff::new(90))));
        stream.open = Some(extent(2, 90));
        stream.plan(Some((ExtentId::new(2), ERoff::new(80))));
        stream.open = Some(extent(3, 170));

        assert_eq!(stream.holding(SRoff::new(0)).unwrap().id, ExtentId::new(1));
        assert_eq!(stream.holding(SRoff::new(89)).unwrap().id, ExtentId::new(1));
        assert_eq!(stream.holding(SRoff::new(90)).unwrap().id, ExtentId::new(2));
        assert_eq!(stream.holding(SRoff::new(169)).unwrap().id, ExtentId::new(2));
        // In the extent still open, so the SN has to be asked.
        assert!(stream.holding(SRoff::new(170)).is_none());
        assert!(stream.holding(SRoff::new(5000)).is_none());
    }

    /// An FE that never wrote an extent can still learn where its records are, because
    /// an SN naming a sealed extent says how much it holds. Otherwise a read-only front
    /// end asks about every read for ever.
    #[test]
    fn a_read_teaches_an_fe_that_never_wrote() {
        let mut stream = Stream::default();
        assert!(stream.holding(SRoff::new(5)).is_none(), "nothing known yet");

        // What a locate answers for a sealed extent: 90 records from 0.
        let mut sealed = extent(1, 0);
        sealed.records = ERoff::new(90);
        stream.remember(sealed);
        assert_eq!(stream.holding(SRoff::new(5)).unwrap().id, ExtentId::new(1));
        assert_eq!(stream.holding(SRoff::new(89)).unwrap().id, ExtentId::new(1));
        assert!(stream.holding(SRoff::new(90)).is_none());

        // Learning it twice keeps one copy, since appends and reads both teach it.
        stream.remember(stream.holding(SRoff::new(5)).unwrap());
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
        assert!(fe.defers(Op::BatchInfo));
        assert!(fe.defers(Op::BatchCreate));
        assert!(!fe.defers(Op::Ping));
        // Only control operations are inline; everything that talks to another process goes to workers.
        assert!(fe.inline(Op::Ping));
        assert!(fe.inline(Op::Status));
        assert!(!fe.inline(Op::AppendRecord));
        assert!(!fe.inline(Op::ReadRecord));
        assert!(!fe.inline(Op::ObjectInfo));
        assert!(!fe.inline(Op::CreateObject));
        assert!(!fe.inline(Op::BatchInfo));
        assert!(!fe.inline(Op::BatchCreate));
    }

    /// An unbuilt operation is still one FE is the right place to send.
    #[test]
    fn the_unbuilt_record_operations_are_still_ours() {
        assert!(SERVES.contains(&Op::Scan));
        assert!(!COORDINATED.contains(&Op::Scan));
        assert!(!FORWARDED.contains(&Op::Scan));
    }
}
