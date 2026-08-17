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

use common::config::{FEConfig, ServiceKind};
use common::framing::Framing;
use common::logw;
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
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

/// An extent a stream is appending to, as the SN described it.
#[derive(Clone)]
struct Open {
    sn: u32,
    id: u64,
    created: u64,
    /// The stream-local number of this extent's first record, which is what turns
    /// an extent-local number into a durable reference.
    first_record: u64,
    /// The instances, primary first.
    instances: Vec<u32>,
}

impl Open {
    /// The id, the second, the first record, then the instances, which is the
    /// order the SN answers in.
    fn of(sn: u32, args: &[Arg]) -> Option<Open> {
        let [Arg::Number(id), Arg::Number(created), Arg::Number(first_record), instances @ ..] =
            args
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
            instances,
        })
    }
}

struct FE {
    config: FEConfig,
    /// The open extent per `(DnId, StreamId)`.
    open: Mutex<HashMap<(u32, u64), Open>>,
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
        vec![
            format!("stateless {}", self.config.stateless),
            format!("open_extents {}", self.open.lock().expect("extents").len()),
        ]
    }
}

impl FE {
    fn new(config: FEConfig) -> FE {
        FE { config, open: Mutex::new(HashMap::new()) }
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
        self.resolving(process, path, make, true, reply, then)
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
        let ask = Request::new(Op::ObjectInfo).text(&path);
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
        let (path, framing, body) = match request.args.as_slice() {
            [Arg::Text(path), Arg::Number(kind), Arg::Number(width), Arg::Bytes(body)] => {
                match Framing::of_wire(*kind, *width) {
                    Ok(framing) => (path.clone(), framing, body.clone()),
                    Err(e) => {
                        return reply.send(Response::error(Status::BadRequest, e.to_string()));
                    }
                }
            }
            _ => {
                return reply.send(Response::error(
                    Status::BadRequest,
                    "append takes a name, a framing and bytes",
                ));
            }
        };
        reply.about(format_args!("{path},bytes {}", body.len()));
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
            fe.to_extent(process, located, body, reply);
        });
    }

    /// The extent this stream is appending to, asked of the SN only if it is not
    /// already known.
    fn to_extent(
        self: &Arc<Self>,
        process: &Arc<Process>,
        located: Located,
        body: Vec<u8>,
        reply: Reply,
    ) {
        let key = (located.dn, located.stream);
        if let Some(open) = self.open.lock().expect("extents").get(&key) {
            return self.to_primary(process, located, open.clone(), body, TRIES, reply);
        }
        let identity = format!("SN{}", located.sn);
        let ask = Request::new(Op::OpenExtent).number(located.dn as u64).number(located.stream);
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
            fe.open.lock().expect("extents").insert(key, open.clone());
            fe.to_primary(&onward, located, open, body, TRIES, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {named},{e}");
        }
    }

    /// The bytes, to the primary instance. The framing and the instance list travel
    /// with them: the primary is what counts the records and what broadcasts to the
    /// others, and the SN is not in this path to be asked again.
    ///
    /// `tries` is what is left of the retries an append gets: a full extent is
    /// answered `Sealed`, and the append belongs in the next one. Several writers can
    /// fill successive extents while one append is being placed, so a retry is not
    /// always the last one needed.
    fn to_primary(
        self: &Arc<Self>,
        process: &Arc<Process>,
        located: Located,
        open: Open,
        body: Vec<u8>,
        tries: u32,
        reply: Reply,
    ) {
        let identity = format!("EN{}", open.instances[0]);
        let (kind, width) = located.framing.wire();
        let mut ask = Request::new(Op::AppendExtent)
            .number(open.sn as u64)
            .number(open.id)
            .number(open.created)
            .number(kind)
            .number(width);
        for en in &open.instances {
            ask = ask.number(*en as u64);
        }
        let ask = ask.bytes(body.clone());
        let named = identity.clone();
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status == Status::Sealed {
                return fe.after_seal(&onward, located, open, body, tries, answer, reply);
            }
            reply.send(receipt(&located, &open, answer));
        });
        if let Err(e) = asked {
            logw!("could not reach an EN", "en {named},{e}");
        }
    }

    /// The extent filled up. Tell the SN what it came to, take the next one, and
    /// send the append again — once. A fresh extent that also says it is sealed
    /// means the append is larger than an extent can hold, which is a bad request
    /// and not a loop to keep going round.
    #[allow(clippy::too_many_arguments)]
    fn after_seal(
        self: &Arc<Self>,
        process: &Arc<Process>,
        located: Located,
        open: Open,
        body: Vec<u8>,
        tries: u32,
        answer: Response,
        reply: Reply,
    ) {
        let [Arg::Number(records), Arg::Number(length)] = answer.args.as_slice() else {
            return reply
                .send(Response::error(Status::Internal, "a seal said nothing about the extent"));
        };
        // An extent that sealed with nothing in it is one this block could not fit
        // even empty, which is the block's fault and not worth another extent.
        if *records == 0 && *length == 0 {
            return reply.send(Response::error(
                Status::BadRequest,
                format!("{} bytes will not fit an extent of its own", body.len()),
            ));
        }
        if tries == 0 {
            return reply.send(Response::error(
                Status::Internal,
                "kept finding a sealed extent, which should not go on this long",
            ));
        }
        // What was cached is closed, so nothing should hand it out again.
        let key = (located.dn, located.stream);
        self.open.lock().expect("extents").remove(&key);

        let identity = format!("SN{}", located.sn);
        let ask = Request::new(Op::SealExtent)
            .number(located.dn as u64)
            .number(located.stream)
            .number(open.id)
            .number(*records)
            .number(*length);
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let named = identity.clone();
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(next) = Open::of(located.sn, &answer.args) else {
                return reply.send(Response::error(Status::Internal, "that seal opened no extent"));
            };
            fe.open.lock().expect("extents").insert(key, next.clone());
            fe.to_primary(&onward, located, next, body, tries - 1, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {named},{e}");
        }
    }

    /// A read: resolve the name, ask the SN which extent holds the record, read it
    /// from an instance.
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
        self.resolve(process, &path, None, reply, move |fe, process, located, reply| {
            fe.locate(process, located, record, reply);
        });
    }

    /// Which extent holds a record. The SN answers that and nothing about the
    /// record itself: it keeps its record totals lazily, so what it knows is where
    /// an extent's numbering starts.
    fn locate(
        self: &Arc<Self>,
        process: &Arc<Process>,
        located: Located,
        record: u64,
        reply: Reply,
    ) {
        let identity = format!("SN{}", located.sn);
        let ask = Request::new(Op::LocateRecord)
            .number(located.dn as u64)
            .number(located.stream)
            .number(record);
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
            from_instance(&onward, &located, &open, record, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {named},{e}");
        }
    }
}

/// Make the object an append is for, and go on with it. A create answers with the
/// object it made, in the shape a lookup answers, so this costs one hop and not two.
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
            return fe.resolving(&onward, &path, Some(framing), false, reply, then);
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
    let (kind, width) = located.framing.wire();
    // The record number the extent knows is its own, counted from its first.
    let within = record.saturating_sub(open.first_record);
    let ask = Request::new(Op::ReadExtent)
        .number(open.sn as u64)
        .number(open.id)
        .number(open.created)
        .number(kind)
        .number(width)
        .number(within);
    let named = identity.clone();
    if let Err(e) = process.ask(&identity, &ask, move |answer| reply.send(answer)) {
        logw!("could not reach an EN", "en {named},{e}");
    }
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
    Response {
        id: 0,
        status: Status::Ok,
        args: vec![
            Arg::Number(located.stream),
            Arg::Number(open.first_record + first),
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
        ] = args
        else {
            return None;
        };
        Some(Located {
            dn,
            stream: *stream,
            sn: *sn as u32,
            framing: Framing::of_wire(*kind, *width).ok()?,
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
            Arg::Number(2),
            Arg::Number(3),
            Arg::Number(0),
            Arg::Number(1),
        ];
        let open = Open::of(0, &args).expect("an extent");
        assert_eq!((open.sn, open.id, open.created, open.first_record), (0, 7, 1_000_000, 40));
        assert_eq!(open.instances, [2, 3, 0, 1]);
        // The primary is the first, which is the one an append is sent to.
        assert_eq!(open.instances[0], 2);
    }

    /// An extent with nowhere to put it is not an extent.
    #[test]
    fn an_extent_with_no_instances_is_refused() {
        assert!(Open::of(0, &[Arg::Number(7), Arg::Number(1_000_000), Arg::Number(0)]).is_none());
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
        ];
        let located = Located::of(3, &args).expect("an object");
        assert_eq!((located.dn, located.stream, located.sn), (3, 17, 0));
        assert_eq!(located.framing, Framing::CrLf);
        // A framing the DN could not have meant is not an object either.
        assert!(Located::of(3, &args[..5]).is_none());
    }

    /// The receipt leads with the durable reference and follows with the hint, and
    /// the record number it reports is stream-local rather than extent-local.
    #[test]
    fn a_receipt_leads_with_the_durable_reference() {
        let located = Located { dn: 0, stream: 17, sn: 0, framing: Framing::Lf };
        let open =
            Open { sn: 0, id: 7, created: 1_000_000, first_record: 40, instances: vec![2, 3] };
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
        let located = Located { dn: 0, stream: 17, sn: 0, framing: Framing::Lf };
        let open = Open { sn: 0, id: 7, created: 1_000_000, first_record: 0, instances: vec![2] };
        let refused = Response::error(Status::Internal, "two instances would not apply it");
        assert_eq!(receipt(&located, &open, refused.clone()), refused);
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
