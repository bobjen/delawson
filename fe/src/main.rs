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

/// What FE coordinates rather than forwards.
const COORDINATED: &[Op] = &[Op::AppendRecord];

/// An extent a stream is appending to, as the SN described it.
#[derive(Clone)]
struct Open {
    sn: u32,
    id: u64,
    created: u64,
    /// The instances, primary first.
    instances: Vec<u32>,
}

impl Open {
    /// The id, the second, then the instances, which is the order the SN answers
    /// in.
    fn of(sn: u32, args: &[Arg]) -> Option<Open> {
        let [Arg::Number(id), Arg::Number(created), instances @ ..] = args else {
            return None;
        };
        let instances: Vec<u32> = instances
            .iter()
            .filter_map(|arg| match arg {
                Arg::Number(en) => Some(*en as u32),
                _ => None,
            })
            .collect();
        (!instances.is_empty()).then_some(Open { sn, id: *id, created: *created, instances })
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
        if COORDINATED.contains(&request.op) {
            return self.append(process, &request, reply);
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

    /// The first hop of an append: ask a DN which stream the object is.
    fn append(self: &Arc<Self>, process: &Arc<Process>, request: &Request, mut reply: Reply) {
        let (path, body) = match request.args.as_slice() {
            [Arg::Text(path), Arg::Bytes(body)] => (path.clone(), body.clone()),
            _ => {
                return reply
                    .send(Response::error(Status::BadRequest, "append takes a name and bytes"));
            }
        };
        let dn = match process.pick(ServiceKind::Dn) {
            Ok(dn) => dn.to_string(),
            Err(e) => return reply.send(Response::error(Status::Internal, e.to_string())),
        };
        // The DN that answers is the one that owns the name, so its number is half
        // of the stream's name.
        let owner = process.cluster.service(&dn).map(|entry| entry.number()).unwrap_or(0);
        reply.about(format_args!("{path},bytes {},dn {dn}", body.len()));

        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let ask = Request::new(Op::ObjectInfo).text(&path);
        let asked = process.ask(&dn, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let [_, Arg::Number(stream), Arg::Number(sn), ..] = answer.args.as_slice() else {
                return reply
                    .send(Response::error(Status::Internal, "that answer named no stream"));
            };
            // A create takes the name first and names the stream afterwards, so a
            // zero here is an object caught between the two.
            if *stream == 0 {
                return reply
                    .send(Response::error(Status::Internal, format!("{path} has no stream yet")));
            }
            fe.to_extent(&onward, owner, *stream, *sn as u32, body, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach a DN", "dn {dn},{e}");
        }
    }

    /// The second hop: the extent this stream is appending to, asked of the SN only
    /// if it is not already known.
    fn to_extent(
        self: &Arc<Self>,
        process: &Arc<Process>,
        dn: u32,
        stream: u64,
        sn: u32,
        body: Vec<u8>,
        reply: Reply,
    ) {
        if let Some(open) = self.open.lock().expect("extents").get(&(dn, stream)) {
            return self.to_primary(process, open.clone(), body, reply);
        }
        let identity = format!("SN{sn}");
        let ask = Request::new(Op::OpenExtent).number(dn as u64).number(stream);
        let fe = Arc::clone(self);
        let onward = Arc::clone(process);
        let named = identity.clone();
        let asked = process.ask(&identity, &ask, move |answer| {
            if answer.status != Status::Ok {
                return reply.send(answer);
            }
            let Some(open) = Open::of(sn, &answer.args) else {
                return reply
                    .send(Response::error(Status::Internal, "that answer named no extent"));
            };
            fe.open.lock().expect("extents").insert((dn, stream), open.clone());
            fe.to_primary(&onward, open, body, reply);
        });
        if let Err(e) = asked {
            logw!("could not reach an SN", "sn {named},{e}");
        }
    }

    /// The third hop: the bytes, to the primary instance. The instance list travels
    /// with them, because the primary is what broadcasts to the others and the SN is
    /// not in this path to be asked again.
    fn to_primary(&self, process: &Arc<Process>, open: Open, body: Vec<u8>, reply: Reply) {
        let identity = format!("EN{}", open.instances[0]);
        let mut ask = Request::new(Op::AppendExtent)
            .number(open.sn as u64)
            .number(open.id)
            .number(open.created);
        for en in &open.instances {
            ask = ask.number(*en as u64);
        }
        let ask = ask.bytes(body);
        let named = identity.clone();
        // The receipt is whatever the EN says: where the bytes landed is its answer
        // to give, and FE has nothing to add to it.
        if let Err(e) = process.ask(&identity, &ask, move |answer| reply.send(answer)) {
            logw!("could not reach an EN", "en {named},{e}");
        }
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

    /// The SN answers with the id, the second, and then the instances.
    #[test]
    fn an_open_extent_is_read_out_of_the_sns_answer() {
        let args = vec![
            Arg::Number(7),
            Arg::Number(1_000_000),
            Arg::Number(2),
            Arg::Number(3),
            Arg::Number(0),
            Arg::Number(1),
        ];
        let open = Open::of(0, &args).expect("an extent");
        assert_eq!((open.sn, open.id, open.created), (0, 7, 1_000_000));
        assert_eq!(open.instances, [2, 3, 0, 1]);
        // The primary is the first, which is the one an append is sent to.
        assert_eq!(open.instances[0], 2);
    }

    /// An extent with nowhere to put it is not an extent.
    #[test]
    fn an_extent_with_no_instances_is_refused() {
        assert!(Open::of(0, &[Arg::Number(7), Arg::Number(1_000_000)]).is_none());
        assert!(Open::of(0, &[Arg::Number(7)]).is_none());
        assert!(Open::of(0, &[]).is_none());
    }

    /// Both are answered by somebody else, so neither may be answered on the read
    /// loop that read it.
    #[test]
    fn everything_fe_does_waits_on_another_process() {
        let fe = FE::new(testing::fe_config());
        assert!(fe.defers(Op::AppendRecord));
        assert!(fe.defers(Op::CreateObject));
        assert!(!fe.defers(Op::Ping));
        // And issuing the first hop is cheap enough to do on the read loop.
        assert!(fe.inline(Op::AppendRecord));
    }

    /// An unbuilt operation is still one FE is the right place to send.
    #[test]
    fn the_unbuilt_record_operations_are_still_ours() {
        assert!(SERVES.contains(&Op::ReadRecord));
        assert!(!COORDINATED.contains(&Op::ReadRecord));
        assert!(!FORWARDED.contains(&Op::ReadRecord));
    }
}
