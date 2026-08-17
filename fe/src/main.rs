//! FE, the Front End: authorization, and coordinating a client's request across
//! the tiers.
//!
//! Authorization does not exist yet, so what FE does today is forward: a create
//! or a lookup goes to a DN, and the DN goes on to an SN. It holds no state, so
//! it needs no more than the connection it keeps to each DN.
//!
//! **A forward holds nothing while it waits.** The request is handed to a DN with
//! the client's `Reply` attached to the answer, and the read loop goes back to
//! reading. What is in flight is a table entry per request, not a thread, which is
//! the difference between a forwarding tier that costs a forward and one that
//! costs a thread.

use common::config::{FEConfig, ServiceKind};
use common::logw;
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use std::sync::Arc;

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

/// What a DN can answer today. The rest of `SERVES` is here so that a client
/// sending it is told it is unbuilt rather than misdirected.
const FORWARDED: &[Op] = &[Op::CreateObject, Op::BatchCreate, Op::ObjectInfo, Op::BatchInfo];

struct FE {
    config: FEConfig,
}

impl Service for FE {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    /// Forwarded operations are answered by a DN, so the answer does not come
    /// from the thread that read the request.
    fn defers(&self, op: Op) -> bool {
        FORWARDED.contains(&op)
    }

    /// Issuing a forward is a write, not work, so it happens on the read loop.
    fn inline(&self, op: Op) -> bool {
        FORWARDED.contains(&op) || matches!(op, Op::Ping | Op::Status | Op::Shutdown | Op::Sites)
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, mut reply: Reply) {
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
        vec![format!("stateless {}", self.config.stateless)]
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
        server::main(|process| Ok(FE { config: FEConfig::load(&process.config.service_config)? }));
    if let Err(e) = result {
        eprintln!("fe: {e}");
        std::process::exit(1);
    }
}
