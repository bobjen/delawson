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
use common::server::{self, Process, Reply, Service};
use common::wire::{Op, Request, Response, Status};
use common::{logi, logw};
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
        FORWARDED.contains(&op)
            || matches!(op, Op::Ping | Op::Status | Op::Shutdown | Op::Histogram)
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        if !FORWARDED.contains(&request.op) {
            reply.send(server::unbuilt(process, SERVES, &request));
            return;
        }
        let dn = match process.pick(ServiceKind::Dn) {
            Ok(dn) => dn.to_string(),
            Err(e) => return reply.send(Response::error(Status::Internal, e.to_string())),
        };
        logi!("forwarding, op {} dn {dn}", request.op);
        // The answer is whatever the DN says, since FE adds nothing to it yet.
        if let Err(e) = process.ask(&dn, &request, move |answer| reply.send(answer)) {
            // The reply went into the closure, so this needs its own.
            logw!("could not reach {dn}: {e}");
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![format!("stateless {}", self.config.stateless)]
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
