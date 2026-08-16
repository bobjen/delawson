//! FE, the Front End: authorization, and coordinating a client's request across the tiers.
//!
//! Nothing here is built yet, so the operations it will answer come back
//! `unimplemented` and anything else comes back `bad request` — a misdirected
//! call should not look like an unbuilt one.

use common::config::FEConfig;
use common::server::{self, Process, Service};
use common::wire::Op;

const SERVES: &[Op] = &[
    Op::MakeDirectory,
    Op::RemoveDirectory,
    Op::List,
    Op::Stat,
    Op::CreateObject,
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
}

fn main() {
    let result =
        server::main(|process| Ok(FE { config: FEConfig::load(&process.config.service_config)? }));
    if let Err(e) = result {
        eprintln!("fe: {e}");
        std::process::exit(1);
    }
}
