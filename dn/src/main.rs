//! DN, the Directory Node: the directory hierarchy, object metadata, and the streams an object is made of.
//!
//! Nothing here is built yet, so the operations it will answer come back
//! `unimplemented` and anything else comes back `bad request` — a misdirected
//! call should not look like an unbuilt one.

use common::config::DNConfig;
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
    Op::Count,
    Op::TimeRange,
];

struct DN {
    config: DNConfig,
}

impl Service for DN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }
}

fn main() {
    let result =
        server::main(|process| Ok(DN { config: DNConfig::load(&process.config.service_config)? }));
    if let Err(e) = result {
        eprintln!("dn: {e}");
        std::process::exit(1);
    }
}
