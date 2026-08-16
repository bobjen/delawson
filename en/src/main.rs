//! EN, the Extent Node: the bytes on the disks.
//!
//! Nothing here is built yet, and it serves no operation of its own: it is a
//! lifecycle, a registration, and a ping.

use common::config::ENConfig;
use common::server::{self, Process, Service};
use common::wire::Op;

const SERVES: &[Op] = &[];

struct EN {
    config: ENConfig,
}

impl Service for EN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }
}

fn main() {
    let result =
        server::main(|process| Ok(EN { config: ENConfig::load(&process.config.service_config)? }));
    if let Err(e) = result {
        eprintln!("en: {e}");
        std::process::exit(1);
    }
}
