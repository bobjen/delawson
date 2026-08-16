//! SN, the Stream Node: the extents of a stream and the instances of an extent.
//!
//! Nothing here is built yet, and it serves no operation of its own: it is a
//! lifecycle, a registration, and a ping.

use common::config::SNConfig;
use common::server::{self, Process, Service};
use common::wire::Op;

const SERVES: &[Op] = &[];

struct SN {
    config: SNConfig,
}

impl Service for SN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }
}

fn main() {
    let result =
        server::main(|process| Ok(SN { config: SNConfig::load(&process.config.service_config)? }));
    if let Err(e) = result {
        eprintln!("sn: {e}");
        std::process::exit(1);
    }
}
