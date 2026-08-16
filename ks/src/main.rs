//! KS, the Key Store: one random master key per active day, and derived
//! per-SN-per-day keys handed to ENs. Master keys never leave here.
//!
//! Only the lifecycle and the request surface exist so far. The key operations
//! answer `unimplemented`, which is deliberate: a well-formed request for
//! something unbuilt should be distinguishable from a malformed one.

use common::config::KsConfig;
use common::server::{self, Process, Service};
use common::wire::{Op, Request, Response, Status};
use std::sync::atomic::{AtomicBool, Ordering};

struct Ks {
    config: KsConfig,
    /// Whether the operator-and-hardware ceremony has been performed. Until it
    /// has, the master keys on disk cannot be decrypted, so no key request can
    /// be answered.
    unlocked: AtomicBool,
}

impl Service for Ks {
    fn handle(&self, _process: &Process, request: &Request) -> Response {
        match request.op {
            Op::Unlock | Op::SnMasterKey | Op::RotateMasterKey => {
                if !self.unlocked.load(Ordering::SeqCst) && request.op != Op::Unlock {
                    return Response::error(Status::Locked, "KS is locked");
                }
                Response::error(Status::Unimplemented, format!("{} is not built yet", request.op))
            }
            op => Response::error(Status::BadRequest, format!("KS does not serve {op}")),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("lazy_day_keys {}", self.config.lazy_day_keys),
            format!("require_unlock {}", self.config.require_unlock),
            format!("unlocked {}", self.unlocked.load(Ordering::SeqCst)),
            "master_keys 0".to_string(),
        ]
    }
}

fn main() {
    let result = server::main(|process| {
        let config = KsConfig::load(&process.config.service_config)?;
        if !config.require_unlock {
            process.log("no unlock ceremony required: this is a test configuration");
        }
        let unlocked = AtomicBool::new(!config.require_unlock);
        Ok(Ks { config, unlocked })
    });
    if let Err(e) = result {
        eprintln!("ks: {e}");
        std::process::exit(1);
    }
}
