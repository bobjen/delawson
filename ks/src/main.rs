//! KS, the Key Store: the process registry, and eventually one random master key
//! per active day with derived per-SN-per-day keys handed to ENs. Master keys
//! never leave here.
//!
//! KS listens on the one address a cluster cannot discover, so it is also where
//! every other process announces the port it was given and asks where its peers
//! are. The keys come later; the key operations answer `unimplemented`, which is
//! deliberate — a well-formed request for something unbuilt should be
//! distinguishable from a malformed one.

use common::config::KsConfig;
use common::logn;
use common::registry::{self, Peer};
use common::server::{self, Process, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

struct Ks {
    config: KsConfig,
    /// Whether the operator-and-hardware ceremony has been performed. Until it
    /// has, the master keys on disk cannot be decrypted, so no key request can
    /// be answered.
    unlocked: AtomicBool,
    /// In memory only, so a KS restart makes every other process re-register.
    peers: Mutex<BTreeMap<String, Peer>>,
}

const SERVES: &[Op] =
    &[Op::Register, Op::Lookup, Op::Registry, Op::Unlock, Op::SnMasterKey, Op::RotateMasterKey];

impl Service for Ks {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    fn handle(&self, process: &Process, request: &Request) -> Response {
        match request.op {
            Op::Register => self.enroll(process, request),
            Op::Lookup => self.lookup(request),
            Op::Registry => registry::response(self.peers.lock().expect("peers").values().cloned()),

            Op::Unlock | Op::SnMasterKey | Op::RotateMasterKey => {
                logn!("asked for {}", request.op);
                if !self.unlocked.load(Ordering::SeqCst) && request.op != Op::Unlock {
                    return Response::error(Status::Locked, "KS is locked");
                }
                server::unbuilt(process, SERVES, request)
            }
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn inline(&self, op: Op) -> bool {
        match op {
            Op::Ping => self.config.inline_ping,
            Op::Status | Op::Shutdown | Op::Histogram => true,
            // A registry lookup takes a lock, so it goes to a worker.
            _ => false,
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("lazy_day_keys {}", self.config.lazy_day_keys),
            format!("require_unlock {}", self.config.require_unlock),
            format!("inline_ping {}", self.config.inline_ping),
            format!("unlocked {}", self.unlocked.load(Ordering::SeqCst)),
            format!("registered {}", self.peers.lock().expect("peers").len()),
            "master_keys 0".to_string(),
        ]
    }
}

impl Ks {
    /// A registration announces a port; it does not invent an identity. The
    /// roster says who may exist, so a mismatch is a misconfiguration to report
    /// rather than a peer to record.
    fn enroll(&self, process: &Process, request: &Request) -> Response {
        let peer = match Peer::from_args(&request.args) {
            Ok(peer) => peer,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        let Some(entry) = process.cluster.service(&peer.identity) else {
            return Response::error(
                Status::NotFound,
                format!("{} is not in the roster", peer.identity),
            );
        };
        if entry.kind != peer.kind {
            return Response::error(
                Status::BadRequest,
                format!("{} is a {} in the roster, not a {}", peer.identity, entry.kind, peer.kind),
            );
        }
        logn!("registered {} at {}", peer.identity, peer.address);
        self.peers.lock().expect("peers").insert(peer.identity.clone(), peer);
        Response::ok()
    }

    fn lookup(&self, request: &Request) -> Response {
        let Some(Arg::Text(identity)) = request.args.first() else {
            return Response::error(Status::BadRequest, "lookup takes one identity");
        };
        match self.peers.lock().expect("peers").get(identity) {
            Some(peer) => Response::text(peer.address.to_string()),
            None => Response::error(Status::NotFound, format!("{identity} has not registered")),
        }
    }
}

fn main() {
    let result = server::main(|process| {
        let config = KsConfig::load(&process.config.service_config)?;
        if !config.require_unlock {
            logn!("no unlock ceremony required: this is a test configuration");
        }
        let unlocked = AtomicBool::new(!config.require_unlock);
        Ok(Ks { config, unlocked, peers: Mutex::new(BTreeMap::new()) })
    });
    if let Err(e) = result {
        eprintln!("ks: {e}");
        std::process::exit(1);
    }
}
