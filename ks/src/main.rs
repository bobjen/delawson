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
use common::crypto;
use common::logn;
use common::registry::{self, Peer};
use common::server::{self, Process, Service};
use common::wire::{Op, Req, Resp, Status, structure};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};

struct Ks {
    config: KsConfig,
    /// Whether the operator-and-hardware ceremony has been performed. Until it
    /// has, the master keys on disk cannot be decrypted, so no key request can
    /// be answered.
    unlocked: AtomicBool,
    /// In memory only, so a KS restart makes every other process re-register.
    peers: Mutex<BTreeMap<String, Peer>>,
    /// Cached SN daily keys.
    sn_keys: RwLock<BTreeMap<(common::ids::SnId, u64), [u8; 32]>>,
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

    fn answer(&self, process: &Process, request: &Req) -> Resp {
        match request.op {
            Op::Register => self.enroll(process, request),
            Op::Lookup => self.lookup(request),
            Op::Registry => registry::response(self.peers.lock().expect("peers").values().cloned()),

            Op::SnMasterKey => {
                if !self.unlocked.load(Ordering::SeqCst) {
                    return Resp::error(Status::Locked, "KS is locked");
                }
                self.sn_key(request)
            }

            Op::Unlock | Op::RotateMasterKey => {
                logn!("asked for a key op", "op {}", request.op);
                if !self.unlocked.load(Ordering::SeqCst) && request.op != Op::Unlock {
                    return Resp::error(Status::Locked, "KS is locked");
                }
                server::unbuilt(process, SERVES, request)
            }
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn inline(&self, op: Op) -> bool {
        match op {
            Op::Ping => self.config.inline_ping,
            Op::Status | Op::Shutdown | Op::Sites => true,
            // A registry lookup or key derivation takes a lock/time, so it goes to a worker.
            _ => false,
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        let sn_keys = self.sn_keys.read().expect("sn_keys").len();
        vec![
            format!("lazy_day_keys {}", self.config.lazy_day_keys),
            format!("require_unlock {}", self.config.require_unlock),
            format!("inline_ping {}", self.config.inline_ping),
            format!("unlocked {}", self.unlocked.load(Ordering::SeqCst)),
            format!("registered {}", self.peers.lock().expect("peers").len()),
            format!("master_keys 1"),
            format!("sn_daily_keys {sn_keys}"),
        ]
    }
}

impl Ks {
    fn new(config: KsConfig) -> Ks {
        let unlocked = AtomicBool::new(!config.require_unlock);
        Ks {
            config,
            unlocked,
            peers: Mutex::new(BTreeMap::new()),
            sn_keys: RwLock::new(BTreeMap::new()),
        }
    }

    fn sn_key(&self, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args, Op::SnMasterKey, "an SN master-key request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        if !rest.is_empty() {
            return Resp::error(
                Status::BadRequest,
                "an SN master-key request has trailing arguments",
            );
        }
        let sn = match payload
            .number_u32()
            .map(|value| value as u64)
            .and_then(common::ids::SnId::of_number)
        {
            Ok(sn) => sn,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let day = match payload.number_u64().and_then(|day| payload.done().map(|()| day)) {
            Ok(day) => day,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        self.sn_key_for(sn, day)
    }

    fn sn_key_for(&self, sn: common::ids::SnId, day: u64) -> Resp {
        {
            let keys = self.sn_keys.read().expect("sn_keys");
            if let Some(key) = keys.get(&(sn, day)) {
                return Resp::bytes(key.to_vec());
            }
        }

        let key = crypto::sn_daily_key(sn, day);
        self.sn_keys.write().expect("sn_keys").insert((sn, day), key);
        Resp::bytes(key.to_vec())
    }

    /// A registration announces a port; it does not invent an identity. The
    /// roster says who may exist, so a mismatch is a misconfiguration to report
    /// rather than a peer to record.
    fn enroll(&self, process: &Process, request: &Req) -> Resp {
        let peer = match Peer::from_args(&request.args) {
            Ok(peer) => peer,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let Some(entry) = process.cluster.service(&peer.identity) else {
            return Resp::error(
                Status::NotFound,
                format!("{} is not in the roster", peer.identity),
            );
        };
        if entry.kind != peer.kind {
            return Resp::error(
                Status::BadRequest,
                format!("{} is a {} in the roster, not a {}", peer.identity, entry.kind, peer.kind),
            );
        }
        logn!("registered a peer", "identity {},at {}", peer.identity, peer.address);
        self.peers.lock().expect("peers").insert(peer.identity.clone(), peer);
        Resp::ok()
    }

    fn lookup(&self, request: &Req) -> Resp {
        let (mut payload, rest) = match structure(&request.args, Op::Lookup, "a lookup request") {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if !rest.is_empty() {
            return Resp::error(Status::BadRequest, "a lookup request has trailing arguments");
        }
        let identity = match payload.text().and_then(|identity| payload.done().map(|()| identity)) {
            Ok(identity) => identity,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        match self.peers.lock().expect("peers").get(&identity) {
            Some(peer) => Resp::text(peer.address.to_string()),
            None => Resp::error(Status::NotFound, format!("{identity} has not registered")),
        }
    }
}

fn main() {
    let result = server::main(|process| {
        let config = KsConfig::load(&process.config.service_config)?;
        if !config.require_unlock {
            logn!("no unlock ceremony required because this is a test configuration");
        }
        Ok(Ks::new(config))
    });
    if let Err(e) = result {
        eprintln!("ks: {e}");
        std::process::exit(1);
    }
}
