//! KS: process registry and in-memory master-key epochs.
//! Unlock verifies operator-supplied material. Encrypted persistence is not yet built.

use common::config::KsConfig;
use common::crypto;
use common::logn;
use common::registry::{self, Peer};
use common::server::{self, Process, Service};
use common::time::TimeSec;
use common::wire::{Op, Req, Resp, SnKey, Status, structure};
use common::{Context, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};

struct MasterEpoch {
    since: TimeSec,
    key: [u8; 32],
}

fn random_master_key() -> Result<[u8; 32]> {
    let mut key = [0u8; 32];
    File::open("/dev/urandom")
        .context("opening the operating system random source")?
        .read_exact(&mut key)
        .context("reading a master key from the operating system random source")?;
    Ok(key)
}

struct Ks {
    config: KsConfig,
    unlock_verifier: Option<[u8; 32]>,
    /// Whether the operator-and-hardware ceremony has been performed. Until it
    /// has, the master keys on disk cannot be decrypted, so no key request can
    /// be answered.
    unlocked: AtomicBool,
    /// In memory only, so a KS restart makes every other process re-register.
    peers: Mutex<BTreeMap<String, Peer>>,
    /// Cached SN keys, addressed by the exact time requested. An emergency
    /// master-key epoch can therefore change the answer within one day.
    sn_keys: RwLock<BTreeMap<(common::ids::SnId, TimeSec), SnKey>>,
    /// Master keys sorted by the second at which each becomes effective.
    master_keys: RwLock<Vec<MasterEpoch>>,
}

const SERVES: &[Op] = &[
    Op::Register,
    Op::Lookup,
    Op::Registry,
    Op::AssignPeers,
    Op::Unlock,
    Op::SnMasterKey,
    Op::RotateMasterKey,
];

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
            Op::AssignPeers => self.assign_peers(process, request),
            Op::Registry => registry::response(self.peers.lock().expect("peers").values().cloned()),

            Op::SnMasterKey => {
                if !self.unlocked.load(Ordering::SeqCst) {
                    return Resp::error(Status::Locked, "KS is locked");
                }
                self.sn_key(request)
            }

            Op::RotateMasterKey => {
                if !self.unlocked.load(Ordering::SeqCst) {
                    return Resp::error(Status::Locked, "KS is locked");
                }
                match self.rotate_master_key(request) {
                    Ok(()) => Resp::ok(),
                    Err(e) => Resp::error(Status::BadRequest, e.to_string()),
                }
            }

            Op::Unlock => match self.unlock(request) {
                Ok(()) => Resp::ok(),
                Err(e) => Resp::error(Status::BadRequest, e.to_string()),
            },
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
            format!("master_keys {}", self.master_keys.read().expect("master_keys").len()),
            format!("sn_keys {sn_keys}"),
        ]
    }
}

impl Ks {
    fn new(config: KsConfig) -> Result<Ks> {
        let unlock_verifier =
            config.unlock_verifier.as_deref().map(common::shamir::unhex).transpose()?;
        if config.require_unlock && unlock_verifier.is_none() {
            return Err(common::Error::new("require_unlock needs unlock_verifier"));
        }
        let unlocked = AtomicBool::new(!config.require_unlock);
        Ok(Ks {
            config,
            unlock_verifier,
            unlocked,
            peers: Mutex::new(BTreeMap::new()),
            sn_keys: RwLock::new(BTreeMap::new()),
            master_keys: RwLock::new(vec![MasterEpoch {
                since: TimeSec::EPOCH,
                key: random_master_key().expect("OS randomness for initial master key"),
            }]),
        })
    }

    fn unlock(&self, request: &Req) -> Result<()> {
        let (mut payload, rest) = structure(&request.args, Op::Unlock, "an unlock request")?;
        let bytes = payload.bytes()?;
        payload.done()?;
        if !rest.is_empty() {
            return Err(common::Error::new("trailing unlock arguments"));
        }
        let key: [u8; 32] =
            bytes.try_into().map_err(|_| common::Error::new("unlock key must be 32 bytes"))?;
        let expected = self
            .unlock_verifier
            .as_ref()
            .ok_or_else(|| common::Error::new("no unlock verifier configured"))?;
        if !common::shamir::matches(&key, expected) {
            return Err(common::Error::new("incorrect unlock key"));
        }
        self.unlocked.store(true, Ordering::SeqCst);
        Ok(())
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
        let sn = match payload.sn_id() {
            Ok(sn) => sn,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let at = match payload.time_sec().and_then(|at| payload.done().map(|()| at)) {
            Ok(at) => at,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        match self.sn_key_for(sn, at) {
            Ok(key) => Resp { id: 0, status: Status::Ok, args: vec![key.write()] },
            Err(e) => Resp::error(Status::Internal, e.to_string()),
        }
    }

    fn sn_key_for(&self, sn: common::ids::SnId, at: TimeSec) -> Result<SnKey> {
        self.ensure_day(at.day_start())?;
        let horizon = at.plus_secs(self.config.master_key_delay_secs)?;
        self.ensure_day(horizon.day_start())?;
        {
            let keys = self.sn_keys.read().expect("sn_keys");
            if let Some(key) = keys.get(&(sn, at)) {
                return Ok(key.clone());
            }
        }

        let epochs = self.master_keys.read().expect("master_keys");
        let count = epochs.partition_point(|epoch| epoch.since <= at);
        let current = &epochs[count - 1];
        let current_key = crypto::sn_key(&current.key, sn, current.since);
        let next = epochs
            .get(count)
            .map(|epoch| (epoch.since, crypto::sn_key(&epoch.key, sn, epoch.since)));
        let result = SnKey { current: current_key, next };
        drop(epochs);
        self.sn_keys.write().expect("sn_keys").insert((sn, at), result.clone());
        Ok(result)
    }

    fn ensure_day(&self, day: TimeSec) -> Result<()> {
        let mut epochs = self.master_keys.write().expect("master_keys");
        if epochs.iter().any(|epoch| epoch.since == day) {
            return Ok(());
        }
        let key = random_master_key()?;
        let at = epochs.partition_point(|epoch| epoch.since < day);
        epochs.insert(at, MasterEpoch { since: day, key });
        Ok(())
    }

    fn rotate_master_key(&self, request: &Req) -> Result<()> {
        let (payload, rest) =
            structure(&request.args, Op::RotateMasterKey, "a rotate-master-key request")?;
        payload.done()?;
        if !rest.is_empty() {
            return Err(common::Error::new("a rotate-master-key request has trailing arguments"));
        }
        let since = TimeSec::now().plus_secs(self.config.master_key_delay_secs)?;
        let mut epochs = self.master_keys.write().expect("master_keys");
        let key = random_master_key()?;
        let at = epochs.partition_point(|epoch| epoch.since < since);
        if epochs.get(at).is_some_and(|epoch| epoch.since == since) {
            epochs[at] = MasterEpoch { since, key };
        } else {
            epochs.insert(at, MasterEpoch { since, key });
        }
        self.sn_keys.write().expect("sn_keys").clear();
        Ok(())
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

    fn assign_peers(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args, Op::AssignPeers, "an assign-peers request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        let source = match payload.text() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let Some(source_peer) = self.peers.lock().expect("peers").get(&source).cloned() else {
            return Resp::error(Status::NotFound, format!("{source} has not registered"));
        };
        let count = match payload.number_u64() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let mut forwarded = common::wire::StructWriter::new();
        forwarded.text(&source);
        forwarded.number_u64(count);
        for n in 0..count {
            let identity = match payload.text() {
                Ok(value) => value,
                Err(e) => {
                    return Resp::error(
                        Status::BadRequest,
                        format!("got through {n} destinations: {e}"),
                    );
                }
            };
            let address = match payload.text() {
                Ok(value) => value,
                Err(e) => {
                    return Resp::error(
                        Status::BadRequest,
                        format!("got through {n} destinations: {e}"),
                    );
                }
            };
            if self.peers.lock().expect("peers").get(&identity).is_none() {
                return Resp::error(Status::NotFound, format!("{identity} has not registered"));
            }
            if address.parse::<std::net::SocketAddr>().is_err() {
                return Resp::error(Status::BadRequest, format!("invalid address for {identity}"));
            }
            forwarded.text(&identity);
            forwarded.text(&address);
        }
        if !rest.is_empty() || payload.done().is_err() {
            return Resp::error(
                Status::BadRequest,
                format!("got through {count} destinations, then found trailing data"),
            );
        }
        let request = Req::new(Op::AssignPeers).structure(forwarded);
        match server::ask(
            source_peer.address,
            &request,
            process.timeout(),
            &process.cluster.messaging,
        ) {
            Ok(response) => response,
            Err(e) => Resp::error(Status::Internal, format!("could not relay to {source}: {e}")),
        }
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
        Ks::new(config)
    });
    if let Err(e) = result {
        eprintln!("ks: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod unlock_tests {
    use super::*;
    fn request(bytes: &[u8]) -> Req {
        let mut payload = common::wire::StructWriter::new();
        payload.bytes(bytes);
        Req::new(Op::Unlock).structure(payload)
    }
    #[test]
    fn only_correct_well_formed_key_opens_gate() {
        let key = [42; 32];
        let config = KsConfig {
            unlock_verifier: Some(common::shamir::hex(&common::shamir::verifier(&key))),
            knobs: common::config::Knobs { workers: 1 },
            lazy_day_keys: true,
            master_key_delay_secs: common::time::TimeIntSec64::new(3600),
            require_unlock: true,
            inline_ping: true,
        };
        let ks = Ks::new(config).unwrap();
        assert!(!ks.unlocked.load(Ordering::SeqCst));
        for bad in [request(&[0; 32]), request(&key[..31]), Req::new(Op::Unlock)] {
            assert!(ks.unlock(&bad).is_err());
            assert!(!ks.unlocked.load(Ordering::SeqCst));
        }
        assert!(ks.unlock(&request(&key)).is_ok());
        assert!(ks.unlocked.load(Ordering::SeqCst));
        assert!(ks.unlock(&request(&key)).is_ok());
    }
}
