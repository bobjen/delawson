//! KS: process registry and encrypted, journaled master-key epochs.
mod journal;

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

#[derive(Clone)]
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

#[derive(Clone, Default)]
struct KeyHistory {
    epochs: Vec<MasterEpoch>,
    applied: u64,
}

struct Ks {
    config: KsConfig,
    snapshot_stopping: AtomicBool,
    snapshot_task: Mutex<()>,
    snapshot_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    unlock_verifier: Option<[u8; 32]>,
    /// Whether the operator-and-hardware ceremony has been performed. Until it
    /// has, the master keys on disk cannot be decrypted, so no key request can
    /// be answered.
    unlocked: AtomicBool,
    /// In memory only, so a KS restart makes every other process re-register.
    peers: Mutex<BTreeMap<String, Peer>>,
    /// Serializes proposals and disk I/O, without blocking reads of committed keys.
    journal: Mutex<journal::Journal>,
    /// Master keys sorted by the second at which each becomes effective.
    master_keys: RwLock<KeyHistory>,
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
    fn start(self: &std::sync::Arc<Self>) -> Result<()> {
        let weak = std::sync::Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name("ks-snapshots".into())
            .spawn(move || {
                loop {
                    std::thread::park_timeout(std::time::Duration::from_secs(1));
                    let Some(ks) = weak.upgrade() else {
                        break;
                    };
                    if ks.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    if !ks.unlocked.load(Ordering::SeqCst) {
                        continue;
                    }
                    ks.maybe_snapshot();
                }
            })
            .context("starting KS snapshot timer")?;
        *self.snapshot_worker.lock().expect("snapshot_worker") = Some(handle);
        Ok(())
    }

    fn stop(&self, _process: &Process) -> Result<()> {
        self.stop_snapshot_worker()
    }

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
        let (key_count, applied) = {
            let history = self.master_keys.read().expect("master_keys");
            (history.epochs.len(), history.applied)
        };
        vec![
            format!("lazy_day_keys {}", self.config.lazy_day_keys),
            format!("require_unlock {}", self.config.require_unlock),
            format!("inline_ping {}", self.config.inline_ping),
            format!("unlocked {}", self.unlocked.load(Ordering::SeqCst)),
            format!("registered {}", self.peers.lock().expect("peers").len()),
            format!("master_keys {key_count}"),
            format!("applied_commit {applied}"),
        ]
    }
}

impl Ks {
    fn stop_snapshot_worker(&self) -> Result<()> {
        self.snapshot_stopping.store(true, Ordering::SeqCst);
        if let Some(handle) = self.snapshot_worker.lock().expect("snapshot_worker").take() {
            handle.thread().unpark();
            handle.join().map_err(|_| common::Error::new("KS snapshot worker panicked"))?;
        }
        Ok(())
    }

    fn new(config: KsConfig, path: &std::path::Path) -> Result<Ks> {
        let unlock_verifier =
            config.unlock_verifier.as_deref().map(common::shamir::unhex).transpose()?;
        if config.require_unlock && unlock_verifier.is_none() {
            return Err(common::Error::new("require_unlock needs unlock_verifier"));
        }
        let test_mode = !config.require_unlock;
        let journal = journal::Journal::open(path, config.snapshot_retain)?;
        let ks = Ks {
            config,
            snapshot_stopping: AtomicBool::new(false),
            snapshot_task: Mutex::new(()),
            snapshot_worker: Mutex::new(None),
            unlock_verifier,
            unlocked: AtomicBool::new(false),
            peers: Mutex::new(BTreeMap::new()),
            journal: Mutex::new(journal),
            master_keys: RwLock::new(KeyHistory::default()),
        };
        if test_mode {
            // Explicit insecure development mode, never a production unlock secret.
            ks.open_keys(crypto::MASTER_KEY)?;
        }
        Ok(ks)
    }

    fn open_keys(&self, key: [u8; 32]) -> Result<()> {
        let mut journal = self.journal.lock().expect("journal");
        if self.unlocked.load(Ordering::SeqCst) {
            return Ok(());
        }
        let mut epochs = journal.replay(key)?;
        if epochs.is_empty() {
            let epoch = MasterEpoch { key: random_master_key()?, since: TimeSec::now() };
            journal.append(&epoch)?;
            epochs.push(epoch);
        }
        *self.master_keys.write().expect("master_keys") =
            KeyHistory { epochs, applied: journal.commit() };
        self.unlocked.store(true, Ordering::SeqCst);
        drop(journal);
        self.maybe_snapshot();
        Ok(())
    }

    fn choose_key(&self) -> Result<MasterEpoch> {
        let key = random_master_key()?;
        let delay = self.config.master_key_delay_secs.to_duration()?.as_secs();
        let since = TimeSec::now()
            .number()
            .checked_add(delay)
            .ok_or_else(|| common::Error::new("key activation time overflow"))?;
        Ok(MasterEpoch { key, since: TimeSec::new(since) })
    }

    fn apply_key(&self, epoch: MasterEpoch, commit: u64) {
        let mut history = self.master_keys.write().expect("master_keys");
        history.epochs.push(epoch);
        history.applied = commit;
    }

    fn maybe_snapshot(&self) {
        let Ok(_task) = self.snapshot_task.try_lock() else {
            return;
        };
        let Ok(mut journal) = self.journal.try_lock() else {
            return;
        };
        let now = TimeSec::now().number();
        if !journal.snapshot_due(
            self.config.snapshot_min_log_bytes,
            self.config.snapshot_interval_secs,
            now,
        ) {
            return;
        }
        // Freeze execution, not logging. This read guard also avoids copying the
        // whole state solely to write a snapshot.
        let history = self.master_keys.read().expect("master_keys");
        let pending = match journal.prepare_snapshot(&history.epochs, history.applied, now) {
            Ok(pending) => pending,
            Err(e) => {
                logn!("KS snapshot preparation failed", "{e}");
                return;
            }
        };
        drop(journal);
        let result = pending.write(&history.epochs);
        // Never reacquire the logging lock while holding the execution lock: an
        // applier may hold the former while waiting for the latter's write lock.
        drop(history);
        let result = result
            .and_then(|size| self.journal.lock().expect("journal").finish_snapshot(&pending, size));
        if let Err(e) = result {
            logn!("KS snapshot failed; durable journal retained", "{e}");
        }
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
        self.open_keys(key)
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
        self.ensure_future_key()?;
        let epochs = self.master_keys.read().expect("master_keys");
        let epochs = &epochs.epochs;
        let count = epochs.partition_point(|epoch| epoch.since <= at);
        let current = count
            .checked_sub(1)
            .and_then(|i| epochs.get(i))
            .ok_or_else(|| common::Error::new("no master key is active at the requested second"))?;
        let current_key = crypto::sn_key(&current.key, sn, current.since);
        let next = epochs
            .get(count)
            .map(|epoch| (epoch.since, crypto::sn_key(&epoch.key, sn, epoch.since)));
        let result = SnKey { current: current_key, next };
        Ok(result)
    }

    fn ensure_future_key(&self) -> Result<()> {
        let horizon = TimeSec::now().plus_secs(self.config.master_key_delay_secs)?;
        if self
            .master_keys
            .read()
            .expect("master_keys")
            .epochs
            .last()
            .is_some_and(|last| last.since.day() >= horizon.day())
        {
            return Ok(());
        }
        // Reads may continue using committed keys while another proposal is syncing.
        let mut journal = match self.journal.try_lock() {
            Ok(journal) => journal,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(common::Error::new("KS journal lock poisoned"));
            }
        };
        let now = TimeSec::now();
        let horizon = now.plus_secs(self.config.master_key_delay_secs)?;
        let last = self.master_keys.read().expect("master_keys").epochs.last().map(|e| e.since);
        if last.is_some_and(|last| last.day() >= horizon.day()) {
            return Ok(());
        }
        let mut epoch = self.choose_key()?;
        // When anticipating tomorrow, activate no earlier than its midnight.
        epoch.since = epoch.since.max(horizon.day_start());
        let commit = journal.append(&epoch)?;
        self.apply_key(epoch, commit);
        drop(journal);
        self.maybe_snapshot();
        Ok(())
    }

    fn rotate_master_key(&self, request: &Req) -> Result<()> {
        let (payload, rest) =
            structure(&request.args, Op::RotateMasterKey, "a rotate-master-key request")?;
        payload.done()?;
        if !rest.is_empty() {
            return Err(common::Error::new("a rotate-master-key request has trailing arguments"));
        }
        let mut journal = self.journal.lock().expect("journal");
        let epoch = self.choose_key()?;
        let commit = journal.append(&epoch)?;
        self.apply_key(epoch, commit);
        drop(journal);
        self.maybe_snapshot();
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
        Ks::new(config, &process.config.data_dir.join("master-keys"))
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
            snapshot_retain: 3,
            snapshot_interval_secs: 86400,
            snapshot_min_log_bytes: 1024 * 1024,
            unlock_verifier: Some(common::shamir::hex(&common::shamir::verifier(&key))),
            knobs: common::config::Knobs { workers: 1 },
            lazy_day_keys: true,
            master_key_delay_secs: common::time::TimeIntSec64::new(3600),
            require_unlock: true,
            inline_ping: true,
        };
        let dir = std::env::temp_dir().join(format!("delawson-ks-unlock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("keys");
        let ks = Ks::new(config, &path).unwrap();
        assert!(!ks.unlocked.load(Ordering::SeqCst));
        for bad in [request(&[0; 32]), request(&key[..31]), Req::new(Op::Unlock)] {
            assert!(ks.unlock(&bad).is_err());
            assert!(!ks.unlocked.load(Ordering::SeqCst));
        }
        assert!(ks.unlock(&request(&key)).is_ok());
        assert!(ks.unlocked.load(Ordering::SeqCst));
        assert!(ks.unlock(&request(&key)).is_ok());
        drop(ks);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use crate::journal::tests::Temp;

    fn config(key: &[u8; 32]) -> KsConfig {
        KsConfig {
            snapshot_retain: 3,
            snapshot_interval_secs: 86400,
            snapshot_min_log_bytes: 1024 * 1024,
            unlock_verifier: Some(common::shamir::hex(&common::shamir::verifier(key))),
            knobs: common::config::Knobs { workers: 1 },
            lazy_day_keys: true,
            master_key_delay_secs: common::time::TimeIntSec64::new(1),
            require_unlock: true,
            inline_ping: true,
        }
    }

    #[test]
    fn first_key_is_immediate_later_keys_wait_and_restart_preserves_both() {
        let dir = Temp::new();
        let key = [42; 32];
        let ks = Ks::new(config(&key), &dir.path()).unwrap();
        assert!(ks.master_keys.read().unwrap().epochs.is_empty());
        ks.open_keys(key).unwrap();
        let initial = ks.master_keys.read().unwrap().epochs[0].since;
        assert!(initial <= TimeSec::now());
        let expected = ks.master_keys.read().unwrap().epochs[0].key;
        let before = TimeSec::now().plus_secs(ks.config.master_key_delay_secs).unwrap();
        let replacement = ks.choose_key().unwrap();
        assert!(replacement.since >= before);
        let future = replacement.since;
        let replacement_key = replacement.key;
        let commit = ks.journal.lock().unwrap().append(&replacement).unwrap();
        ks.apply_key(replacement, commit);
        // The proposal lock is deliberately held: ordinary reads need only a shared state lock.
        let writer = ks.journal.lock().unwrap();
        let derived = ks.sn_key_for(common::ids::SnId::new(0), initial).unwrap();
        assert_eq!(derived.current, crypto::sn_key(&expected, common::ids::SnId::new(0), initial));
        drop(writer);
        drop(ks);
        let restored = Ks::new(config(&key), &dir.path()).unwrap();
        assert!(!restored.unlocked.load(Ordering::SeqCst));
        assert!(restored.master_keys.read().unwrap().epochs.is_empty());
        restored.open_keys(key).unwrap();
        let history = restored.master_keys.read().unwrap();
        let epochs = &history.epochs;
        assert_eq!(history.applied, 2);
        assert_eq!(epochs.len(), 2);
        assert_eq!((epochs[0].key, epochs[0].since), (expected, initial));
        assert_eq!((epochs[1].key, epochs[1].since), (replacement_key, future));
    }

    #[test]
    fn failed_persistence_does_not_publish_a_key() {
        let dir = Temp::new();
        let key = [42; 32];
        let ks = Ks::new(config(&key), &dir.path()).unwrap();
        ks.open_keys(key).unwrap();
        let mut journal = ks.journal.lock().unwrap();
        // Simulate a disk write failure without relying on permissions or root identity.
        journal.make_read_only(&dir.path());
        drop(journal);
        let request = Req::new(Op::RotateMasterKey).structure(common::wire::StructWriter::new());
        assert!(ks.rotate_master_key(&request).is_err());
        assert_eq!(ks.master_keys.read().unwrap().epochs.len(), 1);
        assert!(ks.rotate_master_key(&request).is_err());
    }

    #[test]
    fn timer_snapshots_dirty_idle_state() {
        let dir = Temp::new();
        let key = [42; 32];
        let mut config = config(&key);
        config.snapshot_interval_secs = 1;
        let ks = std::sync::Arc::new(Ks::new(config, &dir.path()).unwrap());
        ks.open_keys(key).unwrap();
        let request = Req::new(Op::RotateMasterKey).structure(common::wire::StructWriter::new());
        ks.rotate_master_key(&request).unwrap();
        ks.start().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let captured = loop {
            let bytes = match std::fs::read(dir.path().join("snapshot-00000000000000000002.bin")) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if std::time::Instant::now() >= deadline {
                        break 0;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    continue;
                }
                Err(e) => panic!("reading test snapshot: {e}"),
            };
            let mut number = [0; 8];
            number.copy_from_slice(&bytes[40..48]);
            let captured = u64::from_le_bytes(number);
            if captured == 2 || std::time::Instant::now() >= deadline {
                break captured;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        ks.stop_snapshot_worker().unwrap();
        assert_eq!(captured, 2);
    }
}
