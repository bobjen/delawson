//! KS: process registry and encrypted, journaled master-key epochs.
mod journal;
mod lease;
mod raft;
mod runtime;

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
    unlock_verifier: Option<[u8; 32]>,
    /// Whether the operator-and-hardware ceremony has been performed. Until it
    /// has, the master keys on disk cannot be decrypted, so no key request can
    /// be answered.
    unlocked: AtomicBool,
    /// In memory only, so a KS restart makes every other process re-register.
    peers: Mutex<BTreeMap<String, Peer>>,
    /// Serializes proposals and disk I/O, without blocking reads of committed keys.
    journal: Mutex<raft::Node>,
    runtime: runtime::Settings,
    unlock_key: Mutex<Option<[u8; 32]>>,
    unlock_requests: Mutex<std::collections::BTreeSet<String>>,
    proposing: Mutex<()>,
    raft_workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// Global execution lock: application takes write access; key serving takes read access.
    /// Master keys are sorted by the second at which each becomes effective.
    master_keys: RwLock<KeyHistory>,
}

const SERVES: &[Op] = &[
    Op::Raft,
    Op::RaftMembers,
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
        self.start_raft()
    }

    fn stop(&self, _process: &Process) -> Result<()> {
        self.stop_snapshot_worker()
    }

    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn defers(&self, op: Op) -> bool {
        matches!(op, Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers | Op::Raft)
    }
    fn handle(
        self: &std::sync::Arc<Self>,
        process: &std::sync::Arc<Process>,
        request: Req,
        reply: server::Reply,
    ) {
        if !runtime::waits_for_quorum(&request) {
            reply.send(self.answer(process, &request));
            return;
        }
        let (sender, receiver) = std::sync::mpsc::channel::<(Req, server::Reply)>();
        let ks = std::sync::Arc::clone(self);
        let process = std::sync::Arc::clone(process);
        let spawned = std::thread::Builder::new().name("ks-request".into()).spawn(move || {
            if let Ok((request, reply)) = receiver.recv() {
                let response = if ks.snapshot_stopping.load(Ordering::SeqCst) {
                    Resp::error(Status::Internal, "KS is stopping")
                } else {
                    ks.answer(&process, &request)
                };
                reply.send(response);
            }
        });
        match spawned {
            Ok(_) => {
                if let Err(e) = sender.send((request, reply)) {
                    e.0.1.send(Resp::error(Status::Internal, "KS request worker stopped"));
                }
            }
            Err(e) => reply
                .send(Resp::error(Status::Internal, format!("starting KS request worker: {e}"))),
        }
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    fn answer(&self, process: &Process, request: &Req) -> Resp {
        if matches!(request.op, Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers) {
            if let Some(answer) = self.forward(request) {
                return answer;
            }
        }
        match request.op {
            Op::Raft => self.raft_rpc(request),
            Op::RaftMembers => self.change_members(request),
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
            self.raft_status(),
        ]
    }
}

impl Ks {
    fn stop_snapshot_worker(&self) -> Result<()> {
        self.snapshot_stopping.store(true, Ordering::SeqCst);
        loop {
            let handles = std::mem::take(
                &mut *self
                    .raft_workers
                    .lock()
                    .map_err(|_| common::Error::new("Raft worker lock poisoned"))?,
            );
            if handles.is_empty() {
                break;
            }
            for handle in &handles {
                handle.thread().unpark();
            }
            for handle in handles {
                handle.join().map_err(|_| common::Error::new("Raft worker panicked"))?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn new(config: KsConfig, path: &std::path::Path) -> Result<Ks> {
        let settings = runtime::Settings::testing(&config);
        Self::with_runtime(config, path, settings)
    }

    fn with_runtime(
        config: KsConfig,
        path: &std::path::Path,
        runtime: runtime::Settings,
    ) -> Result<Ks> {
        runtime.validate(&config.raft)?;
        let unlock_verifier =
            config.unlock_verifier.as_deref().map(common::shamir::unhex).transpose()?;
        if config.require_unlock && unlock_verifier.is_none() {
            return Err(common::Error::new("require_unlock needs unlock_verifier"));
        }
        let initial = raft::State::initial(runtime.ring.clone(), runtime.members.clone())?;
        let journal = journal::Journal::open(path, initial, config.snapshot_retain, &runtime.id)?;
        let mut node = raft::Node::new(runtime.id.clone(), journal);
        node.lease = lease::Lease::configured(&config.raft);
        if node.state().membership().members().len() == 1
            && node.state().membership().members().contains_key(&runtime.id)
        {
            node.campaign()?;
        }
        let test_mode = !config.require_unlock;
        let ks = Ks {
            config,
            runtime,
            snapshot_stopping: AtomicBool::new(false),
            snapshot_task: Mutex::new(()),
            raft_workers: Mutex::new(vec![]),
            proposing: Mutex::new(()),
            unlock_key: Mutex::new(None),
            unlock_requests: Mutex::new(std::collections::BTreeSet::new()),
            unlock_verifier,
            unlocked: AtomicBool::new(false),
            peers: Mutex::new(BTreeMap::new()),
            journal: Mutex::new(node),
            master_keys: RwLock::new(KeyHistory::default()),
        };
        if test_mode {
            ks.open_keys(crypto::MASTER_KEY)?;
        }
        Ok(ks)
    }

    fn open_keys(&self, key: [u8; 32]) -> Result<()> {
        let mut node = self.journal.lock().map_err(|_| common::Error::new("Raft lock poisoned"))?;
        let history = runtime::decode_history(node.state(), &key)?;
        *self.unlock_key.lock().map_err(|_| common::Error::new("unlock lock poisoned"))? =
            Some(key);
        *self.master_keys.write().map_err(|_| common::Error::new("key history lock poisoned"))? =
            history;
        self.unlocked.store(true, Ordering::SeqCst);
        // Only a leader establishes the first key, and never over an uncommitted proposal.
        self.initial_key(&mut node)?;
        self.refresh_keys(&node)?;
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

    fn maybe_snapshot(&self) {
        let Ok(_task) = self.snapshot_task.try_lock() else {
            return;
        };
        let pending = {
            let Ok(mut node) = self.journal.try_lock() else {
                return;
            };
            let now = TimeSec::now().number();
            if !node.journal.snapshot_due(
                self.config.snapshot_min_log_bytes,
                self.config.snapshot_interval_secs,
                now,
            ) {
                return;
            }
            match node.journal.prepare_snapshot(now) {
                Ok(pending) => pending,
                Err(e) => {
                    logn!("KS snapshot preparation failed", "{e}");
                    return;
                }
            }
        };
        if let Some(pending) = pending {
            let result = pending.write().and_then(|()| {
                self.journal
                    .lock()
                    .map_err(|_| common::Error::new("Raft lock poisoned"))?
                    .journal
                    .finish_snapshot(&pending)
            });
            if let Err(e) = result {
                logn!("KS snapshot failed; journal retained", "{e}");
            }
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
        if !self.unlocked.load(Ordering::SeqCst) {
            return Resp::error(Status::Locked, "KS is locked");
        }
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
        self.confirm_leader()?;
        let (index, term) = {
            let mut node =
                self.journal.lock().map_err(|_| common::Error::new("Raft lock poisoned"))?;
            self.initial_key(&mut node)?;
            (node.state().last(), node.state().term)
        };
        self.wait_committed(index, term)?;
        self.ensure_future_key()?;
        // Recheck after any proposal or lock wait. Keep consensus and execution stable
        // while capturing the result, then check time again to cover pauses during capture.
        self.confirm_leader()?;
        let node = self.journal.lock().map_err(|_| common::Error::new("Raft lock poisoned"))?;
        if node.journal.failed || !node.can_read(lease::now()?) {
            return Err(common::Error::new("KS read lease expired"));
        }
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
        if !node.can_read(lease::now()?) {
            return Err(common::Error::new("KS read lease expired during key derivation"));
        }
        Ok(result)
    }

    fn ensure_future_key(&self) -> Result<()> {
        let horizon = TimeSec::now().plus_secs(self.config.master_key_delay_secs)?;
        if self
            .master_keys
            .read()
            .map_err(|_| common::Error::new("key history lock poisoned"))?
            .epochs
            .last()
            .is_some_and(|last| last.since.day() >= horizon.day())
        {
            return Ok(());
        }
        let Ok(_proposal) = self.proposing.try_lock() else {
            return Ok(());
        };
        let mut epoch = self.choose_key()?;
        epoch.since = epoch.since.max(horizon.day_start());
        self.propose_key(epoch).map(|_| ())
    }

    fn rotate_master_key(&self, request: &Req) -> Result<()> {
        let (payload, rest) =
            structure(&request.args, Op::RotateMasterKey, "a rotate-master-key request")?;
        payload.done()?;
        if !rest.is_empty() {
            return Err(common::Error::new("trailing rotate arguments"));
        }
        let _proposal =
            self.proposing.lock().map_err(|_| common::Error::new("proposal lock poisoned"))?;
        let epoch = self.choose_key()?;
        self.propose_key(epoch).map(|_| ())
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
        let settings = runtime::Settings::for_process(process, &config)?;
        Ks::with_runtime(config, &process.config.data_dir.join("master-keys"), settings)
    });
    if let Err(e) = result {
        eprintln!("ks: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    fn config() -> KsConfig {
        KsConfig {
            raft: common::config::RaftConfig::default(),
            snapshot_retain: 3,
            snapshot_interval_secs: 86400,
            snapshot_min_log_bytes: 1048576,
            unlock_verifier: Some(common::shamir::hex(&common::shamir::verifier(&[42; 32]))),
            knobs: common::config::Knobs { workers: 1 },
            lazy_day_keys: true,
            master_key_delay_secs: common::time::TimeIntSec64::new(1),
            require_unlock: true,
            inline_ping: true,
        }
    }
    fn unlock(key: &[u8]) -> Req {
        let mut payload = common::wire::StructWriter::new();
        payload.bytes(key);
        Req::new(Op::Unlock).structure(payload)
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn unlock_validation_and_initial_key_exception_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Ks::new(config(), dir.path()).unwrap();
        for request in [unlock(&[0; 32]), unlock(&[42; 31]), Req::new(Op::Unlock)] {
            let result = ks.unlock(&request);
            assert!(result.is_err());
        }
        let locked = ks.unlocked.load(Ordering::SeqCst);
        assert!(!locked);
        ks.unlock(&unlock(&[42; 32])).unwrap();
        let initial = ks.master_keys.read().unwrap().epochs[0].clone();
        assert!(initial.since <= TimeSec::now());
        let epoch = ks.choose_key().unwrap();
        assert!(epoch.since > initial.since);
        ks.propose_key(epoch.clone()).unwrap();
        let duplicate = ks.propose_key(epoch.clone());
        assert!(duplicate.is_err());
        drop(ks);
        let ks = Ks::new(config(), dir.path()).unwrap();
        let locked = ks.unlocked.load(Ordering::SeqCst);
        assert!(!locked);
        ks.unlock(&unlock(&[42; 32])).unwrap();
        let history = ks.master_keys.read().unwrap();
        assert_eq!(history.epochs.len(), 2);
        assert_eq!(history.epochs[0].key, initial.key);
        assert_eq!(history.epochs[1].key, epoch.key);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn io_failure_does_not_publish_a_new_key() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Ks::new(config(), dir.path()).unwrap();
        ks.open_keys([42; 32]).unwrap();
        ks.journal.lock().unwrap().journal.fail_writes();
        let epoch = ks.choose_key().unwrap();
        let result = ks.propose_key(epoch);
        assert!(result.is_err());
        assert_eq!(ks.master_keys.read().unwrap().epochs.len(), 1);
    }
    #[test]
    fn locked_members_journal_commit_and_snapshot_without_the_unlock_key() {
        let mut leader = raft::tests::node(0, raft::tests::members(3));
        let mut follower = raft::tests::node(1, raft::tests::members(3));
        leader.campaign().unwrap();
        let exchange = |leader: &mut raft::tests::TestNode,
                        follower: &mut raft::tests::TestNode| {
            let request = leader.request(&follower.id).unwrap();
            let reply = follower.receive(&leader.id, &request).unwrap();
            leader.response(&follower.id, &request, &reply).unwrap();
        };
        exchange(&mut leader, &mut follower);
        exchange(&mut leader, &mut follower);
        let key = [42; 32];
        let epoch = MasterEpoch { key: [123; 32], since: TimeSec::new(19) };
        let index = leader.state().last() + 1;
        let sealed =
            runtime::seal_key(&key, &leader.state().ring, index, leader.state().term, &epoch)
                .unwrap();
        leader.propose(raft::Command::Key(sealed)).unwrap();
        exchange(&mut leader, &mut follower);
        exchange(&mut leader, &mut follower);
        follower.journal.snapshot(100).unwrap();
        let follower = raft::tests::restart(&follower);
        let history = runtime::decode_history(follower.state(), &key).unwrap();
        assert_eq!(history.epochs[0].key, epoch.key);
        let wrong = runtime::decode_history(follower.state(), &[1; 32]);
        assert!(wrong.is_err());
        let bytes = serde_json::to_vec(&follower.journal.records).unwrap();
        let contains = bytes.windows(32).any(|w| w == key || w == epoch.key);
        assert!(!contains);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn timer_snapshots_dirty_idle_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.snapshot_interval_secs = 1;
        let ks = std::sync::Arc::new(Ks::new(config, dir.path()).unwrap());
        ks.open_keys([42; 32]).unwrap();
        ks.start().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let count = loop {
            let count = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot-"))
                .count();
            if count > 1 || std::time::Instant::now() >= deadline {
                break count;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        ks.stop_snapshot_worker().unwrap();
        assert!(count > 1);
    }
}
