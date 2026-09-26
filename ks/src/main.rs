//! KS: process registry and encrypted, journaled master-key epochs.
use raft::runtime::Host;
use raft::{journal, lease};
mod runtime;

use common::config::KsConfig;
use common::crypto;
use common::logn;
use common::registry::{self, Peer};
use common::server::{self, Process, Service};
use common::time::TimeSec;
use common::{Context, Result};
use common::{Op, Req, Resp, SnKey, Status, structure};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::Ordering;
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

impl raft::execution::Application for KeyHistory {
    fn applied(&self) -> u64 {
        self.applied
    }
    fn set_applied(&mut self, index: u64) {
        self.applied = index;
    }
}

struct Ks {
    common: raft::runtime::Runtime<KeyHistory>,
    config: KsConfig,
    unlock_verifier: Option<[u8; 32]>,
    unlock_key: Mutex<Option<[u8; 32]>>,
    peers: RwLock<runtime::Registrations>,
    peer_cursor: Mutex<raft::execution::Cursor>,
}
impl std::ops::Deref for Ks {
    type Target = raft::runtime::Runtime<KeyHistory>;
    fn deref(&self) -> &Self::Target {
        &self.common
    }
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
        SERVES.contains(&op)
    }
    fn handle(
        self: &std::sync::Arc<Self>,
        process: &std::sync::Arc<Process>,
        request: Req,
        reply: server::Reply,
    ) {
        if !runtime::needs_request_worker(&request) {
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
        match request.op() {
            Op::Raft => self.raft_rpc(process, request),
            Op::Register => self.register_request(process, request, true),
            Op::Lookup => self.lookup(request),
            Op::Unlock => match self.unlock(request) {
                Ok(()) => Resp::ok(),
                Err(e) => Resp::error(Status::BadRequest, e.to_string()),
            },
            _ => self.client_request(process, request, true),
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
            let history = self.global_lock.read().expect("global_lock");
            (history.epochs.len(), history.applied)
        };
        vec![
            format!("lazy_day_keys {}", self.config.lazy_day_keys),
            format!("require_unlock {}", self.config.require_unlock),
            format!("inline_ping {}", self.config.inline_ping),
            format!("unlocked {}", self.ready.load(Ordering::SeqCst)),
            format!("registered {}", self.peers.read().expect("peers").peers.len()),
            format!("master_keys {key_count}"),
            format!("applied_commit {applied}"),
            self.raft_status(),
        ]
    }
}

impl Ks {
    /// Direct requests and forwarded client requests share this admission boundary.
    fn client_request(&self, process: &Process, request: &Req, allow_forward: bool) -> Resp {
        if allow_forward
            && matches!(request.op(), Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers)
        {
            if let Some(answer) = self.forward(request) {
                return answer;
            }
        }
        // Key generation is internal maintenance; proposals still require write authority.
        if request.op() == Op::SnMasterKey && self.ready.load(Ordering::SeqCst) {
            if let Err(e) = self.prepare_key_read() {
                return Resp::error(Status::Internal, e.to_string());
            }
        }
        let admission = match self.admit_client(request.op() == Op::SnMasterKey) {
            Ok(admission) => admission,
            Err(e) => return Resp::error(Status::Internal, e.to_string()),
        };
        match request.op() {
            Op::RaftMembers => self.change_members(request),
            Op::Lookup => self.lookup(request),
            Op::AssignPeers => self.assign_peers(process, request),
            Op::Registry => {
                registry::response(self.peers.read().expect("peers").peers.values().cloned())
            }
            Op::SnMasterKey => match admission.history.as_deref() {
                Some(history) => self.sn_key(request, history),
                None => Resp::error(Status::Internal, "missing admitted key history"),
            },
            Op::RotateMasterKey => {
                if !self.ready.load(Ordering::SeqCst) {
                    return Resp::error(Status::Locked, "KS is locked");
                }
                match self.rotate_master_key(request) {
                    Ok(()) => Resp::ok(),
                    Err(e) => Resp::error(Status::BadRequest, e.to_string()),
                }
            }
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn stop_snapshot_worker(&self) -> Result<()> {
        self.snapshot_stopping.store(true, Ordering::SeqCst);
        self.wake_execution();
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
        if let Some(fence) = self.io_fence.get() {
            fence.wait_all()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn new(config: KsConfig, path: &std::path::Path) -> Result<Ks> {
        let settings = runtime::Settings::testing(&config.raft);
        // Test ring creation performs the same explicit provisioning as start-laws.
        if !path.join("identity").exists() {
            journal::provision_test_founder(
                path,
                &settings.ring,
                &settings.name,
                settings.identities[&0].identity.clone(),
            )?;
        }
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
        let mut initial = raft::State::initial(
            runtime.ring.clone(),
            if runtime.identities.is_empty() {
                Default::default()
            } else {
                runtime.members.clone()
            },
            runtime.identities.clone(),
        )?;
        initial.ignore_ring_identity = runtime.ignore_ring_identity;
        let journal = journal::Journal::open(path, initial, config.snapshot_retain, &runtime.name)?;
        let id =
            journal.state.membership().slot(&journal.identity.current).unwrap_or(raft::UNASSIGNED);
        let mut node = raft::Node::new(id, journal);
        node.merge_history_scn()?;
        node.identity = Some(node.journal.identity.current.clone());
        node.lease = lease::Lease::configured(&config.raft);
        if node.state().membership().members().len() == 1
            && node.state().membership().members().contains(&id)
        {
            node.campaign()?;
        }
        let test_mode = !config.require_unlock;
        let options = raft::runtime::Options {
            raft: config.raft.clone(),
            snapshot_min_log_bytes: config.snapshot_min_log_bytes,
            snapshot_interval_secs: config.snapshot_interval_secs,
            allow_stale_reads: config.knobs.allow_stale_reads,
        };
        let ks = Ks {
            config,
            unlock_verifier,
            unlock_key: Mutex::new(None),
            peers: RwLock::new(runtime::Registrations::default()),
            peer_cursor: Mutex::new(raft::execution::Cursor::default()),
            common: raft::runtime::Runtime::new(runtime, node, options),
        };
        // Routing must be recovered before transport starts, even while locked.
        let commit = ks.on_raft(|host, node| {
            host.refresh_application(node)?;
            Ok(node.state().commit)
        })?;
        while ks.peers.read().map_err(|_| common::Error::new("registrations poisoned"))?.applied
            < commit
        {
            ks.execute_pending()?;
        }
        if test_mode {
            ks.open_keys(crypto::MASTER_KEY)?;
        }
        Ok(ks)
    }

    fn open_keys(&self, key: [u8; 32]) -> Result<()> {
        let _active =
            self.active_work.lock().map_err(|_| common::Error::new("active execution poisoned"))?;
        if self.ready.load(Ordering::SeqCst) {
            return Ok(());
        }
        let (state, apply) = self.on_raft(|host, node| {
            Ok((
                node.state().clone(),
                node.role == raft::Role::Leader || host.applying.load(Ordering::SeqCst),
            ))
        })?;
        // Decryption and application locking happen outside the consensus loop.
        let history =
            if apply { runtime::decode_history(&state, &key)? } else { KeyHistory::default() };
        *self.unlock_key.lock().map_err(|_| common::Error::new("unlock lock poisoned"))? =
            Some(key);
        *self.global_lock.write().map_err(|_| common::Error::new("key history lock poisoned"))? =
            history;
        *self.execution.lock().map_err(|_| common::Error::new("execution cursor poisoned"))? =
            raft::execution::Cursor::default();
        self.ready.store(true, Ordering::SeqCst);
        // Only a leader establishes the first key, and never over an uncommitted proposal.
        self.on_raft(|host, node| {
            host.initial_key(node)?;
            host.refresh_application(node)
        })?;
        drop(_active);
        if !self.execution_started.load(Ordering::SeqCst) {
            self.execute_pending()?;
        }
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

    fn unlock(&self, request: &Req) -> Result<()> {
        let (mut payload, rest) = structure(&request.args(), Op::Unlock, "an unlock request")?;
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

    fn sn_key(&self, request: &Req, history: &KeyHistory) -> Resp {
        if !self.ready.load(Ordering::SeqCst) {
            return Resp::error(Status::Locked, "KS is locked");
        }
        let (mut payload, rest) =
            match structure(&request.args(), Op::SnMasterKey, "an SN master-key request") {
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
        match Self::sn_key_for(history, sn, at) {
            Ok(key) => Resp::new(Status::Ok, key.write()),
            Err(e) => Resp::error(Status::Internal, e.to_string()),
        }
    }

    fn prepare_key_read(&self) -> Result<()> {
        let index = self.on_raft(|host, node| {
            host.initial_key(node)?;
            Ok(node.state().last())
        })?;
        self.wait_committed(index)?;
        self.ensure_future_key()
    }

    fn sn_key_for(history: &KeyHistory, sn: common::ids::SnId, at: TimeSec) -> Result<SnKey> {
        let epochs = &history.epochs;
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
            .global_lock
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
            structure(&request.args(), Op::RotateMasterKey, "a rotate-master-key request")?;
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
    fn register_request(&self, process: &Process, request: &Req, allow_forward: bool) -> Resp {
        // Direct registration was machine-authenticated by server::read_loop;
        // forwarded registration comes only from an authenticated ring member.
        if allow_forward {
            if let Some(answer) = self.forward(request) {
                return answer;
            }
        }
        self.enroll(process, request)
    }
    fn enroll(&self, process: &Process, request: &Req) -> Resp {
        let peer = match Peer::from_args(&request.args()) {
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
        if let Err(e) = peer.check_ring(
            process.cluster.ring_identities.get(&peer.identity),
            process.config.ignore_ring_identity,
        ) {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        match self.propose_registration(peer) {
            Ok(()) => Resp::ok(),
            Err(e) => Resp::error(Status::Internal, e.to_string()),
        }
    }

    fn assign_peers(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::AssignPeers, "an assign-peers request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        let source = match payload.text() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let Some(source_peer) = self.peers.read().expect("peers").peers.get(&source).cloned()
        else {
            return Resp::error(Status::NotFound, format!("{source} has not registered"));
        };
        let count = match payload.number_u64() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let mut forwarded = common::StructWriter::new();
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
            if self.peers.read().expect("peers").peers.get(&identity).is_none() {
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
        let mut messaging = process.cluster.messaging.clone();
        messaging.expected_peer = Some(source.to_string());
        match server::ask(source_peer.address, &request, process.timeout(), &messaging) {
            Ok(response) => response,
            Err(e) => Resp::error(Status::Internal, format!("could not relay to {source}: {e}")),
        }
    }

    fn lookup(&self, request: &Req) -> Resp {
        let (mut payload, rest) = match structure(&request.args(), Op::Lookup, "a lookup request") {
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
        match self.peers.read().expect("peers").peers.get(&identity) {
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
        let settings = runtime::Settings::for_process(process, &config.raft)?;
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
            knobs: common::config::Knobs { workers: 1, allow_stale_reads: false },
            lazy_day_keys: true,
            master_key_delay_secs: common::time::TimeIntSec64::new(1),
            require_unlock: true,
            inline_ping: true,
        }
    }
    fn unlock(key: &[u8]) -> Req {
        let mut payload = common::StructWriter::new();
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
        let locked = ks.ready.load(Ordering::SeqCst);
        assert!(!locked);
        ks.unlock(&unlock(&[42; 32])).unwrap();
        let initial = ks.global_lock.read().unwrap().epochs[0].clone();
        assert!(initial.since <= TimeSec::now());
        let epoch = ks.choose_key().unwrap();
        assert!(epoch.since > initial.since);
        ks.propose_key(epoch.clone()).unwrap();
        let duplicate = ks.propose_key(epoch.clone());
        assert!(duplicate.is_err());
        drop(ks);
        let ks = Ks::new(config(), dir.path()).unwrap();
        let locked = ks.ready.load(Ordering::SeqCst);
        assert!(!locked);
        ks.unlock(&unlock(&[42; 32])).unwrap();
        let history = ks.global_lock.read().unwrap();
        assert_eq!(history.epochs.len(), 2);
        assert_eq!(history.epochs[0].key, initial.key);
        assert_eq!(history.epochs[1].key, epoch.key);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn locked_registration_is_idempotent_survives_restart_and_rejects_disk_failure() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Ks::new(config(), dir.path()).unwrap();
        let mut peer = Peer {
            identity: "EN0".into(),
            kind: common::config::ServiceKind::En,
            address: "127.0.0.1:9001".parse().unwrap(),
            domain: vec!["dc0".into()],
            media_class: None,
            ring_identity: None,
        };
        ks.propose_registration(peer.clone()).unwrap();
        let before = ks.on_raft(|_, node| Ok(node.state().last())).unwrap();
        ks.propose_registration(peer.clone()).unwrap();
        let after = ks.on_raft(|_, node| Ok(node.state().last())).unwrap();
        assert_eq!(before, after);
        drop(ks);
        let ks = Ks::new(config(), dir.path()).unwrap();
        let restored = ks.peers.read().unwrap().peers["EN0"].clone();
        let unlocked = ks.ready.load(Ordering::SeqCst);
        assert_eq!(restored, peer);
        assert!(!unlocked);
        ks.on_raft(|_, node| {
            node.journal.fail_writes();
            Ok(())
        })
        .unwrap();
        peer.address.set_port(9002);
        let failed = ks.propose_registration(peer).is_err();
        let retained = ks.peers.read().unwrap().peers["EN0"].address.port();
        assert!(failed);
        assert_eq!(retained, 9001);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn io_failure_does_not_publish_a_new_key() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Ks::new(config(), dir.path()).unwrap();
        ks.open_keys([42; 32]).unwrap();
        ks.on_raft(|_, node| {
            node.journal.fail_writes();
            Ok(())
        })
        .unwrap();
        let epoch = ks.choose_key().unwrap();
        let result = ks.propose_key(epoch);
        assert!(result.is_err());
        assert_eq!(ks.global_lock.read().unwrap().epochs.len(), 1);
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
        leader.propose(raft::Command::Client(vec![raft::Action::new(Op::KsKey, sealed)])).unwrap();
        exchange(&mut leader, &mut follower);
        exchange(&mut leader, &mut follower);
        follower.journal.snapshot(100).unwrap();
        let follower = raft::tests::restart(&follower);
        let history = runtime::decode_history(follower.state(), &key).unwrap();
        assert_eq!(history.epochs[0].key, epoch.key);
        let wrong = runtime::decode_history(follower.state(), &[1; 32]);
        assert!(wrong.is_err());
        let bytes = raft::codec::encode(&follower.journal.records).unwrap();
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
