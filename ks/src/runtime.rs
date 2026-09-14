//! KS transport, election timers, and the boundary between consensus and key execution.
use super::*;
use crate::raft::{Answer, Command, Entry, Members, Membership, Node, Role, Rpc, State};
use common::config::{Messaging, RaftConfig, ServiceKind};
use common::wire::Arg;
use common::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct SnapshotProduction<'a>(&'a AtomicBool);
impl Drop for SnapshotProduction<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

pub struct Admission<'a> {
    pub history: Option<std::sync::RwLockReadGuard<'a, KeyHistory>>,
}

pub struct Settings {
    pub ignore_ring_identity: bool,
    pub id: String,
    pub ring: String,
    pub members: Members,
    messaging: Option<Messaging>,
    admitted: BTreeSet<String>,
}
impl Settings {
    pub fn for_process(process: &Process, config: &KsConfig) -> Result<Self> {
        let members = if config.raft.members.is_empty() {
            BTreeMap::from([(process.config.identity.clone(), process.config.listen)])
        } else {
            config.raft.members.clone()
        };
        for id in members.keys() {
            if !process.cluster.service(id).is_some_and(|e| e.kind == ServiceKind::Ks) {
                return Err(Error::new(format!("Raft member {id} is not a KS in the roster")));
            }
        }
        if let Some(address) = members.get(&process.config.identity) {
            if address != &process.config.listen {
                return Err(Error::new("KS listen address differs from Raft membership"));
            }
        }
        Ok(Self {
            id: process.config.identity.clone(),
            ignore_ring_identity: process.config.ignore_ring_identity,
            ring: process
                .cluster
                .ring_identities
                .get(if config.raft.members.is_empty() {
                    &process.config.identity
                } else {
                    &config.raft.ring
                })
                .ok_or_else(|| Error::new("missing KS ring identity in bootstrap configuration"))?
                .to_string(),
            members,
            admitted: process
                .cluster
                .services_of_kind(ServiceKind::Ks)
                .map(|e| e.identity.clone())
                .collect(),
            messaging: Some(process.cluster.messaging.clone()),
        })
    }
    #[cfg(test)]
    pub fn testing(config: &KsConfig) -> Self {
        Self {
            id: "KS0".into(),
            ignore_ring_identity: false,
            ring: "01".repeat(64),
            members: if config.raft.members.is_empty() {
                BTreeMap::from([("KS0".into(), "127.0.0.1:1".parse().unwrap())])
            } else {
                config.raft.members.clone()
            },
            admitted: BTreeSet::new(),
            messaging: None,
        }
    }
    pub fn validate(&self, config: &RaftConfig) -> Result<()> {
        if self.members.is_empty() {
            return Err(Error::new("Raft needs nonempty members"));
        }
        config.validate_timing()?;
        if self.members.len() > 1 && config.apply_members < 2 {
            return Err(Error::new(
                "a multi-member ring needs at least two applying members for checkpoints",
            ));
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
enum Message {
    Consensus(Rpc),
    Checkpoint { after: u64 },
    CheckpointStart,
    Unlock([u8; 32]),
    Forward { op: u16, payload: Vec<u8>, depth: u8 },
}
#[derive(Serialize, Deserialize)]
struct Packet {
    ring: String,
    unlocked: bool,
    #[serde(default)]
    applying: Option<BTreeSet<String>>,
    message: Message,
}
#[derive(Serialize, Deserialize)]
struct ConsensusReply {
    member: String,
    answer: Answer,
    unlocked: bool,
}

pub fn waits_for_quorum(request: &Req) -> bool {
    if matches!(request.op, Op::Unlock | Op::Register) {
        return false;
    }
    if request.op != Op::Raft {
        return true;
    }
    match request.args.as_slice() {
        [Arg::Text(_), Arg::Bytes(bytes)] => parse::<Packet>(bytes).is_ok_and(|p| {
            matches!(p.message, Message::Forward { .. } | Message::Checkpoint { .. })
        }),
        _ => false,
    }
}

fn json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::new(e.to_string()))
}
fn parse<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::new(e.to_string()))
}
fn error(e: impl std::fmt::Display) -> Resp {
    Resp::error(Status::Internal, e.to_string())
}

fn ad(ring: &str, index: u64, term: u64) -> Vec<u8> {
    [b"DLAW-RAFT-KEY-1".as_slice(), ring.as_bytes(), &index.to_le_bytes(), &term.to_le_bytes()]
        .concat()
}
pub fn seal_key(
    key: &[u8; 32],
    ring: &str,
    index: u64,
    term: u64,
    epoch: &MasterEpoch,
) -> Result<Vec<u8>> {
    let nonce = random_master_key()?;
    let plain = [epoch.key.as_slice(), &epoch.since.number().to_le_bytes()].concat();
    let (bytes, tag) =
        aegis::aegis256::Aegis256::<32>::new(key, &nonce).encrypt(&plain, &ad(ring, index, term));
    Ok([nonce.as_slice(), &bytes, &tag].concat())
}
fn open_key(key: &[u8; 32], ring: &str, entry: &Entry) -> Result<MasterEpoch> {
    let Command::Key(bytes) = &entry.command else {
        return Err(Error::new("expected key entry"));
    };
    if bytes.len() != 104 {
        return Err(Error::new("invalid encrypted KS key length"));
    }
    let nonce: [u8; 32] = bytes[..32].try_into().map_err(|_| Error::new("invalid nonce"))?;
    let tag: [u8; 32] = bytes[72..].try_into().map_err(|_| Error::new("invalid tag"))?;
    let plain = aegis::aegis256::Aegis256::<32>::new(key, &nonce)
        .decrypt(&bytes[32..72], &tag, &ad(ring, entry.index, entry.term))
        .map_err(|_| Error::new("KS key authentication failed"))?;
    Ok(MasterEpoch {
        key: plain[..32].try_into().map_err(|_| Error::new("invalid key"))?,
        since: TimeSec::new(u64::from_le_bytes(
            plain[32..].try_into().map_err(|_| Error::new("invalid activation time"))?,
        )),
    })
}
pub fn decode_history(state: &State, key: &[u8; 32]) -> Result<KeyHistory> {
    let snapshot = state.capture();
    let mut epochs: Vec<MasterEpoch> = vec![];
    for entry in &snapshot.keys {
        let epoch = open_key(key, &state.ring, entry)?;
        if epochs.last().is_some_and(|last| last.since >= epoch.since) {
            return Err(Error::new("KS key activation order is invalid"));
        }
        epochs.push(epoch);
    }
    Ok(KeyHistory { epochs, applied: state.commit })
}

fn apply_history(state: &State, key: &[u8; 32], history: &mut KeyHistory) -> Result<()> {
    if history.applied == state.commit {
        return Ok(());
    }
    if history.applied < state.snapshot.index {
        *history = decode_history(state, key)?;
    } else {
        let mut additions = Vec::new();
        let mut since = history.epochs.last().map(|e| e.since);
        for entry in
            state.log.iter().filter(|e| e.index > history.applied && e.index <= state.commit)
        {
            if matches!(entry.command, Command::Key(_)) {
                let epoch = open_key(key, &state.ring, entry)?;
                if since.is_some_and(|last| last >= epoch.since) {
                    return Err(Error::new("KS key activation order is invalid"));
                }
                since = Some(epoch.since);
                additions.push(epoch);
            }
        }
        history.epochs.extend(additions);
        history.applied = state.commit;
    }
    Ok(())
}
pub(crate) fn cancel_checkpoint_after_election(role: Role, member_count: usize) -> bool {
    role == Role::Leader && member_count > 1
}
fn election_delay(config: &RaftConfig, random: u64, hot: bool, writing: bool) -> Duration {
    Duration::from_millis(
        config
            .election_min_ms
            .saturating_add(random % (config.election_max_ms - config.election_min_ms))
            .saturating_add(if !hot {
                config.cold_election_delay_ms
            } else if writing {
                config.checkpoint_election_delay_ms
            } else {
                0
            }),
    )
}

impl Ks {
    fn accept_placement(&self, selected: Option<&BTreeSet<String>>, node: &Node) -> Result<()> {
        if let Some(selected) = selected {
            let members = node.state().membership().members();
            let mut placement =
                self.placement.lock().map_err(|_| Error::new("placement lock poisoned"))?;
            placement.selected =
                selected.iter().filter(|id| members.contains_key(*id)).cloned().collect();
            self.applying.store(placement.selected.contains(&self.runtime.id), Ordering::SeqCst);
        }
        Ok(())
    }
    fn serve_checkpoint(&self, after: u64) -> Result<Option<crate::raft::Snapshot>> {
        let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
        if node.role == Role::Leader || node.role == Role::Stopped || node.journal.failed {
            return Err(Error::new("primary does not serve checkpoints"));
        }
        if node.state().snapshot.index <= after {
            if self.applying.load(Ordering::SeqCst) && self.unlocked.load(Ordering::SeqCst) {
                self.checkpoint_requested.store(true, Ordering::SeqCst);
            }
            return Ok(None);
        }
        Ok(Some(node.state().snapshot.clone()))
    }
    fn start_checkpoints(self: &Arc<Self>) -> Result<()> {
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name("ks-checkpoints".into())
            .spawn(move || {
                while let Some(ks) = weak.upgrade() {
                    if ks.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    ks.maybe_snapshot();
                    drop(ks);
                    std::thread::park_timeout(Duration::from_millis(200));
                }
            })
            .context("starting checkpoint worker")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }
    pub fn maybe_snapshot(&self) {
        if let Err(e) = self.checkpoint_work() {
            logn!("KS checkpoint deferred; journals retained", "{e}");
        }
    }
    fn checkpoint_work(&self) -> Result<()> {
        let Ok(_task) = self.snapshot_task.try_lock() else {
            return Ok(());
        };
        let _production = SnapshotProduction(&self.producing_snapshot);
        let now = TimeSec::now().number();
        let (pending, peers, after) = {
            let mut node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
            if node.journal.failed || matches!(node.role, Role::Stopped | Role::Candidate) {
                return Ok(());
            }
            let needed = self.checkpoint_needed.load(Ordering::SeqCst);
            let requested = self.checkpoint_requested.load(Ordering::SeqCst)
                || needed > node.state().snapshot.index;
            if !requested
                && !node.journal.snapshot_due(
                    self.config.snapshot_min_log_bytes,
                    self.config.snapshot_interval_secs,
                    now,
                )
            {
                return Ok(());
            }
            let members = node.state().membership().members();
            let producer = needed <= node.state().commit
                && (node.role == Role::Follower
                    || (node.role == Role::Leader && members.len() == 1))
                && (node.role == Role::Leader || self.applying.load(Ordering::SeqCst))
                && self.unlocked.load(Ordering::SeqCst);
            if producer {
                self.refresh_keys(&node)?;
                if members.len() > 1 {
                    let lease = self
                        .checkpoint_lease
                        .lock()
                        .map_err(|_| Error::new("checkpoint lease lock poisoned"))?;
                    if let Some((index, holder)) = lease.holder(
                        Instant::now(),
                        Duration::from_secs(self.config.raft.checkpoint_lease_secs),
                    ) {
                        if holder != &self.runtime.id
                            || *index <= self.checkpoint_started.load(Ordering::SeqCst)
                        {
                            return Ok(());
                        }
                        self.checkpoint_started.store(*index, Ordering::SeqCst);
                    } else {
                        let target = node
                            .leader
                            .as_ref()
                            .and_then(|id| members.get(id).map(|address| (id.clone(), *address)));
                        drop(lease);
                        drop(node);
                        if let Some((id, address)) = target {
                            self.send_raft(&id, address, Message::CheckpointStart)?;
                        }
                        return Ok(());
                    }
                }
                self.cancel_snapshot.store(false, Ordering::SeqCst);
                // Set under the consensus lock, before any campaign can begin.
                self.producing_snapshot.store(true, Ordering::SeqCst);
                self.refresh_keys(&node)?;
                let history =
                    self.global_lock.read().map_err(|_| Error::new("global lock poisoned"))?;
                if history.applied != node.state().commit {
                    return Ok(());
                }
                let pending = node.journal.prepare_snapshot(now)?;
                // Capture and rotate under the execution read lock; disk publication is independent.
                (pending, vec![], node.state().snapshot.index)
            } else {
                let peers: Vec<_> = members
                    .into_iter()
                    .filter(|(id, _)| id != &self.runtime.id && Some(id) != node.leader.as_ref())
                    .collect();
                (None, peers, node.state().snapshot.index)
            }
        };
        if let Some(pending) = pending {
            pending.write_cancellable(|| self.cancel_snapshot.load(Ordering::SeqCst))?;
            self.journal
                .lock()
                .map_err(|_| Error::new("Raft lock poisoned"))?
                .journal
                .finish_snapshot(&pending)?;
            self.checkpoint_requested.store(false, Ordering::SeqCst);
            return Ok(());
        }
        for (id, address) in peers {
            let Ok(response) = self.send_raft(&id, address, Message::Checkpoint { after }) else {
                continue;
            };
            if response.status != Status::Ok {
                continue;
            }
            let (mut body, rest) = structure(&response.args, Op::BytesResp, "checkpoint response")?;
            let bytes = body.bytes()?;
            body.done()?;
            if !rest.is_empty() {
                return Err(Error::new("trailing checkpoint response"));
            }
            let Some(snapshot) = parse::<Option<crate::raft::Snapshot>>(&bytes)? else {
                continue;
            };
            let pending = {
                let mut node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                if node.role == Role::Stopped || node.journal.failed {
                    return Ok(());
                }
                if snapshot.index <= node.state().snapshot.index
                    || snapshot.term > node.state().term
                {
                    continue;
                }
                if snapshot.index > node.state().commit {
                    // Trusted member's durable checkpoint proves commitment; application is separate.
                    // A primary must re-prove its own log's commitment through Raft.
                    if node.role == Role::Leader {
                        continue;
                    }
                    node.journal.write(&[crate::raft::Record::Install(snapshot.clone())])?;
                }
                let pending = node.journal.prepare_copy(now, snapshot)?;
                self.refresh_keys(&node)?;
                pending
            };
            if let Some(pending) = pending {
                pending.write()?;
                self.journal
                    .lock()
                    .map_err(|_| Error::new("Raft lock poisoned"))?
                    .journal
                    .finish_snapshot(&pending)?;
                self.checkpoint_requested.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }
        Ok(())
    }
    pub fn refresh_keys(&self, node: &Node) -> Result<()> {
        // A candidate can keep writing. Only winning requires abandoning production;
        // singleton primaries remain the explicit checkpoint-production exception.
        if cancel_checkpoint_after_election(node.role, node.state().membership().members().len())
            && self.producing_snapshot.load(Ordering::SeqCst)
        {
            self.cancel_snapshot.store(true, Ordering::SeqCst);
        }
        self.checkpoint_lease
            .lock()
            .map_err(|_| Error::new("checkpoint lease lock poisoned"))?
            .observe(node.state().checkpoint_start(), Instant::now());
        if node.role != Role::Leader && !self.applying.load(Ordering::SeqCst) {
            *self.global_lock.write().map_err(|_| Error::new("global lock poisoned"))? =
                KeyHistory::default();
            return Ok(());
        }
        if let Some(key) =
            *self.unlock_key.lock().map_err(|_| Error::new("unlock lock poisoned"))?
        {
            let mut history =
                self.global_lock.write().map_err(|_| Error::new("global lock poisoned"))?;
            apply_history(node.state(), &key, &mut history)?;
        }
        Ok(())
    }
    pub fn initial_key(&self, node: &mut Node) -> Result<()> {
        if node.role != Role::Leader
            || node.state().last() != node.state().commit
            || node.state().has_keys()
        {
            return Ok(());
        }
        let Some(key) = *self.unlock_key.lock().map_err(|_| Error::new("unlock lock poisoned"))?
        else {
            return Ok(());
        };
        let epoch = MasterEpoch { key: random_master_key()?, since: TimeSec::now() };
        let index =
            node.state().last().checked_add(1).ok_or_else(|| Error::new("Raft index overflow"))?;
        let sealed = seal_key(&key, &node.state().ring, index, node.state().term, &epoch)?;
        node.propose(Command::Key(sealed))?;
        Ok(())
    }
    pub fn admit_client(&self, read: bool) -> Result<Admission<'_>> {
        let began = Instant::now();
        let deadline = began
            + Duration::from_millis(
                self.config.raft.election_max_ms.saturating_add(self.config.raft.primary_wait_ms),
            );
        loop {
            {
                let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                if node.role != Role::Leader || node.journal.failed {
                    return Err(Error::new("KS is not a healthy Raft leader"));
                }
                if node.can_admit(lease::now()?) {
                    self.refresh_keys(&node)?;
                    let history = if read {
                        Some(
                            self.global_lock
                                .read()
                                .map_err(|_| Error::new("global lock poisoned"))?,
                        )
                    } else {
                        None
                    };
                    // Admission occurs after lock acquisition and application catch-up.
                    if node.can_admit(lease::now()?) {
                        return Ok(Admission { history });
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(Error::new("KS cannot confirm leadership with a quorum"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn propose_key(&self, epoch: MasterEpoch) -> Result<u64> {
        let index = {
            let mut node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
            if node.state().last() != node.state().commit {
                return Err(Error::new(
                    "KS has an outstanding Raft proposal; retry after it commits",
                ));
            }
            let key = self
                .unlock_key
                .lock()
                .map_err(|_| Error::new("unlock lock poisoned"))?
                .ok_or_else(|| Error::new("KS is locked"))?;
            let history = decode_history(node.state(), &key)?;
            if history.epochs.last().is_some_and(|last| last.since >= epoch.since) {
                return Err(Error::new(
                    "new key activation must be later than the last recorded activation",
                ));
            }
            let index = node
                .state()
                .last()
                .checked_add(1)
                .ok_or_else(|| Error::new("Raft index overflow"))?;
            let sealed = seal_key(&key, &node.state().ring, index, node.state().term, &epoch)?;
            let index = node.propose(Command::Key(sealed))?;
            self.refresh_keys(&node)?;
            index
        };
        self.wait_committed(index)?;
        Ok(index)
    }
    pub fn wait_committed(&self, index: u64) -> Result<()> {
        let deadline = Instant::now()
            + Duration::from_millis(
                self.config
                    .raft
                    .election_max_ms
                    .saturating_mul(2)
                    .saturating_add(self.config.raft.primary_wait_ms),
            );
        loop {
            {
                let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                if node.journal.failed {
                    return Err(Error::new("KS journal failed"));
                }
                if node.role != Role::Leader {
                    return Err(Error::new(
                        "KS leadership changed; request outcome may be unknown",
                    ));
                }
                if node.state().commit >= index {
                    self.refresh_keys(&node)?;
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(Error::new("KS quorum timeout; request outcome may be unknown"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn raft_status(&self) -> String {
        match self.journal.lock() {
            Ok(node) => format!(
                "raft_role {:?}\nraft_term {}\nraft_leader {}\nraft_last {}\nraft_commit {}\nraft_failed {}\nraft_applying {}",
                node.role,
                node.state().term,
                node.leader.as_deref().unwrap_or("none"),
                node.state().last(),
                node.state().commit,
                node.journal.failed,
                node.role == Role::Leader || self.applying.load(Ordering::SeqCst)
            ),
            Err(_) => "raft_failed true".into(),
        }
    }
    fn send_raft(
        &self,
        peer: &str,
        address: std::net::SocketAddr,
        message: Message,
    ) -> Result<Resp> {
        let timeout = if matches!(message, Message::Forward { .. }) {
            self.config
                .raft
                .election_max_ms
                .saturating_mul(2)
                .saturating_add(self.config.raft.primary_wait_ms)
                .saturating_add(self.config.raft.request_timeout_ms)
        } else {
            self.config.raft.request_timeout_ms
        };
        let mut messaging =
            self.runtime.messaging.clone().ok_or_else(|| Error::new("no Raft transport"))?;
        messaging.expected_peer = Some(peer.into());
        let applying = {
            let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
            if node.role == Role::Leader {
                Some(
                    self.placement
                        .lock()
                        .map_err(|_| Error::new("placement lock poisoned"))?
                        .selected
                        .clone(),
                )
            } else {
                None
            }
        };
        let request = Req::new(Op::Raft).text(&self.runtime.id).bytes(json(&Packet {
            ring: self.runtime.ring.clone(),
            applying,
            unlocked: self.unlocked.load(Ordering::SeqCst),
            message,
        })?);
        server::ask(address, &request, Duration::from_millis(timeout), &messaging)
    }
    pub fn forward(&self, request: &Req) -> Option<Resp> {
        let target = {
            let node = match self.journal.lock() {
                Ok(node) => node,
                Err(e) => return Some(error(e)),
            };
            if node.role == Role::Leader {
                return None;
            }
            node.leader.as_ref().and_then(|id| {
                node.state().membership().members().get(id).map(|address| (id.clone(), *address))
            })
        };
        let Some((id, address)) = target else {
            return Some(error("KS has no known Raft leader"));
        };
        let [Arg::Struct(_, payload, depth)] = request.args.as_slice() else {
            return Some(error("expected structured KS request"));
        };
        Some(
            self.send_raft(
                &id,
                address,
                Message::Forward { op: request.op.code(), payload: payload.clone(), depth: *depth },
            )
            .unwrap_or_else(error),
        )
    }
    pub fn raft_rpc(&self, process: &Process, request: &Req) -> Resp {
        let result = (|| -> Result<Resp> {
            let [Arg::Text(sender), Arg::Bytes(bytes)] = request.args.as_slice() else {
                return Err(Error::new("invalid Raft RPC"));
            };
            let packet: Packet = parse(bytes)?;
            if !self.runtime.ignore_ring_identity && packet.ring != self.runtime.ring {
                return Err(Error::new("Raft ring mismatch"));
            }
            {
                let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                let admitted =
                    if matches!(packet.message, Message::Consensus(_) | Message::Checkpoint { .. })
                    {
                        // A lagging member may not yet know the current leader's configuration.
                        self.runtime.admitted.contains(sender)
                            || self.runtime.members.contains_key(sender)
                    } else {
                        node.state().membership().members().contains_key(sender)
                    };
                if !admitted {
                    return Err(Error::new("Raft sender is not admitted"));
                }
            }
            if !packet.unlocked {
                self.unlock_requests
                    .lock()
                    .map_err(|_| Error::new("unlock request lock poisoned"))?
                    .insert(sender.clone());
            }
            match packet.message {
                Message::CheckpointStart => {
                    let mut node =
                        self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                    if node.role != Role::Leader || sender == &self.runtime.id {
                        return Err(Error::new(
                            "checkpoint announcements require the primary and a secondary writer",
                        ));
                    }
                    if !packet.unlocked
                        || !self
                            .placement
                            .lock()
                            .map_err(|_| Error::new("placement lock poisoned"))?
                            .selected
                            .contains(sender)
                    {
                        return Err(Error::new(
                            "checkpoint writer is not an unlocked applying member",
                        ));
                    }
                    self.refresh_keys(&node)?;
                    let lease = self
                        .checkpoint_lease
                        .lock()
                        .map_err(|_| Error::new("checkpoint lease lock poisoned"))?;
                    if lease
                        .holder(
                            Instant::now(),
                            Duration::from_secs(self.config.raft.checkpoint_lease_secs),
                        )
                        .is_none()
                        && node.state().last() == node.state().commit
                    {
                        node.propose(Command::CheckpointStart(sender.clone()))?;
                    }
                    Ok(Resp::ok())
                }
                Message::Checkpoint { after } => {
                    let snapshot = self.serve_checkpoint(after)?;
                    Ok(Resp::bytes(json(&snapshot)?))
                }
                Message::Consensus(rpc) => {
                    let mut node =
                        self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                    let recent_leader = node.election_quiet(
                        Instant::now(),
                        Duration::from_millis(self.config.raft.election_min_ms),
                    );
                    let answer = if matches!(rpc, Rpc::Vote { .. }) && recent_leader {
                        Answer { term: node.state().term, success: false, matched: 0 }
                    } else {
                        node.receive(sender, &rpc)?
                    };
                    if !matches!(rpc, Rpc::Vote { .. })
                        && rpc.term() == node.state().term
                        && node.leader.as_deref() == Some(sender)
                    {
                        self.accept_placement(packet.applying.as_ref(), &node)?;
                        if let Rpc::Checkpoint { index, .. } = rpc {
                            if !answer.success {
                                self.checkpoint_needed.fetch_max(index, Ordering::SeqCst);
                            }
                        }
                    }
                    self.refresh_keys(&node)?;
                    Ok(Resp::bytes(json(&ConsensusReply {
                        member: self.runtime.id.clone(),
                        answer,
                        unlocked: self.unlocked.load(Ordering::SeqCst),
                    })?))
                }
                Message::Unlock(key) => {
                    if self
                        .unlock_verifier
                        .as_ref()
                        .is_some_and(|expected| !common::shamir::matches(&key, expected))
                    {
                        return Err(Error::new("wrong ring unlock key"));
                    }
                    self.open_keys(key)?;
                    Ok(Resp::ok())
                }
                Message::Forward { op, payload, depth } => {
                    let op = Op::from_code(op)?;
                    if self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?.role
                        != Role::Leader
                    {
                        return Err(Error::new("forward target is no longer KS leader"));
                    }
                    let request = Req { id: 0, op, args: vec![Arg::Struct(op, payload, depth)] };
                    if !matches!(op, Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers) {
                        return Err(Error::new("operation cannot be forwarded through Raft"));
                    }
                    Ok(self.client_request(process, &request, false))
                }
            }
        })();
        result.unwrap_or_else(error)
    }
    pub fn change_members(&self, request: &Req) -> Resp {
        let result = (|| -> Result<()> {
            let (mut payload, rest) =
                structure(&request.args, Op::RaftMembers, "a Raft membership request")?;
            let members: Members = parse(&payload.bytes()?)?;
            payload.done()?;
            if !rest.is_empty()
                || members.is_empty()
                || (members.len() > 1 && self.config.raft.apply_members < 2)
            {
                return Err(Error::new("invalid Raft membership request"));
            }
            // Machine admission is independent of consensus membership. Every proposed
            // member must already be enrolled; this is not user authorization.
            if let Some(auth) =
                self.runtime.messaging.as_ref().and_then(|m| m.authentication.as_ref())
            {
                for id in members.keys() {
                    if !auth.members.iter().any(|m| m.services.contains(id)) {
                        return Err(Error::new("new KS member is not enrolled"));
                    }
                }
            }
            if self.runtime.messaging.is_some()
                && members.keys().any(|id| !self.runtime.admitted.contains(id))
            {
                return Err(Error::new("new member is not a KS in the roster"));
            }
            let target = Membership { old: members.clone(), new: None };
            let _proposal =
                self.proposing.lock().map_err(|_| Error::new("proposal lock poisoned"))?;
            let index = {
                let mut node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                let old = node.state().membership();
                if old.new.is_some() || node.state().commit != node.state().last() {
                    return Err(Error::new("Raft configuration or proposal already pending"));
                }
                node.propose(Command::Members(Membership { old: old.old, new: Some(members) }))?
            };
            let deadline = Instant::now()
                + Duration::from_millis(
                    self.config
                        .raft
                        .election_max_ms
                        .saturating_mul(2)
                        .saturating_add(self.config.raft.primary_wait_ms),
                );
            loop {
                {
                    let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                    if node.state().commit >= index && node.state().committed_membership() == target
                    {
                        return Ok(());
                    }
                    if node.role != Role::Leader {
                        return Err(Error::new("leadership changed during membership change"));
                    }
                }
                if Instant::now() >= deadline {
                    return Err(Error::new("membership change pending; quorum timeout"));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        })();
        result.map(|()| Resp::ok()).unwrap_or_else(error)
    }
    pub fn start_raft(self: &Arc<Self>) -> Result<()> {
        self.start_checkpoints()?;
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name("ks-raft-timer".into())
            .spawn(move || {
                let mut contact = 0;
                let Some(initial) = weak.upgrade() else {
                    return;
                };
                let random = match random_master_key() {
                    Ok(bytes) => u64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8])),
                    Err(_) => return,
                };
                let timing = &initial.config.raft;
                let mut deadline = Instant::now() + election_delay(timing, random, true, false);
                drop(initial);
                let mut peers: BTreeSet<String> = BTreeSet::new();
                loop {
                    let Some(ks) = weak.upgrade() else {
                        break;
                    };
                    if ks.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    let result = (|| -> Result<()> {
                        let mut node =
                            ks.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                        if node.journal.failed {
                            return Ok(());
                        }
                        // Apply the current penalty to the base deadline: starting or finishing
                        // a checkpoint between heartbeats must affect election preference too.
                        let c = &ks.config.raft;
                        let penalty = election_delay(
                            c,
                            0,
                            ks.applying.load(Ordering::SeqCst),
                            ks.producing_snapshot.load(Ordering::SeqCst),
                        ) - election_delay(c, 0, true, false);
                        let eligible = deadline + penalty;
                        if eligible <= Instant::now() || node.contact != contact {
                            if eligible <= Instant::now()
                                && node.contact == contact
                                && node.role != Role::Leader
                            {
                                node.campaign()?;
                            }
                            let random = u64::from_le_bytes(
                                random_master_key()?[..8]
                                    .try_into()
                                    .map_err(|_| Error::new("random timeout"))?,
                            );
                            let c = &ks.config.raft;
                            deadline = Instant::now() + election_delay(c, random, true, false);
                            contact = node.contact;
                        }
                        if node.role == Role::Leader {
                            ks.placement
                                .lock()
                                .map_err(|_| Error::new("placement lock poisoned"))?
                                .select(
                                    &ks.runtime.id,
                                    &node.state().membership().members(),
                                    ks.config.raft.apply_members,
                                    Instant::now(),
                                    Duration::from_millis(ks.config.raft.cold_election_delay_ms),
                                );
                        }
                        ks.initial_key(&mut node)?;
                        ks.refresh_keys(&node)?;
                        let members = node.state().membership().members();
                        drop(node);
                        for id in members.keys() {
                            if id != &ks.runtime.id
                                && peers.insert(id.clone())
                                && ks.runtime.messaging.is_some()
                            {
                                ks.start_peer(id.clone())?;
                            }
                        }
                        Ok(())
                    })();
                    if let Err(e) = result {
                        logn!("KS Raft maintenance failed", "{e}");
                    }

                    drop(ks);
                    std::thread::park_timeout(Duration::from_millis(20));
                }
            })
            .context("starting Raft timer")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }
    fn start_peer(self: &Arc<Self>, peer: String) -> Result<()> {
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name(format!("raft-{peer}"))
            .spawn(move || {
                let mut unlocked = false;
                loop {
                    let Some(ks) = weak.upgrade() else {
                        break;
                    };
                    if ks.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    let result = (|| -> Result<()> {
                        let (address, request) = {
                            let node =
                                ks.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                            if node.journal.failed {
                                return Ok(());
                            }
                            let Some(address) =
                                node.state().membership().members().get(&peer).copied()
                            else {
                                return Ok(());
                            };
                            (address, node.request_reference(&peer))
                        };
                        let requested = ks
                            .unlock_requests
                            .lock()
                            .map_err(|_| Error::new("unlock request lock poisoned"))?
                            .contains(&peer);
                        if !unlocked || requested {
                            let key = *ks
                                .unlock_key
                                .lock()
                                .map_err(|_| Error::new("unlock lock poisoned"))?;
                            if let Some(key) = key {
                                unlocked =
                                    ks.send_raft(&peer, address, Message::Unlock(key))?.status
                                        == Status::Ok;
                                if unlocked {
                                    ks.unlock_requests
                                        .lock()
                                        .map_err(|_| Error::new("unlock request lock poisoned"))?
                                        .remove(&peer);
                                }
                            }
                        }
                        if let Some(request) = request {
                            let sent = lease::now()?;
                            let response =
                                ks.send_raft(&peer, address, Message::Consensus(request.clone()))?;
                            if response.status != Status::Ok {
                                return Err(Error::new(format!("peer {peer} rejected Raft RPC")));
                            }
                            let (mut body, rest) =
                                structure(&response.args, Op::BytesResp, "Raft response")?;
                            let bytes = body.bytes()?;
                            body.done()?;
                            if !rest.is_empty() {
                                return Err(Error::new("trailing Raft response"));
                            }
                            let response: ConsensusReply = parse(&bytes)?;
                            if response.member != peer {
                                return Err(Error::new("Raft reply came from the wrong member"));
                            }
                            unlocked = response.unlocked;
                            ks.placement
                                .lock()
                                .map_err(|_| Error::new("placement lock poisoned"))?
                                .acknowledge(&peer, Instant::now());
                            let mut node =
                                ks.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                            node.response(&peer, &request, &response.answer)?;
                            if matches!(request, Rpc::Append { .. })
                                && response.answer.success
                                && request.term() == node.state().term
                                && node.role == Role::Leader
                            {
                                node.lease.acknowledge(&peer, request.term(), sent);
                            }
                            ks.refresh_keys(&node)?;
                        }
                        Ok(())
                    })();
                    if result.is_err() {
                        unlocked = false;
                    }
                    let interval = Duration::from_millis(ks.config.raft.heartbeat_ms);
                    drop(ks);
                    std::thread::park_timeout(interval);
                }
            })
            .context("starting Raft peer worker")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn debugging_foreign_checkpoint_preserves_key_binding_but_not_override() {
        let key = [42; 32];
        let mut source = State::initial("02".repeat(64), crate::raft::tests::members(1)).unwrap();
        source.apply(&crate::raft::Record::Hard { term: 1, vote: None }).unwrap();
        let epoch = MasterEpoch { key: [7; 32], since: TimeSec::new(1) };
        let sealed = seal_key(&key, &source.ring, 1, 1, &epoch).unwrap();
        source
            .apply(&crate::raft::Record::Entries {
                from: 1,
                entries: vec![Entry { index: 1, term: 1, command: Command::Key(sealed) }],
            })
            .unwrap();
        source.apply(&crate::raft::Record::Commit(1)).unwrap();
        let mut target = State::initial("01".repeat(64), crate::raft::tests::members(1)).unwrap();
        target.ignore_ring_identity = true;
        target.apply(&crate::raft::Record::Hard { term: 1, vote: None }).unwrap();
        target.apply(&crate::raft::Record::Install(source.capture())).unwrap();
        let history = decode_history(&target, &key).unwrap();
        let wrong_key = decode_history(&target, &[43; 32]).is_err();
        let decoded: State = parse(&json(&target).unwrap()).unwrap();
        assert_eq!(history.epochs[0].key, epoch.key);
        assert!(wrong_key);
        assert!(!decoded.ignore_ring_identity);
    }
    #[test]
    fn cold_candidates_wait_longer_without_changing_hot_backup_elections() {
        let config = RaftConfig::default();
        let hot = election_delay(&config, 37, true, false);
        let cold = election_delay(&config, 37, false, false);
        let writing = election_delay(&config, 37, true, true);
        assert_eq!(hot, Duration::from_millis(187));
        assert!(hot < writing && writing < cold);
        assert_eq!(writing - hot, Duration::from_millis(config.checkpoint_election_delay_ms));
        assert_eq!(cold - hot, Duration::from_millis(config.cold_election_delay_ms));
    }
    #[test]
    fn application_catches_up_from_snapshot_then_only_decodes_new_commits() {
        let key = [7; 32];
        let mut state = State::initial("01".repeat(64), crate::raft::tests::members(3)).unwrap();
        state.term = 1;
        for index in 1..=3 {
            let epoch = MasterEpoch { since: TimeSec::new(index), key: [index as u8; 32] };
            let encrypted = seal_key(&key, &state.ring, index, 1, &epoch).unwrap();
            state.log.push(Entry { index, term: 1, command: Command::Key(encrypted) });
        }
        state.commit = 2;
        let mut history = KeyHistory::default();
        apply_history(&state, &key, &mut history).unwrap();
        let committed_count = history.epochs.len();
        let checkpoint = state.capture();
        state.apply(&crate::raft::Record::Install(checkpoint)).unwrap();
        let mut promoted = KeyHistory::default();
        apply_history(&state, &key, &mut promoted).unwrap();
        let promoted_count = promoted.epochs.len();
        // Already applied ciphertext need not be decrypted again.
        state.snapshot.keys[0].command = Command::Key(vec![]);
        state.commit = 3;
        apply_history(&state, &key, &mut history).unwrap();
        let final_count = history.epochs.len();
        let final_index = history.applied;
        assert_eq!(committed_count, 2);
        assert_eq!(promoted_count, 2);
        assert_eq!(final_count, 3);
        assert_eq!(final_index, 3);
    }
    #[test]
    fn clients_wait_off_the_raft_worker_including_forwarded_requests() {
        let clients: Vec<_> = SERVES
            .iter()
            .copied()
            .filter(|op| !matches!(op, Op::Raft | Op::Unlock | Op::Register))
            .collect();
        let all_clients = clients.iter().all(|op| waits_for_quorum(&Req::new(*op)));
        let unknown_client = waits_for_quorum(&Req::new(Op::BytesResp));
        let unlock = waits_for_quorum(&Req::new(Op::Unlock));
        let registration = waits_for_quorum(&Req::new(Op::Register));
        let packet = |message| {
            Req::new(Op::Raft).text("KS0").bytes(
                json(&Packet { ring: "test".into(), unlocked: false, applying: None, message })
                    .unwrap(),
            )
        };
        let consensus = waits_for_quorum(&packet(Message::Consensus(Rpc::Vote {
            term: 1,
            last: 0,
            last_term: 0,
        })));
        let transfer = waits_for_quorum(&packet(Message::Unlock([0; 32])));
        let forwarded = waits_for_quorum(&packet(Message::Forward {
            op: Op::SnMasterKey.code(),
            payload: vec![],
            depth: 0,
        }));
        assert!(all_clients);
        assert!(unknown_client);
        assert!(!unlock);
        assert!(!registration);
        assert!(!consensus);
        assert!(!transfer);
        assert!(forwarded);
    }
}
