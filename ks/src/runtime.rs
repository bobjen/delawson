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

pub struct Settings {
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
            ring: format!(
                "{}/{}",
                process.cluster.cluster_name,
                if config.raft.members.is_empty() {
                    &process.config.identity
                } else {
                    &config.raft.ring
                }
            ),
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
            ring: "test/KS0".into(),
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
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
enum Message {
    Consensus(Rpc),
    Unlock([u8; 32]),
    Forward { op: u16, payload: Vec<u8>, depth: u8 },
}
#[derive(Serialize, Deserialize)]
struct Packet {
    ring: String,
    unlocked: bool,
    message: Message,
}
#[derive(Serialize, Deserialize)]
struct ConsensusReply {
    member: String,
    answer: Answer,
    unlocked: bool,
}

pub fn waits_for_quorum(request: &Req) -> bool {
    if matches!(request.op, Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers) {
        return true;
    }
    if request.op != Op::Raft {
        return false;
    }
    match request.args.as_slice() {
        [Arg::Text(_), Arg::Bytes(bytes)] => {
            parse::<Packet>(bytes).is_ok_and(|p| matches!(p.message, Message::Forward { .. }))
        }
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

impl Ks {
    pub fn refresh_keys(&self, node: &Node) -> Result<()> {
        if let Some(key) =
            *self.unlock_key.lock().map_err(|_| Error::new("unlock lock poisoned"))?
        {
            let mut history =
                self.master_keys.write().map_err(|_| Error::new("key history lock poisoned"))?;
            if history.applied != node.state().commit {
                *history = decode_history(node.state(), &key)?;
            }
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
        let sealed = seal_key(&key, &self.runtime.ring, index, node.state().term, &epoch)?;
        node.propose(Command::Key(sealed))?;
        Ok(())
    }
    pub fn confirm_leader(&self) -> Result<()> {
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
                if node.can_read(lease::now()?) {
                    self.refresh_keys(&node)?;
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(Error::new("KS cannot confirm leadership with a quorum"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn propose_key(&self, epoch: MasterEpoch) -> Result<u64> {
        let (index, term) = {
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
            let term = node.state().term;
            let index = node
                .state()
                .last()
                .checked_add(1)
                .ok_or_else(|| Error::new("Raft index overflow"))?;
            let sealed = seal_key(&key, &self.runtime.ring, index, term, &epoch)?;
            let index = node.propose(Command::Key(sealed))?;
            self.refresh_keys(&node)?;
            (index, term)
        };
        self.wait_committed(index, term)?;
        Ok(index)
    }
    pub fn wait_committed(&self, index: u64, term: u64) -> Result<()> {
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
                if node.state().term != term || node.role != Role::Leader {
                    return Err(Error::new(
                        "KS leadership changed; request outcome may be unknown",
                    ));
                }
                if node.state().commit >= index && node.lease.ready(term, lease::now()?) {
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
                "raft_role {:?}\nraft_term {}\nraft_leader {}\nraft_last {}\nraft_commit {}\nraft_failed {}",
                node.role,
                node.state().term,
                node.leader.as_deref().unwrap_or("none"),
                node.state().last(),
                node.state().commit,
                node.journal.failed
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
        let request = Req::new(Op::Raft).text(&self.runtime.id).bytes(json(&Packet {
            ring: self.runtime.ring.clone(),
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
    pub fn raft_rpc(&self, request: &Req) -> Resp {
        let result = (|| -> Result<Resp> {
            let [Arg::Text(sender), Arg::Bytes(bytes)] = request.args.as_slice() else {
                return Err(Error::new("invalid Raft RPC"));
            };
            let packet: Packet = parse(bytes)?;
            if packet.ring != self.runtime.ring {
                return Err(Error::new("Raft ring mismatch"));
            }
            {
                let node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                let admitted = if matches!(packet.message, Message::Consensus(_)) {
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
                    match op {
                        Op::SnMasterKey => Ok(self.sn_key(&request)),
                        Op::RotateMasterKey => {
                            self.rotate_master_key(&request).map(|()| Resp::ok())
                        }
                        Op::RaftMembers => Ok(self.change_members(&request)),
                        _ => Err(Error::new("operation cannot be forwarded through Raft")),
                    }
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
            if !rest.is_empty() || members.is_empty() {
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
            // Finish the election service delay before publishing a configuration result,
            // including a final configuration that removes this primary.
            self.confirm_leader()?;
            let target = Membership { old: members.clone(), new: None };
            let _proposal =
                self.proposing.lock().map_err(|_| Error::new("proposal lock poisoned"))?;
            let (index, term) = {
                let mut node = self.journal.lock().map_err(|_| Error::new("Raft lock poisoned"))?;
                if !node.lease.ready(node.state().term, lease::now()?) {
                    return Err(Error::new(
                        "KS primary is waiting for previous read leases to expire",
                    ));
                }
                let old = node.state().membership();
                if old.new.is_some() || node.state().commit != node.state().last() {
                    return Err(Error::new("Raft configuration or proposal already pending"));
                }
                let term = node.state().term;
                let index = node
                    .propose(Command::Members(Membership { old: old.old, new: Some(members) }))?;
                (index, term)
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
                    if node.state().term != term || node.role != Role::Leader {
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
                let mut deadline = Instant::now()
                    + Duration::from_millis(
                        timing.election_min_ms
                            + random % (timing.election_max_ms - timing.election_min_ms),
                    );
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
                        if deadline <= Instant::now() || node.contact != contact {
                            if deadline <= Instant::now()
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
                            deadline = Instant::now()
                                + Duration::from_millis(
                                    c.election_min_ms
                                        + random % (c.election_max_ms - c.election_min_ms),
                                );
                            contact = node.contact;
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
                    ks.maybe_snapshot();
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
                            (address, node.request(&peer))
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
