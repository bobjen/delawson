//! Raft transport, election timers, and the boundary between consensus and client execution.
use crate::execution::Application;
use crate::{Answer, Command, Entry, Members, Membership, Node, Role, Rpc, State};
use crate::{Member, MemberId, MemberNames, Retirement, Storage, UNASSIGNED};
use crate::{application, journal, lease, raft, random_bytes};
use common::Arg;
use common::config::{Messaging, RaftConfig};
use common::server::{self, Process};
use common::time::TimeSec;
use common::{Context, logn};
use common::{Error, Result};
use common::{MessagePayload, Op, Req, Resp, Status, structure};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

pub struct Options {
    pub raft: RaftConfig,
    pub snapshot_min_log_bytes: u64,
    pub snapshot_interval_secs: u64,
    pub allow_stale_reads: bool,
}
pub enum PendingWork {
    Apply(crate::execution::Work),
    Unload,
}
#[derive(Default)]
pub struct WorkQueue {
    generation: u64,
    pending: Option<PendingWork>,
    last: u64,
    snapshot: Option<Arc<crate::Snapshot>>,
    enabled: bool,
}
impl WorkQueue {
    fn enqueue(&mut self, state: &State, enabled: bool) {
        let changed = self.generation != state.generation;
        if changed {
            *self = Self { generation: state.generation, ..Self::default() };
        }

        if !enabled {
            if self.enabled {
                self.pending = Some(PendingWork::Unload);
                self.last = 0;
                self.snapshot = None;
            }
            self.enabled = false;
            return;
        }
        if !self.enabled {
            self.last = 0;
            self.snapshot = None;
            self.pending = None;
        }
        self.enabled = true;
        if state.commit <= self.last && !changed {
            return;
        }
        let snapshot = self.snapshot.get_or_insert_with(|| Arc::new(state.snapshot.clone()));
        if snapshot.index != state.snapshot.index {
            *snapshot = Arc::new(state.snapshot.clone());
        }
        let entries: Vec<_> = state
            .log
            .iter()
            .filter(|e| e.index > self.last && e.index <= state.commit)
            .cloned()
            .collect();
        if let Some(PendingWork::Apply(work)) = &mut self.pending {
            work.entries.extend(entries);
            work.entries.retain(|e| e.index > snapshot.index);
            work.snapshot = snapshot.clone();
            work.commit = state.commit;
        } else {
            self.pending = Some(PendingWork::Apply(crate::execution::Work {
                generation: state.generation,
                ring: state.ring.clone(),
                snapshot: snapshot.clone(),
                entries,
                commit: state.commit,
            }));
        }
        self.last = state.commit;
    }
}
pub enum Deferred {
    Message(Message),
    Read(crate::io::Read<Vec<u8>>),
}
enum CheckpointPlan {
    Request(MemberId),
    Produce(State),
    Copy { after: u64, primary: Option<(MemberId, u64)>, peers: Vec<(MemberId, bool)> },
}
pub struct Runtime<S> {
    pub options: Options,
    pub runtime: Settings,
    pub journal: crate::owner::Owner<Node>,
    pub host: std::sync::OnceLock<std::sync::Weak<dyn std::any::Any + Send + Sync>>,
    pub placement: Mutex<application::Placement>,
    pub applying: AtomicBool,
    pub producing_snapshot: AtomicBool,
    pub cancel_snapshot: AtomicBool,
    pub checkpoint_lease: Mutex<application::CheckpointLease>,
    pub checkpoint_started: AtomicU64,
    pub checkpoint_requested: AtomicBool,
    pub checkpoint_needed: AtomicU64,
    pub snapshot_stopping: AtomicBool,
    pub io_fence: std::sync::OnceLock<crate::io::Fence>,
    pub snapshot_task: Mutex<()>,
    pub transport: Option<common::peer::Peers>,
    pub ready: AtomicBool,
    pub readiness_requests: Mutex<BTreeSet<MemberId>>,
    pub proposing: Mutex<()>,
    pub raft_workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    pub global_lock: RwLock<S>,
    pub execution: Mutex<crate::execution::Cursor>,
    pub application_generation: AtomicU64,
    pub metadata_generation: AtomicU64,
    pub work_queue: Mutex<WorkQueue>,
    metadata_queue: Mutex<WorkQueue>,
    metadata_active: Mutex<Option<PendingWork>>,
    pub active_work: Mutex<Option<PendingWork>>,
    pub execution_started: AtomicBool,
    pub execution_error: Mutex<Option<String>>,
    execution_wake: std::sync::mpsc::SyncSender<()>,
    execution_receive: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl<S: Default> Runtime<S> {
    pub fn new(runtime: Settings, node: Node, options: Options) -> Self {
        let membership = node.state().membership();
        let placement = application::Placement::new(
            &membership.replacements(&membership.members()),
            options.raft.apply_members,
        );
        let transport = runtime.messaging.as_ref().map(|messaging| {
            let mut messaging = messaging.clone();
            messaging.peer_conns = 1;
            common::peer::Peers::new(
                messaging,
                Duration::from_millis(options.raft.request_timeout_ms),
            )
        });
        let (execution_wake, execution_receive) = std::sync::mpsc::sync_channel(1);
        Self {
            execution_wake,
            execution_receive: Mutex::new(Some(execution_receive)),
            applying: AtomicBool::new(placement.selected.contains(&node.id)),
            placement: Mutex::new(placement),
            options,
            runtime,
            journal: crate::owner::Owner::new(node),
            host: std::sync::OnceLock::new(),
            transport,
            producing_snapshot: AtomicBool::new(false),
            cancel_snapshot: AtomicBool::new(false),
            checkpoint_lease: Mutex::new(application::CheckpointLease::default()),
            checkpoint_started: AtomicU64::new(0),
            checkpoint_requested: AtomicBool::new(false),
            checkpoint_needed: AtomicU64::new(0),
            snapshot_stopping: AtomicBool::new(false),
            io_fence: std::sync::OnceLock::new(),
            snapshot_task: Mutex::new(()),
            ready: AtomicBool::new(false),
            readiness_requests: Mutex::new(BTreeSet::new()),
            proposing: Mutex::new(()),
            raft_workers: Mutex::new(vec![]),
            global_lock: RwLock::new(S::default()),
            execution: Mutex::new(crate::execution::Cursor::default()),
            application_generation: AtomicU64::new(0),
            metadata_generation: AtomicU64::new(0),
            work_queue: Mutex::new(WorkQueue::default()),
            metadata_queue: Mutex::new(WorkQueue::default()),
            metadata_active: Mutex::new(None),
            active_work: Mutex::new(None),
            execution_started: AtomicBool::new(false),
            execution_error: Mutex::new(None),
        }
    }
}

struct SnapshotProduction<'a>(&'a AtomicBool);
impl Drop for SnapshotProduction<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

pub struct Admission<'a, S> {
    pub history: Option<std::sync::RwLockReadGuard<'a, S>>,
}

pub struct Settings {
    pub ignore_ring_identity: bool,
    pub name: String,
    pub identities: MemberNames,
    pub founders: BTreeMap<MemberId, String>,
    pub ring: String,
    pub members: Members,
    pub messaging: Option<Messaging>,
    seeds: BTreeMap<String, std::net::SocketAddr>,
    registrar: Option<(String, std::net::SocketAddr)>,
    admitted: BTreeSet<String>,
}
impl Settings {
    pub fn for_process(process: &Process, config: &RaftConfig) -> Result<Self> {
        let members = if config.members.is_empty() {
            BTreeMap::from([(process.config.identity.clone(), process.config.listen)])
        } else {
            config.members.clone()
        };
        for id in members.keys() {
            if !process.cluster.service(id).is_some_and(|e| e.kind == process.config.kind) {
                return Err(Error::new(format!("Raft member {id} is not a Raft in the roster")));
            }
        }
        let expected = process
            .cluster
            .messaging
            .local_ring
            .as_ref()
            .ok_or_else(|| Error::new("missing local ring identity"))?;
        let ring_name =
            if config.members.is_empty() { &process.config.identity } else { &config.ring };
        if !process.config.ignore_ring_identity
            && process.cluster.ring_identities.get(ring_name) != Some(expected)
        {
            return Err(Error::new("process and Raft configuration name different rings"));
        }
        let indexes = if config.members.is_empty() {
            BTreeMap::from([(process.config.identity.clone(), 0)])
        } else {
            config.member_indexes.clone()
        };
        let founders: BTreeMap<MemberId, String> = members
            .keys()
            .enumerate()
            .map(|(i, name)| {
                let slot = u64::from(indexes.get(name).copied().unwrap_or(i as u32));
                (slot, name.clone())
            })
            .collect();
        if founders.len() != members.len() || founders.contains_key(&UNASSIGNED) {
            return Err(Error::new("founding slots must be distinct"));
        }
        if !process.config.ignore_ring_identity
            && founders
                .values()
                .any(|name| process.cluster.ring_identities.get(name) != Some(expected))
        {
            return Err(Error::new("founding participant belongs to a different ring"));
        }
        Ok(Self {
            name: process.config.identity.clone(),
            identities: BTreeMap::new(),
            founders: founders.clone(),
            ignore_ring_identity: process.config.ignore_ring_identity,
            ring: process
                .cluster
                .ring_identities
                .get(if config.members.is_empty() {
                    &process.config.identity
                } else {
                    &config.ring
                })
                .ok_or_else(|| Error::new("missing Raft ring identity in bootstrap configuration"))?
                .to_string(),
            members: founders.keys().copied().collect(),
            seeds: members,
            registrar: Some((
                process.cluster.registrar()?.identity.clone(),
                process.cluster.ks.address,
            )),
            admitted: process
                .cluster
                .services_of_kind(process.config.kind)
                .map(|e| e.identity.clone())
                .collect(),
            messaging: Some(process.cluster.messaging.clone()),
        })
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing(config: &RaftConfig) -> Self {
        Self {
            name: "member-0".into(),
            identities: crate::raft::tests::names(),
            founders: crate::raft::tests::names()
                .into_iter()
                .map(|(i, m)| (i, m.process))
                .collect(),
            ignore_ring_identity: false,
            ring: "01".repeat(64),
            members: if config.members.is_empty() {
                BTreeSet::from([0])
            } else {
                config.members.keys().map(|name| u64::from(config.member_indexes[name])).collect()
            },
            seeds: config.members.clone(),
            registrar: None,
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
pub enum Message {
    Identity,
    Bootstrap { founders: Membership, membership: Membership },
    Consensus(Rpc),
    Checkpoint { after: u64, primary_fallback: bool },
    Journal { after: u64, through: u64, primary_fallback: bool },
    JournalTip { index: u64, term: u64 },
    CheckpointStart,
    Client(crate::Action),
    Forward { op: u16, payload: common::MessageBytes, depth: u8 },
}
pub struct Packet {
    member: Sender,
    target: Option<MemberId>,
    retirement: Option<Retirement>,
    ready: bool,

    applying: Option<BTreeSet<MemberId>>,
    message: Message,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Sender {
    Slot(MemberId),
    Applicant(common::ring::MemberIdentity),
}
struct ConsensusReply {
    member: Sender,
    retirement: Option<Retirement>,
    answer: Answer,
    ready: bool,
}

/// Keep client setup, forwarding, and checkpoint waits off the shared RPC workers.
/// This classifies scheduling, not whether an operation requires quorum.
pub fn needs_request_worker(request: &Req) -> bool {
    if matches!(request.op(), Op::Register | Op::Lookup) {
        return false;
    }
    if request.op() != Op::Raft {
        return true;
    }
    raft_request_payload(request)
        .and_then(|(_, bytes)| crate::codec::decode_message::<Packet>(&bytes))
        .is_ok_and(|p| {
            matches!(
                p.message,
                Message::Forward { .. } | Message::Checkpoint { .. } | Message::Client(_)
            )
        })
}

fn raft_request_payload(request: &Req) -> Result<(String, MessagePayload)> {
    let (mut payload, rest) = structure(request.args(), Op::Raft, "a Raft RPC")?;
    if !rest.is_empty() {
        return Err(Error::new("Raft RPC has trailing arguments"));
    }
    let sender = payload.text()?;
    let length = payload.number_u32()? as usize;
    let bytes = payload.raw_message_bytes(length)?;
    payload.done()?;
    Ok((sender, bytes.into()))
}

fn binary<T: crate::codec::Wire>(value: &T) -> Result<Vec<u8>> {
    crate::codec::encode(value)
}
fn parse<T: crate::codec::Wire>(bytes: &[u8]) -> Result<T> {
    crate::codec::decode(bytes)
}
fn error(e: impl std::fmt::Display) -> Resp {
    Resp::error(Status::Internal, e.to_string())
}

pub(crate) fn cancel_checkpoint_after_election(role: Role, member_count: usize) -> bool {
    role == Role::Leader && member_count > 1
}
fn replacement_delay(config: &RaftConfig, pending: bool) -> Duration {
    if pending {
        Duration::from_millis(config.lost_state_election_delay_ms)
    } else {
        Duration::ZERO
    }
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

/// Retirement contacts may still be needed while an old slot is in the voting
/// configuration, but client setup belongs only to its live replacement.
fn peer_eligibility(membership: &Membership, local: MemberId, peer: MemberId) -> (bool, bool) {
    let contact = peer != local && membership.participants().contains(&peer);
    (contact, contact && !membership.retired.contains_key(&peer))
}
fn recovery_sources(
    members: raft::Members,
    local: MemberId,
    primary: Option<MemberId>,
) -> Vec<(MemberId, bool)> {
    let mut sources: Vec<_> = members
        .into_iter()
        .filter(|id| *id != local && Some(*id) != primary)
        .map(|id| (id, false))
        .collect();
    if let Some(primary) = primary.filter(|id| *id != local) {
        sources.push((primary, true));
    }
    sources
}

pub trait Host:
    std::ops::Deref<Target = Runtime<Self::Application>> + Send + Sync + Sized + 'static
{
    type Application: crate::execution::Application;
    fn apply_client(&self, state: &crate::execution::Work) -> Result<()>;
    /// Small public routing state needed even by locked or non-applying members.
    /// Runs on the application worker, never on the consensus owner.
    fn has_metadata(&self) -> bool {
        false
    }
    fn apply_metadata(&self, _state: &crate::execution::Work) -> Result<bool> {
        Ok(true)
    }
    fn prepare_primary(&self, node: &mut Node) -> Result<()>;
    fn checkpoint_actions(
        &self,
        state: &State,
        application: &Self::Application,
    ) -> Result<Vec<Entry>>;
    fn peer_message(&self) -> Option<crate::Action>;
    fn receive_client(&self, process: &Process, action: crate::Action) -> Result<Resp>;
    fn dispatch_forwarded(&self, process: &Process, request: &Req) -> Resp;
    fn forwardable(&self, op: Op) -> bool;
    fn registered_endpoint(&self, name: &str) -> Option<std::net::SocketAddr>;

    /// All consensus calls run on raft-main after startup. Inputs and results are owned;
    /// no consensus reference or lock guard can escape into a helper thread.
    fn on_raft<R: Send + 'static>(
        &self,
        f: impl FnOnce(&Self, &mut Node) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        if self.host.get().is_none() {
            return self.journal.initial_call(|node| f(self, node));
        }
        let host = self
            .host
            .get()
            .and_then(|weak| weak.upgrade())
            .and_then(|host| host.downcast::<Self>().ok())
            .ok_or_else(|| Error::new("Raft host has not started"))?;
        let (result, fence) = self.journal.call(move |node| {
            node.journal.poll_io()?;
            let result = f(&host, node);
            Ok((result, node.journal.fence()))
        })?;
        // The calling helper can wait; raft-main must never wait for a disk fence.
        if let Some(fence) = fence {
            fence.wait()?;
        }
        result
    }
    fn accept_founders(&self, founders: Membership) -> Result<()> {
        founders.validate()?;
        let names: BTreeMap<_, _> =
            founders.identities.iter().map(|(slot, m)| (*slot, m.process.clone())).collect();
        if founders.new.is_some()
            || names != self.runtime.founders
            || founders.old != self.runtime.members
        {
            return Err(Error::new("bootstrap participant/slot list differs from configuration"));
        }
        self.on_raft(move |host, node| {
            if let Some((_, member)) =
                founders.identities.iter().find(|(_, m)| m.process == host.runtime.name)
            {
                if member.identity != node.journal.identity.current
                    && node.state().founders.as_ref() != Some(&founders)
                {
                    return Err(Error::new(
                        "bootstrap does not name this process's persistent identity",
                    ));
                }
            }
            node.initialize_founders(founders)
        })
    }
    fn bootstrap_message(&self, founders: Membership) -> Result<Message> {
        self.on_raft(move |_, node| {
            Ok(Message::Bootstrap { founders, membership: node.state().membership() })
        })
    }
    fn query_member(&self, name: &str) -> Result<Member> {
        if name == self.runtime.name {
            return self.on_raft(|host, node| {
                Ok(Member {
                    identity: node.journal.identity.current.clone(),
                    process: host.runtime.name.clone(),
                })
            });
        }
        let response = self.send_named(name, Message::Identity)?;
        if response.status != Status::Ok {
            return Err(Error::new("member identity query failed"));
        }
        let (mut body, rest) = structure(response.args(), Op::BytesResp, "member identity")?;
        let member: Member = parse(&body.bytes()?)?;
        body.done()?;
        if !rest.is_empty() || member.process != name {
            return Err(Error::new("member identity reply mismatch"));
        }
        Ok(member)
    }
    fn bootstrap_step(&self) -> Result<bool> {
        let (needed, chosen) = self.on_raft(|_, node| {
            Ok((node.journal.needs_bootstrap(), node.state().founders.clone()))
        })?;
        if !needed {
            return Ok(true);
        }
        if !self.runtime.founders.values().any(|name| name == &self.runtime.name) {
            return Err(Error::new("bootstrap coordinator is not a founding participant"));
        }
        let founders = if let Some(chosen) = chosen {
            chosen
        } else {
            let mut identities = MemberNames::new();
            for (slot, name) in &self.runtime.founders {
                identities.insert(*slot, self.query_member(name)?);
            }
            let membership = Membership {
                retired: Default::default(),
                identities,
                old: self.runtime.members.clone(),
                new: None,
            };
            // Persist the single chosen table before distributing it. Retry reuses it.
            self.accept_founders(membership.clone())?;
            membership
        };
        for member in founders.identities.values() {
            if member.process == self.runtime.name {
                continue;
            }
            let response =
                self.send_named(&member.process, self.bootstrap_message(founders.clone())?)?;
            if response.status != Status::Ok {
                return Err(Error::new("founding member has not accepted bootstrap"));
            }
        }
        self.on_raft(|_, node| node.journal.finish_bootstrap())?;
        Ok(true)
    }
    fn start_bootstrap(self: &Arc<Self>) -> Result<()> {
        if !self.on_raft(|_, node| Ok(node.journal.needs_bootstrap()))? {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name("raft-bootstrap".into())
            .spawn(move || {
                while let Some(ks) = weak.upgrade() {
                    if ks.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    match ks.bootstrap_step() {
                        Ok(true) => break,
                        Ok(false) => {}
                        Err(e) => logn!("Raft bootstrap waiting", "{e}"),
                    }
                    drop(ks);
                    std::thread::park_timeout(Duration::from_millis(200));
                }
            })
            .context("starting bootstrap coordinator")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }
    fn accept_placement(&self, selected: Option<&BTreeSet<MemberId>>, node: &Node) -> Result<()> {
        if let Some(selected) = selected {
            let members = node.state().membership().members();
            let mut placement =
                self.placement.lock().map_err(|_| Error::new("placement lock poisoned"))?;
            placement.selected =
                selected.iter().filter(|id| members.contains(*id)).cloned().collect();
            self.applying.store(placement.selected.contains(&node.id), Ordering::SeqCst);
        }
        Ok(())
    }
    fn serve_checkpoint(
        &self,
        after: u64,
        primary_fallback: bool,
    ) -> Result<Option<raft::Snapshot>> {
        self.on_raft(move |host, node| host.checkpoint_from(node, after, primary_fallback))
    }
    fn checkpoint_from(
        &self,
        node: &Node,
        after: u64,
        primary_fallback: bool,
    ) -> Result<Option<raft::Snapshot>> {
        if (node.role == Role::Leader && !primary_fallback)
            || node.role == Role::Stopped
            || node.repairing
            || node.journal.failed
        {
            return Err(Error::new("primary does not serve checkpoints"));
        }
        if after != u64::MAX && node.state().snapshot.index <= after {
            if node.role != Role::Leader
                && self.applying.load(Ordering::SeqCst)
                && self.ready.load(Ordering::SeqCst)
            {
                self.checkpoint_requested.store(true, Ordering::SeqCst);
            }
            return Ok(None);
        }
        Ok(Some(node.state().snapshot.clone()))
    }
    fn start_checkpoints(self: &Arc<Self>) -> Result<()> {
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name("raft-checkpoints".into())
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
    fn maybe_snapshot(&self) {
        if let Err(e) = self.checkpoint_work() {
            logn!("Raft checkpoint deferred; journals retained", "{e}");
        }
    }
    fn primary_history_tip(
        &self,
        primary: MemberId,
        term: u64,
        index: u64,
    ) -> Result<Option<(u64, common::scn::Scn)>> {
        let local = self.on_raft(move |_, node| {
            if node.id != primary {
                return Ok(None);
            }
            if node.role != Role::Leader || node.state().term != term {
                return Err(Error::new("history proof requires the current primary"));
            }
            Ok(Some(node.journal.read_history(move |history| history.committed_tip(index))?))
        })?;
        if let Some(value) = local {
            return value.wait();
        }
        let response = self.send_raft(&primary, Message::JournalTip { index, term })?;
        if response.status != Status::Ok {
            return Err(Error::new("primary history proof unavailable"));
        }
        let (mut body, rest) = structure(response.args(), Op::BytesResp, "journal tip response")?;
        let value = parse(&body.bytes()?)?;
        body.done()?;
        if !rest.is_empty() {
            return Err(Error::new("trailing journal tip response"));
        }
        Ok(value)
    }
    fn copy_committed_journal(&self) -> Result<bool> {
        let plan = self.on_raft(|host, node| {
            let through = host.checkpoint_needed.load(Ordering::SeqCst);
            if node.role != Role::Follower
                || node.repairing
                || node.journal.failed
                || through <= node.state().commit
            {
                return Ok(None);
            }
            let Some(leader) = node.leader else {
                return Ok(None);
            };
            let peers =
                recovery_sources(node.state().membership().participants(), node.id, Some(leader));
            Ok(Some((node.state().commit, through, leader, node.state().term, peers)))
        })?;
        let Some((after, through, leader, term, peers)) = plan else {
            return Ok(false);
        };
        for (peer, primary_fallback) in peers {
            let Ok(response) =
                self.send_raft(&peer, Message::Journal { after, through, primary_fallback })
            else {
                continue;
            };
            if response.status != Status::Ok {
                continue;
            }
            let decoded = (|| -> Result<Option<journal::CommittedJournal>> {
                let (mut body, rest) =
                    structure(response.args(), Op::BytesResp, "journal response")?;
                let value = parse(&body.bytes()?)?;
                body.done()?;
                if !rest.is_empty() {
                    return Err(Error::new("trailing journal response"));
                }
                Ok(value)
            })();
            let copied = match decoded {
                Ok(Some(copied)) => copied,
                Ok(None) => continue,
                Err(error) => {
                    logn!("Raft journal source unusable", "peer {},error {}", peer, error);
                    continue;
                }
            };
            let Some(last) = copied.entries.last() else {
                continue;
            };
            // Bulk data still comes from secondaries first. The chosen primary supplies
            // only the term at the end of this range, in the same election term.
            let response =
                match self.send_raft(&leader, Message::JournalTip { index: last.index, term }) {
                    Ok(response) if response.status == Status::Ok => response,
                    _ => continue,
                };
            let proof = (|| -> Result<Option<(u64, common::scn::Scn)>> {
                let (mut body, rest) =
                    structure(response.args(), Op::BytesResp, "journal tip response")?;
                let value = parse(&body.bytes()?)?;
                body.done()?;
                if !rest.is_empty() {
                    return Err(Error::new("trailing journal tip response"));
                }
                Ok(value)
            })();
            let primary_term = match proof {
                Ok(Some(term)) => term,
                Ok(None) => {
                    logn!(
                        "Raft primary journal tip unavailable",
                        "primary {},index {}",
                        leader,
                        last.index
                    );
                    continue;
                }
                Err(error) => {
                    logn!(
                        "Raft journal tip response unusable",
                        "primary {},index {},error {}",
                        leader,
                        last.index,
                        error
                    );
                    continue;
                }
            };
            if !copied.matches_tip(last.index, primary_term) {
                logn!(
                    "Raft copied journal differs from primary",
                    "peer {},primary {},index {},copied_term {},primary_tip {:?}",
                    peer,
                    leader,
                    last.index,
                    last.term,
                    primary_term
                );
                continue;
            }
            // A proved range tip cannot by itself authorize replacing our
            // committed prefix. Ask the primary about the copied range's base.
            let prefix_proof = self.primary_history_tip(leader, term, copied.after)?;
            if prefix_proof != Some((copied.previous_term, copied.previous_scn)) {
                continue;
            }
            if self.on_raft(move |host, node| {
                if node.role != Role::Follower
                    || node.leader != Some(leader)
                    || node.state().term != term
                    || node.state().commit != after
                {
                    return Ok(false);
                }
                if node.state().history_at(copied.after) != prefix_proof {
                    node.repairing = true;
                    node.journal.require_history_repair()?;
                    logn!(
                        "Raft journal proves committed history conflict",
                        "index {}",
                        copied.after
                    );
                    return Ok(false);
                }
                if copied.validate(node.state(), through).is_err() {
                    return Ok(false);
                }
                if let Some(scn) = copied.largest_scn() {
                    node.merge_scn(scn)?;
                }
                node.journal.import_committed(&copied, through)?;
                logn!(
                    "Raft committed journal copied",
                    "peer {},after {},through {}",
                    peer,
                    after,
                    through
                );
                node.refresh_slot();
                host.refresh_application(&node)?;
                Ok(true)
            })? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn checkpoint_work(&self) -> Result<()> {
        let Ok(_task) = self.snapshot_task.try_lock() else {
            return Ok(());
        };
        if self.copy_committed_journal()? {
            return Ok(());
        }
        let _production = SnapshotProduction(&self.producing_snapshot);
        let now = TimeSec::now().number();
        // The application lock is obtained only by this checkpoint helper, never raft-main.
        let history = self.global_lock.read().map_err(|_| Error::new("global lock poisoned"))?;
        let applied = history.applied();
        let plan = self.on_raft(move |host, node| {
            if node.journal.failed || matches!(node.role, Role::Stopped | Role::Candidate) {
                return Ok(None);
            }
            let needed = host.checkpoint_needed.load(Ordering::SeqCst);
            if !node.repairing
                && !host.checkpoint_requested.load(Ordering::SeqCst)
                && needed <= node.state().snapshot.index
                && !node.journal.snapshot_due(
                    host.options.snapshot_min_log_bytes,
                    host.options.snapshot_interval_secs,
                    now,
                )
            {
                return Ok(None);
            }
            let members = node.state().membership().members();
            let producer = !node.repairing
                && host.application_generation.load(Ordering::SeqCst) == node.state().generation
                && needed <= node.state().commit
                && (node.role == Role::Follower
                    || (node.role == Role::Leader && members.len() == 1))
                && (node.role == Role::Leader || host.applying.load(Ordering::SeqCst))
                && host.ready.load(Ordering::SeqCst);
            if producer {
                host.refresh_application(node)?;
                if members.len() > 1 {
                    let lease = host
                        .checkpoint_lease
                        .lock()
                        .map_err(|_| Error::new("checkpoint lease lock poisoned"))?;
                    if let Some((index, holder)) = lease.holder(
                        Instant::now(),
                        Duration::from_secs(host.options.raft.checkpoint_lease_secs),
                    ) {
                        if holder != &node.id
                            || *index <= host.checkpoint_started.load(Ordering::SeqCst)
                        {
                            return Ok(None);
                        }
                        if applied != node.state().commit {
                            return Ok(None);
                        }
                        host.checkpoint_started.store(*index, Ordering::SeqCst);
                    } else {
                        return Ok(node.leader.map(CheckpointPlan::Request));
                    }
                }
                if applied != node.state().commit {
                    return Ok(None);
                }
                host.cancel_snapshot.store(false, Ordering::SeqCst);
                host.producing_snapshot.store(true, Ordering::SeqCst);
                Ok(Some(CheckpointPlan::Produce(node.state().clone())))
            } else {
                Ok(Some(CheckpointPlan::Copy {
                    after: if node.repairing { u64::MAX } else { node.state().snapshot.index },
                    primary: node.leader.map(|id| (id, node.state().term)),
                    peers: recovery_sources(
                        node.state().membership().participants(),
                        node.id,
                        node.leader,
                    ),
                }))
            }
        })?;
        match plan {
            None => Ok(()),
            Some(CheckpointPlan::Request(peer)) => {
                drop(history);
                self.send_raft(&peer, Message::CheckpointStart)?;
                Ok(())
            }
            Some(CheckpointPlan::Produce(state)) => {
                logn!("Raft checkpoint started", "index {},term {}", state.commit, state.term);
                let mut snapshot = state.capture();
                snapshot.application = self.checkpoint_actions(&state, &history)?;
                let pending = self.on_raft(move |host, node| {
                    if host.cancel_snapshot.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    node.journal.prepare_copy(now, snapshot)
                })?;
                if let Some(pending) = pending {
                    pending.write_cancellable(|| self.cancel_snapshot.load(Ordering::SeqCst))?;
                    drop(history);
                    self.on_raft(move |_, node| node.journal.finish_snapshot(&pending))?;
                    self.checkpoint_requested.store(false, Ordering::SeqCst);
                    logn!("Raft checkpoint completed", "index {}", state.commit);
                }
                Ok(())
            }
            Some(CheckpointPlan::Copy { after, primary, peers }) => {
                drop(history);
                let Some((primary, election_term)) = primary else { return Ok(()) };
                for (id, primary_fallback) in peers {
                    let Ok(response) =
                        self.send_raft(&id, Message::Checkpoint { after, primary_fallback })
                    else {
                        continue;
                    };
                    if response.status != Status::Ok {
                        continue;
                    }
                    let decoded = (|| -> Result<Option<raft::Snapshot>> {
                        let (mut body, rest) =
                            structure(response.args(), Op::BytesResp, "checkpoint response")?;
                        let bytes = body.bytes()?;
                        body.done()?;
                        if !rest.is_empty() {
                            return Err(Error::new("trailing checkpoint response"));
                        }
                        parse(&bytes)
                    })();
                    let snapshot = match decoded {
                        Ok(Some(snapshot)) => snapshot,
                        Ok(None) => continue,
                        Err(error) => {
                            logn!("Raft checkpoint source unusable", "peer {},error {}", id, error);
                            continue;
                        }
                    };
                    let proof = self.primary_history_tip(primary, election_term, snapshot.index);
                    if !matches!(proof, Ok(Some(tip)) if tip == (snapshot.term, snapshot.scn)) {
                        logn!(
                            "Raft checkpoint history rejected",
                            "peer {},primary {},index {},checkpoint_term {},proof {:?}",
                            id,
                            primary,
                            snapshot.index,
                            snapshot.term,
                            proof
                        );
                        continue;
                    }
                    let verified_term = proof?;
                    let pending = self.on_raft(move |host, node| {
                        if node.role == Role::Stopped
                            || node.journal.failed
                            || node.leader != Some(primary)
                            || node.state().term != election_term
                        {
                            return Ok(None);
                        }
                        if node.repairing {
                            if let Some(scn) = snapshot.largest_scn() {
                                node.merge_scn(scn)?;
                            }
                            node.journal.repair_history(snapshot.clone(), verified_term)?;
                            node.identity = Some(node.journal.identity.current.clone());
                            node.refresh_slot();
                            return Ok(None);
                        }
                        if snapshot.index <= node.state().commit
                            && node.state().history_at(snapshot.index)
                                != Some((snapshot.term, snapshot.scn))
                        {
                            logn!(
                                "Raft checkpoint proves committed history conflict",
                                "index {}",
                                snapshot.index
                            );
                            node.repairing = true;
                            node.journal.require_history_repair()?;
                            return Ok(None);
                        }
                        let install =
                            journal::checkpoint_install(node.state(), &snapshot, verified_term)?;
                        if let Some(install) = install {
                            if node.role == Role::Leader {
                                return Ok(None);
                            }
                            if let Some(scn) = snapshot.largest_scn() {
                                node.merge_scn(scn)?;
                            }
                            node.journal.write(&[install])?;
                            node.refresh_slot();
                        }
                        logn!(
                            "Raft checkpoint copied",
                            "peer {},index {},term {}",
                            id,
                            snapshot.index,
                            snapshot.term
                        );
                        let pending = node.journal.prepare_copy(now, snapshot)?;
                        node.refresh_slot();
                        host.refresh_application(node)?;
                        Ok(pending)
                    })?;
                    if after == u64::MAX {
                        self.on_raft(|host, node| {
                            if !node.journal.identity.repairing {
                                node.repairing = false;
                                host.refresh_application(node)?;
                                logn!(
                                    "Raft conflicting history rebuilt",
                                    "index {},term {}",
                                    node.state().commit,
                                    node.state().term
                                );
                            }
                            Ok(())
                        })?;
                        break;
                    }
                    if let Some(pending) = pending {
                        pending.write()?;
                        self.on_raft(move |_, node| node.journal.finish_snapshot(&pending))?;
                        self.checkpoint_requested.store(false, Ordering::SeqCst);
                        break;
                    }
                }
                Ok(())
            }
        }
    }
    fn refresh_application(&self, node: &Node) -> Result<()> {
        if node.repairing {
            return Ok(());
        }
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
            .observe(node.journal.durable_state().checkpoint_start(), Instant::now());
        if self.has_metadata() {
            let mut queue =
                self.metadata_queue.lock().map_err(|_| Error::new("metadata queue poisoned"))?;
            queue.enqueue(node.journal.durable_state(), true);
            if queue.pending.is_some() {
                self.wake_execution();
            }
        }
        let enabled = self.ready.load(Ordering::SeqCst)
            && (node.role == Role::Leader || self.applying.load(Ordering::SeqCst));
        let mut queue =
            self.work_queue.lock().map_err(|_| Error::new("application queue poisoned"))?;
        queue.enqueue(node.journal.durable_state(), enabled);
        let pending = queue.pending.is_some();
        drop(queue);
        if pending {
            self.wake_execution();
        }
        Ok(())
    }

    /// Runs without the journal/consensus mutex. Only the execution worker owns
    /// active work; the consensus thread merely appends immutable committed work.
    fn execute_pending(&self) -> Result<()> {
        let result = self.execute_pending_work();
        let mut previous_error =
            self.execution_error.lock().map_err(|_| Error::new("execution error lock poisoned"))?;
        let current_error = result.as_ref().err().map(ToString::to_string);
        if *previous_error != current_error {
            if let Some(error) = &current_error {
                logn!("Raft client application failed", "error {}", error);
            } else {
                logn!("Raft client application recovered", "");
            }
        }
        *previous_error = current_error;
        drop(previous_error);
        result
    }
    fn execute_pending_work(&self) -> Result<()> {
        // Keep routing usable without loading the service's bulk application state.
        // Retain failed work for retry, just as for normal application work.
        if self.has_metadata() {
            let mut active = self
                .metadata_active
                .lock()
                .map_err(|_| Error::new("metadata execution poisoned"))?;
            if active.is_none() {
                *active = self
                    .metadata_queue
                    .lock()
                    .map_err(|_| Error::new("metadata queue poisoned"))?
                    .pending
                    .take();
            }
            if let Some(PendingWork::Apply(work)) = active.as_ref() {
                if self.apply_metadata(work)? {
                    self.metadata_generation.store(work.generation, Ordering::SeqCst);
                    *active = None;
                }
            }
        }
        let mut active =
            self.active_work.lock().map_err(|_| Error::new("active execution poisoned"))?;
        if active.is_none() {
            *active = self
                .work_queue
                .lock()
                .map_err(|_| Error::new("application queue poisoned"))?
                .pending
                .take();
        }
        let Some(work) = active.as_ref() else {
            return Ok(());
        };
        let result = (|| -> Result<bool> {
            match work {
                PendingWork::Unload => {
                    let mut cursor = self
                        .execution
                        .lock()
                        .map_err(|_| Error::new("execution cursor poisoned"))?;
                    *self.global_lock.write().map_err(|_| Error::new("global lock poisoned"))? =
                        Self::Application::default();
                    *cursor = crate::execution::Cursor::default();
                    Ok(true)
                }
                PendingWork::Apply(work) => {
                    self.apply_client(work)?;
                    let complete = self
                        .global_lock
                        .read()
                        .map_err(|_| Error::new("global lock poisoned"))?
                        .applied()
                        >= work.commit;
                    if complete {
                        self.application_generation.store(work.generation, Ordering::SeqCst);
                    }
                    Ok(complete)
                }
            }
        })();
        if result? {
            *active = None;
        }
        Ok(())
    }
    fn wake_execution(&self) {
        let _ = self.execution_wake.try_send(());
    }
    fn start_execution(self: &Arc<Self>) -> Result<()> {
        self.execution_started.store(true, Ordering::SeqCst);
        let weak = Arc::downgrade(self);
        let receive = self
            .execution_receive
            .lock()
            .map_err(|_| Error::new("execution receiver poisoned"))?
            .take()
            .ok_or_else(|| Error::new("application worker already started"))?;
        let handle = std::thread::Builder::new()
            .name("raft-application".into())
            .spawn(move || {
                while receive.recv().is_ok() {
                    let Some(host) = weak.upgrade() else {
                        break;
                    };
                    if host.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    loop {
                        if host.execute_pending().is_err() {
                            break;
                        }
                        let active =
                            host.active_work.lock().map(|work| work.is_some()).unwrap_or(false);
                        let queued = host
                            .work_queue
                            .lock()
                            .map(|queue| queue.pending.is_some())
                            .unwrap_or(false);
                        let metadata =
                            host.metadata_active.lock().map(|work| work.is_some()).unwrap_or(false)
                                || host
                                    .metadata_queue
                                    .lock()
                                    .map(|q| q.pending.is_some())
                                    .unwrap_or(false);
                        if !active && !queued && !metadata {
                            break;
                        }
                        if host.snapshot_stopping.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                }
            })
            .context("starting application worker")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }

    fn admit_client(&self, read: bool) -> Result<Admission<'_, Self::Application>> {
        let deadline = Instant::now() + Duration::from_millis(self.options.raft.election_max_ms);
        loop {
            let commit = self.on_raft(move |host, node| {
                if node.role != Role::Leader || node.journal.failed {
                    return Err(Error::new("Raft is not a healthy leader"));
                }
                host.refresh_application(node)?;
                Ok((node.can_admit_with_policy(
                    lease::now()?,
                    read,
                    host.options.allow_stale_reads,
                ) && host.application_generation.load(Ordering::SeqCst)
                    == node.state().generation)
                    .then_some(node.state().commit))
            })?;
            if let Some(commit) = commit {
                let guard = self.global_lock.try_read().ok();
                if guard.as_ref().is_some_and(|data| data.applied() >= commit) {
                    let valid = self.on_raft(move |host, node| {
                        Ok(node.can_admit_with_policy(
                            lease::now()?,
                            read,
                            host.options.allow_stale_reads,
                        ))
                    })?;
                    if valid {
                        return Ok(Admission { history: if read { guard } else { None } });
                    }
                }
            }
            if !self.execution_started.load(Ordering::SeqCst) {
                self.execute_pending()?;
            }
            if Instant::now() >= deadline {
                return Err(Error::new("Raft quorum or application catch-up timeout"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn wait_committed(&self, index: u64) -> Result<()> {
        let deadline = Instant::now()
            + Duration::from_millis(self.options.raft.election_max_ms.saturating_mul(2));
        loop {
            let committed = self.on_raft(move |host, node| {
                if node.journal.failed {
                    return Err(Error::new("Raft journal failed"));
                }
                if node.role != Role::Leader {
                    return Err(Error::new(
                        "Raft leadership changed; request outcome may be unknown",
                    ));
                }
                host.refresh_application(node)?;
                Ok(node.state().commit >= index
                    && host.application_generation.load(Ordering::SeqCst)
                        == node.state().generation)
            })?;
            if committed && self.global_lock.try_read().is_ok_and(|data| data.applied() >= index) {
                return Ok(());
            }
            if !self.execution_started.load(Ordering::SeqCst) {
                self.execute_pending()?;
            }
            if let Some(error) = self
                .execution_error
                .lock()
                .map_err(|_| Error::new("execution error lock poisoned"))?
                .as_ref()
            {
                return Err(Error::new(error.clone()));
            }
            if Instant::now() >= deadline {
                return Err(Error::new("Raft quorum timeout; request outcome may be unknown"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn raft_status(&self) -> String {
        self.on_raft(|host, node| Ok(format!(
                "raft_role {:?}\nraft_term {}\nraft_leader {}\nraft_last {}\nraft_commit {}\nraft_failed {}\nraft_applying {}\nraft_member_id {}\nraft_member_slot {}\nraft_member_incarnation {}\nraft_lost_state {}\nraft_repairing {}",
                node.role,
                node.state().term,
                node.leader.map(|id| id.to_string()).unwrap_or_else(|| "none".into()),
                node.state().last(),
                node.state().commit,
                node.journal.failed,
                node.role == Role::Leader || host.applying.load(Ordering::SeqCst),
                node.journal.identity.current,
                if node.id == UNASSIGNED {
                    "none".into()
                } else {
                    (node.id % raft::INCARNATION).to_string()
                },
                if node.id == UNASSIGNED {
                    "none".into()
                } else {
                    (node.id / raft::INCARNATION).to_string()
                },
                node.journal.pending_replacement().is_some(),
                node.repairing,
            ))).unwrap_or_else(|_| "raft_failed true".into())
    }
    fn member_address(&self, peer: &str) -> Result<std::net::SocketAddr> {
        if let Some(address) = self.registered_endpoint(peer) {
            return Ok(address);
        }
        if let Some((registrar, address)) = &self.runtime.registrar {
            if peer == registrar {
                return Ok(*address);
            }
            if registrar != &self.runtime.name {
                let mut payload = common::StructWriter::new();
                payload.text(peer);
                let response = server::ask(
                    *address,
                    &Req::new(Op::Lookup).structure(payload),
                    Duration::from_millis(self.options.raft.request_timeout_ms),
                    self.runtime
                        .messaging
                        .as_ref()
                        .ok_or_else(|| Error::new("no endpoint discovery"))?,
                );
                if let Ok(response) = response {
                    if response.status == Status::Ok {
                        return response
                            .message()
                            .ok_or_else(|| Error::new("missing member endpoint"))?
                            .parse()
                            .map_err(|_| Error::new("invalid member endpoint"));
                    }
                }
            }
        }
        self.runtime.seeds.get(peer).copied().ok_or_else(|| Error::new("member has not registered"))
    }
    fn send_raft(&self, peer: &MemberId, message: Message) -> Result<Resp> {
        let peer = *peer;
        let name = self.on_raft(move |host, node| {
            node.state()
                .membership()
                .identities
                .get(&peer)
                .or_else(|| node.state().contacts.get(&peer))
                .map(|m| m.process.clone())
                .or_else(|| host.runtime.founders.get(&peer).cloned())
                .ok_or_else(|| Error::new("unknown member index"))
        })?;
        self.send_named_to(&name, Some(peer), message)
    }
    fn send_named(&self, name: &str, message: Message) -> Result<Resp> {
        self.send_named_to(name, None, message)
    }
    fn send_named_to(
        &self,
        name: &str,
        target: Option<MemberId>,
        message: Message,
    ) -> Result<Resp> {
        let timeout = if matches!(message, Message::Forward { .. }) {
            self.options
                .raft
                .election_max_ms
                .saturating_mul(2)
                .saturating_add(self.options.raft.request_timeout_ms)
        } else {
            self.options.raft.request_timeout_ms
        };
        let applying = Some(
            self.placement
                .lock()
                .map_err(|_| Error::new("placement lock poisoned"))?
                .selected
                .clone(),
        );
        let (member, retirement) = self.on_raft(|_, node| {
            Ok((
                if node.id == UNASSIGNED {
                    Sender::Applicant(node.journal.identity.current.clone())
                } else {
                    Sender::Slot(node.id)
                },
                node.journal.pending_replacement(),
            ))
        })?;
        let request = Req::new(Op::Raft).text(&self.runtime.name).bytes(
            crate::codec::encode_message(&Packet {
                member,
                target,
                retirement,
                applying,
                ready: self.ready.load(Ordering::SeqCst),
                message,
            })?,
        );
        self.transport
            .as_ref()
            .ok_or_else(|| Error::new("no Raft transport"))?
            .get(name, || self.member_address(name))?
            .call_timeout(&request, Duration::from_millis(timeout))
    }
    fn forward(&self, request: &Req) -> Option<Resp> {
        let (local, target) = match self.on_raft(|_, node| {
            Ok((
                node.role == Role::Leader,
                node.leader.filter(|id| node.state().membership().members().contains(id)),
            ))
        }) {
            Ok(value) => value,
            Err(e) => return Some(error(e)),
        };
        if local {
            return None;
        }
        let Some(id) = target else {
            return Some(error("Raft has no known Raft leader"));
        };
        let [arg] = request.args() else {
            return Some(error("expected structured Raft request"));
        };
        if arg.is_group() {
            return Some(error("expected structured Raft request"));
        }
        Some(
            self.send_raft(
                &id,
                Message::Forward {
                    op: request.op().code(),
                    payload: arg.bytes().clone().into(),
                    depth: arg.depth,
                },
            )
            .unwrap_or_else(error),
        )
    }
    fn raft_rpc(&self, process: &Process, request: &Req) -> Resp {
        let result = (|| -> Result<Resp> {
            let (sender, bytes) = raft_request_payload(request)?;
            let packet: Packet = crate::codec::decode_message(&bytes)?;
            let (response, deferred) = self.on_raft(move |host, node| {
                let mut deferred = None;
                let response = host.raft_packet(node, &sender, packet, &mut deferred)?;
                Ok((response, deferred))
            })?;
            match deferred {
                Some(Deferred::Message(Message::Client(action))) => {
                    self.receive_client(process, action)
                }
                Some(Deferred::Message(Message::Forward { op, payload, depth })) => {
                    let op = Op::from_code(op)?;
                    Ok(self.dispatch_forwarded(
                        process,
                        &Req { id: 0, payload: Arg::new(op, payload.into(), depth) },
                    ))
                }
                Some(Deferred::Read(read)) => Ok(Resp::bytes(read.wait()?)),
                None => Ok(response),
                _ => Err(Error::new("invalid deferred Raft work")),
            }
        })();
        let sender = raft_request_payload(request).map(|(sender, _)| sender).unwrap_or_default();
        match result {
            Ok(response) => {
                if response.status != Status::Ok {
                    logn!(
                        "Raft client RPC rejected",
                        "sender {},status {:?},reason {:?}",
                        sender,
                        response.status,
                        response.message()
                    );
                }
                response
            }
            Err(reason) => {
                logn!("Raft RPC rejected", "sender {},reason {}", sender, reason);
                error(reason)
            }
        }
    }
    fn raft_packet(
        &self,
        node: &mut Node,
        sender: &String,
        packet: Packet,
        deferred: &mut Option<Deferred>,
    ) -> Result<Resp> {
        let sender = {
            let enrolled = self.runtime.admitted.contains(sender)
                || self.runtime.founders.values().any(|name| name == sender);
            if !enrolled {
                return Err(Error::new("Raft sender is not admitted"));
            }
            if let Some(retired) = &packet.retirement {
                if &retired.old.process != sender
                    || &retired.member.process != sender
                    || packet.member != Sender::Slot(retired.next)
                {
                    return Err(Error::new("retirement sender mismatch"));
                }
                node.observe_retirement(retired.clone())?;
            }
            let membership = node.state().membership();
            if let Some(target) = packet.target {
                if target != node.id {
                    if let Some(retired) =
                        node.journal.pending_replacement().filter(|r| r.previous == target)
                    {
                        if !matches!(packet.message, Message::Consensus(_)) {
                            return Err(Error::new("request addressed to retired member"));
                        }
                        // This is no vote/ack from the retired member. Do not execute
                        // the old member's RPC using the replacement's identity.
                        return Ok(Resp::bytes(crate::codec::encode_message(&ConsensusReply {
                            member: Sender::Slot(retired.next),
                            retirement: Some(retired),
                            answer: Answer { term: node.state().term, success: false, matched: 0 },
                            ready: self.ready.load(Ordering::SeqCst),
                        })?));
                    }
                    // A slotless ordinary applicant can receive its first membership
                    // entries. Its full random identity is checked in the reply.
                    if node.id != UNASSIGNED {
                        return Err(Error::new("RPC addressed to another member incarnation"));
                    }
                }
            }
            if matches!(packet.message, Message::Identity) {
                return Ok(Resp::bytes(crate::codec::encode_message(&Member {
                    identity: node.journal.identity.current.clone(),
                    process: self.runtime.name.clone(),
                })?));
            }
            if let Message::Bootstrap { founders, membership } = &packet.message {
                membership.validate()?;
                if founders
                    .identities
                    .iter()
                    .any(|(slot, member)| membership.identities.get(slot) != Some(member))
                {
                    return Err(Error::new("bootstrap membership changed founding identities"));
                }
                let proposed_sender = membership
                    .identities
                    .iter()
                    .find(|(slot, m)| &m.process == sender && membership.voters().contains(slot));
                let valid = proposed_sender.is_some_and(|(slot, m)| match &packet.member {
                    Sender::Slot(sent) => sent == slot,
                    Sender::Applicant(id) => id == &m.identity,
                });
                if !valid {
                    return Err(Error::new("bootstrap sender identity mismatch"));
                }

                let names: BTreeMap<_, _> = founders
                    .identities
                    .iter()
                    .map(|(slot, m)| (*slot, m.process.clone()))
                    .collect();
                if founders.new.is_some()
                    || names != self.runtime.founders
                    || founders.old != self.runtime.members
                {
                    return Err(Error::new(
                        "bootstrap participant/slot list differs from configuration",
                    ));
                }
                if founders.identities.values().any(|m| {
                    m.process == self.runtime.name && m.identity != node.journal.identity.current
                }) && node.state().founders.as_ref() != Some(founders)
                {
                    return Err(Error::new(
                        "bootstrap does not name this process's persistent identity",
                    ));
                }
                node.initialize_founders(founders.clone())?;
                node.learn_contacts(membership.identities.clone())?;
                return Ok(Resp::ok());
            }
            let index = match &packet.member {
                Sender::Slot(slot) => {
                    if membership
                        .identities
                        .get(slot)
                        .or_else(|| node.state().contacts.get(slot))
                        .is_none_or(|m| &m.process != sender)
                    {
                        return Err(Error::new("Raft sender slot does not match routing label"));
                    }
                    *slot
                }
                Sender::Applicant(identity) => membership.slot(identity).unwrap_or(UNASSIGNED),
            };
            if (index == UNASSIGNED
                && !matches!(
                    packet.message,
                    Message::Checkpoint { .. }
                        | Message::Journal { .. }
                        | Message::JournalTip { .. }
                ))
                || (index != UNASSIGNED
                    && membership
                        .identities
                        .get(&index)
                        .or_else(|| node.state().contacts.get(&index))
                        .is_none_or(|m| &m.process != sender))
            {
                return Err(Error::new("Raft applicant has no assigned slot"));
            }
            let admitted = if matches!(
                packet.message,
                Message::Consensus(_)
                    | Message::Checkpoint { .. }
                    | Message::Journal { .. }
                    | Message::JournalTip { .. }
            ) {
                true
            } else {
                membership.members().contains(&index)
            };
            if !admitted {
                return Err(Error::new("Raft sender is not a member"));
            }
            index
        };
        let sender = &sender;
        if !packet.ready && *sender != UNASSIGNED {
            self.readiness_requests
                .lock()
                .map_err(|_| Error::new("unlock request lock poisoned"))?
                .insert(*sender);
        }
        match packet.message {
            Message::Identity | Message::Bootstrap { .. } => {
                return Err(Error::new("identity query passed its admission handler"));
            }
            Message::CheckpointStart => {
                if node.role != Role::Leader || sender == &node.id {
                    return Err(Error::new(
                        "checkpoint announcements require the primary and a secondary writer",
                    ));
                }
                if !packet.ready
                    || !self
                        .placement
                        .lock()
                        .map_err(|_| Error::new("placement lock poisoned"))?
                        .selected
                        .contains(sender)
                {
                    return Err(Error::new("checkpoint writer is not an unlocked applying member"));
                }
                self.refresh_application(&node)?;
                let lease = self
                    .checkpoint_lease
                    .lock()
                    .map_err(|_| Error::new("checkpoint lease lock poisoned"))?;
                if lease
                    .holder(
                        Instant::now(),
                        Duration::from_secs(self.options.raft.checkpoint_lease_secs),
                    )
                    .is_none()
                    && node.state().last() == node.state().commit
                {
                    node.propose(Command::CheckpointStart(*sender))?;
                }
                Ok(Resp::ok())
            }
            Message::JournalTip { index, term } => {
                if node.role != Role::Leader || node.state().term != term {
                    return Err(Error::new("journal history proof requires the current primary"));
                }
                let read = node
                    .journal
                    .read_history(move |history| binary(&history.committed_tip(index)?))?;
                *deferred = Some(Deferred::Read(read));
                Ok(Resp::ok())
            }
            Message::Journal { after, through, primary_fallback } => {
                if node.role == Role::Stopped
                    || node.repairing
                    || node.journal.failed
                    || (node.role == Role::Leader && !primary_fallback)
                {
                    return Err(Error::new("journal source unavailable"));
                }
                let read = node.journal.read_history(move |history| {
                    binary(&history.committed_suffix(after, through)?)
                })?;
                *deferred = Some(Deferred::Read(read));
                Ok(Resp::ok())
            }
            Message::Checkpoint { after, primary_fallback } => {
                let snapshot = self.checkpoint_from(node, after, primary_fallback)?;
                Ok(Resp::bytes(crate::codec::encode_message(&snapshot)?))
            }
            Message::Consensus(rpc) => {
                let was_repairing = node.repairing;
                let recent_leader = node.election_quiet(
                    Instant::now(),
                    Duration::from_millis(self.options.raft.election_min_ms),
                );
                let copy_first = match &rpc {
                    Rpc::Append { commit, .. }
                        if node.journal.pending_replacement().is_some()
                            && *commit > node.state().commit =>
                    {
                        Some(*commit)
                    }
                    _ => None,
                };
                let answer = if let Some(commit) = copy_first {
                    let contact = Rpc::Append {
                        term: rpc.term(),
                        previous: node.state().last(),
                        previous_term: node.state().term_at(node.state().last()).unwrap_or(0),
                        previous_scn: node
                            .state()
                            .scn_at(node.state().last())
                            .unwrap_or(common::scn::Scn::new(0)),
                        entries: vec![],
                        commit: node.state().commit,
                    };
                    let mut answer = node.receive(sender, &contact)?;
                    if rpc.term() == node.state().term && node.leader == Some(*sender) {
                        self.checkpoint_needed.fetch_max(commit, Ordering::SeqCst);
                    }
                    answer.success = false;
                    answer
                } else if matches!(rpc, Rpc::Vote { .. }) && recent_leader {
                    Answer { term: node.state().term, success: false, matched: 0 }
                } else {
                    node.receive(sender, &rpc)?
                };
                if node.repairing {
                    if !was_repairing {
                        logn!(
                            "Raft committed history conflict; rebuilding follower",
                            "member {},primary {:?},commit {}",
                            node.id,
                            node.leader,
                            node.state().commit
                        );
                        node.journal.require_history_repair()?;
                    }
                    if let Rpc::Append { commit, .. } = &rpc {
                        self.checkpoint_needed.fetch_max(*commit, Ordering::SeqCst);
                    }
                }
                if !matches!(rpc, Rpc::Vote { .. })
                    && rpc.term() == node.state().term
                    && node.leader.as_ref() == Some(sender)
                {
                    self.accept_placement(packet.applying.as_ref(), &node)?;
                    if let Rpc::Checkpoint { index, .. } = rpc {
                        if !answer.success {
                            self.checkpoint_needed.fetch_max(index, Ordering::SeqCst);
                        }
                    }
                }
                self.refresh_application(&node)?;
                Ok(Resp::bytes(crate::codec::encode_message(&ConsensusReply {
                    member: if node.id == UNASSIGNED {
                        Sender::Applicant(node.journal.identity.current.clone())
                    } else {
                        Sender::Slot(node.id)
                    },
                    retirement: None,
                    answer,
                    ready: self.ready.load(Ordering::SeqCst),
                })?))
            }
            Message::Client(action) => {
                *deferred = Some(Deferred::Message(Message::Client(action)));
                Ok(Resp::ok())
            }
            Message::Forward { op, payload, depth } => {
                let op = Op::from_code(op)?;
                if node.role != Role::Leader {
                    return Err(Error::new("forward target is no longer Raft leader"));
                }
                let request = Req { id: 0, payload: Arg::new(op, payload.clone().into(), depth) };
                if !self.forwardable(op) {
                    return Err(Error::new("operation cannot be forwarded through Raft"));
                }
                *deferred = Some(Deferred::Message(Message::Forward {
                    op: request.op().code(),
                    payload,
                    depth,
                }));
                Ok(Resp::ok())
            }
        }
    }
    fn change_members(&self, request: &Req) -> Resp {
        let result = (|| -> Result<()> {
            let (mut payload, rest) =
                structure(&request.args(), Op::RaftMembers, "a Raft membership request")?;
            let count = usize::try_from(payload.number_u64()?)
                .map_err(|_| Error::new("membership count overflow"))?;
            if count > payload.remaining() {
                return Err(Error::new("invalid membership count"));
            }
            let proposed: Vec<String> =
                (0..count).map(|_| payload.text()).collect::<Result<_>>()?;
            let members: BTreeSet<String> = proposed.iter().cloned().collect();
            payload.done()?;
            if !rest.is_empty()
                || members.is_empty()
                || members.len() != proposed.len()
                || (members.len() > 1 && self.options.raft.apply_members < 2)
            {
                return Err(Error::new("invalid Raft membership request"));
            }
            // Machine admission is independent of consensus membership. Every proposed
            // member must already be enrolled; this is not user authorization.
            if let Some(auth) =
                self.runtime.messaging.as_ref().and_then(|m| m.authentication.as_ref())
            {
                for id in proposed.iter() {
                    if !auth.members.iter().any(|m| m.services.contains(id)) {
                        return Err(Error::new("new Raft member is not enrolled"));
                    }
                }
            }
            if self.runtime.messaging.is_some()
                && proposed.iter().any(|id| !self.runtime.admitted.contains(id))
            {
                return Err(Error::new("new member is not a Raft in the roster"));
            }
            if !self.runtime.ignore_ring_identity
                && self.runtime.messaging.as_ref().is_some_and(|m| {
                    proposed.iter().any(|name| {
                        m.ring_identities
                            .get(name)
                            .is_none_or(|id| id.to_string() != self.runtime.ring)
                    })
                })
            {
                return Err(Error::new("proposed member belongs to a different ring"));
            }
            let mut applicants = Vec::new();
            for name in &proposed {
                let name_owned = name.clone();
                let known = self.on_raft(move |_, node| {
                    let membership = node.state().membership();
                    Ok(membership
                        .members()
                        .iter()
                        .filter_map(|slot| membership.identities.get(slot))
                        .find(|m| m.process == name_owned)
                        .cloned())
                })?;
                let observed = self.query_member(name);
                let unchanged = known
                    .as_ref()
                    .is_some_and(|old| observed.as_ref().map_or(true, |member| member == old));
                let member = if unchanged {
                    known.unwrap()
                } else {
                    let member = observed?;
                    let founders = self.on_raft(|_, node| {
                        node.state()
                            .founders
                            .clone()
                            .ok_or_else(|| Error::new("ring is not bootstrapped"))
                    })?;
                    let response = self.send_named(name, self.bootstrap_message(founders)?)?;
                    if response.status != Status::Ok {
                        return Err(Error::new("applicant cannot initialize founding history"));
                    }
                    member
                };
                applicants.push(member);
            }
            let _proposal =
                self.proposing.lock().map_err(|_| Error::new("proposal lock poisoned"))?;
            let (index, target) = self.on_raft(move |_, node| {
                let old = node.state().membership();
                if old.new.is_some() || node.state().commit != node.state().last() {
                    return Err(Error::new("Raft configuration or proposal already pending"));
                }
                let mut assigned = old.clone();
                let mut members = Members::new();
                for member in applicants {
                    members.insert(assigned.assign(member)?);
                }
                let identities = assigned.identities;
                let target = Membership {
                    retired: old.retired.clone(),
                    identities: identities.clone(),
                    old: members.clone(),
                    new: None,
                };
                target.validate()?;
                let index = node.propose(Command::Members(Membership {
                    retired: old.retired.clone(),
                    identities,
                    old: old.old,
                    new: Some(members),
                }))?;
                Ok((index, target))
            })?;
            let deadline = Instant::now()
                + Duration::from_millis(self.options.raft.election_max_ms.saturating_mul(2));
            loop {
                let target = target.clone();
                if self.on_raft(move |_, node| {
                    if node.state().commit >= index && node.state().committed_membership() == target
                    {
                        return Ok(true);
                    }
                    if node.role != Role::Leader {
                        return Err(Error::new("leadership changed during membership change"));
                    }
                    Ok(false)
                })? {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(Error::new("membership change pending; quorum timeout"));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        })();
        result.map(|()| Resp::ok()).unwrap_or_else(error)
    }
    fn start_raft(self: &Arc<Self>) -> Result<()> {
        let erased: Arc<dyn std::any::Any + Send + Sync> = self.clone();
        self.host.set(Arc::downgrade(&erased)).map_err(|_| Error::new("Raft already started"))?;
        let weak = Arc::downgrade(self);
        let completion_host = weak.clone();
        let fence = self.journal.initial_call(move |node| {
            node.journal.start_io(move || {
                if let Some(host) = completion_host.upgrade() {
                    host.journal.wake();
                }
            })?;
            node.journal.fence().ok_or_else(|| Error::new("missing I/O completion fence"))
        })?;
        self.io_fence.set(fence).map_err(|_| Error::new("I/O completion already installed"))?;
        let mut contact = 0;
        let random = u64::from_le_bytes(random_bytes()?[..8].try_into().unwrap());
        let mut deadline = Instant::now() + election_delay(&self.options.raft, random, true, false);
        let mut peers: BTreeMap<MemberId, Arc<AtomicBool>> = BTreeMap::new();
        let handle = self.journal.start(move |node| {
            let Some(host) = weak.upgrade() else {
                return false;
            };
            if host.snapshot_stopping.load(Ordering::SeqCst) {
                return false;
            }
            let result = (|| -> Result<()> {
                if node.journal.failed {
                    return Ok(());
                }
                node.journal.poll_io()?;
                // Apply the current penalty to the base deadline: starting or finishing
                // a checkpoint between heartbeats must affect election preference too.
                let c = &host.options.raft;
                let penalty = election_delay(
                    c,
                    0,
                    host.applying.load(Ordering::SeqCst),
                    host.producing_snapshot.load(Ordering::SeqCst),
                ) - election_delay(c, 0, true, false);
                let recovery_penalty =
                    replacement_delay(c, node.journal.pending_replacement().is_some());
                let eligible = deadline + penalty + recovery_penalty;
                if eligible <= Instant::now() || node.contact != contact {
                    if eligible <= Instant::now()
                        && node.contact == contact
                        && node.role != Role::Leader
                    {
                        node.campaign()?;
                    }
                    let random = u64::from_le_bytes(
                        random_bytes()?[..8]
                            .try_into()
                            .map_err(|_| Error::new("random timeout"))?,
                    );
                    let c = &host.options.raft;
                    deadline = Instant::now() + election_delay(c, random, true, false);
                    contact = node.contact;
                }
                if node.role == Role::Leader {
                    host.placement
                        .lock()
                        .map_err(|_| Error::new("placement lock poisoned"))?
                        .select(
                            &node.id,
                            &node.state().membership().voters(),
                            host.options.raft.apply_members,
                            Instant::now(),
                            Duration::from_millis(host.options.raft.cold_election_delay_ms),
                        );
                }
                node.advance()?;
                node.accept_replacements()?;
                host.prepare_primary(node)?;
                host.refresh_application(&node)?;
                let members = node.state().membership().participants();
                let local_id = node.id;
                peers.retain(|id, cancelled| {
                    let keep = members.contains(id) && *id != local_id;
                    if !keep {
                        cancelled.store(true, Ordering::SeqCst);
                    }
                    keep
                });
                for id in members.iter() {
                    if id != &local_id
                        && !peers.contains_key(id)
                        && host.runtime.messaging.is_some()
                    {
                        let owned = host
                            .host
                            .get()
                            .and_then(|w| w.upgrade())
                            .and_then(|v| v.downcast::<Self>().ok())
                            .ok_or_else(|| Error::new("Raft host stopped"))?;
                        let cancelled = Arc::new(AtomicBool::new(false));
                        owned.start_peer(*id, cancelled.clone())?;
                        peers.insert(*id, cancelled);
                    }
                }
                Ok(())
            })();
            if let Err(e) = result {
                logn!("Raft maintenance failed", "{e}");
            }
            true
        })?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        self.start_execution()?;
        self.start_bootstrap()?;
        self.start_checkpoints()?;
        Ok(())
    }
    fn start_peer(self: &Arc<Self>, peer: MemberId, cancelled: Arc<AtomicBool>) -> Result<()> {
        let weak = Arc::downgrade(self);
        let handle = std::thread::Builder::new()
            .name(format!("raft-{peer}"))
            .spawn(move || {
                let mut unlocked = false;
                let mut last_error: Option<String> = None;
                loop {
                    let Some(ks) = weak.upgrade() else {
                        break;
                    };
                    if ks.snapshot_stopping.load(Ordering::SeqCst)
                        || cancelled.load(Ordering::SeqCst)
                    {
                        break;
                    }
                    let mut contacted = false;
                    let result = (|| -> Result<()> {
                        let plan = ks.on_raft(move |_, node| {
                            let (contact, setup) =
                                peer_eligibility(&node.state().membership(), node.id, peer);
                            if node.journal.failed || !contact {
                                return Ok(None);
                            }
                            Ok(Some((node.request_reference(&peer), setup)))
                        })?;
                        let Some((request, setup)) = plan else {
                            return Ok(());
                        };
                        let requested = ks
                            .readiness_requests
                            .lock()
                            .map_err(|_| Error::new("unlock request lock poisoned"))?
                            .contains(&peer);
                        if setup && (!unlocked || requested) {
                            if let Some(action) = ks.peer_message() {
                                unlocked = ks.send_raft(&peer, Message::Client(action))?.status
                                    == Status::Ok;
                                contacted |= unlocked;
                                if unlocked {
                                    ks.readiness_requests
                                        .lock()
                                        .map_err(|_| Error::new("unlock request lock poisoned"))?
                                        .remove(&peer);
                                }
                            }
                        }
                        if let Some(request) = request {
                            let sent = lease::now()?;
                            let response =
                                ks.send_raft(&peer, Message::Consensus(request.clone()))?;
                            if response.status != Status::Ok {
                                return Err(Error::new(format!(
                                    "peer {peer} rejected Raft RPC: {:?}",
                                    response.message()
                                )));
                            }
                            let (mut body, rest) =
                                structure(response.args(), Op::BytesResp, "Raft response")?;
                            let bytes = body.bytes()?;
                            body.done()?;
                            if !rest.is_empty() {
                                return Err(Error::new("trailing Raft response"));
                            }
                            let response: ConsensusReply = parse(&bytes)?;
                            contacted = true;
                            unlocked = response.ready;
                            ks.on_raft(move |host, node| {
                                if let Some(retired) = response.retirement {
                                    if retired.previous != peer
                                        || response.member != Sender::Slot(retired.next)
                                    {
                                        return Err(Error::new(
                                            "retirement reply does not match requested member",
                                        ));
                                    }
                                    node.observe_retirement(retired)?;
                                    return Ok(());
                                }
                                let identity_matches = match &response.member {
                                    Sender::Slot(slot) => *slot == peer,
                                    Sender::Applicant(identity) => node
                                        .state()
                                        .membership()
                                        .identities
                                        .get(&peer)
                                        .is_some_and(|m| &m.identity == identity),
                                };
                                if !identity_matches {
                                    return Err(Error::new(
                                        "Raft reply came from the wrong member",
                                    ));
                                }

                                host.placement
                                    .lock()
                                    .map_err(|_| Error::new("placement lock poisoned"))?
                                    .acknowledge(&peer, Instant::now());

                                node.response_sent(&peer, &request, &response.answer, sent)?;
                                host.refresh_application(&node)?;
                                Ok(())
                            })?;
                        }
                        Ok(())
                    })();
                    match result {
                        Err(error) => {
                            unlocked = false;
                            let message = error.to_string();
                            if last_error.as_ref() != Some(&message) {
                                logn!("Raft peer failed", "peer {},error {}", peer, message);
                                last_error = Some(message);
                            }
                        }
                        Ok(()) if contacted && last_error.take().is_some() => {
                            logn!("Raft peer recovered", "peer {}", peer);
                        }
                        Ok(()) => {}
                    }
                    let interval = Duration::from_millis(ks.options.raft.heartbeat_ms);
                    drop(ks);
                    std::thread::park_timeout(interval);
                }
            })
            .context("starting Raft peer worker")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }
}

crate::wire_struct!(Packet, RaftPacket, {member,target,retirement,ready,applying,message});

crate::wire_struct!(ConsensusReply, RaftConsensusReply, {member,retirement,answer,ready});

impl crate::codec::Wire for Message {
    fn write(&self, w: &mut common::StructWriter) {
        match self {
            Self::Identity => w.nested(common::Op::RaftMessageIdentity, |_| {}),
            Self::Bootstrap { founders, membership } => {
                w.nested(common::Op::RaftMessageBootstrap, |w| {
                    founders.write(w);
                    membership.write(w);
                })
            }
            Self::Consensus(value) => w.nested(common::Op::RaftMessageConsensus, |w| {
                value.write(w);
            }),
            Self::Checkpoint { after, primary_fallback } => {
                w.nested(common::Op::RaftMessageCheckpoint, |w| {
                    after.write(w);
                    primary_fallback.write(w);
                })
            }
            Self::Journal { after, through, primary_fallback } => {
                w.nested(common::Op::RaftMessageJournal, |w| {
                    after.write(w);
                    through.write(w);
                    primary_fallback.write(w);
                })
            }
            Self::JournalTip { index, term } => w.nested(Op::RaftMessageJournalTip, |w| {
                index.write(w);
                term.write(w);
            }),
            Self::CheckpointStart => w.nested(common::Op::RaftMessageCheckpointStart, |_| {}),
            Self::Client(value) => w.nested(common::Op::RaftMessageClient, |w| {
                value.write(w);
            }),
            Self::Forward { op, payload, depth } => w.nested(common::Op::RaftMessageForward, |w| {
                op.write(w);
                payload.write(w);
                depth.write(w);
            }),
        }
    }
    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        use crate::codec::Wire;
        r.nested_any(|op, r| match op {
            common::Op::RaftMessageIdentity => Ok(Self::Identity),
            common::Op::RaftMessageBootstrap => {
                Ok(Self::Bootstrap { founders: Wire::read(r)?, membership: Wire::read(r)? })
            }
            common::Op::RaftMessageConsensus => Ok(Self::Consensus(Wire::read(r)?)),
            common::Op::RaftMessageCheckpoint => {
                Ok(Self::Checkpoint { after: Wire::read(r)?, primary_fallback: Wire::read(r)? })
            }
            common::Op::RaftMessageJournal => Ok(Self::Journal {
                after: Wire::read(r)?,
                through: Wire::read(r)?,
                primary_fallback: Wire::read(r)?,
            }),
            Op::RaftMessageJournalTip => {
                Ok(Self::JournalTip { index: Wire::read(r)?, term: Wire::read(r)? })
            }
            common::Op::RaftMessageCheckpointStart => Ok(Self::CheckpointStart),
            common::Op::RaftMessageClient => Ok(Self::Client(Wire::read(r)?)),
            common::Op::RaftMessageForward => Ok(Self::Forward {
                op: Wire::read(r)?,
                payload: Wire::read(r)?,
                depth: Wire::read(r)?,
            }),
            _ => Err(common::Error::new("unexpected Message tag")),
        })
    }
}

impl crate::codec::Wire for Sender {
    fn write(&self, w: &mut common::StructWriter) {
        match self {
            Self::Slot(value) => w.nested(common::Op::RaftSenderSlot, |w| {
                value.write(w);
            }),
            Self::Applicant(value) => w.nested(common::Op::RaftSenderApplicant, |w| {
                value.write(w);
            }),
        }
    }
    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        use crate::codec::Wire;
        r.nested_any(|op, r| match op {
            common::Op::RaftSenderSlot => Ok(Self::Slot(Wire::read(r)?)),
            common::Op::RaftSenderApplicant => Ok(Self::Applicant(Wire::read(r)?)),
            _ => Err(common::Error::new("unexpected Sender tag")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retired_and_inactive_peers_do_not_receive_client_setup() {
        let mut membership =
            State::initial("01".repeat(64), raft::tests::members(3), raft::tests::names())
                .unwrap()
                .membership();
        let old = membership.identities[&1].clone();
        let next = raft::INCARNATION + 1;
        let member = Member {
            identity: membership.identities[&10].identity.clone(),
            process: old.process.clone(),
        };
        membership.identities.insert(next, member.clone());
        membership.retired.insert(1, Retirement { previous: 1, next, old, member });
        let retired_contact = peer_eligibility(&membership, 0, 1);
        let replacement = peer_eligibility(&membership, 0, next);
        let myself = peer_eligibility(&membership, 0, 0);
        assert_eq!(retired_contact, (true, false));
        assert_eq!(replacement, (true, true));
        assert_eq!(myself, (false, false));
        membership.old = Members::from([0, 2, next]);
        let removed = peer_eligibility(&membership, 0, 1);
        assert_eq!(removed, (false, false));
    }
    #[test]
    fn recovery_tries_all_secondaries_before_primary() {
        let peers = raft::Members::from([0, 1, 2, 3]);
        let sources = recovery_sources(peers.clone(), 1, Some(0));
        assert_eq!(sources, vec![(2, false), (3, false), (0, true)]);
        let own_primary = recovery_sources(peers, 0, Some(0));
        assert_eq!(own_primary, vec![(1, false), (2, false), (3, false)]);
    }
    #[test]
    fn replacement_delay_is_constant_per_campaign_and_clears_on_acceptance() {
        let config = RaftConfig::default();
        for random in [0, 149, 9876] {
            let ordinary = election_delay(&config, random, true, false);
            let recovering = ordinary + replacement_delay(&config, true);
            assert_eq!(recovering - ordinary, Duration::from_millis(200));
            let accepted = ordinary + replacement_delay(&config, false);
            assert_eq!(accepted, ordinary);
        }
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
    fn clients_wait_off_the_raft_worker_including_forwarded_requests() {
        let clients: Vec<_> = [Op::SnMasterKey, Op::RotateMasterKey, Op::RaftMembers]
            .iter()
            .copied()
            .filter(|op| !matches!(op, Op::Raft | Op::Unlock | Op::Register | Op::Lookup))
            .collect();
        let all_clients = clients.iter().all(|op| needs_request_worker(&Req::new(*op)));
        let unknown_client = needs_request_worker(&Req::new(Op::BytesResp));

        let registration = needs_request_worker(&Req::new(Op::Register));
        let packet = |message| {
            Req::new(Op::Raft).text("KS0").bytes(
                binary(&Packet {
                    target: None,
                    retirement: None,
                    member: Sender::Slot(0),
                    ready: false,
                    applying: None,
                    message,
                })
                .unwrap(),
            )
        };
        let consensus = needs_request_worker(&packet(Message::Consensus(Rpc::Vote {
            last_scn: common::scn::Scn::new(0),
            term: 1,
            last: 0,
            last_term: 0,
        })));
        let transfer = needs_request_worker(&packet(Message::Client(crate::Action::new(
            Op::Unlock,
            vec![0; 32],
        ))));
        let forwarded = needs_request_worker(&packet(Message::Forward {
            op: Op::SnMasterKey.code(),
            payload: common::MessageBytes::new(),
            depth: 0,
        }));
        assert!(all_clients);
        assert!(unknown_client);

        assert!(!registration);
        assert!(!consensus);
        assert!(transfer);
        assert!(forwarded);
    }
}
