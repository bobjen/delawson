//! Raft transport, election timers, and the boundary between consensus and client execution.
use crate::execution::Application;
use crate::{Action, Answer, Command, Entry, Members, Membership, Node, Role, Rpc, State};
use crate::{Member, MemberId, MemberNames, Retirement, Storage, UNASSIGNED};
use crate::{application, journal, lease, raft, random_bytes};
use common::Arg;
use common::config::{Messaging, RaftConfig};
use common::server::{self, Process};
use common::time::TimeSec;
use common::{Context, logi, logn};
use common::{Error, Result};
use common::{MessagePayload, Op, Req, Resp, Status, structure};
use parking_lot::{Condvar, Mutex as ParkMutex, RwLock, RwLockReadGuard};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn take_callback<T>(slot: &Mutex<Option<T>>) -> Option<T> {
    slot.lock().expect("callback").take()
}

pub struct Options {
    pub raft: RaftConfig,
    pub snapshot_min_log_bytes: u64,
    pub snapshot_interval_secs: u64,
    pub allow_stale_reads: bool,
}
const CLIENT_BATCH: usize = 128;
const CLIENT_INTERVAL: Duration = Duration::ZERO;
const CLIENT_BYTES: usize = 64 * 1024;
// Fixed framing for an encoded Raft Entry containing a Client action list.
const CLIENT_ENTRY_BYTES: usize = 39;
const CLIENT_ACTION_BYTES: usize = 6;
// Applied progress wakes waiters; this only rechecks leadership and failures.
const APPLY_TICK: Duration = Duration::from_millis(100);
struct ApplyWatch {
    applied: Arc<AtomicU64>,
    version: ParkMutex<u64>,
    changed: Condvar,
}
impl ApplyWatch {
    fn new(applied: u64) -> Self {
        Self {
            applied: Arc::new(AtomicU64::new(applied)),
            version: ParkMutex::new(0),
            changed: Condvar::new(),
        }
    }
    fn version(&self) -> u64 {
        *self.version.lock()
    }
    fn publish(&self, applied: u64) {
        self.applied.store(applied, Ordering::Release);
        self.signal();
    }
    fn signal(&self) {
        let mut version = self.version.lock();
        *version = version.wrapping_add(1);
        drop(version);
        self.changed.notify_all();
    }
    fn wait(&self, version: u64, timeout: Duration) {
        let mut current = self.version.lock();
        if *current == version {
            self.changed.wait_for(&mut current, timeout);
        }
    }
}
struct ClientProposal {
    action: Action,
    reply: Box<dyn FnOnce(Result<u64>) + Send>,
}
type AppliedDone = Box<dyn FnOnce(Result<()>) + Send>;
struct AppliedWaiter {
    index: u64,
    deadline: Instant,
    done: AppliedDone,
}
#[derive(Default)]
struct AppliedWaiters {
    waiting: Vec<AppliedWaiter>,
    ready: VecDeque<(AppliedDone, Result<()>)>,
}
pub struct ClientTicket(std::sync::mpsc::Receiver<Result<u64>>);
impl ClientTicket {
    pub fn wait(self) -> Result<u64> {
        self.0.recv().map_err(|_| Error::new("Raft proposal queue stopped"))?
    }
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
    pub(crate) fn enqueue<S: Storage>(
        &mut self,
        node: &Node<S>,
        state: &State,
        enabled: bool,
    ) -> Result<()> {
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
            return Ok(());
        }
        if !self.enabled {
            self.last = state.snapshot.index;
            self.snapshot = None;
            self.pending = None;
        }
        self.enabled = true;
        let snapshot = self.snapshot.get_or_insert_with(|| Arc::new(state.snapshot.clone()));
        if snapshot.index != state.snapshot.index {
            *snapshot = Arc::new(state.snapshot.clone());
            self.last = self.last.max(snapshot.index);
        }
        let queued = match &self.pending {
            Some(PendingWork::Apply(work)) => work.entries.len(),
            _ => 0,
        };
        let room = node.entry_capacity().saturating_sub(queued);
        let start = self.last.saturating_add(1).max(snapshot.index.saturating_add(1));
        let end = state.commit.min(start.saturating_add(room.saturating_sub(1) as u64));
        let mut entries = crate::raft::EntryLog::default();
        if room > 0 && start <= end {
            for index in start..=end {
                let entry = node
                    .shared_entry_in(state, index)
                    .ok_or_else(|| Error::new("committed Raft entry missing from retained log"))?;
                entries.push_shared(entry);
            }
        }
        debug_assert!(entries.iter().all(|entry| entry.is_written() && entry.is_durable()));
        if entries.is_empty() {
            return Ok(());
        }
        let through = entries.last().map_or(self.last, |entry| entry.index);
        if let Some(PendingWork::Apply(work)) = &mut self.pending {
            for entry in entries.shared_iter() {
                work.entries.push_shared(Arc::clone(entry));
            }
            work.entries.retain(|e| e.index > snapshot.index);
            work.snapshot = snapshot.clone();
            work.commit = through;
        } else {
            self.pending = Some(PendingWork::Apply(crate::execution::Work {
                generation: state.generation,
                ring: state.ring.clone(),
                snapshot: snapshot.clone(),
                entries,
                commit: through,
            }));
        }
        self.last = through;
        Ok(())
    }
}
pub enum Deferred {
    Message(Message),
    Read(crate::io::Read<Vec<u8>>),
}
enum TipRead {
    Retained((u64, common::scn::Scn)),
    Disk(crate::io::Read<Option<(u64, common::scn::Scn)>>),
}
enum CheckpointPlan {
    Request(MemberId),
    Produce { state: State, snapshot: raft::Snapshot, mark: journal::CheckpointMark },
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
    client_proposals: Mutex<Vec<ClientProposal>>,
    client_changed: std::sync::Condvar,
    client_last_send: Mutex<Option<Instant>>,
    applied_waiters: Mutex<AppliedWaiters>,
    pub raft_workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    peer_threads: Mutex<BTreeMap<MemberId, std::thread::Thread>>,
    pub global_lock: RwLock<S>,
    pub execution: Mutex<crate::execution::Cursor>,
    metadata_execution: Mutex<crate::execution::Cursor>,
    apply_watch: ApplyWatch,
    metadata_applied: AtomicU64,
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

impl<S: Default + Application> Runtime<S> {
    pub fn new(runtime: Settings, node: Node, options: Options) -> Self {
        Self::with_state(runtime, node, options, S::default())
    }
}

impl<S> Runtime<S> {
    /// Construct a runtime around application state that needs configuration to build.
    pub fn with_state(runtime: Settings, mut node: Node, options: Options, initial: S) -> Self
    where
        S: Application,
    {
        let applied = 0;
        let apply_watch = ApplyWatch::new(applied);
        node.set_applied(Arc::clone(&apply_watch.applied));
        let membership = node.state().membership();
        let placement = application::Placement::new(
            &membership.replacements(&membership.members()),
            options.raft.apply_members,
        );
        let applying = placement.selected.contains(&node.id);
        node.set_applying_state(applying).expect("valid retained log range");
        let transport = runtime.messaging.as_ref().map(|messaging| {
            let mut messaging = messaging.clone();
            messaging.peer_conns = 1;
            common::peer::Peers::new_raft(
                messaging,
                Duration::from_millis(options.raft.request_timeout_ms),
            )
        });
        let (execution_wake, execution_receive) = std::sync::mpsc::sync_channel(1);
        Self {
            execution_wake,
            execution_receive: Mutex::new(Some(execution_receive)),
            applying: AtomicBool::new(applying),
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
            client_proposals: Mutex::new(Vec::new()),
            client_changed: std::sync::Condvar::new(),
            client_last_send: Mutex::new(None),
            applied_waiters: Mutex::new(AppliedWaiters::default()),
            raft_workers: Mutex::new(vec![]),
            peer_threads: Mutex::new(BTreeMap::new()),
            global_lock: RwLock::new(initial),
            execution: Mutex::new(crate::execution::Cursor::default()),
            metadata_execution: Mutex::new(crate::execution::Cursor::default()),
            apply_watch,
            metadata_applied: AtomicU64::new(0),
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
    pub fn applied(&self) -> u64 {
        self.apply_watch.applied.load(Ordering::Acquire)
    }
    fn wake_application(&self) {
        let _ = self.execution_wake.try_send(());
    }
    fn wake_peers(&self) {
        if let Ok(peers) = self.peer_threads.lock() {
            for thread in peers.values() {
                thread.unpark();
            }
        }
    }
    pub fn publish_applied(&self, index: u64) {
        self.apply_watch.publish(index);
        self.journal.wake();
    }
    pub fn metadata_applied(&self) -> u64 {
        self.metadata_applied.load(Ordering::Acquire)
    }
    /// Queue one leader action; proposals join until the time or size cutoff.
    pub fn queue_client(&self, action: Action) -> Result<ClientTicket> {
        let (reply, receive) = std::sync::mpsc::sync_channel(1);
        self.queue_client_async(action, move |result| {
            let _ = reply.send(result);
        });
        Ok(ClientTicket(receive))
    }
    /// Queue a leader action and call back when it has an index, without waiting
    /// on the Raft owner thread.
    pub fn queue_client_async(
        &self,
        action: Action,
        reply: impl FnOnce(Result<u64>) + Send + 'static,
    ) {
        if self.host.get().is_none() {
            let result =
                self.journal.initial_call(|node| node.propose(Command::Client(vec![action])));
            reply(result);
            return;
        }
        let mut pending = match self.client_proposals.lock() {
            Ok(pending) => pending,
            Err(_) => {
                reply(Err(Error::new("client proposal queue poisoned")));
                return;
            }
        };
        if self.snapshot_stopping.load(Ordering::SeqCst) {
            drop(pending);
            reply(Err(Error::new("Raft runtime is stopping")));
            return;
        }
        let wake = pending.is_empty();
        pending.push(ClientProposal { action, reply: Box::new(reply) });
        drop(pending);
        self.client_changed.notify_one();
        if wake {
            self.journal.wake();
        }
    }
    /// Resume `done` on the application thread after this index is applied.
    pub fn when_applied(&self, index: u64, done: impl FnOnce(Result<()>) + Send + 'static) {
        let mut waiters = self.applied_waiters.lock().expect("applied callbacks");
        waiters.waiting.push(AppliedWaiter {
            index,
            deadline: Instant::now() + Duration::from_secs(60),
            done: Box::new(done),
        });
        drop(waiters);
        self.wake_application();
    }
    fn collect_applied(&self, failure: Option<&str>, expire: bool) {
        let applied = self.applied();
        let now = Instant::now();
        let mut waiters = self.applied_waiters.lock().expect("applied callbacks");
        let mut at = 0;
        while at < waiters.waiting.len() {
            let waiter = &waiters.waiting[at];
            let result = if let Some(error) = failure {
                Some(Err(Error::new(error)))
            } else if applied >= waiter.index {
                Some(Ok(()))
            } else if expire && now >= waiter.deadline {
                Some(Err(Error::new("Raft quorum timeout; request outcome may be unknown")))
            } else {
                None
            };
            if let Some(result) = result {
                let waiter = waiters.waiting.swap_remove(at);
                waiters.ready.push_back((waiter.done, result));
            } else {
                at += 1;
            }
        }
        let ready = !waiters.ready.is_empty();
        drop(waiters);
        if ready {
            self.wake_application();
        }
    }
    fn run_applied(&self) {
        let ready = {
            let mut waiters = self.applied_waiters.lock().expect("applied callbacks");
            std::mem::take(&mut waiters.ready)
        };
        for (done, result) in ready {
            done(result);
        }
    }
    pub fn propose_client(&self, action: Action) -> Result<u64> {
        self.queue_client(action)?.wait()
    }
    fn flush_client_proposals(&self, node: &mut Node) -> Result<()> {
        let mut queue = self
            .client_proposals
            .lock()
            .map_err(|_| Error::new("client proposal queue poisoned"))?;
        let trigger = loop {
            if queue.is_empty() || !node.has_entry_room() {
                return Ok(());
            }
            let (_, bytes) = client_batch_shape(&queue, CLIENT_BATCH, CLIENT_BYTES);
            let size_due = bytes > CLIENT_BYTES;
            let last_send = *self
                .client_last_send
                .lock()
                .map_err(|_| Error::new("client send time poisoned"))?;
            let delay = if size_due {
                Duration::ZERO
            } else {
                client_delay(last_send, Instant::now(), CLIENT_INTERVAL)
            };
            if delay.is_zero() {
                break if size_due { "size" } else { "time" };
            }
            queue = self
                .client_changed
                .wait_timeout(queue, delay)
                .map_err(|_| Error::new("client proposal queue poisoned"))?
                .0;
        };
        let (_, bytes) = client_batch_shape(&queue, CLIENT_BATCH, CLIENT_BYTES);
        let pending = take_client_batch(&mut queue, CLIENT_BATCH, CLIENT_BYTES);
        drop(queue);
        if pending.is_empty() {
            return Ok(());
        }
        let mut actions = Vec::with_capacity(pending.len());
        let mut replies = Vec::with_capacity(pending.len());
        for proposal in pending {
            actions.push(proposal.action);
            replies.push(proposal.reply);
        }
        logi!(
            "Raft client batch",
            "member {},count {},bytes {},trigger {}",
            node.id,
            actions.len(),
            bytes,
            trigger
        );
        let result = node.propose(Command::Client(actions));
        match result {
            Ok(index) => {
                *self
                    .client_last_send
                    .lock()
                    .map_err(|_| Error::new("client send time poisoned"))? = Some(Instant::now());
                for reply in replies {
                    reply(Ok(index));
                }
            }
            Err(error) => {
                let message = error.to_string();
                logn!(
                    "Raft client batch rejected",
                    "member {},count {},error {}",
                    node.id,
                    replies.len(),
                    message
                );
                for reply in replies {
                    reply(Err(Error::new(message.clone())));
                }
            }
        }
        let more = !self
            .client_proposals
            .lock()
            .map_err(|_| Error::new("client proposal queue poisoned"))?
            .is_empty();
        if more {
            self.journal.wake();
        }
        Ok(())
    }
    fn fail_client_proposals(&self, message: &str) {
        let pending = self
            .client_proposals
            .lock()
            .map(|mut queue| std::mem::take(&mut *queue))
            .unwrap_or_default();
        for proposal in pending {
            (proposal.reply)(Err(Error::new(message)));
        }
    }
}
fn client_batch_shape(
    queue: &[ClientProposal],
    max_count: usize,
    max_bytes: usize,
) -> (usize, usize) {
    let mut count = 0;
    let mut bytes = CLIENT_ENTRY_BYTES;
    for proposal in queue.iter().take(max_count) {
        bytes =
            bytes.saturating_add(CLIENT_ACTION_BYTES).saturating_add(proposal.action.fields.len());
        count += 1;
        if bytes > max_bytes {
            break;
        }
    }
    (count, bytes)
}
fn take_client_batch(
    queue: &mut Vec<ClientProposal>,
    max_count: usize,
    max_bytes: usize,
) -> Vec<ClientProposal> {
    let (count, _) = client_batch_shape(queue, max_count, max_bytes);
    queue.drain(..count).collect()
}
fn client_delay(last_send: Option<Instant>, now: Instant, interval: Duration) -> Duration {
    last_send
        .map(|last_send| (last_send + interval).saturating_duration_since(now))
        .unwrap_or_default()
}

struct SnapshotProduction<'a>(&'a AtomicBool);
impl Drop for SnapshotProduction<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

pub struct Admission<'a, S> {
    pub history: Option<RwLockReadGuard<'a, S>>,
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
            registrar: {
                let registrar = process.cluster.registrar()?.identity.clone();
                Some((registrar.clone(), process.peer(&registrar)?))
            },
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
pub struct ConsensusReply {
    member: Sender,
    retirement: Option<Retirement>,
    answer: Answer,
    ready: bool,
}
const APPEND_WINDOW: usize = 2;
struct PendingRpc {
    id: u64,
    request: Rpc,
    sent: Duration,
    started: Instant,
    connection: Arc<common::peer::Peer>,
}
struct PeerAnswer {
    id: u64,
    result: std::result::Result<ConsensusReply, String>,
}

fn decode_consensus(response: Resp) -> Result<ConsensusReply> {
    if response.status != Status::Ok {
        return Err(Error::new(format!("peer rejected Raft RPC: {:?}", response.message())));
    }
    let (mut body, rest) = structure(response.args(), Op::BytesResp, "Raft response")?;
    let bytes = body.bytes()?;
    body.done()?;
    if !rest.is_empty() {
        return Err(Error::new("trailing Raft response"));
    }
    parse(&bytes)
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
    type Done: Send + 'static;
    /// Apply one committed action while Raft holds the global write lock.
    fn apply_client(
        &self,
        state: &mut Self::Application,
        context: crate::execution::Context<'_>,
        action: &Action,
        done: &mut Vec<Self::Done>,
    ) -> Result<()>;
    /// Copy service metrics or other small reports while the global lock is held.
    fn finish_apply(&self, _state: &Self::Application) {}
    /// Handle primary-local completions after Raft releases the global lock.
    fn complete_apply(&self, _done: Vec<Self::Done>) {}
    /// Small public routing state needed even by locked or non-applying members.
    /// Runs on the application worker, never on the consensus owner.
    fn has_metadata(&self) -> bool {
        false
    }
    fn apply_metadata(
        &self,
        _state: &mut Self::Application,
        _context: crate::execution::Context<'_>,
        _action: &Action,
    ) -> Result<()> {
        Ok(())
    }
    fn metadata_action(&self, _action: &Action) -> bool {
        true
    }
    fn reset_metadata(&self, state: &mut Self::Application) {
        state.reset();
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
    fn dispatch_forwarded_async(
        &self,
        process: Arc<Process>,
        request: Req,
        done: Box<dyn FnOnce(Resp) + Send + 'static>,
    ) {
        done(self.dispatch_forwarded(&process, &request));
    }
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
    /// Run on raft-main and return after journal work is queued, without waiting for its
    /// durability fence. Callers must wait for commit separately before reporting success.
    fn on_raft_async<R: Send + 'static>(
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
        self.journal.call(move |node| {
            node.journal.poll_io()?;
            f(&host, node)
        })
    }
    /// Queue a consensus-owner query and dispatch its result after releasing the owner
    /// thread. Request workers use this instead of waiting for raft-main.
    fn on_raft_callback<R: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&Self, &mut Node) -> Result<R> + Send + 'static,
        done: impl FnOnce(Result<R>) + Send + 'static,
    ) {
        let done: Arc<Mutex<Option<Box<dyn FnOnce(Result<R>) + Send>>>> =
            Arc::new(Mutex::new(Some(Box::new(done))));
        let finish = |done: &Arc<Mutex<Option<Box<dyn FnOnce(Result<R>) + Send>>>>,
                      result: Result<R>| {
            let done = done.lock().expect("Raft callback").take();
            if let Some(done) = done {
                done(result);
            }
        };
        if self.host.get().is_none() {
            let result = self.journal.initial_call(|node| f(self, node));
            finish(&done, result);
            return;
        }
        let host = self
            .host
            .get()
            .and_then(|weak| weak.upgrade())
            .and_then(|host| host.downcast::<Self>().ok());
        let Some(host) = host else {
            finish(&done, Err(Error::new("Raft host has not started")));
            return;
        };
        let callback = Arc::clone(&done);
        let result = self.journal.call_async(
            move |node| {
                node.journal.poll_io()?;
                f(&host, node)
            },
            move |result| finish(&callback, result),
        );
        if let Err(error) = result {
            finish(&done, Err(error));
        }
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
                while let Some(runtime) = weak.upgrade() {
                    if runtime.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    match runtime.bootstrap_step() {
                        Ok(true) => break,
                        Ok(false) => {}
                        Err(e) => logn!("Raft bootstrap waiting", "{e}"),
                    }
                    drop(runtime);
                    std::thread::park_timeout(Duration::from_millis(200));
                }
            })
            .context("starting bootstrap coordinator")?;
        self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?.push(handle);
        Ok(())
    }
    fn accept_placement(
        &self,
        selected: Option<&BTreeSet<MemberId>>,
        node: &mut Node,
    ) -> Result<()> {
        if let Some(selected) = selected {
            let members = node.state().membership().members();
            let mut placement =
                self.placement.lock().map_err(|_| Error::new("placement lock poisoned"))?;
            placement.selected =
                selected.iter().filter(|id| members.contains(*id)).cloned().collect();
            let applying = placement.selected.contains(&node.id);
            node.set_applying_state(applying)?;
            self.applying.store(applying, Ordering::SeqCst);
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
                while let Some(runtime) = weak.upgrade() {
                    if runtime.snapshot_stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    runtime.maybe_snapshot();
                    drop(runtime);
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
            if let Some(tip) = node.retained_tip(index) {
                return Ok(Some(TipRead::Retained(tip)));
            }
            Ok(Some(TipRead::Disk(
                node.journal.read_history(move |history| history.committed_tip(index))?,
            )))
        })?;
        if let Some(value) = local {
            return match value {
                TipRead::Retained(tip) => Ok(Some(tip)),
                TipRead::Disk(read) => read.wait(),
            };
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
                node.reset_retained()?;
                logn!(
                    "Raft committed journal copied",
                    "peer {},after {},through {}",
                    peer,
                    after,
                    through
                );
                node.refresh_slot();
                host.refresh_application(node)?;
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
        let history = self.global_lock.read();
        let applied = self.applied();
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
                let state = node.state().clone();
                let snapshot = state.capture();
                let mark = node.journal.begin_checkpoint(&state, &snapshot)?;
                Ok(Some(CheckpointPlan::Produce { state, snapshot, mark }))
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
            Some(CheckpointPlan::Produce { mut state, mut snapshot, mark }) => {
                logn!("Raft checkpoint started", "index {},term {}", state.commit, state.term);
                let checkpoint_index = state.commit;
                snapshot.application = self.checkpoint_actions(&state, &history)?;
                state.snapshot = snapshot;
                state.log.retain(|entry| entry.index > state.snapshot.index);
                let pending = self.on_raft(move |host, node| {
                    if host.cancel_snapshot.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    node.journal.prepare_checkpoint(now, state, mark)
                })?;
                if let Some(pending) = pending {
                    pending.write_cancellable(|| self.cancel_snapshot.load(Ordering::SeqCst))?;
                    drop(history);
                    self.on_raft(move |_, node| node.journal.finish_snapshot(&pending))?;
                    self.checkpoint_requested.store(false, Ordering::SeqCst);
                    logn!("Raft checkpoint completed", "index {}", checkpoint_index);
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
                            node.reset_retained()?;
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
    fn refresh_application(&self, node: &mut Node) -> Result<()> {
        if node.repairing {
            return Ok(());
        }
        node.refresh_retained()?;
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
            queue.enqueue(node, node.journal.durable_state(), true)?;
            if queue.pending.is_some() {
                self.wake_execution();
            }
        }
        let enabled = self.ready.load(Ordering::SeqCst)
            && (node.role == Role::Leader || self.applying.load(Ordering::SeqCst));
        let mut queue =
            self.work_queue.lock().map_err(|_| Error::new("application queue poisoned"))?;
        queue.enqueue(node, node.journal.durable_state(), enabled)?;
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
        let changed = *previous_error != current_error;
        if changed {
            if let Some(error) = &current_error {
                logn!("Raft client application failed", "error {}", error);
            } else {
                logn!("Raft client application recovered", "");
            }
        }
        *previous_error = current_error;
        drop(previous_error);
        if changed {
            self.apply_watch.signal();
        }
        let failure = result.as_ref().err().map(ToString::to_string);
        self.collect_applied(failure.as_deref(), true);
        self.run_applied();
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
                let complete = crate::execution::execute_state(
                    &self.global_lock,
                    &self.metadata_execution,
                    &self.metadata_applied,
                    work,
                    Duration::from_millis(self.options.raft.execution_lock_ms),
                    true,
                    |state| self.reset_metadata(state),
                    |action| self.metadata_action(action),
                    |state, context, action| self.apply_metadata(state, context, action),
                )?;
                if complete {
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
                    self.global_lock.write().reset();
                    *cursor = crate::execution::Cursor::default();
                    self.apply_watch.publish(0);
                    Ok(true)
                }
                PendingWork::Apply(work) => {
                    let mut done = Vec::new();
                    let applied = crate::execution::exec_report(
                        &self.global_lock,
                        &self.execution,
                        &self.apply_watch.applied,
                        work,
                        Duration::from_millis(self.options.raft.execution_lock_ms),
                        |state, context, action| {
                            self.apply_client(state, context, action, &mut done)
                        },
                        |state| self.finish_apply(state),
                    );
                    self.complete_apply(done);
                    let (progress, ()) = applied?;
                    let complete = progress.complete && progress.applied >= work.commit;
                    if complete {
                        self.application_generation.store(work.generation, Ordering::SeqCst);
                    }
                    // exec_report has published the atomic index before unlocking state.
                    self.apply_watch.signal();
                    self.journal.wake();
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
                if self.applied() >= commit {
                    let guard = if read { self.global_lock.try_read() } else { None };
                    if read && guard.is_none() {
                        continue;
                    }
                    let valid = self.on_raft(move |host, node| {
                        Ok(node.can_admit_with_policy(
                            lease::now()?,
                            read,
                            host.options.allow_stale_reads,
                        ))
                    })?;
                    if valid {
                        return Ok(Admission { history: guard });
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
    fn wait_applied(&self, index: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let version = self.apply_watch.version();
            let (committed, current_generation) = self.on_raft(move |host, node| {
                if node.journal.failed {
                    return Err(Error::new("Raft journal failed"));
                }
                if node.role != Role::Leader {
                    return Err(Error::new(
                        "Raft leadership changed; request outcome may be unknown",
                    ));
                }
                host.refresh_application(node)?;
                Ok((
                    node.state().commit >= index,
                    host.application_generation.load(Ordering::SeqCst) == node.state().generation,
                ))
            })?;
            if committed
                && current_generation
                && self.apply_watch.applied.load(Ordering::Acquire) >= index
            {
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
            if committed {
                self.apply_watch.wait(version, APPLY_TICK);
            } else {
                std::thread::sleep(Duration::from_millis(5));
            }
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
        let request = self.raft_request(target, message)?;
        self.transport
            .as_ref()
            .ok_or_else(|| Error::new("no Raft transport"))?
            .get(name, || self.member_address(name))?
            .call_timeout(&request, Duration::from_millis(timeout))
    }
    fn raft_request(&self, target: Option<MemberId>, message: Message) -> Result<Req> {
        self.on_raft_async(move |host, node| host.raft_request_for_node(node, target, message))
    }
    fn raft_request_for_node(
        &self,
        node: &Node,
        target: Option<MemberId>,
        message: Message,
    ) -> Result<Req> {
        let applying = Some(
            self.placement
                .lock()
                .map_err(|_| Error::new("placement lock poisoned"))?
                .selected
                .clone(),
        );
        let member = if node.id == UNASSIGNED {
            Sender::Applicant(node.journal.identity.current.clone())
        } else {
            Sender::Slot(node.id)
        };
        let retirement = node.journal.pending_replacement();
        Ok(Req::new(Op::Raft).text(&self.runtime.name).bytes(crate::codec::encode_message(
            &Packet {
                member,
                target,
                retirement,
                applying,
                ready: self.ready.load(Ordering::SeqCst),
                message,
            },
        )?))
    }
    fn peer_name(&self, peer: MemberId) -> Result<String> {
        self.on_raft_async(move |host, node| {
            node.state()
                .membership()
                .identities
                .get(&peer)
                .or_else(|| node.state().contacts.get(&peer))
                .map(|m| m.process.clone())
                .or_else(|| host.runtime.founders.get(&peer).cloned())
                .ok_or_else(|| Error::new("unknown member index"))
        })
    }
    fn ask_raft(
        &self,
        peer: MemberId,
        message: Message,
        then: impl FnOnce(Resp) + Send + 'static,
    ) -> Result<Arc<common::peer::Peer>> {
        let name = self.peer_name(peer)?;
        let request = self.raft_request(Some(peer), message)?;
        self.ask_named(&name, &request, then)
    }
    fn ask_named(
        &self,
        name: &str,
        request: &Req,
        then: impl FnOnce(Resp) + Send + 'static,
    ) -> Result<Arc<common::peer::Peer>> {
        let connection = self
            .transport
            .as_ref()
            .ok_or_else(|| Error::new("no Raft transport"))?
            .get(&name, || self.member_address(&name))?;
        if let Err(error) = connection.ask(&request, then) {
            connection.close();
            return Err(error);
        }
        Ok(connection)
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
    /// Forward a client request without holding its server worker for the response.
    /// The local-leader check and outgoing request construction do not wait for a
    /// journal fence; remote replies arrive through the peer reader callback.
    fn forward_async(
        self: &Arc<Self>,
        process: Arc<Process>,
        request: &Req,
        done: impl FnOnce(Option<Resp>) + Send + 'static,
    ) {
        enum Plan {
            Local,
            Remote { name: String, request: Req },
        }
        let request = request.clone();
        let runtime = Arc::clone(self);
        let done: Arc<Mutex<Option<Box<dyn FnOnce(Option<Resp>) + Send>>>> =
            Arc::new(Mutex::new(Some(Box::new(done))));
        let callback = Arc::clone(&done);
        let process_for_result = Arc::clone(&process);
        self.on_raft_callback(
            move |host, node| {
                if node.role == Role::Leader {
                    return Ok(Plan::Local);
                }
                let id = node
                    .leader
                    .filter(|id| node.state().membership().members().contains(id))
                    .ok_or_else(|| Error::new("Raft has no known Raft leader"))?;
                let [arg] = request.args() else {
                    return Err(Error::new("expected structured Raft request"));
                };
                if arg.is_group() {
                    return Err(Error::new("expected structured Raft request"));
                }
                let name = node
                    .state()
                    .membership()
                    .identities
                    .get(&id)
                    .or_else(|| node.state().contacts.get(&id))
                    .map(|member| member.process.clone())
                    .or_else(|| host.runtime.founders.get(&id).cloned())
                    .ok_or_else(|| Error::new("unknown member index"))?;
                let message = Message::Forward {
                    op: request.op().code(),
                    payload: arg.bytes().clone().into(),
                    depth: arg.depth,
                };
                let outbound = host.raft_request_for_node(node, Some(id), message)?;
                Ok(Plan::Remote { name, request: outbound })
            },
            move |result| {
                let runtime = Arc::clone(&runtime);
                let done = Arc::clone(&callback);
                process_for_result.run(move || match result {
                    Ok(Plan::Local) => {
                        if let Some(done) = take_callback(&done) {
                            done(None);
                        }
                    }
                    Ok(Plan::Remote { name, request }) => {
                        let callback = Arc::clone(&done);
                        let result = runtime.ask_named(&name, &request, move |answer| {
                            if let Some(done) = take_callback(&callback) {
                                done(Some(answer));
                            }
                        });
                        if let Err(error) = result {
                            if let Some(done) = take_callback(&done) {
                                done(Some(Resp::error(Status::Internal, error.to_string())));
                            }
                        }
                    }
                    Err(error) => {
                        if let Some(done) = take_callback(&done) {
                            done(Some(Resp::error(Status::Internal, error.to_string())));
                        }
                    }
                });
            },
        );
    }
    /// Answer a Raft RPC, allowing a forwarded client request to continue without
    /// keeping the server worker while the leader applies it.
    fn raft_rpc_async(
        self: &Arc<Self>,
        process: Arc<Process>,
        request: Req,
        done: impl FnOnce(Resp) + Send + 'static,
    ) {
        let (sender, bytes) = match raft_request_payload(&request) {
            Ok((sender, bytes)) => (sender, bytes),
            Err(error) => return done(Resp::error(Status::Internal, error.to_string())),
        };
        let packet = match crate::codec::decode_message::<Packet>(&bytes) {
            Ok(packet) => packet,
            Err(error) => return done(Resp::error(Status::Internal, error.to_string())),
        };
        let wait_for_journal = !matches!(&packet.message, Message::Forward { .. });
        let packet_sender = sender.clone();
        let done: Arc<Mutex<Option<Box<dyn FnOnce(Resp) + Send>>>> =
            Arc::new(Mutex::new(Some(Box::new(done))));
        let reply = Arc::clone(&done);
        let runtime = Arc::clone(self);
        self.on_raft_callback(
            move |host, node| {
                let mut deferred = None;
                let response = host.raft_packet(node, &packet_sender, packet, &mut deferred);
                let fence = wait_for_journal.then(|| node.journal.fence()).flatten();
                Ok((response, deferred, fence))
            },
            move |result| {
                let process = Arc::clone(&process);
                let runtime = Arc::clone(&runtime);
                let reply = Arc::clone(&reply);
                let continue_rpc =
                    move |durable: Result<()>, result: Result<(Result<Resp>, Option<Deferred>)>| {
                        let process = Arc::clone(&process);
                        let runtime = Arc::clone(&runtime);
                        let reply = Arc::clone(&reply);
                        let worker = Arc::clone(&process);
                        worker.run(move || {
                            let answer = match durable {
                                Err(error) => Err(error),
                                Ok(()) => result,
                            };
                            let answer = match answer {
                                Err(error) => {
                                    logn!(
                                        "Raft RPC rejected",
                                        "sender {},reason {}",
                                        sender,
                                        error
                                    );
                                    Some(Resp::error(Status::Internal, error.to_string()))
                                }
                                Ok((response, deferred)) => match response {
                                    Err(error) => {
                                        Some(Resp::error(Status::Internal, error.to_string()))
                                    }
                                    Ok(response) => match deferred {
                                        Some(Deferred::Message(Message::Forward {
                                            op,
                                            payload,
                                            depth,
                                        })) => match Op::from_code(op) {
                                            Ok(op) => {
                                                let forward_reply = Arc::clone(&reply);
                                                runtime.dispatch_forwarded_async(
                                                    Arc::clone(&process),
                                                    Req {
                                                        id: 0,
                                                        payload: Arg::new(
                                                            op,
                                                            payload.into(),
                                                            depth,
                                                        ),
                                                    },
                                                    Box::new(move |answer| {
                                                        if let Some(done) =
                                                            take_callback(&forward_reply)
                                                        {
                                                            done(answer);
                                                        }
                                                    }),
                                                );
                                                None
                                            }
                                            Err(error) => Some(Resp::error(
                                                Status::Internal,
                                                error.to_string(),
                                            )),
                                        },
                                        Some(Deferred::Message(Message::Client(action))) => Some(
                                            runtime
                                                .receive_client(&process, action)
                                                .unwrap_or_else(|error| {
                                                    Resp::error(Status::Internal, error.to_string())
                                                }),
                                        ),
                                        Some(Deferred::Read(read)) => Some(
                                            read.wait().map(Resp::bytes).unwrap_or_else(|error| {
                                                Resp::error(Status::Internal, error.to_string())
                                            }),
                                        ),
                                        None => Some(response),
                                        _ => Some(Resp::error(
                                            Status::Internal,
                                            "invalid deferred Raft work",
                                        )),
                                    },
                                },
                            };
                            if let Some(answer) = answer {
                                if let Some(reply) = take_callback(&reply) {
                                    reply(answer);
                                }
                            }
                        });
                    };
                match result {
                    Ok((response, deferred, Some(fence))) => {
                        fence.then(move |durable| continue_rpc(durable, Ok((response, deferred))));
                    }
                    Ok((response, deferred, None)) => {
                        continue_rpc(Ok(()), Ok((response, deferred)));
                    }
                    Err(error) => continue_rpc(Ok(()), Err(error)),
                }
            },
        );
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
                .map_err(|_| Error::new("peer readiness lock poisoned"))?
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
                self.refresh_application(node)?;
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
                if let Some(tip) = node.retained_tip(index) {
                    return Ok(Resp::bytes(binary(&Some(tip))?));
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
                if let Some(suffix) = node.retained_suffix(after, through) {
                    return Ok(Resp::bytes(binary(&Some(suffix))?));
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
                    self.accept_placement(packet.applying.as_ref(), node)?;
                    if let Rpc::Checkpoint { index, .. } = rpc {
                        if !answer.success {
                            self.checkpoint_needed.fetch_max(index, Ordering::SeqCst);
                        }
                    }
                }
                self.refresh_application(node)?;
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
                host.fail_client_proposals("Raft runtime is stopping");
                host.collect_applied(Some("Raft runtime is stopping"), false);
                return false;
            }
            let result = (|| -> Result<()> {
                if node.journal.failed {
                    host.collect_applied(Some("Raft journal failed"), false);
                    return Ok(());
                }
                let before =
                    (node.role, node.state().term, node.state().last(), node.state().commit);
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
                host.flush_client_proposals(node)?;
                host.refresh_application(node)?;
                if node.role != Role::Leader {
                    host.collect_applied(
                        Some("Raft leadership changed; request outcome may be unknown"),
                        false,
                    );
                } else {
                    host.collect_applied(None, true);
                }
                if before
                    != (node.role, node.state().term, node.state().last(), node.state().commit)
                {
                    host.wake_peers();
                }
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
    fn stop_raft(&self) -> Result<()> {
        self.snapshot_stopping.store(true, Ordering::SeqCst);
        self.wake_execution();
        loop {
            let handles = std::mem::take(
                &mut *self.raft_workers.lock().map_err(|_| Error::new("worker lock poisoned"))?,
            );
            if handles.is_empty() {
                break;
            }
            for handle in &handles {
                handle.thread().unpark();
            }
            for handle in handles {
                handle.join().map_err(|_| Error::new("Raft worker panicked"))?;
            }
        }
        if let Some(fence) = self.io_fence.get() {
            fence.wait_all()?;
        }
        Ok(())
    }
    fn record_peer_reply(
        &self,
        peer: MemberId,
        request: Rpc,
        response: ConsensusReply,
        sent: Duration,
    ) -> Result<(bool, Option<bool>, bool)> {
        self.on_raft_async(move |host, node| {
            let before = (node.role, node.state().term, node.state().last(), node.state().commit);
            let ready = response.ready;
            let success = response.answer.success;
            if let Some(retired) = response.retirement {
                if retired.previous != peer || response.member != Sender::Slot(retired.next) {
                    return Err(Error::new("retirement reply does not match requested member"));
                }
                node.observe_retirement(retired)?;
                return Ok((ready, None, false));
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
                return Err(Error::new("Raft reply came from the wrong member"));
            }
            host.placement
                .lock()
                .map_err(|_| Error::new("placement lock poisoned"))?
                .acknowledge(&peer, Instant::now());
            let before_next = node.next.get(&peer).copied().unwrap_or_else(|| match &request {
                Rpc::Append { previous, .. } => previous.saturating_add(1),
                _ => 0,
            });
            node.response_sent(&peer, &request, &response.answer, sent)?;
            let retry_now = !success
                && matches!(request, Rpc::Append { .. })
                && node.next.get(&peer).is_some_and(|next| *next < before_next);
            host.refresh_application(node)?;
            if before != (node.role, node.state().term, node.state().last(), node.state().commit) {
                host.wake_peers();
            }
            Ok((ready, Some(success), retry_now))
        })
    }
    fn start_peer(self: &Arc<Self>, peer: MemberId, cancelled: Arc<AtomicBool>) -> Result<()> {
        let weak = Arc::downgrade(self);
        let heartbeat = Duration::from_millis(self.options.raft.heartbeat_ms);
        let request_timeout = Duration::from_millis(self.options.raft.request_timeout_ms);
        let handle = std::thread::Builder::new()
            .name(format!("raft-{peer}"))
            .spawn(move || {
                let mut unlocked = false;
                let mut last_error: Option<String> = None;
                let current = std::thread::current();
                let (answer_tx, answer_rx) = std::sync::mpsc::channel::<PeerAnswer>();
                let mut pending: Vec<PendingRpc> = Vec::new();
                let mut request_id = 0u64;
                let mut next_heartbeat = Instant::now();
                let mut retry_at: Option<Instant> = None;
                let mut stalled = false;
                loop {
                    let Some(runtime) = weak.upgrade() else {
                        break;
                    };
                    if runtime.snapshot_stopping.load(Ordering::SeqCst)
                        || cancelled.load(Ordering::SeqCst)
                    {
                        break;
                    }

                    let mut contacted = false;
                    while let Ok(answer) = answer_rx.try_recv() {
                        let Some(at) = pending.iter().position(|entry| entry.id == answer.id) else {
                            continue;
                        };
                        let entry = pending.remove(at);
                        match answer.result {
                            Err(message) => {
                                if last_error.as_ref() != Some(&message) {
                                    logn!("Raft peer failed", "peer {},error {}", peer, message);
                                    last_error = Some(message);
                                }
                                unlocked = false;
                                stalled = true;
                                retry_at = Some(Instant::now() + heartbeat);
                                if pending.is_empty() {
                                    stalled = false;
                                }
                            }
                            Ok(response) => {
                                match runtime.record_peer_reply(
                                    peer,
                                    entry.request,
                                    response,
                                    entry.sent,
                                ) {
                                    Ok((ready, success, retry_now)) => {
                                        contacted = true;
                                        unlocked = ready;
                                        if success == Some(false) {
                                            stalled = true;
                                        }
                                        if retry_now {
                                            next_heartbeat = Instant::now();
                                        }
                                        retry_at = None;
                                        if let Some(message) = last_error.take() {
                                            logn!("Raft peer recovered", "peer {}", peer);
                                            let _ = message;
                                        }
                                    }
                                    Err(error) => {
                                        let message = error.to_string();
                                        if last_error.as_ref() != Some(&message) {
                                            logn!(
                                                "Raft peer failed",
                                                "peer {},error {}",
                                                peer,
                                                message
                                            );
                                            last_error = Some(message);
                                        }
                                        unlocked = false;
                                        stalled = true;
                                        retry_at = Some(Instant::now() + heartbeat);
                                    }
                                }
                            }
                        }
                    }
                    if pending.is_empty() {
                        stalled = false;
                    }

                    let now = Instant::now();
                    if let Some(at) =
                        pending.iter().position(|entry| now >= entry.started + request_timeout)
                    {
                        let message =
                            format!("peer {peer} did not answer a pipelined Raft RPC in time");
                        if last_error.as_ref() != Some(&message) {
                            logn!("Raft peer failed", "peer {},error {}", peer, message);
                            last_error = Some(message);
                        }
                        pending[at].connection.close();
                        pending.clear();
                        stalled = false;
                        unlocked = false;
                        retry_at = Some(now + heartbeat);
                    }

                    let retry_ready = retry_at.is_none_or(|at| now >= at);
                    let pending_empty = pending.is_empty();
                    let pending_tails: Vec<_> = pending
                        .iter()
                        .filter_map(|entry| match &entry.request {
                            Rpc::Append { previous, entries, .. } => Some((
                                entry.request.term(),
                                entries.last().map_or(*previous, |e| e.index).saturating_add(1),
                            )),
                            _ => None,
                        })
                        .collect();
                    let can_send = !stalled && retry_ready && pending.len() < APPEND_WINDOW;
                    let plan = runtime.on_raft_async(move |_, node| {
                        let (contact, setup) =
                            peer_eligibility(&node.state().membership(), node.id, peer);
                        if node.journal.failed || !contact {
                            return Ok(None);
                        }
                        let request = if node.role == Role::Leader {
                            let next = node
                                .next
                                .get(&peer)
                                .copied()
                                .unwrap_or(node.state().last().saturating_add(1));
                            let cursor = pending_tails
                                .iter()
                                .filter(|(term, _)| *term == node.state().term)
                                .map(|(_, next)| *next)
                                .max()
                                .unwrap_or(1)
                                .max(next);
                            node.request_from(&peer, cursor)
                        } else if pending_empty {
                            node.request_reference(&peer)
                        } else {
                            None
                        };
                        Ok(Some((request, setup)))
                    });

                    let mut sent_data = false;
                    match plan {
                        Err(error) => {
                            let message = error.to_string();
                            if last_error.as_ref() != Some(&message) {
                                logn!("Raft peer failed", "peer {},error {}", peer, message);
                                last_error = Some(message);
                            }
                            retry_at = Some(Instant::now() + heartbeat);
                        }
                        Ok(Some((request, setup))) => {
                            let requested = runtime
                                .readiness_requests
                                .lock()
                                .map_err(|_| Error::new("peer readiness lock poisoned"));
                            let requested = match requested {
                                Ok(requested) => requested.contains(&peer),
                                Err(error) => {
                                    let message = error.to_string();
                                    if last_error.as_ref() != Some(&message) {
                                        logn!(
                                            "Raft peer failed",
                                            "peer {},error {}",
                                            peer,
                                            message
                                        );
                                        last_error = Some(message);
                                    }
                                    false
                                }
                            };
                            if pending_empty && setup && (!unlocked || requested) {
                                if let Some(action) = runtime.peer_message() {
                                    match runtime.send_raft(&peer, Message::Client(action)) {
                                        Ok(response) => {
                                            unlocked = response.status == Status::Ok;
                                            contacted |= unlocked;
                                            if unlocked {
                                                if let Ok(mut requests) =
                                                    runtime.readiness_requests.lock()
                                                {
                                                    requests.remove(&peer);
                                                }
                                            }
                                        }
                                        Err(error) => {
                                            let message = error.to_string();
                                            if last_error.as_ref() != Some(&message) {
                                                logn!(
                                                    "Raft peer failed",
                                                    "peer {},error {}",
                                                    peer,
                                                    message
                                                );
                                                last_error = Some(message);
                                            }
                                            unlocked = false;
                                            retry_at = Some(Instant::now() + heartbeat);
                                        }
                                    }
                                }
                            }
                            if let Some(request) = request {
                                let is_append = matches!(request, Rpc::Append { .. });
                                let is_data = matches!(&request, Rpc::Append { entries, .. } if !entries.is_empty());
                                let send_heartbeat = pending_empty && now >= next_heartbeat;
                                let should_send = can_send
                                    && if is_append {
                                        is_data || send_heartbeat
                                    } else {
                                        pending_empty
                                    };
                                if should_send {
                                    match lease::now() {
                                        Err(error) => {
                                            let message = error.to_string();
                                            if last_error.as_ref() != Some(&message) {
                                                logn!(
                                                    "Raft peer failed",
                                                    "peer {},error {}",
                                                    peer,
                                                    message
                                                );
                                                last_error = Some(message);
                                            }
                                            retry_at = Some(Instant::now() + heartbeat);
                                        }
                                        Ok(sent) => {
                                            request_id = request_id.wrapping_add(1);
                                            let id = request_id;
                                            let tx = answer_tx.clone();
                                            let wake = current.clone();
                                            let request_for_answer = request.clone();
                                            let result = runtime.ask_raft(
                                                peer,
                                                Message::Consensus(request.clone()),
                                                move |response| {
                                                    let result = decode_consensus(response)
                                                        .map_err(|error| error.to_string());
                                                    let _ = tx.send(PeerAnswer { id, result });
                                                    wake.unpark();
                                                },
                                            );
                                            match result {
                                                Ok(connection) => {
                                                    pending.push(PendingRpc {
                                                        id,
                                                        request: request_for_answer,
                                                            sent,
                                                            started: Instant::now(),
                                                            connection,
                                                        });
                                                    next_heartbeat = Instant::now() + heartbeat;
                                                    sent_data = is_data;
                                                }
                                                Err(error) => {
                                                    let message = error.to_string();
                                                    if last_error.as_ref() != Some(&message) {
                                                        logn!(
                                                            "Raft peer failed",
                                                            "peer {},error {}",
                                                            peer,
                                                            message
                                                        );
                                                        last_error = Some(message);
                                                    }
                                                    unlocked = false;
                                                    retry_at = Some(Instant::now() + heartbeat);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => {}
                    }

                    if contacted && last_error.take().is_some() {
                        logn!("Raft peer recovered", "peer {}", peer);
                    }
                    if sent_data && pending.len() < APPEND_WINDOW {
                        drop(runtime);
                        continue;
                    }

                    let now = Instant::now();
                    let deadline = if pending.is_empty() {
                        retry_at.filter(|retry| *retry > now).unwrap_or(next_heartbeat.max(now))
                    } else {
                        pending
                            .iter()
                            .map(|entry| entry.started + request_timeout)
                            .min()
                            .unwrap_or(now + heartbeat)
                    };
                    let wait = deadline.saturating_duration_since(now);
                    drop(runtime);
                    if wait.is_zero() {
                        continue;
                    }
                    std::thread::park_timeout(wait);
                }
                if let Some(runtime) = weak.upgrade() {
                    if let Ok(mut peers) = runtime.peer_threads.lock() {
                        if peers.get(&peer).is_some_and(|thread| thread.id() == current.id()) {
                            peers.remove(&peer);
                        }
                    }
                }
            })
            .context("starting Raft peer worker")?;
        self.peer_threads
            .lock()
            .map_err(|_| Error::new("peer thread lock poisoned"))?
            .insert(peer, handle.thread().clone());
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
    fn recovery_loads_committed_entries_in_bounded_chunks() {
        let base = raft::tests::node(0, raft::tests::members(1));
        let mut node = Node::new_with_log2(base.id, base.journal, 2).unwrap();
        let applied = Arc::new(AtomicU64::new(0));
        node.set_applied(applied);
        node.set_applying_state(true).unwrap();
        node.journal.write(&[raft::Record::Hard { term: 1, vote: None, vote_scn: None }]).unwrap();
        let entries: Vec<_> = (1..=5)
            .map(|index| Entry {
                scn: common::scn::Scn::new(index),
                index,
                term: 1,
                command: Command::Noop,
                entry_buf: None,
            })
            .collect();
        node.journal
            .write(&[raft::Record::Entries { from: 1, entries }, raft::Record::Commit(5)])
            .unwrap();
        node.refresh_retained().unwrap();

        let mut queue = WorkQueue::default();
        queue.enqueue(&node, node.journal.durable_state(), true).unwrap();
        let Some(PendingWork::Apply(work)) = queue.pending.as_ref() else {
            panic!("expected replay work");
        };
        assert_eq!(work.entries.len(), 4);
        assert_eq!(work.commit, 4);
        assert!(Arc::ptr_eq(&node.shared_entry(1).unwrap(), &work.entries.shared(0).unwrap()));

        queue.pending.take();
        queue.enqueue(&node, node.journal.durable_state(), true).unwrap();
        let Some(PendingWork::Apply(work)) = queue.pending.as_ref() else {
            panic!("expected remaining replay work");
        };
        assert_eq!(work.entries.len(), 1);
        assert_eq!(work.entries.get(0).unwrap().index, 5);
        assert_eq!(work.commit, 5);
    }
    #[test]
    fn client_batches_preserve_actions_in_order() {
        let mut queue = Vec::new();
        let mut expected = Vec::new();
        for number in 0..(CLIENT_BATCH + 2) {
            let fields = if number == 5 { vec![number as u8; 4096] } else { vec![number as u8] };
            let action = Action::new(Op::Ping, fields);
            expected.push(action.clone());
            queue.push(ClientProposal { action, reply: Box::new(|_| {}) });
        }

        let first = take_client_batch(&mut queue, CLIENT_BATCH, CLIENT_BYTES);
        assert_eq!(first.len(), CLIENT_BATCH);
        for (proposal, action) in first.iter().zip(&expected) {
            assert_eq!(&proposal.action, action);
        }
        let last = take_client_batch(&mut queue, CLIENT_BATCH, CLIENT_BYTES);
        assert_eq!(last.len(), 2);
        for (proposal, action) in last.iter().zip(&expected[CLIENT_BATCH..]) {
            assert_eq!(&proposal.action, action);
        }
        assert!(queue.is_empty());
    }
    #[test]
    fn client_batch_waits_until_the_time_cutoff() {
        let now = Instant::now();
        let interval = Duration::from_millis(1);
        assert_eq!(client_delay(None, now, interval), Duration::ZERO);
        assert_eq!(client_delay(Some(now), now, interval), interval);
        assert_eq!(client_delay(Some(now - interval), now, interval), Duration::ZERO);
    }
    #[test]
    fn client_batch_stops_after_its_size_cutoff() {
        let mut queue = Vec::new();
        for fields in [vec![1; 4], vec![2; 8], vec![3; 1]] {
            queue.push(ClientProposal {
                action: Action::new(Op::Ping, fields),
                reply: Box::new(|_| {}),
            });
        }
        let limit = CLIENT_ENTRY_BYTES + CLIENT_ACTION_BYTES + 4 + 6;
        let (count, bytes) = client_batch_shape(&queue, 10, limit);
        assert_eq!(count, 2);
        assert!(bytes > limit);

        let actions = queue.iter().take(count).map(|p| p.action.clone()).collect();
        let encoded = crate::codec::encode(&Entry {
            scn: common::scn::Scn::new(1),
            index: 1,
            term: 1,
            command: Command::Client(actions),
            entry_buf: None,
        })
        .unwrap();
        assert_eq!(bytes, encoded.len());

        let batch = take_client_batch(&mut queue, 10, limit);
        assert_eq!(batch.len(), 2);
        assert_eq!(queue.len(), 1);
    }
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
        let clients: Vec<_> = [Op::Ping, Op::Status, Op::RaftMembers]
            .iter()
            .copied()
            .filter(|op| !matches!(op, Op::Raft | Op::Register | Op::Lookup))
            .collect();
        let all_clients = clients.iter().all(|op| needs_request_worker(&Req::new(*op)));
        let unknown_client = needs_request_worker(&Req::new(Op::BytesResp));

        let registration = needs_request_worker(&Req::new(Op::Register));
        let packet = |message| {
            Req::new(Op::Raft).text("member-0").bytes(
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
        let transfer =
            needs_request_worker(&packet(Message::Client(crate::Action::new(Op::Ping, vec![]))));
        let forwarded = needs_request_worker(&packet(Message::Forward {
            op: Op::Ping.code(),
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
    #[test]
    fn apply_watch_wakes_waiters_without_losing_early_signals() {
        let watch = Arc::new(ApplyWatch::new(0));
        let version = watch.version();
        watch.publish(7);
        watch.wait(version, Duration::ZERO);
        assert_eq!(watch.applied.load(Ordering::Acquire), 7);

        let version = watch.version();
        let waiting = watch.clone();
        let (wake, woke) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            waiting.wait(version, Duration::from_secs(5));
            wake.send(waiting.applied.load(Ordering::Acquire)).unwrap();
        });
        watch.publish(9);
        assert_eq!(woke.recv_timeout(Duration::from_secs(1)).unwrap(), 9);
        worker.join().unwrap();
    }
}
