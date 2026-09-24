//! The lifecycle every daemon has: read one process configuration, listen,
//! serve, stop.
//!
//! Nothing here is lock-step. A caller may have many requests outstanding on one
//! connection and a service may answer them in any order; response ids are what
//! let the caller sort them out, and a whole frame is written under one lock so
//! frames never interleave.
//!
//! Answers are coalesced, so a connection with several requests in flight costs
//! one read and one write rather than one of each per message. Two ways to decide
//! when to send, chosen by configuration:
//!
//! - a **sender per connection**, looping: take everything queued, write it as one
//!   batch, and let the next batch be whatever piled up while that was in flight.
//!   Self-clocking — batch size rises with load and falls to one when idle — and it
//!   works no matter which thread produced the answer.
//! - **flush on the read loop**, when nothing more has arrived. Cheaper by a queue
//!   hop, but it coalesces only what the read loop itself answered.
//! - **adaptive**: the second until an answer is produced off the read loop, the
//!   first from then on. The read loop knows at dispatch which it is about to do,
//!   and it is the only thread that switches, so the switch needs no agreement.
//!
//! Neither waits for anything, so neither adds latency: both send at the moment
//! the alternative would have been to block.
//!
//! **A request that has to wait on another process does not hold a worker.** A
//! handler is given a `Reply` it may keep: it asks the process it needs, hands the
//! `Reply` to the answer, and returns. The waiting is a table entry, not a thread,
//! which is what makes a forwarding tier cost a forward rather than a thread per
//! request in flight.
//!
//! Where an answer is computed is per message type, and the trade is real:
//! handing a request to a worker costs a queue hop and a wakeup, while answering
//! it on the read loop stalls the next read for as long as it takes. A ping is
//! cheap enough that inline plainly wins; anything that touches a disk plainly
//! does not; the middle is a measurement, so `Service::inline` is a knob rather
//! than a decision.
//!
//! A process is ready when its `ready` file exists, written once the listener is
//! accepting and, for everything but KS itself, once KS has been told which port
//! that is. Stopping is a request, not a signal, so a process with state to flush
//! gets the chance; a signal is the fallback.

use crate::config::{ClusterConfig, Messaging, ProcessConfig, SendMode, ServiceEntry, ServiceKind};
use crate::peer::Peers;
use crate::random::Rng;
use crate::registry::Peer;
use crate::routing;
use crate::wire::{
    self, Arg, BUF_BYTES, MAX_FRAME_BYTES, Op, Req, Resp, Status, StructWriter, Wire, put_response,
    structure,
};
use crate::{Context, Error, Result};
use crate::{logd, logi, logn, logsec, logw};
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

pub const READY_FILE: &str = "ready";

thread_local! {
    /// Framing scratch for a worker, so answering allocates nothing.
    static SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUF_BYTES));
    /// One generator per thread, so padding needs no lock.
    static NOISE: RefCell<Rng> = RefCell::new(Rng::new());
}

pub struct Process {
    pub config: ProcessConfig,
    pub cluster: ClusterConfig,
    pub entry: ServiceEntry,
    pub run_dir: PathBuf,
    pub started: Instant,
    /// Connections to the other processes this one talks to.
    pub(crate) peers: Peers,
    pub(crate) assigned: RwLock<HashMap<String, SocketAddr>>,
    /// Where round-robin over a kind has got to.
    pub(crate) turn: AtomicU64,
    pub(crate) stopping: AtomicBool,
    /// Requests answered, so a client's throughput claim can be checked against
    /// what the server thinks it did.
    pub(crate) answered: AtomicU64,
    /// Connections that have moved from writing directly to a batching sender.
    pub(crate) senders: AtomicU64,
}

/// What a machine type adds to the common lifecycle. A new type needs `serves`
/// and nothing else; everything below has an answer that is right until that type
/// has something of its own to say.
pub trait Service: Send + Sync + Sized + 'static {
    /// Start service-specific maintenance after construction and before serving.
    fn start(self: &Arc<Self>) -> Result<()> {
        Ok(())
    }

    /// The operations this machine type is the right place to send. Anything
    /// here that is not built yet answers `unimplemented`; anything else answers
    /// `bad request`, so a misdirected call is distinguishable from an unbuilt
    /// one.
    fn serves(&self) -> &'static [Op] {
        &[]
    }

    /// Everything but the control operations, which are answered here. The
    /// default is for a type that answers on the spot; override `handle` to
    /// answer later.
    fn answer(&self, process: &Process, request: &Req) -> Resp {
        unbuilt(process, self.serves(), request)
    }

    /// Answer, now or later. Whoever takes the `Reply` owes exactly one answer,
    /// whenever it has one — taking `Arc<Self>` so a handler can carry itself into
    /// the answer it is waiting for.
    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        reply.send(self.answer(process, &request));
    }

    /// Operations whose answer comes later, from another thread. The connection
    /// they arrive on is put on a batching sender, since the read loop will not
    /// be the one to send them.
    fn defers(&self, _op: Op) -> bool {
        false
    }

    /// Threads answering requests off the read loop. Zero means one per core.
    fn workers(&self) -> usize {
        0
    }

    /// Whether to answer this operation on the connection's read loop instead of
    /// handing it to a worker. Only for work too small to be worth a queue hop.
    fn inline(&self, op: Op) -> bool {
        // Reading log files is neither cheap nor urgent, so it goes to a worker.
        matches!(op, Op::Ping | Op::Status | Op::Shutdown | Op::Sites)
    }

    /// Lines appended to the common `status` reply.
    fn status(&self, _process: &Process) -> Vec<String> {
        Vec::new()
    }

    /// Called once, after the last connection, before the process exits.
    fn stop(&self, _process: &Process) -> Result<()> {
        Ok(())
    }
}

/// Whether a kind may be chosen in turn.
///
/// ⚠️ **A free function so that the test tests the rule and not a copy of it.** The rule
/// lives in one place, `Process::pick` calls it, and so does the test — which matters more
/// here than usual, since a test that restates a rule catches nothing when the rule moves.
pub fn pickable(kind: ServiceKind) -> Result<()> {
    match kind {
        ServiceKind::Dn => {
            Err(Error::new("a DN is chosen by the name it owns, not in turn — use Process::owner"))
        }
        _ => Ok(()),
    }
}

impl Process {
    pub fn load(process_config: &Path) -> Result<Process> {
        let config = ProcessConfig::load(process_config)?;
        let mut cluster = ClusterConfig::load(&config.cluster_config)?;
        if let Some(path) = &config.machine_identity {
            cluster
                .messaging
                .authentication
                .as_mut()
                .ok_or_else(|| Error::new("machine identity requires public enrollment"))?
                .load_identity(path)?;
        }
        cluster.messaging.expected_peer = Some(cluster.registrar()?.identity.clone());
        cluster.messaging.local_ring = cluster.ring_identities.get(&config.identity).cloned();
        cluster.messaging.ignore_ring_identity = config.ignore_ring_identity;
        let entry = cluster
            .service(&config.identity)
            .ok_or_else(|| {
                Error::new(format!(
                    "{} is not in the service directory of {}",
                    config.identity,
                    config.cluster_config.display()
                ))
            })?
            .clone();
        if entry.kind != config.kind {
            return Err(Error::new(format!(
                "{} is a {} here but a {} in the service directory",
                config.identity, config.kind, entry.kind
            )));
        }
        if matches!(entry.kind, ServiceKind::Ks | ServiceKind::Sn | ServiceKind::Dn)
            && !cluster.ring_identities.contains_key(&config.identity)
        {
            return Err(Error::new("missing ring identity in bootstrap configuration"));
        }
        std::fs::create_dir_all(&config.data_dir).context(config.data_dir.display())?;
        let run_dir = process_config.parent().unwrap_or(Path::new(".")).to_path_buf();
        crate::log::open(
            &run_dir.join("logs"),
            &config.identity,
            cluster.logging.build(&cluster.media_classes)?,
        )?;
        if config.ignore_ring_identity {
            eprintln!(
                "DANGER: ignore_ring_identity=true IS A GOOD WAY TO LOSE ALL YOUR DATA. Ring identity protection is DISABLED for debugging."
            );
            logn!(
                "DANGER: ring identity protection DISABLED",
                "A GOOD WAY TO LOSE ALL YOUR DATA: ignore_ring_identity=true"
            );
        }
        let peers =
            Peers::new(cluster.messaging.clone(), cluster.timeouts.request_ms.to_duration()?);
        Ok(Process {
            peers,
            assigned: RwLock::new(HashMap::new()),
            turn: AtomicU64::new(0),
            config,
            cluster,
            entry,
            run_dir,
            started: Instant::now(),
            stopping: AtomicBool::new(false),
            answered: AtomicU64::new(0),
            senders: AtomicU64::new(0),
        })
    }

    pub fn identity(&self) -> &str {
        &self.config.identity
    }

    pub fn address(&self) -> SocketAddr {
        self.config.listen
    }

    /// Where a peer is, which only KS knows.
    pub fn peer(&self, identity: &str) -> Result<SocketAddr> {
        if let Some(address) = self.assigned.read().expect("assigned peers").get(identity) {
            return Ok(*address);
        }
        self.lookup_peer(identity)
    }
    fn lookup_peer(&self, identity: &str) -> Result<SocketAddr> {
        if let Some(address) = self.cluster.fixed_address(identity) {
            return Ok(address);
        }
        let mut payload = crate::wire::StructWriter::new();
        payload.text(identity);
        let response = ask(
            self.cluster.ks.address,
            &Req::new(Op::Lookup).structure(payload),
            self.timeout(),
            &self.cluster.messaging,
        )?;
        match response.status {
            Status::Ok => match response.message() {
                Some(address) => address
                    .parse()
                    .map_err(|_| Error::new(format!("KS answered {address:?} for {identity}"))),
                None => Err(Error::new(format!("KS answered nothing for {identity}"))),
            },
            status => self
                .assigned
                .read()
                .expect("assigned peers")
                .get(identity)
                .copied()
                .ok_or_else(|| Error::new(format!("KS answered {status} for {identity}"))),
        }
    }

    pub fn assign_peer(&self, identity: String, address: SocketAddr) {
        self.assigned.write().expect("assigned peers").insert(identity, address);
    }

    /// Ask another process something and say what to do with the answer, holding
    /// nothing in the meantime. This is what a request being served uses.
    pub fn ask(
        &self,
        identity: &str,
        request: &Req,
        then: impl FnOnce(Resp) + Send + 'static,
    ) -> Result<()> {
        let peer = self.peers.get_refresh(identity, |reconnect| {
            if reconnect { self.lookup_peer(identity) } else { self.peer(identity) }
        })?;
        peer.ask(request, then)
    }

    /// Ask and wait, for a startup path with nothing else to get on with.
    pub fn call(&self, identity: &str, request: &Req) -> Result<Resp> {
        let peer = self.peers.get_refresh(identity, |reconnect| {
            if reconnect { self.lookup_peer(identity) } else { self.peer(identity) }
        })?;
        peer.call(request)
    }

    /// The next process of a kind, taken in turn. Round robin is not placement —
    /// it spreads without regard to fullness or failure domains — but it is
    /// honest striping until there is a placement solver to ask.
    ///
    /// ⚠️ **Refuses a DN, on purpose** (#106). A DN is chosen by the name it owns, never
    /// in turn, and the reason this is a refusal rather than a comment is that a comment
    /// did not work: §17.24.0 converted one of the three call sites, and the one it missed
    /// silently sent every append for one object to a different machine. The generic
    /// helper is where the mistake was cheap to make, so the generic helper is where it is
    /// now impossible — `owner` is the only way to a DN (`A-NAME-HAS-ONE-OWNER`).
    ///
    /// **There is no escape hatch.** One existed for `batch-create` while it still sent
    /// every name to one DN; #98 grouped them by owner, so the hatch went with the debt it
    /// was holding open.
    pub fn pick(&self, kind: ServiceKind) -> Result<String> {
        pickable(kind)?;
        let assigned: Vec<String> = self
            .assigned
            .read()
            .expect("assigned peers")
            .keys()
            .filter(|identity| {
                self.cluster.service(identity).is_some_and(|entry| entry.kind == kind)
            })
            .cloned()
            .collect();
        let of_kind: Vec<&str> = if assigned.is_empty() {
            self.cluster.services_of_kind(kind).map(|entry| entry.identity.as_str()).collect()
        } else {
            assigned.iter().map(String::as_str).collect()
        };
        if of_kind.is_empty() {
            return Err(Error::new(format!("no {} in the roster", kind.as_str().to_uppercase())));
        }
        let turn = self.turn.fetch_add(1, Ordering::Relaxed) as usize;
        Ok(of_kind[turn % of_kind.len()].to_string())
    }

    pub fn assigned_of_kind(&self, kind: ServiceKind) -> Vec<String> {
        self.assigned
            .read()
            .expect("assigned peers")
            .keys()
            .filter(|identity| {
                self.cluster.service(identity).is_some_and(|entry| entry.kind == kind)
            })
            .cloned()
            .collect()
    }

    /// The process of a kind that owns a name. ⚠️ **Not `pick`** — a name has an owner,
    /// and taking DNs in turn means a create and the lookup that follows it go to
    /// different machines, which with more than one DN loses the object. See
    /// `routing`, and #75 for the real thing.
    pub fn owner(&self, kind: ServiceKind, name: &str) -> Result<&str> {
        let of_kind: Vec<&ServiceEntry> = self.cluster.services_of_kind(kind).collect();
        if of_kind.is_empty() {
            return Err(Error::new(format!("no {} in the roster", kind.as_str().to_uppercase())));
        }
        Ok(&of_kind[routing::owner_of(name, of_kind.len())].identity)
    }

    /// Every process of a kind, in roster order, so a fan-out can address them all and
    /// an index into this list means the same thing to every caller.
    pub fn all_of(&self, kind: ServiceKind) -> Vec<&str> {
        self.cluster.services_of_kind(kind).map(|entry| entry.identity.as_str()).collect()
    }

    pub fn timeout(&self) -> Duration {
        self.cluster.timeouts.request_ms.to_duration().expect("validated request timeout")
    }

    /// Announce this process's port to KS, which is how anything else finds it.
    fn register(&self) -> Result<()> {
        let peer = Peer {
            identity: self.identity().to_string(),
            kind: self.entry.kind,
            address: self.address(),
            domain: self.entry.domain.clone(),
            media_class: self.entry.media_class.clone(),
            ring_identity: self.cluster.ring_identities.get(self.identity()).cloned(),
        };
        let response =
            ask(self.cluster.ks.address, &peer.request(), self.timeout(), &self.cluster.messaging)?;
        match response.status {
            Status::Ok => Ok(()),
            status => Err(Error::new(format!(
                "KS refused the registration: {status}, {}",
                response.message().unwrap_or_else(|| "no reason given".into())
            ))),
        }
    }

    /// Ask the accept loop to stop. It is blocked in `accept`, so set the flag
    /// and then connect once to wake it.
    fn stop_soon(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address());
    }

    fn ready_file(&self) -> PathBuf {
        self.run_dir.join(READY_FILE)
    }
}

/// A fixed set of threads answering requests, so a slow handler delays neither
/// the connection's reader nor another connection.
struct Pool {
    jobs: Sender<Job>,
}

type Job = Box<dyn FnOnce() + Send>;

impl Pool {
    fn new(threads: usize) -> Pool {
        let (jobs, rx) = channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..threads {
            let rx = Arc::clone(&rx);
            std::thread::spawn(move || {
                loop {
                    let job = match rx.lock().expect("job queue").recv() {
                        Ok(job) => job,
                        Err(_) => return,
                    };
                    job();
                }
            });
        }
        Pool { jobs }
    }

    fn run(&self, job: impl FnOnce() + Send + 'static) {
        let _ = self.jobs.send(Box::new(job));
    }
}

/// One request on a fresh connection, for the few calls a daemon makes itself.
pub fn ask(
    address: SocketAddr,
    request: &Req,
    timeout: Duration,
    config: &Messaging,
) -> Result<Resp> {
    let stream =
        TcpStream::connect_timeout(&address, timeout).context(format!("connecting {address}"))?;
    stream.set_read_timeout(Some(timeout))?;
    let config = config.clone();
    if config.authentication.is_some() && config.expected_peer.is_none() {
        return Err(Error::new("KS target identity missing"));
    }
    wire::connect(stream, &config)?.call(request)
}

/// The answer for an operation this machine type has not built, or should never
/// have been sent.
pub fn unbuilt(process: &Process, serves: &[Op], request: &Req) -> Resp {
    if serves.contains(&request.op) {
        Resp::error(Status::Unimplemented, format!("{} is not built yet", request.op))
    } else {
        Resp::error(
            Status::BadRequest,
            format!("{} does not serve {}", process.entry.kind.as_str().to_uppercase(), request.op),
        )
    }
}

/// This process's resident memory, which is what says whether a benchmark's
/// objects still fit in cache or in RAM.
pub fn rss() -> u64 {
    let pages = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
        .unwrap_or(0);
    pages * 4096
}

fn cores() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
}

/// Bind, announce readiness, and serve until asked to stop.
pub fn serve(process: Arc<Process>, service: Arc<impl Service>) -> Result<()> {
    let listener =
        TcpListener::bind(process.address()).context(format!("binding {}", process.address()))?;
    let address = listener.local_addr()?;
    let workers = if service.workers() == 0 { cores() } else { service.workers() };
    let pool = Arc::new(Pool::new(workers));
    logn!("listening", "address {address} workers {workers}");

    // KS cannot register with itself, and needs no one's permission to serve.
    if process.cluster.fixed_address(process.identity()).is_none() {
        if process.entry.kind == ServiceKind::Ks {
            // A registration now needs a committed Raft entry. Accept consensus
            // connections while registering, otherwise startup waits on itself.
            let registering = Arc::clone(&process);
            std::thread::Builder::new()
                .name("ks-registration".into())
                .spawn(move || {
                    let mut previous = None;
                    while !registering.stopping.load(Ordering::SeqCst) {
                        match registering.register() {
                            Ok(()) => {
                                logn!(
                                    "registered with KS",
                                    "at {}",
                                    registering.cluster.ks.address
                                );
                                return;
                            }
                            Err(error) => {
                                let error = error.to_string();
                                if previous.as_ref() != Some(&error) {
                                    logn!("KS registration pending", "error {}", error);
                                    previous = Some(error);
                                }
                            }
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                })
                .context("starting KS registration")?;
        } else {
            process.register()?;
            logn!("registered with KS", "at {}", process.cluster.ks.address);
        }
    }

    // Ready last: a process is ready when it can be found, not merely when it is
    // listening. KS is the bootstrap exception: it must serve Raft before registration commits.
    std::fs::write(
        process.ready_file(),
        format!("pid={}\naddress={address}\n", std::process::id()),
    )
    .context(process.ready_file().display())?;

    for connection in listener.incoming() {
        if process.stopping.load(Ordering::SeqCst) {
            break;
        }
        let stream = match connection {
            Ok(stream) => stream,
            Err(e) => {
                logw!("accept failed", "{e}");
                continue;
            }
        };
        let process = Arc::clone(&process);
        let service = Arc::clone(&service);
        let pool = Arc::clone(&pool);
        std::thread::spawn(move || {
            if let Err(e) = read_loop(&process, &service, &pool, stream) {
                // Notice until proven noisy: a connection ending is worth seeing,
                // and a workload of short-lived connections is what would demote it.
                logn!("connection ended", "{e}");
            }
        });
    }

    let stopped = service.stop(&process);
    let _ = std::fs::remove_file(process.ready_file());
    logn!("stopped");
    crate::log::flush();
    stopped
}

/// Read requests as fast as they arrive, answering each here or on a worker.
fn read_loop<S: Service>(
    process: &Arc<Process>,
    service: &Arc<S>,
    pool: &Pool,
    stream: TcpStream,
) -> Result<()> {
    let (mut input, output) = wire::accept(stream, &process.cluster.messaging)?;
    let policy = process.cluster.messaging.send;
    let out = Arc::new(match policy {
        SendMode::Sender => Out::sending(output),
        SendMode::Direct | SendMode::Adaptive => Out::direct(output),
    });

    loop {
        // A client that closed the connection is the normal case.
        let Ok(request) = input.request() else {
            out.close();
            return Ok(());
        };
        if let Some(auth) = &process.cluster.messaging.authentication {
            let allowed = match request.op {
                Op::Raft => match request.args.first() {
                    Some(Arg::Text(sender)) => {
                        process
                            .cluster
                            .service(sender)
                            .is_some_and(|entry| entry.kind == ServiceKind::Ks)
                            && auth.permits(input.peer_key(), sender)
                    }
                    _ => false,
                },
                Op::Register => Peer::from_args(&request.args)
                    .is_ok_and(|peer| auth.permits(input.peer_key(), &peer.identity)),
                Op::SnMasterKey => process
                    .cluster
                    .services_of_kind(ServiceKind::En)
                    .any(|entry| auth.permits(input.peer_key(), &entry.identity)),
                _ => true,
            };
            if !allowed {
                let response =
                    Resp::error(Status::BadRequest, "machine is not admitted for this operation")
                        .to(&request);
                out.put(&response, true)?;
                continue;
            }
        }
        // An answer that will come from another thread cannot be flushed by this
        // one, so the connection goes onto a sender first — the same reason a
        // pooled answer does.
        let deferred = service.defers(request.op);
        if (deferred || !service.inline(request.op))
            && policy == SendMode::Adaptive
            && out.batch.get().is_none()
        {
            out.hand_to_sender();
            logn!(
                "switched to a batching sender because an answer will not come from the read loop"
            );
            process.senders.fetch_add(1, Ordering::Relaxed);
        }

        let arrived = input.arrived();
        if service.inline(request.op) {
            // Nothing more in hand means the next read would block, so send. A
            // deferred answer is sent whenever it arrives, which is always a
            // moment this loop knows nothing about.
            let settled = deferred || !input.more();
            answer_here(process, service, &out, request, settled, arrived);
            continue;
        }
        let process = Arc::clone(process);
        let service = Arc::clone(service);
        let out = Arc::clone(&out);
        pool.run(move || answer_here(&process, &service, &out, request, true, arrived));
    }
}

/// The right to answer one request, once. It may be carried to another thread and
/// answered when that thread has something to say, which is how a request waits
/// without holding a worker.
///
/// Sending the answer is also where the request is logged, one line, because that
/// is the only moment that knows how long it took — and how long it took is what
/// a live-site argument is about.
pub struct Reply {
    out: Arc<Out>,
    id: u32,
    op: Op,
    /// Whether the caller knows nothing more is in hand, so a direct writer
    /// should send what it has.
    settled: bool,
    /// When the batch this request arrived in was read, if anything is going to
    /// read the answer's line. One clock read serves every request in a batch.
    started: Option<Instant>,
    /// What the line calls this request: by default whatever its first argument
    /// is, which for anything about an object is the name.
    about: String,
}

impl Reply {
    #[cfg(feature = "testing")]
    pub fn new_test(out: Arc<Out>) -> Reply {
        Reply { out, id: 0, op: Op::Ping, settled: true, started: None, about: String::new() }
    }

    /// Say what this request was about, where the service knows better than its
    /// first argument does.
    pub fn about(&mut self, what: impl std::fmt::Display) {
        if self.started.is_some() {
            self.about = what.to_string();
        }
    }

    pub fn send(self, response: Resp) {
        let status = response.status;
        let response = Resp { id: self.id, ..response };
        if let Err(e) = self.out.put(&response, self.settled) {
            logw!("could not answer", "op {},{e}", self.op);
        }
        if let Some(started) = self.started {
            let us = started.elapsed().as_micros();
            // An answer that went wrong gets its own constant, not just a
            // different value: a histogram separates kinds by constant, so one
            // refusal among a million successes is a row of its own — findable
            // without knowing to look for it. That is what the rare end is for.
            match status {
                Status::Ok => {
                    logi!("answered", "op {},about {},us {us}", self.op, self.about);
                }
                _ => {
                    // Notice, not info: it is rare, and until a histogram proves
                    // otherwise a new line belongs in both logs.
                    logn!(
                        "answered with a problem",
                        "op {},status {status},about {},us {us}",
                        self.op,
                        self.about
                    );
                }
            }
        }
    }
}

/// A connection's outgoing bytes, in two buffers: producers append to one while
/// the sender writes the other. The next batch is whatever piled up during the
/// last write, so batch size follows load with nothing to tune and no timer.
pub(crate) struct Batch {
    pub(crate) filling: Mutex<Vec<u8>>,
    pub(crate) ready: Condvar,
    pub(crate) open: AtomicBool,
}

impl Batch {
    fn new() -> Batch {
        Batch {
            filling: Mutex::new(Vec::with_capacity(BUF_BYTES)),
            ready: Condvar::new(),
            open: AtomicBool::new(true),
        }
    }

    /// Frame outside the lock, then hold it only for the copy. Wake the sender
    /// only when it has something to wake for.
    fn put(&self, response: &Resp) -> Result<()> {
        SCRATCH.with(|scratch| {
            let mut framed = scratch.borrow_mut();
            framed.clear();
            put_response(&mut framed, response)?;
            let mut filling = self.filling.lock().expect("connection");
            let woke = filling.is_empty();
            filling.extend_from_slice(&framed);
            drop(filling);
            if woke {
                self.ready.notify_one();
            }
            Ok(())
        })
    }

    fn close(&self) {
        self.open.store(false, Ordering::SeqCst);
        self.ready.notify_one();
    }

    /// Take everything waiting, leaving the caller's empty buffer in its place.
    /// Returns false when the connection is done.
    fn take(&self, buf: &mut Vec<u8>) -> bool {
        let mut filling = self.filling.lock().expect("connection");
        while filling.is_empty() {
            if !self.open.load(Ordering::SeqCst) {
                return false;
            }
            filling = self.ready.wait(filling).expect("connection");
        }
        std::mem::swap(&mut *filling, buf);
        true
    }
}

fn run_sender(mut wire: Wire, batch: Arc<Batch>) {
    let mut buf = Vec::with_capacity(BUF_BYTES);
    while batch.take(&mut buf) {
        // One segment per batch: sealing happens here, off every other thread.
        if wire.send(&mut buf).is_err() {
            return;
        }
    }
}

/// Where a connection's answers go. Starts as one of the two, and may move from
/// direct to a sender exactly once — after which `batch` is set for good, so the
/// hot path is one relaxed load.
pub struct Out {
    pub(crate) direct: Mutex<Option<Direct>>,
    pub(crate) batch: OnceLock<Arc<Batch>>,
}

/// The read loop's own writer: messages accumulate here and are sealed when it
/// says nothing more is in hand.
#[cfg(not(feature = "testing"))]
pub(crate) struct Direct {
    pub(crate) wire: Wire,
    pub(crate) plain: Vec<u8>,
}

#[cfg(feature = "testing")]
pub struct Direct {
    pub(crate) wire: Wire,
    pub(crate) plain: Vec<u8>,
}

impl Out {
    #[cfg(feature = "testing")]
    pub fn direct(wire: Wire) -> Out {
        Out {
            direct: Mutex::new(Some(Direct { wire, plain: Vec::with_capacity(BUF_BYTES) })),
            batch: OnceLock::new(),
        }
    }

    #[cfg(not(feature = "testing"))]
    pub(crate) fn direct(wire: Wire) -> Out {
        Out {
            direct: Mutex::new(Some(Direct { wire, plain: Vec::with_capacity(BUF_BYTES) })),
            batch: OnceLock::new(),
        }
    }

    fn sending(wire: Wire) -> Out {
        let out = Out::direct(wire);
        out.hand_to_sender();
        out
    }

    /// Give the writer to a sender thread. Only the read loop calls this, and
    /// only before it dispatches the first answer it will not write itself, so
    /// nothing else is writing while it happens.
    fn hand_to_sender(&self) {
        let Some(mut direct) = self.direct.lock().expect("connection").take() else {
            return;
        };
        // Anything already accumulated goes first, so the switch cannot reorder.
        let _ = direct.wire.send(&mut direct.plain);
        let batch = Arc::new(Batch::new());
        let sending = Arc::clone(&batch);
        std::thread::spawn(move || run_sender(direct.wire, sending));
        let _ = self.batch.set(batch);
    }

    /// `settled` means the caller knows nothing more is coming right now, so a
    /// direct writer should flush. It means nothing to a sender, which decides
    /// for itself.
    fn put(&self, response: &Resp, settled: bool) -> Result<()> {
        if let Some(batch) = self.batch.get() {
            return batch.put(response);
        }
        let mut direct = self.direct.lock().expect("connection");
        let direct = direct.as_mut().ok_or_else(|| Error::new("the writer moved mid-answer"))?;
        put_response(&mut direct.plain, response)?;
        if settled {
            direct.wire.send(&mut direct.plain)?;
        }
        Ok(())
    }

    fn close(&self) {
        match self.batch.get() {
            Some(batch) => batch.close(),
            None => {
                if let Some(direct) = self.direct.lock().expect("connection").as_mut() {
                    let _ = direct.wire.send(&mut direct.plain);
                }
            }
        }
    }
}

/// Answer one request, or hand the answering to whoever the service asks. Pooled
/// work sends for itself, since it finishes at a moment the read loop knows
/// nothing about; inline work leaves that to the read loop, which knows whether
/// more requests are already in hand.
fn answer_here<S: Service>(
    process: &Arc<Process>,
    service: &Arc<S>,
    out: &Arc<Out>,
    request: Req,
    settled: bool,
    arrived: Instant,
) {
    process.answered.fetch_add(1, Ordering::Relaxed);
    let op = request.op;
    // The subject is only for the answer's line, so it is not paid when the level
    // is above info. The arrival time was read once for the whole batch.
    let logging = crate::log::enabled(crate::log::Level::Info);
    let mut reply = Reply {
        out: Arc::clone(out),
        id: request.id,
        op,
        settled,
        started: logging.then_some(arrived),
        about: String::new(),
    };
    if logging && let Some(arg) = request.args.first() {
        reply.about(arg);
    }
    match op {
        Op::Ping => reply.send(ping(process, &request)),
        Op::Status => reply.send(Resp::text(status(process, &**service))),
        Op::Shutdown => reply.send(Resp::ok()),
        Op::LogHistogram | Op::LogGrep | Op::LogLines => reply.send(read_logs(process, &request)),
        Op::AssignPeers if process.entry.kind != ServiceKind::Ks => {
            reply.send(assign_peers(process, &request))
        }
        Op::Sites => reply.send(Resp::text(
            crate::log::histogram()
                .iter()
                .map(|(count, stride, where_)| {
                    let sampled =
                        if *stride > 1 { format!("1 in {stride}") } else { "all".to_string() };
                    format!("{count:>12} to info {sampled:>10}  {where_}")
                })
                .collect::<Vec<_>>()
                .join("\n"),
        )),
        _ => service.handle(process, request, reply),
    }
    if op == Op::Shutdown {
        logn!("shutdown requested");
        process.stop_soon();
    }
}

fn assign_peers(process: &Process, request: &Req) -> Resp {
    let (mut payload, rest) =
        match structure(&request.args, Op::AssignPeers, "an assign-peers request") {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
    let source = match payload.text() {
        Ok(value) => value,
        Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
    };
    if source != process.identity() {
        return Resp::error(
            Status::BadRequest,
            format!("assignment for {source} reached {}", process.identity()),
        );
    }
    let count = match payload.number_u64() {
        Ok(value) => value,
        Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
    };
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
        let address = match payload.text().and_then(|text| {
            text.parse().map_err(|_| Error::new(format!("invalid address for {identity}")))
        }) {
            Ok(value) => value,
            Err(e) => {
                return Resp::error(
                    Status::BadRequest,
                    format!("got through {n} destinations: {e}"),
                );
            }
        };
        process.assign_peer(identity, address);
    }
    if !rest.is_empty() || payload.done().is_err() {
        return Resp::error(
            Status::BadRequest,
            format!("got through {count} destinations, then found trailing data"),
        );
    }
    Resp::text(format!("{} got through {count} destinations", process.identity()))
}

/// Answer one of the three questions about this process's own log files. It runs
/// here because the machine holding the files runs signed code, and this process
/// is already signed and already there — the standalone `logs` tool is the same
/// library, for when the process is not answering.
fn read_logs(process: &Process, request: &Req) -> Resp {
    let (which, from, to, what, limit) = {
        let (mut payload, rest) = match structure(&request.args, request.op, "a log request") {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if !rest.is_empty() {
            return Resp::error(Status::BadRequest, "a log request has trailing arguments");
        }
        let which = match payload.text() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let from = match payload.text() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let to = match payload.text() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let (what, limit) = match request.op {
            Op::LogHistogram => (None, 100),
            Op::LogGrep => {
                let what = match payload.text() {
                    Ok(value) => value,
                    Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
                };
                let limit = match payload.number_u64() {
                    Ok(value) => value as usize,
                    Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
                };
                (Some(what), limit)
            }
            Op::LogLines => {
                let limit = match payload.number_u64() {
                    Ok(value) => value as usize,
                    Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
                };
                (None, limit)
            }
            _ => return Resp::error(Status::BadRequest, "not a log command"),
        };
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        (which, from, to, what, limit)
    };

    // The info log by name, so a reader can ask for the long-lived one.
    let which = match which.as_str() {
        "info" => format!("{}-info", process.identity().to_lowercase()),
        _ => process.identity().to_lowercase(),
    };
    let dir = crate::log::dir_of(&process.run_dir);
    // Everything this process has said is on its way to those files; without
    // this, the last few hundred lines are missing from every answer.
    crate::log::flush();
    // All of it unless the caller says otherwise: reading a log takes as long as
    // it takes, and doing it serially is how it stays out of the way of the work.
    let budget = u64::MAX;
    let logs = match crate::logread::Logs::open(&dir, &which, budget) {
        Ok(logs) => logs,
        Err(e) => return Resp::error(Status::Internal, e.to_string()),
    };

    // Each of the three takes the same window: a rare thing is looked for in the
    // minute it happened.
    let read = match request.op {
        Op::LogHistogram => logs.histogram(&from, &to),
        Op::LogGrep => logs.grep(what.as_deref().unwrap_or_default(), &from, &to, limit),
        _ => logs.range(&from, &to, limit),
    };
    match read {
        Err(e) => Resp::error(Status::Internal, e.to_string()),
        // The count of lines scanned comes back too: an answer with nothing in it
        // means something different if it read nothing than if it read millions.
        Ok((seen, lines)) => {
            let mut payload = StructWriter::new();
            payload.number_u64(seen);
            payload.text(&lines.join("\n"));
            Resp { id: 0, status: Status::Ok, args: vec![payload.finish(Op::LogResp)] }
        }
    }
}

/// A ping asks for a reply of a given size, so that message size is a knob and
/// not a constant. Bare, it answers in words.
fn ping(process: &Process, request: &Req) -> Resp {
    let size = match request.args.first() {
        None => 0,
        Some(Arg::Number(size)) => *size as usize,
        Some(other) => {
            return Resp::error(Status::BadRequest, format!("ping takes a size, not {other}"));
        }
    };
    if size == 0 {
        // At debug, so it is off by default: a line per ping costs several times
        // what the ping does. Turn the level down to reproduce that measurement.
        logd!("pinged", "reply size {size}");
        // And once a second regardless, which says the same thing for nothing.
        logsec!("pinged", "reply size {size}");
        return Resp::text(format!("{} is alive", process.identity()));
    }
    // Random padding is the honest default; the repetitive kind exists to give
    // compression something to find.
    let dull = matches!(request.args.get(1), Some(Arg::Flag(true)));
    match if dull { repetition(size) } else { noise(size) } {
        Some(bytes) => Resp::bytes(bytes),
        None => Resp::error(
            Status::BadRequest,
            format!("a {size}-byte reply is over the {MAX_FRAME_BYTES}-byte frame limit"),
        ),
    }
}

/// Padding a compressor can do something with: a short phrase repeated. Cut from
/// one block, which is honest here in a way it was not for random padding —
/// repetition is the point.
fn repetition(size: usize) -> Option<Vec<u8>> {
    static BLOCK: OnceLock<Vec<u8>> = OnceLock::new();
    let block = BLOCK.get_or_init(|| {
        const WORDS: &[u8] = b"the same words over and over, as a batch of small messages is. ";
        WORDS.iter().copied().cycle().take(MAX_FRAME_BYTES).collect()
    });
    block.get(..size).map(|slice| slice.to_vec())
}

/// Random bytes for a reply body, fresh each time, from a generator this thread
/// owns. Handing out slices of one block was cheaper but wrong: every reply was
/// then the same bytes, so a batch of them compressed, and a measurement of
/// compression measured the padding instead of the wire. What generating them
/// costs is measurable on its own — `laws generator`.
fn noise(size: usize) -> Option<Vec<u8>> {
    if size == 0 || size > MAX_FRAME_BYTES {
        return None;
    }
    NOISE.with(|rng| {
        let mut body = vec![0u8; size];
        rng.borrow_mut().fill_bytes(&mut body);
        Some(body)
    })
}

fn status(process: &Process, service: &impl Service) -> String {
    let mut lines = vec![
        format!("identity {}", process.identity()),
        format!("kind {}", process.entry.kind),
        format!("cluster {}", process.cluster.cluster_name),
        format!("address {}", process.address()),
        format!("pid {}", std::process::id()),
        format!("workers {}", if service.workers() == 0 { cores() } else { service.workers() }),
        format!("uptime_seconds {}", process.started.elapsed().as_secs()),
        format!("answered {}", process.answered.load(Ordering::Relaxed)),
        format!("rss_bytes {}", rss()),
        format!("send {:?}", process.cluster.messaging.send),
        {
            let (plain, wire) = crate::seal::totals();
            format!("sealed_plain {plain} sealed_wire {wire}")
        },
        {
            let (lines, files) = crate::log::written();
            let ratio = lines as f64 / files.max(1) as f64;
            format!("log_lines {lines} log_files {files} log_ratio {ratio:.1}")
        },
        format!("log_block {}", crate::log::block()),
        format!("senders {}", process.senders.load(Ordering::Relaxed)),
        format!("domain {}", process.entry.domain.join("/")),
    ];
    if let Some(media_class) = &process.entry.media_class {
        lines.push(format!("media_class {media_class}"));
    }
    if let (Ok(scn), Ok(stalled)) = (crate::scn::current(), crate::scn::is_stalled()) {
        lines.push(format!("scn {scn}"));
        lines.push(format!("scn_stalled {stalled}"));
    }
    lines.extend(service.status(process));
    lines.join("\n")
}

/// Every daemon's `main`: one argument, the path to its process configuration.
pub fn main<S: Service>(build: impl FnOnce(&Process) -> Result<S>) -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or_else(|| Error::new("usage: <daemon> <process-config.toml>"))?;
    if args.next().is_some() {
        return Err(Error::new("usage: <daemon> <process-config.toml>"));
    }
    let process = Arc::new(Process::load(Path::new(&path))?);
    crate::pool::initialize(process.config.pool_max_blocks, process.config.pool_initial_blocks)?;
    crate::scn::initialize(process.cluster.scn)?;
    let service = Arc::new(build(&process)?);
    service.start()?;
    serve(process, service)
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    /// ⚠️ **The spot #106 was at.** A DN is chosen by the name it owns, and the generic
    /// round-robin helper must not be able to return one — a comment saying so did not stop
    /// one of three call sites from using it, and that call site sent every append for one
    /// object to a different machine (§17.30).
    #[test]
    fn a_dn_cannot_be_picked_in_turn() {
        let said = pickable(ServiceKind::Dn).expect_err("not in turn").to_string();
        assert!(said.contains("owner"), "and it says what to use instead: {said}");
    }

    /// Every other kind still takes its turn: nothing owns them, so round robin is the
    /// honest answer and the refusal must not spread to them.
    #[test]
    fn other_kinds_still_take_turns() {
        for kind in [ServiceKind::En, ServiceKind::Sn, ServiceKind::Fe, ServiceKind::Ks] {
            assert!(pickable(kind).is_ok(), "{kind:?} is picked in turn");
        }
    }
}
