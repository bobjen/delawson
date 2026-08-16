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
//!
//! Neither waits for anything, so neither adds latency: both send at the moment
//! the alternative would have been to block.
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

use crate::config::{ClusterConfig, ProcessConfig, ServiceEntry};
use crate::random::Rng;
use crate::registry::Peer;
use crate::wire::{
    Arg, BUF_BYTES, Chan, MAX_FRAME_BYTES, Op, Request, Response, Status, frame_into, read_request,
    read_response, write_request, write_response,
};
use crate::{Context, Error, Result};
use std::cell::RefCell;
use std::io::{BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const READY_FILE: &str = "ready";

thread_local! {
    /// Framing scratch for a worker, so answering allocates nothing.
    static SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUF_BYTES));
}

pub struct Process {
    pub config: ProcessConfig,
    pub cluster: ClusterConfig,
    pub entry: ServiceEntry,
    pub run_dir: PathBuf,
    pub started: Instant,
    stopping: AtomicBool,
    /// Requests answered, so a client's throughput claim can be checked against
    /// what the server thinks it did.
    answered: AtomicU64,
}

/// What a machine type adds to the common lifecycle.
pub trait Service: Send + Sync + 'static {
    /// Everything but the control operations, which are answered here.
    fn handle(&self, process: &Process, request: &Request) -> Response;

    /// Whether to answer this operation on the connection's read loop instead of
    /// handing it to a worker. Only for work too small to be worth a queue hop.
    fn inline(&self, op: Op) -> bool {
        matches!(op, Op::Ping | Op::Status | Op::Shutdown)
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

impl Process {
    pub fn load(process_config: &Path) -> Result<Process> {
        let config = ProcessConfig::load(process_config)?;
        let cluster = ClusterConfig::load(&config.cluster_config)?;
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
        std::fs::create_dir_all(&config.data_dir).context(config.data_dir.display())?;
        let run_dir = process_config.parent().unwrap_or(Path::new(".")).to_path_buf();
        Ok(Process {
            config,
            cluster,
            entry,
            run_dir,
            started: Instant::now(),
            stopping: AtomicBool::new(false),
            answered: AtomicU64::new(0),
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
        if let Some(address) = self.cluster.fixed_address(identity) {
            return Ok(address);
        }
        let response =
            ask(self.cluster.ks.address, &Request::new(Op::Lookup).text(identity), self.timeout())?;
        match response.status {
            Status::Ok => match response.message() {
                Some(address) => address
                    .parse()
                    .map_err(|_| Error::new(format!("KS answered {address:?} for {identity}"))),
                None => Err(Error::new(format!("KS answered nothing for {identity}"))),
            },
            status => Err(Error::new(format!("KS answered {status} for {identity}"))),
        }
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.cluster.timeouts.request_ms)
    }

    /// Announce this process's port to KS, which is how anything else finds it.
    fn register(&self) -> Result<()> {
        let peer = Peer {
            identity: self.identity().to_string(),
            kind: self.entry.kind,
            address: self.address(),
            domain: self.entry.domain.clone(),
            media_class: self.entry.media_class.clone(),
        };
        let response = ask(self.cluster.ks.address, &peer.request(), self.timeout())?;
        match response.status {
            Status::Ok => Ok(()),
            status => Err(Error::new(format!(
                "KS refused the registration: {status}, {}",
                response.message().unwrap_or("no reason given")
            ))),
        }
    }

    pub fn log(&self, message: impl std::fmt::Display) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        eprintln!("{now} {} {message}", self.config.identity);
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
pub fn ask(address: SocketAddr, request: &Request, timeout: Duration) -> Result<Response> {
    let mut stream =
        TcpStream::connect_timeout(&address, timeout).context(format!("connecting {address}"))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(timeout))?;
    write_request(&mut stream, request)?;
    stream.flush()?;
    read_response(&mut stream)
}

fn workers() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
}

/// Bind, announce readiness, and serve until asked to stop.
pub fn serve(process: Arc<Process>, service: Arc<impl Service>) -> Result<()> {
    let listener =
        TcpListener::bind(process.address()).context(format!("binding {}", process.address()))?;
    let address = listener.local_addr()?;
    let pool = Arc::new(Pool::new(workers()));
    process.log(format!("listening on {address} with {} workers", workers()));

    // KS cannot register with itself, and needs no one's permission to serve.
    if process.cluster.fixed_address(process.identity()).is_none() {
        process.register()?;
        process.log(format!("registered with KS at {}", process.cluster.ks.address));
    }

    // Ready last: a process is ready when it can be found, not merely when it is
    // listening.
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
                process.log(format!("accept failed: {e}"));
                continue;
            }
        };
        let process = Arc::clone(&process);
        let service = Arc::clone(&service);
        let pool = Arc::clone(&pool);
        std::thread::spawn(move || {
            if let Err(e) = read_loop(&process, &service, &pool, stream) {
                process.log(format!("connection ended: {e}"));
            }
        });
    }

    let stopped = service.stop(&process);
    let _ = std::fs::remove_file(process.ready_file());
    process.log("stopped");
    stopped
}

/// Read requests as fast as they arrive, answering each here or on a worker.
fn read_loop<S: Service>(
    process: &Arc<Process>,
    service: &Arc<S>,
    pool: &Pool,
    stream: TcpStream,
) -> Result<()> {
    let chan = Chan::new(stream)?;
    let mut input = chan.input;
    let out = if process.cluster.messaging.batch_sender {
        let batch = Arc::new(Batch::new());
        let sending = Arc::clone(&batch);
        std::thread::spawn(move || run_sender(chan.output, sending));
        Arc::new(Out::Queued(batch))
    } else {
        Arc::new(Out::Direct(Mutex::new(chan.output)))
    };

    loop {
        // A client that closed the connection is the normal case.
        let Ok(request) = read_request(&mut input) else {
            out.close();
            return Ok(());
        };
        if service.inline(request.op) {
            // Nothing more in hand means the next read would block, so send.
            let settled = input.buffer().is_empty();
            reply(process, &**service, &out, request, settled);
            continue;
        }
        let process = Arc::clone(process);
        let service = Arc::clone(service);
        let out = Arc::clone(&out);
        // A worker cannot see what else has arrived, so with a direct writer it
        // must send its own answer; with a sender, it need not.
        pool.run(move || reply(&process, &*service, &out, request, true));
    }
}

/// A connection's outgoing bytes, in two buffers: producers append to one while
/// the sender writes the other. The next batch is whatever piled up during the
/// last write, so batch size follows load with nothing to tune and no timer.
struct Batch {
    filling: Mutex<Vec<u8>>,
    ready: Condvar,
    open: AtomicBool,
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
    fn put(&self, response: &Response) -> Result<()> {
        SCRATCH.with(|scratch| {
            let mut framed = scratch.borrow_mut();
            framed.clear();
            frame_into(&mut framed, |buf| response.encode(buf))?;
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

fn run_sender(mut writer: BufWriter<TcpStream>, batch: Arc<Batch>) {
    let mut buf = Vec::with_capacity(BUF_BYTES);
    while batch.take(&mut buf) {
        let sent = writer.write_all(&buf).and_then(|()| writer.flush());
        buf.clear();
        if sent.is_err() {
            return;
        }
    }
}

/// Where a connection's answers go.
enum Out {
    /// Written by whoever produced them, coalescing only within one thread.
    Direct(Mutex<BufWriter<TcpStream>>),
    /// Handed to this connection's sender, which batches whatever has arrived.
    Queued(Arc<Batch>),
}

impl Out {
    /// `settled` means the caller knows nothing more is coming right now, so a
    /// direct writer should flush. It means nothing to a sender, which decides
    /// for itself.
    fn put(&self, response: &Response, settled: bool) -> Result<()> {
        match self {
            Out::Direct(writer) => {
                let mut writer = writer.lock().expect("connection");
                write_response(&mut *writer, response)?;
                if settled {
                    writer.flush()?;
                }
                Ok(())
            }
            Out::Queued(batch) => batch.put(response),
        }
    }

    fn close(&self) {
        match self {
            Out::Direct(writer) => {
                let _ = writer.lock().expect("connection").flush();
            }
            Out::Queued(batch) => batch.close(),
        }
    }
}

fn reply(process: &Process, service: &impl Service, out: &Out, request: Request, settled: bool) {
    let response = answer(process, service, &request).to(&request);
    process.answered.fetch_add(1, Ordering::Relaxed);
    if let Err(e) = out.put(&response, settled) {
        process.log(format!("could not answer {}: {e}", request.op));
    }
    if request.op == Op::Shutdown {
        process.log("shutdown requested");
        process.stop_soon();
    }
}

fn answer(process: &Process, service: &impl Service, request: &Request) -> Response {
    match request.op {
        Op::Ping => ping(process, request),
        Op::Status => Response::text(status(process, service)),
        Op::Shutdown => Response::ok(),
        _ => service.handle(process, request),
    }
}

/// A ping asks for a reply of a given size, so that message size is a knob and
/// not a constant. Bare, it answers in words.
fn ping(process: &Process, request: &Request) -> Response {
    let size = match request.args.first() {
        None => 0,
        Some(Arg::Number(size)) => *size as usize,
        Some(other) => {
            return Response::error(Status::BadRequest, format!("ping takes a size, not {other}"));
        }
    };
    if size == 0 {
        return Response::text(format!("{} is alive", process.identity()));
    }
    match noise(size) {
        Some(bytes) => Response { id: 0, status: Status::Ok, args: vec![Arg::Bytes(bytes)] },
        None => Response::error(
            Status::BadRequest,
            format!("a {size}-byte reply is over the {MAX_FRAME_BYTES}-byte frame limit"),
        ),
    }
}

/// Random bytes for a reply body, cut from one block generated at first use.
/// Generating them per reply would measure this generator instead of the
/// message path, and zeros would let compression and the kernel cheat.
fn noise(size: usize) -> Option<Vec<u8>> {
    static BLOCK: OnceLock<Vec<u8>> = OnceLock::new();
    let block = BLOCK.get_or_init(|| {
        let mut block = vec![0u8; MAX_FRAME_BYTES];
        Rng::new().fill(&mut block);
        block
    });
    block.get(..size).map(|slice| slice.to_vec())
}

fn status(process: &Process, service: &impl Service) -> String {
    let mut lines = vec![
        format!("identity {}", process.identity()),
        format!("kind {}", process.entry.kind),
        format!("cluster {}", process.cluster.cluster_name),
        format!("address {}", process.address()),
        format!("pid {}", std::process::id()),
        format!("workers {}", workers()),
        format!("uptime_seconds {}", process.started.elapsed().as_secs()),
        format!("answered {}", process.answered.load(Ordering::Relaxed)),
        format!("batch_sender {}", process.cluster.messaging.batch_sender),
        format!("domain {}", process.entry.domain.join("/")),
    ];
    if let Some(media_class) = &process.entry.media_class {
        lines.push(format!("media_class {media_class}"));
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
    let service = Arc::new(build(&process)?);
    serve(process, service)
}
