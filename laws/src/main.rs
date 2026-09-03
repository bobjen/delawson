//! `laws`, the client: every command the store will answer, whether or not
//! anything answers it yet. A command that reaches a service which has not built
//! it comes back `unimplemented`, and a command with no service to send it to
//! fails locally saying so.
//!
//! The client holds no persistent state. All it needs is the cluster
//! configuration, to find the service directory.

mod client;

use clap::{Parser, Subcommand, ValueEnum};
use client::{Conn, Stats, Tally};
use common::block::{Holds, expand, squeeze};
use common::config::Compress;
use common::config::{ClusterConfig, Messaging, ServiceKind};
use common::framing::Framing;
use common::ids::{ObjectId, RangeId, StreamId};
use common::info::{Batch, Described, Expecting, ExtentRow, ObjectName, Shape, StreamRow, UNKNOWN};
use common::placement::{self, Expansion};

use common::random::Rng;
use common::range::{Budget, Covered, Detail, Piece, Range, Start, Want};
use common::time::TimeSec;
use common::wire::{Arg, Op, Req, Resp, Status, structure};
use common::{Error, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "laws", version, about = "The DeLawson client")]
struct Cli {
    /// The cluster configuration, which is where the service directory lives.
    #[arg(long, env = "LAWS_CONFIG", default_value = "config/cluster.toml", global = true)]
    config: PathBuf,

    /// Which process to talk to. Control commands accept any identity; the rest
    /// go to an FE, since that is the only thing a client talks to in
    /// production. Anything but KS is looked up in KS's registry.
    #[arg(long, global = true)]
    to: Option<String>,

    /// Talk straight to this address, skipping the registry. For a process that
    /// has not registered, or a KS that is not answering.
    #[arg(long, global = true)]
    at: Option<SocketAddr>,

    #[command(subcommand)]
    cmd: Cmd,
}

/// The `Detail` bits, as a command-line choice. Mirrors `common::range::Detail` rather than
/// replacing it — the bit values are the wire contract and live there.
#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Want1 {
    Streams,
    Extents,
    Instances,
    Capabilities,
    Counts,
    Flags,
}

impl Want1 {
    fn detail(self) -> Detail {
        match self {
            Want1::Streams => Detail::Streams,
            Want1::Extents => Detail::Extents,
            Want1::Instances => Detail::Instances,
            Want1::Capabilities => Detail::Capabilities,
            Want1::Counts => Detail::Counts,
            Want1::Flags => Detail::Flags,
        }
    }
}

/// What a run of `--want` choices comes to, with implications applied.
fn asking(want: &[Want1], limit: u64, from: u64) -> Want {
    let mut asked = Want { detail: 0, limit, from: Start::Record(from), ..Want::plain() };
    for one in want {
        asked = asked.with(one.detail());
    }
    asked
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Flavor {
    /// Records addressed by 64-bit record number; inserts only at the end.
    Unkeyed,
    /// Records addressed by user key; inserts anywhere.
    Keyed,
}

#[derive(Subcommand)]
enum Cmd {
    /// Round trip a message. With --count, report the timing.
    Ping {
        /// Round trips per connection.
        #[arg(long, default_value_t = 1)]
        count: usize,
        /// Connections, each on its own thread, all pinging at once.
        #[arg(long, default_value_t = 1)]
        conns: usize,
        /// Bytes of padding to ask for in the reply. Zero answers in words;
        /// anything else makes message size the variable.
        #[arg(long, default_value_t = 0)]
        size: u64,
        /// Pad with repetitive bytes instead of random ones, so compression has
        /// something to find.
        #[arg(long)]
        dull: bool,
        /// Open a fresh connection for each round trip.
        #[arg(long)]
        reconnect: bool,
    },
    /// Ping from several clients at once: warm up, then measure throughput and
    /// latency for a while.
    Bench {
        /// Connections pinging in parallel, one thread each.
        #[arg(long, default_value_t = 1)]
        clients: usize,
        /// Seconds of unmeasured pinging first.
        #[arg(long, default_value_t = 2)]
        warmup: u64,
        /// Bytes of padding in each reply.
        #[arg(long, default_value_t = 0)]
        size: u64,
        /// Pad with repetitive bytes instead of random ones.
        #[arg(long)]
        dull: bool,
        /// Independent requests each client keeps in flight. Above one, pending
        /// messages leave in one syscall — the only kind of batching there is.
        #[arg(long, default_value_t = 1)]
        depth: usize,
        /// Seconds to measure.
        #[arg(long, default_value_t = 10)]
        seconds: u64,
        /// How many times to measure. One number is not a measurement: the spread
        /// is what says whether a difference means anything.
        #[arg(long, default_value_t = 3)]
        trials: usize,
    },
    /// What a process says about itself.
    Status,
    /// Ask a process to stop.
    Shutdown {
        /// Every configured process, in reverse cold-start order.
        #[arg(long)]
        all: bool,
    },
    /// Perform the unlocking ceremony, without which KS can decrypt nothing.
    Unlock,
    /// The daily per-SN key an EN needs to decrypt that SN's extents.
    SnMasterKey {
        #[arg(long)]
        sn: u64,
        /// Days since the epoch, which is the creation second over 86,400.
        #[arg(long)]
        day: u64,
    },
    /// Generate today's master key.
    RotateMasterKey,

    /// The configured roster, in cold-start order. Needs no running cluster.
    Services,
    /// Every process KS knows about, with the port it registered.
    Registry,
    /// What a process logs, by site, busiest first: the live counters.
    Sites,
    /// A histogram of a process's log files: a row per place in the code that said
    /// anything, most frequent first. Read the bottom for what happened once, which
    /// is how a rare event is found without knowing what to look for; then grep for
    /// that row.
    LogHistogram {
        /// Narrow it to a window, which is usually what a rare event wants.
        #[arg(long, default_value = "")]
        from: String,
        #[arg(long, default_value = "")]
        to: String,
        /// Which log: the full one, or "info" for the one reaching further back.
        #[arg(long, default_value = "full")]
        which: String,
    },
    /// Whole log lines containing something.
    LogGrep {
        what: String,
        #[arg(long, default_value = "")]
        from: String,
        #[arg(long, default_value = "")]
        to: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long, default_value = "full")]
        which: String,
    },
    /// Log lines between two times, or the last few.
    LogLines {
        #[arg(long, default_value = "")]
        from: String,
        #[arg(long, default_value = "")]
        to: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long, default_value = "full")]
        which: String,
    },
    /// KS's fixed address and the port base, which is what a starter needs.
    Bootstrap,
    /// Time the random generator that fills ping padding, single-threaded, so
    /// its cost is known rather than mixed into a measurement of the wire.
    Generator {
        #[arg(long, default_value_t = 1 << 30)]
        bytes: usize,
        /// Bytes per call, as a reply body would be.
        #[arg(long, default_value_t = 16384)]
        chunk: usize,
    },
    /// Which spreading rules this cluster can enforce.
    Placement,
    /// The per-media-class constant table.
    MediaClasses,

    /// Create a directory.
    Mkdir { path: String },
    /// Remove an empty directory.
    Rmdir { path: String },
    /// List a directory.
    Ls { path: String },
    /// Metadata for one object or directory.
    Stat { path: String },

    /// Create an object.
    Create {
        path: String,
        #[arg(long, value_enum)]
        flavor: Flavor,
        /// Where one record ends: nul, lf, crlf, length or fixed:WIDTH.
        #[arg(long, default_value = "lf")]
        framing: String,
        #[arg(long)]
        media_class: Option<String>,
        /// Delete the object this many seconds from now.
        #[arg(long)]
        expiry: Option<u64>,
    },
    /// Delete an object, into the soft-delete window.
    Delete { path: String },
    /// Recover an object still inside its soft-delete window.
    Undelete { path: String },
    /// Set or clear an object's declared expiry.
    SetExpiry {
        path: String,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// What an object is: id, stream, SN, when it was made, flavour.
    ///
    /// Naming no `--want` asks for exactly what this asked for before the flag existed, and
    /// costs exactly that (§9.1.3.11).
    ObjectInfo {
        path: String,
        /// Extra detail to return. Repeatable. Asking for something that implies another —
        /// instances imply extents — brings the other along.
        #[arg(long, value_enum)]
        want: Vec<Want1>,
        /// The most entries an answer may carry, over objects plus streams plus extents. Zero
        /// means the cluster's configured default.
        #[arg(long, default_value_t = 0)]
        limit: u64,
        /// Describe from this record onward, for an object too large to describe at once.
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// What several objects are, in one request. Names may belong to different DNs, and
    /// the answer says how many it covered and where to carry on (§9.1.3.12).
    BatchObjectInfo {
        /// The names, in the order the answer will describe them.
        paths: Vec<String>,
        #[arg(long, value_enum)]
        want: Vec<Want1>,
        /// The most entries the answer may carry, over objects plus streams. Zero means
        /// the cluster's configured default.
        #[arg(long, default_value_t = 0)]
        limit: u64,
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// Create many objects, and time it. Paths are `<prefix><n>`.
    BenchCreate {
        #[arg(long, default_value_t = 1_000_000)]
        objects: u64,
        /// Objects per request. One is a create apiece; more is a batch.
        #[arg(long, default_value_t = 1000)]
        batch: usize,
        #[arg(long, default_value_t = 8)]
        clients: usize,
        /// Requests in flight per client.
        #[arg(long, default_value_t = 8)]
        depth: usize,
        #[arg(long, default_value = "/bench/")]
        prefix: String,
        #[arg(long, value_enum, default_value_t = Flavor::Unkeyed)]
        flavor: Flavor,
        /// How many times to measure, each trial creating its own names.
        #[arg(long, default_value_t = 3)]
        trials: usize,
    },
    /// Look objects up at random, and time it. Run after bench-create with the
    /// same count and prefix.
    BenchInfo {
        /// How many objects exist to be looked up.
        #[arg(long, default_value_t = 1_000_000)]
        objects: u64,
        /// Names per request. One is `object-info`; more is `batch-object-info`.
        #[arg(long, default_value_t = 1)]
        batch: usize,
        #[arg(long, default_value_t = 8)]
        clients: usize,
        #[arg(long, default_value_t = 8)]
        depth: usize,
        #[arg(long, default_value_t = 5)]
        seconds: u64,
        #[arg(long, default_value = "/bench/")]
        prefix: String,
        #[arg(long, default_value_t = 3)]
        trials: usize,
        /// Which `bench-create` trial's objects to look up. Creating puts the trial
        /// number in the path so each trial has fresh names; reading does not need
        /// fresh names, so it picks one.
        #[arg(long, default_value_t = 1)]
        trial: usize,
    },

    /// A second name for an object, sharing its bytes.
    Clone { src: String, dst: String },
    /// One object from several, in order.
    Concat {
        dst: String,
        #[arg(required = true)]
        src: Vec<String>,
    },

    /// Append a record to an unkeyed object, creating it if it is not there.
    Append {
        path: String,
        /// The object's framing, used to frame this record and to make the object
        /// if it does not exist yet.
        #[arg(long, default_value = "lf")]
        framing: String,
        /// Send a CRC of the framed block, which is the store's promise back that
        /// what it holds is what was sent. A real caller computes this over its own
        /// buffer, after its own checks; this computes it here.
        #[arg(long)]
        crc: bool,
        /// The object this must still be, as `RANGE:ID:SECOND` from `object-info`. Without
        /// it the path names whatever it points at now, which is what a path means — with
        /// it, a path that was repointed answers "that object was replaced" instead of
        /// answering about something else (§17.36).
        #[arg(long)]
        expect: Option<String>,
        #[command(flatten)]
        body: Body,
    },
    /// Insert a record into a keyed object.
    Insert {
        path: String,
        #[arg(long)]
        key: String,
        #[command(flatten)]
        body: Body,
    },
    /// Replace a record, leaving every count unchanged.
    Update {
        path: String,
        #[command(flatten)]
        at: At,
        #[command(flatten)]
        body: Body,
    },
    /// Tombstone a record, leaving every count unchanged.
    DeleteRecord {
        path: String,
        #[command(flatten)]
        at: At,
    },
    /// Read one record.
    Read {
        path: String,
        /// The object's framing, needed to find the record inside the block that
        /// comes back — nothing below the client parses one.
        #[arg(long, default_value = "lf")]
        framing: String,
        /// The object this must still be, as `RANGE:ID:SECOND` from `object-info`. Without
        /// it the path names whatever it points at now, which is what a path means — with
        /// it, a path that was repointed answers "that object was replaced" instead of
        /// answering about something else (§17.36).
        #[arg(long)]
        expect: Option<String>,
        #[command(flatten)]
        at: At,
    },
    /// Read records in order, from a record number, following the answer's
    /// continuation until there is no more.
    ///
    /// A budget names one of three spaces (§9.1.3.6) and the answer reports all
    /// three. Naming none means logical bytes, because a buffer is the thing a
    /// caller is certain about.
    Scan {
        path: String,
        #[arg(long, default_value = "lf")]
        framing: String,
        /// Record number to start at.
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Stop before this record number. The scan may stop sooner.
        ///
        /// Not `--to`, which is the global argument naming the process to talk to —
        /// and clap resolves that collision by panicking at run time rather than at
        /// build time, so the name matters.
        #[arg(long)]
        before: Option<u64>,
        #[command(flatten)]
        budget: Wants,
        /// Stop after this many requests, so a mistake is not an unbounded loop.
        #[arg(long, default_value_t = 1000)]
        rounds: usize,
    },
    /// How many records an object holds.
    Count { path: String },
    /// Records written in a span of seconds.
    TimeRange {
        path: String,
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
    },
}

/// Where a record is, which is a record number or a key but never both.
#[derive(clap::Args)]
#[group(required = true, multiple = false)]
struct At {
    #[arg(long)]
    record: Option<u64>,
    #[arg(long)]
    key: Option<String>,
}

/// How much one answer may carry. One space, never two: a caller cannot know the
/// compression ratio in advance, so a request bounding two would satisfy neither
/// (§9.1.3.6).
#[derive(clap::Args)]
#[group(multiple = false)]
struct Wants {
    /// At most this many records.
    #[arg(long)]
    records: Option<u64>,
    /// At most this many bytes before compression, which bounds the buffer.
    #[arg(long)]
    logical_bytes: Option<u64>,
    /// At most this many bytes as stored, which bounds the network and the bill.
    #[arg(long)]
    compressed_bytes: Option<u64>,
}

impl Wants {
    fn budget(&self) -> Budget {
        match (self.records, self.logical_bytes, self.compressed_bytes) {
            (Some(n), _, _) => Budget::Records(n),
            (_, Some(n), _) => Budget::Logical(n),
            (_, _, Some(n)) => Budget::Compressed(n),
            _ => Budget::Logical(Range::DEFAULT_LOGICAL),
        }
    }
}

impl At {
    fn arg(&self) -> Arg {
        match (self.record, &self.key) {
            (Some(n), _) => Arg::Number(n),
            (_, Some(key)) => Arg::Text(key.clone()),
            _ => unreachable!("clap requires one of --record and --key"),
        }
    }
}

/// Record bytes: given inline, from a file, or from stdin.
#[derive(clap::Args)]
struct Body {
    #[arg(long)]
    data: Option<String>,
    #[arg(long)]
    file: Option<PathBuf>,
}

impl Body {
    fn bytes(&self) -> Result<Vec<u8>> {
        use common::Context;
        use std::io::Read;
        match (&self.data, &self.file) {
            (Some(_), Some(_)) => Err(Error::new("--data and --file are exclusive")),
            (Some(data), None) => Ok(data.as_bytes().to_vec()),
            (None, Some(path)) => std::fs::read(path).context(path.display()),
            (None, None) => {
                let mut bytes = Vec::new();
                std::io::stdin().read_to_end(&mut bytes)?;
                Ok(bytes)
            }
        }
    }
}

fn main() {
    if let Err(e) = run(Cli::parse()) {
        eprintln!("laws: {e}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let cluster = ClusterConfig::load(&cli.config)?;
    let mut timeout = Duration::from_millis(cluster.timeouts.request_ms);
    // Reading log files is not an ordinary request: it can take minutes, being
    // serial on purpose so as not to compete with the work. So it gets minutes.
    if matches!(cli.cmd, Cmd::LogHistogram { .. } | Cmd::LogGrep { .. } | Cmd::LogLines { .. }) {
        timeout = timeout.max(Duration::from_secs(1800));
    }

    match &cli.cmd {
        Cmd::Services => {
            for s in cluster.cold_start_order() {
                println!("{:<6} {:<3} {}", s.identity, s.kind, s.domain.join("/"));
            }
            Ok(())
        }
        Cmd::Sites => {
            let target = resolve(&cluster, &cli, ServiceKind::Ks)?;
            report(
                &target,
                &open(&target, timeout, &cluster.messaging)?.call(&Req::new(Op::Sites))?,
            )
        }
        Cmd::Registry => {
            let target = resolve(&cluster, &cli, ServiceKind::Ks)?;
            let response =
                open(&target, timeout, &cluster.messaging)?.call(&Req::new(Op::Registry))?;
            if response.args.is_empty() {
                println!("{} nothing has registered", target.identity);
            }
            report(&target, &response)
        }
        Cmd::Bootstrap => {
            println!("registrar {}", cluster.registrar()?.identity);
            println!("address {}", cluster.ks.address);
            println!("port_base {}", cluster.ks.port_base);
            Ok(())
        }
        Cmd::Generator { bytes, chunk } => {
            let mut rng = Rng::new();

            // The generator alone, with nowhere to put the words: this is the
            // dependency chain and nothing else.
            let words = bytes / 8;
            let start = std::time::Instant::now();
            let mut sum = 0u64;
            for _ in 0..words {
                sum ^= rng.next_u64();
            }
            let bare = start.elapsed().as_secs_f64();
            println!(
                "generating:  {:.2} GB/s, {:.2} ns per 8-byte word (checksum {sum:x})",
                (words * 8) as f64 / bare / 1e9,
                bare * 1e9 / words as f64
            );

            // And the same words written into a buffer, which is what filling a
            // reply body actually does.
            let mut buf = vec![0u8; *chunk];
            let mut done = 0;
            let start = std::time::Instant::now();
            while done < *bytes {
                rng.fill_bytes(&mut buf);
                done += *chunk;
            }
            let filling = start.elapsed().as_secs_f64();
            println!(
                "filling:     {:.2} GB/s, {:.1} ns per {chunk}-byte body",
                done as f64 / filling / 1e9,
                filling * 1e9 / (done / chunk) as f64
            );
            Ok(())
        }
        Cmd::Placement => {
            print!("{}", placement::format(&cluster, &placement::report(&cluster)));
            Ok(())
        }
        Cmd::MediaClasses => {
            for m in &cluster.media_classes {
                println!(
                    "{:<5} index {:<9} min_read {:<9} block {:<9} bypass {:<10} \
                     compress_read_win {:<5} repair {} B/s",
                    m.name,
                    m.index_interval,
                    m.min_read_bytes,
                    m.compress_block,
                    m.staging_bypass,
                    m.compress_read_win,
                    m.repair_bytes_sec
                );
            }
            Ok(())
        }
        Cmd::Ping { count, conns, size, dull, reconnect } => {
            let plan = Plan {
                count: *count,
                conns: *conns,
                size: *size,
                dull: *dull,
                reconnect: *reconnect,
                timeout,
            };
            ping(&cluster, &cli, &plan)
        }
        Cmd::Bench { clients, warmup, size, dull, depth, seconds, trials } => {
            // Anything answers a ping, so an unnamed target is KS, which is the
            // one process that is always there.
            let target = resolve(&cluster, &cli, ServiceKind::Ks)?;
            let plan = Bench {
                clients: *clients,
                warmup: *warmup,
                size: *size,
                dull: *dull,
                depth: *depth,
                seconds: *seconds,
                trials: *trials,
                timeout,
            };
            bench(&target, &plan, &cluster.messaging)
        }
        Cmd::Status => {
            for target in targets(&cluster, &cli, ServiceKind::Ks)? {
                report(
                    &target,
                    &open(&target, timeout, &cluster.messaging)?.call(&Req::new(Op::Status))?,
                )?;
            }
            Ok(())
        }
        Cmd::Shutdown { all } => {
            // Reverse cold-start order: nothing outlives what it depends on, and
            // KS goes last because everything else is found through it.
            let mut targets = targets(&cluster, &cli, ServiceKind::Ks)?;
            if *all {
                targets.reverse();
            }
            for target in targets {
                report(
                    &target,
                    &open(&target, timeout, &cluster.messaging)?.call(&Req::new(Op::Shutdown))?,
                )?;
            }
            Ok(())
        }
        Cmd::BenchCreate { objects, batch, clients, depth, prefix, flavor, trials } => {
            let target = resolve(&cluster, &cli, ServiceKind::Fe)?;
            let plan = Objects {
                objects: *objects,
                batch: *batch,
                clients: *clients,
                depth: *depth,
                prefix: prefix.clone(),
                keyed: *flavor == Flavor::Keyed,
                seconds: 0,
                trials: *trials,
                timeout,
            };
            bench_create(&target, &plan, &cluster.messaging)
        }
        Cmd::BenchInfo { objects, batch, clients, depth, seconds, prefix, trials, trial } => {
            let target = resolve(&cluster, &cli, ServiceKind::Fe)?;
            let plan = Objects {
                objects: *objects,
                batch: *batch,
                clients: *clients,
                depth: *depth,
                prefix: prefix.clone(),
                keyed: false,
                seconds: *seconds,
                trials: *trials,
                timeout,
            };
            bench_info(&target, &plan, &cluster.messaging, *trial)
        }
        Cmd::Scan { path, framing, from, before, budget, rounds } => {
            let target = resolve(&cluster, &cli, ServiceKind::Fe)?;
            let mut conn = open(&target, timeout, &cluster.messaging)?;
            scan(
                &mut conn,
                &target,
                path,
                Framing::parse(framing)?,
                *from,
                *before,
                budget.budget(),
                *rounds,
            )
        }
        cmd => {
            let request = build(cmd)?;
            let target = resolve(&cluster, &cli, serves(cmd))?;
            let answer = open(&target, timeout, &cluster.messaging)?.call(&request)?;
            match (cmd, request.op) {
                (_, Op::AppendRecord) => receipt(&target, &answer),
                // The client asked for a record and gets the block holding it, so it
                // needs its own question back to find the record in it.
                (Cmd::Read { framing, at, .. }, Op::ReadRecord) => {
                    let want = match at.arg() {
                        Arg::Number(record) => record,
                        _ => 0,
                    };
                    record(&target, Framing::parse(framing)?, want, &answer)
                }
                (_, Op::ObjectInfo | Op::CreateObject) => {
                    object(&target, &answer, cluster.extents.expansion())
                }
                (Cmd::BatchObjectInfo { paths, want, .. }, Op::BatchInfo) => {
                    batch(&target, paths, !want.is_empty(), &answer, cluster.extents.expansion())
                }
                _ => report(&target, &answer),
            }
        }
    }
}

struct Plan {
    count: usize,
    conns: usize,
    size: u64,
    dull: bool,
    reconnect: bool,
    timeout: Duration,
}

/// A ping, asking for `size` bytes of padding back.
fn ping_request(size: u64, dull: bool) -> Req {
    let request = Req::new(Op::Ping);
    if size == 0 { request } else { request.number(size).flag(dull) }
}

/// The ping is a benchmark, not a feature: the work it asks for is a few hundred
/// instructions, so with a bare reply what comes back is per-message overhead,
/// and with a padded one it is that plus what size costs.
fn ping(cluster: &ClusterConfig, cli: &Cli, plan: &Plan) -> Result<()> {
    let shared = Arc::new(cluster.messaging.clone());
    if plan.count == 0 || plan.conns == 0 {
        return Err(Error::new("--count 0 or --conns 0 measures nothing"));
    }
    for target in targets(cluster, cli, ServiceKind::Ks)? {
        let addr = target.address;
        let start = std::time::Instant::now();
        let threads: Vec<_> = (0..plan.conns)
            .map(|_| {
                let (count, size, dull) = (plan.count, plan.size, plan.dull);
                let (reconnect, timeout) = (plan.reconnect, plan.timeout);
                let messaging = Arc::clone(&shared);
                std::thread::spawn(move || -> Result<(Vec<Duration>, Resp)> {
                    let request = ping_request(size, dull);
                    let mut conn = Conn::open(addr, timeout, &messaging)?;
                    let mut samples = Vec::with_capacity(count);
                    let mut last = conn.call(&request)?;
                    for _ in 0..count {
                        if reconnect {
                            conn = Conn::open(addr, timeout, &messaging)?;
                        }
                        let (response, elapsed) = conn.timed(&request)?;
                        samples.push(elapsed);
                        last = response;
                    }
                    Ok((samples, last))
                })
            })
            .collect();

        let mut samples = Vec::with_capacity(plan.count * plan.conns);
        let mut last = None;
        for thread in threads {
            let (mine, response) =
                thread.join().map_err(|_| Error::new("ping thread panicked"))??;
            samples.extend(mine);
            last = Some(response);
        }
        let wall = start.elapsed();

        if let Some(response) = last {
            report(&target, &response)?;
        }
        if plan.count * plan.conns > 1 {
            let stats = Stats::of(&mut samples).expect("count > 0");
            let rate = samples.len() as f64 / wall.as_secs_f64();
            println!("{} {} conns, {stats}", target.identity, plan.conns);
            println!("{} {rate:.0} round trips per second", target.identity);
        }
    }
    Ok(())
}

/// Run a measurement several times and say what came out.
///
/// **Three is the default, because one number is not a measurement**
/// (Asserted, Bob, 2026-08-16): the spread is what says whether a difference
/// between two configurations means anything, and a single sample hides it. This
/// project has already drawn a wrong conclusion from single samples twice.
fn trials(name: &str, times: usize, mut once: impl FnMut() -> Result<f64>) -> Result<()> {
    let mut rates = Vec::with_capacity(times);
    for _ in 0..times.max(1) {
        rates.push(once()?);
    }
    if rates.len() > 1 {
        let mean = rates.iter().sum::<f64>() / rates.len() as f64;
        let low = rates.iter().copied().fold(f64::MAX, f64::min);
        let high = rates.iter().copied().fold(0.0, f64::max);
        println!(
            "{name} {mean:.0} per second on average of {}, from {low:.0} to {high:.0}, \
             a spread of {:.1}%",
            rates.len(),
            (high - low) / mean * 100.0
        );
    }
    Ok(())
}

struct Bench {
    clients: usize,
    warmup: u64,
    size: u64,
    dull: bool,
    depth: usize,
    seconds: u64,
    trials: usize,
    timeout: Duration,
}

/// Warm up, then measure. Throughput is the number to want first, and average
/// latency is what says how much of it is per-message overhead.
fn bench(target: &Target, plan: &Bench, messaging: &Messaging) -> Result<()> {
    trials(&target.identity, plan.trials, || once(target, plan, messaging))
}

fn once(target: &Target, plan: &Bench, messaging: &Messaging) -> Result<f64> {
    let shared = Arc::new(messaging.clone());
    if plan.clients == 0 || plan.depth == 0 || plan.seconds == 0 {
        return Err(Error::new("--clients, --depth and --seconds must all be positive"));
    }
    let addr = target.address;
    let warm_end = std::time::Instant::now() + Duration::from_secs(plan.warmup);
    let end = warm_end + Duration::from_secs(plan.seconds);

    let threads: Vec<_> = (0..plan.clients)
        .map(|_| {
            let (size, dull) = (plan.size, plan.dull);
            let (depth, timeout) = (plan.depth, plan.timeout);
            let messaging = Arc::clone(&shared);
            std::thread::spawn(move || -> Result<Tally> {
                let request = ping_request(size, dull);
                let mut conn = Conn::open(addr, timeout, &messaging)?;
                conn.press(&request, depth, warm_end, &mut Tally::new())?;
                let mut tally = Tally::new();
                conn.press(&request, depth, end, &mut tally)?;
                Ok(tally)
            })
        })
        .collect();

    let mut total = Tally::new();
    for thread in threads {
        let tally = thread.join().map_err(|_| Error::new("bench thread panicked"))??;
        total.merge(&tally);
    }
    let measured = std::time::Instant::now().saturating_duration_since(warm_end).as_secs_f64();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    let rate = total.count as f64 / measured;
    println!(
        "{} {} clients x {} in flight, {} byte replies, {measured:.1} s measured after {} s warm-up",
        target.identity, plan.clients, plan.depth, plan.size, plan.warmup
    );
    println!(
        "{} {rate:.0} round trips per second, average {:.1} us, min {:.1} us, max {:.1} us",
        target.identity,
        us(total.mean()),
        us(total.min),
        us(total.max)
    );
    if plan.size > 0 {
        println!(
            "{} {:.0} MB per second of reply body",
            target.identity,
            rate * plan.size as f64 / 1e6
        );
    }
    Ok(rate)
}

#[derive(Clone)]
struct Objects {
    objects: u64,
    batch: usize,
    clients: usize,
    depth: usize,
    prefix: String,
    keyed: bool,
    seconds: u64,
    trials: usize,
    timeout: Duration,
}

/// Create `objects` objects, sharing them out among the clients, each keeping
/// `depth` requests in flight.
/// The prefix one trial's objects live under.
///
/// ⚠️ **Both halves of the benchmark must use this, and for a while only one did.**
/// `bench-create` puts the trial number in the path, because a second trial reusing
/// the first's names creates nothing and measures the refusing. `bench-info` did not,
/// so it looked up `/bench/0` while the objects were at `/bench/1/0` — **every lookup
/// missed, in every run ever recorded** (§17.5g's correction). A miss is cheaper than
/// a hit, so the rate it reported was of the wrong operation.
fn trial_prefix(prefix: &str, trial: usize) -> String {
    format!("{prefix}{trial}/")
}

fn bench_create(target: &Target, plan: &Objects, messaging: &Messaging) -> Result<()> {
    let mut trial = 0;
    trials(&target.identity, plan.trials, || {
        trial += 1;
        let mine = Objects { prefix: trial_prefix(&plan.prefix, trial), ..plan.clone() };
        create_once(target, &mine, messaging)
    })
}

fn create_once(target: &Target, plan: &Objects, messaging: &Messaging) -> Result<f64> {
    if plan.batch == 0 || plan.clients == 0 || plan.depth == 0 {
        return Err(Error::new("--batch, --clients and --depth must all be positive"));
    }
    // Line-framed, which is what a benchmark of creating objects wants: a framing
    // is not what it is measuring, and every object needs one.
    let (kind, width) = Framing::Lf.wire();
    let addr = target.address;
    let shared = Arc::new(messaging.clone());
    let batches = plan.objects.div_ceil(plan.batch as u64);
    let start = std::time::Instant::now();

    let threads: Vec<_> = (0..plan.clients)
        .map(|client| {
            let (messaging, prefix) = (Arc::clone(&shared), plan.prefix.clone());
            let (batch, depth, keyed, timeout) = (plan.batch, plan.depth, plan.keyed, plan.timeout);
            let mine: Vec<u64> = ((client as u64)..batches).step_by(plan.clients).collect();
            std::thread::spawn(move || -> Result<(u64, Tally)> {
                let mut conn = Conn::open(addr, timeout, &messaging)?;
                let mut tally = Tally::new();
                let mut made = 0;
                let mut sent: HashMap<u32, std::time::Instant> = HashMap::new();
                let mut at = 0;
                loop {
                    while sent.len() < depth && at < mine.len() {
                        let first = mine[at] * batch as u64;
                        at += 1;
                        let mut payload = common::wire::StructWriter::new();
                        if batch != 1 {
                            payload.number(batch as u64);
                        }
                        for n in first..first + batch as u64 {
                            let name = format!("{prefix}{n}");
                            payload.text(&name);
                            payload.flag(keyed);
                            payload.number(kind);
                            payload.number(width);
                        }
                        let request =
                            Req::new(if batch == 1 { Op::CreateObject } else { Op::BatchCreate })
                                .structure(payload);
                        let id = conn.send(&request)?;
                        sent.insert(id, std::time::Instant::now());
                    }
                    if sent.is_empty() {
                        break;
                    }
                    conn.flush()?;
                    let answer = conn.recv()?;
                    if answer.status != Status::Ok {
                        return Err(Error::new(format!(
                            "create answered {}: {}",
                            answer.status,
                            answer.message().unwrap_or("")
                        )));
                    }
                    match sent.remove(&answer.id) {
                        Some(at) => tally.add(at.elapsed()),
                        None => return Err(Error::new("unasked answer")),
                    }
                    made += match answer.args.first() {
                        Some(Arg::Number(made)) => *made,
                        _ => 1,
                    };
                }
                Ok((made, tally))
            })
        })
        .collect();

    let mut total = Tally::new();
    let mut made = 0;
    for thread in threads {
        let (mine, tally) = thread.join().map_err(|_| Error::new("a client panicked"))??;
        made += mine;
        total.merge(&tally);
    }
    let seconds = start.elapsed().as_secs_f64();
    println!(
        "{} {made} objects in {seconds:.2} s: {:.0} per second, {} per request, \
         {} clients x {} in flight",
        target.identity,
        made as f64 / seconds,
        plan.batch,
        plan.clients,
        plan.depth
    );
    println!(
        "{} per request: average {:.1} us, min {:.1} us, max {:.1} us",
        target.identity,
        total.mean().as_secs_f64() * 1e6,
        total.min.as_secs_f64() * 1e6,
        total.max.as_secs_f64() * 1e6
    );
    Ok(made as f64 / seconds)
}

/// Look up objects at random for a while. Random because a benchmark that walks
/// in order measures the cache rather than the index.
fn bench_info(target: &Target, plan: &Objects, messaging: &Messaging, trial: usize) -> Result<()> {
    // Every trial reads the same objects — looking up is not destructive, so unlike
    // creating there is no reason for each trial to want its own names.
    let mine = Objects { prefix: trial_prefix(&plan.prefix, trial), ..plan.clone() };
    trials(&target.identity, plan.trials, || info_once(target, &mine, messaging))
}

fn info_once(target: &Target, plan: &Objects, messaging: &Messaging) -> Result<f64> {
    if plan.batch == 0 || plan.clients == 0 || plan.depth == 0 || plan.seconds == 0 {
        return Err(Error::new("--batch, --clients, --depth and --seconds must all be positive"));
    }
    let addr = target.address;
    let shared = Arc::new(messaging.clone());
    let end = std::time::Instant::now() + Duration::from_secs(plan.seconds);
    let start = std::time::Instant::now();

    let threads: Vec<_> = (0..plan.clients)
        .map(|client| {
            let (messaging, prefix) = (Arc::clone(&shared), plan.prefix.clone());
            let (batch, depth, objects, timeout) =
                (plan.batch, plan.depth, plan.objects, plan.timeout);
            std::thread::spawn(move || -> Result<(u64, u64, u64, Tally)> {
                let mut rng = Rng::from_seed(client as u64 + 1);
                let mut conn = Conn::open(addr, timeout, &messaging)?;
                let mut tally = Tally::new();
                // Three counts, not two: names asked, names the server *looked at*, and objects
                // that existed. A miss is not a truncation and the report must not confuse them.
                let (mut asked, mut given, mut found) = (0u64, 0u64, 0u64);
                let mut sent: HashMap<u32, std::time::Instant> = HashMap::new();
                loop {
                    while sent.len() < depth && std::time::Instant::now() < end {
                        let request = if batch == 1 {
                            let mut payload = common::wire::StructWriter::new();
                            payload.text(&format!("{prefix}{}", rng.next_u64() % objects));
                            payload.flag(false);
                            Req::new(Op::ObjectInfo).structure(payload)
                        } else {
                            let mut payload = common::wire::StructWriter::new();
                            payload.number(batch as u64);
                            for _ in 0..batch {
                                payload.text(&format!("{prefix}{}", rng.next_u64() % objects));
                            }
                            payload.flag(false);
                            Req::new(Op::BatchInfo).structure(payload)
                        };
                        let id = conn.send(&request)?;
                        sent.insert(id, std::time::Instant::now());
                        asked += batch as u64;
                    }
                    if sent.is_empty() {
                        break;
                    }
                    conn.flush()?;
                    let answer = conn.recv()?;
                    match sent.remove(&answer.id) {
                        Some(at) => tally.add(at.elapsed()),
                        None => return Err(Error::new("unasked answer")),
                    }
                    // ⚠️ Before the status check. A single lookup that missed still *looked at* the
                    // name, and counting it only on `Ok` made every miss look like a truncation.
                    if batch == 1 {
                        given += 1;
                    }
                    // ⚠️ Counted by parsing the rows, not by chopping the args into
                    // pieces of an assumed width. The assumed width was five and the
                    // row is eight, so this counted every object twice (§17.5g).
                    //
                    // A miss answers `not found` with a message rather than with rows,
                    // and that is not a failure — it is what looking up a name nobody
                    // took looks like, and it is worth measuring on purpose.
                    if answer.status == Status::Ok {
                        // A batch answer leads with what it covered; a single one is a bare row
                        // (§9.1.3.7). Which it is follows from what was asked, not from guessing.
                        let rows = if batch == 1 {
                            &answer.args[..]
                        } else {
                            let (covered, rows) = Batch::of_args(&answer.args)?;
                            given += covered.given;
                            rows
                        };
                        found += Described::all_of_args(rows)?
                            .iter()
                            .filter(|row| row.is_some())
                            .count() as u64;
                    }
                }
                Ok((asked, given, found, tally))
            })
        })
        .collect();

    let mut total = Tally::new();
    let (mut asked, mut given, mut found) = (0, 0, 0);
    for thread in threads {
        let (mine, seen, hit, tally) =
            thread.join().map_err(|_| Error::new("a client panicked"))??;
        asked += mine;
        given += seen;
        found += hit;
        total.merge(&tally);
    }
    let seconds = start.elapsed().as_secs_f64();
    println!(
        "{} {asked} lookups in {seconds:.2} s: {:.0} per second, {given} answered, {found} found, \
         {} per request, {} clients x {} in flight",
        target.identity,
        asked as f64 / seconds,
        plan.batch,
        plan.clients,
        plan.depth
    );
    // ⚠️ The rate above counts names *asked*. When the entry limit truncates each batch the rest
    // are never looked at, so the rate measures the truncation instead (§17.23.3, #97). A name that
    // was looked at and did not exist is a miss, which is a different thing and not a warning.
    if given < asked {
        println!(
            "{} the entry limit answered only {given} of {asked} names ({:.1}%), so the rate above \
             counts asking rather than answering — raise info_entries above --batch {}",
            target.identity,
            100.0 * given as f64 / asked.max(1) as f64,
            plan.batch
        );
    }
    println!(
        "{} per request: average {:.1} us, min {:.1} us, max {:.1} us",
        target.identity,
        total.mean().as_secs_f64() * 1e6,
        total.min.as_secs_f64() * 1e6,
        total.max.as_secs_f64() * 1e6
    );
    Ok(asked as f64 / seconds)
}

/// Frame one record, and refuse data the framing cannot carry.
///
/// This is the only place that holds a record *as a record* before it becomes a
/// byte stream. After this a stray terminator is not a violation anybody can
/// detect — it is simply two records — so the check belongs here or nowhere.
///
/// Framing here also means what the client sends always ends on a record boundary,
/// which is what keeps a writer clear of the awkward corners around sealing.
fn frame(framing: Framing, record: &[u8]) -> Result<Vec<u8>> {
    let mark: &[u8] = match framing {
        Framing::Nul => &[0],
        Framing::Lf => b"\n",
        Framing::CrLf => b"\r\n",
        Framing::Fixed(width) => {
            if record.len() != width as usize {
                return Err(Error::new(format!(
                    "a fixed:{width} record is {width} bytes, and that one is {}",
                    record.len()
                )));
            }
            return Ok(record.to_vec());
        }
        Framing::Length => {
            let length = u32::try_from(record.len())
                .map_err(|_| Error::new("a record longer than a length field can say"))?;
            let mut framed = length.to_le_bytes().to_vec();
            framed.extend_from_slice(record);
            return Ok(framed);
        }
    };
    if record.windows(mark.len()).any(|window| window == mark) {
        return Err(Error::new(format!(
            "{framing} records cannot contain their terminator, and that one does"
        )));
    }
    let mut framed = record.to_vec();
    framed.extend_from_slice(mark);
    Ok(framed)
}

/// What an append answers: where the bytes went, once a quorum of instances had
/// them. Five numbers read as one line rather than five.
fn receipt(target: &Target, response: &Resp) -> Result<()> {
    if let [
        Arg::Number(stream),
        Arg::Number(record),
        Arg::Number(object_record),
        Arg::Number(records),
        Arg::Number(extent),
        Arg::Number(created),
        Arg::Number(offset),
        Arg::Number(length),
    ] = response.args.as_slice()
    {
        let stream = StreamId::of_number(*stream)?;
        // The record number to read it back by comes first, then the durable
        // reference it is made of, then where the bytes went.
        match records {
            0 => println!(
                "{} stream {stream}, no record finished yet, {length} bytes at offset {offset} of extent {extent}",
                target.identity
            ),
            1 => println!(
                "{} record {object_record} (stream {stream} record {record}), {length} logical bytes at logical offset {offset} of extent {extent} made in second {created}",
                target.identity
            ),
            _ => println!(
                "{} records {object_record} to {} (stream {stream} from record {record}), {length} bytes at offset {offset} of extent {extent} made in second {created}",
                target.identity,
                object_record + records - 1
            ),
        }
        return Ok(());
    }
    report(target, response)
}

/// What an object is, which is what both a lookup and a create answer with. Seven
/// numbers read as one line.
fn object(target: &Target, response: &Resp, expansion: Expansion) -> Result<()> {
    // A plain answer is the row alone; a levelled one leads with its shape, so which
    // it is comes from whether the row parses at the front (§9.1.3.2).
    if let Ok((Some(it), [])) = Described::of_args(&response.args) {
        return say(target, &it, None, &[], expansion);
    }
    let Ok((shape, rest)) = Shape::of_args(&response.args) else {
        return report(target, response);
    };
    let Ok((Some(it), rest)) = Described::of_args(rest) else {
        return report(target, response);
    };
    // Interleaved: a stream, then the extents it says follow, repeated (§17.22).
    let mut rest = rest;
    let mut streams = Vec::new();
    for _ in 0..shape.given {
        let Ok((row, after)) = StreamRow::of_args(rest) else { return report(target, response) };
        let Ok((extents, after)) = ExtentRow::some_of_args(after, row.given) else {
            return report(target, response);
        };
        streams.push((row, extents));
        rest = after;
    }
    if !rest.is_empty() {
        return report(target, response);
    }
    say(target, &it, Some(shape), &streams, expansion)
}

/// `RANGE:ID:SECOND`, the name `object-info` prints, or nothing for "whatever the path points at
/// now" — which is what a path means and what almost every caller wants (§17.36).
fn expecting(text: &Option<String>) -> Result<Expecting> {
    let Some(text) = text else { return Ok(Expecting::whatever()) };
    let parts: Vec<&str> = text.split(':').collect();
    let [range, object, created] = parts.as_slice() else {
        return Err(Error::new(format!("--expect takes RANGE:ID:SECOND, not {text}")));
    };
    let number = |what: &str, of: &str| {
        of.parse::<u64>().map_err(|_| Error::new(format!("{what} {of} is not a number")))
    };
    Ok(Expecting::of(ObjectName {
        range: RangeId::of_number(number("a range", range)?)?,
        object: ObjectId::of_number(number("an object", object)?)?,
        created: TimeSec::new(number("a second", created)?),
    }))
}

/// Several objects, in the order asked about, then what the answer did not cover.
///
/// ⚠️ **The names come from the request, not the answer.** A batch answer is a prefix of
/// what was asked for and carries no names at all (§17.23), so printing them side by side
/// is the only way to see which object each row is — and getting that wrong silently is
/// exactly the failure §17.12 warned about.
fn batch(
    target: &Target,
    paths: &[String],
    levelled: bool,
    response: &Resp,
    expansion: Expansion,
) -> Result<()> {
    let (covered, rows) = match Batch::of_args(&response.args) {
        Ok(read) => read,
        Err(_) => return report(target, response),
    };
    let pieces = match levelled {
        true => match Batch::pieces(rows, covered.given) {
            Ok(pieces) => pieces,
            Err(_) => return report(target, response),
        },
        false => match Described::some_of_args(rows, covered.given) {
            Ok(pieces) => pieces,
            Err(_) => return report(target, response),
        },
    };
    for (n, piece) in pieces.iter().enumerate() {
        let name = paths.get(n).map(String::as_str).unwrap_or("?");
        // ⚠️ A name nobody has is answered, not skipped, and it says so rather than printing
        // its row of zeros — which is what it did until this line, and a batch of a thousand
        // names with a typo in one was fifteen lines of unreadable numbers.
        if absent(piece) {
            println!("{name}: {} no object", target.identity);
            continue;
        }
        print!("{name}: ");
        object(target, &Resp { id: 0, status: Status::Ok, args: piece.to_vec() }, expansion)?;
    }
    let e = if covered.entries == 1 { "y" } else { "ies" };
    match covered.next {
        UNKNOWN => println!(
            "{} {} of {} names described, {} entr{e}, nothing more",
            target.identity, covered.given, covered.asked, covered.entries
        ),
        next => println!(
            "{} {} of {} names described, {} entr{e}, carry on at name {next} ({})",
            target.identity,
            covered.given,
            covered.asked,
            covered.entries,
            paths.get(next as usize).map(String::as_str).unwrap_or("?")
        ),
    }
    Ok(())
}

/// Whether a batch answer's slot is a name nobody has. The object row is the zeros
/// `Described::missing` writes, whether or not a shape came first.
fn absent(piece: &[Arg]) -> bool {
    let row = match Shape::of_args(piece) {
        Ok((_, rest)) => rest,
        Err(_) => piece,
    };
    matches!(Described::of_args(row), Ok((None, _)))
}

/// An object, and whatever detail came with it.
/// `expansion` is the storage class's ratio, which is how a physical size is arrived at — 4/1
/// for four copies, 4/3 for 3+1 erasure coding. ⚠️ **Never the length of an extent's instance
/// list**: that is what happens to be up right now, and billing charges the class
/// (`BILLING-CHARGES-THE-CLASS-NOT-THE-COPIES`).
fn say(
    target: &Target,
    it: &Described,
    shape: Option<Shape>,
    streams: &[(StreamRow, Vec<ExtentRow>)],
    expansion: Expansion,
) -> Result<()> {
    let flavor = if it.keyed { "keyed" } else { "unkeyed" };
    println!(
        "{} object {} {flavor} framed {}, made in second {}, in range {} on DN{}, at stream {} on SN{} \
         (made in second {}) from record {}",
        target.identity,
        it.object.object,
        it.framing,
        it.object.created,
        it.object.range,
        it.dn,
        it.stream.stream,
        it.sn,
        it.stream.created,
        it.first_record
    );
    let Some(shape) = shape else { return Ok(()) };
    for (row, extents) in streams {
        // UNKNOWN is "nobody asked" or "not knowable here", never a count of zero.
        let records = match row.records {
            UNKNOWN => "still growing".to_string(),
            n => format!("{n} records"),
        };
        let of = match row.totals.extents {
            UNKNOWN => String::new(),
            n => format!(", {n} extents"),
        };
        // The sizes, which is what a listing is for. ⚠️ Marked *so far* when the stream is
        // still growing, because then they are a point in time rather than a total
        // (§17.25.1) — a listing that cannot tell the difference is a listing that quietly
        // under-reports every object still being written.
        let asof = if row.records == UNKNOWN { " so far" } else { "" };
        let sizes = match (row.totals.logical, row.totals.compressed) {
            (UNKNOWN, _) | (_, UNKNOWN) => String::new(),
            (logical, compressed) => format!(
                ", {logical} bytes{asof}, {compressed} stored, {} billed",
                row.totals.physical(expansion)
            ),
        };
        println!(
            "  stream {} on SN{} from record {}, made in second {}, {records}{of}{sizes}",
            row.stream, row.sn, row.first_record, row.created
        );
        for extent in extents {
            let held = match extent.records {
                UNKNOWN => "still growing".to_string(),
                n => format!("{n} records, {} stored", extent.length),
            };
            // The whole name, since that is what a hint is built from (§8.1.1a-10).
            // ⚠️ As numbers, not as `{:?}` — a typed id's `Debug` is `DiskId(0)`, which is right
            // in a panic and noise in a listing.
            let disks: Vec<u16> = extent.disks.iter().map(|(_, disk)| disk.get()).collect();
            let ens: Vec<u32> = extent.disks.iter().map(|(en, _)| en.get()).collect();
            println!(
                "    extent {} of SN{} made in second {}, from record {}, {held}, \
                 on disks {disks:?} at ENs {ens:?}",
                extent.id, extent.sn, extent.created, extent.first_record
            );
        }
    }
    // What it did not tell you, which is what makes a partial answer usable.
    let of = match shape.streams {
        UNKNOWN => String::new(),
        total => format!(" of {total}"),
    };
    let s = if shape.given == 1 { "" } else { "s" };
    let e = if shape.entries == 1 { "y" } else { "ies" };
    match shape.next {
        UNKNOWN => println!(
            "{} {} stream{s}{of} described, {} entr{e}, nothing more",
            target.identity, shape.given, shape.entries
        ),
        next => println!(
            "{} {} stream{s}{of} described, {} entr{e}, ask from record {next}",
            target.identity, shape.given, shape.entries
        ),
    }
    Ok(())
}

/// A record, out of the block that holds it.
///
/// **This is the far end of the chain the client started when it appended.** It
/// compressed the block and computed the CRC going in; here it decompresses, checks
/// the CRC against what it promised, parses by the framing, and takes the record it
/// wanted. Nothing in between ever looked inside, and nothing in between could have
/// checked this.
/// Every record a range answer holds, in order, with the number each one has.
///
/// **This is the far end of the chain the client started when it appended.** It
/// compressed each block and computed its CRC going in; here it decompresses, checks
/// the CRC against what it promised, parses by the framing, and numbers what it
/// finds. Nothing in between ever looked inside, and nothing in between could have
/// checked this.
fn records_of(framing: Framing, pieces: &[Piece]) -> Result<Vec<(u64, Vec<u8>)>> {
    let mut found = Vec::new();
    // ⚠️ A record may span blocks (§8.2.3), so the tail of one block is carried into
    // the next rather than parsed on its own. A block that completed no record
    // contributes only carry.
    let mut carry: Vec<u8> = Vec::new();
    let mut numbering = pieces.first().map_or(0, |piece| piece.first_record);
    for piece in pieces {
        let body_len = piece.logical - piece.records * 4;
        let expected = body_len + piece.records * framing.overhead() as u64;
        let block = expand(Compress::Lz4, &piece.bytes, expected as usize)?;
        if piece.crc != common::crc::of(&block) {
            return Err(Error::new(format!(
                "{} bytes came back and do not match the CRC they were written with",
                block.len()
            )));
        }
        carry.extend_from_slice(&block);
        // Whole records out of what has accumulated; whatever is left over stays.
        // `measure` says how many bytes those took, which is the only way to know
        // for a framing whose boundary is not a fixed width.
        let (_, taken) = framing.measure(&carry);
        for record in framing.records(&carry) {
            found.push((numbering, record.to_vec()));
            numbering += 1;
        }
        carry.drain(..taken);
    }
    Ok(found)
}

/// One record out of a range answer, which is what a point read asks for.
fn record(target: &Target, framing: Framing, want: u64, response: &Resp) -> Result<()> {
    let Ok((_covered, rest)) = Covered::of_args(&response.args) else {
        return report(target, response);
    };
    let pieces = Piece::all_of_args(rest)?;
    let found = records_of(framing, &pieces)?;
    let Some((_, bytes)) = found.iter().find(|(number, _)| *number == want) else {
        return Err(Error::new(format!("that answer does not hold record {want}")));
    };
    match std::str::from_utf8(bytes) {
        Ok(text) => println!("{text}"),
        Err(_) => println!("{} {} bytes, not text", target.identity, bytes.len()),
    }
    Ok(())
}

/// Records in order, following each answer's continuation until there is no more.
///
/// ⚠️ **The continuation is a record number the store computed, never an offset this
/// client worked out from where the bytes stopped** (§9.1.3). That is the whole point
/// of the scheme: a caller resuming from a row is resuming from a name, and a caller
/// resuming from an offset is resuming from a hint that is allowed to move.
///
/// **Serial, and §9.1.3.1 says why that is the cost.** Each request waits for the
/// previous answer to learn where to start, so a large object pays a round trip per
/// page. Consistent read is what would let the boundaries be computed instead of
/// discovered, and turn this loop into requests issued all at once.
#[allow(clippy::too_many_arguments)]
fn scan(
    conn: &mut Conn,
    target: &Target,
    path: &str,
    framing: Framing,
    from: u64,
    before: Option<u64>,
    budget: Budget,
    rounds: usize,
) -> Result<()> {
    let mut at = from;
    let mut total = Covered::default();
    let mut asked = 0;
    let mut printed = 0u64;
    for _ in 0..rounds {
        if before.is_some_and(|stop| at >= stop) {
            break;
        }
        let mut payload = common::wire::StructWriter::new();
        payload.text(path);
        Expecting::whatever().write(&mut payload);
        Range { start: Start::Record(at), budget }.write_nested(&mut payload);
        let request = Req::new(Op::ReadRecord).structure(payload);
        let answer = conn.call(&request)?;
        asked += 1;
        // Running off the end is how a scan of an object whose length it does not
        // know finds out, so it is the ordinary way to stop rather than a failure.
        if answer.status == Status::NotFound {
            break;
        }
        if answer.status != Status::Ok {
            return report(target, &answer);
        }
        let (covered, shown) = ranged(target, framing, &answer, before)?;
        total.records += covered.records;
        total.logical += covered.logical;
        total.compressed += covered.compressed;
        printed += shown;
        // ⚠️ No progress means stop, whatever the answer said. A continuation that
        // does not advance would otherwise be an unbounded loop, and this is the one
        // place a client can defend itself against one.
        if covered.next <= at || !covered.more {
            break;
        }
        at = covered.next;
    }
    // The stored figure is the one that costs, since §12.3 bills compressed bytes —
    // so it is reported whatever the budget was named in. **The record count is very
    // nearly uncorrelated with cost**: one record and a million cost about the same
    // if they arrive in the same 2 MB, which is why a caller that wants to control
    // its bill has to budget in `--compressed-bytes` and gets no such control from
    // `--records`.
    //
    // `shown` differs from `records` only when `--before` trimmed the display, which
    // is a convenience rather than a saving: the blocks came back either way.
    let trimmed =
        if printed == total.records { String::new() } else { format!(", {printed} shown") };
    println!(
        "{} {} records{trimmed}, {} logical bytes, {} stored, in {asked} requests",
        target.identity, total.records, total.logical, total.compressed
    );
    Ok(())
}

/// A range of records, and what the answer said it covered.
fn ranged(
    target: &Target,
    framing: Framing,
    response: &Resp,
    before: Option<u64>,
) -> Result<(Covered, u64)> {
    let Ok((covered, rest)) = Covered::of_args(&response.args) else {
        report(target, response)?;
        return Err(Error::new("that answer did not say what it covered"));
    };
    let pieces = Piece::all_of_args(rest)?;
    let mut printed = 0;
    for (number, bytes) in records_of(framing, &pieces)? {
        // The store stops on a block boundary and the caller asked for a record
        // boundary, so the last block may carry records past `to`. Printing is where
        // that is trimmed, since the bytes had to travel anyway.
        // A bound only trims what is shown. The store stops on a block boundary, so
        // the blocks arrived and were paid for regardless.
        if before.is_some_and(|stop| number >= stop) {
            break;
        }
        match std::str::from_utf8(&bytes) {
            Ok(text) => println!("{number} {text}"),
            Err(_) => println!("{number} {} bytes, not text", bytes.len()),
        }
        printed += 1;
    }
    Ok((covered, printed))
}

/// Print a response, and fail the process if the service refused.
fn report(target: &Target, response: &Resp) -> Result<()> {
    for arg in &response.args {
        match arg {
            // A reply that already names its sender does not get named twice.
            Arg::Text(text) => {
                for line in text.lines() {
                    if line.starts_with(&target.identity) {
                        println!("{line}");
                    } else {
                        println!("{} {line}", target.identity);
                    }
                }
            }
            other => println!("{} {other}", target.identity),
        }
    }
    if response.status == Status::Ok {
        Ok(())
    } else {
        Err(Error::new(format!("{} answered {}", target.identity, response.status)))
    }
}

/// A process to talk to: a name to print, and an address to reach it at.
#[derive(Clone)]
struct Target {
    identity: String,
    address: SocketAddr,
}

fn open(target: &Target, timeout: Duration, config: &Messaging) -> Result<Conn> {
    Conn::open(target.address, timeout, config)
}

/// Which machine type answers a command, when none was named.
fn serves(cmd: &Cmd) -> ServiceKind {
    match cmd {
        // Any process can be asked about its own logs; without --to, ask KS.
        Cmd::LogHistogram { .. } | Cmd::LogGrep { .. } | Cmd::LogLines { .. } => ServiceKind::Ks,
        Cmd::Unlock | Cmd::SnMasterKey { .. } | Cmd::RotateMasterKey | Cmd::Registry => {
            ServiceKind::Ks
        }
        _ => ServiceKind::Fe,
    }
}

/// One process: whatever --at says, or the named one, or the first of the kind
/// that answers this command.
fn resolve(cluster: &ClusterConfig, cli: &Cli, kind: ServiceKind) -> Result<Target> {
    if let Some(address) = cli.at {
        return Ok(Target {
            identity: cli.to.clone().unwrap_or_else(|| address.to_string()),
            address,
        });
    }
    let identity = match &cli.to {
        Some(identity) => identity.clone(),
        None => {
            let entry = cluster.services_of_kind(kind).next().ok_or_else(|| {
                Error::new(format!("no {} in the roster", kind.as_str().to_uppercase()))
            })?;
            entry.identity.clone()
        }
    };
    if cluster.service(&identity).is_none() {
        return Err(Error::new(format!("{identity} is not in the roster")));
    }
    let address = match cluster.fixed_address(&identity) {
        Some(address) => address,
        None => lookup(cluster, &identity)?,
    };
    Ok(Target { identity, address })
}

/// Ask KS where a process is. Nothing but KS has an address of its own.
fn lookup(cluster: &ClusterConfig, identity: &str) -> Result<SocketAddr> {
    let timeout = Duration::from_millis(cluster.timeouts.request_ms);
    let mut ks = Conn::open(cluster.ks.address, timeout, &cluster.messaging)?;
    let mut payload = common::wire::StructWriter::new();
    payload.text(identity);
    let response = ks.call(&Req::new(Op::Lookup).structure(payload))?;
    match response.status {
        Status::Ok => response
            .message()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| Error::new(format!("KS gave no usable address for {identity}"))),
        status => Err(Error::new(format!(
            "KS answered {status} for {identity}: {}",
            response.message().unwrap_or("no reason given")
        ))),
    }
}

/// Which processes a control command addresses: the one named, or every one that
/// has registered, in cold-start order.
fn targets(cluster: &ClusterConfig, cli: &Cli, kind: ServiceKind) -> Result<Vec<Target>> {
    if cli.at.is_some() || cli.to.is_some() {
        return Ok(vec![resolve(cluster, cli, kind)?]);
    }
    let mut targets = Vec::new();
    for entry in cluster.cold_start_order() {
        match cluster.fixed_address(&entry.identity) {
            Some(address) => {
                targets.push(Target { identity: entry.identity.clone(), address });
            }
            // A process that has not registered is not reachable, and saying so
            // beats failing the whole command.
            None => match lookup(cluster, &entry.identity) {
                Ok(address) => targets.push(Target { identity: entry.identity.clone(), address }),
                Err(e) => eprintln!("laws: skipping {}: {e}", entry.identity),
            },
        }
    }
    if targets.is_empty() {
        return Err(Error::new("nothing in the roster is reachable"));
    }
    Ok(targets)
}

fn build(cmd: &Cmd) -> Result<Req> {
    Ok(match cmd {
        Cmd::Unlock => Req::new(Op::Unlock),
        Cmd::SnMasterKey { sn, day } => {
            let mut payload = common::wire::StructWriter::new();
            payload.number(*sn);
            payload.number(*day);
            Req::new(Op::SnMasterKey).structure(payload)
        }
        Cmd::RotateMasterKey => Req::new(Op::RotateMasterKey),

        Cmd::LogHistogram { from, to, which } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(which);
            payload.text(from);
            payload.text(to);
            Req::new(Op::LogHistogram).structure(payload)
        }
        Cmd::LogGrep { what, from, to, limit, which } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(which);
            payload.text(what);
            payload.text(from);
            payload.text(to);
            payload.number(*limit as u64);
            Req::new(Op::LogGrep).structure(payload)
        }
        Cmd::LogLines { from, to, limit, which } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(which);
            payload.text(from);
            payload.text(to);
            payload.number(*limit as u64);
            Req::new(Op::LogLines).structure(payload)
        }
        Cmd::ObjectInfo { path, want, limit, from } => {
            let asked = asking(want, *limit, *from);
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            payload.flag(asked != Want::plain());
            if asked != Want::plain() {
                asked.write(&mut payload);
            }
            Req::new(Op::ObjectInfo).structure(payload)
        }
        Cmd::BatchObjectInfo { paths, want, limit, from } => {
            let asked = asking(want, *limit, *from);
            let mut payload = common::wire::StructWriter::new();
            payload.number(paths.len() as u64);
            for path in paths {
                payload.text(path);
            }
            payload.flag(asked != Want::plain());
            if asked != Want::plain() {
                asked.write(&mut payload);
            }
            Req::new(Op::BatchInfo).structure(payload)
        }
        Cmd::Mkdir { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::MakeDirectory).structure(payload)
        }
        Cmd::Rmdir { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::RemoveDirectory).structure(payload)
        }
        Cmd::Ls { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::List).structure(payload)
        }
        Cmd::Stat { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::Stat).structure(payload)
        }

        Cmd::Create { path, flavor, framing, media_class, expiry } => {
            let (kind, width) = Framing::parse(framing)?.wire();
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            payload.flag(*flavor == Flavor::Keyed);
            payload.number(kind);
            payload.number(width);
            let media_class = media_class.clone().unwrap_or_default();
            payload.text(&media_class);
            payload.flag(expiry.is_some());
            if let Some(seconds) = expiry {
                payload.number(*seconds);
            }
            Req::new(Op::CreateObject).structure(payload)
        }
        Cmd::Delete { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::DeleteObject).structure(payload)
        }
        Cmd::Undelete { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::UndeleteObject).structure(payload)
        }
        Cmd::SetExpiry { path, seconds } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            match seconds {
                Some(seconds) => {
                    payload.flag(true);
                    payload.number(*seconds);
                }
                None => payload.flag(false),
            }
            Req::new(Op::SetExpiry).structure(payload)
        }
        Cmd::Clone { src, dst } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(src);
            payload.text(dst);
            Req::new(Op::CloneObject).structure(payload)
        }
        Cmd::Concat { dst, src } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(dst);
            payload.number(src.len() as u64);
            for one in src {
                payload.text(one);
            }
            Req::new(Op::ConcatObjects).structure(payload)
        }

        Cmd::Append { path, framing, crc, body, expect } => {
            let framing = Framing::parse(framing)?;
            let (kind, width) = framing.wire();
            let block = frame(framing, &body.bytes()?)?;
            // Over the framed block, which is the logical form — not what it will be
            // compressed to, since that is the system's representation of the data
            // rather than the data.
            let checksum = if *crc { common::crc::of(&block) } else { 0 };

            // Compressed here and nowhere below: the client is as far upstream as
            // compression goes, so the bytes are smaller across the internet and no
            // tier in the middle spends CPU on them.
            let stored = squeeze(Compress::Lz4, &block).unwrap_or_else(|| block.clone());
            let holds = Holds::of(framing, b"", &block, stored.len())?;

            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            expecting(expect)?.write(&mut payload);
            payload.number(kind);
            payload.number(width);
            payload.number(checksum);
            holds.write(&mut payload);
            payload.bytes(&stored);
            Req::new(Op::AppendRecord).structure(payload)
        }
        Cmd::Insert { path, key, body } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            payload.text(key);
            payload.bytes(&body.bytes()?);
            Req::new(Op::InsertRecord).structure(payload)
        }
        Cmd::Update { path, at, body } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            match at.arg() {
                Arg::Number(record) => {
                    payload.number(0);
                    payload.number(record);
                }
                Arg::Text(key) => {
                    payload.number(1);
                    payload.text(&key);
                }
                _ => unreachable!(),
            }
            payload.bytes(&body.bytes()?);
            Req::new(Op::UpdateRecord).structure(payload)
        }
        Cmd::DeleteRecord { path, at } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            match at.arg() {
                Arg::Number(record) => {
                    payload.number(0);
                    payload.number(record);
                }
                Arg::Text(key) => {
                    payload.number(1);
                    payload.text(&key);
                }
                _ => unreachable!(),
            }
            Req::new(Op::DeleteRecord).structure(payload)
        }
        Cmd::Read { path, at, expect, .. } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            expecting(expect)?.write(&mut payload);
            match at.arg() {
                // A point read is a range of one record (§9.1.3.6): the block
                // holding it comes back whole either way, and asking this way means
                // there is one shape on the read path rather than two.
                Arg::Number(record) => {
                    Range { start: Start::Record(record), budget: Budget::Records(1) }
                        .write_nested(&mut payload)
                }
                // A key, which is a keyed read and unbuilt. Sent as it stands so the
                // answer says so rather than the client guessing.
                key => {
                    let mut payload = common::wire::StructWriter::new();
                    payload.text(path);
                    expecting(expect)?.write(&mut payload);
                    payload.number(1);
                    if let Arg::Text(key) = key {
                        payload.text(&key);
                    } else {
                        unreachable!();
                    }
                    return Ok(Req::new(Op::ReadRecord).structure(payload));
                }
            }
            Req::new(Op::ReadRecord).structure(payload)
        }

        // Scan is a loop over range reads rather than one request, so it is driven
        // in `scan` and never reaches here.
        Cmd::Scan { .. } => return Err(Error::new("a scan is many requests")),
        Cmd::Count { path } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            Req::new(Op::Count).structure(payload)
        }
        Cmd::TimeRange { path, from, to } => {
            let mut payload = common::wire::StructWriter::new();
            payload.text(path);
            payload.number(*from);
            payload.number(*to);
            Req::new(Op::TimeRange).structure(payload)
        }

        // The commands above are the ones with a request to build; the rest are
        // answered locally or handled by their own paths.
        other => return Err(Error::new(format!("{} is not a request", name(other)))),
    })
}

fn name(cmd: &Cmd) -> &'static str {
    match cmd {
        Cmd::Ping { .. } => "ping",
        Cmd::Status => "status",
        Cmd::Shutdown { .. } => "shutdown",
        Cmd::Services => "services",
        Cmd::Placement => "placement",
        Cmd::MediaClasses => "media-classes",
        _ => "that command",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ⚠️ **The plain request must stay byte-identical to what it was before `--want` existed**,
    /// because §17.5g's 263k/s is the baseline #79 will be measured against. If naming no detail
    /// started sending four extra arguments, the comparison would be against a different request
    /// and the regression would be invisible.
    #[test]
    fn asking_for_nothing_is_the_request_that_predates_the_field() {
        assert_eq!(asking(&[], 0, 0), Want::plain());
        let bare = Cmd::ObjectInfo { path: "/x".into(), want: Vec::new(), limit: 0, from: 0 };
        let request = build(&bare).expect("a request");
        let (mut payload, rest) =
            structure(&request.args, Op::ObjectInfo, "an object-info request")
                .expect("a self-contained request");
        assert!(rest.is_empty());
        assert_eq!(payload.text().unwrap(), "/x");
        assert!(!payload.flag().unwrap());
        payload.done().unwrap();
    }

    /// And anything actually asked for does travel, in the four arguments `Want` defines.
    #[test]
    fn asking_for_something_sends_the_want_field() {
        let asked =
            Cmd::ObjectInfo { path: "/x".into(), want: vec![Want1::Instances], limit: 7, from: 3 };
        let request = build(&asked).expect("a request");
        let (mut payload, rest) =
            structure(&request.args, Op::ObjectInfo, "an object-info request")
                .expect("a self-contained request");
        assert!(rest.is_empty());
        assert_eq!(payload.text().unwrap(), "/x");
        assert!(payload.flag().unwrap());
        let want = Want::read(&mut payload).expect("a want");
        payload.done().unwrap();
        assert!(want.wants(Detail::Instances) && want.wants(Detail::Extents));
        assert_eq!((want.limit, want.from), (7, Start::Record(3)));
    }

    /// A limit or a start without any detail is not detail, so it must not make a plain lookup
    /// look like one asking for something unbuilt.
    #[test]
    fn a_limit_alone_asks_for_no_detail() {
        let asked = asking(&[], 500, 0);
        assert_eq!(asked.detail, 0);
        assert!(asked.beyond(0).is_empty(), "so nothing is beyond what is built");
        assert_ne!(asked, Want::plain(), "but it still travels, since the limit is meant");
    }

    /// Decompressing checks the CRC over the uncompressed block bytes.
    #[test]
    fn records_of_verifies_crc_over_block_bytes() {
        let framing = Framing::Lf;
        let body1 = b"hello";
        let body2 = b"world";
        let block1 = frame(framing, body1).unwrap();
        let block2 = frame(framing, body2).unwrap();
        let piece1 = Piece {
            logical: framing.canonical_size(0, &block1).unwrap(),
            first_record: 0,
            records: 1,
            crc: common::crc::of(&block1),
            bytes: block1.clone(),
        };
        let piece2 = Piece {
            logical: framing.canonical_size(0, &block2).unwrap(),
            first_record: 1,
            records: 1,
            crc: common::crc::of(&block2),
            bytes: block2.clone(),
        };
        let found = records_of(framing, &[piece1.clone(), piece2]).unwrap();
        assert_eq!(found, vec![(0, body1.to_vec()), (1, body2.to_vec())]);

        // A wrong CRC is detected and refused.
        let corrupt = Piece { crc: 0xdeadbeef, ..piece1 };
        assert!(records_of(framing, &[corrupt]).is_err());
    }
}
