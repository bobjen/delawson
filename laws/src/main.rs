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
use common::config::{ClusterConfig, ServiceKind};
use common::placement;
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
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
        /// Bytes of random padding to ask for in the reply. Zero answers in
        /// words; anything else makes message size the variable.
        #[arg(long, default_value_t = 0)]
        size: u64,
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
        /// Bytes of random padding in each reply.
        #[arg(long, default_value_t = 0)]
        size: u64,
        /// Seconds to measure.
        #[arg(long, default_value_t = 10)]
        seconds: u64,
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
    /// KS's fixed address and the port base, which is what a starter needs.
    Bootstrap,
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
    /// A second name for an object, sharing its bytes.
    Clone { src: String, dst: String },
    /// One object from several, in order.
    Concat {
        dst: String,
        #[arg(required = true)]
        src: Vec<String>,
    },

    /// Append a record to an unkeyed object.
    Append {
        path: String,
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
        #[command(flatten)]
        at: At,
    },
    /// Read records in order.
    Scan {
        path: String,
        /// Record number or key to start at.
        #[arg(long)]
        from: Option<String>,
        /// Record number or key to stop before.
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        limit: Option<u64>,
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
    let timeout = Duration::from_millis(cluster.timeouts.request_ms);

    match &cli.cmd {
        Cmd::Services => {
            for s in cluster.cold_start_order() {
                println!("{:<6} {:<3} {}", s.identity, s.kind, s.domain.join("/"));
            }
            Ok(())
        }
        Cmd::Registry => {
            let target = resolve(&cluster, &cli, ServiceKind::Ks)?;
            let response = open(&target, timeout)?.call(&Request::new(Op::Registry))?;
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
        Cmd::Ping { count, conns, size, reconnect } => {
            let plan =
                Plan { count: *count, conns: *conns, size: *size, reconnect: *reconnect, timeout };
            ping(&cluster, &cli, &plan)
        }
        Cmd::Bench { clients, warmup, size, seconds } => {
            // Anything answers a ping, so an unnamed target is KS, which is the
            // one process that is always there.
            let target = resolve(&cluster, &cli, ServiceKind::Ks)?;
            let plan = Bench {
                clients: *clients,
                warmup: *warmup,
                size: *size,
                seconds: *seconds,
                timeout,
            };
            bench(&target, &plan)
        }
        Cmd::Status => {
            for target in targets(&cluster, &cli, ServiceKind::Ks)? {
                report(&target, &open(&target, timeout)?.call(&Request::new(Op::Status))?)?;
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
                report(&target, &open(&target, timeout)?.call(&Request::new(Op::Shutdown))?)?;
            }
            Ok(())
        }
        cmd => {
            let request = build(cmd)?;
            let target = resolve(&cluster, &cli, serves(cmd))?;
            report(&target, &open(&target, timeout)?.call(&request)?)
        }
    }
}

struct Plan {
    count: usize,
    conns: usize,
    size: u64,
    reconnect: bool,
    timeout: Duration,
}

/// A ping, asking for `size` bytes of padding back.
fn ping_request(size: u64) -> Request {
    let request = Request::new(Op::Ping);
    if size == 0 { request } else { request.number(size) }
}

/// The ping is a benchmark, not a feature: the work it asks for is a few hundred
/// instructions, so with a bare reply what comes back is per-message overhead,
/// and with a padded one it is that plus what size costs.
fn ping(cluster: &ClusterConfig, cli: &Cli, plan: &Plan) -> Result<()> {
    if plan.count == 0 || plan.conns == 0 {
        return Err(Error::new("--count 0 or --conns 0 measures nothing"));
    }
    for target in targets(cluster, cli, ServiceKind::Ks)? {
        let addr = target.address;
        let start = std::time::Instant::now();
        let threads: Vec<_> = (0..plan.conns)
            .map(|_| {
                let (count, size) = (plan.count, plan.size);
                let (reconnect, timeout) = (plan.reconnect, plan.timeout);
                std::thread::spawn(move || -> Result<(Vec<Duration>, Response)> {
                    let request = ping_request(size);
                    let mut conn = Conn::open(addr, timeout)?;
                    let mut samples = Vec::with_capacity(count);
                    let mut last = conn.call(&request)?;
                    for _ in 0..count {
                        if reconnect {
                            conn = Conn::open(addr, timeout)?;
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

struct Bench {
    clients: usize,
    warmup: u64,
    size: u64,
    seconds: u64,
    timeout: Duration,
}

/// Warm up, then measure. Throughput is the number to want first, and average
/// latency is what says how much of it is per-message overhead.
fn bench(target: &Target, plan: &Bench) -> Result<()> {
    if plan.clients == 0 || plan.seconds == 0 {
        return Err(Error::new("--clients and --seconds must both be positive"));
    }
    let addr = target.address;
    let warm_end = std::time::Instant::now() + Duration::from_secs(plan.warmup);
    let end = warm_end + Duration::from_secs(plan.seconds);

    let threads: Vec<_> = (0..plan.clients)
        .map(|_| {
            let (size, timeout) = (plan.size, plan.timeout);
            std::thread::spawn(move || -> Result<Tally> {
                let request = ping_request(size);
                let mut conn = Conn::open(addr, timeout)?;
                conn.press(&request, warm_end, &mut Tally::new())?;
                let mut tally = Tally::new();
                conn.press(&request, end, &mut tally)?;
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
        "{} {} clients, {} byte replies, {measured:.1} s measured after {} s warm-up",
        target.identity, plan.clients, plan.size, plan.warmup
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
    Ok(())
}

/// Print a response, and fail the process if the service refused.
fn report(target: &Target, response: &Response) -> Result<()> {
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

fn open(target: &Target, timeout: Duration) -> Result<Conn> {
    Conn::open(target.address, timeout)
}

/// Which machine type answers a command, when none was named.
fn serves(cmd: &Cmd) -> ServiceKind {
    match cmd {
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
    let mut ks = Conn::open(cluster.ks.address, timeout)?;
    let response = ks.call(&Request::new(Op::Lookup).text(identity))?;
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

fn build(cmd: &Cmd) -> Result<Request> {
    Ok(match cmd {
        Cmd::Unlock => Request::new(Op::Unlock),
        Cmd::SnMasterKey { sn, day } => Request::new(Op::SnMasterKey).number(*sn).number(*day),
        Cmd::RotateMasterKey => Request::new(Op::RotateMasterKey),

        Cmd::Mkdir { path } => Request::new(Op::MakeDirectory).text(path),
        Cmd::Rmdir { path } => Request::new(Op::RemoveDirectory).text(path),
        Cmd::Ls { path } => Request::new(Op::List).text(path),
        Cmd::Stat { path } => Request::new(Op::Stat).text(path),

        Cmd::Create { path, flavor, media_class, expiry } => {
            let mut request = Request::new(Op::CreateObject)
                .text(path)
                .flag(*flavor == Flavor::Keyed)
                .text(media_class.clone().unwrap_or_default());
            if let Some(seconds) = expiry {
                request = request.number(*seconds);
            }
            request
        }
        Cmd::Delete { path } => Request::new(Op::DeleteObject).text(path),
        Cmd::Undelete { path } => Request::new(Op::UndeleteObject).text(path),
        Cmd::SetExpiry { path, seconds } => {
            let request = Request::new(Op::SetExpiry).text(path);
            match seconds {
                Some(seconds) => request.number(*seconds),
                None => request.flag(false),
            }
        }
        Cmd::Clone { src, dst } => Request::new(Op::CloneObject).text(src).text(dst),
        Cmd::Concat { dst, src } => {
            let mut request = Request::new(Op::ConcatObjects).text(dst);
            for one in src {
                request = request.text(one);
            }
            request
        }

        Cmd::Append { path, body } => {
            Request::new(Op::AppendRecord).text(path).bytes(body.bytes()?)
        }
        Cmd::Insert { path, key, body } => {
            Request::new(Op::InsertRecord).text(path).text(key).bytes(body.bytes()?)
        }
        Cmd::Update { path, at, body } => {
            let mut request = Request::new(Op::UpdateRecord).text(path);
            request.args.push(at.arg());
            request.bytes(body.bytes()?)
        }
        Cmd::DeleteRecord { path, at } => {
            let mut request = Request::new(Op::DeleteRecord).text(path);
            request.args.push(at.arg());
            request
        }
        Cmd::Read { path, at } => {
            let mut request = Request::new(Op::ReadRecord).text(path);
            request.args.push(at.arg());
            request
        }
        Cmd::Scan { path, from, to, limit } => Request::new(Op::Scan)
            .text(path)
            .text(from.clone().unwrap_or_default())
            .text(to.clone().unwrap_or_default())
            .number(limit.unwrap_or(0)),
        Cmd::Count { path } => Request::new(Op::Count).text(path),
        Cmd::TimeRange { path, from, to } => {
            Request::new(Op::TimeRange).text(path).number(*from).number(*to)
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
