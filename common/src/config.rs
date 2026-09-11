//! Configuration, in three layers.
//!
//! | Layer | One per | Checked in? | Holds |
//! |---|---|---|---|
//! | cluster | cluster | yes | everything every process must agree on |
//! | service | machine type | yes | knobs one machine type owns |
//! | process | process | no, written by `start-laws` | identity, and where the other two are |
//!
//! A process is told who it is, which port to listen on, and where to look;
//! everything else comes from files that are identical fleet-wide.
//!
//! Only KS has a fixed address, and it is in the cluster file. Every other
//! process is handed a port by whatever starts it, announces that port to KS, and
//! asks KS where its peers are — so no process's address is checked in, and no
//! two copies of one address can disagree.

use crate::offset::{ECoff, ELoff};
use crate::placement::Expansion;
use crate::time::{TimeIntMilli64, TimeIntSec64};
use crate::{Context, Error, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceKind {
    /// Key Store: the daily master keys.
    Ks,
    /// Directory Node: paths, objects, streams.
    Dn,
    /// Stream Node: extents and their instance lists.
    Sn,
    /// Extent Node: bytes on disks.
    En,
    /// Front End: authorization, and coordination for clients.
    Fe,
}

impl ServiceKind {
    /// KS first, because it is the one root keyed by humans rather than by
    /// DeLawson; FE last, once there is something to serve.
    pub const COLD_START_ORDER: [ServiceKind; 5] =
        [ServiceKind::Ks, ServiceKind::Dn, ServiceKind::Sn, ServiceKind::En, ServiceKind::Fe];

    /// The name used in configuration files and as the executable name.
    pub fn as_str(self) -> &'static str {
        match self {
            ServiceKind::Ks => "ks",
            ServiceKind::Dn => "dn",
            ServiceKind::Sn => "sn",
            ServiceKind::En => "en",
            ServiceKind::Fe => "fe",
        }
    }

    pub fn parse(s: &str) -> Result<ServiceKind> {
        ServiceKind::COLD_START_ORDER
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| Error::new(format!("unknown service kind {s:?}")))
    }
}

impl std::fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A roster line. One line may stand for several processes of one kind, which is
/// what a test cluster wants and what a real one wants per rack.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceSpec {
    /// Given only for a single process. With a count, identities are the kind
    /// and a number, taken in roster order.
    identity: Option<String>,
    kind: ServiceKind,
    #[serde(default = "one")]
    count: usize,
    domain: Vec<String>,
    #[serde(default)]
    media_class: Option<String>,
}

fn one() -> usize {
    1
}

#[derive(Debug, Clone)]
pub struct ServiceEntry {
    /// `KS0`, `FE0`: kind plus instance number.
    pub identity: String,
    pub kind: ServiceKind,
    /// Position in the nested failure-domain hierarchy, outermost first, one
    /// entry per level of `FailureDomains::levels`.
    pub domain: Vec<String>,
    /// Which media-class row this process's storage follows. ENs have one;
    /// nothing else does.
    pub media_class: Option<String>,
}

/// Where KS listens, which is the one address a cluster cannot discover.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KsBootstrap {
    pub address: SocketAddr,
    /// Ports from here upward are what a starter hands out to everything else.
    pub port_base: u16,
}

/// Nested failure domains, coarse to fine, each level a refinement of the one
/// above. Cross-cutting domain types are a bug, not a configuration option.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDomains {
    pub levels: Vec<String>,
}

/// Per-device-class constants. These are per class, not global, and the fleet
/// will be mixed for years, so they live in one table rather than scattered
/// through the code. Every value is a guess until measured.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaClass {
    pub name: String,
    /// Sparse-index checkpoint interval, in physical bytes.
    pub index_interval: u64,
    /// Never read less than this from the device: half the break-even, since past
    /// that point the seek is already amortized.
    pub min_read_bytes: u64,
    /// And never write less than this, which is the whole break-even rather than
    /// half of it: a short read still delivers the bytes asked for, while a short
    /// write pays the same seek for less work.
    pub min_write_bytes: u64,
    pub compress_block: u64,
    /// Appends at least this large skip EN-internal staging.
    pub staging_bypass: u64,
    /// True on HDD, where compression buys read bandwidth; false on SSD, where
    /// it costs.
    pub compress_read_win: bool,
    pub repair_bytes_sec: u64,
}

/// Who decides when a connection sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SendMode {
    /// Whoever produced the answer writes it, and flushes when it knows nothing
    /// more is in hand. Coalesces only what one thread holds.
    Direct,
    /// A sender per connection loops: write everything queued, and let the next
    /// batch be whatever piled up meanwhile. Coalesces whoever produced it.
    Sender,
    /// Direct until an answer is produced off the read loop, then a sender for
    /// the rest of that connection's life.
    Adaptive,
}

/// What to do to a batch before it is encrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compress {
    None,
    Lz4,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Messaging {
    pub send: SendMode,
    /// Every message is encrypted. False exists to measure what that costs, and
    /// is not a configuration to run.
    pub encrypt: bool,
    pub compress: Compress,
    /// Batches smaller than this are not worth trying to compress.
    pub compress_min: usize,
    /// Compress this many bytes of a large batch first, and give up on the rest
    /// if that much did not shrink. Zero always compresses the whole batch.
    pub compress_sample: usize,
    /// Connections a process keeps to each process it talks to. One is a
    /// bottleneck: the far side reads one connection with one thread, so a
    /// forwarding tier cannot outrun a single read loop however many workers it
    /// has.
    pub peer_conns: usize,
    /// ⚠️ A pre-shared key, in hex, which is a stand-in until KS hands out
    /// message keys. Everything in a cluster shares it, so it is a test key and
    /// nothing else.
    pub key: String,
}

impl Messaging {
    pub fn key(&self) -> Result<[u8; 16]> {
        let hex = self.key.as_bytes();
        if hex.len() != 32 {
            return Err(Error::new("messaging.key is 32 hex digits, for 16 bytes"));
        }
        let mut key = [0u8; 16];
        for (byte, pair) in key.iter_mut().zip(hex.chunks(2)) {
            let digits = std::str::from_utf8(pair).map_err(|_| Error::new("messaging.key"))?;
            *byte = u8::from_str_radix(digits, 16)
                .map_err(|_| Error::new(format!("messaging.key has {digits:?} in it")))?;
        }
        Ok(key)
    }
}

/// How much log a process keeps, and where the line between the two logs falls.
/// Sizes are here rather than per machine type because a fleet wants one answer;
/// a type that needs its own can grow one later.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Logging {
    /// debug, info, notice, warning, error.
    pub level: String,
    /// Store the files compressed, since the log tool is what reads them.
    pub compress: bool,
    /// Which media class the log disk is, by name in the cluster table. A block
    /// of lines is accumulated until it will be a write that device wants: the
    /// table's minimum read is the same number.
    pub disk: String,
    /// What a block of lines is expected to compress by, which is what turns the
    /// device's wanted write size into an amount of text to accumulate. A guess,
    /// and `status` reports what was actually achieved. It applies with compression
    /// off as well, because what the block sets is how often a write happens.
    pub compress_ratio: u64,
    /// Bytes per file, before the next one is started.
    pub size: u64,
    /// Files kept of the full log, which carries every line. Size times count is
    /// how far back it reaches — hours.
    pub full_files: usize,
    /// Files kept of the info log, which carries everything but the chatty
    /// lines, so the same bytes reach back days.
    pub info_files: usize,
    /// The share of the full log's lines the info log should carry. Rare lines
    /// pass whole; busy ones are sampled to make the total come out here.
    pub info_share: f64,
    /// Lines between recounts of which sites are hot.
    pub recount: usize,
}

impl Logging {
    /// The media-class table is what says how large a write the log disk wants, so
    /// the block size is derived rather than configured twice.
    pub fn build(&self, media: &[MediaClass]) -> Result<crate::log::Logging> {
        let disk = media.iter().find(|class| class.name == self.disk).ok_or_else(|| {
            Error::new(format!("logging.disk {:?} is not a media class", self.disk))
        })?;
        // What the device wants of a write, times what the text shrinks by —
        // whether or not it is being compressed. What a block decides is how often
        // a write happens, and measurement says that is the cost: at 275 MB/s of
        // lines, writing 40 KB at a time cost 15% of a ping's throughput against
        // writing ten times less often. Bytes are nearly free by comparison, since
        // a write lands in the page cache.
        let block = disk.min_write_bytes * self.compress_ratio.max(1);
        Ok(crate::log::Logging {
            block: block as usize,
            level: crate::log::Level::parse(&self.level).ok_or_else(|| {
                Error::new(format!("logging.level {:?} is not a level", self.level))
            })?,
            compress: self.compress,
            size: self.size,
            full_files: self.full_files,
            info_files: self.info_files,
            info_share: self.info_share,
            recount: self.recount,
        })
    }
}

/// How many instances an extent has, and how many must have applied an append
/// before it is durable.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Extents {
    pub instances: usize,
    pub quorum: usize,
    /// Compressed bytes an extent may accumulate before it seals. The design's
    /// figure is 32 MB and it is a guess bracketed from both sides; a test cluster
    /// sets it small so that sealing is the common path rather than the rare one.
    /// It bounds the first write only — recompression may leave an extent larger.
    pub seal_bytes: u32,
    /// Extents a stream may hold before the stream itself is sealed and the object
    /// needs another. A real stream holds as many as its SN has memory for; a test
    /// cluster sets it low so that a stream sealing is the common path.
    pub per_stream: usize,
    /// Maximum logical bytes allowed in a single append. Default is 32 MB (33,554,432 bytes),
    /// bounded above by ELoff::MAX.
    #[serde(default = "default_max_append_bytes")]
    pub max_append_bytes: u32,
    /// Bytes of device a stored byte costs, which is what billing charges for
    /// (§17.25.3). Absent means plain replication, so `instances`.
    ///
    /// ⚠️ **A double, and the first parameter here that is one** (Asserted, Bob,
    /// 2026-08-20). Erasure coding's factors are not whole numbers — 4/3, 7/3, 6/5 — and
    /// the next scheme need not even be a ratio, so this is the number itself rather than
    /// anything to be divided later (§18.2).
    #[serde(default)]
    pub expansion: Option<f64>,
}

fn default_max_append_bytes() -> u32 {
    32 * 1024 * 1024
}

impl Extents {
    /// How many of the smallest responses a failure-mode seal discards. With four
    /// instances and a quorum of three that is one, so "the minimum, discarding the
    /// smallest"; with three of three it is none, so plainly the minimum.
    pub fn discard(&self) -> usize {
        self.instances.saturating_sub(self.quorum)
    }

    /// Responses a failure-mode seal needs before it can compute anything at all:
    /// one more than it discards.
    pub fn least_heard(&self) -> usize {
        self.discard() + 1
    }

    /// What a stored byte costs on the device. ⚠️ Configured where it is not simply the
    /// copy count, which is every scheme except plain replication.
    pub fn expansion(&self) -> Expansion {
        match self.expansion {
            Some(factor) => Expansion::of(factor),
            None => Expansion::copies(self.instances as u32),
        }
    }

    fn validate(&self) -> Result<()> {
        if let Some(factor) = self.expansion
            && !(factor.is_finite() && factor >= 1.0)
        {
            return Err(Error::new(format!(
                "an expansion of {factor} would store less than was written"
            )));
        }
        if self.per_stream == 0 {
            return Err(Error::new(
                "extents.per_stream of zero leaves a stream nowhere to put an extent",
            ));
        }
        if self.max_append_bytes == 0 {
            return Err(Error::new(
                "extents.max_append_bytes of zero leaves no room for an append",
            ));
        }
        if self.max_append_bytes > ELoff::MAX.get() {
            return Err(Error::new(format!(
                "extents.max_append_bytes {} is past what a position in an extent can name",
                self.max_append_bytes
            )));
        }
        if self.seal_bytes == 0 {
            return Err(Error::new("extents.seal_bytes of zero seals every extent empty"));
        }
        if self.seal_bytes > ECoff::MAX.get() {
            return Err(Error::new(format!(
                "extents.seal_bytes {} is past what a position in an extent can name",
                self.seal_bytes
            )));
        }
        if self.quorum == 0 || self.instances == 0 {
            return Err(Error::new("extents.instances and extents.quorum must be positive"));
        }
        if self.quorum > self.instances {
            return Err(Error::new(format!(
                "extents.quorum {} is more instances than the {} an extent has",
                self.quorum, self.instances
            )));
        }
        // Two quorums must overlap, or two disjoint sets could each hold an append
        // the other never saw and a new primary polling one of them would miss it.
        // Three of four overlaps in two; two of four does not overlap at all.
        if 2 * self.quorum <= self.instances {
            return Err(Error::new(format!(
                "extents.quorum {} of {} lets two quorums be disjoint because {} plus {} is not more than {}",
                self.quorum, self.instances, self.quorum, self.quorum, self.instances
            )));
        }
        Ok(())
    }
}

/// Raft ring composition. One number fleet-wide for now; nothing yet
/// distinguishes KS, DN and SN ring sizes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rings {
    pub members: usize,
}

/// Hedge delays are deliberately absent: they differ per tier by orders of
/// magnitude and are undesigned, and a wrong global value is worse than none.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timeouts {
    #[serde(deserialize_with = "nonnegative_milliseconds")]
    pub request_ms: TimeIntMilli64,
    /// How long `stop-laws` waits after asking a process to stop, before
    /// escalating to a signal.
    #[serde(deserialize_with = "nonnegative_milliseconds")]
    pub shutdown_grace_ms: TimeIntMilli64,
}

fn nonnegative_milliseconds<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<TimeIntMilli64, D::Error> {
    let interval = TimeIntMilli64::new(i64::deserialize(d)?);
    interval.to_duration().map_err(serde::de::Error::custom)?;
    Ok(interval)
}

fn nonnegative_seconds<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<TimeIntSec64, D::Error> {
    let interval = TimeIntSec64::new(i64::deserialize(d)?);
    interval.to_duration().map_err(serde::de::Error::custom)?;
    Ok(interval)
}

/// The cluster file as written. Roster lines are expanded into one entry per
/// process before anything else sees them.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterFile {
    schema_version: u32,
    cluster_name: String,
    #[serde(
        default = "default_max_soft_delete_retention_secs",
        deserialize_with = "nonnegative_seconds"
    )]
    max_soft_delete_retention_secs: TimeIntSec64,
    ks: KsBootstrap,
    failure_domains: FailureDomains,
    extents: Extents,
    rings: Rings,
    messaging: Messaging,
    logging: Logging,
    timeouts: Timeouts,
    #[serde(default, rename = "service")]
    service: Vec<ServiceSpec>,
    #[serde(default, rename = "media_class")]
    media_class: Vec<MediaClass>,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Bumped on an incompatible change, so a process refuses a configuration it
    /// does not understand rather than misreading it.
    pub schema_version: u32,
    pub cluster_name: String,
    /// Upper bound enforced independently by FE, DN, SN, and EN.
    pub max_soft_delete_retention_secs: TimeIntSec64,
    pub ks: KsBootstrap,
    pub failure_domains: FailureDomains,
    pub extents: Extents,
    pub rings: Rings,
    pub messaging: Messaging,
    pub logging: Logging,
    pub timeouts: Timeouts,
    pub services: Vec<ServiceEntry>,
    pub media_classes: Vec<MediaClass>,
}

pub const SCHEMA_VERSION: u32 = 1;

pub fn default_max_soft_delete_retention_secs() -> TimeIntSec64 {
    TimeIntSec64::new(30 * 24 * 60 * 60)
}

impl ServiceEntry {
    /// The number in the identity, which is this process's id among its kind:
    /// the `DnId` a stream is named by, the `SnId` an extent is named by, or the `EnId`
    /// holding an extent instance.
    pub fn number(&self) -> u64 {
        self.identity.trim_start_matches(|c: char| !c.is_ascii_digit()).parse().unwrap_or(0)
    }
}

impl ClusterConfig {
    pub fn load(path: &Path) -> Result<ClusterConfig> {
        let text = std::fs::read_to_string(path).context(path.display())?;
        ClusterConfig::parse(&text).context(path.display())
    }

    pub fn parse(text: &str) -> Result<ClusterConfig> {
        let file: ClusterFile = toml::from_str(text).context("cluster config")?;

        // One entry per process, numbered per kind in roster order, so a line
        // saying `count = 4` needs no hand-written identities to go with it.
        let mut services = Vec::new();
        let mut next: BTreeMap<ServiceKind, usize> = BTreeMap::new();
        for spec in file.service {
            if spec.identity.is_some() && spec.count != 1 {
                return Err(Error::new(format!(
                    "{:?} names one process but asks for {}",
                    spec.identity.unwrap_or_default(),
                    spec.count
                )));
            }
            for _ in 0..spec.count {
                let number = next.entry(spec.kind).or_insert(0);
                let identity = spec
                    .identity
                    .clone()
                    .unwrap_or_else(|| format!("{}{number}", spec.kind.as_str().to_uppercase()));
                *number += 1;
                services.push(ServiceEntry {
                    identity,
                    kind: spec.kind,
                    domain: spec.domain.clone(),
                    media_class: spec.media_class.clone(),
                });
            }
        }

        let config = ClusterConfig {
            schema_version: file.schema_version,
            cluster_name: file.cluster_name,
            max_soft_delete_retention_secs: file.max_soft_delete_retention_secs,
            ks: file.ks,
            failure_domains: file.failure_domains,
            extents: file.extents,
            rings: file.rings,
            messaging: file.messaging,
            logging: file.logging,
            timeouts: file.timeouts,
            services,
            media_classes: file.media_class,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        self.max_soft_delete_retention_secs.to_duration()?;
        if self.schema_version != SCHEMA_VERSION {
            return Err(Error::new(format!(
                "schema version {}, this build understands {SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.failure_domains.levels.is_empty() {
            return Err(Error::new("failure_domains.levels is empty"));
        }
        self.extents.validate()?;
        let mut seen = BTreeSet::new();
        for service in &self.services {
            if !seen.insert(&service.identity) {
                return Err(Error::new(format!(
                    "two services claim identity {:?}",
                    service.identity
                )));
            }
            let want = self.failure_domains.levels.len();
            if service.domain.len() != want {
                return Err(Error::new(format!(
                    "{} has {} domain components, expected {want} for levels {:?}",
                    service.identity,
                    service.domain.len(),
                    self.failure_domains.levels
                )));
            }
            if let Some(name) = &service.media_class
                && self.media_class(name).is_none()
            {
                return Err(Error::new(format!(
                    "{} names media class {name:?}, which the table does not define",
                    service.identity
                )));
            }
        }
        Ok(())
    }

    pub fn service(&self, identity: &str) -> Option<&ServiceEntry> {
        self.services.iter().find(|s| s.identity == identity)
    }

    /// The KS that holds the registry. One for now; a KS ring of several members
    /// needs an address for each, since none of them can be looked up.
    pub fn registrar(&self) -> Result<&ServiceEntry> {
        self.services_of_kind(ServiceKind::Ks)
            .next()
            .ok_or_else(|| Error::new("no KS in the roster, so nothing can register"))
    }

    /// The address of a process, for the one process whose address is known
    /// without asking KS.
    pub fn fixed_address(&self, identity: &str) -> Option<SocketAddr> {
        let registrar = self.registrar().ok()?;
        (registrar.identity == identity).then_some(self.ks.address)
    }

    pub fn services_of_kind(&self, kind: ServiceKind) -> impl Iterator<Item = &ServiceEntry> {
        self.services.iter().filter(move |s| s.kind == kind)
    }

    pub fn media_class(&self, name: &str) -> Option<&MediaClass> {
        self.media_classes.iter().find(|m| m.name == name)
    }

    pub fn cold_start_order(&self) -> Vec<&ServiceEntry> {
        let rank = |kind: ServiceKind| {
            ServiceKind::COLD_START_ORDER.iter().position(|c| *c == kind).unwrap_or(usize::MAX)
        };
        let mut ordered: Vec<&ServiceEntry> = self.services.iter().collect();
        ordered.sort_by(|a, b| {
            rank(a.kind).cmp(&rank(b.kind)).then_with(|| a.identity.cmp(&b.identity))
        });
        ordered
    }
}

/// The part of a service configuration every machine type has. Each type's file
/// carries this table and then whatever else that type owns.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Knobs {
    /// Threads answering requests off the read loop. Zero means one per core.
    pub workers: usize,
}

/// KS's own configuration. Every other machine type gets a file of this shape,
/// holding the knobs that type owns and nothing else.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KsConfig {
    pub knobs: Knobs,
    /// Generate a master key only for a day that creates extents. Marching one
    /// key per elapsed day through a long dormancy would take the store from
    /// megabytes to terabytes.
    pub lazy_day_keys: bool,
    /// Delay between generating a replacement master key and making it
    /// effective, so ENs can cache the current derived key safely.
    #[serde(deserialize_with = "nonnegative_seconds")]
    pub master_key_delay_secs: TimeIntSec64,
    /// Whether the operator-and-hardware unlocking ceremony is required before
    /// KS will answer key requests. False on a test box — and `status` says so
    /// rather than staying quiet about it.
    pub require_unlock: bool,
    /// Answer a ping on the connection's read loop rather than handing it to a
    /// worker. Here to be turned off, since what it costs to hand one small
    /// message to a scheduler is a number worth having.
    pub inline_ping: bool,
}

impl KsConfig {
    pub fn load(path: &Path) -> Result<KsConfig> {
        read_toml(path)
    }
}

/// DN's own configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DNConfig {
    pub knobs: Knobs,
    /// Object metadata is itself DeLawson keyed objects, reached through a root
    /// held outside the object hierarchy. Nothing reads this yet; it is here so
    /// the file exists where the decision will land.
    pub self_hosted: bool,
    /// The most entries — objects plus streams plus extents — one `object-info`
    /// answer may carry (§9.1.3.7). **One number over all three kinds**, because what
    /// has to be bounded is the answer's size and a per-kind cap lets one pathological
    /// object evade the total. A request may name a smaller cap; a larger one is
    /// capped rather than refused.
    pub info_entries: u64,
    /// Maximum number of sealed objects one concatenate request may name.
    #[serde(default = "default_max_concat_sources")]
    pub max_concat_sources: u64,
}

fn default_max_concat_sources() -> u64 {
    1024
}

/// SN's own configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SNConfig {
    pub knobs: Knobs,
    /// A machine's extent budget is split between streams and extents, and the
    /// split encodes an assumed workload mix.
    pub stream_share: f64,
}

/// EN's own configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ENConfig {
    pub knobs: Knobs,
    /// Which media-class row this node's disks follow. Must name a row in the
    /// cluster table.
    pub media_class: String,
}

/// FE's own configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FEConfig {
    pub knobs: Knobs,
    /// FE holds no persistent state, and this says so out loud: a front end that
    /// starts keeping things is a design change, not a configuration one.
    pub stateless: bool,
    /// §9.1.3.7's entry limit again, because a batch spanning DNs has to be *divided*
    /// before any DN sees it, and dividing needs the number here (§9.1.3.12).
    ///
    /// ⚠️ **The DN's copy is still the authority** — it takes the smaller of this and its
    /// own, so setting this higher makes FE's division optimistic and nothing worse: the
    /// answer comes back shorter than planned, says so, and the assembly stops at the
    /// first name it does not have (`A-BATCH-ANSWER-IS-A-PREFIX`).
    pub info_entries: u64,
}

macro_rules! loaders {
    ($($config:ident),*) => {
        $(impl $config {
            pub fn load(path: &Path) -> Result<$config> {
                read_toml(path)
            }
        })*
    };
}

loaders!(DNConfig, SNConfig, ENConfig, FEConfig);

/// Written into each run directory by `start-laws`. Its whole job is to say who
/// this process is and where the shared files are; identity comes from here
/// rather than argv, so a process's identity is one file a harness or a
/// deployment system writes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    pub identity: String,
    pub kind: ServiceKind,
    /// Assigned by whatever started this process, and announced to KS.
    pub listen: SocketAddr,
    pub cluster_config: PathBuf,
    pub service_config: PathBuf,
    /// Persistent state. KS, DN, SN and EN have state; FE and the client do not.
    pub data_dir: PathBuf,
}

impl ProcessConfig {
    pub fn load(path: &Path) -> Result<ProcessConfig> {
        let mut config: ProcessConfig = read_toml(path)?;
        let base = path.parent().unwrap_or(Path::new("."));
        config.cluster_config = resolve(base, &config.cluster_config);
        config.service_config = resolve(base, &config.service_config);
        config.data_dir = resolve(base, &config.data_dir);
        Ok(config)
    }
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() { path.to_path_buf() } else { base.join(path) }
}

fn read_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let text = std::fs::read_to_string(path).context(path.display())?;
    toml::from_str(&text).context(path.display())
}

#[cfg(test)]
mod expansion_tests {
    use super::*;

    /// ⚠️ **A parameter can be a double**, and this is the proof — TOML has floats and serde
    /// wants nothing extra. It is also the first one, so it is worth a test that fails
    /// loudly if the config layer ever grows a rule against them (§18.2).
    #[test]
    fn a_parameter_can_be_a_fraction() {
        let extents: Extents = toml::from_str(
            "instances = 4\nquorum = 3\nseal_bytes = 4096\nper_stream = 8\nexpansion = 1.3333",
        )
        .expect("a double parses");
        assert_eq!(extents.expansion, Some(1.3333));
        // ⚠️ 3,999,900 rather than 4,000,000, and the shortfall is the *written* figure's
        // four decimal places, not the double's 53 bits. Anyone wanting 3 data plus 1 parity
        // exactly should say so and let the code divide, which is what `coded` is for.
        assert_eq!(extents.expansion().bytes(3_000_000), 3_999_900);
        // Writing it as a division gets much closer, though ⚠️ **not exact either**: 4/3 has
        // no binary representation, so `4.0 / 3.0` is a shade under it and only the round-up
        // lands this on a whole number. A factor a shade *over* would charge one byte more.
        // That is the price of a double, taken deliberately (§18.2).
        assert_eq!(Expansion::coded(3, 1).bytes(3_000_000), 4_000_000);
        assert!(Expansion::coded(3, 1).factor() < 4.0 / 3.0 + f64::EPSILON);
    }

    /// Absent means plain replication, so the copy count.
    #[test]
    fn no_expansion_means_the_copies() {
        let extents: Extents =
            toml::from_str("instances = 4\nquorum = 3\nseal_bytes = 4096\nper_stream = 8")
                .expect("it parses without one");
        assert_eq!(extents.expansion().bytes(1_000), 4_000);
    }

    /// ⚠️ Storing less than was written is not a storage class.
    #[test]
    fn an_expansion_below_one_is_refused() {
        let extents = Extents {
            instances: 4,
            quorum: 3,
            seal_bytes: 4096,
            per_stream: 8,
            max_append_bytes: 32 * 1024 * 1024,
            expansion: Some(0.5),
        };
        assert!(extents.validate().is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_cap_defaults_to_thirty_days_and_must_be_finite_nonnegative() {
        let file: ClusterFile = toml::from_str(HEADER).unwrap();
        assert_eq!(file.max_soft_delete_retention_secs, TimeIntSec64::new(2592000));
        for value in [0, 10, 2592000] {
            let file: ClusterFile =
                toml::from_str(&format!("max_soft_delete_retention_secs = {value}\n{HEADER}"))
                    .unwrap();
            assert_eq!(file.max_soft_delete_retention_secs, TimeIntSec64::new(value));
        }
        for value in [-1, i64::MIN, i64::MAX - 1, i64::MAX] {
            assert!(
                toml::from_str::<ClusterFile>(&format!(
                    "max_soft_delete_retention_secs = {value}\n{HEADER}"
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn configured_intervals_are_finite_and_nonnegative() {
        let timeouts: Timeouts =
            toml::from_str("request_ms = 1500\nshutdown_grace_ms = 0").unwrap();
        assert_eq!(timeouts.request_ms, TimeIntMilli64::new(1500));
        assert_eq!(timeouts.shutdown_grace_ms, TimeIntMilli64::ZERO);
        #[derive(Deserialize)]
        struct Seconds {
            #[serde(deserialize_with = "nonnegative_seconds")]
            value: TimeIntSec64,
        }
        assert_eq!(toml::from_str::<Seconds>("value = 60").unwrap().value, TimeIntSec64::new(60));
        for value in [-1, i64::MIN, i64::MAX - 1, i64::MAX] {
            assert!(
                toml::from_str::<Timeouts>(&format!("request_ms = {value}\nshutdown_grace_ms = 0"))
                    .is_err()
            );
            assert!(
                toml::from_str::<Timeouts>(&format!("request_ms = 0\nshutdown_grace_ms = {value}"))
                    .is_err()
            );
            assert!(toml::from_str::<Seconds>(&format!("value = {value}")).is_err());
        }
    }

    const HEADER: &str = r#"
        schema_version = 1
        cluster_name = "test"
        ks = { address = "127.0.0.1:9601", port_base = 9602 }
        failure_domains = { levels = ["rack", "machine"] }
        extents = { instances = 4, quorum = 3, seal_bytes = 4096, per_stream = 8 }
        rings = { members = 1 }
        messaging = { send = "direct", encrypt = false, compress = "none", compress_min = 256, compress_sample = 4096, peer_conns = 2, key = "0f1e2d3c4b5a69788796a5b4c3d2e1f0" }
        timeouts = { request_ms = 1000, shutdown_grace_ms = 1000 }
        logging = { level = "info", compress = false, disk = "hdd", compress_ratio = 6, size = 1024, full_files = 2, info_files = 2, info_share = 0.1, recount = 16 }
    "#;

    fn parse(roster: &str) -> Result<ClusterConfig> {
        ClusterConfig::parse(&format!("{HEADER}{roster}"))
    }

    #[test]
    fn a_count_becomes_numbered_processes() {
        let cluster = parse(
            r#"
            [[service]]
            kind = "ks"
            domain = ["rack0", "box0"]

            [[service]]
            kind = "en"
            count = 3
            domain = ["rack0", "box0"]
            media_class = "hdd"

            [[media_class]]
            name = "hdd"
            index_interval = 1048576
            min_read_bytes = 1048576
            min_write_bytes = 2097152
            compress_block = 262144
            staging_bypass = 4194304
            compress_read_win = true
            repair_bytes_sec = 20000000
            "#,
        )
        .unwrap();
        let names: Vec<&str> = cluster.services.iter().map(|s| s.identity.as_str()).collect();
        assert_eq!(names, ["KS0", "EN0", "EN1", "EN2"]);
        assert!(cluster.services_of_kind(ServiceKind::En).all(|en| en.media_class.is_some()));
    }

    /// Numbering runs on across lines, so a second group of a kind does not
    /// collide with the first.
    #[test]
    fn numbering_runs_on() {
        let cluster = parse(
            r#"
            [[service]]
            kind = "ks"
            domain = ["rack0", "box0"]

            [[service]]
            kind = "sn"
            count = 2
            domain = ["rack0", "box0"]

            [[service]]
            kind = "sn"
            count = 2
            domain = ["rack1", "box1"]
            "#,
        )
        .unwrap();
        let sns: Vec<&str> =
            cluster.services_of_kind(ServiceKind::Sn).map(|s| s.identity.as_str()).collect();
        assert_eq!(sns, ["SN0", "SN1", "SN2", "SN3"]);
    }

    #[test]
    fn a_named_process_may_not_also_be_counted() {
        assert!(
            parse(
                r#"
                [[service]]
                identity = "EN7"
                kind = "en"
                count = 3
                domain = ["rack0", "box0"]
                "#,
            )
            .is_err()
        );
    }

    /// Two quorums must overlap, so a quorum has to be more than half. Two of four
    /// is the case the design warns about: two disjoint pairs could each hold an
    /// append the other never saw.
    #[test]
    fn a_quorum_that_does_not_overlap_itself_is_refused() {
        let extents = |instances, quorum| {
            Extents {
                instances,
                quorum,
                seal_bytes: 4096,
                per_stream: 8,
                max_append_bytes: 32 * 1024 * 1024,
                expansion: None,
            }
            .validate()
        };
        assert!(extents(4, 3).is_ok(), "the shipped shape");
        assert!(extents(3, 3).is_ok(), "every instance, which is its own quorum");
        assert!(extents(5, 3).is_ok());
        assert!(extents(5, 4).is_ok());
        assert!(extents(4, 2).is_err(), "two disjoint pairs");
        assert!(extents(6, 3).is_err(), "two disjoint triples");
        assert!(extents(4, 5).is_err(), "more than there are");
        assert!(extents(4, 0).is_err());
        assert!(extents(0, 0).is_err());
    }

    /// A cap of zero would seal every extent while it was still empty, and one past
    /// what a position can name could never be reached.
    #[test]
    fn a_seal_limit_has_to_be_reachable() {
        let capped = |seal_bytes| {
            Extents {
                instances: 4,
                quorum: 3,
                seal_bytes,
                per_stream: 8,
                max_append_bytes: 32 * 1024 * 1024,
                expansion: None,
            }
            .validate()
        };
        assert!(capped(64).is_ok());
        assert!(capped(32 << 20).is_ok());
        assert!(capped(ECoff::MAX.get()).is_ok());
        assert!(capped(0).is_err());
        assert!(capped(ECoff::NONE.get()).is_err());
        // And a stream that can hold no extents can hold nothing at all.
        assert!(
            Extents {
                instances: 4,
                quorum: 3,
                seal_bytes: 4096,
                per_stream: 0,
                max_append_bytes: 32 * 1024 * 1024,
                expansion: None
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn a_max_append_bytes_limit_has_to_be_valid() {
        let check = |max_append_bytes| {
            Extents {
                instances: 4,
                quorum: 3,
                seal_bytes: 4096,
                per_stream: 8,
                max_append_bytes,
                expansion: None,
            }
            .validate()
        };
        assert!(check(32 * 1024 * 1024).is_ok());
        assert!(check(1).is_ok());
        assert!(check(ELoff::MAX.get()).is_ok());
        assert!(check(0).is_err());
        assert!(check(ELoff::NONE.get()).is_err());
    }

    /// What a failure-mode seal discards, which is what makes the same rule work at
    /// any size: the minimum over responses, less the smallest few.
    #[test]
    fn what_a_seal_discards_follows_from_the_two_numbers() {
        let shape = |instances, quorum| {
            let extents = Extents {
                instances,
                quorum,
                seal_bytes: 4096,
                per_stream: 8,
                max_append_bytes: 32 * 1024 * 1024,
                expansion: None,
            };
            (extents.discard(), extents.least_heard())
        };
        // Four of three discards one, so "the minimum, discarding the smallest".
        assert_eq!(shape(4, 3), (1, 2));
        // Three of three discards none, so plainly the minimum.
        assert_eq!(shape(3, 3), (0, 1));
        assert_eq!(shape(5, 3), (2, 3));
        assert_eq!(shape(5, 4), (1, 2));
    }

    #[test]
    fn two_processes_may_not_share_an_identity() {
        assert!(
            parse(
                r#"
                [[service]]
                identity = "EN0"
                kind = "en"
                domain = ["rack0", "box0"]

                [[service]]
                identity = "EN0"
                kind = "en"
                domain = ["rack0", "box0"]
                "#,
            )
            .is_err()
        );
    }
}
