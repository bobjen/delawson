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

use crate::{Context, Error, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEntry {
    /// `KS0`, `FE0`: kind plus instance number.
    pub identity: String,
    pub kind: ServiceKind,
    /// Position in the nested failure-domain hierarchy, outermost first, one
    /// entry per level of `FailureDomains::levels`.
    pub domain: Vec<String>,
    /// Which media-class row this process's storage follows. ENs have one;
    /// nothing else does.
    #[serde(default)]
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
    /// Never read less than this from the device.
    pub min_read_bytes: u64,
    pub compress_block: u64,
    /// Appends at least this large skip EN-internal staging.
    pub staging_bypass: u64,
    /// True on HDD, where compression buys read bandwidth; false on SSD, where
    /// it costs.
    pub compress_read_win: bool,
    pub repair_bytes_sec: u64,
}

/// How a connection decides when to send.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Messaging {
    /// Give each connection a sender that loops: take everything queued, write it
    /// as one batch, and let the next batch be whatever piles up meanwhile. False
    /// leaves each answer to be flushed by whoever produced it, which coalesces
    /// only what one thread happens to hold.
    pub batch_sender: bool,
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
    pub request_ms: u64,
    /// How long `stop-laws` waits after asking a process to stop, before
    /// escalating to a signal.
    pub shutdown_grace_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Bumped on an incompatible change, so a process refuses a configuration it
    /// does not understand rather than misreading it.
    pub schema_version: u32,
    pub cluster_name: String,
    pub ks: KsBootstrap,
    pub failure_domains: FailureDomains,
    pub rings: Rings,
    pub messaging: Messaging,
    pub timeouts: Timeouts,
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceEntry>,
    #[serde(default, rename = "media_class")]
    pub media_classes: Vec<MediaClass>,
}

pub const SCHEMA_VERSION: u32 = 1;

impl ClusterConfig {
    pub fn load(path: &Path) -> Result<ClusterConfig> {
        let config: ClusterConfig = read_toml(path)?;
        config.validate().context(path.display())?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(Error::new(format!(
                "schema version {}, this build understands {SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.failure_domains.levels.is_empty() {
            return Err(Error::new("failure_domains.levels is empty"));
        }
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

/// KS's own configuration. Every other machine type gets a file of this shape,
/// holding the knobs that type owns and nothing else.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KsConfig {
    /// Generate a master key only for a day that creates extents. Marching one
    /// key per elapsed day through a long dormancy would take the store from
    /// megabytes to terabytes.
    pub lazy_day_keys: bool,
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
