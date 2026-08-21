//! Which spreading rules this cluster can actually enforce.
//!
//! A rule that cannot find a domain type with enough domains is skipped, not
//! failed: it degrades down the nested hierarchy, and where no level qualifies
//! it is simply not enforced. That is what makes one machine a legal cluster,
//! and it is also why a one-box harness must say out loud that it enforces
//! nothing — otherwise the first thing the test infrastructure teaches the
//! system is how to violate its own invariant.

use crate::config::{ClusterConfig, ServiceKind};
use std::collections::BTreeSet;

pub const EXTENT_INSTANCES: usize = 4;

/// Which EN serves a disk.
///
/// ⚠️ **An extent's instance list names *disks*, not ENs** (Asserted, Bob, 2026-08-20). An
/// EN serves several disks, and a disk can be moved to another EN — by a rebuild, by a
/// chassis swap, by capacity balancing — without any extent on it changing. So the durable
/// fact is *which disk holds this instance*, and *which EN to ask* is a lookup against a
/// mapping that changes underneath it.
///
/// **This is that lookup, and it is the identity function**, because the code still runs one
/// disk per EN and numbers them the same. Everything that turns an instance into a process
/// to talk to goes through here, so the real mapping replaces one function rather than being
/// hunted for (§17.29.3, #107).
pub fn en_of(disk: u32) -> u32 {
    disk
}

/// How many bytes of device a stored byte actually costs (Asserted, Bob, 2026-08-20).
///
/// ⚠️ **Not an integer multiplier.** Plain replication is a whole number — four copies is
/// 4.0 — but erasure coding is not: 3 data plus 1 parity is 4/3, 3 plus 4 is 7/3, 5 plus 1
/// is 6/5. A design that stored the factor as a count of copies would have to be unpicked
/// the day the first EC class arrives.
///
/// ⚠️ **And a `f64` rather than a rational**, on Bob's call: *"we don't have any
/// fibonacci-like things yet but it's easy to imagine them showing up."* A ratio of two
/// integers is exact for every scheme that exists today and assumes the next one is also a
/// ratio, which is an assumption with nothing behind it. A double costs nothing here —
/// 53 bits of mantissa is exact on byte counts to 9 PB and the relative error past that is
/// around 10^-16, against a bill nobody computes to the byte — and it does not have to be
/// revisited.
///
/// This is a property of the **storage class**, and it is what billing reads. ⚠️ It is
/// never derived from how many instances happen to exist right now
/// (`BILLING-CHARGES-THE-CLASS-NOT-THE-COPIES`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Expansion(f64);

impl Expansion {
    /// Plain replication: every copy is the whole thing.
    pub fn copies(instances: u32) -> Expansion {
        Expansion(instances.max(1) as f64)
    }

    /// Erasure coding of `data` fragments plus `parity`, so 3 and 1 is 4/3.
    pub fn coded(data: u32, parity: u32) -> Expansion {
        let data = data.max(1) as f64;
        Expansion((data + parity as f64) / data)
    }

    /// Whatever the class says it costs. Below 1.0 is not a storage class but a mistake, and
    /// a bill that rounds down is worse than a loud one.
    pub fn of(factor: f64) -> Expansion {
        Expansion(if factor.is_finite() && factor >= 1.0 { factor } else { 1.0 })
    }

    pub fn factor(&self) -> f64 {
        self.0
    }

    /// Bytes of device that many stored bytes cost.
    ///
    /// **Rounded up**, because a fragment that is not a whole one still occupies a whole one
    /// — and because a bill that rounds down is a bill somebody is not paying.
    pub fn bytes(&self, stored: u64) -> u64 {
        let want = (stored as f64 * self.0).ceil();
        if want >= u64::MAX as f64 { u64::MAX } else { want as u64 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spread {
    /// Distinct failure domains: reliability and availability.
    Domains,
    /// Distinct processes: capacity arithmetic, which is a different problem.
    Processes,
}

#[derive(Debug, Clone)]
pub struct RuleReport {
    pub rule: String,
    pub spread: Spread,
    pub needs: usize,
    /// The coarsest level with enough domains; `None` means unenforced.
    pub level: Option<String>,
    /// Domains available at the level chosen, or at the finest level if none
    /// qualifies.
    pub available: usize,
}

impl RuleReport {
    pub fn enforced(&self) -> bool {
        self.level.is_some()
    }

    /// A rule asking for one of something constrains nothing, so reporting it as
    /// enforced would overstate what the cluster does.
    pub fn vacuous(&self) -> bool {
        self.needs <= 1
    }
}

pub fn report(cluster: &ClusterConfig) -> Vec<RuleReport> {
    let mut reports = vec![domain_rule(
        cluster,
        "instances of one extent on ENs in distinct failure domains",
        ServiceKind::En,
        EXTENT_INSTANCES,
    )];

    for kind in [ServiceKind::Ks, ServiceKind::Dn, ServiceKind::Sn] {
        reports.push(domain_rule(
            cluster,
            &format!(
                "members of one {} Raft ring in distinct failure domains",
                kind.as_str().to_uppercase()
            ),
            kind,
            cluster.rings.members,
        ));
    }

    let sn_count = cluster.services_of_kind(ServiceKind::Sn).count();
    reports.push(RuleReport {
        rule: "streams of one object on distinct SNs".into(),
        spread: Spread::Processes,
        needs: 2,
        level: (sn_count >= 2).then(|| "process".to_string()),
        available: sn_count,
    });

    reports
}

fn domain_rule(cluster: &ClusterConfig, rule: &str, kind: ServiceKind, needs: usize) -> RuleReport {
    let members: Vec<&Vec<String>> =
        cluster.services_of_kind(kind).map(|service| &service.domain).collect();

    let mut finest_available = 0;
    for (depth, level) in cluster.failure_domains.levels.iter().enumerate() {
        // A domain is identified by its path from the root: nesting means
        // "rack0" under two datacenters is two racks.
        let available =
            members.iter().map(|domain| &domain[..=depth]).collect::<BTreeSet<_>>().len();
        finest_available = available;
        if available >= needs {
            return RuleReport {
                rule: rule.into(),
                spread: Spread::Domains,
                needs,
                level: Some(level.clone()),
                available,
            };
        }
    }

    RuleReport {
        rule: rule.into(),
        spread: Spread::Domains,
        needs,
        level: None,
        available: finest_available,
    }
}

pub fn format(cluster: &ClusterConfig, reports: &[RuleReport]) -> String {
    let mut out = format!(
        "cluster {:?}, failure-domain levels {:?}\n",
        cluster.cluster_name, cluster.failure_domains.levels
    );
    let mut unenforced = 0;
    for report in reports {
        if report.vacuous() {
            out.push_str(&format!(
                "  vacuous                    needs {:<2} has {:<3}  {}\n",
                report.needs, report.available, report.rule
            ));
            continue;
        }
        match &report.level {
            Some(level) => out.push_str(&format!(
                "  enforced    at {level:<10} needs {:<2} has {:<3}  {}\n",
                report.needs, report.available, report.rule
            )),
            None => {
                unenforced += 1;
                out.push_str(&format!(
                    "  UNENFORCED                 needs {:<2} has {:<3}  {}\n",
                    report.needs, report.available, report.rule
                ));
            }
        }
    }
    if unenforced > 0 {
        out.push_str(&format!(
            "\n  !! {unenforced} of {} spreading rules are not enforced here. Legal — a rule\n\
             \x20    with too few domains is skipped rather than failed — but nothing placed\n\
             \x20    in this cluster is protected by the rules it appears to satisfy.\n",
            reports.len()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = r#"
        schema_version = 1
        cluster_name = "test"
        ks = { address = "127.0.0.1:9601", port_base = 9602 }
        failure_domains = { levels = ["datacenter", "rack", "machine"] }
        rings = { members = 5 }
        extents = { instances = 4, quorum = 3, seal_bytes = 4096, per_stream = 8 }
        messaging = { send = "direct", encrypt = false, compress = "none", compress_min = 256, compress_sample = 4096, peer_conns = 2, key = "0f1e2d3c4b5a69788796a5b4c3d2e1f0" }
        timeouts = { request_ms = 1000, shutdown_grace_ms = 1000 }
        logging = { level = "info", compress = false, disk = "hdd", compress_ratio = 6, size = 1048576, full_files = 4, info_files = 8, info_share = 0.1, recount = 4096 }
    "#;

    /// Four ENs, with the domain components each `en` closure supplies.
    fn four_ens(domain: impl Fn(usize) -> String) -> ClusterConfig {
        let mut toml = HEADER.to_string();
        for en in 0..4 {
            toml.push_str(&format!(
                "[[service]]\nidentity = \"EN{en}\"\nkind = \"en\"\ndomain = {}\n",
                domain(en)
            ));
        }
        ClusterConfig::parse(&toml).expect("test config parses")
    }

    #[test]
    fn one_machine_bare() {
        let cluster = four_ens(|_| "[\"dc0\", \"rack0\", \"box0\"]".into());
        let extents = &report(&cluster)[0];
        assert!(!extents.enforced());
        assert_eq!(extents.available, 1);
    }

    #[test]
    fn racks_enforce_rule() {
        let cluster = four_ens(|i| format!("[\"dc0\", \"rack{i}\", \"box{i}\"]"));
        let extents = &report(&cluster)[0];
        assert_eq!(extents.level.as_deref(), Some("rack"));
        assert_eq!(extents.available, 4);
    }

    #[test]
    fn coarsest_wins() {
        let cluster = four_ens(|i| format!("[\"dc{i}\", \"rack{i}\", \"box{i}\"]"));
        assert_eq!(report(&cluster)[0].level.as_deref(), Some("datacenter"));
    }

    #[test]
    fn same_name_two_parents() {
        let cluster = four_ens(|i| format!("[\"dc{}\", \"rack0\", \"box0\"]", i % 2));
        let extents = &report(&cluster)[0];
        assert!(!extents.enforced());
        assert_eq!(extents.available, 2);
    }
}
