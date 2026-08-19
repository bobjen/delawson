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
