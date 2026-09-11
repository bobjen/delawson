//! Configurations built in code, for tests.
//!
//! A unit test may not read a configuration from disk — not because a file is
//! slow, but because it makes the test depend on a deployment instead of on the
//! code it is testing. So there is one routine per configuration here, giving the
//! defaults a test would otherwise write out again, and a test that cares about a
//! knob sets that field afterwards.
//!
//! Everything here is behind the `testing` feature, so it is compiled for tests
//! and for nothing else.
//!
//! **A parameter protecting a new feature is on here and off in the checked-in
//! cluster file.** The two are deliberately opposite: the fleet turns a feature on
//! once it is deployed everywhere, and the tests exercise what the code is going to
//! do. A test wanting the old path turns it off itself.

use crate::config::{
    ClusterConfig, Compress, DNConfig, ENConfig, Extents, FEConfig, FailureDomains, Knobs,
    KsBootstrap, Logging, MediaClass, Messaging, Rings, SCHEMA_VERSION, SNConfig, SendMode,
    ServiceEntry, ServiceKind, Timeouts,
};
use crate::server::Process;
use std::sync::Arc;

/// One of everything, four ENs because that is an extent's instance count, all in
/// one failure domain. The single-box shape, which is what a test wants too.
pub fn cluster() -> ClusterConfig {
    let mut services = vec![
        entry("KS0", ServiceKind::Ks, None),
        entry("DN0", ServiceKind::Dn, None),
        entry("SN0", ServiceKind::Sn, None),
    ];
    for n in 0..extents().instances {
        services.push(entry(&format!("EN{n}"), ServiceKind::En, Some("hdd")));
    }
    services.push(entry("FE0", ServiceKind::Fe, None));

    ClusterConfig {
        schema_version: SCHEMA_VERSION,
        cluster_name: "test".to_string(),
        max_soft_delete_retention_secs: crate::config::default_max_soft_delete_retention_secs(),
        ks: KsBootstrap { address: "127.0.0.1:9601".parse().expect("address"), port_base: 9602 },
        failure_domains: FailureDomains {
            levels: ["region", "datacenter", "rack", "machine", "disk"]
                .map(str::to_string)
                .to_vec(),
        },
        extents: extents(),
        rings: Rings { members: 1 },
        messaging: messaging(),
        logging: logging(),
        timeouts: Timeouts {
            request_ms: crate::time::TimeIntMilli64::new(1000),
            shutdown_grace_ms: crate::time::TimeIntMilli64::new(1000),
        },
        services,
        media_classes: vec![hdd(), ssd()],
    }
}

/// Four instances and a quorum of three, which is the shipped shape.
pub fn extents() -> Extents {
    // Small enough that a test can seal an extent with a few short records.
    Extents {
        instances: 4,
        quorum: 3,
        seal_bytes: 64,
        per_stream: 3,
        max_append_bytes: 32 * 1024 * 1024,
        expansion: None,
    }
}

/// A roster entry in the one domain a single box has.
pub fn entry(identity: &str, kind: ServiceKind, media_class: Option<&str>) -> ServiceEntry {
    ServiceEntry {
        identity: identity.to_string(),
        kind,
        domain: ["region0", "dc0", "rack0", "box0", "disk0"].map(str::to_string).to_vec(),
        media_class: media_class.map(str::to_string),
    }
}

/// Plain: nothing sealed and nothing compressed, so a test that is not about
/// either does not measure either.
pub fn messaging() -> Messaging {
    Messaging {
        send: SendMode::Direct,
        encrypt: false,
        compress: Compress::None,
        compress_min: 256,
        compress_sample: 4096,
        peer_conns: 2,
        key: "0f1e2d3c4b5a69788796a5b4c3d2e1f0".to_string(),
    }
}

/// Small files, so a test that wants to see rotation does not have to write
/// megabytes to get there.
pub fn logging() -> Logging {
    Logging {
        level: "info".to_string(),
        compress: false,
        disk: "hdd".to_string(),
        compress_ratio: 7,
        size: 4096,
        full_files: 2,
        info_files: 2,
        info_share: 0.1,
        recount: 64,
    }
}

pub fn hdd() -> MediaClass {
    MediaClass {
        name: "hdd".to_string(),
        index_interval: 1 << 20,
        min_read_bytes: 1 << 20,
        min_write_bytes: 2 << 20,
        compress_block: 256 << 10,
        staging_bypass: 4 << 20,
        compress_read_win: true,
        repair_bytes_sec: 20_000_000,
    }
}

pub fn ssd() -> MediaClass {
    MediaClass {
        name: "ssd".to_string(),
        index_interval: 20 << 10,
        min_read_bytes: 20 << 10,
        min_write_bytes: 40 << 10,
        compress_block: 64 << 10,
        staging_bypass: 1 << 20,
        compress_read_win: false,
        repair_bytes_sec: 400_000_000,
    }
}

/// One worker, because a unit test is single-threaded and a pool of them would
/// only add a scheduler to whatever is being measured.
pub fn knobs() -> Knobs {
    Knobs { workers: 1 }
}

pub fn en_config() -> ENConfig {
    ENConfig { knobs: knobs(), media_class: "hdd".to_string() }
}

pub fn fe_config() -> FEConfig {
    FEConfig { knobs: knobs(), stateless: true, info_entries: 10_000 }
}

pub fn dn_config() -> DNConfig {
    // Small on purpose, so §15.3.2's default exercises paging rather than the one
    // answer that fits everything.
    DNConfig { knobs: knobs(), self_hosted: false, info_entries: 8, max_concat_sources: 1024 }
}

pub fn sn_config() -> SNConfig {
    SNConfig { knobs: knobs(), stream_share: 0.333 }
}

/// A connected pair of wires, for testing the wire protocol without a network.
pub fn link() -> (crate::wire::Wire, crate::wire::Reader) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = std::net::TcpStream::connect(addr).unwrap();
    let server = listener.incoming().next().unwrap().unwrap();
    let messaging = messaging();
    (
        crate::wire::connect(client, &messaging).unwrap().wire,
        crate::wire::accept(server, &messaging).unwrap().0,
    )
}

/// A reply that sends to a dummy wire.
pub fn reply() -> crate::server::Reply {
    crate::server::Reply::new_test(Arc::new(crate::server::Out::direct(link().0)))
}

/// A process, with the test cluster's configuration and the first EN's identity.
pub fn process() -> Arc<Process> {
    Arc::new(Process {
        config: crate::config::ProcessConfig {
            identity: "EN0".to_string(),
            kind: ServiceKind::En,
            listen: "127.0.0.1:0".parse().unwrap(),
            cluster_config: "cluster.toml".into(),
            service_config: "en.toml".into(),
            data_dir: "data".into(),
        },
        cluster: cluster(),
        entry: entry("EN0", ServiceKind::En, Some("hdd")),
        run_dir: ".".into(),
        started: std::time::Instant::now(),
        peers: crate::peer::Peers::new(messaging(), std::time::Duration::from_secs(1)),
        assigned: std::sync::RwLock::new(std::collections::HashMap::new()),
        turn: std::sync::atomic::AtomicU64::new(0),
        stopping: std::sync::atomic::AtomicBool::new(false),
        answered: std::sync::atomic::AtomicU64::new(0),
        senders: std::sync::atomic::AtomicU64::new(0),
    })
}
