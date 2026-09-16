//! Primary read/write leases use elapsed time, including time spent suspended. Never persisted.
use crate::raft::MemberId;
use crate::raft::Membership;
use common::{Error, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

pub fn now() -> Result<Duration> {
    let mut value = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // CLOCK_BOOTTIME is monotonic and includes system suspend, unlike Instant on Linux.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut value) };
    if result != 0 || value.tv_sec < 0 || !(0..1_000_000_000).contains(&value.tv_nsec) {
        return Err(Error::new("cannot read Raft lease clock"));
    }
    Ok(Duration::new(value.tv_sec as u64, value.tv_nsec as u32))
}

pub struct Lease {
    duration: Duration,
    elected: Option<(u64, Duration)>,
    acks: BTreeMap<MemberId, Duration>,
}
impl Default for Lease {
    fn default() -> Self {
        Self::configured(&common::config::RaftConfig::default())
    }
}
impl Lease {
    pub fn configured(config: &common::config::RaftConfig) -> Self {
        Self {
            duration: Duration::from_millis(config.election_min_ms),
            elected: None,
            acks: BTreeMap::new(),
        }
    }
    pub fn reset(&mut self) {
        self.elected = None;
        self.acks.clear();
    }
    pub fn start(&mut self, term: u64, at: Duration) {
        self.reset();
        self.elected = Some((term, at));
    }
    #[cfg(test)]
    pub fn elected(term: u64, at: Duration) -> Self {
        let mut lease = Self::default();
        lease.start(term, at);
        lease
    }
    pub fn acknowledge(&mut self, peer: &MemberId, term: u64, sent: Duration) {
        if self.elected.is_some_and(|(t, at)| t == term && sent >= at) {
            self.acks.entry(*peer).and_modify(|old| *old = (*old).max(sent)).or_insert(sent);
        }
    }
    pub fn active(&self, term: u64, at: Duration) -> bool {
        self.elected.is_some_and(|(t, elected)| t == term && at >= elected)
    }
    /// Election establishes initial authority; after that interval only recent
    /// successful AppendEntries acknowledgments can authorize primary writes.
    pub fn can_write(
        &self,
        term: u64,
        id: &MemberId,
        membership: &Membership,
        at: Duration,
    ) -> bool {
        let initial = self.elected.is_some_and(|(t, elected)| {
            t == term && at.checked_sub(elected).is_some_and(|elapsed| elapsed < self.duration)
        });
        initial || self.valid(term, id, membership, at)
    }

    pub fn valid(&self, term: u64, id: &MemberId, membership: &Membership, at: Duration) -> bool {
        if !self.active(term, at) {
            return false;
        }
        let mut votes = BTreeSet::from([id.to_owned()]);
        votes.extend(
            self.acks
                .iter()
                .filter(|(_, sent)| {
                    at.checked_sub(**sent).is_some_and(|elapsed| elapsed < self.duration)
                })
                .map(|(id, _)| id.clone()),
        );
        membership.quorum(&votes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::tests::members;
    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }
    #[test]
    fn no_post_election_wait_and_send_time_expiry_boundaries() {
        let membership =
            Membership { identities: crate::raft::tests::names(), old: members(3), new: None };
        let mut lease = Lease::elected(2, ms(1000));
        lease.acknowledge(&1, 2, ms(1100));
        let early = lease.valid(2, &0, &membership, ms(1149));
        let ready = lease.valid(2, &0, &membership, ms(1150));
        let before_expiry = lease.valid(2, &0, &membership, ms(1249));
        let expired = lease.valid(2, &0, &membership, ms(1250));
        assert!(early);
        assert!(ready);
        assert!(before_expiry);
        assert!(!expired);
    }
    #[test]
    fn delayed_responses_old_terms_and_restart_do_not_extend_lease() {
        let membership =
            Membership { identities: crate::raft::tests::names(), old: members(3), new: None };
        let mut lease = Lease::elected(4, ms(1000));
        lease.acknowledge(&1, 3, ms(1200));
        lease.acknowledge(&2, 4, ms(999));
        let old_term = lease.valid(4, &0, &membership, ms(1200));
        lease.acknowledge(&1, 4, ms(1001));
        let delayed = lease.valid(4, &0, &membership, ms(1200));
        lease.acknowledge(&1, 4, ms(1190));
        lease.acknowledge(&1, 4, ms(1100));
        let reordered = lease.valid(4, &0, &membership, ms(1300));
        let suspended = lease.valid(4, &0, &membership, ms(10000));
        let restarted = Lease::default().valid(4, &0, &membership, ms(1300));
        assert!(!old_term);
        assert!(!delayed);
        assert!(reordered);
        assert!(!suspended);
        assert!(!restarted);
    }
    #[test]
    fn joint_configuration_needs_both_majorities() {
        let old = members(3);
        let new = members(5).into_iter().filter(|id| id != &1 && id != &2).collect();
        let membership =
            Membership { identities: crate::raft::tests::names(), old, new: Some(new) };
        let mut lease = Lease::elected(1, ms(0));
        lease.acknowledge(&1, 1, ms(100));
        let old_only = lease.valid(1, &0, &membership, ms(160));
        lease.acknowledge(&3, 1, ms(120));
        let both = lease.valid(1, &0, &membership, ms(160));
        let old_expired = lease.valid(1, &0, &membership, ms(250));
        assert!(!old_only);
        assert!(both);
        assert!(!old_expired);
    }
    #[test]
    fn timing_parameters_validate_relationships_and_control_the_lease() {
        use common::config::RaftConfig;
        let mut config = RaftConfig::default();
        let defaults = config.validate_timing();
        config.heartbeat_ms = 76;
        let heartbeat = config.validate_timing();
        config.heartbeat_ms = 75;
        config.election_max_ms = 150;
        let election = config.validate_timing();
        config.election_max_ms = 300;
        config.heartbeat_ms = u64::MAX;
        let overflow = config.validate_timing();
        assert!(defaults.is_ok());
        assert!(heartbeat.is_err());
        assert!(election.is_err());
        assert!(overflow.is_err());

        config = RaftConfig { election_min_ms: 220, ..RaftConfig::default() };
        let valid_config = config.validate_timing();
        let membership =
            Membership { identities: crate::raft::tests::names(), old: members(3), new: None };
        let mut lease = Lease::configured(&config);
        lease.start(1, ms(0));
        lease.acknowledge(&1, 1, ms(100));
        let early = lease.valid(1, &0, &membership, ms(219));
        let live = lease.valid(1, &0, &membership, ms(319));
        let expired = lease.valid(1, &0, &membership, ms(320));
        lease.reset();
        lease.start(2, ms(1000));
        let old_acks_cleared = lease.valid(2, &0, &membership, ms(1000));
        assert!(valid_config.is_ok());
        assert!(early);
        assert!(live);
        assert!(!expired);
        assert!(!old_acks_cleared);
    }
}
