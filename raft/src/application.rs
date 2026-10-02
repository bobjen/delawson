//! Volatile application placement; this never changes the voting quorum.
use crate::raft::MemberId;
use crate::raft::Members;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

pub struct Placement {
    pub selected: BTreeSet<MemberId>,
    seen: BTreeMap<MemberId, Instant>,
}
impl Placement {
    pub fn new(members: &Members, count: usize) -> Self {
        Self { selected: members.iter().take(count).cloned().collect(), seen: BTreeMap::new() }
    }
    pub fn acknowledge(&mut self, id: &MemberId, now: Instant) {
        self.seen.insert(*id, now);
    }
    pub fn select(
        &mut self,
        primary: &MemberId,
        members: &Members,
        count: usize,
        now: Instant,
        grace: Duration,
    ) {
        let healthy =
            |id: &MemberId| self.seen.get(id).is_some_and(|seen| now.duration_since(*seen) < grace);
        let mut candidates: Vec<_> = members.iter().filter(|id| *id != primary).cloned().collect();
        // Preserve existing healthy backups, then promote reachable cold members.
        candidates.sort_by_key(|id| (!healthy(id), !self.selected.contains(id), id.clone()));
        self.selected = std::iter::once(primary.to_owned())
            .chain(candidates.into_iter().take(count.saturating_sub(1)))
            .collect();
    }
}

/// This is scheduling, not a read lease. Start its local timer once on observing
/// commitment. Recovery conservatively waits a full lease for the last announcement.
#[derive(Default)]
pub struct CheckpointLease {
    pub announcement: Option<(u64, MemberId)>,
    observed: Option<Instant>,
}
impl CheckpointLease {
    pub fn observe(&mut self, announcement: Option<(u64, MemberId)>, now: Instant) {
        if announcement != self.announcement {
            self.announcement = announcement;
            self.observed = Some(now);
        }
    }
    pub fn holder(&self, now: Instant, duration: Duration) -> Option<&(u64, MemberId)> {
        self.observed.filter(|seen| now.saturating_duration_since(*seen) < duration)?;
        self.announcement.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_lease_starts_on_commit_observation_and_heartbeats_do_not_extend_it() {
        let now = Instant::now();
        let duration = Duration::from_secs(300);
        let mut lease = CheckpointLease::default();
        let mut state = crate::raft::State::initial(
            "01".repeat(64),
            crate::raft::tests::members(3),
            crate::raft::tests::names(),
        )
        .unwrap();
        state.term = 1;
        state.log.push(crate::raft::Entry {
            scn: common::scn::Scn::new(1),
            index: 1,
            term: 1,
            command: crate::raft::Command::CheckpointStart(1),
            entry_buf: None,
        });
        lease.observe(state.checkpoint_start(), now);
        let uncommitted = lease.holder(now, duration).is_none();
        state.commit = 1;
        lease.observe(state.checkpoint_start(), now);
        let holder = lease.holder(now, duration).cloned();
        lease.observe(state.checkpoint_start(), now + Duration::from_secs(299));
        let expired = lease.holder(now + duration, duration).is_none();
        let snapshot = state.capture();
        state.apply(&crate::raft::Record::Install(snapshot)).unwrap();
        let mut restarted = CheckpointLease::default();
        restarted.observe(state.checkpoint_start(), now + duration);
        let after_recovery = restarted.holder(now + duration, duration).cloned();
        assert!(uncommitted);
        assert_eq!(holder, Some((1, 1)));
        assert!(expired);
        assert_eq!(after_recovery, holder);
    }
    #[test]
    fn placement_preserves_backups_replaces_failed_members_and_keeps_primary() {
        let members: Members = (0..7).collect();
        let mut p = Placement::new(&members, 3);
        let now = Instant::now();
        for id in members.iter() {
            p.acknowledge(id, now);
        }
        p.select(&6, &members, 3, now, Duration::from_secs(2));
        let first = p.selected.clone();
        p.acknowledge(&2, now + Duration::from_secs(3));
        p.select(&6, &members, 3, now + Duration::from_secs(3), Duration::from_secs(2));
        let replacement = p.selected.contains(&2);
        let count = p.selected.len();
        assert_eq!(first, BTreeSet::from([0, 1, 6]));
        assert!(replacement);
        assert_eq!(count, 3);
    }
}

pub fn cancel_checkpoint_after_election(role: crate::Role, members: usize) -> bool {
    role == crate::Role::Leader && members > 1
}
