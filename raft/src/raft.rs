//! Raft consensus. Disk mutations complete before the corresponding response is sent.
use crate::journal::Journal;
use common::scn::Scn;
use common::{Error, MessageBytes, Result};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Stable process identities within this ring. Endpoints are discovered, never voted on.
// Low 32 bits are the logical slot; high 32 bits distinguish replacements.
// Incarnations prevent a reply from a replacement from counting as its predecessor.
pub type MemberId = u64;
pub const INCARNATION: u64 = 1 << 32;
pub type Members = BTreeSet<MemberId>;
/// Reserved in RAM for a process that has not been assigned a voting slot.
pub const UNASSIGNED: MemberId = u64::MAX;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Member {
    pub identity: common::ring::MemberIdentity,
    /// Existing routing/machine-admission label, not the executable name.
    pub process: String,
}
pub type MemberNames = BTreeMap<MemberId, Member>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    /// Includes retired slots: a member index is never rebound to another process.
    pub identities: MemberNames,
    pub old: Members,
    pub new: Option<Members>,
    pub retired: BTreeMap<MemberId, Retirement>,
}
impl Membership {
    pub fn members(&self) -> Members {
        let mut result = self.old.clone();
        if let Some(new) = &self.new {
            result.extend(new.clone());
        }
        result
    }
    fn majority(&self, members: &Members, votes: &Members) -> bool {
        members.iter().filter(|id| !self.retired.contains_key(id) && votes.contains(id)).count()
            > members.len() / 2
    }
    pub fn retirement_quorum(&self, members: &Members) -> bool {
        members.iter().filter(|id| self.retired.contains_key(id)).count() > members.len() / 2
    }
    pub fn replacements(&self, members: &Members) -> Members {
        members
            .iter()
            .map(|id| {
                let mut id = *id;
                while let Some(retired) = self.retired.get(&id) {
                    id = retired.next;
                }
                id
            })
            .collect()
    }
    pub fn voters(&self) -> Members {
        let mut voters = if self.retirement_quorum(&self.old) {
            self.replacements(&self.old)
        } else {
            self.old.clone()
        };
        if let Some(new) = &self.new {
            voters.extend(new);
        }
        voters.retain(|id| !self.retired.contains_key(id));
        voters
    }
    pub fn participants(&self) -> Members {
        let mut members = self.members();
        members.extend(self.replacements(&members.clone()));
        members
    }
    pub fn quorum(&self, votes: &Members) -> bool {
        match &self.new {
            Some(new) => {
                (self.majority(&self.old, votes) || self.retirement_quorum(&self.old))
                    && self.majority(new, votes)
            }
            None if self.retirement_quorum(&self.old) => {
                self.majority(&self.replacements(&self.old), votes)
            }
            None => self.majority(&self.old, votes),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.old.is_empty() || self.new.as_ref().is_some_and(|m| m.is_empty()) {
            return Err(Error::new("empty Raft membership"));
        }
        if self.members().iter().any(|id| !self.identities.contains_key(id)) {
            return Err(Error::new("missing Raft member identity"));
        }
        let unique: BTreeSet<_> = self.identities.values().map(|m| &m.identity).collect();
        let voters = self.voters();
        let processes: BTreeSet<_> =
            voters.iter().filter_map(|id| self.identities.get(id).map(|m| &m.process)).collect();
        if self.identities.contains_key(&UNASSIGNED)
            || unique.len() != self.identities.len()
            || (self.new.is_none() && processes.len() != voters.len())
            || self.identities.values().any(|m| m.process.is_empty())
        {
            return Err(Error::new("duplicate or empty member name"));
        }
        for members in std::iter::once(&self.old).chain(self.new.iter()) {
            let slots: BTreeSet<_> = members.iter().map(|id| id % INCARNATION).collect();
            let routes: BTreeSet<_> = members
                .iter()
                .filter_map(|id| self.identities.get(id).map(|m| &m.process))
                .collect();
            if slots.len() != members.len() || routes.len() != members.len() {
                return Err(Error::new("multiple members occupy one logical slot or route"));
            }
        }
        for (id, retired) in &self.retired {
            retired.validate()?;
            if *id != retired.previous
                || self.identities.get(id) != Some(&retired.old)
                || self.identities.get(&retired.next) != Some(&retired.member)
            {
                return Err(Error::new("retirement identity mismatch"));
            }
        }
        Ok(())
    }
    pub fn slot(&self, identity: &common::ring::MemberIdentity) -> Option<MemberId> {
        self.identities.iter().find_map(|(slot, m)| (&m.identity == identity).then_some(*slot))
    }
    pub fn assign(&mut self, member: Member) -> Result<MemberId> {
        if let Some(slot) = self.slot(&member.identity) {
            if self.identities[&slot] != member {
                return Err(Error::new(
                    "member identity cannot change its registered routing label",
                ));
            }
            return Ok(slot);
        }
        let slot = self
            .identities
            .keys()
            .map(|id| id % INCARNATION)
            .max()
            .map_or(Some(0), |last| last.checked_add(1))
            .filter(|slot| *slot < INCARNATION - 1)
            .ok_or_else(|| Error::new("member slots exhausted"))?;
        self.identities.insert(slot, member);
        Ok(slot)
    }
    fn extends(&self, previous: &Self) -> Result<()> {
        if previous.identities.iter().any(|(id, name)| self.identities.get(id) != Some(name)) {
            return Err(Error::new("member indexes cannot be removed or rebound"));
        }
        Ok(())
    }
}

/// One irreversible, one-for-one replacement. This is not a successful vote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retirement {
    pub previous: MemberId,
    pub old: Member,
    pub next: MemberId,
    pub member: Member,
}
impl Retirement {
    pub fn validate(&self) -> Result<()> {
        if self.previous.checked_add(INCARNATION) != Some(self.next)
            || self.next == UNASSIGNED
            || self.old.process != self.member.process
            || self.old.identity == self.member.identity
        {
            return Err(Error::new("invalid member retirement"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Noop,
    /// Opaque client actions. Only the owning service dispatches these tags.
    Client(Vec<crate::Action>),
    Members(Membership),
    CheckpointStart(MemberId),
}
/// A single entry's encoded bytes, shared by journal I/O and apply work.
#[derive(Clone, Debug)]
pub struct EntryBuf {
    pub(crate) bytes: MessageBytes,
    pub(crate) depth: u8,
    pub(crate) lazy_actions: bool,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub scn: Scn,
    pub index: u64,
    pub term: u64,
    pub command: Command,
    /// Serialized bytes are immutable and shared across journal and apply clones.
    pub entry_buf: Option<Arc<EntryBuf>>,
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        if self.scn != other.scn || self.index != other.index || self.term != other.term {
            return false;
        }
        match (&self.command, &other.command) {
            (Command::Client(left), Command::Client(right)) => {
                if left == right {
                    return true;
                }
                matches!(
                    (self.client_actions(), other.client_actions()),
                    (Ok(left), Ok(right)) if left == right
                )
            }
            _ => self.command == other.command,
        }
    }
}
impl Eq for Entry {}
impl Entry {
    /// Borrow already-decoded actions or decode a retained wire entry on demand.
    pub fn client_actions(&self) -> Result<Cow<'_, [crate::Action]>> {
        let Command::Client(actions) = &self.command else {
            return Err(Error::new("Raft entry is not a client command"));
        };
        if !actions.is_empty() || !self.entry_buf.as_ref().is_some_and(|bytes| bytes.lazy_actions) {
            return Ok(Cow::Borrowed(actions));
        }
        Ok(Cow::Owned(crate::codec::entry_actions(self.entry_buf.as_ref().unwrap())?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub scn: Scn,
    pub ring: String,
    pub index: u64,
    pub term: u64,
    pub membership: Membership,
    /// Committed client actions preserve their original index and term.
    pub application: Vec<Entry>,

    pub checkpoint_start: Option<(u64, MemberId)>,
}
impl Snapshot {
    pub fn largest_scn(&self) -> Option<Scn> {
        self.application
            .iter()
            .map(|entry| entry.scn)
            .chain((self.index != 0).then_some(self.scn))
            .reduce(|a, b| if b.compare(a).is_gt() { b } else { a })
    }
    pub fn validate(&self) -> Result<()> {
        if self.membership.old.is_empty() {
            if self.index != 0
                || self.term != 0
                || !self.application.is_empty()
                || !self.membership.identities.is_empty()
                || self.membership.new.is_some()
            {
                return Err(Error::new("invalid unjoined snapshot"));
            }
        } else {
            self.membership.validate()?;
        }
        if (self.index == 0) != (self.term == 0) {
            return Err(Error::new("invalid snapshot index/term"));
        }
        if self.checkpoint_start.as_ref().is_some_and(|(index, member)| {
            *index == 0 || *index > self.index || !self.membership.identities.contains_key(member)
        }) {
            return Err(Error::new("invalid checkpoint announcement"));
        }
        let mut previous = 0;
        for entry in &self.application {
            if entry.index <= previous
                || entry.index > self.index
                || entry.term == 0
                || entry.term > self.term
                || !matches!(entry.command, Command::Client(_))
                || (entry.index == self.index && (entry.term, entry.scn) != (self.term, self.scn))
            {
                return Err(Error::new("invalid snapshot key entry"));
            }
            previous = entry.index;
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct State {
    /// Local application generation; changes when abandoned history is replaced.
    pub generation: u64,
    /// Largest observed entry SCN, including a subsequently replaced suffix.
    pub max_scn: Option<Scn>,
    /// Process-local debugging override: never inherited from a snapshot or journal.
    pub ignore_ring_identity: bool,
    pub ring: String,
    /// Initial table learned at creation or before joining; independent of later membership.
    pub founders: Option<Membership>,
    /// Learned routing identities only; never changes the electorate.
    pub contacts: MemberNames,
    pub retirements: BTreeMap<MemberId, Retirement>,
    pub term: u64,
    pub vote: Option<MemberId>,
    pub snapshot: Snapshot,
    pub log: Vec<Entry>,
    pub commit: u64,
}
impl State {
    pub fn validate(&self) -> Result<()> {
        self.snapshot.validate()?;
        if let Some(founders) = &self.founders {
            founders.validate()?;
        } else if self.term != 0 || self.vote.is_some() || self.last() != 0 || self.commit != 0 {
            return Err(Error::new("unjoined member has voting history"));
        }
        if self.term < self.snapshot.term
            || self.commit < self.snapshot.index
            || self.commit > self.last()
        {
            return Err(Error::new("invalid persisted Raft positions"));
        }
        let mut previous = self.snapshot.index;
        let mut term = self.snapshot.term;
        let mut membership = &self.snapshot.membership;
        for entry in &self.log {
            if previous.checked_add(1) != Some(entry.index)
                || entry.term < term
                || entry.term > self.term
            {
                return Err(Error::new("invalid persisted Raft log"));
            }
            if let Command::Members(m) = &entry.command {
                m.validate()?;
                m.extends(membership)?;
                membership = m;
            }
            previous = entry.index;
            term = entry.term;
        }
        Ok(())
    }

    pub fn check_ring(&self, snapshot: &Snapshot) -> Result<()> {
        if !self.ignore_ring_identity && snapshot.ring != self.ring {
            return Err(Error::new("checkpoint ring identity mismatch"));
        }
        Ok(())
    }
    pub fn initial(ring: String, members: Members, identities: MemberNames) -> Result<Self> {
        common::ring::RingIdentity::try_from(ring.clone())?;
        let membership =
            Membership { retired: BTreeMap::new(), identities, old: members, new: None };
        if !membership.old.is_empty() {
            membership.validate()?;
        } else if !membership.identities.is_empty() {
            return Err(Error::new("unjoined state has no assigned identities"));
        }
        Ok(Self {
            generation: 0,
            max_scn: None,
            founders: (!membership.old.is_empty()).then_some(membership.clone()),
            retirements: BTreeMap::new(),
            contacts: MemberNames::new(),
            ring: ring.clone(),
            ignore_ring_identity: false,
            term: 0,
            vote: None,
            snapshot: Snapshot {
                scn: Scn::new(0),
                ring,
                index: 0,
                term: 0,
                membership,
                application: vec![],
                checkpoint_start: None,
            },
            log: vec![],
            commit: 0,
        })
    }
    pub fn last(&self) -> u64 {
        self.log.last().map_or(self.snapshot.index, |e| e.index)
    }
    pub fn scn_at(&self, index: u64) -> Option<Scn> {
        if index == self.snapshot.index {
            return Some(self.snapshot.scn);
        }
        index
            .checked_sub(self.snapshot.index)
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| self.log.get(i as usize))
            .map(|e| e.scn)
    }
    pub fn history_at(&self, index: u64) -> Option<(u64, Scn)> {
        self.term_at(index).zip(self.scn_at(index))
    }
    pub fn largest_scn(&self) -> Option<Scn> {
        let mut maximum = self.max_scn;
        for scn in self
            .snapshot
            .application
            .iter()
            .map(|e| e.scn)
            .chain((self.snapshot.index != 0).then_some(self.snapshot.scn))
            .chain(self.log.iter().map(|e| e.scn))
        {
            if maximum.is_none_or(|old| scn.compare(old).is_gt()) {
                maximum = Some(scn);
            }
        }
        maximum
    }
    fn observe_scn(&mut self, scn: Scn) {
        if self.max_scn.is_none_or(|old| scn.compare(old).is_gt()) {
            self.max_scn = Some(scn);
        }
    }
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot.index {
            return Some(self.snapshot.term);
        }
        index
            .checked_sub(self.snapshot.index)
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| self.log.get(i as usize))
            .map(|e| e.term)
    }
    pub fn membership(&self) -> Membership {
        let mut membership = self
            .log
            .iter()
            .rev()
            .find_map(|e| match &e.command {
                Command::Members(m) => Some(m.clone()),
                _ => None,
            })
            .unwrap_or_else(|| self.snapshot.membership.clone());
        for retirement in self.retirements.values() {
            membership.identities.insert(retirement.previous, retirement.old.clone());
            membership.identities.insert(retirement.next, retirement.member.clone());
            membership.retired.insert(retirement.previous, retirement.clone());
        }
        membership
    }
    pub fn has_actions(&self) -> bool {
        !self.snapshot.application.is_empty()
            || self
                .log
                .iter()
                .any(|e| e.index <= self.commit && matches!(e.command, Command::Client(_)))
    }
    pub fn committed_membership(&self) -> Membership {
        self.log
            .iter()
            .rev()
            .filter(|e| e.index <= self.commit)
            .find_map(|e| match &e.command {
                Command::Members(m) => Some(m.clone()),
                _ => None,
            })
            .unwrap_or_else(|| self.snapshot.membership.clone())
    }
    pub fn checkpoint_start(&self) -> Option<(u64, MemberId)> {
        self.log
            .iter()
            .rev()
            .filter(|e| e.index <= self.commit)
            .find_map(|e| match &e.command {
                Command::CheckpointStart(member) => Some((e.index, member.clone())),
                _ => None,
            })
            .or_else(|| self.snapshot.checkpoint_start.clone())
    }
    pub fn capture(&self) -> Snapshot {
        let mut snapshot = self.snapshot.clone();
        for e in self.log.iter().take_while(|e| e.index <= self.commit) {
            match &e.command {
                Command::Client(_) => snapshot.application.push(e.clone()),
                Command::Members(m) => snapshot.membership = m.clone(),
                Command::CheckpointStart(member) => {
                    snapshot.checkpoint_start = Some((e.index, member.clone()))
                }
                Command::Noop => {}
            }
            snapshot.index = e.index;
            snapshot.term = e.term;
            snapshot.scn = e.scn;
        }
        snapshot
    }
    pub fn apply(&mut self, record: &Record) -> Result<()> {
        match record {
            Record::Contacts(contacts) => {
                let membership = self.membership();
                for (slot, member) in contacts {
                    if *slot == UNASSIGNED
                        || member.process.is_empty()
                        || membership.identities.get(slot).is_some_and(|old| old != member)
                        || self.contacts.get(slot).is_some_and(|old| old != member)
                    {
                        return Err(Error::new("conflicting catch-up peer identity"));
                    }
                }
                self.contacts.extend(contacts.clone());
            }
            Record::Retired(retired) => {
                retired.validate()?;
                let membership = self.membership();
                if membership.identities.get(&retired.previous) != Some(&retired.old)
                    || membership.retired.get(&retired.previous).is_some_and(|old| old != retired)
                    || membership
                        .identities
                        .get(&retired.next)
                        .is_some_and(|m| m != &retired.member)
                {
                    return Err(Error::new("unknown or conflicting retirement"));
                }
                self.retirements.insert(retired.previous, retired.clone());
            }
            Record::Bootstrap(membership) => {
                membership.validate()?;
                if membership.new.is_some() {
                    return Err(Error::new("joint founding membership"));
                }
                if let Some(founders) = &self.founders {
                    if founders != membership {
                        return Err(Error::new("founding membership cannot be replaced"));
                    }
                } else {
                    if self.term != 0
                        || self.vote.is_some()
                        || self.last() != 0
                        || !self.snapshot.membership.old.is_empty()
                    {
                        return Err(Error::new("bootstrap cannot replace existing history"));
                    }
                    self.founders = Some(membership.clone());
                    self.snapshot.membership = membership.clone();
                }
            }
            Record::Carry { entries, commit } => {
                // A rotated journal must replay both with and without its checkpoint.
                // Already committed entries must agree; only the remaining suffix is replaced.
                for entry in entries.iter().filter(|e| e.index <= self.commit) {
                    if self.history_at(entry.index) != Some((entry.term, entry.scn)) {
                        return Err(Error::new("carried journal conflicts with committed state"));
                    }
                }
                let suffix: Vec<_> =
                    entries.iter().filter(|e| e.index > self.commit).cloned().collect();
                if let Some(first) = suffix.first() {
                    self.apply(&Record::Entries { from: first.index, entries: suffix })?;
                }
                self.apply(&Record::Commit(self.commit.max(*commit)))?;
            }
            Record::Hard { term, vote } => {
                if *term < self.term
                    || (*term == self.term && self.vote.is_some() && vote != &self.vote)
                {
                    return Err(Error::new("Raft term or vote regression"));
                }
                self.term = *term;
                self.vote = vote.clone();
            }
            Record::Entries { from, entries } => {
                if *from <= self.commit
                    || *from <= self.snapshot.index
                    || *from > self.last().saturating_add(1)
                {
                    return Err(Error::new("invalid Raft log replacement boundary"));
                }
                let mut expected = *from;
                for e in entries {
                    if e.index != expected || e.term == 0 || e.term > self.term {
                        return Err(Error::new("invalid Raft entry position or term"));
                    }
                    if let Command::Members(m) = &e.command {
                        m.validate()?;
                    }
                    expected =
                        expected.checked_add(1).ok_or_else(|| Error::new("Raft index overflow"))?;
                }
                self.log.truncate((*from - self.snapshot.index - 1) as usize);
                self.log.extend(entries.clone());
                for entry in entries {
                    self.observe_scn(entry.scn);
                }
            }
            Record::Commit(index) => {
                if *index < self.commit || *index > self.last() {
                    return Err(Error::new("invalid Raft commit marker"));
                }
                self.commit = *index;
            }
            Record::Install(snapshot) => {
                self.check_ring(snapshot)?;
                snapshot.validate()?;
                if snapshot.index < self.commit || snapshot.term > self.term {
                    return Err(Error::new("invalid Raft snapshot boundary"));
                }
                let keep = self.history_at(snapshot.index) == Some((snapshot.term, snapshot.scn));
                self.log = if keep {
                    self.log.iter().filter(|e| e.index > snapshot.index).cloned().collect()
                } else {
                    vec![]
                };
                self.commit = snapshot.index;
                if self.ignore_ring_identity {
                    self.ring = snapshot.ring.clone();
                }
                self.snapshot = snapshot.clone();
                if snapshot.index != 0 {
                    self.observe_scn(snapshot.scn);
                }
                for entry in &snapshot.application {
                    self.observe_scn(entry.scn);
                }
            }
        }
        self.validate()
    }
}
#[derive(Clone, Debug)]
pub enum Record {
    Contacts(MemberNames),
    Retired(Retirement),
    Bootstrap(Membership),
    Carry { entries: Vec<Entry>, commit: u64 },
    Hard { term: u64, vote: Option<MemberId> },
    Entries { from: u64, entries: Vec<Entry> },
    Commit(u64),
    Install(Snapshot),
}
#[derive(Clone, Debug)]
pub enum Rpc {
    Vote {
        term: u64,
        last: u64,
        last_term: u64,
        last_scn: Scn,
    },
    Append {
        term: u64,
        previous: u64,
        previous_term: u64,
        previous_scn: Scn,
        entries: Vec<Entry>,
        commit: u64,
    },
    Install {
        term: u64,
        snapshot: Snapshot,
    },
    Checkpoint {
        term: u64,
        index: u64,
        snapshot_term: u64,
        snapshot_scn: Scn,
    },
}
impl Rpc {
    pub fn term(&self) -> u64 {
        match self {
            Self::Vote { term, .. }
            | Self::Append { term, .. }
            | Self::Install { term, .. }
            | Self::Checkpoint { term, .. } => *term,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Answer {
    pub term: u64,
    pub success: bool,
    pub matched: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    Stopped,
}

/// Writes accept an ordered batch; durable_state advances only after journal I/O.
/// Synchronous/in-memory stores use the default immediate durability boundary.
pub trait Storage {
    fn history_repair_required(&self) -> bool {
        false
    }
    fn state(&self) -> &State;
    fn durable_state(&self) -> &State {
        self.state()
    }
    fn write(&mut self, records: &[Record]) -> Result<()>;
    fn write_owned(&mut self, records: Vec<Record>) -> Result<()> {
        self.write(&records)
    }
    fn pending_replacement(&self) -> Option<Retirement> {
        None
    }
}

pub struct Node<S: Storage = Journal> {
    pub id: MemberId,
    pub identity: Option<common::ring::MemberIdentity>,
    pub journal: S,
    pub role: Role,
    pub repairing: bool,
    pub leader: Option<MemberId>,
    pub votes: Members,
    /// Absolute CLOCK_BOOTTIME deadline; independent of randomized campaign scheduling.
    campaign_deadline: Option<std::time::Duration>,
    pub matched: BTreeMap<MemberId, u64>,
    pub next: BTreeMap<MemberId, u64>,
    /// Runtime resets its election deadline after valid leader contact or a granted vote.
    pub contact: u64,
    pub leader_contact: Option<std::time::Instant>,
    pub lease: crate::lease::Lease,
    #[cfg(any(test, feature = "testing"))]
    clock: Option<std::time::Duration>,
    #[cfg(any(test, feature = "testing"))]
    scn_clock: Scn,
    #[cfg(any(test, feature = "testing"))]
    scn_stalled: bool,
}
impl<S: Storage> Node<S> {
    pub fn new(id: MemberId, journal: S) -> Self {
        Self {
            #[cfg(any(test, feature = "testing"))]
            scn_clock: journal.state().largest_scn().unwrap_or(Scn::new(0)),
            #[cfg(any(test, feature = "testing"))]
            scn_stalled: false,
            id,
            identity: None,
            repairing: journal.history_repair_required(),
            journal,
            role: Role::Follower,
            leader: None,
            votes: BTreeSet::new(),
            campaign_deadline: None,
            matched: BTreeMap::new(),
            next: BTreeMap::new(),
            contact: 0,
            // Restart must not erase the quiet period and immediately grant a vote.
            leader_contact: Some(std::time::Instant::now()),
            lease: crate::lease::Lease::default(),
            #[cfg(any(test, feature = "testing"))]
            clock: None,
        }
    }
    pub fn state(&self) -> &State {
        self.journal.state()
    }
    fn now(&self) -> Result<std::time::Duration> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(now) = self.clock {
            return Ok(now);
        }
        crate::lease::now()
    }
    fn write_authority(&self) -> Result<bool> {
        Ok(self.lease.can_write(
            self.state().term,
            &self.id,
            &self.state().membership(),
            self.now()?,
        ))
    }
    pub fn merge_scn(&mut self, scn: Scn) -> Result<()> {
        #[cfg(any(test, feature = "testing"))]
        {
            if scn.compare(self.scn_clock).is_gt() {
                self.scn_clock = scn;
            }
            Ok(())
        }
        #[cfg(not(any(test, feature = "testing")))]
        {
            common::scn::merge(scn).map(|_| ())
        }
    }
    pub fn merge_history_scn(&mut self) -> Result<()> {
        if let Some(scn) = self.state().largest_scn() {
            self.merge_scn(scn)?;
        }
        Ok(())
    }
    fn next_scn(&mut self) -> Result<Option<Scn>> {
        // The cached high-water avoids scanning the log on every proposal.
        if let Some(scn) = self.state().max_scn {
            self.merge_scn(scn)?;
        }
        #[cfg(any(test, feature = "testing"))]
        {
            if self.scn_stalled {
                return Ok(None);
            }
            self.scn_clock = Scn::new(self.scn_clock.number().wrapping_add(1));
            Ok(Some(self.scn_clock))
        }
        #[cfg(not(any(test, feature = "testing")))]
        {
            common::scn::try_next()
        }
    }
    fn persist(&mut self, records: Vec<Record>) -> Result<()> {
        let membership_changed = records.iter().any(|record| match record {
            Record::Bootstrap(_)
            | Record::Install(_)
            | Record::Retired(_)
            | Record::Contacts(_) => true,
            Record::Entries { entries, .. } => {
                entries.iter().any(|e| matches!(e.command, Command::Members(_)))
            }
            _ => false,
        });
        let mut accepted = Vec::new();
        for record in &records {
            if let Record::Entries { entries, .. } = record {
                accepted.extend(entries.iter().map(|entry| (entry.index, entry.term, entry.scn)));
            }
        }
        let previous_commit = self.state().commit;
        self.journal.write_owned(records)?;
        for (index, term, scn) in accepted {
            common::logn!(
                "Raft entry accepted",
                "member {},index {},term {},scn {}",
                self.id,
                index,
                term,
                scn
            );
        }
        let state = self.state();
        if state.commit > previous_commit {
            let first = state.log.partition_point(|entry| entry.index <= previous_commit);
            for entry in state.log[first..].iter().take_while(|entry| entry.index <= state.commit) {
                common::logn!(
                    "Raft entry committed",
                    "member {},index {},term {}",
                    self.id,
                    entry.index,
                    entry.term
                );
            }
        }
        if membership_changed {
            self.refresh_slot();
            let membership = self.state().membership();
            common::logn!(
                "Raft membership history changed",
                "member {},term {},old {:?},new {:?},retired {:?}",
                self.id,
                self.state().term,
                membership.old,
                membership.new,
                membership.retired.keys().collect::<Vec<_>>()
            );
        }
        Ok(())
    }
    pub fn observe_retirement(&mut self, retired: Retirement) -> Result<()> {
        if self.state().membership().retired.get(&retired.previous) == Some(&retired) {
            return Ok(());
        }
        self.persist(vec![Record::Retired(retired)])
    }
    pub fn accept_replacements(&mut self) -> Result<()> {
        if self.role != Role::Leader
            || !self.write_authority()?
            || self.state().last() != self.state().commit
        {
            return Ok(());
        }
        let old = self.state().membership();
        if old.new.is_some() {
            return Ok(());
        }
        let new = old.replacements(&old.old);
        if new != old.old {
            self.propose(Command::Members(Membership { new: Some(new), ..old }))?;
        }
        Ok(())
    }
    pub fn learn_contacts(&mut self, contacts: MemberNames) -> Result<()> {
        if contacts.iter().all(|(slot, member)| self.state().contacts.get(slot) == Some(member)) {
            return Ok(());
        }
        self.persist(vec![Record::Contacts(contacts)])
    }
    pub fn initialize_founders(&mut self, membership: Membership) -> Result<()> {
        if self.state().founders.as_ref() == Some(&membership) {
            return Ok(());
        }
        self.persist(vec![Record::Bootstrap(membership)])
    }
    pub fn refresh_slot(&mut self) {
        if let Some(identity) = &self.identity {
            self.id = self.state().membership().slot(identity).unwrap_or(UNASSIGNED);
        }
    }
    fn stop_primary(&mut self) -> Result<()> {
        self.role = Role::Stopped;
        self.lease.reset();
        self.leader = None;
        // A process has only one primary tenure; exit discards its delayed messages.
        // If demotion ever keeps this process alive, delayed messages must retain
        // their admission term and check it before proposing work in a new term.
        #[cfg(not(any(test, feature = "testing")))]
        {
            // Arm the hard deadline before any logging or stack symbolization can block.
            let watchdog = std::thread::Builder::new().name("raft-exit".into()).spawn(|| {
                std::thread::sleep(std::time::Duration::from_secs(5));
                unsafe { libc::_exit(0) }
            });
            if watchdog.is_err() {
                unsafe { libc::_exit(1) }
            }
            common::logn!(
                "Raft primary demoted; ending process",
                "member {},term {}",
                self.id,
                self.state().term
            );
            common::log::flush();
            let stack: String = std::backtrace::Backtrace::force_capture()
                .to_string()
                .chars()
                .take(16 * 1024)
                .map(|c| if c == '\n' { '|' } else { c })
                .collect();
            common::logn!("Raft primary demotion stack", "{}", stack);
            common::log::flush();
            // Avoid unbounded atexit handlers after the bounded cleanup attempt.
            unsafe { libc::_exit(0) }
        }
        // Unit tests simulate process death without terminating the whole test runner.
        #[cfg(any(test, feature = "testing"))]
        Err(Error::new("Raft primary process ended"))
    }
    fn running(&self) -> Result<()> {
        if self.role == Role::Stopped {
            return Err(Error::new("Raft process has ended"));
        }
        Ok(())
    }
    fn follow(&mut self, term: u64) -> Result<()> {
        self.running()?;
        if self.role == Role::Leader {
            return self.stop_primary();
        }
        if term > self.state().term {
            self.persist(vec![Record::Hard { term, vote: None }])?;
        }
        self.lease.reset();
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        self.campaign_deadline = None;
        Ok(())
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.running()?;
        if self.repairing {
            return Ok(());
        }
        if self.role == Role::Leader {
            return self.stop_primary();
        }
        if !self.state().membership().voters().contains(&self.id)
            && self.journal.pending_replacement().is_none()
        {
            return Ok(());
        }
        // Start the lifetime before journaling our vote: disk stalls consume it too.
        let deadline = self
            .now()?
            .checked_add(self.lease.duration())
            .ok_or_else(|| Error::new("campaign deadline overflow"))?;
        let term =
            self.state().term.checked_add(1).ok_or_else(|| Error::new("Raft term overflow"))?;
        self.persist(vec![Record::Hard { term, vote: Some(self.id.clone()) }])?;
        self.lease.reset();
        self.role = Role::Candidate;
        common::logn!(
            "Raft campaign started",
            "member {},term {},last {},commit {}",
            self.id,
            term,
            self.state().last(),
            self.state().commit
        );
        self.campaign_deadline = Some(deadline);
        self.leader = None;
        self.votes = BTreeSet::from([self.id.clone()]);
        self.win()
    }
    fn campaign_live(&self, now: std::time::Duration) -> bool {
        self.role == Role::Candidate && self.campaign_deadline.is_some_and(|end| now < end)
    }
    fn expire_campaign(&mut self) -> Result<()> {
        if self.role == Role::Candidate && !self.campaign_live(self.now()?) {
            common::logn!(
                "Raft campaign expired",
                "member {},term {},votes {:?}",
                self.id,
                self.state().term,
                self.votes
            );
            self.role = Role::Follower;
            self.campaign_deadline = None;
            self.votes.clear();
            // Keep the durable term/vote. Only a fresh campaign may ask again.
        }
        Ok(())
    }
    fn win(&mut self) -> Result<()> {
        self.expire_campaign()?;
        if self.role == Role::Candidate && self.state().membership().quorum(&self.votes) {
            self.lease.start(self.state().term, self.now()?);
            self.role = Role::Leader;
            common::logn!(
                "Raft primary elected",
                "member {},term {},votes {:?},last {},commit {}",
                self.id,
                self.state().term,
                self.votes,
                self.state().last(),
                self.state().commit
            );
            self.campaign_deadline = None;
            self.leader = Some(self.id.clone());
            let next = self
                .state()
                .last()
                .checked_add(1)
                .ok_or_else(|| Error::new("Raft index overflow"))?;
            self.next = self
                .state()
                .membership()
                .participants()
                .iter()
                .map(|id| (id.clone(), next))
                .collect();
            self.matched.clear();
            self.advance()?;
        }
        Ok(())
    }
    pub fn propose(&mut self, command: Command) -> Result<u64> {
        if self.role != Role::Leader {
            let membership = self.state().membership();
            let member = membership
                .identities
                .get(&self.id)
                .map(|identity| identity.process.as_str())
                .unwrap_or("unknown member");
            return Err(Error::new(format!("{member} is not the Raft leader")));
        }
        if !self.write_authority()? {
            return Err(Error::new("Raft primary lease expired; retry after quorum heartbeat"));
        }
        let index =
            self.state().last().checked_add(1).ok_or_else(|| Error::new("Raft index overflow"))?;
        let scn = self.next_scn()?.ok_or_else(|| {
            Error::new("SCN allocation stalled; retry after clock synchronization")
        })?;
        let entry = Entry { scn, index, term: self.state().term, command, entry_buf: None };
        self.persist(vec![Record::Entries { from: index, entries: vec![entry] }])?;
        self.advance()?;
        Ok(index)
    }
    pub fn advance(&mut self) -> Result<()> {
        if self.role != Role::Leader || !self.write_authority()? {
            return Ok(());
        }
        // A stalled SCN allocator must not stall the consensus owner. Retry the
        // mandatory current-term entry on maintenance ticks; heartbeats still run.
        if self.state().term_at(self.state().last()) != Some(self.state().term) {
            let Some(scn) = self.next_scn()? else {
                return Ok(());
            };
            let index = self
                .state()
                .last()
                .checked_add(1)
                .ok_or_else(|| Error::new("Raft index overflow"))?;
            self.persist(vec![Record::Entries {
                from: index,
                entries: vec![Entry {
                    scn,
                    index,
                    term: self.state().term,
                    command: Command::Noop,
                    entry_buf: None,
                }],
            }])?;
        }
        let membership = self.state().membership();
        let mut commit = self.state().commit;
        for index in ((commit + 1)..=self.state().last()).rev() {
            if self.state().term_at(index) != Some(self.state().term) {
                continue;
            }
            let mut votes: BTreeSet<_> = self
                .matched
                .iter()
                .filter(|(_, matched)| **matched >= index)
                .map(|(id, _)| id.clone())
                .collect();
            let durable = self.journal.durable_state();
            if durable.history_at(index) == self.state().history_at(index) {
                votes.insert(self.id);
            }
            if membership.quorum(&votes) {
                commit = index;
                break;
            }
        }
        if commit > self.state().commit {
            self.persist(vec![Record::Commit(commit)])?;
        }
        // Complete a joint configuration only once the joint entry is committed.
        let committed = self.state().committed_membership();
        if committed.new.is_some() && committed == self.state().membership() {
            let new =
                committed.new.clone().ok_or_else(|| Error::new("missing joint configuration"))?;
            self.propose(Command::Members(Membership {
                retired: membership.retired.clone(),
                identities: membership.identities.clone(),
                old: new,
                new: None,
            }))?;
        }
        let current = self.state().membership();
        let final_committed = self.state().committed_membership() == current;
        if final_committed && !current.voters().contains(&self.id) {
            self.follow(self.state().term)?;
        }
        Ok(())
    }
    pub fn election_quiet(&self, now: std::time::Instant, minimum: std::time::Duration) -> bool {
        self.role == Role::Leader
            || self.leader_contact.is_some_and(|seen| {
                now.checked_duration_since(seen).is_none_or(|elapsed| elapsed < minimum)
            })
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn can_admit(&self, now: std::time::Duration) -> bool {
        self.can_admit_with_policy(now, false, false)
    }
    pub fn can_admit_with_policy(
        &self,
        now: std::time::Duration,
        read: bool,
        allow_stale_reads: bool,
    ) -> bool {
        self.role == Role::Leader
            && self.state().term_at(self.state().commit) == Some(self.state().term)
            && self.lease.active(self.state().term, now)
            && ((read && allow_stale_reads)
                || self.lease.valid(self.state().term, &self.id, &self.state().membership(), now))
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn request(&self, peer: &MemberId) -> Option<Rpc> {
        self.make_request(peer, true)
    }
    /// Runtime sends a checkpoint reference, never checkpoint contents from a primary.
    pub fn request_reference(&self, peer: &MemberId) -> Option<Rpc> {
        self.make_request(peer, false)
    }
    fn make_request(&self, peer: &MemberId, contents: bool) -> Option<Rpc> {
        let s = self.state();
        match self.role {
            Role::Follower | Role::Stopped => None,
            Role::Candidate => {
                if !self.campaign_live(self.now().ok()?) {
                    return None;
                }
                Some(Rpc::Vote {
                    term: s.term,
                    last: s.last(),
                    last_term: s.term_at(s.last())?,
                    last_scn: s.scn_at(s.last())?,
                })
            }
            Role::Leader => {
                let next = self.next.get(peer).copied().unwrap_or(s.last().saturating_add(1));
                if next <= s.snapshot.index {
                    return Some(if contents {
                        Rpc::Install { term: s.term, snapshot: s.snapshot.clone() }
                    } else {
                        Rpc::Checkpoint {
                            term: s.term,
                            index: s.snapshot.index,
                            snapshot_term: s.snapshot.term,
                            snapshot_scn: s.snapshot.scn,
                        }
                    });
                }
                let previous = next - 1;
                Some(Rpc::Append {
                    term: s.term,
                    previous,
                    previous_term: s.term_at(previous)?,
                    previous_scn: s.scn_at(previous)?,
                    entries: s.log.iter().filter(|e| e.index >= next).take(128).cloned().collect(),
                    commit: s.commit,
                })
            }
        }
    }
    pub fn receive(&mut self, sender: &MemberId, rpc: &Rpc) -> Result<Answer> {
        self.running()?;
        if let Rpc::Append { term, previous, previous_term, previous_scn, entries, commit } = rpc {
            common::logn!(
                "Raft heartbeat received",
                "member {},primary {},term {},previous {},previous_term {},previous_scn {},entries {},commit {}",
                self.id,
                sender,
                term,
                previous,
                previous_term,
                previous_scn,
                entries.len(),
                commit
            );
        }
        if let Rpc::Install { snapshot, .. } = rpc {
            self.state().check_ring(snapshot)?;
        }
        if self.state().membership().retired.contains_key(sender) {
            return Ok(Answer { term: self.state().term, success: false, matched: 0 });
        }
        // A removed/lagging member can still send votes from its old configuration.
        // It must not repeatedly terminate the primary by advancing its private term.
        if matches!(rpc, Rpc::Vote { .. }) && !self.state().membership().voters().contains(sender) {
            return Ok(Answer { term: self.state().term, success: false, matched: 0 });
        }
        // Network entries retain the creator's SCN, even when the local prefix
        // does not match yet. Bulk history imports merge their maximum once.
        match rpc {
            Rpc::Append { previous, previous_scn, entries, .. } => {
                if *previous != 0 {
                    self.merge_scn(*previous_scn)?;
                }
                for entry in entries {
                    self.merge_scn(entry.scn)?;
                }
            }
            Rpc::Vote { last, last_scn, .. } if *last != 0 => self.merge_scn(*last_scn)?,
            Rpc::Install { snapshot, .. } => {
                if let Some(scn) = snapshot.largest_scn() {
                    self.merge_scn(scn)?;
                }
            }
            Rpc::Checkpoint { index, snapshot_scn, .. } if *index != 0 => {
                self.merge_scn(*snapshot_scn)?
            }
            _ => {}
        }
        if rpc.term() > self.state().term {
            self.follow(rpc.term())?;
        }
        let mut answer = Answer { term: self.state().term, success: false, matched: 0 };
        if rpc.term() < self.state().term {
            return Ok(answer);
        }
        match rpc {
            Rpc::Checkpoint { term, index, snapshot_term, snapshot_scn } => {
                // A reference is also leader contact. A matching local prefix can acknowledge
                // immediately; otherwise the runtime fetches committed state from a peer.
                return self.receive(
                    sender,
                    &Rpc::Append {
                        term: *term,
                        previous: *index,
                        previous_term: *snapshot_term,
                        previous_scn: *snapshot_scn,
                        entries: vec![],
                        commit: *index,
                    },
                );
            }
            Rpc::Vote { last, last_term, .. } => {
                if self.repairing {
                    return Ok(answer);
                }
                if !self.state().membership().voters().contains(&self.id) {
                    return Ok(answer);
                }
                let fresh = (*last_term, *last)
                    >= (
                        self.state().term_at(self.state().last()).unwrap_or(0),
                        self.state().last(),
                    );
                if fresh && self.state().vote.as_ref().is_none_or(|v| v == sender) {
                    self.persist(vec![Record::Hard {
                        term: self.state().term,
                        vote: Some(*sender),
                    }])?;
                    self.contact = self.contact.wrapping_add(1);
                    answer.success = true;
                }
            }
            Rpc::Append { previous, previous_term, previous_scn, entries, commit, .. } => {
                let changed = self.leader != Some(*sender) || self.role != Role::Follower;
                self.follow(self.state().term)?;
                if changed {
                    common::logn!(
                        "Raft primary recognized",
                        "member {},primary {},term {}",
                        self.id,
                        sender,
                        self.state().term
                    );
                }
                self.leader = Some(*sender);
                self.contact = self.contact.wrapping_add(1);
                self.leader_contact = Some(std::time::Instant::now());
                if self.repairing {
                    return Ok(answer);
                }
                if self.state().history_at(*previous) != Some((*previous_term, *previous_scn)) {
                    // Uncommitted differences are ordinary Raft suffix replacement.
                    // A conflict in committed/compacted history requires a full rebuild.
                    if *previous <= self.state().commit {
                        self.repairing = true;
                    }

                    common::logn!(
                        "Raft append prefix mismatch",
                        "member {},primary {},term {},previous {},expected_term {},expected_scn {},local_tip {:?},last {},commit {}",
                        self.id,
                        sender,
                        self.state().term,
                        previous,
                        previous_term,
                        previous_scn,
                        self.state().history_at(*previous),
                        self.state().last(),
                        self.state().commit
                    );
                    return Ok(answer);
                }
                let mut expected =
                    previous.checked_add(1).ok_or_else(|| Error::new("Raft index overflow"))?;
                for e in entries {
                    if e.index != expected || e.term > rpc.term() || e.term == 0 {
                        return Err(Error::new("malformed AppendEntries"));
                    }
                    expected =
                        expected.checked_add(1).ok_or_else(|| Error::new("Raft index overflow"))?;
                }
                let mut records = vec![];
                if let Some(start) = entries
                    .iter()
                    .position(|e| self.state().history_at(e.index) != Some((e.term, e.scn)))
                {
                    if entries[start].index <= self.state().commit {
                        self.repairing = true;
                        return Ok(answer);
                    }
                    records.push(Record::Entries {
                        from: entries[start].index,
                        entries: entries[start..].to_vec(),
                    });
                }
                let matched = expected - 1;
                let new_commit = (*commit).min(matched);
                if new_commit > self.state().commit {
                    records.push(Record::Commit(new_commit));
                }
                self.persist(records)?;
                answer.success = true;
                answer.matched = matched;
            }
            Rpc::Install { snapshot, .. } => {
                if self.repairing {
                    return Ok(answer);
                }
                self.follow(self.state().term)?;
                self.leader = Some(*sender);
                self.contact = self.contact.wrapping_add(1);
                self.leader_contact = Some(std::time::Instant::now());
                if snapshot.index > self.state().commit {
                    self.persist(vec![Record::Install(snapshot.clone())])?;
                }
                answer.success = true;
                answer.matched = snapshot.index;
            }
        }
        Ok(answer)
    }
    /// Record send-time authority before a successful response can advance commits.
    pub fn response_sent(
        &mut self,
        peer: &MemberId,
        request: &Rpc,
        answer: &Answer,
        sent: std::time::Duration,
    ) -> Result<()> {
        if self.role == Role::Leader
            && matches!(request, Rpc::Append { .. })
            && answer.success
            && request.term() == self.state().term
            && answer.term == self.state().term
            && self.state().membership().voters().contains(peer)
        {
            self.lease.acknowledge(peer, request.term(), sent);
        }
        self.response(peer, request, answer)
    }

    pub fn response(&mut self, peer: &MemberId, request: &Rpc, answer: &Answer) -> Result<()> {
        self.running()?;
        if answer.term > self.state().term {
            return self.follow(answer.term);
        }
        if request.term() != self.state().term || answer.term != self.state().term {
            return Ok(());
        }
        match request {
            Rpc::Vote { .. } if self.role == Role::Candidate => {
                // Check on receipt, not just in the timer: it may be stalled or suspended.
                self.expire_campaign()?;
                if self.role == Role::Candidate
                    && answer.success
                    && self.state().membership().voters().contains(peer)
                {
                    self.votes.insert(*peer);
                    self.win()?;
                }
            }
            Rpc::Append { previous, entries, .. } if self.role == Role::Leader => {
                if answer.success {
                    let matched = entries.last().map_or(*previous, |e| e.index);
                    self.matched
                        .entry(*peer)
                        .and_modify(|v| *v = (*v).max(matched))
                        .or_insert(matched);
                    self.next.insert(*peer, matched.saturating_add(1));
                    self.advance()?;
                } else {
                    let next = self.next.entry(*peer).or_insert(previous.saturating_add(1));
                    *next = next.saturating_sub(1).max(1);
                }
            }
            Rpc::Checkpoint { index, .. } if self.role == Role::Leader && answer.success => {
                self.matched.insert(*peer, *index);
                self.next.insert(*peer, index.saturating_add(1));
                self.advance()?;
            }
            Rpc::Install { snapshot, .. } if self.role == Role::Leader && answer.success => {
                self.matched.insert(*peer, snapshot.index);
                self.next.insert(*peer, snapshot.index.saturating_add(1));
                self.advance()?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod tests {
    use super::*;
    #[cfg(test)]
    fn retirement(id: MemberId) -> Retirement {
        Retirement {
            previous: id,
            old: names()[&id].clone(),
            next: id + INCARNATION,
            member: Member {
                process: names()[&id].process.clone(),
                identity: common::ring::MemberIdentity::try_from(
                    format!("{:02x}", 100 + id).repeat(64),
                )
                .unwrap(),
            },
        }
    }
    #[cfg(test)]
    fn replace_node(old: &TestNode, retired: &Retirement) -> TestNode {
        let mut state = old.state().clone();
        state.snapshot = state.capture();
        state.log.clear();
        state.vote = None;
        state.apply(&Record::Retired(retired.clone())).unwrap();
        let mut node = Node::new(
            retired.next,
            Memory {
                initial: state.clone(),
                state,
                pending: Some(retired.clone()),
                records: vec![],
                failed: false,
            },
        );
        node.identity = Some(retired.member.identity.clone());
        node.clock = Some(std::time::Duration::ZERO);
        node
    }
    #[test]
    fn creator_scns_survive_replication_and_advance_the_next_primary() {
        let mut nodes = cluster(3);
        nodes[0].scn_clock = Scn::new(1000);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        let original = nodes[0].state().log[0].scn;
        let copied = nodes[1].state().log[0].scn;
        nodes[1].campaign().unwrap();
        pair(&mut nodes, 1, 2);
        let next = nodes[1].state().log.last().unwrap().scn;
        assert_eq!(original, Scn::new(1001));
        assert_eq!(copied, original);
        assert!(next.compare(original).is_gt());
    }
    #[test]
    fn votes_merge_the_candidates_entry_scn_without_copying_its_log() {
        let mut member = node(1, members(3));
        let vote = Rpc::Vote { term: 1, last: 9, last_term: 1, last_scn: Scn::new(5000) };
        let answer = member.receive(&0, &vote).unwrap();
        let next = member.next_scn().unwrap().unwrap();
        assert!(answer.success);
        assert_eq!(member.state().last(), 0);
        assert_eq!(next, Scn::new(5001));
    }
    #[test]
    fn matching_term_and_index_with_a_different_scn_is_not_a_matching_prefix() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1); // Deliver the commit marker before corrupting the prefix.
        let mut request = nodes[0].request(&1).unwrap();
        if let Rpc::Append { previous_scn, .. } = &mut request {
            *previous_scn = Scn::new(previous_scn.number() + 1);
        } else {
            panic!("heartbeat");
        }
        let before = nodes[1].journal.records.len();
        let rejected = nodes[1].receive(&0, &request).unwrap();
        assert!(!rejected.success);
        assert_eq!(nodes[1].journal.records.len(), before);
        assert!(nodes[1].repairing);
        let old_term = nodes[1].state().term;
        nodes[1].campaign().unwrap();
        assert_eq!(nodes[1].state().term, old_term);
        let vote = nodes[1]
            .receive(
                &2,
                &Rpc::Vote {
                    term: old_term + 1,
                    last: 99,
                    last_term: old_term,
                    last_scn: Scn::new(100),
                },
            )
            .unwrap();
        assert!(!vote.success);
    }
    #[test]
    fn scn_stall_defers_new_term_entry_without_blocking_heartbeats() {
        let mut primary = node(0, members(1));
        primary.scn_stalled = true;
        primary.campaign().unwrap();
        let heartbeat = primary.request(&0);
        let blocked = primary.propose(Command::Noop).is_err();
        assert_eq!(primary.role, Role::Leader);
        assert_eq!(primary.state().last(), 0);
        assert!(matches!(heartbeat, Some(Rpc::Append { .. })));
        assert!(blocked);
        primary.scn_stalled = false;
        primary.advance().unwrap();
        assert_eq!(primary.state().commit, 1);
    }
    #[test]
    fn accepted_local_entries_are_not_quorum_votes_until_journaled() {
        struct Delayed {
            accepted: State,
            durable: State,
        }
        impl Storage for Delayed {
            fn state(&self) -> &State {
                &self.accepted
            }
            fn durable_state(&self) -> &State {
                &self.durable
            }
            fn write(&mut self, records: &[Record]) -> Result<()> {
                for record in records {
                    self.accepted.apply(record)?;
                }
                Ok(())
            }
        }
        let initial = node(0, members(1)).state().clone();
        let mut primary = Node::new(0, Delayed { accepted: initial.clone(), durable: initial });
        primary.campaign().unwrap();
        assert_eq!(primary.state().last(), 1);
        assert_eq!(primary.state().commit, 0);
        primary.journal.durable = primary.state().clone();
        primary.advance().unwrap();
        assert_eq!(primary.state().commit, 1);
        assert_eq!(primary.journal.durable_state().commit, 0);
        primary.journal.durable = primary.state().clone();
        assert_eq!(primary.journal.durable_state().commit, 1);

        let first = primary.propose(Command::Noop).unwrap();
        let second = primary.propose(Command::Noop).unwrap();
        assert_eq!((first, second), (2, 3));
        assert_eq!(primary.state().last(), second);
        assert_eq!(primary.state().commit, 1);

        // Completing the write through index 2 may commit that prefix, but not index 3.
        let mut through_first = primary.state().clone();
        through_first.log.pop();
        primary.journal.durable = through_first;
        primary.advance().unwrap();
        assert_eq!(primary.state().commit, first);
        assert_eq!(primary.state().last(), second);

        primary.journal.durable = primary.state().clone();
        primary.advance().unwrap();
        assert_eq!(primary.state().commit, second);
    }

    #[test]
    fn retirements_are_not_votes_and_only_majority_allows_replacement_elections() {
        let mut nodes = cluster(5);
        let lost: Vec<_> = (2..5).map(retirement).collect();
        for retired in &lost {
            nodes[retired.previous as usize] =
                replace_node(&nodes[retired.previous as usize], retired);
        }
        // Everyone has heard of two retirements. That is not an old quorum.
        for node in &mut nodes {
            for retired in &lost[..2] {
                node.observe_retirement(retired.clone()).unwrap();
            }
        }
        nodes[2].campaign().unwrap();
        pair(&mut nodes, 2, 0);
        pair(&mut nodes, 2, 1);
        pair(&mut nodes, 2, 3);
        assert_eq!(nodes[2].role, Role::Candidate);
        assert_eq!(nodes[0].state().term, 0); // Unauthorized campaign did not advance it.
        // With three known retirements, the replacement electorate can act.
        for node in &mut nodes {
            node.observe_retirement(lost[2].clone()).unwrap();
        }
        nodes[2].campaign().unwrap();
        for _ in 0..4 {
            pair(&mut nodes, 2, 3);
            pair(&mut nodes, 2, 4);
        }
        assert_eq!(nodes[2].role, Role::Leader);
        assert!(nodes[2].journal.pending_replacement().is_some());
        nodes[2].accept_replacements().unwrap();
        for _ in 0..6 {
            pair(&mut nodes, 2, 3);
            pair(&mut nodes, 2, 4);
        }
        for node in &nodes[2..] {
            let membership = node.state().membership();
            assert!(membership.new.is_none());
            assert_eq!(membership.old.len(), 5);
            assert!(membership.old.contains(&node.id));
            assert!(node.journal.pending_replacement().is_none());
        }
    }
    #[test]
    fn live_primary_replaces_a_minority_using_joint_consensus() {
        let mut nodes = cluster(5);
        for node in &mut nodes {
            node.clock = Some(std::time::Duration::ZERO);
        }
        nodes[0].campaign().unwrap();
        for _ in 0..4 {
            pair(&mut nodes, 0, 1);
            pair(&mut nodes, 0, 2);
        }
        let lost = retirement(4);
        nodes[4] = replace_node(&nodes[4], &lost);
        nodes[0].observe_retirement(lost.clone()).unwrap();
        let proof_only = nodes[0].state().membership();
        assert!(!proof_only.quorum(&Members::from([0, lost.next])));
        nodes[0].accept_replacements().unwrap();
        // The old quorum and new quorum overlap in the three surviving originals.
        for _ in 0..6 {
            pair(&mut nodes, 0, 1);
            pair(&mut nodes, 0, 2);
            pair(&mut nodes, 0, 4);
        }
        assert!(nodes[4].journal.pending_replacement().is_none());
        let membership = nodes[0].state().membership();
        assert_eq!(membership.old.len(), 5);
        assert!(!membership.old.contains(&lost.previous));
        assert!(membership.old.contains(&lost.next));
        let term = nodes[0].state().term;
        let reply = nodes[0]
            .receive(
                &lost.previous,
                &Rpc::Vote {
                    last_scn: Scn::new(0),
                    term: term + 10,
                    last: u64::MAX,
                    last_term: u64::MAX,
                },
            )
            .unwrap();
        assert!(!reply.success);
        assert_eq!(nodes[0].state().term, term);
    }
    #[test]
    fn retirement_cannot_rebind_an_unrelated_identity_or_slot() {
        let mut node = node(0, members(5));
        let mut bad = retirement(1);
        bad.next += 1;
        let wrong_slot = node.observe_retirement(bad);
        let mut bad = retirement(1);
        bad.old = names()[&2].clone();
        let wrong_member = node.observe_retirement(bad);
        assert!(wrong_slot.is_err());
        assert!(wrong_member.is_err());
        assert!(node.state().retirements.is_empty());
    }

    #[test]
    fn campaign_deadline_rejects_late_quorum_without_timer_activity() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        for (arrival, elected) in [(1149, true), (1150, false), (1151, false), (10000, false)] {
            let mut nodes = cluster(5);
            nodes[0].clock = Some(ms(1000));
            nodes[0].campaign().unwrap();
            let request = nodes[0].request(&1).unwrap();
            let first = nodes[1].receive(&0, &request).unwrap();
            let second = nodes[2].receive(&0, &request).unwrap();
            nodes[0].clock = Some(ms(1100));
            nodes[0].response(&1, &request, &first).unwrap();
            let records = nodes[0].journal.records.len();
            // No election timer runs between these replies, even across a long pause.
            nodes[0].clock = Some(ms(arrival));
            let sends = nodes[0].request(&2).is_some();
            nodes[0].response(&2, &request, &second).unwrap();
            assert_eq!(sends, elected);
            assert_eq!(nodes[0].role == Role::Leader, elected);
            if !elected {
                assert_eq!(nodes[0].role, Role::Follower);
                assert!(nodes[0].votes.is_empty());
                assert_eq!(nodes[0].journal.records.len(), records);
                assert_eq!(nodes[0].state().vote, Some(0));
                assert_eq!(nodes[0].state().term, 1);
            }
        }
    }

    #[test]
    fn delayed_campaign_has_its_own_lifetime_and_rejects_previous_campaign_votes() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        let mut nodes = cluster(3);
        let config = common::config::RaftConfig {
            election_min_ms: 220,
            election_max_ms: 900,
            cold_election_delay_ms: 5000,
            ..Default::default()
        };
        nodes[0].lease = crate::lease::Lease::configured(&config);
        nodes[0].clock = Some(ms(10000));
        nodes[0].campaign().unwrap();
        let old_request = nodes[0].request(&1).unwrap();
        let old_answer = nodes[1].receive(&0, &old_request).unwrap();
        nodes[0].clock = Some(ms(10219));
        let live = nodes[0].request(&1).is_some();
        nodes[0].clock = Some(ms(10220));
        let expired = nodes[0].request(&1).is_none();
        assert!(live && expired);
        // Scheduling may wait much longer than the lifetime of either campaign.
        nodes[0].clock = Some(ms(20000));
        nodes[0].campaign().unwrap();
        nodes[0].response(&1, &old_request, &old_answer).unwrap();
        assert_eq!(nodes[0].role, Role::Candidate);
        assert_eq!(nodes[0].votes, Members::from([0]));
        let new_request = nodes[0].request(&1).unwrap();
        let new_answer = nodes[1].receive(&0, &new_request).unwrap();
        nodes[0].clock = Some(ms(20219));
        nodes[0].response(&1, &new_request, &new_answer).unwrap();
        assert_eq!(nodes[0].role, Role::Leader);
        assert_eq!(nodes[0].state().term, 2);
    }

    #[test]
    fn catch_up_contacts_do_not_grant_votes_or_rebind_identities() {
        let mut nodes = cluster(3);
        let before = nodes[0].state().membership();
        let contact = Member {
            identity: common::ring::MemberIdentity::generate().unwrap(),
            process: "later-member".into(),
        };
        nodes[0].learn_contacts(MemberNames::from([(99, contact.clone())])).unwrap();
        assert_eq!(nodes[0].state().membership(), before);
        assert_eq!(nodes[0].state().contacts.get(&99), Some(&contact));
        let vote = Rpc::Vote { last_scn: Scn::new(0), term: 100, last: 0, last_term: 0 };
        let answer = nodes[0].receive(&99, &vote).unwrap();
        assert!(!answer.success);
        assert_eq!(nodes[0].state().term, 0);
        let conflicting = nodes[0].learn_contacts(MemberNames::from([(0, contact)]));
        assert!(conflicting.is_err());
    }

    #[test]
    fn bootstrap_assigns_local_slot_and_retries_preserve_voting_history() {
        let initial = State::initial("01".repeat(64), Members::new(), MemberNames::new()).unwrap();
        let storage = Memory {
            pending: None,
            state: initial.clone(),
            initial,
            records: Vec::new(),
            failed: false,
        };
        let mut node = Node::new(UNASSIGNED, storage);
        let identities = names();
        node.identity = Some(identities[&0].identity.clone());
        node.campaign().unwrap();
        assert_eq!(node.role, Role::Follower);
        assert!(node.journal.records.is_empty());
        let founders =
            Membership { retired: BTreeMap::new(), identities, old: members(3), new: None };
        node.initialize_founders(founders.clone()).unwrap();
        assert_eq!(node.id, 0);
        node.campaign().unwrap();
        let term = node.state().term;
        let vote = node.state().vote;
        let count = node.journal.records.len();
        node.initialize_founders(founders.clone()).unwrap();
        assert_eq!(node.journal.records.len(), count);
        let mut conflicting = founders;
        conflicting.old = members(2);
        let rejected = node.initialize_founders(conflicting);
        assert!(rejected.is_err());
        let recovered = node.journal.recover();
        assert_eq!(recovered.state.term, term);
        assert_eq!(recovered.state.vote, vote);
        assert_eq!(recovered.state.founders, node.state().founders);
    }

    pub struct Memory {
        pending: Option<Retirement>,
        state: State,
        initial: State,
        pub records: Vec<Record>,
        failed: bool,
    }
    impl Storage for Memory {
        fn pending_replacement(&self) -> Option<Retirement> {
            self.pending.clone().filter(|r| {
                let members = self.state.membership().members();
                !(members.contains(&r.next) && !members.contains(&r.previous))
            })
        }
        fn state(&self) -> &State {
            &self.state
        }
        fn write(&mut self, records: &[Record]) -> Result<()> {
            if self.failed {
                return Err(Error::new("injected journal failure"));
            }
            let mut state = self.state.clone();
            for record in records {
                state.apply(record)?;
            }
            self.records.extend_from_slice(records);
            self.state = state;
            Ok(())
        }
    }
    impl Memory {
        pub fn snapshot(&mut self, _now: u64) -> Result<()> {
            self.write(&[Record::Install(self.state.capture())])
        }
        pub fn fail_writes(&mut self) {
            self.failed = true;
        }
        fn recover(&self) -> Self {
            let mut state = self.initial.clone();
            for record in &self.records {
                state.apply(record).unwrap();
            }
            Self {
                pending: self.pending.clone(),
                state,
                initial: self.initial.clone(),
                records: self.records.clone(),
                failed: false,
            }
        }
    }
    pub type TestNode = Node<Memory>;
    pub fn restart(node: &TestNode) -> TestNode {
        Node::new(node.id.clone(), node.journal.recover())
    }
    pub fn names() -> MemberNames {
        (0..32)
            .map(|i| {
                (
                    i,
                    Member {
                        identity: common::ring::MemberIdentity::try_from(
                            format!("{:02x}", i + 1).repeat(64),
                        )
                        .unwrap(),
                        process: format!("member-{i}"),
                    },
                )
            })
            .collect()
    }
    pub fn members(n: usize) -> Members {
        (0..n as u64).collect()
    }
    pub fn node(id: usize, members: Members) -> TestNode {
        let state = State::initial("01".repeat(64), members, names()).unwrap();
        Node::new(
            id as u64,
            Memory { pending: None, initial: state.clone(), state, records: vec![], failed: false },
        )
    }
    #[cfg(test)]
    fn exchange(a: &mut TestNode, b: &mut TestNode) {
        if let Some(request) = a.request(&b.id) {
            let response = match b.receive(&a.id, &request) {
                Ok(response) => response,
                Err(_) if b.role == Role::Stopped => {
                    *b = restart(b);
                    return;
                }
                Err(error) => panic!("{error}"),
            };
            let result = a.response(&b.id, &request, &response);
            if a.role == Role::Stopped {
                *a = restart(a);
            } else {
                result.unwrap();
            }
        }
    }
    #[cfg(test)]
    fn pair(nodes: &mut [TestNode], a: usize, b: usize) {
        if a < b {
            let (left, right) = nodes.split_at_mut(b);
            exchange(&mut left[a], &mut right[0]);
        } else {
            let (left, right) = nodes.split_at_mut(a);
            exchange(&mut right[0], &mut left[b]);
        }
    }
    #[cfg(test)]
    fn cluster(n: usize) -> Vec<TestNode> {
        (0..n).map(|i| node(i, members(n))).collect()
    }
    #[test]
    fn random_identity_assignment_is_idempotent_and_retires_slots_permanently() {
        let mut membership = Membership {
            retired: BTreeMap::new(),
            identities: names(),
            old: members(3),
            new: None,
        };
        let applicant = Member {
            identity: common::ring::MemberIdentity::try_from("ab".repeat(64)).unwrap(),
            process: "route-not-slot-32".into(),
        };
        let first = membership.assign(applicant.clone()).unwrap();
        let retry = membership.assign(applicant.clone()).unwrap();
        membership.old.insert(first);
        membership.validate().unwrap();
        let bytes = crate::codec::encode(&membership).unwrap();
        let mut recovered: Membership = crate::codec::decode(&bytes).unwrap();
        let retained = recovered.slot(&applicant.identity);
        recovered.old.remove(&first);
        let replacement = Member {
            identity: common::ring::MemberIdentity::try_from("ac".repeat(64)).unwrap(),
            process: applicant.process.clone(),
        };
        let next = recovered.assign(replacement).unwrap();
        let mut impostor = applicant;
        impostor.process = "different-route".into();
        let rebound = recovered.assign(impostor);
        assert_eq!(first, 32);
        assert_eq!(retry, first);
        assert_eq!(retained, Some(first));
        assert_eq!(next, 33);
        assert!(rebound.is_err());
    }

    #[test]
    fn retired_member_slots_remain_bound_after_snapshot_and_restart() {
        let mut original = State::initial("01".repeat(64), members(3), names()).unwrap();
        original.apply(&Record::Hard { term: 1, vote: None }).unwrap();
        let retired = Membership {
            retired: BTreeMap::new(),
            identities: names(),
            old: BTreeSet::from([0, 2]),
            new: None,
        };
        original
            .apply(&Record::Entries {
                from: 1,
                entries: vec![Entry {
                    scn: common::scn::Scn::new(1),
                    index: 1,
                    term: 1,
                    command: Command::Members(retired.clone()),
                    entry_buf: None,
                }],
            })
            .unwrap();
        original.apply(&Record::Commit(1)).unwrap();
        let checkpoint = original.capture();
        original.apply(&Record::Install(checkpoint)).unwrap();
        let mut rebound = retired;
        rebound.identities.get_mut(&1).unwrap().identity =
            common::ring::MemberIdentity::try_from("ff".repeat(64)).unwrap();
        let record = Record::Entries {
            from: 2,
            entries: vec![Entry {
                scn: common::scn::Scn::new(2),
                index: 2,
                term: 1,
                command: Command::Members(rebound),
                entry_buf: None,
            }],
        };
        let mut storage = Memory {
            pending: None,
            initial: original.clone(),
            state: original,
            records: vec![],
            failed: false,
        };
        let rejected = storage.write(&[record]).is_err();
        let old_name = storage.state.membership().identities[&1].clone();
        let vote = crate::codec::encode(&Record::Hard { term: 2, vote: Some(1) }).unwrap();
        assert!(rejected);
        assert_eq!(old_name.process, "member-1");
        let decoded: Record = crate::codec::decode(&vote).unwrap();
        assert!(matches!(decoded, Record::Hard { term: 2, vote: Some(1) }));
    }
    #[test]
    fn foreign_checkpoint_rejected_before_state_or_term_changes() {
        let mut node = node(0, members(3));
        let mut foreign = node.state().capture();
        foreign.ring = "02".repeat(64);
        let term = node.state().term;
        let before = node.state().capture();
        let rpc = Rpc::Install { term: term + 10, snapshot: foreign.clone() };
        let rejected_rpc = node.receive(&1, &rpc).is_err();
        let rejected_replay = node.journal.write(&[Record::Install(foreign)]).is_err();
        assert!(rejected_rpc && rejected_replay);
        assert_eq!(node.state().term, term);
        assert_eq!(node.state().capture(), before);
    }
    #[test]
    fn election_quiet_period_survives_restart_and_leader_contact() {
        use std::time::{Duration, Instant};
        let node = node(0, members(3));
        let start = node.leader_contact.unwrap();
        let minimum = Duration::from_millis(150);
        let early = node.election_quiet(start + Duration::from_millis(149), minimum);
        let allowed = node.election_quiet(start + minimum, minimum);
        let restored = restart(&node);
        let restarted = restored.election_quiet(Instant::now(), minimum);
        assert!(early);
        assert!(!allowed);
        assert!(restarted);
    }

    #[test]
    fn lease_reads_need_committed_noop_and_do_not_journal_heartbeats() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1); // Elect, but do not yet commit the new term's no-op.
        let term = nodes[0].state().term;
        nodes[0].lease = crate::lease::Lease::elected(term, ms(0));
        nodes[0].clock = Some(ms(0));
        nodes[0].lease.acknowledge(&1, term, ms(100));
        let no_commit = nodes[0].can_admit(ms(160));
        pair(&mut nodes, 0, 1); // Commit the no-op.
        pair(&mut nodes, 0, 1); // Tell the follower that it is committed.
        let before = (nodes[0].journal.records.len(), nodes[1].journal.records.len());
        pair(&mut nodes, 0, 1); // Ordinary empty heartbeat: no persistent state changes.
        let after = (nodes[0].journal.records.len(), nodes[1].journal.records.len());
        let ready = nodes[0].can_admit(ms(160));
        let repeated_reads = (160..250).all(|at| nodes[0].can_admit(ms(at)));
        let expired = nodes[0].can_admit(ms(250));
        let restored = restart(&nodes[0]);
        let restart_can_read = restored.can_admit(ms(160));
        let old = nodes[0].state();
        let vote =
            Rpc::Vote { last_scn: Scn::new(0), term: term + 1, last: old.last(), last_term: term };
        let answer = nodes[0].receive(&2, &vote);
        let stepped_down = nodes[0].can_admit(ms(160));
        assert!(!no_commit);
        assert_eq!(before, after);
        assert!(ready);
        assert!(repeated_reads);
        assert!(!expired);
        assert!(!restart_can_read);
        assert!(answer.is_err());
        assert!(!stepped_down);
    }

    #[test]
    fn elected_primary_admits_as_soon_as_current_term_commits_with_fresh_quorum() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        let mut nodes = cluster(3);
        nodes[0].clock = Some(ms(1000));
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        let before_commit = nodes[0].can_admit(ms(1001));
        let request = nodes[0].request(&1).unwrap();
        let answer = nodes[1].receive(&0, &request).unwrap();
        nodes[0].clock = Some(ms(1002));
        nodes[0].response_sent(&1, &request, &answer, ms(1001)).unwrap();
        let immediate = nodes[0].can_admit(ms(1002));
        let expired = nodes[0].can_admit(ms(1151));
        assert!(!before_commit);
        assert!(immediate);
        assert!(!expired);
    }

    #[test]
    fn stale_read_policy_retains_committed_term_and_leader_checks() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        let term = nodes[0].state().term;
        nodes[0].lease = crate::lease::Lease::elected(term, ms(0));
        nodes[0].clock = Some(ms(0));
        let uncommitted = nodes[0].can_admit_with_policy(ms(500), true, true);
        pair(&mut nodes, 0, 1);
        nodes[0].lease.acknowledge(&1, term, ms(100));
        let early = nodes[0].can_admit_with_policy(ms(149), true, true);
        let live_write = nodes[0].can_admit_with_policy(ms(160), false, true);
        let default_read = nodes[0].can_admit_with_policy(ms(250), true, false);
        let stale_read = nodes[0].can_admit_with_policy(ms(250), true, true);
        let expired_write = nodes[0].can_admit_with_policy(ms(250), false, true);
        let follower = nodes[1].can_admit_with_policy(ms(250), true, true);
        let restored = restart(&nodes[0]);
        let restarted = restored.can_admit_with_policy(ms(250), true, true);
        let vote = Rpc::Vote {
            last_scn: Scn::new(0),
            term: term + 1,
            last: nodes[0].state().last(),
            last_term: term,
        };
        let demotion = nodes[0].receive(&2, &vote);
        let demoted_read = nodes[0].can_admit_with_policy(ms(250), true, true);
        assert!(!uncommitted);
        assert!(early);
        assert!(live_write);
        assert!(!default_read);
        assert!(stale_read);
        assert!(!expired_write);
        assert!(!follower);
        assert!(!restarted);
        assert!(demotion.is_err());
        assert!(!demoted_read);
    }

    #[test]
    fn expired_primary_needs_fresh_quorum_before_proposals_or_commits() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        let mut nodes = cluster(5);
        nodes[0].clock = Some(ms(0));
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 2);
        let pending = nodes[0].propose(Command::Noop).unwrap();
        let old_commit = nodes[0].state().commit;
        let request1 = nodes[0].request(&1).unwrap();
        let request2 = nodes[0].request(&2).unwrap();
        let answer1 = nodes[1].receive(&0, &request1).unwrap();
        let answer2 = nodes[2].receive(&0, &request2).unwrap();
        nodes[0].clock = Some(ms(500));
        let before = nodes[0].journal.records.len();
        let rejected = nodes[0].propose(Command::Noop);
        let after = nodes[0].journal.records.len();
        // A replication majority is insufficient when both responses are old.
        nodes[0].response_sent(&1, &request1, &answer1, ms(100)).unwrap();
        nodes[0].response_sent(&2, &request2, &answer2, ms(100)).unwrap();
        let delayed_commit = nodes[0].state().commit;
        let heartbeat1 = nodes[0].request(&1).unwrap();
        let heartbeat2 = nodes[0].request(&2).unwrap();
        let fresh1 = nodes[1].receive(&0, &heartbeat1).unwrap();
        let fresh2 = nodes[2].receive(&0, &heartbeat2).unwrap();
        nodes[0].response_sent(&1, &heartbeat1, &fresh1, ms(490)).unwrap();
        let minority_commit = nodes[0].state().commit;
        nodes[0].response_sent(&2, &heartbeat2, &fresh2, ms(490)).unwrap();
        let renewed_commit = nodes[0].state().commit;
        let renewed_proposal = nodes[0].propose(Command::Noop);
        assert!(rejected.is_err());
        assert_eq!(before, after);
        assert_eq!(delayed_commit, old_commit);
        assert_eq!(minority_commit, old_commit);
        assert_eq!(renewed_commit, pending);
        assert!(renewed_proposal.is_ok());
        // Singleton authority needs no remote heartbeat after the initial interval.
        let mut single = node(0, members(1));
        single.clock = Some(ms(0));
        single.campaign().unwrap();
        single.clock = Some(ms(500));
        let singleton = single.propose(Command::Noop);
        assert!(singleton.is_ok());
    }

    #[test]
    fn writes_commit_with_authority_and_demotion_discards_them() {
        use std::time::Duration;
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        let term = nodes[0].state().term;
        nodes[0].lease = crate::lease::Lease::elected(term, Duration::ZERO);
        nodes[0].lease.acknowledge(&1, term, Duration::from_millis(100));
        let admitted = nodes[0].can_admit(Duration::from_millis(160));
        let after_expiry = nodes[0].can_admit(Duration::from_millis(250));
        nodes[0].clock = Some(Duration::from_millis(160));
        let accepted = nodes[0].propose(Command::Noop);
        let index = accepted.unwrap();
        pair(&mut nodes, 0, 1);
        let committed = nodes[0].state().commit;
        let vote =
            Rpc::Vote { last_scn: Scn::new(0), term: term + 1, last: index, last_term: term };
        let answer = nodes[0].receive(&2, &vote);
        let before = nodes[0].journal.records.len();
        let rejected = nodes[0].propose(Command::Noop);
        let after = nodes[0].journal.records.len();
        assert!(admitted);
        assert!(!after_expiry);
        assert_eq!(committed, index);
        assert!(answer.is_err());
        assert!(rejected.is_err());
        assert_eq!(before, after);
    }

    #[test]
    fn demotion_ends_primary_before_higher_term_or_delayed_work() {
        let leader = || {
            let mut nodes = cluster(3);
            nodes[0].campaign().unwrap();
            pair(&mut nodes, 0, 1);
            pair(&mut nodes, 0, 1);
            nodes.remove(0)
        };
        for trigger in 0..7 {
            let mut node = leader();
            let old_term = node.state().term;
            let before = node.journal.records.len();
            let higher = old_term + 1;
            let result = match trigger {
                0 => node
                    .receive(
                        &1,
                        &Rpc::Vote {
                            last_scn: Scn::new(0),
                            term: higher,
                            last: 1,
                            last_term: old_term,
                        },
                    )
                    .map(|_| ()),
                1 | 2 => node
                    .receive(
                        &1,
                        &Rpc::Append {
                            term: if trigger == 1 { higher } else { old_term },
                            previous: 1,
                            previous_term: old_term,
                            previous_scn: common::scn::Scn::new(1),
                            entries: vec![],
                            commit: 1,
                        },
                    )
                    .map(|_| ()),
                3 | 4 => node
                    .receive(
                        &1,
                        &Rpc::Install {
                            term: if trigger == 3 { higher } else { old_term },
                            snapshot: node.state().capture(),
                        },
                    )
                    .map(|_| ()),
                5 => node.response(
                    &1,
                    &node.request(&1).unwrap(),
                    &Answer { term: higher, success: false, matched: 0 },
                ),
                _ => node.campaign(),
            };
            let stopped = node.role;
            let delayed_proposal = node.propose(Command::Noop);
            let delayed_campaign = node.campaign();
            let delayed_rpc = node.receive(
                &1,
                &Rpc::Vote { last_scn: Scn::new(0), term: higher, last: 1, last_term: old_term },
            );
            let delayed_response = node.response(
                &1,
                &Rpc::Vote { last_scn: Scn::new(0), term: higher, last: 1, last_term: old_term },
                &Answer { term: higher, success: true, matched: 1 },
            );
            let no_request = node.request(&1);
            let after = node.journal.records.len();
            let persisted_term = node.state().term;
            let restored = restart(&node);
            assert!(result.is_err());
            assert_eq!(stopped, Role::Stopped);
            assert!(delayed_proposal.is_err());
            assert!(delayed_campaign.is_err());
            assert!(delayed_rpc.is_err());
            assert!(delayed_response.is_err());
            assert!(no_request.is_none());
            assert_eq!(before, after);
            assert_eq!(persisted_term, old_term);
            assert_eq!(restored.role, Role::Follower);
        }
    }

    #[test]
    fn removing_primary_commits_final_membership_then_ends_process() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        let mut new = members(3);
        new.remove(&0);
        nodes[0]
            .propose(Command::Members(Membership {
                retired: BTreeMap::new(),
                identities: names(),
                old: members(3),
                new: Some(new.clone()),
            }))
            .unwrap();
        for _ in 0..8 {
            for other in 1..3 {
                if nodes[0].role == Role::Stopped {
                    break;
                }
                let request = nodes[0].request(&(other as u64)).unwrap();
                let answer = nodes[other].receive(&0, &request).unwrap();
                let result = nodes[0].response(&(other as u64), &request, &answer);
                if nodes[0].role != Role::Stopped {
                    result.unwrap();
                }
            }
        }
        let stopped = nodes[0].role;
        let committed = nodes[0].state().committed_membership();
        let delayed = nodes[0].propose(Command::Noop);
        assert_eq!(stopped, Role::Stopped);
        assert_eq!(
            committed,
            Membership { retired: BTreeMap::new(), identities: names(), old: new, new: None }
        );
        assert!(delayed.is_err());
    }

    #[test]
    fn votes_survive_restart_and_stale_logs_cannot_win() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        let term = nodes[1].state().term;
        let mut restored = restart(&nodes[1]);
        let vote = Rpc::Vote { last_scn: Scn::new(0), term, last: 0, last_term: 0 };
        let answer = restored.receive(&2, &vote).unwrap();
        assert!(!answer.success);
        exchange(&mut nodes[0], &mut restored);
        let vote = Rpc::Vote { last_scn: Scn::new(0), term: term + 1, last: 0, last_term: 0 };
        let answer = restored.receive(&2, &vote).unwrap();
        assert!(!answer.success);
    }
    #[test]
    fn minority_cannot_commit_and_new_leader_replaces_its_suffix() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 2);
        pair(&mut nodes, 0, 1);
        let committed = nodes[0].state().commit;
        let abandoned = nodes[0]
            .propose(Command::Client(vec![crate::Action::new(common::Op::BytesResp, vec![1; 104])]))
            .unwrap();
        assert!(abandoned > committed);
        assert_eq!(nodes[0].state().commit, committed);
        assert!(nodes[0].state().capture().application.is_empty());
        nodes[1].campaign().unwrap();
        pair(&mut nodes, 1, 2);
        for _ in 0..4 {
            pair(&mut nodes, 1, 2);
            pair(&mut nodes, 1, 0);
        }
        assert_eq!(nodes[0].role, Role::Follower);
        assert_eq!(nodes[0].state().log, nodes[1].state().log);
        assert!(nodes[0].state().capture().application.is_empty());
        assert!(nodes[1].state().commit > abandoned - 1);
    }
    #[test]
    fn old_term_majority_waits_for_current_term_entry() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        let index = nodes[0]
            .propose(Command::Client(vec![crate::Action::new(common::Op::BytesResp, vec![7; 104])]))
            .unwrap();
        // The follower journals the entry, but the leader loses the acknowledgment.
        let request = nodes[0].request(&1).unwrap();
        let _answer = nodes[1].receive(&0, &request).unwrap();
        assert!(nodes[1].state().commit < index);
        nodes[1].campaign().unwrap();
        pair(&mut nodes, 1, 2);
        assert!(nodes[1].state().commit < index);
        for _ in 0..4 {
            pair(&mut nodes, 1, 2);
        }
        assert!(nodes[1].state().commit > index);
        assert_eq!(nodes[1].state().capture().application.len(), 1);
    }
    #[test]
    fn short_heartbeat_never_truncates_a_longer_matching_log() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        nodes[0]
            .propose(Command::Client(vec![crate::Action::new(common::Op::BytesResp, vec![2; 104])]))
            .unwrap();
        pair(&mut nodes, 0, 1);
        let last = nodes[1].state().last();
        let request = Rpc::Append {
            term: nodes[0].state().term,
            previous: 1,
            previous_term: 1,
            previous_scn: common::scn::Scn::new(1),
            entries: vec![],
            commit: last,
        };
        let answer = nodes[1].receive(&0, &request).unwrap();
        assert!(answer.success);
        assert_eq!(nodes[1].state().last(), last);
        assert!(nodes[1].state().commit < last);
    }
    #[test]
    fn failed_disk_never_acknowledges_vote_or_entry() {
        let mut nodes = cluster(3);
        nodes[1].journal.fail_writes();
        let result = nodes[1]
            .receive(&0, &Rpc::Vote { last_scn: Scn::new(0), term: 1, last: 0, last_term: 0 });
        assert!(result.is_err());
        assert_eq!(nodes[1].state().term, 0);
        let result = nodes[1].receive(
            &0,
            &Rpc::Append {
                term: 1,
                previous: 0,
                previous_term: 0,
                previous_scn: common::scn::Scn::new(0),
                entries: vec![],
                commit: 0,
            },
        );
        assert!(result.is_err());
    }
    #[test]
    fn excluded_candidate_cannot_advance_term_or_demote_primary() {
        let mut nodes = cluster(2);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        let term = nodes[0].state().term;
        let vote =
            Rpc::Vote { last_scn: Scn::new(0), term: term + 100, last: 999, last_term: term + 100 };
        let answer = nodes[0].receive(&2, &vote).unwrap();
        let role = nodes[0].role;
        let unchanged = nodes[0].state().term == term;
        assert!(!answer.success);
        assert!(unchanged);
        assert_eq!(role, Role::Leader);
    }
    #[test]
    fn checkpoint_reference_acknowledges_only_after_peer_copy_then_resumes_append() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        nodes[0]
            .propose(Command::Client(vec![crate::Action::new(common::Op::BytesResp, vec![9; 104])]))
            .unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        nodes[1].journal.snapshot(100).unwrap();
        let checkpoint = nodes[1].state().snapshot.clone();
        nodes[0].journal.write(&[Record::Install(checkpoint.clone())]).unwrap();
        nodes[0].next.insert(2, 1);
        let reference = nodes[0].request_reference(&2).unwrap();
        let is_reference = matches!(reference, Rpc::Checkpoint { .. });
        let before = nodes[2].receive(&0, &reference).unwrap();
        let known_leader = nodes[2].leader.clone();
        // The receiver persists a copy obtained from another member, without decrypting it.
        nodes[2].journal.write(&[Record::Install(checkpoint)]).unwrap();
        let after = nodes[2].receive(&0, &reference).unwrap();
        nodes[0].response(&2, &reference, &after).unwrap();
        let next = nodes[0].request_reference(&2).unwrap();
        let resumes_append = matches!(next, Rpc::Append { .. });
        let recovered = nodes[2].state().capture() == nodes[0].state().capture();
        assert!(is_reference);
        assert!(!before.success);
        assert_eq!(known_leader.as_ref(), Some(&0));
        assert!(after.success);
        assert!(resumes_append);
        assert!(recovered);
    }
    #[test]
    fn lagging_follower_installs_snapshot_then_replays_suffix() {
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        nodes[0]
            .propose(Command::Client(vec![crate::Action::new(common::Op::BytesResp, vec![9; 104])]))
            .unwrap();
        pair(&mut nodes, 0, 1);
        nodes[0].journal.snapshot(100).unwrap();
        nodes[0]
            .propose(Command::Client(vec![crate::Action::new(
                common::Op::BytesResp,
                vec![10; 104],
            )]))
            .unwrap();
        pair(&mut nodes, 0, 1);
        for _ in 0..8 {
            pair(&mut nodes, 0, 2);
        }
        assert_eq!(nodes[2].state().capture(), nodes[0].state().capture());
        assert!(nodes[2].state().snapshot.index > 0);
    }
    #[test]
    fn joint_membership_requires_both_majorities() {
        let joint = Membership {
            retired: BTreeMap::new(),
            identities: names(),
            old: members(3),
            new: Some(members(5)),
        };
        let old_only = BTreeSet::from([0, 1]);
        let new_only = BTreeSet::from([2, 3, 4]);
        let both = BTreeSet::from([0, 1, 4]);
        assert!(!joint.quorum(&old_only));
        assert!(!joint.quorum(&new_only));
        assert!(joint.quorum(&both));
        let mut nodes = cluster(3);
        nodes[0].campaign().unwrap();
        pair(&mut nodes, 0, 1);
        pair(&mut nodes, 0, 1);
        let new = BTreeSet::from([0, 2]);
        nodes[0]
            .propose(Command::Members(Membership {
                retired: BTreeMap::new(),
                identities: names(),
                old: members(3),
                new: Some(new.clone()),
            }))
            .unwrap();
        pair(&mut nodes, 0, 1);
        assert!(nodes[0].state().membership().new.is_some());
        for _ in 0..8 {
            pair(&mut nodes, 0, 2);
            pair(&mut nodes, 0, 1);
        }
        assert_eq!(
            nodes[0].state().capture().membership,
            Membership { retired: BTreeMap::new(), identities: names(), old: new, new: None }
        );
    }
    #[test]
    fn delayed_messages_crashes_and_snapshots_preserve_committed_keys() {
        for seed in 1..=4u64 {
            let initial = cluster(5);
            let mut nodes: Vec<Option<TestNode>> = initial.into_iter().map(Some).collect();
            let mut messages: Vec<(usize, usize, Rpc)> = vec![];
            let mut acknowledgments: Vec<(usize, usize, Rpc, Answer)> = vec![];
            let mut known: BTreeMap<u64, Entry> = BTreeMap::new();
            let mut random = seed;
            for step in 0..600 {
                for node in &mut nodes {
                    if node.as_ref().unwrap().role == Role::Stopped {
                        *node = Some(restart(node.as_ref().unwrap()));
                    }
                }
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let a = (random as usize >> 8) % 5;
                let b = (random as usize >> 16) % 5;
                match random % 11 {
                    0 => {
                        let n = nodes[a].as_mut().unwrap();
                        if n.role != Role::Leader {
                            n.campaign().unwrap();
                        }
                    }
                    1 if nodes[a].as_ref().unwrap().role == Role::Leader => {
                        nodes[a]
                            .as_mut()
                            .unwrap()
                            .propose(Command::Client(vec![crate::Action::new(
                                common::Op::BytesResp,
                                vec![(step % 251) as u8; 104],
                            )]))
                            .unwrap();
                    }
                    2 => {
                        let restored = restart(nodes[a].as_ref().unwrap());
                        nodes[a] = Some(restored);
                    }
                    3 => nodes[a].as_mut().unwrap().journal.snapshot(step).unwrap(),
                    4 | 5 if !messages.is_empty() => {
                        let index = (random as usize >> 24) % messages.len();
                        let (from, to, request) = messages.swap_remove(index);
                        let result = nodes[to].as_mut().unwrap().receive(&(from as u64), &request);
                        if nodes[to].as_ref().unwrap().role != Role::Stopped {
                            acknowledgments.push((from, to, request, result.unwrap()));
                        }
                    }
                    6 | 7 if !acknowledgments.is_empty() => {
                        let index = (random as usize >> 24) % acknowledgments.len();
                        let (from, to, request, answer) = acknowledgments.swap_remove(index);
                        let result =
                            nodes[from].as_mut().unwrap().response(&(to as u64), &request, &answer);
                        if nodes[from].as_ref().unwrap().role != Role::Stopped {
                            result.unwrap();
                        }
                    }
                    _ if a != b => {
                        if let Some(request) = nodes[a].as_ref().unwrap().request(&(b as u64)) {
                            messages.push((a, b, request));
                        }
                    }
                    _ => {}
                }
                for node in &nodes {
                    let state = node.as_ref().unwrap().state();
                    let snapshot = state.capture();
                    for key in &snapshot.application {
                        if let Some(previous) = known.insert(key.index, key.clone()) {
                            assert_eq!(&previous, key, "seed {seed}, step {step}");
                        }
                    }
                    for (index, key) in &known {
                        if *index <= state.commit {
                            let actual = snapshot.application.iter().find(|e| e.index == *index);
                            assert_eq!(actual, Some(key), "seed {seed}, step {step}");
                        }
                    }
                }
            }
            // Finish with a connected network so the run also exercises successful commitment.
            messages.clear();
            acknowledgments.clear();
            let mut converged = false;
            for _ in 0..12 {
                for node in &mut nodes {
                    if node.as_ref().unwrap().role == Role::Stopped {
                        *node = Some(restart(node.as_ref().unwrap()));
                    }
                }
                let freshest = (0..5)
                    .max_by_key(|i| {
                        let s = nodes[*i].as_ref().unwrap().state();
                        (s.term_at(s.last()), s.last())
                    })
                    .unwrap();
                if nodes[freshest].as_ref().unwrap().role != Role::Leader {
                    nodes[freshest].as_mut().unwrap().campaign().unwrap();
                }
                for _ in 0..5 {
                    for other in 0..5 {
                        if other == freshest {
                            continue;
                        }
                        let Some(request) =
                            nodes[freshest].as_ref().unwrap().request(&(other as u64))
                        else {
                            break;
                        };
                        let result =
                            nodes[other].as_mut().unwrap().receive(&(freshest as u64), &request);
                        if nodes[other].as_ref().unwrap().role == Role::Stopped {
                            nodes[other] = Some(restart(nodes[other].as_ref().unwrap()));
                            continue;
                        }
                        let answer = result.unwrap();
                        let result = nodes[freshest].as_mut().unwrap().response(
                            &(other as u64),
                            &request,
                            &answer,
                        );
                        if nodes[freshest].as_ref().unwrap().role != Role::Stopped {
                            result.unwrap();
                        }
                    }
                }
                if nodes[freshest].as_ref().unwrap().role == Role::Leader {
                    nodes[freshest]
                        .as_mut()
                        .unwrap()
                        .propose(Command::Client(vec![crate::Action::new(
                            common::Op::BytesResp,
                            vec![77; 104],
                        )]))
                        .unwrap();
                    for other in 0..5 {
                        if other == freshest {
                            continue;
                        }
                        let Some(request) =
                            nodes[freshest].as_ref().unwrap().request(&(other as u64))
                        else {
                            break;
                        };
                        let result =
                            nodes[other].as_mut().unwrap().receive(&(freshest as u64), &request);
                        if nodes[other].as_ref().unwrap().role == Role::Stopped {
                            nodes[other] = Some(restart(nodes[other].as_ref().unwrap()));
                            continue;
                        }
                        let answer = result.unwrap();
                        let result = nodes[freshest].as_mut().unwrap().response(
                            &(other as u64),
                            &request,
                            &answer,
                        );
                        if nodes[freshest].as_ref().unwrap().role != Role::Stopped {
                            result.unwrap();
                        }
                    }
                    let count =
                        nodes[freshest].as_ref().unwrap().state().capture().application.len();
                    assert!(count > 0);
                    converged = true;
                    break;
                }
            }
            assert!(converged, "seed {seed} failed to elect with a connected network");
        }
    }
}

crate::wire_struct!(Member, RaftMember, {identity,process});

crate::wire_struct!(Membership, RaftMembership, {identities,old,new,retired});

crate::wire_struct!(Retirement, RaftRetirement, {previous,old,next,member});

crate::wire_struct!(Snapshot, RaftSnapshot, {ring,index,term,scn,membership,application,checkpoint_start});

crate::wire_struct!(Answer, RaftAnswer, {term,success,matched});

impl crate::codec::Wire for Command {
    fn write(&self, w: &mut common::StructWriter) {
        match self {
            Self::Noop => w.nested(common::Op::RaftCommandNoop, |_| {}),
            Self::Client(value) => w.nested(common::Op::RaftCommandClient, |w| {
                value.write(w);
            }),
            Self::Members(value) => w.nested(common::Op::RaftCommandMembers, |w| {
                value.write(w);
            }),
            Self::CheckpointStart(value) => w.nested(common::Op::RaftCommandCheckpointStart, |w| {
                value.write(w);
            }),
        }
    }
    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        use crate::codec::Wire;
        r.nested_any(|op, r| match op {
            common::Op::RaftCommandNoop => Ok(Self::Noop),
            common::Op::RaftCommandClient => Ok(Self::Client(Wire::read(r)?)),
            common::Op::RaftCommandMembers => Ok(Self::Members(Wire::read(r)?)),
            common::Op::RaftCommandCheckpointStart => Ok(Self::CheckpointStart(Wire::read(r)?)),
            _ => Err(common::Error::new("unexpected Command tag")),
        })
    }
}

impl crate::codec::Wire for Record {
    fn write(&self, w: &mut common::StructWriter) {
        match self {
            Self::Contacts(value) => w.nested(common::Op::RaftRecordContacts, |w| {
                value.write(w);
            }),
            Self::Retired(value) => w.nested(common::Op::RaftRecordRetired, |w| {
                value.write(w);
            }),
            Self::Bootstrap(value) => w.nested(common::Op::RaftRecordBootstrap, |w| {
                value.write(w);
            }),
            Self::Carry { entries, commit } => w.nested(common::Op::RaftRecordCarry, |w| {
                entries.write(w);
                commit.write(w);
            }),
            Self::Hard { term, vote } => w.nested(common::Op::RaftRecordHard, |w| {
                term.write(w);
                vote.write(w);
            }),
            Self::Entries { from, entries } => w.nested(common::Op::RaftRecordEntries, |w| {
                from.write(w);
                entries.write(w);
            }),
            Self::Commit(value) => w.nested(common::Op::RaftRecordCommit, |w| {
                value.write(w);
            }),
            Self::Install(value) => w.nested(common::Op::RaftRecordInstall, |w| {
                value.write(w);
            }),
        }
    }
    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        use crate::codec::Wire;
        r.nested_any(|op, r| match op {
            common::Op::RaftRecordContacts => Ok(Self::Contacts(Wire::read(r)?)),
            common::Op::RaftRecordRetired => Ok(Self::Retired(Wire::read(r)?)),
            common::Op::RaftRecordBootstrap => Ok(Self::Bootstrap(Wire::read(r)?)),
            common::Op::RaftRecordCarry => {
                Ok(Self::Carry { entries: Wire::read(r)?, commit: Wire::read(r)? })
            }
            common::Op::RaftRecordHard => {
                Ok(Self::Hard { term: Wire::read(r)?, vote: Wire::read(r)? })
            }
            common::Op::RaftRecordEntries => {
                Ok(Self::Entries { from: Wire::read(r)?, entries: Wire::read(r)? })
            }
            common::Op::RaftRecordCommit => Ok(Self::Commit(Wire::read(r)?)),
            common::Op::RaftRecordInstall => Ok(Self::Install(Wire::read(r)?)),
            _ => Err(common::Error::new("unexpected Record tag")),
        })
    }
}

impl crate::codec::Wire for Rpc {
    fn write(&self, w: &mut common::StructWriter) {
        match self {
            Self::Vote { term, last, last_term, last_scn } => {
                w.nested(common::Op::RaftRpcVote, |w| {
                    term.write(w);
                    last.write(w);
                    last_term.write(w);
                    last_scn.write(w);
                })
            }
            Self::Append { term, previous, previous_term, previous_scn, entries, commit } => w
                .nested(common::Op::RaftRpcAppend, |w| {
                    term.write(w);
                    previous.write(w);
                    previous_term.write(w);
                    previous_scn.write(w);
                    entries.write(w);
                    commit.write(w);
                }),
            Self::Install { term, snapshot } => w.nested(common::Op::RaftRpcInstall, |w| {
                term.write(w);
                snapshot.write(w);
            }),
            Self::Checkpoint { term, index, snapshot_term, snapshot_scn } => {
                w.nested(common::Op::RaftRpcCheckpoint, |w| {
                    term.write(w);
                    index.write(w);
                    snapshot_term.write(w);
                    snapshot_scn.write(w);
                })
            }
        }
    }
    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        use crate::codec::Wire;
        r.nested_any(|op, r| match op {
            common::Op::RaftRpcVote => Ok(Self::Vote {
                term: Wire::read(r)?,
                last: Wire::read(r)?,
                last_term: Wire::read(r)?,
                last_scn: Wire::read(r)?,
            }),
            common::Op::RaftRpcAppend => Ok(Self::Append {
                term: Wire::read(r)?,
                previous: Wire::read(r)?,
                previous_term: Wire::read(r)?,
                previous_scn: Wire::read(r)?,
                entries: Wire::read(r)?,
                commit: Wire::read(r)?,
            }),
            common::Op::RaftRpcInstall => {
                Ok(Self::Install { term: Wire::read(r)?, snapshot: Wire::read(r)? })
            }
            common::Op::RaftRpcCheckpoint => Ok(Self::Checkpoint {
                term: Wire::read(r)?,
                index: Wire::read(r)?,
                snapshot_term: Wire::read(r)?,
                snapshot_scn: Wire::read(r)?,
            }),
            _ => Err(common::Error::new("unexpected Rpc tag")),
        })
    }
}

impl crate::codec::Wire for State {
    fn write(&self, w: &mut common::StructWriter) {
        w.nested(common::Op::RaftState, |w| {
            self.ring.write(w);
            self.founders.write(w);
            self.contacts.write(w);
            self.retirements.write(w);
            self.term.write(w);
            self.vote.write(w);
            self.snapshot.write(w);
            self.log.write(w);
            self.commit.write(w);
            self.max_scn.write(w);
            self.generation.write(w);
        });
    }
    fn read(r: &mut common::StructReader<'_>) -> common::Result<Self> {
        use crate::codec::Wire;
        r.nested(common::Op::RaftState, "state", |r| {
            Ok(Self {
                ignore_ring_identity: false,
                ring: Wire::read(r)?,
                founders: Wire::read(r)?,
                contacts: Wire::read(r)?,
                retirements: Wire::read(r)?,
                term: Wire::read(r)?,
                vote: Wire::read(r)?,
                snapshot: Wire::read(r)?,
                log: Wire::read(r)?,
                commit: Wire::read(r)?,
                max_scn: Wire::read(r)?,
                generation: Wire::read(r)?,
            })
        })
    }
}
