//! One ordered journal for Raft entries, terms/votes, commit markers and installs.
//! Key commands are already encrypted; consensus metadata remains readable while locked.
use crate::raft::{INCARNATION, Member, MemberId, Membership, Record, Retirement, State};
use crate::random_master_key;
use common::{Context, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"DLAWKR03";
const CURRENT: &str = "current.log";
const HEADER: u64 = 8 + 128 + 16;
fn journal_header(ring: &str, start: u64) -> Vec<u8> {
    [MAGIC.as_slice(), ring.as_bytes(), &start.to_le_bytes(), &(!start).to_le_bytes()].concat()
}
fn check_journal_header(reader: &mut impl Read, ring: &str, ignore: bool) -> Result<u64> {
    let mut header = [0; HEADER as usize];
    reader.read_exact(&mut header)?;
    if &header[..8] != MAGIC || (!ignore && &header[8..136] != ring.as_bytes()) {
        return Err(Error::new("journal ring identity mismatch"));
    }
    let start = u64::from_le_bytes(header[136..144].try_into().unwrap());
    let inverse = u64::from_le_bytes(header[144..152].try_into().unwrap());
    if start == 0 || inverse != !start {
        return Err(Error::new("invalid journal starting sequence"));
    }
    Ok(start)
}
const LIMIT: usize = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct Batch {
    sequence: u64,
    records: Vec<Record>,
}
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    sequence: u64,
    second: u64,
    state: State,
}

/// Directory-local identity; never imported from another member's history.
/// This inode is also the lifetime lock and must not be replaced while in use.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalIdentity {
    pub ring: String,
    pub process: String,
    pub current: common::ring::MemberIdentity,
    /// Kept until membership no longer needs their state-loss responses.
    pub retired: Vec<common::ring::MemberIdentity>,
    pub slot: Option<MemberId>,
    membership: Option<Membership>,
    founders: Option<Membership>,
    initialized: bool,
    pending: Option<Retirement>,
    recovery: Option<RecoveryPlan>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RecoveryPlan {
    archive: String,
    checkpoint: String,
    files: Vec<String>,
}

pub struct Journal {
    pub identity: LocalIdentity,
    pub state: State,
    directory: PathBuf,
    bootstrap_pending: bool,
    _ownership: File,
    file: File,
    current_start: u64,
    sequence: u64,
    change_sequence: u64,
    snapshot_sequence: u64,
    snapshot_second: u64,
    snapshot_bytes: u64,
    log_bytes: u64,
    retain: usize,
    pub failed: bool,
}
impl crate::raft::Storage for Journal {
    fn state(&self) -> &State {
        &self.state
    }
    fn write(&mut self, records: &[Record]) -> Result<()> {
        Journal::write(self, records)
    }
    fn pending_replacement(&self) -> Option<Retirement> {
        self.identity.pending.clone()
    }
}
impl Journal {
    pub fn open(directory: &Path, initial: State, retain: usize, member: &str) -> Result<Self> {
        let ignore = initial.ignore_ring_identity;
        if retain == 0 {
            return Err(Error::new("snapshot_retain must be positive"));
        }
        std::fs::create_dir_all(directory)?;
        sync_directory(directory.parent().unwrap_or(Path::new(".")))?;
        let mut ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("identity"))?;
        ownership.try_lock().context("locking KS persistence directory")?;
        // Observe creation intent once at startup, never by polling a running store.
        let bootstrap_pending = directory.join("need-bootstrap").try_exists()?;
        let saved_identity = read_identity(&mut ownership)?;
        let snapshots = numbered(directory, "snapshot")?;
        let segments = numbered(directory, "journal")?;
        let current = directory.join(CURRENT);
        let current_exists = current.try_exists()?;
        let mut local: LocalIdentity = if saved_identity.is_none() {
            if !snapshots.is_empty() || !segments.is_empty() || current_exists {
                return Err(Error::new("missing KS identity beside persistence files"));
            }
            let current = common::ring::MemberIdentity::generate()?;
            let local = LocalIdentity {
                ring: initial.ring.clone(),
                process: member.into(),
                current,
                retired: vec![],
                slot: None,
                membership: None,
                founders: None,
                initialized: false,
                pending: None,
                recovery: None,
            };
            append_identity(&mut ownership, &local)?;
            sync_directory(directory)?;
            local
        } else {
            let local = saved_identity.ok_or_else(|| Error::new("missing local identity"))?;
            if local.process != member || (!ignore && local.ring != initial.ring) {
                return Err(Error::new("KS persistence ring/member mismatch"));
            }
            if local.retired.contains(&local.current)
                || local.retired.iter().collect::<std::collections::BTreeSet<_>>().len()
                    != local.retired.len()
            {
                return Err(Error::new("invalid retired member identities"));
            }
            local
        };
        if local.recovery.is_some() {
            finish_recovery(directory, &mut ownership, &mut local)?;
            drop(ownership);
            return Self::open(directory, initial, retain, member);
        }
        let (mut state, mut sequence, second, snapshot_bytes) = if let Some((seq, path)) =
            snapshots.last()
        {
            let bytes = std::fs::read(path)?;
            let checkpoint: Checkpoint = decode(&bytes)?;
            if checkpoint.sequence != *seq || (!ignore && checkpoint.state.ring != initial.ring) {
                return Err(Error::new("KS snapshot identity mismatch"));
            }
            (checkpoint.state, *seq, checkpoint.second, bytes.len() as u64)
        } else {
            if local.initialized {
                let recovered = initial.clone();
                prepare_recovery(directory, &mut ownership, &mut local, recovered)?;
                drop(ownership);
                return Self::open(directory, initial, retain, member);
            }
            // Initial publication can leave only an empty current header. It cannot
            // have acknowledged votes or entries before snapshot zero exists.
            if !segments.is_empty()
                || (current_exists && std::fs::read(&current)? != journal_header(&initial.ring, 1))
            {
                return Err(Error::new("KS snapshot missing beside journal"));
            }
            let checkpoint = Checkpoint {
                sequence: 0,
                second: common::time::TimeSec::now().number(),
                state: initial.clone(),
            };
            let bytes = encode(&checkpoint)?;
            if !current_exists {
                publish(&current, &journal_header(&initial.ring, 1))?;
            }
            publish(&named(directory, "snapshot", 0), &bytes)?;
            (initial.clone(), 0, checkpoint.second, bytes.len() as u64)
        };
        state.ignore_ring_identity = ignore;
        state.check_ring(&state.snapshot)?;
        state.validate()?;
        let snapshot_sequence = sequence;
        let current_exists = current.try_exists()?;
        let mut lost = !current_exists;
        let current_start = if current_exists {
            check_journal_header(&mut File::open(&current)?, &state.ring, ignore)?
        } else {
            u64::MAX
        };
        if current_start <= snapshot_sequence {
            return Err(Error::new("current KS journal predates snapshot"));
        }
        let mut segments: Vec<_> = numbered(directory, "journal")?
            .into_iter()
            .filter(|(seq, _)| *seq > snapshot_sequence)
            .collect();
        if segments.iter().any(|(start, _)| *start >= current_start) {
            return Err(Error::new("numbered KS journal overlaps current journal"));
        }
        if current_exists {
            segments.push((current_start, current.clone()));
        }
        // Even history beyond a hole must belong to this ring before recovery
        // is allowed to retire an identity or move any files.
        for (start, path) in &segments {
            let header_start = check_journal_header(&mut File::open(path)?, &state.ring, ignore)?;
            if header_start != *start {
                return Err(Error::new("journal filename/header sequence mismatch"));
            }
        }
        let mut active = None;
        let mut log_bytes = 0;
        'segments: for (start, path) in &segments {
            if *start
                != sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?
            {
                lost = true;
                break;
            }
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let size = file.metadata()?.len();
            let header_start = check_journal_header(&mut file, &state.ring, ignore)?;
            if header_start != *start {
                return Err(Error::new("journal filename/header sequence mismatch"));
            }
            let mut offset = HEADER;
            while offset < size {
                let remaining = size - offset;
                if remaining < 16 {
                    if path != &current {
                        return Err(Error::new("partial closed KS journal"));
                    }
                    break;
                }
                let mut length = [0; 8];
                file.read_exact(&mut length)?;
                let length = u64::from_le_bytes(length);
                let mut inverse = [0; 8];
                file.read_exact(&mut inverse)?;
                if u64::from_le_bytes(inverse) != !length {
                    return Err(Error::new("corrupt KS journal frame length"));
                }
                if length > LIMIT as u64 || length < 32 {
                    return Err(Error::new("invalid KS journal frame length"));
                }
                if remaining < 16 + length {
                    if path != &current {
                        return Err(Error::new("partial closed KS journal frame"));
                    }
                    break;
                }
                let mut bytes = vec![0; length as usize];
                file.read_exact(&mut bytes)?;
                let batch: Batch = decode(&bytes)?;
                if batch.sequence != sequence + 1 {
                    lost = true;
                    break 'segments;
                }
                for record in batch.records {
                    state.apply(&record)?;
                }
                sequence = batch.sequence;
                offset += 16 + length;
            }
            if path == &current {
                file.set_len(offset)?;
                file.sync_all()?;
                file.seek(SeekFrom::End(0))?;
                active = Some(file);
            }
            log_bytes += offset;
        }
        if lost {
            prepare_recovery(directory, &mut ownership, &mut local, state)?;
            drop(ownership);
            return Self::open(directory, initial, retain, member);
        }
        let before = local.clone();
        local.initialized = true;
        let membership = state.membership();
        local.slot = membership.slot(&local.current).or(local.slot);
        if local.slot.is_some_and(|id| membership.members().contains(&id))
            && local.pending.as_ref().is_none_or(|r| !membership.members().contains(&r.previous))
        {
            local.pending = None;
        }
        local.membership = Some(membership);
        local.founders = state.founders.clone();
        if local != before {
            append_identity(&mut ownership, &local)?;
        }
        let result = Self {
            identity: local,
            state,
            directory: directory.into(),
            bootstrap_pending,
            _ownership: ownership,
            file: active.ok_or_else(|| Error::new("no active KS journal"))?,
            current_start,
            sequence,
            change_sequence: sequence,
            snapshot_sequence,
            snapshot_second: second,
            snapshot_bytes,
            log_bytes,
            retain,
            failed: false,
        };
        result.prune()?;
        Ok(result)
    }
    pub fn needs_bootstrap(&self) -> bool {
        self.bootstrap_pending
    }
    pub fn finish_bootstrap(&mut self) -> Result<()> {
        std::fs::remove_file(self.directory.join("need-bootstrap"))?;
        sync_directory(&self.directory)?;
        self.bootstrap_pending = false;
        Ok(())
    }
    pub fn write(&mut self, records: &[Record]) -> Result<()> {
        if self.failed {
            return Err(Error::new("KS journal fenced after I/O failure; restart required"));
        }
        if records.is_empty() {
            return Ok(());
        }
        // Validate the entire batch before writing; after sync its state is published atomically.
        let mut state = self.state.clone();
        for record in records {
            state.apply(record)?;
        }
        let sequence =
            self.sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        let frame = frame(&Batch { sequence, records: records.to_vec() })?;
        if let Err(error) = self.file.write_all(&frame).and_then(|()| self.file.sync_all()) {
            self.failed = true;
            return Err(error.into());
        }
        self.sequence = sequence;
        self.change_sequence = sequence;
        self.state = state;
        self.log_bytes += frame.len() as u64;
        if records.iter().any(|record| matches!(record, Record::Bootstrap(_) | Record::Install(_))
            || matches!(record, Record::Entries { entries, .. } if entries.iter().any(|e| matches!(e.command, crate::raft::Command::Members(_))))) {
            let membership = self.state.membership();
            let slot = membership.slot(&self.identity.current);
            let accepted = slot.is_some_and(|id| membership.members().contains(&id))
                && self.identity.pending.as_ref().is_none_or(|r| !membership.members().contains(&r.previous));
            self.identity.slot = slot.or(self.identity.slot);
            self.identity.membership = Some(membership);
            self.identity.founders = self.state.founders.clone();
            if accepted { self.identity.pending = None; }
            if let Err(error) = append_identity(&mut self._ownership, &self.identity) {
                self.failed = true;
                return Err(error);
            }
        }
        Ok(())
    }
    pub fn snapshot_due(&self, min_bytes: u64, seconds: u64, now: u64) -> bool {
        self.change_sequence > self.snapshot_sequence
            && (self.log_bytes >= min_bytes.max(self.snapshot_bytes)
                || now.saturating_sub(self.snapshot_second) >= seconds)
    }
    pub fn prepare_snapshot(&mut self, now: u64) -> Result<Option<PendingSnapshot>> {
        if self.failed {
            return Err(Error::new("KS journal fenced"));
        }
        if self.change_sequence <= self.snapshot_sequence {
            return Ok(None);
        }
        self.prepare_copy(now, self.state.capture())
    }
    /// Adopt committed application state without importing another member's hard state.
    pub fn prepare_copy(
        &mut self,
        now: u64,
        snapshot: crate::raft::Snapshot,
    ) -> Result<Option<PendingSnapshot>> {
        if self.failed {
            return Err(Error::new("KS journal fenced"));
        }
        snapshot.validate()?;
        self.state.check_ring(&snapshot)?;
        if snapshot.index < self.state.snapshot.index
            || snapshot.index > self.state.commit
            || self.state.term_at(snapshot.index) != Some(snapshot.term)
        {
            return Err(Error::new("checkpoint does not match local committed history"));
        }
        let mut state = self.state.clone();
        state.log.retain(|e| e.index > snapshot.index);
        state.commit = snapshot.index;
        if self.state.ignore_ring_identity {
            state.ring = snapshot.ring.clone();
        }
        state.snapshot = snapshot;
        let sequence = self.sequence;
        let captured_bytes = self.log_bytes;
        let suffix = std::mem::take(&mut state.log);
        let checkpoint = Checkpoint { sequence, second: now, state: state.clone() };
        let bytes = encode(&checkpoint)?;
        let start =
            self.sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        let carried = if suffix.is_empty() {
            vec![]
        } else {
            frame(&Batch {
                sequence: start,
                records: vec![Record::Carry { entries: suffix, commit: self.state.commit }],
            })?
        };
        // Rotation precedes snapshot publication. Old snapshots remain recoverable
        // through the closed journals and current.log, including its carried suffix.
        self.rotate(start, &carried)?;
        if !carried.is_empty() {
            self.sequence = start;
            self.log_bytes += carried.len() as u64;
        }
        Ok(Some(PendingSnapshot {
            path: named(&self.directory, "snapshot", sequence),
            bytes,
            sequence,
            second: now,
            snapshot: state.snapshot,
            log_bytes: captured_bytes,
        }))
    }
    /// Close the old segment before creating its successor. A crash in the gap
    /// deliberately leaves current.log missing, detecting possible voting-state loss.
    fn rotate(&mut self, start: u64, carried: &[u8]) -> Result<()> {
        if start == self.current_start {
            // Retrying a snapshot without intervening writes needs no new segment.
            return Ok(());
        }
        let result = (|| -> Result<File> {
            let current = self.directory.join(CURRENT);
            let closed = named(&self.directory, "journal", self.current_start);
            if closed.try_exists()? {
                return Err(Error::new("closed KS journal already exists"));
            }
            self.file.sync_all()?;
            std::fs::rename(&current, closed)?;
            sync_directory(&self.directory)?;
            let bytes = [journal_header(&self.state.ring, start), carried.to_vec()].concat();
            publish(&current, &bytes)?;
            Ok(OpenOptions::new().read(true).append(true).open(current)?)
        })();
        match result {
            Ok(file) => {
                self.file = file;
                self.current_start = start;
                self.log_bytes += HEADER;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
    pub fn finish_snapshot(&mut self, pending: &PendingSnapshot) -> Result<()> {
        if pending.sequence <= self.snapshot_sequence {
            return Ok(());
        }
        let snapshot = &pending.snapshot;
        // New entries, commits and terms may have arrived while the file was written.
        if snapshot.index >= self.state.snapshot.index
            && self.state.term_at(snapshot.index) == Some(snapshot.term)
        {
            self.state.log.retain(|e| e.index > snapshot.index);
            if self.state.ignore_ring_identity {
                self.state.ring = snapshot.ring.clone();
            }
            self.state.snapshot = snapshot.clone();
        }
        self.snapshot_sequence = pending.sequence;
        self.snapshot_second = pending.second;
        self.snapshot_bytes = pending.bytes.len() as u64;
        self.log_bytes = self.log_bytes.saturating_sub(pending.log_bytes);
        self.prune()
    }
    #[cfg(test)]
    pub fn snapshot(&mut self, now: u64) -> Result<()> {
        if let Some(pending) = self.prepare_snapshot(now)? {
            pending.write()?;
            self.finish_snapshot(&pending)?;
        }
        Ok(())
    }
    fn prune(&self) -> Result<()> {
        let snapshots = numbered(&self.directory, "snapshot")?;
        let remove = snapshots.len().saturating_sub(self.retain);
        let Some((oldest, _)) = snapshots.get(remove) else {
            return Ok(());
        };
        for (_, path) in &snapshots[..remove] {
            std::fs::remove_file(path)?;
        }
        sync_directory(&self.directory)?;
        for (start, path) in numbered(&self.directory, "journal")? {
            if start <= *oldest {
                std::fs::remove_file(path)?;
            }
        }
        sync_directory(&self.directory)
    }
    #[cfg(test)]
    pub fn fail_writes(&mut self) {
        self.file = File::open(self.directory.join("identity")).unwrap();
    }
}
// Identity updates are append-only on the locked inode. Only membership/recovery
// changes write here; votes still require just the existing journal write.
fn append_identity(file: &mut File, local: &LocalIdentity) -> Result<()> {
    let bytes = encode(local)?;
    let length = bytes.len() as u64;
    file.seek(SeekFrom::End(0))?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&(!length).to_le_bytes())?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}
fn read_identity(file: &mut File) -> Result<Option<LocalIdentity>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut offset = 0;
    let mut last = None;
    while bytes.len() - offset >= 16 {
        let length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let inverse = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        if inverse != !length || length > LIMIT as u64 || length < 32 {
            return Err(Error::new("invalid local identity frame"));
        }
        let end = offset + 16 + length as usize;
        if end > bytes.len() {
            break;
        }
        last = Some(decode(&bytes[offset + 16..end])?);
        offset = end;
    }
    if offset != bytes.len() {
        // No process could advertise an update until this whole frame was synced.
        file.set_len(offset as u64)?;
        file.sync_all()?;
    }
    Ok(last)
}
fn prepare_recovery(
    directory: &Path,
    ownership: &mut File,
    local: &mut LocalIdentity,
    mut state: State,
) -> Result<()> {
    let current = common::ring::MemberIdentity::generate()?;
    let membership = local
        .membership
        .clone()
        .filter(|m| !m.old.is_empty())
        .unwrap_or_else(|| state.membership());
    let slot = local.slot.or_else(|| membership.slot(&local.current));
    let pending = if let Some(previous) = slot {
        let next = previous
            .checked_add(INCARNATION)
            .filter(|id| *id != crate::raft::UNASSIGNED)
            .ok_or_else(|| Error::new("member incarnations exhausted"))?;
        Some(Retirement {
            previous,
            old: Member { identity: local.current.clone(), process: local.process.clone() },
            next,
            member: Member { identity: current.clone(), process: local.process.clone() },
        })
    } else {
        None
    };
    // Keep the last verified committed prefix. Never apply records across a hole.
    let mut snapshot = state.capture();
    snapshot.membership = membership;
    state.snapshot = snapshot;
    state.log.clear();
    state.vote = None;
    state.founders = state.founders.or_else(|| local.founders.clone());
    if let Some(retired) = &pending {
        state.apply(&Record::Retired(retired.clone()))?;
    }
    state.validate()?;
    let checkpoint_name = format!("recovery-{current}.bin");
    let checkpoint =
        Checkpoint { sequence: 0, second: common::time::TimeSec::now().number(), state };
    publish(&directory.join(&checkpoint_name), &encode(&checkpoint)?)?;
    let mut files: Vec<String> = numbered(directory, "snapshot")?
        .into_iter()
        .chain(numbered(directory, "journal")?)
        .map(|(_, path)| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    if directory.join(CURRENT).try_exists()? {
        files.push(CURRENT.into());
    }
    local.retired.push(local.current.clone());
    local.current = current.clone();
    local.slot = pending.as_ref().map(|r| r.next);
    local.pending = pending;
    local.recovery = Some(RecoveryPlan {
        archive: format!("retired-{current}"),
        checkpoint: checkpoint_name,
        files,
    });
    // Retirement is durable before moving history or advertising the replacement.
    append_identity(ownership, local)?;
    finish_recovery(directory, ownership, local)
}
fn finish_recovery(
    directory: &Path,
    ownership: &mut File,
    local: &mut LocalIdentity,
) -> Result<()> {
    let plan = local.recovery.clone().ok_or_else(|| Error::new("missing recovery plan"))?;
    let bytes = std::fs::read(directory.join(&plan.checkpoint))?;
    let checkpoint: Checkpoint = decode(&bytes)?;
    let archive = directory.join(&plan.archive);
    std::fs::create_dir_all(&archive)?;
    sync_directory(directory)?;
    for name in &plan.files {
        let destination = archive.join(name);
        if !destination.try_exists()? {
            std::fs::rename(directory.join(name), destination)?;
        }
    }
    sync_directory(&archive)?;
    sync_directory(directory)?;
    publish(&directory.join(CURRENT), &journal_header(&checkpoint.state.ring, 1))?;
    publish(&named(directory, "snapshot", 0), &bytes)?;
    local.initialized = true;
    local.recovery = None;
    append_identity(ownership, local)?;
    Ok(())
}

pub struct PendingSnapshot {
    path: PathBuf,
    bytes: Vec<u8>,
    sequence: u64,
    second: u64,
    snapshot: crate::raft::Snapshot,
    log_bytes: u64,
}
impl PendingSnapshot {
    pub fn write(&self) -> Result<()> {
        publish(&self.path, &self.bytes)
    }
    pub fn write_cancellable(&self, cancelled: impl Fn() -> bool) -> Result<()> {
        publish_cancellable(&self.path, &self.bytes, cancelled)
    }
}
fn frame(batch: &Batch) -> Result<Vec<u8>> {
    let bytes = encode(batch)?;
    let length = bytes.len() as u64;
    Ok([length.to_le_bytes().as_slice(), (!length).to_le_bytes().as_slice(), &bytes].concat())
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(|e| Error::new(e.to_string()))?;
    bytes.extend(Sha256::digest(&bytes));
    if bytes.len() > LIMIT {
        return Err(Error::new("KS persistence frame too large"));
    }
    Ok(bytes)
}
fn decode<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    let body =
        bytes.len().checked_sub(32).ok_or_else(|| Error::new("short KS persistence frame"))?;
    if Sha256::digest(&bytes[..body]).as_slice() != &bytes[body..] {
        return Err(Error::new("KS persistence checksum mismatch"));
    }
    serde_json::from_slice(&bytes[..body]).map_err(|e| Error::new(e.to_string()))
}
fn named(directory: &Path, kind: &str, sequence: u64) -> PathBuf {
    directory
        .join(format!("{kind}-{sequence:020}.{}", if kind == "snapshot" { "bin" } else { "log" }))
}
fn numbered(directory: &Path, kind: &str) -> Result<Vec<(u64, PathBuf)>> {
    let mut result = vec![];
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let suffix = if kind == "snapshot" { ".bin" } else { ".log" };
        if !name.starts_with(&format!("{kind}-")) || !name.ends_with(suffix) {
            continue;
        }
        let sequence: u64 = name[kind.len() + 1..name.len() - suffix.len()]
            .parse()
            .map_err(|_| Error::new("invalid numbered KS file"))?;
        if entry.path() != named(directory, kind, sequence) || !entry.file_type()?.is_file() {
            return Err(Error::new("invalid numbered KS file"));
        }
        result.push((sequence, entry.path()));
    }
    result.sort_by_key(|v| v.0);
    Ok(result)
}
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}
fn publish(path: &Path, bytes: &[u8]) -> Result<()> {
    publish_cancellable(path, bytes, || false)
}
fn write_checkpoint_chunks(
    writer: &mut impl Write,
    bytes: &[u8],
    cancelled: impl Fn() -> bool,
) -> Result<()> {
    for chunk in bytes.chunks(1024 * 1024) {
        if cancelled() {
            return Err(Error::new("checkpoint cancelled after election victory"));
        }
        writer.write_all(chunk)?;
    }
    if cancelled() {
        return Err(Error::new("checkpoint cancelled after election victory"));
    }
    Ok(())
}
fn publish_cancellable(path: &Path, bytes: &[u8], cancelled: impl Fn() -> bool) -> Result<()> {
    let check = || {
        if cancelled() {
            Err(Error::new("checkpoint cancelled after election victory"))
        } else {
            Ok(())
        }
    };
    let temporary =
        path.with_extension(format!("{}.tmp", common::shamir::hex(&random_master_key()?)));
    let result = (|| -> Result<()> {
        let mut file =
            OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temporary)?;
        write_checkpoint_chunks(&mut file, bytes, &cancelled)?;
        file.sync_all()?;
        check()?;
        std::fs::rename(&temporary, path)?;
        sync_directory(path.parent().unwrap_or(Path::new(".")))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{Command, Entry, Members, Node, Storage};
    #[test]
    fn local_identity_keeps_retired_ids_and_detects_corruption() {
        let local = LocalIdentity {
            ring: "01".repeat(64),
            process: "routing-label".into(),
            current: common::ring::MemberIdentity::try_from("02".repeat(64)).unwrap(),
            retired: vec![common::ring::MemberIdentity::try_from("03".repeat(64)).unwrap()],
            slot: None,
            membership: None,
            founders: None,
            initialized: false,
            pending: None,
            recovery: None,
        };
        let mut bytes = encode(&local).unwrap();
        let restored: LocalIdentity = decode(&bytes).unwrap();
        bytes[40] ^= 1;
        let corrupt = decode::<LocalIdentity>(&bytes);
        assert_eq!(restored, local);
        assert!(corrupt.is_err());
    }

    #[test]
    fn journal_header_preserves_start_and_rejects_corruption() {
        let ring = "01".repeat(64);
        let mut bytes = journal_header(&ring, 12345);
        let start = check_journal_header(&mut bytes.as_slice(), &ring, false).unwrap();
        bytes[140] ^= 1;
        let corrupt = check_journal_header(&mut bytes.as_slice(), &ring, false);
        let zero = check_journal_header(&mut journal_header(&ring, 0).as_slice(), &ring, false);
        assert_eq!(start, 12345);
        assert!(corrupt.is_err());
        assert!(zero.is_err());
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn record_hole_retires_identity_and_primary_list_ends_pending_state() {
        let dir = tempfile::tempdir().unwrap();
        let old = crate::raft::tests::names()[&0].identity.clone();
        provision_test_founder(dir.path(), &initial().ring, "member-0", old.clone()).unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        for term in 1..=3 {
            journal.write(&[Record::Hard { term, vote: Some(0) }]).unwrap();
        }
        drop(journal);
        let path = dir.path().join(CURRENT);
        let bytes = std::fs::read(&path).unwrap();
        let frame_end = |offset: usize| {
            offset + 16 + u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize
        };
        let first_end = frame_end(HEADER as usize);
        let second_end = frame_end(first_end);
        std::fs::write(&path, [&bytes[..first_end], &bytes[second_end..]].concat()).unwrap();
        let mut recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        let retired = recovered.pending_replacement().unwrap();
        assert_eq!(retired.previous, 0);
        assert_eq!(retired.next, INCARNATION);
        assert_eq!(retired.old.identity, old);
        assert_eq!(recovered.state.term, 1); // Never replayed term 3 across the missing record.
        assert_eq!(recovered.state.vote, None);
        let mut membership = recovered.state.membership();
        membership.old = Members::from([retired.next]);
        // Receipt of the primary's final list clears pending state, even before
        // a later commit marker is observed, as required by the admission protocol.
        recovered
            .write(&[Record::Entries {
                from: 1,
                entries: vec![Entry { index: 1, term: 1, command: Command::Members(membership) }],
            }])
            .unwrap();
        assert!(recovered.pending_replacement().is_none());
        let current = recovered.identity.current.clone();
        drop(recovered);
        let restarted = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(restarted.identity.current, current);
        assert!(restarted.pending_replacement().is_none());
        assert_eq!(restarted.identity.retired, vec![old]);
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn interrupted_recovery_resumes_the_recorded_identity() {
        let dir = tempfile::tempdir().unwrap();
        let old = crate::raft::tests::names()[&0].identity.clone();
        provision_test_founder(dir.path(), &initial().ring, "member-0", old.clone()).unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal.write(&[Record::Hard { term: 2, vote: Some(0) }]).unwrap();
        let mut local = journal.identity.clone();
        drop(journal);
        let new = common::ring::MemberIdentity::generate().unwrap();
        let retirement = Retirement {
            previous: 0,
            old: Member { identity: old.clone(), process: "member-0".into() },
            next: INCARNATION,
            member: Member { identity: new.clone(), process: "member-0".into() },
        };
        let mut state = initial();
        state.apply(&Record::Retired(retirement.clone())).unwrap();
        let checkpoint = Checkpoint { sequence: 0, second: 1, state };
        publish(&dir.path().join("recovery-test.bin"), &encode(&checkpoint).unwrap()).unwrap();
        local.retired.push(old);
        local.current = new.clone();
        local.slot = Some(INCARNATION);
        local.pending = Some(retirement);
        local.recovery = Some(RecoveryPlan {
            archive: "retired-test".into(),
            checkpoint: "recovery-test.bin".into(),
            files: vec![CURRENT.into(), "snapshot-00000000000000000000.bin".into()],
        });
        let mut file =
            OpenOptions::new().read(true).write(true).open(dir.path().join("identity")).unwrap();
        append_identity(&mut file, &local).unwrap();
        drop(file);
        std::fs::create_dir(dir.path().join("retired-test")).unwrap();
        std::fs::rename(dir.path().join(CURRENT), dir.path().join("retired-test").join(CURRENT))
            .unwrap();
        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(recovered.identity.current, new);
        assert_eq!(recovered.identity.retired.len(), 1);
        assert!(recovered.identity.recovery.is_none());
        assert!(recovered.pending_replacement().is_some());
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn rotation_preserves_old_bytes_and_recovers_empty_current_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: Some(0) }]).unwrap();
        let old = std::fs::read(dir.path().join(CURRENT)).unwrap();
        let pending = journal.prepare_snapshot(10).unwrap().unwrap();
        let closed = std::fs::read(named(dir.path(), "journal", 1)).unwrap();
        let current = std::fs::read(dir.path().join(CURRENT)).unwrap();
        assert_eq!(old, closed);
        assert_eq!(current, journal_header(&initial().ring, 2));
        // A retry before publication should reuse the empty current file.
        journal.prepare_snapshot(11).unwrap().unwrap();
        assert_eq!(numbered(dir.path(), "journal").unwrap().len(), 1);
        pending.write().unwrap();
        journal.finish_snapshot(&pending).unwrap();
        drop(journal);
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert_eq!(journal.current_start, 2);
        journal.write(&[Record::Hard { term: 2, vote: Some(0) }]).unwrap();
        journal.snapshot(12).unwrap();
        let second_closed = named(dir.path(), "journal", 2).is_file();
        drop(journal);
        let recovered = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert!(second_closed);
        assert_eq!(recovered.state.term, 2);
        assert_eq!(recovered.current_start, 3);
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn missing_current_or_rotation_gap_never_resumes_an_old_journal() {
        for rename in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
            journal.write(&[Record::Hard { term: 1, vote: Some(0) }]).unwrap();
            journal.snapshot(10).unwrap();
            journal.write(&[Record::Hard { term: 2, vote: Some(0) }]).unwrap();
            let start = journal.current_start;
            drop(journal);
            if rename {
                std::fs::rename(dir.path().join(CURRENT), named(dir.path(), "journal", start))
                    .unwrap();
            } else {
                std::fs::remove_file(dir.path().join(CURRENT)).unwrap();
            }
            let recovered = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
            assert_eq!(recovered.identity.retired.len(), 1);
            assert!(dir.path().join(CURRENT).exists());
            let replacement = recovered.identity.current.clone();
            drop(recovered);
            let restarted = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
            assert_eq!(restarted.identity.current, replacement);
        }
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn directory_lock_and_member_identity_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        let id = journal.identity.current.clone();
        let absent = journal.state.membership().slot(&id);
        let competing = Journal::open(dir.path(), initial(), 3, "member-0");
        assert!(absent.is_none());
        assert!(competing.is_err());
        std::fs::write(dir.path().join("need-bootstrap"), []).unwrap();
        assert!(!journal.needs_bootstrap());
        drop(journal);
        let restarted = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert!(restarted.needs_bootstrap());
        assert_eq!(restarted.identity.current, id);
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn bootstrap_intent_and_chosen_founders_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("need-bootstrap"), []).unwrap();
        let empty =
            State::initial("01".repeat(64), Default::default(), Default::default()).unwrap();
        let mut journal = Journal::open(dir.path(), empty.clone(), 3, "member-0").unwrap();
        let id = journal.identity.current.clone();
        let membership = crate::raft::Membership {
            retired: Default::default(),
            identities: std::collections::BTreeMap::from([(
                0,
                crate::raft::Member { identity: id.clone(), process: "member-0".into() },
            )]),
            old: std::collections::BTreeSet::from([0]),
            new: None,
        };
        journal.write(&[Record::Bootstrap(membership.clone())]).unwrap();
        drop(journal);
        let mut restarted = Journal::open(dir.path(), empty, 3, "member-0").unwrap();
        assert!(restarted.needs_bootstrap());
        assert_eq!(restarted.identity.current, id);
        assert_eq!(restarted.state.founders, Some(membership));
        restarted.finish_bootstrap().unwrap();
        assert!(!restarted.needs_bootstrap());
    }

    fn initial() -> State {
        State::initial("01".repeat(64), crate::raft::tests::members(1), crate::raft::tests::names())
            .unwrap()
    }
    #[test]
    fn journal_header_requires_expected_ring_even_for_empty_journals() {
        let ring = "01".repeat(64);
        let other = "02".repeat(64);
        let bytes = journal_header(&ring, 1);
        let accepted = check_journal_header(&mut bytes.as_slice(), &ring, false).is_ok();
        let rejected = check_journal_header(&mut bytes.as_slice(), &other, false).is_err();
        let partial = check_journal_header(&mut &bytes[..bytes.len() - 1], &ring, false).is_err();
        let debug = check_journal_header(&mut bytes.as_slice(), &other, true).is_ok();
        assert!(accepted && rejected && partial && debug);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn foreign_files_with_matching_member_and_positions_are_rejected() {
        for kind in ["journal", "snapshot"] {
            let local = tempfile::tempdir().unwrap();
            let foreign = tempfile::tempdir().unwrap();
            let mut other = initial();
            other.ring = "02".repeat(64);
            other.snapshot.ring = other.ring.clone();
            drop(Journal::open(local.path(), initial(), 3, "KS0").unwrap());
            drop(Journal::open(foreign.path(), other, 3, "KS0").unwrap());
            let index = if kind == "journal" { 1 } else { 0 };
            let path = |dir: &Path| {
                if kind == "journal" { dir.join(CURRENT) } else { named(dir, kind, index) }
            };
            std::fs::copy(path(foreign.path()), path(local.path())).unwrap();
            let rejected = Journal::open(local.path(), initial(), 3, "KS0").is_err();
            assert!(rejected, "foreign {kind}");
            let mut debug = initial();
            debug.ignore_ring_identity = true;
            let recovered = Journal::open(local.path(), debug, 3, "KS0").unwrap();
            assert!(recovered.state.ignore_ring_identity);
            drop(recovered);
            // The override comes from the process, never the copied file.
            let still_rejected = Journal::open(local.path(), initial(), 3, "KS0").is_err();
            assert!(still_rejected);
        }
    }
    #[test]
    fn checkpoint_writes_continue_as_candidate_but_stop_after_winning() {
        use crate::raft::Role;
        use std::cell::Cell;
        struct Writer<'a> {
            role: &'a Cell<Role>,
            next: Role,
            count: usize,
        }
        impl Write for Writer<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.count += bytes.len();
                self.role.set(self.next);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = vec![0; 2 * 1024 * 1024];
        let role = Cell::new(Role::Follower);
        let mut writer = Writer { role: &role, next: Role::Candidate, count: 0 };
        let campaigning = write_checkpoint_chunks(&mut writer, &bytes, || {
            crate::runtime::cancel_checkpoint_after_election(role.get(), 3)
        })
        .is_ok();
        let completed = writer.count;
        writer.next = Role::Leader;
        writer.count = 0;
        let elected = write_checkpoint_chunks(&mut writer, &bytes, || {
            crate::runtime::cancel_checkpoint_after_election(role.get(), 3)
        })
        .is_err();
        let stopped = writer.count;
        assert!(campaigning);
        assert_eq!(completed, bytes.len());
        assert!(elected);
        assert_eq!(stopped, 1024 * 1024);
    }
    #[test]
    fn carried_suffix_replays_with_or_without_checkpoint_without_regressing_hard_state() {
        let mut original = initial();
        original.apply(&Record::Hard { term: 2, vote: Some(0) }).unwrap();
        let entries: Vec<_> =
            (1..=4).map(|index| Entry { index, term: 2, command: Command::Noop }).collect();
        original.apply(&Record::Entries { from: 1, entries }).unwrap();
        original.apply(&Record::Commit(1)).unwrap();
        let checkpoint = original.capture();
        original.apply(&Record::Commit(3)).unwrap();
        let carry = Record::Carry {
            entries: original.log.iter().filter(|e| e.index > 1).cloned().collect(),
            commit: 3,
        };
        let mut published = original.clone();
        published.snapshot = checkpoint;
        published.log.clear();
        published.commit = 1;
        published.apply(&carry).unwrap();
        let before = original.clone();
        original.apply(&carry).unwrap();
        let same_log = original.log == before.log;
        let recovered_suffix = published.log == before.log[1..];
        let committed = published.commit;
        let term = published.term;
        let vote = published.vote.clone();
        let mut bad = carry.clone();
        if let Record::Carry { entries, .. } = &mut bad {
            entries[0].term = 1;
        }
        let rejected = original.apply(&bad).is_err();
        assert!(same_log);
        assert!(recovered_suffix);
        assert_eq!(committed, 3);
        assert_eq!(term, 2);
        assert_eq!(vote.as_ref(), Some(&0));
        assert!(rejected);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn copying_older_checkpoint_keeps_committed_and_uncommitted_suffix_across_restart() {
        for publish_snapshot in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
            let entries =
                (1..=4).map(|index| Entry { index, term: 2, command: Command::Noop }).collect();
            journal
                .write(&[
                    Record::Hard { term: 2, vote: Some(0) },
                    Record::Entries { from: 1, entries },
                    Record::Commit(1),
                ])
                .unwrap();
            let checkpoint = journal.state.capture();
            journal.write(&[Record::Commit(3)]).unwrap();
            let pending = journal.prepare_copy(100, checkpoint).unwrap().unwrap();
            if publish_snapshot {
                pending.write().unwrap();
                journal.finish_snapshot(&pending).unwrap();
            }
            drop(journal);
            let recovered = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
            let positions = (recovered.state.commit, recovered.state.last(), recovered.state.term);
            let vote = recovered.state.vote.clone();
            assert_eq!(positions, (3, 4, 2));
            assert_eq!(vote.as_ref(), Some(&0));
        }
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn replay_restores_vote_uncommitted_suffix_and_commit_markers() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 3, vote: Some(0) },
                Record::Entries {
                    from: 1,
                    entries: vec![Entry { index: 1, term: 3, command: Command::Noop }],
                },
            ])
            .unwrap();
        drop(journal);
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert_eq!(journal.state.term, 3);
        assert_eq!(journal.state.vote.as_ref(), Some(&0));
        assert_eq!(journal.state.commit, 0);
        assert_eq!(journal.state.last(), 1);
        journal.write(&[Record::Commit(1)]).unwrap();
        drop(journal);
        let journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert_eq!(journal.state.commit, 1);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn every_partial_active_frame_is_discarded_but_corruption_is_rejected() {
        let source = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(source.path(), initial(), 3, "KS0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: Some(0) }]).unwrap();
        drop(journal);
        let frame = std::fs::read(source.path().join(CURRENT)).unwrap();
        for count in HEADER as usize..frame.len() {
            let dir = tempfile::tempdir().unwrap();
            for name in ["identity", "snapshot-00000000000000000000.bin"] {
                std::fs::copy(source.path().join(name), dir.path().join(name)).unwrap();
            }
            let path = dir.path().join(CURRENT);
            std::fs::write(&path, &frame[..count]).unwrap();
            let result = Journal::open(dir.path(), initial(), 3, "KS0");
            assert!(result.is_ok(), "partial length {count}");
            let size = std::fs::metadata(path).unwrap().len();
            assert_eq!(size, HEADER);
        }
        let mut bad = frame.clone();
        bad[HEADER as usize + 20] ^= 1;
        std::fs::write(source.path().join(CURRENT), bad).unwrap();
        let result = Journal::open(source.path(), initial(), 3, "KS0");
        assert!(result.is_err());
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshots_retain_complete_replay_suffixes_and_lock_survives_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = Node::new(0, Journal::open(dir.path(), initial(), 3, "KS0").unwrap());
        node.campaign().unwrap();
        for n in 0..5 {
            node.propose(Command::Key(vec![n; 104])).unwrap();
            node.journal.snapshot(n as u64 + 1).unwrap();
        }
        let expected = node.state().capture();
        let snapshots = numbered(dir.path(), "snapshot").unwrap();
        assert_eq!(snapshots.len(), 3);
        let result = Journal::open(dir.path(), initial(), 3, "KS0");
        assert!(result.is_err());
        drop(node);
        for (sequence, snapshot) in snapshots {
            let restore = tempfile::tempdir().unwrap();
            std::fs::copy(dir.path().join("identity"), restore.path().join("identity")).unwrap();
            std::fs::copy(&snapshot, restore.path().join(snapshot.file_name().unwrap())).unwrap();
            for (start, path) in numbered(dir.path(), "journal").unwrap() {
                if start > sequence {
                    std::fs::copy(&path, restore.path().join(path.file_name().unwrap())).unwrap();
                }
            }
            std::fs::copy(dir.path().join(CURRENT), restore.path().join(CURRENT)).unwrap();
            let restored = Journal::open(restore.path(), initial(), 3, "KS0").unwrap();
            assert_eq!(restored.state.capture(), expected);
        }
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshot_preserves_uncommitted_suffix_and_replay_accepts_unpublished_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 1, vote: None },
                Record::Entries {
                    from: 1,
                    entries: vec![Entry { index: 1, term: 1, command: Command::Noop }],
                },
            ])
            .unwrap();
        journal.snapshot(1).unwrap();
        let seq = journal.snapshot_sequence;
        journal.write(&[Record::Commit(1)]).unwrap();
        drop(journal);
        // Simulate snapshot publication never completing, leaving the switched journal.
        std::fs::remove_file(named(dir.path(), "snapshot", seq)).unwrap();
        let restored = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert_eq!(restored.state.commit, 1);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshot_triggers_use_elapsed_time_and_previous_file_size_with_floor() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: None }]).unwrap();
        let time_due = journal.snapshot_due(u64::MAX, 1, journal.snapshot_second + 1);
        let not_yet = journal.snapshot_due(u64::MAX, 1, journal.snapshot_second);
        assert!(time_due);
        assert!(!not_yet);
        journal.snapshot(42).unwrap();
        let clean = journal.snapshot_due(0, 0, 100);
        assert!(!clean);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshot_write_can_overlap_new_journal_records_without_regressing_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 1, vote: None },
                Record::Entries {
                    from: 1,
                    entries: vec![crate::raft::Entry {
                        index: 1,
                        term: 1,
                        command: Command::Key(vec![8; 104]),
                    }],
                },
                Record::Commit(1),
            ])
            .unwrap();
        let pending = journal.prepare_snapshot(10).unwrap().unwrap();
        // A follower can journal entries and a newer term while its snapshot writes.
        journal
            .write(&[
                Record::Entries {
                    from: 2,
                    entries: vec![crate::raft::Entry {
                        index: 2,
                        term: 1,
                        command: Command::Key(vec![9; 104]),
                    }],
                },
                Record::Commit(2),
                Record::Hard { term: 2, vote: None },
            ])
            .unwrap();
        let term = journal.state.term;
        let commit = journal.state.commit;
        pending.write().unwrap();
        journal.finish_snapshot(&pending).unwrap();
        assert_eq!(journal.state.term, term);
        assert_eq!(journal.state.commit, commit);
        let expected = journal.state.capture();
        drop(journal);
        let restored = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert_eq!(restored.state.capture(), expected);
        assert_eq!(restored.state.term, term);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn wrong_member_and_corrupt_length_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: None }]).unwrap();
        drop(journal);
        let wrong_member = Journal::open(dir.path(), initial(), 3, "KS1");
        assert!(wrong_member.is_err());
        let path = dir.path().join(CURRENT);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[1] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let corrupt = Journal::open(dir.path(), initial(), 3, "KS0");
        assert!(corrupt.is_err());
    }
}

#[cfg(test)]
pub fn provision_test_founder(
    directory: &Path,
    ring: &str,
    process: &str,
    current: common::ring::MemberIdentity,
) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join("identity"))?;
    file.try_lock().context("locking founding identity")?;
    append_identity(
        &mut file,
        &LocalIdentity {
            ring: ring.into(),
            process: process.into(),
            current,
            retired: vec![],
            slot: None,
            membership: None,
            founders: None,
            initialized: false,
            pending: None,
            recovery: None,
        },
    )?;
    file.sync_all()?;
    sync_directory(directory)
}
