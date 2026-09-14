//! One ordered journal for Raft entries, terms/votes, commit markers and installs.
//! Key commands are already encrypted; consensus metadata remains readable while locked.
use crate::raft::{Record, State};
use crate::random_master_key;
use common::{Context, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"DLAWKR01";
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

pub struct Journal {
    pub state: State,
    directory: PathBuf,
    _ownership: File,
    file: File,
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
}
impl Journal {
    pub fn open(directory: &Path, initial: State, retain: usize, member: &str) -> Result<Self> {
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
        let mut identity = Vec::new();
        ownership.read_to_end(&mut identity)?;
        let expected =
            [MAGIC.as_slice(), initial.ring.as_bytes(), b"\0", member.as_bytes()].concat();
        let snapshots = numbered(directory, "snapshot")?;
        let segments = numbered(directory, "journal")?;
        if identity.is_empty() {
            if !snapshots.is_empty() || !segments.is_empty() {
                return Err(Error::new("missing KS identity beside persistence files"));
            }
            ownership.write_all(&expected)?;
            ownership.sync_all()?;
            sync_directory(directory)?;
        } else if identity != expected {
            return Err(Error::new(
                "KS persistence ring mismatch or pre-Raft format; migration required",
            ));
        }
        let (mut state, mut sequence, second, snapshot_bytes) = if let Some((seq, path)) =
            snapshots.last()
        {
            let bytes = std::fs::read(path)?;
            let checkpoint: Checkpoint = decode(&bytes)?;
            if checkpoint.sequence != *seq || checkpoint.state.ring != initial.ring {
                return Err(Error::new("KS snapshot identity mismatch"));
            }
            (checkpoint.state, *seq, checkpoint.second, bytes.len() as u64)
        } else {
            if segments.iter().any(|(_, p)| std::fs::metadata(p).map_or(true, |m| m.len() != 0)) {
                return Err(Error::new("KS snapshot missing beside journal"));
            }
            let checkpoint = Checkpoint {
                sequence: 0,
                second: common::time::TimeSec::now().number(),
                state: initial.clone(),
            };
            let bytes = encode(&checkpoint)?;
            publish(&named(directory, "journal", 1), &[])?;
            publish(&named(directory, "snapshot", 0), &bytes)?;
            (initial, 0, checkpoint.second, bytes.len() as u64)
        };
        state.validate()?;
        let snapshot_sequence = sequence;
        let segments: Vec<_> = numbered(directory, "journal")?
            .into_iter()
            .filter(|(seq, _)| *seq > snapshot_sequence)
            .collect();
        if segments.is_empty() {
            return Err(Error::new("missing journal after KS snapshot"));
        }
        let mut active = None;
        let mut log_bytes = 0;
        for (n, (start, path)) in segments.iter().enumerate() {
            if *start
                != sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?
            {
                return Err(Error::new("KS journal sequence gap"));
            }
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let size = file.metadata()?.len();
            let mut offset = 0;
            while offset < size {
                let remaining = size - offset;
                if remaining < 16 {
                    if n + 1 != segments.len() {
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
                    if n + 1 != segments.len() {
                        return Err(Error::new("partial closed KS journal frame"));
                    }
                    break;
                }
                let mut bytes = vec![0; length as usize];
                file.read_exact(&mut bytes)?;
                let batch: Batch = decode(&bytes)?;
                if batch.sequence != sequence + 1 {
                    return Err(Error::new("KS journal record sequence gap"));
                }
                for record in batch.records {
                    state.apply(&record)?;
                }
                sequence = batch.sequence;
                offset += 16 + length;
            }
            if n + 1 == segments.len() {
                file.set_len(offset)?;
                file.sync_all()?;
                file.seek(SeekFrom::End(0))?;
                active = Some(file);
            }
            log_bytes += offset;
        }
        let result = Self {
            state,
            directory: directory.into(),
            _ownership: ownership,
            file: active.ok_or_else(|| Error::new("no active KS journal"))?,
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
        let snapshot = self.state.capture();
        let mut state = self.state.clone();
        state.apply(&Record::Install(snapshot))?;
        let sequence = self.sequence;
        let captured_bytes = self.log_bytes;
        let suffix = std::mem::take(&mut state.log);
        let checkpoint = Checkpoint { sequence, second: now, state: state.clone() };
        let bytes = encode(&checkpoint)?;
        let start =
            self.sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        let path = named(&self.directory, "journal", start);
        let carried = if suffix.is_empty() {
            vec![]
        } else {
            frame(&Batch {
                sequence: start,
                records: vec![Record::Entries { from: suffix[0].index, entries: suffix }],
            })?
        };
        // Rotation precedes snapshot publication. If publication fails, replay the old
        // snapshot and every subsequent segment, including this successor and its carried uncommitted suffix.
        if path.exists() && std::fs::metadata(&path)?.len() != 0 {
            return Err(Error::new("nonempty successor journal"));
        }
        let rotate = (|| -> Result<File> {
            publish(&path, &carried)?;
            Ok(OpenOptions::new().read(true).append(true).open(path)?)
        })();
        match rotate {
            Ok(file) => self.file = file,
            Err(e) => {
                self.failed = true;
                return Err(e);
            }
        }
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
    let temporary =
        path.with_extension(format!("{}.tmp", common::shamir::hex(&random_master_key()?)));
    let result = (|| -> Result<()> {
        let mut file =
            OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
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
    use crate::raft::{Command, Entry, Node};
    fn initial() -> State {
        State::initial("test/KS0".into(), crate::raft::tests::members(1)).unwrap()
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn replay_restores_vote_uncommitted_suffix_and_commit_markers() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 3, vote: Some("KS0".into()) },
                Record::Entries {
                    from: 1,
                    entries: vec![Entry { index: 1, term: 3, command: Command::Noop }],
                },
            ])
            .unwrap();
        drop(journal);
        let mut journal = Journal::open(dir.path(), initial(), 3, "KS0").unwrap();
        assert_eq!(journal.state.term, 3);
        assert_eq!(journal.state.vote.as_deref(), Some("KS0"));
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
        journal.write(&[Record::Hard { term: 1, vote: Some("KS0".into()) }]).unwrap();
        drop(journal);
        let frame = std::fs::read(named(source.path(), "journal", 1)).unwrap();
        for count in 0..frame.len() {
            let dir = tempfile::tempdir().unwrap();
            for name in ["identity", "snapshot-00000000000000000000.bin"] {
                std::fs::copy(source.path().join(name), dir.path().join(name)).unwrap();
            }
            let path = named(dir.path(), "journal", 1);
            std::fs::write(&path, &frame[..count]).unwrap();
            let result = Journal::open(dir.path(), initial(), 3, "KS0");
            assert!(result.is_ok(), "partial length {count}");
            let size = std::fs::metadata(path).unwrap().len();
            assert_eq!(size, 0);
        }
        let mut bad = frame.clone();
        bad[20] ^= 1;
        std::fs::write(named(source.path(), "journal", 1), bad).unwrap();
        let result = Journal::open(source.path(), initial(), 3, "KS0");
        assert!(result.is_err());
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshots_retain_complete_replay_suffixes_and_lock_survives_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut node =
            Node::new("KS0".into(), Journal::open(dir.path(), initial(), 3, "KS0").unwrap());
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
        let path = named(dir.path(), "journal", 1);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[1] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let corrupt = Journal::open(dir.path(), initial(), 3, "KS0");
        assert!(corrupt.is_err());
    }
}
