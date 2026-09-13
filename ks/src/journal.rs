//! Encrypted key snapshots and numbered journal segments for a single KS member.
use super::{MasterEpoch, random_master_key};
use aegis::aegis256::Aegis256;
use common::time::TimeSec;
use common::{Context, Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const ID_MAGIC: &[u8; 8] = b"DLAWKI01";
const MAGIC: &[u8; 8] = b"DLAWKS03";
const HEADER: usize = 80; // version, store identity, starting commit, chain anchor
const ENTRY: usize = 112;
const SNAP_MAGIC: &[u8; 8] = b"DLAWSS01";
const SNAP_HEADER: usize = 112;

pub struct Journal {
    directory: PathBuf,
    _ownership: File,
    file: Option<File>,
    segment_start: u64,
    id: [u8; 32],
    key: Option<[u8; 32]>,
    previous: [u8; 32],
    last: Option<TimeSec>,
    failed: bool,
    commit: u64,
    snapshot_commit: u64,
    snapshot_second: u64,
    snapshot_bytes: u64,
    retain: usize,
}

impl Journal {
    pub fn open(directory: &Path, retain: usize) -> Result<Self> {
        if retain == 0 {
            return Err(Error::new("snapshot_retain must be at least one"));
        }
        if directory.with_extension("journal").is_file()
            || directory.with_extension("snapshot").is_file()
        {
            return Err(Error::new(
                "legacy flat KS files require migration before opening a segmented store",
            ));
        }
        std::fs::create_dir_all(directory).context("creating KS persistence directory")?;
        sync_directory(directory.parent().unwrap_or(Path::new(".")))?;
        let mut ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("identity"))
            .context("opening KS store identity")?;
        ownership.try_lock().context("locking KS store (another KS may be running)")?;
        let mut header = [0; 40];
        if ownership.metadata().context("stat KS identity")?.len() == 0 {
            if !numbered(directory, "snapshot")?.is_empty()
                || !numbered(directory, "journal")?.is_empty()
            {
                return Err(Error::new("missing KS identity beside existing persistence files"));
            }
            header[..8].copy_from_slice(ID_MAGIC);
            header[8..].copy_from_slice(&random_master_key()?);
            ownership.write_all(&header).context("writing KS identity")?;
        } else {
            ownership.read_exact(&mut header).context("reading KS identity")?;
            if &header[..8] != ID_MAGIC {
                return Err(Error::new("invalid KS identity header"));
            }
        }
        ownership.sync_all().context("syncing KS identity")?;
        sync_directory(directory)?;
        let mut id = [0; 32];
        id.copy_from_slice(&header[8..]);
        Ok(Self {
            directory: directory.to_owned(),
            _ownership: ownership,
            file: None,
            segment_start: 1,
            id,
            key: None,
            previous: [0; 32],
            last: None,
            failed: false,
            commit: 0,
            snapshot_commit: 0,
            snapshot_second: 0,
            snapshot_bytes: 0,
            retain,
        })
    }

    fn named(&self, kind: &str, commit: u64) -> PathBuf {
        self.directory.join(filename(kind, commit))
    }

    fn ad(&self, previous: &[u8; 32], commit: u64) -> Vec<u8> {
        [MAGIC.as_slice(), self.id.as_slice(), previous.as_slice(), &commit.to_le_bytes()].concat()
    }

    pub fn replay(&mut self, key: [u8; 32]) -> Result<Vec<MasterEpoch>> {
        if self.failed {
            return Err(Error::new("KS journal requires restart after an I/O failure"));
        }
        let snapshots = numbered(&self.directory, "snapshot")?;
        let Some((named_commit, path)) = snapshots.last() else {
            // An empty first segment can survive interrupted commit-zero initialization.
            let segments = numbered(&self.directory, "journal")?;
            if !segments.is_empty() {
                if segments.len() != 1 || segments[0].0 != 1 {
                    return Err(Error::new("KS snapshots are missing beside journal segments"));
                }
                let mut file = open_segment(&segments[0].1)?;
                self.check_header(&mut file, 1, Some(&[0; 32]))?;
                if file.metadata().context("stat initial segment")?.len() != HEADER as u64 {
                    return Err(Error::new("KS snapshot is missing beside a nonempty journal"));
                }
            }
            self.key = Some(key);
            self.snapshot(&[], 0, TimeSec::now().number())?;
            return Ok(Vec::new());
        };
        let (mut epochs, mut commit, mut previous, second, size) =
            self.load_snapshot(path, *named_commit, &key)?;
        let snapshot_commit = commit;
        let segments: Vec<_> = numbered(&self.directory, "journal")?
            .into_iter()
            .filter(|(start, _)| *start > snapshot_commit)
            .collect();
        let mut active = None;
        let mut active_start = 0;
        for (index, (start, path)) in segments.iter().enumerate() {
            let expected = commit.checked_add(1).ok_or_else(|| Error::new("KS commit overflow"))?;
            if *start != expected {
                return Err(Error::new("KS journal segment gap or overlap"));
            }
            let mut file = open_segment(path)?;
            self.check_header(&mut file, *start, Some(&previous))?;
            let length = file.metadata().context("stat KS journal segment")?.len();
            let entries = (length - HEADER as u64) / ENTRY as u64;
            let tail = (length - HEADER as u64) % ENTRY as u64;
            if tail != 0 && index + 1 != segments.len() {
                return Err(Error::new("partial entry in a closed KS journal segment"));
            }
            for _ in 0..entries {
                let mut record = [0; ENTRY];
                file.read_exact(&mut record).context("reading KS journal entry")?;
                let expected =
                    commit.checked_add(1).ok_or_else(|| Error::new("KS commit overflow"))?;
                let recorded = read_u64(&record[..8]);
                if recorded != expected {
                    return Err(Error::new("KS journal commit sequence is not contiguous"));
                }
                let mut nonce = [0; 32];
                nonce.copy_from_slice(&record[8..40]);
                let mut tag = [0; 32];
                tag.copy_from_slice(&record[80..]);
                let plain = Aegis256::<32>::new(&key, &nonce)
                    .decrypt(&record[40..80], &tag, &self.ad(&previous, recorded))
                    .map_err(|_| Error::new("KS journal authentication failed"))?;
                let epoch = decode_epoch(&plain);
                if epochs.last().is_some_and(|e| e.since >= epoch.since) {
                    return Err(Error::new("KS journal activation times are not increasing"));
                }
                epochs.push(epoch);
                previous = tag;
                commit = recorded;
            }
            if index + 1 == segments.len() {
                // Mutate only after the entire required suffix has authenticated.
                let result = file
                    .set_len(HEADER as u64 + entries * ENTRY as u64)
                    .and_then(|()| file.sync_all())
                    .and_then(|()| file.seek(SeekFrom::End(0)));
                if let Err(e) = result {
                    self.failed = true;
                    return Err(Error::new(format!("repairing KS tail: {e}")));
                }
                active = Some(file);
                active_start = *start;
            }
        }
        self.key = Some(key);
        self.previous = previous;
        self.last = epochs.last().map(|e| e.since);
        self.commit = commit;
        self.snapshot_commit = snapshot_commit;
        self.snapshot_second = second;
        self.snapshot_bytes = size;
        self.file = active;
        self.segment_start = active_start;
        if self.file.is_none() {
            return Err(Error::new("missing journal successor for KS snapshot"));
        }
        self.prune()?;
        Ok(epochs)
    }

    fn check_header(&self, file: &mut File, start: u64, previous: Option<&[u8; 32]>) -> Result<()> {
        let mut header = [0; HEADER];
        file.read_exact(&mut header).context("reading KS segment header")?;
        if &header[..8] != MAGIC
            || header[8..40] != self.id
            || read_u64(&header[40..48]) != start
            || previous.is_some_and(|previous| header[48..80] != *previous)
        {
            return Err(Error::new("invalid KS segment header, filename, or chain anchor"));
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        let start = self.commit.checked_add(1).ok_or_else(|| Error::new("KS commit overflow"))?;
        let path = self.named("journal", start);
        if path.exists() {
            let mut file = open_segment(&path)?;
            self.check_header(&mut file, start, Some(&self.previous))?;
            if file.metadata().context("stat successor segment")?.len() != HEADER as u64 {
                return Err(Error::new("refusing to replace a nonempty successor journal"));
            }
            file.sync_all().context("syncing successor segment")?;
            sync_directory(&self.directory)?;
            self.file = Some(file);
        } else {
            let header = [
                MAGIC.as_slice(),
                self.id.as_slice(),
                &start.to_le_bytes(),
                self.previous.as_slice(),
            ]
            .concat();
            publish(&path, &header)?;
            let mut file = open_segment(&path)?;
            file.seek(SeekFrom::End(0)).context("seeking new segment")?;
            self.file = Some(file);
        }
        self.segment_start = start;
        Ok(())
    }

    fn prune(&self) -> Result<()> {
        let snapshots = numbered(&self.directory, "snapshot")?;
        let remove = snapshots.len().saturating_sub(self.retain);
        let Some((oldest, _)) = snapshots.get(remove) else {
            return Ok(());
        };
        // Remove old snapshots durably before deleting their journal dependencies.
        // Interruption may leave extra files, never a retained snapshot without its suffix.
        for (_, path) in &snapshots[..remove] {
            std::fs::remove_file(path).context("pruning old KS snapshot")?;
        }
        sync_directory(&self.directory)?;
        for (start, path) in numbered(&self.directory, "journal")? {
            if start <= *oldest {
                std::fs::remove_file(path).context("pruning covered KS journal")?;
            }
        }
        sync_directory(&self.directory)
    }
    pub fn append(&mut self, epoch: &MasterEpoch) -> Result<u64> {
        if self.failed {
            return Err(Error::new("KS journal requires restart after an I/O failure"));
        }
        let key = self.key.as_ref().ok_or_else(|| Error::new("KS journal is locked"))?;
        if self.last.is_some_and(|last| epoch.since <= last) {
            return Err(Error::new(
                "new key activation must be later than the last recorded activation",
            ));
        }
        let commit = self.commit.checked_add(1).ok_or_else(|| Error::new("KS commit overflow"))?;
        let nonce = random_master_key()?;
        let body = [epoch.key.as_slice(), &epoch.since.number().to_le_bytes()].concat();
        let (sealed, tag) =
            Aegis256::<32>::new(key, &nonce).encrypt(&body, &self.ad(&self.previous, commit));
        let record =
            [&commit.to_le_bytes(), nonce.as_slice(), sealed.as_slice(), tag.as_slice()].concat();
        let file =
            self.file.as_mut().ok_or_else(|| Error::new("KS journal has no active segment"))?;
        if let Err(e) = file.write_all(&record).and_then(|()| file.sync_all()) {
            self.failed = true;
            return Err(Error::new(format!("persisting KS key: {e}")));
        }
        self.previous = tag;
        self.last = Some(epoch.since);
        self.commit = commit;
        Ok(commit)
    }

    pub fn commit(&self) -> u64 {
        self.commit
    }

    pub fn snapshot_due(&self, minimum_bytes: u64, seconds: u64, now: u64) -> bool {
        self.commit > self.snapshot_commit
            && ((self.commit - self.snapshot_commit).saturating_mul(ENTRY as u64)
                >= self.snapshot_bytes.max(minimum_bytes)
                || now.saturating_sub(self.snapshot_second) >= seconds)
    }

    fn load_snapshot(
        &self,
        path: &Path,
        named_commit: u64,
        key: &[u8; 32],
    ) -> Result<(Vec<MasterEpoch>, u64, [u8; 32], u64, u64)> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) => return Err(Error::new(format!("reading KS snapshot: {e}"))),
        };
        if bytes.len() < SNAP_HEADER + 8 + 32
            || &bytes[..8] != SNAP_MAGIC
            || bytes[8..40] != self.id
        {
            return Err(Error::new("invalid KS snapshot header or journal identity"));
        }
        let commit = read_u64(&bytes[40..48]);
        if commit != named_commit {
            return Err(Error::new("snapshot filename and captured commit disagree"));
        }
        let mut previous = [0; 32];
        previous.copy_from_slice(&bytes[48..80]);
        let mut nonce = [0; 32];
        nonce.copy_from_slice(&bytes[80..112]);
        let split = bytes.len() - 32;
        let mut tag = [0; 32];
        tag.copy_from_slice(&bytes[split..]);
        let plain = Aegis256::<32>::new(key, &nonce)
            .decrypt(&bytes[SNAP_HEADER..split], &tag, &bytes[..80])
            .map_err(|_| Error::new("KS snapshot authentication failed"))?;
        if (plain.len() - 8) % 40 != 0 || (plain.len() - 8) as u64 / 40 != commit {
            return Err(Error::new("invalid KS snapshot key count"));
        }
        if commit == 0 && previous != [0; 32] {
            return Err(Error::new("empty KS snapshot has a nonzero chain anchor"));
        }
        let second = read_u64(&plain[..8]);
        let epochs: Vec<_> = plain[8..].chunks_exact(40).map(decode_epoch).collect();
        if epochs.windows(2).any(|w| w[0].since >= w[1].since) {
            return Err(Error::new("KS snapshot activation times are not increasing"));
        }
        Ok((epochs, commit, previous, second, bytes.len() as u64))
    }

    /// Coordinate one executed/durable boundary with the journal switch.
    /// The caller can release its logging lock while writing the prepared snapshot.
    pub fn prepare_snapshot(
        &mut self,
        epochs: &[MasterEpoch],
        applied: u64,
        now: u64,
    ) -> Result<PendingSnapshot> {
        if self.failed {
            return Err(Error::new("KS journal requires restart after an I/O failure"));
        }
        let key = self.key.ok_or_else(|| Error::new("KS journal is locked"))?;
        if applied != self.commit
            || applied < self.snapshot_commit
            || epochs.len() as u64 != applied
            || epochs.windows(2).any(|w| w[0].since >= w[1].since)
        {
            return Err(Error::new("KS snapshot must capture an applied durable prefix"));
        }
        if epochs.last().map(|e| e.since) != self.last {
            return Err(Error::new("snapshot final key position mismatch"));
        }
        let previous = self.previous;
        if let Err(e) = self.rotate() {
            self.failed = true;
            return Err(e);
        }
        Ok(PendingSnapshot {
            path: self.named("snapshot", applied),
            id: self.id,
            key,
            previous,
            applied,
            now,
        })
    }

    pub fn finish_snapshot(&mut self, pending: &PendingSnapshot, size: u64) -> Result<()> {
        if pending.id != self.id
            || pending.applied < self.snapshot_commit
            || pending.applied > self.commit
        {
            return Err(Error::new("invalid completed KS snapshot position"));
        }
        self.snapshot_commit = pending.applied;
        self.snapshot_second = pending.now;
        self.snapshot_bytes = size;
        self.prune()
    }

    pub fn snapshot(&mut self, epochs: &[MasterEpoch], applied: u64, now: u64) -> Result<()> {
        let pending = self.prepare_snapshot(epochs, applied, now)?;
        let size = pending.write(epochs)?;
        self.finish_snapshot(&pending, size)
    }
}

pub struct PendingSnapshot {
    path: PathBuf,
    id: [u8; 32],
    key: [u8; 32],
    previous: [u8; 32],
    applied: u64,
    now: u64,
}
impl PendingSnapshot {
    pub fn write(&self, epochs: &[MasterEpoch]) -> Result<u64> {
        if epochs.len() as u64 != self.applied {
            return Err(Error::new("snapshot state changed after capture"));
        }
        let nonce = random_master_key()?;
        let ad = [
            SNAP_MAGIC.as_slice(),
            self.id.as_slice(),
            &self.applied.to_le_bytes(),
            self.previous.as_slice(),
        ]
        .concat();
        let mut body = self.now.to_le_bytes().to_vec();
        for epoch in epochs {
            body.extend_from_slice(&epoch.key);
            body.extend_from_slice(&epoch.since.number().to_le_bytes());
        }
        let (ciphertext, tag) = Aegis256::<32>::new(&self.key, &nonce).encrypt(&body, &ad);
        let bytes =
            [ad.as_slice(), nonce.as_slice(), ciphertext.as_slice(), tag.as_slice()].concat();
        publish(&self.path, &bytes)?;
        Ok(bytes.len() as u64)
    }
}

fn filename(kind: &str, commit: u64) -> String {
    let extension = if kind == "snapshot" { "bin" } else { "log" };
    format!("{kind}-{commit:020}.{extension}")
}

fn numbered(directory: &Path, kind: &str) -> Result<Vec<(u64, PathBuf)>> {
    let mut result = Vec::new();
    let suffix = if kind == "snapshot" { ".bin" } else { ".log" };
    for entry in std::fs::read_dir(directory).context("listing KS persistence files")? {
        let entry = entry.context("reading KS directory entry")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&format!("{kind}-")) || !name.ends_with(suffix) {
            continue;
        }
        let digits = &name[kind.len() + 1..name.len() - suffix.len()];
        let commit: u64 = digits.parse().map_err(|_| Error::new("invalid numbered KS filename"))?;
        if name != filename(kind, commit) || !entry.file_type().context("stat KS file")?.is_file() {
            return Err(Error::new("invalid numbered KS file"));
        }
        result.push((commit, entry.path()));
    }
    result.sort_by_key(|(commit, _)| *commit);
    Ok(result)
}

fn open_segment(path: &Path) -> Result<File> {
    OpenOptions::new().read(true).write(true).open(path).context("opening KS journal segment")
}
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path).context("opening KS directory")?.sync_all().context("syncing KS directory")
}
fn publish(path: &Path, bytes: &[u8]) -> Result<()> {
    let nonce = random_master_key()?;
    let temporary = path.with_extension(format!("{}.tmp", common::shamir::hex(&nonce)));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .context("creating KS temporary file")?;
        file.write_all(bytes).context("writing KS persistence file")?;
        file.sync_all().context("syncing KS persistence file")?;
        std::fs::rename(&temporary, path).context("publishing KS persistence file")?;
        sync_directory(path.parent().unwrap_or(Path::new(".")))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}
fn read_u64(bytes: &[u8]) -> u64 {
    let mut number = [0; 8];
    number.copy_from_slice(bytes);
    u64::from_le_bytes(number)
}
fn decode_epoch(bytes: &[u8]) -> MasterEpoch {
    let mut key = [0; 32];
    key.copy_from_slice(&bytes[..32]);
    MasterEpoch { key, since: TimeSec::new(read_u64(&bytes[32..40])) }
}

#[cfg(test)]
impl Journal {
    pub fn make_read_only(&mut self, _directory: &Path) {
        self.file = Some(File::open(self.named("journal", self.segment_start)).unwrap());
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    pub struct Temp(PathBuf);
    impl Temp {
        pub fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "delawson-ks-segments-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        pub fn path(&self) -> PathBuf {
            self.0.join("keys")
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn epoch(n: u64) -> MasterEpoch {
        MasterEpoch { key: [n as u8; 32], since: TimeSec::new(n) }
    }
    fn open(dir: &Temp, keep: usize) -> Journal {
        let mut j = Journal::open(&dir.path(), keep).unwrap();
        j.replay([7; 32]).unwrap();
        j
    }
    fn numbers(dir: &Temp, kind: &str) -> Vec<u64> {
        numbered(&dir.path(), kind).unwrap().into_iter().map(|(n, _)| n).collect()
    }
    fn add(j: &mut Journal, epochs: &mut Vec<MasterEpoch>, n: u64) {
        assert_eq!(j.append(&epoch(n)).unwrap(), n);
        epochs.push(epoch(n));
    }

    #[test]
    fn encrypted_bootstrap_commit_numbers_and_lock_survive_rotation() {
        let dir = Temp::new();
        let mut j = open(&dir, 3);
        assert_eq!(numbers(&dir, "snapshot"), vec![0]);
        assert_eq!(numbers(&dir, "journal"), vec![1]);
        assert!(Journal::open(&dir.path(), 3).is_err());
        let mut epochs = Vec::new();
        add(&mut j, &mut epochs, 1);
        let raw = std::fs::read(j.named("journal", 1)).unwrap();
        assert!(!raw.windows(32).any(|w| w == [1; 32]));
        assert!(j.append(&epoch(1)).is_err());
        assert!(j.append(&epoch(0)).is_err());
        j.snapshot(&epochs, 1, 100).unwrap();
        assert_eq!(numbers(&dir, "journal"), vec![1, 2]);
        assert!(Journal::open(&dir.path(), 3).is_err());
        add(&mut j, &mut epochs, 2);
        drop(j);
        let mut j = Journal::open(&dir.path(), 3).unwrap();
        assert!(j.replay([8; 32]).is_err());
        let recovered = j.replay([7; 32]).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[1].key, [2; 32]);
        assert_eq!(j.commit(), 2);
        assert_eq!(j.append(&epoch(3)).unwrap(), 3);
    }

    #[test]
    fn retains_three_snapshots_and_every_journal_needed_by_each() {
        let dir = Temp::new();
        let mut j = open(&dir, 3);
        let mut epochs = Vec::new();
        for n in 1..=8 {
            add(&mut j, &mut epochs, n);
            if n % 2 == 0 {
                j.snapshot(&epochs, n, 100 + n).unwrap();
            }
        }
        add(&mut j, &mut epochs, 9);
        assert_eq!(numbers(&dir, "snapshot"), vec![4, 6, 8]);
        assert_eq!(numbers(&dir, "journal"), vec![5, 7, 9]);
        let identity = std::fs::read(dir.path().join("identity")).unwrap();
        drop(j);
        // Each retained snapshot independently recovers the entire remaining history.
        for cut in [4, 6, 8] {
            let copy = Temp::new();
            std::fs::create_dir(copy.path()).unwrap();
            std::fs::write(copy.path().join("identity"), &identity).unwrap();
            std::fs::copy(
                dir.path().join(filename("snapshot", cut)),
                copy.path().join(filename("snapshot", cut)),
            )
            .unwrap();
            for (start, path) in numbered(&dir.path(), "journal").unwrap() {
                if start > cut {
                    std::fs::copy(path, copy.path().join(filename("journal", start))).unwrap();
                }
            }
            let mut reader = Journal::open(&copy.path(), 3).unwrap();
            let recovered = reader.replay([7; 32]).unwrap();
            assert_eq!(recovered.len(), 9);
            assert_eq!(recovered[8].key, [9; 32]);
        }
    }

    #[test]
    fn configurable_retention_and_missing_snapshot_never_erase_history() {
        let dir = Temp::new();
        assert!(Journal::open(&dir.path(), 0).is_err());
        let mut j = open(&dir, 5);
        let mut epochs = Vec::new();
        for n in 1..=5 {
            add(&mut j, &mut epochs, n);
            j.snapshot(&epochs, n, 100 + n).unwrap();
        }
        assert_eq!(numbers(&dir, "snapshot"), vec![1, 2, 3, 4, 5]);
        drop(j);
        let _j = open(&dir, 2);
        assert_eq!(numbers(&dir, "snapshot"), vec![4, 5]);
        assert_eq!(numbers(&dir, "journal"), vec![5, 6]);
        drop(_j);
        for (_, path) in numbered(&dir.path(), "snapshot").unwrap() {
            std::fs::remove_file(path).unwrap();
        }
        let before = numbers(&dir, "journal");
        let mut j = Journal::open(&dir.path(), 2).unwrap();
        assert!(j.replay([7; 32]).is_err());
        assert_eq!(numbers(&dir, "journal"), before);
    }

    #[test]
    fn log_continues_during_snapshot_and_pruning_waits_for_publication() {
        let dir = Temp::new();
        let mut j = open(&dir, 1);
        let mut epochs = Vec::new();
        add(&mut j, &mut epochs, 1);
        add(&mut j, &mut epochs, 2);
        let captured = epochs.clone();
        let pending = j.prepare_snapshot(&captured, 2, 100).unwrap();
        assert_eq!(numbers(&dir, "snapshot"), vec![0]);
        assert_eq!(numbers(&dir, "journal"), vec![1, 3]);
        add(&mut j, &mut epochs, 3);
        add(&mut j, &mut epochs, 4);
        let size = pending.write(&captured).unwrap();
        assert_eq!(numbers(&dir, "journal"), vec![1, 3]);
        j.finish_snapshot(&pending, size).unwrap();
        assert_eq!(numbers(&dir, "snapshot"), vec![2]);
        assert_eq!(numbers(&dir, "journal"), vec![3]);
        drop(j);
        let mut j = Journal::open(&dir.path(), 1).unwrap();
        assert_eq!(j.replay([7; 32]).unwrap().len(), 4);
        assert_eq!(j.snapshot_commit, 2);
    }

    #[test]
    fn rotated_journals_replay_without_corresponding_snapshots() {
        let dir = Temp::new();
        let mut j = open(&dir, 3);
        let mut epochs = Vec::new();
        add(&mut j, &mut epochs, 1);
        let pending = j.prepare_snapshot(&epochs, 1, 100).unwrap();
        // Force publication failure after the journal switch, without damaging files.
        let mut bad = pending;
        bad.path = dir.path().join("missing/snapshot.bin");
        assert!(bad.write(&epochs).is_err());
        add(&mut j, &mut epochs, 2);
        let _interrupted = j.prepare_snapshot(&epochs, 2, 101).unwrap();
        add(&mut j, &mut epochs, 3);
        drop(j);
        assert_eq!(numbers(&dir, "snapshot"), vec![0]);
        assert_eq!(numbers(&dir, "journal"), vec![1, 2, 3]);
        let mut j = Journal::open(&dir.path(), 3).unwrap();
        assert_eq!(j.replay([7; 32]).unwrap().len(), 3);
    }

    #[test]
    fn recovery_resumes_pruning_after_snapshot_publication() {
        let dir = Temp::new();
        let mut j = open(&dir, 1);
        let mut epochs = Vec::new();
        add(&mut j, &mut epochs, 1);
        let pending = j.prepare_snapshot(&epochs, 1, 100).unwrap();
        pending.write(&epochs).unwrap(); // Terminated before finish_snapshot/prune.
        add(&mut j, &mut epochs, 2);
        drop(j);
        let mut j = Journal::open(&dir.path(), 1).unwrap();
        assert_eq!(j.replay([7; 32]).unwrap().len(), 2);
        assert_eq!(numbers(&dir, "snapshot"), vec![1]);
        assert_eq!(numbers(&dir, "journal"), vec![2]);
    }

    #[test]
    fn interrupted_commit_zero_initialization_completes_safely() {
        let dir = Temp::new();
        let mut j = Journal::open(&dir.path(), 3).unwrap();
        j.key = Some([7; 32]);
        j.rotate().unwrap();
        drop(j);
        let j = open(&dir, 3);
        assert_eq!(j.commit(), 0);
        assert_eq!(numbers(&dir, "snapshot"), vec![0]);
    }

    #[test]
    fn malformed_headers_gaps_and_closed_partial_entries_fail_closed() {
        for fault in 0..6 {
            let dir = Temp::new();
            let mut j = open(&dir, 3);
            let mut epochs = Vec::new();
            add(&mut j, &mut epochs, 1);
            let _pending = j.prepare_snapshot(&epochs, 1, 100).unwrap();
            add(&mut j, &mut epochs, 2);
            drop(j);
            let first = dir.path().join(filename("journal", 1));
            let second = dir.path().join(filename("journal", 2));
            match fault {
                0 => {
                    std::fs::remove_file(&first).unwrap();
                }
                1 => {
                    std::fs::rename(&second, dir.path().join(filename("journal", 3))).unwrap();
                }
                2 => {
                    let mut raw = std::fs::read(&second).unwrap();
                    raw[48] ^= 1;
                    std::fs::write(&second, raw).unwrap();
                }
                3 => {
                    let mut raw = std::fs::read(&second).unwrap();
                    raw[HEADER + 40] ^= 1;
                    std::fs::write(&second, raw).unwrap();
                }
                4 => {
                    let mut raw = std::fs::read(&first).unwrap();
                    raw.push(1);
                    std::fs::write(&first, raw).unwrap();
                }
                _ => {
                    let raw = std::fs::read(&second).unwrap();
                    std::fs::write(&second, &raw[..HEADER - 1]).unwrap();
                }
            }
            let mut j = Journal::open(&dir.path(), 3).unwrap();
            assert!(j.replay([7; 32]).is_err(), "fault {fault}");
            assert_eq!(numbers(&dir, "snapshot"), vec![0]);
        }
    }

    #[test]
    fn every_partial_active_tail_is_trimmed_and_commit_reused() {
        let dir = Temp::new();
        let mut j = open(&dir, 3);
        let mut epochs = Vec::new();
        add(&mut j, &mut epochs, 1);
        j.snapshot(&epochs, 1, 100).unwrap();
        add(&mut j, &mut epochs, 2);
        let path = j.named("journal", 2);
        let raw = std::fs::read(&path).unwrap();
        drop(j);
        for tail in 0..ENTRY {
            std::fs::write(&path, &raw[..HEADER + tail]).unwrap();
            let mut j = Journal::open(&dir.path(), 3).unwrap();
            assert_eq!(j.replay([7; 32]).unwrap().len(), 1);
            assert_eq!(j.append(&epoch(2)).unwrap(), 2);
        }
    }

    #[test]
    fn snapshot_integrity_filename_and_store_identity_are_verified() {
        for fault in 0..7 {
            let dir = Temp::new();
            let mut j = open(&dir, 3);
            let mut epochs = Vec::new();
            add(&mut j, &mut epochs, 1);
            j.snapshot(&epochs, 1, 100).unwrap();
            let path = j.named("snapshot", 1);
            drop(j);
            if fault == 6 {
                std::fs::rename(&path, dir.path().join(filename("snapshot", 2))).unwrap();
            } else {
                let mut raw = std::fs::read(&path).unwrap();
                raw[[0, 8, 40, 48, 80, 112][fault]] ^= 1;
                std::fs::write(&path, raw).unwrap();
            }
            let mut j = Journal::open(&dir.path(), 3).unwrap();
            assert!(j.replay([7; 32]).is_err());
            assert_eq!(numbers(&dir, "journal"), vec![1, 2]);
        }
    }

    #[test]
    fn time_and_size_policy_counts_entries_across_unsnapshotted_segments() {
        let dir = Temp::new();
        let mut j = open(&dir, 3);
        let mut epochs = Vec::new();
        let now = j.snapshot_second;
        assert!(!j.snapshot_due(0, 86400, now + 86400));
        add(&mut j, &mut epochs, 1);
        let _pending = j.prepare_snapshot(&epochs, 1, now).unwrap();
        add(&mut j, &mut epochs, 2);
        assert!(!j.snapshot_due(1024 * 1024, 86400, now));
        assert!(j.snapshot_due(224, 86400, now));
        assert!(!j.snapshot_due(225, 86400, now));
        assert!(j.snapshot_due(1024 * 1024, 86400, now + 86400));
        j.snapshot(&epochs, 2, now).unwrap();
        add(&mut j, &mut epochs, 3);
        add(&mut j, &mut epochs, 4);
        assert!(!j.snapshot_due(1, 86400, now));
        add(&mut j, &mut epochs, 5);
        assert!(j.snapshot_due(1, 86400, now));
    }

    #[test]
    fn snapshot_boundary_must_match_execution_and_current_log_switch() {
        let dir = Temp::new();
        let mut j = open(&dir, 3);
        let mut epochs = Vec::new();
        add(&mut j, &mut epochs, 1);
        add(&mut j, &mut epochs, 2);
        assert!(j.prepare_snapshot(&epochs[..1], 1, 100).is_err());
        assert!(j.prepare_snapshot(&epochs, 3, 100).is_err());
        assert_eq!(numbers(&dir, "journal"), vec![1]);
    }
}
