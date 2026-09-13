//! Single-member durable key log. Replication will replace the local commit step.
use super::{MasterEpoch, random_master_key};
use aegis::aegis256::Aegis256;
use common::time::TimeSec;
use common::{Context, Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"DLAWKS02";
const HEADER: usize = 40;
const ENTRY: usize = 112; // commit u64, nonce, encrypted (key, TimeSec), tag
const SNAP_MAGIC: &[u8; 8] = b"DLAWSS01";
const SNAP_HEADER: usize = 112; // magic, journal identity, commit, chain anchor, nonce

pub struct Journal {
    file: File,
    id: [u8; 32],
    key: Option<[u8; 32]>,
    previous: [u8; 32],
    last: Option<TimeSec>,
    failed: bool,
    commit: u64,
    snapshot_commit: u64,
    snapshot_second: u64,
    snapshot_bytes: u64,
    snapshot_path: PathBuf,
}

impl Journal {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .context("opening KS journal")?;
        file.try_lock().context("locking KS journal (another KS may be running)")?;
        let mut header = [0; HEADER];
        if file.metadata().context("stat KS journal")?.len() == 0 {
            header[..8].copy_from_slice(MAGIC);
            header[8..].copy_from_slice(&random_master_key()?);
            file.write_all(&header).context("creating KS journal")?;
        } else {
            file.read_exact(&mut header).context("reading KS journal header")?;
            if &header[..8] != MAGIC {
                return Err(Error::new(
                    "invalid or unsupported KS journal header (expected DLAWKS02)",
                ));
            }
        }
        // Also completes durability if the previous creation stopped before sync.
        file.sync_all().context("syncing KS journal header")?;
        File::open(path.parent().unwrap_or(Path::new(".")))
            .context("opening KS journal directory")?
            .sync_all()
            .context("syncing KS journal directory")?;
        let mut id = [0; 32];
        id.copy_from_slice(&header[8..]);
        Ok(Self {
            file,
            id,
            key: None,
            previous: [0; 32],
            last: None,
            failed: false,
            commit: 0,
            snapshot_commit: 0,
            snapshot_second: 0,
            snapshot_bytes: 0,
            snapshot_path: path.with_extension("snapshot"),
        })
    }

    fn ad(&self, previous: &[u8; 32], commit: u64) -> Vec<u8> {
        [MAGIC.as_slice(), self.id.as_slice(), previous.as_slice(), &commit.to_le_bytes()].concat()
    }

    pub fn replay(&mut self, key: [u8; 32]) -> Result<Vec<MasterEpoch>> {
        if self.failed {
            return Err(Error::new("KS journal requires restart after an I/O failure"));
        }
        // Bootstrap is the only path allowed to create a missing snapshot, and
        // only before there are journal entries. Publish commit zero before keys.
        match std::fs::metadata(&self.snapshot_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if self.file.metadata().context("stat KS journal")?.len() != HEADER as u64 {
                    return Err(Error::new("KS snapshot is missing for a nonempty journal"));
                }
                self.key = Some(key);
                self.snapshot(&[], 0, TimeSec::now().number())?;
            }
            Err(e) => return Err(Error::new(format!("stat KS snapshot: {e}"))),
            Ok(_) => {}
        }
        let (mut epochs, mut commit, mut previous, snapshot_second, snapshot_bytes) =
            self.load_snapshot(&key)?;
        let snapshot_commit = commit;
        let start = commit
            .checked_mul(ENTRY as u64)
            .and_then(|n| n.checked_add(HEADER as u64))
            .ok_or_else(|| Error::new("KS snapshot commit offset overflow"))?;
        if self.file.metadata().context("stat KS journal")?.len() < start {
            return Err(Error::new("KS journal is shorter than its snapshot prefix"));
        }
        self.file.seek(SeekFrom::Start(start)).context("seeking after KS snapshot")?;
        let mut bytes = Vec::new();
        self.file.read_to_end(&mut bytes).context("reading KS journal suffix")?;
        for record in bytes.chunks_exact(ENTRY) {
            let expected = commit.checked_add(1).ok_or_else(|| Error::new("KS commit overflow"))?;
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
        // An incomplete final write was never acknowledged. Authenticate the complete
        // prefix before removing it; complete but corrupt entries always fail closed.
        let length = start + (bytes.len() / ENTRY * ENTRY) as u64;
        let repair = (|| -> std::io::Result<()> {
            self.file.set_len(length as u64)?;
            self.file.sync_all()?;
            self.file.seek(SeekFrom::End(0))?;
            Ok(())
        })();
        if let Err(e) = repair {
            self.failed = true;
            return Err(Error::new(format!("repairing KS journal tail: {e}")));
        }
        self.key = Some(key);
        self.previous = previous;
        self.last = epochs.last().map(|e| e.since);
        self.commit = commit;
        self.snapshot_commit = snapshot_commit;
        self.snapshot_second = snapshot_second;
        self.snapshot_bytes = snapshot_bytes;
        Ok(epochs)
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
        if let Err(e) = self.file.write_all(&record).and_then(|()| self.file.sync_all()) {
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

    fn load_snapshot(&self, key: &[u8; 32]) -> Result<(Vec<MasterEpoch>, u64, [u8; 32], u64, u64)> {
        let bytes = match std::fs::read(&self.snapshot_path) {
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

    /// The caller captures applied state while holding the proposal mutex. Snapshot
    /// I/O uses that copy after releasing the shared state lock, never its write lock.
    pub fn snapshot(&mut self, epochs: &[MasterEpoch], applied: u64, now: u64) -> Result<()> {
        if self.failed {
            return Err(Error::new("KS journal requires restart after an I/O failure"));
        }
        let key = self.key.as_ref().ok_or_else(|| Error::new("KS journal is locked"))?;
        if applied != self.commit
            || epochs.len() as u64 != applied
            || epochs.last().map(|e| e.since) != self.last
            || epochs.windows(2).any(|w| w[0].since >= w[1].since)
        {
            return Err(Error::new("KS snapshot must capture the complete applied durable prefix"));
        }
        let nonce = random_master_key()?;
        let ad = [
            SNAP_MAGIC.as_slice(),
            self.id.as_slice(),
            &applied.to_le_bytes(),
            self.previous.as_slice(),
        ]
        .concat();
        let mut body = now.to_le_bytes().to_vec();
        for epoch in epochs {
            body.extend_from_slice(&epoch.key);
            body.extend_from_slice(&epoch.since.number().to_le_bytes());
        }
        let (ciphertext, tag) = Aegis256::<32>::new(key, &nonce).encrypt(&body, &ad);
        let bytes =
            [ad.as_slice(), nonce.as_slice(), ciphertext.as_slice(), tag.as_slice()].concat();
        // A unique temporary name avoids following or truncating an existing file.
        let temporary = self
            .snapshot_path
            .with_extension(format!("snapshot-{}.tmp", common::shamir::hex(&nonce)));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)
                .context("creating KS snapshot temporary file")?;
            file.write_all(&bytes).context("writing KS snapshot")?;
            file.sync_all().context("syncing KS snapshot")?;
            std::fs::rename(&temporary, &self.snapshot_path).context("publishing KS snapshot")?;
            File::open(self.snapshot_path.parent().unwrap_or(Path::new(".")))
                .context("opening KS snapshot directory")?
                .sync_all()
                .context("syncing KS snapshot directory")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result?;
        self.snapshot_commit = applied;
        self.snapshot_second = now;
        self.snapshot_bytes = bytes.len() as u64;
        Ok(())
    }
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
    pub fn make_read_only(&mut self, path: &Path) {
        self.file = File::open(path).unwrap();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    pub struct Temp(PathBuf);
    impl Temp {
        pub fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "delawson-ks-journal-{}-{}",
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
    fn epoch(second: u64) -> MasterEpoch {
        MasterEpoch { key: [second as u8; 32], since: TimeSec::new(second) }
    }
    fn populate(path: &Path) -> Vec<u8> {
        let mut j = Journal::open(path).unwrap();
        j.replay([7; 32]).unwrap();
        for second in [10, 20, 30] {
            j.append(&epoch(second)).unwrap();
        }
        std::fs::read(path).unwrap()
    }

    #[test]
    fn restart_authentication_ordering_and_exclusive_owner() {
        let dir = Temp::new();
        let path = dir.path();
        let raw = populate(&path);
        assert!(!raw.windows(32).any(|w| w == [10; 32]));
        let mut j = Journal::open(&path).unwrap();
        assert!(Journal::open(&path).is_err());
        assert!(j.replay([8; 32]).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), raw);
        let epochs = j.replay([7; 32]).unwrap();
        assert_eq!(epochs.len(), 3);
        assert_eq!(epochs[2].key, [30; 32]);
        for second in [20, 30] {
            assert!(j.append(&epoch(second)).is_err());
        }
        assert_eq!(std::fs::read(&path).unwrap(), raw);
        j.append(&epoch(40)).unwrap();
    }

    #[test]
    fn every_incomplete_tail_recovers_only_complete_entries() {
        let dir = Temp::new();
        let path = dir.path();
        let raw = populate(&path);
        for tail in 0..ENTRY {
            std::fs::write(&path, &raw[..HEADER + ENTRY * 2 + tail]).unwrap();
            let mut j = Journal::open(&path).unwrap();
            let entries = j.replay([7; 32]).unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(j.file.metadata().unwrap().len(), (HEADER + ENTRY * 2) as u64);
            j.append(&epoch(40)).unwrap();
            drop(j);
            let mut j = Journal::open(&path).unwrap();
            assert_eq!(j.replay([7; 32]).unwrap()[2].since, TimeSec::new(40));
        }
    }

    #[test]
    fn corruption_reordering_and_cross_journal_splicing_fail_closed() {
        let dir = Temp::new();
        let path = dir.path();
        let raw = populate(&path);
        let other = Temp::new();
        let other_raw = populate(&other.path());
        let mut variants = Vec::new();
        for index in [8, HEADER, HEADER + 32, HEADER + 72, raw.len() - 1] {
            let mut bytes = raw.clone();
            bytes[index] ^= 1;
            variants.push(bytes);
        }
        variants.push(
            [
                &raw[..HEADER],
                &raw[HEADER + ENTRY..HEADER + 2 * ENTRY],
                &raw[HEADER..HEADER + ENTRY],
                &raw[HEADER + 2 * ENTRY..],
            ]
            .concat(),
        );
        variants.push([&raw[..HEADER + ENTRY], &other_raw[HEADER + ENTRY..]].concat());
        for bytes in variants {
            std::fs::write(&path, &bytes).unwrap();
            let mut j = Journal::open(&path).unwrap();
            assert!(j.replay([7; 32]).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn snapshot_restores_prefix_without_reading_it_then_replays_numbered_suffix() {
        let dir = Temp::new();
        let path = dir.path();
        populate(&path);
        let mut j = Journal::open(&path).unwrap();
        let mut epochs = j.replay([7; 32]).unwrap();
        j.snapshot(&epochs, 3, 100).unwrap();
        let snapshot = std::fs::read(&j.snapshot_path).unwrap();
        assert_eq!(read_u64(&snapshot[40..48]), 3);
        assert!(!snapshot.windows(32).any(|w| w == [10; 32]));
        assert_eq!(j.append(&epoch(40)).unwrap(), 4);
        drop(j);
        let mut raw = std::fs::read(&path).unwrap();
        // Even corrupted ciphertext in the captured prefix is not read or decrypted.
        raw[HEADER + 40] ^= 1;
        std::fs::write(&path, &raw).unwrap();
        let mut j = Journal::open(&path).unwrap();
        epochs = j.replay([7; 32]).unwrap();
        assert_eq!(
            epochs.iter().map(|e| e.since.number()).collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
        assert_eq!(j.commit(), 4);
        assert_eq!(j.append(&epoch(50)).unwrap(), 5);
        epochs.push(epoch(50));
        j.snapshot(&epochs, 5, 200).unwrap();
        drop(j);
        let mut j = Journal::open(&path).unwrap();
        assert_eq!(j.replay([7; 32]).unwrap().len(), 5);
        assert_eq!(j.snapshot_commit, 5);
    }

    #[test]
    fn snapshot_policy_checks_dirty_state_log_bytes_and_elapsed_time() {
        let dir = Temp::new();
        let path = dir.path();
        let mut j = Journal::open(&path).unwrap();
        j.replay([7; 32]).unwrap();
        assert!(!j.snapshot_due(0, 86400, 100));
        j.append(&epoch(10)).unwrap();
        assert!(!j.snapshot_due(0, 86400, 100));
        j.snapshot(&[epoch(10)], 1, 100).unwrap();
        assert!(!j.snapshot_due(0, 86400, 100000));
        j.append(&epoch(20)).unwrap();
        assert_eq!(j.snapshot_bytes, (SNAP_HEADER + 8 + 40 + 32) as u64);
        assert!((ENTRY as u64) < j.snapshot_bytes);
        assert!(!j.snapshot_due(0, 86400, 101));
        assert!(!j.snapshot_due(0, 86400, 99));
        assert!(j.snapshot_due(0, 86400, 86500));
        j.append(&epoch(30)).unwrap();
        assert!(j.snapshot_due(0, 86400, 102));
    }

    #[test]
    fn bad_snapshots_and_commit_gaps_fail_without_modifying_journal() {
        let dir = Temp::new();
        let path = dir.path();
        populate(&path);
        let mut j = Journal::open(&path).unwrap();
        let epochs = j.replay([7; 32]).unwrap();
        j.snapshot(&epochs, 3, 100).unwrap();
        j.append(&epoch(40)).unwrap();
        let snapshot_path = j.snapshot_path.clone();
        let snapshot = std::fs::read(&snapshot_path).unwrap();
        let raw = std::fs::read(&path).unwrap();
        drop(j);
        for index in [0, 8, 40, 48, 80, 112, snapshot.len() - 1] {
            let mut bad = snapshot.clone();
            bad[index] ^= 1;
            std::fs::write(&snapshot_path, &bad).unwrap();
            let mut j = Journal::open(&path).unwrap();
            assert!(j.replay([7; 32]).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), raw);
        }
        std::fs::write(&snapshot_path, &snapshot).unwrap();
        let mut j = Journal::open(&path).unwrap();
        assert!(j.replay([8; 32]).is_err());
        drop(j);
        let mut bad = raw.clone();
        bad[HEADER + 3 * ENTRY..HEADER + 3 * ENTRY + 8].copy_from_slice(&5u64.to_le_bytes());
        std::fs::write(&path, &bad).unwrap();
        let mut j = Journal::open(&path).unwrap();
        assert!(j.replay([7; 32]).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bad);
        drop(j);
        std::fs::write(&path, &raw[..HEADER + 2 * ENTRY]).unwrap();
        let mut j = Journal::open(&path).unwrap();
        assert!(j.replay([7; 32]).is_err());
    }

    #[test]
    fn interrupted_snapshot_leaves_old_snapshot_and_suffix_recoverable() {
        let dir = Temp::new();
        let path = dir.path();
        let mut j = Journal::open(&path).unwrap();
        j.replay([7; 32]).unwrap();
        j.append(&epoch(10)).unwrap();
        j.snapshot(&[epoch(10)], 1, 100).unwrap();
        let original = j.snapshot_path.clone();
        let old = std::fs::read(&original).unwrap();
        j.append(&epoch(20)).unwrap();
        // Failed snapshot creation must not retract commit 2 or replace snapshot 1.
        j.snapshot_path = dir.0.join("missing/snapshot");
        assert!(j.snapshot(&[epoch(10), epoch(20)], 2, 101).is_err());
        assert_eq!(j.snapshot_commit, 1);
        assert_eq!(j.commit(), 2);
        j.snapshot_path = original.clone();
        assert_eq!(std::fs::read(&original).unwrap(), old);
        // Model termination during a later temporary-file write; recovery ignores it.
        std::fs::write(original.with_extension("snapshot-interrupted.tmp"), &old[..old.len() / 2])
            .unwrap();
        drop(j);
        let mut j = Journal::open(&path).unwrap();
        assert_eq!(j.replay([7; 32]).unwrap().len(), 2);
        assert_eq!(j.snapshot_commit, 1);
    }

    #[test]
    fn partial_suffix_after_snapshot_is_trimmed_without_reapplying_prefix() {
        let dir = Temp::new();
        let path = dir.path();
        let mut j = Journal::open(&path).unwrap();
        j.replay([7; 32]).unwrap();
        j.append(&epoch(10)).unwrap();
        j.snapshot(&[epoch(10)], 1, 100).unwrap();
        j.append(&epoch(20)).unwrap();
        let raw = std::fs::read(&path).unwrap();
        drop(j);
        for tail in [0, 1, 8, 40, 80, ENTRY - 1] {
            std::fs::write(&path, &raw[..HEADER + ENTRY + tail]).unwrap();
            let mut j = Journal::open(&path).unwrap();
            assert_eq!(j.replay([7; 32]).unwrap().len(), 1);
            assert_eq!(j.commit(), 1);
            assert_eq!(j.append(&epoch(30)).unwrap(), 2);
        }
    }

    #[test]
    fn snapshot_cannot_label_unapplied_state_as_durable() {
        let dir = Temp::new();
        let path = dir.path();
        populate(&path);
        let mut j = Journal::open(&path).unwrap();
        let epochs = j.replay([7; 32]).unwrap();
        assert!(j.snapshot(&epochs, 4, 100).is_err());
        assert!(j.snapshot(&epochs[..2], 3, 100).is_err());
        assert_eq!(read_u64(&std::fs::read(&j.snapshot_path).unwrap()[40..48]), 0);
    }

    #[test]
    fn initializes_empty_snapshot_before_entries_and_requires_it_on_recovery() {
        let dir = Temp::new();
        let path = dir.path();
        let mut j = Journal::open(&path).unwrap();
        assert!(!j.snapshot_path.exists());
        assert!(j.replay([7; 32]).unwrap().is_empty());
        assert_eq!(j.commit(), 0);
        assert_eq!(j.snapshot_bytes, (SNAP_HEADER + 8 + 32) as u64);
        assert_eq!(read_u64(&std::fs::read(&j.snapshot_path).unwrap()[40..48]), 0);
        j.append(&epoch(10)).unwrap();
        let snapshot_path = j.snapshot_path.clone();
        drop(j);
        let mut j = Journal::open(&path).unwrap();
        assert_eq!(j.replay([7; 32]).unwrap().len(), 1);
        assert_eq!(j.snapshot_commit, 0);
        drop(j);
        std::fs::remove_file(snapshot_path).unwrap();
        let raw = std::fs::read(&path).unwrap();
        let mut j = Journal::open(&path).unwrap();
        assert!(j.replay([7; 32]).is_err());
        assert_eq!(std::fs::read(path).unwrap(), raw);
    }

    #[test]
    fn configurable_log_floor_and_snapshot_size_both_bound_trigger() {
        let dir = Temp::new();
        let path = dir.path();
        let mut j = Journal::open(&path).unwrap();
        j.replay([7; 32]).unwrap();
        let now = j.snapshot_second;
        j.append(&epoch(10)).unwrap();
        j.append(&epoch(20)).unwrap();
        // 224 bytes exceed the empty snapshot, but not the configured floor.
        assert!(j.snapshot_due(0, 86400, now));
        assert!(!j.snapshot_due(1024 * 1024, 86400, now));
        assert!(!j.snapshot_due(225, 86400, now));
        assert!(j.snapshot_due(224, 86400, now));
        assert!(j.snapshot_due(1024 * 1024, 86400, now + 86400));
        j.snapshot(&[epoch(10), epoch(20)], 2, now).unwrap();
        j.append(&epoch(30)).unwrap();
        j.append(&epoch(40)).unwrap();
        // The new snapshot is 232 bytes, larger than two 112-byte entries.
        assert!(!j.snapshot_due(1, 86400, now));
        j.append(&epoch(50)).unwrap();
        assert!(j.snapshot_due(1, 86400, now));
    }
}
