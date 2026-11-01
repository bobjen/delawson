//! One ordered journal for Raft entries, terms/votes, commit markers and installs.
//! Applications encrypt private command data before proposing it; consensus metadata stays readable.
use crate::codec::{self, Wire};
use crate::raft::{INCARNATION, Member, MemberId, Membership, Record, Retirement, Snapshot, State};
use crate::random_bytes;
use common::scn::Scn;
use common::{Context, Error, Result};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
#[cfg(test)]
use std::io::IoSlice;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAGIC: &[u8; 8] = b"DLAWKR06";
const HEADER: u64 = 8 + 128 + 16 + 24;
fn journal_header(ring: &str, start: u64) -> Vec<u8> {
    [MAGIC.as_slice(), ring.as_bytes(), &start.to_le_bytes(), &(!start).to_le_bytes(), &[0; 24]]
        .concat()
}
fn journal_header_after(state: &State, start: u64) -> Vec<u8> {
    let mut bytes = journal_header(&state.ring, start);
    bytes[152..160].copy_from_slice(&state.last().to_le_bytes());
    bytes[160..168].copy_from_slice(&state.term_at(state.last()).unwrap().to_le_bytes());
    bytes[168..176].copy_from_slice(&state.scn_at(state.last()).unwrap().number().to_le_bytes());
    bytes
}
fn check_journal_header(reader: &mut impl Read, ring: &str, ignore: bool) -> Result<u64> {
    Ok(read_journal_header(reader, ring, ignore)?.0)
}
fn read_journal_header(
    reader: &mut impl Read,
    ring: &str,
    ignore: bool,
) -> Result<(u64, u64, u64, Scn)> {
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
    Ok((
        start,
        u64::from_le_bytes(header[152..160].try_into().unwrap()),
        u64::from_le_bytes(header[160..168].try_into().unwrap()),
        Scn::new(u64::from_le_bytes(header[168..176].try_into().unwrap())),
    ))
}
/// Validate before Install can make an untrusted checkpoint look locally committed.
pub(crate) fn checkpoint_install(
    state: &State,
    snapshot: &crate::Snapshot,
    proof: Option<(u64, Scn)>,
) -> Result<Option<Record>> {
    snapshot.validate()?;
    state.check_ring(snapshot)?;
    if proof != Some((snapshot.term, snapshot.scn)) || snapshot.term > state.term {
        return Err(Error::new("checkpoint does not match primary history"));
    }
    if snapshot.index <= state.snapshot.index {
        return Err(Error::new("checkpoint does not advance snapshot"));
    }
    if snapshot.index <= state.commit {
        if state.history_at(snapshot.index) != Some((snapshot.term, snapshot.scn)) {
            return Err(Error::new("checkpoint differs from local committed history"));
        }
        Ok(None)
    } else {
        Ok(Some(Record::Install(snapshot.clone())))
    }
}
const LIMIT: usize = 64 * 1024 * 1024;

struct Batch {
    sequence: u64,
    records: Vec<Record>,
}

enum JournalFrame {
    Flat(Vec<u8>),
    Shared(SharedFrame),
}
struct SharedFrame {
    head: Vec<u8>,
    entries: Vec<Arc<crate::raft::EntryBuf>>,
    tail: Vec<u8>,
    length: usize,
}
impl JournalFrame {
    fn len(&self) -> usize {
        match self {
            Self::Flat(bytes) => bytes.len(),
            Self::Shared(frame) => frame.length,
        }
    }
    #[cfg(test)]
    fn write_to(&self, out: &mut impl Write) -> std::io::Result<()> {
        match self {
            Self::Flat(bytes) => out.write_all(bytes),
            Self::Shared(frame) => frame.write_to(out),
        }
    }
    fn write_at(&self, file: &File, mut offset: u64) -> std::io::Result<()> {
        match self {
            Self::Flat(bytes) => write_all_at(file, bytes, &mut offset),
            Self::Shared(frame) => frame.write_at(file, offset),
        }
    }
}
fn write_all_at(file: &File, mut bytes: &[u8], offset: &mut u64) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let written = file.write_at(bytes, *offset)?;
        if written == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "journal write"));
        }
        *offset = (*offset)
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("journal offset overflow"))?;
        bytes = &bytes[written..];
    }
    Ok(())
}
impl SharedFrame {
    fn write_at(&self, file: &File, mut offset: u64) -> std::io::Result<()> {
        write_all_at(file, &self.head, &mut offset)?;
        for entry in &self.entries {
            let mut at = 0;
            while at < entry.bytes.len() {
                let chunk = entry.bytes.chunk(at);
                if chunk.is_empty() {
                    return Err(std::io::Error::other("empty Raft entry buffer segment"));
                }
                write_all_at(file, chunk, &mut offset)?;
                at += chunk.len();
            }
        }
        write_all_at(file, &self.tail, &mut offset)
    }
    #[cfg(test)]
    fn write_to(&self, out: &mut impl Write) -> std::io::Result<()> {
        let mut pieces = Vec::new();
        pieces.push(self.head.as_slice());
        for entry in &self.entries {
            let mut at = 0;
            while at < entry.bytes.len() {
                let chunk = entry.bytes.chunk(at);
                if chunk.is_empty() {
                    return Err(std::io::Error::other("empty Raft entry buffer segment"));
                }
                pieces.push(chunk);
                at += chunk.len();
            }
        }
        pieces.push(self.tail.as_slice());
        let mut first = 0;
        while first < pieces.len() {
            let end = (first + 1024).min(pieces.len());
            let written = {
                let slices: Vec<_> =
                    pieces[first..end].iter().map(|bytes| IoSlice::new(bytes)).collect();
                out.write_vectored(&slices)?
            };
            if written == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "journal write"));
            }
            let mut left = written;
            while first < pieces.len() && left >= pieces[first].len() {
                left -= pieces[first].len();
                first += 1;
            }
            if left != 0 {
                pieces[first] = &pieces[first][left..];
            }
        }
        Ok(())
    }
}
struct Checkpoint {
    sequence: u64,
    second: u64,
    state: State,
}

#[derive(Clone, Copy, Debug)]
pub struct CheckpointMark {
    sequence: u64,
    log_bytes: u64,
}

impl Checkpoint {
    fn bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = encode(self)?;
        bytes.extend(scn_footer(self.state.largest_scn()));
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let length = bytes
            .len()
            .checked_sub(SCN_FOOTER as usize)
            .ok_or_else(|| Error::new("missing snapshot SCN footer"))?;
        let maximum = parse_scn_footer(&bytes[length..])?;
        let checkpoint: Self = decode(&bytes[..length])?;
        if checkpoint.state.largest_scn() != maximum {
            return Err(Error::new("snapshot SCN footer mismatch"));
        }
        Ok(checkpoint)
    }
}
const SCN_MAGIC: &[u8; 8] = b"DLAWSCN1";
const SCN_FOOTER: u64 = 64;
fn scn_footer(maximum: Option<Scn>) -> Vec<u8> {
    let value = maximum.map_or(0, Scn::number);
    let mut bytes = [
        SCN_MAGIC.as_slice(),
        &(maximum.is_some() as u64).to_le_bytes(),
        &value.to_le_bytes(),
        &(!value).to_le_bytes(),
    ]
    .concat();
    bytes.extend(Sha256::digest(&bytes));
    bytes
}
fn parse_scn_footer(bytes: &[u8]) -> Result<Option<Scn>> {
    if bytes.len() != SCN_FOOTER as usize
        || &bytes[..8] != SCN_MAGIC
        || Sha256::digest(&bytes[..32]).as_slice() != &bytes[32..]
    {
        return Err(Error::new("invalid SCN footer"));
    }
    let present = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let value = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let inverse = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    if present > 1 || value != !inverse || (present == 0 && value != 0) {
        return Err(Error::new("invalid SCN footer fields"));
    }
    Ok((present == 1).then_some(Scn::new(value)))
}
/// Fast high-water lookup for a snapshot or closed journal. The identity-selected
/// latest journal has no footer during normal operation and must be replayed.
/// This validates the footer; callers must still validate the file's ring,
/// contents and history before use.
pub fn file_scn(path: &Path) -> Result<Option<Scn>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::End(-(SCN_FOOTER as i64)))?;
    let mut footer = [0; SCN_FOOTER as usize];
    file.read_exact(&mut footer)?;
    parse_scn_footer(&footer)
}

/// A peer copy contains committed entries only, never term/vote records.
#[derive(Clone)]
pub struct CommittedJournal {
    pub ring: String,
    pub after: u64,
    pub previous_term: u64,
    pub previous_scn: Scn,
    pub entries: Vec<Arc<crate::raft::Entry>>,
}
impl CommittedJournal {
    pub fn largest_scn(&self) -> Option<Scn> {
        self.entries
            .iter()
            .map(|entry| entry.scn)
            .reduce(|a, b| if b.compare(a).is_gt() { b } else { a })
    }

    /// Match the creator SCN as well as the primary's term/index proof.
    pub fn matches_tip(&self, index: u64, tip: (u64, Scn)) -> bool {
        self.entries
            .last()
            .is_some_and(|entry| entry.index == index && (entry.term, entry.scn) == tip)
    }
    pub fn validate(&self, state: &State, through: u64) -> Result<()> {
        if !state.ignore_ring_identity && self.ring != state.ring {
            return Err(Error::new("copied journal ring mismatch"));
        }
        if self.after != state.commit
            || state.history_at(self.after) != Some((self.previous_term, self.previous_scn))
            || self.entries.is_empty()
        {
            return Err(Error::new("copied journal prefix mismatch"));
        }
        let mut index = self.after;
        let mut term = self.previous_term;
        for entry in &self.entries {
            index = index.checked_add(1).ok_or_else(|| Error::new("journal index overflow"))?;
            if entry.index != index
                || entry.index > through
                || entry.term == 0
                || entry.term < term
                || entry.term > state.term
            {
                return Err(Error::new("invalid copied journal entry"));
            }
            term = entry.term;
        }
        Ok(())
    }
}
/// Directory-local identity; never imported from another member's history.
/// This inode is also the lifetime lock and must not be replaced while in use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalIdentity {
    pub repairing: bool,
    pub ring: String,
    pub process: String,
    pub current: common::ring::MemberIdentity,
    /// Kept until membership no longer needs their state-loss responses.
    pub retired: Vec<common::ring::MemberIdentity>,
    pub slot: Option<MemberId>,
    /// Starting sequence of the last journal file created for this member.
    latest_journal: u64,
    membership: Option<Membership>,
    founders: Option<Membership>,
    initialized: bool,
    pending: Option<Retirement>,
    recovery: Option<RecoveryPlan>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct RecoveryPlan {
    archive: String,
    checkpoint: String,
    files: Vec<String>,
}

struct Disk {
    ownership: Mutex<File>,
    file: Mutex<Arc<File>>,
    process: String,
    ring: String,
    timing: IoTiming,
}
#[derive(Default)]
struct IoTiming {
    frames: AtomicU64,
    write_ns: AtomicU64,
    sync_ns: AtomicU64,
    syncs: AtomicU64,
}
impl IoTiming {
    fn record(&self, write_ns: u64, sync_ns: u64, batch_at: usize, process: &str, ring: &str) {
        let write_total = self.write_ns.fetch_add(write_ns, Ordering::Relaxed) + write_ns;
        let sync_total = if batch_at == 0 {
            self.sync_ns.fetch_add(sync_ns, Ordering::Relaxed) + sync_ns
        } else {
            self.sync_ns.load(Ordering::Relaxed)
        };
        let syncs = if batch_at == 0 {
            self.syncs.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.syncs.load(Ordering::Relaxed)
        };
        let frames = self.frames.fetch_add(1, Ordering::Relaxed) + 1;
        if frames % 1000 == 0 {
            common::logi!(
                "Raft journal I/O timing",
                "process {},ring {},frames {},syncs {},write_all_avg_us {:.1},sync_all_avg_us {:.1},frames_per_sync {:.2}",
                process,
                ring,
                frames,
                syncs,
                write_total as f64 / frames as f64 / 1000.0,
                sync_total as f64 / syncs.max(1) as f64 / 1000.0,
                frames as f64 / syncs.max(1) as f64
            );
        }
    }
}
impl Disk {
    fn current_file(&self) -> Result<Arc<File>> {
        self.file.lock().map(|file| file.clone()).map_err(|_| Error::new("journal file poisoned"))
    }
    fn replace_file(&self, file: File) -> Result<()> {
        *self.file.lock().map_err(|_| Error::new("journal file poisoned"))? = Arc::new(file);
        Ok(())
    }
    fn lock_ownership(&self) -> Result<std::sync::MutexGuard<'_, File>> {
        self.ownership.lock().map_err(|_| Error::new("journal identity poisoned"))
    }
}

pub struct Journal {
    pub identity: LocalIdentity,
    pub state: State,
    directory: PathBuf,
    bootstrap_pending: bool,
    disk: Arc<Disk>,
    io: Option<crate::io::Queue>,
    durable: State,
    pending_io: VecDeque<(u64, DurableChange)>,
    active_start: u64,
    active_bytes: u64,
    sequence: u64,
    change_sequence: u64,
    snapshot_sequence: u64,
    snapshot_second: u64,
    snapshot_bytes: u64,
    log_bytes: u64,
    retain: usize,
    pub failed: bool,
}
enum DurableChange {
    Records(Arc<Vec<Record>>),
    Snapshot(Snapshot),
    Replace(State),
}
fn mark_records(records: &[Record], durable: bool) {
    for record in records {
        let entries: &[crate::raft::Entry] = match record {
            Record::Entries { entries, .. } | Record::Carry { entries, .. } => entries,
            Record::Install(snapshot) => &snapshot.application,
            _ => continue,
        };
        for entry in entries {
            if let Some(buffer) = &entry.entry_buf {
                if durable {
                    buffer.mark_durable();
                } else {
                    buffer.mark_written();
                }
            }
        }
    }
}
fn mark_loaded(state: &State) {
    for entry in state.snapshot.application.iter().chain(state.log.iter()) {
        if let Some(buffer) = &entry.entry_buf {
            buffer.mark_durable();
        }
    }
}
impl crate::raft::Storage for Journal {
    fn history_repair_required(&self) -> bool {
        self.identity.repairing
    }
    fn durable_state(&self) -> &State {
        &self.durable
    }
    fn state(&self) -> &State {
        &self.state
    }
    fn write(&mut self, records: &[Record]) -> Result<()> {
        Journal::write(self, records)
    }
    fn write_owned(&mut self, records: Vec<Record>) -> Result<()> {
        Journal::write_owned(self, records)
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
        ownership.try_lock().context(format!("locking {member} persistence directory"))?;
        // Observe creation intent once at startup, never by polling a running store.
        let bootstrap_pending = directory.join("need-bootstrap").try_exists()?;
        let saved_identity = read_identity(&mut ownership)?;
        let identity_missing = saved_identity.is_none();
        let snapshots = numbered(directory, "snapshot")?;
        let journals = numbered(directory, "journal")?;
        let mut local: LocalIdentity = if saved_identity.is_none() {
            let current = common::ring::MemberIdentity::generate()?;
            let local = LocalIdentity {
                repairing: false,
                ring: initial.ring.clone(),
                process: member.into(),
                current,
                retired: vec![],
                slot: None,
                latest_journal: 0,
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
                return Err(Error::new(format!("{member} persistence ring/member mismatch")));
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
        let mut chosen = None;
        for (seq, path) in snapshots.iter().rev() {
            match read_checkpoint(path, *seq, &initial.ring, ignore) {
                Ok(value) => {
                    chosen = Some(value);
                    break;
                }
                Err(error) => quarantine(path, &error.to_string())?,
            }
        }
        let (mut state, mut sequence, second, snapshot_bytes) = if let Some(value) = chosen {
            value
        } else {
            let first_journal = journals.first().map(|(sequence, _)| *sequence);
            if first_journal != Some(1)
                && (local.initialized || !snapshots.is_empty() || !journals.is_empty())
            {
                prepare_recovery(directory, &mut ownership, &mut local, initial.clone())?;
                drop(ownership);
                return Self::open(directory, initial, retain, member);
            }
            let checkpoint = Checkpoint {
                sequence: 0,
                second: common::time::TimeSec::now().number(),
                state: initial.clone(),
            };
            let bytes = checkpoint.bytes()?;
            if journals.is_empty() && local.latest_journal == 0 {
                let first = named(directory, "journal", 1);
                publish(&first, &journal_header_after(&initial, 1))?;
                local.latest_journal = 1;
                append_identity(&mut ownership, &local)?;
            }
            publish(&named(directory, "snapshot", 0), &bytes)?;
            (initial.clone(), 0, checkpoint.second, bytes.len() as u64)
        };
        // Files cannot establish ownership of a voting identity. Keep their committed
        // data only, then begin a fresh slotless applicant's local journal.
        if (identity_missing || !local.initialized)
            && (!snapshots.is_empty() || !journals.is_empty())
        {
            prepare_recovery(directory, &mut ownership, &mut local, state)?;
            drop(ownership);
            return Self::open(directory, initial, retain, member);
        }
        state.ignore_ring_identity = ignore;
        state.check_ring(&state.snapshot)?;
        state.validate()?;
        let snapshot_sequence = sequence;
        let active_start = local.latest_journal;
        let active_path = named(directory, "journal", active_start);
        let active_exists = active_start != 0 && active_path.try_exists()?;
        let mut all_journals = numbered(directory, "journal")?;
        let last_start = all_journals.last().map(|(start, _)| *start);
        let mut lost = !active_exists || last_start != Some(active_start);
        if active_exists {
            match check_journal_header(&mut File::open(&active_path)?, &state.ring, ignore) {
                Ok(start) if start == active_start && start > snapshot_sequence => {}
                _ => {
                    quarantine(&active_path, "invalid latest journal header or position")?;
                    lost = true;
                    all_journals = numbered(directory, "journal")?;
                }
            }
        }
        let segments: Vec<_> = all_journals
            .into_iter()
            .filter(|(seq, _)| *seq > snapshot_sequence && *seq <= active_start)
            .collect();
        let mut active = None;
        let mut log_bytes = 0;
        for (start, path) in &segments {
            if *start
                != sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?
            {
                lost = true;
                break;
            }
            let is_active = *start == active_start;
            let replay = replay_segment(path, *start, &mut state, &mut sequence, is_active, ignore);
            match replay {
                Ok((file, size)) => {
                    log_bytes += size;
                    if is_active {
                        active = Some(file);
                    }
                }
                Err(error) => {
                    quarantine(path, &error.to_string())?;
                    lost = true;
                    break;
                }
            }
        }
        if lost {
            prepare_recovery(directory, &mut ownership, &mut local, state)?;
            drop(ownership);
            return Self::open(directory, initial, retain, member);
        }
        mark_loaded(&state);
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
        let active = active.ok_or_else(|| Error::new(format!("no active journal for {member}")))?;
        let disk_process = local.process.clone();
        let disk_ring = local.ring.clone();
        let active_bytes = active.metadata()?.len();
        let mut result = Self {
            identity: local,
            durable: state.clone(),
            state,
            directory: directory.into(),
            bootstrap_pending,
            disk: Arc::new(Disk {
                ownership: Mutex::new(ownership),
                file: Mutex::new(Arc::new(active)),
                process: disk_process,
                ring: disk_ring,
                timing: IoTiming::default(),
            }),
            io: None,
            pending_io: VecDeque::new(),
            active_start,
            active_bytes,
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
    pub fn start_io(&mut self, wake: impl Fn() + Send + Sync + 'static) -> Result<()> {
        if self.io.is_some() {
            return Err(Error::new("journal I/O already started"));
        }
        self.io = Some(crate::io::Queue::new(wake)?);
        Ok(())
    }
    pub fn fence(&self) -> Option<crate::io::Fence> {
        self.io.as_ref().map(|io| io.fence())
    }
    pub fn durable_state(&self) -> &State {
        &self.durable
    }
    pub fn poll_io(&mut self) -> Result<()> {
        let Some(io) = &self.io else { return Ok(()) };
        let completed = match io.fence().completed() {
            Ok(completed) => completed,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        while self.pending_io.front().is_some_and(|(ticket, _)| *ticket <= completed) {
            let change = self.pending_io.pop_front().unwrap().1;
            if let Err(error) = self.apply_durable(change) {
                self.failed = true;
                return Err(error);
            }
        }
        Ok(())
    }
    fn apply_durable(&mut self, change: DurableChange) -> Result<()> {
        match change {
            DurableChange::Records(records) => {
                mark_records(records.as_slice(), true);
                for record in records.iter() {
                    self.durable.apply_durable(record)?;
                }
            }
            DurableChange::Snapshot(snapshot) => {
                if snapshot.index >= self.durable.snapshot.index {
                    self.durable.log.retain(|entry| entry.index > snapshot.index);
                    self.durable.commit = self.durable.commit.max(snapshot.index);
                    if self.durable.ignore_ring_identity {
                        self.durable.ring = snapshot.ring.clone();
                    }
                    self.durable.snapshot = snapshot;
                    self.durable.validate()?;
                }
            }
            DurableChange::Replace(state) => self.durable = state,
        }
        Ok(())
    }
    fn track_change(&mut self, change: DurableChange) -> Result<()> {
        if let Some(io) = &self.io {
            self.pending_io.push_back((io.ticket(), change));
        } else {
            self.apply_durable(change)?;
        }
        Ok(())
    }
    fn perform(
        &mut self,
        kind: crate::io::Kind,
        job: impl FnOnce(&Disk) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        self.poll_io()?;
        let disk = self.disk.clone();
        let task = move || job(&disk);
        let result = if let Some(io) = &mut self.io {
            io.submit_kind(kind, task).map(|_| ())
        } else {
            task()
        };
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn perform_sync(
        &mut self,
        kind: crate::io::Kind,
        job: impl FnOnce(Arc<Disk>) -> Result<crate::io::SyncRequest> + Send + 'static,
    ) -> Result<()> {
        self.poll_io()?;
        let disk = self.disk.clone();
        let task = move || job(disk);
        let result = if let Some(io) = &mut self.io {
            io.submit_sync_kind(kind, task).map(|_| ())
        } else {
            task().and_then(crate::io::SyncRequest::run)
        };
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn append_frame(
        &mut self,
        frame: JournalFrame,
        offset: u64,
        records: Arc<Vec<Record>>,
        identity: Option<LocalIdentity>,
    ) -> Result<()> {
        let kind =
            if identity.is_some() { crate::io::Kind::Identity } else { crate::io::Kind::Parallel };
        self.perform_sync(kind, move |disk| {
            let file = disk.current_file()?;
            let write_start = Instant::now();
            frame.write_at(&file, offset)?;
            let write_ns = write_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            mark_records(&records, false);
            let disk = disk.clone();
            Ok(crate::io::SyncRequest::new(file, move |sync_ns, batch_at| {
                disk.timing.record(write_ns, sync_ns, batch_at, &disk.process, &disk.ring);
                if let Some(identity) = identity {
                    let mut ownership = disk.lock_ownership()?;
                    append_identity(&mut ownership, &identity)?;
                }
                Ok(())
            }))
        })
    }
    fn history(&self) -> History {
        History {
            state: self.state.clone(),
            directory: self.directory.clone(),
            active_start: self.active_start,
        }
    }
    pub(crate) fn read_history<R: Send + 'static>(
        &mut self,
        f: impl FnOnce(&History) -> Result<R> + Send + 'static,
    ) -> Result<crate::io::Read<R>> {
        let history = self.history();
        if let Some(io) = &mut self.io {
            io.read(move || f(&history))
        } else {
            Ok(crate::io::Read::ready(f(&history)))
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn committed_term(&self, index: u64) -> Result<Option<u64>> {
        self.history().committed_tip(index).map(|tip| tip.map(|(term, _)| term))
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn committed_suffix(&self, after: u64, through: u64) -> Result<Option<CommittedJournal>> {
        self.history().committed_suffix(after, through)
    }
    pub fn import_committed(&mut self, copied: &CommittedJournal, through: u64) -> Result<()> {
        copied.validate(&self.state, through)?;
        let directory = self.directory.join("copied-journals");
        let root = self.directory.clone();
        let name = named(&directory, "journal", copied.after + 1);
        let mut bytes = encode(copied)?;
        bytes.extend(scn_footer(copied.largest_scn()));
        self.perform(crate::io::Kind::Parallel, move |_| {
            std::fs::create_dir_all(&directory)?;
            sync_directory(&root)?;
            publish(&name, &bytes)
        })?;
        self.write(&[
            Record::Entries {
                from: copied.after + 1,
                entries: copied.entries.iter().map(|entry| (**entry).clone()).collect(),
            },
            Record::Commit(copied.entries.last().unwrap().index),
        ])
    }
    /// Fence voting durably before attempting a history replacement. This does
    /// not discard files until a verified replacement checkpoint is available.
    pub fn require_history_repair(&mut self) -> Result<()> {
        if self.identity.repairing {
            return Ok(());
        }
        self.identity.repairing = true;
        let identity = self.identity.clone();
        self.perform(crate::io::Kind::Identity, move |disk| {
            let mut ownership = disk.lock_ownership()?;
            append_identity(&mut ownership, &identity)
        })
    }
    pub fn repair_history(
        &mut self,
        snapshot: crate::Snapshot,
        proof: Option<(u64, Scn)>,
    ) -> Result<()> {
        snapshot.validate()?;
        self.state.check_ring(&snapshot)?;
        if !self.identity.repairing
            || proof != Some((snapshot.term, snapshot.scn))
            || snapshot.term > self.state.term
        {
            return Err(Error::new("history repair needs a verified primary checkpoint"));
        }
        let mut state = self.state.clone();
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::new("history generation exhausted"))?;
        state.commit = snapshot.index;
        state.snapshot = snapshot;
        state.log.clear();
        // Keep the SCN high water, but retire the old voting identity exactly
        // as for a corrupt journal. The replacement retains only verified history.
        state.max_scn = state.largest_scn();
        state.validate()?;
        let directory = self.directory.clone();
        let mut identity = self.identity.clone();
        retire_identity(&mut identity, &mut state)?;
        let mut accepted_identity = identity.clone();
        accepted_identity.latest_journal = 1;
        let checkpoint = Checkpoint {
            sequence: 0,
            second: common::time::TimeSec::now().number(),
            state: state.clone(),
        };
        let second = checkpoint.second;
        let bytes = checkpoint.bytes()?;
        let snapshot_bytes = bytes.len() as u64;
        self.perform(crate::io::Kind::Ordered, move |disk| {
            let name = format!("repair-{}.bin", common::ring::MemberIdentity::generate()?);
            publish(&directory.join(&name), &bytes)?;
            let files: Vec<_> = numbered(&directory, "snapshot")?
                .into_iter()
                .chain(numbered(&directory, "journal")?)
                .map(|(_, path)| path.file_name().unwrap().to_string_lossy().into_owned())
                .collect();
            identity.recovery =
                Some(RecoveryPlan { archive: format!("rejected-{name}"), checkpoint: name, files });
            // The durable plan makes interrupted copying/renames restartable.
            let mut ownership = disk.lock_ownership()?;
            append_identity(&mut ownership, &identity)?;
            finish_recovery(&directory, &mut ownership, &mut identity)?;
            drop(ownership);
            disk.replace_file(
                OpenOptions::new().read(true).write(true).open(named(&directory, "journal", 1))?,
            )?;
            identity.repairing = false;
            let mut ownership = disk.lock_ownership()?;
            append_identity(&mut ownership, &identity)
        })?;
        self.state = state;
        self.identity = accepted_identity;
        self.identity.repairing = false;
        self.sequence = 0;
        self.active_start = 1;
        self.change_sequence = 0;
        self.snapshot_sequence = 0;
        self.snapshot_second = second;
        self.snapshot_bytes = snapshot_bytes;
        self.log_bytes = HEADER;
        self.active_bytes = HEADER;
        self.track_change(DurableChange::Replace(self.state.clone()))?;
        Ok(())
    }
    pub fn needs_bootstrap(&self) -> bool {
        self.bootstrap_pending
    }
    pub fn finish_bootstrap(&mut self) -> Result<()> {
        let directory = self.directory.clone();
        self.perform(crate::io::Kind::Ordered, move |_| {
            std::fs::remove_file(directory.join("need-bootstrap"))?;
            sync_directory(&directory)
        })?;
        self.bootstrap_pending = false;
        Ok(())
    }
    pub fn write(&mut self, records: &[Record]) -> Result<()> {
        self.write_owned(records.to_vec())
    }
    pub fn write_owned(&mut self, mut held: Vec<Record>) -> Result<()> {
        if self.failed {
            return Err(Error::new(format!(
                "{} journal fenced after I/O failure; restart required",
                self.identity.process
            )));
        }
        if held.is_empty() {
            return Ok(());
        }
        // Keep one immutable serialization per entry for state, apply, and journal I/O.
        for record in &mut held {
            if let Record::Entries { entries, .. } = record {
                for entry in entries {
                    entry.entry_buf = Some(codec::entry_buffer(entry)?);
                    if let crate::raft::Command::Client(actions) = &mut entry.command {
                        *actions = Vec::new();
                    }
                }
            }
        }
        if let Some(from) = held.iter().find_map(|record| match record {
            Record::Entries { from, entries }
                if !entries.is_empty() && *from <= self.state.last() =>
            {
                Some(*from)
            }
            _ => None,
        }) {
            return self.replace_suffix(held, from);
        }
        let sequence =
            self.sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        let batch = Batch { sequence, records: held };
        let frame = journal_frame(&batch)?;
        let frame_len = frame.len() as u64;
        let durable_records = Arc::new(batch.records);
        // Apply against the live state with a small undo log. The entry log is
        // copy-on-write, so this retains only the prior log root and new entries.
        let undo = self.state.apply_journal_batch(durable_records.as_slice())?;
        let mut identity = None;
        if durable_records.iter().any(|record| matches!(record, Record::Bootstrap(_) | Record::Install(_))
            || matches!(record, Record::Entries { entries, .. } if entries.iter().any(|e| matches!(e.command, crate::raft::Command::Members(_))))) {
            let mut updated = self.identity.clone();
            let membership = self.state.membership();
            let slot = membership.slot(&self.identity.current);
            let accepted = slot.is_some_and(|id| membership.members().contains(&id))
                && self.identity.pending.as_ref().is_none_or(|r| !membership.members().contains(&r.previous));
            updated.slot = slot.or(updated.slot);
            updated.membership = Some(membership);
            updated.founders = self.state.founders.clone();
            if accepted { updated.pending = None; }
            identity = Some(updated);
        }
        let write_identity = identity.clone();
        let offset = self.active_bytes;
        let next_offset =
            offset.checked_add(frame_len).ok_or_else(|| Error::new("journal offset overflow"))?;
        if let Err(error) =
            self.append_frame(frame, offset, durable_records.clone(), write_identity)
        {
            undo.rollback(&mut self.state);
            return Err(error);
        }
        self.active_bytes = next_offset;
        self.sequence = sequence;
        self.change_sequence = sequence;
        self.log_bytes += frame_len;
        if let Some(identity) = identity {
            self.identity = identity;
        }
        self.track_change(DurableChange::Records(durable_records))?;
        Ok(())
    }
    fn replace_suffix(&mut self, records: Vec<Record>, from: u64) -> Result<()> {
        let directory = self.directory.clone();
        let oldest = numbered(&directory, "snapshot")?
            .first()
            .map(|(sequence, _)| *sequence)
            .ok_or_else(|| Error::new("missing checkpoint for journal suffix rewrite"))?;
        let undo = self.state.apply_journal_batch(&records)?;
        let maximum = self.state.largest_scn();
        let watermark = maximum.map(Record::MaxScn).into_iter().collect::<Vec<_>>();
        let watermark_undo = match self.state.apply_journal_batch(&watermark) {
            Ok(undo) => undo,
            Err(error) => {
                undo.rollback(&mut self.state);
                return Err(error);
            }
        };
        let mut durable = records.clone();
        durable.extend(watermark.iter().cloned());
        let durable = Arc::new(durable);
        let mut identity = None;
        if durable.iter().any(|record| matches!(record, Record::Bootstrap(_) | Record::Install(_))
            || matches!(record, Record::Entries { entries, .. } if entries.iter().any(|e| matches!(e.command, crate::raft::Command::Members(_))))) {
            let mut updated = self.identity.clone();
            let membership = self.state.membership();
            let slot = membership.slot(&self.identity.current);
            let accepted = slot.is_some_and(|id| membership.members().contains(&id))
                && self.identity.pending.as_ref().is_none_or(|r| !membership.members().contains(&r.previous));
            updated.slot = slot.or(updated.slot);
            updated.membership = Some(membership);
            updated.founders = self.state.founders.clone();
            if accepted { updated.pending = None; }
            identity = Some(updated);
        }

        let first = match self.sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                watermark_undo.rollback(&mut self.state);
                undo.rollback(&mut self.state);
                return Err(Error::new("journal sequence overflow"));
            }
        };
        let mut batches = vec![Batch { sequence: first, records }];
        if let Some(record) = watermark.into_iter().next() {
            let sequence = match first.checked_add(1) {
                Some(sequence) => sequence,
                None => {
                    watermark_undo.rollback(&mut self.state);
                    undo.rollback(&mut self.state);
                    return Err(Error::new("journal sequence overflow"));
                }
            };
            batches.push(Batch { sequence, records: vec![record] });
        }
        let ring = self.state.ring.clone();
        let ignore = self.state.ignore_ring_identity;
        let latest = self.snapshot_sequence;
        let active_start = self.active_start;
        let old_sequence = self.sequence;
        let stats = Arc::new(Mutex::new(None));
        let write_stats = stats.clone();
        let rollback_identity = self.identity.clone();
        let write_identity = identity.clone().unwrap_or_else(|| self.identity.clone());
        let durable_write = durable.clone();
        let result = self.perform(crate::io::Kind::Ordered, move |disk| {
            let (active_bytes, log_bytes, final_sequence, active_start, identity) =
                rewrite_journals(
                    &directory,
                    &ring,
                    ignore,
                    oldest,
                    latest,
                    active_start,
                    old_sequence,
                    from,
                    batches,
                    rollback_identity,
                    write_identity,
                    disk,
                )?;
            mark_records(durable_write.as_slice(), false);
            *write_stats.lock().map_err(|_| Error::new("journal rewrite stats poisoned"))? =
                Some((active_bytes, log_bytes, final_sequence, active_start, identity));
            Ok(())
        });
        if let Err(error) = result {
            watermark_undo.rollback(&mut self.state);
            undo.rollback(&mut self.state);
            return Err(error);
        }
        self.track_change(DurableChange::Records(durable.clone()))?;
        if let Some(io) = &self.io {
            if let Err(error) = io.fence().wait().and_then(|()| self.poll_io()) {
                self.failed = true;
                watermark_undo.rollback(&mut self.state);
                undo.rollback(&mut self.state);
                return Err(error);
            }
        }
        let saved = stats.lock().map_err(|_| Error::new("journal rewrite stats poisoned"))?.clone();
        let Some((active_bytes, log_bytes, sequence, active_start, identity)) = saved else {
            self.failed = true;
            watermark_undo.rollback(&mut self.state);
            undo.rollback(&mut self.state);
            return Err(Error::new("journal suffix rewrite did not complete"));
        };
        self.active_bytes = active_bytes;
        self.active_start = active_start;
        self.log_bytes = log_bytes;
        self.sequence = sequence;
        self.change_sequence = sequence;
        self.identity = identity;
        Ok(())
    }
    pub fn snapshot_due(&self, min_bytes: u64, seconds: u64, now: u64) -> bool {
        self.change_sequence > self.snapshot_sequence
            && (self.log_bytes >= min_bytes.max(self.snapshot_bytes)
                || now.saturating_sub(self.snapshot_second) >= seconds)
    }
    pub fn prepare_snapshot(&mut self, now: u64) -> Result<Option<PendingSnapshot>> {
        if self.failed {
            return Err(Error::new(format!("{} journal fenced", self.identity.process)));
        }
        if self.change_sequence <= self.snapshot_sequence {
            return Ok(None);
        }
        let mut state = self.state.clone();
        let snapshot = state.capture();
        let mark = self.begin_checkpoint(&state, &snapshot)?;
        state.snapshot = snapshot;
        state.log.retain(|entry| entry.index > state.snapshot.index);
        self.prepare_checkpoint(now, state, mark)
    }
    /// Mark a state capture and carry its log suffix into the next journal segment.
    pub fn begin_checkpoint(
        &mut self,
        state: &State,
        snapshot: &Snapshot,
    ) -> Result<CheckpointMark> {
        if self.failed {
            return Err(Error::new(format!("{} journal fenced", self.identity.process)));
        }
        let mark = CheckpointMark { sequence: self.sequence, log_bytes: self.log_bytes };
        let start =
            self.sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        snapshot.validate()?;
        if snapshot.index > state.commit
            || state.history_at(snapshot.index) != Some((snapshot.term, snapshot.scn))
        {
            return Err(Error::new("checkpoint does not match captured Raft history"));
        }
        let suffix: Vec<_> =
            state.log.iter().filter(|entry| entry.index > snapshot.index).cloned().collect();
        let carried = if suffix.is_empty() {
            vec![]
        } else {
            frame(&Batch {
                sequence: start,
                records: vec![Record::Carry { entries: suffix, commit: state.commit }],
            })?
        };
        self.rotate(start, &carried)?;
        if !carried.is_empty() {
            self.sequence = start;
            self.log_bytes += carried.len() as u64;
        }
        Ok(mark)
    }
    /// Write captured Raft state; its log suffix lives in the journal after the checkpoint.
    pub fn prepare_checkpoint(
        &mut self,
        now: u64,
        mut state: State,
        mark: CheckpointMark,
    ) -> Result<Option<PendingSnapshot>> {
        if self.failed {
            return Err(Error::new(format!("{} journal fenced", self.identity.process)));
        }
        if mark.sequence <= self.snapshot_sequence {
            return Ok(None);
        }
        if mark.sequence > self.sequence {
            return Err(Error::new("checkpoint is ahead of journal"));
        }
        if state.ignore_ring_identity {
            state.ring.clone_from(&self.identity.ring);
            state.snapshot.ring.clone_from(&self.identity.ring);
        }
        state.log.clear();
        state.commit = state.snapshot.index;
        state.ignore_ring_identity = false;
        state.validate()?;
        let snapshot = state.snapshot.clone();
        let checkpoint = Checkpoint { sequence: mark.sequence, second: now, state };
        Ok(Some(PendingSnapshot {
            path: named(&self.directory, "snapshot", mark.sequence),
            checkpoint,
            written_bytes: std::sync::atomic::AtomicU64::new(0),
            sequence: mark.sequence,
            second: now,
            snapshot,
            log_bytes: mark.log_bytes,
        }))
    }
    /// Create this member's disk checkpoint from a peer's application snapshot.
    /// The peer sends no Raft state; local term, vote and log suffix stay local.
    pub fn prepare_copy(
        &mut self,
        now: u64,
        snapshot: crate::raft::Snapshot,
    ) -> Result<Option<PendingSnapshot>> {
        if self.failed {
            return Err(Error::new(format!("{} journal fenced", self.identity.process)));
        }
        snapshot.validate()?;
        self.state.check_ring(&snapshot)?;
        if snapshot.index < self.state.snapshot.index
            || snapshot.index > self.state.commit
            || self.state.history_at(snapshot.index) != Some((snapshot.term, snapshot.scn))
        {
            return Err(Error::new("checkpoint does not match local committed history"));
        }
        let mut state = self.state.clone();
        state.snapshot = snapshot.clone();
        let mark = self.begin_checkpoint(&state, &snapshot)?;
        self.prepare_checkpoint(now, state, mark)
    }
    /// Close the old segment before creating its successor. Identity points at
    /// the latest complete file; an unrecorded successor is detected at open.
    fn rotate(&mut self, start: u64, carried: &[u8]) -> Result<()> {
        if start == self.active_start {
            // Retrying a snapshot without intervening writes needs no new segment.
            return Ok(());
        }
        let directory = self.directory.clone();
        let active_bytes = self.active_bytes;
        let bytes = [journal_header_after(&self.state, start), carried.to_vec()].concat();
        let footer = scn_footer(self.state.largest_scn());
        let mut identity = self.identity.clone();
        identity.latest_journal = start;
        let saved_identity = identity.clone();
        self.perform(crate::io::Kind::Ordered, move |disk| {
            let next = named(&directory, "journal", start);
            if next.try_exists()? {
                return Err(Error::new("next journal file already exists"));
            }
            let mut offset = active_bytes;
            let file = disk.current_file()?;
            write_all_at(&file, &footer, &mut offset)?;
            file.sync_all()?;
            let mut next_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&next)?;
            next_file.write_all(&bytes)?;
            next_file.sync_all()?;
            sync_directory(&directory)?;
            let mut ownership = disk.lock_ownership()?;
            append_identity(&mut ownership, &identity)?;
            drop(ownership);
            disk.replace_file(next_file)?;
            Ok(())
        })?;
        self.identity = saved_identity;
        self.active_start = start;
        self.active_bytes = HEADER + carried.len() as u64;
        self.log_bytes += HEADER + SCN_FOOTER;
        Ok(())
    }
    pub fn finish_snapshot(&mut self, pending: &PendingSnapshot) -> Result<()> {
        if pending.sequence <= self.snapshot_sequence {
            return Ok(());
        }
        let snapshot = &pending.snapshot;
        // New entries, commits and terms may have arrived while the file was written.
        let adopted = snapshot.index >= self.state.snapshot.index
            && self.state.history_at(snapshot.index) == Some((snapshot.term, snapshot.scn));
        if adopted {
            self.state.log.retain(|e| e.index > snapshot.index);
            if self.state.ignore_ring_identity {
                self.state.ring = snapshot.ring.clone();
            }
            self.state.snapshot = snapshot.clone();
        }
        self.snapshot_sequence = pending.sequence;
        self.snapshot_second = pending.second;
        self.snapshot_bytes = pending.written_bytes.load(std::sync::atomic::Ordering::SeqCst);
        self.log_bytes = self.log_bytes.saturating_sub(pending.log_bytes);
        self.prune()?;
        if adopted {
            self.track_change(DurableChange::Snapshot(snapshot.clone()))?;
        }
        Ok(())
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn snapshot(&mut self, now: u64) -> Result<()> {
        if let Some(pending) = self.prepare_snapshot(now)? {
            pending.write()?;
            self.finish_snapshot(&pending)?;
        }
        Ok(())
    }
    fn prune(&mut self) -> Result<()> {
        let directory = self.directory.clone();
        let retain = self.retain;
        self.perform(crate::io::Kind::Ordered, move |_| {
            let snapshots = numbered(&directory, "snapshot")?;
            let remove = snapshots.len().saturating_sub(retain);
            let Some((oldest, _)) = snapshots.get(remove) else {
                return Ok(());
            };
            for (_, path) in &snapshots[..remove] {
                std::fs::remove_file(path)?;
            }
            sync_directory(&directory)?;
            for (start, path) in numbered(&directory, "journal")? {
                if start <= *oldest {
                    std::fs::remove_file(path)?;
                }
            }
            sync_directory(&directory)
        })
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn fail_writes(&mut self) {
        *self.disk.file.lock().unwrap() =
            Arc::new(File::open(self.directory.join("identity")).unwrap());
    }
}
pub(crate) struct History {
    state: State,
    directory: PathBuf,
    active_start: u64,
}
impl History {
    // Replays retained files with the same sequence and predecessor checks as restart.
    fn retained_history(&self, after: u64) -> Result<Option<(State, Option<(u64, Scn)>)>> {
        let mut state = self.state.clone();
        let mut found = state.history_at(after);
        if after < state.snapshot.index {
            // Retained closed journals can outlive their in-memory entries.
            let mut base = None;
            for (sequence, path) in numbered(&self.directory, "snapshot")?.into_iter().rev() {
                if let Ok(value) =
                    read_checkpoint(&path, sequence, &state.ring, state.ignore_ring_identity)
                {
                    if value.0.snapshot.index <= after {
                        base = Some(value);
                        break;
                    }
                }
            }
            let Some((mut restored, mut sequence, _, _)) = base else {
                return Ok(None);
            };
            let files: Vec<_> = numbered(&self.directory, "journal")?
                .into_iter()
                .filter(|(start, _)| *start > sequence && *start <= self.active_start)
                .collect();
            for (start, path) in files {
                if start != sequence + 1 {
                    return Ok(None);
                }
                if let Some(term) = restored.history_at(after) {
                    found = Some(term);
                }
                let mut proof = (after, found);
                replay_segment_checked(
                    &path,
                    start,
                    &mut restored,
                    &mut sequence,
                    start == self.active_start,
                    state.ignore_ring_identity,
                    Some(&mut proof),
                )?;
                found = proof.1;
            }
            if restored.last() != self.state.last()
                || restored.history_at(self.state.last())
                    != self.state.history_at(self.state.last())
                || restored.commit != self.state.commit
            {
                return Err(Error::new("archived journals differ from current history"));
            }
            if let Some(term) = restored.history_at(after) {
                found = Some(term);
            }
            state = restored;
        }
        Ok(Some((state, found)))
    }
    pub fn committed_tip(&self, index: u64) -> Result<Option<(u64, Scn)>> {
        if index > self.state.commit {
            return Ok(None);
        }
        if let Some(term) = self.state.history_at(index) {
            return Ok(Some(term));
        }
        Ok(self.retained_history(index)?.and_then(|(_, term)| term))
    }
    pub fn committed_suffix(&self, after: u64, through: u64) -> Result<Option<CommittedJournal>> {
        let Some((state, _)) = self.retained_history(after)? else { return Ok(None) };
        if after < state.snapshot.index || after >= state.commit.min(through) {
            return Ok(None);
        }
        Ok(Some(CommittedJournal {
            ring: state.ring.clone(),
            after,
            previous_term: state
                .term_at(after)
                .ok_or_else(|| Error::new("missing journal prefix"))?,
            previous_scn: state
                .scn_at(after)
                .ok_or_else(|| Error::new("missing journal prefix SCN"))?,
            entries: state
                .log
                .shared_iter()
                .filter(|e| e.index > after && e.index <= state.commit.min(through))
                .take(128)
                .cloned()
                .collect(),
        }))
    }
}
// Rename, do not delete: the suffix excludes the file from future selection and pruning.
fn quarantine(path: &Path, reason: &str) -> Result<()> {
    let mut number = 0u64;
    let destination = loop {
        let candidate = path.with_file_name(format!(
            "{}.corrupt-{number}",
            path.file_name().unwrap().to_string_lossy()
        ));
        if !candidate.try_exists()? {
            break candidate;
        }
        number += 1;
    };
    std::fs::rename(path, &destination)?;
    sync_directory(path.parent().unwrap())?;
    eprintln!("preserved corrupt Raft file {}: {reason}", destination.display());
    Ok(())
}
fn read_checkpoint(
    path: &Path,
    sequence: u64,
    ring: &str,
    ignore: bool,
) -> Result<(State, u64, u64, u64)> {
    let maximum = file_scn(path)?;
    let bytes = std::fs::read(path)?;
    let mut checkpoint = Checkpoint::decode(&bytes)?;
    if checkpoint.state.largest_scn() != maximum {
        return Err(Error::new("snapshot SCN changed while reading"));
    }
    if checkpoint.sequence != sequence || (!ignore && checkpoint.state.ring != ring) {
        return Err(Error::new("snapshot ring or sequence identity mismatch"));
    }
    if ignore {
        checkpoint.state.ring = ring.to_owned();
        checkpoint.state.snapshot.ring = ring.to_owned();
    }
    checkpoint.state.ignore_ring_identity = ignore;
    checkpoint.state.check_ring(&checkpoint.state.snapshot)?;
    checkpoint.state.validate()?;
    Ok((checkpoint.state, sequence, checkpoint.second, bytes.len() as u64))
}
/// A closed-segment footer retains the SCN high-water after entries are removed.
fn apply_scn_footer(state: &mut State, maximum: Option<Scn>) -> Result<()> {
    match (state.largest_scn(), maximum) {
        (Some(found), Some(maximum)) if found.compare(maximum).is_gt() => {
            Err(Error::new("journal SCN footer is below its contents"))
        }
        (Some(_), None) => Err(Error::new("journal SCN footer omits its high-water mark")),
        (_, Some(maximum)) => state.apply(&Record::MaxScn(maximum)),
        (None, None) => Ok(()),
    }
}
fn keep_before(entries: Vec<crate::raft::Entry>, from: u64) -> Vec<crate::raft::Entry> {
    entries.into_iter().take_while(|entry| entry.index < from).collect()
}
fn filter_suffix(record: Record, from: u64) -> Option<Record> {
    match record {
        Record::Entries { entries, .. } => {
            let entries = keep_before(entries, from);
            let first = entries.first()?.index;
            Some(Record::Entries { from: first, entries })
        }
        Record::Carry { entries, commit } => {
            Some(Record::Carry { entries: keep_before(entries, from), commit })
        }
        record => Some(record),
    }
}
/// Rewrite the affected suffix into the first segment that contains it. The
/// identity pointer moves back before later segment files are removed.
fn rewrite_journals(
    directory: &Path,
    ring: &str,
    ignore: bool,
    oldest_snapshot: u64,
    latest_snapshot: u64,
    active_start: u64,
    old_sequence: u64,
    from: u64,
    appended: Vec<Batch>,
    mut identity: LocalIdentity,
    disk: &Disk,
) -> Result<(u64, u64, u64, u64, LocalIdentity)> {
    let checkpoint = named(directory, "snapshot", oldest_snapshot);
    let (mut state, _, _, _) = read_checkpoint(&checkpoint, oldest_snapshot, ring, ignore)?;
    let files: Vec<_> = numbered(directory, "journal")?
        .into_iter()
        .filter(|(start, _)| *start > oldest_snapshot && *start <= active_start)
        .collect();
    if files.last().is_none_or(|(start, _)| *start != active_start) {
        return Err(Error::new("identity latest journal is missing during suffix rewrite"));
    }
    let target_at = suffix_segment(&files, latest_snapshot, active_start, ring, ignore, from)?
        .ok_or_else(|| Error::new("replacement suffix is missing from journal files"))?;
    let mut next =
        oldest_snapshot.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
    let mut log_bytes = 0u64;
    let mut sequence = oldest_snapshot;
    for (start, path) in files.iter().take(target_at) {
        if *start != next {
            return Err(Error::new("journal sequence gap during suffix rewrite"));
        }
        let (_, size) = replay_segment(path, *start, &mut state, &mut sequence, false, ignore)?;
        next = sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        if *start > latest_snapshot {
            log_bytes = log_bytes
                .checked_add(size)
                .ok_or_else(|| Error::new("journal byte count overflow"))?;
        }
    }
    let (target_start, target_path) = &files[target_at];
    if *target_start != next {
        return Err(Error::new("journal sequence gap at suffix rewrite target"));
    }
    let temporary =
        target_path.with_extension(format!("{}.tmp", common::shamir::hex(&random_bytes()?)));
    let result = (|| -> Result<(u64, u64, u64)> {
        let mut output =
            OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temporary)?;
        output.write_all(&journal_header_after(&state, *target_start))?;
        let mut out_at = HEADER;
        for (start, path) in files.iter().skip(target_at) {
            if *start != next {
                return Err(Error::new("journal sequence gap during suffix rewrite"));
            }
            let is_active = *start == active_start;
            let mut input = OpenOptions::new().read(true).open(path)?;
            let (found_start, _, _, _) = read_journal_header(&mut input, ring, ignore)?;
            if found_start != *start {
                return Err(Error::new("journal filename/header sequence mismatch during rewrite"));
            }
            let (size, footer) = journal_body(&mut input, is_active)?;
            input.seek(SeekFrom::Start(HEADER))?;
            let mut in_at = HEADER;
            while in_at < size {
                let remaining = size - in_at;
                if remaining < 16 {
                    return Err(Error::new("partial journal frame during suffix rewrite"));
                }
                let mut lengths = [0; 16];
                input.read_exact(&mut lengths)?;
                let length = u64::from_le_bytes(lengths[..8].try_into().unwrap());
                let inverse = u64::from_le_bytes(lengths[8..].try_into().unwrap());
                if inverse != !length || length < 32 || length > LIMIT as u64 {
                    return Err(Error::new("corrupt journal frame during suffix rewrite"));
                }
                if remaining < 16 + length {
                    return Err(Error::new("partial journal frame during suffix rewrite"));
                }
                let mut bytes = vec![0; length as usize];
                input.read_exact(&mut bytes)?;
                let old: Batch = decode(&bytes)?;
                if old.sequence
                    != sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::new("journal sequence overflow"))?
                {
                    return Err(Error::new("journal sequence gap during suffix rewrite"));
                }
                sequence = old.sequence;
                let mut rewritten = Batch { sequence, records: vec![] };
                for record in old.records {
                    if let Some(record) = filter_suffix(record, from) {
                        rewritten.records.push(record);
                    }
                }
                for record in &rewritten.records {
                    state.apply(record)?;
                }
                state.validate()?;
                let encoded = frame(&rewritten)?;
                output.write_all(&encoded)?;
                out_at += encoded.len() as u64;
                in_at += 16 + length;
            }
            if let Some(maximum) = footer {
                apply_scn_footer(&mut state, maximum)?;
            }
            next =
                sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?;
        }
        if sequence != old_sequence {
            return Err(Error::new("journal sequence changed during suffix rewrite"));
        }
        for batch in &appended {
            if batch.sequence
                != sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?
            {
                return Err(Error::new("replacement journal sequence is not contiguous"));
            }
            for record in &batch.records {
                state.apply(record)?;
            }
            state.validate()?;
            let encoded = journal_frame(batch)?;
            encoded.write_at(&output, out_at)?;
            out_at += encoded.len() as u64;
            sequence = batch.sequence;
        }
        output.sync_all()?;
        drop(output);
        std::fs::rename(&temporary, target_path)?;
        sync_directory(directory)?;

        identity.latest_journal = *target_start;
        let mut ownership = disk.lock_ownership()?;
        append_identity(&mut ownership, &identity)?;
        drop(ownership);

        for (start, path) in numbered(directory, "journal")? {
            if start > *target_start {
                std::fs::remove_file(path)?;
            }
        }
        sync_directory(directory)?;
        disk.replace_file(OpenOptions::new().read(true).write(true).open(target_path)?)?;
        if *target_start > latest_snapshot
            || files.iter().skip(target_at + 1).any(|(start, _)| *start > latest_snapshot)
        {
            log_bytes = log_bytes
                .checked_add(out_at)
                .ok_or_else(|| Error::new("journal byte count overflow"))?;
        }
        state.validate()?;
        Ok((out_at, log_bytes, sequence))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    let (active_bytes, log_bytes, final_sequence) = result?;
    Ok((active_bytes, log_bytes, final_sequence, *target_start, identity))
}

fn suffix_segment(
    files: &[(u64, PathBuf)],
    after: u64,
    active_start: u64,
    ring: &str,
    ignore: bool,
    from: u64,
) -> Result<Option<usize>> {
    for (at, (start, path)) in files.iter().enumerate() {
        if *start <= after {
            continue;
        }
        let mut input = OpenOptions::new().read(true).open(path)?;
        let (found_start, _, _, _) = read_journal_header(&mut input, ring, ignore)?;
        if found_start != *start {
            return Err(Error::new("journal filename/header sequence mismatch during suffix scan"));
        }
        let (size, _) = journal_body(&mut input, *start == active_start)?;
        input.seek(SeekFrom::Start(HEADER))?;
        let mut offset = HEADER;
        while offset < size {
            let remaining = size - offset;
            if remaining < 16 {
                return Err(Error::new("partial journal frame during suffix scan"));
            }
            let mut lengths = [0; 16];
            input.read_exact(&mut lengths)?;
            let length = u64::from_le_bytes(lengths[..8].try_into().unwrap());
            let inverse = u64::from_le_bytes(lengths[8..].try_into().unwrap());
            if inverse != !length || length < 32 || length > LIMIT as u64 || remaining < 16 + length
            {
                return Err(Error::new("corrupt journal frame during suffix scan"));
            }
            let mut bytes = vec![0; length as usize];
            input.read_exact(&mut bytes)?;
            let batch: Batch = decode(&bytes)?;
            if batch.records.iter().any(|record| match record {
                Record::Entries { entries, .. } | Record::Carry { entries, .. } => {
                    entries.iter().any(|entry| entry.index >= from)
                }
                _ => false,
            }) {
                return Ok(Some(at));
            }
            offset += 16 + length;
        }
    }
    Ok(None)
}

fn journal_body(file: &mut File, active: bool) -> Result<(u64, Option<Option<Scn>>)> {
    let physical = file.metadata()?.len();
    if physical < HEADER + SCN_FOOTER {
        if active {
            return Ok((physical, None));
        }
        return Err(Error::new("short closed journal SCN footer"));
    }
    file.seek(SeekFrom::End(-(SCN_FOOTER as i64)))?;
    let mut bytes = [0; SCN_FOOTER as usize];
    file.read_exact(&mut bytes)?;
    if &bytes[..8] == SCN_MAGIC {
        let maximum = parse_scn_footer(&bytes)?;
        Ok((physical - SCN_FOOTER, Some(maximum)))
    } else if active {
        Ok((physical, None))
    } else {
        Err(Error::new("missing closed journal SCN footer"))
    }
}
fn replay_segment(
    path: &Path,
    start: u64,
    state: &mut State,
    sequence: &mut u64,
    active: bool,
    ignore: bool,
) -> Result<(File, u64)> {
    replay_segment_checked(path, start, state, sequence, active, ignore, None)
}
fn replay_segment_checked(
    path: &Path,
    start: u64,
    state: &mut State,
    sequence: &mut u64,
    active: bool,
    ignore: bool,
    mut proof: Option<&mut (u64, Option<(u64, Scn)>)>,
) -> Result<(File, u64)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let (found_start, previous, previous_term, previous_scn) =
        read_journal_header(&mut file, &state.ring, ignore)?;
    if found_start != start {
        return Err(Error::new("journal filename/header sequence mismatch"));
    }
    let mut boundary = state.last() == previous
        && state.history_at(previous) == Some((previous_term, previous_scn));
    // Starting from a compact checkpoint can omit the carried uncommitted suffix.
    let can_restore_carry = state.last() == state.snapshot.index && state.last() < previous;
    if !boundary && !can_restore_carry {
        return Err(Error::new("journal predecessor index/term mismatch"));
    }
    let physical_size = file.metadata()?.len();
    let footer = if physical_size >= HEADER + SCN_FOOTER {
        file.seek(SeekFrom::End(-(SCN_FOOTER as i64)))?;
        let mut bytes = [0; SCN_FOOTER as usize];
        file.read_exact(&mut bytes)?;
        if &bytes[..8] == SCN_MAGIC {
            Some(parse_scn_footer(&bytes)?)
        } else if active {
            None
        } else {
            return Err(Error::new("missing closed journal SCN footer"));
        }
    } else if active {
        None
    } else {
        return Err(Error::new("short closed journal SCN footer"));
    };
    let size = physical_size - if footer.is_some() { SCN_FOOTER } else { 0 };
    file.seek(SeekFrom::Start(HEADER))?;
    let mut offset = HEADER;
    while offset < size {
        let remaining = size - offset;
        if remaining < 16 {
            if active {
                break;
            }
            return Err(Error::new("partial closed journal"));
        }
        let mut lengths = [0; 16];
        file.read_exact(&mut lengths)?;
        if active && remaining < SCN_FOOTER && &lengths[..8] == SCN_MAGIC {
            break;
        }
        let length = u64::from_le_bytes(lengths[..8].try_into().unwrap());
        let inverse = u64::from_le_bytes(lengths[8..].try_into().unwrap());
        if inverse != !length || length < 32 || length > LIMIT as u64 {
            return Err(Error::new("corrupt journal frame length"));
        }
        if remaining < 16 + length {
            if active {
                break;
            }
            return Err(Error::new("partial closed journal frame"));
        }
        let mut bytes = vec![0; length as usize];
        file.read_exact(&mut bytes)?;
        let batch: Batch = decode(&bytes)?;
        if batch.sequence
            != sequence.checked_add(1).ok_or_else(|| Error::new("journal sequence overflow"))?
        {
            return Err(Error::new("journal sequence gap"));
        }
        let mut next = state.clone();
        if !boundary {
            match batch.records.as_slice() {
                [Record::Carry { entries, commit }]
                    if *commit >= state.snapshot.index
                        && *commit <= previous
                        && entries.last().is_some_and(|e| {
                            e.index == previous && e.term == previous_term && e.scn == previous_scn
                        }) => {}
                _ => return Err(Error::new("journal carried predecessor mismatch")),
            }
        }
        for record in &batch.records {
            if let Some((index, found)) = proof.as_deref_mut() {
                // An Install can replace history we cannot link. Never carry a proof
                // from that abandoned prefix through an unrelated checkpoint.
                if let Record::Install(snapshot) = record {
                    if snapshot.index > *index
                        && next.history_at(snapshot.index) != Some((snapshot.term, snapshot.scn))
                    {
                        *found = None;
                    }
                }
            }
            next.apply(record)?;
            if let Some((index, found)) = proof.as_deref_mut() {
                if *index <= next.commit {
                    if let Some(term) = next.history_at(*index) {
                        *found = Some(term);
                    }
                }
            }
        }
        next.validate()?;
        if !boundary {
            if next.last() != previous
                || next.history_at(previous) != Some((previous_term, previous_scn))
            {
                return Err(Error::new("journal restored predecessor mismatch"));
            }
            boundary = true;
        }
        *state = next;
        *sequence = batch.sequence;
        offset += 16 + length;
    }
    if !boundary {
        return Err(Error::new("missing carried journal predecessor"));
    }
    if let Some(maximum) = footer {
        apply_scn_footer(state, maximum)?;
    }
    if active {
        file.set_len(offset)?;
        file.sync_all()?;
        file.seek(SeekFrom::End(0))?;
    }
    Ok((file, offset + if !active && footer.is_some() { SCN_FOOTER } else { 0 }))
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
fn retire_identity(local: &mut LocalIdentity, state: &mut State) -> Result<()> {
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
    state.founders = state.founders.clone().or_else(|| local.founders.clone());
    if let Some(retired) = &pending {
        state.apply(&Record::Retired(retired.clone()))?;
    }
    local.retired.push(local.current.clone());
    local.current = current;
    local.slot = pending.as_ref().map(|r| r.next);
    local.pending = pending;
    state.validate()?;
    Ok(())
}
fn prepare_recovery(
    directory: &Path,
    ownership: &mut File,
    local: &mut LocalIdentity,
    mut state: State,
) -> Result<()> {
    retire_identity(local, &mut state)?;
    let current = local.current.clone();
    let checkpoint_name = format!("recovery-{current}.bin");
    let checkpoint =
        Checkpoint { sequence: 0, second: common::time::TimeSec::now().number(), state };
    publish(&directory.join(&checkpoint_name), &checkpoint.bytes()?)?;
    let files: Vec<String> = numbered(directory, "snapshot")?
        .into_iter()
        .chain(numbered(directory, "journal")?)
        .map(|(_, path)| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
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
    let checkpoint = Checkpoint::decode(&bytes)?;
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
    publish(&named(directory, "journal", 1), &journal_header_after(&checkpoint.state, 1))?;
    local.latest_journal = 1;
    append_identity(ownership, local)?;
    publish(&named(directory, "snapshot", 0), &bytes)?;
    local.initialized = true;
    local.recovery = None;
    append_identity(ownership, local)?;
    Ok(())
}

pub struct PendingSnapshot {
    path: PathBuf,
    checkpoint: Checkpoint,
    written_bytes: std::sync::atomic::AtomicU64,
    sequence: u64,
    second: u64,
    snapshot: crate::raft::Snapshot,
    log_bytes: u64,
}
impl PendingSnapshot {
    pub fn write(&self) -> Result<()> {
        self.write_cancellable(|| false)
    }
    pub fn write_cancellable(&self, cancelled: impl Fn() -> bool) -> Result<()> {
        let bytes = self.checkpoint.bytes()?;
        publish_cancellable(&self.path, &bytes, cancelled)?;
        self.written_bytes.store(bytes.len() as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
fn frame(batch: &Batch) -> Result<Vec<u8>> {
    let bytes = encode(batch)?;
    let length = bytes.len() as u64;
    Ok([length.to_le_bytes().as_slice(), (!length).to_le_bytes().as_slice(), &bytes].concat())
}
fn journal_frame(batch: &Batch) -> Result<JournalFrame> {
    if let Some(frame) = shared_frame(batch)? {
        return Ok(JournalFrame::Shared(frame));
    }
    Ok(JournalFrame::Flat(frame(batch)?))
}
fn shared_frame(batch: &Batch) -> Result<Option<SharedFrame>> {
    let (from, entries, commit) = match batch.records.as_slice() {
        [Record::Entries { from, entries }] => (*from, entries, None),
        [Record::Entries { from, entries }, Record::Commit(commit)] => {
            (*from, entries, Some(*commit))
        }
        _ => return Ok(None),
    };
    if entries.is_empty() {
        return Ok(None);
    }
    let entries: Vec<_> = entries
        .iter()
        .map(|entry| {
            entry
                .entry_buf
                .clone()
                .ok_or_else(|| Error::new("journal entry has no shared serialization"))
        })
        .collect::<Result<_>>()?;
    let depth = entries.iter().map(|entry| entry.depth).max().unwrap_or(1).saturating_add(4);
    if depth > common::MAX_RECURSION_DEPTH {
        return Err(Error::new("Raft encoding exceeds recursion limit"));
    }

    // This prefix is the normal tagged Batch/Record/Entries-list path up to its
    // first entry. Entry bytes are separate shared pieces in the journal frame.
    let mut prefix = Vec::with_capacity(64);
    prefix.push(depth);
    put_u16(&mut prefix, common::Op::RaftBatch.code());
    put_u64(&mut prefix, batch.sequence);
    put_u16(&mut prefix, common::Op::RaftList.code());
    put_u64(&mut prefix, batch.records.len() as u64);
    put_u16(&mut prefix, common::Op::RaftRecordEntries.code());
    put_u64(&mut prefix, from);
    put_u16(&mut prefix, common::Op::RaftList.code());
    put_u64(&mut prefix, entries.len() as u64);

    let mut tail = Vec::with_capacity(42);
    if let Some(commit) = commit {
        put_u16(&mut tail, common::Op::RaftRecordCommit.code());
        put_u64(&mut tail, commit);
    }
    let entry_bytes = entries.iter().try_fold(0usize, |size, entry| {
        entry.bytes.check()?;
        size.checked_add(entry.bytes.len())
            .ok_or_else(|| Error::new("journal entry frame length overflow"))
    })?;
    let body_len = prefix
        .len()
        .checked_add(entry_bytes)
        .and_then(|size| size.checked_add(tail.len()))
        .and_then(|size| size.checked_add(32))
        .ok_or_else(|| Error::new("journal frame length overflow"))?;
    if body_len > LIMIT {
        return Err(Error::new("Raft persistence frame too large"));
    }
    let mut hash = Sha256::new();
    hash.update(&prefix);
    for entry in &entries {
        let mut at = 0;
        while at < entry.bytes.len() {
            let chunk = entry.bytes.chunk(at);
            if chunk.is_empty() {
                return Err(Error::new("empty Raft entry buffer segment"));
            }
            hash.update(chunk);
            at += chunk.len();
        }
    }
    hash.update(&tail);
    tail.extend(hash.finalize());

    let length = body_len as u64;
    let mut head = Vec::with_capacity(16 + prefix.len());
    head.extend(length.to_le_bytes());
    head.extend((!length).to_le_bytes());
    head.extend(prefix);
    Ok(Some(SharedFrame { head, entries, tail, length: 16 + body_len }))
}
fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend(value.to_le_bytes());
}
fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend(value.to_le_bytes());
}
fn encode<T: Wire>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = codec::encode(value)?;
    bytes.extend(Sha256::digest(&bytes));
    if bytes.len() > LIMIT {
        return Err(Error::new("Raft persistence frame too large"));
    }
    Ok(bytes)
}
fn decode<T: Wire>(bytes: &[u8]) -> Result<T> {
    let body =
        bytes.len().checked_sub(32).ok_or_else(|| Error::new("short Raft persistence frame"))?;
    if Sha256::digest(&bytes[..body]).as_slice() != &bytes[body..] {
        return Err(Error::new("Raft persistence checksum mismatch"));
    }
    codec::decode(&bytes[..body])
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
            .map_err(|_| Error::new("invalid numbered Raft file"))?;
        if entry.path() != named(directory, kind, sequence) || !entry.file_type()?.is_file() {
            return Err(Error::new("invalid numbered Raft file"));
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
    let temporary = path.with_extension(format!("{}.tmp", common::shamir::hex(&random_bytes()?)));
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
    fn shared_entry_frames_match_the_regular_wire_encoding() {
        let mut entry = Entry {
            scn: Scn::new(1),
            index: 1,
            term: 1,
            command: Command::Client(vec![crate::Action::new(
                common::Op::BytesResp,
                vec![37; 128 * 1024],
            )]),
            entry_buf: None,
        };
        entry.entry_buf = Some(codec::entry_buffer(&entry).unwrap());
        let shared = entry.entry_buf.as_ref().unwrap();
        assert!(shared.bytes.is_segmented());
        let batch = Batch {
            sequence: 7,
            records: vec![
                Record::Entries { from: 1, entries: vec![entry.clone()] },
                Record::Commit(1),
            ],
        };

        let mut actual = Vec::new();
        journal_frame(&batch).unwrap().write_to(&mut actual).unwrap();
        assert_eq!(actual, frame(&batch).unwrap());
        let decoded: Batch = decode(&actual[16..]).unwrap();
        match (&decoded.records[..], &batch.records[..]) {
            (
                [Record::Entries { entries: left, .. }, Record::Commit(left_commit)],
                [Record::Entries { entries: right, .. }, Record::Commit(right_commit)],
            ) => {
                assert_eq!(left, right);
                assert_eq!(left_commit, right_commit);
            }
            _ => panic!("shared frame decoded to different record structure"),
        }

        let cloned = entry.clone();
        assert!(Arc::ptr_eq(entry.entry_buf.as_ref().unwrap(), cloned.entry_buf.as_ref().unwrap()));

        let mut second = Entry {
            scn: Scn::new(2),
            index: 2,
            term: 1,
            command: Command::Client(vec![crate::Action::new(
                common::Op::BytesResp,
                vec![41; 16 * 1024],
            )]),
            entry_buf: None,
        };
        second.entry_buf = Some(codec::entry_buffer(&second).unwrap());
        let batch = Batch {
            sequence: 8,
            records: vec![Record::Entries { from: 1, entries: vec![entry, second] }],
        };
        let mut actual = Vec::new();
        journal_frame(&batch).unwrap().write_to(&mut actual).unwrap();
        assert_eq!(actual, frame(&batch).unwrap());
    }

    #[test]
    fn journal_replays_lazy_actions_from_the_shared_entry_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: None, vote_scn: None }]).unwrap();
        journal
            .write(&[
                Record::Entries {
                    from: 1,
                    entries: vec![Entry {
                        scn: Scn::new(1),
                        index: 1,
                        term: 1,
                        command: Command::Client(vec![crate::Action::new(
                            common::Op::BytesResp,
                            vec![53; 128 * 1024],
                        )]),
                        entry_buf: None,
                    }],
                },
                Record::Commit(1),
            ])
            .unwrap();
        assert!(matches!(
            &journal.state.log[0].command,
            Command::Client(actions) if actions.is_empty()
        ));
        assert!(journal.state.log[0].entry_buf.as_ref().unwrap().bytes.is_segmented());
        drop(journal);

        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(recovered.state.commit, 1);
        assert!(recovered.state.log[0].is_written());
        assert!(recovered.state.log[0].is_durable());
        let actions = recovered.state.log[0].client_actions().unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].fields, vec![53; 128 * 1024]);
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn asynchronous_writes_publish_durability_only_after_completion() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal.start_io(|| {}).unwrap();
        let (release, blocked) = std::sync::mpsc::channel();
        journal
            .io
            .as_mut()
            .unwrap()
            .submit(move || {
                blocked.recv().unwrap();
                Ok(())
            })
            .unwrap();
        journal
            .write(&[
                Record::Hard { term: 1, vote: Some(0), vote_scn: Some(common::scn::Scn::new(0)) },
                Record::Entries {
                    from: 1,
                    entries: vec![Entry {
                        scn: common::scn::Scn::new(1),
                        index: 1,
                        term: 1,
                        command: Command::Noop,
                        entry_buf: None,
                    }],
                },
            ])
            .unwrap();
        journal.poll_io().unwrap();
        assert_eq!(journal.state.last(), 1);
        assert_eq!(journal.durable_state().last(), 0);
        assert_eq!(journal.durable_state().term, 0);
        assert!(!journal.state.log[0].is_durable());
        release.send(()).unwrap();
        journal.fence().unwrap().wait().unwrap();
        journal.poll_io().unwrap();
        assert_eq!(journal.durable_state().last(), 1);
        assert!(journal.state.log[0].is_written());
        assert!(journal.state.log[0].is_durable());
        assert!(journal.durable_state().log[0].is_durable());
        assert!(Arc::ptr_eq(
            journal.state.log[0].entry_buf.as_ref().unwrap(),
            journal.durable_state().log[0].entry_buf.as_ref().unwrap(),
        ));
        journal.write(&[Record::Commit(1)]).unwrap();
        journal.fence().unwrap().wait().unwrap();
        journal.poll_io().unwrap();
        assert_eq!(journal.durable_state().commit, 1);
        journal.io.as_mut().unwrap().submit(|| Err(Error::new("injected disk failure"))).unwrap();
        let failed = journal.fence().unwrap().wait().is_err();
        let rejected = journal.poll_io().is_err();
        assert!(failed && rejected && journal.failed);
        assert_eq!(journal.durable_state().commit, 1);
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn conflicting_history_retires_identity_and_rebuilds_from_older_verified_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(
            dir.path(),
            State::initial("01".repeat(64), Default::default(), Default::default()).unwrap(),
            3,
            "member-0",
        )
        .unwrap();
        let mut membership = initial().membership();
        membership.identities.insert(
            0,
            Member { identity: journal.identity.current.clone(), process: "member-0".into() },
        );
        journal.write(&[Record::Bootstrap(membership)]).unwrap();
        journal
            .write(&[
                Record::Hard { term: 2, vote: Some(0), vote_scn: Some(common::scn::Scn::new(0)) },
                Record::Entries {
                    from: 1,
                    entries: (1..=3)
                        .map(|index| Entry {
                            index,
                            term: 2,
                            scn: Scn::new(index),
                            command: Command::Noop,
                            entry_buf: None,
                        })
                        .collect(),
                },
                Record::Commit(3),
            ])
            .unwrap();
        journal.snapshot(10).unwrap();
        {
            let mut ownership = journal.disk.lock_ownership().unwrap();
            append_identity(&mut ownership, &journal.identity).unwrap();
        }
        let old = journal.identity.current.clone();
        let old_journal = journal.active_start;
        journal.require_history_repair().unwrap();
        drop(journal);
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert!(journal.identity.repairing);
        let mut replacement = initial().capture();
        replacement.index = 1;
        replacement.term = 2;
        replacement.scn = Scn::new(500);
        let rejected = journal.repair_history(replacement.clone(), Some((2, Scn::new(1)))).is_err();
        assert!(rejected);
        assert_eq!(journal.identity.current, old);
        journal.repair_history(replacement, Some((2, Scn::new(500)))).unwrap();
        let new = journal.identity.current.clone();
        let retirement = journal.pending_replacement().unwrap();
        let archive = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.file_name().unwrap().to_string_lossy().starts_with("rejected-"))
            .unwrap();
        assert!(
            archive.join(named(dir.path(), "journal", old_journal).file_name().unwrap()).is_file()
        );
        assert_ne!(old, new);
        assert_eq!(retirement.previous, 0);
        assert_eq!(retirement.next, INCARNATION);
        assert_eq!(retirement.old.identity, old);
        assert_eq!(journal.state.commit, 1);
        assert_eq!(journal.state.vote, None);
        assert_eq!(journal.state.generation, 1);
        assert!(!journal.identity.repairing);
        drop(journal);
        let restarted = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(restarted.identity.current, new);
        assert_eq!(restarted.pending_replacement().unwrap(), retirement);
        assert_eq!(restarted.state.history_at(1), Some((2, Scn::new(500))));
        assert_eq!(restarted.state.commit, 1);
    }

    #[test]
    fn scn_footer_detects_corruption_and_preserves_wrapped_zero() {
        for maximum in [None, Some(Scn::new(0)), Some(Scn::new(u64::MAX))] {
            let bytes = scn_footer(maximum);
            let decoded = parse_scn_footer(&bytes).unwrap();
            assert_eq!(decoded, maximum);
            for offset in 0..bytes.len() {
                let mut corrupt = bytes.clone();
                corrupt[offset] ^= 1;
                let rejected = parse_scn_footer(&corrupt).is_err();
                assert!(rejected);
            }
        }
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn interrupted_footer_write_keeps_current_journal_and_identity() {
        for length in [1, 7, 16, 32, 63, 64] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            journal
                .write(&[Record::Hard {
                    term: 1,
                    vote: Some(0),
                    vote_scn: Some(common::scn::Scn::new(0)),
                }])
                .unwrap();
            let identity = journal.identity.current.clone();
            let path = named(dir.path(), "journal", journal.active_start);
            let size = std::fs::metadata(&path).unwrap().len();
            drop(journal);
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&scn_footer(None)[..length]).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            let restored_size = std::fs::metadata(&path).unwrap().len();
            assert_eq!(recovered.identity.current, identity);
            assert_eq!(recovered.state.vote, Some(0));
            assert_eq!(restored_size, size);
            assert!(recovered.pending_replacement().is_none());
        }
    }

    #[test]
    fn checkpoint_proof_is_checked_before_installing_new_history() {
        let mut state = initial();
        state.term = 9;
        let mut snapshot = state.capture();
        snapshot.index = 20;
        snapshot.term = 7;
        for proof in [None, Some((9, snapshot.scn)), Some((7, Scn::new(123)))] {
            let rejected = checkpoint_install(&state, &snapshot, proof).is_err();
            assert!(rejected);
            assert_eq!(state.commit, 0);
        }
        snapshot.term = 9;
        let install =
            checkpoint_install(&state, &snapshot, Some((9, snapshot.scn))).unwrap().unwrap();
        state.apply(&install).unwrap();
        assert_eq!(state.commit, 20);
        assert_eq!(state.term_at(20), Some(9));
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn retained_journals_prove_compacted_terms_and_reject_wrong_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 5, "member-0").unwrap();
        for index in 1..=3 {
            journal
                .write(&[
                    Record::Hard {
                        term: index,
                        vote: Some(0),
                        vote_scn: Some(common::scn::Scn::new(0)),
                    },
                    Record::Entries {
                        from: index,
                        entries: vec![Entry {
                            scn: common::scn::Scn::new(index),
                            index,
                            term: index,
                            command: Command::Noop,
                            entry_buf: None,
                        }],
                    },
                    Record::Commit(index),
                ])
                .unwrap();
            let pending = journal.prepare_snapshot(index).unwrap().unwrap();
            pending.write().unwrap();
            journal.finish_snapshot(&pending).unwrap();
        }
        assert_eq!(journal.state.term_at(1), None);
        assert_eq!(journal.committed_term(1).unwrap(), Some(1));
        assert_eq!(journal.committed_term(3).unwrap(), Some(3));
        assert_eq!(journal.committed_term(4).unwrap(), None);
        // Physical sequence/ring still match, but predecessor index, term or SCN differs.
        let journals = numbered(dir.path(), "journal").unwrap();
        let (_, path) = &journals[1];
        let original = std::fs::read(path).unwrap();
        for offset in [152, 160, 168] {
            let mut bad = original.clone();
            bad[offset..offset + 8].copy_from_slice(&99u64.to_le_bytes());
            std::fs::write(path, bad).unwrap();
            let rejected = journal.committed_term(0).is_err();
            assert!(rejected);
        }
        std::fs::write(path, original).unwrap();
        // A later installed checkpoint that cannot be linked must invalidate
        // an older proof, even if that term was present earlier in the same file.
        let mut replacement = journal.state.capture();
        replacement.index = 5;
        replacement.term = 9;
        journal
            .write(&[
                Record::Hard { term: 9, vote: None, vote_scn: None },
                Record::Install(replacement),
            ])
            .unwrap();
        assert_eq!(journal.committed_term(1).unwrap(), None);
        // Losing all retained bases makes proof unavailable, not an assumed match.
        for (_, path) in numbered(dir.path(), "snapshot").unwrap() {
            std::fs::remove_file(path).unwrap();
        }
        assert_eq!(journal.committed_term(1).unwrap(), None);
    }

    #[test]
    fn old_committed_copy_cannot_replace_the_new_primary_history() {
        let mut state = initial();
        state.snapshot.index = 9;
        state.snapshot.term = 4;
        state.commit = 9;
        state.term = 6;
        let mut copied = CommittedJournal {
            ring: state.ring.clone(),
            after: 9,
            previous_term: 4,
            previous_scn: state.snapshot.scn,
            entries: vec![Arc::new(crate::Entry {
                scn: common::scn::Scn::new(10),
                index: 10,
                term: 4,
                command: crate::Command::Noop,
                entry_buf: None,
            })],
        };
        // Ring identity and the retained prefix alone accept this abandoned history.
        let structurally_valid = copied.validate(&state, 10).is_ok();
        let matches_primary = copied.matches_tip(10, (6, Scn::new(10)));
        assert!(structurally_valid);
        assert!(!matches_primary);
        Arc::make_mut(&mut copied.entries[0]).term = 6;
        let valid =
            copied.validate(&state, 10).is_ok() && copied.matches_tip(10, (6, Scn::new(10)));
        assert!(valid);
        state
            .apply(&Record::Entries {
                from: 10,
                entries: copied.entries.into_iter().map(|entry| (*entry).clone()).collect(),
            })
            .unwrap();
        state.apply(&Record::Commit(10)).unwrap();
        assert_eq!(state.term_at(10), Some(6));
        assert_eq!(state.commit, 10);
    }
    #[test]
    fn copied_journal_is_bounded_contiguous_and_ring_checked() {
        let mut state = initial();
        state
            .apply(&Record::Hard {
                term: 2,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            })
            .unwrap();
        let good = CommittedJournal {
            ring: state.ring.clone(),
            after: 0,
            previous_term: 0,
            previous_scn: Scn::new(0),
            entries: vec![Arc::new(Entry {
                scn: common::scn::Scn::new(1),
                index: 1,
                term: 1,
                command: Command::Noop,
                entry_buf: None,
            })],
        };
        assert!(good.validate(&state, 1).is_ok());
        let mut wrong = good.clone();
        wrong.ring = "02".repeat(64);
        assert!(wrong.validate(&state, 1).is_err());
        let mut gap = good.clone();
        Arc::make_mut(&mut gap.entries[0]).index = 2;
        assert!(gap.validate(&state, 2).is_err());
        assert!(good.validate(&state, 0).is_err());
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn unparseable_checkpoint_falls_back_without_retiring_and_missing_identity_is_slotless() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        let identity = journal.identity.current.clone();
        journal
            .write(&[Record::Hard {
                term: 1,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            }])
            .unwrap();
        journal.snapshot(10).unwrap();
        let path = numbered(dir.path(), "snapshot").unwrap().last().unwrap().1.clone();
        drop(journal);
        // Valid existing checksum over an invalid JSON body exercises parsing detection.
        let mut bad = b"not-a-tagged-frame".to_vec();
        bad.extend(Sha256::digest(&bad));
        std::fs::write(&path, &bad).unwrap();
        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(recovered.identity.current, identity);
        assert_eq!(recovered.state.vote, Some(0));
        drop(recovered);
        std::fs::remove_file(dir.path().join("identity")).unwrap();
        let applicant = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_ne!(applicant.identity.current, identity);
        assert!(applicant.identity.slot.is_none());
        assert!(applicant.identity.pending.is_none());
        assert!(applicant.state.vote.is_none());
        let new_identity = applicant.identity.current.clone();
        drop(applicant);
        let restarted = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(restarted.identity.current, new_identity);
        assert!(restarted.identity.slot.is_none());
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn copied_journal_excludes_uncommitted_entries_and_preserves_local_votes() {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(source.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 2, vote: Some(0), vote_scn: Some(common::scn::Scn::new(0)) },
                Record::Entries {
                    from: 1,
                    entries: (1..=3)
                        .map(|index| Entry {
                            scn: common::scn::Scn::new(index),
                            index,
                            term: 2,
                            command: Command::Noop,
                            entry_buf: None,
                        })
                        .collect(),
                },
                Record::Commit(2),
            ])
            .unwrap();
        journal.snapshot(10).unwrap();
        let copied = journal.committed_suffix(0, 3).unwrap().unwrap();
        assert_eq!(copied.entries.len(), 2);
        let mut dest = Journal::open(target.path(), initial(), 3, "member-0").unwrap();
        dest.write(&[Record::Hard {
            term: 3,
            vote: Some(1),
            vote_scn: Some(common::scn::Scn::new(0)),
        }])
        .unwrap();
        dest.import_committed(&copied, 2).unwrap();
        assert_eq!(dest.state.vote, Some(1));
        assert_eq!(dest.state.term, 3);
        assert_eq!(dest.state.commit, 2);
        assert!(named(&target.path().join("copied-journals"), "journal", 1).exists());
        assert_eq!(numbered(&target.path().join("copied-journals"), "journal").unwrap().len(), 1);
        drop(dest);
        let restarted = Journal::open(target.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(restarted.state.vote, Some(1));
        assert_eq!(restarted.state.commit, 2);
    }
    #[test]
    fn local_identity_keeps_retired_ids_and_detects_corruption() {
        let local = LocalIdentity {
            repairing: false,
            ring: "01".repeat(64),
            process: "routing-label".into(),
            current: common::ring::MemberIdentity::try_from("02".repeat(64)).unwrap(),
            retired: vec![common::ring::MemberIdentity::try_from("03".repeat(64)).unwrap()],
            slot: None,
            latest_journal: 0,
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
            journal
                .write(&[Record::Hard {
                    term,
                    vote: Some(0),
                    vote_scn: Some(common::scn::Scn::new(0)),
                }])
                .unwrap();
        }
        let active_start = journal.active_start;
        drop(journal);
        let path = named(dir.path(), "journal", active_start);
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
                entries: vec![Entry {
                    scn: common::scn::Scn::new(1),
                    index: 1,
                    term: 1,
                    command: Command::Members(crate::MemberChange {
                        before: recovered.state.membership(),
                        after: membership,
                    }),
                    entry_buf: None,
                }],
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
        journal
            .write(&[Record::Hard {
                term: 2,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            }])
            .unwrap();
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
        publish(&dir.path().join("recovery-test.bin"), &checkpoint.bytes().unwrap()).unwrap();
        local.retired.push(old);
        local.current = new.clone();
        local.slot = Some(INCARNATION);
        local.pending = Some(retirement);
        local.recovery = Some(RecoveryPlan {
            archive: "retired-test".into(),
            checkpoint: "recovery-test.bin".into(),
            files: vec![
                "journal-00000000000000000001.log".into(),
                "snapshot-00000000000000000000.bin".into(),
            ],
        });
        let mut file =
            OpenOptions::new().read(true).write(true).open(dir.path().join("identity")).unwrap();
        append_identity(&mut file, &local).unwrap();
        drop(file);
        std::fs::create_dir(dir.path().join("retired-test")).unwrap();
        std::fs::rename(
            named(dir.path(), "journal", 1),
            dir.path().join("retired-test/journal-00000000000000000001.log"),
        )
        .unwrap();
        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(recovered.identity.current, new);
        assert_eq!(recovered.identity.retired.len(), 1);
        assert!(recovered.identity.recovery.is_none());
        assert!(recovered.pending_replacement().is_some());
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn rotation_preserves_old_bytes_and_recovers_empty_active_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[Record::Hard {
                term: 1,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            }])
            .unwrap();
        let old = std::fs::read(named(dir.path(), "journal", journal.active_start)).unwrap();
        let pending = journal.prepare_snapshot(10).unwrap().unwrap();
        let closed = std::fs::read(named(dir.path(), "journal", 1)).unwrap();
        let current = std::fs::read(named(dir.path(), "journal", journal.active_start)).unwrap();
        assert_eq!(old, closed[..closed.len() - SCN_FOOTER as usize]);
        assert_eq!(parse_scn_footer(&closed[old.len()..]).unwrap(), None);
        assert_eq!(current, journal_header(&initial().ring, 2));
        // A retry before publication should reuse the empty active file.
        journal.prepare_snapshot(11).unwrap().unwrap();
        assert_eq!(numbered(dir.path(), "journal").unwrap().len(), 2);
        pending.write().unwrap();
        journal.finish_snapshot(&pending).unwrap();
        drop(journal);
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(journal.active_start, 2);
        journal
            .write(&[Record::Hard {
                term: 2,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            }])
            .unwrap();
        journal.snapshot(12).unwrap();
        let second_closed = named(dir.path(), "journal", 2).is_file();
        drop(journal);
        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert!(second_closed);
        assert_eq!(recovered.state.term, 2);
        assert_eq!(recovered.active_start, 3);
    }

    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn missing_latest_or_rotation_gap_never_resumes_an_old_journal() {
        for rename in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            journal
                .write(&[Record::Hard {
                    term: 1,
                    vote: Some(0),
                    vote_scn: Some(common::scn::Scn::new(0)),
                }])
                .unwrap();
            journal.snapshot(10).unwrap();
            journal
                .write(&[Record::Hard {
                    term: 2,
                    vote: Some(0),
                    vote_scn: Some(common::scn::Scn::new(0)),
                }])
                .unwrap();
            let start = journal.active_start;
            drop(journal);
            if rename {
                std::fs::rename(
                    named(dir.path(), "journal", start),
                    named(dir.path(), "journal", start + 1),
                )
                .unwrap();
            } else {
                std::fs::remove_file(named(dir.path(), "journal", start)).unwrap();
            }
            let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            assert_eq!(recovered.identity.retired.len(), 1);
            assert!(named(dir.path(), "journal", recovered.active_start).exists());
            let replacement = recovered.identity.current.clone();
            drop(recovered);
            let restarted = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
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
    fn checkpoint_serializes_captured_state_and_replays_later_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 2, vote: Some(0), vote_scn: Some(Scn::new(100)) },
                Record::Entries {
                    from: 1,
                    entries: (1..=2)
                        .map(|index| Entry {
                            scn: Scn::new(index),
                            index,
                            term: 2,
                            command: Command::Noop,
                            entry_buf: None,
                        })
                        .collect(),
                },
                Record::Commit(1),
            ])
            .unwrap();

        let mut captured = journal.state.clone();
        let snapshot = captured.capture();
        let mark = journal.begin_checkpoint(&captured, &snapshot).unwrap();
        journal
            .write(&[Record::Hard { term: 3, vote: None, vote_scn: None }, Record::Commit(2)])
            .unwrap();

        captured.snapshot = snapshot;
        captured.log.retain(|entry| entry.index > captured.snapshot.index);
        let pending = journal.prepare_checkpoint(100, captured, mark).unwrap().unwrap();
        assert_eq!(pending.checkpoint.state.term, 2);
        assert_eq!(pending.checkpoint.state.vote, Some(0));
        assert_eq!(pending.checkpoint.state.vote_scn, Some(Scn::new(100)));
        assert_eq!(pending.checkpoint.state.max_scn, Some(Scn::new(100)));
        assert_eq!(pending.checkpoint.state.snapshot.index, 1);
        assert_eq!(pending.checkpoint.state.snapshot.term, 2);
        assert_eq!(pending.checkpoint.state.snapshot.scn, Scn::new(1));
        assert_eq!(pending.checkpoint.state.commit, 1);
        assert!(pending.checkpoint.state.log.is_empty());

        pending.write().unwrap();
        journal.finish_snapshot(&pending).unwrap();
        drop(journal);
        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(recovered.state.term, 3);
        assert_eq!(recovered.state.vote, None);
        assert_eq!(recovered.state.vote_scn, None);
        assert_eq!(recovered.state.max_scn, Some(Scn::new(100)));
        assert_eq!(recovered.state.commit, 2);
        assert_eq!(recovered.state.last(), 2);
    }
    #[test]
    fn checkpoint_read_overrides_only_the_ring_identity() {
        let dir = tempfile::tempdir().unwrap();
        let ring = initial().ring;
        let foreign = "02".repeat(64);
        let mut state = initial();
        state
            .apply(&Record::Hard {
                term: 1,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            })
            .unwrap();
        state
            .apply(&Record::Entries {
                from: 1,
                entries: vec![Entry {
                    scn: Scn::new(7),
                    index: 1,
                    term: 1,
                    command: Command::Noop,
                    entry_buf: None,
                }],
            })
            .unwrap();
        state.apply(&Record::Commit(1)).unwrap();
        state.snapshot = state.capture();
        state.log.clear();
        state.ring = foreign.clone();
        state.snapshot.ring = foreign;
        let path = dir.path().join("snapshot.bin");
        std::fs::write(&path, Checkpoint { sequence: 7, second: 9, state }.bytes().unwrap())
            .unwrap();

        let (state, _, _, _) = read_checkpoint(&path, 7, &ring, true).unwrap();
        assert_eq!(state.ring, ring);
        assert_eq!(state.snapshot.ring, ring);
        assert_eq!(state.snapshot.index, 1);
        assert_eq!(state.snapshot.term, 1);
        assert_eq!(state.snapshot.scn, Scn::new(7));
        assert_eq!(state.term, 1);
        assert_eq!(state.vote, Some(0));
        assert_eq!(state.commit, 1);
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
    fn foreign_files_are_preserved_and_recovered_without_importing_their_state() {
        for kind in ["journal", "snapshot"] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            let identity = journal.identity.current.clone();
            journal
                .write(&[Record::Hard {
                    term: 1,
                    vote: Some(0),
                    vote_scn: Some(common::scn::Scn::new(0)),
                }])
                .unwrap();
            journal.snapshot(10).unwrap();
            let active_start = journal.active_start;
            drop(journal);
            let path = if kind == "journal" {
                named(dir.path(), "journal", active_start)
            } else {
                numbered(dir.path(), "snapshot").unwrap().last().unwrap().1.clone()
            };
            let bytes = if kind == "journal" {
                journal_header(&"02".repeat(64), 2)
            } else {
                let mut checkpoint = Checkpoint::decode(&std::fs::read(&path).unwrap()).unwrap();
                checkpoint.state.ring = "02".repeat(64);
                checkpoint.state.snapshot.ring = checkpoint.state.ring.clone();
                checkpoint.bytes().unwrap()
            };
            std::fs::write(&path, &bytes).unwrap();
            let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            let corrupt = path.with_file_name(format!(
                "{}.corrupt-0",
                path.file_name().unwrap().to_string_lossy()
            ));
            assert_eq!(std::fs::read(corrupt).unwrap(), bytes);
            assert_eq!(recovered.state.ring, initial().ring);
            if kind == "snapshot" {
                assert_eq!(recovered.identity.current, identity);
                assert_eq!(recovered.state.vote, Some(0));
            } else {
                assert_ne!(recovered.identity.current, identity);
            }
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
            crate::application::cancel_checkpoint_after_election(role.get(), 3)
        })
        .is_ok();
        let completed = writer.count;
        writer.next = Role::Leader;
        writer.count = 0;
        let elected = write_checkpoint_chunks(&mut writer, &bytes, || {
            crate::application::cancel_checkpoint_after_election(role.get(), 3)
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
        original
            .apply(&Record::Hard {
                term: 2,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            })
            .unwrap();
        let entries: Vec<_> = (1..=4)
            .map(|index| Entry {
                scn: common::scn::Scn::new(index),
                index,
                term: 2,
                command: Command::Noop,
                entry_buf: None,
            })
            .collect();
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
        let recovered_suffix = published.log.iter().eq(before.log.iter().skip(1));
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
            let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
            let entries = (1..=4)
                .map(|index| Entry {
                    scn: common::scn::Scn::new(index),
                    index,
                    term: 2,
                    command: Command::Noop,
                    entry_buf: None,
                })
                .collect();
            journal
                .write(&[
                    Record::Hard {
                        term: 2,
                        vote: Some(0),
                        vote_scn: Some(common::scn::Scn::new(0)),
                    },
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
            let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
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
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 3, vote: Some(0), vote_scn: Some(common::scn::Scn::new(0)) },
                Record::Entries {
                    from: 1,
                    entries: vec![Entry {
                        scn: common::scn::Scn::new(1),
                        index: 1,
                        term: 3,
                        command: Command::Noop,
                        entry_buf: None,
                    }],
                },
            ])
            .unwrap();
        drop(journal);
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(journal.state.term, 3);
        assert_eq!(journal.state.vote.as_ref(), Some(&0));
        assert_eq!(journal.state.commit, 0);
        assert_eq!(journal.state.last(), 1);
        journal.write(&[Record::Commit(1)]).unwrap();
        drop(journal);
        let journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(journal.state.commit, 1);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn partial_active_tail_is_discarded_but_corrupt_frames_retire_identity() {
        let source = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(source.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[Record::Hard {
                term: 1,
                vote: Some(0),
                vote_scn: Some(common::scn::Scn::new(0)),
            }])
            .unwrap();
        let active_start = journal.active_start;
        drop(journal);
        let source_active = named(source.path(), "journal", active_start);
        let frame = std::fs::read(&source_active).unwrap();
        for count in HEADER as usize..frame.len() {
            let dir = tempfile::tempdir().unwrap();
            for name in ["identity", "snapshot-00000000000000000000.bin"] {
                std::fs::copy(source.path().join(name), dir.path().join(name)).unwrap();
            }
            let path = named(dir.path(), "journal", 1);
            std::fs::write(&path, &frame[..count]).unwrap();
            let result = Journal::open(dir.path(), initial(), 3, "member-0");
            assert!(result.is_ok(), "partial length {count}");
            let size = std::fs::metadata(path).unwrap().len();
            assert_eq!(size, HEADER);
        }
        let mut bad = frame.clone();
        bad[HEADER as usize + 20] ^= 1;
        std::fs::write(&source_active, bad).unwrap();
        let result = Journal::open(source.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(result.identity.retired.len(), 1);
        assert!(source.path().join("journal-00000000000000000001.log.corrupt-0").exists());
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshots_retain_complete_replay_suffixes_and_lock_survives_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = Node::new(0, Journal::open(dir.path(), initial(), 3, "member-0").unwrap());
        node.campaign().unwrap();
        for n in 0..5 {
            node.propose(Command::Client(vec![crate::Action::new(
                common::Op::BytesResp,
                vec![n; 104],
            )]))
            .unwrap();
            node.journal.snapshot(n as u64 + 1).unwrap();
        }
        let expected = node.state().capture();
        let snapshots = numbered(dir.path(), "snapshot").unwrap();
        assert_eq!(snapshots.len(), 3);
        let result = Journal::open(dir.path(), initial(), 3, "member-0");
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
            let restored = Journal::open(restore.path(), initial(), 3, "member-0").unwrap();
            assert_eq!(restored.state.capture(), expected);
        }
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshot_preserves_uncommitted_suffix_and_replay_accepts_unpublished_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 1, vote: None, vote_scn: None },
                Record::Entries {
                    from: 1,
                    entries: vec![Entry {
                        scn: common::scn::Scn::new(1),
                        index: 1,
                        term: 1,
                        command: Command::Noop,
                        entry_buf: None,
                    }],
                },
            ])
            .unwrap();
        journal.snapshot(1).unwrap();
        let seq = journal.snapshot_sequence;
        journal.write(&[Record::Commit(1)]).unwrap();
        drop(journal);
        // Simulate snapshot publication never completing, leaving the switched journal.
        std::fs::remove_file(named(dir.path(), "snapshot", seq)).unwrap();
        let restored = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(restored.state.commit, 1);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn snapshot_triggers_use_elapsed_time_and_previous_file_size_with_floor() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: None, vote_scn: None }]).unwrap();
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
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal
            .write(&[
                Record::Hard { term: 1, vote: None, vote_scn: None },
                Record::Entries {
                    from: 1,
                    entries: vec![crate::raft::Entry {
                        scn: common::scn::Scn::new(1),
                        index: 1,
                        term: 1,
                        command: Command::Client(vec![crate::Action::new(
                            common::Op::BytesResp,
                            vec![8; 104],
                        )]),
                        entry_buf: None,
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
                        scn: common::scn::Scn::new(2),
                        index: 2,
                        term: 1,
                        command: Command::Client(vec![crate::Action::new(
                            common::Op::BytesResp,
                            vec![9; 104],
                        )]),
                        entry_buf: None,
                    }],
                },
                Record::Commit(2),
                Record::Hard { term: 2, vote: None, vote_scn: None },
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
        let restored = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(restored.state.capture(), expected);
        assert_eq!(restored.state.term, term);
    }
    #[test]
    #[ignore = "filesystem integration; run explicitly with --ignored"]
    fn wrong_member_is_rejected_and_corrupt_journal_is_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        journal.write(&[Record::Hard { term: 1, vote: None, vote_scn: None }]).unwrap();
        let active_start = journal.active_start;
        drop(journal);
        let wrong_member = Journal::open(dir.path(), initial(), 3, "member-1");
        assert!(wrong_member.is_err());
        let path = named(dir.path(), "journal", active_start);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[1] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let recovered = Journal::open(dir.path(), initial(), 3, "member-0").unwrap();
        assert_eq!(recovered.identity.retired.len(), 1);
        assert!(dir.path().join("journal-00000000000000000001.log.corrupt-0").exists());
    }
}

#[cfg(any(test, feature = "testing"))]
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
            repairing: false,
            ring: ring.into(),
            process: process.into(),
            current,
            retired: vec![],
            slot: None,
            latest_journal: 0,
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

crate::wire_struct!(Batch, RaftBatch, {sequence,records});

crate::wire_struct!(Checkpoint, RaftCheckpoint, {sequence,second,state});

crate::wire_struct!(CommittedJournal, RaftCommittedJournal, {ring,after,previous_term,previous_scn,entries});

crate::wire_struct!(LocalIdentity, RaftLocalIdentity, {ring,process,current,retired,slot,latest_journal,membership,founders,initialized,pending,recovery,repairing});

crate::wire_struct!(RecoveryPlan, RaftRecoveryPlan, {archive,checkpoint,files});
