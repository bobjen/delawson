//! Per-disk staging and metadata journal files.
//!
//! Each file is at most 4 GiB. Journal offsets are reserved before positional
//! writes are issued. Queued records are batched; stage and journal ranges are
//! written concurrently, then staging is synced before the journal.

use common::crypto;
use common::ids::{DiskId, EnId};
use common::scn::Scn;
use common::time::TimeSec;
use common::{Error, Op, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock};

const JOURNAL_MAGIC: &[u8; 8] = b"DLAWENJ1";
const STAGE_MAGIC: &[u8; 8] = b"DLAWENS1";
const HEADER_LEN: usize = 40;
const HEADER: u64 = HEADER_LEN as u64;
pub(crate) const STAGE_PREFIX: usize = 20;
pub(crate) const STAGE_TAG: usize = 16;
const FILE_LIMIT: u64 = 1u64 << 32;
const CHECKPOINT_MIN_JOURNAL_BYTES: u64 = 1024 * 1024;
const CHECKPOINT_MAGIC: &[u8; 8] = b"DLAWENK1";
const CHECKPOINT_FIXED: u64 = 20;
const STAGE_SLOTS: usize = 16 * 1024;
const NO_STAGE_FILE: StageId = StageId(u16::MAX);

pub(crate) type KeyLookup = Arc<dyn Fn(TimeSec) -> Result<[u8; 32]> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct StageId(u16);

impl StageId {
    const FIRST: Self = Self(0);

    pub(crate) fn from_number(value: u16) -> Result<Self> {
        Self::new(value)
    }

    fn new(value: u16) -> Result<Self> {
        if usize::from(value) >= STAGE_SLOTS {
            return Err(Error::new("EN StageId exceeds the 16K slot table"));
        }
        Ok(Self(value))
    }

    pub(crate) const fn get(self) -> u16 {
        self.0
    }

    fn index(self) -> Result<usize> {
        if usize::from(self.0) >= STAGE_SLOTS {
            return Err(Error::new("invalid EN StageId"));
        }
        Ok(usize::from(self.0))
    }
}

struct StageFile {
    file: Arc<File>,
    created: TimeSec,
}

type StageTable = Box<[Option<StageFile>; STAGE_SLOTS]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StageRef {
    pub file: StageId,
    pub offset: u32,
    pub len: u32,
}

pub(crate) struct StageData {
    pub timestamp: TimeSec,
    pub metadata: Vec<u8>,
    pub cipher: Vec<u8>,
    pub tag: [u8; STAGE_TAG],
}

#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub scn: Scn,
    pub op: Op,
    pub stage: StageRef,
    pub stage_tag: [u8; 16],
    pub metadata: Vec<u8>,
    pub written: bool,
    pub file: u32,
    /// End offset of the containing journal frame.
    pub end: u64,
}

type Done = Box<dyn FnOnce(Result<Option<StageRef>>) + Send + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JournalCursor {
    pub file: u32,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Checkpoint {
    pub generation: u64,
    pub cursor: JournalCursor,
    pub max_scn: Option<Scn>,
    pub data: Arc<[u8]>,
    pub size: u64,
}

#[derive(Debug, Clone)]
struct CheckpointMeta {
    generation: u64,
    cursor: JournalCursor,
}

impl From<&Checkpoint> for CheckpointMeta {
    fn from(checkpoint: &Checkpoint) -> Self {
        Self { generation: checkpoint.generation, cursor: checkpoint.cursor }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckpointBoundary {
    pub cursor: JournalCursor,
    bytes_since_previous: u64,
}

enum WriterMessage {
    Job(Request),
    Checkpoint(Sender<Result<CheckpointBoundary>>),
}

struct Request {
    scn: Scn,
    op: Op,
    metadata: Vec<u8>,
    stage: Vec<u8>,
    stage_ref: Option<StageRef>,
    done: Done,
}

struct JournalEntry {
    scn: Scn,
    op: Op,
    stage: StageRef,
    stage_tag: [u8; 16],
    metadata: Vec<u8>,
}

struct WriteAt {
    file: Arc<File>,
    offset: u64,
    bytes: Vec<u8>,
}

struct BatchDone {
    seq: u64,
    jobs: Vec<Request>,
    stage_files: BTreeMap<StageId, Arc<File>>,
    journal_files: BTreeMap<u32, Arc<File>>,
    journal_bytes: u64,
    result: std::result::Result<(), String>,
}

/// A numbered collection of files, with the current append position.
struct FileSet {
    dir: PathBuf,
    stem: &'static str,
    suffix: &'static str,
    magic: &'static [u8; 8],
    /// Stage IDs are referenced by journal records; journal IDs come from names.
    id_header: bool,
    en: u64,
    disk: u16,
    files: Arc<RwLock<BTreeMap<u32, Arc<File>>>>,
    current_len: u64,
    max_len: u64,
}

/// Per-disk stage files occupy a fixed, persistent 16K-slot namespace.
/// StageId is the slot, not a position in a compacted list.
struct StageFiles {
    dir: PathBuf,
    en: u64,
    disk: u16,
    files: Arc<RwLock<StageTable>>,
    current: StageId,
    current_len: u64,
}

impl StageFiles {
    fn open(dir: &Path, en: u64, disk: u16) -> Result<(StageFiles, bool)> {
        let mut files: StageTable = Box::new(std::array::from_fn(|_| None));
        let mut current = None;
        let mut created = false;
        for item in fs::read_dir(dir)? {
            let item = item?;
            let name = item.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(number) = name.strip_prefix("stage-").and_then(|n| n.strip_suffix(".bin"))
            else {
                continue;
            };
            let raw = number.parse::<u16>().map_err(|_| Error::new("invalid EN stage id"))?;
            let id = StageId::new(raw)?;
            let (file, stage_time) =
                open_file(&item.path(), STAGE_MAGIC, en, disk, u32::from(raw), true)?;
            let len = file.metadata()?.len();
            if len > FILE_LIMIT {
                return Err(Error::new(format!(
                    "EN stage file {} exceeds 4 GiB",
                    item.path().display()
                )));
            }
            files[id.index()?] = Some(StageFile { file: Arc::new(file), created: stage_time });
            if current.is_none_or(|old: StageId| id > old) {
                current = Some(id);
            }
        }

        if current.is_none() {
            let id = StageId::FIRST;
            let stage_time = TimeSec::now();
            let (file, new) = create_file(
                dir,
                "stage-",
                ".bin",
                STAGE_MAGIC,
                en,
                disk,
                u32::from(id.get()),
                true,
                stage_time,
            )?;
            files[id.index()?] = Some(StageFile { file: Arc::new(file), created: stage_time });
            current = Some(id);
            created = new;
        }

        let current = current.expect("stage file created or found");
        let current_len = files[current.index()?]
            .as_ref()
            .expect("current stage file exists")
            .file
            .metadata()?
            .len();
        Ok((
            StageFiles {
                dir: dir.to_path_buf(),
                en,
                disk,
                files: Arc::new(RwLock::new(files)),
                current,
                current_len,
            },
            created,
        ))
    }

    fn reserve(
        &mut self,
        len: usize,
        key_lookup: &KeyLookup,
    ) -> Result<(StageId, u32, TimeSec, Arc<File>)> {
        let len = u64::try_from(len).map_err(|_| Error::new("EN stage write length overflow"))?;
        if len > FILE_LIMIT - HEADER {
            return Err(Error::new("one EN stage write exceeds the 4 GiB file limit"));
        }
        if self.current_len.checked_add(len).is_none_or(|end| end > FILE_LIMIT) {
            let stage_time = TimeSec::now();
            key_lookup(stage_time)?;
            self.new_file(stage_time)?;
        }
        let stage_file = self.get(self.current)?;
        let offset = u32::try_from(self.current_len)
            .map_err(|_| Error::new("EN stage offset exceeds u32"))?;
        self.current_len += len;
        Ok((self.current, offset, stage_file.created, stage_file.file))
    }

    fn ensure_current_key(&mut self, key_lookup: &KeyLookup) -> Result<bool> {
        let current = self.get(self.current)?;
        if key_lookup(current.created).is_ok() {
            return Ok(false);
        }
        if current.file.metadata()?.len() != HEADER {
            return Err(Error::new(format!(
                "EN stage file {:?} has data but no key for its creation time {}",
                self.current,
                current.created.number()
            )));
        }

        let stage_time = TimeSec::now();
        key_lookup(stage_time)?;
        self.new_file(stage_time)?;
        Ok(true)
    }

    fn new_file(&mut self, stage_time: TimeSec) -> Result<()> {
        let next = usize::from(self.current.get()) + 1;
        if next == STAGE_SLOTS {
            return Err(Error::new("all 16K EN stage file slots are occupied"));
        }
        let id = StageId::new(next as u16)?;
        if self.files.read().expect("stage files")[next].is_some() {
            return Err(Error::new("next EN stage file slot is already occupied"));
        }
        let (file, _) = create_file(
            &self.dir,
            "stage-",
            ".bin",
            STAGE_MAGIC,
            self.en,
            self.disk,
            u32::from(id.get()),
            true,
            stage_time,
        )?;
        self.files.write().expect("stage files")[id.index()?] =
            Some(StageFile { file: Arc::new(file), created: stage_time });
        self.current = id;
        self.current_len = HEADER;
        Ok(())
    }

    fn get(&self, id: StageId) -> Result<StageFile> {
        self.files
            .read()
            .expect("stage files")
            .get(id.index()?)
            .and_then(Option::as_ref)
            .map(|file| StageFile { file: Arc::clone(&file.file), created: file.created })
            .ok_or_else(|| Error::new(format!("missing EN stage file {id:?}")))
    }

    fn sync(&self, id: StageId) -> Result<()> {
        self.get(id)?.file.sync_all()?;
        Ok(())
    }
}

impl FileSet {
    fn open(
        dir: &Path,
        stem: &'static str,
        suffix: &'static str,
        magic: &'static [u8; 8],
        en: u64,
        disk: u16,
        id_header: bool,
        max_len: u64,
    ) -> Result<(FileSet, bool)> {
        if max_len <= HEADER || max_len > FILE_LIMIT {
            return Err(Error::new("invalid configured EN journal file size"));
        }
        let mut names = Vec::new();
        for item in fs::read_dir(dir)? {
            let item = item?;
            let name = item.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(number) = name.strip_prefix(stem).and_then(|n| n.strip_suffix(suffix)) else {
                continue;
            };
            let id = number.parse::<u32>().map_err(|_| Error::new("invalid EN file number"))?;
            names.push((id, item.path()));
        }
        names.sort_by_key(|(id, _)| *id);
        // Journal files are retired from the oldest end, so their retained IDs
        // must form a contiguous suffix. Stage files can be retired independently:
        // their IDs are stable references from journal records and may have gaps.
        if stem == "journal-" {
            for pair in names.windows(2) {
                if pair[0].0.checked_add(1) != Some(pair[1].0) {
                    return Err(Error::new("gap in retained EN journal files"));
                }
            }
        }

        let mut files = BTreeMap::new();
        let mut created = false;
        if names.is_empty() {
            let (file, new) =
                create_file(dir, stem, suffix, magic, en, disk, 0, id_header, TimeSec::now())?;
            files.insert(0, Arc::new(file));
            created = new;
        } else {
            for (id, path) in &names {
                let (file, _) = open_file(path, magic, en, disk, *id, id_header)?;
                if file.metadata()?.len() > FILE_LIMIT {
                    return Err(Error::new(format!("EN {} file exceeds 4 GiB", path.display())));
                }
                files.insert(*id, Arc::new(file));
            }
        }
        let current_len = files.last_key_value().expect("at least one EN file").1.metadata()?.len();
        Ok((
            FileSet {
                dir: dir.to_path_buf(),
                stem,
                suffix,
                magic,
                id_header,
                en,
                disk,
                files: Arc::new(RwLock::new(files)),
                current_len,
                max_len,
            },
            created,
        ))
    }

    fn reserve(&mut self, len: usize) -> Result<(u32, u32, Arc<File>)> {
        let len = u64::try_from(len).map_err(|_| Error::new("EN write length overflow"))?;
        if len > self.max_len.saturating_sub(HEADER) {
            return Err(Error::new("one EN journal frame exceeds its configured file limit"));
        }
        if self.current_len.checked_add(len).is_none_or(|end| end > self.max_len) {
            let id = self
                .files
                .read()
                .expect("files")
                .last_key_value()
                .map_or(Some(0), |(&last, _)| last.checked_add(1))
                .ok_or_else(|| Error::new("EN file id overflow"))?;
            let (file, _) = create_file(
                &self.dir,
                self.stem,
                self.suffix,
                self.magic,
                self.en,
                self.disk,
                id,
                self.id_header,
                TimeSec::now(),
            )?;
            self.current_len = HEADER;
            self.files.write().expect("files").insert(id, Arc::new(file));
        }
        let files = self.files.read().expect("files");
        let (&id, file) = files.last_key_value().expect("at least one EN file");
        let file = Arc::clone(file);
        let offset = u32::try_from(self.current_len)
            .map_err(|_| Error::new("EN file offset exceeds u32"))?;
        self.current_len += len;
        drop(files);
        Ok((id, offset, file))
    }

    fn rotate(&mut self) -> Result<JournalCursor> {
        let files = self.files.read().expect("files");
        let (&old_id, _) =
            files.last_key_value().ok_or_else(|| Error::new("EN journal has no current file"))?;
        let cursor = JournalCursor { file: old_id, end: self.current_len };
        drop(files);
        let id = old_id.checked_add(1).ok_or_else(|| Error::new("EN journal file id overflow"))?;
        let (file, _) = create_file(
            &self.dir,
            self.stem,
            self.suffix,
            self.magic,
            self.en,
            self.disk,
            id,
            self.id_header,
            TimeSec::now(),
        )?;
        self.files.write().expect("files").insert(id, Arc::new(file));
        self.current_len = HEADER;
        Ok(cursor)
    }

    fn ensure_file_after(&mut self, id: u32) -> Result<()> {
        let last = self
            .files
            .read()
            .expect("files")
            .last_key_value()
            .map(|(&last, _)| last)
            .ok_or_else(|| Error::new("EN journal has no files"))?;
        if last > id {
            return Ok(());
        }
        let next = id.checked_add(1).ok_or_else(|| Error::new("EN journal file id overflow"))?;
        let (file, _) = create_file(
            &self.dir,
            self.stem,
            self.suffix,
            self.magic,
            self.en,
            self.disk,
            next,
            self.id_header,
            TimeSec::now(),
        )?;
        self.files.write().expect("files").insert(next, Arc::new(file));
        self.current_len = HEADER;
        Ok(())
    }

    fn get(&self, id: u32) -> Result<Arc<File>> {
        self.files
            .read()
            .expect("files")
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::new(format!("missing EN {} file {id}", self.stem)))
    }

    fn len(&self) -> usize {
        self.files.read().expect("files").len()
    }

    fn bytes_after(&self, after_file: Option<u32>) -> Result<u64> {
        self.files
            .read()
            .expect("files")
            .iter()
            .filter(|(id, _)| after_file.is_none_or(|after| **id > after))
            .try_fold(0u64, |sum, (_, file)| {
                let payload = file.metadata()?.len().saturating_sub(HEADER);
                sum.checked_add(payload).ok_or_else(|| Error::new("EN journal byte count overflow"))
            })
    }
}

fn checkpoint_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("checkpoint-{generation:08}.bin"))
}

fn read_checkpoint(path: &Path) -> Result<Checkpoint> {
    let bytes = fs::read(path)?;
    const PREFIX: usize = 12;
    if bytes.len() < PREFIX + 2 + 39 + CHECKPOINT_FIXED as usize || &bytes[..8] != CHECKPOINT_MAGIC
    {
        return Err(Error::new("truncated EN checkpoint"));
    }
    let op = |at: usize| -> Result<Op> {
        Op::from_code(u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap()))
    };
    if op(8)? != Op::EnCkHeader || u16::from_le_bytes(bytes[10..12].try_into().unwrap()) != 1 {
        return Err(Error::new("unsupported EN checkpoint header"));
    }
    let end_at = bytes.len() - CHECKPOINT_FIXED as usize;
    if op(end_at)? != Op::CheckpointEnd
        || u64::from_le_bytes(bytes[end_at + 2..end_at + 10].try_into().unwrap())
            != bytes.len() as u64
    {
        return Err(Error::new("invalid EN checkpoint fixed footer"));
    }
    let footer_len =
        usize::try_from(u64::from_le_bytes(bytes[end_at + 10..end_at + 18].try_into().unwrap()))
            .map_err(|_| Error::new("EN checkpoint footer is too large"))?;
    if op(end_at + 18)? != Op::EnCkFooter || footer_len != 39 || footer_len > end_at {
        return Err(Error::new("invalid EN checkpoint footer length"));
    }
    let footer_at = end_at - footer_len;
    if op(footer_at)? != Op::EnCkFooter {
        return Err(Error::new("wrong EN checkpoint variable footer"));
    }
    let generation = u64::from_le_bytes(bytes[footer_at + 2..footer_at + 10].try_into().unwrap());
    let cursor = JournalCursor {
        file: u32::from_le_bytes(bytes[footer_at + 10..footer_at + 14].try_into().unwrap()),
        end: u64::from_le_bytes(bytes[footer_at + 14..footer_at + 22].try_into().unwrap()),
    };
    if cursor.end < HEADER || cursor.end > FILE_LIMIT {
        return Err(Error::new("invalid EN checkpoint journal position"));
    }
    let max_scn = match bytes[footer_at + 22] {
        0 => None,
        1 => Some(Scn::new(u64::from_le_bytes(
            bytes[footer_at + 23..footer_at + 31].try_into().unwrap(),
        ))),
        _ => return Err(Error::new("invalid EN checkpoint SCN flag")),
    };
    let checksum = u64::from_le_bytes(bytes[footer_at + 31..footer_at + 39].try_into().unwrap());
    if op(PREFIX)? != Op::EnCkState || common::crc::of(&bytes[..footer_at + 31]) != checksum {
        return Err(Error::new("invalid EN checkpoint checksum or footer"));
    }
    Ok(Checkpoint {
        generation,
        cursor,
        max_scn,
        data: Arc::from(&bytes[PREFIX + 2..footer_at]),
        size: bytes.len() as u64,
    })
}

fn encode_checkpoint(
    generation: u64,
    cursor: JournalCursor,
    max_scn: Option<Scn>,
    data: &[u8],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(data.len().saturating_add(64));
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&Op::EnCkHeader.code().to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&Op::EnCkState.code().to_le_bytes());
    bytes.extend_from_slice(data);
    bytes.extend_from_slice(&Op::EnCkFooter.code().to_le_bytes());
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes.extend_from_slice(&cursor.file.to_le_bytes());
    bytes.extend_from_slice(&cursor.end.to_le_bytes());
    if let Some(scn) = max_scn {
        bytes.push(1);
        bytes.extend_from_slice(&scn.number().to_le_bytes());
    } else {
        bytes.push(0);
        bytes.extend_from_slice(&0u64.to_le_bytes());
    }
    let checksum = common::crc::of(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    let total_len = u64::try_from(bytes.len())
        .map_err(|_| Error::new("EN checkpoint length overflow"))?
        .checked_add(CHECKPOINT_FIXED)
        .ok_or_else(|| Error::new("EN checkpoint length overflow"))?;
    bytes.extend_from_slice(&Op::CheckpointEnd.code().to_le_bytes());
    bytes.extend_from_slice(&total_len.to_le_bytes());
    bytes.extend_from_slice(&39u64.to_le_bytes());
    bytes.extend_from_slice(&Op::EnCkFooter.code().to_le_bytes());
    Ok(bytes)
}

fn load_checkpoints(dir: &Path) -> Result<(Option<Checkpoint>, Vec<CheckpointMeta>, u64)> {
    let mut paths = Vec::new();
    for item in fs::read_dir(dir)? {
        let item = item?;
        let name = item.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(generation) =
            name.strip_prefix("checkpoint-").and_then(|value| value.strip_suffix(".bin"))
        else {
            continue;
        };
        let generation = generation
            .parse::<u64>()
            .map_err(|_| Error::new("invalid EN checkpoint generation"))?;
        paths.push((generation, item.path()));
    }
    paths.sort_by_key(|(generation, _)| *generation);
    let mut latest = None;
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    let mut next_generation = 0;
    for (generation, path) in paths {
        next_generation = next_generation.max(generation.saturating_add(1));
        match read_checkpoint(&path) {
            Ok(checkpoint) if checkpoint.generation == generation => {
                valid.push(CheckpointMeta::from(&checkpoint));
                latest = Some(checkpoint);
            }
            Ok(_) => invalid.push(path),
            Err(_) => invalid.push(path),
        }
    }
    if latest.is_none() && !invalid.is_empty() {
        return Err(Error::new("no valid EN checkpoint remains"));
    }
    Ok((latest, valid, next_generation))
}

/// Open the per-disk files and replay journal metadata. Stage payloads are not read.
pub(crate) struct DiskFiles {
    stage: Arc<RwLock<StageTable>>,
    journal: Arc<RwLock<BTreeMap<u32, Arc<File>>>>,
    dir: PathBuf,
    en: EnId,
    disk: DiskId,
    tx: Sender<WriterMessage>,
    checkpoint_signal: Sender<()>,
    progress: Arc<SyncProgress>,
    checkpoint_rx: Mutex<Option<Receiver<()>>>,
    checkpoint_data: Mutex<CheckpointData>,
    retention: usize,
}

struct CheckpointData {
    latest: Option<Checkpoint>,
    next_generation: u64,
    valid: Vec<CheckpointMeta>,
}

impl DiskFiles {
    pub(crate) fn open(
        dir: &Path,
        en: EnId,
        disk: DiskId,
        key_lookup: KeyLookup,
        max_journal_bytes: u64,
        retention: usize,
    ) -> Result<(Arc<DiskFiles>, Option<Checkpoint>, Vec<Record>)> {
        let en_number = en.get();
        let disk_number = disk.get();
        fs::create_dir_all(dir)?;
        let (mut stage, mut stage_new) = StageFiles::open(dir, en_number, disk_number)?;
        stage_new |= stage.ensure_current_key(&key_lookup)?;
        let (latest, valid_checkpoints, next_generation) = load_checkpoints(dir)?;
        let (mut journal, journal_new) = FileSet::open(
            dir,
            "journal-",
            ".log",
            JOURNAL_MAGIC,
            en_number,
            disk_number,
            false,
            max_journal_bytes,
        )?;
        if stage_new || journal_new {
            File::open(dir)?.sync_all()?;
        }

        let mut records = Vec::new();
        let journal_count = journal.len();
        let journal_ids = journal.files.read().expect("files").keys().copied().collect::<Vec<_>>();
        if let Some(checkpoint) = &latest {
            if !journal_ids.iter().any(|id| *id > checkpoint.cursor.file) {
                journal.ensure_file_after(checkpoint.cursor.file)?;
            }
        }
        for (index, id) in journal_ids.iter().enumerate() {
            if latest.as_ref().is_some_and(|checkpoint| *id <= checkpoint.cursor.file) {
                continue;
            }
            let file = journal.get(*id)?;
            let is_last = index + 1 == journal_count;
            let (mut found, end) = read_journal(&file, *id, is_last)?;
            if end != file.metadata()?.len() {
                file.set_len(end)?;
                file.sync_all()?;
            }
            records.append(&mut found);
        }
        if let Some((_, last)) = journal.files.read().expect("files").last_key_value() {
            journal.current_len = last.metadata()?.len();
        }
        let written_before = records
            .iter()
            .filter(|record| record.op == Op::EnJDurable)
            .map(|record| (record.file, record.end))
            .max();
        let mut stage_files_to_sync = BTreeSet::new();
        let mut journal_files_to_sync = BTreeSet::new();
        for record in &mut records {
            // A durable marker is only written after earlier batches have synced.
            // The marker's own frame and everything after it may be torn, so
            // validate those append payloads and sync their files before applying.
            if written_before.is_some_and(|at| (record.file, record.end) < at) {
                record.written = true;
                continue;
            }
            journal_files_to_sync.insert(record.file);
            if record.stage.len == 0 {
                if record.op == Op::EnJAppend {
                    return Err(Error::new("EN append journal record has no stage range"));
                }
                continue;
            }
            if record.op == Op::EnJAppend && record.stage.len < (STAGE_PREFIX + STAGE_TAG) as u32 {
                return Err(Error::new("EN append stage range lacks its header or tag"));
            }
            if record.stage.file == NO_STAGE_FILE {
                return Err(Error::new("EN journal stage reference has no file"));
            }
            let stage_file = stage.get(record.stage.file)?;
            let end = u64::from(record.stage.offset) + u64::from(record.stage.len);
            if end > stage_file.file.metadata()?.len() {
                return Err(Error::new("EN journal refers past a stage file"));
            }
            stage_files_to_sync.insert(record.stage.file);
            if record.op == Op::EnJAppend {
                let stage_data = read_stage_frame(&stage_file.file, record.stage)?;
                if stage_data.metadata != record.metadata
                    || stage_data.tag != record.stage_tag
                    || stage_data.timestamp != record.scn.time_sec()
                {
                    return Err(Error::new("EN stage header disagrees with its journal record"));
                }
                let daily = key_lookup(stage_file.created)?;
                let (key, nonce) = crypto::derive_stage_secrets(
                    &daily,
                    stage_file.created,
                    en,
                    disk,
                    record.stage.file.get(),
                    record.stage.offset,
                );
                let aad = stage_aad(stage_data.timestamp, &stage_data.metadata);
                crypto::decrypt_block(&key, &nonce, &aad, &stage_data.cipher, &stage_data.tag)?;
            }
        }
        if !stage_files_to_sync.is_empty() || !journal_files_to_sync.is_empty() {
            for id in stage_files_to_sync {
                stage.sync(id)?;
            }
            let journal_files = journal.files.read().expect("journal files");
            for id in journal_files_to_sync {
                journal_files.get(&id).expect("journal record file exists").sync_all()?;
            }
            drop(journal_files);
            for record in &mut records {
                record.written = true;
            }
        }

        let stage_files = Arc::clone(&stage.files);
        let bytes_since_checkpoint =
            journal.bytes_after(latest.as_ref().map(|cp| cp.cursor.file))?;
        let previous_checkpoint_size = latest.as_ref().map_or(0, |cp| cp.size);
        let progress =
            Arc::new(SyncProgress::new(bytes_since_checkpoint, previous_checkpoint_size));
        let journal_files = Arc::clone(&journal.files);
        let (tx, checkpoint_signal, checkpoint_rx) =
            start_writer(stage, journal, Arc::clone(&key_lookup), en, disk, Arc::clone(&progress))?;
        let storage = Arc::new(DiskFiles {
            stage: stage_files,
            journal: journal_files,
            dir: dir.to_path_buf(),
            en,
            disk,
            tx,
            checkpoint_signal,
            progress,
            checkpoint_rx: Mutex::new(Some(checkpoint_rx)),
            checkpoint_data: Mutex::new(CheckpointData {
                latest: latest.clone(),
                next_generation,
                valid: valid_checkpoints,
            }),
            retention,
        });
        storage.enforce_retention()?;
        Ok((storage, latest, records))
    }

    pub(crate) fn submit(
        &self,
        scn: Scn,
        op: Op,
        metadata: Vec<u8>,
        stage: Vec<u8>,
        done: impl FnOnce(Result<Option<StageRef>>) + Send + 'static,
    ) -> Result<()> {
        let request = Request { scn, op, metadata, stage, stage_ref: None, done: Box::new(done) };
        match self.tx.send(WriterMessage::Job(request)) {
            Ok(()) => Ok(()),
            Err(error) => {
                if let WriterMessage::Job(request) = error.0 {
                    (request.done)(Err(Error::new("EN journal writer stopped")));
                }
                Err(Error::new("EN journal writer stopped"))
            }
        }
    }

    pub(crate) fn latest_checkpoint(&self) -> Option<Checkpoint> {
        self.checkpoint_data.lock().expect("EN checkpoint data").latest.clone()
    }

    pub(crate) fn take_checkpoint_receiver(&self) -> Receiver<()> {
        self.checkpoint_rx
            .lock()
            .expect("EN checkpoint receiver")
            .take()
            .expect("checkpoint receiver taken once")
    }

    pub(crate) fn request_checkpoint_if_due(&self) {
        if self.progress.mark_checkpoint_due() {
            let _ = self.checkpoint_signal.send(());
        }
    }

    pub(crate) fn checkpoint_failed(&self) {
        self.progress.checkpoint_failed();
    }

    pub(crate) fn rotate_for_checkpoint(&self) -> Result<CheckpointBoundary> {
        let (reply, response) = mpsc::channel();
        self.tx
            .send(WriterMessage::Checkpoint(reply))
            .map_err(|_| Error::new("EN journal writer stopped before checkpoint rotation"))?;
        response
            .recv()
            .map_err(|_| Error::new("EN journal writer stopped during checkpoint rotation"))?
    }

    pub(crate) fn records_since_checkpoint(&self, through: JournalCursor) -> Result<Vec<Record>> {
        let previous = self.latest_checkpoint();
        let after = previous.map(|checkpoint| checkpoint.cursor.file);
        let ids = self.journal.read().expect("journal files").keys().copied().collect::<Vec<_>>();
        let mut records = Vec::new();
        for id in ids {
            if after.is_some_and(|after| id <= after) || id > through.file {
                continue;
            }
            let file = self
                .journal
                .read()
                .expect("journal files")
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::new("EN journal disappeared during checkpoint"))?;
            let (mut found, _) = read_journal(&file, id, false)?;
            found.retain(|record| {
                record.file < through.file
                    || (record.file == through.file && record.end <= through.end)
            });
            for record in &mut found {
                record.written = true;
            }
            records.append(&mut found);
        }
        Ok(records)
    }

    pub(crate) fn publish_checkpoint(
        &self,
        boundary: CheckpointBoundary,
        max_scn: Option<Scn>,
        data: Vec<u8>,
    ) -> Result<Checkpoint> {
        let mut checkpoints = self.checkpoint_data.lock().expect("EN checkpoint data");
        let generation = checkpoints.next_generation;
        let encoded = encode_checkpoint(generation, boundary.cursor, max_scn, &data)?;
        let size = u64::try_from(encoded.len())
            .map_err(|_| Error::new("EN checkpoint length overflow"))?;
        let path = checkpoint_path(&self.dir, generation);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp =
            self.dir.join(format!(".checkpoint-{}-{generation}-{stamp}.tmp", std::process::id()));
        let result = (|| -> Result<()> {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file =
                OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temp)?;
            use std::io::Write;
            file.write_all(&encoded)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, &path)?;
            File::open(&self.dir)?.sync_all()?;
            Ok(())
        })();
        match result {
            Ok(()) => {}
            Err(error) => {
                let _ = fs::remove_file(&temp);
                return Err(error);
            }
        }
        let checkpoint = Checkpoint {
            generation,
            cursor: boundary.cursor,
            max_scn,
            data: Arc::from(data),
            size,
        };
        checkpoints.next_generation = generation.saturating_add(1);
        checkpoints.valid.push(CheckpointMeta::from(&checkpoint));
        checkpoints.valid.sort_by_key(|item| item.generation);
        while checkpoints.valid.len() > self.retention {
            let old = checkpoints.valid.remove(0);
            fs::remove_file(checkpoint_path(&self.dir, old.generation))?;
        }
        checkpoints.latest = Some(checkpoint.clone());
        let oldest_journal = checkpoints
            .valid
            .first()
            .map(|item| item.cursor.file)
            .ok_or_else(|| Error::new("EN checkpoint retention removed every checkpoint"))?;
        let mut journal_files = self.journal.write().expect("journal files");
        let expired = journal_files.range(..=oldest_journal).map(|(&id, _)| id).collect::<Vec<_>>();
        for id in expired {
            journal_files.remove(&id);
            fs::remove_file(self.dir.join(segment_name("journal-", id, ".log")))?;
        }
        drop(journal_files);
        File::open(&self.dir)?.sync_all()?;
        if self.progress.checkpoint_finished(boundary.bytes_since_previous, checkpoint.size) {
            let _ = self.checkpoint_signal.send(());
        }
        Ok(checkpoint)
    }

    fn enforce_retention(&self) -> Result<()> {
        let mut checkpoints = self.checkpoint_data.lock().expect("EN checkpoint data");
        while checkpoints.valid.len() > self.retention {
            let old = checkpoints.valid.remove(0);
            fs::remove_file(checkpoint_path(&self.dir, old.generation))?;
        }
        if let Some(oldest) = checkpoints.valid.first() {
            let mut journal_files = self.journal.write().expect("journal files");
            let expired =
                journal_files.range(..=oldest.cursor.file).map(|(&id, _)| id).collect::<Vec<_>>();
            for id in expired {
                journal_files.remove(&id);
                fs::remove_file(self.dir.join(segment_name("journal-", id, ".log")))?;
            }
            drop(journal_files);
            File::open(&self.dir)?.sync_all()?;
        }
        Ok(())
    }

    pub(crate) fn read_stage(&self, loc: StageRef) -> Result<StageData> {
        if loc.file == NO_STAGE_FILE || loc.len == 0 {
            return Err(Error::new("invalid EN stage reference"));
        }
        // Stage IDs index a fixed slot table and never shift when another slot
        // is retired or reused.
        let file = self
            .stage
            .read()
            .expect("stage files")
            .get(loc.file.index()?)
            .and_then(Option::as_ref)
            .map(|stage| Arc::clone(&stage.file))
            .ok_or_else(|| Error::new(format!("missing EN stage file {:?}", loc.file)))?;
        read_stage_frame(&file, loc)
    }

    pub(crate) fn stage_time(&self, id: StageId) -> Result<TimeSec> {
        self.stage
            .read()
            .expect("stage files")
            .get(id.index()?)
            .and_then(Option::as_ref)
            .map(|stage| stage.created)
            .ok_or_else(|| Error::new(format!("missing EN stage file {id:?}")))
    }

    pub(crate) const fn en_id(&self) -> EnId {
        self.en
    }

    pub(crate) const fn disk_id(&self) -> DiskId {
        self.disk
    }
}

fn start_writer(
    stage: StageFiles,
    journal: FileSet,
    key_lookup: KeyLookup,
    en: EnId,
    disk: DiskId,
    progress: Arc<SyncProgress>,
) -> Result<(Sender<WriterMessage>, Sender<()>, Receiver<()>)> {
    let (tx, rx) = mpsc::channel::<WriterMessage>();
    let (done_tx, done_rx) = mpsc::channel::<BatchDone>();
    let (checkpoint_signal, checkpoint_rx) = mpsc::channel();
    let stage_sync = Arc::clone(&stage.files);
    let journal_sync = Arc::clone(&journal.files);
    let sync_progress = Arc::clone(&progress);
    let sync_checkpoint_signal = checkpoint_signal.clone();
    std::thread::Builder::new().name("en-journal-sync".into()).spawn(move || {
        sync_loop(done_rx, stage_sync, journal_sync, sync_progress, sync_checkpoint_signal);
    })?;
    std::thread::Builder::new().name("en-journal-write".into()).spawn(move || {
        write_loop(rx, done_tx, stage, journal, key_lookup, en, disk, progress);
    })?;
    Ok((tx, checkpoint_signal, checkpoint_rx))
}

/// Lets the write thread place EnJDurable frames after a known locally synced
/// journal prefix. Recovery can then trust earlier frames without rereading all
/// their staged append payloads.
struct SyncProgress {
    state: Mutex<ProgressState>,
    changed: Condvar,
}

struct ProgressState {
    completed: u64,
    error: Option<String>,
    bytes_since_checkpoint: u64,
    previous_checkpoint_size: u64,
    checkpoint_active: bool,
}

impl SyncProgress {
    fn new(bytes_since_checkpoint: u64, previous_checkpoint_size: u64) -> Self {
        Self {
            state: Mutex::new(ProgressState {
                completed: 0,
                error: None,
                bytes_since_checkpoint,
                previous_checkpoint_size,
                checkpoint_active: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait_for(&self, sequence: u64) -> Result<()> {
        let mut state = self.state.lock().expect("journal sync progress");
        while state.completed < sequence && state.error.is_none() {
            state = self.changed.wait(state).expect("journal sync progress");
        }
        if let Some(error) = &state.error {
            return Err(Error::new(error.clone()));
        }
        Ok(())
    }

    fn complete_through(&self, sequence: u64, error: Option<String>, journal_bytes: u64) -> bool {
        let mut state = self.state.lock().expect("journal sync progress");
        state.completed = sequence;
        if let Some(error) = error {
            if state.error.is_none() {
                state.error = Some(error);
            }
        } else {
            state.bytes_since_checkpoint =
                state.bytes_since_checkpoint.saturating_add(journal_bytes);
        }
        let due = state.error.is_none()
            && !state.checkpoint_active
            && state.bytes_since_checkpoint
                >= CHECKPOINT_MIN_JOURNAL_BYTES.max(state.previous_checkpoint_size);
        if due {
            state.checkpoint_active = true;
        }
        self.changed.notify_all();
        due
    }

    fn mark_checkpoint_due(&self) -> bool {
        let mut state = self.state.lock().expect("journal sync progress");
        if state.error.is_some()
            || state.checkpoint_active
            || state.bytes_since_checkpoint
                < CHECKPOINT_MIN_JOURNAL_BYTES.max(state.previous_checkpoint_size)
        {
            return false;
        }
        state.checkpoint_active = true;
        true
    }

    fn checkpoint_cut_bytes(&self) -> u64 {
        self.state.lock().expect("journal sync progress").bytes_since_checkpoint
    }

    fn checkpoint_finished(&self, cut_bytes: u64, checkpoint_size: u64) -> bool {
        let mut state = self.state.lock().expect("journal sync progress");
        state.bytes_since_checkpoint = state.bytes_since_checkpoint.saturating_sub(cut_bytes);
        state.previous_checkpoint_size = checkpoint_size;
        let due = state.bytes_since_checkpoint
            >= CHECKPOINT_MIN_JOURNAL_BYTES.max(state.previous_checkpoint_size);
        state.checkpoint_active = due;
        self.changed.notify_all();
        due
    }

    fn checkpoint_failed(&self) {
        self.state.lock().expect("journal sync progress").checkpoint_active = false;
    }
}

fn write_loop(
    rx: Receiver<WriterMessage>,
    done: Sender<BatchDone>,
    mut stage: StageFiles,
    mut journal: FileSet,
    key_lookup: KeyLookup,
    en: EnId,
    disk: DiskId,
    progress: Arc<SyncProgress>,
) {
    let mut seq = 0u64;
    let mut pending = None;
    loop {
        let message = match pending.take() {
            Some(message) => message,
            None => match rx.recv() {
                Ok(message) => message,
                Err(_) => break,
            },
        };
        let first = match message {
            WriterMessage::Job(job) => job,
            WriterMessage::Checkpoint(reply) => {
                let result = progress.wait_for(seq).and_then(|_| {
                    let cursor = journal.rotate()?;
                    Ok(CheckpointBoundary {
                        cursor,
                        bytes_since_previous: progress.checkpoint_cut_bytes(),
                    })
                });
                let _ = reply.send(result);
                continue;
            }
        };
        let mut jobs = vec![first];
        while let Ok(message) = rx.try_recv() {
            match message {
                WriterMessage::Job(job) => jobs.push(job),
                message @ WriterMessage::Checkpoint(_) => {
                    pending = Some(message);
                    break;
                }
            }
        }
        let mut stage_writes = BTreeMap::<StageId, WriteAt>::new();
        let mut journal_writes = BTreeMap::<u32, WriteAt>::new();
        let mut stage_files = BTreeMap::new();
        let mut journal_files = BTreeMap::new();
        let mut journal_bytes = 0;
        let mut entries = Vec::with_capacity(jobs.len());
        let batch_seq = seq;
        seq = seq.wrapping_add(1);
        let mut build_error = if jobs.iter().any(|job| job.op == Op::EnJDurable) {
            progress.wait_for(batch_seq).err().map(|error| error.to_string())
        } else {
            None
        };

        for job in &mut jobs {
            if build_error.is_some() {
                break;
            }
            let mut stage_tag = [0; 16];
            let stage_loc = if job.stage.is_empty() && job.op != Op::EnJAppend {
                // Empty metadata-only records do not pin an arbitrary stage file.
                StageRef { file: NO_STAGE_FILE, offset: 0, len: 0 }
            } else {
                let timestamp = job.scn.time_sec();
                let Some(frame_len) = STAGE_PREFIX
                    .checked_add(job.metadata.len())
                    .and_then(|len| len.checked_add(job.stage.len()))
                    .and_then(|len| len.checked_add(STAGE_TAG))
                else {
                    build_error = Some("stage append frame length overflow".into());
                    break;
                };
                let len = match u32::try_from(frame_len) {
                    Ok(len) => len,
                    Err(_) => {
                        build_error = Some("stage append exceeds u32".into());
                        break;
                    }
                };
                match stage.reserve(frame_len, &key_lookup) {
                    Ok((id, offset, stage_time, file)) => {
                        let daily = match key_lookup(stage_time) {
                            Ok(key) => key,
                            Err(error) => {
                                build_error = Some(error.to_string());
                                break;
                            }
                        };
                        let (key, nonce) = crypto::derive_stage_secrets(
                            &daily,
                            stage_time,
                            en,
                            disk,
                            id.get(),
                            offset,
                        );
                        let aad = stage_aad(timestamp, &job.metadata);
                        let (cipher, tag) = crypto::encrypt_block(&key, &nonce, &aad, &job.stage);
                        let frame =
                            match encode_stage_frame(timestamp, &job.metadata, &cipher, &tag) {
                                Ok(frame) => frame,
                                Err(error) => {
                                    build_error = Some(error.to_string());
                                    break;
                                }
                            };
                        if let Err(error) = add_write(&mut stage_writes, id, offset, &file, &frame)
                        {
                            build_error = Some(error.to_string());
                            break;
                        }
                        stage_files.insert(id, file);
                        stage_tag = tag;
                        StageRef { file: id, offset, len }
                    }
                    Err(error) => {
                        build_error = Some(error.to_string());
                        break;
                    }
                }
            };
            job.stage_ref = (stage_loc.file != NO_STAGE_FILE).then_some(stage_loc);
            entries.push(JournalEntry {
                scn: job.scn,
                op: job.op,
                stage: stage_loc,
                stage_tag,
                metadata: job.metadata.clone(),
            });
        }

        if build_error.is_none() {
            match encode_frame(&entries) {
                Ok(frame) => match journal.reserve(frame.len()) {
                    Ok((id, offset, file)) => {
                        journal_bytes = frame.len() as u64;
                        if let Err(error) =
                            add_write(&mut journal_writes, id, offset, &file, &frame)
                        {
                            build_error = Some(error.to_string());
                        } else {
                            journal_files.insert(id, file);
                        }
                    }
                    Err(error) => build_error = Some(error.to_string()),
                },
                Err(error) => build_error = Some(error.to_string()),
            }
        }

        if let Some(error) = build_error {
            if let Some(WriterMessage::Checkpoint(reply)) = pending.take() {
                let _ = reply.send(Err(Error::new(format!("EN journal writer stopped: {error}"))));
            }
            let batch = BatchDone {
                seq: batch_seq,
                jobs,
                stage_files,
                journal_files,
                journal_bytes,
                result: Err(error.clone()),
            };
            if let Err(batch) = done.send(batch) {
                for job in batch.0.jobs {
                    (job.done)(Err(Error::new("EN journal sync worker stopped")));
                }
            }
            while let Ok(message) = rx.recv() {
                match message {
                    WriterMessage::Job(job) => {
                        (job.done)(Err(Error::new(format!("EN journal writer stopped: {error}"))));
                    }
                    WriterMessage::Checkpoint(reply) => {
                        let _ = reply
                            .send(Err(Error::new(format!("EN journal writer stopped: {error}"))));
                    }
                }
            }
            break;
        }
        let writes =
            stage_writes.into_values().chain(journal_writes.into_values()).collect::<Vec<_>>();
        let batch_done = done.clone();
        std::thread::spawn(move || {
            let result = std::thread::scope(|scope| {
                let handles = writes
                    .iter()
                    .map(|write| {
                        scope.spawn(move || {
                            write_all_at(&write.file, &write.bytes, u64::from(write.offset))
                        })
                    })
                    .collect::<Vec<_>>();
                let mut first_error = None;
                for handle in handles {
                    match handle
                        .join()
                        .unwrap_or_else(|_| Err(io::Error::other("EN file writer panicked")))
                    {
                        Ok(()) => {}
                        Err(error) if first_error.is_none() => first_error = Some(error),
                        Err(_) => {}
                    }
                }
                first_error.map_or(Ok(()), Err)
            })
            .map_err(|error| error.to_string());
            let _ = batch_done.send(BatchDone {
                seq: batch_seq,
                jobs,
                stage_files,
                journal_files,
                journal_bytes,
                result,
            });
        });
    }
}

fn add_write<K: Ord>(
    groups: &mut BTreeMap<K, WriteAt>,
    id: K,
    offset: u32,
    file: &Arc<File>,
    bytes: &[u8],
) -> Result<()> {
    if let Some(group) = groups.get_mut(&id) {
        if group.offset as usize + group.bytes.len() != offset as usize {
            return Err(Error::new("non-contiguous EN batch file ranges"));
        }
        group.bytes.extend_from_slice(bytes);
    } else {
        groups.insert(
            id,
            WriteAt { file: Arc::clone(file), offset: u64::from(offset), bytes: bytes.to_vec() },
        );
    }
    Ok(())
}

fn sync_loop(
    rx: Receiver<BatchDone>,
    stage: Arc<RwLock<StageTable>>,
    journal: Arc<RwLock<BTreeMap<u32, Arc<File>>>>,
    progress: Arc<SyncProgress>,
    checkpoint_signal: Sender<()>,
) {
    let mut next = 0u64;
    let mut ready = BTreeMap::new();
    let mut failed: Option<String> = None;
    while let Ok(batch) = rx.recv() {
        ready.insert(batch.seq, batch);
        while let Ok(batch) = rx.try_recv() {
            ready.insert(batch.seq, batch);
        }
        let mut batches = Vec::new();
        while let Some(batch) = ready.remove(&next) {
            batches.push(batch);
            next = next.wrapping_add(1);
        }
        if batches.is_empty() {
            continue;
        }

        let mut stage_ids = BTreeSet::<StageId>::new();
        let mut journal_ids = BTreeSet::new();
        let mut journal_bytes = 0u64;
        let mut jobs = Vec::new();
        let mut write_error = failed.clone();
        for batch in batches {
            if write_error.is_none() {
                write_error = batch.result.err();
            }
            stage_ids.extend(batch.stage_files.keys().copied());
            journal_ids.extend(batch.journal_files.keys().copied());
            journal_bytes = journal_bytes.saturating_add(batch.journal_bytes);
            jobs.extend(batch.jobs);
        }
        let result = if let Some(error) = write_error {
            failed = Some(error.clone());
            Err(Error::new(format!("EN journal write failed: {error}")))
        } else {
            let sync = (|| -> io::Result<()> {
                let stage_files = stage.read().expect("stage files");
                for id in stage_ids {
                    stage_files
                        .get(id.index().map_err(|error| io::Error::other(error.to_string()))?)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| io::Error::other("missing stage segment"))?
                        .file
                        .sync_all()?;
                }
                let journal_files = journal.read().expect("journal files");
                for id in journal_ids {
                    journal_files
                        .get(&id)
                        .ok_or_else(|| io::Error::other("missing journal segment"))?
                        .sync_all()?;
                }
                Ok(())
            })();
            match sync {
                Ok(()) => Ok(()),
                Err(error) => {
                    let message = format!("EN durable sync failed: {error}");
                    failed = Some(message.clone());
                    Err(Error::new(message))
                }
            }
        };
        let checkpoint_due = progress.complete_through(
            next,
            result.as_ref().err().map(|error| error.to_string()),
            journal_bytes,
        );
        if checkpoint_due {
            let _ = checkpoint_signal.send(());
        }
        for job in jobs {
            let response = result
                .as_ref()
                .map(|_| job.stage_ref)
                .map_err(|error| Error::new(error.to_string()));
            (job.done)(response);
        }
    }
}

fn segment_name(stem: &str, id: u32, suffix: &str) -> String {
    format!("{stem}{id:08}{suffix}")
}

fn make_header(magic: &[u8; 8], en: u64, disk: u16, id: u32, created: TimeSec) -> [u8; HEADER_LEN] {
    let mut header = [0; HEADER_LEN];
    header[..8].copy_from_slice(magic);
    header[8..16].copy_from_slice(&en.to_le_bytes());
    header[16..18].copy_from_slice(&disk.to_le_bytes());
    header[18..20].copy_from_slice(&1u16.to_le_bytes());
    header[20..24].copy_from_slice(&id.to_le_bytes());
    header[24..32].copy_from_slice(&created.number().to_le_bytes());
    let crc = common::crc::of(&header[..32]);
    header[32..40].copy_from_slice(&crc.to_le_bytes());
    header
}

fn create_file(
    dir: &Path,
    stem: &str,
    suffix: &str,
    magic: &[u8; 8],
    en: u64,
    disk: u16,
    id: u32,
    id_header: bool,
    created: TimeSec,
) -> Result<(File, bool)> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = dir.join(segment_name(stem, id, suffix));
    let file = OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(path)?;
    write_all_at(&file, &make_header(magic, en, disk, if id_header { id } else { 0 }, created), 0)?;
    file.sync_all()?;
    File::open(dir)?.sync_all()?;
    Ok((file, true))
}

fn open_file(
    path: &Path,
    magic: &[u8; 8],
    en: u64,
    disk: u16,
    id: u32,
    id_header: bool,
) -> Result<(File, TimeSec)> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut header = [0; HEADER_LEN];
    read_exact_at(&file, &mut header, 0)?;
    let expected_id = if id_header { id } else { 0 };
    let created = TimeSec::new(u64::from_le_bytes(header[24..32].try_into().unwrap()));
    if &header[..8] != magic
        || u64::from_le_bytes(header[8..16].try_into().unwrap()) != en
        || u16::from_le_bytes(header[16..18].try_into().unwrap()) != disk
        || u16::from_le_bytes(header[18..20].try_into().unwrap()) != 1
        || u32::from_le_bytes(header[20..24].try_into().unwrap()) != expected_id
        || common::crc::of(&header[..32]) != u64::from_le_bytes(header[32..40].try_into().unwrap())
    {
        return Err(Error::new(format!("EN file identity mismatch: {}", path.display())));
    }
    Ok((file, created))
}

fn encode_frame(entries: &[JournalEntry]) -> Result<Vec<u8>> {
    let count =
        u32::try_from(entries.len()).map_err(|_| Error::new("too many EN journal entries"))?;
    let mut body = Vec::new();
    body.extend_from_slice(&count.to_le_bytes());
    for entry in entries {
        let metadata_len = u32::try_from(entry.metadata.len())
            .map_err(|_| Error::new("EN journal metadata exceeds 4 GiB"))?;
        body.extend_from_slice(&entry.scn.number().to_le_bytes());
        body.extend_from_slice(&entry.op.code().to_le_bytes());
        body.extend_from_slice(&entry.stage.file.get().to_le_bytes());
        body.extend_from_slice(&entry.stage.offset.to_le_bytes());
        body.extend_from_slice(&entry.stage.len.to_le_bytes());
        body.extend_from_slice(&entry.stage_tag);
        body.extend_from_slice(&metadata_len.to_le_bytes());
        body.extend_from_slice(&entry.metadata);
    }
    body.extend_from_slice(&common::crc::of(&body).to_le_bytes());
    let body_len = body.len();
    let length =
        u32::try_from(body_len).map_err(|_| Error::new("EN journal record exceeds 4 GiB"))?;
    let mut frame = Vec::with_capacity(8 + body.len());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&(!length).to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn read_journal(file: &File, id: u32, last_file: bool) -> Result<(Vec<Record>, u64)> {
    let file_len = file.metadata()?.len();
    let mut at = HEADER;
    let mut records = Vec::new();
    while at < file_len {
        let mut prefix = [0; 8];
        if read_exact_at(file, &mut prefix, at).is_err() {
            if last_file {
                break;
            }
            return Err(Error::new("truncated EN journal segment"));
        }
        let len = u32::from_le_bytes(prefix[..4].try_into().unwrap());
        let inverse = u32::from_le_bytes(prefix[4..].try_into().unwrap());
        if inverse != !len {
            if last_file && file_len - at <= 8 {
                break;
            }
            return Err(Error::new("invalid EN journal frame length"));
        }
        let end = at
            .checked_add(8 + u64::from(len))
            .ok_or_else(|| Error::new("EN journal length overflow"))?;
        if len < 52 || end > file_len {
            if last_file {
                break;
            }
            return Err(Error::new("truncated EN journal frame"));
        }
        let mut body = vec![0; len as usize];
        read_exact_at(file, &mut body, at + 8)?;
        let payload_end = body.len() - 8;
        let stored = u64::from_le_bytes(body[payload_end..].try_into().unwrap());
        if common::crc::of(&body[..payload_end]) != stored {
            if last_file && end == file_len {
                break;
            }
            return Err(Error::new("corrupt EN journal checksum"));
        }
        if payload_end < 4 {
            return Err(Error::new("truncated EN journal frame count"));
        }
        let count = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
        if count == 0 {
            return Err(Error::new("empty EN journal frame"));
        }
        let mut pos = 4usize;
        for _ in 0..count {
            let header_end = pos
                .checked_add(40)
                .ok_or_else(|| Error::new("EN journal record offset overflow"))?;
            if header_end > payload_end {
                return Err(Error::new("truncated EN journal record header"));
            }
            let metadata_len =
                u32::from_le_bytes(body[pos + 36..pos + 40].try_into().unwrap()) as usize;
            let metadata_end = header_end
                .checked_add(metadata_len)
                .ok_or_else(|| Error::new("EN journal metadata length overflow"))?;
            if metadata_end > payload_end {
                return Err(Error::new("truncated EN journal metadata"));
            }
            let stage_raw = u16::from_le_bytes(body[pos + 10..pos + 12].try_into().unwrap());
            let stage_file =
                if stage_raw == u16::MAX { NO_STAGE_FILE } else { StageId::new(stage_raw)? };
            records.push(Record {
                scn: Scn::new(u64::from_le_bytes(body[pos..pos + 8].try_into().unwrap())),
                op: Op::from_code(u16::from_le_bytes(body[pos + 8..pos + 10].try_into().unwrap()))?,
                stage: StageRef {
                    file: stage_file,
                    offset: u32::from_le_bytes(body[pos + 12..pos + 16].try_into().unwrap()),
                    len: u32::from_le_bytes(body[pos + 16..pos + 20].try_into().unwrap()),
                },
                stage_tag: body[pos + 20..pos + 36].try_into().unwrap(),
                metadata: body[header_end..metadata_end].to_vec(),
                written: false,
                file: id,
                end,
            });
            pos = metadata_end;
        }
        if pos != payload_end {
            return Err(Error::new("trailing bytes in EN journal frame"));
        }
        at = end;
    }
    Ok((records, at))
}

pub(crate) fn stage_aad(timestamp: TimeSec, metadata: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(8 + metadata.len());
    aad.extend_from_slice(&timestamp.number().to_le_bytes());
    aad.extend_from_slice(metadata);
    aad
}

fn encode_stage_frame(
    timestamp: TimeSec,
    metadata: &[u8],
    cipher: &[u8],
    tag: &[u8; STAGE_TAG],
) -> Result<Vec<u8>> {
    let metadata_len =
        u32::try_from(metadata.len()).map_err(|_| Error::new("EN stage metadata exceeds u32"))?;
    let rest_len = 8usize
        .checked_add(4)
        .and_then(|len| len.checked_add(metadata.len()))
        .and_then(|len| len.checked_add(cipher.len()))
        .and_then(|len| len.checked_add(STAGE_TAG))
        .ok_or_else(|| Error::new("EN stage frame length overflow"))?;
    let frame_len =
        u32::try_from(rest_len).map_err(|_| Error::new("EN stage frame exceeds u32"))?;
    let mut frame = Vec::with_capacity(8 + rest_len);
    frame.extend_from_slice(&frame_len.to_le_bytes());
    frame.extend_from_slice(&(!frame_len).to_le_bytes());
    frame.extend_from_slice(&timestamp.number().to_le_bytes());
    frame.extend_from_slice(&metadata_len.to_le_bytes());
    frame.extend_from_slice(metadata);
    frame.extend_from_slice(cipher);
    frame.extend_from_slice(tag);
    Ok(frame)
}

fn read_stage_frame(file: &File, loc: StageRef) -> Result<StageData> {
    if loc.len < (STAGE_PREFIX + STAGE_TAG) as u32 {
        return Err(Error::new("truncated EN stage frame"));
    }
    let mut prefix = [0; STAGE_PREFIX];
    read_exact_at(file, &mut prefix, u64::from(loc.offset))?;
    let frame_len = u32::from_le_bytes(prefix[..4].try_into().unwrap());
    let inverse = u32::from_le_bytes(prefix[4..8].try_into().unwrap());
    if inverse != !frame_len || u64::from(frame_len) + 8 != u64::from(loc.len) {
        return Err(Error::new("invalid EN stage frame length"));
    }
    let timestamp = TimeSec::new(u64::from_le_bytes(prefix[8..16].try_into().unwrap()));
    let metadata_len = u32::from_le_bytes(prefix[16..20].try_into().unwrap()) as usize;
    let metadata_end = STAGE_PREFIX
        .checked_add(metadata_len)
        .ok_or_else(|| Error::new("EN stage metadata length overflow"))?;
    let tag_start = (loc.len as usize)
        .checked_sub(STAGE_TAG)
        .ok_or_else(|| Error::new("truncated EN stage tag"))?;
    if metadata_end > tag_start {
        return Err(Error::new("EN stage metadata extends past its frame"));
    }
    let mut metadata = vec![0; metadata_len];
    read_exact_at(file, &mut metadata, u64::from(loc.offset) + STAGE_PREFIX as u64)?;
    let mut cipher = vec![0; tag_start - metadata_end];
    read_exact_at(file, &mut cipher, u64::from(loc.offset) + metadata_end as u64)?;
    let mut tag = [0; STAGE_TAG];
    read_exact_at(file, &mut tag, u64::from(loc.offset) + tag_start as u64)?;
    Ok(StageData { timestamp, metadata, cipher, tag })
}

fn write_all_at(file: &File, mut bytes: &[u8], mut at: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let n = file.write_at(bytes, at)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "EN positional write"));
        }
        at += n as u64;
        bytes = &bytes[n..];
    }
    Ok(())
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], mut at: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let n = file.read_at(bytes, at)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EN positional read"));
        }
        at += n as u64;
        bytes = &mut bytes[n..];
    }
    Ok(())
}
