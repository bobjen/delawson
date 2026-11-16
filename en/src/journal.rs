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
use std::sync::{Arc, Mutex, RwLock};

const JOURNAL_MAGIC: &[u8; 8] = b"DLAWENJ1";
const STAGE_MAGIC: &[u8; 8] = b"DLAWENS1";
const ID_MAGIC: &[u8; 8] = b"DLAWENI1";
const HEADER_LEN: usize = 40;
const HEADER: u64 = HEADER_LEN as u64;
pub(crate) const STAGE_PREFIX: usize = 20;
pub(crate) const STAGE_TAG: usize = 16;
const FILE_LIMIT: u64 = 1u64 << 32;
const STAGE_SLOTS: usize = 16 * 1024;
const NO_STAGE_FILE: StageId = StageId(u16::MAX);
const ID_SLOT: usize = 56;
const ID_LEN: u64 = (2 * ID_SLOT) as u64;

pub(crate) type KeyLookup = Arc<dyn Fn(TimeSec) -> Result<[u8; 32]> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct StageId(u16);

impl StageId {
    const FIRST: Self = Self(0);

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
    pub end: u64,
}

type Done = Box<dyn FnOnce(Result<Option<StageRef>>) + Send + 'static>;

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
    identity: Option<Arc<Mutex<Identity>>>,
}

struct Identity {
    file: Arc<File>,
    last_journal: u32,
    written_file: u32,
    written_end: u64,
    generation: u64,
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
    ) -> Result<(FileSet, bool)> {
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
                identity: None,
            },
            created,
        ))
    }

    fn reserve(&mut self, len: usize) -> Result<(u32, u32, Arc<File>)> {
        let len = u64::try_from(len).map_err(|_| Error::new("EN write length overflow"))?;
        if len > FILE_LIMIT - HEADER {
            return Err(Error::new("one EN write exceeds the 4 GiB file limit"));
        }
        if self.current_len.checked_add(len).is_none_or(|end| end > FILE_LIMIT) {
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
            if !self.id_header {
                if let Some(identity) = &self.identity {
                    let mut identity = identity.lock().expect("identity");
                    identity.generation = write_identity(
                        &identity.file,
                        self.en,
                        self.disk,
                        id,
                        identity.written_file,
                        identity.written_end,
                        identity.generation,
                        true,
                    )?;
                    identity.last_journal = id;
                }
            }
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
}

/// Open the per-disk files and replay journal metadata. Stage payloads are not read.
pub(crate) struct DiskFiles {
    stage: Arc<RwLock<StageTable>>,
    en: EnId,
    disk: DiskId,
    tx: Sender<Request>,
}

impl DiskFiles {
    pub(crate) fn open(
        dir: &Path,
        en: EnId,
        disk: DiskId,
        key_lookup: KeyLookup,
    ) -> Result<(Arc<DiskFiles>, Vec<Record>)> {
        let en_number = en.get();
        let disk_number = disk.get();
        fs::create_dir_all(dir)?;
        let identity_record = read_identity(&dir.join("identity"), en_number, disk_number)?;
        let (mut stage, mut stage_new) = StageFiles::open(dir, en_number, disk_number)?;
        stage_new |= stage.ensure_current_key(&key_lookup)?;
        let (mut journal, journal_new) =
            FileSet::open(dir, "journal-", ".log", JOURNAL_MAGIC, en_number, disk_number, false)?;
        if stage_new || journal_new {
            File::open(dir)?.sync_all()?;
        }

        let last_journal = *journal
            .files
            .read()
            .expect("journal files")
            .last_key_value()
            .expect("at least one journal file")
            .0;
        let identity = match identity_record {
            Some((file, expected, written_file, written_end, generation)) => {
                if journal_new {
                    return Err(Error::new("EN identity names a missing journal file"));
                }
                if last_journal != expected {
                    // A crash after creating a new empty segment but before updating
                    // identity is safe to repair; no data is written before the update.
                    let latest = journal.get(last_journal)?;
                    let persisted_file = journal.get(written_file)?;
                    if expected.checked_add(1) != Some(last_journal)
                        || written_file > expected
                        || persisted_file.metadata()?.len() < written_end
                        || latest.metadata()?.len() != HEADER
                    {
                        return Err(Error::new("EN identity does not match the last journal file"));
                    }
                    let generation = write_identity(
                        &file,
                        en_number,
                        disk_number,
                        last_journal,
                        written_file,
                        written_end,
                        generation,
                        true,
                    )?;
                    Identity {
                        file: Arc::new(file),
                        last_journal,
                        written_file,
                        written_end,
                        generation,
                    }
                } else {
                    Identity {
                        file: Arc::new(file),
                        last_journal: expected,
                        written_file,
                        written_end,
                        generation,
                    }
                }
            }
            None => {
                let files = journal.files.read().expect("files");
                for file in files.values() {
                    if file.metadata()?.len() != HEADER {
                        return Err(Error::new("EN journal exists without its identity file"));
                    }
                }
                drop(files);
                let (file, generation) =
                    create_identity(dir, en_number, disk_number, last_journal)?;
                Identity {
                    file: Arc::new(file),
                    last_journal,
                    written_file: last_journal,
                    written_end: HEADER,
                    generation,
                }
            }
        };
        let written_file = journal.get(identity.written_file)?;
        if identity.written_end < HEADER || identity.written_end > written_file.metadata()?.len() {
            return Err(Error::new("EN identity has an invalid written journal position"));
        }
        journal.identity = Some(Arc::new(Mutex::new(identity)));

        let mut records = Vec::new();
        let journal_count = journal.len();
        let journal_ids = journal.files.read().expect("files").keys().copied().collect::<Vec<_>>();
        for (index, id) in journal_ids.iter().enumerate() {
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
        let (written_file, written_end) = {
            let identity = journal.identity.as_ref().expect("identity").lock().expect("identity");
            (identity.written_file, identity.written_end)
        };
        let mut dirty_stage = BTreeSet::new();
        let mut dirty_journal = BTreeSet::new();
        for record in &mut records {
            record.written = record.file < written_file
                || (record.file == written_file && record.end <= written_end);
            // The disk-level watermark turns the recovered journal into a local
            // written prefix. A surviving frame after it is checked against the
            // bytes it names before it can be accepted as written.
            if !record.written {
                dirty_journal.insert(record.file);
            }
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
            if !record.written && record.op == Op::EnJAppend {
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
                dirty_stage.insert(record.stage.file);
            }
        }
        if !dirty_stage.is_empty() || !dirty_journal.is_empty() {
            for id in dirty_stage {
                stage.sync(id)?;
            }
            let journal_files = journal.files.read().expect("journal files");
            for id in dirty_journal {
                journal_files.get(&id).expect("journal record file exists").sync_all()?;
            }
            drop(journal_files);
            for record in &mut records {
                record.written = true;
            }
            if let Some(last) = records.last() {
                let mut identity =
                    journal.identity.as_ref().expect("identity").lock().expect("identity");
                if (last.file, last.end) > (identity.written_file, identity.written_end) {
                    identity.generation = write_identity(
                        &identity.file,
                        en_number,
                        disk_number,
                        identity.last_journal,
                        last.file,
                        last.end,
                        identity.generation,
                        false,
                    )?;
                    identity.written_file = last.file;
                    identity.written_end = last.end;
                }
            }
        }

        let stage_files = Arc::clone(&stage.files);
        let tx = start_writer(stage, journal, Arc::clone(&key_lookup), en, disk)?;
        Ok((Arc::new(DiskFiles { stage: stage_files, en, disk, tx }), records))
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
        match self.tx.send(request) {
            Ok(()) => Ok(()),
            Err(error) => {
                (error.0.done)(Err(Error::new("EN journal writer stopped")));
                Err(Error::new("EN journal writer stopped"))
            }
        }
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
) -> Result<Sender<Request>> {
    let (tx, rx) = mpsc::channel::<Request>();
    let (done_tx, done_rx) = mpsc::channel::<BatchDone>();
    let stage_sync = Arc::clone(&stage.files);
    let journal_sync = Arc::clone(&journal.files);
    std::thread::Builder::new().name("en-journal-sync".into()).spawn(move || {
        sync_loop(done_rx, stage_sync, journal_sync);
    })?;
    std::thread::Builder::new().name("en-journal-write".into()).spawn(move || {
        write_loop(rx, done_tx, stage, journal, key_lookup, en, disk);
    })?;
    Ok(tx)
}

fn write_loop(
    rx: Receiver<Request>,
    done: Sender<BatchDone>,
    mut stage: StageFiles,
    mut journal: FileSet,
    key_lookup: KeyLookup,
    en: EnId,
    disk: DiskId,
) {
    let mut seq = 0u64;
    while let Ok(first) = rx.recv() {
        let mut jobs = vec![first];
        while let Ok(job) = rx.try_recv() {
            jobs.push(job);
        }
        let mut stage_writes = BTreeMap::<StageId, WriteAt>::new();
        let mut journal_writes = BTreeMap::<u32, WriteAt>::new();
        let mut stage_files = BTreeMap::new();
        let mut journal_files = BTreeMap::new();
        let mut entries = Vec::with_capacity(jobs.len());
        let mut build_error = None;

        for job in &mut jobs {
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

        let batch_seq = seq;
        seq = seq.wrapping_add(1);
        if let Some(error) = build_error {
            let batch = BatchDone {
                seq: batch_seq,
                jobs,
                stage_files,
                journal_files,
                result: Err(error.clone()),
            };
            if let Err(batch) = done.send(batch) {
                for job in batch.0.jobs {
                    (job.done)(Err(Error::new("EN journal sync worker stopped")));
                }
            }
            while let Ok(job) = rx.recv() {
                (job.done)(Err(Error::new(format!("EN journal writer stopped: {error}"))));
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
        let mut jobs = Vec::new();
        let mut write_error = failed.clone();
        for batch in batches {
            if write_error.is_none() {
                write_error = batch.result.err();
            }
            stage_ids.extend(batch.stage_files.keys().copied());
            journal_ids.extend(batch.journal_files.keys().copied());
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

fn identity_slot(
    en: u64,
    disk: u16,
    last: u32,
    written_file: u32,
    written_end: u64,
    generation: u64,
) -> [u8; ID_SLOT] {
    let mut bytes = [0; ID_SLOT];
    bytes[..8].copy_from_slice(ID_MAGIC);
    bytes[8..16].copy_from_slice(&en.to_le_bytes());
    bytes[16..18].copy_from_slice(&disk.to_le_bytes());
    bytes[18..20].copy_from_slice(&1u16.to_le_bytes());
    bytes[20..24].copy_from_slice(&last.to_le_bytes());
    bytes[24..28].copy_from_slice(&written_file.to_le_bytes());
    bytes[32..40].copy_from_slice(&written_end.to_le_bytes());
    bytes[40..48].copy_from_slice(&generation.to_le_bytes());
    let crc = common::crc::of(&bytes[..48]);
    bytes[48..56].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_identity(bytes: &[u8; ID_SLOT], en: u64, disk: u16) -> Option<(u32, u32, u64, u64)> {
    if &bytes[..8] != ID_MAGIC
        || u64::from_le_bytes(bytes[8..16].try_into().ok()?) != en
        || u16::from_le_bytes(bytes[16..18].try_into().ok()?) != disk
        || u16::from_le_bytes(bytes[18..20].try_into().ok()?) != 1
        || common::crc::of(&bytes[..48]) != u64::from_le_bytes(bytes[48..56].try_into().ok()?)
    {
        return None;
    }
    let generation = u64::from_le_bytes(bytes[40..48].try_into().ok()?);
    if generation == 0 {
        return None;
    }
    Some((
        u32::from_le_bytes(bytes[20..24].try_into().ok()?),
        u32::from_le_bytes(bytes[24..28].try_into().ok()?),
        u64::from_le_bytes(bytes[32..40].try_into().ok()?),
        generation,
    ))
}

fn read_identity(path: &Path, en: u64, disk: u16) -> Result<Option<(File, u32, u32, u64, u64)>> {
    if !path.exists() {
        return Ok(None);
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    if file.metadata()?.len() < ID_LEN {
        return Err(Error::new("truncated EN identity file"));
    }
    let mut best: Option<(u32, u32, u64, u64)> = None;
    for slot in 0..2 {
        let mut bytes = [0; ID_SLOT];
        if read_exact_at(&file, &mut bytes, (slot * ID_SLOT) as u64).is_ok()
            && let Some((last, written_file, written_end, generation)) =
                decode_identity(&bytes, en, disk)
            && best.is_none_or(|(_, _, _, old_generation)| generation > old_generation)
        {
            best = Some((last, written_file, written_end, generation));
        }
    }
    let (last, written_file, written_end, generation) =
        best.ok_or_else(|| Error::new("invalid EN identity file"))?;
    Ok(Some((file, last, written_file, written_end, generation)))
}

fn create_identity(dir: &Path, en: u64, disk: u16, last: u32) -> Result<(File, u64)> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = dir.join("identity");
    let file = OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(path)?;
    file.set_len(ID_LEN)?;
    write_all_at(&file, &identity_slot(en, disk, last, last, HEADER, 1), 0)?;
    file.sync_all()?;
    File::open(dir)?.sync_all()?;
    Ok((file, 1))
}

fn write_identity(
    file: &File,
    en: u64,
    disk: u16,
    last: u32,
    written_file: u32,
    written_end: u64,
    generation: u64,
    sync: bool,
) -> Result<u64> {
    let generation =
        generation.checked_add(1).ok_or_else(|| Error::new("EN identity generation overflow"))?;
    let slot = ((generation - 1) % 2) as usize;
    write_all_at(
        file,
        &identity_slot(en, disk, last, written_file, written_end, generation),
        (slot * ID_SLOT) as u64,
    )?;
    if sync {
        file.sync_all()?;
    }
    Ok(generation)
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
