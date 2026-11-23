//! EN-wide durable inventory of its local disks.
//!
//! This state lives outside the per-disk journals because it must still name a
//! disk when that disk is missing. The process configuration locates devices;
//! this journal records the devices EN has admitted.

use common::config::ProcessDiskConfig;
use common::ids::DiskId;
use common::{Error, Op, Result, logw};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const JOURNAL_MAGIC: &[u8; 8] = b"DLAWENDJ";
const CHECKPOINT_MAGIC: &[u8; 8] = b"DLAWENDC";
const VERSION: u16 = 1;
const JOURNAL_HEADER: usize = 18;
const CHECKPOINT_FOOTER: usize = 18;
const JOURNAL_NAME: &str = "disk-roster.log";
const CHECKPOINT_NAME: &str = "disk-roster.checkpoint";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskState {
    Active = 1,
    Retired = 2,
}

impl DiskState {
    fn read(value: u8) -> Result<DiskState> {
        match value {
            1 => Ok(DiskState::Active),
            2 => Ok(DiskState::Retired),
            _ => Err(Error::new("invalid EN disk lifecycle state")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiskRecord {
    id: DiskId,
    device_identity: String,
    path: String,
    capacity_bytes: u64,
    state: DiskState,
}

pub(crate) struct DiskRoster {
    dir: PathBuf,
    journal: File,
    records: BTreeMap<DiskId, DiskRecord>,
    sequence: u64,
    has_checkpoint: bool,
}

impl DiskRoster {
    pub(crate) fn open(dir: &Path) -> Result<DiskRoster> {
        fs::create_dir_all(dir)?;
        let journal_path = dir.join(JOURNAL_NAME);
        let mut journal =
            OpenOptions::new().read(true).write(true).create(true).open(&journal_path)?;
        if journal.metadata()?.len() == 0 {
            journal.write_all(&journal_header())?;
            journal.sync_all()?;
            File::open(dir)?.sync_all()?;
        }
        let journal_bytes = fs::read(&journal_path)?;
        validate_journal_header(&journal_bytes)?;

        let checkpoint_path = dir.join(CHECKPOINT_NAME);
        let (checkpoint, has_checkpoint) = if checkpoint_path.exists() {
            match read_checkpoint(&checkpoint_path) {
                Ok(checkpoint) => (Some(checkpoint), true),
                Err(error) => {
                    logw!(
                        "invalid EN disk roster checkpoint; rebuilding from journal",
                        "{}",
                        error
                    );
                    (None, false)
                }
            }
        } else {
            (None, false)
        };
        let checkpoint_sequence = checkpoint.as_ref().map_or(0, |(sequence, _)| *sequence);
        let mut records = checkpoint.map_or_else(BTreeMap::new, |(_, records)| records);

        let mut at = JOURNAL_HEADER;
        let mut valid_end = at;
        let mut expected_sequence = 1u64;
        while at < journal_bytes.len() {
            if journal_bytes.len() - at < 4 {
                break;
            }
            let body_len =
                u32::from_le_bytes(journal_bytes[at..at + 4].try_into().unwrap()) as usize;
            let frame_len = 4usize
                .checked_add(body_len)
                .and_then(|len| len.checked_add(8))
                .ok_or_else(|| Error::new("EN disk roster record length overflow"))?;
            if journal_bytes.len() - at < frame_len {
                break;
            }
            let body_at = at + 4;
            let body_end = body_at + body_len;
            let expected_crc =
                u64::from_le_bytes(journal_bytes[body_end..body_end + 8].try_into().unwrap());
            if common::crc::of(&journal_bytes[body_at..body_end]) != expected_crc {
                return Err(Error::new("EN disk roster journal checksum mismatch"));
            }
            let (sequence, record) = decode_journal_record(&journal_bytes[body_at..body_end])?;
            if sequence != expected_sequence {
                return Err(Error::new("EN disk roster journal sequence is discontinuous"));
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or_else(|| Error::new("EN disk roster sequence overflow"))?;
            if sequence > checkpoint_sequence {
                apply_record(&mut records, record)?;
            }
            at += frame_len;
            valid_end = at;
        }

        let journal_sequence = expected_sequence - 1;
        if checkpoint_sequence > journal_sequence {
            return Err(Error::new("EN disk roster checkpoint is ahead of its journal"));
        }
        if valid_end != journal_bytes.len() {
            journal.set_len(valid_end as u64)?;
            journal.sync_all()?;
        }
        journal.seek(SeekFrom::End(0))?;

        Ok(DiskRoster {
            dir: dir.to_path_buf(),
            journal,
            records,
            sequence: journal_sequence,
            has_checkpoint,
        })
    }

    /// Check the durable roster against this host's configured devices. New
    /// devices are admitted with a synced journal record before EN becomes ready.
    pub(crate) fn reconcile(&mut self, configured: &[ProcessDiskConfig]) -> Result<()> {
        if configured.is_empty() {
            return Err(Error::new("EN process.toml must list at least one disk"));
        }

        let mut by_id = BTreeMap::new();
        let mut device_ids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for disk in configured {
            let id = DiskId::new(disk.id);
            if disk.device_identity.trim().is_empty() {
                return Err(Error::new(format!("EN disk {id} has an empty device_identity")));
            }
            let path = fs::canonicalize(&disk.path).map_err(|error| {
                Error::new(format!(
                    "EN disk {id} path is unavailable: {} ({error})",
                    disk.path.display()
                ))
            })?;
            if !path.is_dir() {
                return Err(Error::new(format!(
                    "EN disk {id} path is missing or is not a directory: {}",
                    path.display()
                )));
            }
            if disk.capacity_bytes < 40 {
                return Err(Error::new(format!("EN disk {id} capacity must be at least 40 bytes")));
            }
            let path_text = path
                .to_str()
                .ok_or_else(|| Error::new(format!("EN disk {id} path is not valid UTF-8")))?
                .to_owned();
            let record = DiskRecord {
                id,
                device_identity: disk.device_identity.clone(),
                path: path_text,
                capacity_bytes: disk.capacity_bytes,
                state: DiskState::Active,
            };
            if by_id.insert(id, record).is_some() {
                return Err(Error::new(format!("EN process.toml lists disk {id} more than once")));
            }
            if !device_ids.insert(disk.device_identity.as_str()) {
                return Err(Error::new(format!(
                    "EN process.toml reuses device_identity {:?}",
                    disk.device_identity
                )));
            }
            if !paths.insert(path.clone()) {
                return Err(Error::new(format!("EN process.toml reuses path {}", path.display())));
            }
        }

        for (id, old) in &self.records {
            if old.state == DiskState::Retired {
                if by_id.contains_key(id) {
                    return Err(Error::new(format!("retired EN disk ID {id} cannot be reused")));
                }
                continue;
            }
            let Some(new) = by_id.get(id) else {
                return Err(Error::new(format!(
                    "EN disk {id} is in the durable roster but missing from process.toml"
                )));
            };
            if old.device_identity != new.device_identity || old.path != new.path {
                return Err(Error::new(format!(
                    "EN disk {id} identity or path differs from the durable roster"
                )));
            }
        }

        let mut changed = Vec::new();
        for (id, configured_record) in by_id {
            match self.records.get(&id) {
                Some(old) if old.state == DiskState::Retired => {
                    return Err(Error::new(format!("retired EN disk ID {id} cannot be reused")));
                }
                Some(old) if old.capacity_bytes == configured_record.capacity_bytes => {}
                Some(_) | None => changed.push(configured_record),
            }
        }

        if !changed.is_empty() {
            self.append_states(changed)?;
        }
        if !self.has_checkpoint {
            self.write_checkpoint()?;
        }
        Ok(())
    }

    fn append_states(&mut self, records: Vec<DiskRecord>) -> Result<()> {
        let mut sequence = self.sequence;
        let mut journal_bytes = Vec::new();
        let mut next_records = self.records.clone();
        for record in records {
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::new("EN disk roster sequence overflow"))?;
            let body = encode_journal_record(sequence, &record)?;
            let body_len = u32::try_from(body.len())
                .map_err(|_| Error::new("EN disk roster record is too large"))?;
            journal_bytes.extend_from_slice(&body_len.to_le_bytes());
            journal_bytes.extend_from_slice(&body);
            journal_bytes.extend_from_slice(&common::crc::of(&body).to_le_bytes());
            apply_record(&mut next_records, record)?;
        }
        self.journal.write_all(&journal_bytes)?;
        self.journal.sync_all()?;

        self.records = next_records;
        self.sequence = sequence;
        self.write_checkpoint()
    }

    fn write_checkpoint(&mut self) -> Result<()> {
        let bytes = encode_checkpoint(self.sequence, &self.records)?;
        let path = self.dir.join(CHECKPOINT_NAME);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp = self.dir.join(format!(".disk-roster-{}-{nonce}.tmp", std::process::id()));
        let result = (|| -> Result<()> {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file =
                OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, &path)?;
            File::open(&self.dir)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
        self.has_checkpoint = true;
        Ok(())
    }
}

fn apply_record(records: &mut BTreeMap<DiskId, DiskRecord>, record: DiskRecord) -> Result<()> {
    if let Some(old) = records.get(&record.id) {
        if old.device_identity != record.device_identity || old.path != record.path {
            return Err(Error::new(format!(
                "EN disk {} changed identity or path in its durable journal",
                record.id
            )));
        }
        if old.state == DiskState::Retired && record.state != DiskState::Retired {
            return Err(Error::new(format!("retired EN disk {} was reactivated", record.id)));
        }
    }
    records.insert(record.id, record);
    Ok(())
}

fn journal_header() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(JOURNAL_HEADER);
    bytes.extend_from_slice(JOURNAL_MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&common::crc::of(&bytes).to_le_bytes());
    bytes
}

fn validate_journal_header(bytes: &[u8]) -> Result<()> {
    if bytes.len() < JOURNAL_HEADER
        || &bytes[..8] != JOURNAL_MAGIC
        || u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != VERSION
        || common::crc::of(&bytes[..10]) != u64::from_le_bytes(bytes[10..18].try_into().unwrap())
    {
        return Err(Error::new("invalid EN disk roster journal header"));
    }
    Ok(())
}

fn encode_journal_record(sequence: u64, record: &DiskRecord) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&Op::EnJDiskState.code().to_le_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    encode_record_fields(&mut bytes, record)?;
    Ok(bytes)
}

fn decode_journal_record(bytes: &[u8]) -> Result<(u64, DiskRecord)> {
    let mut input = Input { bytes, at: 0 };
    input.op(Op::EnJDiskState)?;
    let sequence = input.u64()?;
    let record = input.record_fields()?;
    input.done()?;
    Ok((sequence, record))
}

fn encode_record_fields(bytes: &mut Vec<u8>, record: &DiskRecord) -> Result<()> {
    bytes.extend_from_slice(&record.id.get().to_le_bytes());
    bytes.push(record.state as u8);
    bytes.extend_from_slice(&record.capacity_bytes.to_le_bytes());
    put_text(bytes, &record.device_identity)?;
    put_text(bytes, &record.path)?;
    Ok(())
}

fn encode_checkpoint(sequence: u64, records: &BTreeMap<DiskId, DiskRecord>) -> Result<Vec<u8>> {
    let count = u32::try_from(records.len()).map_err(|_| Error::new("too many EN disks"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&Op::EnCkDiskRoster.code().to_le_bytes());
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    for record in records.values() {
        bytes.extend_from_slice(&Op::EnCkDiskRecord.code().to_le_bytes());
        encode_record_fields(&mut bytes, record)?;
    }
    let total_len = bytes
        .len()
        .checked_add(CHECKPOINT_FOOTER)
        .ok_or_else(|| Error::new("EN disk roster checkpoint length overflow"))?;
    bytes.extend_from_slice(&Op::CheckpointEnd.code().to_le_bytes());
    bytes.extend_from_slice(
        &u64::try_from(total_len)
            .map_err(|_| Error::new("EN disk roster checkpoint exceeds u64"))?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&common::crc::of(&bytes).to_le_bytes());
    Ok(bytes)
}

fn read_checkpoint(path: &Path) -> Result<(u64, BTreeMap<DiskId, DiskRecord>)> {
    let bytes = fs::read(path)?;
    if bytes.len() < 8 + 2 + 2 + 8 + 4 + CHECKPOINT_FOOTER
        || &bytes[..8] != CHECKPOINT_MAGIC
        || u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != Op::EnCkDiskRoster.code()
        || u16::from_le_bytes(bytes[10..12].try_into().unwrap()) != VERSION
    {
        return Err(Error::new("invalid EN disk roster checkpoint header"));
    }
    let footer_at = bytes.len() - CHECKPOINT_FOOTER;
    if u16::from_le_bytes(bytes[footer_at..footer_at + 2].try_into().unwrap())
        != Op::CheckpointEnd.code()
        || u64::from_le_bytes(bytes[footer_at + 2..footer_at + 10].try_into().unwrap())
            != bytes.len() as u64
        || u64::from_le_bytes(bytes[footer_at + 10..footer_at + 18].try_into().unwrap())
            != common::crc::of(&bytes[..footer_at])
    {
        return Err(Error::new("invalid EN disk roster checkpoint footer"));
    }

    let sequence = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let count = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
    let mut input = Input { bytes: &bytes[..footer_at], at: 24 };
    let mut records = BTreeMap::new();
    for _ in 0..count {
        input.op(Op::EnCkDiskRecord)?;
        let record = input.record_fields()?;
        if records.insert(record.id, record).is_some() {
            return Err(Error::new("duplicate disk ID in EN roster checkpoint"));
        }
    }
    input.done()?;
    Ok((sequence, records))
}

fn put_text(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| Error::new("EN disk roster text too large"))?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Input<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Input<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8]> {
        let end =
            self.at.checked_add(len).ok_or_else(|| Error::new("EN roster offset overflow"))?;
        if end > self.bytes.len() {
            return Err(Error::new("truncated EN disk roster record"));
        }
        let result = &self.bytes[self.at..end];
        self.at = end;
        Ok(result)
    }

    fn op(&mut self, expected: Op) -> Result<()> {
        let op = Op::from_code(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))?;
        if op != expected {
            return Err(Error::new(format!("expected EN roster {expected}, found {op}")));
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn text(&mut self) -> Result<String> {
        let len =
            usize::try_from(self.u32()?).map_err(|_| Error::new("EN roster text too large"))?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| Error::new("EN roster text is not UTF-8"))
    }

    fn record_fields(&mut self) -> Result<DiskRecord> {
        Ok(DiskRecord {
            id: DiskId::new(self.u16()?),
            state: DiskState::read(self.u8()?)?,
            capacity_bytes: self.u64()?,
            device_identity: self.text()?,
            path: self.text()?,
        })
    }

    fn done(&self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::new("trailing EN disk roster bytes"))
        }
    }
}
