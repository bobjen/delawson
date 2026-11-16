//! EN, the Extent Node: the bytes on the disks.
//!
//! An extent's instances live here, and an append is what puts bytes in one. The
//! primary instance assigns the append its logical offset in the extent — the one
//! serialized step — and then broadcasts the same bytes at that fixed offset to
//! the other three instances. Broadcast rather than a chain, because a quorum only
//! buys straggler tolerance if the writes are issued at once.
//!
//! **Durable means three of the four instances have written it**, the primary
//! included, so one slow instance costs nothing.
//!
//! **A caller's CRC is carried and stored, and not checked here.** A Disk holds what
//! the writer compressed, and it never decompresses — so the logical bytes the CRC is
//! over do not exist on this machine. Checking would work today only because nothing
//! compresses yet, and would start failing on every append the moment something did.
//! The check belongs where the logical bytes come back.
//!
//! ⚠️ **Nothing is on a real disk yet.** The extent table and the appends themselves are
//! two vectors in memory, which is enough for the operation to work end to end and
//! is not evidence about what a durable append costs. What replaces it is a
//! journal of extent metadata per disk, snapshot and log.

mod checkpoint;
mod journal;

use common::block::Holds;
use common::config::ENConfig;
use common::crypto;
use common::deletion::{Delete, Target, bad, read_soft_deleted};
use common::framing::Framing;
use common::ids::{DiskId, EnId, ExtentId, SnId};
use common::offset::{ECoff, ELoff, ERoff};
use common::range::{Budget, ExtentCovered, ExtentRange, ExtentStart, Filled, Piece};
use common::scn::Scn;
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::{
    AppendBlock, AppendPiece, Arg, EnKey, EnReceipt, Op, Req, Resp, SnKey, Status, StructWriter,
    logi, structure,
};
use common::{Error, Result, logw};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const SERVES: &[Op] = &[
    Op::SoftDeleteExtentInstance,
    Op::DeleteExtentInstance,
    Op::AppendExtent,
    Op::WriteAppend,
    Op::ReadExtent,
    Op::ForceSealExtent,
    Op::WriteDurable,
];

/// Cached SN keys, addressed by the exact extent creation second. The exact
/// time matters because KS may install an emergency key epoch mid-day.
struct DailyKeyCache {
    sn_keys: RwLock<BTreeMap<(SnId, TimeSec), CachedKey>>,
    en_keys: RwLock<BTreeMap<(EnId, TimeSec), CachedKey>>,
}

struct CachedKey {
    key: [u8; 32],
    /// Exclusive end of the key epoch, or no known end.
    until: Option<TimeSec>,
}

impl DailyKeyCache {
    fn new() -> Self {
        Self { sn_keys: RwLock::new(BTreeMap::new()), en_keys: RwLock::new(BTreeMap::new()) }
    }

    fn get(&self, process: &Process, sn_id: SnId, ts: TimeSec) -> Result<[u8; 32]> {
        {
            let keys = self.sn_keys.read().expect("SN keys");
            if let Some((_, cached)) = keys.range((sn_id, TimeSec::EPOCH)..=(sn_id, ts)).next_back()
                && cached.until.is_none_or(|until| ts < until)
            {
                return Ok(cached.key);
            }
        }

        // Fetch from KS.
        let mut payload = StructWriter::new();
        payload.sn_id(sn_id);
        payload.time_sec(ts);
        let ask = Req::new(Op::SnMasterKey).structure(payload);
        let registrar = process.cluster.registrar()?.identity.clone();
        let key_cell = Arc::new(Mutex::new(None::<SnKey>));
        let key_cell2 = Arc::clone(&key_cell);
        let error_cell = Arc::new(Mutex::new(None));
        let error_cell2 = Arc::clone(&error_cell);
        let cvar = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let cvar2 = Arc::clone(&cvar);

        process
            .ask(&registrar, &ask, move |answer| {
                let mut guard = cvar2.0.lock().expect("cvar");
                if answer.status == Status::Ok {
                    match common::SnKey::read(answer.args()) {
                        Ok(response) => {
                            let mut key_guard = key_cell2.lock().expect("key_cell");
                            *key_guard = Some(response);
                            *guard = true;
                        }
                        Err(e) => {
                            let mut error_guard = error_cell2.lock().expect("error_cell");
                            *error_guard = Some(e);
                        }
                    }
                } else {
                    let mut error_guard = error_cell2.lock().expect("error_cell");
                    *error_guard =
                        Some(Error::new(format!("KS returned error: {}", answer.status)));
                }
                *guard = true;
                cvar2.1.notify_one();
            })
            .unwrap_or_else(|_| {
                let mut guard = cvar.0.lock().expect("cvar");
                *guard = true;
                cvar.1.notify_one();
            });

        let mut guard = cvar.0.lock().expect("cvar");
        while !*guard {
            guard = cvar.1.wait(guard).expect("cvar");
        }

        if let Some(e) = error_cell.lock().expect("error_cell").take() {
            return Err(e);
        }

        let Some(response) = key_cell.lock().expect("key_cell").take() else {
            #[cfg(test)]
            {
                return Ok(crypto::sn_key(&crypto::MASTER_KEY, sn_id, ts.day_start()));
            }
            #[cfg(not(test))]
            {
                return Err(Error::new("KS returned no key"));
            }
        };
        let until = response.next.map(|(at, _)| at);
        let key = response.current;
        let mut keys = self.sn_keys.write().expect("SN keys");
        keys.insert((sn_id, ts), CachedKey { key, until });
        if let Some((at, key)) = response.next {
            keys.insert((sn_id, at), CachedKey { key, until: None });
        }
        Ok(key)
    }

    fn get_en(&self, process: &Process, en_id: EnId, ts: TimeSec) -> Result<[u8; 32]> {
        {
            let keys = self.en_keys.read().expect("EN keys");
            if let Some((_, cached)) = keys.range((en_id, TimeSec::EPOCH)..=(en_id, ts)).next_back()
                && cached.until.is_none_or(|until| ts < until)
            {
                return Ok(cached.key);
            }
        }

        let mut payload = StructWriter::new();
        payload.en_id(en_id);
        payload.time_sec(ts);
        let ask = Req::new(Op::EnMasterKey).structure(payload);
        let registrar = process.cluster.registrar()?.identity.clone();
        let key_cell = Arc::new(Mutex::new(None::<EnKey>));
        let key_cell2 = Arc::clone(&key_cell);
        let error_cell = Arc::new(Mutex::new(None));
        let error_cell2 = Arc::clone(&error_cell);
        let cvar = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let cvar2 = Arc::clone(&cvar);

        process
            .ask(&registrar, &ask, move |answer| {
                let mut guard = cvar2.0.lock().expect("cvar");
                if answer.status == Status::Ok {
                    match EnKey::read(answer.args()) {
                        Ok(response) => *key_cell2.lock().expect("key cell") = Some(response),
                        Err(error) => *error_cell2.lock().expect("error cell") = Some(error),
                    }
                } else {
                    *error_cell2.lock().expect("error cell") =
                        Some(Error::new(format!("KS returned error: {}", answer.status)));
                }
                *guard = true;
                cvar2.1.notify_one();
            })
            .unwrap_or_else(|error| {
                *error_cell.lock().expect("error cell") = Some(Error::new(error.to_string()));
                let mut guard = cvar.0.lock().expect("cvar");
                *guard = true;
                cvar.1.notify_one();
            });

        let mut guard = cvar.0.lock().expect("cvar");
        while !*guard {
            guard = cvar.1.wait(guard).expect("cvar");
        }
        if let Some(error) = error_cell.lock().expect("error cell").take() {
            return Err(error);
        }
        let Some(response) = key_cell.lock().expect("key cell").take() else {
            #[cfg(test)]
            {
                return Ok(crypto::en_key(&crypto::MASTER_KEY, en_id, ts));
            }
            #[cfg(not(test))]
            {
                return Err(Error::new("KS returned no EN key"));
            }
        };
        let until = response.next.map(|(at, _)| at);
        let key = response.current;
        let mut keys = self.en_keys.write().expect("EN keys");
        keys.insert((en_id, ts), CachedKey { key, until });
        if let Some((at, key)) = response.next {
            keys.insert((en_id, at), CachedKey { key, until: None });
        }
        Ok(key)
    }
}

/// Cached extent keys.
struct ExtentKeyCache {
    keys: RwLock<HashMap<(SnId, ExtentId, TimeSec), [u8; 32]>>,
}

impl ExtentKeyCache {
    fn new() -> Self {
        Self { keys: RwLock::new(HashMap::new()) }
    }

    fn get(
        &self,
        daily_cache: &DailyKeyCache,
        process: &Process,
        sn_id: SnId,
        extent_id: ExtentId,
        ts: TimeSec,
    ) -> Result<[u8; 32]> {
        {
            let keys = self.keys.read().expect("keys");
            if let Some(key) = keys.get(&(sn_id, extent_id, ts)) {
                return Ok(*key);
            }
        }

        let sn_daily_key = daily_cache.get(process, sn_id, ts)?;
        let key = crypto::extent_key(&sn_daily_key, sn_id, extent_id, ts);
        self.keys.write().expect("keys").insert((sn_id, extent_id, ts), key);
        Ok(key)
    }
}

/// Appends held back per extent, waiting for a record to close.
type Waiting = HashMap<(SnId, ExtentId), Vec<Arc<Pending>>>;

/// A logical offset and a block's metadata and bytes.
type QueuedAppend = (AppendBlock, Reply);

/// The version of the extent on-disk layout.
pub const EXTENT_VERSION: u32 = 1;
/// The constant difference between compressed user data length and physical block size.
/// Plaintext Holds (12 bytes) + AEGIS-128X2 tag (16 bytes) + Encrypted Header (16 bytes: CRC u64 + Framing u32/u32).
pub const APPEND_OVERHEAD: usize = 44;

/// An extent instance held here.
struct Extent {
    deletion: common::deletion::Flags,
    sn_id: SnId,
    id: ExtentId,
    created: TimeSec,
    /// Closed: no more appends, however much room is left.
    sealed: bool,
    /// Where the next append starts, in stored bytes.
    end: ECoff,
    /// And where it starts in the canonical logical numbering (payload bytes only).
    logical_end: ELoff,
    /// Records completed in this extent, which is what numbers them.
    records: ERoff,
    /// Whether a record has begun and not ended.
    open_record: bool,
    /// First extent record introduced at each append SCN while this extent is staged.
    scn_first: ScnIndex,
}

/// One append, as it arrived.
struct Append {
    /// Which extent, as an index into the extent table.
    extent: usize,
    /// Where in the extent's stored bytes it sits.
    offset: ECoff,
    /// The canonical logical offset.
    logical: ELoff,
    /// The extent-local number of the first record this block completed.
    first_record: ERoff,
    /// SCN assigned by the primary EN for this client append.
    scn: Option<Scn>,
    /// Its physical staging frame after the local stage write is durable.
    stage: Option<journal::StageRef>,
    /// Metadata in the clear, to avoid expensive decryption during scans.
    holds: Holds,
    written: bool,
    quorum: bool,
    /// Independently encrypted blocks in this append. The extent does not index
    /// these yet; reads walk them in order.
    blocks: Vec<Encrypted>,
}

struct Encrypted {
    logical: ELoff,
    first_record: ERoff,
    holds: Holds,
    crc: u64,
    framing: Framing,
    /// Extent-key encrypted body while live in memory. Recovered blocks use stage.
    sealed: Vec<u8>,
    /// Recovered blocks point into their append's independently encrypted stage range.
    stage: Option<journal::StageRef>,
    stage_tag: [u8; 16],
    stage_ad: Option<Arc<[u8]>>,
    stage_timestamp: Option<TimeSec>,
    stage_part: Option<(u32, u32)>,
}

fn block_ad(
    logical: ELoff,
    first_record: ERoff,
    holds: Holds,
    crc: u64,
    framing: Framing,
) -> Vec<u8> {
    let mut ad = Vec::with_capacity(33);
    put_u32(&mut ad, logical.get());
    put_u32(&mut ad, first_record.get());
    put_holds(&mut ad, holds);
    put_u64(&mut ad, crc);
    let (kind, width) = framing.wire();
    ad.push(kind);
    put_u32(&mut ad, width);
    ad
}

impl Encrypted {
    #[allow(clippy::too_many_arguments)]
    fn encrypt(
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        extent: &Extent,
        _offset: ECoff,
        logical: ELoff,
        first_record: ERoff,
        framing: Framing,
        crc: u64,
        holds: Holds,
        body: &[u8],
    ) -> Result<Self> {
        let extent_key =
            extent_cache.get(daily_cache, process, extent.sn_id, extent.id, extent.created)?;
        let (key, nonce) =
            crypto::derive_block_secrets(&extent_key, logical.number(), first_record.number());
        let ad = block_ad(logical, first_record, holds, crc, framing);
        let (mut sealed, tag) = crypto::encrypt_block(&key, &nonce, &ad, body);
        sealed.extend_from_slice(&tag);
        Ok(Self {
            logical,
            first_record,
            holds,
            crc,
            framing,
            sealed,
            stage: None,
            stage_tag: [0; 16],
            stage_ad: None,
            stage_timestamp: None,
            stage_part: None,
        })
    }

    fn decrypt(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        extent: &Extent,
        stage: Option<&journal::DiskFiles>,
    ) -> Result<(u64, Framing, Vec<u8>)> {
        let data = if let Some(loc) = self.stage {
            let stage = stage.ok_or_else(|| Error::new("staging file is unavailable"))?;
            let (key, nonce) = {
                let stage_time = stage.stage_time(loc.file)?;
                let en_key = daily_cache.get_en(process, stage.en_id(), stage_time)?;
                crypto::derive_stage_secrets(
                    &en_key,
                    stage_time,
                    stage.en_id(),
                    stage.disk_id(),
                    loc.file.get(),
                    loc.offset,
                )
            };
            let stage_data = stage.read_stage(loc)?;
            let ad = self
                .stage_ad
                .as_deref()
                .ok_or_else(|| Error::new("recovered EN append has no stage associated data"))?;
            if stage_data.metadata != ad || stage_data.tag != self.stage_tag {
                return Err(Error::new("staged EN header disagrees with its append record"));
            }
            if self.stage_timestamp.is_some_and(|timestamp| timestamp != stage_data.timestamp) {
                return Err(Error::new("staged EN timestamp disagrees with its append SCN"));
            }
            let stage_aad = journal::stage_aad(stage_data.timestamp, &stage_data.metadata);
            let plain = crypto::decrypt_block(
                &key,
                &nonce,
                &stage_aad,
                &stage_data.cipher,
                &stage_data.tag,
            )?;
            let (rel, len) = self
                .stage_part
                .ok_or_else(|| Error::new("recovered EN block has no stage range"))?;
            let start = rel as usize;
            let end = start
                .checked_add(len as usize)
                .ok_or_else(|| Error::new("staged EN block length overflow"))?;
            plain
                .get(start..end)
                .ok_or_else(|| Error::new("EN block extends past its staged append"))?
                .to_vec()
        } else {
            let extent_key =
                extent_cache.get(daily_cache, process, extent.sn_id, extent.id, extent.created)?;
            let (key, nonce) = crypto::derive_block_secrets(
                &extent_key,
                self.logical.number(),
                self.first_record.number(),
            );
            if self.sealed.len() < 16 {
                return Err(Error::new("stored block is shorter than its authentication tag"));
            }
            let (cipher, tag) = self.sealed.split_at(self.sealed.len() - 16);
            let tag: [u8; 16] = tag.try_into().unwrap();
            let ad = block_ad(self.logical, self.first_record, self.holds, self.crc, self.framing);
            crypto::decrypt_block(&key, &nonce, &ad, cipher, &tag)?
        };
        Ok((self.crc, self.framing, data))
    }
}

/// The blocks a range read is answered with, and what they came to.
struct Ranged {
    pieces: Vec<Piece>,
    covered: ExtentCovered,
}

/// What became of an append.
#[derive(Debug)]
enum Took {
    Landed(Landed),
    /// The logical offset is in the future, so the append was queued.
    Queued,
    Sealed {
        came_to: Filled,
        closing: bool,
    },
}

/// What a secondary append write did.
enum Written {
    Stored,
    Already,
}

/// What a truncation discarded, and what the extent came to.
#[allow(dead_code)]
struct Truncated {
    discarded: usize,
    records: ERoff,
    length: ECoff,
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_holds(bytes: &mut Vec<u8>, holds: Holds) {
    put_u32(bytes, holds.compressed.get());
    put_u32(bytes, holds.logical.get());
    put_u32(bytes, holds.records.get());
}

fn put_mark(bytes: &mut Vec<u8>, mark: Mark) {
    put_u32(bytes, mark.logical.get());
    put_u32(bytes, mark.records.get());
}

fn read_mark(input: &mut JournalRead<'_>) -> Result<Mark> {
    Ok(Mark { logical: ELoff::new(input.u32()?), records: ERoff::new(input.u32()?) })
}

fn put_marks(bytes: &mut Vec<u8>, marks: Marks) {
    put_mark(bytes, marks.written);
    put_mark(bytes, marks.durable);
}

fn read_marks(input: &mut JournalRead<'_>) -> Result<Marks> {
    Ok(Marks { written: read_mark(input)?, durable: read_mark(input)? })
}

struct JournalRead<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> JournalRead<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end =
            self.at.checked_add(len).ok_or_else(|| Error::new("EN journal offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| Error::new("truncated EN journal metadata"))?;
        self.at = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn flag(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::new("invalid flag in EN journal")),
        }
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn holds(&mut self) -> Result<Holds> {
        Ok(Holds {
            compressed: ECoff::new(self.u32()?),
            logical: ELoff::new(self.u32()?),
            records: ERoff::new(self.u32()?),
        })
    }

    fn done(self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::new("trailing bytes in EN journal metadata"))
        }
    }
}

#[derive(Clone, Copy)]
struct ExtentState {
    sn: SnId,
    id: ExtentId,
    created: TimeSec,
    deletion: common::deletion::Flags,
    sealed: bool,
    open_record: bool,
    end: ECoff,
    logical_end: ELoff,
    records: ERoff,
    retain_until: TimeSec,
}

fn put_extent_state(bytes: &mut Vec<u8>, state: ExtentState) {
    put_u64(bytes, state.sn.get());
    put_u32(bytes, state.id.get());
    put_u64(bytes, state.created.number());
    bytes.push(state.deletion.bits());
    bytes.push(u8::from(state.sealed));
    bytes.push(u8::from(state.open_record));
    put_u32(bytes, state.end.get());
    put_u32(bytes, state.logical_end.get());
    put_u32(bytes, state.records.get());
    put_u64(bytes, state.retain_until.number());
}

fn read_extent_state(input: &mut JournalRead<'_>) -> Result<ExtentState> {
    Ok(ExtentState {
        sn: SnId::new(input.u64()?),
        id: ExtentId::new(input.u32()?),
        created: TimeSec::new(input.u64()?),
        deletion: common::deletion::Flags::from_bits(input.u8()?)?,
        sealed: input.flag()?,
        open_record: input.flag()?,
        end: ECoff::new(input.u32()?),
        logical_end: ELoff::new(input.u32()?),
        records: ERoff::new(input.u32()?),
        retain_until: TimeSec::new(input.u64()?),
    })
}

fn journal_recovery_mark(record: &journal::Record) -> Result<Option<journal::RecoveryMark>> {
    if !matches!(record.op, Op::EnJAppend | Op::EnJSeal) {
        return Ok(None);
    }
    let mut input = JournalRead::new(&record.metadata);
    let state = read_extent_state(&mut input)?;
    let state_end = input.at;
    let marks = read_marks(&mut input)?;
    let after_marks = input.at;
    let append_end = if record.op == Op::EnJAppend {
        let _offset = input.u32()?;
        let logical = ELoff::new(input.u32()?);
        let first_record = ERoff::new(input.u32()?);
        let holds = input.holds()?;
        Some((logical.plus(holds.logical)?.get(), first_record.plus(holds.records)?.get()))
    } else {
        input.done()?;
        None
    };
    let stage_metadata = if record.op == Op::EnJAppend {
        let mut metadata = Vec::with_capacity(record.metadata.len() - (after_marks - state_end));
        metadata.extend_from_slice(&record.metadata[..state_end]);
        metadata.extend_from_slice(&record.metadata[after_marks..]);
        Some(metadata)
    } else {
        None
    };
    Ok(Some(journal::RecoveryMark {
        extent: (state.sn.get(), state.id.get(), state.created.number()),
        written: (marks.written.logical.get(), marks.written.records.get()),
        append_end,
        stage_metadata,
    }))
}

/// Where an append landed and what it numbered.
#[derive(Debug, Clone, Copy)]
struct Landed {
    #[cfg_attr(not(test), allow(dead_code))]
    offset: ECoff,
    logical: ELoff,
    first_record: ERoff,
    records: ERoff,
}

/// Everything this Disk holds.
struct Extents {
    extras: common::deletion::Extras<(SnId, ExtentId, TimeSec)>,
    extents: Vec<Extent>,
    at: HashMap<(SnId, ExtentId), usize>,
    appends: Vec<Append>,
    /// Physical stage location to the logical append identity stored there.
    stage_index: BTreeMap<(journal::StageId, u32), StageAppend>,
    stage: Option<Arc<journal::DiskFiles>>,
    marks: HashMap<(SnId, ExtentId, TimeSec), Marks>,
}

type ExtentName = (SnId, ExtentId, TimeSec);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StageAppend {
    extent: ExtentName,
    logical: ELoff,
    first_record: ERoff,
}

/// A per-extent ordered SCN index. The retained staged SCNs span far less than
/// half the wrapping SCN range, so deltas from `base` sort in logical order.
#[derive(Default)]
struct ScnIndex {
    base: Option<Scn>,
    entries: BTreeMap<u64, (Scn, ERoff)>,
}

impl ScnIndex {
    fn insert(&mut self, scn: Scn, first_record: ERoff) -> Result<()> {
        let Some(base) = self.base else {
            self.base = Some(scn);
            self.entries.insert(0, (scn, first_record));
            return Ok(());
        };
        let delta = scn.number().wrapping_sub(base.number());
        if delta <= i64::MAX as u64 {
            return self.insert_delta(delta, scn, first_record);
        }
        if scn.compare(base) != std::cmp::Ordering::Less {
            return Err(Error::new("staged extent SCN span exceeds the comparison window"));
        }

        let mut rebased = BTreeMap::new();
        rebased.insert(0, (scn, first_record));
        for (_, (old_scn, old_record)) in &self.entries {
            let delta = old_scn.number().wrapping_sub(scn.number());
            if delta > i64::MAX as u64 {
                return Err(Error::new("staged extent SCN span exceeds the comparison window"));
            }
            rebased.insert(delta, (*old_scn, *old_record));
        }
        self.base = Some(scn);
        self.entries = rebased;
        Ok(())
    }

    fn insert_delta(&mut self, delta: u64, scn: Scn, first_record: ERoff) -> Result<()> {
        if let Some((old_scn, old_record)) = self.entries.get(&delta) {
            if *old_scn != scn || *old_record != first_record {
                return Err(Error::new("staged extent has conflicting entries for one SCN"));
            }
            return Ok(());
        }
        self.entries.insert(delta, (scn, first_record));
        Ok(())
    }

    fn get(&self, scn: Scn) -> Option<ERoff> {
        let base = self.base?;
        let delta = scn.number().wrapping_sub(base.number());
        if delta > i64::MAX as u64 {
            return None;
        }
        self.entries.get(&delta).map(|(_, first_record)| *first_record)
    }

    fn at_or_before(&self, scn: Scn) -> Option<ERoff> {
        let base = self.base?;
        let delta = scn.number().wrapping_sub(base.number());
        if delta > i64::MAX as u64 {
            return None;
        }
        self.entries.range(..=delta).next_back().map(|(_, (_, first_record))| *first_record)
    }

    fn remove(&mut self, scn: Scn) -> Option<ERoff> {
        let base = self.base?;
        let delta = scn.number().wrapping_sub(base.number());
        if delta > i64::MAX as u64 {
            return None;
        }
        let removed = self.entries.remove(&delta).map(|(_, first_record)| first_record);
        if self.entries.is_empty() {
            self.base = None;
        } else if delta == 0 {
            let old = std::mem::take(&mut self.entries);
            let new_base = old.first_key_value().expect("nonempty SCN index").1.0;
            self.base = Some(new_base);
            for (_, (scn, first_record)) in old {
                let delta = scn.number().wrapping_sub(new_base.number());
                self.entries.insert(delta, (scn, first_record));
            }
        }
        removed
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Mark {
    logical: ELoff,
    records: ERoff,
}

#[derive(Default, Clone, Copy)]
struct Marks {
    written: Mark,
    durable: Mark,
}

struct JournalEvent {
    /// Journal state, including the advisory written and durable marks.
    metadata: Vec<u8>,
    /// Stage header and AAD; deliberately excludes those advisory marks.
    stage_metadata: Vec<u8>,
    stage: Vec<u8>,
}

impl JournalEvent {
    fn state(metadata: Vec<u8>) -> Self {
        Self { metadata, stage_metadata: Vec::new(), stage: Vec::new() }
    }
}

impl Extents {
    fn new() -> Extents {
        Extents {
            extras: common::deletion::Extras::default(),
            extents: Vec::new(),
            at: HashMap::new(),
            appends: Vec::new(),
            stage_index: BTreeMap::new(),
            stage: None,
            marks: HashMap::new(),
        }
    }

    fn remove_appends(&mut self, mut remove: impl FnMut(&Append) -> bool) {
        let appends = std::mem::take(&mut self.appends);
        let mut kept = Vec::with_capacity(appends.len());
        for append in appends {
            if remove(&append) {
                if let Some(stage) = append.stage {
                    self.stage_index.remove(&(stage.file, stage.offset));
                }
                if let (Some(scn), Some(extent)) = (append.scn, self.extents.get_mut(append.extent))
                {
                    extent.scn_first.remove(scn);
                }
            } else {
                kept.push(append);
            }
        }
        self.appends = kept;
    }

    fn index_staged_append(
        &mut self,
        place: usize,
        stage: journal::StageRef,
        scn: Scn,
        logical: ELoff,
        first_record: ERoff,
    ) -> Result<()> {
        let extent = self
            .extents
            .get(place)
            .ok_or_else(|| Error::new("staged append extent index is out of range"))?;
        let value = StageAppend {
            extent: (extent.sn_id, extent.id, extent.created),
            logical,
            first_record,
        };
        let key = (stage.file, stage.offset);
        if self.stage_index.get(&key).is_some_and(|existing| *existing != value) {
            return Err(Error::new("stage location is already indexed for a different append"));
        }
        self.extents[place].scn_first.insert(scn, first_record)?;
        self.stage_index.insert(key, value);
        Ok(())
    }

    fn unindex_staged_append(
        &mut self,
        place: usize,
        stage: Option<journal::StageRef>,
        scn: Option<Scn>,
    ) {
        if let Some(stage) = stage {
            self.stage_index.remove(&(stage.file, stage.offset));
        }
        if let (Some(scn), Some(extent)) = (scn, self.extents.get_mut(place)) {
            extent.scn_first.remove(scn);
        }
    }

    #[allow(dead_code)]
    fn staged_append_at(&self, file: journal::StageId, offset: u32) -> Option<StageAppend> {
        self.stage_index.get(&(file, offset)).copied()
    }

    #[allow(dead_code)]
    fn first_record_at_scn(
        &self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        scn: Scn,
    ) -> Option<ERoff> {
        let place = *self.at.get(&(sn, id))?;
        let extent = &self.extents[place];
        (extent.created == created).then(|| extent.scn_first.get(scn)).flatten()
    }

    #[allow(dead_code)]
    fn first_record_at_or_before_scn(
        &self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        scn: Scn,
    ) -> Option<ERoff> {
        let place = *self.at.get(&(sn, id))?;
        let extent = &self.extents[place];
        (extent.created == created).then(|| extent.scn_first.at_or_before(scn)).flatten()
    }

    fn sn_ids(&self) -> Vec<SnId> {
        self.extents.iter().map(|extent| extent.sn_id).collect()
    }

    fn journal_append(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        sn: SnId,
        id: ExtentId,
        landed: Landed,
    ) -> Result<JournalEvent> {
        let place =
            *self.at.get(&(sn, id)).ok_or_else(|| Error::new("append extent disappeared"))?;
        let extent = &self.extents[place];
        let append = self
            .appends
            .iter()
            .rev()
            .find(|a| {
                a.extent == place
                    && a.logical == landed.logical
                    && a.first_record == landed.first_record
            })
            .ok_or_else(|| Error::new("append metadata disappeared"))?;
        let key = (extent.sn_id, extent.id, extent.created);
        let retain_until = self.extras.get(&key).map_or(TimeSec::EPOCH, |extra| extra.retain_until);

        let mut stage_metadata = Vec::new();
        put_extent_state(
            &mut stage_metadata,
            ExtentState {
                sn: extent.sn_id,
                id: extent.id,
                created: extent.created,
                deletion: extent.deletion,
                sealed: extent.sealed,
                open_record: extent.open_record,
                end: extent.end,
                logical_end: extent.logical_end,
                records: extent.records,
                retain_until,
            },
        );
        let mut metadata = stage_metadata.clone();
        put_marks(&mut metadata, self.marks.get(&key).copied().unwrap_or_default());
        let mut append_metadata = Vec::new();
        put_u32(&mut append_metadata, append.offset.get());
        put_u32(&mut append_metadata, append.logical.get());
        put_u32(&mut append_metadata, append.first_record.get());
        put_holds(&mut append_metadata, append.holds);
        put_u32(
            &mut append_metadata,
            u32::try_from(append.blocks.len()).map_err(|_| Error::new("too many append blocks"))?,
        );

        let mut stage = Vec::new();
        for block in &append.blocks {
            let (crc, framing, bytes) =
                block.decrypt(daily_cache, extent_cache, process, extent, self.stage.as_deref())?;
            let rel = u32::try_from(stage.len())
                .map_err(|_| Error::new("append stage data exceeds 4 GiB"))?;
            let len =
                u32::try_from(bytes.len()).map_err(|_| Error::new("append block exceeds 4 GiB"))?;
            put_u32(&mut append_metadata, block.logical.get());
            put_u32(&mut append_metadata, block.first_record.get());
            put_holds(&mut append_metadata, block.holds);
            put_u64(&mut append_metadata, crc);
            let (kind, width) = framing.wire();
            append_metadata.push(kind);
            put_u32(&mut append_metadata, width);
            put_u32(&mut append_metadata, rel);
            put_u32(&mut append_metadata, len);
            stage.extend_from_slice(&bytes);
        }
        metadata.extend_from_slice(&append_metadata);
        stage_metadata.extend_from_slice(&append_metadata);
        Ok(JournalEvent { metadata, stage_metadata, stage })
    }

    fn journal_state(&self, sn: SnId, id: ExtentId, include_marks: bool) -> Result<Vec<u8>> {
        let place = *self.at.get(&(sn, id)).ok_or_else(|| Error::new("extent disappeared"))?;
        let extent = &self.extents[place];
        let key = (extent.sn_id, extent.id, extent.created);
        let state = ExtentState {
            sn: extent.sn_id,
            id: extent.id,
            created: extent.created,
            deletion: extent.deletion,
            sealed: extent.sealed,
            open_record: extent.open_record,
            end: extent.end,
            logical_end: extent.logical_end,
            records: extent.records,
            retain_until: self.extras.get(&key).map_or(TimeSec::EPOCH, |extra| extra.retain_until),
        };
        let mut metadata = Vec::new();
        put_extent_state(&mut metadata, state);
        if include_marks {
            put_marks(&mut metadata, self.marks.get(&key).copied().unwrap_or_default());
        }
        Ok(metadata)
    }

    fn apply_state(&mut self, state: ExtentState) -> Result<usize> {
        let key = (state.sn, state.id, state.created);
        let place = if let Some(&place) = self.at.get(&(state.sn, state.id)) {
            if self.extents[place].created != state.created {
                return Err(Error::new(
                    "journal repeats an extent ID with a different creation second",
                ));
            }
            place
        } else {
            let place = self.extents.len();
            self.extents.push(Extent {
                deletion: state.deletion,
                sn_id: state.sn,
                id: state.id,
                created: state.created,
                sealed: state.sealed,
                end: state.end,
                logical_end: state.logical_end,
                records: state.records,
                open_record: state.open_record,
                scn_first: ScnIndex::default(),
            });
            self.at.insert((state.sn, state.id), place);
            place
        };
        let extent = &mut self.extents[place];
        extent.deletion = state.deletion;
        extent.sealed = state.sealed;
        extent.end = state.end;
        extent.logical_end = state.logical_end;
        extent.records = state.records;
        extent.open_record = state.open_record;
        if state.deletion.extra() {
            self.extras.restore(
                key,
                common::deletion::Extra { retain_until: state.retain_until, references: () },
            );
        } else {
            self.extras.remove(&key);
        }
        self.marks.entry(key).or_default();
        Ok(place)
    }

    fn advance(&self, place: usize, written: bool) -> Result<Mark> {
        let extent = &self.extents[place];
        let key = (extent.sn_id, extent.id, extent.created);
        let mut mark = self
            .marks
            .get(&key)
            .map_or(Mark::default(), |marks| if written { marks.written } else { marks.durable });
        loop {
            let Some(append) = self.appends.iter().find(|append| {
                append.extent == place
                    && append.logical == mark.logical
                    && append.first_record == mark.records
                    && if written { append.written } else { append.quorum }
            }) else {
                break;
            };
            let next = Mark {
                logical: append.logical.plus(append.holds.logical)?,
                records: append.first_record.plus(append.holds.records)?,
            };
            if next == mark {
                return Err(Error::new("EN append does not advance its watermark"));
            }
            mark = next;
        }
        Ok(mark)
    }

    fn mark_written(
        &mut self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        logical: ELoff,
        first_record: ERoff,
        stage: Option<journal::StageRef>,
    ) -> Result<()> {
        let place = *self
            .at
            .get(&(sn, id))
            .ok_or_else(|| Error::new("written append extent disappeared"))?;
        if self.extents[place].created != created {
            return Err(Error::new("written append has the wrong creation second"));
        }
        let (old_stage, scn) = {
            let append = self
                .appends
                .iter_mut()
                .find(|append| {
                    append.extent == place
                        && append.logical == logical
                        && append.first_record == first_record
                })
                .ok_or_else(|| Error::new("written append metadata disappeared"))?;
            let old_stage = append.stage;
            append.stage = stage;
            append.written = true;
            (old_stage, append.scn)
        };
        self.unindex_staged_append(place, old_stage, scn);
        if let (Some(scn), Some(stage)) = (scn, stage) {
            self.index_staged_append(place, stage, scn, logical, first_record)?;
        }
        let next = self.advance(place, true)?;
        self.marks.entry((sn, id, created)).or_default().written = next;
        Ok(())
    }

    fn mark_quorum(
        &mut self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        logical: ELoff,
        first_record: ERoff,
    ) -> Result<Option<Mark>> {
        let place = *self
            .at
            .get(&(sn, id))
            .ok_or_else(|| Error::new("durable append extent disappeared"))?;
        if self.extents[place].created != created {
            return Err(Error::new("durable append has the wrong creation second"));
        }
        let append = self
            .appends
            .iter_mut()
            .find(|append| {
                append.extent == place
                    && append.logical == logical
                    && append.first_record == first_record
            })
            .ok_or_else(|| Error::new("durable append metadata disappeared"))?;
        append.quorum = true;
        let old = self.marks.entry((sn, id, created)).or_default().durable;
        let next = self.advance(place, false)?;
        self.marks.entry((sn, id, created)).or_default().durable = next;
        Ok((next > old).then_some(next))
    }

    fn apply_durable(
        &mut self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        mark: Mark,
    ) -> Result<()> {
        let current = self.marks.entry((sn, id, created)).or_default();
        current.durable = current.durable.max(mark);
        let durable = current.durable;
        if let Some(&place) = self.at.get(&(sn, id)) {
            for append in self.appends.iter_mut().filter(|append| append.extent == place) {
                let end = Mark {
                    logical: append.logical.plus(append.holds.logical)?,
                    records: append.first_record.plus(append.holds.records)?,
                };
                if end <= durable {
                    append.quorum = true;
                }
            }
        }
        Ok(())
    }

    fn restore_state(&mut self, record: journal::Record) -> Result<()> {
        let mut input = JournalRead::new(&record.metadata);
        let state = read_extent_state(&mut input)?;
        let marks = if record.op == Op::EnJSeal { Some(read_marks(&mut input)?) } else { None };
        input.done()?;
        common::scn::merge(record.scn)?;
        let place = self.apply_state(state)?;
        if let Some(marks) = marks {
            self.apply_journal_marks(state.sn, state.id, state.created, marks)?;
        }
        if record.op == Op::EnJDelete && state.deletion.retired() {
            self.remove_appends(|append| append.extent == place);
        }
        Ok(())
    }

    fn restore_append(&mut self, record: journal::Record) -> Result<()> {
        let stage_metadata = journal_recovery_mark(&record)?
            .and_then(|mark| mark.stage_metadata)
            .ok_or_else(|| Error::new("EN append has no stage metadata"))?;
        let mut input = JournalRead::new(&record.metadata);
        let state = read_extent_state(&mut input)?;
        let marks = read_marks(&mut input)?;
        let offset = ECoff::new(input.u32()?);
        let logical = ELoff::new(input.u32()?);
        let first_record = ERoff::new(input.u32()?);
        let holds = input.holds()?;
        let count = input.u32()? as usize;
        let stage_overhead = journal::STAGE_PREFIX
            .checked_add(stage_metadata.len())
            .and_then(|len| len.checked_add(journal::STAGE_TAG))
            .ok_or_else(|| Error::new("EN stage append overhead overflow"))?;
        let stage_body_len = (record.stage.len as usize)
            .checked_sub(stage_overhead)
            .ok_or_else(|| Error::new("EN append stage frame is shorter than its header"))?;
        let stage_ad: Arc<[u8]> = Arc::from(stage_metadata);
        let mut blocks = Vec::with_capacity(count);
        for _ in 0..count {
            let logical = ELoff::new(input.u32()?);
            let first_record = ERoff::new(input.u32()?);
            let holds = input.holds()?;
            let crc = input.u64()?;
            let kind = input.u8()?;
            let width = input.u32()?;
            let framing = Framing::of_wire(kind, width)?;
            let rel = input.u32()?;
            let len = input.u32()?;
            let end = u64::from(rel) + u64::from(len);
            if end > stage_body_len as u64 {
                return Err(Error::new("append block extends past its stage range"));
            }
            blocks.push(Encrypted {
                logical,
                first_record,
                holds,
                crc,
                framing,
                sealed: Vec::new(),
                stage: Some(record.stage),
                stage_tag: record.stage_tag,
                stage_ad: Some(Arc::clone(&stage_ad)),
                stage_timestamp: Some(record.scn.time_sec()),
                stage_part: Some((rel, len)),
            });
        }
        input.done()?;
        common::scn::merge(record.scn)?;
        let place = self.apply_state(state)?;
        if state.deletion.retired() {
            return Err(Error::new("journal appends to a retired EN extent"));
        }
        self.apply_journal_marks(state.sn, state.id, state.created, marks)?;
        self.remove_appends(|a| {
            a.extent == place && a.logical == logical && a.first_record == first_record
        });
        self.appends.push(Append {
            extent: place,
            offset,
            logical,
            first_record,
            scn: Some(record.scn),
            stage: Some(record.stage),
            holds,
            written: record.written,
            quorum: false,
            blocks,
        });
        self.index_staged_append(place, record.stage, record.scn, logical, first_record)?;
        let append = self.appends.last().expect("recovered append");
        let end = Mark {
            logical: append.logical.plus(append.holds.logical)?,
            records: append.first_record.plus(append.holds.records)?,
        };
        if let Some(mark) = self.marks.get(&(state.sn, state.id, state.created))
            && end <= mark.durable
        {
            self.appends.last_mut().expect("recovered append").quorum = true;
        }
        let key = (state.sn, state.id, state.created);
        let written = self.advance(place, true)?;
        self.marks.entry(key).or_default().written = written;
        let durable = self.advance(place, false)?;
        self.marks.entry(key).or_default().durable = durable;
        Ok(())
    }

    fn apply_journal_marks(
        &mut self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        marks: Marks,
    ) -> Result<()> {
        let current = self.marks.entry((sn, id, created)).or_default();
        current.written = current.written.max(marks.written);
        self.apply_durable(sn, id, created, marks.durable)
    }

    fn delete_instance(
        &mut self,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        retention: Option<common::time::TimeIntSec64>,
        hard: bool,
        maximum: common::time::TimeIntSec64,
    ) -> Result<()> {
        let now = TimeSec::now();
        let key = (sn, id, created);
        let place = if let Some(&place) = self.at.get(&(sn, id)) {
            if self.extents[place].created != created {
                return Err(Error::new("wrong extent creation second"));
            }
            if self.extents[place].deletion.retired() {
                return Ok(());
            }
            place
        } else {
            // EN creates instances lazily on the first append. A soft-delete must
            // also fence an as-yet-unwritten replica against a late write-append.
            if retention.is_none() {
                return Err(Error::new("hard delete requires a soft-deleted instance"));
            }
            let mut probe = common::deletion::Extras::<_>::default();
            probe.transition(
                key,
                &mut common::deletion::Flags::default(),
                now,
                retention,
                hard,
                maximum,
            )?;
            self.place(sn, id, created)?
        };
        let extent = &mut self.extents[place];
        self.extras.transition(key, &mut extent.deletion, now, retention, hard, maximum)?;
        if hard && !extent.deletion.retired() {
            assert!(self.extras.reclaim(&key, &mut extent.deletion, now));
            self.remove_appends(|append| append.extent == place);
        }
        Ok(())
    }

    fn place(&mut self, sn: SnId, id: ExtentId, created: TimeSec) -> Result<usize> {
        if let Some(place) = self.at.get(&(sn, id)) {
            let held = self.extents[*place].created;
            if held != created {
                return Err(Error::new(format!(
                    "extent {id} of sn {sn} was made in second {held} because a second is part of a name, not {created}"
                )));
            }
            if !self.extents[*place].deletion.readable(false) {
                return Err(Error::new("extent is deleted"));
            }
            return Ok(*place);
        }
        self.extents.push(Extent {
            deletion: common::deletion::Flags::default(),
            sn_id: sn,
            id,
            created,
            sealed: false,
            end: ECoff::ZERO,
            logical_end: ELoff::ZERO,
            records: ERoff::ZERO,
            open_record: false,
            scn_first: ScnIndex::default(),
        });
        let place = self.extents.len() - 1;
        self.at.insert((sn, id), place);
        Ok(place)
    }

    /// No encrypted piece may leave both components of its identity unchanged.
    fn check_progress(appending: &AppendBlock) -> Result<()> {
        let check = |holds: Holds| {
            if holds.logical == ELoff::ZERO && holds.records == ERoff::ZERO {
                Err(Error::new("an encrypted block must advance bytes or records"))
            } else {
                Ok(())
            }
        };
        if appending.pieces.is_empty() {
            check(appending.holds)?;
        }
        for piece in &appending.pieces {
            check(piece.holds)?;
        }
        Ok(())
    }

    fn check_retry(
        existing: &Append,
        appending: &AppendBlock,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        extent: &Extent,
        stage: Option<&journal::DiskFiles>,
    ) -> Result<()> {
        let single;
        let pieces = if appending.pieces.is_empty() {
            single = vec![AppendPiece {
                holds: appending.holds,
                crc: appending.crc,
                bytes: appending.bytes.clone(),
            }];
            &single
        } else {
            &appending.pieces
        };
        if existing.holds != appending.holds || existing.blocks.len() != pieces.len() {
            return Err(Error::new("retry conflicts with existing append"));
        }
        for (old, new) in existing.blocks.iter().zip(pieces) {
            let (crc, framing, bytes) =
                old.decrypt(daily_cache, extent_cache, process, extent, stage)?;
            if old.holds != new.holds
                || crc != new.crc
                || framing != appending.framing
                || bytes != new.bytes
            {
                return Err(Error::new("retry conflicts with existing append"));
            }
        }
        Ok(())
    }

    fn take(
        &mut self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        appending: &AppendBlock,
        cap: ECoff,
        stage: Option<&journal::DiskFiles>,
        scn: Scn,
    ) -> Result<Took> {
        let logical_size = appending.holds.logical;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];

        let offset = extent.end;
        let logical_offset = extent.logical_end;
        let first_record = extent.records;

        if appending.record_offset.is_some() && appending.logical_offset.is_none() {
            return Err(Error::new("an expected record count requires a fixed byte offset"));
        }
        if appending.logical_offset.is_some()
            && logical_size == ELoff::ZERO
            && appending.record_offset.is_none()
        {
            return Err(Error::new(
                "a zero-length fixed-offset append requires an expected record count",
            ));
        }
        Self::check_progress(appending)?;
        if let Some(wanted) = appending.logical_offset {
            let wanted_record = appending.record_offset;
            if wanted < logical_offset || wanted_record.is_some_and(|n| n < first_record) {
                if let Some(existing) = self.appends.iter().find(|a| {
                    a.extent == place
                        && a.logical == wanted
                        && wanted_record.is_none_or(|n| n == a.first_record)
                        && a.holds == appending.holds
                }) {
                    Self::check_retry(
                        existing,
                        appending,
                        daily_cache,
                        extent_cache,
                        process,
                        extent,
                        stage,
                    )?;
                    return Ok(Took::Landed(Landed {
                        offset: existing.offset,
                        logical: existing.logical,
                        first_record: existing.first_record,
                        records: existing.holds.records,
                    }));
                }
                return Err(Error::new(format!(
                    "fixed-offset append to {wanted} conflicts with existing data"
                )));
            }
            if wanted > logical_offset || wanted_record.is_some_and(|n| n > first_record) {
                return Ok(Took::Queued);
            }
        }

        let stored_len = appending.holds.compressed;

        let sealable = !extent.open_record;
        let full = extent.end.plus(stored_len)? > cap
            || extent.logical_end.plus(logical_size).is_err()
            || extent.records.plus(appending.holds.records).is_err();
        if extent.sealed || (sealable && full) {
            let closing = !extent.sealed;
            extent.sealed = true;
            let came_to = Filled {
                records: extent.records,
                logical: extent.logical_end,
                compressed: extent.end,
            };
            return Ok(Took::Sealed { came_to, closing });
        }

        extent.end = offset.plus(stored_len)?;
        extent.logical_end = extent.logical_end.plus(logical_size)?;
        let records = appending.holds.records;
        extent.records = first_record.plus(records)?;
        if appending.holds.records > ERoff::ZERO {
            extent.open_record = false;
        } else if appending.holds.logical > ELoff::ZERO {
            extent.open_record = true;
        }

        let pieces = if appending.pieces.is_empty() {
            vec![AppendPiece {
                holds: appending.holds,
                crc: appending.crc,
                bytes: appending.bytes.clone(),
            }]
        } else {
            appending.pieces.clone()
        };
        let mut encrypted = Vec::with_capacity(pieces.len());
        let mut piece_logical = logical_offset;
        let mut piece_records = first_record;
        for piece in pieces {
            encrypted.push(Encrypted::encrypt(
                daily_cache,
                extent_cache,
                process,
                extent,
                offset,
                piece_logical,
                piece_records,
                appending.framing,
                piece.crc,
                piece.holds,
                &piece.bytes,
            )?);
            piece_logical = piece_logical.plus(piece.holds.logical)?;
            piece_records = piece_records.plus(piece.holds.records)?;
        }
        let append = Append {
            extent: place,
            offset,
            logical: logical_offset,
            first_record,
            scn: Some(scn),
            stage: None,
            holds: appending.holds,
            written: false,
            quorum: false,
            blocks: encrypted,
        };
        self.appends.push(append);

        Ok(Took::Landed(Landed { offset, logical: logical_offset, first_record, records }))
    }

    fn write_append(
        &mut self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        appending: &AppendBlock,
        stage: Option<&journal::DiskFiles>,
        scn: Scn,
    ) -> Result<Written> {
        let logical_size = appending.holds.logical;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        if self.extents[place].sealed {
            return Err(Error::new(format!(
                "extent {} of sn {} is sealed, so nothing more goes in it",
                appending.extent, appending.sn
            )));
        }

        let logical_offset = appending
            .logical_offset
            .ok_or_else(|| Error::new("write-append to EN must carry a logical offset"))?;

        let offset = self.extents[place].end;
        let first_record = appending
            .record_offset
            .ok_or_else(|| Error::new("write-append must carry an extent record offset"))?;
        Self::check_progress(appending)?;
        if let Some(existing) = self.appends.iter().find(|a| {
            a.extent == place && a.logical == logical_offset && a.first_record == first_record
        }) {
            if existing.scn != Some(scn) {
                return Err(Error::new("replicated append retry has a different SCN"));
            }
            Self::check_retry(
                existing,
                appending,
                daily_cache,
                extent_cache,
                process,
                &self.extents[place],
                stage,
            )?;
            return Ok(Written::Already);
        }
        // Overlapping identities must not be encrypted again with different data.
        let end = (first_record.plus(appending.holds.records)?, logical_offset.plus(logical_size)?);
        for existing in self.appends.iter().filter(|a| a.extent == place) {
            let old_end = (
                existing.first_record.plus(existing.holds.records)?,
                existing.logical.plus(existing.holds.logical)?,
            );
            if (first_record, logical_offset) < old_end
                && (existing.first_record, existing.logical) < end
            {
                return Err(Error::new("replicated append overlaps existing data"));
            }
        }

        let extent = &mut self.extents[place];
        let stored_len = appending.holds.compressed;

        let pieces = if appending.pieces.is_empty() {
            vec![AppendPiece {
                holds: appending.holds,
                crc: appending.crc,
                bytes: appending.bytes.clone(),
            }]
        } else {
            appending.pieces.clone()
        };
        let mut encrypted = Vec::with_capacity(pieces.len());
        let mut piece_logical = logical_offset;
        let mut piece_records = first_record;
        for piece in pieces {
            encrypted.push(Encrypted::encrypt(
                daily_cache,
                extent_cache,
                process,
                extent,
                offset,
                piece_logical,
                piece_records,
                appending.framing,
                piece.crc,
                piece.holds,
                &piece.bytes,
            )?);
            piece_logical = piece_logical.plus(piece.holds.logical)?;
            piece_records = piece_records.plus(piece.holds.records)?;
        }

        let extent = &mut self.extents[place];
        extent.end = extent.end.max(offset.plus(stored_len)?);
        extent.logical_end = extent.logical_end.max(logical_offset.plus(logical_size)?);
        extent.records = extent.records.max(first_record.plus(appending.holds.records)?);
        if appending.holds.records > ERoff::ZERO {
            extent.open_record = false;
        } else if appending.holds.logical > ELoff::ZERO {
            extent.open_record = true;
        }

        let append = Append {
            extent: place,
            offset,
            logical: logical_offset,
            first_record,
            scn: Some(scn),
            stage: None,
            holds: appending.holds,
            written: false,
            quorum: false,
            blocks: encrypted,
        };
        self.appends.push(append);
        let key = (appending.sn, appending.extent, appending.created);
        let end = Mark {
            logical: logical_offset.plus(appending.holds.logical)?,
            records: first_record.plus(appending.holds.records)?,
        };
        if self.marks.get(&key).is_some_and(|marks| end <= marks.durable) {
            self.appends.last_mut().expect("stored append").quorum = true;
        }

        Ok(Written::Stored)
    }

    #[allow(dead_code)]
    fn truncate(
        &mut self,
        _daily_cache: &DailyKeyCache,
        _extent_cache: &ExtentKeyCache,
        _process: &Process,
        sn: SnId,
        id: ExtentId,
        above: ELoff,
    ) -> Result<Truncated> {
        let Some(&place) = self.at.get(&(sn, id)) else {
            return Err(Error::new(format!("no extent {id} of sn {sn} here")));
        };

        let mut boundary = above == ELoff::ZERO;
        let mut records = ERoff::ZERO;
        let mut logical = ELoff::ZERO;
        let mut length = ECoff::ZERO;
        let mut discarded = 0usize;
        let mut last: Option<(ELoff, bool)> = None;
        for append in self.appends.iter().filter(|append| append.extent == place) {
            let past = append.logical.plus(append.holds.logical)?;
            if past == above {
                boundary = true;
            }
            if append.logical >= above {
                discarded += 1;
                continue;
            }
            if past > above {
                return Err(Error::new(format!(
                    "logical offset {above} is inside the block at {}, and a block is never split",
                    append.logical
                )));
            }
            records = records.plus(append.holds.records)?;
            logical = logical.plus(append.holds.logical)?;
            length = length.plus(append.holds.compressed)?;
            let is_open = append.holds.records == ERoff::ZERO && append.holds.logical > ELoff::ZERO;
            if last.is_none_or(|(at, _)| append.logical >= at) {
                last = Some((append.logical, is_open));
            }
        }
        if !boundary {
            return Err(Error::new(format!(
                "logical offset {above} is past everything extent {id} of sn {sn} holds"
            )));
        }
        if last.is_some_and(|(_, open)| open) {
            return Err(Error::new(format!(
                "truncating extent {id} of sn {sn} to {above} would seal it with a record open"
            )));
        }

        self.remove_appends(|append| append.extent == place && append.logical >= above);
        let extent = &mut self.extents[place];
        extent.sealed = true;
        extent.end = length;
        extent.logical_end = logical;
        extent.records = records;
        extent.open_record = false;
        Ok(Truncated { discarded, records, length })
    }

    fn range(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        place: usize,
        from: ERoff,
        budget: Budget,
    ) -> Result<Option<Ranged>> {
        let mut blocks: Vec<&Append> =
            self.appends.iter().filter(|append| append.extent == place).collect();
        blocks.sort_by_key(|append| (append.first_record, append.logical));

        let extent = &self.extents[place];

        let Some(first) = blocks.iter().position(|a| {
            a.first_record >= from
                || from.number() < a.first_record.number() + a.holds.records.number()
        }) else {
            return Ok(None);
        };

        let mut covered = ExtentCovered::default();
        let mut pieces: Vec<Piece> = Vec::new();
        'append: for append in &blocks[first..] {
            for block in &append.blocks {
                let (crc, _framing, plain_bytes) = block.decrypt(
                    daily_cache,
                    extent_cache,
                    process,
                    extent,
                    self.stage.as_deref(),
                )?;
                covered.took(block.holds.records, block.holds.logical, block.holds.compressed);
                pieces.push(Piece {
                    logical: block.holds.logical,
                    first_record: block.first_record,
                    records: block.holds.records,
                    crc,
                    bytes: plain_bytes,
                });
                let is_open =
                    block.holds.records == ERoff::ZERO && block.holds.logical > ELoff::ZERO;
                if (budget.met(&covered) || covered.compressed.number() >= 1 << 20) && !is_open {
                    break 'append;
                }
            }
        }

        let last_piece = pieces.last().unwrap();
        covered.next =
            last_piece.first_record.plus(last_piece.records).expect("extent record position fits");
        let total_blocks: usize = blocks.iter().map(|append| append.blocks.len()).sum();
        covered.more = first + pieces.len() < total_blocks;
        Ok(Some(Ranged { pieces, covered }))
    }

    /// Locate payload bytes using only Holds on preceding blocks. Stored payloads
    /// are decrypted only once their byte interval intersects the requested range.
    fn payload_range(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        place: usize,
        from: ERoff,
        mut skip: u64,
        amount: u64,
    ) -> Result<Vec<Arg>> {
        let mut appends: Vec<_> = self.appends.iter().filter(|a| a.extent == place).collect();
        appends.sort_by_key(|a| a.offset);
        let mut seek = common::range::ByteSeek { skipped: 0, next: from };
        let mut pieces = Vec::new();
        let mut remaining = amount;
        for block in appends.into_iter().flat_map(|a| &a.blocks) {
            let end = block.first_record.plus(block.holds.records)?;
            if end <= from {
                continue;
            }
            if block.first_record < from {
                return Err(Error::new("payload seek must start at a block boundary"));
            }
            let payload = Framing::Bytes
                .visible_length(block.holds.logical.number(), block.holds.records.number())
                .ok_or_else(|| Error::new("invalid payload length in block header"))?;
            if skip >= payload {
                seek.next = end;
                skip -= payload;
                seek.skipped += payload;
                continue;
            }
            if remaining == 0 {
                break;
            }
            let (crc, framing, bytes) = block.decrypt(
                daily_cache,
                extent_cache,
                process,
                &self.extents[place],
                self.stage.as_deref(),
            )?;
            if framing != Framing::Length {
                return Err(Error::new("payload seek requires length-framed storage"));
            }
            seek.next = end;
            pieces.push(Piece {
                logical: block.holds.logical,
                first_record: block.first_record,
                records: block.holds.records,
                crc,
                bytes,
            });
            remaining = remaining.saturating_sub(payload - skip);
            skip = 0;
            if remaining == 0 {
                break;
            }
        }
        let mut args = seek.args();
        for piece in pieces {
            args.extend(piece.args());
        }
        Ok(args)
    }

    fn created(&self, sn: SnId, id: ExtentId) -> Option<TimeSec> {
        self.at.get(&(sn, id)).map(|place| self.extents[*place].created)
    }

    #[cfg(test)]
    fn read(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        sn: SnId,
        id: ExtentId,
    ) -> Vec<u8> {
        let Some(&place) = self.at.get(&(sn, id)) else { return Vec::new() };
        let mut found: Vec<&Append> =
            self.appends.iter().filter(|append| append.extent == place).collect();
        found.sort_by_key(|append| (append.first_record, append.logical));
        let extent = &self.extents[place];
        found
            .iter()
            .flat_map(|append| {
                append.blocks.iter().flat_map(|block| {
                    block
                        .decrypt(daily_cache, extent_cache, process, extent, self.stage.as_deref())
                        .unwrap()
                        .2
                        .into_iter()
                })
            })
            .collect()
    }
}

fn durable_request(
    en: EnId,
    disk: DiskId,
    sn: SnId,
    extent: ExtentId,
    created: TimeSec,
    mark: Mark,
) -> Req {
    let mut payload = StructWriter::new();
    payload.en_id(en);
    payload.disk_id(disk);
    payload.sn_id(sn);
    payload.extent_id(extent);
    payload.time_sec(created);
    payload.eloff(mark.logical);
    payload.eroff(mark.records);
    Req::new(Op::WriteDurable).structure(payload)
}

struct Disk {
    held: Mutex<Extents>,
    storage: RwLock<Option<Arc<journal::DiskFiles>>>,
    waiting: Mutex<Waiting>,
    appends: AtomicU64,
    bytes: AtomicU64,
    sealed: AtomicU64,
    checksummed: AtomicU64,
    daily_cache: Arc<DailyKeyCache>,
    extent_cache: Arc<ExtentKeyCache>,
    pending_fixed: Mutex<HashMap<(SnId, ExtentId, ELoff, Option<ERoff>), QueuedAppend>>,
}

impl Disk {
    fn new(daily_cache: Arc<DailyKeyCache>) -> Disk {
        Disk {
            held: Mutex::new(Extents::new()),
            storage: RwLock::new(None),
            waiting: Mutex::new(HashMap::new()),
            appends: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            checksummed: AtomicU64::new(0),
            daily_cache,
            extent_cache: Arc::new(ExtentKeyCache::new()),
            pending_fixed: Mutex::new(HashMap::new()),
        }
    }

    fn recover(
        self: &Arc<Self>,
        process: &Arc<Process>,
        disk: DiskId,
        max_journal_bytes: u64,
        checkpoint_count: usize,
    ) -> Result<()> {
        let en = EnId::new(process.entry.number());
        let directory = process.config.data_dir.join(format!("disk-{disk}"));
        let key_process = Arc::clone(process);
        let daily_cache = Arc::clone(&self.daily_cache);
        let key_lookup: journal::KeyLookup =
            Arc::new(move |ts| daily_cache.get_en(&key_process, en, ts));
        let (storage, checkpoint, records) = journal::DiskFiles::open(
            &directory,
            en,
            disk,
            key_lookup,
            max_journal_bytes,
            checkpoint_count,
            journal_recovery_mark,
        )?;
        {
            let mut held = self.held.lock().expect("extents");
            held.stage = Some(Arc::clone(&storage));
            if let Some(checkpoint) = checkpoint {
                if let Some(scn) = checkpoint.max_scn {
                    common::scn::merge(scn)?;
                }
                *held = checkpoint::decode(&checkpoint.data, Some(Arc::clone(&storage)))?;
            }
            for record in records {
                match record.op {
                    Op::EnJAppend => held.restore_append(record)?,
                    Op::EnJExtent | Op::EnJDelete | Op::EnJSeal => held.restore_state(record)?,
                    op => return Err(Error::new(format!("unsupported EN journal operation {op}"))),
                }
            }
            let appends = held.appends.len() as u64;
            self.appends.store(appends, Ordering::Relaxed);
            self.sealed.store(
                held.extents.iter().filter(|extent| extent.sealed).count() as u64,
                Ordering::Relaxed,
            );
        }
        *self.storage.write().expect("disk storage") = Some(Arc::clone(&storage));
        let checkpoint_rx = storage.take_checkpoint_receiver();
        let storage_weak = Arc::downgrade(&storage);
        thread::Builder::new().name(format!("en-checkpoint-{disk}")).spawn(move || {
            while checkpoint_rx.recv().is_ok() {
                let Some(storage) = storage_weak.upgrade() else {
                    break;
                };
                if let Err(error) = checkpoint::write_next(&storage) {
                    storage.checkpoint_failed();
                    logw!("EN disk checkpoint failed", "disk {disk},{error}");
                }
            }
        })?;
        storage.request_checkpoint_if_due();
        Ok(())
    }

    fn sn_ids(&self) -> Vec<SnId> {
        self.held.lock().expect("extents").sn_ids()
    }

    fn persist_record(
        &self,
        op: Op,
        event: Option<JournalEvent>,
        done: impl FnOnce(Result<()>) + Send + 'static,
    ) {
        if self.storage.read().expect("disk storage").is_none() {
            done(Ok(()));
            return;
        }
        let scn = match common::scn::next() {
            Ok(scn) => scn,
            Err(error) => {
                done(Err(error));
                return;
            }
        };
        self.persist_record_at(scn, op, event, move |result| {
            done(result.map(|_| ()));
        });
    }

    fn persist_record_at(
        &self,
        scn: Scn,
        op: Op,
        event: Option<JournalEvent>,
        done: impl FnOnce(Result<Option<journal::StageRef>>) + Send + 'static,
    ) {
        let Some(storage) = self.storage.read().expect("disk storage").clone() else {
            done(Ok(None));
            return;
        };
        let Some(event) = event else {
            done(Err(Error::new("missing EN journal event")));
            return;
        };
        if let Err(error) =
            storage.submit(scn, op, event.metadata, event.stage_metadata, event.stage, done)
        {
            logw!("could not queue EN journal record", "{error}");
        }
    }

    fn notify_durable(
        self: &Arc<Self>,
        process: &Arc<Process>,
        peers: Vec<(EnId, DiskId)>,
        sn: SnId,
        id: ExtentId,
        created: TimeSec,
        mark: Mark,
    ) {
        for (en, disk) in peers {
            let request = durable_request(en, disk, sn, id, created, mark);
            let identity = format!("EN{en}");
            let target = identity.clone();
            if let Err(error) = process.ask(&identity, &request, move |answer| {
                if answer.status != Status::Ok {
                    logw!("EN durable update was rejected", "en {target},{}", answer.status);
                }
            }) {
                logw!("could not send EN durable update", "en {identity},{error}");
            }
        }
    }

    fn durable_wait(&self, sn: SnId, id: ExtentId, created: TimeSec, mark: Mark, reply: Reply) {
        let result = self.held.lock().expect("extents").apply_durable(sn, id, created, mark);
        match result {
            Ok(()) => reply.send(Resp::ok()),
            Err(error) => reply.send(bad(error)),
        }
    }

    fn delete_wait(
        self: &Arc<Self>,
        sn: SnId,
        extent: ExtentId,
        created: TimeSec,
        retention: Option<common::time::TimeIntSec64>,
        hard: bool,
        maximum: common::time::TimeIntSec64,
        reply: Reply,
    ) {
        let storage = self.storage.read().expect("disk storage").clone();
        let event = {
            let mut held = self.held.lock().expect("extents");
            match held.delete_instance(sn, extent, created, retention, hard, maximum) {
                Err(error) => {
                    reply.send(bad(error));
                    return;
                }
                Ok(()) if storage.is_some() => match held.journal_state(sn, extent, false) {
                    Ok(metadata) => Some(JournalEvent::state(metadata)),
                    Err(error) => {
                        reply.send(bad(error));
                        return;
                    }
                },
                Ok(()) => None,
            }
        };

        let disk = Arc::clone(self);
        self.persist_record(Op::EnJDelete, event, move |result| match result {
            Ok(()) => {
                let mut queued = disk.pending_fixed.lock().expect("queue");
                let keys: Vec<_> = queued
                    .keys()
                    .filter(|(s, e, _, _)| *s == sn && *e == extent)
                    .copied()
                    .collect();
                for key in keys {
                    if let Some((_, waiting)) = queued.remove(&key) {
                        waiting.send(bad("extent was deleted"));
                    }
                }
                drop(queued);
                for pending in
                    disk.waiting.lock().expect("waiting").remove(&(sn, extent)).unwrap_or_default()
                {
                    pending.lost();
                }
                reply.send(Resp::ok());
            }
            Err(error) => {
                logw!("EN deletion was not durable", "{}", error);
                reply.send(Resp::error(Status::Internal, error.to_string()));
            }
        });
    }

    fn seal_wait(self: &Arc<Self>, sn: SnId, extent: ExtentId, created: TimeSec, reply: Reply) {
        let storage = self.storage.read().expect("disk storage").clone();
        let sealed: Result<(Filled, bool, Option<JournalEvent>)> = {
            let mut held = self.held.lock().expect("extents");
            let Some(&place) = held.at.get(&(sn, extent)) else {
                reply.send(bad(format!("no extent {extent} of sn {sn}")));
                return;
            };
            let current = &mut held.extents[place];
            if current.created != created {
                reply.send(bad("force-seal extent has the wrong creation second"));
                return;
            }
            if !current.deletion.readable(false) {
                reply.send(bad("extent is deleted"));
                return;
            }
            let closing = !current.sealed;
            current.sealed = true;
            let filled = Filled {
                records: current.records,
                logical: current.logical_end,
                compressed: current.end,
            };
            let event = if storage.is_some() {
                match held.journal_state(sn, extent, true) {
                    Ok(metadata) => Some(JournalEvent::state(metadata)),
                    Err(error) => {
                        reply.send(bad(error));
                        return;
                    }
                }
            } else {
                None
            };
            Ok((filled, closing, event))
        };
        let (filled, closing, event) = match sealed {
            Ok(value) => value,
            Err(error) => {
                reply.send(bad(error));
                return;
            }
        };
        let disk = Arc::clone(self);
        self.persist_record(Op::EnJSeal, event, move |result| match result {
            Ok(()) => {
                if closing {
                    disk.sealed.fetch_add(1, Ordering::Relaxed);
                }
                reply.send(Resp::from_args(Status::Ok, filled.args()).id(0));
            }
            Err(error) => {
                logw!("EN seal was not durable", "{}", error);
                reply.send(Resp::error(Status::Internal, error.to_string()));
            }
        });
    }

    fn append(
        self: &Arc<Self>,
        process: &Arc<Process>,
        mine: (EnId, DiskId),
        appending: AppendBlock,
        reply: Reply,
    ) -> Result<()> {
        for i in 0..appending.instances.len() {
            for j in i + 1..appending.instances.len() {
                if appending.instances[i] == appending.instances[j] {
                    reply.send(Resp::error(
                        Status::BadRequest,
                        format!("duplicate instance {:?} in replica set", appending.instances[i]),
                    ));
                    return Ok(());
                }
            }
        }

        if let Err(error) = Self::check_size(&appending, process.cluster.extents.max_append_bytes) {
            reply.send(bad(error));
            return Ok(());
        }

        if appending.logical_offset.is_none()
            && appending.holds.records == ERoff::ZERO
            && appending.holds.logical > ELoff::ZERO
        {
            reply.send(Resp::error(
                Status::BadRequest,
                "non-fixed-offset appends may not leave an open record",
            ));
            return Ok(());
        }

        self.try_append(process, mine, appending, reply)
    }

    fn check_size(appending: &AppendBlock, maximum: u32) -> Result<()> {
        // Payload accounting must not turn a compressed run of empty records into
        // an unbounded decompression. Allow one hidden bytes-mode prefix beyond the cap.
        let framed = appending
            .framing
            .visible_length(appending.holds.logical.number(), appending.holds.records.number());
        if appending.holds.logical.number() > maximum as u64
            || framed.is_none_or(|n| n > maximum as u64 + 4)
        {
            return Err(Error::new("append exceeds maximum payload or framed size"));
        }
        Ok(())
    }

    fn try_append(
        self: &Arc<Self>,
        process: &Arc<Process>,
        mine: (EnId, DiskId),
        appending: AppendBlock,
        mut reply: Reply,
    ) -> Result<()> {
        let length: u32 = match u32::try_from(appending.bytes.len()) {
            Ok(len) => len,
            Err(_) => {
                reply.send(Resp::error(Status::BadRequest, "append bytes exceed 32 bits"));
                return Ok(());
            }
        };

        let cap = ECoff::new(process.cluster.extents.seal_bytes);
        let storage = self.storage.read().expect("disk storage").clone();
        // Capture the journal event while the same lock still protects the state
        // transition. A later append must not leak into this record's extent totals.
        let captured = {
            let mut held = self.held.lock().expect("extents");
            let scn = if storage.is_some() {
                match common::scn::next() {
                    Ok(scn) => scn,
                    Err(error) => {
                        reply.send(bad(error));
                        return Ok(());
                    }
                }
            } else {
                Scn::new(0)
            };
            match held.take(
                &self.daily_cache,
                &self.extent_cache,
                process,
                &appending,
                cap,
                storage.as_deref(),
                scn,
            ) {
                Ok(took) => {
                    let append_scn = if let Took::Landed(landed) = &took {
                        let place = held.at[&(appending.sn, appending.extent)];
                        held.appends
                            .iter()
                            .find(|append| {
                                append.extent == place
                                    && append.logical == landed.logical
                                    && append.first_record == landed.first_record
                            })
                            .and_then(|append| append.scn)
                    } else {
                        None
                    };
                    let event = match (&took, storage.is_some()) {
                        (Took::Landed(landed), true) => {
                            match held.journal_append(
                                &self.daily_cache,
                                &self.extent_cache,
                                process,
                                appending.sn,
                                appending.extent,
                                *landed,
                            ) {
                                Ok(event) => Some(event),
                                Err(error) => {
                                    return {
                                        reply.send(bad(error));
                                        Ok(())
                                    };
                                }
                            }
                        }
                        (Took::Sealed { .. }, true) => {
                            match held.journal_state(appending.sn, appending.extent, true) {
                                Ok(metadata) => Some(JournalEvent::state(metadata)),
                                Err(error) => {
                                    return {
                                        reply.send(bad(error));
                                        Ok(())
                                    };
                                }
                            }
                        }
                        _ => None,
                    };
                    Ok((took, event, append_scn))
                }
                Err(error) => Err(error),
            }
        };
        let (took, event, append_scn) = match captured {
            Ok(value) => value,
            Err(error) => {
                reply.send(bad(error));
                return Ok(());
            }
        };

        let landed = match took {
            Took::Landed(landed) => landed,
            Took::Queued => {
                let lo = appending.logical_offset.unwrap();
                let mut queue = self.pending_fixed.lock().expect("queue");
                let held = self.held.lock().expect("extents");
                let place = held.at[&(appending.sn, appending.extent)];
                if !held.extents[place].deletion.readable(false) {
                    reply.send(bad("extent was deleted"));
                    return Ok(());
                }
                queue.insert(
                    (appending.sn, appending.extent, lo, appending.record_offset),
                    (appending, reply),
                );
                return Ok(());
            }
            Took::Sealed { came_to, closing } => {
                reply.about(format_args!(
                    "extent {} of sn {},records {},logical {},bytes {}",
                    appending.extent,
                    appending.sn,
                    came_to.records,
                    came_to.logical,
                    came_to.compressed
                ));
                let disk = Arc::clone(self);
                self.persist_record(Op::EnJSeal, event, move |result| match result {
                    Ok(()) => {
                        if closing {
                            disk.sealed.fetch_add(1, Ordering::Relaxed);
                        }
                        reply.send(Resp::from_args(Status::Sealed, came_to.args()).id(0));
                    }
                    Err(error) => {
                        logw!("EN seal was not durable", "{}", error);
                        reply.send(Resp::error(Status::Internal, error.to_string()));
                    }
                });
                return Ok(());
            }
        };
        let scn = append_scn.ok_or_else(|| Error::new("landed EN append has no SCN"))?;
        reply.about(format_args!(
            "extent {} of sn {},offset {},bytes {length},records {}",
            appending.extent, appending.sn, landed.logical, landed.records
        ));

        let others: Vec<(EnId, DiskId)> =
            appending.instances.iter().filter(|&inst| *inst != mine).cloned().collect();

        let receipt = vec![
            EnReceipt {
                sn: appending.sn,
                logical: landed.logical,
                first_record: landed.first_record,
                records: landed.records,
                extent: appending.extent,
                created: appending.created,
                length: appending.holds.logical,
            }
            .write(),
        ];

        let quorum_wants = process.cluster.extents.quorum;
        let sn = appending.sn;
        let extent = appending.extent;
        let created = appending.created;
        let logical = landed.logical;
        let first_record = landed.first_record;
        let quorum_disk = Arc::clone(self);
        let after_quorum: QuorumHook = Box::new(move || {
            let result = quorum_disk.held.lock().expect("extents").mark_quorum(
                sn,
                extent,
                created,
                logical,
                first_record,
            );
            match result {
                Ok(mark) => mark,
                Err(error) => {
                    logw!("could not advance EN durable watermark", "{}", error);
                    None
                }
            }
        });
        let notifier_disk = Arc::clone(self);
        let notifier_process = Arc::clone(process);
        let peers = others.clone();
        let after_reply: ReplyHook = Box::new(move |mark| {
            if let Some(mark) = mark {
                notifier_disk.notify_durable(&notifier_process, peers, sn, extent, created, mark);
            }
        });
        let pending =
            Arc::new(Pending::new(receipt, quorum_wants, reply, after_quorum, after_reply));
        match landed.records == ERoff::ZERO {
            true => {
                let mut waiting = self.waiting.lock().expect("waiting");
                let held = self.held.lock().expect("extents");
                let place = held.at[&(appending.sn, appending.extent)];
                if !held.extents[place].deletion.readable(false) {
                    pending.lost();
                    return Ok(());
                }
                waiting
                    .entry((appending.sn, appending.extent))
                    .or_default()
                    .push(Arc::clone(&pending));
            }
            false => {
                pending.records_are_whole();
                self.finish(appending.sn, appending.extent);
            }
        }
        let quorum = Arc::new(Quorum::new(others.len(), quorum_wants, pending));
        let local = Arc::clone(&quorum);
        let local_disk = Arc::clone(self);
        let local_key = (sn, extent, created, logical, first_record);
        self.persist_record_at(scn, Op::EnJAppend, event, move |result| {
            let written = match result {
                Ok(stage_ref) => match local_disk.held.lock().expect("extents").mark_written(
                    local_key.0,
                    local_key.1,
                    local_key.2,
                    local_key.3,
                    local_key.4,
                    stage_ref,
                ) {
                    Ok(()) => true,
                    Err(error) => {
                        logw!("could not advance EN written watermark", "{}", error);
                        false
                    }
                },
                Err(error) => {
                    logw!("local EN append was not durable", "{}", error);
                    false
                }
            };
            local.local_written(written);
        });

        let mut onward = appending.clone();
        onward.instances = Vec::new();
        onward.logical_offset = Some(landed.logical);
        onward.record_offset = Some(landed.first_record);

        for (en, disk) in others {
            let write_request = onward.request_with_scn(en, disk, scn);
            let identity = format!("EN{en}");
            let waiting = Arc::clone(&quorum);
            if let Err(e) = process.ask(&identity, &write_request, move |answer| {
                waiting.written(answer.status == Status::Ok);
            }) {
                logw!("could not reach an instance", "en {identity},{e}");
                quorum.written(false);
            }
        }

        // Wake both byte-only requests and requests naming this record boundary.
        // Requests overtaken by an append must wake too, to receive a conflict.
        let next_lo = landed.logical.plus(appending.holds.logical)?;
        let next_record = landed.first_record.plus(landed.records)?;
        let ready = {
            let mut queue = self.pending_fixed.lock().expect("queue");
            let keys: Vec<_> = queue
                .keys()
                .filter(|(sn, extent, lo, record)| {
                    *sn == appending.sn
                        && *extent == appending.extent
                        && (*lo < next_lo
                            || record.is_some_and(|n| n < next_record)
                            || (*lo == next_lo && record.is_none_or(|n| n == next_record)))
                })
                .copied()
                .collect();
            keys.into_iter().filter_map(|key| queue.remove(&key)).collect::<Vec<_>>()
        };
        for (next_app, next_reply) in ready {
            self.try_append(process, mine, next_app, next_reply)?;
        }

        Ok(())
    }

    #[cfg(test)]
    fn take(&self, process: &Process, appending: &AppendBlock, cap: ECoff) -> Result<Took> {
        let scn = Scn::new(0);
        let took = self.held.lock().expect("extents").take(
            &self.daily_cache,
            &self.extent_cache,
            process,
            appending,
            cap,
            None,
            scn,
        )?;
        if let Took::Landed(_) = &took {
            self.appends.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
            self.checksummed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(took)
    }

    fn finish(&self, sn: SnId, extent: ExtentId) {
        let waiting = self.waiting.lock().expect("waiting").remove(&(sn, extent));
        for pending in waiting.unwrap_or_default() {
            pending.records_are_whole();
        }
    }

    #[cfg(test)]
    fn write_append(&self, process: &Process, appending: AppendBlock) -> Resp {
        if let Err(error) = Self::check_size(&appending, process.cluster.extents.max_append_bytes) {
            return bad(error);
        }
        let storage = self.storage.read().expect("disk storage").clone();
        let scn = Scn::new(0);
        match self.held.lock().expect("extents").write_append(
            &self.daily_cache,
            &self.extent_cache,
            process,
            &appending,
            storage.as_deref(),
            scn,
        ) {
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            Ok(Written::Already) => return Resp::ok(),
            Ok(Written::Stored) => {}
        }
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
        if appending.holds.records > ERoff::ZERO {
            self.finish(appending.sn, appending.extent);
        }
        Resp::ok()
    }

    fn write_wait(
        self: &Arc<Self>,
        process: &Process,
        appending: AppendBlock,
        scn: Scn,
        reply: Reply,
    ) {
        if let Err(error) = Self::check_size(&appending, process.cluster.extents.max_append_bytes) {
            reply.send(bad(error));
            return;
        }
        let logical = match appending.logical_offset {
            Some(value) => value,
            None => {
                reply.send(bad("write-append lacks logical offset"));
                return;
            }
        };
        let first_record = match appending.record_offset {
            Some(value) => value,
            None => {
                reply.send(bad("write-append lacks record offset"));
                return;
            }
        };
        if let Err(error) = common::scn::merge(scn) {
            reply.send(bad(error));
            return;
        }
        let sn = appending.sn;
        let extent = appending.extent;
        let created = appending.created;
        let storage = self.storage.read().expect("disk storage").clone();
        let captured = {
            let mut held = self.held.lock().expect("extents");
            match held.write_append(
                &self.daily_cache,
                &self.extent_cache,
                process,
                &appending,
                storage.as_deref(),
                scn,
            ) {
                Err(error) => Err(error),
                Ok(written) => {
                    let event = if storage.is_some() {
                        let place = match held.at.get(&(appending.sn, appending.extent)) {
                            Some(place) => *place,
                            None => return reply.send(bad("written extent disappeared")),
                        };
                        let Some(append) = held.appends.iter().rev().find(|append| {
                            append.extent == place
                                && append.logical == logical
                                && append.first_record == first_record
                        }) else {
                            return reply.send(bad("written append disappeared"));
                        };
                        let landed = Landed {
                            offset: append.offset,
                            logical: append.logical,
                            first_record: append.first_record,
                            records: append.holds.records,
                        };
                        match held.journal_append(
                            &self.daily_cache,
                            &self.extent_cache,
                            process,
                            appending.sn,
                            appending.extent,
                            landed,
                        ) {
                            Ok(event) => Some(event),
                            Err(error) => return reply.send(bad(error)),
                        }
                    } else {
                        None
                    };
                    Ok((written, event))
                }
            }
        };
        let (written, event) = match captured {
            Ok(value) => value,
            Err(error) => {
                reply.send(bad(error));
                return;
            }
        };
        let disk = Arc::clone(self);
        let bytes = appending.bytes.len() as u64;
        let records = appending.holds.records;
        self.persist_record_at(scn, Op::EnJAppend, event, move |result| match result {
            Ok(stage_ref) => {
                let marked = disk.held.lock().expect("extents").mark_written(
                    sn,
                    extent,
                    created,
                    logical,
                    first_record,
                    stage_ref,
                );
                if let Err(error) = marked {
                    logw!("could not advance replica EN written watermark", "{}", error);
                    reply.send(Resp::error(Status::Internal, error.to_string()));
                    return;
                }
                if matches!(written, Written::Stored) {
                    disk.appends.fetch_add(1, Ordering::Relaxed);
                    disk.bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                if records > ERoff::ZERO {
                    disk.finish(sn, extent);
                }
                reply.send(Resp::ok());
            }
            Err(error) => {
                logw!("replica EN append was not durable", "{}", error);
                reply.send(Resp::error(Status::Internal, error.to_string()));
            }
        });
    }

    #[cfg(test)]
    fn range(
        &self,
        process: &Process,
        sn: SnId,
        id: ExtentId,
        from: ERoff,
        budget: Budget,
    ) -> Result<Option<Ranged>> {
        let held = self.held.lock().expect("extents");
        let Some(&place) = held.at.get(&(sn, id)) else { return Ok(None) };
        held.range(&self.daily_cache, &self.extent_cache, process, place, from, budget)
    }

    #[cfg(test)]
    fn read(
        &self,
        process: &Process,
        sn: SnId,
        extent: ExtentId,
        created: TimeSec,
        range: ExtentRange,
    ) -> Resp {
        self.read_including(process, sn, extent, created, range, false)
    }

    fn read_including(
        &self,
        process: &Process,
        sn: SnId,
        extent: ExtentId,
        created: TimeSec,
        range: ExtentRange,
        include: bool,
    ) -> Resp {
        let from = match range.start {
            ExtentStart::Record(from) | ExtentStart::Payload(from, _) => from,
            _ => {
                return Resp::error(
                    Status::Unimplemented,
                    "only record-boundary and payload starts are implemented",
                );
            }
        };

        let held = self.held.lock().expect("extents");
        match held.created(sn, extent) {
            None => {
                return Resp::error(
                    Status::NotFound,
                    format!("no extent {extent} of sn {sn} here"),
                );
            }
            Some(second) if second != created => {
                return Resp::error(
                    Status::NotFound,
                    format!("extent {extent} of sn {sn} was made in second {second}"),
                );
            }
            Some(_) => {}
        }
        let place = held.at[&(sn, extent)];
        if !held.extents[place].deletion.readable(include) {
            return Resp::error(Status::NotFound, "extent is deleted");
        }
        if let ExtentStart::Payload(_, skip) = range.start {
            let Budget::Logical(amount) = range.budget else {
                return Resp::error(Status::BadRequest, "payload seek needs a byte budget");
            };
            return match held.payload_range(
                &self.daily_cache,
                &self.extent_cache,
                process,
                place,
                from,
                skip,
                amount.min(common::range::Range::DEFAULT_LOGICAL),
            ) {
                Ok(args) => Resp::from_args(Status::Ok, args).id(0),
                Err(e) => Resp::error(Status::Internal, e.to_string()),
            };
        }
        let ranged = match held.range(
            &self.daily_cache,
            &self.extent_cache,
            process,
            place,
            from,
            range.budget,
        ) {
            Ok(Some(ranged)) => ranged,
            Ok(None) => {
                return Resp::error(
                    Status::NotFound,
                    format!("{extent} of sn {sn} has no record {from}"),
                );
            }
            Err(e) => return Resp::error(Status::Internal, e.to_string()),
        };

        let mut args = ranged.covered.args();
        for piece in ranged.pieces {
            args.extend(piece.args());
        }
        Resp::from_args(Status::Ok, args).id(0)
    }

    fn status(&self, _process: &Process, config: &ENConfig) -> Vec<String> {
        let extents = self.held.lock().expect("extents").extents.len();
        vec![
            format!("extents {extents}"),
            format!("appends {}", self.appends.load(Ordering::Relaxed)),
            format!("appended_bytes {}", self.bytes.load(Ordering::Relaxed)),
            format!("sealed_extents {}", self.sealed.load(Ordering::Relaxed)),
            format!("blocks_with_a_crc {}", self.checksummed.load(Ordering::Relaxed)),
            format!("media_class {}", config.media_class),
        ]
    }
}

struct EN {
    config: ENConfig,
    disks: Vec<Arc<Disk>>,
    _daily_cache: Arc<DailyKeyCache>,
    accepting: AtomicBool,
    startup_stop: AtomicBool,
    startup_status: Mutex<String>,
    startup_error: Mutex<Option<String>>,
    startup_thread: Mutex<Option<JoinHandle<()>>>,
}

impl EN {
    fn new(config: ENConfig) -> EN {
        let daily_cache = Arc::new(DailyKeyCache::new());
        EN {
            config,
            disks: (0..5).map(|_| Arc::new(Disk::new(Arc::clone(&daily_cache)))).collect(),
            _daily_cache: daily_cache,
            accepting: AtomicBool::new(false),
            startup_stop: AtomicBool::new(false),
            startup_status: Mutex::new("loading keys".to_owned()),
            startup_error: Mutex::new(None),
            startup_thread: Mutex::new(None),
        }
    }

    fn disk(&self, id: DiskId) -> Result<Arc<Disk>> {
        self.disks
            .get(id.get() as usize)
            .cloned()
            .ok_or_else(|| Error::new(format!("no disk {id} here")))
    }

    fn initialize(&self, process: &Arc<Process>) -> Result<()> {
        self.config.validate()?;
        let en_id = EnId::new(process.entry.number());
        loop {
            if self.startup_stop.load(Ordering::Acquire) {
                return Err(Error::new("EN startup stopped"));
            }
            *self.startup_status.lock().expect("startup status") =
                "loading current EN key".to_owned();
            match self._daily_cache.get_en(process, en_id, TimeSec::now()) {
                Ok(_) => break,
                Err(error) => {
                    *self.startup_status.lock().expect("startup status") =
                        format!("waiting for current EN key: {error}");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }

        *self.startup_status.lock().expect("startup status") = "replaying journals".to_owned();
        for (index, disk) in self.disks.iter().enumerate() {
            if self.startup_stop.load(Ordering::Acquire) {
                return Err(Error::new("EN startup stopped"));
            }
            disk.recover(
                process,
                DiskId::new(index as u16),
                self.config.journal_file_bytes,
                self.config.checkpoint_count,
            )?;
        }

        let mut sn_ids = HashSet::new();
        for disk in &self.disks {
            sn_ids.extend(disk.sn_ids());
        }
        let current = TimeSec::now();
        for sn_id in sn_ids {
            loop {
                if self.startup_stop.load(Ordering::Acquire) {
                    return Err(Error::new("EN startup stopped"));
                }
                *self.startup_status.lock().expect("startup status") =
                    format!("loading current SN key for {sn_id}");
                match self._daily_cache.get(process, sn_id, current) {
                    Ok(_) => break,
                    Err(error) => {
                        *self.startup_status.lock().expect("startup status") =
                            format!("waiting for current SN key for {sn_id}: {error}");
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Service for EN {
    fn start_with_process(self: &Arc<Self>, process: &Arc<Process>) -> Result<()> {
        let en = Arc::clone(self);
        let process = Arc::clone(process);
        let worker =
            thread::Builder::new().name("en-recovery".to_owned()).spawn(move || {
                match en.initialize(&process) {
                    Ok(()) if !en.startup_stop.load(Ordering::Acquire) => {
                        *en.startup_status.lock().expect("startup status") = "ready".to_owned();
                        en.accepting.store(true, Ordering::Release);
                    }
                    Ok(()) => {}
                    Err(_) if en.startup_stop.load(Ordering::Acquire) => {}
                    Err(error) => {
                        let message = error.to_string();
                        *en.startup_status.lock().expect("startup status") = "failed".to_owned();
                        *en.startup_error.lock().expect("startup error") = Some(message.clone());
                        logw!("EN startup failed", "{message}");
                    }
                }
            })?;
        *self.startup_thread.lock().expect("startup thread") = Some(worker);
        Ok(())
    }

    fn stop(&self, _process: &Process) -> Result<()> {
        self.startup_stop.store(true, Ordering::Release);
        if let Some(worker) = self.startup_thread.lock().expect("startup thread").take() {
            worker.join().map_err(|_| Error::new("EN startup thread panicked"))?;
        }
        Ok(())
    }

    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    fn defers(&self, op: Op) -> bool {
        matches!(
            op,
            Op::AppendExtent
                | Op::WriteAppend
                | Op::SoftDeleteExtentInstance
                | Op::DeleteExtentInstance
                | Op::ForceSealExtent
                | Op::WriteDurable
        )
    }

    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::AppendExtent | Op::WriteAppend | Op::Ping | Op::Status | Op::Shutdown | Op::Sites
        )
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        if !self.accepting.load(Ordering::Acquire) {
            let detail = self
                .startup_error
                .lock()
                .expect("startup error")
                .as_ref()
                .map(|error| format!("EN is not accepting appends yet: {error}"))
                .unwrap_or_else(|| "EN is not accepting appends yet".to_owned());
            return reply.send(Resp::error(Status::Locked, detail));
        }
        match request.op() {
            Op::SoftDeleteExtentInstance | Op::DeleteExtentInstance => {
                self.delete_wait(process, &request, reply)
            }
            Op::AppendExtent => self.append(process, request, reply),
            Op::WriteAppend => self.write_wait(process, request, reply),
            Op::WriteDurable => self.write_durable(process, request, reply),
            Op::ReadExtent => reply.send(self.read(process, &request)),
            Op::ForceSealExtent => self.seal_wait(process, &request, reply),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, process: &Process) -> Vec<String> {
        let mut lines =
            vec![format!("startup {}", self.startup_status.lock().expect("startup status"))];
        for (i, disk) in self.disks.iter().enumerate() {
            lines.push(format!("disk {i}"));
            for line in disk.status(process, &self.config) {
                lines.push(format!("  {line}"));
            }
        }
        lines
    }
}

impl EN {
    fn write_durable(&self, process: &Process, request: Req, reply: Reply) {
        let (mut payload, rest) =
            match structure(&request.args(), Op::WriteDurable, "an EN durable update") {
                Ok(value) => value,
                Err(error) => return reply.send(bad(error)),
            };
        let en = match payload.en_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let disk_id = match payload.disk_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let sn = match payload.sn_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let extent = match payload.extent_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let created = match payload.time_sec() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let mark = Mark {
            logical: match payload.eloff() {
                Ok(value) => value,
                Err(error) => return reply.send(bad(error)),
            },
            records: match payload.eroff() {
                Ok(value) => value,
                Err(error) => return reply.send(bad(error)),
            },
        };
        if !rest.is_empty() || payload.done().is_err() {
            return reply.send(bad("EN durable update has trailing fields"));
        }
        if en != EnId::new(process.entry.number()) {
            return reply.send(bad("EN durable update reached the wrong EN"));
        }
        match self.disk(disk_id) {
            Ok(disk) => disk.durable_wait(sn, extent, created, mark, reply),
            Err(error) => reply.send(bad(error)),
        }
    }

    fn delete_wait(&self, process: &Process, request: &Req, reply: Reply) {
        let deletion =
            match Delete::of_with_limit(request, process.cluster.max_soft_delete_retention_secs) {
                Ok(value) => value,
                Err(error) => return reply.send(bad(error)),
            };
        let Target::Instance { en, disk, sn, extent, created } = deletion.target else {
            return reply.send(bad("expected an extent instance"));
        };
        if en.number() != process.entry.number() {
            return reply.send(bad("deletion addressed to a different EN"));
        }
        match self.disk(disk) {
            Ok(disk) => disk.delete_wait(
                sn,
                extent,
                created,
                deletion.retention,
                deletion.hard,
                process.cluster.max_soft_delete_retention_secs,
                reply,
            ),
            Err(error) => reply.send(bad(error)),
        }
    }

    fn seal_wait(&self, process: &Process, request: &Req, reply: Reply) {
        let (mut payload, rest) =
            match structure(&request.args(), Op::ForceSealExtent, "a force-seal request") {
                Ok(value) => value,
                Err(error) => return reply.send(bad(error)),
            };
        let en = match payload.en_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let disk = match payload.disk_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let sn = match payload.sn_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let extent = match payload.extent_id() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        let created = match payload.time_sec() {
            Ok(value) => value,
            Err(error) => return reply.send(bad(error)),
        };
        if !rest.is_empty() || payload.done().is_err() {
            return reply.send(bad("force-seal request has trailing arguments"));
        }
        if en != EnId::new(process.entry.number()) {
            return reply.send(bad("force-seal request reached the wrong EN"));
        }
        match self.disk(disk) {
            Ok(disk) => disk.seal_wait(sn, extent, created, reply),
            Err(error) => reply.send(bad(error)),
        }
    }

    #[cfg(test)]
    fn delete_instance(&self, process: &Process, request: &Req) -> Resp {
        let deletion =
            match Delete::of_with_limit(request, process.cluster.max_soft_delete_retention_secs) {
                Ok(value) => value,
                Err(error) => return bad(error),
            };
        let Target::Instance { en, disk, sn, extent, created } = deletion.target else {
            return bad("expected an extent instance");
        };
        if en.number() != process.entry.number() {
            return bad("deletion addressed to a different EN");
        }
        let disk = match self.disk(disk) {
            Ok(disk) => disk,
            Err(error) => return bad(error),
        };
        let result = {
            let mut held = disk.held.lock().unwrap();
            held.delete_instance(
                sn,
                extent,
                created,
                deletion.retention,
                deletion.hard,
                process.cluster.max_soft_delete_retention_secs,
            )
        };
        match result {
            Err(error) => bad(error),
            Ok(()) => {
                let mut queued = disk.pending_fixed.lock().unwrap();
                let keys: Vec<_> = queued
                    .keys()
                    .filter(|(s, e, _, _)| *s == sn && *e == extent)
                    .copied()
                    .collect();
                for key in keys {
                    if let Some((_, reply)) = queued.remove(&key) {
                        reply.send(bad("extent was deleted"));
                    }
                }
                drop(queued);
                for pending in
                    disk.waiting.lock().unwrap().remove(&(sn, extent)).unwrap_or_default()
                {
                    pending.lost();
                }
                Resp::ok()
            }
        }
    }

    fn append(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        let (en, disk_id, appending) = match AppendBlock::of(&request) {
            Ok(res) => res,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return reply.send(Resp::error(
                Status::BadRequest,
                format!("append aimed at EN{en} reached EN{mine}"),
            ));
        }
        match self.disk(disk_id) {
            Ok(disk) => {
                if let Err(e) = disk.append(process, (en, disk_id), appending, reply) {
                    logw!("append failed", "disk {disk_id},{e}");
                }
            }
            Err(e) => reply.send(Resp::error(Status::BadRequest, e.to_string())),
        }
    }

    #[cfg(test)]
    fn write_append(&self, process: &Process, request: Req) -> Resp {
        let (en, disk_id, appending) = match AppendBlock::of(&request) {
            Ok(res) => res,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return Resp::error(
                Status::BadRequest,
                format!("write-append aimed at EN{en} reached EN{mine}"),
            );
        }
        match self.disk(disk_id) {
            Ok(disk) => disk.write_append(process, appending),
            Err(e) => Resp::error(Status::BadRequest, e.to_string()),
        }
    }

    fn write_wait(&self, process: &Process, request: Req, reply: Reply) {
        let (en, disk_id, scn, appending) = match AppendBlock::of_with_scn(&request) {
            Ok(res) => res,
            Err(e) => return reply.send(Resp::error(Status::BadRequest, e.to_string())),
        };
        let Some(scn) = scn else {
            return reply.send(Resp::error(Status::BadRequest, "write-append lacks primary SCN"));
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return reply.send(Resp::error(
                Status::BadRequest,
                format!("write-append aimed at EN{en} reached EN{mine}"),
            ));
        }
        match self.disk(disk_id) {
            Ok(disk) => disk.write_wait(process, appending, scn, reply),
            Err(e) => reply.send(Resp::error(Status::BadRequest, e.to_string())),
        }
    }

    fn read(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::ReadExtent, "a read-extent request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        let en = match payload.en_id() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let disk = match payload.disk_id() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let sn = match payload.sn_id() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let extent = match payload.extent_id() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let created = match payload.time_sec() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let range = match ExtentRange::read_nested(&mut payload) {
            Ok(range) => range,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let include = match read_soft_deleted(&mut payload, rest) {
            Ok(value) => value,
            Err(error) => return bad(error),
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return Resp::error(
                Status::BadRequest,
                format!("read aimed at EN{en} reached EN{mine}"),
            );
        }
        match self.disk(disk) {
            Ok(disk) => disk.read_including(process, sn, extent, created, range, include),
            Err(e) => Resp::error(Status::BadRequest, e.to_string()),
        }
    }
}

struct Counting {
    unanswered: usize,
    wanted: usize,
    written: usize,
    local_pending: bool,
    settled: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Durable,
    Lost,
}

impl Counting {
    fn new(others: usize, quorum: usize) -> Counting {
        Counting {
            unanswered: others,
            wanted: quorum,
            written: 0,
            local_pending: true,
            settled: false,
        }
    }

    fn answer(&mut self, written: bool, local: bool) -> Option<Verdict> {
        if self.settled {
            return None;
        }
        if local {
            if !self.local_pending {
                return None;
            }
            self.local_pending = false;
        } else if self.unanswered > 0 {
            self.unanswered -= 1;
        } else {
            return None;
        }
        match written {
            true => self.written += 1,
            false => {}
        }
        let verdict = if self.written >= self.wanted {
            Verdict::Durable
        } else if self.written + self.unanswered + usize::from(self.local_pending) < self.wanted {
            Verdict::Lost
        } else {
            return None;
        };
        self.settled = true;
        Some(verdict)
    }
}

struct Quorum {
    counting: Mutex<Counting>,
    pending: Arc<Pending>,
}

impl Quorum {
    fn new(others: usize, quorum: usize, pending: Arc<Pending>) -> Quorum {
        let quorum = Quorum { counting: Mutex::new(Counting::new(others, quorum)), pending };
        let counting = quorum.counting.lock().expect("quorum");
        if counting.unanswered + usize::from(counting.local_pending) < counting.wanted {
            quorum.pending.lost();
        }
        drop(counting);
        quorum
    }

    fn written(&self, written: bool) {
        self.answer(written, false);
    }

    fn local_written(&self, written: bool) {
        self.answer(written, true);
    }

    fn answer(&self, written: bool, local: bool) {
        match self.counting.lock().expect("quorum").answer(written, local) {
            Some(Verdict::Durable) => self.pending.quorum_is_in(),
            Some(Verdict::Lost) => self.pending.lost(),
            None => {}
        }
    }
}

type QuorumHook = Box<dyn FnOnce() -> Option<Mark> + Send + 'static>;
type ReplyHook = Box<dyn FnOnce(Option<Mark>) + Send + 'static>;
type PendingDone = (Reply, Option<Mark>, Option<ReplyHook>);

struct Pending {
    gates: Mutex<Gates>,
    receipt: Vec<Arg>,
    quorum: usize,
}

struct Gates {
    quorum: bool,
    whole: bool,
    reply: Option<Reply>,
    mark: Option<Mark>,
    on_quorum: Option<QuorumHook>,
    after_reply: Option<ReplyHook>,
}

impl Pending {
    fn new(
        receipt: Vec<Arg>,
        quorum: usize,
        reply: Reply,
        on_quorum: QuorumHook,
        after_reply: ReplyHook,
    ) -> Pending {
        Pending {
            gates: Mutex::new(Gates {
                quorum: false,
                whole: false,
                reply: Some(reply),
                mark: None,
                on_quorum: Some(on_quorum),
                after_reply: Some(after_reply),
            }),
            receipt,
            quorum,
        }
    }

    fn quorum_is_in(&self) {
        let done = {
            let mut gates = self.gates.lock().expect("pending");
            if gates.quorum {
                return;
            }
            gates.mark = gates.on_quorum.take().and_then(|hook| hook());
            gates.quorum = true;
            self.settle(&mut gates)
        };
        self.complete(done);
    }

    fn records_are_whole(&self) {
        let done = {
            let mut gates = self.gates.lock().expect("pending");
            gates.whole = true;
            self.settle(&mut gates)
        };
        self.complete(done);
    }

    fn settle(&self, gates: &mut Gates) -> Option<PendingDone> {
        if !(gates.quorum && gates.whole) {
            return None;
        }
        Some((gates.reply.take()?, gates.mark.take(), gates.after_reply.take()))
    }

    fn complete(&self, done: Option<PendingDone>) {
        if let Some((reply, mark, after_reply)) = done {
            reply.send(Resp::from_args(Status::Ok, self.receipt.clone()).id(0));
            if let Some(after_reply) = after_reply {
                after_reply(mark);
            }
        }
    }

    fn lost(&self) {
        let mut gates = self.gates.lock().expect("pending");
        if let Some(reply) = gates.reply.take() {
            reply.send(Resp::error(
                Status::Internal,
                format!("could not reach a quorum of {} instances", self.quorum),
            ));
        }
        gates.on_quorum = None;
        gates.after_reply = None;
    }
}

fn main() {
    if let Err(e) = server::main(|_process| {
        let config = ENConfig::load(std::path::Path::new("config/en.toml"))?;
        Ok(EN::new(config))
    }) {
        eprintln!("en: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn deleted_instance_denies_reads_and_late_appends_and_reclaims_payloads() {
        use common::time::TimeIntSec64;
        let en = EN::new(testing::en_config());
        let process = testing::process();
        append(&en, 1, b"one\n");
        let disk = en.disk(DiskId::FIRST).unwrap();
        let target = Target::Instance {
            en: EnId::FIRST,
            disk: DiskId::FIRST,
            sn: SnId::FIRST,
            extent: ExtentId::new(1),
            created: TimeSec::new(1_000_000),
        };
        let request =
            |retention, hard| Delete { target: target.clone(), retention, hard }.request().unwrap();
        assert_eq!(en.delete_instance(&process, &request(None, true)).status, Status::BadRequest);
        assert_eq!(disk.held.lock().unwrap().appends.len(), 1);
        let soft = request(Some(TimeIntSec64::MIN), false);
        assert_eq!(en.delete_instance(&process, &soft).status, Status::Ok);
        assert_eq!(en.delete_instance(&process, &soft).status, Status::Ok);
        assert_eq!(
            disk.read(
                &process,
                SnId::FIRST,
                ExtentId::new(1),
                TimeSec::new(1_000_000),
                ExtentRange::from(ERoff::ZERO)
            )
            .status,
            Status::NotFound
        );
        assert_eq!(
            disk.read_including(
                &process,
                SnId::FIRST,
                ExtentId::new(1),
                TimeSec::new(1_000_000),
                ExtentRange::from(ERoff::ZERO),
                true
            )
            .status,
            Status::Ok
        );
        assert!(disk.take(&process, &block(0, 1, b"late\n"), ROOM).is_err());
        assert_eq!(en.delete_instance(&process, &request(None, true)).status, Status::Ok);
        assert!(disk.held.lock().unwrap().appends.is_empty());
        assert_eq!(
            disk.read_including(
                &process,
                SnId::FIRST,
                ExtentId::new(1),
                TimeSec::new(1_000_000),
                ExtentRange::from(ERoff::ZERO),
                true
            )
            .status,
            Status::NotFound
        );
        assert!(disk.take(&process, &block(0, 1, b"late\n"), ROOM).is_err());
        assert_eq!(en.delete_instance(&process, &soft).status, Status::Ok);
        assert_eq!(en.delete_instance(&process, &request(None, true)).status, Status::Ok);
        assert!(disk.held.lock().unwrap().appends.is_empty());
    }

    #[test]
    fn lazy_replica_deletion_fences_first_append_and_rejects_wrong_identity() {
        use common::time::TimeIntSec64;
        let en = EN::new(testing::en_config());
        let process = testing::process();
        let target = Target::Instance {
            en: EnId::FIRST,
            disk: DiskId::FIRST,
            sn: SnId::FIRST,
            extent: ExtentId::new(1),
            created: TimeSec::new(1_000_000),
        };
        let soft = Delete {
            target: target.clone(),
            retention: Some(TimeIntSec64::new(1000)),
            hard: false,
        }
        .request()
        .unwrap();
        assert_eq!(en.delete_instance(&process, &soft).status, Status::Ok);
        let hard = Delete { target, retention: None, hard: true }.request().unwrap();
        assert_eq!(en.delete_instance(&process, &hard).status, Status::BadRequest);
        let disk = en.disk(DiskId::FIRST).unwrap();
        assert!(disk.take(&process, &block(0, 1, b"late\n"), ROOM).is_err());
        let wrong = Delete {
            target: Target::Instance {
                en: EnId::FIRST,
                disk: DiskId::FIRST,
                sn: SnId::FIRST,
                extent: ExtentId::new(1),
                created: TimeSec::new(2),
            },
            retention: Some(TimeIntSec64::MIN),
            hard: true,
        }
        .request()
        .unwrap();
        assert_eq!(en.delete_instance(&process, &wrong).status, Status::BadRequest);
        assert!(!disk.held.lock().unwrap().extents[0].deletion.deleted());
    }
    use super::*;

    #[test]
    fn disk_read_returns_without_relocking_and_refuses_interior_starts() {
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let en = EN::new(common::testing::en_config());
            append(&en, 1, b"one\n");
            let disk = en.disk(DiskId::FIRST).unwrap();
            let process = common::testing::process();
            let answer = disk.read(
                &process,
                SnId::FIRST,
                ExtentId::new(1),
                TimeSec::new(1_000_000),
                ExtentRange::from(ERoff::ZERO),
            );
            assert_eq!(answer.status, Status::Ok);
            let (covered, rest) = ExtentCovered::of_args(answer.args()).unwrap();
            assert_eq!(covered.records, ERoff::new(1));
            assert_eq!(Piece::all_of_args(rest).unwrap()[0].bytes, b"one\n");
            let answer = disk.read(
                &process,
                SnId::FIRST,
                ExtentId::new(1),
                TimeSec::new(1_000_000),
                ExtentRange {
                    start: ExtentStart::RecordThen(ERoff::ZERO, ELoff::new(1)),
                    budget: Budget::Records(1),
                },
            );
            assert_eq!(answer.status, Status::Unimplemented);
            send.send(()).unwrap();
        });
        receive.recv_timeout(std::time::Duration::from_secs(5)).expect("read must finish");
    }

    #[test]
    fn a_full_extent_replies_with_all_three_measures() {
        let en = EN::new(common::testing::en_config());
        append(&en, 1, b"one\n");
        let disk = en.disk(DiskId::FIRST).unwrap();
        let (wire, mut reader) = common::testing::link();
        let reply = Reply::new_test(Arc::new(common::server::Out::direct(wire)));
        disk.try_append(
            &common::testing::process(),
            (EnId::FIRST, DiskId::FIRST),
            block(0, 1, b"two\n"),
            reply,
        )
        .unwrap();
        let response = reader.response().unwrap();
        assert_eq!(response.status, Status::Sealed);
        let (filled, rest) = Filled::of_args(response.args()).unwrap();
        assert!(rest.is_empty());
        assert_eq!(filled.records, ERoff::new(1));
        assert_eq!(filled.logical, ELoff::new(3));
        assert_eq!(filled.compressed, block(0, 1, b"one\n").holds.compressed);
    }
    use common::framing::Framing;
    use common::testing;

    const ROOM: ECoff = ECoff::new(1 << 20);

    fn line(text: &str) -> Vec<u8> {
        format!("{text}\n").into_bytes()
    }

    fn block(sn: u64, extent: u32, bytes: &[u8]) -> AppendBlock {
        let holds = Holds::of(Framing::Lf, b"", bytes, bytes.len()).expect("a well-formed block");
        AppendBlock {
            sn: SnId::new(sn),
            extent: ExtentId::new(extent),
            created: TimeSec::new(1_000_000),
            framing: Framing::Lf,
            crc: common::crc::of(bytes),
            holds,
            instances: Vec::new(),
            logical_offset: None,
            record_offset: None,
            bytes: bytes.to_vec(),
            pieces: Vec::new(),
        }
    }

    fn append(en: &EN, extent: u32, bytes: &[u8]) -> Landed {
        let disk_id = DiskId::FIRST;
        let disk = en.disk(disk_id).unwrap();
        match disk.take(&testing::process(), &block(0, extent, bytes), ROOM).expect("an append") {
            Took::Landed(landed) => landed,
            _ => panic!("should have landed"),
        }
    }

    #[test]
    fn empty_fixed_appends_need_record_offsets_and_replicate_distinctly() {
        let primary = EN::new(testing::en_config());
        let disk = primary.disk(DiskId::FIRST).unwrap();
        let replica = EN::new(testing::en_config());
        let other = replica.disk(DiskId::FIRST).unwrap();
        let process = testing::process();
        let mut empty = block(0, 1, b"\n");
        empty.logical_offset = Some(ELoff::ZERO);
        assert!(disk.take(&process, &empty, ROOM).is_err());
        empty.record_offset = Some(ERoff::ZERO);
        assert!(matches!(disk.take(&process, &empty, ROOM), Ok(Took::Landed(_))));
        // Exact retry does not insert another record.
        assert!(matches!(disk.take(&process, &empty, ROOM), Ok(Took::Landed(_))));
        let mut next = empty.clone();
        next.record_offset = Some(ERoff::new(1));
        assert!(matches!(disk.take(&process, &next, ROOM), Ok(Took::Landed(_))));
        // Delivery can be out of order; the replica uses the primary's record offset.
        let response = other.write_append(&process, next.clone());
        assert_eq!(response.status, Status::Ok);
        let response = other.write_append(&process, empty.clone());
        assert_eq!(response.status, Status::Ok);
        let response = other.write_append(&process, next);
        assert_eq!(response.status, Status::Ok);
        let held = disk.held.lock().unwrap();
        let mut replicated = other.held.lock().unwrap();
        assert_eq!(held.extents[0].records, ERoff::new(2));
        assert_eq!(held.extents[0].logical_end, ELoff::ZERO);
        assert_eq!(replicated.extents[0].records, ERoff::new(2));
        assert_eq!(replicated.appends.len(), 2);
        assert_ne!(held.appends[0].blocks[0].sealed, held.appends[1].blocks[0].sealed);
        assert_eq!(held.appends[0].blocks[0].sealed, replicated.appends[1].blocks[0].sealed);
        // Changing the record offset alone breaks decryption.
        replicated.appends[1].blocks[0].first_record = ERoff::new(1);
        assert!(
            replicated.appends[1].blocks[0]
                .decrypt(
                    &replica._daily_cache,
                    &other.extent_cache,
                    &process,
                    &replicated.extents[0],
                    None,
                )
                .is_err()
        );
        drop(replicated);
        drop(held);
        // A following nonempty append at the same byte offset remains possible.
        let mut following = block(0, 1, b"a\n");
        following.logical_offset = Some(ELoff::ZERO);
        following.record_offset = Some(ERoff::new(2));
        assert!(matches!(disk.take(&process, &following, ROOM), Ok(Took::Landed(_))));
        let mut conflict = following.clone();
        conflict.bytes = b"b\n".to_vec();
        conflict.crc = common::crc::of(&conflict.bytes);
        assert!(disk.take(&process, &conflict, ROOM).is_err());
        following.record_offset = None;
        assert!(matches!(disk.take(&process, &following, ROOM), Ok(Took::Landed(_))));
    }

    #[test]
    fn queued_empty_appends_wake_at_the_requested_record_count() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let mut process = testing::process();
        Arc::get_mut(&mut process).unwrap().cluster.extents.seal_bytes = 4096;
        let mut later = block(0, 1, b"\n");
        later.logical_offset = Some(ELoff::ZERO);
        later.record_offset = Some(ERoff::new(1));
        disk.append(&process, (EnId::FIRST, DiskId::FIRST), later, testing::reply()).unwrap();
        assert_eq!(disk.pending_fixed.lock().unwrap().len(), 1);
        disk.append(&process, (EnId::FIRST, DiskId::FIRST), block(0, 1, b"\n"), testing::reply())
            .unwrap();
        assert!(disk.pending_fixed.lock().unwrap().is_empty());
        assert_eq!(disk.held.lock().unwrap().extents[0].records, ERoff::new(2));
    }

    #[test]
    fn payload_seek_uses_headers_without_decrypting_skipped_blocks() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let process = testing::process();
        for payload in [b"a".as_slice(), b"", b"", b"bc", b"tail"] {
            let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
            bytes.extend_from_slice(payload);
            let append = AppendBlock {
                framing: Framing::Length,
                holds: Holds::of(Framing::Length, b"", &bytes, bytes.len()).unwrap(),
                crc: common::crc::of(&bytes),
                bytes,
                ..block(0, 1, b"")
            };
            assert!(matches!(disk.take(&process, &append, ROOM).unwrap(), Took::Landed(_)));
        }
        {
            let mut held = disk.held.lock().unwrap();
            for append in &mut held.appends[..3] {
                append.blocks[0].sealed.clear(); // touching these ciphertexts must fail
            }
            held.appends[4].blocks[0].sealed.clear(); // past the requested budget too
        }
        let read = |from, offset, amount| {
            disk.read(
                &process,
                SnId::FIRST,
                ExtentId::new(1),
                TimeSec::new(1_000_000),
                ExtentRange {
                    start: ExtentStart::Payload(ERoff::new(from), offset),
                    budget: Budget::Logical(amount),
                },
            )
        };
        let answer = read(0, 1, 1);
        assert_eq!(answer.status, Status::Ok);
        let (seek, rest) = common::range::ByteSeek::of_args(answer.args()).unwrap();
        assert_eq!(seek.skipped, 1);
        assert_eq!(seek.next, ERoff::new(4));
        let pieces = Piece::all_of_args(rest).unwrap();
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].first_record, ERoff::new(3));
        assert_eq!(pieces[0].bytes, b"\x02\0\0\0bc");
        // Starting inside a payload returns only its containing block.
        assert_eq!(read(0, 2, 1).status, Status::Ok);
        assert_eq!(read(3, 1, 1).status, Status::Ok);
        // Beyond EOF needs no payload, even when every skipped ciphertext is broken.
        let answer = read(0, u64::MAX - 1, 1);
        assert_eq!(answer.status, Status::Ok);
        let (seek, rest) = common::range::ByteSeek::of_args(answer.args()).unwrap();
        assert_eq!(seek.skipped, 7);
        assert_eq!(seek.next, ERoff::new(5));
        assert!(rest.is_empty());
        // Confirm the corruption is real and fails if the requested interval overlaps it.
        assert_eq!(read(0, 0, 1).status, Status::Internal);
    }

    #[test]
    fn one_append_holds_multiple_encrypted_blocks() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let first = b"one\n";
        let second = b"two\n";
        let first_holds = Holds::of(Framing::Lf, b"", first, first.len()).unwrap();
        let second_holds = Holds::of(Framing::Lf, b"", second, second.len()).unwrap();
        let total = Holds {
            compressed: first_holds.compressed.plus(second_holds.compressed).unwrap(),
            logical: first_holds.logical.plus(second_holds.logical).unwrap(),
            records: first_holds.records.plus(second_holds.records).unwrap(),
        };
        let mut batch = block(0, 1, first);
        batch.holds = total;
        batch.pieces = vec![
            common::AppendPiece { holds: first_holds, crc: 0, bytes: first.to_vec() },
            common::AppendPiece { holds: second_holds, crc: 0, bytes: second.to_vec() },
        ];
        assert!(matches!(disk.take(&testing::process(), &batch, ROOM), Ok(Took::Landed(_))));
        let held = disk.held.lock().unwrap();
        assert_eq!(held.appends.len(), 1);
        assert_eq!(held.appends[0].blocks.len(), 2);
    }

    #[test]
    fn multi_block_append_is_all_or_nothing_when_extent_is_full() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let first = b"one\n";
        let second = b"two\n";
        let first_holds = Holds::of(Framing::Lf, b"", first, first.len()).unwrap();
        let second_holds = Holds::of(Framing::Lf, b"", second, second.len()).unwrap();
        let batch = AppendBlock {
            holds: Holds {
                compressed: first_holds.compressed.plus(second_holds.compressed).unwrap(),
                logical: first_holds.logical.plus(second_holds.logical).unwrap(),
                records: first_holds.records.plus(second_holds.records).unwrap(),
            },
            pieces: vec![
                common::AppendPiece { holds: first_holds, crc: 0, bytes: first.to_vec() },
                common::AppendPiece { holds: second_holds, crc: 0, bytes: second.to_vec() },
            ],
            ..block(0, 2, first)
        };
        assert!(matches!(
            disk.take(&testing::process(), &batch, ECoff::new(1)),
            Ok(Took::Sealed { .. })
        ));
        assert!(disk.held.lock().unwrap().appends.is_empty());
    }

    #[test]
    fn offsets_follow_one_another() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("hello")).offset, ECoff::new(0));
        // physical stored length = APPEND_OVERHEAD(44) + body(6) = 50.
        assert_eq!(append(&en, 1, &line("there")).offset, ECoff::new(50));
    }

    #[test]
    fn extents_do_not_share_offsets() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("one")).offset, ECoff::new(0));
        assert_eq!(append(&en, 2, &line("two")).offset, ECoff::new(0));
        // physical stored length = APPEND_OVERHEAD(44) + body(4) = 48.
        assert_eq!(append(&en, 1, &line("more")).offset, ECoff::new(48));
    }

    #[test]
    fn extents_are_named_by_its_sn_too() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("one")).offset, ECoff::new(0));
        let disk = en.disk(DiskId::FIRST).unwrap();
        match disk
            .take(
                &testing::process(),
                &AppendBlock { sn: SnId::new(1), ..block(0, 1, &line("two")) },
                ROOM,
            )
            .unwrap()
        {
            Took::Landed(landed) => assert_eq!(landed.offset, ECoff::new(0)),
            _ => panic!("should have landed"),
        }
    }

    #[test]
    fn an_append_is_told_the_records_it_closed() {
        let en = EN::new(testing::en_config());
        let first = append(&en, 1, &line("one"));
        assert_eq!((first.first_record, first.records), (ERoff::new(0), ERoff::new(1)));
        let pair = append(&en, 1, b"two\nthree\n");
        assert_eq!((pair.first_record, pair.records), (ERoff::new(1), ERoff::new(2)));
    }

    #[test]
    fn a_block_completing_no_record_is_carried_anyway() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", 10).unwrap();
        match disk.take(
            &testing::process(),
            &AppendBlock {
                holds: opened,
                logical_offset: Some(ELoff::ZERO),
                ..block(0, 1, b"half a rec")
            },
            ROOM,
        ) {
            Ok(Took::Landed(landed)) => assert_eq!(landed.records, ERoff::new(0)),
            other => panic!("expected landed, got {other:?}"),
        }
    }

    #[test]
    fn an_open_record_keeps_an_extent_from_sealing() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let cap = ECoff::new(100);
        let opened = Holds::of(Framing::Lf, b"", b"half", 4).unwrap();
        let first = AppendBlock {
            holds: opened,
            logical_offset: Some(ELoff::ZERO),
            ..block(0, 1, b"half")
        };
        assert!(matches!(disk.take(&testing::process(), &first, cap), Ok(Took::Landed(_))));

        let closed = Holds::of(Framing::Lf, b"half", b" and half\n", 10).unwrap();
        let mut second = block(0, 1, b" and half\n");
        second.holds = closed;
        second.logical_offset = Some(first.holds.logical);
        assert!(matches!(disk.take(&testing::process(), &second, cap), Ok(Took::Landed(_))));
        assert!(matches!(
            disk.take(&testing::process(), &block(0, 1, b"x\n"), cap),
            Ok(Took::Sealed { .. })
        ));
    }

    #[test]
    fn a_full_extent_seals_instead_of_taking_the_block() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let cap = ECoff::new(60);
        assert!(matches!(
            disk.take(&testing::process(), &block(0, 1, b"one\n"), cap),
            Ok(Took::Landed(_))
        ));
        match disk.take(&testing::process(), &block(0, 1, b"two\n"), cap) {
            Ok(Took::Sealed { came_to, .. }) => {
                assert_eq!(came_to.records, ERoff::new(1));
            }
            other => panic!("should have sealed, not {:?}", other),
        }
    }

    #[test]
    fn a_compressed_block_is_stored_and_returned_untouched() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let records = (0..40).flat_map(|n| line(&format!("record {n}"))).collect::<Vec<u8>>();
        let stored = common::block::squeeze(common::config::Compress::Lz4, &records)
            .expect("that should compress");
        let holds = Holds::of(Framing::Lf, b"", &records, stored.len()).unwrap();

        let mut compressed = block(0, 1, b"");
        compressed.holds = holds;
        compressed.crc = common::crc::of(&records);
        compressed.bytes = stored.clone();
        assert!(matches!(disk.take(&testing::process(), &compressed, ROOM), Ok(Took::Landed(_))));

        let held = disk.held.lock().unwrap();
        assert_eq!(
            held.read(
                &en._daily_cache,
                &disk.extent_cache,
                &testing::process(),
                SnId::new(0),
                ExtentId::new(1)
            ),
            stored
        );
    }

    #[test]
    fn a_read_returns_the_block_holding_the_record() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, b"one\ntwo\n");
        append(&en, 1, b"three\nfour\nfive\n");

        let one = |record| {
            disk.range(
                &testing::process(),
                SnId::new(0),
                ExtentId::new(1),
                ERoff::new(record),
                Budget::Records(1),
            )
            .unwrap()
            .map(|ranged| ranged.pieces[0].bytes.clone())
        };
        assert_eq!(one(0).unwrap(), b"one\ntwo\n");
        assert_eq!(one(2).unwrap(), b"three\nfour\nfive\n");
    }

    #[test]
    fn a_budget_is_honoured_in_whichever_space_it_names() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, b"one\ntwo\n");
        append(&en, 1, b"three\nfour\nfive\n");

        let ranged = |budget| {
            disk.range(&testing::process(), SnId::new(0), ExtentId::new(1), ERoff::new(0), budget)
                .unwrap()
                .unwrap()
        };

        let one = ranged(Budget::Records(1));
        assert_eq!(one.pieces.len(), 1);
        let both = ranged(Budget::Records(5));
        assert_eq!(both.pieces.len(), 2);
    }

    #[test]
    fn a_budget_smaller_than_a_block_still_gets_a_block() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, &b"x".repeat(100));
        let ranged = disk
            .range(
                &testing::process(),
                SnId::new(0),
                ExtentId::new(1),
                ERoff::new(0),
                Budget::Records(0),
            )
            .unwrap()
            .unwrap();
        assert_eq!(ranged.pieces.len(), 1);
    }

    #[test]
    fn a_range_does_not_stop_in_the_middle_of_a_record() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", 10).unwrap();
        disk.take(
            &testing::process(),
            &AppendBlock {
                holds: opened,
                logical_offset: Some(ELoff::ZERO),
                ..block(0, 1, b"half a rec")
            },
            ROOM,
        )
        .unwrap();
        let closed = Holds::of(Framing::Lf, b"half a rec", b"ord\n", 4).unwrap();
        let mut second = block(0, 1, b"ord\n");
        second.holds = closed;
        second.logical_offset = Some(opened.logical);
        disk.take(&testing::process(), &second, ROOM).unwrap();

        let ranged = disk
            .range(
                &testing::process(),
                SnId::new(0),
                ExtentId::new(1),
                ERoff::new(0),
                Budget::Records(1),
            )
            .unwrap()
            .unwrap();
        assert_eq!(ranged.pieces.len(), 2);
    }

    #[test]
    fn a_record_spanning_blocks_is_a_flag_here_and_bytes_at_the_writer() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let opened = Holds::of(Framing::Lf, b"", b"start", 5).unwrap();
        disk.take(
            &testing::process(),
            &AppendBlock {
                holds: opened,
                logical_offset: Some(ELoff::ZERO),
                ..block(0, 1, b"start")
            },
            ROOM,
        )
        .unwrap();
        assert!(en.disk(DiskId::FIRST).unwrap().held.lock().unwrap().extents[0].open_record);
    }

    #[test]
    fn a_written_append_keeps_the_offset_it_was_given() {
        let en = EN::new(testing::en_config());
        let disk_id = DiskId::FIRST;
        let mut block = block(0, 1, &line("late"));
        block.logical_offset = Some(ELoff::ZERO);
        block.record_offset = Some(ERoff::ZERO);
        let request = block.request(EnId::new(0), disk_id, Op::WriteAppend);
        let response = en.write_append(&testing::process(), request);
        assert_eq!(response.status, Status::Ok);
    }

    #[test]
    fn a_crc_is_kept_and_not_judged() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, &line("checksummed"));
        let mut wrong = block(0, 1, &line("also fine"));
        wrong.crc = 0;
        assert!(matches!(disk.take(&testing::process(), &wrong, ROOM), Ok(Took::Landed(_))));
    }

    #[test]
    fn primary_broadcasts_to_the_instances_it_was_given() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let instances = vec![(EnId::new(0), DiskId::new(0)), (EnId::new(1), DiskId::new(0))];
        let mut appending = block(0, 1, &line("broadcast"));
        appending.instances = instances;
        let process = testing::process();
        disk.append(&process, (EnId::new(0), DiskId::FIRST), appending, testing::reply()).unwrap();
    }

    #[test]
    fn the_same_bytes_at_the_same_offset_are_a_retry_not_a_clash() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let mut held = disk.held.lock().unwrap();
        let mut same = block(0, 1, b"once\n");
        same.logical_offset = Some(ELoff::ZERO);
        same.record_offset = Some(ERoff::ZERO);
        let result = held.write_append(
            &en._daily_cache,
            &disk.extent_cache,
            &testing::process(),
            &same,
            None,
            Scn::new(0),
        );
        assert!(matches!(result, Ok(Written::Stored)));
        let result = held.write_append(
            &en._daily_cache,
            &disk.extent_cache,
            &testing::process(),
            &same,
            None,
            Scn::new(0),
        );
        assert!(matches!(result, Ok(Written::Already)));
    }

    #[test]
    fn a_truncation_seals_so_a_freed_offset_is_never_reused() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, &line("keep"));
        let mut held = disk.held.lock().unwrap();
        let end = ELoff::of_number(held.extents[0].logical_end.number()).unwrap();
        held.truncate(
            &en._daily_cache,
            &disk.extent_cache,
            &testing::process(),
            SnId::new(0),
            ExtentId::new(1),
            end,
        )
        .unwrap();
    }

    #[test]
    fn a_truncation_that_would_leave_a_record_open_is_refused() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", 10).unwrap();
        match disk
            .take(
                &testing::process(),
                &AppendBlock {
                    holds: opened,
                    logical_offset: Some(ELoff::ZERO),
                    ..block(0, 1, b"half a rec")
                },
                ROOM,
            )
            .unwrap()
        {
            Took::Landed(_landed) => (),
            _ => panic!("should have landed"),
        };
        let mut held = disk.held.lock().unwrap();
        let mid = held.extents[0].logical_end;
        assert!(
            held.truncate(
                &en._daily_cache,
                &disk.extent_cache,
                &testing::process(),
                SnId::new(0),
                ExtentId::new(1),
                mid
            )
            .is_err()
        );
    }

    #[test]
    fn replica_set_must_be_distinct() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let instances = vec![(EnId::new(0), DiskId::new(0)), (EnId::new(0), DiskId::new(0))];
        let mut appending = block(0, 1, &line("x"));
        appending.instances = instances;
        disk.append(
            &testing::process(),
            (EnId::new(0), DiskId::FIRST),
            appending,
            testing::reply(),
        )
        .unwrap();
    }
}
