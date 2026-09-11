//! EN, the Extent Node: the bytes on the disks.
//!
//! An extent's instances live here, and an append is what puts bytes in one. The
//! primary instance assigns the append its logical offset in the extent — the one
//! serialized step — and then broadcasts the same bytes at that fixed offset to
//! the other three instances. Broadcast rather than a chain, because a quorum only
//! buys straggler tolerance if the writes are issued at once.
//!
//! **Durable means three of the four instances have applied it**, the primary
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

use common::block::Holds;
use common::config::ENConfig;
use common::crypto;
use common::deletion::{Delete, Target, bad, read_soft_deleted};
use common::framing::Framing;
use common::ids::{DiskId, EnId, ExtentId, SnId};
use common::offset::{ECoff, ELoff, ERoff};
use common::range::{Budget, ExtentCovered, ExtentRange, ExtentStart, Filled, Piece};
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::wire::{
    AppendBlock, AppendPiece, Arg, EnReceipt, Op, Req, Resp, SnKey, Status, StructWriter, structure,
};
use common::{Error, Result, logw};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const SERVES: &[Op] = &[
    Op::SoftDeleteExtentInstance,
    Op::DeleteExtentInstance,
    Op::AppendExtent,
    Op::ApplyAppend,
    Op::ReadExtent,
    Op::ForceSealExtent,
];

/// Cached SN keys, addressed by the exact extent creation second. The exact
/// time matters because KS may install an emergency key epoch mid-day.
struct DailyKeyCache {
    keys: RwLock<BTreeMap<(SnId, TimeSec), CachedSnKey>>,
}

struct CachedSnKey {
    key: [u8; 32],
    /// Exclusive end of the key epoch, or no known end.
    until: Option<TimeSec>,
}

impl DailyKeyCache {
    fn new() -> Self {
        Self { keys: RwLock::new(BTreeMap::new()) }
    }

    fn get(&self, process: &Process, sn_id: SnId, ts: TimeSec) -> Result<[u8; 32]> {
        {
            let keys = self.keys.read().expect("keys");
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
                    match common::wire::SnKey::read(&answer.args) {
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
        let mut keys = self.keys.write().expect("keys");
        keys.insert((sn_id, ts), CachedSnKey { key, until });
        if let Some((at, key)) = response.next {
            keys.insert((sn_id, at), CachedSnKey { key, until: None });
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
    /// And where it starts in the canonical logical numbering (body + records*4).
    logical_end: ELoff,
    /// Records completed in this extent, which is what numbers them.
    records: ERoff,
    /// Whether a record has begun and not ended.
    open_record: bool,
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
    /// Metadata in the clear, to avoid expensive decryption during scans.
    holds: Holds,
    /// Independently encrypted blocks in this append. The extent does not index
    /// these yet; reads walk them in order.
    blocks: Vec<Encrypted>,
}

struct Encrypted {
    logical: ELoff,
    first_record: ERoff,
    holds: Holds,
    /// The encrypted bytes (tag + encrypted header + compressed body).
    sealed: Vec<u8>,
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
        let (key, nonce) = crypto::derive_block_secrets(&extent_key, logical.number());

        let (kind, width) = framing.wire();
        let mut plain = Vec::with_capacity(16 + body.len());
        plain.extend(crc.to_le_bytes());
        plain.extend((kind as u32).to_le_bytes());
        plain.extend(width.to_le_bytes());
        plain.extend(body);

        let ad = [];
        let (mut sealed, tag) = crypto::encrypt_block(&key, &nonce, &ad, &plain);
        sealed.extend(tag);

        Ok(Self { logical, first_record, holds, sealed })
    }

    fn decrypt(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        extent: &Extent,
    ) -> Result<(u64, Framing, Vec<u8>)> {
        let extent_key =
            extent_cache.get(daily_cache, process, extent.sn_id, extent.id, extent.created)?;
        let (key, nonce) = crypto::derive_block_secrets(&extent_key, self.logical.number());

        if self.sealed.len() < 16 {
            return Err(Error::new("stored block too short for tag"));
        }
        let (cipher, tag) = self.sealed.split_at(self.sealed.len() - 16);
        let tag: [u8; 16] = tag.try_into().unwrap();
        let ad = [];
        let plain = crypto::decrypt_block(&key, &nonce, &ad, cipher, &tag)?;

        if plain.len() < 16 {
            return Err(Error::new("decrypted block too short"));
        }
        let crc = u64::from_le_bytes(plain[0..8].try_into().unwrap());
        let kind = u32::from_le_bytes(plain[8..12].try_into().unwrap());
        let width = u32::from_le_bytes(plain[12..16].try_into().unwrap());
        let framing = Framing::of_wire(
            u8::try_from(kind).map_err(|_| Error::new(format!("unknown framing {kind}")))?,
            width,
        )?;
        let body = plain[16..].to_vec();

        Ok((crc, framing, body))
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

/// What an apply did.
enum Applied {
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

/// Where an append landed and what it numbered.
#[derive(Debug, Clone, Copy)]
struct Landed {
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
}

impl Extents {
    fn new() -> Extents {
        Extents {
            extras: common::deletion::Extras::default(),
            extents: Vec::new(),
            at: HashMap::new(),
            appends: Vec::new(),
        }
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
            // also fence an as-yet-unwritten replica against a late apply-append.
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
            self.appends.retain(|append| append.extent != place);
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
        });
        let place = self.extents.len() - 1;
        self.at.insert((sn, id), place);
        Ok(place)
    }

    fn take(
        &mut self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        appending: &AppendBlock,
        cap: ECoff,
    ) -> Result<Took> {
        let logical_size = appending.holds.logical;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];

        let offset = extent.end;
        let logical_offset = extent.logical_end;
        let first_record = extent.records;

        if let Some(wanted) = appending.logical_offset {
            if wanted < logical_offset {
                // Check if it's a retry of existing data.
                if let Some(existing) = self.appends.iter().find(|a| {
                    a.extent == place
                        && a.logical == wanted
                        && a.holds.records == appending.holds.records
                        && a.holds.logical == appending.holds.logical
                        && a.holds.compressed == appending.holds.compressed
                }) {
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
            if wanted > logical_offset {
                return Ok(Took::Queued);
            }
        }

        let stored_len = appending.holds.compressed;

        let sealable = !extent.open_record;
        let full =
            extent.end.plus(stored_len)? > cap || extent.logical_end.plus(logical_size).is_err();
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
            holds: appending.holds,
            blocks: encrypted,
        };
        self.appends.push(append);

        Ok(Took::Landed(Landed { offset, logical: logical_offset, first_record, records }))
    }

    fn apply(
        &mut self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        appending: &AppendBlock,
    ) -> Result<Applied> {
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
            .ok_or_else(|| Error::new("apply-append to EN must carry a logical offset"))?;

        let offset = self.extents[place].end;
        let first_record = self.extents[place].records;

        if self.appends.iter().any(|a| a.extent == place && a.logical == logical_offset) {
            return Ok(Applied::Already);
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
            holds: appending.holds,
            blocks: encrypted,
        };
        self.appends.push(append);

        Ok(Applied::Stored)
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

        self.appends.retain(|append| append.extent != place || append.logical < above);
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
        blocks.sort_by_key(|append| append.offset);

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
                let (crc, _framing, plain_bytes) =
                    block.decrypt(daily_cache, extent_cache, process, extent)?;
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
                if budget.met(&covered) && !is_open {
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
        found.sort_by_key(|append| append.offset);
        let extent = &self.extents[place];
        found
            .iter()
            .flat_map(|append| {
                append.blocks.iter().flat_map(|block| {
                    block.decrypt(daily_cache, extent_cache, process, extent).unwrap().2.into_iter()
                })
            })
            .collect()
    }
}

struct Disk {
    held: Mutex<Extents>,
    waiting: Mutex<Waiting>,
    appends: AtomicU64,
    bytes: AtomicU64,
    sealed: AtomicU64,
    checksummed: AtomicU64,
    daily_cache: Arc<DailyKeyCache>,
    extent_cache: Arc<ExtentKeyCache>,
    pending_fixed: Mutex<HashMap<(SnId, ExtentId, ELoff), QueuedAppend>>,
}

impl Disk {
    fn new(daily_cache: Arc<DailyKeyCache>) -> Disk {
        Disk {
            held: Mutex::new(Extents::new()),
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

    fn append(
        &self,
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

        // Holds.logical is canonical length-format size, so this limit is a
        // format-independent upper bound on the decompressed append buffer.
        if appending.holds.logical.get() > process.cluster.extents.max_append_bytes {
            reply.send(Resp::error(
                Status::BadRequest,
                format!(
                    "append of {} logical bytes exceeds maximum {}",
                    appending.holds.logical, process.cluster.extents.max_append_bytes
                ),
            ));
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

    fn force_seal(&self, sn: SnId, extent: ExtentId, created: TimeSec) -> Result<Filled> {
        let mut held = self.held.lock().expect("extents");
        let place = *held
            .at
            .get(&(sn, extent))
            .ok_or_else(|| Error::new(format!("no extent {extent} of sn {sn}")))?;
        let current = &mut held.extents[place];
        if current.created != created {
            return Err(Error::new("force-seal extent has the wrong creation second"));
        }
        if !current.deletion.readable(false) {
            return Err(Error::new("extent is deleted"));
        }
        if !current.sealed {
            current.sealed = true;
            self.sealed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Filled {
            records: current.records,
            logical: current.logical_end,
            compressed: current.end,
        })
    }

    fn try_append(
        &self,
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
        let took = self.held.lock().expect("extents").take(
            &self.daily_cache,
            &self.extent_cache,
            process,
            &appending,
            cap,
        );
        let took = match took {
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
                queue.insert((appending.sn, appending.extent, lo), (appending, reply));
                return Ok(());
            }
            Took::Sealed { came_to, closing } => {
                if closing {
                    self.sealed.fetch_add(1, Ordering::Relaxed);
                }
                reply.about(format_args!(
                    "extent {} of sn {},records {},logical {},bytes {}",
                    appending.extent,
                    appending.sn,
                    came_to.records,
                    came_to.logical,
                    came_to.compressed
                ));
                reply.send(Resp { id: 0, status: Status::Sealed, args: came_to.args() });
                return Ok(());
            }
        };
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
        let pending = Arc::new(Pending::new(receipt, quorum_wants, reply));
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

        let mut onward = appending.clone();
        onward.instances = Vec::new();
        onward.logical_offset = Some(landed.logical);

        for (en, disk) in others {
            let apply = onward.request(en, disk, Op::ApplyAppend);
            let identity = format!("EN{en}");
            let waiting = Arc::clone(&quorum);
            if let Err(e) = process.ask(&identity, &apply, move |answer| {
                waiting.applied(answer.status == Status::Ok);
            }) {
                logw!("could not reach an instance", "en {identity},{e}");
                quorum.applied(false);
            }
        }

        let next_lo = landed.logical.plus(appending.holds.logical)?;
        let next = self.pending_fixed.lock().expect("queue").remove(&(
            appending.sn,
            appending.extent,
            next_lo,
        ));
        if let Some((next_app, next_reply)) = next {
            self.try_append(process, mine, next_app, next_reply)?;
        }

        Ok(())
    }

    #[cfg(test)]
    fn take(&self, process: &Process, appending: &AppendBlock, cap: ECoff) -> Result<Took> {
        let took = self.held.lock().expect("extents").take(
            &self.daily_cache,
            &self.extent_cache,
            process,
            appending,
            cap,
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

    fn apply(&self, process: &Process, appending: AppendBlock) -> Resp {
        match self.held.lock().expect("extents").apply(
            &self.daily_cache,
            &self.extent_cache,
            process,
            &appending,
        ) {
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            Ok(Applied::Already) => return Resp::ok(),
            Ok(Applied::Stored) => {}
        }
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
        if appending.holds.records > ERoff::ZERO {
            self.finish(appending.sn, appending.extent);
        }
        Resp::ok()
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
        let ExtentStart::Record(from) = range.start else {
            return Resp::error(
                Status::Unimplemented,
                "only record-boundary starts are implemented",
            );
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
        Resp { id: 0, status: Status::Ok, args }
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
}

impl EN {
    fn new(config: ENConfig) -> EN {
        let daily_cache = Arc::new(DailyKeyCache::new());
        EN {
            config,
            disks: (0..5).map(|_| Arc::new(Disk::new(Arc::clone(&daily_cache)))).collect(),
            _daily_cache: daily_cache,
        }
    }

    fn disk(&self, id: DiskId) -> Result<Arc<Disk>> {
        self.disks
            .get(id.get() as usize)
            .cloned()
            .ok_or_else(|| Error::new(format!("no disk {id} here")))
    }
}

impl Service for EN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    fn defers(&self, op: Op) -> bool {
        matches!(op, Op::AppendExtent)
    }

    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::AppendExtent | Op::ApplyAppend | Op::Ping | Op::Status | Op::Shutdown | Op::Sites
        )
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        match request.op {
            Op::SoftDeleteExtentInstance | Op::DeleteExtentInstance => {
                reply.send(self.delete_instance(process, &request))
            }
            Op::AppendExtent => self.append(process, request, reply),
            Op::ApplyAppend => reply.send(self.apply(process, request)),
            Op::ReadExtent => reply.send(self.read(process, &request)),
            Op::ForceSealExtent => reply.send(self.force_seal(process, &request)),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, process: &Process) -> Vec<String> {
        let mut lines = Vec::new();
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
                let keys: Vec<_> =
                    queued.keys().filter(|(s, e, _)| *s == sn && *e == extent).copied().collect();
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

    fn force_seal(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args, Op::ForceSealExtent, "a force-seal request") {
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
        if !rest.is_empty() || payload.done().is_err() {
            return Resp::error(Status::BadRequest, "force-seal request has trailing arguments");
        }
        if en != EnId::new(process.entry.number()) {
            return Resp::error(Status::BadRequest, "force-seal request reached the wrong EN");
        }
        match self.disk(disk).and_then(|disk| disk.force_seal(sn, extent, created)) {
            Ok(filled) => Resp { id: 0, status: Status::Ok, args: filled.args() },
            Err(e) => Resp::error(Status::BadRequest, e.to_string()),
        }
    }

    fn apply(&self, process: &Process, request: Req) -> Resp {
        let (en, disk_id, appending) = match AppendBlock::of(&request) {
            Ok(res) => res,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return Resp::error(
                Status::BadRequest,
                format!("apply aimed at EN{en} reached EN{mine}"),
            );
        }
        match self.disk(disk_id) {
            Ok(disk) => disk.apply(process, appending),
            Err(e) => Resp::error(Status::BadRequest, e.to_string()),
        }
    }

    fn read(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args, Op::ReadExtent, "a read-extent request") {
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
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        let include = match read_soft_deleted(rest) {
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
    others: usize,
    wanted: usize,
    applied: usize,
    failed: usize,
    settled: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Durable,
    Lost,
}

impl Counting {
    fn new(others: usize, quorum: usize) -> Counting {
        Counting { others, wanted: quorum.saturating_sub(1), applied: 0, failed: 0, settled: false }
    }

    fn answer(&mut self, applied: bool) -> Option<Verdict> {
        if self.settled {
            return None;
        }
        match applied {
            true => self.applied += 1,
            false => self.failed += 1,
        }
        let verdict = if self.applied >= self.wanted {
            Verdict::Durable
        } else if !self.possible() {
            Verdict::Lost
        } else {
            return None;
        };
        self.settled = true;
        Some(verdict)
    }

    fn possible(&self) -> bool {
        self.others - self.failed >= self.wanted
    }
}

struct Quorum {
    counting: Mutex<Counting>,
    pending: Arc<Pending>,
}

impl Quorum {
    fn new(others: usize, quorum: usize, pending: Arc<Pending>) -> Quorum {
        let quorum = Quorum { counting: Mutex::new(Counting::new(others, quorum)), pending };
        if !quorum.counting.lock().expect("quorum").possible() {
            quorum.pending.lost();
        }
        quorum
    }

    fn applied(&self, applied: bool) {
        match self.counting.lock().expect("quorum").answer(applied) {
            Some(Verdict::Durable) => self.pending.quorum_is_in(),
            Some(Verdict::Lost) => self.pending.lost(),
            None => {}
        }
    }
}

struct Pending {
    gates: Mutex<Gates>,
    receipt: Vec<Arg>,
    quorum: usize,
}

struct Gates {
    quorum: bool,
    whole: bool,
    reply: Option<Reply>,
}

impl Pending {
    fn new(receipt: Vec<Arg>, quorum: usize, reply: Reply) -> Pending {
        Pending {
            gates: Mutex::new(Gates { quorum: false, whole: false, reply: Some(reply) }),
            receipt,
            quorum,
        }
    }

    fn quorum_is_in(&self) {
        let mut gates = self.gates.lock().expect("pending");
        gates.quorum = true;
        self.settle(&mut gates);
    }

    fn records_are_whole(&self) {
        let mut gates = self.gates.lock().expect("pending");
        gates.whole = true;
        self.settle(&mut gates);
    }

    fn settle(&self, gates: &mut Gates) {
        if !(gates.quorum && gates.whole) {
            return;
        }
        if let Some(reply) = gates.reply.take() {
            reply.send(Resp { id: 0, status: Status::Ok, args: self.receipt.clone() });
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
            let (covered, rest) = ExtentCovered::of_args(&answer.args).unwrap();
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
        let (filled, rest) = Filled::of_args(&response.args).unwrap();
        assert!(rest.is_empty());
        assert_eq!(filled.records, ERoff::new(1));
        assert_eq!(filled.logical, ELoff::new(7));
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
            common::wire::AppendPiece { holds: first_holds, crc: 0, bytes: first.to_vec() },
            common::wire::AppendPiece { holds: second_holds, crc: 0, bytes: second.to_vec() },
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
                common::wire::AppendPiece { holds: first_holds, crc: 0, bytes: first.to_vec() },
                common::wire::AppendPiece { holds: second_holds, crc: 0, bytes: second.to_vec() },
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
    fn an_applied_append_keeps_the_offset_it_was_given() {
        let en = EN::new(testing::en_config());
        let disk_id = DiskId::FIRST;
        let mut block = block(0, 1, &line("late"));
        block.logical_offset = Some(ELoff::ZERO);
        let request = block.request(EnId::new(0), disk_id, Op::ApplyAppend);
        assert_eq!(en.apply(&testing::process(), request).status, Status::Ok);
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
        assert!(matches!(
            held.apply(&en._daily_cache, &disk.extent_cache, &testing::process(), &same),
            Ok(Applied::Stored)
        ));
        assert!(matches!(
            held.apply(&en._daily_cache, &disk.extent_cache, &testing::process(), &same),
            Ok(Applied::Already)
        ));
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
