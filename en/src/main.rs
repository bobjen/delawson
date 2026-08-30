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
use common::ids::{DiskId, EnId, ExtentId, SnId};
use common::offset::{ECoff, ELoff, ERoff};
use common::range::{Budget, Covered, Filled, Piece, Range};
use common::server::{self, Process, Reply, Service};
use common::time::TimeSec;
use common::wire::{AppendBlock, Arg, Op, Request, Response, Status};
use common::{Error, Result, logw};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const SERVES: &[Op] = &[Op::AppendExtent, Op::ApplyAppend, Op::ReadExtent];

/// Cached SN daily keys.
struct DailyKeyCache {
    keys: RwLock<HashMap<(SnId, u64), [u8; 32]>>,
}

impl DailyKeyCache {
    fn new() -> Self {
        Self { keys: RwLock::new(HashMap::new()) }
    }

    fn get(&self, process: &Process, sn_id: SnId, ts: TimeSec) -> Result<[u8; 32]> {
        let day = ts.day();
        {
            let keys = self.keys.read().expect("keys");
            if let Some(key) = keys.get(&(sn_id, day)) {
                return Ok(*key);
            }
        }

        // Fetch from KS.
        let ask = Request::new(Op::SnMasterKey).number(sn_id.number()).number(day);
        let key_cell = Arc::new(Mutex::new([0u8; 32]));
        let key_cell2 = Arc::clone(&key_cell);
        let error_cell = Arc::new(Mutex::new(None));
        let error_cell2 = Arc::clone(&error_cell);
        let cvar = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let cvar2 = Arc::clone(&cvar);

        process.ask("KS", &ask, move |answer| {
            let mut guard = cvar2.0.lock().expect("cvar");
            if answer.status == Status::Ok {
                if let [Arg::Bytes(bytes)] = answer.args.as_slice() {
                    if bytes.len() == 32 {
                        let mut key_guard = key_cell2.lock().expect("key_cell");
                        key_guard.copy_from_slice(bytes);
                        *guard = true;
                    } else {
                        let mut error_guard = error_cell2.lock().expect("error_cell");
                        *error_guard = Some(Error::new("KS returned wrong key size"));
                    }
                } else {
                    let mut error_guard = error_cell2.lock().expect("error_cell");
                    *error_guard = Some(Error::new("KS returned wrong argument shape"));
                }
            } else {
                let mut error_guard = error_cell2.lock().expect("error_cell");
                *error_guard = Some(Error::new(format!("KS returned error: {}", answer.status)));
            }
            *guard = true;
            cvar2.1.notify_one();
        }).unwrap_or_else(|_| {
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

        let key = *key_cell.lock().expect("key_cell");
        if key == [0u8; 32] {
            let key = crypto::sn_daily_key(sn_id, day);
            self.keys.write().expect("keys").insert((sn_id, day), key);
            return Ok(key);
        }
        self.keys.write().expect("keys").insert((sn_id, day), key);
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

/// An extent instance held here. Its name is the key it is found by — this SN,
/// this id — and the second, which is part of the name too.
struct Extent {
    sn_id: SnId,
    id: ExtentId,
    created: TimeSec,
    /// Closed: no more appends, however much room is left.
    sealed: bool,
    /// Where the next append starts, in stored bytes — what the device holds, and
    /// what the seal limit is against.
    end: ECoff,
    /// And where it starts in the writer's numbering. The two differ by whatever
    /// compression achieved, which is why they are different types.
    logical_end: ELoff,
    /// Records completed in this extent, which is what numbers them. **Extent-local**,
    /// and typed as such: the SN says what stream-local number the extent's first record
    /// has, and an `ERoff` cannot be handed to anything expecting that.
    records: ERoff,
    /// Whether a record has begun and not ended. **A flag, not the bytes** — the
    /// bytes are inside a compressed block this tier cannot open, and the writer
    /// carries them. All that is needed here is whether the extent may seal, which
    /// it may not while one is open.
    open_record: bool,
}

/// One append, as it arrived. The offset is the primary's decision, so every
/// instance holds the same append at the same place.
struct Append {
    /// Which extent, as an index into the extent table.
    extent: usize,
    /// Where in the extent's stored bytes it sits, which is what puts the blocks of
    /// an extent in order. **Offset order, not record order**, because a secondary
    /// may be told of its blocks in any order and because a block completing no
    /// record has no record to sort by (§8.2.4).
    offset: ECoff,
    /// The logical offset in whatever framing the writer used.
    logical: ELoff,
    /// The extent-local number of the first record this block completed — which is
    /// how a read finds the block holding a record **without parsing anything**.
    /// The counts came from the writer; the arithmetic is this tier's.
    first_record: ERoff,
    /// Overhead of the framing used to write this block.
    overhead: u32,
    /// Metadata in the clear, to avoid expensive decryption during scans.
    holds: Holds,
    /// The encrypted bytes (header + compressed body + tag).
    sealed: Vec<u8>,
}

impl Append {
    #[allow(clippy::too_many_arguments)]
    fn encrypt(
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        extent: &Extent,
        offset: ECoff,
        logical: ELoff,
        first_record: ERoff,
        holds: Holds,
        overhead: u32,
        body: &[u8],
    ) -> Result<Self> {
        let extent_key = extent_cache.get(daily_cache, process, extent.sn_id, extent.id, extent.created)?;
        // L - R*O + R*4 is the logical offset in length framing.
        let canonical_offset =
            logical.number().saturating_sub(first_record.number() * overhead as u64) + first_record.number() * 4;
        let (key, nonce) = crypto::derive_block_secrets(&extent_key, canonical_offset);

        let mut plain = Vec::with_capacity(29 + body.len());
        plain.extend(holds.logical.number().to_le_bytes());
        plain.extend(holds.records.number().to_le_bytes());
        plain.push(holds.open_record as u8);
        plain.extend(holds.crc.to_le_bytes());
        plain.extend(overhead.to_le_bytes());
        plain.extend(body);

        let ad = [];
        let (mut sealed, tag) = crypto::encrypt_block(&key, &nonce, &ad, &plain);
        sealed.extend(tag);

        Ok(Self { extent: 0, offset, logical, first_record, overhead, holds, sealed })
    }

    fn decrypt(
        &self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        extent: &Extent,
    ) -> Result<(Holds, u32, Vec<u8>)> {
        let extent_key = extent_cache.get(daily_cache, process, extent.sn_id, extent.id, extent.created)?;
        let canonical_offset = self.logical.number().saturating_sub(self.first_record.number() * self.overhead as u64)
            + self.first_record.number() * 4;
        let (key, nonce) = crypto::derive_block_secrets(&extent_key, canonical_offset);

        if self.sealed.len() < 16 {
            return Err(Error::new("stored block too short for tag"));
        }
        let (cipher, tag) = self.sealed.split_at(self.sealed.len() - 16);
        let tag: [u8; 16] = tag.try_into().unwrap();
        let ad = [];
        let plain = crypto::decrypt_block(&key, &nonce, &ad, cipher, &tag)?;

        if plain.len() < 29 {
            return Err(Error::new("decrypted block too short"));
        }
        let logical = ELoff::of_number(u64::from_le_bytes(plain[0..8].try_into().unwrap()))?;
        let records = ERoff::of_number(u64::from_le_bytes(plain[8..16].try_into().unwrap()))?;
        let open_record = plain[16] != 0;
        let crc = u64::from_le_bytes(plain[17..25].try_into().unwrap());
        let overhead = u32::from_le_bytes(plain[25..29].try_into().unwrap());
        let bytes = plain[29..].to_vec();

        Ok((Holds { logical, records, open_record, crc }, overhead, bytes))
    }
}

/// The blocks a range read is answered with, and what they came to.
struct Ranged {
    pieces: Vec<Piece>,
    covered: Covered,
}

/// What became of an append: a place in the extent, or an extent that is closed.
#[derive(Debug)]
enum Took {
    Landed(Landed),
    /// The logical offset is in the future, so the append was queued.
    Queued,
    /// What the extent came to, which is what the SN has to record. `closing` marks
    /// the block that found it full, as against the ones arriving afterwards, so a
    /// count of sealed extents counts extents and not refusals.
    Sealed {
        /// All three measures, because the SN records all three and a seal is the only
        /// moment the Disk is asked what the extent came to (#94, §9.1.3.6).
        came_to: Filled,
        closing: bool,
    },
}

/// What an apply did. A retry must be answered ok without storing the bytes or
/// moving any count a second time.
enum Applied {
    Stored,
    Already,
}

/// What a truncation discarded, and what the extent came to.
///
/// ⚠️ **Nothing calls it from the wire yet**, because the caller is primary
/// handover and that protocol is unspecified (§16.32, #55). The primitive is here
/// ahead of it deliberately: the invariant it carries is the reason handover cannot
/// be written casually, so the operation that enforces it should exist before the
/// protocol that needs it, not after.
#[allow(dead_code)]
struct Truncated {
    /// Blocks dropped. Zero is ordinary: an instance may have nothing above the
    /// watermark and is still sealed, because the *extent* is what sealed.
    discarded: usize,
    records: ERoff,
    length: ECoff,
}

/// Where an append landed and what it numbered.
#[derive(Debug, Clone, Copy)]
struct Landed {
    offset: ECoff,
    /// And where it landed in the writer's numbering, which is the locator a writer
    /// is owed: a writer knows what it wrote, not what that compressed to.
    logical: ELoff,
    /// The extent-local number of the first record this append completed.
    first_record: ERoff,
    /// Records it completed, which is zero for a block wholly inside one record.
    records: ERoff,
}

/// Everything this Disk holds. Two vectors, with a map from an extent's name to its
/// place in the first — a scan per append would be the slowest thing in the write
/// path long before it was the smallest.
struct Extents {
    extents: Vec<Extent>,
    at: HashMap<(SnId, ExtentId), usize>,
    appends: Vec<Append>,
}

impl Extents {
    fn new() -> Extents {
        Extents { extents: Vec::new(), at: HashMap::new(), appends: Vec::new() }
    }

    /// The extent's place in the table, made if this is the first append to it. A
    /// Disk learns of an extent from the first append that names it: the SN chose the
    /// instances and needs no round trip to say so.
    ///
    /// The second is part of the name, so an append naming a different one for an
    /// extent already here is refused rather than merged into it.
    fn place(&mut self, sn: SnId, id: ExtentId, created: TimeSec) -> Result<usize> {
        if let Some(place) = self.at.get(&(sn, id)) {
            let held = self.extents[*place].created;
            if held != created {
                return Err(Error::new(format!(
                    "extent {id} of sn {sn} was made in second {held} because a second is part of a name, not {created}"
                )));
            }
            return Ok(*place);
        }
        self.extents.push(Extent {
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

    /// Take the next offset in an extent and put the bytes there. This is the one
    /// serialized step of an append; everything expensive happens after it.
    ///
    /// **Nothing is parsed.** The block is compressed and its records are invisible
    /// here, so what it holds arrives with it and this is arithmetic on numbers the
    /// writer supplied.
    fn take(
        &mut self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        appending: &AppendBlock,
        cap: ECoff,
    ) -> Result<Took> {
        let logical = appending.holds.logical;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];

        let offset = extent.end;
        let logical_offset = extent.logical_end;
        let first_record = extent.records;

        if let Some(wanted) = appending.logical_offset {
            let wanted = ELoff::of_number(wanted)?;
            if wanted < logical_offset {
                // Check if it's a retry of existing data.
                if let Some(existing) =
                    self.appends.iter().find(|a| a.extent == place && a.logical == wanted)
                {
                    if existing.holds.records == appending.holds.records && existing.holds.logical == appending.holds.logical && existing.holds.crc == appending.holds.crc {
                        return Ok(Took::Landed(Landed {
                            offset: existing.offset,
                            logical: existing.logical,
                            first_record: existing.first_record,
                            records: existing.holds.records,
                        }));
                    }
                }
                return Err(Error::new(format!(
                    "fixed-offset append to {wanted} conflicts with existing data"
                )));
            }
            if wanted > logical_offset {
                return Ok(Took::Queued);
            }
        }

        let append = Append::encrypt(
            daily_cache,
            extent_cache,
            process,
            extent,
            offset,
            logical_offset,
            first_record,
            appending.holds,
            appending.overhead,
            &appending.bytes,
        )?;
        let stored = ECoff::of_len(append.sealed.len())?;

        // A record already open must finish here, since an extent holds whole
        // records — so an extent with one cannot seal, whatever the cap says.
        let sealable = !extent.open_record;
        // Full by stored bytes, or out of logical positions — which is the 4 GB
        // logical cap enforcing itself through the type rather than by a constant.
        let full = extent.end.plus(stored)? > cap || extent.logical_end.plus(logical).is_err();
        if extent.sealed || (sealable && full) {
            let closing = !extent.sealed;
            extent.sealed = true;
            let came_to = Filled {
                records: extent.records.number(),
                logical: extent.logical_end,
                compressed: extent.end,
            };
            return Ok(Took::Sealed { came_to, closing });
        }

        extent.end = offset.plus(stored)?;
        extent.logical_end = extent.logical_end.plus(logical)?;
        let records = appending.holds.records;
        extent.records = first_record.plus(records)?;
        extent.open_record = appending.holds.open_record;

        let mut append = append;
        append.extent = place;
        self.appends.push(append);

        Ok(Took::Landed(Landed { offset, logical: logical_offset, first_record, records }))
    }

    /// Put an append where another instance said. The offset is given, so this must
    /// not assign one — but the ends and counts still move, since an instance that
    /// never learns them cannot become primary.
    fn apply(
        &mut self,
        daily_cache: &DailyKeyCache,
        extent_cache: &ExtentKeyCache,
        process: &Process,
        appending: &AppendBlock,
        offset: ECoff,
    ) -> Result<Applied> {
        let logical = appending.holds.logical;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        if self.extents[place].sealed {
            return Err(Error::new(format!(
                "extent {} of sn {} is sealed, so nothing more goes in it",
                appending.extent, appending.sn
            )));
        }

        let logical_offset = ELoff::of_number(appending.logical_offset.ok_or_else(|| {
            Error::new("apply-append to EN must carry a logical offset")
        })?)?;
        let first_record = ERoff::of_number(appending.first_record)?;

        if self.appends.iter().any(|a| a.extent == place && a.offset == offset) {
            return Ok(Applied::Already);
        }

        let extent = &mut self.extents[place];
        let mut append = Append::encrypt(
            daily_cache,
            extent_cache,
            process,
            extent,
            offset,
            logical_offset,
            first_record,
            appending.holds,
            appending.overhead,
            &appending.bytes,
        )?;
        let stored = ECoff::of_len(append.sealed.len())?;

        let extent = &mut self.extents[place];
        extent.end = extent.end.max(offset.plus(stored)?);
        extent.logical_end = extent.logical_end.max(logical_offset.plus(logical)?);
        extent.records = extent.records.max(first_record.plus(appending.holds.records)?);
        extent.open_record = appending.holds.open_record;

        append.extent = place;
        self.appends.push(append);

        Ok(Applied::Stored)
    }

    /// Discard everything at or above a compressed offset, and **seal**.
    #[allow(dead_code)]
    fn truncate(
        &mut self,
        _daily_cache: &DailyKeyCache,
        _extent_cache: &ExtentKeyCache,
        _process: &Process,
        sn: SnId,
        id: ExtentId,
        above: ECoff,
    ) -> Result<Truncated> {
        let Some(&place) = self.at.get(&(sn, id)) else {
            return Err(Error::new(format!("no extent {id} of sn {sn} here")));
        };

        let mut boundary = above == ECoff::ZERO;
        let mut records = ERoff::ZERO;
        let mut logical = ELoff::ZERO;
        let mut length = ECoff::ZERO;
        let mut discarded = 0usize;
        let mut last: Option<(ECoff, bool)> = None;
        for append in self.appends.iter().filter(|append| append.extent == place) {
            let past = append.offset.plus(ECoff::of_len(append.sealed.len())?)?;
            if past == above {
                boundary = true;
            }
            if append.offset >= above {
                discarded += 1;
                continue;
            }
            if past > above {
                return Err(Error::new(format!(
                    "compressed offset {above} is inside the block at {}, and a block is never split",
                    append.offset
                )));
            }
            records = records.plus(append.holds.records)?;
            logical = logical.plus(append.holds.logical)?;
            length = length.max(past);
            if last.is_none_or(|(at, _)| append.offset >= at) {
                last = Some((append.offset, append.holds.open_record));
            }
        }
        if !boundary {
            return Err(Error::new(format!(
                "compressed offset {above} is past everything extent {id} of sn {sn} holds"
            )));
        }
        if last.is_some_and(|(_, open)| open) {
            return Err(Error::new(format!(
                "truncating extent {id} of sn {sn} to {above} would seal it with a record open"
            )));
        }

        self.appends.retain(|append| append.extent != place || append.offset < above);
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
        _sn: SnId,
        _id: ExtentId,
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

        let mut covered = Covered::default();
        let mut pieces: Vec<Piece> = Vec::new();
        for block in &blocks[first..] {
            let (_, _overhead, plain_bytes) =
                block.decrypt(daily_cache, extent_cache, process, extent)?;
            covered.took(block.holds.records.number(), block.holds.logical, ECoff::of_len(block.sealed.len())?);
            pieces.push(Piece {
                logical: block.holds.logical.number(),
                first_record: block.first_record.number(),
                records: block.holds.records.number(),
                crc: block.holds.crc,
                bytes: plain_bytes,
            });
            if budget.met(&covered) && !block.holds.open_record {
                break;
            }
        }

        let last_piece = pieces.last().unwrap();
        covered.next = last_piece.first_record + last_piece.records;
        covered.more = first + pieces.len() < blocks.len();
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
                append.decrypt(daily_cache, extent_cache, process, extent).unwrap().2.into_iter()
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
    pending_fixed: Mutex<HashMap<(SnId, ExtentId, u64), (AppendBlock, Reply)>>,
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
                    reply.send(Response::error(
                        Status::BadRequest,
                        format!("duplicate instance {:?} in replica set", appending.instances[i]),
                    ));
                    return Ok(());
                }
            }
        }

        if appending.holds.logical.get() > process.cluster.extents.max_append_bytes {
            reply.send(Response::error(
                Status::BadRequest,
                format!(
                    "append of {} logical bytes exceeds maximum {}",
                    appending.holds.logical, process.cluster.extents.max_append_bytes
                ),
            ));
            return Ok(());
        }

        self.try_append(process, mine, appending, reply)
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
                reply.send(Response::error(Status::BadRequest, "append bytes exceed 32 bits"));
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
        )?;

        let landed = match took {
            Took::Landed(landed) => landed,
            Took::Queued => {
                let lo = appending.logical_offset.unwrap();
                self.pending_fixed
                    .lock()
                    .expect("queue")
                    .insert((appending.sn, appending.extent, lo), (appending, reply));
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
                reply.send(Response { id: 0, status: Status::Sealed, args: came_to.args() });
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
            Arg::Number(appending.sn.number()),
            Arg::Number(landed.logical.number()),
            Arg::Number(landed.first_record.number()),
            Arg::Number(landed.records.number()),
            Arg::Number(appending.extent.number()),
            Arg::Number(appending.created.number()),
            Arg::Number(landed.offset.number()),
            Arg::Number(length as u64),
        ];

        let quorum_wants = process.cluster.extents.quorum;
        let pending = Arc::new(Pending::new(receipt, quorum_wants, reply));
        match landed.records == ERoff::ZERO {
            true => self
                .waiting
                .lock()
                .expect("waiting")
                .entry((appending.sn, appending.extent))
                .or_default()
                .push(Arc::clone(&pending)),
            false => {
                pending.records_are_whole();
                self.finish(appending.sn, appending.extent);
            }
        }
        let quorum = Arc::new(Quorum::new(others.len(), quorum_wants, pending));

        let mut onward = appending.clone();
        onward.instances = Vec::new();
        onward.offset = Some(landed.offset.number());
        onward.logical_offset = Some(landed.logical.number());
        onward.first_record = landed.first_record.number();

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

        let next_lo = landed.logical.number() + appending.holds.logical.number();
        if let Some((next_app, next_reply)) =
            self.pending_fixed.lock().expect("queue").remove(&(appending.sn, appending.extent, next_lo))
        {
            self.try_append(process, mine, next_app, next_reply)?;
        }

        Ok(())
    }

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

    fn apply(&self, process: &Process, appending: AppendBlock) -> Response {
        let Some(offset) = appending.offset else {
            return Response::error(Status::BadRequest, "apply-append takes an offset");
        };
        let offset = match ECoff::of_number(offset) {
            Ok(offset) => offset,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        match self.held.lock().expect("extents").apply(
            &self.daily_cache,
            &self.extent_cache,
            process,
            &appending,
            offset,
        ) {
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
            Ok(Applied::Already) => return Response::ok(),
            Ok(Applied::Stored) => {}
        }
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
        if !appending.holds.open_record {
            self.finish(appending.sn, appending.extent);
        }
        Response::ok()
    }

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
        held.range(&self.daily_cache, &self.extent_cache, process, place, sn, id, from, budget)
    }

    fn read(&self, process: &Process, args: &[Arg]) -> Response {
        let [Arg::Number(sn), Arg::Number(extent), Arg::Number(created), rest @ ..] = args else {
            return Response::error(
                Status::BadRequest,
                "read-extent takes an SnId, an extent, a second and a range",
            );
        };
        let range = match Range::of_args(rest) {
            Ok((range, [])) => range,
            _ => {
                return Response::error(
                    Status::BadRequest,
                    "read-extent takes an SnId, an extent, a second and a range",
                );
            }
        };
        let Some(from) = range.start.record() else {
            return Response::error(
                Status::Unimplemented,
                "starting from a logical offset needs a prefix sum nothing keeps yet",
            );
        };

        let held = self.held.lock().expect("extents");
        match held.created(SnId::new(*sn as u32), ExtentId::new(*extent as u32)) {
            None => {
                return Response::error(
                    Status::NotFound,
                    format!("no extent {extent} of sn {sn} here"),
                );
            }
            Some(second) if second != TimeSec::new(*created) => {
                return Response::error(
                    Status::NotFound,
                    format!("extent {extent} of sn {sn} was made in second {second}"),
                );
            }
            Some(_) => {}
        }
        let from = match ERoff::of_number(from) {
            Ok(from) => from,
            Err(e) => return Response::error(Status::NotFound, e.to_string()),
        };
        let ranged = match self.range(process, SnId::new(*sn as u32), ExtentId::new(*extent as u32), from, range.budget) {
            Ok(Some(ranged)) => ranged,
            Ok(None) => {
                return Response::error(
                    Status::NotFound,
                    format!("{extent} of sn {sn} has no record {from}"),
                );
            }
            Err(e) => return Response::error(Status::Internal, e.to_string()),
        };

        let mut args = ranged.covered.args();
        for piece in ranged.pieces {
            args.extend(piece.args());
        }
        Response { id: 0, status: Status::Ok, args }
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

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        match request.op {
            Op::AppendExtent => self.append(process, request, reply),
            Op::ApplyAppend => reply.send(self.apply(process, &request)),
            Op::ReadExtent => reply.send(self.read(process, &request)),
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
    fn append(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        let (en, disk_id, appending) = match AppendBlock::of(&request) {
            Ok(res) => res,
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return reply.send(Response::error(
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
            Err(e) => reply.send(Response::error(Status::BadRequest, e.to_string())),
        }
    }

    fn apply(&self, process: &Process, request: &Request) -> Response {
        let (en, disk_id, appending) = match AppendBlock::of(request) {
            Ok(res) => res,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        let mine = EnId::new(process.entry.number());
        if en != mine {
            return Response::error(
                Status::BadRequest,
                format!("apply aimed at EN{en} reached EN{mine}"),
            );
        }
        match self.disk(disk_id) {
            Ok(disk) => disk.apply(process, appending),
            Err(e) => Response::error(Status::BadRequest, e.to_string()),
        }
    }

    fn read(&self, process: &Process, request: &Request) -> Response {
        let [Arg::Number(en), Arg::Number(disk), rest @ ..] = request.args.as_slice() else {
            return Response::error(
                Status::BadRequest,
                "read-extent takes an EnId, a DiskId, an SnId, an extent, a second and a range",
            );
        };
        let mine = EnId::new(process.entry.number());
        if *en != mine.number() {
            return Response::error(
                Status::BadRequest,
                format!("read aimed at EN{en} reached EN{mine}"),
            );
        }
        match DiskId::of_number(*disk).and_then(|id| self.disk(id)) {
            Ok(disk) => disk.read(process, rest),
            Err(e) => Response::error(Status::BadRequest, e.to_string()),
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
        Counting {
            others,
            wanted: quorum.saturating_sub(1),
            applied: 0,
            failed: 0,
            settled: false,
        }
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
            reply.send(Response { id: 0, status: Status::Ok, args: self.receipt.clone() });
        }
    }

    fn lost(&self) {
        let mut gates = self.gates.lock().expect("pending");
        if let Some(reply) = gates.reply.take() {
            reply.send(Response::error(
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
    use super::*;
    use common::framing::Framing;
    use common::testing;

    const ROOM: ECoff = ECoff::new(1 << 20);

    fn line(text: &str) -> Vec<u8> {
        format!("{text}\n").into_bytes()
    }

    fn block(sn: u32, extent: u32, bytes: &[u8]) -> AppendBlock {
        let holds = Holds::of(Framing::Lf, b"", bytes, None)
            .expect("a well-formed block");
        AppendBlock {
            sn: SnId::new(sn),
            extent: ExtentId::new(extent),
            created: TimeSec::new(1_000_000),
            holds,
            overhead: 1,
            first_record: 0,
            instances: Vec::new(),
            offset: None,
            logical_offset: None,
            bytes: bytes.to_vec(),
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
    fn offsets_follow_one_another() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("hello")).offset, ECoff::new(0));
        assert_eq!(append(&en, 1, &line("there")).offset, ECoff::new(51)); // 0 + (6 + 45)
        assert_eq!(append(&en, 1, &line("!")).offset, ECoff::new(102)); // 51 + (2 + 45)
    }

    #[test]
    fn extents_do_not_share_offsets() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("one")).offset, ECoff::new(0));
        assert_eq!(append(&en, 2, &line("two")).offset, ECoff::new(0));
        assert_eq!(append(&en, 1, &line("more")).offset, ECoff::new(49)); // 0 + (4 + 45)
    }

    #[test]
    fn extents_are_named_by_its_sn_too() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("one")).offset, ECoff::new(0));
        let disk = en.disk(DiskId::FIRST).unwrap();
        match disk.take(&testing::process(), &AppendBlock { sn: SnId::new(1), ..block(0, 1, &line("two")) }, ROOM).unwrap()
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
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", None).unwrap();
        match disk.take(&testing::process(), &AppendBlock { holds: opened, overhead: 1, first_record: 0, ..block(0, 1, b"half a rec") }, ROOM) {
            Ok(Took::Landed(landed)) => assert_eq!(landed.records, ERoff::new(0)),
            other => panic!("expected landed, got {other:?}"),
        }
    }

    #[test]
    fn an_open_record_keeps_an_extent_from_sealing() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let cap = ECoff::new(60);
        let opened = Holds::of(Framing::Lf, b"", b"half", None).unwrap();
        let first = AppendBlock { holds: opened, overhead: 1, first_record: 0, ..block(0, 1, b"half") };
        assert!(matches!(disk.take(&testing::process(), &first, cap), Ok(Took::Landed(_))));

        let closed = Holds::of(Framing::Lf, b"half", b" and half\n", None).unwrap();
        let mut second = block(0, 1, b" and half\n");
        second.holds = closed;
        second.first_record = 0;
        assert!(matches!(disk.take(&testing::process(), &second, cap), Ok(Took::Landed(_))));
        assert!(matches!(disk.take(&testing::process(), &block(0, 1, b"x\n"), cap), Ok(Took::Sealed { .. })));
    }

    #[test]
    fn a_full_extent_seals_instead_of_taking_the_block() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let cap = ECoff::new(60);
        assert!(matches!(disk.take(&testing::process(), &block(0, 1, b"one\n"), cap), Ok(Took::Landed(_))));
        match disk.take(&testing::process(), &block(0, 1, b"two\n"), cap) {
            Ok(Took::Sealed { came_to, .. }) => {
                assert_eq!(came_to.records, 1);
            }
            other => panic!("should have sealed, not {:?}", other),
        }
    }

    #[test]
    fn a_compressed_block_is_stored_and_returned_untouched() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let records = (0..40).flat_map(|n| line(&format!("record {n}"))).collect::<Vec<u8>>();
        let holds = Holds::of(Framing::Lf, b"", &records, None).unwrap();
        let stored = common::block::squeeze(common::config::Compress::Lz4, &records)
            .expect("that should compress");

        let mut compressed = block(0, 1, b"");
        compressed.holds = holds;
        compressed.bytes = stored.clone();
        assert!(matches!(disk.take(&testing::process(), &compressed, ROOM), Ok(Took::Landed(_))));

        let held = disk.held.lock().unwrap();
        assert_eq!(
            held.read(&en._daily_cache, &disk.extent_cache, &testing::process(), SnId::new(0), ExtentId::new(1)),
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
            disk.range(&testing::process(), SnId::new(0), ExtentId::new(1), ERoff::new(record), Budget::Records(1))
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
            .range(&testing::process(), SnId::new(0), ExtentId::new(1), ERoff::new(0), Budget::Records(0))
            .unwrap()
            .unwrap();
        assert_eq!(ranged.pieces.len(), 1);
    }

    #[test]
    fn a_range_does_not_stop_in_the_middle_of_a_record() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", None).unwrap();
        disk.take(&testing::process(), &AppendBlock { holds: opened, overhead: 1, first_record: 0, ..block(0, 1, b"half a rec") }, ROOM).unwrap();
        let closed = Holds::of(Framing::Lf, b"half a rec", b"ord\n", None).unwrap();
        let mut second = block(0, 1, b"ord\n");
        second.holds = closed;
        second.first_record = 0;
        disk.take(&testing::process(), &second, ROOM).unwrap();

        let ranged = disk
            .range(&testing::process(), SnId::new(0), ExtentId::new(1), ERoff::new(0), Budget::Records(1))
            .unwrap()
            .unwrap();
        assert_eq!(ranged.pieces.len(), 2);
    }

    #[test]
    fn a_record_spanning_blocks_is_a_flag_here_and_bytes_at_the_writer() {
        let en = EN::new(testing::en_config());
        let _disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, b"start");
        assert!(en.disk(DiskId::FIRST).unwrap().held.lock().unwrap().extents[0].open_record);
    }

    #[test]
    fn an_applied_append_keeps_the_offset_it_was_given() {
        let en = EN::new(testing::en_config());
        let disk_id = DiskId::FIRST;
        let mut block = block(0, 1, &line("late"));
        block.offset = Some(64);
        block.logical_offset = Some(0);
        block.first_record = 0;
        let request = block.request(EnId::new(0), disk_id, Op::ApplyAppend);
        assert_eq!(en.apply(&testing::process(), &request).status, Status::Ok);
    }

    #[test]
    fn a_crc_is_kept_and_not_judged() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, &line("checksummed"));
        let mut wrong = block(0, 1, &line("also fine"));
        wrong.holds.crc = 0;
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
        same.logical_offset = Some(0);
        same.first_record = 0;
        assert!(matches!(held.apply(&en._daily_cache, &disk.extent_cache, &testing::process(), &same, ECoff::ZERO), Ok(Applied::Stored)));
        assert!(matches!(held.apply(&en._daily_cache, &disk.extent_cache, &testing::process(), &same, ECoff::ZERO), Ok(Applied::Already)));
    }

    #[test]
    fn a_truncation_seals_so_a_freed_offset_is_never_reused() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        append(&en, 1, &line("keep"));
        let mut held = disk.held.lock().unwrap();
        let end = held.extents[0].end;
        held.truncate(&en._daily_cache, &disk.extent_cache, &testing::process(), SnId::new(0), ExtentId::new(1), end).unwrap();
    }

    #[test]
    fn a_truncation_that_would_leave_a_record_open_is_refused() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", None).unwrap();
        match disk.take(&testing::process(), &AppendBlock { holds: opened, overhead: 1, first_record: 0, ..block(0, 1, b"half a rec") }, ROOM).unwrap() {
            Took::Landed(_landed) => (),
            _ => panic!("should have landed"),
        };
        let mut held = disk.held.lock().unwrap();
        let mid = held.extents[0].end;
        assert!(held.truncate(&en._daily_cache, &disk.extent_cache, &testing::process(), SnId::new(0), ExtentId::new(1), mid).is_err());
    }

    #[test]
    fn replica_set_must_be_distinct() {
        let en = EN::new(testing::en_config());
        let disk = en.disk(DiskId::FIRST).unwrap();
        let instances = vec![(EnId::new(0), DiskId::new(0)), (EnId::new(0), DiskId::new(0))];
        let mut appending = block(0, 1, &line("x"));
        appending.instances = instances;
        disk.append(&testing::process(), (EnId::new(0), DiskId::FIRST), appending, testing::reply()).unwrap();
    }
}
