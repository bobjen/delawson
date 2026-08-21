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
//! **A caller's CRC is carried and stored, and not checked here.** An EN holds what
//! the writer compressed, and it never decompresses — so the logical bytes the CRC is
//! over do not exist on this machine. Checking would work today only because nothing
//! compresses yet, and would start failing on every append the moment something did.
//! The check belongs where the logical bytes come back.
//!
//! ⚠️ **Nothing is on a disk yet.** The extent table and the appends themselves are
//! two vectors in memory, which is enough for the operation to work end to end and
//! is not evidence about what a durable append costs. What replaces it is a
//! journal of extent metadata per disk, snapshot and log.

use common::block::Holds;
use common::config::ENConfig;
use common::offset::{COff, LOff};
use common::placement;
use common::range::{Budget, Covered, Filled, Piece, Range};
use common::server::{self, Process, Reply, Service};
use common::wire::{AppendBlock, Arg, Op, Request, Response, Status};
use common::{Error, Result, logw};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SERVES: &[Op] = &[Op::AppendExtent, Op::ApplyAppend, Op::ReadExtent];

/// Appends held back per extent, waiting for a record to close.
type Waiting = HashMap<(u32, u64), Vec<Arc<Pending>>>;

/// An extent instance held here. Its name is the key it is found by — this SN,
/// this id — and the second, which is part of the name too.
struct Extent {
    created: u64,
    /// Closed: no more appends, however much room is left.
    sealed: bool,
    /// Where the next append starts, in stored bytes — what the device holds, and
    /// what the seal limit is against.
    end: COff,
    /// And where it starts in the writer's numbering. The two differ by whatever
    /// compression achieved, which is why they are different types.
    logical_end: LOff,
    /// Records completed in this extent, which is what numbers them. Extent-local:
    /// the SN says what stream-local number the extent's first record has.
    records: u64,
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
    offset: COff,
    /// The extent-local number of the first record this block completed — which is
    /// how a read finds the block holding a record **without parsing anything**.
    /// The counts came from the writer; the arithmetic is this tier's.
    first_record: u64,
    /// What the writer said about these bytes: logical size, records, whether one
    /// is left open, and the CRC. None of it is recoverable from the bytes here.
    holds: Holds,
    bytes: Vec<u8>,
}

/// The blocks a range read is answered with, and what they came to.
struct Ranged<'a> {
    blocks: Vec<&'a Append>,
    covered: Covered,
}

/// What became of an append: a place in the extent, or an extent that is closed.
enum Took {
    Landed(Landed),
    /// What the extent came to, which is what the SN has to record. `closing` marks
    /// the block that found it full, as against the ones arriving afterwards, so a
    /// count of sealed extents counts extents and not refusals.
    Sealed {
        /// All three measures, because the SN records all three and a seal is the only
        /// moment the EN is asked what the extent came to (#94, §9.1.3.6).
        came_to: Filled,
        closing: bool,
    },
}

/// What an extent already holds at an offset, which is what decides whether an
/// append aimed there is a retry or a violation.
enum Covering {
    /// Nothing is stored there.
    Free,
    /// The same bytes at the same offset — an append that already landed, arriving
    /// again. It changes nothing, so it is answered ok and not stored twice.
    Same,
    /// Something else is there, and the offset it starts at.
    Taken(COff),
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
    records: u64,
    length: COff,
}

/// Where an append landed and what it numbered.
struct Landed {
    offset: COff,
    /// And where it landed in the writer's numbering, which is the locator a writer
    /// is owed: a writer knows what it wrote, not what that compressed to.
    logical: LOff,
    /// The extent-local number of the first record this append completed.
    first_record: u64,
    /// Records it completed, which is zero for a block wholly inside one record.
    records: u64,
}

/// Everything this EN holds. Two vectors, with a map from an extent's name to its
/// place in the first — a scan per append would be the slowest thing in the write
/// path long before it was the smallest.
struct Extents {
    extents: Vec<Extent>,
    at: HashMap<(u32, u64), usize>,
    appends: Vec<Append>,
}

impl Extents {
    fn new() -> Extents {
        Extents { extents: Vec::new(), at: HashMap::new(), appends: Vec::new() }
    }

    /// The extent's place in the table, made if this is the first append to it. An
    /// EN learns of an extent from the first append that names it: the SN chose the
    /// instances and needs no round trip to say so.
    ///
    /// The second is part of the name, so an append naming a different one for an
    /// extent already here is refused rather than merged into it.
    fn place(&mut self, sn: u32, id: u64, created: u64) -> Result<usize> {
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
            created,
            sealed: false,
            end: COff::ZERO,
            logical_end: LOff::ZERO,
            records: 0,
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
    fn take(&mut self, appending: &AppendBlock, cap: COff) -> Result<Took> {
        let stored = COff::of_len(appending.bytes.len())?;
        let logical = LOff::of_len(appending.holds.logical as usize)?;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];

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
                records: extent.records,
                logical: extent.logical_end,
                compressed: extent.end,
            };
            return Ok(Took::Sealed { came_to, closing });
        }

        let offset = extent.end;
        let logical_offset = extent.logical_end;
        extent.end = offset.plus(stored)?;
        extent.logical_end = extent.logical_end.plus(logical)?;
        let first_record = extent.records;
        extent.records += appending.holds.records;
        extent.open_record = appending.holds.open_record;
        self.appends.push(Append {
            extent: place,
            offset,
            first_record,
            holds: appending.holds,
            bytes: appending.bytes.clone(),
        });
        Ok(Took::Landed(Landed {
            offset,
            logical: logical_offset,
            first_record,
            records: appending.holds.records,
        }))
    }

    /// What is already stored across a byte range of an extent.
    ///
    /// ⚠️ **This is the `EXTENT-OFFSET-WRITE-ONCE` check (§7.2.4), and it is a
    /// crypto check rather than a tidiness one.** The block IV is derived from the
    /// offset, so two different plaintexts at one offset are two plaintexts under
    /// one key and one IV — which leaks their XOR and, with AEGIS, hands over tag
    /// forgery under that extent's key. Storing them is not a mess to clean up
    /// later; it is unrecoverable the moment both are written.
    ///
    /// A scan of this extent's blocks, which is what the placeholder can do — the
    /// journal that replaces these vectors indexes by offset.
    fn covering(&self, place: usize, offset: COff, stored: COff, bytes: &[u8]) -> Result<Covering> {
        let end = offset.plus(stored)?;
        for append in self.appends.iter().filter(|append| append.extent == place) {
            let past = append.offset.plus(COff::of_len(append.bytes.len())?)?;
            // Disjoint: ends at or before it starts, or starts at or after it ends.
            if end <= append.offset || offset >= past {
                continue;
            }
            // The same bytes at the same offset is the one benign overlap: an
            // instance being told twice about an append it already has.
            if append.offset == offset && append.bytes == bytes {
                return Ok(Covering::Same);
            }
            return Ok(Covering::Taken(append.offset));
        }
        Ok(Covering::Free)
    }

    /// Put an append where another instance said. The offset is given, so this must
    /// not assign one — but the ends and counts still move, since an instance that
    /// never learns them cannot become primary.
    fn apply(&mut self, appending: &AppendBlock, offset: COff) -> Result<Applied> {
        let stored = COff::of_len(appending.bytes.len())?;
        let logical = LOff::of_len(appending.holds.logical as usize)?;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        // ⚠️ Both checks before anything moves, because a rejected apply must leave
        // the extent exactly as it was.
        if self.extents[place].sealed {
            return Err(Error::new(format!(
                "extent {} of sn {} is sealed, so nothing more goes in it",
                appending.extent, appending.sn
            )));
        }
        match self.covering(place, offset, stored, &appending.bytes)? {
            Covering::Same => return Ok(Applied::Already),
            Covering::Taken(at) => {
                return Err(Error::new(format!(
                    "extent {} of sn {} already holds other bytes at compressed offset {at}, \
                     and an offset is written at most once",
                    appending.extent, appending.sn
                )));
            }
            Covering::Free => {}
        }
        let extent = &mut self.extents[place];
        let first_record = extent.records;
        extent.end = extent.end.max(offset.plus(stored)?);
        extent.logical_end = extent.logical_end.plus(logical)?;
        extent.records += appending.holds.records;
        extent.open_record = appending.holds.open_record;
        self.appends.push(Append {
            extent: place,
            offset,
            first_record,
            holds: appending.holds,
            bytes: appending.bytes.clone(),
        });
        Ok(Applied::Stored)
    }

    /// Discard everything at or above a compressed offset, and **seal**.
    ///
    /// ⚠️ **The sealing is not a tidy-up afterwards; it is why this is one method**
    /// (§7.2.4, `TRUNCATION-SEALS`). Truncating frees offsets, and an extent that
    /// stayed appendable would hand those offsets to different bytes under the same
    /// key and IV. Separating the two operations would make it possible to truncate
    /// and forget, so they are not separable here — the same reasoning as §17.12.1's
    /// waiters, where the fix was structural rather than careful.
    ///
    /// **Sealing is unconditional, even when nothing was discarded.** A handover
    /// seals the *extent* at one agreed length (§8.1.7), so an instance that happens
    /// to hold nothing above that length is still part of a sealed extent. Sealing
    /// only the instances that discarded would leave the others appendable, and a
    /// new primary computing its end from one of those would hand out an offset
    /// another instance has already written.
    #[allow(dead_code)]
    fn truncate(&mut self, sn: u32, id: u64, above: COff) -> Result<Truncated> {
        let Some(&place) = self.at.get(&(sn, id)) else {
            return Err(Error::new(format!("no extent {id} of sn {sn} here")));
        };

        // One walk over this extent's blocks: what survives, what goes, and whether
        // the cut lands where a block ends.
        let mut boundary = above == COff::ZERO;
        let mut records = 0u64;
        let mut logical = LOff::ZERO;
        let mut length = COff::ZERO;
        let mut discarded = 0usize;
        let mut last: Option<(COff, bool)> = None;
        for append in self.appends.iter().filter(|append| append.extent == place) {
            let past = append.offset.plus(COff::of_len(append.bytes.len())?)?;
            if past == above {
                boundary = true;
            }
            if append.offset >= above {
                discarded += 1;
                continue;
            }
            // ⚠️ A cut inside a block would split an append, which §8.2.4 spends a
            // section making impossible. A durable watermark is a block boundary by
            // construction, so a cut that is not one did not come from a watermark
            // and the caller is confused about what it is truncating to.
            if past > above {
                return Err(Error::new(format!(
                    "compressed offset {above} is inside the block at {}, and a block is never split",
                    append.offset
                )));
            }
            records += append.holds.records;
            logical = logical.plus(LOff::of_len(append.holds.logical as usize)?)?;
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
        // ⚠️ An extent holds whole records (§8.2.3), so it may not seal with one
        // open — and a durable watermark never lands mid-record, because an append
        // that closed no record is not durable until a later one closes it
        // (`INCOMPLETE-RECORD-NOT-DURABLE`). So this is unreachable from a real
        // watermark, and refusing says so rather than sealing something invalid.
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

    /// The blocks of an extent from a record onward, whole, until the budget is met.
    ///
    /// ⚠️ **Whole blocks, even for a one-record range.** A block is compressed, so
    /// cutting a record out of it means decompressing — CPU this tier does not have,
    /// for a job belonging to whoever compressed it (§9.1). Finding *which* blocks is
    /// arithmetic on the counts the writer supplied, and that this tier can do.
    ///
    /// `None` means no block holds that record. Ordered by offset rather than by
    /// record, because a secondary may have been told about its blocks in any order
    /// and because a block completing no record has no record to sort by.
    fn range(&self, sn: u32, id: u64, from: u64, budget: Budget) -> Result<Option<Ranged<'_>>> {
        let Some(&place) = self.at.get(&(sn, id)) else { return Ok(None) };
        let mut blocks: Vec<&Append> =
            self.appends.iter().filter(|append| append.extent == place).collect();
        blocks.sort_by_key(|append| append.offset);

        // Where to start: the block that completes `from` or a later record, or the
        // one `from` falls inside. ⚠️ **A block completing no record still counts** —
        // it sits inside a record an earlier block began (§8.2.4), and its bytes are
        // part of that record, so a range over that record has to carry it.
        let Some(first) = blocks
            .iter()
            .position(|a| a.first_record >= from || from < a.first_record + a.holds.records)
        else {
            return Ok(None);
        };

        let mut covered = Covered::default();
        let mut taken: Vec<&Append> = Vec::new();
        for block in &blocks[first..] {
            covered.took(
                block.holds.records,
                LOff::of_len(block.holds.logical as usize)?,
                COff::of_len(block.bytes.len())?,
            );
            taken.push(block);
            // ⚠️ Two rules, and the order of the test is both of them. The budget is
            // checked **after** taking a block, so one block always comes back even
            // past it (§9.1.3.6) — a smaller budget must not return nothing, or a
            // caller with a small buffer retries for ever. And stopping is only
            // allowed with no record open, so `next` is a real record number rather
            // than the one we started at: §8.2.3's durability gate, arriving on the
            // read path for the same reason.
            if budget.met(&covered) && !block.holds.open_record {
                break;
            }
        }

        // Where to carry on. The last block taken completed records up to here — and
        // if it completed none, because the extent ran out mid-record, this is that
        // open record's own number and `more` is false, so a caller stops rather than
        // asking for the same thing again.
        covered.next = taken.last().map_or(from, |last| last.first_record + last.holds.records);
        covered.more = first + taken.len() < blocks.len();
        Ok(Some(Ranged { blocks: taken, covered }))
    }

    /// The second an extent here was made in, which is the rest of its name.
    fn created(&self, sn: u32, id: u64) -> Option<u64> {
        self.at.get(&(sn, id)).map(|place| self.extents[*place].created)
    }

    /// Everything stored for an extent, in order. No read path wants this — a
    /// reader asks for a block — but a test does, to say the blocks landed where
    /// they were meant to.
    #[cfg(test)]
    fn read(&self, sn: u32, id: u64) -> Vec<u8> {
        let Some(place) = self.at.get(&(sn, id)) else { return Vec::new() };
        let mut found: Vec<&Append> =
            self.appends.iter().filter(|append| append.extent == *place).collect();
        found.sort_by_key(|append| append.offset);
        found.iter().flat_map(|append| append.bytes.iter().copied()).collect()
    }
}

struct EN {
    config: ENConfig,
    held: Mutex<Extents>,
    /// Appends whose bytes are in, and whose record is not finished yet. Keyed by
    /// extent, since a later append to the same extent is what completes it.
    waiting: Mutex<Waiting>,
    appends: AtomicU64,
    bytes: AtomicU64,
    sealed: AtomicU64,
    /// Blocks stored with a CRC their caller gave, which is what a reader will have
    /// to check with.
    checksummed: AtomicU64,
}

impl Service for EN {
    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    /// An append to the primary is answered once a quorum of instances has applied
    /// it, which is other processes' business.
    fn defers(&self, op: Op) -> bool {
        matches!(op, Op::AppendExtent)
    }

    /// Applying an append is a copy, and issuing the broadcast is a write, so
    /// neither is worth a queue hop. A read parses an extent, which is not small,
    /// so it goes to a worker.
    fn inline(&self, op: Op) -> bool {
        matches!(
            op,
            Op::AppendExtent | Op::ApplyAppend | Op::Ping | Op::Status | Op::Shutdown | Op::Sites
        )
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Request, reply: Reply) {
        match request.op {
            Op::AppendExtent => self.append(process, &request, reply),
            Op::ApplyAppend => reply.send(self.applied(&request)),
            Op::ReadExtent => reply.send(self.read(&request)),
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        let extents = self.held.lock().expect("extents").extents.len();
        vec![
            format!("extents {extents}"),
            format!("appends {}", self.appends.load(Ordering::Relaxed)),
            format!("appended_bytes {}", self.bytes.load(Ordering::Relaxed)),
            format!("sealed_extents {}", self.sealed.load(Ordering::Relaxed)),
            format!("blocks_with_a_crc {}", self.checksummed.load(Ordering::Relaxed)),
            format!("media_class {}", self.config.media_class),
        ]
    }
}

impl EN {
    fn new(config: ENConfig) -> EN {
        EN {
            config,
            held: Mutex::new(Extents::new()),
            waiting: Mutex::new(HashMap::new()),
            appends: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            checksummed: AtomicU64::new(0),
        }
    }

    /// The primary's half: take the offset, apply here, and send the same bytes at
    /// that offset to the other instances. The answer waits for a quorum of them,
    /// and waits without holding a worker.
    fn append(self: &Arc<Self>, process: &Arc<Process>, request: &Request, mut reply: Reply) {
        let appending = match AppendBlock::of(request) {
            Ok(appending) => appending,
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        let length = appending.bytes.len() as u64;
        let cap = match COff::of_number(process.cluster.extents.seal_bytes) {
            Ok(cap) => cap,
            Err(e) => return reply.send(Response::error(Status::Internal, e.to_string())),
        };
        let landed = match self.take(&appending, cap) {
            Ok(Took::Landed(landed)) => landed,
            // Sealed: what the extent came to, so whoever asked can tell the SN and
            // come back with the next extent.
            Ok(Took::Sealed { came_to, closing }) => {
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
                return reply.send(Response {
                    id: 0,
                    status: Status::Sealed,
                    args: came_to.args(),
                });
            }
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        reply.about(format_args!(
            "extent {} of sn {},offset {},bytes {length},records {}",
            appending.extent, appending.sn, landed.logical, landed.records
        ));

        // This process is one of the instances, and has just applied it, so the
        // rest are who else must hear about it.
        //
        // ⚠️ **The list names disks, so it is compared after the lookup, not before.** With
        // one disk per EN the two numbers coincide and this reads as a no-op; with several
        // disks on one EN, filtering on the raw disk number would have this process send
        // itself its own append (`placement::en_of`, §17.29.3).
        let mine = process.entry.number();
        let others: Vec<u32> = appending
            .instances
            .iter()
            .map(|disk| placement::en_of(*disk))
            .filter(|en| *en != mine)
            .collect();

        // ⚠️ The **logical** offset and length, which is what a writer is owed: it
        // knows what it wrote, not what that compressed to. The stored position is
        // this tier's business and stays here.
        let receipt = vec![
            Arg::Number(appending.sn as u64),
            Arg::Number(appending.extent),
            Arg::Number(appending.created),
            Arg::Number(landed.logical.number()),
            Arg::Number(appending.holds.logical),
            Arg::Number(landed.first_record),
            Arg::Number(landed.records),
        ];
        // Two gates, and an append passes both or is not durable: a quorum of
        // instances, and a record that has actually finished.
        let quorum_wants = process.cluster.extents.quorum;
        let pending = Arc::new(Pending::new(receipt, quorum_wants, reply));
        match landed.records {
            0 => self
                .waiting
                .lock()
                .expect("waiting")
                .entry((appending.sn, appending.extent))
                .or_default()
                .push(Arc::clone(&pending)),
            _ => {
                pending.records_are_whole();
                self.finish(appending.sn, appending.extent);
            }
        }
        let quorum = Arc::new(Quorum::new(others.len(), quorum_wants, pending));

        // The same block, with the offset this primary chose and no instance list —
        // a secondary has nobody to pass it on to.
        let apply = AppendBlock {
            instances: Vec::new(),
            offset: Some(landed.offset.number()),
            ..appending.clone()
        }
        .request(Op::ApplyAppend);
        // Issued to all of them before any of them answers: a quorum is straggler
        // tolerance only if the writes go out together.
        for en in others {
            let identity = format!("EN{en}");
            let waiting = Arc::clone(&quorum);
            if let Err(e) = process.ask(&identity, &apply, move |answer| {
                waiting.applied(answer.status == Status::Ok);
            }) {
                logw!("could not reach an instance", "en {identity},{e}");
                quorum.applied(false);
            }
        }
    }

    fn take(&self, appending: &AppendBlock, cap: COff) -> Result<Took> {
        let took = self.held.lock().expect("extents").take(appending, cap)?;
        if let Took::Landed(_) = &took {
            self.appends.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
            if appending.holds.crc.is_some() {
                self.checksummed.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(took)
    }

    /// A record closed, so every append still waiting on this extent is waiting on
    /// nothing: a fragment only ever sits at the end, so anything before the record
    /// that just closed is inside a whole one.
    fn finish(&self, sn: u32, extent: u64) {
        let waiting = self.waiting.lock().expect("waiting").remove(&(sn, extent));
        for pending in waiting.unwrap_or_default() {
            pending.records_are_whole();
        }
    }

    /// A secondary's half: the offset came with the request, so there is nothing
    /// here to decide. Anything the instances have to agree about arrives the same
    /// way — the primary decided it, and an instance that worked it out for itself
    /// would be deciding something the extent did not.
    fn applied(&self, request: &Request) -> Response {
        let appending = match AppendBlock::of(request) {
            Ok(appending) => appending,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        let Some(offset) = appending.offset else {
            return Response::error(Status::BadRequest, "apply-append takes an offset");
        };
        let offset = match COff::of_number(offset) {
            Ok(offset) => offset,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        match self.held.lock().expect("extents").apply(&appending, offset) {
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
            // A retry of an append this instance already has. Ok, because the
            // primary is owed the same answer it would have got the first time —
            // and counted nowhere, because nothing was stored.
            Ok(Applied::Already) => return Response::ok(),
            Ok(Applied::Stored) => {}
        }
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
        Response::ok()
    }

    /// The block holding a record, whole, with what the writer said about it.
    ///
    /// A reader gets bytes it must decompress, check against the CRC, and parse —
    /// all of which it can do and none of which this tier can (§9.1). What comes
    /// back beside the bytes is what makes that possible: the logical size, the
    /// record the block starts at, how many it holds, and the CRC.
    fn read(&self, request: &Request) -> Response {
        let [Arg::Number(sn), Arg::Number(extent), Arg::Number(created), rest @ ..] =
            request.args.as_slice()
        else {
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
        // The extent-local record to start at. The other two start forms need a
        // prefix sum of logical lengths, which §4.5 keeps lazily and nothing here
        // has — so they are well-formed requests for something unbuilt (§9.1.3.4).
        let Some(from) = range.start.record() else {
            return Response::error(
                Status::Unimplemented,
                "starting from a logical offset needs a prefix sum nothing keeps yet",
            );
        };

        let held = self.held.lock().expect("extents");
        match held.created(*sn as u32, *extent) {
            None => {
                return Response::error(
                    Status::NotFound,
                    format!("no extent {extent} of sn {sn} here"),
                );
            }
            // The second is part of the name, so a reader asking with the wrong one
            // is asking about an extent that no longer exists rather than this one.
            Some(second) if second != *created => {
                return Response::error(
                    Status::NotFound,
                    format!("extent {extent} of sn {sn} was made in second {second}"),
                );
            }
            Some(_) => {}
        }
        let ranged = match held.range(*sn as u32, *extent, from, range.budget) {
            Ok(Some(ranged)) => ranged,
            Ok(None) => {
                return Response::error(
                    Status::NotFound,
                    format!("extent {extent} of sn {sn} has no record {from}"),
                );
            }
            Err(e) => return Response::error(Status::Internal, e.to_string()),
        };

        // What it covered first, then the blocks. Built through the shapes in
        // `common` so that nothing here and nothing in the FE counts positions
        // independently — §17.13.3's mistake.
        let mut args = ranged.covered.args();
        for block in ranged.blocks {
            args.extend(
                Piece {
                    logical: block.holds.logical,
                    first_record: block.first_record,
                    records: block.holds.records,
                    crc: block.holds.crc,
                    bytes: block.bytes.clone(),
                }
                .args(),
            );
        }
        Response { id: 0, status: Status::Ok, args }
    }
}

/// Counting instances towards a quorum. Separate from what to do about it, so the
/// arithmetic can be tested without a connection to answer down.
///
/// This is an ordinary quorum, the same as Paxos and Raft: nothing here is new, and
/// the order statistic a failure-mode seal uses is the one Raft advances its commit
/// index by.
struct Counting {
    /// Instances asked besides the primary.
    others: usize,
    /// How many of them must apply before the append is durable.
    wanted: usize,
    applied: usize,
    failed: usize,
    settled: bool,
}

/// What the count came to, once it came to anything.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Durable,
    /// Too many instances failed for a quorum to be reachable.
    Lost,
}

impl Counting {
    /// `others` is how many instances besides the primary were asked. The primary
    /// has already applied it, so it is the first of the quorum.
    fn new(others: usize, quorum: usize) -> Counting {
        Counting {
            others,
            // The primary has already applied it, so it is the first of the quorum.
            wanted: quorum.saturating_sub(1),
            applied: 0,
            failed: 0,
            settled: false,
        }
    }

    /// One instance's answer. Says something the first time the count settles, and
    /// nothing after that, so the reply is sent exactly once.
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
            // Enough have failed that the ones left cannot make up the quorum, so
            // there is nothing to gain by waiting for them.
            Verdict::Lost
        } else {
            return None;
        };
        self.settled = true;
        Some(verdict)
    }

    /// Whether a quorum is still reachable: the instances that have not failed are
    /// enough to make one. False before any answer means the append was sent to too
    /// few instances to have ever succeeded.
    fn possible(&self) -> bool {
        self.others - self.failed >= self.wanted
    }
}

/// The count, feeding one of the two gates an append has to pass.
struct Quorum {
    counting: Mutex<Counting>,
    pending: Arc<Pending>,
}

impl Quorum {
    fn new(others: usize, quorum: usize, pending: Arc<Pending>) -> Quorum {
        let quorum = Quorum { counting: Mutex::new(Counting::new(others, quorum)), pending };
        // An append that cannot reach a quorum however well it goes is refused now
        // rather than after waiting for answers that cannot add up.
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

/// One append's answer, and the two things it waits for: a quorum of instances,
/// and a record that has finished. Bytes on three disks are not a record, so an
/// append that closed none of them is not durable however well the write went.
struct Pending {
    gates: Mutex<Gates>,
    receipt: Vec<Arg>,
    /// Only so a refusal can say what it wanted.
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

    /// A quorum that cannot be reached, which no amount of waiting fixes.
    fn lost(&self) {
        let mut gates = self.gates.lock().expect("pending");
        if let Some(reply) = gates.reply.take() {
            reply.send(Response::error(
                Status::Internal,
                format!("fewer than the {} instances a quorum wants applied it", self.quorum),
            ));
        }
    }
}

fn main() {
    let result =
        server::main(|process| Ok(EN::new(ENConfig::load(&process.config.service_config)?)));
    if let Err(e) = result {
        eprintln!("en: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::framing::Framing;
    use common::testing;

    /// Room enough that nothing seals, for the tests not about sealing.
    const ROOM: COff = COff::new(1 << 20);

    /// One whole record, framed as the client would frame it.
    fn line(text: &str) -> Vec<u8> {
        format!("{text}\n").into_bytes()
    }

    /// A block as a client would present it: measured before compressing, and here
    /// stored uncompressed, which is what a client does when compressing did not
    /// help. What the EN does is the same either way — it never looks inside.
    fn block(sn: u32, extent: u64, bytes: &[u8]) -> AppendBlock {
        let holds = Holds::of(Framing::Lf, b"", bytes, Some(common::crc::of(bytes)))
            .expect("a well-formed block");
        AppendBlock {
            sn,
            extent,
            created: 1_000_000,
            holds,
            instances: Vec::new(),
            offset: None,
            bytes: bytes.to_vec(),
        }
    }

    fn append(en: &EN, extent: u64, bytes: &[u8]) -> Landed {
        match en.take(&block(0, extent, bytes), ROOM).expect("an append") {
            Took::Landed(landed) => landed,
            Took::Sealed { .. } => panic!("that should have fitted"),
        }
    }

    /// Offsets are handed out in order, each one where the last block ended — and
    /// they count *stored* bytes, which is what a device holds.
    #[test]
    fn offsets_follow_one_another() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("hello")).offset, COff::new(0));
        assert_eq!(append(&en, 1, &line("there")).offset, COff::new(6));
        assert_eq!(append(&en, 1, &line("!")).offset, COff::new(12));
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"hello\nthere\n!\n");
    }

    /// Two extents are two byte streams, whatever order the appends arrive in.
    #[test]
    fn extents_do_not_share_offsets() {
        let en = EN::new(testing::en_config());
        assert_eq!(append(&en, 1, &line("one")).offset, COff::new(0));
        assert_eq!(append(&en, 2, &line("two")).offset, COff::new(0));
        assert_eq!(append(&en, 1, &line("more")).offset, COff::new(4));
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"one\nmore\n");
    }

    /// The same extent id from two SNs is two extents: an id is a name only
    /// alongside the SN that gave it.
    #[test]
    fn an_extent_is_named_by_its_sn_too() {
        let en = EN::new(testing::en_config());
        en.take(&block(0, 1, &line("from sn0")), ROOM).unwrap();
        en.take(&block(1, 1, &line("from sn1")), ROOM).unwrap();
        assert_eq!(en.held.lock().unwrap().extents.len(), 2);
    }

    /// And the second is part of the name, so an append that disagrees about it is
    /// refused rather than added to the extent that is here.
    #[test]
    fn a_second_that_disagrees_is_refused() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("first"));
        let later = AppendBlock { created: 1_000_001, ..block(0, 1, &line("second")) };
        assert!(en.take(&later, ROOM).is_err());
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"first\n");
    }

    /// Records are numbered from zero within an extent, from the counts the writer
    /// supplied — nothing here parses a block to find them.
    #[test]
    fn an_append_is_told_the_records_it_closed() {
        let en = EN::new(testing::en_config());
        let first = append(&en, 1, &line("one"));
        assert_eq!((first.first_record, first.records), (0, 1));
        let pair = append(&en, 1, b"two\nthree\n");
        assert_eq!((pair.first_record, pair.records), (1, 2));
        assert_eq!(en.held.lock().unwrap().extents[0].records, 3);
    }

    /// A record spanning blocks: the first closes nothing and says a record is open,
    /// the second closes it. The EN tracks the flag, never the bytes.
    #[test]
    fn a_record_spanning_blocks_is_a_flag_here_and_bytes_at_the_writer() {
        let en = EN::new(testing::en_config());
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", None).unwrap();
        let first = AppendBlock { holds: opened, ..block(0, 1, b"half a rec") };
        assert!(matches!(en.take(&first, ROOM), Ok(Took::Landed(_))));
        assert!(en.held.lock().unwrap().extents[0].open_record, "one is open");

        // The writer carried the opening bytes and measures the rest against them.
        let closed = Holds::of(Framing::Lf, b"half a rec", b"ord\n", None).unwrap();
        let second = AppendBlock { holds: closed, ..block(0, 1, b"ord\n") };
        match en.take(&second, ROOM) {
            Ok(Took::Landed(landed)) => assert_eq!(landed.records, 1),
            _ => panic!("the record should close"),
        }
        assert!(!en.held.lock().unwrap().extents[0].open_record);
    }

    /// An extent with a record still open cannot seal, however full it is.
    #[test]
    fn an_open_record_keeps_an_extent_from_sealing() {
        let en = EN::new(testing::en_config());
        let cap = COff::new(4);
        let opened = Holds::of(Framing::Lf, b"", b"half", None).unwrap();
        let first = AppendBlock { holds: opened, ..block(0, 1, b"half") };
        assert!(matches!(en.take(&first, cap), Ok(Took::Landed(_))));

        let closed = Holds::of(Framing::Lf, b"half", b" and half\n", None).unwrap();
        let second = AppendBlock { holds: closed, ..block(0, 1, b" and half\n") };
        assert!(
            matches!(en.take(&second, cap), Ok(Took::Landed(_))),
            "it must be allowed to finish"
        );
        // Now that nothing is open, the next block finds it full.
        assert!(matches!(en.take(&block(0, 1, b"x\n"), cap), Ok(Took::Sealed { .. })));
    }

    /// An extent that has no room for the next block is closed rather than stretched,
    /// and the answer says what it came to.
    #[test]
    fn a_full_extent_seals_instead_of_taking_the_block() {
        let en = EN::new(testing::en_config());
        let cap = COff::new(10);
        assert!(matches!(en.take(&block(0, 1, b"one\n"), cap), Ok(Took::Landed(_))));
        assert!(matches!(en.take(&block(0, 1, b"two\n"), cap), Ok(Took::Landed(_))));
        match en.take(&block(0, 1, b"three\n"), cap) {
            Ok(Took::Sealed { came_to, .. }) => {
                assert_eq!(came_to.records, 2, "what the extent came to");
                assert_eq!(came_to.compressed, COff::new(8), "in stored bytes");
                // ⚠️ And the logical length, which the SN had no way to learn before #94:
                // the same eight bytes here, because nothing was compressed.
                assert_eq!(came_to.logical, LOff::new(8), "and in the bytes the writer wrote");
            }
            other => panic!("should have sealed, not {:?}", other.is_ok()),
        }
        // And it stays sealed: the block that found it full does not land later.
        assert!(matches!(en.take(&block(0, 1, b"x\n"), cap), Ok(Took::Sealed { .. })));
    }

    /// A compressed block is stored as it arrives and handed back the same way. The
    /// EN never decompresses, so what it holds is not what the records are.
    #[test]
    fn a_compressed_block_is_stored_and_returned_untouched() {
        let en = EN::new(testing::en_config());
        let records = (0..40).flat_map(|n| line(&format!("record {n}"))).collect::<Vec<u8>>();
        let holds = Holds::of(Framing::Lf, b"", &records, Some(common::crc::of(&records))).unwrap();
        let stored = common::block::squeeze(common::config::Compress::Lz4, &records)
            .expect("that should compress");
        assert!(stored.len() < records.len());

        let compressed = AppendBlock { holds, bytes: stored.clone(), ..block(0, 1, b"") };
        assert!(matches!(en.take(&compressed, ROOM), Ok(Took::Landed(_))));

        // Stored bytes are the compressed ones; the logical size is what the writer
        // said, and the two are different numbers for the same block.
        let held = en.held.lock().unwrap();
        assert_eq!(held.extents[0].end, COff::of_len(stored.len()).unwrap());
        assert_eq!(held.extents[0].logical_end, LOff::of_len(records.len()).unwrap());
        assert_eq!(held.read(0, 1), stored, "handed back as it arrived");
    }

    /// A point read is a range of one record: it finds the block holding it by
    /// arithmetic on the counts and returns the whole thing.
    #[test]
    fn a_read_returns_the_block_holding_the_record() {
        let en = EN::new(testing::en_config());
        append(&en, 1, b"one\ntwo\n");
        append(&en, 1, b"three\nfour\nfive\n");
        let held = en.held.lock().unwrap();
        let one = |record| {
            held.range(0, 1, record, Budget::Records(1))
                .expect("no arithmetic to fail")
                .map(|ranged| ranged.blocks[0].bytes.clone())
        };
        assert_eq!(one(0).unwrap(), b"one\ntwo\n");
        assert_eq!(one(1).unwrap(), b"one\ntwo\n");
        assert_eq!(one(2).unwrap(), b"three\nfour\nfive\n");
        assert_eq!(one(4).unwrap(), b"three\nfour\nfive\n");
        assert!(one(5).is_none(), "past the end");

        // And the answer says where the block starts, so a reader can index into it,
        // and where to carry on, which is what makes it a range.
        let ranged = held.range(0, 1, 4, Budget::Records(1)).unwrap().expect("a block");
        assert_eq!(ranged.blocks[0].first_record, 2);
        assert_eq!(ranged.covered.next, 5, "one past the last record it completed");
        assert!(!ranged.covered.more, "and this extent has no more blocks");
    }

    /// A budget in each of the three spaces, over the same two blocks. The caller
    /// names one and the answer reports all three (§9.1.3.6).
    #[test]
    fn a_budget_is_honoured_in_whichever_space_it_names() {
        let en = EN::new(testing::en_config());
        append(&en, 1, b"one\ntwo\n"); // 2 records, 8 bytes
        append(&en, 1, b"three\nfour\nfive\n"); // 3 records, 16 bytes
        let held = en.held.lock().unwrap();
        let ranged = |budget| held.range(0, 1, 0, budget).unwrap().expect("blocks");

        // Enough for the first block only, in each space.
        for budget in [Budget::Records(2), Budget::Logical(8), Budget::Compressed(8)] {
            let one = ranged(budget);
            assert_eq!(one.blocks.len(), 1, "{budget:?}");
            assert_eq!((one.covered.records, one.covered.logical), (2, 8), "{budget:?}");
            assert_eq!(one.covered.compressed, 8, "nothing compressed, so equal");
            assert_eq!(one.covered.next, 2, "{budget:?}");
            assert!(one.covered.more, "{budget:?}");
        }
        // Enough for both.
        for budget in [Budget::Records(5), Budget::Logical(24), Budget::Compressed(24)] {
            let both = ranged(budget);
            assert_eq!(both.blocks.len(), 2, "{budget:?}");
            assert_eq!((both.covered.records, both.covered.logical), (5, 24), "{budget:?}");
            assert_eq!(both.covered.next, 5, "{budget:?}");
            assert!(!both.covered.more, "{budget:?}");
        }
    }

    /// ⚠️ One block always comes back, even past the budget. A budget below the
    /// first block's size returning nothing would be a livelock: the caller retries
    /// for ever and never learns it asked for too little.
    #[test]
    fn a_budget_smaller_than_a_block_still_gets_a_block() {
        let en = EN::new(testing::en_config());
        append(&en, 1, b"one\ntwo\n");
        append(&en, 1, b"three\n");
        let held = en.held.lock().unwrap();
        for budget in [Budget::Records(0), Budget::Logical(0), Budget::Logical(1)] {
            let ranged = held.range(0, 1, 0, budget).unwrap().expect("a block");
            assert_eq!(ranged.blocks.len(), 1, "{budget:?}");
            assert_eq!(ranged.covered.logical, 8, "and it is reported at its real size");
            assert!(ranged.covered.more);
        }
    }

    /// ⚠️ A range never stops with a record open, so `next` is always a real record
    /// number rather than the one it started at. That is §8.2.3's durability gate
    /// arriving on the read path: a caller resuming from `next` must make progress.
    #[test]
    fn a_range_does_not_stop_in_the_middle_of_a_record() {
        let en = EN::new(testing::en_config());
        append(&en, 1, b"one\n");
        // A record spanning two blocks: the first completes nothing.
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", None).unwrap();
        en.take(&AppendBlock { holds: opened, ..block(0, 1, b"half a rec") }, ROOM).unwrap();
        let closed = Holds::of(Framing::Lf, b"half a rec", b"ord\n", None).unwrap();
        en.take(&AppendBlock { holds: closed, ..block(0, 1, b"ord\n") }, ROOM).unwrap();

        let held = en.held.lock().unwrap();
        // A one-record budget from record 1 cannot stop after the opening block,
        // because that block completed nothing and `next` would be 1 again.
        let ranged = held.range(0, 1, 1, Budget::Records(1)).unwrap().expect("blocks");
        assert_eq!(ranged.blocks.len(), 2, "the opener and the closer");
        assert_eq!(ranged.blocks[0].bytes, b"half a rec");
        assert_eq!(ranged.covered.records, 1);
        assert_eq!(ranged.covered.next, 2, "past the record that spanned them");
        assert!(!ranged.covered.more);
    }

    /// A block that completes no record is still part of the answer, because its
    /// bytes are part of a record somebody asked for. The old point read could not
    /// find one at all.
    #[test]
    fn a_block_completing_no_record_is_carried_anyway() {
        let en = EN::new(testing::en_config());
        let opened = Holds::of(Framing::Lf, b"", b"start", None).unwrap();
        en.take(&AppendBlock { holds: opened, ..block(0, 1, b"start") }, ROOM).unwrap();
        let held = en.held.lock().unwrap();
        // Record 0 has begun and not finished, so there is nothing complete to
        // report — but the bytes that exist come back, and `more` is false because
        // the extent holds nothing further.
        let ranged = held.range(0, 1, 0, Budget::Records(1)).unwrap().expect("blocks");
        assert_eq!(ranged.blocks.len(), 1);
        assert_eq!(ranged.covered.records, 0);
        assert_eq!(ranged.covered.next, 0, "record 0 is still open");
        assert!(!ranged.covered.more, "so a caller stops rather than asking again");
    }

    /// A secondary puts the append where the primary said, and does not choose.
    #[test]
    fn an_applied_append_keeps_the_offset_it_was_given() {
        let en = EN::new(testing::en_config());
        let request =
            AppendBlock { offset: Some(64), ..block(0, 1, &line("late")) }.request(Op::ApplyAppend);
        assert_eq!(en.applied(&request).status, Status::Ok);
        let held = en.held.lock().unwrap();
        assert_eq!(held.appends[0].offset, COff::new(64));
        // The end moves past it, or this instance could not take over as primary.
        assert_eq!(held.extents[0].end, COff::new(69));
    }

    #[test]
    fn an_append_of_the_wrong_shape_is_a_bad_request() {
        let en = EN::new(testing::en_config());
        let request = Request::new(Op::ApplyAppend).number(0).number(1);
        assert_eq!(en.applied(&request).status, Status::BadRequest);
    }

    /// A secondary is told where the primary put it, so an apply without an offset
    /// asks this instance to decide something that is not its to decide.
    #[test]
    fn an_applied_append_needs_an_offset() {
        let en = EN::new(testing::en_config());
        let request = block(0, 1, &line("x")).request(Op::ApplyAppend);
        assert_eq!(en.applied(&request).status, Status::BadRequest);
    }

    /// A caller's CRC is stored with the block and not checked here: an EN holds
    /// what the writer compressed and never decompresses it.
    #[test]
    fn a_crc_is_kept_and_not_judged() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("checksummed"));
        let mut wrong = block(0, 1, &line("also fine"));
        wrong.holds.crc = Some(0);
        assert!(matches!(en.take(&wrong, ROOM), Ok(Took::Landed(_))));

        let held = en.held.lock().unwrap();
        assert_eq!(held.appends[0].holds.crc, Some(common::crc::of(&line("checksummed"))));
        assert_eq!(held.appends[1].holds.crc, Some(0), "kept, wrong or not");
        assert_eq!(en.checksummed.load(Ordering::Relaxed), 2);
    }

    /// Three of four, so two of the three secondaries settle it and the third's
    /// answer changes nothing.
    #[test]
    fn two_of_three_secondaries_make_it_durable() {
        let quorum = testing::extents().quorum;
        let mut counting = Counting::new(testing::extents().instances - 1, quorum);
        assert_eq!(counting.answer(true), None);
        assert_eq!(counting.answer(true), Some(Verdict::Durable));
        assert_eq!(counting.answer(true), None, "the answer is sent once");
    }

    /// A straggler costs nothing: the quorum is reached by the other two.
    #[test]
    fn one_slow_instance_does_not_hold_it_up() {
        let quorum = testing::extents().quorum;
        let mut counting = Counting::new(testing::extents().instances - 1, quorum);
        assert_eq!(counting.answer(false), None);
        assert_eq!(counting.answer(true), None);
        assert_eq!(counting.answer(true), Some(Verdict::Durable));
    }

    /// Two failures, and three of four is out of reach: say so rather than wait for
    /// an answer that cannot add up.
    #[test]
    fn two_failures_lose_the_append() {
        let quorum = testing::extents().quorum;
        let mut counting = Counting::new(testing::extents().instances - 1, quorum);
        assert_eq!(counting.answer(false), None);
        assert_eq!(counting.answer(false), Some(Verdict::Lost));
    }

    /// An append that could only be sent to one other instance can never reach three
    /// of four, and that is known before any answer arrives.
    #[test]
    fn too_few_instances_to_ask_is_hopeless_from_the_start() {
        assert!(!Counting::new(1, 3).possible());
        assert!(Counting::new(2, 3).possible());
    }

    // `EXTENT-OFFSET-WRITE-ONCE` and `TRUNCATION-SEALS` (§7.2.4). These are the
    // crypto invariants, so the tests are about what is *refused*: the block IV
    // comes from the offset, and two plaintexts at one offset under one key leak
    // their XOR whatever anybody does about it afterwards.

    /// A secondary told to put different bytes where it already has some refuses.
    /// Nothing above this tier can be trusted to have got the offset right, and the
    /// damage is not repairable once both are written.
    #[test]
    fn an_offset_holds_one_plaintext_and_refuses_a_second() {
        let en = EN::new(testing::en_config());
        let mut held = en.held.lock().unwrap();
        assert!(matches!(held.apply(&block(0, 1, b"first\n"), COff::ZERO), Ok(Applied::Stored)));

        let clash = held.apply(&block(0, 1, b"other\n"), COff::ZERO);
        assert!(clash.is_err(), "different bytes at a written offset");
        // Overlapping partway is the same violation, and so is straddling the start.
        assert!(held.apply(&block(0, 1, b"xx\n"), COff::new(3)).is_err());
        assert_eq!(held.read(0, 1), b"first\n", "and the refusal changed nothing");
    }

    /// The one benign overlap: the same bytes at the same offset, which is an append
    /// this instance already has arriving again. It is answered ok and stored once,
    /// because a retry must look to the primary like the first attempt did.
    #[test]
    fn the_same_bytes_at_the_same_offset_are_a_retry_not_a_clash() {
        let en = EN::new(testing::en_config());
        let mut held = en.held.lock().unwrap();
        let same = block(0, 1, b"once\n");
        assert!(matches!(held.apply(&same, COff::ZERO), Ok(Applied::Stored)));
        assert!(matches!(held.apply(&same, COff::ZERO), Ok(Applied::Already)));
        assert_eq!(held.read(0, 1), b"once\n", "stored once, not twice");
        assert_eq!(held.extents[0].records, 1, "and counted once");
    }

    /// Truncating and sealing are one operation, so an offset a truncation freed can
    /// never be handed to different bytes. This is the whole point of #83.
    #[test]
    fn a_truncation_seals_so_a_freed_offset_is_never_reused() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("keep"));
        append(&en, 1, &line("drop"));
        let mut held = en.held.lock().unwrap();
        assert_eq!(held.extents[0].end, COff::new(10));

        let cut = held.truncate(0, 1, COff::new(5)).expect("a boundary");
        assert_eq!((cut.discarded, cut.records, cut.length), (1, 1, COff::new(5)));
        assert_eq!(held.read(0, 1), b"keep\n", "the discarded block is gone");
        assert!(held.extents[0].sealed, "and the extent is sealed");
        assert_eq!(held.extents[0].records, 1, "counts rebuilt from what survived");
        assert_eq!(held.extents[0].logical_end, LOff::new(5));

        // The freed offset 5 is not handed out again, by either path.
        assert!(held.apply(&block(0, 1, &line("reuse")), COff::new(5)).is_err());
        drop(held);
        assert!(matches!(en.take(&block(0, 1, &line("reuse")), ROOM), Ok(Took::Sealed { .. })));
    }

    /// Sealing happens even when the instance had nothing above the watermark: a
    /// handover seals the extent, not just the instances that lost bytes. An
    /// instance left appendable is one a new primary could get an offset from.
    #[test]
    fn a_truncation_that_discards_nothing_still_seals() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("all of it"));
        let mut held = en.held.lock().unwrap();
        let cut = held.truncate(0, 1, COff::new(10)).expect("the end is a boundary");
        assert_eq!(cut.discarded, 0);
        assert!(held.extents[0].sealed);
        assert_eq!(held.read(0, 1), b"all of it\n", "and kept everything");
    }

    /// A cut inside a block would split an append, which §8.2.4 makes impossible —
    /// and a durable watermark is always a block boundary, so a cut that is not one
    /// did not come from a watermark.
    #[test]
    fn a_truncation_must_land_where_a_block_ends() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("first"));
        append(&en, 1, &line("second"));
        let mut held = en.held.lock().unwrap();
        assert!(held.truncate(0, 1, COff::new(3)).is_err(), "inside the first block");
        assert!(held.truncate(0, 1, COff::new(9)).is_err(), "inside the second");
        assert!(held.truncate(0, 1, COff::new(99)).is_err(), "past everything");
        assert!(!held.extents[0].sealed, "and a refused truncation seals nothing");
        assert_eq!(held.read(0, 1), b"first\nsecond\n");

        // Zero is always a boundary: discarding the lot is a truncation too.
        assert_eq!(held.truncate(0, 1, COff::ZERO).expect("the start").discarded, 2);
        assert_eq!(held.extents[0].records, 0);
        assert!(held.read(0, 1).is_empty());
    }

    /// An extent holds whole records, so it may not seal with one open. A durable
    /// watermark never lands mid-record — an append closing no record is not durable
    /// until a later one closes it — so this says the caller is confused rather than
    /// sealing something invalid.
    #[test]
    fn a_truncation_that_would_leave_a_record_open_is_refused() {
        let en = EN::new(testing::en_config());
        let opened = Holds::of(Framing::Lf, b"", b"half a rec", None).unwrap();
        en.take(&AppendBlock { holds: opened, ..block(0, 1, b"half a rec") }, ROOM).unwrap();
        let closed = Holds::of(Framing::Lf, b"half a rec", b"ord\n", None).unwrap();
        en.take(&AppendBlock { holds: closed, ..block(0, 1, b"ord\n") }, ROOM).unwrap();

        let mut held = en.held.lock().unwrap();
        // Cutting between the two blocks is a block boundary and a record's middle.
        assert!(held.truncate(0, 1, COff::new(10)).is_err());
        assert!(!held.extents[0].sealed);
        // Keeping both is fine, because the record closed.
        assert!(held.truncate(0, 1, COff::new(14)).is_ok());
    }

    /// A sealed extent takes nothing more by either path. `take` already answered
    /// `Sealed`; `apply` used to accept, which would have let a secondary keep
    /// growing an extent the primary had closed.
    #[test]
    fn a_sealed_extent_refuses_an_apply_as_well_as_an_append() {
        let en = EN::new(testing::en_config());
        let cap = COff::new(6);
        en.take(&block(0, 1, &line("full")), cap).unwrap();
        assert!(matches!(en.take(&block(0, 1, &line("x")), cap), Ok(Took::Sealed { .. })));
        let mut held = en.held.lock().unwrap();
        assert!(held.extents[0].sealed);
        assert!(held.apply(&block(0, 1, &line("x")), COff::new(5)).is_err());
    }
}
