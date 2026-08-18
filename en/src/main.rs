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
    /// Where in the extent's stored bytes it sits. Nothing in a running process
    /// reads it back yet — a reader asks for a block by record, not by offset —
    /// and what will is the device write and the journal that describes it.
    #[allow(dead_code)]
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

impl Append {
    fn covers(&self, record: u64) -> bool {
        record >= self.first_record && record < self.first_record + self.holds.records
    }
}

/// What became of an append: a place in the extent, or an extent that is closed.
enum Took {
    Landed(Landed),
    /// What the extent came to, which is what the SN has to record. `closing` marks
    /// the block that found it full, as against the ones arriving afterwards, so a
    /// count of sealed extents counts extents and not refusals.
    Sealed {
        records: u64,
        length: COff,
        closing: bool,
    },
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
            return Ok(Took::Sealed { records: extent.records, length: extent.end, closing });
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

    /// Put an append where another instance said. The offset is given, so this must
    /// not assign one — but the ends and counts still move, since an instance that
    /// never learns them cannot become primary.
    fn apply(&mut self, appending: &AppendBlock, offset: COff) -> Result<()> {
        let stored = COff::of_len(appending.bytes.len())?;
        let logical = LOff::of_len(appending.holds.logical as usize)?;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
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
        Ok(())
    }

    /// The block holding a record, whole.
    ///
    /// ⚠️ **Whole, even though the caller asked for one record.** A block is
    /// compressed, so cutting a record out of it means decompressing — CPU this tier
    /// does not have, for a job belonging to whoever compressed it. Finding *which*
    /// block is arithmetic on record counts, and that this tier can do.
    fn block(&self, sn: u32, id: u64, record: u64) -> Option<&Append> {
        let place = self.at.get(&(sn, id))?;
        self.appends.iter().find(|append| append.extent == *place && append.covers(record))
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
            Ok(Took::Sealed { records, length, closing }) => {
                if closing {
                    self.sealed.fetch_add(1, Ordering::Relaxed);
                }
                reply.about(format_args!(
                    "extent {} of sn {},records {records},bytes {length}",
                    appending.extent, appending.sn
                ));
                return reply.send(Response {
                    id: 0,
                    status: Status::Sealed,
                    args: vec![Arg::Number(records), Arg::Number(length.number())],
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
        let mine = process.entry.number();
        let others: Vec<u32> =
            appending.instances.iter().copied().filter(|en| *en != mine).collect();

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
        if let Err(e) = self.held.lock().expect("extents").apply(&appending, offset) {
            return Response::error(Status::BadRequest, e.to_string());
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
        let [Arg::Number(sn), Arg::Number(extent), Arg::Number(created), Arg::Number(record)] =
            request.args.as_slice()
        else {
            return Response::error(
                Status::BadRequest,
                "read-extent takes an SnId, an extent, a second and a record",
            );
        };
        let held = self.held.lock().expect("extents");
        match held.created(*sn as u32, *extent) {
            None => {
                Response::error(Status::NotFound, format!("no extent {extent} of sn {sn} here"))
            }
            // The second is part of the name, so a reader asking with the wrong one
            // is asking about an extent that no longer exists rather than this one.
            Some(second) if second != *created => Response::error(
                Status::NotFound,
                format!("extent {extent} of sn {sn} was made in second {second}"),
            ),
            Some(_) => match held.block(*sn as u32, *extent, *record) {
                Some(block) => Response {
                    id: 0,
                    status: Status::Ok,
                    args: vec![
                        Arg::Number(block.holds.logical),
                        Arg::Number(block.first_record),
                        Arg::Number(block.holds.records),
                        Arg::Flag(block.holds.crc.is_some()),
                        Arg::Number(block.holds.crc.unwrap_or(0)),
                        Arg::Bytes(block.bytes.clone()),
                    ],
                },
                None => Response::error(
                    Status::NotFound,
                    format!("extent {extent} of sn {sn} has no record {record}"),
                ),
            },
        }
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
            Ok(Took::Sealed { records, length, .. }) => {
                assert_eq!(records, 2, "what the extent came to");
                assert_eq!(length, COff::new(8));
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

    /// A read finds the block holding a record by arithmetic on the counts, and
    /// returns the whole thing.
    #[test]
    fn a_read_returns_the_block_holding_the_record() {
        let en = EN::new(testing::en_config());
        append(&en, 1, b"one\ntwo\n");
        append(&en, 1, b"three\nfour\nfive\n");
        let held = en.held.lock().unwrap();
        assert_eq!(held.block(0, 1, 0).unwrap().bytes, b"one\ntwo\n");
        assert_eq!(held.block(0, 1, 1).unwrap().bytes, b"one\ntwo\n");
        assert_eq!(held.block(0, 1, 2).unwrap().bytes, b"three\nfour\nfive\n");
        assert_eq!(held.block(0, 1, 4).unwrap().bytes, b"three\nfour\nfive\n");
        assert!(held.block(0, 1, 5).is_none(), "past the end");
        // And the block says where it starts, so a reader can index into it.
        assert_eq!(held.block(0, 1, 4).unwrap().first_record, 2);
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
}
