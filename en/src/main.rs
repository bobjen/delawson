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
//! ⚠️ **Nothing is on a disk yet.** The extent table and the appends themselves are
//! two vectors in memory, which is enough for the operation to work end to end and
//! is not evidence about what a durable append costs. What replaces it is a
//! journal of extent metadata per disk, snapshot and log.

use common::config::ENConfig;
use common::framing::Framing;
use common::offset::COff;
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
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
    /// Closed: no more appends, however much room is left. What is here is what the
    /// extent came to, for ever.
    sealed: bool,
    /// Where the next append to this extent will start. Compressed, because what
    /// arrives here is what a writer compressed and what a device will hold.
    end: COff,
    /// Records completed in this extent, which is what numbers them. Extent-local:
    /// the SN says what stream-local number the extent's first record has.
    records: u64,
    /// The bytes of a record that has begun and not ended. A record may span
    /// appends, so a block can leave one open — and while one is open the extent
    /// cannot seal, which is what stops a seal ever having to cut a record.
    open: Vec<u8>,
}

/// One append, as it arrived. The offset is the primary's decision, so every
/// instance holds the same append at the same place.
struct Append {
    /// Which extent, as an index into the extent table.
    extent: usize,
    offset: COff,
    bytes: Vec<u8>,
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
    /// The extent-local number of the first record this append completed.
    first_record: u64,
    /// Records it completed, which is zero for a block wholly inside one record.
    records: u64,
}

/// Which extent an append is for, and what it carries.
struct Appending<'a> {
    sn: u32,
    extent: u64,
    created: u64,
    bytes: &'a [u8],
}

impl<'a> Appending<'a> {
    /// Three numbers naming the extent, and the bytes last. What comes between
    /// them differs by operation: the instance list on the way in, the offset the
    /// primary chose on the way out.
    fn of(args: &'a [Arg]) -> Result<(Appending<'a>, &'a [Arg])> {
        let (
            [Arg::Number(sn), Arg::Number(extent), Arg::Number(created), middle @ ..],
            Some(Arg::Bytes(bytes)),
        ) = (args, args.last())
        else {
            return Err(Error::new("an append takes an SnId, an extent, a second and bytes"));
        };
        let appending = Appending { sn: *sn as u32, extent: *extent, created: *created, bytes };
        // The bytes are the last argument, so they are not one of the middle ones.
        Ok((appending, &middle[..middle.len() - 1]))
    }
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
            records: 0,
            open: Vec::new(),
        });
        let place = self.extents.len() - 1;
        self.at.insert((sn, id), place);
        Ok(place)
    }

    /// Take the next offset in an extent, put the bytes there, and count the
    /// records they complete. This is the one serialized step of an append;
    /// everything expensive happens after it.
    ///
    /// Or seal, if the extent is full: a block goes wholly into this extent or
    /// wholly into the next one, never split, so a block that will not fit gets an
    /// extent that is closed instead of a place in it.
    fn take(&mut self, appending: &Appending, framing: Framing, cap: COff) -> Result<Took> {
        let length = COff::of_len(appending.bytes.len())?;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];

        // A record already open must finish here, since an extent holds whole
        // records — so an extent with one cannot seal, whatever the cap says. The
        // client never leaves one open, so this is for a writer using the protocol
        // directly.
        let sealable = extent.open.is_empty();
        if extent.sealed || (sealable && extent.end.plus(length)? > cap) {
            // Whether this block is the one that closed it, so a counter of sealed
            // extents counts extents and not refusals.
            let closing = !extent.sealed;
            extent.sealed = true;
            return Ok(Took::Sealed { records: extent.records, length: extent.end, closing });
        }

        // Before the bytes are taken, so a block that breaks the record rule
        // changes nothing.
        let (closed, open) = framing.advance(&extent.open, appending.bytes)?;
        let offset = extent.end;
        extent.end = offset.plus(length)?;
        let first_record = extent.records;
        extent.records += closed;
        extent.open = open;
        self.appends.push(Append { extent: place, offset, bytes: appending.bytes.to_vec() });
        Ok(Took::Landed(Landed { offset, first_record, records: closed }))
    }

    /// Put an append at an offset another instance chose. The offset is given, so
    /// this must not assign one — but the extent's end still moves, since an
    /// instance that never learns the end cannot become primary.
    fn apply(&mut self, appending: &Appending, offset: COff) -> Result<()> {
        let length = COff::of_len(appending.bytes.len())?;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];
        extent.end = extent.end.max(offset.plus(length)?);
        self.appends.push(Append { extent: place, offset, bytes: appending.bytes.to_vec() });
        Ok(())
    }

    /// One record's content, parsed forward from the start of the extent. There is
    /// no sparse index yet, so this is the whole extent every time — the index is
    /// what turns it into one granule read.
    fn record(&self, sn: u32, id: u64, framing: Framing, record: u64) -> Option<Vec<u8>> {
        let bytes = self.read(sn, id);
        framing.records(&bytes).nth(record as usize).map(<[u8]>::to_vec)
    }

    /// The second an extent here was made in, which is the rest of its name.
    fn created(&self, sn: u32, id: u64) -> Option<u64> {
        self.at.get(&(sn, id)).map(|place| self.extents[*place].created)
    }

    /// The bytes of an extent, in order. Stitched from the appends every time,
    /// which is what a vector of appends costs and what the journal replaces.
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
        }
    }

    /// The primary's half: take the offset, apply here, and send the same bytes at
    /// that offset to the other instances. The answer waits for a quorum of them,
    /// and waits without holding a worker.
    fn append(self: &Arc<Self>, process: &Arc<Process>, request: &Request, mut reply: Reply) {
        let (appending, middle) = match Appending::of(&request.args) {
            Ok(parsed) => parsed,
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        // The framing comes with the append, since the primary is what counts the
        // records and an instance must not decide that for itself.
        let (framing, instances) = match middle {
            [Arg::Number(kind), Arg::Number(width), instances @ ..] => {
                match Framing::of_wire(*kind, *width) {
                    Ok(framing) => (framing, instances),
                    Err(e) => {
                        return reply.send(Response::error(Status::BadRequest, e.to_string()));
                    }
                }
            }
            _ => {
                return reply.send(Response::error(
                    Status::BadRequest,
                    "an append takes a framing and an instance list",
                ));
            }
        };
        let length = appending.bytes.len() as u64;
        let cap = match COff::of_number(process.cluster.extents.seal_bytes) {
            Ok(cap) => cap,
            Err(e) => return reply.send(Response::error(Status::Internal, e.to_string())),
        };
        let landed = match self.take(&appending, framing, cap) {
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
            appending.extent, appending.sn, landed.offset, landed.records
        ));

        // This process is one of the instances, and has just applied it, so the
        // rest are who else must hear about it.
        let mine = process.entry.number();
        let others: Vec<u32> = instances
            .iter()
            .filter_map(|arg| match arg {
                Arg::Number(en) => Some(*en as u32),
                _ => None,
            })
            .filter(|en| *en != mine)
            .collect();

        let receipt = vec![
            Arg::Number(appending.sn as u64),
            Arg::Number(appending.extent),
            Arg::Number(appending.created),
            Arg::Number(landed.offset.number()),
            Arg::Number(length),
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

        let apply = Request::new(Op::ApplyAppend)
            .number(appending.sn as u64)
            .number(appending.extent)
            .number(appending.created)
            .number(landed.offset.number())
            .bytes(appending.bytes.to_vec());
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

    fn take(&self, appending: &Appending, framing: Framing, cap: COff) -> Result<Took> {
        let took = self.held.lock().expect("extents").take(appending, framing, cap)?;
        if let Took::Landed(_) = &took {
            self.appends.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
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

    /// One record of an extent, for a reader that knows which one it wants.
    fn read(&self, request: &Request) -> Response {
        let [
            Arg::Number(sn),
            Arg::Number(extent),
            Arg::Number(created),
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(record),
        ] = request.args.as_slice()
        else {
            return Response::error(
                Status::BadRequest,
                "read-extent takes an SnId, an extent, a second, a framing and a record",
            );
        };
        let framing = match Framing::of_wire(*kind, *width) {
            Ok(framing) => framing,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
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
            Some(_) => match held.record(*sn as u32, *extent, framing, *record) {
                Some(bytes) => {
                    Response { id: 0, status: Status::Ok, args: vec![Arg::Bytes(bytes)] }
                }
                None => Response::error(
                    Status::NotFound,
                    format!("extent {extent} of sn {sn} has no record {record}"),
                ),
            },
        }
    }

    /// A secondary's half: the offset came with the request, so there is nothing
    /// here to decide. Anything the instances have to agree about arrives the same
    /// way — the primary decided it, and an instance that worked it out for itself
    /// would be deciding something the extent did not.
    fn applied(&self, request: &Request) -> Response {
        let (appending, middle) = match Appending::of(&request.args) {
            Ok(parsed) => parsed,
            Err(e) => return Response::error(Status::BadRequest, e.to_string()),
        };
        let Some(Arg::Number(offset)) = middle.first() else {
            return Response::error(Status::BadRequest, "apply-append takes an offset");
        };
        let offset = match COff::of_number(*offset) {
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
    use common::testing;

    const LF: Framing = Framing::Lf;

    fn appending<'a>(sn: u32, extent: u64, bytes: &'a [u8]) -> Appending<'a> {
        Appending { sn, extent, created: 1_000_000, bytes }
    }

    /// One whole record, which is what the client always sends.
    fn line(text: &str) -> Vec<u8> {
        format!("{text}\n").into_bytes()
    }

    /// Room enough that nothing seals, for the tests that are not about sealing.
    const ROOM: COff = COff::new(1 << 20);

    fn append(en: &EN, extent: u64, bytes: &[u8]) -> Landed {
        match en.take(&appending(0, extent, bytes), LF, ROOM).expect("an append") {
            Took::Landed(landed) => landed,
            Took::Sealed { .. } => panic!("that should have fitted"),
        }
    }

    /// Offsets are handed out in order, each one where the last append ended.
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
        assert_eq!(en.held.lock().unwrap().read(0, 2), b"two\n");
    }

    /// The same extent id from two SNs is two extents: an id is a name only
    /// alongside the SN that gave it.
    #[test]
    fn an_extent_is_named_by_its_sn_too() {
        let en = EN::new(testing::en_config());
        en.take(&appending(0, 1, &line("from sn0")), LF, ROOM).unwrap();
        en.take(&appending(1, 1, &line("from sn1")), LF, ROOM).unwrap();
        assert_eq!(en.held.lock().unwrap().extents.len(), 2);
    }

    /// And the second is part of the name, so an append that disagrees about it is
    /// refused rather than added to the extent that is here.
    #[test]
    fn a_second_that_disagrees_is_refused() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("first"));
        let later = Appending { sn: 0, extent: 1, created: 1_000_001, bytes: &line("second") };
        assert!(en.take(&later, LF, ROOM).is_err());
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"first\n");
    }

    /// Records are numbered from zero within an extent, and an append is told which
    /// ones it closed.
    #[test]
    fn an_append_is_told_the_records_it_closed() {
        let en = EN::new(testing::en_config());
        let first = append(&en, 1, &line("one"));
        assert_eq!((first.first_record, first.records), (0, 1));
        let pair = append(&en, 1, b"two\nthree\n");
        assert_eq!((pair.first_record, pair.records), (1, 2));
        assert_eq!(en.held.lock().unwrap().extents[0].records, 3);
    }

    /// A record may span appends: the block that starts it closes nothing, and the
    /// block that finishes it gets the number.
    #[test]
    fn a_record_spanning_appends_is_numbered_by_the_append_that_closes_it() {
        let en = EN::new(testing::en_config());
        let opened = append(&en, 1, b"half a rec");
        assert_eq!(opened.records, 0, "nothing is closed yet");
        let closed = append(&en, 1, b"ord\n");
        assert_eq!((closed.first_record, closed.records), (0, 1));
        let held = en.held.lock().unwrap();
        assert_eq!(held.record(0, 1, LF, 0).unwrap(), b"half a record");
        assert_eq!(held.extents[0].records, 1);
    }

    /// A block may not close records and then end inside another: that is what
    /// keeps a seal from ever having to cut one in half.
    #[test]
    fn a_block_that_closes_a_record_may_not_end_inside_another() {
        let en = EN::new(testing::en_config());
        assert!(en.take(&appending(0, 1, b"one\ntw"), LF, ROOM).is_err());
        // And the refusal left the extent alone.
        assert!(en.held.lock().unwrap().read(0, 1).is_empty());
    }

    /// A secondary puts the append where the primary said, and does not choose.
    #[test]
    fn an_applied_append_keeps_the_offset_it_was_given() {
        let en = EN::new(testing::en_config());
        let request = Request::new(Op::ApplyAppend)
            .number(0)
            .number(1)
            .number(1_000_000)
            .number(64)
            .bytes(b"late".to_vec());
        assert_eq!(en.applied(&request).status, Status::Ok);
        let held = en.held.lock().unwrap();
        assert_eq!(held.appends[0].offset, COff::new(64));
        // The end moves past it, or this instance could not take over as primary
        // and hand out the next offset.
        assert_eq!(held.extents[0].end, COff::new(68));
    }

    /// Out of order, the end is the furthest any append reached rather than the
    /// last one to arrive.
    #[test]
    fn applying_out_of_order_leaves_the_end_furthest_on() {
        let en = EN::new(testing::en_config());
        for offset in [100u64, 10] {
            let request = Request::new(Op::ApplyAppend)
                .number(0)
                .number(1)
                .number(1_000_000)
                .number(offset)
                .bytes(vec![0u8; 4]);
            assert_eq!(en.applied(&request).status, Status::Ok);
        }
        assert_eq!(en.held.lock().unwrap().extents[0].end, COff::new(104));
    }

    #[test]
    fn an_append_without_bytes_is_a_bad_request() {
        let en = EN::new(testing::en_config());
        let request = Request::new(Op::ApplyAppend).number(0).number(1);
        assert_eq!(en.applied(&request).status, Status::BadRequest);
    }

    #[test]
    fn an_applied_append_needs_an_offset() {
        let en = EN::new(testing::en_config());
        let request =
            Request::new(Op::ApplyAppend).number(0).number(1).number(1_000_000).bytes(vec![1, 2]);
        assert_eq!(en.applied(&request).status, Status::BadRequest);
    }

    /// A read asks for one record by number, and gets its content without the
    /// terminator that found it.
    #[test]
    fn a_read_returns_one_record() {
        let en = EN::new(testing::en_config());
        append(&en, 1, b"one\ntwo\nthree\n");
        let (kind, width) = LF.wire();
        let read = |record: u64| {
            en.read(
                &Request::new(Op::ReadExtent)
                    .number(0)
                    .number(1)
                    .number(1_000_000)
                    .number(kind)
                    .number(width)
                    .number(record),
            )
        };
        assert_eq!(read(0).args, vec![Arg::Bytes(b"one".to_vec())]);
        assert_eq!(read(2).args, vec![Arg::Bytes(b"three".to_vec())]);
        // Past the end is not found rather than empty.
        assert_eq!(read(3).status, Status::NotFound);
    }

    /// An extent nobody here holds, and one whose second disagrees, are both
    /// answered the same way: that extent is not here.
    #[test]
    fn a_read_of_the_wrong_extent_is_not_found() {
        let en = EN::new(testing::en_config());
        append(&en, 1, &line("only"));
        let (kind, width) = LF.wire();
        let read = |extent: u64, created: u64| {
            en.read(
                &Request::new(Op::ReadExtent)
                    .number(0)
                    .number(extent)
                    .number(created)
                    .number(kind)
                    .number(width)
                    .number(0),
            )
            .status
        };
        assert_eq!(read(1, 1_000_000), Status::Ok);
        assert_eq!(read(2, 1_000_000), Status::NotFound, "no such extent");
        assert_eq!(read(1, 1_000_001), Status::NotFound, "a rewritten extent is a new one");
    }

    /// An extent that has no room for the next block is closed rather than stretched,
    /// and the answer says what it came to.
    #[test]
    fn a_full_extent_seals_instead_of_taking_the_block() {
        let en = EN::new(testing::en_config());
        let cap = COff::new(10);
        let took = |en: &EN, bytes: &'static [u8]| en.take(&appending(0, 1, bytes), LF, cap);
        // "one\n" and "two\n" fit in ten bytes; "three\n" does not.
        assert!(matches!(took(&en, b"one\n"), Ok(Took::Landed(_))));
        assert!(matches!(took(&en, b"two\n"), Ok(Took::Landed(_))));
        match took(&en, b"three\n") {
            Ok(Took::Sealed { records, length, .. }) => {
                assert_eq!(records, 2, "what the extent came to");
                assert_eq!(length, COff::new(8));
            }
            other => panic!("should have sealed, not {:?}", other.is_ok()),
        }
        // And it stays sealed: the block that found it full does not land later.
        assert!(matches!(took(&en, b"x\n"), Ok(Took::Sealed { .. })));
        // What was in it is still readable.
        assert_eq!(en.held.lock().unwrap().record(0, 1, LF, 1).unwrap(), b"two");
    }

    /// An extent with a record still open cannot seal, however full it is: an extent
    /// holds whole records, so the open one has to finish here.
    #[test]
    fn an_open_record_keeps_an_extent_from_sealing() {
        let en = EN::new(testing::en_config());
        let cap = COff::new(4);
        // A block with no boundary in it, which leaves a record open.
        assert!(matches!(en.take(&appending(0, 1, b"half"), LF, cap), Ok(Took::Landed(_))));
        // Past the cap, and it still has to be taken, because the record is open.
        match en.take(&appending(0, 1, b" and half\n"), LF, cap) {
            Ok(Took::Landed(landed)) => assert_eq!(landed.records, 1),
            _ => panic!("an open record must be allowed to finish"),
        }
        assert_eq!(en.held.lock().unwrap().record(0, 1, LF, 0).unwrap(), b"half and half");
        // Now that nothing is open, the next block finds it full.
        assert!(matches!(en.take(&appending(0, 1, b"x\n"), LF, cap), Ok(Took::Sealed { .. })));
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

    /// Two failures, and three of four is out of reach: say so rather than wait
    /// for an answer that cannot add up.
    #[test]
    fn two_failures_lose_the_append() {
        let quorum = testing::extents().quorum;
        let mut counting = Counting::new(testing::extents().instances - 1, quorum);
        assert_eq!(counting.answer(false), None);
        assert_eq!(counting.answer(false), Some(Verdict::Lost));
    }

    /// An append that could only be sent to one other instance can never reach
    /// three of four, and that is known before any answer arrives.
    #[test]
    fn too_few_instances_to_ask_is_hopeless_from_the_start() {
        assert!(!Counting::new(1, 3).possible());
        assert!(Counting::new(2, 3).possible());
    }
}
