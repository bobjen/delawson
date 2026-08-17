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
use common::offset::COff;
use common::server::{self, Process, Reply, Service};
use common::wire::{Arg, Op, Request, Response, Status};
use common::{Error, Result, logw};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SERVES: &[Op] = &[Op::AppendExtent, Op::ApplyAppend];

/// An extent instance held here. Its name is the key it is found by — this SN,
/// this id — and the second, which is part of the name too.
struct Extent {
    created: u64,
    /// Where the next append to this extent will start. Compressed, because what
    /// arrives here is what a writer compressed and what a device will hold.
    end: COff,
}

/// One append, as it arrived. The offset is the primary's decision, so every
/// instance holds the same append at the same place.
struct Append {
    /// Which extent, as an index into the extent table.
    extent: usize,
    offset: COff,
    bytes: Vec<u8>,
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
        self.extents.push(Extent { created, end: COff::ZERO });
        let place = self.extents.len() - 1;
        self.at.insert((sn, id), place);
        Ok(place)
    }

    /// Take the next offset in an extent and put the bytes there. This is the one
    /// serialized step of an append; everything expensive happens after it.
    fn take(&mut self, appending: &Appending) -> Result<COff> {
        let length = COff::of_len(appending.bytes.len())?;
        let place = self.place(appending.sn, appending.extent, appending.created)?;
        let extent = &mut self.extents[place];
        let offset = extent.end;
        extent.end = offset.plus(length)?;
        self.appends.push(Append { extent: place, offset, bytes: appending.bytes.to_vec() });
        Ok(offset)
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

    /// The bytes of an extent, in order. Nothing in a running process reads an
    /// append back yet — there is no read operation — so for now this is what says
    /// the appends landed where they were meant to.
    #[allow(dead_code)]
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
    appends: AtomicU64,
    bytes: AtomicU64,
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
    /// neither is worth a queue hop.
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
            _ => reply.send(server::unbuilt(process, SERVES, &request)),
        }
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        let extents = self.held.lock().expect("extents").extents.len();
        vec![
            format!("extents {extents}"),
            format!("appends {}", self.appends.load(Ordering::Relaxed)),
            format!("appended_bytes {}", self.bytes.load(Ordering::Relaxed)),
            format!("media_class {}", self.config.media_class),
        ]
    }
}

impl EN {
    fn new(config: ENConfig) -> EN {
        EN {
            config,
            held: Mutex::new(Extents::new()),
            appends: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
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
        let length = appending.bytes.len() as u64;
        let offset = match self.take(&appending) {
            Ok(offset) => offset,
            Err(e) => return reply.send(Response::error(Status::BadRequest, e.to_string())),
        };
        reply.about(format_args!(
            "extent {} of sn {},offset {offset},bytes {length}",
            appending.extent, appending.sn
        ));

        // The middle arguments are the instance list. This process is one of them,
        // and has just applied it, so the rest are who else must hear about it.
        let mine = process.entry.number();
        let others: Vec<u32> = middle
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
            Arg::Number(offset.number()),
            Arg::Number(length),
        ];
        let quorum = Arc::new(Quorum::new(others.len(), receipt, reply));

        let apply = Request::new(Op::ApplyAppend)
            .number(appending.sn as u64)
            .number(appending.extent)
            .number(appending.created)
            .number(offset.number())
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

    fn take(&self, appending: &Appending) -> Result<COff> {
        let offset = self.held.lock().expect("extents").take(appending)?;
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(appending.bytes.len() as u64, Ordering::Relaxed);
        Ok(offset)
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
    fn new(others: usize) -> Counting {
        Counting {
            others,
            wanted: common::QUORUM.saturating_sub(1),
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

/// The count, plus the reply it answers when the count settles.
struct Quorum {
    counting: Mutex<Counting>,
    receipt: Vec<Arg>,
    reply: Mutex<Option<Reply>>,
}

impl Quorum {
    fn new(others: usize, receipt: Vec<Arg>, reply: Reply) -> Quorum {
        let counting = Counting::new(others);
        let quorum =
            Quorum { receipt, counting: Mutex::new(counting), reply: Mutex::new(Some(reply)) };
        // An append that cannot reach a quorum however well it goes is refused
        // now rather than after waiting for answers that cannot add up.
        if !quorum.counting.lock().expect("quorum").possible() {
            quorum.say(Verdict::Lost);
        }
        quorum
    }

    fn applied(&self, applied: bool) {
        let verdict = self.counting.lock().expect("quorum").answer(applied);
        if let Some(verdict) = verdict {
            self.say(verdict);
        }
    }

    fn say(&self, verdict: Verdict) {
        let Some(reply) = self.reply.lock().expect("quorum").take() else { return };
        match verdict {
            Verdict::Durable => {
                reply.send(Response { id: 0, status: Status::Ok, args: self.receipt.clone() })
            }
            Verdict::Lost => reply.send(Response::error(
                Status::Internal,
                format!(
                    "fewer than {} of {} instances applied it",
                    common::QUORUM,
                    common::INSTANCES
                ),
            )),
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

    fn appending<'a>(sn: u32, extent: u64, bytes: &'a [u8]) -> Appending<'a> {
        Appending { sn, extent, created: 1_000_000, bytes }
    }

    /// Offsets are handed out in order, each one where the last append ended.
    #[test]
    fn offsets_follow_one_another() {
        let en = EN::new(testing::en_config());
        assert_eq!(en.take(&appending(0, 1, b"hello")).unwrap(), COff::new(0));
        assert_eq!(en.take(&appending(0, 1, b" there")).unwrap(), COff::new(5));
        assert_eq!(en.take(&appending(0, 1, b"!")).unwrap(), COff::new(11));
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"hello there!");
    }

    /// Two extents are two byte streams, whatever order the appends arrive in.
    #[test]
    fn extents_do_not_share_offsets() {
        let en = EN::new(testing::en_config());
        assert_eq!(en.take(&appending(0, 1, b"one")).unwrap(), COff::new(0));
        assert_eq!(en.take(&appending(0, 2, b"two")).unwrap(), COff::new(0));
        assert_eq!(en.take(&appending(0, 1, b"more")).unwrap(), COff::new(3));
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"onemore");
        assert_eq!(en.held.lock().unwrap().read(0, 2), b"two");
    }

    /// The same extent id from two SNs is two extents: an id is a name only
    /// alongside the SN that gave it.
    #[test]
    fn an_extent_is_named_by_its_sn_too() {
        let en = EN::new(testing::en_config());
        assert_eq!(en.take(&appending(0, 1, b"from sn0")).unwrap(), COff::new(0));
        assert_eq!(en.take(&appending(1, 1, b"from sn1")).unwrap(), COff::new(0));
        assert_eq!(en.held.lock().unwrap().extents.len(), 2);
    }

    /// And the second is part of the name, so an append that disagrees about it is
    /// refused rather than added to the extent that is here.
    #[test]
    fn a_second_that_disagrees_is_refused() {
        let en = EN::new(testing::en_config());
        en.take(&appending(0, 1, b"first")).unwrap();
        let later = Appending { sn: 0, extent: 1, created: 1_000_001, bytes: b"second" };
        assert!(en.take(&later).is_err());
        assert_eq!(en.held.lock().unwrap().read(0, 1), b"first");
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

    /// The instance list is what is between the name and the bytes, and the bytes
    /// are not one of it.
    #[test]
    fn the_instances_are_the_arguments_between_the_name_and_the_bytes() {
        let request = Request::new(Op::AppendExtent)
            .number(0)
            .number(7)
            .number(1_000_000)
            .number(2)
            .number(3)
            .number(1)
            .bytes(b"body".to_vec());
        let (appending, middle) = Appending::of(&request.args).unwrap();
        assert_eq!((appending.sn, appending.extent, appending.created), (0, 7, 1_000_000));
        assert_eq!(appending.bytes, b"body");
        assert_eq!(middle.len(), 3);
    }

    /// Three of four, so two of the three secondaries settle it and the third's
    /// answer changes nothing.
    #[test]
    fn two_of_three_secondaries_make_it_durable() {
        let mut counting = Counting::new(common::INSTANCES - 1);
        assert_eq!(counting.answer(true), None);
        assert_eq!(counting.answer(true), Some(Verdict::Durable));
        assert_eq!(counting.answer(true), None, "the answer is sent once");
    }

    /// A straggler costs nothing: the quorum is reached by the other two.
    #[test]
    fn one_slow_instance_does_not_hold_it_up() {
        let mut counting = Counting::new(common::INSTANCES - 1);
        assert_eq!(counting.answer(false), None);
        assert_eq!(counting.answer(true), None);
        assert_eq!(counting.answer(true), Some(Verdict::Durable));
    }

    /// Two failures, and three of four is out of reach: say so rather than wait
    /// for an answer that cannot add up.
    #[test]
    fn two_failures_lose_the_append() {
        let mut counting = Counting::new(common::INSTANCES - 1);
        assert_eq!(counting.answer(false), None);
        assert_eq!(counting.answer(false), Some(Verdict::Lost));
    }

    /// An append that could only be sent to one other instance can never reach
    /// three of four, and that is known before any answer arrives.
    #[test]
    fn too_few_instances_to_ask_is_hopeless_from_the_start() {
        assert!(!Counting::new(1).possible());
        assert!(Counting::new(2).possible());
    }
}
