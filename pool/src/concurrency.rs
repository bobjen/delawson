//! Coverage-driven trials: https://burtleburtle.net/bob/testing/concurrencyflags.html
use super::*;
use common::random::Rng;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Barrier;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
#[repr(u32)]
pub(super) enum Case {
    Reused,
    Fresh,
    Exhausted,
    RetiredEmpty,
    RetiredNonempty,
    RecycleEmpty,
    SpliceEmpty,
    SpliceNonempty,
    PopRetry,
    RetireRetry,
    SpliceRetry,
    RecycleBusy,
}

const NAMES: [&str; 12] = [
    "allocate recycled block",
    "issue fresh block",
    "allocation exhausted",
    "return to empty deleted list",
    "return to nonempty deleted list",
    "recycle empty deleted list",
    "splice onto empty free list",
    "splice onto nonempty free list",
    "pop CAS retry",
    "return CAS retry",
    "splice CAS retry",
    "inactive list busy",
];
const CASES: usize = NAMES.len();
const ALL: u32 = (1 << CASES) - 1;
const THREADS: usize = 8;
const BLOCKS: usize = 16;
const HISTORY: usize = 4096;
// One fresh-pool loop per trial. Confidence comes from repetition, not padding.
// 1000 trials: rarest flag hit 856 externally and 999 inline (debug, Sep 2026).
const TRIAL_TIME: Duration = Duration::from_millis(3);
const TRIALS: u32 = 40;
// Separate watchdog: allow all forty full trials plus setup/teardown overhead.
const BUDGET: Duration = Duration::from_secs(1);

type TestPool<L> = Pool<4096, 4096, L>;
type TestBlock<'a, L> = Block<'a, 4096, 4096, L>;

thread_local! {
    static HITS: [Cell<u64>; CASES] = const { [const { Cell::new(0) }; CASES] };
}

#[derive(Default)]
pub(super) struct Probe {
    bits: AtomicU32,
    aborted: AtomicBool,
    deadline: Option<Instant>,
    coverage_end: OnceLock<Instant>,
    count_hits: bool,
}

impl Probe {
    pub(super) fn hit(&self, case: Case) {
        if self.count_hits {
            HITS.with(|hits| {
                let count = &hits[case as usize];
                count.set(count.get() + 1);
            });
        }
        let bit = 1 << case as u32;
        if self.bits.load(Relaxed) & bit == 0 {
            if self.coverage_end.get().is_some_and(|&end| Instant::now() >= end) {
                return;
            }
            self.bits.fetch_or(bit, Relaxed);
        }
    }

    pub(super) fn abort(&self) {
        self.aborted.store(true, Relaxed);
    }

    // Cooperative cancellation also covers a broken pool's retry loops. Once
    // cancelled, this test instance is discarded, never audited or used again.
    pub(super) fn cancelled(&self) -> bool {
        if self.aborted.load(Relaxed) {
            return true;
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            self.abort();
            return true;
        }
        false
    }
}

#[derive(Debug)]
struct Record {
    begin: usize,
    end: usize,
    op: char,
    index: u32,
}

struct Worker<'a, L: Links> {
    pool: &'a TestPool<L>,
    owners: &'a [AtomicU32; BLOCKS],
    sequence: &'a AtomicUsize,
    id: u32,
    history: Vec<Record>,
    error: Option<String>,
}

impl<L: Links> Worker<'_, L> {
    fn begin(&mut self, op: char, index: u32) {
        self.history.push(Record {
            begin: self.sequence.fetch_add(1, Relaxed),
            end: usize::MAX,
            op,
            index,
        });
    }

    fn end(&mut self, index: u32) {
        let record = self.history.last_mut().unwrap();
        record.index = index;
        record.end = self.sequence.fetch_add(1, Relaxed);
    }

    fn fail(&mut self, message: String) {
        self.error = Some(message);
        self.pool.probe.abort();
    }

    fn release(&mut self, block: TestBlock<'_, L>, marker: u32) {
        let index = block.index();
        self.begin('F', index);
        let observed = block.link().load(SeqCst);
        if observed != marker {
            self.fail(format!("block {index} changed while owned: {observed:#x} != {marker:#x}"));
        } else if let Err(owner) =
            self.owners[index as usize].compare_exchange(self.id, 0, SeqCst, SeqCst)
        {
            self.fail(format!("free of block {index}: owner {owner}, freeing thread {}", self.id));
        }
        if self.pool.probe.cancelled() {
            std::mem::forget(block);
        } else {
            drop(block);
        }
        self.end(index);
    }

    fn run(&mut self, seed: u64, end: Instant) {
        let mut rng = Rng::from_seed(seed);
        let mut held = Vec::with_capacity(BLOCKS);
        // Reserve history slots for releasing everything held at loop end.
        for op in 0..HISTORY - BLOCKS {
            if self.pool.probe.cancelled() || (op % 32 == 0 && Instant::now() >= end) {
                break;
            }
            if held.is_empty() || rng.next_u32() & 1 == 0 {
                self.begin('A', NONE);
                match self.pool.alloc() {
                    Ok(block) => {
                        let index = block.index();
                        self.end(index);
                        if index as usize >= BLOCKS {
                            self.fail(format!("allocated index {index} outside pool"));
                            std::mem::forget(block);
                            break;
                        }
                        if let Err(owner) =
                            self.owners[index as usize].compare_exchange(0, self.id, SeqCst, SeqCst)
                        {
                            self.fail(format!(
                                "block {index} allocated twice: owners {owner} and {}",
                                self.id
                            ));
                            std::mem::forget(block);
                            break;
                        }
                        let expected =
                            self.pool.mapping.base.as_ptr() as usize + index as usize * 4096;
                        if block.as_ptr() as usize != expected || expected % 4096 != 0 {
                            self.fail(format!("bad address for block {index}"));
                            std::mem::forget(block);
                            break;
                        }
                        // An allocated inline link may contain arbitrary application data.
                        let marker = rng.next_u32() | 0x8000_0000;
                        block.link().store(marker, SeqCst);
                        held.push((block, marker));
                    }
                    Err(_) => self.end(NONE),
                }
            } else {
                let at = rng.next_u32() as usize % held.len();
                let (block, marker) = held.swap_remove(at);
                self.release(block, marker);
            }
        }
        for (block, marker) in held {
            if self.pool.probe.cancelled() {
                std::mem::forget(block);
            } else {
                self.release(block, marker);
            }
        }
    }
}

struct Report {
    seed: u64,
    layout: &'static str,
    flags: u32,
    counts: [u64; CASES],
    history: Vec<Vec<Record>>,
    error: Option<String>,
}

impl Report {
    fn dump(&self) -> String {
        use std::fmt::Write;
        let mut out = format!(
            "layout={} loop_seed={} flags={:#x} error={:?}\n",
            self.layout, self.seed, self.flags, self.error
        );
        // Tickets bracket API calls; they do not claim a linearization order.
        for (thread, records) in self.history.iter().enumerate() {
            for record in records {
                writeln!(
                    out,
                    "t{} {}..{} {} {}",
                    thread + 1,
                    record.begin,
                    record.end,
                    record.op,
                    record.index
                )
                .unwrap();
            }
        }
        out
    }
}

fn audit<L: Links>(
    pool: &TestPool<L>,
    owners: &[AtomicU32; BLOCKS],
) -> std::result::Result<(), String> {
    if owners.iter().any(|owner| owner.load(SeqCst) != 0) {
        return Err("owner remains after all handles were returned".into());
    }
    let issued = pool.next.load(SeqCst) as usize;
    if issued > BLOCKS {
        return Err(format!("issued {issued} blocks, capacity {BLOCKS}"));
    }
    let mut seen = [false; BLOCKS];
    for list in 0..3 {
        let mut index = if list == 0 {
            pool.free.load(SeqCst)
        } else {
            pool.deleted[list - 1].head.load(SeqCst)
        };
        let mut tail = NONE;
        while index != NONE {
            if index as usize >= issued {
                return Err(format!("list {list} contains unissued index {index}"));
            }
            if std::mem::replace(&mut seen[index as usize], true) {
                return Err(format!("duplicate or cycle at index {index}, list {list}"));
            }
            tail = index;
            index = pool.link(index).load(SeqCst);
        }
        if list != 0 && pool.deleted[list - 1].tail.load(SeqCst) != tail {
            return Err(format!("incorrect tail for deleted list {}", list - 1));
        }
    }
    if seen[..issued].iter().any(|&present| !present) {
        return Err(format!("issued blocks lost: {seen:?}"));
    }
    Ok(())
}

fn run_loop<L: Links>(seed: u64, deadline: Instant, count_hits: bool) -> Report {
    let mut pool = TestPool::<L>::new(BLOCKS as u32, 0).unwrap();
    pool.probe.deadline = Some(deadline);
    pool.probe.count_hits = count_hits;
    let owners = std::array::from_fn(|_| AtomicU32::new(0));
    let sequence = AtomicUsize::new(0);
    let ready = Barrier::new(THREADS + 1);
    let end = OnceLock::new();
    let mut report = Report {
        seed,
        layout: std::any::type_name::<L>(),
        flags: 0,
        counts: [0; CASES],
        history: Vec::new(),
        error: None,
    };
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for thread in 0..THREADS {
            let mut worker = Worker {
                pool: &pool,
                owners: &owners,
                sequence: &sequence,
                id: thread as u32 + 1,
                history: Vec::with_capacity(HISTORY),
                error: None,
            };
            let ready = &ready;
            let end = &end;
            workers.push(scope.spawn(move || {
                ready.wait();
                let loop_end = *end.get_or_init(|| {
                    let loop_end = Instant::now() + TRIAL_TIME;
                    worker.pool.probe.coverage_end.set(loop_end).unwrap();
                    loop_end
                });
                let result = catch_unwind(AssertUnwindSafe(|| {
                    worker.run(seed.wrapping_add(thread as u64), loop_end)
                }));
                if let Err(error) = result {
                    worker.fail(format!(
                        "worker panic: {:?}",
                        error
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| error.downcast_ref::<&str>().copied())
                    ));
                }
                (
                    worker.history,
                    worker.error,
                    HITS.with(|hits| std::array::from_fn::<_, CASES, _>(|i| hits[i].get())),
                )
            }));
        }
        ready.wait();
        for worker in workers {
            let (history, error, counts) = worker.join().unwrap();
            for (total, count) in report.counts.iter_mut().zip(counts) {
                *total += count;
            }
            report.history.push(history);
            if error.is_some() {
                report.error = error;
            }
        }
    });
    report.flags = pool.probe.bits.load(Relaxed);
    if report.error.is_none() {
        report.error = if pool.probe.cancelled() {
            Some("trial cancelled or exceeded its deadline".into())
        } else {
            audit(&pool, &owners).err()
        };
    }
    report
}

fn missing(flags: u32) -> Vec<&'static str> {
    NAMES
        .iter()
        .enumerate()
        .filter_map(|(case, &name)| (flags & (1 << case) == 0).then_some(name))
        .collect()
}

fn seed() -> u64 {
    std::env::var("POOL_TEST_SEED")
        .map(|s| s.parse().expect("POOL_TEST_SEED must be a u64"))
        .unwrap_or_else(|_| Rng::new().next_u64())
}

fn randomized<L: Links>() {
    let seed = seed();
    let start = Instant::now();
    let deadline = start + BUDGET;
    let mut rng = Rng::from_seed(seed);
    let mut seen = 0;
    let mut last = None;
    for trial in 1..=TRIALS {
        let report = run_loop::<L>(rng.next_u64(), deadline, false);
        assert!(report.error.is_none(), "{}", report.dump());
        seen |= report.flags;
        last = Some(report);
        if seen == ALL {
            assert!(start.elapsed() < BUDGET, "coverage exceeded its deadline; seed={seed}");
            eprintln!(
                "pool coverage {}: 12/12 cases, trials={trial}, seed={seed}, elapsed={:?}",
                std::any::type_name::<L>(),
                start.elapsed()
            );
            return;
        }
    }
    panic!(
        "pool coverage missing {:?}; seed={seed}\n{}",
        missing(seen),
        last.as_ref().map(Report::dump).unwrap_or_default()
    );
}

#[test]
fn randomized_external_links() {
    randomized::<ExternalLinks>();
}

#[test]
fn randomized_inline_links() {
    randomized::<InlineLinks<8>>();
}

/// Forty trials give a <=2^-40 per-case miss probability only if each trial's
/// conditional hit probability is >=1/2. For 12 cases the union bound is 12*2^-40.
fn validate_windows<L: Links>() {
    let seed = seed();
    let trials: u32 =
        std::env::var("POOL_CALIBRATION_WINDOWS").map(|s| s.parse().unwrap()).unwrap_or(1000);
    assert!(trials > 0);
    let mut rng = Rng::from_seed(seed);
    let mut counts = [0u32; CASES];
    let mut elapsed = Duration::ZERO;
    let mut longest = Duration::ZERO;
    for _ in 0..trials {
        let start = Instant::now();
        let report = run_loop::<L>(rng.next_u64(), start + BUDGET, false);
        assert!(report.error.is_none(), "{}", report.dump());
        let flags = report.flags;
        let duration = start.elapsed();
        elapsed += duration;
        longest = longest.max(duration);
        for (bit, count) in counts.iter_mut().enumerate() {
            *count += u32::from(flags & (1 << bit) != 0);
        }
    }
    eprintln!(
        "pool calibration {} seed={seed}, {trials} single-loop trials, mean {:?}, max {:?}",
        std::any::type_name::<L>(),
        elapsed / trials,
        longest
    );
    for (name, count) in NAMES.iter().zip(counts) {
        eprintln!("{name:40} {count}/{trials}");
    }
    assert!(
        counts.iter().all(|&count| count * 2 >= trials),
        "a case missed the empirical 1/2 target"
    );
}

#[test]
#[ignore = "coverage frequency calibration"]
fn validate_external_windows() {
    validate_windows::<ExternalLinks>();
}

#[test]
#[ignore = "coverage frequency calibration"]
fn validate_inline_windows() {
    validate_windows::<InlineLinks<8>>();
}

fn measure_rates<L: Links>() {
    let seed = seed();
    let mut rng = Rng::from_seed(seed);
    for pass in 1..=3 {
        let start = Instant::now();
        let end = start + Duration::from_secs(1);
        let mut counts = [0u64; CASES];
        while Instant::now() < end {
            let report = run_loop::<L>(rng.next_u64(), end + Duration::from_millis(100), true);
            assert!(report.error.is_none(), "{}", report.dump());
            for (total, count) in counts.iter_mut().zip(report.counts) {
                *total += count;
            }
        }
        let elapsed = start.elapsed();
        eprintln!(
            "pool event rates {} pass={pass} seed={seed}, duration={elapsed:?}",
            std::any::type_name::<L>()
        );
        for (name, count) in NAMES.iter().zip(counts) {
            eprintln!("{name:40} {count} hits, {:.1}/sec", count as f64 / elapsed.as_secs_f64());
        }
        let rarest = *counts.iter().min().unwrap();
        assert!(rarest > 0, "cannot estimate a window for an unobserved event");
        let interval_ms = elapsed.as_secs_f64() * 1000.0 / rarest as f64;
        eprintln!(
            "rarest: mean interval {interval_ms:.3}ms, half-hit estimate {:.3}ms",
            interval_ms * std::f64::consts::LN_2
        );
    }
}

#[test]
#[ignore = "three one-second event-rate measurements"]
fn calibrate_external_links() {
    measure_rates::<ExternalLinks>();
}

#[test]
#[ignore = "three one-second event-rate measurements"]
fn calibrate_inline_links() {
    measure_rates::<InlineLinks<8>>();
}
