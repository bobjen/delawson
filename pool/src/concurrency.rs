//! Calibrated contention coverage for the bitmap pool.
//!
//! This follows the coverage-driven method described at
//! https://burtleburtle.net/bob/testing/concurrencyflags.html: measure each
//! condition's chance of appearing in one independent trial, then run enough
//! fresh trials that the union-bound chance of missing any condition is at
//! most 2^-40.

use super::bitmap::{Race, clear_hits, take_hits};
use super::{Block, Pool, PoolItem};
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const THREADS: usize = 8;
const INITIAL: u32 = 64;
const MAX_BLOCKS: u32 = 256;
const MAX_HELD: usize = 32;
const RANDOM_PROBES: usize = 16;
const TRIAL_TIME: Duration = Duration::from_millis(3);
const CALIB_TIME: Duration = Duration::from_secs(5);
const FAILURE_TARGET: f64 = 1.0 / ((1u64 << 40) as f64);
const CASES: usize = Race::ALL.len();
const ALL_FLAGS: u64 = (1u64 << CASES) - 1;
type Hits = u64;

#[repr(C, align(8))]
struct Item {
    owner: AtomicU64,
}

unsafe impl PoolItem for Item {
    unsafe fn init(slot: *mut Self) {
        // SAFETY: the pool calls init once for a fresh, aligned Item slot.
        unsafe { slot.write(Self { owner: AtomicU64::new(0) }) };
    }
}

type TestPool = Pool<Item, 8>;
type TestBlock<'a> = Block<'a, Item, 8>;

fn owner<'a>(block: &'a TestBlock<'_>) -> &'a AtomicU64 {
    // SAFETY: every returned block contains an initialized Item and is uniquely
    // owned until it is dropped. The atomic remains valid while the handle lives.
    unsafe { &(*block.as_ptr().cast::<Item>()).owner }
}

fn fail(failed: &AtomicBool, error: &mut Option<String>, message: String) {
    failed.store(true, Ordering::Relaxed);
    if error.is_none() {
        *error = Some(message);
    }
}

fn worker(pool: &TestPool, failed: &AtomicBool, id: u64) -> (Hits, Option<String>) {
    let mut held: Vec<TestBlock<'_>> = Vec::with_capacity(MAX_HELD);
    let mut error = None;

    let end = Instant::now() + TRIAL_TIME;

    while Instant::now() < end && !failed.load(Ordering::Relaxed) {
        let allocate = held.is_empty() || (held.len() < MAX_HELD && random::thread_u64() & 1 == 0);
        if allocate {
            match pool.alloc() {
                Ok(block) => {
                    let index = block.index();
                    if owner(&block)
                        .compare_exchange(0, id, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        fail(
                            failed,
                            &mut error,
                            format!("slot {index} was handed out while still owned"),
                        );
                        std::mem::forget(block);
                        break;
                    }
                    held.push(block);
                }
                Err(_) => std::hint::spin_loop(),
            }
        } else {
            let at = random::thread_u64() as usize % held.len();
            let block = held.swap_remove(at);
            let index = block.index();
            if owner(&block).compare_exchange(id, 0, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                fail(
                    failed,
                    &mut error,
                    format!("thread {id} no longer owned slot {index} when returning it"),
                );
                std::mem::forget(block);
                break;
            }
            drop(block);
        }
    }

    for block in held {
        let index = block.index();
        if owner(&block).compare_exchange(id, 0, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            fail(
                failed,
                &mut error,
                format!("thread {id} no longer owned slot {index} during cleanup"),
            );
            std::mem::forget(block);
        } else {
            drop(block);
        }
    }

    (take_hits(), error)
}

enum Cmd {
    Run(Arc<TestPool>, Arc<AtomicBool>),
    Stop,
}

struct Harness {
    cmds: Vec<Sender<Cmd>>,
    done: Receiver<(Hits, Option<String>)>,
    start: Arc<Barrier>,
    joins: Vec<JoinHandle<()>>,
}

impl Harness {
    fn new() -> Self {
        let start = Arc::new(Barrier::new(THREADS + 1));
        let ready = Arc::new(Barrier::new(THREADS + 1));
        let (done_tx, done) = mpsc::channel();
        let mut cmds = Vec::with_capacity(THREADS);
        let mut joins = Vec::with_capacity(THREADS);

        for id in 0..THREADS {
            let (cmd_tx, cmd_rx) = mpsc::channel();
            let done_tx = done_tx.clone();
            let start = Arc::clone(&start);
            let ready = Arc::clone(&ready);
            joins.push(std::thread::spawn(move || {
                // Initialize the regular per-thread RNG before calibration;
                // it remains attached to this worker for every trial.
                let _ = random::thread_u64();
                ready.wait();
                while let Ok(cmd) = cmd_rx.recv() {
                    match cmd {
                        Cmd::Run(pool, failed) => {
                            clear_hits();
                            start.wait();
                            let result = worker(&pool, &failed, id as u64 + 1);
                            done_tx.send(result).expect("test controller stopped");
                        }
                        Cmd::Stop => break,
                    }
                }
            }));
            cmds.push(cmd_tx);
        }

        // Ensure every worker has initialized its thread-owned RNG before the
        // first trial starts. Workers keep that RNG for the harness lifetime.
        ready.wait();
        Self { cmds, done, start, joins }
    }

    fn run_trial(&self) -> Hits {
        let pool =
            Arc::new(TestPool::with_random_probes(MAX_BLOCKS, INITIAL, RANDOM_PROBES).unwrap());
        let failed = Arc::new(AtomicBool::new(false));
        for cmd in &self.cmds {
            cmd.send(Cmd::Run(Arc::clone(&pool), Arc::clone(&failed)))
                .expect("pool worker stopped");
        }
        self.start.wait();

        let mut hits = 0u64;
        let mut errors = Vec::new();
        for _ in 0..THREADS {
            let (thread_hits, error) = self.done.recv().expect("pool worker stopped");
            hits |= thread_hits;
            if let Some(error) = error {
                errors.push(error);
            }
        }

        assert!(errors.is_empty(), "bitmap pool ownership violation: {errors:?}");
        let issued = pool.issued_blocks();
        assert!(issued <= MAX_BLOCKS);
        for index in 0..issued {
            assert!(pool.is_free(index).unwrap(), "slot {index} was lost after trial cleanup");
            let address = unsafe {
                pool.mapping.base.as_ptr().add(index as usize * std::mem::size_of::<Item>())
            };
            // SAFETY: every issued item was initialized and all block handles
            // have been returned before this audit.
            assert_eq!(unsafe { &*address.cast::<Item>() }.owner.load(Ordering::SeqCst), 0);
        }

        hits
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        for cmd in &self.cmds {
            let _ = cmd.send(Cmd::Stop);
        }
        for join in self.joins.drain(..) {
            join.join().expect("pool worker panicked");
        }
    }
}

fn miss_bound(trials: usize, rates: &[f64]) -> f64 {
    rates.iter().map(|&hit| if hit >= 1.0 { 0.0 } else { (1.0 - hit).powf(trials as f64) }).sum()
}

fn need_trials(rates: &[f64]) -> usize {
    let mut high = 1usize;
    while miss_bound(high, rates) > FAILURE_TARGET {
        high = high.checked_mul(2).expect("coverage trial count overflow");
    }
    let mut low = high / 2;
    while low + 1 < high {
        let mid = low + (high - low) / 2;
        if miss_bound(mid, rates) <= FAILURE_TARGET {
            high = mid;
        } else {
            low = mid;
        }
    }
    high
}

#[test]
fn bitmap_races() {
    let harness = Harness::new();
    let cal_start = Instant::now();
    let mut cal_trials = 0usize;
    let mut cond_trials = [0usize; CASES];
    while cal_start.elapsed() < CALIB_TIME {
        let hits = harness.run_trial();
        for (at, seen) in cond_trials.iter_mut().enumerate() {
            *seen += usize::from(hits & (1u64 << at) != 0);
        }
        cal_trials += 1;
    }
    let cal_time = cal_start.elapsed();

    let rates: Vec<f64> = cond_trials
        .iter()
        .enumerate()
        .map(|(at, &seen)| {
            assert!(
                seen != 0,
                "condition '{}' did not occur in {} calibration trials",
                Race::ALL[at].name(),
                cal_trials
            );
            seen as f64 / cal_trials as f64
        })
        .collect();
    let required = need_trials(&rates);
    let mut covered = 0u64;
    let valid_start = Instant::now();
    let mut valid_trials = 0usize;
    for _ in 0..required {
        let hits = harness.run_trial();
        valid_trials += 1;
        covered |= hits;
        if covered == ALL_FLAGS {
            break;
        }
    }

    println!(
        "bitmap race calibration: {cal_trials} trials in {:.2}s; validation: {valid_trials}/{required} trials in {:.2}s; estimated miss probability <= {:.3e}",
        cal_time.as_secs_f64(),
        valid_start.elapsed().as_secs_f64(),
        miss_bound(required, &rates),
    );
    for (at, race) in Race::ALL.iter().enumerate() {
        println!(
            "  {:<25} {:>5}/{:<5} p={:.5} validation={}",
            race.name(),
            cond_trials[at],
            cal_trials,
            rates[at],
            if covered & (1u64 << at) != 0 { "hit" } else { "MISSED" },
        );
    }
    assert_eq!(covered, ALL_FLAGS, "validation missed a calibrated bitmap condition");
}
