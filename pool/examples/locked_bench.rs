//! LockedPool timing with exclusive operation batches and periodic reclamation.
use parking_lot::Mutex;
use pool::LockedPool;
use random::Rng;
use std::mem::MaybeUninit;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 16_000_000;
const OPS_PER_LOCK: usize = 32;
const MAX_BLOCKS: u32 = 1_000_000;
const RECLAIM_PERIOD_US: u64 = 50;
const RECLAIM_PERIOD: Duration = Duration::from_micros(RECLAIM_PERIOD_US);
const SEED: u64 = 42;

#[repr(C, align(4096))]
struct BenchBlock {
    link: u32,
    bytes: [MaybeUninit<u8>; 4092],
}

// SAFETY: the pool initializes `link` before exposing a fresh BenchBlock; the
// remaining fields are MaybeUninit.
unsafe impl pool::PoolItem for BenchBlock {}
unsafe impl pool::LockedItem for BenchBlock {}

type TestPool = LockedPool<BenchBlock, 4096, { std::mem::offset_of!(BenchBlock, link) }>;

fn run_pool(pool: &Mutex<TestPool>) -> (Duration, [u64; 3], u64) {
    let ready = Barrier::new(THREADS + 2);
    let start = Barrier::new(THREADS + 2);
    let done = Barrier::new(THREADS + 1);
    let stop = AtomicBool::new(false);
    let reclaims = AtomicU64::new(0);

    thread::scope(|scope| {
        let ready_ref = &ready;
        let start_ref = &start;
        let stop_ref = &stop;
        let reclaims_ref = &reclaims;
        let pool_ref = pool;
        let gc = scope.spawn(move || {
            ready_ref.wait();
            start_ref.wait();
            while !stop_ref.load(Ordering::Acquire) {
                let deadline = Instant::now() + RECLAIM_PERIOD;
                {
                    pool_ref.lock().reclaim();
                    reclaims_ref.fetch_add(1, Ordering::Relaxed);
                }
                while !stop_ref.load(Ordering::Acquire) && Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            }
        });

        let mut workers = Vec::with_capacity(THREADS);
        for thread_id in 0..THREADS {
            let ready_ref = ready_ref;
            let start_ref = start_ref;
            let done = &done;
            let pool_ref = pool;
            workers.push(scope.spawn(move || {
                let mut rng = Rng::from_seed(SEED.wrapping_add(thread_id as u64));
                let mut held = Vec::with_capacity(MAX_BLOCKS as usize);
                let mut counts = [0u64; 3];
                ready_ref.wait();
                start_ref.wait();
                assert_eq!(OPS_PER_THREAD % OPS_PER_LOCK, 0);
                for _ in 0..OPS_PER_THREAD / OPS_PER_LOCK {
                    let mut pool = pool_ref.lock();
                    for _ in 0..OPS_PER_LOCK {
                        if held.is_empty() || rng.next_u32() & 1 == 0 {
                            match pool.alloc() {
                                Ok(index) => {
                                    held.push(index);
                                    counts[0] += 1;
                                }
                                Err(_) => counts[1] += 1,
                            }
                        } else {
                            let at = rng.next_u32() as usize % held.len();
                            let index = held.swap_remove(at);
                            // SAFETY: this thread owns this allocated index and returns it once.
                            unsafe { pool.retire_index(index) };
                            counts[2] += 1;
                        }
                    }
                }
                {
                    let mut pool = pool_ref.lock();
                    for index in held.drain(..) {
                        // SAFETY: this thread owns this allocated index and returns it once.
                        unsafe { pool.retire_index(index) };
                        counts[2] += 1;
                    }
                }
                done.wait();
                counts
            }));
        }

        ready.wait();
        let begin = Instant::now();
        start.wait();
        done.wait();
        let elapsed = begin.elapsed();
        stop.store(true, Ordering::Release);
        gc.join().unwrap();

        let mut totals = [0; 3];
        for worker in workers {
            for (total, count) in totals.iter_mut().zip(worker.join().unwrap()) {
                *total += count;
            }
        }
        (elapsed, totals, reclaims.load(Ordering::Relaxed))
    })
}

fn main() {
    let pool = Mutex::new(TestPool::new(MAX_BLOCKS, 0).unwrap());
    let (elapsed, counts, reclaims) = run_pool(&pool);
    println!(
        "pool=locked buckets=128 threads={THREADS} ops_per_thread={OPS_PER_THREAD} ops_per_lock={OPS_PER_LOCK} max_blocks={MAX_BLOCKS} reclaim_period_us={RECLAIM_PERIOD_US} elapsed_ms={:.3} allocs={} failed={} frees={} issued={} reclaim_cycles={} cycles_per_sec={:.0}",
        elapsed.as_secs_f64() * 1000.0,
        counts[0],
        counts[1],
        counts[2],
        pool.lock().issued_blocks(),
        reclaims,
        reclaims as f64 / elapsed.as_secs_f64(),
    );
}
