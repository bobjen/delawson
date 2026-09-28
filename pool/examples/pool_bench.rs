//! Production-path timing: the workload is set by constants below, while the
//! bucket count and probe count come directly from pool/src/lib.rs. Change those
//! pool constants, rebuild in release mode, and run this example for a new test.
//! This benchmark intentionally has no command-line selectors.
use pool::{DEFAULT_BUCKETS, DEFAULT_CHOICES, ExternalLinks, Pool};
use random::Rng;
use std::sync::Barrier;
use std::time::{Duration, Instant};

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 16_000_000;
const MAX_BLOCKS: u32 = 1_000_000;
const SEED: u64 = 42;

fn run(pool: &Pool<[u8; 4096], 4096, ExternalLinks>) -> (Duration, [u64; 3]) {
    let ready = Barrier::new(THREADS + 1);
    let start = Barrier::new(THREADS + 1);
    let done = Barrier::new(THREADS + 1);
    std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(THREADS);
        for thread in 0..THREADS {
            let ready = &ready;
            let start = &start;
            let done = &done;
            workers.push(scope.spawn(move || {
                let mut rng = Rng::from_seed(SEED.wrapping_add(thread as u64));
                let mut held = Vec::with_capacity(MAX_BLOCKS as usize);
                let mut counts = [0u64; 3];
                ready.wait();
                start.wait();
                for _ in 0..OPS_PER_THREAD {
                    if held.is_empty() || rng.next_u32() & 1 == 0 {
                        match pool.alloc() {
                            Ok(block) => {
                                held.push(block);
                                counts[0] += 1;
                            }
                            Err(_) => counts[1] += 1,
                        }
                    } else {
                        let at = rng.next_u32() as usize % held.len();
                        drop(held.swap_remove(at));
                        counts[2] += 1;
                    }
                }
                counts[2] += held.len() as u64;
                drop(held);
                done.wait();
                counts
            }));
        }
        ready.wait();
        let begin = Instant::now();
        start.wait();
        done.wait();
        let elapsed = begin.elapsed();
        let mut totals = [0; 3];
        for worker in workers {
            for (total, count) in totals.iter_mut().zip(worker.join().unwrap()) {
                *total += count;
            }
        }
        (elapsed, totals)
    })
}

fn main() {
    assert!(DEFAULT_BUCKETS.is_power_of_two());
    assert!(DEFAULT_CHOICES > 0 && DEFAULT_CHOICES <= DEFAULT_BUCKETS);
    let pool = Pool::<[u8; 4096], 4096, ExternalLinks>::new(MAX_BLOCKS, 0).unwrap();
    let (elapsed, counts) = run(&pool);
    println!(
        "buckets={DEFAULT_BUCKETS} probes={DEFAULT_CHOICES} threads={THREADS} ops_per_thread={OPS_PER_THREAD} max_blocks={MAX_BLOCKS} elapsed_ms={:.3} allocs={} failed={} frees={} issued={}",
        elapsed.as_secs_f64() * 1000.0,
        counts[0],
        counts[1],
        counts[2],
        pool.issued_blocks(),
    );
}
