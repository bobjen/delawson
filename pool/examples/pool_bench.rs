//! Production-path timing for the fixed bitmap pool. The workload is set by
//! constants below; there are no runtime selectors.
use pool::Pool;
use random::Rng;
use std::sync::Barrier;
use std::time::{Duration, Instant};

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 16_000_000;
const MAX_BLOCKS: u32 = 1_000_000;
const RANDOM_PROBES: usize = 2840;
const SEED: u64 = 42;
const INDEX_ITEMS: usize = 65_536;
const INDEX_ROUNDS: usize = 32;
const INDEX_TRIALS: usize = 3;

macro_rules! run_test {
    ($name:ident, $pool:ty, $method:ident) => {
        run_test!(@impl $name, $pool, pool, pool.$method());
    };
    (@impl $name:ident, $pool:ty, $pool_var:ident, $call:expr) => {
        fn $name($pool_var: &$pool) -> (Duration, [u64; 3]) {
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
                                match $call {
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
    };
}

run_test!(run_pool, Pool<[u8; 4096], 4096>, alloc);

fn sum_indexes(indexes: &[u32]) -> u64 {
    indexes.iter().fold(0, |sum, &index| sum.wrapping_add(index as u64))
}

fn sum_pointers(pool: &Pool<[u8; 4096], 4096>, pointers: &[*const u8]) -> u64 {
    pointers.iter().fold(0, |sum, &ptr| sum.wrapping_add(pool.index_of(ptr).unwrap() as u64))
}

fn time_indexes(indexes: &[u32]) -> (Duration, u64) {
    let start = Instant::now();
    let mut sum = 0u64;
    for _ in 0..INDEX_ROUNDS {
        sum = sum.wrapping_add(sum_indexes(std::hint::black_box(indexes)));
    }
    (start.elapsed(), std::hint::black_box(sum))
}

fn time_pointers(pool: &Pool<[u8; 4096], 4096>, pointers: &[*const u8]) -> (Duration, u64) {
    let start = Instant::now();
    let mut sum = 0u64;
    for _ in 0..INDEX_ROUNDS {
        sum = sum.wrapping_add(sum_pointers(pool, std::hint::black_box(pointers)));
    }
    (start.elapsed(), std::hint::black_box(sum))
}

fn benchmark_indexes() {
    let pool = Pool::<[u8; 4096], 4096>::new(INDEX_ITEMS as u32, 1).unwrap();
    let mut blocks = Vec::with_capacity(INDEX_ITEMS);
    blocks.push(pool.alloc().unwrap());
    blocks.extend((1..INDEX_ITEMS).map(|_| pool.alloc_fresh().unwrap()));
    let indexes: Vec<_> = blocks.iter().map(|block| block.index()).collect();
    let pointers: Vec<_> = blocks.iter().map(|block| block.as_ptr() as *const u8).collect();
    let mut stored_ns = [0.0; INDEX_TRIALS];
    let mut derived_ns = [0.0; INDEX_TRIALS];
    let operations = (INDEX_ITEMS * INDEX_ROUNDS) as f64;

    for trial in 0..INDEX_TRIALS {
        let (stored, stored_sum, derived, derived_sum) = if trial % 2 == 0 {
            let (elapsed, sum) = time_indexes(&indexes);
            let (derived, derived_sum) = time_pointers(&pool, &pointers);
            (elapsed, sum, derived, derived_sum)
        } else {
            let (elapsed, sum) = time_pointers(&pool, &pointers);
            let (stored, stored_sum) = time_indexes(&indexes);
            (stored, stored_sum, elapsed, sum)
        };
        assert_eq!(stored_sum, derived_sum);
        stored_ns[trial] = stored.as_secs_f64() * 1e9 / operations;
        derived_ns[trial] = derived.as_secs_f64() * 1e9 / operations;
    }

    let mean = |samples: &[f64; INDEX_TRIALS]| samples.iter().sum::<f64>() / INDEX_TRIALS as f64;
    println!(
        "index_lookup blocks={INDEX_ITEMS} rounds={INDEX_ROUNDS} trials={INDEX_TRIALS} stored_ns={stored_ns:?} stored_mean_ns={:.2} rederived_ns={derived_ns:?} rederived_mean_ns={:.2}",
        mean(&stored_ns),
        mean(&derived_ns),
    );
}

fn main() {
    benchmark_indexes();
    let pool = Pool::<[u8; 4096], 4096>::with_random_probes(MAX_BLOCKS, 0, RANDOM_PROBES).unwrap();
    let block = pool.alloc().unwrap();
    assert_eq!(pool.index_of(block.as_ptr()).unwrap(), 0);
    drop(block);
    let (elapsed, counts) = run_pool(&pool);
    println!(
        "pool=bitmap interleave=4096 second_probe=first-popcnt-1 threads={THREADS} ops_per_thread={OPS_PER_THREAD} max_blocks={MAX_BLOCKS} random_probes={RANDOM_PROBES} elapsed_ms={:.3} allocs={} failed={} frees={} issued={}",
        elapsed.as_secs_f64() * 1000.0,
        counts[0],
        counts[1],
        counts[2],
        pool.issued_blocks(),
    );
}
