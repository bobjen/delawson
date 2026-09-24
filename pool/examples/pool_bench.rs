//! Production-path timing: no unit-test probes, histories, or invariant checks.
//! Sep 2026, 8 threads * 1M operations, 16 blocks, three runs (seconds):
//! external RwLock 2.840/2.500/2.595; CAS AtomicI64 1.709/2.544/2.549.
//! inline RwLock 2.437/2.451/2.836; CAS AtomicI64 2.400/2.414/2.489.
//! Ranges overlap; see DESIGN.md for the earlier fetch-add/rollback comparison.
//! Capped malloc/free: 0.262/0.264/0.268 seconds in a subsequent paired comparison.
//! Aligned allocation/free: 0.257/0.224/0.254 seconds; concurrent comparison malloc
//! 0.203/0.269/0.271. All modes enforce the global block limit.
//! The aligned mode requests 4KiB alignment; malloc does not.
//! Compare fixed work, not the coverage tests' timed loops.
use common::random::Rng;
use pool::{ExternalLinks, InlineLinks, Pool};
use std::ptr::NonNull;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use std::time::{Duration, Instant};

struct MallocPool {
    live: AtomicU32,
    max: u32,
}

impl MallocPool {
    fn alloc<const ALIGNED: bool>(&self) -> common::Result<MallocBlock<'_>> {
        self.live.fetch_update(SeqCst, SeqCst, |n| (n < self.max).then(|| n + 1)).map_err(
            |_| common::Error::new("block pool exhausted; some blocks may await reclamation"),
        )?;
        // SAFETY: 4096 is a valid alignment and size is a multiple of alignment.
        // Both allocation functions return storage releasable with free.
        let raw =
            unsafe { if ALIGNED { libc::aligned_alloc(4096, 4096) } else { libc::malloc(4096) } };
        let Some(ptr) = NonNull::new(raw) else {
            self.live.fetch_sub(1, SeqCst);
            return Err(common::Error::new("system allocation failed"));
        };
        Ok(MallocBlock { ptr, pool: self })
    }
}

// Ownership pairs each system allocation with exactly one free.
struct MallocBlock<'a> {
    ptr: NonNull<libc::c_void>,
    pool: &'a MallocPool,
}

impl Drop for MallocBlock<'_> {
    fn drop(&mut self) {
        // SAFETY: this pointer came from malloc/aligned_alloc and has not been freed.
        unsafe { libc::free(self.ptr.as_ptr()) };
        self.pool.live.fetch_sub(1, SeqCst);
    }
}

fn run<T>(
    alloc: impl Fn() -> Option<T> + Sync,
    threads: usize,
    blocks: u32,
    operations: usize,
    seed: u64,
) -> (Duration, [u64; 3]) {
    let ready = Barrier::new(threads + 1);
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);
    std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(threads);
        for thread in 0..threads {
            let alloc = &alloc;
            let ready = &ready;
            let start = &start;
            let done = &done;
            workers.push(scope.spawn(move || {
                let mut rng = Rng::from_seed(seed.wrapping_add(thread as u64));
                let mut held = Vec::with_capacity(blocks as usize);
                let mut counts = [0u64; 3];
                ready.wait();
                start.wait();
                for _ in 0..operations {
                    if held.is_empty() || rng.next_u32() & 1 == 0 {
                        match alloc() {
                            Some(block) => {
                                held.push(block);
                                counts[0] += 1;
                            }
                            None => counts[1] += 1,
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
    let args: Vec<_> = std::env::args().collect();
    let layout = args.get(1).map(String::as_str).unwrap_or("external");
    let threads = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(8);
    let operations = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1_000_000);
    let blocks = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(16);
    let seed = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(42);
    let (elapsed, counts) = match layout {
        "external" => {
            let pool = Pool::<4096, 4096, ExternalLinks>::new(blocks, 0).unwrap();
            run(|| pool.alloc().ok(), threads, blocks, operations, seed)
        }
        "inline" => {
            let pool = Pool::<4096, 4096, InlineLinks<8>>::new(blocks, 0).unwrap();
            run(|| pool.alloc().ok(), threads, blocks, operations, seed)
        }
        "malloc" | "aligned" => {
            let pool = MallocPool { live: AtomicU32::new(0), max: blocks };
            let result = if layout == "aligned" {
                // Verify alignment outside the measured workload.
                if blocks > 0 {
                    let sample = pool.alloc::<true>().unwrap();
                    assert_eq!(sample.ptr.as_ptr() as usize % 4096, 0);
                }
                run(|| pool.alloc::<true>().ok(), threads, blocks, operations, seed)
            } else {
                run(|| pool.alloc::<false>().ok(), threads, blocks, operations, seed)
            };
            assert_eq!(pool.live.load(SeqCst), 0);
            result
        }
        _ => panic!("layout must be external, inline, malloc or aligned"),
    };
    println!(
        "{layout},{threads},{blocks},{operations},{:.6},{},{},{}",
        elapsed.as_secs_f64() * 1000.0,
        counts[0],
        counts[1],
        counts[2]
    );
}
