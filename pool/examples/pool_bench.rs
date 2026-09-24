//! Production-path timing: no unit-test probes, histories, or invariant checks.
//! Sep 2026, 8 threads * 1M operations, 16 blocks, three runs (seconds):
//! external RwLock 2.840/2.500/2.595; CAS AtomicI64 1.709/2.544/2.549.
//! inline RwLock 2.437/2.451/2.836; CAS AtomicI64 2.400/2.414/2.489.
//! Ranges overlap; see DESIGN.md for the earlier fetch-add/rollback comparison.
//! Capped malloc/free: 0.262/0.264/0.268 seconds in a subsequent paired comparison.
//! Aligned allocation/free: 0.257/0.224/0.254 seconds; concurrent comparison malloc
//! 0.203/0.269/0.271. Those historical measurements used a shared capacity counter.
//! The aligned mode requests 4KiB alignment; malloc does not.
//! Pool capacity defaults to 1,000,000 blocks, starting empty. System allocation
//! now has no capacity limit or shared allocation counter.
//! Earlier measurements above explicitly used 16 blocks.
//! Compare fixed work, not the coverage tests' timed loops.
use common::random::Rng;
use pool::{ExternalLinks, InlineLinks, Pool};
use std::ptr::NonNull;
use std::sync::Barrier;
use std::time::{Duration, Instant};

impl MallocBlock {
    fn alloc<const ALIGNED: bool>() -> common::Result<Self> {
        // SAFETY: 4096 is a valid alignment and size is a multiple of alignment.
        // Both allocation functions return storage releasable with free.
        let raw =
            unsafe { if ALIGNED { libc::aligned_alloc(4096, 4096) } else { libc::malloc(4096) } };
        let Some(ptr) = NonNull::new(raw) else {
            return Err(common::Error::new("system allocation failed"));
        };
        Ok(MallocBlock { ptr })
    }
}

// Ownership pairs each system allocation with exactly one free.
struct MallocBlock {
    ptr: NonNull<libc::c_void>,
}

impl Drop for MallocBlock {
    fn drop(&mut self) {
        // SAFETY: this pointer came from malloc/aligned_alloc and has not been freed.
        unsafe { libc::free(self.ptr.as_ptr()) };
    }
}

fn run<T>(
    alloc: impl Fn(&mut Rng) -> Option<T> + Sync,
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
                let mut bucket_rng =
                    Rng::from_seed(seed.wrapping_add(thread as u64) ^ 0x76543210abcdef98);
                let mut held = Vec::with_capacity(blocks as usize);
                let mut counts = [0u64; 3];
                ready.wait();
                start.wait();
                for _ in 0..operations {
                    if held.is_empty() || rng.next_u32() & 1 == 0 {
                        match alloc(&mut bucket_rng) {
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
    let blocks = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(1_000_000);
    let seed = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(42);
    let bucket_count: usize = if layout.contains("64") { 64 } else { 32 };
    let mask = bucket_count - 1;
    let shift = bucket_count.trailing_zeros();
    let mut issued = None;
    let (elapsed, counts) = match layout {
        "external" | "external32" | "external32x2" | "external32x4" | "external32x4all"
        | "external64x4all" => {
            let pool = Pool::<4096, 4096, ExternalLinks>::with_buckets(
                blocks,
                0,
                if layout == "external" { 1 } else { bucket_count },
            )
            .unwrap();
            let result = if layout == "external32x2"
                || layout == "external32x4"
                || layout == "external32x4all"
                || layout == "external64x4all"
            {
                let choices = if layout.ends_with("x2") { 2 } else { 4 };
                run(
                    |rng| {
                        let val = rng.next_u64() as usize;
                        let first = (val ^ mask) & mask;
                        let delta = (((val >> shift) & (bucket_count / 2 - 1)) << 1) | 1;
                        let walk = (0..choices).map(|step| (first + step * delta) & mask);
                        if layout.ends_with("all") {
                            pool.alloc_reclaiming_each(walk).ok()
                        } else {
                            pool.alloc_from_buckets(walk).ok()
                        }
                    },
                    threads,
                    blocks,
                    operations,
                    seed,
                )
            } else if layout == "external32" {
                run(
                    |rng| pool.alloc_from_bucket(rng.next_u64() as usize).ok(),
                    threads,
                    blocks,
                    operations,
                    seed,
                )
            } else {
                run(|_| pool.alloc().ok(), threads, blocks, operations, seed)
            };
            issued = Some(pool.issued_blocks());
            result
        }
        "inline" | "inline32" | "inline32x2" | "inline32x4" | "inline32x4all" | "inline64x4all" => {
            let pool = Pool::<4096, 4096, InlineLinks<8>>::with_buckets(
                blocks,
                0,
                if layout == "inline" { 1 } else { bucket_count },
            )
            .unwrap();
            let result = if layout == "inline32x2"
                || layout == "inline32x4"
                || layout == "inline32x4all"
                || layout == "inline64x4all"
            {
                let choices = if layout.ends_with("x2") { 2 } else { 4 };
                run(
                    |rng| {
                        let val = rng.next_u64() as usize;
                        let first = (val ^ mask) & mask;
                        let delta = (((val >> shift) & (bucket_count / 2 - 1)) << 1) | 1;
                        let walk = (0..choices).map(|step| (first + step * delta) & mask);
                        if layout.ends_with("all") {
                            pool.alloc_reclaiming_each(walk).ok()
                        } else {
                            pool.alloc_from_buckets(walk).ok()
                        }
                    },
                    threads,
                    blocks,
                    operations,
                    seed,
                )
            } else if layout == "inline32" {
                run(
                    |rng| pool.alloc_from_bucket(rng.next_u64() as usize).ok(),
                    threads,
                    blocks,
                    operations,
                    seed,
                )
            } else {
                run(|_| pool.alloc().ok(), threads, blocks, operations, seed)
            };
            issued = Some(pool.issued_blocks());
            result
        }
        "malloc" | "aligned" => {
            // No shared counter or capacity limit for system allocation. The
            // blocks argument only reserves worker Vec capacity in these modes.
            if layout == "aligned" {
                let sample = MallocBlock::alloc::<true>().unwrap();
                assert_eq!(sample.ptr.as_ptr() as usize % 4096, 0);
                drop(sample);
                run(|_| MallocBlock::alloc::<true>().ok(), threads, blocks, operations, seed)
            } else {
                run(|_| MallocBlock::alloc::<false>().ok(), threads, blocks, operations, seed)
            }
        }
        _ => panic!(
            "layout must be external, external32, external32x2, external32x4, external32x4all, external64x4all, inline, inline32, inline32x2, inline32x4, inline32x4all, inline64x4all, malloc or aligned"
        ),
    };
    println!(
        "{layout},{threads},{blocks},{operations},{:.6},{},{},{},{}",
        elapsed.as_secs_f64() * 1000.0,
        counts[0],
        counts[1],
        counts[2],
        issued.map(|n| n.to_string()).unwrap_or_default()
    );
}
