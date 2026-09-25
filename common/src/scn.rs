//! Process-wide system change numbers, approximately UTC nanoseconds.
//!
//! Allocation never reads a clock. Only the periodic synchronizer sets/clears
//! the stall flag. Small merges may temporarily run beyond the UTC allowance;
//! the next synchronization detects that. SCNs order changes, not commits.
use crate::{Error, Result};
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Scn(u64);
impl Scn {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn number(self) -> u64 {
        self.0
    }
    /// Compare by signed eight-byte wrapping subtraction. Callers must keep
    /// the whole range being compared within u64::MAX / 2 ticks. Older values
    /// outside that window have no valid ordering; there is no global Ord.
    pub fn compare(self, other: Self) -> std::cmp::Ordering {
        self.difference(other).cmp(&0)
    }
    fn difference(self, other: Self) -> i64 {
        self.0.wrapping_sub(other.0) as i64
    }
    fn ahead_by_more_than(self, other: Self, allowance: u64) -> bool {
        self.difference(other) > allowance as i64
    }
}
impl std::fmt::Display for Scn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub sync_interval_ms: u64,
    pub max_ahead_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self { sync_interval_ms: 10, max_ahead_ms: 60_000 }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.sync_interval_ms == 0 {
            return Err(Error::new("SCN sync_interval_ms must be positive"));
        }
        self.sync_interval_ms
            .checked_mul(1_000_000)
            .filter(|nanos| *nanos <= u64::MAX / 2)
            .ok_or_else(|| {
                Error::new("SCN synchronization interval exceeds the comparison window")
            })?;
        self.max_ahead_ms
            .checked_mul(1_000_000)
            .filter(|nanos| *nanos <= u64::MAX / 2)
            .ok_or_else(|| Error::new("SCN max_ahead_ms exceeds the comparison window"))?;
        Ok(())
    }
}

struct Generator {
    latest: AtomicU64,
    stalled: AtomicBool,
    max_ahead: u64,
    // Only stalled allocators and the synchronizer touch this mutex. Clearing
    // the flag under the same mutex as Condvar::wait prevents lost wakeups.
    wait: Mutex<()>,
    resumed: Condvar,
}
impl Generator {
    fn new(utc: u64, max_ahead: u64) -> Self {
        Self {
            latest: AtomicU64::new(utc),
            stalled: AtomicBool::new(false),
            max_ahead,
            wait: Mutex::new(()),
            resumed: Condvar::new(),
        }
    }
    fn current(&self) -> Scn {
        Scn(self.latest.load(Ordering::Relaxed))
    }
    fn is_stalled(&self) -> bool {
        self.stalled.load(Ordering::Acquire)
    }

    fn try_next(&self) -> Result<Option<Scn>> {
        let mut old = self.latest.load(Ordering::Relaxed);
        loop {
            if self.is_stalled() {
                return Ok(None);
            }
            let new = old.wrapping_add(1);
            match self.latest.compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(Some(Scn(new))),
                Err(current) => old = current,
            }
        }
    }
    fn next(&self) -> Result<Scn> {
        loop {
            if let Some(scn) = self.try_next()? {
                return Ok(scn);
            }
            let mut waiting = self.wait.lock().map_err(|_| Error::new("SCN wait lock poisoned"))?;
            while self.is_stalled() {
                waiting =
                    self.resumed.wait(waiting).map_err(|_| Error::new("SCN wait lock poisoned"))?;
            }
        }
    }
    fn advance_to(&self, incoming: Scn) -> Scn {
        let mut current = self.latest.load(Ordering::Relaxed);
        loop {
            if incoming.compare(Scn(current)) != std::cmp::Ordering::Greater {
                return Scn(current);
            }
            // fetch_max would choose the wrong side of a wrap.
            match self.latest.compare_exchange_weak(
                current,
                incoming.0,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return incoming,
                Err(newer) => current = newer,
            }
        }
    }
    fn merge_with(&self, incoming: Scn, utc: impl FnOnce() -> Result<u64>) -> Result<Scn> {
        let current = self.current();
        // An older value cannot advance us, including across wrap. Compare
        // before reading UTC, not after every merge.
        if incoming.ahead_by_more_than(current, self.max_ahead)
            && incoming.ahead_by_more_than(Scn(utc()?), self.max_ahead)
        {
            return Err(Error::new("merged SCN exceeds UTC plus max_ahead_ms"));
        }
        Ok(self.advance_to(incoming))
    }
    fn synchronize_with(&self, utc: impl FnOnce() -> Result<u64>) -> Result<()> {
        let now = utc();
        let _waiting = self.wait.lock().map_err(|_| Error::new("SCN wait lock poisoned"))?;
        let stalled = match &now {
            Ok(now) => {
                let latest = self.advance_to(Scn(*now));
                latest.ahead_by_more_than(Scn(*now), self.max_ahead)
            }
            // Stop allocation if UTC is unavailable. A later successful tick
            // can recover; never fabricate a zero timestamp or move backward.
            Err(_) => true,
        };
        let previous = self.stalled.swap(stalled, Ordering::AcqRel);
        if previous != stalled {
            crate::logn!(
                "SCN allocation stall changed",
                "stalled {},scn {}",
                stalled,
                self.current()
            );
        }
        if previous && !stalled {
            self.resumed.notify_all();
        }
        now.map(|_| ())
    }
}

fn utc_nanos() -> Result<u64> {
    utc_nanos_at(SystemTime::now())
}
fn utc_nanos_at(now: SystemTime) -> Result<u64> {
    let elapsed =
        now.duration_since(UNIX_EPOCH).map_err(|_| Error::new("UTC is before the Unix epoch"))?;
    // Deliberately keep the low eight bytes: UTC and logical increments wrap
    // together, and comparisons use the bounded signed-difference window.
    Ok(elapsed.as_nanos() as u64)
}
struct Global {
    generator: Arc<Generator>,
    config: Config,
}
static GLOBAL: OnceLock<Global> = OnceLock::new();
static INITIALIZING: Mutex<()> = Mutex::new(());

/// Initialize once, before service code can allocate SCNs. Repeated calls with
/// the same configuration are harmless; reconfiguration never resets the counter.
/// The one synchronizer thread lives until process exit.
pub fn initialize(config: Config) -> Result<()> {
    config.validate()?;
    let _initializing =
        INITIALIZING.lock().map_err(|_| Error::new("SCN initialization poisoned"))?;
    if let Some(global) = GLOBAL.get() {
        return if global.config == config {
            Ok(())
        } else {
            Err(Error::new("SCN already initialized with different parameters"))
        };
    }
    let generator = Arc::new(Generator::new(utc_nanos()?, config.max_ahead_ms * 1_000_000));
    let syncing = generator.clone();
    std::thread::Builder::new().name("scn-clock".into()).spawn(move || {
        let mut previous_error = None;
        loop {
            std::thread::sleep(Duration::from_millis(config.sync_interval_ms));
            let error = syncing.synchronize_with(utc_nanos).err().map(|e| e.to_string());
            if error != previous_error {
                if let Some(error) = &error {
                    crate::logn!("SCN clock synchronization failed", "error {}", error);
                } else {
                    crate::logn!("SCN clock synchronization recovered");
                }
                previous_error = error;
            }
        }
    })?;
    GLOBAL.set(Global { generator, config }).map_err(|_| Error::new("SCN initialization raced"))
}
fn generator() -> Result<&'static Generator> {
    GLOBAL.get().map(|g| g.generator.as_ref()).ok_or_else(|| Error::new("SCN is not initialized"))
}
/// One greater than the process's previous SCN. Blocks while stalled, without
/// polling or reading the clock. Initialization must happen explicitly first.
pub fn next() -> Result<Scn> {
    generator()?.next()
}
/// Nonblocking allocation for event loops. None means wait/retry after clock sync;
/// this has the same clock-free allocation path as next().
pub fn try_next() -> Result<Option<Scn>> {
    generator()?.try_next()
}

/// Advance to max(local, incoming). Large forward jumps check UTC and may fail
/// without changing local state. Merge neither waits for nor clears a stall.
pub fn merge(incoming: Scn) -> Result<Scn> {
    generator()?.merge_with(incoming, utc_nanos)
}
/// Inspect state without allocating a number or reading the clock.
pub fn current() -> Result<Scn> {
    Ok(generator()?.current())
}
pub fn is_stalled() -> Result<bool> {
    Ok(generator()?.is_stalled())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn no_clock() -> Result<u64> {
        panic!("this operation must not read UTC")
    }

    #[test]
    fn concurrent_merges_and_allocations_preserve_the_maximum_across_wrap() {
        let base = u64::MAX - 1000;
        let scns = Arc::new(Generator::new(base, 1_000_000));
        std::thread::scope(|scope| {
            for worker in 0..8u64 {
                let scns = scns.clone();
                scope.spawn(move || {
                    for step in 0..1000u64 {
                        let incoming = Scn(base.wrapping_add(worker * 1000 + step));
                        let merged = scns.merge_with(incoming, no_clock).unwrap();
                        assert!(!merged.compare(incoming).is_lt());
                        scns.next().unwrap();
                    }
                });
            }
        });
        let final_scn = scns.current();
        assert!(final_scn.compare(Scn(base.wrapping_add(7999))).is_gt());
    }

    #[test]
    fn increments_and_small_merges_never_read_a_clock() {
        let scns = Generator::new(100, 60);
        let first = scns.next().unwrap();
        let merged = scns.merge_with(Scn(161), no_clock).unwrap();
        let older = scns.merge_with(Scn(0), no_clock).unwrap();
        let same = scns.merge_with(Scn(161), no_clock).unwrap();
        let next = scns.next().unwrap();
        assert_eq!(
            (first, merged, older, same, next),
            (Scn(101), Scn(161), Scn(161), Scn(161), Scn(162))
        );
    }
    #[test]
    fn large_merges_check_utc_and_failure_does_not_modify_state() {
        let scns = Generator::new(100, 60);
        let boundary = scns.merge_with(Scn(200), || Ok(140)).unwrap();
        let refused = scns.merge_with(Scn(300), || Ok(239)).is_err();
        let unchanged = scns.current();
        let clock_failed = scns.merge_with(Scn(300), || Err(Error::new("no clock"))).is_err();
        assert_eq!(boundary, Scn(200));
        assert!(refused && clock_failed);
        assert_eq!(unchanged, Scn(200));
        assert!(!scns.is_stalled()); // Merge failure does not control the stall flag.
    }
    #[test]
    fn only_periodic_sync_stalls_and_resumes_chains_of_small_merges() {
        let scns = Generator::new(100, 60);
        for value in [160, 220, 280] {
            scns.merge_with(Scn(value), no_clock).unwrap();
        }
        let before_sync = scns.next().unwrap();
        scns.synchronize_with(|| Ok(100)).unwrap();
        let blocked = scns.try_next().unwrap();
        scns.merge_with(Scn(290), no_clock).unwrap();
        let still_stalled = scns.is_stalled();
        scns.synchronize_with(|| Ok(229)).unwrap();
        let outside = scns.try_next().unwrap();
        scns.synchronize_with(|| Ok(230)).unwrap();
        let resumed = scns.next().unwrap();
        assert_eq!(before_sync, Scn(281));
        assert_eq!(blocked, None);
        assert!(still_stalled);
        assert_eq!(outside, None);
        assert_eq!(resumed, Scn(291));
    }
    #[test]
    fn clock_changes_never_regress_scn_and_clock_failure_stalls() {
        let scns = Generator::new(100, 60);
        scns.synchronize_with(|| Ok(200)).unwrap();
        let raised = scns.current();
        scns.synchronize_with(|| Ok(139)).unwrap();
        let backward = scns.current();
        let stalled = scns.is_stalled();
        scns.synchronize_with(|| Ok(140)).unwrap();
        let resumed = !scns.is_stalled();
        let failure = scns.synchronize_with(|| Err(Error::new("clock failed"))).is_err();
        let failure_stalled = scns.is_stalled();
        scns.synchronize_with(|| Ok(200)).unwrap();
        assert_eq!(raised, Scn(200));
        assert_eq!(backward, raised);
        assert!(stalled && resumed && failure && failure_stalled);
        assert!(!scns.is_stalled());
    }
    #[test]
    fn blocked_allocator_wakes_when_sync_clears_flag() {
        let scns = Arc::new(Generator::new(200, 60));
        scns.synchronize_with(|| Ok(100)).unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        let other = scns.clone();
        let worker = std::thread::spawn(move || send.send(other.next().unwrap()).unwrap());
        let blocked = recv.recv_timeout(Duration::from_millis(10));
        scns.synchronize_with(|| Ok(140)).unwrap();
        let resumed = recv.recv_timeout(Duration::from_secs(2)).unwrap();
        worker.join().unwrap();
        assert!(matches!(blocked, Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(resumed, Scn(201));
    }
    #[test]
    fn concurrent_allocations_are_unique_and_merges_never_regress() {
        let scns = Arc::new(Generator::new(1, u64::MAX / 2));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let scns = scns.clone();
                std::thread::spawn(move || {
                    (0..1000)
                        .map(|_| {
                            scns.merge_with(Scn(100), no_clock).unwrap();
                            scns.next().unwrap().number()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut values: Vec<_> = workers.into_iter().flat_map(|w| w.join().unwrap()).collect();
        values.sort_unstable();
        values.dedup();
        assert_eq!(values.len(), 4000);
        assert_eq!(values.first(), Some(&101));
        assert_eq!(values.last(), Some(&4100));
    }
    #[test]
    fn allocation_wraps_and_configuration_respects_the_comparison_window() {
        let scns = Generator::new(u64::MAX - 1, 60);
        let last = scns.next().unwrap();
        let wrapped = scns.next().unwrap();
        let bad_interval = Config { sync_interval_ms: 0, ..Config::default() }.validate().is_err();
        let bad_ahead = Config { max_ahead_ms: u64::MAX, ..Config::default() }.validate().is_err();
        let defaults: Config = toml::from_str("").unwrap();
        let custom: Config = toml::from_str("sync_interval_ms = 20\nmax_ahead_ms = 0").unwrap();
        custom.validate().unwrap();
        assert_eq!(std::mem::size_of::<Scn>(), 8);
        assert_eq!(last, Scn(u64::MAX));
        assert_eq!(wrapped, Scn(0));
        assert!(bad_interval && bad_ahead);
        assert_eq!(defaults, Config::default());
        assert_eq!(custom.sync_interval_ms, 20);
    }
    #[test]
    fn comparisons_work_across_wrap_within_the_half_range() {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let before = Scn(u64::MAX - 2);
        let after = Scn(3);
        let max_distance = Scn(u64::MAX / 2);
        let comparisons = [
            before.compare(after),
            after.compare(before),
            after.compare(after),
            max_distance.compare(Scn(0)),
            Scn(0).compare(max_distance),
        ];
        assert_eq!(comparisons, [Less, Greater, Equal, Greater, Less]);
    }
    #[test]
    fn merge_and_clock_stalls_use_wrapping_differences() {
        let scns = Generator::new(u64::MAX - 10, 60);
        let crossed = scns.merge_with(Scn(10), no_clock).unwrap();
        let old = scns.merge_with(Scn(u64::MAX - 1), no_clock).unwrap();
        let rejected = scns.merge_with(Scn(100), || Ok(u64::MAX - 10)).is_err();
        let accepted = scns.merge_with(Scn(100), || Ok(40)).unwrap();
        // Across wrap, 100 is 111 ns ahead of MAX-10, not an older timestamp.
        scns.synchronize_with(|| Ok(u64::MAX - 10)).unwrap();
        let stalled = scns.is_stalled();
        scns.synchronize_with(|| Ok(40)).unwrap();
        let resumed = scns.next().unwrap();
        scns.synchronize_with(|| Ok(200)).unwrap();
        let from_clock = scns.current();
        assert_eq!(crossed, Scn(10));
        assert_eq!(old, Scn(10));
        assert!(rejected && stalled);
        assert_eq!(accepted, Scn(100));
        assert_eq!(resumed, Scn(101));
        assert_eq!(from_clock, Scn(200));
        let near_wrap = Generator::new(u64::MAX - 10, 60);
        near_wrap.synchronize_with(|| Ok(5)).unwrap();
        assert_eq!(near_wrap.current(), Scn(5));
    }
    #[test]
    fn utc_conversion_wraps_at_eight_bytes() {
        let wrap = UNIX_EPOCH + Duration::new(18_446_744_073, 709_551_616);
        let last = utc_nanos_at(wrap - Duration::from_nanos(1)).unwrap();
        let zero = utc_nanos_at(wrap).unwrap();
        let next = utc_nanos_at(wrap + Duration::from_nanos(1)).unwrap();
        assert_eq!((last, zero, next), (u64::MAX, 0, 1));
    }
    #[test]
    fn wire_scn_is_exactly_eight_bytes() {
        use crate::wire::{Op, StructReader, StructWriter};
        let mut writer = StructWriter::new();
        writer.scn(Scn(0x0102030405060708));
        let arg = writer.finish(Op::Raft);
        let bytes = &arg.bytes;
        let mut reader = StructReader::from_payload(bytes, arg.depth);
        let decoded = reader.scn().unwrap();
        reader.done().unwrap();
        assert_eq!(bytes.to_vec(), [8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(decoded, Scn(0x0102030405060708));
    }
}
