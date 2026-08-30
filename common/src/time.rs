//! A moment, counted in whole seconds since the epoch.
//!
//! Seconds are the unit throughout: an extent's creation second selects its key
//! (§7.3.3), a stream's says when a user made it (§17.25.1), and a hint's expiry is
//! the window in §10.1. Nothing in the store needs finer than that, and the one place
//! that does — log timestamps — is not a stored fact about an object.
//!
//! ⚠️ **A type rather than a `u64`** (Asserted, Bob, 2026-08-20), for the same reason
//! `ELoff` is not an `ECoff`: the store is full of `u64`s that are counts of records,
//! numbers of streams, byte lengths and moments, and a moment passed where a count
//! belongs is not a compile error unless it is a different type. It also stops the
//! quieter version — seconds handed to something expecting milliseconds — which no
//! amount of care in one file prevents in another.

use crate::{Error, Result};
use std::time::{SystemTime, UNIX_EPOCH};

/// A moment, in whole seconds since the Unix epoch.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeSec(u64);

impl TimeSec {
    /// The epoch itself, which is also what a default is.
    pub const EPOCH: TimeSec = TimeSec(0);

    /// **No moment given.** The top of the range, per `EVERY-SPACE-RESERVES-A-SENTINEL`
    /// — and unlike a length, zero here is a real moment nobody will legitimately mean,
    /// so the choice is uniformity rather than necessity.
    pub const NONE: TimeSec = TimeSec(u64::MAX);

    /// The latest moment that is a moment, one below `NONE`.
    pub const MAX_REAL: TimeSec = TimeSec(u64::MAX - 1);

    /// The clock, read once. ⚠️ **The only place that reads it**, so that three tiers
    /// do not each keep their own copy of the same four lines — which they did, and
    /// which is how a unit would have come to differ between them.
    ///
    /// A clock before the epoch is a machine that is badly wrong; it reads as the epoch
    /// rather than panicking, because refusing to serve is worse than serving with a
    /// visibly absurd timestamp.
    pub fn now() -> TimeSec {
        TimeSec(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs())
    }

    /// ⚠️ **Unchecked, for literals and tests.** `new(u64::MAX)` is `NONE`.
    pub const fn new(seconds: u64) -> TimeSec {
        TimeSec(seconds)
    }

    /// From the wire.
    pub fn of_number(number: u64) -> Result<TimeSec> {
        Ok(TimeSec(number))
    }

    pub const fn number(self) -> u64 {
        self.0
    }

    pub const fn is_none(self) -> bool {
        self.0 == u64::MAX
    }

    pub const fn is_some(self) -> bool {
        self.0 != u64::MAX
    }

    /// This moment plus a span of seconds — an expiry from a window, a retention from a
    /// creation. Saturates, because a moment past the end of time is not a moment to
    /// wrap around to the epoch.
    pub fn plus_secs(self, seconds: u64) -> TimeSec {
        TimeSec(self.0.saturating_add(seconds).min(u64::MAX - 1))
    }

    /// Seconds from an earlier moment to this one. ⚠️ Saturating: a clock that went
    /// backwards gives zero rather than an age of half the range.
    pub fn since(self, earlier: TimeSec) -> u64 {
        self.0.saturating_sub(earlier.0)
    }

    /// Whether this moment has passed, which is what an expiry is asked.
    pub fn past(self, now: TimeSec) -> bool {
        self.is_some() && self.0 <= now.0
    }

    /// The day this falls in, which is what selects a master key (§7.3).
    pub fn day(self) -> u64 {
        self.0 / 86_400
    }

    /// ⚠️ **Refuses a clock that went backwards**, which is the check §7.3.3 needs
    /// before an extent's creation second selects its key: two extents made in the same
    /// second are fine, one made *before* the last one is a machine to stop trusting.
    pub fn not_before(self, latest: TimeSec) -> Result<TimeSec> {
        match self.0 >= latest.0 {
            true => Ok(self),
            false => Err(Error::new(format!("the clock went back to {self} from {latest}"))),
        }
    }
}

impl std::fmt::Display for TimeSec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_clock_moves_forward_and_says_so() {
        let then = TimeSec::new(1_000_000);
        assert_eq!(then.plus_secs(600).since(then), 600);
        assert!(TimeSec::now() > TimeSec::new(1_700_000_000), "later than 2023");
    }

    /// A clock that went backwards gives zero, not an age of half the range.
    #[test]
    fn going_backwards_saturates_rather_than_wrapping() {
        assert_eq!(TimeSec::new(10).since(TimeSec::new(400)), 0);
        assert_eq!(TimeSec::new(u64::MAX - 5).plus_secs(1000), TimeSec::MAX_REAL);
    }

    /// The same second twice is fine; earlier than the last one is a machine to stop
    /// trusting, because a creation second selects a key (§7.3.3).
    #[test]
    fn the_same_second_is_fine_and_an_earlier_one_is_not() {
        let last = TimeSec::new(1_000_000);
        assert!(last.not_before(last).is_ok(), "two extents in one second");
        assert!(TimeSec::new(1_000_001).not_before(last).is_ok());
        let said = TimeSec::new(999_999).not_before(last).expect_err("backwards").to_string();
        assert!(said.contains("clock went back"), "{said}");
    }

    /// An expiry that was never set has not passed, however late it is.
    #[test]
    fn no_expiry_never_passes() {
        let now = TimeSec::new(2_000_000);
        assert!(TimeSec::new(1_999_999).past(now));
        assert!(TimeSec::new(2_000_000).past(now), "at the moment it names");
        assert!(!TimeSec::new(2_000_001).past(now));
        assert!(!TimeSec::NONE.past(now), "unset is not expired");
    }

    #[test]
    fn a_day_is_what_selects_a_key() {
        assert_eq!(TimeSec::new(0).day(), 0);
        assert_eq!(TimeSec::new(86_399).day(), 0);
        assert_eq!(TimeSec::new(86_400).day(), 1);
    }
}
