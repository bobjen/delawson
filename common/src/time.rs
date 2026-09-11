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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Define a signed interval with a distinct type, width, and unit.
/// `MIN` and `MAX` are negative and positive infinity; `NONE` is unset.
/// Finite values occupy `MIN_REAL..=MAX_REAL`. No unit conversion is implicit.
///
/// ```
/// common::timeint!(IntervalSec, i64, "seconds");
/// common::timeint!(IntervalMillis, "milliseconds"); // defaults to i32
/// assert!(IntervalSec::MIN < IntervalSec::new(-10));
/// assert!(IntervalSec::new(10) < IntervalSec::MAX);
/// assert!(IntervalMillis::default().is_none());
/// ```
///
/// ```compile_fail
/// common::timeint!(UnsignedInterval, u32, "seconds");
/// ```
#[macro_export]
macro_rules! timeint {
    ($name:ident, $ty:ty, $unit:literal) => {
        // Require signed storage even if no negative value is constructed.
        const _: $ty = -1;

        #[doc = concat!("A signed interval in ", $unit, ".")]
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($ty);

        impl Default for $name {
            fn default() -> Self {
                Self::NONE
            }
        }

        impl $name {
            /// Negative infinity.
            pub const MIN: Self = Self(<$ty>::MIN);
            /// Positive infinity.
            pub const MAX: Self = Self(<$ty>::MAX - 1);
            /// Unset. Sorts after `MAX`, but has no temporal meaning.
            pub const NONE: Self = Self(<$ty>::MAX);
            pub const MIN_REAL: Self = Self(<$ty>::MIN + 1);
            pub const MAX_REAL: Self = Self(<$ty>::MAX - 2);
            pub const ZERO: Self = Self(0);

            /// From the raw representation, including reserved encodings.
            pub const fn new(interval: $ty) -> Self {
                Self(interval)
            }

            pub const fn get(self) -> $ty {
                self.0
            }

            pub const fn is_none(self) -> bool {
                self.0 == Self::NONE.0
            }

            /// Includes both infinities as well as finite intervals.
            pub const fn is_some(self) -> bool {
                !self.is_none()
            }

            pub const fn is_finite(self) -> bool {
                self.0 > Self::MIN.0 && self.0 < Self::MAX.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
    ($name:ident, $unit:literal) => {
        $crate::timeint!($name, i32, $unit);
    };
}

timeint!(TimeIntMilli64, i64, "milliseconds");
timeint!(TimeIntSec64, i64, "seconds");

// Runtime waits remain std::time::Duration. Reject sentinels and negative spans
// at that boundary rather than turning them into very long waits.
macro_rules! duration_conversion {
    ($name:ident, $constructor:ident) => {
        impl $name {
            pub fn to_duration(self) -> Result<Duration> {
                if !self.is_finite() || self < Self::ZERO {
                    return Err(Error::new("expected a finite, nonnegative time interval"));
                }
                Ok(Duration::$constructor(self.get() as u64))
            }
        }
    };
}

duration_conversion!(TimeIntMilli64, from_millis);
duration_conversion!(TimeIntSec64, from_secs);

impl TimeIntMilli64 {
    /// Deduct elapsed time from a finite budget. Exhaustion clamps at zero;
    /// even a very large Duration cannot overflow into a sentinel.
    pub fn remaining_after(self, elapsed: Duration) -> Result<Self> {
        if !self.is_finite() {
            return Err(Error::new("expected a finite time budget"));
        }
        let budget = self.get().max(0) as u128;
        Ok(Self::new(budget.saturating_sub(elapsed.as_millis()) as i64))
    }
}

/// A moment, in whole seconds since the Unix epoch. Every `u64` value is a timestamp;
/// absence or an unbounded interval must be represented separately.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeSec(u64);

impl TimeSec {
    /// The epoch itself, which is also what a default is.
    pub const EPOCH: TimeSec = TimeSec(0);

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

    /// From seconds since the epoch, using the full `u64` range.
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

    /// This moment plus a span of seconds — an expiry from a window, a retention from a
    /// creation. Saturates, because a moment past the end of time is not a moment to
    /// wrap around to the epoch.
    pub fn plus_secs(self, seconds: TimeIntSec64) -> Result<TimeSec> {
        Ok(TimeSec(self.0.saturating_add(seconds.to_duration()?.as_secs())))
    }

    /// Seconds from an earlier moment to this one. ⚠️ Saturating: a clock that went
    /// backwards gives zero rather than an age of half the range.
    pub fn since(self, earlier: TimeSec) -> u64 {
        self.0.saturating_sub(earlier.0)
    }

    /// Whether this moment has passed, which is what an expiry is asked.
    pub fn past(self, now: TimeSec) -> bool {
        self.0 <= now.0
    }

    /// The day this falls in, which is what selects a master key (§7.3).
    pub fn day(self) -> u64 {
        self.0 / 86_400
    }

    /// Midnight at the start of this moment's UTC day.
    pub fn day_start(self) -> TimeSec {
        TimeSec::new(self.day() * 86_400)
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
    fn duration_conversion_checks_units_and_rejects_special_values() {
        assert_eq!(TimeIntMilli64::new(1500).to_duration().unwrap(), Duration::from_millis(1500));
        assert_eq!(TimeIntSec64::new(1500).to_duration().unwrap(), Duration::from_secs(1500));
        for value in [
            TimeIntMilli64::MIN,
            TimeIntMilli64::MAX,
            TimeIntMilli64::NONE,
            TimeIntMilli64::new(-1),
        ] {
            assert!(value.to_duration().is_err());
        }
        for value in
            [TimeIntSec64::MIN, TimeIntSec64::MAX, TimeIntSec64::NONE, TimeIntSec64::new(-1)]
        {
            assert!(value.to_duration().is_err());
            assert!(TimeSec::EPOCH.plus_secs(value).is_err());
        }
    }

    #[test]
    fn budget_exhaustion_cannot_produce_a_sentinel() {
        let budget = TimeIntMilli64::new(10);
        assert_eq!(
            budget.remaining_after(Duration::from_millis(3)).unwrap(),
            TimeIntMilli64::new(7)
        );
        assert_eq!(budget.remaining_after(Duration::from_micros(999)).unwrap(), budget);
        assert_eq!(budget.remaining_after(Duration::MAX).unwrap(), TimeIntMilli64::ZERO);
        assert_eq!(
            TimeIntMilli64::new(-1).remaining_after(Duration::ZERO).unwrap(),
            TimeIntMilli64::ZERO
        );
        for value in [TimeIntMilli64::MIN, TimeIntMilli64::MAX, TimeIntMilli64::NONE] {
            assert!(value.remaining_after(Duration::ZERO).is_err());
        }
    }

    timeint!(Interval32, "seconds");
    timeint!(Interval64, i64, "timestamp ticks");

    #[test]
    fn intervals_preserve_width_sign_and_reserved_values() {
        assert_eq!(std::mem::size_of::<Interval32>(), 4);
        assert_eq!(std::mem::size_of::<Interval64>(), 8);
        macro_rules! check {
            ($name:ident, $value:expr) => {
                let negative = $name::new($value);
                assert_eq!(negative.get(), $value);
                assert_eq!(negative.to_string(), $value.to_string());
                assert!(negative < $name::ZERO);
                assert!($name::MIN < $name::MIN_REAL);
                assert!($name::MIN_REAL <= negative);
                assert!($name::ZERO < $name::MAX_REAL);
                assert!($name::MAX_REAL < $name::MAX);
                for value in [$name::MIN_REAL, negative, $name::ZERO, $name::MAX_REAL] {
                    assert!(value.is_finite());
                    assert!(value.is_some());
                }
                for value in [$name::MIN, $name::MAX, $name::NONE] {
                    assert!(!value.is_finite());
                    assert_eq!($name::new(value.get()), value);
                }
                assert!($name::MIN.is_some());
                assert!($name::MAX.is_some());
                assert!($name::NONE.is_none());
                assert_eq!($name::default(), $name::NONE);
            };
        }
        check!(Interval32, -60);
        check!(Interval64, -(1_i64 << 40));
    }

    #[test]
    fn the_clock_moves_forward_and_says_so() {
        let then = TimeSec::new(1_000_000);
        assert_eq!(then.plus_secs(TimeIntSec64::new(600)).unwrap().since(then), 600);
        assert!(TimeSec::now() > TimeSec::new(1_700_000_000), "later than 2023");
    }

    /// A clock that went backwards gives zero, not an age of half the range.
    #[test]
    fn going_backwards_saturates_rather_than_wrapping() {
        assert_eq!(TimeSec::new(10).since(TimeSec::new(400)), 0);
        assert_eq!(
            TimeSec::new(u64::MAX - 5).plus_secs(TimeIntSec64::new(1000)).unwrap(),
            TimeSec::new(u64::MAX)
        );
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

    /// The uppermost encoding is an ordinary timestamp, including at its boundary.
    #[test]
    fn every_timestamp_can_pass() {
        let now = TimeSec::new(2_000_000);
        assert!(TimeSec::new(1_999_999).past(now));
        assert!(TimeSec::new(2_000_000).past(now), "at the moment it names");
        assert!(!TimeSec::new(2_000_001).past(now));
        let last = TimeSec::new(u64::MAX);
        assert!(!last.past(TimeSec::new(u64::MAX - 1)));
        assert!(last.past(last));
        assert_eq!(last.plus_secs(TimeIntSec64::ZERO).unwrap(), last);
        assert_eq!(TimeSec::of_number(u64::MAX).unwrap().number(), u64::MAX);
    }

    #[test]
    fn a_day_is_what_selects_a_key() {
        assert_eq!(TimeSec::new(0).day(), 0);
        assert_eq!(TimeSec::new(86_399).day(), 0);
        assert_eq!(TimeSec::new(86_400).day(), 1);
    }
}
