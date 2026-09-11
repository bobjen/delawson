//! Small bounded waits for eventually-true conditions.
//!
//! The first call to [`Retry::done`] starts the clock and returns `false`.
//! The next call measures the work that happened since then without sleeping;
//! subsequent calls sleep with exponential backoff until the chosen budget ends.

use crate::time::TimeIntMilli64;
use crate::{Error, Result};
use std::thread;
use std::time::{Duration, Instant};

enum Limit<'a> {
    Deadline(Duration),
    Budget(&'a mut TimeIntMilli64),
}

/// A bounded, exponentially backed-off retry wait.
pub struct Retry<'a> {
    limit: Limit<'a>,
    message: String,
    started: Option<Instant>,
    previous: Instant,
    wait: Duration,
    logged: bool,
}

impl Retry<'static> {
    /// Retry for at most `milliseconds` from the first call to [`Retry::done`].
    pub fn new(milliseconds: TimeIntMilli64, message: impl Into<String>) -> Result<Retry<'static>> {
        Ok(Retry {
            limit: Limit::Deadline(milliseconds.to_duration()?),
            message: message.into(),
            started: None,
            previous: Instant::now(),
            wait: Duration::ZERO,
            logged: false,
        })
    }
}

impl<'a> Retry<'a> {
    /// Retry while deducting elapsed milliseconds from a shared budget.
    ///
    /// This is useful when several operations share one total wait allowance:
    /// pass the same mutable counter to each retry in turn.
    pub fn with_budget(
        milliseconds: &'a mut TimeIntMilli64,
        message: impl Into<String>,
    ) -> Result<Retry<'a>> {
        if !milliseconds.is_finite() {
            return Err(Error::new("expected a finite retry budget"));
        }
        Ok(Retry {
            limit: Limit::Budget(milliseconds),
            message: message.into(),
            started: None,
            previous: Instant::now(),
            wait: Duration::ZERO,
            logged: false,
        })
    }

    /// Return whether the retry budget is exhausted, sleeping before later tries.
    pub fn done(&mut self) -> bool {
        let now = Instant::now();
        let Some(started) = self.started else {
            self.started = Some(now);
            self.previous = now;
            return false;
        };

        let elapsed = now.saturating_duration_since(self.previous);
        self.previous = now;
        match &mut self.limit {
            Limit::Deadline(limit) => {
                if started.elapsed() >= *limit {
                    return true;
                }
            }
            Limit::Budget(remaining) => {
                **remaining = remaining.remaining_after(elapsed).expect("finite retry budget");
                if **remaining <= TimeIntMilli64::ZERO {
                    return true;
                }
            }
        }

        if self.wait.is_zero() {
            self.wait = elapsed.saturating_mul(2).max(Duration::from_millis(1));
            // The second call calibrates the first delay; it never sleeps.
            return false;
        }
        if self.wait >= Duration::from_secs(1) && !self.logged {
            crate::logn!("retrying", "{}", self.message);
            self.logged = true;
        }

        let sleep = match &mut self.limit {
            Limit::Deadline(limit) => self.wait.min(limit.saturating_sub(started.elapsed())),
            Limit::Budget(remaining) => {
                let sleep = self.wait.min(remaining.to_duration().expect("positive retry budget"));
                **remaining = remaining.remaining_after(sleep).expect("finite retry budget");
                sleep
            }
        };
        if !sleep.is_zero() {
            thread::sleep(sleep);
        }

        if self.wait < Duration::from_secs(1) {
            self.wait = self.wait.saturating_mul(2).min(Duration::from_secs(1));
        } else {
            self.wait = self.wait.saturating_add(self.wait / 10);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn special_values_cannot_become_retry_waits() {
        for mut value in [TimeIntMilli64::MIN, TimeIntMilli64::MAX, TimeIntMilli64::NONE] {
            assert!(Retry::new(value, "test").is_err());
            assert!(Retry::with_budget(&mut value, "test").is_err());
        }
        let mut expired = TimeIntMilli64::new(-1);
        let mut retry = Retry::with_budget(&mut expired, "test").unwrap();
        assert!(!retry.done());
        assert!(retry.done());
    }

    #[test]
    fn first_done_starts_without_waiting() {
        let mut retry = Retry::new(TimeIntMilli64::new(10), "test").unwrap();
        let before = Instant::now();
        assert!(!retry.done());
        assert!(before.elapsed() < Duration::from_millis(5));
    }

    #[test]
    fn second_done_calibrates_without_waiting() {
        let mut retry = Retry::new(TimeIntMilli64::new(100), "test").unwrap();
        assert!(!retry.done());
        let before = Instant::now();
        assert!(!retry.done());
        assert!(before.elapsed() < Duration::from_millis(5));
    }

    #[test]
    fn budget_is_shared_and_exhausts() {
        let mut budget = TimeIntMilli64::ZERO;
        let mut retry = Retry::with_budget(&mut budget, "test").unwrap();
        assert!(!retry.done());
        assert!(retry.done());
    }
}
