//! Small bounded waits for eventually-true conditions.
//!
//! The first call to [`Retry::done`] starts the clock and returns `false`.
//! The next call measures the work that happened since then without sleeping;
//! subsequent calls sleep with exponential backoff until the chosen budget ends.

use std::thread;
use std::time::{Duration, Instant};

enum Limit<'a> {
    Deadline(Duration),
    Budget(&'a mut i64),
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
    pub fn new(milliseconds: u64, message: impl Into<String>) -> Retry<'static> {
        Retry {
            limit: Limit::Deadline(Duration::from_millis(milliseconds)),
            message: message.into(),
            started: None,
            previous: Instant::now(),
            wait: Duration::ZERO,
            logged: false,
        }
    }
}

impl<'a> Retry<'a> {
    /// Retry while deducting elapsed milliseconds from a shared budget.
    ///
    /// This is useful when several operations share one total wait allowance:
    /// pass the same mutable counter to each retry in turn.
    pub fn with_budget(milliseconds: &'a mut i64, message: impl Into<String>) -> Retry<'a> {
        Retry {
            limit: Limit::Budget(milliseconds),
            message: message.into(),
            started: None,
            previous: Instant::now(),
            wait: Duration::ZERO,
            logged: false,
        }
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
                **remaining -= elapsed.as_millis().min(i64::MAX as u128) as i64;
                if **remaining <= 0 {
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
                let sleep = self.wait.min(Duration::from_millis((**remaining).max(0) as u64));
                **remaining -= sleep.as_millis().min(i64::MAX as u128) as i64;
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
    fn first_done_starts_without_waiting() {
        let mut retry = Retry::new(10, "test");
        let before = Instant::now();
        assert!(!retry.done());
        assert!(before.elapsed() < Duration::from_millis(5));
    }

    #[test]
    fn second_done_calibrates_without_waiting() {
        let mut retry = Retry::new(100, "test");
        assert!(!retry.done());
        let before = Instant::now();
        assert!(!retry.done());
        assert!(before.elapsed() < Duration::from_millis(5));
    }

    #[test]
    fn budget_is_shared_and_exhausts() {
        let mut budget = 0;
        let mut retry = Retry::with_budget(&mut budget, "test");
        assert!(!retry.done());
        assert!(retry.done());
    }
}
