//! Talking to one process, and timing it.
//!
//! Calls are asynchronous: `send` and `recv` are separate, so a caller may keep
//! many requests outstanding on one connection and match answers by id. `call` is
//! the one-at-a-time convenience on top.
//!
//! Sends are buffered and flushed deliberately, so requests that are pending
//! together leave in one syscall. Nothing here is a batch *message* — every
//! request is separate and independently answerable; only the transport coalesces.

use common::wire::{Chan, Request, Response, Status, read_response, write_request};
use common::{Context, Error, Result};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

pub struct Conn {
    chan: Chan,
    next_id: u32,
}

impl Conn {
    pub fn open(addr: SocketAddr, timeout: Duration) -> Result<Conn> {
        let stream =
            TcpStream::connect_timeout(&addr, timeout).context(format!("connecting {addr}"))?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Conn { chan: Chan::new(stream)?, next_id: 0 })
    }

    /// Buffer a request without waiting for it, and return the id to expect back.
    /// Nothing goes out until a flush.
    pub fn send(&mut self, request: &Request) -> Result<u32> {
        self.next_id = self.next_id.wrapping_add(1);
        let id = self.next_id;
        write_request(&mut self.chan.output, &request.clone().id(id))?;
        Ok(id)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.chan.flush()
    }

    pub fn recv(&mut self) -> Result<Response> {
        read_response(&mut self.chan.input)
    }

    pub fn call(&mut self, request: &Request) -> Result<Response> {
        let id = self.send(request)?;
        self.flush()?;
        let response = self.recv()?;
        if response.id != id {
            return Err(Error::new(format!("asked {id} and heard {}", response.id)));
        }
        Ok(response)
    }

    /// One request, timed. The work inside a `ping` is a few hundred
    /// instructions, so what this measures is almost entirely message overhead.
    pub fn timed(&mut self, request: &Request) -> Result<(Response, Duration)> {
        let start = Instant::now();
        let response = self.call(request)?;
        Ok((response, start.elapsed()))
    }

    /// Keep `depth` requests in flight until `until`, then drain, tallying each
    /// round trip. Sends pile up in the buffer and go out when there is nothing
    /// left to read, so depth is what gives the transport something to coalesce.
    ///
    /// Latency here includes queuing, which is the point: depth trades it for
    /// throughput.
    pub fn press(
        &mut self,
        request: &Request,
        depth: usize,
        until: Instant,
        tally: &mut Tally,
    ) -> Result<()> {
        // Keyed by id, not by a slot: answers may come back in any order, and a
        // slow one can stay outstanding while ids run far past it.
        let mut sent: HashMap<u32, Instant> = HashMap::with_capacity(depth * 2);
        let mut out = 0;
        loop {
            while out < depth && Instant::now() < until {
                let id = self.send(request)?;
                sent.insert(id, Instant::now());
                out += 1;
            }
            if out == 0 {
                return self.flush();
            }
            // Nothing more to read, so the next recv would block: send.
            if !self.chan.more() {
                self.flush()?;
            }
            let response = self.recv()?;
            if response.status != Status::Ok {
                return Err(Error::new(format!("answered {}", response.status)));
            }
            match sent.remove(&response.id) {
                Some(at) => tally.add(at.elapsed()),
                None => return Err(Error::new(format!("unasked answer {}", response.id))),
            }
            out -= 1;
        }
    }
}

/// What one client counted during a measured run. Kept as running totals rather
/// than a list of samples, so the measuring loop allocates nothing.
pub struct Tally {
    pub count: u64,
    pub sum: Duration,
    pub min: Duration,
    pub max: Duration,
}

impl Tally {
    pub fn new() -> Tally {
        Tally { count: 0, sum: Duration::ZERO, min: Duration::MAX, max: Duration::ZERO }
    }

    pub fn add(&mut self, d: Duration) {
        self.count += 1;
        self.sum += d;
        self.min = self.min.min(d);
        self.max = self.max.max(d);
    }

    pub fn merge(&mut self, other: &Tally) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    pub fn mean(&self) -> Duration {
        if self.count == 0 { Duration::ZERO } else { self.sum / self.count as u32 }
    }
}

/// Round-trip times, summarized. The maximum is reported because job latency is
/// the maximum over requests, not the average.
pub struct Stats {
    pub count: usize,
    pub min: Duration,
    pub p50: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub mean: Duration,
}

impl Stats {
    pub fn of(samples: &mut [Duration]) -> Option<Stats> {
        if samples.is_empty() {
            return None;
        }
        samples.sort();
        let at = |fraction: f64| {
            let i = (fraction * samples.len() as f64) as usize;
            samples[i.min(samples.len() - 1)]
        };
        let total: Duration = samples.iter().sum();
        Some(Stats {
            count: samples.len(),
            min: samples[0],
            p50: at(0.50),
            p99: at(0.99),
            max: samples[samples.len() - 1],
            mean: total / samples.len() as u32,
        })
    }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let us = |d: Duration| d.as_secs_f64() * 1e6;
        write!(
            f,
            "{} round trips, microseconds: min {:.1}  p50 {:.1}  p99 {:.1}  max {:.1}  mean {:.1}",
            self.count,
            us(self.min),
            us(self.p50),
            us(self.p99),
            us(self.max),
            us(self.mean)
        )
    }
}
