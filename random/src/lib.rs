//! Fast non-cryptographic random values. Do not use these for keys.

use rand_core::RngCore;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Rng {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

impl Rng {
    /// Seed a generator from the current clock.
    pub fn new() -> Rng {
        let nanos =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
        Rng::from_seed(nanos)
    }

    pub fn from_seed(seed: u64) -> Rng {
        let mut rng = Rng { a: 0xf1ea_5eed, b: seed, c: seed, d: seed };
        for _ in 0..20 {
            rng.next_u64();
        }
        rng
    }

    pub fn next_u64(&mut self) -> u64 {
        let e = self.a.wrapping_sub(self.b.rotate_left(7));
        self.a = self.b ^ self.c.rotate_left(13);
        self.b = self.c.wrapping_add(self.d.rotate_left(37));
        self.c = self.d.wrapping_add(e);
        self.d = e.wrapping_add(self.a);
        self.d
    }

    pub fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    pub fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut chunks = buf.chunks_exact_mut(8);
        for chunk in &mut chunks {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let tail = chunks.into_remainder();
        if !tail.is_empty() {
            let bytes = self.next_u64().to_le_bytes();
            tail.copy_from_slice(&bytes[..tail.len()]);
        }
    }
}

impl RngCore for Rng {
    fn next_u32(&mut self) -> u32 {
        Rng::next_u32(self)
    }

    fn next_u64(&mut self) -> u64 {
        Rng::next_u64(self)
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        Rng::fill_bytes(self, dest);
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> std::result::Result<(), rand_core::Error> {
        Rng::fill_bytes(self, dest);
        Ok(())
    }
}

impl Default for Rng {
    fn default() -> Rng {
        Rng::new()
    }
}

static THREAD_SEED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_RNG: RefCell<Rng> = RefCell::new(new_thread_rng());
}

fn new_thread_rng() -> Rng {
    let nanos =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
    let sequence = THREAD_SEED.fetch_add(0x9e37_79b9_7f4a_7c15, Relaxed);
    Rng::from_seed(nanos ^ sequence)
}

/// Draw from a generator owned by the calling thread.
pub fn thread_u64() -> u64 {
    THREAD_RNG.with(|rng| rng.borrow_mut().next_u64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn fill_covers_odd_lengths() {
        for len in [0, 1, 7, 8, 9, 1000] {
            let mut buf = vec![0u8; len];
            Rng::from_seed(len as u64 + 1).fill_bytes(&mut buf);
            assert_eq!(buf.len(), len);
            if len >= 8 {
                assert!(buf.iter().any(|b| *b != 0));
            }
        }
    }

    #[test]
    fn a_seed_repeats() {
        let mut a = Rng::from_seed(42);
        let mut b = Rng::from_seed(42);
        assert_eq!(a.next_u64(), b.next_u64());
        assert_ne!(a.next_u64(), Rng::from_seed(43).next_u64());
    }

    #[test]
    fn no_early_repeat() {
        let mut rng = Rng::from_seed(1);
        let mut seen = BTreeSet::new();
        for _ in 0..10_000 {
            assert!(seen.insert(rng.next_u64()), "repeated inside 10,000 draws");
        }
        let mut buf = [0; 4096];
        rng.fill_bytes(&mut buf);
        let bytes: BTreeSet<u8> = buf.into_iter().collect();
        assert_eq!(bytes.len(), 256);
    }

    #[test]
    fn thread_rngs_are_independent() {
        let local = [thread_u64(), thread_u64()];
        let remote = std::thread::spawn(|| [thread_u64(), thread_u64()]).join().unwrap();
        assert_ne!(local[0], local[1]);
        assert_ne!(remote[0], remote[1]);
        assert_ne!(local, remote);
    }
}
