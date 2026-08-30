//! Enough randomness to fill a buffer with values nothing downstream can
//! shortcut — not zeros, which compress and which a page-sharing kernel can
//! cheat on. Not for keys.
//!
//! Bob's small fast generator, 64-bit variant: four words of state, three
//! rotates, no multiplies, and no dependence on whatever the platform's
//! generator happens to be this year.

use rand_core::RngCore;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Rng {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

impl Rng {
    pub fn new() -> Rng {
        let nanos =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
        Rng::from_seed(nanos)
    }

    pub fn from_seed(seed: u64) -> Rng {
        let mut rng = Rng { a: 0xf1ea_5eed, b: seed, c: seed, d: seed };
        // Twenty rounds to bury the seed before anything is handed out.
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
        // chunks_exact_mut, not chunks_mut: the bulk loop then has no per-word
        // length check, which was costing more than generating the word.
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
        self.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.fill_bytes(dest);
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> std::result::Result<(), rand_core::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

impl Default for Rng {
    fn default() -> Rng {
        Rng::new()
    }
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

    /// Not a test of randomness, just of not being obviously broken: no short
    /// cycle, and every byte value shows up.
    #[test]
    fn no_early_repeat() {
        let mut rng = Rng::from_seed(1);
        let mut seen = BTreeSet::new();
        for _ in 0..10_000 {
            assert!(seen.insert(rng.next_u64()), "repeated inside 10,000 draws");
        }
        let mut buf = [0u8; 4096];
        rng.fill_bytes(&mut buf);
        let bytes: BTreeSet<u8> = buf.iter().copied().collect();
        assert_eq!(bytes.len(), 256);
    }
}
