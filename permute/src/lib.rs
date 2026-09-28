//! Pseudorandom permutations over the integer range `0..n`.
//!
//! Each cycle yields every value in the range exactly once. The generator uses
//! constant storage and has expected constant work per value. It is not
//! cryptographically secure.

const CONSTANTS: usize = 12;
const SEED_MUL: u64 = 0xdead_beef_cafe_fee7;
const VALUE_MUL: u64 = 0xba5e_ba11_baad_5eed;

/// Generate a deterministic pseudorandom permutation of `0..n`.
///
/// Construct with [`Permute::new`], read the initial result with [`value`],
/// and call [`next_value`] for subsequent results. One complete cycle contains
/// every value in `0..n` exactly once; after that, the sequence repeats.
///
/// [`value`]: Permute::value
/// [`next_value`]: Permute::next_value
pub struct Permute {
    n: u64,
    state: u64,
    value: u64,
    mask: u64,
    inc: u64,
    d: [u64; CONSTANTS],
}

impl Permute {
    /// Create a permutation of `0..n` using `seed`.
    ///
    /// `n` must be nonzero. Construction also computes the first value, so
    /// [`value`](Self::value) is immediately usable.
    pub fn new(n: u64, seed: u64) -> Self {
        assert!(n != 0, "permutation range must be nonempty");

        // `mask + 1` is the next power of two at least as large as `n`.
        let mut mask = n - 1;
        for shift in [1, 2, 4, 8, 16, 32] {
            mask |= mask >> shift;
        }

        // Scramble the seed and derive independent reversible offsets.
        let seed = seed.wrapping_mul(SEED_MUL);
        let mut d = [0; CONSTANTS];
        for (i, offset) in d.iter_mut().zip((0..CONSTANTS).map(|i| (64 / CONSTANTS) * i + 1)) {
            *i = seed.rotate_left(offset as u32);
        }

        // An odd increment traverses the whole power-of-two state space.
        let start = seed.rotate_left(22) & mask;
        let inc = seed.rotate_left(22) | 1;
        let mut permute = Self { n, state: start, value: 0, mask, inc, d };
        permute.advance();
        permute
    }

    /// Return the current value without advancing the permutation.
    pub fn value(&self) -> u64 {
        self.value
    }

    /// Advance and return the next value.
    pub fn next_value(&mut self) -> u64 {
        self.advance()
    }

    fn advance(&mut self) -> u64 {
        loop {
            self.state = self.state.wrapping_add(self.inc) & self.mask;
            let mut value = self.state;

            // Each step is a reversible transform over the masked bit width.
            for (i, offset) in self.d.iter().enumerate() {
                value = value.wrapping_add(*offset) & self.mask;
                value ^= (value >> 1) ^ (value >> (i + 2));
            }
            value = value.wrapping_mul(VALUE_MUL) & self.mask;

            // Rejection maps the power-of-two permutation onto exactly 0..n.
            if value < self.n {
                self.value = value;
                return value;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Permute;

    #[test]
    fn cycles_are_permutations_for_small_ranges() {
        for n in 1..=257 {
            for seed in [0, 1, 42, u64::MAX] {
                let mut permute = Permute::new(n, seed);
                let first = permute.value();
                let mut counts = vec![0u32; n as usize];
                let mut wrapped_at = None;

                for calls in 1..=1000 {
                    let value = permute.value();
                    assert!(value < n, "n={n}, seed={seed}, value={value}");
                    counts[value as usize] += 1;

                    permute.next_value();
                    if permute.value() == first {
                        wrapped_at = Some(calls);
                        break;
                    }
                }

                assert_eq!(wrapped_at, Some(n), "n={n}, seed={seed}");
                assert!(counts.iter().all(|&count| count != 0), "n={n}, seed={seed}");
            }
        }
    }

    #[test]
    fn same_seed_repeats_the_same_values() {
        let mut a = Permute::new(10_003, 1234);
        let mut b = Permute::new(10_003, 1234);
        for _ in 0..10_000 {
            assert_eq!(a.value(), b.value());
            a.next_value();
            b.next_value();
        }
    }

    #[test]
    fn large_ranges_do_not_repeat_in_a_prefix() {
        let mut permute = Permute::new(1 << 32, 0xfeed_beef);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..20_000 {
            assert!(seen.insert(permute.value()));
            permute.next_value();
        }
    }

    #[test]
    #[should_panic(expected = "permutation range must be nonempty")]
    fn rejects_an_empty_range() {
        Permute::new(0, 0);
    }
}
