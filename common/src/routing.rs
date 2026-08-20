//! Which DN owns a name.
//!
//! ⚠️ **A placeholder for #75**, which makes the range a hint and the key the name so a
//! range can be split and moved. This is the crude version of the same idea: a hash of
//! the name over however many DNs the roster holds. It is wrong in every way #75 is
//! about — a DN cannot be added without moving names, and a range cannot be split at
//! all — and it is right in the one way that matters first, which is that **the same
//! name always resolves to the same DN**. Round-robin does not, and a create landing
//! somewhere a lookup will not go is not a routing policy but a lost object.
//!
//! Everything that maps a name to a DN goes through here, so replacing it is one edit
//! rather than a hunt (§17.13.3's reason for putting wire shapes in `common`).

/// A hash of a name, spread well enough to divide by a small number.
///
/// The whole name, unlike `dn`'s shard hash, which reads eight bytes because it is
/// choosing between 64 buckets inside one process and the tail is where a generated
/// name varies. This one decides which *machine* holds an object, so a collision is
/// not a slightly uneven bucket but a permanently mislaid name, and reading the whole
/// name costs nothing next to the round trip it is about to choose.
pub fn hash_of(name: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in name.as_bytes() {
        hash = (hash ^ *byte as u64).wrapping_mul(0x100_0000_01b3);
    }
    // The low bits of an FNV hash are the least mixed, and `% count` reads exactly
    // those. One more round of mixing before anybody divides by a small number.
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^ (hash >> 33)
}

/// Which of `count` owners holds a name. Zero owners is not a question this can answer,
/// so it says the first and lets the caller find there is nothing there.
pub fn owner_of(name: &str, count: usize) -> usize {
    match count {
        0 => 0,
        count => hash_of(name) as usize % count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one property the crude version has to have: a name resolves the same way
    /// every time, or a create and a lookup disagree about where an object lives.
    #[test]
    fn a_name_always_lands_in_the_same_place() {
        for name in ["/a", "/bench/000001", "", "/x/y/z"] {
            let once = owner_of(name, 7);
            for _ in 0..4 {
                assert_eq!(owner_of(name, 7), once, "{name}");
            }
        }
    }

    /// Names differing only in the last character must not pile onto one DN, which is
    /// exactly what a generated benchmark name looks like.
    #[test]
    fn generated_names_spread() {
        let count = 8;
        let mut hit = vec![0u32; count];
        for i in 0..8000 {
            hit[owner_of(&format!("/bench/{i:06}"), count)] += 1;
        }
        let (low, high) = (*hit.iter().min().unwrap(), *hit.iter().max().unwrap());
        assert!(high < low * 2, "lumpy: {hit:?}");
    }

    /// And names differing in the *middle*, since a path has directories in it.
    #[test]
    fn paths_spread() {
        let count = 5;
        let mut hit = vec![0u32; count];
        for i in 0..5000 {
            hit[owner_of(&format!("/d{i}/object"), count)] += 1;
        }
        let (low, high) = (*hit.iter().min().unwrap(), *hit.iter().max().unwrap());
        assert!(high < low * 2, "lumpy: {hit:?}");
    }

    /// One DN is where this starts, and everything has to land on it.
    #[test]
    fn one_owner_takes_everything() {
        assert_eq!(owner_of("/anything", 1), 0);
        assert_eq!(owner_of("/anything", 0), 0);
    }
}
