//! The things this store counts, each with a type of its own.
//!
//! ⚠️ **A raw integer is suspicious and a raw `u64` more so** (Asserted, Bob, 2026-08-20). The
//! store is full of numbers that are not interchangeable — a DN, a range, an object, a
//! stream, an extent, an SN, an EN, a disk — and a raw integer holding one of them says nothing
//! about which. §8.1.6 made that argument for byte offsets and §17.32 for record offsets; this
//! is the same argument for identity, which is the largest remaining population of bare
//! integers.
//!
//! **Most IDs are 32 bits, with `DiskId` a 16-bit id scoped within an EN.** It is not guaranteed
//! that all IDs will stay 32 bits for ever — for example, extreme scaling could eventually require
//! 64 bits for `EnId` — but types encapsulate their width so widening or narrowing is localized.
//!
//! **The top of the range is the sentinel**, the same convention `offset` uses, and a caller
//! looking one of these up and not finding it gets `NONE`.
//!
//! ⚠️ **Zero was tried and is worse** (Asserted, Bob, 2026-08-20). It is tempting, since a thing
//! numbered zero is a thing nobody minted — and §14.6.2 makes **the id the index** into its arena,
//! so reserving zero reserves slot 0, and then *"the count is one bigger than it ought to be"* in
//! every length, every total, every answer to how many things an arena holds. Spending that slot
//! on the arena's header does not rescue it: it puts *"a type-shaped thing that somehow isn't a
//! type"* in an array of records — and the header does not fit anyway, being a singleton with
//! statistics, accumulating statistics and locks that GC reads (§17.37.2).
//!
//! **The one thing that would justify zero is a niche.** `Option<Id>` is 8 bytes over a plain
//! `u32` and **4 over a `NonZeroU32`** — the same as the id — so reserving *zero* would let the
//! compiler insist on the not-found branch for free, where a sentinel asks every caller to
//! remember. That is a real argument and it is the only one; taking it means taking the wasted
//! slot with it (#112). Until it is taken, the top of the range costs nothing and leaves the
//! arenas dense.

use crate::{Error, Result};

macro_rules! id {
    ($name:ident, $ty:ty, $what:literal) => {
        #[doc = concat!("Which ", $what, ".")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($ty);

        /// ⚠️ **`NONE`, not zero.** Zero is a real id now that the sentinel is at the top, so
        /// a derived `Default` would hand out the first slot of an arena as "unset".
        impl Default for $name {
            fn default() -> $name {
                $name::NONE
            }
        }

        impl $name {
            /// **No such thing** — the top of the range, so that every slot of an arena
            /// indexed by one of these is a slot a real thing can occupy (§14.6.2).
            pub const NONE: $name = $name(<$ty>::MAX);

            /// The largest real one, one below `NONE`.
            pub const MAX: $name = $name(<$ty>::MAX - 1);

            /// The first one a minter hands out, which is the first slot of its arena.
            pub const FIRST: $name = $name(0);

            pub const fn new(id: $ty) -> $name {
                $name(id)
            }

            pub const fn get(self) -> $ty {
                self.0
            }

            /// For the wire, where every number is 64 bits.
            pub const fn number(self) -> u64 {
                self.0 as u64
            }

            /// And back. ⚠️ **Refuses rather than truncating**: a number too large to be one of
            /// these is a caller that thinks the space is wider than it is, and silently
            /// keeping the low bits would hand back a different thing's id.
            pub fn of_number(number: u64) -> Result<$name> {
                <$ty>::try_from(number)
                    .map($name)
                    .map_err(|_| Error::new(format!(concat!("{} is not ", $what), number)))
            }

            pub const fn is_none(self) -> bool {
                self.0 == <$ty>::MAX
            }

            pub const fn is_some(self) -> bool {
                self.0 != <$ty>::MAX
            }

            /// The next one after this, for a minter counting upward. ⚠️ Wraps to the first
            /// rather than landing on the sentinel — and a minter that reaches here is one
            /// #110's gate has to have been keeping honest, since a wrapped id collides.
            pub fn next(self) -> $name {
                match self.0 + 1 {
                    <$ty>::MAX => $name::FIRST,
                    next => $name(next),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
    ($name:ident, $what:literal) => {
        id!($name, u32, $what);
    };
}

id!(DnId, "a DN");
id!(SnId, "an SN");
id!(EnId, "an EN");
// ⚠️ A **disk** within an EN, scoped by `(EnId, DiskId)`. 16 bits within its EN.
id!(DiskId, u16, "a disk");
// ⚠️ An SN-only index into its array of all known disks across all ENs.
id!(SnDiskId, "an SN disk index");
// ⚠️ A **range**, not the DN serving it, for the same reason a disk is not an EN. But note what
// #75 actually settles: the range is a **hint** and the *key* is the durable name — a range is more
// stable than a DN and is not itself stable, because a split renames it (§17.38.1).
id!(RangeId, "a range");
id!(ObjectId, "an object");
id!(StreamId, "a stream");
id!(ExtentId, "an extent");

#[cfg(test)]
mod tests {
    use super::*;

    /// ⚠️ **The point of the whole module**: these do not substitute for one another. This does
    /// not compile if any two collapse into an alias, which is what a `type` would have done.
    #[test]
    fn one_kind_of_id_is_not_another() {
        let object = ObjectId::new(7);
        let stream = StreamId::new(7);
        assert_eq!(object.number(), stream.number(), "the same number");
        // And still not the same thing: `object == stream` does not compile, which is the
        // property this test exists to hold rather than to demonstrate.
        assert_eq!(object, ObjectId::new(7));
    }

    /// ⚠️ **Zero is a real id**, which is the whole reason the sentinel is at the top: §14.6.2
    /// makes the id the index into its arena, so a reserved zero would leave slot 0 empty and
    /// every count one larger than the thing it counts (§17.37.2).
    #[test]
    fn zero_is_a_real_id_and_the_top_is_not() {
        assert!(ExtentId::FIRST.is_some(), "the first slot of an arena holds a real extent");
        assert_eq!(ExtentId::FIRST.get(), 0);
        assert!(ExtentId::NONE.is_none());
        assert!(ExtentId::MAX.is_some());
        // ⚠️ And an unset id is `NONE` rather than the derived zero, which would have handed
        // out the first slot of an arena as "nothing here".
        assert_eq!(ExtentId::default(), ExtentId::NONE, "an unset id is no id");
    }

    /// ⚠️ A number too wide is refused, not truncated: keeping the low bits would hand back a
    /// different thing's id, which is worse than an error by exactly the amount that a wrong
    /// answer is worse than no answer.
    #[test]
    fn a_number_too_wide_is_refused() {
        assert_eq!(SnId::of_number(4_000_000_000).unwrap().get(), 4_000_000_000);
        assert!(SnId::of_number(u32::MAX as u64 + 1).is_err());
        let said = DnId::of_number(1 << 40).expect_err("too wide").to_string();
        assert!(said.contains("a DN"), "and it says which kind: {said}");

        // DiskId is 16-bit
        assert_eq!(DiskId::of_number(60_000).unwrap().get(), 60_000);
        assert!(DiskId::of_number(u16::MAX as u64 + 1).is_err());
        let said_disk = DiskId::of_number(1 << 20).expect_err("too wide").to_string();
        assert!(said_disk.contains("a disk"), "and it says which kind: {said_disk}");
    }

    /// Counting upward never lands on the sentinel, so a minter that wraps hands out a
    /// colliding id rather than a nonexistent one — which #110's gate is what really prevents.
    #[test]
    fn counting_skips_the_sentinel() {
        assert_eq!(ObjectId::FIRST.next(), ObjectId::new(1));
        assert_eq!(ObjectId::MAX.next(), ObjectId::FIRST, "wrapped, not nothing");
        assert_eq!(DiskId::MAX.next(), DiskId::FIRST, "wrapped 16-bit");
    }
}
