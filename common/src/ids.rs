//! The things this store counts, each with a type of its own.
//!
//! ⚠️ **A raw `u32` is suspicious and a raw `u64` more so** (Asserted, Bob, 2026-08-20). The
//! store is full of numbers that are not interchangeable — a DN, a range, an object, a
//! stream, an extent, an SN, an EN, a disk — and a `u64` holding one of them says nothing
//! about which. §8.1.6 made that argument for byte offsets and §17.32 for record offsets; this
//! is the same argument for identity, which is the largest remaining population of bare
//! integers.
//!
//! **All of them are 32 bits**, and that is a decision rather than an economy: every one is
//! part of a 16-byte triple (§17.34) whose third component is a creation second, so an id has
//! only to be unique among what one minter makes in one second. A `u64` id would be eight
//! bytes buying a range nothing can reach.
//!
//! **Zero is the sentinel**, not the top of the range — the opposite of `offset`'s convention,
//! and for a reason (Asserted, Bob, 2026-08-20): *"it's not like offsets where 0 is definitely a
//! legal offset."* A count of zero is a count somebody means; a *thing* numbered zero is a thing
//! nobody minted. Both conventions reserve one value and differ in which value cannot occur
//! naturally.
//!
//! ⚠️ **And that asymmetry is worth more than tidiness.** A caller looking one of these up has to
//! deal with *not found*, and the two ways are a sentinel or an `Option` — except that reserving
//! **zero** makes them the same choice, because zero is a *niche*. Measured:
//!
//! | | size |
//! |---|---|
//! | `Id` over a plain `u32`, and `Option<Id>` | 4, **8** |
//! | `Id` over a `NonZeroU32`, and `Option<Id>` | 4, **4** |
//!
//! So `Option<Id>` can cost nothing and have the compiler insist on the *not found* branch, where
//! a sentinel costs nothing and asks every caller to remember. An offset cannot have that, because
//! its reserved value is at the top and a top-valued niche is not one Rust exposes. **The sentinel
//! stays for now** — it is what the rest of the store already does — and #112 is the conversion.

use crate::{Error, Result};

macro_rules! id {
    ($name:ident, $what:literal) => {
        #[doc = concat!("Which ", $what, ".")]
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u32);

        impl $name {
            /// **No such thing**, which for an id is zero: nothing is minted with it.
            pub const NONE: $name = $name(0);

            /// The first one a minter hands out, since zero is the sentinel.
            pub const FIRST: $name = $name(1);

            pub const fn new(id: u32) -> $name {
                $name(id)
            }

            pub const fn get(self) -> u32 {
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
                u32::try_from(number)
                    .map($name)
                    .map_err(|_| Error::new(format!(concat!("{} is not ", $what), number)))
            }

            pub const fn is_none(self) -> bool {
                self.0 == 0
            }

            pub const fn is_some(self) -> bool {
                self.0 != 0
            }

            /// The next one after this, for a minter counting upward.
            pub fn next(self) -> $name {
                $name(self.0.wrapping_add(1).max(1))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id!(DnId, "a DN");
id!(SnId, "an SN");
id!(EnId, "an EN");
// ⚠️ A **disk**, not the EN serving it: an EN serves several and a disk moves between them, so
// which EN to ask is a lookup (`placement::en_of`, §17.29.3).
id!(DiskId, "a disk");
// ⚠️ A **range**, not the DN serving it, for the same reason a disk is not an EN: #75 makes the
// range the durable key precisely because it moves between DNs.
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

    /// Zero is nothing, which is the opposite of `offset`'s sentinel and deliberately so.
    #[test]
    fn zero_is_nothing_and_one_is_the_first() {
        assert!(ExtentId::NONE.is_none());
        assert!(ExtentId::FIRST.is_some());
        assert_eq!(ExtentId::default(), ExtentId::NONE, "an unset id is no id");
        assert_eq!(ExtentId::NONE.next(), ExtentId::FIRST);
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
    }

    /// Counting upward never lands on the sentinel, so a minter that wraps hands out a
    /// colliding id rather than a nonexistent one — which #110's gate is what really prevents.
    #[test]
    fn counting_skips_the_sentinel() {
        assert_eq!(ObjectId::new(u32::MAX).next(), ObjectId::FIRST);
    }
}
