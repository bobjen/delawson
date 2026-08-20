//! Two kinds of position in an extent, and they are not interchangeable.
//!
//! A **logical** position counts the bytes a writer wrote. A **compressed** one
//! counts the bytes that were stored. Both are 32-bit — an extent is capped well
//! inside that — so nothing but a type stops one being used where the other
//! belongs, and the two are equal in exactly the case that makes the mistake
//! invisible: when nothing is compressing yet.
//!
//! So they are separate types, and the compiler is what catches it. A length in a
//! space is the same type as a position in it, deliberately: adding a length to a
//! position is ordinary arithmetic, and the confusion being prevented is logical
//! against compressed rather than position against length.
//!
//! A locator handed back to a writer is logical, since a writer knows what it
//! wrote and not what that compressed to. What is on a device is compressed, and
//! the sparse index is what relates the two.

use crate::{Error, Result};

macro_rules! space {
    ($name:ident, $what:literal) => {
        #[doc = concat!("A position or length in ", $what, " bytes.")]
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u32);

        impl $name {
            pub const ZERO: $name = $name(0);

            /// **No value given**, which is the top of the range rather than zero,
            /// because zero is a length a caller can legitimately mean (§8.1.6.2).
            pub const NONE: $name = $name(u32::MAX);

            /// The largest real value, one below `NONE`.
            ///
            /// ⚠️ **Reserving it costs almost nothing, which is why every space does
            /// it** (Asserted, Bob, 2026-08-19): a bound has to be compared against
            /// anyway to keep *one past the end* representable, and comparing against
            /// `MAX - 1` is the same instruction as comparing against `MAX`. So the
            /// reservation is uniform whether or not a given space uses it today —
            /// and a space that needs it later does not have to change its documented
            /// maximum, which would be a format contract change.
            pub const MAX: $name = $name(u32::MAX - 1);

            /// ⚠️ **Unchecked, for literals.** `new(u32::MAX)` is `NONE`, which is
            /// almost certainly not what a caller writing a literal meant; the
            /// checked constructors are `of_len` and `of_number`.
            pub const fn new(bytes: u32) -> $name {
                $name(bytes)
            }

            pub const fn get(self) -> u32 {
                self.0
            }

            pub const fn is_none(self) -> bool {
                self.0 == u32::MAX
            }

            pub const fn is_some(self) -> bool {
                self.0 != u32::MAX
            }

            /// From a count of bytes in hand, which is how a length arrives.
            pub fn of_len(len: usize) -> Result<$name> {
                match u32::try_from(len) {
                    Ok(bytes) if bytes < u32::MAX => Ok($name(bytes)),
                    _ => Err(Error::new(format!(
                        concat!($what, " length {} does not fit an extent"),
                        len
                    ))),
                }
            }

            /// From the wire, where every number is 64 bits. `NONE` survives the trip,
            /// since a field that may be absent has to be able to say so.
            pub fn of_number(number: u64) -> Result<$name> {
                u32::try_from(number).map($name).map_err(|_| {
                    Error::new(format!(
                        concat!($what, " position {} is past the end of any extent"),
                        number
                    ))
                })
            }

            pub const fn number(self) -> u64 {
                self.0 as u64
            }

            /// Move on by a length in the same space. An extent that would run past
            /// what a position can name is full, which is a reason to seal it and
            /// not a reason to wrap.
            ///
            /// **Stops one short of the type's ceiling**, since the top value is
            /// `NONE`. That is the same check either way — §8.1.6.2's point that the
            /// sentinel rides along free.
            pub fn plus(self, len: $name) -> Result<$name> {
                match self.0.checked_add(len.0) {
                    Some(sum) if sum < u32::MAX => Ok($name(sum)),
                    _ => Err(Error::new(format!(
                        concat!("an extent cannot reach ", $what, " byte {} plus {}"),
                        self.0, len.0
                    ))),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

space!(LOff, "logical");
space!(COff, "compressed");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_position_moves_on_by_a_length() {
        let at = COff::new(30);
        assert_eq!(at.plus(COff::new(15)).unwrap(), COff::new(45));
        assert_eq!(COff::ZERO.plus(COff::new(30)).unwrap(), at);
    }

    /// Running out of positions is the extent being full, not an offset wrapping
    /// to the start of one.
    #[test]
    fn running_off_the_end_is_an_error_not_a_wrap() {
        assert!(COff::new(u32::MAX).plus(COff::new(1)).is_err());
        // ⚠️ And it stops one short of the ceiling, because the top value is NONE.
        // This used to be `is_ok`, which would have produced a length equal to the
        // sentinel — a full extent reporting "no value" (§8.1.6.2).
        assert!(LOff::new(u32::MAX - 1).plus(LOff::new(1)).is_err());
        assert_eq!(LOff::MAX.plus(LOff::ZERO).unwrap(), LOff::MAX);
    }

    /// The sentinel is the top of the range, not zero, because zero is a length a
    /// caller can legitimately mean.
    #[test]
    fn nothing_is_the_top_of_the_range_and_zero_is_a_real_length() {
        assert!(COff::NONE.is_none());
        assert!(!COff::ZERO.is_none(), "zero bytes is a length, not an absence");
        assert!(COff::ZERO.is_some());
        assert!(COff::MAX.is_some());
        assert_eq!(COff::MAX.get(), u32::MAX - 1);
    }

    /// A length equal to the sentinel is refused rather than silently becoming it,
    /// which is the whole point of reserving a value.
    #[test]
    fn a_length_may_not_be_the_sentinel() {
        assert!(LOff::of_len(u32::MAX as usize).is_err());
        assert!(
            LOff::of_len(u32::MAX as usize - 1).is_ok(),
            "one below is the largest real length"
        );
    }

    /// But it survives the wire, since a field that may be absent has to say so.
    #[test]
    fn the_sentinel_survives_the_wire() {
        let sent = COff::NONE;
        let back = COff::of_number(sent.number()).expect("none is a value on the wire");
        assert!(back.is_none());
    }

    /// The wire carries 64-bit numbers, and a position that could not have come
    /// from an extent is refused rather than truncated.
    #[test]
    fn a_position_off_the_wire_is_checked() {
        assert_eq!(LOff::of_number(1024).unwrap(), LOff::new(1024));
        assert!(LOff::of_number(u64::from(u32::MAX) + 1).is_err());
        assert_eq!(LOff::new(1024).number(), 1024);
    }

    #[test]
    fn a_length_in_hand_is_checked_too() {
        assert_eq!(COff::of_len(30).unwrap(), COff::new(30));
        assert!(COff::of_len(usize::MAX).is_err());
    }

    /// The point of the exercise: the two do not mix, and this is the test that
    /// would stop compiling if they ever did.
    #[test]
    fn the_two_spaces_do_not_mix() {
        let logical = LOff::new(100);
        let compressed = COff::new(100);
        assert_eq!(logical.get(), compressed.get());
        // Deliberately not written: `logical.plus(compressed)`, which is the bug
        // these types exist to refuse, and it does not compile.
    }
}
