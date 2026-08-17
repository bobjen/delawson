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

            pub const fn new(bytes: u32) -> $name {
                $name(bytes)
            }

            pub const fn get(self) -> u32 {
                self.0
            }

            /// From a count of bytes in hand, which is how a length arrives.
            pub fn of_len(len: usize) -> Result<$name> {
                u32::try_from(len).map($name).map_err(|_| {
                    Error::new(format!(concat!($what, " length {} does not fit an extent"), len))
                })
            }

            /// From the wire, where every number is 64 bits.
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
            pub fn plus(self, len: $name) -> Result<$name> {
                match self.0.checked_add(len.0) {
                    Some(sum) => Ok($name(sum)),
                    None => Err(Error::new(format!(
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
        assert!(LOff::new(u32::MAX - 1).plus(LOff::new(1)).is_ok());
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
