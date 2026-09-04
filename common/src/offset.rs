//! Positions that share a width and must not share a type.
//!
//! Two kinds of position in an extent's bytes, and three in its records.
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
//!
//! ⚠️ **The record spaces are the same argument with a worse precedent.** A record
//! number means one of three things — its position in an *object*, in a *stream*, or
//! in an *extent* — and §17.16.2 is what happened when a conversion between them was
//! skipped: 139 of 200 reads returned the wrong record, invisibly until the first seal,
//! because all three origins are zero while an object fits one extent. Object-local and
//! stream-local also share a width, so nothing but a type separates them.
//!
//! **A count and a position share a type here, deliberately**, as they do for bytes:
//! adding a count to a position is ordinary arithmetic. ⚠️ That leaves one confusion
//! these types do not catch, and §17.29.5 is an instance of it — a count passed where a
//! position was wanted, in the same space. What catches that is not naming the space but
//! naming the *pair*, which is what `sn::Placed` does.

use crate::{Error, Result};

macro_rules! space {
    ($name:ident, $width:ty, $unit:literal, $within:literal) => {
        #[doc = concat!("A position or count of ", $unit, " in ", $within, ".")]
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($width);

        impl $name {
            pub const ZERO: $name = $name(0);

            /// **No value given**, which is the top of the range rather than zero,
            /// because zero is a count a caller can legitimately mean (§8.1.6.2).
            pub const NONE: $name = $name(<$width>::MAX);

            /// The largest real value, one below `NONE`.
            ///
            /// ⚠️ **Reserving it costs almost nothing, which is why every space does
            /// it** (Asserted, Bob, 2026-08-19): a bound has to be compared against
            /// anyway to keep *one past the end* representable, and comparing against
            /// `MAX - 1` is the same instruction as comparing against `MAX`. So the
            /// reservation is uniform whether or not a given space uses it today —
            /// and a space that needs it later does not have to change its documented
            /// maximum, which would be a format contract change.
            pub const MAX: $name = $name(<$width>::MAX - 1);

            /// ⚠️ **Unchecked, for literals.** `new(MAX)` is `NONE`, which is almost
            /// certainly not what a caller writing a literal meant; the checked
            /// constructors are `of_len` and `of_number`.
            pub const fn new(count: $width) -> $name {
                $name(count)
            }

            pub const fn get(self) -> $width {
                self.0
            }

            pub const fn is_none(self) -> bool {
                self.0 == <$width>::MAX
            }

            pub const fn is_some(self) -> bool {
                self.0 != <$width>::MAX
            }

            /// From a count in hand, which is how a length arrives.
            pub fn of_len(len: usize) -> Result<$name> {
                match <$width>::try_from(len) {
                    Ok(count) if count < <$width>::MAX => Ok($name(count)),
                    _ => Err(Error::new(format!(
                        concat!("{} is too many ", $unit, " for ", $within),
                        len
                    ))),
                }
            }

            /// From the wire, where every number is 64 bits. `NONE` survives the trip,
            /// since a field that may be absent has to be able to say so.
            pub fn of_number(number: u64) -> Result<$name> {
                <$width>::try_from(number).map($name).map_err(|_| {
                    Error::new(format!(concat!($unit, " {} is past the end of ", $within), number))
                })
            }

            pub const fn number(self) -> u64 {
                self.0 as u64
            }

            /// Move on by a count in the same space. Running past what a position can
            /// name is a reason to stop, not a reason to wrap.
            ///
            /// **Stops one short of the type's ceiling**, since the top value is
            /// `NONE`. That is the same check either way — §8.1.6.2's point that the
            /// sentinel rides along free.
            pub fn plus(self, count: $name) -> Result<$name> {
                match self.0.checked_add(count.0) {
                    Some(sum) if sum < <$width>::MAX => Ok($name(sum)),
                    _ => Err(Error::new(format!(
                        concat!($within, " cannot reach ", $unit, " {} plus {}"),
                        self.0, count.0
                    ))),
                }
            }

            /// How far this is past an earlier position in the same space. ⚠️ Saturating
            /// rather than wrapping: a position before the origin is a bug at the caller,
            /// and a huge positive number is a worse way to learn about it than a zero.
            pub fn since(self, origin: $name) -> $name {
                $name(self.0.saturating_sub(origin.0))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

// The three byte spaces (logical and compressed):
// Extent-local is 32-bit because an extent is capped at 32 MB.
// Stream-local and object-local are 64-bit because they sum over extents and streams.
space!(ELoff, u32, "logical bytes", "an extent");
space!(ECoff, u32, "compressed bytes", "an extent");
space!(SLoff, u64, "logical bytes", "a stream");
space!(SCoff, u64, "compressed bytes", "a stream");
space!(OLoff, u64, "logical bytes", "an object");
space!(OCoff, u64, "compressed bytes", "an object");

// The three record spaces §17.16.2 cost 139 of 200 reads by confusing (Asserted, Bob,
// 2026-08-20). ⚠️ **Object-local and stream-local are the dangerous pair** — *"exceptionally
// easy to confuse"* — and they are the two that share a width, so a type is the only thing
// that can tell them apart. Extent-local is 32-bit because an extent seals at 32 MB, so even
// one-byte records do not reach 4.3 × 10^9.
space!(ORoff, u64, "records", "an object");
space!(SRoff, u64, "records", "a stream");
space!(ERoff, u32, "records", "an extent");

/// Explicitly move a record position between its nesting levels. These are not wire
/// conversions: naming the source and destination keeps a record in one space from
/// being silently used in another.
impl SRoff {
    pub const fn from_extent(value: ERoff) -> SRoff {
        SRoff::new(value.get() as u64)
    }
}

impl ORoff {
    pub const fn from_stream(value: SRoff) -> ORoff {
        ORoff::new(value.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_position_moves_on_by_a_length() {
        let at = ECoff::new(30);
        assert_eq!(at.plus(ECoff::new(15)).unwrap(), ECoff::new(45));
        assert_eq!(ECoff::ZERO.plus(ECoff::new(30)).unwrap(), at);
    }

    /// Running out of positions is the extent being full, not an offset wrapping
    /// to the start of one.
    #[test]
    fn running_off_the_end_is_an_error_not_a_wrap() {
        assert!(ECoff::new(u32::MAX).plus(ECoff::new(1)).is_err());
        // ⚠️ And it stops one short of the ceiling, because the top value is NONE.
        // This used to be `is_ok`, which would have produced a length equal to the
        // sentinel — a full extent reporting "no value" (§8.1.6.2).
        assert!(ELoff::new(u32::MAX - 1).plus(ELoff::new(1)).is_err());
        assert_eq!(ELoff::MAX.plus(ELoff::ZERO).unwrap(), ELoff::MAX);
    }

    /// The sentinel is the top of the range, not zero, because zero is a length a
    /// caller can legitimately mean.
    #[test]
    fn nothing_is_the_top_of_the_range_and_zero_is_a_real_length() {
        assert!(ECoff::NONE.is_none());
        assert!(!ECoff::ZERO.is_none(), "zero bytes is a length, not an absence");
        assert!(ECoff::ZERO.is_some());
        assert!(ECoff::MAX.is_some());
        assert_eq!(ECoff::MAX.get(), u32::MAX - 1);
    }

    /// A length equal to the sentinel is refused rather than silently becoming it,
    /// which is the whole point of reserving a value.
    #[test]
    fn a_length_may_not_be_the_sentinel() {
        assert!(ELoff::of_len(u32::MAX as usize).is_err());
        assert!(
            ELoff::of_len(u32::MAX as usize - 1).is_ok(),
            "one below is the largest real length"
        );
    }

    /// But it survives the wire, since a field that may be absent has to say so.
    #[test]
    fn the_sentinel_survives_the_wire() {
        let sent = ECoff::NONE;
        let back = ECoff::of_number(sent.number()).expect("none is a value on the wire");
        assert!(back.is_none());
    }

    /// The wire carries 64-bit numbers, and a position that could not have come
    /// from an extent is refused rather than truncated.
    #[test]
    fn a_position_off_the_wire_is_checked() {
        assert_eq!(ELoff::of_number(1024).unwrap(), ELoff::new(1024));
        assert!(ELoff::of_number(u64::from(u32::MAX) + 1).is_err());
        assert_eq!(ELoff::new(1024).number(), 1024);
    }

    #[test]
    fn a_length_in_hand_is_checked_too() {
        assert_eq!(ECoff::of_len(30).unwrap(), ECoff::new(30));
        assert!(ECoff::of_len(usize::MAX).is_err());
    }

    /// The point of the exercise: the two do not mix, and this is the test that
    /// would stop compiling if they ever did.
    #[test]
    fn the_two_spaces_do_not_mix() {
        let logical = ELoff::new(100);
        let compressed = ECoff::new(100);
        assert_eq!(logical.get(), compressed.get());
        // Deliberately not written: `logical.plus(compressed)`, which is the bug
        // these types exist to refuse, and it does not compile.
    }

    /// ⚠️ **The three record spaces are three types, not one.** This does not compile if
    /// they collapse into an alias, which is the whole point — object-local and
    /// stream-local share a width, so the compiler is the only thing between them
    /// (§17.16.2).
    #[test]
    fn a_record_space_is_not_another_record_space() {
        let in_object = ORoff::new(700);
        let in_stream = SRoff::new(40);
        // The conversion is arithmetic the caller has to write down, which is the point:
        // object-local is stream-local plus where the stream starts in the object.
        let converted = ORoff::new(in_object.number() + in_stream.number());
        assert_eq!(converted, ORoff::new(740));
        // And an extent's space is narrower, because an extent seals long before 2^32.
        assert!(ERoff::of_number(u32::MAX as u64 + 1).is_err());
        assert!(SRoff::of_number(u32::MAX as u64 + 1).is_ok());
    }

    /// The stream and object byte spaces are separate 64-bit types.
    #[test]
    fn stream_and_object_byte_spaces_are_separate() {
        let in_obj = OLoff::new(5000);
        let in_stream = SLoff::new(200);
        let converted = OLoff::new(in_obj.number() + in_stream.number());
        assert_eq!(converted, OLoff::new(5200));

        let comp_obj = OCoff::new(4000);
        let comp_stream = SCoff::new(150);
        let comp_converted = OCoff::new(comp_obj.number() + comp_stream.number());
        assert_eq!(comp_converted, OCoff::new(4150));
    }

    /// Going back further than the origin is a caller bug, and a zero says so better
    /// than an enormous positive number does.
    #[test]
    fn a_position_before_its_origin_saturates() {
        assert_eq!(SRoff::new(10).since(SRoff::new(4)), SRoff::new(6));
        assert_eq!(SRoff::new(4).since(SRoff::new(10)), SRoff::ZERO);
    }
    /// ⚠️ **A 16-byte triple against a 16-byte GUID** (Asserted, Bob, 2026-08-20): *"Microsoft
    /// usually used 16-byte GUIDs for identifiers. I'm using 16-byte triples. Mine compress
    /// better and are guaranteed not to collide for a few million years."*
    ///
    /// The same width, so the claim is entirely about what a *list* of them costs — and a
    /// stream's extent list is the largest structure the store keeps (§4.7.1). This measures it.
    #[test]
    fn a_list_of_triples_compresses_and_a_list_of_guids_does_not() {
        let mut rng = crate::random::Rng::new();
        let (mut triples, mut guids, mut beside) = (Vec::new(), Vec::new(), Vec::new());
        // Ten thousand extents of one stream: one SN, consecutive ids, seconds creeping up.
        let mut second = 1_787_000_000u64;
        for id in 0..10_000u32 {
            triples.extend_from_slice(&7u32.to_le_bytes());
            triples.extend_from_slice(&id.to_le_bytes());
            triples.extend_from_slice(&second.to_le_bytes());
            second += u64::from(id % 3 == 0);
            guids.extend_from_slice(&rng.next_u64().to_le_bytes());
            guids.extend_from_slice(&rng.next_u64().to_le_bytes());
            // ⚠️ And what a GUID does *not* carry has to be stored beside it: the creating SN,
            // which §7.3.3 needs to derive the key, and the second, which selects the day's
            // master key and is the extent's age. The triple has both inside its 16 bytes.
            beside.extend_from_slice(&guids[guids.len() - 16..]);
            beside.extend_from_slice(&7u32.to_le_bytes());
            beside.extend_from_slice(&second.to_le_bytes());
        }
        assert_eq!(triples.len(), guids.len(), "16 bytes apiece either way");
        assert_eq!(beside.len(), triples.len() * 28 / 16, "but a GUID needs 12 more beside it");

        let small = lz4_flex::compress_prepend_size(&triples).len();
        let big = lz4_flex::compress_prepend_size(&guids).len();
        let whole = lz4_flex::compress_prepend_size(&beside).len();
        println!(
            "10,000 names: triples {} B raw -> {small} B; guids {} B -> {big} B; \
             guids plus what they do not carry {} B -> {whole} B",
            triples.len(),
            guids.len(),
            beside.len()
        );
        // ⚠️ **3x, not 30x, and the shortfall is the compressor rather than the encoding.**
        // LZ4 matches repeated byte *sequences*: the SN field repeats every 16 bytes and the
        // second barely moves, so those compress — but the id increments, and an incrementing
        // integer has no repeated bytes to find. A delta encoder would spend about a bit on
        // each, taking a name to well under a byte. So 3x is the floor and the ceiling is
        // around 30x, while a GUID has neither, being random on purpose (§17.34).
        assert!(small * 2 < big, "triples {small} against guids {big}");
        assert!(big >= triples.len(), "random bytes do not compress at all");
        // ⚠️ **The honest comparison is against this, not against a bare GUID** (Asserted,
        // Bob, 2026-08-20): the triple *encodes other fields that would have to be stored
        // separately otherwise*. So the triple is smaller before compression as well as
        // after — 16 bytes against 28.
        assert!(whole > small * 3, "triples {small} against guids-and-fields {whole}");
    }
}
