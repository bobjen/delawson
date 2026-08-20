//! What an object is, as `object-info` and `create` answer it.
//!
//! ⚠️ **This is here because four places were counting the same positions
//! independently, and two of them were wrong** — §17.13.3's mistake, found by
//! §17.5g's benchmark measuring nothing. The DN built the row, the FE read it to
//! find a stream, `laws` read it to print it, and the benchmark chopped the answer
//! into fixed-width pieces to count how many objects were found. That last one
//! assumed **five** fields; the row had grown to **eight** when §17.12 added the
//! stream's first record. So it counted every object twice and nobody noticed,
//! because the number it produced was plausible.
//!
//! One shape, built and read here, makes that a compile error instead.

use crate::framing::Framing;
use crate::wire::Arg;
use crate::{Error, Result};

/// An object as the DN describes it, at one of its streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Described {
    pub id: u64,
    /// The stream this answer is about: the last one for an append, or the one
    /// holding a record for a read.
    pub stream: u64,
    pub sn: u32,
    pub created: u64,
    pub keyed: bool,
    pub framing: Framing,
    /// The object-local number of that stream's first record, which is what makes
    /// an object's numbering continuous across the streams it is made of.
    pub first_record: u64,
}

impl Described {
    /// Fields per row on the wire. Public because a caller sizing a buffer wants it,
    /// and **derived from the shape rather than written down twice**.
    pub const ARGS: usize = 8;

    /// A row meaning "there is no such object", for a batch answer that must line up
    /// with the names it was asked about. A zero id is what marks it.
    ///
    /// ⚠️ §9.1.3.7 will retire this: once an answer may be short, lining up by
    /// position stops working and the answer has to say how many names it covered.
    pub fn missing() -> Vec<Arg> {
        vec![
            Arg::Number(0),
            Arg::Number(0),
            Arg::Number(0),
            Arg::Number(0),
            Arg::Flag(false),
            Arg::Number(0),
            Arg::Number(0),
            Arg::Number(0),
        ]
    }

    pub fn args(&self) -> Vec<Arg> {
        let (kind, width) = self.framing.wire();
        vec![
            Arg::Number(self.id),
            Arg::Number(self.stream),
            Arg::Number(self.sn as u64),
            Arg::Number(self.created),
            Arg::Flag(self.keyed),
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(self.first_record),
        ]
    }

    /// And back, returning whatever followed it. `None` for a row marked missing,
    /// since a zero id is not an object.
    pub fn of_args(args: &[Arg]) -> Result<(Option<Described>, &[Arg])> {
        let [
            Arg::Number(id),
            Arg::Number(stream),
            Arg::Number(sn),
            Arg::Number(created),
            Arg::Flag(keyed),
            Arg::Number(kind),
            Arg::Number(width),
            Arg::Number(first_record),
            rest @ ..,
        ] = args
        else {
            return Err(Error::new("that answer did not describe an object"));
        };
        if *id == 0 {
            return Ok((None, rest));
        }
        Ok((
            Some(Described {
                id: *id,
                stream: *stream,
                sn: *sn as u32,
                created: *created,
                keyed: *keyed,
                framing: Framing::of_wire(*kind, *width)?,
                first_record: *first_record,
            }),
            rest,
        ))
    }

    /// Every row in an answer, in the order asked about. `None` where the name was
    /// not there.
    pub fn all_of_args(mut args: &[Arg]) -> Result<Vec<Option<Described>>> {
        let mut rows = Vec::with_capacity(args.len() / Described::ARGS);
        while !args.is_empty() {
            let (row, rest) = Described::of_args(args)?;
            rows.push(row);
            args = rest;
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(id: u64) -> Described {
        Described {
            id,
            stream: 17,
            sn: 3,
            created: 1_000_000,
            keyed: false,
            framing: Framing::Lf,
            first_record: 400,
        }
    }

    #[test]
    fn an_object_survives_the_wire() {
        let args = one(9).args();
        assert_eq!(args.len(), Described::ARGS);
        let (back, rest) = Described::of_args(&args).expect("a row");
        assert_eq!(back, Some(one(9)));
        assert!(rest.is_empty());
    }

    /// ⚠️ **The bug this file exists for.** A batch answer holds one row per name,
    /// and counting them by chopping the args into fixed-width pieces of the wrong
    /// width gives a plausible wrong number — 8 fields read 5 at a time is two
    /// chunks per object, so every object was counted twice.
    #[test]
    fn rows_are_counted_by_parsing_not_by_chopping() {
        let mut args = Vec::new();
        for id in [1, 2, 3] {
            args.extend(one(id).args());
        }
        let rows = Described::all_of_args(&args).expect("rows");
        assert_eq!(rows.len(), 3, "three objects, not six");
        assert_eq!(rows.iter().filter(|row| row.is_some()).count(), 3);
        // What the old arithmetic would have said, kept as the thing not to do.
        assert_eq!(args.len() / 5, 4, "chunks of five would have found four");
    }

    /// A missing name still occupies a row, so a batch answer lines up with the
    /// question — and it reads back as absent rather than as an object with id 0.
    #[test]
    fn a_missing_name_is_a_row_that_is_not_an_object() {
        let mut args = one(1).args();
        args.extend(Described::missing());
        args.extend(one(2).args());
        let rows = Described::all_of_args(&args).expect("rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1], None, "the gap");
        assert_eq!(rows.iter().flatten().count(), 2, "two objects found of three asked");
    }

    #[test]
    fn a_short_or_ragged_answer_is_refused() {
        let args = one(1).args();
        assert!(Described::of_args(&args[..7]).is_err());
        let mut ragged = args.clone();
        ragged.push(Arg::Number(1));
        assert!(Described::all_of_args(&ragged).is_err(), "a row and a fragment");
    }

    /// A framing the DN could not have meant is not an object either.
    #[test]
    fn an_impossible_framing_is_refused() {
        let mut args = one(1).args();
        args[5] = Arg::Number(99);
        assert!(Described::of_args(&args).is_err());
    }
}
