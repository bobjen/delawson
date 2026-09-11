//! Inline flags and sparse retention metadata. Lock the owner before its extras.
use crate::ids::{DiskId, EnId, ExtentId, SnId};
use crate::info::StreamName;
use crate::time::{TimeIntSec64, TimeSec};
use crate::wire::{Op, Req, Resp, Status, StructWriter, structure};
use crate::{Error, Result};
use std::collections::HashMap;
use std::hash::Hash;

/// External names always include the creation second; a vector index is not a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Object(String),
    Stream(StreamName),
    Instance { en: EnId, disk: DiskId, sn: SnId, extent: ExtentId, created: TimeSec },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delete {
    pub target: Target,
    /// Some for soft deletion, None for hard deletion.
    pub retention: Option<TimeIntSec64>,
    /// May accompany soft deletion, making both transitions one atomic operation.
    pub hard: bool,
}

impl Delete {
    pub fn request(&self) -> Result<Req> {
        if let Some(retention) = self.retention {
            valid_interval(retention)?;
        }
        if self.retention.is_none() && !self.hard {
            return Err(Error::new("empty deletion request"));
        }
        let mut payload = StructWriter::new();
        let soft = self.retention.is_some();
        let op = match &self.target {
            Target::Object(path) => {
                payload.text(path);
                if soft { Op::SoftDeleteObject } else { Op::DeleteObject }
            }
            Target::Stream(name) => {
                payload.range_id(name.range);
                payload.stream_id(name.stream);
                payload.time_sec(name.created);
                if soft { Op::SoftDeleteStream } else { Op::DeleteStream }
            }
            Target::Instance { en, disk, sn, extent, created } => {
                payload.en_id(*en);
                payload.disk_id(*disk);
                payload.sn_id(*sn);
                payload.extent_id(*extent);
                payload.time_sec(*created);
                if soft { Op::SoftDeleteExtentInstance } else { Op::DeleteExtentInstance }
            }
        };
        if let Some(retention) = self.retention {
            // Signed two's-complement seconds, not a timestamp or unsigned duration.
            payload.number_u64(retention.get() as u64);
            payload.flag(self.hard);
        }
        Ok(Req::new(op).structure(payload))
    }

    pub fn of(request: &Req) -> Result<Self> {
        let (mut payload, rest) = structure(&request.args, request.op, "a deletion request")?;
        let (target, soft) = match request.op {
            Op::SoftDeleteObject | Op::DeleteObject => {
                (Target::Object(payload.text()?), request.op == Op::SoftDeleteObject)
            }
            Op::SoftDeleteStream | Op::DeleteStream => (
                Target::Stream(StreamName {
                    range: payload.range_id()?,
                    stream: payload.stream_id()?,
                    created: payload.time_sec()?,
                }),
                request.op == Op::SoftDeleteStream,
            ),
            Op::SoftDeleteExtentInstance | Op::DeleteExtentInstance => (
                Target::Instance {
                    en: payload.en_id()?,
                    disk: payload.disk_id()?,
                    sn: payload.sn_id()?,
                    extent: payload.extent_id()?,
                    created: payload.time_sec()?,
                },
                request.op == Op::SoftDeleteExtentInstance,
            ),
            _ => return Err(Error::new("not a deletion operation")),
        };
        let retention = if soft {
            let interval = TimeIntSec64::new(payload.number_u64()? as i64);
            valid_interval(interval)?;
            Some(interval)
        } else {
            None
        };
        let hard = if soft { payload.flag()? } else { true };
        payload.done()?;
        if !rest.is_empty() {
            return Err(Error::new("deletion request has trailing arguments"));
        }
        Ok(Self { target, retention, hard })
    }

    pub fn of_with_limit(request: &Req, maximum: TimeIntSec64) -> Result<Self> {
        let deletion = Self::of(request)?;
        if let Some(retention) = deletion.retention {
            check_limit(retention, maximum)?;
        }
        Ok(deletion)
    }

    /// Phase boundary: validate the message but do not mutate any metadata yet.
    pub fn stub(request: &Req, stage: &str, maximum: TimeIntSec64) -> Resp {
        match Self::of_with_limit(request, maximum) {
            Ok(_) => Resp::error(
                Status::Unimplemented,
                format!("I got this far: {stage}; deletion propagation is not implemented yet"),
            ),
            Err(error) => Resp::error(Status::BadRequest, error.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Flags(u8);

impl Flags {
    const EXTRA: u8 = 1;
    const SOFT_DELETED: u8 = 2;
    const DELETED: u8 = 4;
    pub fn extra(self) -> bool {
        self.0 & Self::EXTRA != 0
    }
    pub fn soft_deleted(self) -> bool {
        self.0 & Self::SOFT_DELETED != 0
    }
    pub fn deleted(self) -> bool {
        self.0 & Self::DELETED != 0
    }
    pub fn readable(self, include_soft_deleted: bool) -> bool {
        !self.deleted() && (!self.soft_deleted() || include_soft_deleted)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Extra {
    pub retain_until: TimeSec,
}

#[derive(Debug)]
pub struct Extras<K>(HashMap<K, Extra>);
impl<K> Default for Extras<K> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}
impl<K: Eq + Hash> Extras<K> {
    pub fn get(&self, key: &K) -> Option<&Extra> {
        self.0.get(key)
    }
    /// First soft-delete notification. Retries do not restart retention.
    /// The caller releases child references only when this returns true.
    pub fn soft_delete(
        &mut self,
        key: K,
        flags: &mut Flags,
        now: TimeSec,
        retention: TimeIntSec64,
        maximum: TimeIntSec64,
    ) -> Result<bool> {
        check_limit(retention, maximum)?;
        let until = deadline(now, retention)?;
        if flags.soft_deleted() {
            return Ok(false);
        }
        self.0.insert(key, Extra { retain_until: until });
        flags.0 |= Flags::EXTRA | Flags::SOFT_DELETED;
        Ok(true)
    }
    /// Hard deletion is rejected until the existing soft-delete retention has expired.
    pub fn delete(&self, key: &K, flags: &mut Flags, now: TimeSec) -> Result<bool> {
        if !flags.soft_deleted()
            || !flags.extra()
            || !self.get(key).is_some_and(|extra| extra.retain_until < now)
        {
            return Err(Error::new("hard delete requires soft deletion with expired retention"));
        }
        let first = !flags.deleted();
        flags.0 |= Flags::DELETED;
        Ok(first)
    }

    /// Atomic combined request. A negative interval permits immediate deletion of a
    /// live record. It cannot shorten an already established retention deadline.
    /// Returns (first soft deletion, first hard deletion) for exactly-once reference release.
    pub fn soft_delete_and_delete(
        &mut self,
        key: K,
        flags: &mut Flags,
        now: TimeSec,
        retention: TimeIntSec64,
        maximum: TimeIntSec64,
    ) -> Result<(bool, bool)> {
        check_limit(retention, maximum)?;
        let until = deadline(now, retention)?;
        if flags.soft_deleted() {
            return Ok((false, self.delete(&key, flags, now)?));
        }
        if until >= now {
            return Err(Error::new("combined deletion requires expired retention"));
        }
        self.0.insert(key, Extra { retain_until: until });
        flags.0 |= Flags::EXTRA | Flags::SOFT_DELETED | Flags::DELETED;
        Ok((true, true))
    }

    pub fn reclaimable(&self, key: &K, flags: Flags, now: TimeSec) -> bool {
        flags.deleted()
            && flags.extra()
            && self.get(key).is_some_and(|extra| extra.retain_until < now)
    }
    /// Caller removes the owning record in the same critical section.
    pub fn reclaim(&mut self, key: &K, flags: Flags, now: TimeSec) -> bool {
        if !self.reclaimable(key, flags, now) {
            return false;
        }
        self.0.remove(key);
        true
    }
}

fn valid_interval(retention: TimeIntSec64) -> Result<()> {
    if !retention.is_finite() && retention != TimeIntSec64::MIN {
        return Err(Error::new("retention must be finite or MIN"));
    }
    Ok(())
}

fn check_limit(retention: TimeIntSec64, maximum: TimeIntSec64) -> Result<()> {
    maximum.to_duration()?;
    valid_interval(retention)?;
    if retention > maximum {
        return Err(Error::new(format!(
            "retention exceeds configured maximum of {} seconds",
            maximum.get()
        )));
    }
    Ok(())
}

fn deadline(now: TimeSec, retention: TimeIntSec64) -> Result<TimeSec> {
    valid_interval(retention)?;
    // MIN means already expired, not a reserved timestamp or arithmetic delta.
    // Store an ordinary representable time immediately before this operation.
    if retention == TimeIntSec64::MIN {
        return now
            .number()
            .checked_sub(1)
            .map(TimeSec::new)
            .ok_or_else(|| Error::new("no timestamp precedes the epoch"));
    }
    now.number()
        .checked_add_signed(retention.get())
        .map(TimeSec::new)
        .ok_or_else(|| Error::new("retention timestamp overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    const CAP: TimeIntSec64 = TimeIntSec64::new(1000);

    #[test]
    fn min_means_expired_without_reserving_a_timestamp() {
        for now in [TimeSec::new(1), TimeSec::new(100), TimeSec::new(u64::MAX)] {
            let mut extras = Extras::default();
            let mut flags = Flags::default();
            assert_eq!(
                extras.soft_delete_and_delete(7, &mut flags, now, TimeIntSec64::MIN, CAP).unwrap(),
                (true, true)
            );
            assert_eq!(extras.get(&7).unwrap().retain_until.number(), now.number() - 1);
            assert!(extras.reclaimable(&7, flags, now));
        }
        assert!(deadline(TimeSec::EPOCH, TimeIntSec64::MIN).is_err());
        // MIN also works without the hard-delete flag.
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        extras.soft_delete(7, &mut flags, TimeSec::new(10), TimeIntSec64::MIN, CAP).unwrap();
        assert!(!extras.reclaimable(&7, flags, TimeSec::new(10)));
        assert!(extras.delete(&7, &mut flags, TimeSec::new(10)).unwrap());
    }

    #[test]
    fn cap_is_enforced_before_metadata_changes() {
        let over = TimeIntSec64::new(CAP.get() + 1);
        for limit in
            [TimeIntSec64::MIN, TimeIntSec64::MAX, TimeIntSec64::NONE, TimeIntSec64::new(-1), CAP]
        {
            let mut extras = Extras::default();
            let mut flags = Flags::default();
            assert!(extras.soft_delete(7, &mut flags, TimeSec::new(10), over, limit).is_err());
            assert!(
                extras
                    .soft_delete_and_delete(7, &mut flags, TimeSec::new(10), over, limit)
                    .is_err()
            );
            assert_eq!(flags, Flags::default());
            assert!(extras.get(&7).is_none());
        }
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        extras.soft_delete(7, &mut flags, TimeSec::new(10), CAP, CAP).unwrap();
        assert_eq!(extras.get(&7).unwrap().retain_until, TimeSec::new(1010));
        assert!(extras.soft_delete(7, &mut flags, TimeSec::new(10), over, CAP).is_err());
        assert_eq!(extras.get(&7).unwrap().retain_until, TimeSec::new(1010));
    }

    #[test]
    fn every_wire_target_checks_the_cap_and_accepts_min() {
        use crate::ids::{RangeId, StreamId};
        let targets = [
            Target::Object("/x".into()),
            Target::Stream(StreamName {
                range: RangeId::FIRST,
                stream: StreamId::FIRST,
                created: TimeSec::new(1),
            }),
            Target::Instance {
                en: EnId::FIRST,
                disk: DiskId::FIRST,
                sn: SnId::FIRST,
                extent: ExtentId::FIRST,
                created: TimeSec::new(1),
            },
        ];
        for target in targets {
            for retention in
                [TimeIntSec64::MIN, TimeIntSec64::ZERO, CAP, TimeIntSec64::new(CAP.get() + 1)]
            {
                let delete =
                    Delete { target: target.clone(), retention: Some(retention), hard: false };
                let request = delete.request().unwrap();
                assert_eq!(Delete::of(&request).unwrap(), delete);
                let expected =
                    if retention > CAP { Status::BadRequest } else { Status::Unimplemented };
                assert_eq!(Delete::stub(&request, "tier", CAP).status, expected);
            }
        }
    }
    #[test]
    fn combined_deletion_is_atomic_and_negative_retention_is_valid() {
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        for retention in [TimeIntSec64::ZERO, TimeIntSec64::new(3), TimeIntSec64::NONE] {
            assert!(
                extras
                    .soft_delete_and_delete(7, &mut flags, TimeSec::new(10), retention, CAP)
                    .is_err()
            );
            assert_eq!(flags, Flags::default());
            assert!(extras.get(&7).is_none());
        }
        assert_eq!(
            extras
                .soft_delete_and_delete(7, &mut flags, TimeSec::new(10), TimeIntSec64::new(-1), CAP)
                .unwrap(),
            (true, true)
        );
        assert_eq!(extras.get(&7).unwrap().retain_until, TimeSec::new(9));
        assert!(extras.reclaimable(&7, flags, TimeSec::new(10)));
        assert_eq!(
            extras
                .soft_delete_and_delete(7, &mut flags, TimeSec::new(11), TimeIntSec64::new(-1), CAP)
                .unwrap(),
            (false, false)
        );
        assert_eq!(extras.get(&7).unwrap().retain_until, TimeSec::new(9));
    }

    #[test]
    fn combined_request_cannot_shorten_existing_retention() {
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        extras.soft_delete(7, &mut flags, TimeSec::new(10), TimeIntSec64::new(100), CAP).unwrap();
        assert!(
            extras
                .soft_delete_and_delete(7, &mut flags, TimeSec::new(20), TimeIntSec64::new(-1), CAP)
                .is_err()
        );
        assert!(!flags.deleted());
        assert_eq!(extras.get(&7).unwrap().retain_until, TimeSec::new(110));
        assert!(
            extras
                .soft_delete_and_delete(
                    7,
                    &mut flags,
                    TimeSec::new(111),
                    TimeIntSec64::new(-1),
                    CAP
                )
                .is_ok()
        );
    }

    #[test]
    fn negative_retention_and_underflow() {
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        assert!(
            extras.soft_delete(7, &mut flags, TimeSec::EPOCH, TimeIntSec64::new(-1), CAP).is_err()
        );
        assert_eq!(flags, Flags::default());
        assert!(
            extras
                .soft_delete(7, &mut flags, TimeSec::new(10), TimeIntSec64::new(-1), CAP)
                .unwrap()
        );
        assert!(!extras.reclaimable(&7, flags, TimeSec::new(10)));
        assert!(extras.delete(&7, &mut flags, TimeSec::new(10)).unwrap());
    }

    #[test]
    fn deletion_messages_round_trip_and_reject_truncation_and_extra_arguments() {
        use crate::ids::{RangeId, StreamId};
        use crate::wire::Arg;
        let targets = [
            Target::Object("/example".into()),
            Target::Stream(StreamName {
                range: RangeId::new(2),
                stream: StreamId::new(3),
                created: TimeSec::new(4),
            }),
            Target::Instance {
                en: EnId::new(5),
                disk: DiskId::new(1),
                sn: SnId::new(6),
                extent: ExtentId::new(7),
                created: TimeSec::new(8),
            },
        ];
        for target in targets {
            for (retention, hard) in [
                (None, true),
                (Some(TimeIntSec64::new(3)), false),
                (Some(TimeIntSec64::new(-1)), true),
            ] {
                let delete = Delete { target: target.clone(), retention, hard };
                let request = delete.request().unwrap();
                assert_eq!(Delete::of(&request).unwrap(), delete);
                assert_eq!(Delete::stub(&request, "test", CAP).status, Status::Unimplemented);
                let mut extra = delete.request().unwrap();
                extra.args.push(Arg::Flag(false));
                assert!(Delete::of(&extra).is_err());
                let Arg::Struct(op, bytes, depth) = &request.args[0] else { panic!() };
                for len in 0..bytes.len() {
                    let mut short = Req::new(*op);
                    short.args.push(Arg::Struct(*op, bytes[..len].to_vec(), *depth));
                    assert!(Delete::of(&short).is_err(), "accepted truncation at {len}");
                }
                let mut trailing = delete.request().unwrap();
                let Arg::Struct(_, bytes, _) = &mut trailing.args[0] else { panic!() };
                bytes.push(0);
                assert!(Delete::of(&trailing).is_err());
            }
        }
    }

    #[test]
    fn malformed_signed_intervals_are_rejected_on_the_wire() {
        for interval in [TimeIntSec64::MAX, TimeIntSec64::NONE] {
            let value = Delete {
                target: Target::Object("/x".into()),
                retention: Some(interval),
                hard: false,
            };
            assert!(value.request().is_err());
            let mut payload = StructWriter::new();
            payload.text("/x");
            payload.number_u64(interval.get() as u64);
            payload.flag(false);
            let request = Req::new(Op::SoftDeleteObject).structure(payload);
            assert_eq!(Delete::stub(&request, "test", CAP).status, Status::BadRequest);
        }
    }

    #[test]
    fn compact_and_sparse() {
        assert_eq!(std::mem::size_of::<Flags>(), 1);
        assert_eq!(std::mem::size_of::<Extra>(), 8);
        assert!(!Flags::default().extra());
        assert!(Flags::default().readable(false));
    }
    #[test]
    fn both_conditions_and_strictly_past_are_required() {
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        assert!(extras.delete(&7, &mut flags, TimeSec::new(1000)).is_err());
        assert_eq!(flags, Flags::default());
        assert!(
            extras.soft_delete(7, &mut flags, TimeSec::new(10), TimeIntSec64::new(3), CAP).unwrap()
        );
        assert!(flags.extra() && flags.soft_deleted());
        assert!(!flags.readable(false));
        assert!(flags.readable(true));
        assert!(extras.delete(&7, &mut flags, TimeSec::new(12)).is_err());
        assert!(extras.delete(&7, &mut flags, TimeSec::new(13)).is_err());
        assert!(!flags.deleted());
        assert!(!extras.reclaim(&7, flags, TimeSec::new(1000)));
        assert!(extras.delete(&7, &mut flags, TimeSec::new(14)).unwrap());
        assert!(!extras.delete(&7, &mut flags, TimeSec::new(14)).unwrap());
        assert!(!flags.readable(true));
        assert!(extras.reclaim(&7, flags, TimeSec::new(14)));
        assert!(extras.get(&7).is_none());
    }
    #[test]
    fn retries_do_not_extend_retention() {
        let mut extras = Extras::default();
        let mut flags = Flags::default();
        assert!(
            extras.soft_delete(7, &mut flags, TimeSec::new(10), TimeIntSec64::ZERO, CAP).unwrap()
        );
        assert!(
            !extras
                .soft_delete(7, &mut flags, TimeSec::new(100), TimeIntSec64::new(99), CAP)
                .unwrap()
        );
        assert_eq!(extras.get(&7).unwrap().retain_until, TimeSec::new(10));
    }
    #[test]
    fn invalid_retention_does_not_change_metadata() {
        for retention in [TimeIntSec64::MAX, TimeIntSec64::NONE, TimeIntSec64::new(2)] {
            let mut extras = Extras::default();
            let mut flags = Flags::default();
            assert!(
                extras
                    .soft_delete(7, &mut flags, TimeSec::new(u64::MAX - 1), retention, CAP)
                    .is_err()
            );
            assert_eq!(flags, Flags::default());
            assert!(extras.get(&7).is_none());
        }
    }
}
