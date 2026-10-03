//! Tagged binary schema, built with the same readers/writers as service messages.
use common::{Error, Result};
use common::{MAX_FRAME_BYTES, MAX_RECURSION_DEPTH, Op, StructReader, StructWriter};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
pub trait Wire: Sized {
    fn write(&self, w: &mut StructWriter);
    fn read(r: &mut StructReader<'_>) -> Result<Self>;
}
pub fn encode<T: Wire>(value: &T) -> Result<Vec<u8>> {
    let mut w = StructWriter::new();
    value.write(&mut w);
    let arg = w.finish(Op::Raft);
    let depth = arg.depth;
    let bytes = arg.into_bytes();
    if bytes.len() >= MAX_FRAME_BYTES || depth > MAX_RECURSION_DEPTH {
        return Err(Error::new("Raft encoding exceeds limits"));
    }
    let mut encoded = vec![depth];
    bytes.write_to(&mut encoded)?;
    Ok(encoded)
}
pub fn decode<T: Wire>(bytes: &[u8]) -> Result<T> {
    let (&depth, bytes) = bytes.split_first().ok_or_else(|| Error::new("empty Raft encoding"))?;
    if depth == 0 || depth > MAX_RECURSION_DEPTH || bytes.len() >= MAX_FRAME_BYTES {
        return Err(Error::new("invalid Raft encoding limits"));
    }
    let mut r = StructReader::new(bytes, depth);
    let value = T::read(&mut r)?;
    r.done()?;
    Ok(value)
}
macro_rules! primitive {
    ($t:ty, $write:ident, $read:ident) => {
        impl Wire for $t {
            fn write(&self, w: &mut StructWriter) {
                w.$write(*self);
            }
            fn read(r: &mut StructReader<'_>) -> Result<Self> {
                r.$read()
            }
        }
    };
}
primitive!(u64, number_u64, number_u64);
primitive!(common::scn::Scn, scn, scn);
primitive!(u16, number_u16, number_u16);
primitive!(u8, number_u8, number_u8);
primitive!(bool, flag, flag);
impl Wire for String {
    fn write(&self, w: &mut StructWriter) {
        w.text(self);
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.text()
    }
}
impl<const N: usize> Wire for [u8; N] {
    fn write(&self, w: &mut StructWriter) {
        w.bytes(self);
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.bytes()?.try_into().map_err(|_| Error::new("wrong byte array length"))
    }
}
impl Wire for common::ring::MemberIdentity {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftIdentity, |w| {
            w.bytes(&common::shamir::unhex::<64>(&self.to_string()).expect("validated identity"))
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftIdentity, "member identity", |r| {
            let bytes = <[u8; 64]>::read(r)?;
            Self::try_from(common::shamir::hex(&bytes))
        })
    }
}
impl<T: Wire> Wire for Vec<T> {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftList, |w| {
            w.number_u64(self.len() as u64);
            for value in self {
                value.write(w);
            }
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftList, "list", |r| {
            let n = r.number_u64()?;
            let mut values = common::checked_vec(n, r.remaining())?;
            for _ in 0..n {
                values.push(T::read(r)?);
            }
            Ok(values)
        })
    }
}
impl<T: Wire> Wire for Arc<T> {
    fn write(&self, w: &mut StructWriter) {
        self.as_ref().write(w);
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        T::read(r).map(Arc::new)
    }
}
impl Wire for crate::raft::EntryLog {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftList, |w| {
            w.number_u64(self.len() as u64);
            for entry in self.iter() {
                entry.write(w);
            }
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftList, "entry log", |r| {
            let n = r.number_u64()?;
            let mut entries = common::checked_vec(n, r.remaining())?;
            for _ in 0..n {
                entries.push(crate::raft::Entry::read(r)?);
            }
            Ok(entries.into())
        })
    }
}
impl<T: Wire> Wire for Option<T> {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftOptional, |w| {
            w.flag(self.is_some());
            if let Some(value) = self {
                value.write(w);
            }
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftOptional, "option", |r| {
            if r.flag()? { Ok(Some(T::read(r)?)) } else { Ok(None) }
        })
    }
}
impl<A: Wire, B: Wire> Wire for (A, B) {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftPair, |w| {
            self.0.write(w);
            self.1.write(w);
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftPair, "pair", |r| Ok((A::read(r)?, B::read(r)?)))
    }
}
impl<T: Wire + Ord> Wire for BTreeSet<T> {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftSet, |w| {
            w.number_u64(self.len() as u64);
            for v in self {
                v.write(w);
            }
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftSet, "set", |r| {
            let n = r.number_u64()?;
            if n > r.remaining() as u64 {
                return Err(Error::new("set exceeds input"));
            }
            let mut set = Self::new();
            for _ in 0..n {
                if !set.insert(T::read(r)?) {
                    return Err(Error::new("duplicate set entry"));
                }
            }
            Ok(set)
        })
    }
}
impl<K: Wire + Ord, V: Wire> Wire for BTreeMap<K, V> {
    fn write(&self, w: &mut StructWriter) {
        w.nested(Op::RaftMap, |w| {
            w.number_u64(self.len() as u64);
            for (k, v) in self {
                w.nested(Op::RaftPair, |w| {
                    k.write(w);
                    v.write(w);
                });
            }
        });
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested(Op::RaftMap, "map", |r| {
            let n = r.number_u64()?;
            if n > r.remaining() as u64 {
                return Err(Error::new("map exceeds input"));
            }
            let mut map = Self::new();
            for _ in 0..n {
                let (k, v) = <(K, V)>::read(r)?;
                if map.insert(k, v).is_some() {
                    return Err(Error::new("duplicate map key"));
                }
            }
            Ok(map)
        })
    }
}
impl Wire for crate::Action {
    fn write(&self, w: &mut StructWriter) {
        w.nested(self.op, |w| w.bytes(&self.fields));
    }
    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        r.nested_any(|op, r| Ok(Self { op, fields: r.bytes()? }))
    }
}

impl Wire for crate::Entry {
    fn write(&self, w: &mut StructWriter) {
        if let Some(bytes) = self.entry_buf.as_ref().filter(|bytes| {
            bytes.lazy_actions
                && matches!(&self.command, crate::Command::Client(actions) if actions.is_empty())
        }) {
            w.raw_message_depth(&bytes.bytes, bytes.depth);
            return;
        }
        w.nested(Op::RaftEntry, |w| {
            self.index.write(w);
            self.term.write(w);
            self.scn.write(w);
            self.command.write(w);
        });
    }

    fn read(r: &mut StructReader<'_>) -> Result<Self> {
        let start = r.position();
        let (mut entry, depth) = r.capture_depth(|r| {
            r.nested(Op::RaftEntry, "entry", |r| {
                Ok(crate::Entry {
                    index: Wire::read(r)?,
                    term: Wire::read(r)?,
                    scn: Wire::read(r)?,
                    command: read_entry_command(r)?,
                    entry_buf: None,
                })
            })
        })?;
        let bytes = r.raw_range(start, r.position() - start)?;
        let lazy_actions = matches!(&entry.command, crate::Command::Client(_));
        entry.entry_buf = Some(Arc::new(crate::EntryBuf::new(bytes, depth, lazy_actions)));
        Ok(entry)
    }
}

fn read_entry_command(r: &mut StructReader<'_>) -> Result<crate::Command> {
    r.nested_any(|op, r| match op {
        Op::RaftCommandNoop => Ok(crate::Command::Noop),
        Op::RaftCommandClient => r.nested(Op::RaftList, "list", |r| {
            let count = r.number_u64()?;
            if count > (r.remaining() / 6) as u64 {
                return Err(Error::new("client action count exceeds entry"));
            }
            for _ in 0..count {
                r.nested_any(|_, r| r.skip_bytes())?;
            }
            Ok(crate::Command::Client(Vec::new()))
        }),
        Op::RaftCommandMembers => Ok(crate::Command::Members(Wire::read(r)?)),
        Op::RaftCommandCheckpointStart => Ok(crate::Command::CheckpointStart(Wire::read(r)?)),
        _ => Err(Error::new("unexpected Command tag")),
    })
}

pub(crate) fn entry_actions(entry: &crate::EntryBuf) -> Result<Vec<crate::Action>> {
    let mut reader = StructReader::from_message(&entry.bytes, entry.depth);
    let command = reader.nested(Op::RaftEntry, "entry", |r| {
        let _: u64 = Wire::read(r)?;
        let _: u64 = Wire::read(r)?;
        let _: common::scn::Scn = Wire::read(r)?;
        <crate::Command as Wire>::read(r)
    })?;
    reader.done()?;
    match command {
        crate::Command::Client(actions) => Ok(actions),
        _ => Err(Error::new("Raft entry is not a client command")),
    }
}

pub(crate) fn entry_buffer(entry: &crate::Entry) -> Result<Arc<crate::EntryBuf>> {
    if let Some(bytes) = entry.entry_buf.as_ref().filter(|bytes| {
        bytes.lazy_actions
            && matches!(&entry.command, crate::Command::Client(actions) if actions.is_empty())
    }) {
        return Ok(bytes.clone());
    }
    let mut writer = StructWriter::new();
    writer.nested(Op::RaftEntry, |w| {
        entry.index.write(w);
        entry.term.write(w);
        entry.scn.write(w);
        entry.command.write(w);
    });
    let arg = writer.finish(Op::Raft);
    let depth = arg.depth;
    let bytes = arg.into_bytes().into();
    let lazy_actions = matches!(&entry.command, crate::Command::Client(_));
    Ok(Arc::new(crate::EntryBuf::new(bytes, depth, lazy_actions)))
}
#[macro_export]
macro_rules! wire_struct {
    ($type:ty, $tag:ident, {$($field:ident),* $(,)?}) => {
        impl $crate::codec::Wire for $type {
            fn write(&self,w:&mut common::StructWriter) {
                w.nested(common::Op::$tag, |w| { $( $crate::codec::Wire::write(&self.$field,w); )* });
            }
            fn read(r:&mut common::StructReader<'_>)->common::Result<Self> {
                r.nested(common::Op::$tag,stringify!($type), |r| Ok(Self { $( $field:$crate::codec::Wire::read(r)?, )* }))
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tagged_entries_round_trip_and_reject_every_truncation() {
        let entry = crate::Entry {
            scn: common::scn::Scn::new(42),
            index: 42,
            term: 7,
            command: crate::Command::Client(vec![
                crate::Action::new(Op::BytesResp, vec![1, 2, 3]),
                crate::Action::new(Op::NumberResp, vec![4, 5]),
            ]),
            entry_buf: None,
        };
        let bytes = encode(&entry).unwrap();
        let round_trip: crate::Entry = decode(&bytes).unwrap();
        assert_eq!(round_trip, entry);
        assert!(
            matches!(&round_trip.command, crate::Command::Client(actions) if actions.is_empty())
        );
        assert_eq!(
            round_trip.client_actions().unwrap().as_ref(),
            match &entry.command {
                crate::Command::Client(actions) => actions.as_slice(),
                _ => unreachable!(),
            }
        );
        assert_eq!(encode(&round_trip).unwrap(), bytes);
        assert_eq!(u16::from_le_bytes(bytes[1..3].try_into().unwrap()), Op::RaftEntry.code());

        let mut changed = round_trip.clone();
        changed.command = crate::Command::Client(vec![crate::Action::new(Op::BytesResp, vec![9])]);
        let changed: crate::Entry = decode(&encode(&changed).unwrap()).unwrap();
        assert_eq!(changed.client_actions().unwrap()[0].fields, [9]);

        for n in 0..bytes.len() {
            let result = decode::<crate::Entry>(&bytes[..n]);
            assert!(result.is_err(), "length {n}");
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode::<crate::Entry>(&trailing).is_err());
        let mut unknown = bytes;
        unknown[1..3].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(decode::<crate::Entry>(&unknown).is_err());
    }
    #[test]
    fn collection_count_and_recursion_are_bounded() {
        let bytes = encode(&vec![1u64]).unwrap();
        let mut bad = bytes.clone();
        bad[3..11].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode::<Vec<u64>>(&bad).is_err());
        let mut shallow = encode(&Some(vec![Some(1u64)])).unwrap();
        shallow[0] = 1;
        assert!(decode::<Option<Vec<Option<u64>>>>(&shallow).is_err());
    }
}

/// Decode a message payload directly across its pool leaves.
pub fn decode_message<T: Wire>(bytes: &common::MessagePayload) -> Result<T> {
    bytes.check()?;
    let depth = *bytes.chunk(0).first().ok_or_else(|| Error::new("empty Raft encoding"))?;
    if depth == 0 || depth > MAX_RECURSION_DEPTH || bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::new("invalid Raft encoding limits"));
    }
    let mut r = StructReader::from_payload(bytes, depth);
    r.number_u8()?;
    let value = T::read(&mut r)?;
    r.done()?;
    Ok(value)
}

/// Network encoding retains pool leaves instead of flattening for the caller.
pub fn encode_message<T: Wire>(value: &T) -> Result<common::MessageBytes> {
    let mut writer = StructWriter::new();
    value.write(&mut writer);
    let arg = writer.finish(Op::Raft);
    let depth = arg.depth;
    let bytes = arg.into_bytes();
    if bytes.len() >= MAX_FRAME_BYTES || depth > MAX_RECURSION_DEPTH {
        return Err(Error::new("Raft encoding exceeds limits"));
    }
    let mut out = common::MessageBytes::new();
    out.push(depth);
    bytes.write_to(&mut out)?;
    out.check()?;
    Ok(out)
}

// Preserve Vec<u8>'s existing tagged-list representation for forwarded payloads.
impl Wire for common::MessageBytes {
    fn write(&self, writer: &mut StructWriter) {
        writer.nested(Op::RaftList, |writer| {
            writer.number_u64(self.len() as u64);
            writer.raw_message_bytes(self);
        });
    }
    fn read(reader: &mut StructReader<'_>) -> Result<Self> {
        reader.nested(Op::RaftList, "list", |reader| {
            let n = reader.number_u64()?;
            if n > reader.remaining() as u64 {
                return Err(Error::new("byte list exceeds remaining payload"));
            }
            reader.raw_message_bytes(n as usize)
        })
    }
}

#[cfg(test)]
mod message_storage_tests {
    use super::*;
    use crate::raft::{Command, Entry, Rpc};
    #[test]
    fn segmented_network_encoding_matches_contiguous_byte_list() {
        let size = common::config::BufferPoolConfig::default().small_message_bytes * 3;
        let bytes: Vec<u8> = (0..size).map(|i| i as u8).collect();
        let payload = common::MessageBytes::from(bytes.clone());
        let encoded: common::MessagePayload = encode_message(&payload).unwrap().into();
        assert!(encoded.is_segmented());
        assert_eq!(encoded.to_vec(), encode(&bytes).unwrap());
        let decoded: common::MessageBytes = decode_message(&encoded).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(decode_message::<Vec<u8>>(&encoded).unwrap(), bytes);
    }

    #[test]
    fn append_decode_keeps_one_lazy_serialization_per_entry() {
        let rpc = Rpc::Append {
            term: 1,
            previous: 0,
            previous_term: 0,
            previous_scn: common::scn::Scn::new(0),
            entries: vec![Entry {
                scn: common::scn::Scn::new(1),
                index: 1,
                term: 1,
                command: Command::Client(vec![crate::Action::new(
                    Op::BytesResp,
                    vec![29; 128 * 1024],
                )]),
                entry_buf: None,
            }],
            commit: 0,
        };
        let encoded = encode_message(&rpc).unwrap();
        assert!(encoded.is_segmented());
        let wire = encoded.to_vec();
        let payload: common::MessagePayload = encoded.into();
        let decoded: Rpc = decode_message(&payload).unwrap();
        let Rpc::Append { entries, .. } = decoded else { panic!("not an append") };
        assert_eq!(entries.len(), 1);
        assert!(matches!(&entries[0].command, Command::Client(actions) if actions.is_empty()));
        assert!(entries[0].entry_buf.as_ref().unwrap().bytes.is_segmented());
        assert_eq!(
            encode_message(&Rpc::Append {
                term: 1,
                previous: 0,
                previous_term: 0,
                previous_scn: common::scn::Scn::new(0),
                entries: entries.clone(),
                commit: 0,
            })
            .unwrap()
            .to_vec(),
            wire
        );
        assert_eq!(entries[0].client_actions().unwrap()[0].fields.len(), 128 * 1024);
    }
}
