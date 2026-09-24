//! Tagged binary schema, built with the same readers/writers as service messages.
use common::wire::{Arg, MAX_FRAME_BYTES, MAX_RECURSION_DEPTH, Op, StructReader, StructWriter};
use common::{Error, Result};
use std::collections::{BTreeMap, BTreeSet};
pub trait Wire: Sized {
    fn write(&self, w: &mut StructWriter);
    fn read(r: &mut StructReader<'_>) -> Result<Self>;
}
pub fn encode<T: Wire>(value: &T) -> Result<Vec<u8>> {
    let mut w = StructWriter::new();
    value.write(&mut w);
    let Arg::Struct(_, bytes, depth) = w.finish(Op::Raft) else { unreachable!() };
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
            let mut values = common::wire::checked_vec(n, r.remaining())?;
            for _ in 0..n {
                values.push(T::read(r)?);
            }
            Ok(values)
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
#[macro_export]
macro_rules! wire_struct {
    ($type:ty, $tag:ident, {$($field:ident),* $(,)?}) => {
        impl $crate::codec::Wire for $type {
            fn write(&self,w:&mut common::wire::StructWriter) {
                w.nested(common::wire::Op::$tag, |w| { $( $crate::codec::Wire::write(&self.$field,w); )* });
            }
            fn read(r:&mut common::wire::StructReader<'_>)->common::Result<Self> {
                r.nested(common::wire::Op::$tag,stringify!($type), |r| Ok(Self { $( $field:$crate::codec::Wire::read(r)?, )* }))
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
        };
        let bytes = encode(&entry).unwrap();
        let round_trip: crate::Entry = decode(&bytes).unwrap();
        assert_eq!(round_trip, entry);
        assert_eq!(u16::from_le_bytes(bytes[1..3].try_into().unwrap()), Op::RaftEntry.code());
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
pub fn decode_message<T: Wire>(bytes: &common::wire::MessageBytes) -> Result<T> {
    bytes.check()?;
    let depth = *bytes.chunk(0).first().ok_or_else(|| Error::new("empty Raft encoding"))?;
    if depth == 0 || depth > MAX_RECURSION_DEPTH || bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::new("invalid Raft encoding limits"));
    }
    let mut r = StructReader::from_message(bytes, depth);
    r.number_u8()?;
    let value = T::read(&mut r)?;
    r.done()?;
    Ok(value)
}

/// Network encoding retains pool leaves instead of flattening for the caller.
pub fn encode_message<T: Wire>(value: &T) -> Result<common::wire::MessageBytes> {
    let mut writer = StructWriter::new();
    value.write(&mut writer);
    let Arg::Struct(_, bytes, depth) = writer.finish(Op::Raft) else { unreachable!() };
    if bytes.len() >= MAX_FRAME_BYTES || depth > MAX_RECURSION_DEPTH {
        return Err(Error::new("Raft encoding exceeds limits"));
    }
    let mut out = common::wire::MessageBytes::new();
    out.push(depth);
    bytes.write_to(&mut out)?;
    out.check()?;
    Ok(out)
}

// Preserve Vec<u8>'s existing tagged-list representation for forwarded payloads.
impl Wire for common::wire::MessageBytes {
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
    #[test]
    fn segmented_network_encoding_matches_contiguous_byte_list() {
        let bytes: Vec<u8> = (0..12000).map(|i| i as u8).collect();
        let payload = common::wire::MessageBytes::from(bytes.clone());
        let encoded = encode_message(&payload).unwrap();
        assert!(encoded.is_segmented());
        assert_eq!(encoded.to_vec(), encode(&bytes).unwrap());
        let decoded: common::wire::MessageBytes = decode_message(&encoded).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(decode_message::<Vec<u8>>(&encoded).unwrap(), bytes);
    }
}
