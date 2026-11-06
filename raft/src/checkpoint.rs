//! Fixed checkpoint footer and its variable-sized footer.
use crate::raft::{MemberId, MemberNames, Membership, Retirement};
use common::scn::Scn;
use common::{Error, Op, Result};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};

mod chunks;
pub use chunks::{ChunkPos, read_chunks, write_chunks};

/// Raft's portion of a service checkpoint's variable footer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftStatus {
    pub ring: String,
    pub index: u64,
    pub term: u64,
    pub scn: Scn,
    pub primary: Option<MemberId>,
    pub membership: Membership,
    pub checkpoint_start: Option<(u64, MemberId)>,
    pub hard_term: u64,
    pub vote: Option<MemberId>,
    pub vote_scn: Option<Scn>,
    pub founders: Option<Membership>,
    pub contacts: MemberNames,
    pub retirements: BTreeMap<MemberId, Retirement>,
    pub max_scn: Option<Scn>,
}
crate::wire_struct!(RaftStatus, RaftCkStatus, {
    ring,index,term,scn,primary,membership,checkpoint_start,hard_term,vote,vote_scn,founders,contacts,retirements,max_scn
});

impl RaftStatus {
    pub fn capture(state: &crate::State, snapshot: &crate::Snapshot) -> Result<Self> {
        let status = Self {
            ring: snapshot.ring.clone(),
            index: snapshot.index,
            term: snapshot.term,
            scn: snapshot.scn,
            primary: snapshot.primary,
            membership: snapshot.membership.clone(),
            checkpoint_start: snapshot.checkpoint_start,
            hard_term: state.term,
            vote: state.vote,
            vote_scn: state.vote_scn,
            founders: state.founders.clone(),
            contacts: state.contacts.clone(),
            retirements: state.retirements.clone(),
            max_scn: state.max_scn,
        };
        status.validate()?;
        Ok(status)
    }

    pub fn validate(&self) -> Result<()> {
        let snapshot = self.snapshot();
        snapshot.validate()?;
        if let Some(founders) = &self.founders {
            founders.validate()?;
        } else if self.index != 0 {
            return Err(Error::new("checkpoint missing founders"));
        }
        for (id, retirement) in &self.retirements {
            retirement.validate()?;
            if *id != retirement.previous {
                return Err(Error::new("invalid checkpoint retirement"));
            }
        }
        if self.index == 0 && self.primary.is_some() {
            return Err(Error::new("primary at empty checkpoint"));
        }
        if self.primary.is_some_and(|id| !self.membership.identities.contains_key(&id)) {
            return Err(Error::new("unknown checkpoint primary"));
        }
        if self.index != 0 && self.max_scn.is_none_or(|max| max.compare(self.scn).is_lt()) {
            return Err(Error::new("checkpoint SCN exceeds high-water"));
        }
        if self.hard_term < self.term
            || self.vote.is_some() != self.vote_scn.is_some()
            || self
                .vote_scn
                .is_some_and(|scn| self.max_scn.is_none_or(|max| scn.compare(max).is_gt()))
        {
            return Err(Error::new("invalid checkpoint hard state"));
        }
        Ok(())
    }

    pub fn snapshot(&self) -> crate::Snapshot {
        crate::Snapshot {
            scn: self.scn,
            ring: self.ring.clone(),
            index: self.index,
            term: self.term,
            primary: self.primary,
            membership: self.membership.clone(),
            application: vec![],
            checkpoint_start: self.checkpoint_start,
        }
    }
}

const TAIL: u64 = 64 * 1024;
const END_SIZE: u64 = 20;

/// The variable footer's position, tag, and serialized bytes.
pub struct Footer {
    pub start: u64,
    pub op: Op,
    pub bytes: Vec<u8>,
}

/// Append a variable footer followed by the fixed footer.
pub fn write_footer(file: &mut (impl Write + Seek), op: Op, bytes: &[u8]) -> Result<()> {
    let start = file.stream_position()?;
    let size = u64::try_from(bytes.len()).map_err(|_| Error::new("checkpoint footer too large"))?;
    let length = start
        .checked_add(size)
        .and_then(|n| n.checked_add(END_SIZE))
        .ok_or_else(|| Error::new("checkpoint length overflow"))?;
    file.write_all(bytes)?;
    file.write_all(&Op::CheckpointEnd.code().to_le_bytes())?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&size.to_le_bytes())?;
    file.write_all(&op.code().to_le_bytes())?;
    Ok(())
}

/// Read the tail once, then fetch the variable footer if it extends beyond it.
pub fn read_footer(file: &mut (impl Read + Seek)) -> Result<Footer> {
    let length = file.seek(SeekFrom::End(0))?;
    if length < END_SIZE {
        return Err(Error::new("checkpoint missing fixed footer"));
    }
    let tail_len = length.min(TAIL) as usize;
    let mut tail = vec![0; tail_len];
    file.seek(SeekFrom::Start(length - tail_len as u64))?;
    file.read_exact(&mut tail)?;
    let end = &tail[tail_len - END_SIZE as usize..];
    let tag = u16::from_le_bytes(end[0..2].try_into().unwrap());
    let recorded = u64::from_le_bytes(end[2..10].try_into().unwrap());
    let size = u64::from_le_bytes(end[10..18].try_into().unwrap());
    let op = Op::from_code(u16::from_le_bytes(end[18..20].try_into().unwrap()))?;
    if tag != Op::CheckpointEnd.code() || recorded != length {
        return Err(Error::new("invalid checkpoint fixed footer"));
    }
    let start = length
        .checked_sub(END_SIZE)
        .and_then(|n| n.checked_sub(size))
        .ok_or_else(|| Error::new("invalid checkpoint footer length"))?;
    let count = usize::try_from(size).map_err(|_| Error::new("checkpoint footer too large"))?;
    let bytes = if size <= tail_len as u64 - END_SIZE {
        let at = tail_len - END_SIZE as usize - count;
        tail[at..at + count].to_vec()
    } else {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(count)
            .map_err(|_| Error::new("checkpoint footer allocation failed"))?;
        bytes.resize(count, 0);
        let missing = count - (tail_len - END_SIZE as usize);
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut bytes[..missing])?;
        bytes[missing..].copy_from_slice(&tail[..tail_len - END_SIZE as usize]);
        bytes
    };
    Ok(Footer { start, op, bytes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn footer_round_trip_and_reject_bad_lengths() {
        for size in [0, 17, 64 * 1024, 128 * 1024] {
            let mut file = Cursor::new(vec![7; 13]);
            file.set_position(13);
            let data = vec![23; size];
            write_footer(&mut file, Op::RaftCheckpoint, &data).unwrap();
            let footer = read_footer(&mut file).unwrap();
            assert_eq!(footer.start, 13);
            assert_eq!(footer.op, Op::RaftCheckpoint);
            assert_eq!(footer.bytes, data);
            let mut damaged = file.into_inner();
            let at = damaged.len() - END_SIZE as usize + 2;
            damaged[at] ^= 1;
            assert!(read_footer(&mut Cursor::new(damaged)).is_err());
        }
    }

    #[test]
    fn rejects_invalid_footer_fields() {
        let mut file = Cursor::new(Vec::new());
        write_footer(&mut file, Op::RaftCheckpoint, b"footer").unwrap();
        let good = file.into_inner();
        let at = good.len() - END_SIZE as usize;
        for field in [0, 2, 10, 18] {
            let mut bytes = good.clone();
            if field == 18 {
                bytes[at + field..at + field + 2].copy_from_slice(&u16::MAX.to_le_bytes());
            } else {
                bytes[at + field] ^= 0xff;
            }
            assert!(read_footer(&mut Cursor::new(bytes)).is_err());
        }
        assert!(read_footer(&mut Cursor::new(good[..19].to_vec())).is_err());
    }

    #[test]
    fn reads_tail_then_only_missing_bytes() {
        struct Counting {
            data: Cursor<Vec<u8>>,
            reads: Vec<usize>,
        }
        impl Read for Counting {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                let n = self.data.read(bytes)?;
                self.reads.push(n);
                Ok(n)
            }
        }
        impl Seek for Counting {
            fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
                self.data.seek(pos)
            }
        }
        for size in [100, 64 * 1024] {
            let mut file = Cursor::new(vec![7; 128 * 1024]);
            file.set_position(128 * 1024);
            write_footer(&mut file, Op::RaftCheckpoint, &vec![23; size]).unwrap();
            let mut reader = Counting { data: file, reads: Vec::new() };
            let footer = read_footer(&mut reader).unwrap();
            assert_eq!(footer.bytes, vec![23; size]);
            if size == 100 {
                assert_eq!(reader.reads, vec![64 * 1024]);
            } else {
                assert_eq!(reader.reads, vec![64 * 1024, END_SIZE as usize]);
            }
        }
    }
}
