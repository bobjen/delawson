//! Shared consensus, persistence, and application scheduling for ring services.
pub mod application;
pub mod checkpoint;
pub mod codec;
pub mod execution;
pub mod journal;
pub mod lease;
pub mod owner;
pub mod raft;
pub mod runtime;
use common::Op;
use common::Result;
use common::time::TimeSec;
pub use raft::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Action {
    pub op: Op,
    pub fields: Vec<u8>,
    now: TimeSec,
}
impl Action {
    pub fn new(op: Op, fields: Vec<u8>) -> Self {
        Self { op, fields, now: TimeSec::EPOCH }
    }

    /// The primary's timestamp from when this action was submitted to Raft.
    pub const fn now(&self) -> TimeSec {
        self.now
    }
}
pub fn random_bytes() -> Result<[u8; 32]> {
    use std::io::Read;
    let mut bytes = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub mod io;
