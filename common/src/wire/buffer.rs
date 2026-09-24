//! Process-shared storage for queued, sealed, and opened message batches.
use super::{MAX_FRAME_BYTES, Req, Resp};
use crate::config::BufferPoolConfig;
use crate::random::Rng;
use crate::{Context, Error, Result};
use pool::{ChunkPool, SegmentedBuffer};
use std::cell::RefCell;
use std::io::Write;
use std::sync::OnceLock;

static POOL: OnceLock<Result<(BufferPoolConfig, ChunkPool)>> = OnceLock::new();
thread_local! { static RANDOM: RefCell<Rng> = RefCell::new(Rng::new()); }

pub(super) fn with_random<T>(f: impl FnOnce(&mut Rng) -> T) -> T {
    RANDOM.with(|rng| f(&mut rng.borrow_mut()))
}

pub(super) fn shared_pool(config: BufferPoolConfig) -> Result<&'static ChunkPool> {
    let (configured, pool) = POOL
        .get_or_init(|| {
            Ok((
                config,
                ChunkPool::new(config.max_blocks, config.initial_blocks)
                    .context("message block pool")?,
            ))
        })
        .as_ref()
        .map_err(|e| Error::new(e.to_string()))?;
    if *configured != config {
        return Err(Error::new("message block pool configuration changed after initialization"));
    }
    Ok(pool)
}

/// A batch borrows the one process-wide block pool; an empty batch owns no blocks.
pub struct MessageBuffer {
    pub(super) bytes: SegmentedBuffer<'static>,
}
impl MessageBuffer {
    pub fn new(pool: &'static ChunkPool) -> Self {
        Self { bytes: SegmentedBuffer::new(pool) }
    }
    pub fn empty_like(&self) -> Self {
        Self::new(self.bytes.pool())
    }
    pub fn len(&self) -> usize {
        self.bytes.len() as usize
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub fn clear(&mut self) {
        *self = self.empty_like();
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<()> {
        RANDOM
            .with(|rng| self.bytes.append(bytes, || rng.borrow_mut().next_u64()))
            .context("message buffer append")
    }
    pub(super) fn receive(&mut self, source: &mut impl std::io::Read, length: usize) -> Result<()> {
        self.clear();
        RANDOM.with(|rng| {
            self.bytes.read_exact_from(source, length, || rng.borrow_mut().next_u64())
        })?;
        Ok(())
    }
    pub(super) fn write_to(&self, output: &mut impl Write) -> Result<()> {
        self.bytes.write_to(output)?;
        Ok(())
    }
    pub(super) fn copy_to(&self, range: std::ops::Range<u64>, out: &mut Vec<u8>) -> Result<()> {
        out.resize((range.end - range.start) as usize, 0);
        self.bytes.read_at(range.start, out).context("message buffer read")
    }
}

/// Encoding shared by contiguous per-message scratch and segmented batches.
pub trait Message {
    fn encode_into(&self, out: &mut impl Write) -> std::io::Result<()>;
}
impl Message for Req {
    fn encode_into(&self, out: &mut impl Write) -> std::io::Result<()> {
        self.encode_to(out)
    }
}
impl Message for Resp {
    fn encode_into(&self, out: &mut impl Write) -> std::io::Result<()> {
        self.encode_to(out)
    }
}
pub trait FrameBuffer {
    fn append_message(&mut self, message: &impl Message) -> Result<()>;
}
impl FrameBuffer for Vec<u8> {
    fn append_message(&mut self, message: &impl Message) -> Result<()> {
        let start = self.len();
        self.extend([0; 4]);
        if let Err(e) = message.encode_into(self) {
            self.truncate(start);
            return Err(e.into());
        }
        let length = self.len() - start - 4;
        if length > MAX_FRAME_BYTES {
            self.truncate(start);
            return Err(Error::new(format!("frame of {length} bytes is too large")));
        }
        self[start..start + 4].copy_from_slice(&(length as u32).to_le_bytes());
        Ok(())
    }
}
impl FrameBuffer for MessageBuffer {
    fn append_message(&mut self, message: &impl Message) -> Result<()> {
        let start = self.bytes.end();
        let result = RANDOM.with(|rng| {
            let mut writer = self.bytes.writer(|| rng.borrow_mut().next_u64());
            writer.write_u32_le(0)?;
            message.encode_into(&mut writer)
        });
        if let Err(e) = result {
            self.bytes.truncate(start).context("frame rollback")?;
            return Err(e.into());
        }
        let length = self.bytes.end() - start - 4;
        if length > MAX_FRAME_BYTES as u64 {
            self.bytes.truncate(start).context("frame rollback")?;
            return Err(Error::new(format!("frame of {length} bytes is too large")));
        }
        self.bytes.write_at(start, &(length as u32).to_le_bytes()).context("frame length")
    }
}
