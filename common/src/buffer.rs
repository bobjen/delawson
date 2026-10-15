//! Process-shared storage for queued, sealed, and opened message batches.
use super::{MAX_FRAME_BYTES, Req, Resp};
use crate::config::BufferPoolConfig;
use crate::{Context, Error, Result};
use pool::{ChunkPool, SegmentedBuffer};
use std::io::Write;
use std::sync::OnceLock;

// Per reusable connection buffer; index nodes are additional to these leaves.
const RETAINED_LEAVES: u32 = 1;

static POOL: OnceLock<Result<(BufferPoolConfig, ChunkPool)>> = OnceLock::new();
// A cached small-write limit is safe only after the process configuration is fixed.
pub(super) fn configured_pool() -> Option<BufferPoolConfig> {
    POOL.get().and_then(|p| p.as_ref().ok()).map(|p| p.0)
}
pub(super) fn pool_config() -> BufferPoolConfig {
    POOL.get().and_then(|p| p.as_ref().ok()).map_or_else(BufferPoolConfig::default, |p| p.0)
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

/// A batch borrows the process-wide pool, allocating lazily and retaining bounded capacity on reset.
pub struct MessageBuffer {
    pub(super) bytes: SegmentedBuffer<'static>,
}
impl MessageBuffer {
    pub fn new(pool: &'static ChunkPool) -> Self {
        Self::with_retained(pool, RETAINED_LEAVES)
    }
    pub(super) fn with_retained(pool: &'static ChunkPool, leaves: u32) -> Self {
        Self { bytes: SegmentedBuffer::with_retained_leaves(pool, leaves) }
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
        self.bytes.reset();
    }
    pub(super) fn set_retained_leaves(&mut self, leaves: u32) {
        self.bytes.set_retained_leaves(leaves);
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<()> {
        self.bytes.append(bytes).context("message buffer append")
    }
    pub(super) fn receive(&mut self, source: &mut impl std::io::Read, length: usize) -> Result<()> {
        self.clear();
        self.bytes.read_exact_from(source, length)?;
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
        let result = (|| {
            let mut writer = self.bytes.writer();
            writer.write_u32_le(0)?;
            message.encode_into(&mut writer)
        })();
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

impl FrameBuffer for super::MessageBytes {
    fn append_message(&mut self, message: &impl Message) -> Result<()> {
        let start = self.len();
        let result = (|| -> std::io::Result<()> {
            self.write_all(&[0; 4])?;
            message.encode_into(self)?;
            let length = self.len() - start - 4;
            if length > MAX_FRAME_BYTES {
                return Err(std::io::Error::other("frame exceeds maximum size"));
            }
            self.write_at(start, &(length as u32).to_le_bytes())
        })();
        if result.is_err() {
            self.truncate(start);
        }
        result.map_err(Into::into)
    }
}
impl MessageBuffer {
    pub fn append_payload(&mut self, bytes: &super::MessageBytes) -> Result<()> {
        bytes.check()?;
        let start = self.bytes.end();
        let result = bytes.write_to(&mut self.bytes.writer());
        if result.is_err() {
            self.bytes.truncate(start).context("batch rollback")?;
        }
        result.map_err(Into::into)
    }
}
