//! Small contiguous message payloads that promote once to process-shared pool storage.
use super::buffer::{shared_pool, with_random};
use crate::config::BufferPoolConfig;
use pool::SegmentedBuffer;
use std::io::{self, Write};

/// Owned serialized message bytes. Crossing the configured threshold copies the
/// small prefix once; subsequent growth allocates leaves without moving it.
/// Serialization errors are retained because StructWriter's field API is infallible.
pub struct MessageBytes {
    storage: Storage,
    error: Option<String>,
}
enum Storage {
    Small(Vec<u8>),
    Large(SegmentedBuffer<'static>),
}
impl Default for MessageBytes {
    fn default() -> Self {
        Self { storage: Storage::Small(Vec::new()), error: None }
    }
}
impl MessageBytes {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Small(v) => v.len(),
            Storage::Large(v) => v.len() as usize,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_segmented(&self) -> bool {
        matches!(self.storage, Storage::Large(_))
    }
    pub fn clear(&mut self) {
        match &mut self.storage {
            Storage::Small(v) => v.clear(),
            Storage::Large(_) => self.storage = Storage::Small(Vec::new()),
        }
        self.error = None;
    }
    pub fn check(&self) -> io::Result<()> {
        match &self.error {
            Some(e) => Err(io::Error::other(e.clone())),
            None => Ok(()),
        }
    }
    pub fn chunk(&self, offset: usize) -> &[u8] {
        match &self.storage {
            Storage::Small(v) => &v[offset..],
            Storage::Large(v) => v.chunk(offset as u64).unwrap_or(&[]),
        }
    }
    pub fn write_to(&self, out: &mut impl Write) -> io::Result<()> {
        self.check()?;
        let mut at = 0;
        while at < self.len() {
            let bytes = self.chunk(at);
            out.write_all(bytes)?;
            at += bytes.len();
        }
        Ok(())
    }
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        self.write_to(&mut out).expect("valid message payload");
        out
    }
    pub fn write_at(&mut self, offset: usize, bytes: &[u8]) -> io::Result<()> {
        self.check()?;
        if offset.checked_add(bytes.len()).is_none_or(|end| end > self.len()) {
            return Err(io::Error::other("message write outside payload"));
        }
        match &mut self.storage {
            Storage::Small(v) => v[offset..offset + bytes.len()].copy_from_slice(bytes),
            Storage::Large(v) => v.write_at(offset as u64, bytes).map_err(io::Error::other)?,
        }
        Ok(())
    }
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        if let Err(e) = self.write_all(bytes) {
            self.error = Some(e.to_string());
        }
    }
    pub(super) fn fail(&mut self, error: impl std::fmt::Display) {
        self.error = Some(error.to_string());
    }
    pub fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }
    pub fn truncate(&mut self, len: usize) {
        match &mut self.storage {
            Storage::Small(v) => v.truncate(len),
            Storage::Large(v) => {
                v.truncate(len.min(v.len() as usize) as u64).expect("truncate payload");
            }
        }
    }
    pub(super) fn append_with_config(
        &mut self,
        bytes: &[u8],
        config: BufferPoolConfig,
    ) -> io::Result<()> {
        self.check()?;
        let end = self
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("message length overflow"))?;
        if let Storage::Small(v) = &mut self.storage {
            if end <= config.small_message_bytes {
                v.extend_from_slice(bytes);
                return Ok(());
            }
            let pool = shared_pool(config).map_err(io::Error::other)?;
            let mut large = SegmentedBuffer::new(pool);
            with_random(|rng| large.append(v, || rng.next_u64())).map_err(io::Error::other)?;
            // Do not discard the prefix until both appends succeed.
            with_random(|rng| large.append(bytes, || rng.next_u64())).map_err(io::Error::other)?;
            self.storage = Storage::Large(large);
            return Ok(());
        }
        if let Storage::Large(v) = &mut self.storage {
            let end = v.end();
            if let Err(e) = with_random(|rng| v.append(bytes, || rng.next_u64())) {
                v.truncate(end).map_err(io::Error::other)?;
                return Err(io::Error::other(e));
            }
        }
        Ok(())
    }
}
impl Write for MessageBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let config = super::buffer::pool_config();
        self.append_with_config(bytes, config)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.check()
    }
}
impl From<Vec<u8>> for MessageBytes {
    fn from(v: Vec<u8>) -> Self {
        if v.len() <= super::buffer::pool_config().small_message_bytes {
            Self { storage: Storage::Small(v), error: None }
        } else {
            let mut out = Self::new();
            out.extend_from_slice(&v);
            out
        }
    }
}
impl Clone for MessageBytes {
    fn clone(&self) -> Self {
        let mut out = Self::new();
        let mut at = 0;
        while at < self.len() {
            let chunk = self.chunk(at);
            out.extend_from_slice(chunk);
            at += chunk.len();
        }
        if self.error.is_some() {
            out.error = self.error.clone();
        }
        out
    }
}
impl std::fmt::Debug for MessageBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageBytes")
            .field("len", &self.len())
            .field("segmented", &self.is_segmented())
            .field("error", &self.error)
            .finish()
    }
}
impl PartialEq for MessageBytes {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() || self.error != other.error {
            return false;
        }
        let mut at = 0;
        while at < self.len() {
            let a = self.chunk(at);
            let b = other.chunk(at);
            let n = a.len().min(b.len());
            if a[..n] != b[..n] {
                return false;
            }
            at += n;
        }
        true
    }
}
impl Eq for MessageBytes {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Arg, Op, Req, StructWriter, structure};

    #[test]
    fn promotes_only_above_threshold_and_preserves_prefix() {
        let threshold = super::super::buffer::pool_config().small_message_bytes;
        let mut bytes = MessageBytes::new();
        let prefix = vec![0x56; threshold];
        bytes.write_all(&prefix).unwrap();
        assert!(!bytes.is_segmented());
        bytes.write_all(&[0x78]).unwrap();
        assert!(bytes.is_segmented());
        let first = bytes.chunk(0).as_ptr();
        bytes.write_all(&vec![0x9a; 10000]).unwrap();
        assert_eq!(bytes.chunk(0).as_ptr(), first, "growth must not move existing leaves");
        let copy = bytes.clone();
        assert_eq!(copy, bytes);
        let mut expected = prefix;
        expected.push(0x78);
        expected.extend_from_slice(&vec![0x9a; 10000]);
        assert_eq!(bytes.to_vec(), expected);
        bytes.clear();
        assert!(!bytes.is_segmented());
        bytes.write_all(b"ping").unwrap();
        assert_eq!(bytes.to_vec(), b"ping");
    }

    #[test]
    fn segmented_payload_round_trip_with_straddling_tag_and_length() {
        let mut writer = StructWriter::new();
        for _ in 0..pool::segmented::CHUNK_BYTES - 1 {
            writer.number_u8(0xa5);
        }
        writer.nested(Op::ReadRecord, |writer| {
            writer.number_u16(42);
            // The next u32 length starts at one byte before the second block boundary.
            for _ in 0..pool::segmented::CHUNK_BYTES - 4 {
                writer.number_u8(0x5a);
            }
            writer.bytes(&vec![7; 9000]);
        });
        let request = Req::new(Op::ReadRecord).structure(writer);
        let Arg::Struct(_, bytes, _) = &request.args[0] else { panic!() };
        assert!(bytes.is_segmented());
        let mut framed = MessageBytes::new();
        request.encode_to(&mut framed).unwrap();
        // Decode via a BufRead whose chunks stop at the real leaf boundaries.
        let mut decoded_input = PayloadInput { bytes: &framed, at: 0 };
        let decoded = Req::decode_from(&mut decoded_input, framed.len()).unwrap();
        assert_eq!(decoded, request);
        let (mut reader, _) = structure(&decoded.args, Op::ReadRecord, "test").unwrap();
        for _ in 0..pool::segmented::CHUNK_BYTES - 1 {
            assert_eq!(reader.number_u8().unwrap(), 0xa5);
        }
        reader
            .nested(Op::ReadRecord, "nested", |reader| {
                assert_eq!(reader.number_u16()?, 42);
                for _ in 0..pool::segmented::CHUNK_BYTES - 4 {
                    assert_eq!(reader.number_u8()?, 0x5a);
                }
                assert_eq!(reader.bytes()?, vec![7; 9000]);
                Ok(())
            })
            .unwrap();
        reader.done().unwrap();
    }

    struct PayloadInput<'a> {
        bytes: &'a MessageBytes,
        at: usize,
    }
    impl std::io::Read for PayloadInput<'_> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let bytes = self.bytes.chunk(self.at);
            let n = out.len().min(bytes.len());
            out[..n].copy_from_slice(&bytes[..n]);
            self.at += n;
            Ok(n)
        }
    }
    impl std::io::BufRead for PayloadInput<'_> {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Ok(self.bytes.chunk(self.at))
        }
        fn consume(&mut self, n: usize) {
            self.at += n;
        }
    }

    #[test]
    fn exhausted_pool_preserves_prefix_and_failed_serialization_is_reported() {
        let pool = Box::leak(Box::new(pool::ChunkPool::new(1, 0).unwrap()));
        let mut bytes =
            MessageBytes { storage: Storage::Large(SegmentedBuffer::new(pool)), error: None };
        bytes.write_all(b"prefix").unwrap();
        assert!(bytes.write_all(&vec![0; pool::segmented::CHUNK_BYTES * 2]).is_err());
        assert_eq!(bytes.to_vec(), b"prefix");
        bytes.extend_from_slice(&vec![0; pool::segmented::CHUNK_BYTES * 2]);
        bytes.push(1);
        let mut output = Vec::new();
        assert!(bytes.write_to(&mut output).is_err());
        assert!(output.is_empty());
    }

    #[test]
    fn configurable_small_threshold_is_deserialized() {
        let config: BufferPoolConfig = toml::from_str("small_message_bytes = 1024").unwrap();
        assert_eq!(config.small_message_bytes, 1024);
        let mut bytes = MessageBytes::new();
        bytes.append_with_config(&vec![1; 1024], config).unwrap();
        assert!(!bytes.is_segmented());
    }
}
