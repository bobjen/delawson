//! Small contiguous message payloads that promote once to process-shared pool storage.
use super::buffer::{shared_pool, with_random};
use crate::config::BufferPoolConfig;
use pool::SegmentedBuffer;
use std::io::{self, Write};

/// Owned serialized bytes, with a cached writable span for field-sized writes.
/// The backing storage's initialized length may lag `len` by bytes written into
/// its spare tail. Reads use the logical length; mutations synchronize first.
/// Serialization errors are retained because StructWriter's field API is infallible.
pub struct MessageBytes {
    storage: Storage,
    error: Option<Box<String>>,
    next: *mut u8,
    // -1 forces even a zero-byte write through the slow error/recovery path.
    available: isize,
    len: usize,
}
enum Storage {
    Small(Vec<u8>),
    Large(SegmentedBuffer<'static>),
}

/// Finished payload storage embedded directly in an Arg. Ordinary payloads carry
/// neither a write cursor nor an error slot. Only segmented storage is boxed.
pub struct MessagePayload(FinishedStorage);
enum FinishedStorage {
    Small(Vec<u8>),
    Large(Box<SegmentedBuffer<'static>>),
    // Preserve a failed writer's partial bytes and error until encoding reports it.
    Failed(Box<MessageBytes>),
}
impl Default for MessagePayload {
    fn default() -> Self {
        Self(FinishedStorage::Small(Vec::new()))
    }
}
impl MessagePayload {
    pub(crate) fn from_small(bytes: Vec<u8>) -> Self {
        Self(FinishedStorage::Small(bytes))
    }

    pub(crate) fn contiguous(&self) -> Option<&[u8]> {
        match &self.0 {
            FinishedStorage::Small(v) => Some(v),
            _ => None,
        }
    }
    pub fn len(&self) -> usize {
        match &self.0 {
            FinishedStorage::Small(v) => v.len(),
            FinishedStorage::Large(v) => v.len() as usize,
            FinishedStorage::Failed(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_segmented(&self) -> bool {
        match &self.0 {
            FinishedStorage::Small(_) => false,
            FinishedStorage::Large(_) => true,
            FinishedStorage::Failed(v) => v.is_segmented(),
        }
    }
    pub fn check(&self) -> io::Result<()> {
        match &self.0 {
            FinishedStorage::Failed(v) => v.check(),
            _ => Ok(()),
        }
    }
    pub fn chunk(&self, offset: usize) -> &[u8] {
        match &self.0 {
            FinishedStorage::Small(v) => &v[offset..],
            FinishedStorage::Large(v) => v.chunk(offset as u64).unwrap_or(&[]),
            FinishedStorage::Failed(v) => v.chunk(offset),
        }
    }
    pub fn write_to(&self, out: &mut impl Write) -> io::Result<()> {
        self.check()?;
        if let Some(bytes) = self.contiguous() {
            return out.write_all(bytes);
        }
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
        self.write_to(&mut out).expect("valid finished payload");
        out
    }
}
impl From<MessageBytes> for MessagePayload {
    fn from(mut bytes: MessageBytes) -> Self {
        bytes.synchronize();
        if bytes.error.is_some() {
            return Self(FinishedStorage::Failed(Box::new(bytes)));
        }
        Self(match bytes.storage {
            Storage::Small(v) => FinishedStorage::Small(v),
            Storage::Large(v) => FinishedStorage::Large(Box::new(v)),
        })
    }
}
impl From<MessagePayload> for MessageBytes {
    fn from(bytes: MessagePayload) -> Self {
        match bytes.0 {
            FinishedStorage::Small(v) => Self::from_storage(Storage::Small(v)),
            FinishedStorage::Large(v) => Self::from_storage(Storage::Large(*v)),
            FinishedStorage::Failed(v) => *v,
        }
    }
}
impl From<Vec<u8>> for MessagePayload {
    fn from(bytes: Vec<u8>) -> Self {
        if bytes.len() <= super::buffer::pool_config().small_message_bytes {
            Self::from_small(bytes)
        } else {
            MessageBytes::from(bytes).into()
        }
    }
}
impl Clone for MessagePayload {
    fn clone(&self) -> Self {
        match &self.0 {
            FinishedStorage::Small(v) => Self(FinishedStorage::Small(v.clone())),
            FinishedStorage::Failed(v) => Self(FinishedStorage::Failed(v.clone())),
            FinishedStorage::Large(_) => {
                let mut out = MessageBytes::new();
                if let Err(e) = self.write_to(&mut out) {
                    out.fail(e);
                }
                out.into()
            }
        }
    }
}
impl PartialEq for MessagePayload {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (FinishedStorage::Failed(a), FinishedStorage::Failed(b)) => return a == b,
            (FinishedStorage::Failed(_), _) | (_, FinishedStorage::Failed(_)) => return false,
            _ => {}
        }
        if self.len() != other.len() {
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
impl Eq for MessagePayload {}
impl std::fmt::Debug for MessagePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessagePayload")
            .field("len", &self.len())
            .field("segmented", &self.is_segmented())
            .finish()
    }
}

// SAFETY: next points only into storage exclusively owned by this value. Moving
// the owner does not move its Vec allocation or pool leaves. Mutation requires
// &mut self; shared methods only read initialized bytes. The backing storage
// and stored error are themselves Send + Sync.
unsafe impl Send for MessageBytes {}
unsafe impl Sync for MessageBytes {}

impl Default for MessageBytes {
    fn default() -> Self {
        Self::from_storage(Storage::Small(Vec::new()))
    }
}
impl MessageBytes {
    fn from_storage(storage: Storage) -> Self {
        let len = match &storage {
            Storage::Small(v) => v.len(),
            Storage::Large(v) => v.len() as usize,
        };
        let mut out = Self {
            storage,
            error: None,
            next: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            available: 0,
            len,
        };
        out.refresh_cache();
        out
    }
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.min(super::buffer::pool_config().small_message_bytes);
        Self::from_storage(Storage::Small(Vec::with_capacity(capacity)))
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn is_segmented(&self) -> bool {
        matches!(self.storage, Storage::Large(_))
    }

    // Only slow paths alter backing lengths. Recovering from an unwind may also
    // need to discard a partial slow append beyond the last published length.
    fn synchronize(&mut self) {
        match &mut self.storage {
            Storage::Small(v) => {
                // SAFETY: len never exceeds capacity; all bytes up to len were
                // initialized by Vec operations or the bounded cached copy.
                unsafe {
                    v.set_len(self.len);
                }
            }
            Storage::Large(v) => {
                let end = v.end() as usize;
                if self.len > end {
                    // SAFETY: cached writes initialized this suffix within the
                    // same owned tail leaf; no read borrow survives &mut self.
                    unsafe {
                        v.advance_initialized_tail(self.len - end);
                    }
                } else if self.len < end {
                    v.truncate(self.len as u64).expect("payload rollback");
                }
            }
        }
    }
    fn refresh_cache(&mut self) {
        if self.error.is_some() {
            self.available = -1;
            return;
        }
        let (next, available) = match &mut self.storage {
            Storage::Small(v) => {
                // Until pool initialization, a later configuration may choose a
                // smaller threshold. Such startup writes stay on the slow path.
                let limit =
                    super::buffer::configured_pool().map_or(self.len, |c| c.small_message_bytes);
                let available =
                    v.capacity().saturating_sub(self.len).min(limit.saturating_sub(self.len));
                // SAFETY: len is within the owned allocation (or its one-past end).
                (unsafe { v.as_mut_ptr().add(self.len) }, available)
            }
            Storage::Large(v) => v.spare_tail_ptr(),
        };
        self.next = next;
        self.available = available.min(usize::MAX - self.len).min(isize::MAX as usize) as isize;
    }
    pub fn clear(&mut self) {
        self.synchronize();
        self.available = -1;
        match &mut self.storage {
            Storage::Small(v) => v.clear(),
            Storage::Large(_) => self.storage = Storage::Small(Vec::new()),
        }
        self.len = 0;
        self.error = None;
        self.refresh_cache();
    }
    #[inline]
    pub fn check(&self) -> io::Result<()> {
        match &self.error {
            Some(e) => Err(Self::stored_error(e)),
            None => Ok(()),
        }
    }
    #[cold]
    fn stored_error(error: &str) -> io::Error {
        io::Error::other(error.to_owned())
    }
    #[inline]
    pub fn chunk(&self, offset: usize) -> &[u8] {
        match &self.storage {
            Storage::Small(v) => {
                // SAFETY: cached writes initialized through logical len, even
                // when Vec's length has not yet been synchronized. &self rules
                // out concurrent writes, and Vec owns the entire allocation.
                &(unsafe { std::slice::from_raw_parts(v.as_ptr(), self.len) })[offset..]
            }
            Storage::Large(v) => {
                if offset >= self.len {
                    return &[];
                }
                let committed = v.end() as usize;
                let tail_start = committed / pool::CHUNK_BYTES * pool::CHUNK_BYTES;
                if self.len > committed && offset >= tail_start {
                    // SAFETY: unsynchronized bytes lie within the last owned
                    // leaf. next retains whole-leaf provenance; the requested
                    // prefix/suffix is initialized and borrowed read-only.
                    unsafe {
                        std::slice::from_raw_parts(
                            self.next.sub(self.len - offset),
                            self.len - offset,
                        )
                    }
                } else {
                    let chunk = v.chunk(offset as u64).unwrap_or(&[]);
                    &chunk[..chunk.len().min(self.len - offset)]
                }
            }
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
        self.synchronize();
        self.available = -1;
        let result = match &mut self.storage {
            Storage::Small(v) => {
                v[offset..offset + bytes.len()].copy_from_slice(bytes);
                Ok(())
            }
            Storage::Large(v) => v.write_at(offset as u64, bytes).map_err(io::Error::other),
        };
        self.refresh_cache();
        result
    }
    #[inline]
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        if let Err(e) = self.write_all(bytes) {
            self.fail(e);
        }
    }
    pub(super) fn fail(&mut self, error: impl std::fmt::Display) {
        self.synchronize();
        self.available = -1;
        self.error = Some(Box::new(error.to_string()));
    }
    #[inline]
    pub fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }
    pub fn truncate(&mut self, len: usize) {
        self.synchronize();
        self.available = -1;
        let len = len.min(self.len);
        match &mut self.storage {
            Storage::Small(v) => v.truncate(len),
            Storage::Large(v) => v.truncate(len as u64).expect("truncate payload"),
        }
        self.len = len;
        self.refresh_cache();
    }
    #[inline(never)]
    fn write_slow(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.append_with_config(bytes, super::buffer::pool_config())
    }
    fn append_with_config(&mut self, bytes: &[u8], config: BufferPoolConfig) -> io::Result<()> {
        self.check()?;
        self.synchronize();
        // Invalidate before any allocation or callback can unwind.
        self.available = -1;
        let end = self
            .len
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("message length overflow"))?;
        let result = (|| {
            match &mut self.storage {
                Storage::Small(v) if end <= config.small_message_bytes => {
                    v.extend_from_slice(bytes)
                }
                Storage::Small(v) => {
                    let pool = shared_pool(config).map_err(io::Error::other)?;
                    let mut large = SegmentedBuffer::new(pool);
                    large
                        .append(v, || with_random(|rng| rng.next_u64()))
                        .map_err(io::Error::other)?;
                    large
                        .append(bytes, || with_random(|rng| rng.next_u64()))
                        .map_err(io::Error::other)?;
                    self.storage = Storage::Large(large);
                }
                Storage::Large(v) => {
                    v.append(bytes, || with_random(|rng| rng.next_u64()))
                        .map_err(io::Error::other)?;
                }
            }
            Ok(())
        })();
        if result.is_ok() {
            self.len = end;
        }
        self.refresh_cache();
        result
    }
}
impl Write for MessageBytes {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_all(bytes)?;
        Ok(bytes.len())
    }
    #[inline]
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        // Valid Rust byte slices fit in isize. -1 routes stored errors through
        // the slow path, including empty writes, without a second hot check.
        if bytes.len() as isize > self.available {
            return self.write_slow(bytes);
        }
        // SAFETY: available is bounded by allocation/leaf space, promotion
        // threshold and representable logical length. Source cannot alias this
        // exclusively borrowed owner. next is non-null even for empty writes.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.next, bytes.len());
            self.next = self.next.add(bytes.len());
        }
        self.len += bytes.len();
        self.available -= bytes.len() as isize;
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.check()
    }
}
impl From<Vec<u8>> for MessageBytes {
    fn from(v: Vec<u8>) -> Self {
        if v.len() <= super::buffer::pool_config().small_message_bytes {
            Self::from_storage(Storage::Small(v))
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
            out.synchronize();
            out.error = self.error.clone();
            out.available = -1;
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
    use crate::wire::{Op, Req, StructWriter, structure};

    #[test]
    fn finished_payload_moves_storage_and_commits_cached_writes() {
        shared_pool(super::super::buffer::pool_config()).unwrap();
        let mut writer = MessageBytes::with_capacity(128);
        writer.write_all(b"cached small payload").unwrap();
        let pointer = writer.chunk(0).as_ptr();
        let payload = MessagePayload::from(writer);
        assert_eq!(payload.chunk(0).as_ptr(), pointer);
        assert_eq!(payload.to_vec(), b"cached small payload");
        assert_eq!(payload.clone(), payload);
        let mut writer = MessageBytes::from(payload);
        assert_eq!(writer.chunk(0).as_ptr(), pointer);
        writer.write_all(b" more").unwrap();
        assert_eq!(writer.to_vec(), b"cached small payload more");

        let data = vec![9; pool::CHUNK_BYTES + 17];
        let mut writer = MessageBytes::from(data.clone());
        writer.write_all(b"cached tail").unwrap();
        let pointer = writer.chunk(0).as_ptr();
        let payload = MessagePayload::from(writer);
        assert!(payload.is_segmented());
        assert_eq!(payload.chunk(0).as_ptr(), pointer);
        assert_eq!(payload.to_vec(), [data.as_slice(), b"cached tail"].concat());
        assert_eq!(payload.clone(), payload);
        let mut writer = MessageBytes::from(payload);
        assert_eq!(writer.chunk(0).as_ptr(), pointer);
        writer.write_all(b" more").unwrap();
        assert_eq!(writer.to_vec(), [data.as_slice(), b"cached tail more"].concat());
    }

    #[test]
    fn finished_payload_preserves_stored_errors() {
        let mut writer = MessageBytes::from(b"prefix".to_vec());
        writer.fail("injected failure");
        let payload = MessagePayload::from(writer);
        assert_eq!(payload.chunk(0), b"prefix");
        assert!(payload.check().is_err());
        assert_eq!(payload.clone(), payload);
        let mut output = Vec::new();
        assert!(payload.write_to(&mut output).is_err());
        assert!(output.is_empty());
        let mut writer = MessageBytes::from(payload);
        assert!(writer.write_all(&[]).is_err());
        writer.clear();
        writer.write_all(b"recovered").unwrap();
        assert_eq!(MessagePayload::from(writer).to_vec(), b"recovered");
    }

    #[test]
    fn cached_writes_are_readable_before_synchronizing_and_survive_moves() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Storage>();
        shared_pool(super::super::buffer::pool_config()).unwrap();
        let mut bytes = MessageBytes::with_capacity(128);
        bytes.write_all(b"first").unwrap();
        bytes.write_all(b" second").unwrap();
        assert_eq!(bytes.len(), 12);
        let Storage::Small(v) = &bytes.storage else { panic!() };
        assert_eq!(v.len(), 0, "ordinary writes should not update backing metadata");
        assert_eq!(bytes.to_vec(), b"first second");
        assert_eq!(bytes.clone(), bytes);
        let mut bytes = std::thread::spawn(move || {
            bytes.write_all(b" third").unwrap();
            bytes
        })
        .join()
        .unwrap();
        bytes.write_at(6, b"SECOND").unwrap();
        assert_eq!(bytes.to_vec(), b"first SECOND third");
        bytes.truncate(5);
        bytes.write_all(b"!").unwrap();
        assert_eq!(bytes.to_vec(), b"first!");
        bytes.fail("injected failure");
        assert!(bytes.write_all(&[]).is_err());
        assert!(bytes.write_all(b"ignored").is_err());
        assert_eq!(bytes.chunk(0), b"first!");
        let mut copy = bytes.clone();
        assert!(copy.write_all(&[]).is_err());
        bytes.clear();
        bytes.write_all(b"reused").unwrap();
        assert_eq!(bytes.to_vec(), b"reused");
    }

    #[test]
    fn cached_leaf_writes_match_a_contiguous_model_through_boundaries() {
        let pool = Box::leak(Box::new(pool::ChunkPool::with_buckets(32, 0, 1).unwrap()));
        let mut bytes = MessageBytes::from_storage(Storage::Large(SegmentedBuffer::new(pool)));
        let mut model = vec![17; pool::CHUNK_BYTES - 11];
        bytes.write_all(&model).unwrap();
        bytes.write_all(b"pending").unwrap();
        model.extend_from_slice(b"pending");
        let Storage::Large(backing) = &bytes.storage else { panic!() };
        assert!(backing.len() < bytes.len() as u64);
        assert_eq!(bytes.chunk(0), model);
        assert_eq!(bytes.chunk(model.len() - 3), &model[model.len() - 3..]);
        let mut state = 0x72657461696e_u64;
        for iteration in 0..1200 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let count = (state % 300) as usize;
            let input = vec![iteration as u8; count];
            bytes.write_all(&input).unwrap();
            model.extend_from_slice(&input);
            if iteration % 13 == 0 {
                let keep = model.len().saturating_sub(count / 2);
                bytes.truncate(keep);
                model.truncate(keep);
            }
            if iteration % 19 == 0 && !model.is_empty() {
                let at = state as usize % model.len();
                bytes.write_at(at, &[99]).unwrap();
                model[at] = 99;
            }
            assert_eq!(bytes.to_vec(), model);
        }
        bytes.clear();
        bytes.write_all(b"small again").unwrap();
        assert!(!bytes.is_segmented());
        assert_eq!(bytes.to_vec(), b"small again");
    }

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
        for _ in 0..pool::CHUNK_BYTES - 1 {
            writer.number_u8(0xa5);
        }
        writer.nested(Op::ReadRecord, |writer| {
            writer.number_u16(42);
            // The next u32 length starts at one byte before the second block boundary.
            for _ in 0..pool::CHUNK_BYTES - 4 {
                writer.number_u8(0x5a);
            }
            writer.bytes(&vec![7; 9000]);
        });
        let request = Req::new(Op::ReadRecord).structure(writer);
        let bytes = request.args()[0].bytes();
        assert!(bytes.is_segmented());
        let mut framed = MessageBytes::new();
        request.encode_to(&mut framed).unwrap();
        // Decode via a BufRead whose chunks stop at the real leaf boundaries.
        let mut decoded_input = PayloadInput { bytes: &framed, at: 0 };
        let decoded = Req::decode_from(&mut decoded_input, framed.len()).unwrap();
        assert_eq!(decoded, request);
        let (mut reader, _) = structure(decoded.args(), Op::ReadRecord, "test").unwrap();
        for _ in 0..pool::CHUNK_BYTES - 1 {
            assert_eq!(reader.number_u8().unwrap(), 0xa5);
        }
        reader
            .nested(Op::ReadRecord, "nested", |reader| {
                assert_eq!(reader.number_u16()?, 42);
                for _ in 0..pool::CHUNK_BYTES - 4 {
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
        let mut bytes = MessageBytes::from_storage(Storage::Large(SegmentedBuffer::new(pool)));
        bytes.write_all(b"prefix").unwrap();
        assert!(bytes.write_all(&vec![0; pool::CHUNK_BYTES * 2]).is_err());
        assert_eq!(bytes.to_vec(), b"prefix");
        bytes.extend_from_slice(&vec![0; pool::CHUNK_BYTES * 2]);
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
