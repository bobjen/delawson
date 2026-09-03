//! Framing and the request/response encoding.
//!
//! One length-prefixed frame per message, hand-encoded: the per-message overhead
//! is the number this project has to drive down, so nothing about it is hidden
//! behind a serialization library.
//!
//! An argument list is tagged rather than positional, which costs one byte per
//! argument and buys a protocol that can gain commands without a version
//! negotiation. Where a request shape settles and its rate matters, it should
//! grow a fixed layout of its own.
//!
//! Every call is asynchronous: a request carries an id, a response echoes it,
//! and nothing requires the two to be adjacent on the connection. A caller may
//! have as many requests outstanding as it likes, and a service may answer them
//! in whatever order it finishes them.
//!
//! Messages are written into a buffer and sent deliberately, so several that
//! happen to be pending on one connection leave in one syscall. That is the only
//! batching there is: no message means "do these N things", the transport simply
//! carries whatever has piled up. Nagle stays off, because the decision of when
//! to send is made here and not by the kernel.
//!
//! **What goes on the wire is a sealed segment, not a message.** Everything a
//! connection sends in one write is compressed and encrypted together, so a
//! nonce, a tag and a compression attempt are paid per write. Inside the segment
//! the messages are just length-prefixed.

use crate::config::Messaging;
use crate::ids::{DiskId, EnId, ExtentId, SnId, StreamId};
use crate::offset::{ELoff, ERoff, ORoff, SRoff};
use crate::random::Rng;
use crate::seal::{self, Seal};
use crate::time::TimeSec;
use crate::{Context, Error, Result};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// Refuse a message larger than this rather than allocating what a peer asks for.
pub const MAX_FRAME_BYTES: usize = 1 << 20;

/// Refuse a message whose tagged structures would require deeper parsing than
/// this. The sender includes its observed maximum so this can be rejected before
/// dispatch, while each nested reader checks the actual depth as it goes.
pub const MAX_RECURSION_DEPTH: u8 = 64;

/// Reserve a message-derived collection only when its count can fit in the
/// available encoded bytes. The parser that knows a larger minimum item size
/// may impose a tighter limit before calling this helper.
pub fn checked_vec<T, N>(count: N, available_bytes: usize) -> Result<Vec<T>>
where
    N: TryInto<usize>,
{
    let count = count.try_into().map_err(|_| Error::new("collection count does not fit"))?;
    if count > available_bytes {
        return Err(Error::new("collection count exceeds message bytes"));
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|e| Error::new(format!("collection allocation: {e}")))?;
    Ok(values)
}

/// And a batch of them is sealed in pieces no larger than this, so one busy
/// connection cannot make the far side allocate without bound.
pub const MAX_SEGMENT_BYTES: usize = 8 << 20;

/// A request whose arguments are one self-contained command payload uses the
/// request's Op as its tag, so it does not repeat that tag inside the payload.
const COMMAND_PAYLOAD: u16 = u16::MAX;

macro_rules! ops {
    ($($name:ident = $code:expr, $text:expr;)*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[repr(u16)]
        pub enum Op { $($name = $code,)* }

        impl Op {
            pub const ALL: &'static [Op] = &[$(Op::$name,)*];

            pub fn code(self) -> u16 { self as u16 }

            pub fn from_code(code: u16) -> Result<Op> {
                match code {
                    $($code => Ok(Op::$name),)*
                    other => Err(Error::new(format!("unknown opcode {other}"))),
                }
            }

            /// The name used on the command line.
            pub fn as_str(self) -> &'static str {
                match self { $(Op::$name => $text,)* }
            }
        }
    };
}

// Op codes are stable wire identifiers. This namespace contains standalone commands and
// nontrivial serialized pieces. Append new commands or schema tags; never renumber existing
// codes. Retired codes become tombstones. Reuse an old tombstone only after the required release
// gap, and only when no unused code remains at the end of the namespace; reuse the oldest
// tombstones first. Old and malicious clients are untrusted input, so every parser must fail
// boundedly rather than crash, hang, or perform an unauthorized action.
ops! {
    // Control, answered by every machine type.
    Ping = 1, "ping";
    Status = 2, "status";
    Shutdown = 3, "shutdown";
    // Sites is the live counters, per logging site in the code; the three below
    // read what was written to the files.
    Sites = 4, "sites";
    LogHistogram = 5, "log-histogram";
    LogGrep = 6, "log-grep";
    LogLines = 7, "log-lines";

    // KS: the registry, then the keys.
    Register = 8, "register";
    Lookup = 9, "lookup";
    Registry = 10, "registry";

    Unlock = 11, "unlock";
    SnMasterKey = 12, "sn-master-key";
    RotateMasterKey = 13, "rotate-master-key";

    // Namespace.
    MakeDirectory = 14, "mkdir";
    RemoveDirectory = 15, "rmdir";
    List = 16, "ls";
    Stat = 17, "stat";

    // Objects.
    CreateObject = 18, "create";
    DeleteObject = 19, "delete";
    UndeleteObject = 20, "undelete";
    SetExpiry = 21, "set-expiry";
    CloneObject = 22, "clone";
    ConcatObjects = 23, "concat";
    ObjectInfo = 24, "object-info";
    BatchCreate = 25, "batch-create";
    BatchInfo = 26, "batch-object-info";

    // SN, from a DN: a stream is named by the DN that owns it.
    CreateStream = 27, "create-stream";
    // And from an FE: the extent a stream is currently appendable at, opened if
    // there is not one already, and which extent holds a given record.
    OpenExtent = 28, "open-extent";
    LocateRecord = 29, "locate-record";
    // Sealing is reported to the SN, which records what the extent came to and
    // opens the next one in the same breath.
    SealExtent = 30, "seal-extent";
    // And a run of a stream's extents, for a caller building hints (§9.1.3.7). The
    // plural of locate-record, and the first thing to ask an SN that is not about
    // finding one place to read or write.
    ListExtents = 31, "list-extents";

    // DN, about an object's streams: which one holds a record, and another one
    // when the last has sealed.
    LocateStream = 32, "locate-stream";
    ExtendObject = 33, "extend-object";

    // EN. An append arrives from an FE at the primary; the primary broadcasts the
    // same bytes at a fixed offset to the other three instances.
    AppendExtent = 34, "append-extent";
    ApplyAppend = 35, "apply-append";
    ReadExtent = 36, "read-extent";

    // Records.
    AppendRecord = 37, "append";
    InsertRecord = 38, "insert";
    UpdateRecord = 39, "update";
    DeleteRecord = 40, "delete-record";
    ReadRecord = 41, "read";
    Scan = 42, "scan";
    Count = 43, "count";
    TimeRange = 44, "time-range";

    // Structure and response identifiers share the Op namespace with commands.
    Range = 45, "range";
    ShapeResp = 46, "shape-response";
    BatchResp = 47, "batch-response";
    DescribedResp = 48, "described-response";
    StreamRowResp = 49, "stream-row-response";
    ExtentRowResp = 50, "extent-row-response";
    PieceResp = 51, "piece-response";
    AppendBlock = 52, "append-block";
    Expecting = 53, "expecting";
    // Resp payloads have their own identifiers so their formats can evolve
    // independently of the requests that caused them. This also leaves the door
    // open to batching both directions in one stream without making request and
    // response formats ambiguous.
    FilledResp = 54, "filled-response";
    CoveredResp = 55, "covered-response";
    ExtentResp = 56, "extent-response";
    ReceiptResp = 57, "receipt-response";
    LogResp = 58, "log-response";
    RegistryResp = 59, "registry-response";
    ExtentRunResp = 60, "extent-run-response";
    EmptyResp = 61, "empty-response";
    TextResp = 62, "text-response";
    BytesResp = 63, "bytes-response";
    NumberResp = 64, "number-response";
    FlagResp = 65, "flag-response";
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Status {
    Ok = 0,
    /// Accepted as a well-formed request for something not yet built.
    Unimplemented = 1,
    BadRequest = 2,
    NotFound = 3,
    /// KS has not been unlocked.
    Locked = 4,
    Internal = 5,
    /// That extent is sealed, so the append belongs in the next one. Not a failure:
    /// the caller is being told where to go, and the answer carries what the extent
    /// came to.
    Sealed = 6,
    /// The path names a different object than the caller expected. **Not `NotFound`**: the
    /// path resolves perfectly well, to something else — and answering about that something
    /// else is the silent wrong answer this exists to prevent (§17.36).
    Replaced = 7,
}

impl Status {
    pub fn from_code(code: u16) -> Result<Status> {
        Ok(match code {
            0 => Status::Ok,
            1 => Status::Unimplemented,
            2 => Status::BadRequest,
            3 => Status::NotFound,
            4 => Status::Locked,
            5 => Status::Internal,
            6 => Status::Sealed,
            7 => Status::Replaced,
            other => return Err(Error::new(format!("unknown status {other}"))),
        })
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Status::Ok => "ok",
            Status::Unimplemented => "unimplemented",
            Status::BadRequest => "bad request",
            Status::NotFound => "not found",
            Status::Locked => "locked",
            Status::Internal => "internal error",
            Status::Sealed => "that extent is sealed",
            Status::Replaced => "that object was replaced",
        };
        f.write_str(text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arg {
    /// A self-contained serialized structure. The structure owns the payload
    /// format; the wire only supplies its schema tag and length.
    /// The third value is the maximum tagged-structure depth observed while
    /// constructing this value. It is not serialized separately; the enclosing
    /// message carries the maximum for all of its values.
    Struct(Op, Vec<u8>, u8),
    Text(String),
    Number(u64),
    Flag(bool),
    Bytes(Vec<u8>),
}

/// Build the self-contained payload of one tagged structure.
pub struct StructWriter {
    bytes: Vec<u8>,
    depth: u8,
    maximum_depth: u8,
}

impl StructWriter {
    pub fn new() -> StructWriter {
        StructWriter { bytes: Vec::new(), depth: 1, maximum_depth: 1 }
    }

    pub fn number_u64(&mut self, value: u64) {
        self.bytes.extend(value.to_le_bytes());
    }

    pub fn number_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn number_u16(&mut self, value: u16) {
        self.bytes.extend(value.to_le_bytes());
    }

    pub fn number_u32(&mut self, value: u32) {
        self.bytes.extend(value.to_le_bytes());
    }

    pub fn flag(&mut self, value: bool) {
        self.bytes.push(value as u8);
    }

    pub fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend((value.len() as u32).to_le_bytes());
        self.bytes.extend(value);
    }

    pub fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    /// Write a nested tagged structure while tracking its depth in the enclosing value.
    pub fn nested(&mut self, op: Op, write: impl FnOnce(&mut StructWriter)) {
        self.bytes.extend(op.code().to_le_bytes());
        self.depth = self.depth.checked_add(1).expect("serialization nesting depth overflow");
        self.maximum_depth = self.maximum_depth.max(self.depth);
        write(self);
        self.depth -= 1;
    }

    pub fn finish(self, op: Op) -> Arg {
        Arg::Struct(op, self.bytes, self.maximum_depth)
    }
}

/// Read the self-contained payload of one tagged structure.
pub struct StructReader<'a> {
    bytes: &'a [u8],
    at: usize,
    depth: u8,
    maximum_depth: u8,
    parse_count: usize,
}

impl<'a> StructReader<'a> {
    pub fn new(bytes: &'a [u8], maximum_depth: u8) -> StructReader<'a> {
        StructReader { bytes, at: 0, depth: 1, maximum_depth, parse_count: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        self.parse_count =
            self.parse_count.checked_add(1).ok_or_else(|| Error::new("parse count overflow"))?;
        if self.parse_count > self.bytes.len() {
            return Err(Error::new("too many parse routines for structure size"));
        }
        let end = self.at.checked_add(count).ok_or_else(|| Error::new("length overflow"))?;
        if end > self.bytes.len() {
            return Err(Error::new("structure payload ends mid-value"));
        }
        let value = &self.bytes[self.at..end];
        self.at = end;
        Ok(value)
    }

    pub fn number_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn number_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn number_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn number_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Allocate a collection whose count came from this structure.
    pub fn checked_vec<T, N>(&self, count: N) -> Result<Vec<T>>
    where
        N: TryInto<usize>,
    {
        checked_vec(count, self.bytes.len() - self.at)
    }

    pub fn flag(&mut self) -> Result<bool> {
        Ok(self.take(1)?[0] != 0)
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        Ok(self.take(length)?.to_vec())
    }

    pub fn text(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).context("text in structure payload")
    }

    pub fn tag(&mut self, expected: Op, what: &str) -> Result<()> {
        let actual = Op::from_code(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))?;
        if actual == expected {
            Ok(())
        } else {
            Err(Error::new(format!("{what} expected {expected}, got {actual}")))
        }
    }

    pub fn nested<T>(
        &mut self,
        expected: Op,
        what: &str,
        read: impl FnOnce(&mut StructReader<'a>) -> Result<T>,
    ) -> Result<T> {
        self.tag(expected, what)?;
        let depth =
            self.depth.checked_add(1).ok_or_else(|| Error::new("recursion depth overflow"))?;
        if depth > self.maximum_depth {
            return Err(Error::new("message recursion depth exceeded"));
        }
        self.depth = depth;
        let result = read(self);
        self.depth -= 1;
        result
    }

    pub fn done(&self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::new("trailing bytes in structure payload"))
        }
    }
}

pub fn structure<'a>(
    args: &'a [Arg],
    expected: Op,
    what: &str,
) -> Result<(StructReader<'a>, &'a [Arg])> {
    let [Arg::Struct(actual, bytes, maximum_depth), rest @ ..] = args else {
        return Err(Error::new(format!("{what} is not self-contained")));
    };
    if *actual != expected {
        return Err(Error::new(format!("expected {expected}, got {actual}")));
    }
    Ok((StructReader::new(bytes, *maximum_depth), rest))
}

impl std::fmt::Display for Arg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Arg::Text(text) => f.write_str(text),
            Arg::Number(n) => write!(f, "{n}"),
            Arg::Flag(b) => write!(f, "{b}"),
            Arg::Bytes(bytes) => write!(f, "{} bytes", bytes.len()),
            Arg::Struct(op, bytes, _) => write!(f, "<{op}: {} bytes>", bytes.len()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Req {
    /// Chosen by the caller, echoed by the response. Unique among that caller's
    /// outstanding requests, and nothing more.
    pub id: u32,
    pub op: Op,
    pub args: Vec<Arg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resp {
    pub id: u32,
    pub status: Status,
    pub args: Vec<Arg>,
}

impl Req {
    pub fn new(op: Op) -> Req {
        Req { id: 0, op, args: Vec::new() }
    }

    pub fn id(mut self, id: u32) -> Req {
        self.id = id;
        self
    }

    pub fn text(mut self, text: impl Into<String>) -> Req {
        self.args.push(Arg::Text(text.into()));
        self
    }

    pub fn number(mut self, n: u64) -> Req {
        self.args.push(Arg::Number(n));
        self
    }

    pub fn flag(mut self, b: bool) -> Req {
        self.args.push(Arg::Flag(b));
        self
    }

    pub fn bytes(mut self, bytes: Vec<u8>) -> Req {
        self.args.push(Arg::Bytes(bytes));
        self
    }

    /// Put this command's self-contained payload in the request. The request's
    /// `Op` is the command tag; the payload is positional and parsed by the
    /// command, not by generic wire code.
    pub fn structure(mut self, payload: StructWriter) -> Req {
        self.args.push(payload.finish(self.op));
        self
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self.id.to_le_bytes());
        out.extend(self.op.code().to_le_bytes());
        out.push(message_depth(&self.args));
        if let [Arg::Struct(op, bytes, _)] = self.args.as_slice()
            && *op == self.op
        {
            out.extend(COMMAND_PAYLOAD.to_le_bytes());
            out.extend(bytes);
        } else {
            encode_args(&self.args, out);
        }
    }

    fn decode(payload: &[u8]) -> Result<Req> {
        let mut reader = Cursor::new(payload);
        let id = reader.u32()?;
        let op = Op::from_code(reader.u16()?)?;
        let maximum_depth = reader.u8()?;
        if maximum_depth == 0 {
            return Err(Error::new("message recursion depth is zero"));
        }
        if maximum_depth > MAX_RECURSION_DEPTH {
            return Err(Error::new("message recursion depth exceeds limit"));
        }
        let count = reader.u16()?;
        let args = if count == COMMAND_PAYLOAD {
            vec![Arg::Struct(op, reader.remaining().to_vec(), maximum_depth)]
        } else {
            reader.args_with_count_and_depth(count, maximum_depth)?
        };
        Ok(Req { id, op, args })
    }
}

impl Resp {
    pub fn ok() -> Resp {
        Resp { id: 0, status: Status::Ok, args: vec![StructWriter::new().finish(Op::EmptyResp)] }
    }

    pub fn text(text: impl Into<String>) -> Resp {
        let text = text.into();
        let mut payload = StructWriter::new();
        payload.text(&text);
        Resp { id: 0, status: Status::Ok, args: vec![payload.finish(Op::TextResp)] }
    }

    pub fn error(status: Status, message: impl Into<String>) -> Resp {
        let message = message.into();
        let mut payload = StructWriter::new();
        payload.text(&message);
        Resp { id: 0, status, args: vec![payload.finish(Op::TextResp)] }
    }

    pub fn flag(b: bool) -> Resp {
        let mut payload = StructWriter::new();
        payload.flag(b);
        Resp { id: 0, status: Status::Ok, args: vec![payload.finish(Op::FlagResp)] }
    }

    pub fn bytes(bytes: Vec<u8>) -> Resp {
        let mut payload = StructWriter::new();
        payload.bytes(&bytes);
        Resp { id: 0, status: Status::Ok, args: vec![payload.finish(Op::BytesResp)] }
    }

    pub fn number(status: Status, number: u64) -> Resp {
        let mut payload = StructWriter::new();
        payload.number_u64(number);
        Resp { id: 0, status, args: vec![payload.finish(Op::NumberResp)] }
    }

    /// Answer this request, whenever the answer is ready.
    pub fn to(mut self, request: &Req) -> Resp {
        self.id = request.id;
        self
    }

    /// The single text argument, if the response is one message.
    pub fn message(&self) -> Option<String> {
        let (mut payload, rest) = structure(&self.args, Op::TextResp, "text response").ok()?;
        if !rest.is_empty() {
            return None;
        }
        let text = payload.text().ok()?;
        payload.done().ok()?;
        Some(text)
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self.id.to_le_bytes());
        out.extend((self.status as u16).to_le_bytes());
        out.push(message_depth(&self.args));
        encode_args(&self.args, out);
    }

    fn decode(payload: &[u8]) -> Result<Resp> {
        let mut reader = Cursor::new(payload);
        let id = reader.u32()?;
        let status = Status::from_code(reader.u16()?)?;
        let maximum_depth = reader.u8()?;
        if maximum_depth == 0 {
            return Err(Error::new("message recursion depth is zero"));
        }
        if maximum_depth > MAX_RECURSION_DEPTH {
            return Err(Error::new("message recursion depth exceeds limit"));
        }
        Ok(Resp { id, status, args: reader.args_with_depth(maximum_depth)? })
    }
}

/// An append as it goes from an FE to the primary EN, and from the primary to the
/// other instances.
///
/// ⚠️ **This exists because the argument order was a contract neither side could
/// test.** Two processes each counting positions in a list pass their own unit tests
/// while disagreeing with each other. One shape, built and read here, makes that a
/// compile error instead.
///
/// **The framing is not in it.** A block arrives compressed (§8.1.4.1), so nothing
/// below the writer can see a record in it — which is why what it *holds* travels
/// alongside as numbers rather than being worked out from the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBlock {
    pub sn: SnId,
    pub extent: ExtentId,
    /// The second the extent was created in, which is part of its name.
    pub created: TimeSec,
    /// Framing of the records in this append block.
    pub framing: crate::framing::Framing,
    /// CRC of the uncompressed block bytes.
    pub crc: u64,
    /// What the writer says these bytes contain.
    pub holds: crate::block::Holds,
    /// The canonical logical offset assigned by the primary, for secondaries.
    pub logical_offset: Option<ELoff>,
    /// The **(EnId, DiskId)** pairs holding it, primary first.
    pub instances: Vec<(EnId, DiskId)>,
    /// The block as stored: compressed, unless compressing it did not help.
    pub bytes: Vec<u8>,
}

/// The receipt a primary EN returns to FE after an append is durable. FE turns
/// the extent-local record number into the stream- and object-local numbers
/// visible to the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnReceipt {
    pub sn: SnId,
    pub logical: ELoff,
    pub first_record: ERoff,
    pub records: ERoff,
    pub extent: ExtentId,
    pub created: TimeSec,
    pub length: ELoff,
}

impl EnReceipt {
    pub fn write(self) -> Arg {
        let mut payload = StructWriter::new();
        payload.number_u32(self.sn.get());
        payload.number_u32(self.logical.get());
        payload.number_u32(self.first_record.get());
        payload.number_u32(self.records.get());
        payload.number_u32(self.extent.get());
        payload.number_u64(self.created.number());
        payload.number_u32(self.length.get());
        payload.finish(Op::ReceiptResp)
    }

    pub fn read(args: &[Arg]) -> Result<EnReceipt> {
        let (mut payload, rest) = structure(args, Op::ReceiptResp, "an EN append receipt")?;
        let receipt = EnReceipt {
            sn: SnId::new(payload.number_u32()?),
            logical: ELoff::new(payload.number_u32()?),
            first_record: ERoff::new(payload.number_u32()?),
            records: ERoff::new(payload.number_u32()?),
            extent: ExtentId::new(payload.number_u32()?),
            created: TimeSec::new(payload.number_u64()?),
            length: ELoff::new(payload.number_u32()?),
        };
        if !rest.is_empty() {
            return Err(Error::new("an EN append receipt has trailing arguments"));
        }
        payload.done()?;
        Ok(receipt)
    }
}

/// The append receipt exposed to a writer after FE has converted the EN's
/// extent-local record number into durable stream- and object-local numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    pub stream: StreamId,
    pub record: SRoff,
    pub object_record: ORoff,
    pub records: ERoff,
    pub extent: ExtentId,
    pub created: TimeSec,
    pub offset: ELoff,
    pub length: ELoff,
}

impl Receipt {
    pub fn write(self) -> Arg {
        let mut payload = StructWriter::new();
        payload.number_u32(self.stream.get());
        payload.number_u64(self.record.number());
        payload.number_u64(self.object_record.number());
        payload.number_u32(self.records.get());
        payload.number_u32(self.extent.get());
        payload.number_u64(self.created.number());
        payload.number_u32(self.offset.get());
        payload.number_u32(self.length.get());
        payload.finish(Op::ReceiptResp)
    }

    pub fn read(args: &[Arg]) -> Result<Receipt> {
        let (mut payload, rest) = structure(args, Op::ReceiptResp, "an append receipt")?;
        let receipt = Receipt {
            stream: StreamId::new(payload.number_u32()?),
            record: SRoff::new(payload.number_u64()?),
            object_record: ORoff::new(payload.number_u64()?),
            records: ERoff::new(payload.number_u32()?),
            extent: ExtentId::new(payload.number_u32()?),
            created: TimeSec::new(payload.number_u64()?),
            offset: ELoff::new(payload.number_u32()?),
            length: ELoff::new(payload.number_u32()?),
        };
        if !rest.is_empty() {
            return Err(Error::new("an append receipt has trailing arguments"));
        }
        payload.done()?;
        Ok(receipt)
    }
}

impl AppendBlock {
    pub fn request(&self, en: EnId, disk: DiskId, op: Op) -> Req {
        let mut request = Req::new(op);
        let mut payload = StructWriter::new();
        let (kind, width) = self.framing.wire();
        payload.number_u32(en.get());
        payload.number_u16(disk.get());
        payload.number_u32(self.sn.get());
        payload.number_u32(self.extent.get());
        payload.number_u64(self.created.number());
        payload.number_u8(kind);
        payload.number_u32(width);
        payload.number_u64(self.crc);
        payload.number_u64(self.holds.compressed.number());
        payload.number_u64(self.holds.logical.number());
        payload.number_u64(self.holds.records.number());
        payload.flag(self.logical_offset.is_some());
        payload.number_u64(self.logical_offset.map(|o| o.number()).unwrap_or(0));
        payload.number_u32(self.instances.len() as u32);
        for (en, disk) in &self.instances {
            payload.number_u32(en.get());
            payload.number_u16(disk.get());
        }
        payload.bytes(&self.bytes);
        request.args.push(payload.finish(op));
        request
    }

    pub fn of(request: &Req) -> Result<(EnId, DiskId, AppendBlock)> {
        let (mut payload, rest) = structure(&request.args, request.op, "an append request")?;
        if !rest.is_empty() {
            return Err(Error::new("an append block has arguments after its payload"));
        }
        let en = EnId::of_number(payload.number_u32()? as u64)?;
        let disk = DiskId::of_number(payload.number_u16()? as u64)?;
        let sn = SnId::of_number(payload.number_u32()? as u64)?;
        let extent = ExtentId::of_number(payload.number_u32()? as u64)?;
        let created = TimeSec::new(payload.number_u64()?);
        let kind = payload.number_u8()?;
        let width = payload.number_u32()?;
        let crc = payload.number_u64()?;
        let holds = crate::block::Holds {
            compressed: crate::offset::ECoff::of_number(payload.number_u64()?)?,
            logical: crate::offset::ELoff::of_number(payload.number_u64()?)?,
            records: crate::offset::ERoff::of_number(payload.number_u64()?)?,
        };
        let has_logical = payload.flag()?;
        let logical = payload.number_u64()?;
        let count = payload.number_u32()?;
        let mut instances = payload.checked_vec(count)?;
        for _ in 0..count {
            instances.push((
                EnId::of_number(payload.number_u32()? as u64)?,
                DiskId::of_number(payload.number_u16()? as u64)?,
            ));
        }
        let bytes = payload.bytes()?;
        payload.done()?;
        Ok((
            en,
            disk,
            AppendBlock {
                sn,
                extent,
                created,
                framing: crate::framing::Framing::of_wire(kind, width)?,
                crc,
                holds,
                instances,
                logical_offset: has_logical.then(|| ELoff::of_number(logical)).transpose()?,
                bytes,
            },
        ))
    }
}

fn encode_args(args: &[Arg], out: &mut Vec<u8>) {
    out.extend((args.len() as u16).to_le_bytes());
    for arg in args {
        match arg {
            Arg::Text(text) => {
                out.push(0);
                out.extend((text.len() as u32).to_le_bytes());
                out.extend(text.as_bytes());
            }
            Arg::Number(n) => {
                out.push(1);
                out.extend(n.to_le_bytes());
            }
            Arg::Flag(b) => {
                out.push(2);
                out.push(*b as u8);
            }
            Arg::Bytes(bytes) => {
                out.push(3);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
            Arg::Struct(op, bytes, _) => {
                out.push(4);
                out.extend(op.code().to_le_bytes());
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
        }
    }
}

fn message_depth(args: &[Arg]) -> u8 {
    args.iter().fold(1, |depth, arg| match arg {
        Arg::Struct(_, _, nested) => depth.max(*nested),
        _ => depth,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
    parse_count: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Cursor<'a> {
        Cursor { bytes, at: 0, parse_count: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        self.parse_count =
            self.parse_count.checked_add(1).ok_or_else(|| Error::new("parse count overflow"))?;
        if self.parse_count > self.bytes.len() {
            return Err(Error::new("too many parse routines for message size"));
        }
        let end = self.at.checked_add(n).ok_or_else(|| Error::new("length overflow"))?;
        if end > self.bytes.len() {
            return Err(Error::new("frame ends mid-value"));
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.at..]
    }

    fn args_with_depth(&mut self, maximum_depth: u8) -> Result<Vec<Arg>> {
        let count = self.u16()?;
        self.args_with_count_and_depth(count, maximum_depth)
    }

    fn args_with_count_and_depth(&mut self, count: u16, maximum_depth: u8) -> Result<Vec<Arg>> {
        let mut args = checked_vec(count, self.bytes.len() - self.at)?;
        for _ in 0..count {
            args.push(match self.u8()? {
                0 => {
                    let len = self.u32()? as usize;
                    let bytes = self.take(len)?;
                    Arg::Text(String::from_utf8(bytes.to_vec()).context("text argument")?)
                }
                1 => Arg::Number(self.u64()?),
                2 => Arg::Flag(self.u8()? != 0),
                3 => {
                    let len = self.u32()? as usize;
                    Arg::Bytes(self.take(len)?.to_vec())
                }
                4 => {
                    let op = Op::from_code(self.u16()?)?;
                    let len = self.u32()? as usize;
                    Arg::Struct(op, self.take(len)?.to_vec(), maximum_depth)
                }
                tag => return Err(Error::new(format!("unknown argument tag {tag}"))),
            });
        }
        if self.at != self.bytes.len() {
            return Err(Error::new("trailing bytes in frame"));
        }
        Ok(args)
    }
}

/// Buffer size on each side of a connection. Big enough that a deep pipeline of
/// small messages leaves in one write.
pub const BUF_BYTES: usize = 64 * 1024;

/// Frame a message into `buf`: four bytes of length, then whatever `body`
/// appends. The length is backfilled, so the body is written once and never
/// copied.
pub fn frame_into(buf: &mut Vec<u8>, body: impl FnOnce(&mut Vec<u8>)) -> Result<()> {
    let start = buf.len();
    buf.extend([0u8; 4]);
    body(buf);
    let length = buf.len() - start - 4;
    if length > MAX_FRAME_BYTES {
        buf.truncate(start);
        return Err(Error::new(format!("frame of {length} bytes is too large")));
    }
    buf[start..start + 4].copy_from_slice(&(length as u32).to_le_bytes());
    Ok(())
}

/// A buffered connection. `more` says whether bytes are already in hand, which is
/// how both sides know they may keep piling up work instead of flushing.
/// The write half of a connection. Producers append messages to a plain buffer
/// of their own; this seals whatever they hand over and writes it as one segment.
pub struct Wire {
    stream: TcpStream,
    seal: Seal,
    sealed: Vec<u8>,
}

impl Wire {
    /// Seal everything in `plain` and send it, leaving `plain` empty. A batch
    /// larger than a segment is split at a message boundary rather than refused.
    pub fn send(&mut self, plain: &mut Vec<u8>) -> Result<()> {
        let mut at = 0;
        while at < plain.len() {
            let end = segment_end(&plain[at..])? + at;
            self.sealed.clear();
            self.seal.seal(&mut plain[at..end].to_vec(), &mut self.sealed)?;
            self.stream.write_all(&self.sealed)?;
            at = end;
        }
        plain.clear();
        Ok(())
    }
}

/// The read half. A segment arrives whole and is opened whole; `more` says
/// whether messages from it are still in hand, which is how the reader knows it
/// may keep working instead of sending.
pub struct Reader {
    stream: TcpStream,
    seal: Seal,
    open: Vec<u8>,
    at: usize,
    /// When the segment now being read arrived. A batch arrives at one moment, so
    /// this is one clock read for however many messages it holds — and it is the
    /// honest start for a duration, being when the bytes got here rather than
    /// when a thread got to them.
    arrived: Instant,
}

impl Reader {
    pub fn more(&self) -> bool {
        self.at < self.open.len()
    }

    pub fn arrived(&self) -> Instant {
        self.arrived
    }

    /// The next message, reading and opening a segment if none is in hand.
    fn next(&mut self) -> Result<&[u8]> {
        if !self.more() {
            self.fill()?;
        }
        let bytes = &self.open[self.at..];
        if bytes.len() < 4 {
            return Err(Error::new("a segment ending mid-message"));
        }
        let length = u32::from_le_bytes(bytes[..4].try_into().expect("length")) as usize;
        if bytes.len() < 4 + length {
            return Err(Error::new("a message longer than its segment"));
        }
        self.at += 4 + length;
        Ok(&bytes[4..4 + length])
    }

    fn fill(&mut self) -> Result<()> {
        let mut header = [0u8; seal::HEADER];
        self.stream.read_exact(&mut header)?;
        self.arrived = Instant::now();
        let length = u32::from_le_bytes(header[..4].try_into().expect("length")) as usize;
        if length > MAX_SEGMENT_BYTES + seal::OVERHEAD {
            return Err(Error::new(format!("peer announced a {length}-byte segment")));
        }
        let mut sealed = vec![0u8; length];
        self.stream.read_exact(&mut sealed)?;
        self.open.clear();
        self.at = 0;
        self.seal.open(&header, &sealed, &mut self.open)
    }

    pub fn request(&mut self) -> Result<Req> {
        Req::decode(self.next()?)
    }

    pub fn response(&mut self) -> Result<Resp> {
        Resp::decode(self.next()?)
    }
}

/// How much of a batch fits in one segment, cut at a message boundary. At least
/// one message always goes, since a message cannot exceed a segment.
fn segment_end(plain: &[u8]) -> Result<usize> {
    let mut at = 0;
    while at < plain.len() {
        if plain.len() - at < 4 {
            return Err(Error::new("a batch ending mid-message"));
        }
        let length = u32::from_le_bytes(plain[at..at + 4].try_into().expect("length")) as usize;
        let next = at + 4 + length;
        if next > plain.len() {
            return Err(Error::new("a message longer than its batch"));
        }
        if next > MAX_SEGMENT_BYTES && at > 0 {
            return Ok(at);
        }
        at = next;
    }
    Ok(at)
}

/// A batching sender for one direction of one connection: producers append
/// messages, and a thread of its own writes whatever has piled up, sealing each
/// batch as one segment. The next batch is whatever arrived while the last one
/// was in flight, so batch size follows load with no timer and nothing to tune.
///
/// Both sides of a process want this — answers to clients and requests to peers —
/// so it lives here rather than twice.
pub struct Outbox {
    filling: Mutex<Vec<u8>>,
    ready: Condvar,
    open: AtomicBool,
}

impl Outbox {
    /// Start a sender on `wire`, and return the box to append to.
    pub fn start(wire: Wire) -> Arc<Outbox> {
        let outbox = Arc::new(Outbox {
            filling: Mutex::new(Vec::with_capacity(BUF_BYTES)),
            ready: Condvar::new(),
            open: AtomicBool::new(true),
        });
        let sending = Arc::clone(&outbox);
        std::thread::spawn(move || sending.run(wire));
        outbox
    }

    /// Append one framed message. The lock is held for a copy, never for a write.
    pub fn put(&self, framed: &[u8]) {
        let mut filling = self.filling.lock().expect("outbox");
        let woke = filling.is_empty();
        filling.extend_from_slice(framed);
        drop(filling);
        // Waking is only needed when the sender has nothing else to find.
        if woke {
            self.ready.notify_one();
        }
    }

    pub fn close(&self) {
        self.open.store(false, Ordering::SeqCst);
        self.ready.notify_one();
    }

    fn run(&self, mut wire: Wire) {
        let mut batch = Vec::with_capacity(BUF_BYTES);
        loop {
            {
                let mut filling = self.filling.lock().expect("outbox");
                while filling.is_empty() {
                    if !self.open.load(Ordering::SeqCst) {
                        return;
                    }
                    filling = self.ready.wait(filling).expect("outbox");
                }
                std::mem::swap(&mut *filling, &mut batch);
            }
            if wire.send(&mut batch).is_err() {
                return;
            }
            batch.clear();
        }
    }
}

/// Append a message to a plain buffer, to be sealed with whatever else is there.
pub fn put_request(plain: &mut Vec<u8>, request: &Req) -> Result<()> {
    frame_into(plain, |buf| request.encode(buf))
}

pub fn put_response(plain: &mut Vec<u8>, response: &Resp) -> Result<()> {
    frame_into(plain, |buf| response.encode(buf))
}

/// The two halves of one connection, for a caller that uses both from one thread.
pub struct Link {
    pub reader: Reader,
    pub wire: Wire,
    pub plain: Vec<u8>,
}

impl Link {
    pub fn call(&mut self, request: &Req) -> Result<Resp> {
        put_request(&mut self.plain, request)?;
        self.wire.send(&mut self.plain)?;
        self.reader.response()
    }
}

/// The connecting side picks the nonce prefix and states it before anything is
/// sealed. Directions differ by a byte in the nonce, so one prefix is enough for
/// both and no two segments under one key can share a nonce.
pub fn connect(stream: TcpStream, config: &Messaging) -> Result<Link> {
    stream.set_nodelay(true)?;
    let mut prefix = [0u8; 8];
    Rng::new().fill_bytes(&mut prefix);
    let mut writing = stream.try_clone()?;
    writing.write_all(&prefix)?;
    Ok(Link {
        reader: Reader {
            stream: stream.try_clone()?,
            seal: Seal::new(config, prefix, 1)?,
            open: Vec::with_capacity(BUF_BYTES),
            at: 0,
            arrived: Instant::now(),
        },
        wire: Wire { stream: writing, seal: Seal::new(config, prefix, 0)?, sealed: Vec::new() },
        plain: Vec::with_capacity(BUF_BYTES),
    })
}

/// The accepting side takes the prefix it is given.
pub fn accept(stream: TcpStream, config: &Messaging) -> Result<(Reader, Wire)> {
    stream.set_nodelay(true)?;
    let mut prefix = [0u8; 8];
    let mut reading = stream.try_clone()?;
    reading.read_exact(&mut prefix)?;
    Ok((
        Reader {
            stream: reading,
            seal: Seal::new(config, prefix, 0)?,
            open: Vec::with_capacity(BUF_BYTES),
            at: 0,
            arrived: Instant::now(),
        },
        Wire { stream, seal: Seal::new(config, prefix, 1)?, sealed: Vec::new() },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_request(request: &Req) -> Req {
        let mut plain = Vec::new();
        put_request(&mut plain, request).unwrap();
        let length = u32::from_le_bytes(plain[..4].try_into().unwrap()) as usize;
        assert_eq!(length + 4, plain.len());
        Req::decode(&plain[4..]).unwrap()
    }

    #[test]
    fn req_round_trip() {
        let request = Req::new(Op::AppendRecord)
            .id(7)
            .text("/bob/notes")
            .number(17)
            .flag(true)
            .bytes(vec![1, 2, 3]);
        assert_eq!(round_trip_request(&request), request);
    }

    #[test]
    fn a_command_can_use_its_op_as_a_self_contained_payload_tag() {
        let mut payload = StructWriter::new();
        payload.number_u64(7);
        payload.number_u64(42);
        let request = Req::new(Op::SnMasterKey).structure(payload);
        assert_eq!(round_trip_request(&request), request);
        assert!(
            encoded_len(&request) < encoded_len(&Req::new(Op::SnMasterKey).number(7).number(42))
        );
    }

    #[test]
    fn a_message_carries_the_depth_seen_while_serializing() {
        let mut payload = StructWriter::new();
        crate::range::Range::from(7).write_nested(&mut payload);
        let request = Req::new(Op::ReadRecord).structure(payload);
        let mut plain = Vec::new();
        put_request(&mut plain, &request).unwrap();
        assert_eq!(plain[10], 2);
    }

    #[test]
    fn a_nested_structure_may_not_exceed_the_message_depth() {
        let mut payload = StructWriter::new();
        crate::range::Range::from(7).write_nested(&mut payload);
        let Arg::Struct(op, bytes, _) = payload.finish(Op::ReadRecord) else {
            panic!("a self-contained payload")
        };
        let request = Req { id: 0, op, args: vec![Arg::Struct(op, bytes, 1)] };
        let (mut reader, rest) = structure(&request.args, op, "a request").unwrap();
        assert!(rest.is_empty());
        assert!(crate::range::Range::read_nested(&mut reader).is_err());
    }

    #[test]
    fn an_advertised_depth_above_the_limit_is_refused_before_dispatch() {
        let request = Req::new(Op::Ping);
        let mut plain = Vec::new();
        put_request(&mut plain, &request).unwrap();
        plain[10] = MAX_RECURSION_DEPTH + 1;
        assert!(Req::decode(&plain[4..]).is_err());
    }

    #[test]
    fn parsing_more_routines_than_bytes_is_refused() {
        let mut reader = StructReader::new(&[7], 1);
        assert_eq!(reader.number_u8().unwrap(), 7);
        assert!(reader.number_u8().is_err());
    }

    #[test]
    fn a_shape_is_one_self_contained_wire_argument() {
        let shape = crate::info::Shape { streams: 3, given: 2, extents: 7, entries: 9, next: 11 };
        let compact = Req { id: 7, op: Op::BatchInfo, args: shape.args() };
        assert_eq!(compact.args.len(), 1, "the shape is one self-contained value");
        assert_eq!(round_trip_request(&compact).args, compact.args);
    }

    fn encoded_len(request: &Req) -> usize {
        let mut plain = Vec::new();
        put_request(&mut plain, request).unwrap();
        plain.len()
    }

    #[test]
    fn resp_round_trip() {
        let response = Resp::error(Status::Unimplemented, "no FE yet");
        let mut plain = Vec::new();
        put_response(&mut plain, &response).unwrap();
        assert_eq!(Resp::decode(&plain[4..]).unwrap(), response);
    }

    /// The shape both an FE and an EN read, so neither counts positions.
    #[test]
    fn an_append_survives_the_wire() {
        let block = AppendBlock {
            sn: SnId::new(3),
            extent: ExtentId::new(17),
            created: TimeSec::new(1_000_000),
            framing: crate::framing::Framing::Lf,
            crc: 0xae8b_1486_0a79_9888,
            holds: crate::block::Holds {
                compressed: crate::offset::ECoff::new(8 + crate::block::APPEND_OVERHEAD as u32),
                logical: crate::offset::ELoff::new(10),
                records: crate::offset::ERoff::new(2),
            },
            logical_offset: None,
            instances: vec![
                (EnId::new(2), DiskId::new(0)),
                (EnId::new(3), DiskId::new(0)),
                (EnId::new(0), DiskId::new(0)),
                (EnId::new(1), DiskId::new(0)),
            ],
            bytes: b"squeezed".to_vec(),
        };
        let en = EnId::new(2);
        let disk = DiskId::new(1);
        let (read_en, read_disk, read_block) =
            AppendBlock::of(&block.request(en, disk, Op::AppendExtent)).unwrap();
        assert_eq!(read_en, en);
        assert_eq!(read_disk, disk);
        assert_eq!(read_block, block);

        // On the way to a secondary: a logical offset, and no instance list.
        let onward = AppendBlock {
            instances: Vec::new(),
            logical_offset: Some(ELoff::new(512)),
            ..block.clone()
        };
        let (read_en, read_disk, read_block) =
            AppendBlock::of(&onward.request(en, disk, Op::ApplyAppend)).unwrap();
        assert_eq!(read_en, en);
        assert_eq!(read_disk, disk);
        assert_eq!(read_block, onward);

        let holds = crate::block::Holds { compressed: crate::offset::ECoff::ZERO, ..block.holds };
        let bare = AppendBlock { holds, ..block };
        let (_, _, read) = AppendBlock::of(&bare.request(en, disk, Op::AppendExtent)).unwrap();
        assert_eq!(read.holds.compressed, crate::offset::ECoff::ZERO);
        assert_eq!(
            read.holds.logical,
            crate::offset::ELoff::new(10),
            "what it holds survives either way"
        );
    }

    #[test]
    fn an_append_of_the_wrong_shape_is_refused() {
        assert!(AppendBlock::of(&Req::new(Op::AppendExtent).number(1)).is_err());
        // Everything but the bytes.
        let headless = Req::new(Op::AppendExtent)
            .number(0) // en
            .number(0) // disk
            .number(0) // sn
            .number(1) // extent
            .number(2) // created
            .number(10) // logical
            .number(1) // records
            .flag(false) // open_record
            .number(0) // crc
            .flag(false) // has_offset
            .number(0); // offset
        assert!(AppendBlock::of(&headless).is_err());
    }

    #[test]
    fn ops_are_unique() {
        let mut codes: Vec<u16> = Op::ALL.iter().map(|op| op.code()).collect();
        codes.sort();
        let count = codes.len();
        codes.dedup();
        assert_eq!(codes.len(), count);

        let mut names: Vec<&str> = Op::ALL.iter().map(|op| op.as_str()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), count);
    }

    #[test]
    fn truncated_frame_errs() {
        let mut plain = Vec::new();
        put_request(&mut plain, &Req::new(Op::Ping).text("hello")).unwrap();
        plain.truncate(plain.len() - 2);
        assert!(Req::decode(&plain[4..]).is_err());
    }

    #[test]
    fn no_trailing_bytes() {
        let mut plain = Vec::new();
        frame_into(&mut plain, |buf| {
            Req::new(Op::Ping).encode(buf);
            buf.push(0xff);
        })
        .unwrap();
        assert!(Req::decode(&plain[4..]).is_err());
    }
}
