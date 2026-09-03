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
use crate::ids::{DiskId, EnId, ExtentId, SnId};
use crate::offset::ELoff;
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
// codes. Any code shipped in a client-visible protocol is permanent: retired codes become
// tombstones and must never be reused for a different format or meaning, because an old client
// could silently misread the new value as the old one. Only an identifier that was never released
// outside the upgraded software may be reused.
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

    // Schema identifiers share the Op namespace with commands.
    Range = 45, "range";
    Shape = 46, "shape";
    Batch = 47, "batch";
    Described = 48, "described";
    StreamRow = 49, "stream-row";
    ExtentRow = 50, "extent-row";
    Piece = 51, "piece";
    AppendBlock = 52, "append-block";
    Expecting = 53, "expecting";
    // Response payloads have their own identifiers so their formats can evolve
    // independently of the requests that caused them. This also leaves the door
    // open to batching both directions in one stream without making request and
    // response formats ambiguous.
    FilledResponse = 54, "filled-response";
    CoveredResponse = 55, "covered-response";
    ExtentResponse = 56, "extent-response";
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
    /// A schema boundary; these use Op codes but are not commands.
    Tag(Op),
    /// A self-contained serialized structure. The structure owns the payload
    /// format; the wire only supplies its schema tag and length.
    Struct(Op, Vec<u8>),
    Text(String),
    Number(u64),
    Flag(bool),
    Bytes(Vec<u8>),
}

pub fn tagged<'a>(args: &'a [Arg], expected: Op, what: &str) -> Result<&'a [Arg]> {
    let [Arg::Tag(actual), rest @ ..] = args else {
        return Err(Error::new(format!("{what} is missing its schema tag")));
    };
    if *actual != expected {
        return Err(Error::new(format!("expected {expected}, got {actual}")));
    }
    Ok(rest)
}

/// Build the self-contained payload of one tagged structure.
pub struct StructWriter {
    bytes: Vec<u8>,
}

impl StructWriter {
    pub fn new() -> StructWriter {
        StructWriter { bytes: Vec::new() }
    }

    pub fn number(&mut self, value: u64) {
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

    /// Mark the start of a nested, command-owned structure. Its parser consumes
    /// the positional fields directly from the enclosing payload; there is no
    /// nested length because an unknown format must fail rather than be skipped.
    pub fn tag(&mut self, op: Op) {
        self.bytes.extend(op.code().to_le_bytes());
    }

    pub fn finish(self, op: Op) -> Arg {
        Arg::Struct(op, self.bytes)
    }
}

/// Read the self-contained payload of one tagged structure.
pub struct StructReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> StructReader<'a> {
    pub fn new(bytes: &'a [u8]) -> StructReader<'a> {
        StructReader { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(count).ok_or_else(|| Error::new("length overflow"))?;
        if end > self.bytes.len() {
            return Err(Error::new("structure payload ends mid-value"));
        }
        let value = &self.bytes[self.at..end];
        self.at = end;
        Ok(value)
    }

    pub fn number(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
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
    let [Arg::Struct(actual, bytes), rest @ ..] = args else {
        return Err(Error::new(format!("{what} is not self-contained")));
    };
    if *actual != expected {
        return Err(Error::new(format!("expected {expected}, got {actual}")));
    }
    Ok((StructReader::new(bytes), rest))
}

impl std::fmt::Display for Arg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Arg::Text(text) => f.write_str(text),
            Arg::Number(n) => write!(f, "{n}"),
            Arg::Flag(b) => write!(f, "{b}"),
            Arg::Bytes(bytes) => write!(f, "{} bytes", bytes.len()),
            Arg::Tag(op) => write!(f, "<{op}>"),
            Arg::Struct(op, bytes) => write!(f, "<{op}: {} bytes>", bytes.len()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Chosen by the caller, echoed by the response. Unique among that caller's
    /// outstanding requests, and nothing more.
    pub id: u32,
    pub op: Op,
    pub args: Vec<Arg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub id: u32,
    pub status: Status,
    pub args: Vec<Arg>,
}

/// Short names for the two wire directions. They are aliases, not new types.
pub type Req = Request;
pub type Resp = Response;

impl Request {
    pub fn new(op: Op) -> Request {
        Request { id: 0, op, args: Vec::new() }
    }

    pub fn id(mut self, id: u32) -> Request {
        self.id = id;
        self
    }

    pub fn text(mut self, text: impl Into<String>) -> Request {
        self.args.push(Arg::Text(text.into()));
        self
    }

    pub fn number(mut self, n: u64) -> Request {
        self.args.push(Arg::Number(n));
        self
    }

    pub fn flag(mut self, b: bool) -> Request {
        self.args.push(Arg::Flag(b));
        self
    }

    pub fn bytes(mut self, bytes: Vec<u8>) -> Request {
        self.args.push(Arg::Bytes(bytes));
        self
    }

    /// Put this command's self-contained payload in the request. The request's
    /// `Op` is the command tag; the payload is positional and parsed by the
    /// command, not by generic wire code.
    pub fn structure(mut self, payload: StructWriter) -> Request {
        self.args.push(payload.finish(self.op));
        self
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self.id.to_le_bytes());
        out.extend(self.op.code().to_le_bytes());
        if let [Arg::Struct(op, bytes)] = self.args.as_slice()
            && *op == self.op
        {
            out.extend(COMMAND_PAYLOAD.to_le_bytes());
            out.extend(bytes);
        } else {
            encode_args(&self.args, out);
        }
    }

    fn decode(payload: &[u8]) -> Result<Request> {
        let mut reader = Cursor::new(payload);
        let id = reader.u32()?;
        let op = Op::from_code(reader.u16()?)?;
        let count = reader.u16()?;
        let args = if count == COMMAND_PAYLOAD {
            vec![Arg::Struct(op, reader.remaining().to_vec())]
        } else {
            reader.args_with_count(count)?
        };
        Ok(Request { id, op, args })
    }
}

impl Response {
    pub fn ok() -> Response {
        Response { id: 0, status: Status::Ok, args: Vec::new() }
    }

    pub fn text(text: impl Into<String>) -> Response {
        Response { id: 0, status: Status::Ok, args: vec![Arg::Text(text.into())] }
    }

    pub fn error(status: Status, message: impl Into<String>) -> Response {
        Response { id: 0, status, args: vec![Arg::Text(message.into())] }
    }

    pub fn flag(mut self, b: bool) -> Response {
        self.args.push(Arg::Flag(b));
        self
    }

    pub fn bytes(mut self, bytes: Vec<u8>) -> Response {
        self.args.push(Arg::Bytes(bytes));
        self
    }

    /// Answer this request, whenever the answer is ready.
    pub fn to(mut self, request: &Request) -> Response {
        self.id = request.id;
        self
    }

    /// The single text argument, if the response is one message.
    pub fn message(&self) -> Option<&str> {
        match self.args.first() {
            Some(Arg::Text(text)) => Some(text),
            _ => None,
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self.id.to_le_bytes());
        out.extend((self.status as u16).to_le_bytes());
        encode_args(&self.args, out);
    }

    fn decode(payload: &[u8]) -> Result<Response> {
        let mut reader = Cursor::new(payload);
        let id = reader.u32()?;
        let status = Status::from_code(reader.u16()?)?;
        Ok(Response { id, status, args: reader.args()? })
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

impl AppendBlock {
    pub fn request(&self, en: EnId, disk: DiskId, op: Op) -> Request {
        let mut request = Request::new(op);
        let mut payload = StructWriter::new();
        let (kind, width) = self.framing.wire();
        payload.number(en.number());
        payload.number(disk.number());
        payload.number(self.sn.number());
        payload.number(self.extent.number());
        payload.number(self.created.number());
        payload.number(kind);
        payload.number(width);
        payload.number(self.crc);
        payload.number(self.holds.compressed.number());
        payload.number(self.holds.logical.number());
        payload.number(self.holds.records.number());
        payload.flag(self.logical_offset.is_some());
        payload.number(self.logical_offset.map(|o| o.number()).unwrap_or(0));
        payload.number(self.instances.len() as u64);
        for (en, disk) in &self.instances {
            payload.number(en.number());
            payload.number(disk.number());
        }
        payload.bytes(&self.bytes);
        request.args.push(payload.finish(op));
        request
    }

    pub fn of(request: &Request) -> Result<(EnId, DiskId, AppendBlock)> {
        let (mut payload, rest) = structure(&request.args, request.op, "an append request")?;
        if !rest.is_empty() {
            return Err(Error::new("an append block has arguments after its payload"));
        }
        let en = EnId::of_number(payload.number()?)?;
        let disk = DiskId::of_number(payload.number()?)?;
        let sn = SnId::of_number(payload.number()?)?;
        let extent = ExtentId::of_number(payload.number()?)?;
        let created = TimeSec::new(payload.number()?);
        let kind = payload.number()?;
        let width = payload.number()?;
        let crc = payload.number()?;
        let holds = crate::block::Holds {
            compressed: crate::offset::ECoff::of_number(payload.number()?)?,
            logical: crate::offset::ELoff::of_number(payload.number()?)?,
            records: crate::offset::ERoff::of_number(payload.number()?)?,
        };
        let has_logical = payload.flag()?;
        let logical = payload.number()?;
        let count = payload.number()? as usize;
        let mut instances = Vec::with_capacity(count);
        for _ in 0..count {
            instances
                .push((EnId::of_number(payload.number()?)?, DiskId::of_number(payload.number()?)?));
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
            Arg::Tag(op) => {
                out.push(4);
                out.extend(op.code().to_le_bytes());
            }
            Arg::Struct(op, bytes) => {
                out.push(5);
                out.extend(op.code().to_le_bytes());
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Cursor<'a> {
        Cursor { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
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

    fn args(&mut self) -> Result<Vec<Arg>> {
        let count = self.u16()?;
        self.args_with_count(count)
    }

    fn args_with_count(&mut self, count: u16) -> Result<Vec<Arg>> {
        let mut args = Vec::with_capacity(count as usize);
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
                4 => Arg::Tag(Op::from_code(self.u16()?)?),
                5 => {
                    let op = Op::from_code(self.u16()?)?;
                    let len = self.u32()? as usize;
                    Arg::Struct(op, self.take(len)?.to_vec())
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

    pub fn request(&mut self) -> Result<Request> {
        Request::decode(self.next()?)
    }

    pub fn response(&mut self) -> Result<Response> {
        Response::decode(self.next()?)
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
pub fn put_request(plain: &mut Vec<u8>, request: &Request) -> Result<()> {
    frame_into(plain, |buf| request.encode(buf))
}

pub fn put_response(plain: &mut Vec<u8>, response: &Response) -> Result<()> {
    frame_into(plain, |buf| response.encode(buf))
}

/// The two halves of one connection, for a caller that uses both from one thread.
pub struct Link {
    pub reader: Reader,
    pub wire: Wire,
    pub plain: Vec<u8>,
}

impl Link {
    pub fn call(&mut self, request: &Request) -> Result<Response> {
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

    fn round_trip_request(request: &Request) -> Request {
        let mut plain = Vec::new();
        put_request(&mut plain, request).unwrap();
        let length = u32::from_le_bytes(plain[..4].try_into().unwrap()) as usize;
        assert_eq!(length + 4, plain.len());
        Request::decode(&plain[4..]).unwrap()
    }

    #[test]
    fn req_round_trip() {
        let request = Request::new(Op::AppendRecord)
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
        payload.number(7);
        payload.number(42);
        let request = Request::new(Op::SnMasterKey).structure(payload);
        assert_eq!(round_trip_request(&request), request);
        assert!(
            encoded_len(&request)
                < encoded_len(&Request::new(Op::SnMasterKey).number(7).number(42))
        );
    }

    #[test]
    fn a_shape_is_one_self_contained_wire_argument() {
        let shape = crate::info::Shape { streams: 3, given: 2, extents: 7, entries: 9, next: 11 };
        let compact = Request { id: 7, op: Op::BatchInfo, args: shape.args() };
        let expanded = Request {
            id: 7,
            op: Op::BatchInfo,
            args: vec![
                Arg::Tag(Op::Shape),
                Arg::Number(shape.streams),
                Arg::Number(shape.given),
                Arg::Number(shape.extents),
                Arg::Number(shape.entries),
                Arg::Number(shape.next),
            ],
        };
        assert!(encoded_len(&compact) < encoded_len(&expanded));
        assert_eq!(round_trip_request(&compact).args, compact.args);
    }

    fn encoded_len(request: &Request) -> usize {
        let mut plain = Vec::new();
        put_request(&mut plain, request).unwrap();
        plain.len()
    }

    #[test]
    fn resp_round_trip() {
        let response = Response::error(Status::Unimplemented, "no FE yet");
        let mut plain = Vec::new();
        put_response(&mut plain, &response).unwrap();
        assert_eq!(Response::decode(&plain[4..]).unwrap(), response);
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
        assert!(AppendBlock::of(&Request::new(Op::AppendExtent).number(1)).is_err());
        // Everything but the bytes.
        let headless = Request::new(Op::AppendExtent)
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
        put_request(&mut plain, &Request::new(Op::Ping).text("hello")).unwrap();
        plain.truncate(plain.len() - 2);
        assert!(Request::decode(&plain[4..]).is_err());
    }

    #[test]
    fn no_trailing_bytes() {
        let mut plain = Vec::new();
        frame_into(&mut plain, |buf| {
            Request::new(Op::Ping).encode(buf);
            buf.push(0xff);
        })
        .unwrap();
        assert!(Request::decode(&plain[4..]).is_err());
    }
}
