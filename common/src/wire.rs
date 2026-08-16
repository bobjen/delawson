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
//! Frames are written into a buffer and flushed deliberately, so several messages
//! that happen to be pending on one connection leave in one syscall. That is the
//! only batching there is: no message means "do these N things", the transport
//! simply carries whatever has piled up. Nagle stays off, because the decision of
//! when to send is made here and not by the kernel.

use crate::{Context, Error, Result};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

/// Refuse anything larger rather than allocating what a peer asks for.
pub const MAX_FRAME_BYTES: usize = 1 << 20;

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

ops! {
    // Control, answered by every machine type.
    Ping = 0x0001, "ping";
    Status = 0x0002, "status";
    Shutdown = 0x0003, "shutdown";

    // KS: the registry, then the keys.
    Register = 0x0110, "register";
    Lookup = 0x0111, "lookup";
    Registry = 0x0112, "registry";

    Unlock = 0x0101, "unlock";
    SnMasterKey = 0x0102, "sn-master-key";
    RotateMasterKey = 0x0103, "rotate-master-key";

    // Namespace.
    MakeDirectory = 0x0201, "mkdir";
    RemoveDirectory = 0x0202, "rmdir";
    List = 0x0203, "ls";
    Stat = 0x0204, "stat";

    // Objects.
    CreateObject = 0x0301, "create";
    DeleteObject = 0x0302, "delete";
    UndeleteObject = 0x0303, "undelete";
    SetExpiry = 0x0304, "set-expiry";
    CloneObject = 0x0305, "clone";
    ConcatObjects = 0x0306, "concat";

    // Records.
    AppendRecord = 0x0401, "append";
    InsertRecord = 0x0402, "insert";
    UpdateRecord = 0x0403, "update";
    DeleteRecord = 0x0404, "delete-record";
    ReadRecord = 0x0405, "read";
    Scan = 0x0406, "scan";
    Count = 0x0407, "count";
    TimeRange = 0x0408, "time-range";
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
        };
        f.write_str(text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arg {
    Text(String),
    Number(u64),
    Flag(bool),
    Bytes(Vec<u8>),
}

impl std::fmt::Display for Arg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Arg::Text(text) => f.write_str(text),
            Arg::Number(n) => write!(f, "{n}"),
            Arg::Flag(b) => write!(f, "{b}"),
            Arg::Bytes(bytes) => write!(f, "{} bytes", bytes.len()),
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

    fn encode(&self) -> Vec<u8> {
        let mut out = self.id.to_le_bytes().to_vec();
        out.extend(self.op.code().to_le_bytes());
        encode_args(&self.args, &mut out);
        out
    }

    fn decode(payload: &[u8]) -> Result<Request> {
        let mut reader = Cursor::new(payload);
        let id = reader.u32()?;
        let op = Op::from_code(reader.u16()?)?;
        Ok(Request { id, op, args: reader.args()? })
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

    fn encode(&self) -> Vec<u8> {
        let mut out = self.id.to_le_bytes().to_vec();
        out.extend((self.status as u16).to_le_bytes());
        encode_args(&self.args, &mut out);
        out
    }

    fn decode(payload: &[u8]) -> Result<Response> {
        let mut reader = Cursor::new(payload);
        let id = reader.u32()?;
        let status = Status::from_code(reader.u16()?)?;
        Ok(Response { id, status, args: reader.args()? })
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

    fn args(&mut self) -> Result<Vec<Arg>> {
        let count = self.u16()?;
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

/// Frames go into the buffer; the caller decides when to flush. Length and
/// payload are one `write_all`, so a frame is never split across flushes.
fn write_frame(out: &mut impl Write, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(Error::new(format!("frame of {} bytes is too large", payload.len())));
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend((payload.len() as u32).to_le_bytes());
    frame.extend(payload);
    out.write_all(&frame)?;
    Ok(())
}

fn read_frame(input: &mut impl Read) -> Result<Vec<u8>> {
    let mut length = [0u8; 4];
    input.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(Error::new(format!("peer announced a {length}-byte frame")));
    }
    let mut payload = vec![0u8; length];
    input.read_exact(&mut payload)?;
    Ok(payload)
}

pub fn write_request(out: &mut impl Write, request: &Request) -> Result<()> {
    write_frame(out, &request.encode())
}

/// A buffered connection. `more` says whether bytes are already in hand, which is
/// how both sides know they may keep piling up work instead of flushing.
pub struct Chan {
    pub input: BufReader<TcpStream>,
    pub output: BufWriter<TcpStream>,
}

impl Chan {
    pub fn new(stream: TcpStream) -> Result<Chan> {
        stream.set_nodelay(true)?;
        Ok(Chan {
            input: BufReader::with_capacity(BUF_BYTES, stream.try_clone()?),
            output: BufWriter::with_capacity(BUF_BYTES, stream),
        })
    }

    /// Bytes already read from the socket and not yet parsed.
    pub fn more(&self) -> bool {
        !self.input.buffer().is_empty()
    }

    pub fn flush(&mut self) -> Result<()> {
        self.output.flush()?;
        Ok(())
    }
}

pub fn read_request(input: &mut impl Read) -> Result<Request> {
    Request::decode(&read_frame(input)?)
}

pub fn write_response(out: &mut impl Write, response: &Response) -> Result<()> {
    write_frame(out, &response.encode())
}

pub fn read_response(input: &mut impl Read) -> Result<Response> {
    Response::decode(&read_frame(input)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_round_trip() {
        let request = Request::new(Op::AppendRecord)
            .id(7)
            .text("/bob/notes")
            .number(17)
            .flag(true)
            .bytes(vec![1, 2, 3]);
        let mut buffer = Vec::new();
        write_request(&mut buffer, &request).unwrap();
        assert_eq!(read_request(&mut buffer.as_slice()).unwrap(), request);
    }

    #[test]
    fn resp_round_trip() {
        let response = Response::error(Status::Unimplemented, "no FE yet");
        let mut buffer = Vec::new();
        write_response(&mut buffer, &response).unwrap();
        assert_eq!(read_response(&mut buffer.as_slice()).unwrap(), response);
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
        let mut buffer = Vec::new();
        write_request(&mut buffer, &Request::new(Op::Ping).text("hello")).unwrap();
        buffer.truncate(buffer.len() - 2);
        // The length prefix promises more than the payload delivers.
        assert!(read_request(&mut buffer.as_slice()).is_err());
    }

    #[test]
    fn no_trailing_bytes() {
        let mut payload = 0u32.to_le_bytes().to_vec();
        payload.extend(Op::Ping.code().to_le_bytes());
        encode_args(&[], &mut payload);
        payload.push(0xff);
        let mut buffer = Vec::new();
        write_frame(&mut buffer, &payload).unwrap();
        assert!(read_request(&mut buffer.as_slice()).is_err());
    }
}
