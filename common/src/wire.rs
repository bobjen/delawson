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

#[path = "buffer.rs"]
mod buffer;
#[path = "payload.rs"]
mod payload;
#[cfg(test)]
#[path = "segmented_tests.rs"]
mod segmented_tests;
pub use payload::{MessageBytes, MessagePayload};
pub fn configure_message_pool(config: crate::config::BufferPoolConfig) -> Result<()> {
    buffer::shared_pool(config).map(|_| ())
}

/// Return the process-wide pool used by segmented buffers.
pub fn message_pool() -> Result<&'static pool::ChunkPool> {
    buffer::shared_pool(buffer::pool_config())
}

pub use buffer::{FrameBuffer, Message, MessageBuffer};

use crate::config::Messaging;
use crate::ids::{DiskId, DnId, EnId, ExtentId, ObjectId, RangeId, SnId, StreamId};
use crate::offset::{ECoff, ELoff, ERoff, OCoff, OEoff, OLoff, ORoff, SCoff, SLoff, SRoff};
use crate::seal::{self, Seal};
use crate::time::TimeSec;
use crate::{Context, Error, Result};
use random::Rng;
use std::io::{BufRead, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// Refuse a message larger than this rather than allocating what a peer asks for.
/// Large enough for one 32 MiB logical append plus its per-piece wire metadata.
/// Append payloads still contain independently compressed pieces so EN never
/// has to read a compressed block larger than its 1 MiB read unit.
pub const MAX_FRAME_BYTES: usize = 64 << 20;

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
pub const MAX_SEGMENT_BYTES: usize = 64 << 20;

// Experimental: this may not be necessary at all. It's a premature
// optimization, but avoiding copies of large Raft messages may be useful later.
const RAFT_KEEP: u32 = 16;

/// Every request has one payload. Its Op is the first field of that payload;
/// the request header contains only the correlation ID and depth limit.

macro_rules! ops {
    ($($name:ident = $code:expr, $text:expr $(, $traits:expr)?;)*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[repr(u16)]
        pub enum Op { $($name = $code,)* }

        impl Op {
            pub const GROUP: u8 = 1;
            pub const ALL: &'static [Op] = &[$(Op::$name,)*];

            pub fn code(self) -> u16 { self as u16 }

            pub fn traits(self) -> u8 {
                match self { $(Op::$name => 0 $(| $traits)?,)* }
            }

            pub fn is_group(self) -> bool { self.traits() & Self::GROUP != 0 }

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

    // SN, from a DN: an explicit list of full stream names to create.
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
    WriteAppend = 35, "write-append";
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
    SnKeyResp = 66, "sn-key-response";
    ExtentCoveredResp = 67, "extent-covered-response";
    ObjectCoveredResp = 68, "object-covered-response";
    // Copy two sealed streams' extent references into an already-created destination stream.
    ConcatStreams = 69, "concat-streams";
    // Explicitly close the current object tail, including its partially filled extent.
    ForceSealExtent = 70, "force-seal-extent";
    SealStream = 71, "seal-stream";
    SealObject = 72, "seal-object";
    ObjectPieceResp = 73, "object-piece-response";
    AssignPeers = 74, "assign-peers";
    SoftDeleteObject = 75, "soft-delete";
    SoftDeleteStream = 76, "soft-delete-stream";
    DeleteStream = 77, "delete-stream";
    SoftDeleteExtentInstance = 78, "soft-delete-extent-instance";
    DeleteExtentInstance = 79, "delete-extent-instance";
    ReadSoftDeleted = 80, "read-soft-deleted";
    ByteSeekResp = 81, "byte-seek-response";
    Raft = 82, "raft";
    RaftMembers = 83, "raft-members";
    KsKey = 84, "ks-key-action";
    RaftIdentity = 85, "raftidentity";
    RaftList = 86, "raftlist";
    RaftOptional = 87, "raftoptional";
    RaftPair = 88, "raftpair";
    RaftSet = 89, "raftset";
    RaftMap = 90, "raftmap";
    RaftState = 91, "raftstate";
    RaftMember = 92, "raftmember";
    RaftMembership = 93, "raftmembership";
    RaftRetirement = 94, "raftretirement";
    RaftEntry = 95, "raftentry";
    RaftSnapshot = 96, "raftsnapshot";
    RaftAnswer = 97, "raftanswer";
    RaftFrame = 98, "raftframe";
    RaftCheckpoint = 99, "raftcheckpoint";
    RaftCommittedJournal = 100, "raftcommittedjournal";
    RaftLocalIdentity = 101, "raftlocalidentity";
    RaftRecoveryPlan = 102, "raftrecoveryplan";
    RaftPacket = 103, "raftpacket";
    RaftConsensusReply = 104, "raftconsensusreply";
    RaftCommandNoop = 105, "raftcommandnoop";
    RaftCommandClient = 106, "raftcommandclient";
    RaftCommandMembers = 107, "raftcommandmembers";
    RaftCommandCheckpointStart = 108, "raftcommandcheckpointstart";
    RaftRecordContacts = 109, "raftrecordcontacts";
    RaftRecordRetired = 110, "raftrecordretired";
    RaftRecordBootstrap = 111, "raftrecordbootstrap";
    RaftRecordCarry = 112, "raftrecordcarry";
    RaftRecordHard = 113, "raftrecordhard";
    RaftRecordEntries = 114, "raftrecordentries";
    RaftRecordCommit = 115, "raftrecordcommit";
    RaftRecordInstall = 116, "raftrecordinstall";
    RaftRpcVote = 117, "raftrpcvote";
    RaftRpcAppend = 118, "raftrpcappend";
    RaftRpcInstall = 119, "raftrpcinstall";
    RaftRpcCheckpoint = 120, "raftrpccheckpoint";
    RaftMessageIdentity = 121, "raftmessageidentity";
    RaftMessageBootstrap = 122, "raftmessagebootstrap";
    RaftMessageConsensus = 123, "raftmessageconsensus";
    RaftMessageCheckpoint = 124, "raftmessagecheckpoint";
    RaftMessageJournal = 125, "raftmessagejournal";
    RaftMessageCheckpointStart = 126, "raftmessagecheckpointstart";
    RaftMessageClient = 127, "raftmessageclient";
    RaftMessageForward = 128, "raftmessageforward";
    RaftSenderSlot = 129, "raftsenderslot";
    RaftSenderApplicant = 130, "raftsenderapplicant";

    KsEnvelope = 131, "ks-encrypted-action";
    RaftMessageJournalTip = 132, "raft-journal-tip";
    BatchInfoResp = 133, "batch-info-response", Op::GROUP;
    ListExtentsResp = 134, "list-extents-response", Op::GROUP;
    ObjectInfoResp = 135, "object-info-response", Op::GROUP;
    CompoundResp = 136, "compound-response", Op::GROUP;
    CompoundReq = 137, "compound-request", Op::GROUP;
    SnCreate = 138, "sn-create-action";
    SnConcat = 139, "sn-concat-action";
    SnOpen = 140, "sn-open-action";
    SnSeal = 141, "sn-seal-action";
    SnSealStream = 142, "sn-seal-stream-action";
    SnDelete = 143, "sn-delete-action";
    SnDeleteDone = 144, "sn-delete-done-action";
    DnCreate = 145, "dn-create-action";
    CheckpointEnd = 146, "checkpoint-end";
    RaftPrimary = 147, "raft-primary";
    KsCkFooter = 148, "ks-checkpoint-footer";
    KsCkRange = 150, "ks-checkpoint-range";
    KsCkPeers = 151, "ks-checkpoint-peers";
    KsCkKeys = 152, "ks-checkpoint-keys";
    KsCkSeal = 153, "ks-checkpoint-sealed";
    KsCkEpoch = 154, "ks-checkpoint-epoch";
    KsCkPeer = 155, "ks-checkpoint-peer";
    KsCkKeyChunk = 156, "ks-checkpoint-key-chunk";
    RaftRecordMaxScn = 157, "raft-record-max-scn";
    RaftCkStatus = 158, "raft-checkpoint-status";
    RaftCheckpointFile = 159, "raft-checkpoint-file";
    RaftCkRead = 160, "raft-checkpoint-read";
    RaftCkFile = 161, "raft-checkpoint-file-request";
    SnCkFooter = 162, "sn-ck-footer";
    SnCkRange = 163, "sn-ck-range";
    SnCkEns = 164, "sn-ck-ens";
    SnCkEn = 165, "sn-ck-en";
    SnCkExtents = 166, "sn-ck-extents";
    SnCkExtent = 167, "sn-ck-extent";
    SnCkStreams = 168, "sn-ck-streams";
    SnCkStream = 169, "sn-ck-stream";
    SnCkRefs = 170, "sn-ck-refs";
    SnCkRetired = 171, "sn-ck-retired";
    SnCkPending = 172, "sn-ck-pending";
    SnCkTask = 173, "sn-ck-task";
    SnCkExtExtra = 174, "sn-ck-ext-extra";
    SnCkDiskRef = 175, "sn-ck-disk-ref";
    SnDiskRef = 176, "sn-disk-ref";
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

/// A tagged wire value. Its Op selects the parser and the payload layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arg {
    pub op: Op,
    payload: ArgPayload,
    pub depth: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ArgPayload {
    // This is only a storage union. The Op's GROUP trait selects the layout.
    // A byte payload therefore does not carry a separate Vec header.
    Bytes(MessagePayload),
    Parts(Vec<Arg>),
}

impl Arg {
    pub fn new(op: Op, bytes: MessagePayload, depth: u8) -> Self {
        assert!(!op.is_group(), "a structure needs a structure Op");
        Self { op, payload: ArgPayload::Bytes(bytes), depth }
    }

    pub fn group(op: Op, parts: Vec<Arg>) -> Self {
        assert!(op.is_group(), "a group needs a group Op");
        Self { op, payload: ArgPayload::Parts(parts), depth: 1 }
    }

    pub fn is_group(&self) -> bool {
        self.op.is_group()
    }

    pub fn bytes(&self) -> &MessagePayload {
        assert!(!self.op.is_group());
        let ArgPayload::Bytes(bytes) = &self.payload else { unreachable!("Op/storage mismatch") };
        bytes
    }

    pub fn bytes_mut(&mut self) -> &mut MessagePayload {
        assert!(!self.op.is_group());
        let ArgPayload::Bytes(bytes) = &mut self.payload else {
            unreachable!("Op/storage mismatch")
        };
        bytes
    }

    pub fn into_bytes(self) -> MessagePayload {
        assert!(!self.op.is_group());
        let ArgPayload::Bytes(bytes) = self.payload else { unreachable!("Op/storage mismatch") };
        bytes
    }

    pub fn parts(&self) -> &[Arg] {
        assert!(self.op.is_group());
        let ArgPayload::Parts(parts) = &self.payload else { unreachable!("Op/storage mismatch") };
        parts
    }

    pub fn into_parts(self) -> Vec<Arg> {
        assert!(self.op.is_group());
        let ArgPayload::Parts(parts) = self.payload else { unreachable!("Op/storage mismatch") };
        parts
    }
}

/// Build the self-contained payload of one tagged structure.
pub struct StructWriter {
    bytes: MessageBytes,
    depth: u8,
    maximum_depth: u8,
}

impl StructWriter {
    pub fn new() -> StructWriter {
        StructWriter { bytes: MessageBytes::new(), depth: 1, maximum_depth: 1 }
    }

    /// Reserve space for a payload whose serialized size is known or estimated.
    pub fn with_capacity(capacity: usize) -> StructWriter {
        StructWriter { bytes: MessageBytes::with_capacity(capacity), depth: 1, maximum_depth: 1 }
    }

    pub fn number_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn number_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn number_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn number_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn dn_id(&mut self, value: DnId) {
        self.number_u64(value.get());
    }

    pub fn sn_id(&mut self, value: SnId) {
        self.number_u64(value.get());
    }

    pub fn en_id(&mut self, value: EnId) {
        self.number_u64(value.get());
    }

    pub fn disk_id(&mut self, value: DiskId) {
        self.number_u16(value.get());
    }

    pub fn range_id(&mut self, value: RangeId) {
        self.number_u64(value.get());
    }

    pub fn object_id(&mut self, value: ObjectId) {
        self.number_u32(value.get());
    }

    pub fn stream_id(&mut self, value: StreamId) {
        self.number_u32(value.get());
    }

    pub fn extent_id(&mut self, value: ExtentId) {
        self.number_u32(value.get());
    }

    pub fn scn(&mut self, value: crate::scn::Scn) {
        self.number_u64(value.number());
    }

    pub fn time_sec(&mut self, value: TimeSec) {
        self.number_u64(value.number());
    }

    pub fn eroff(&mut self, value: ERoff) {
        self.number_u32(value.get());
    }

    pub fn eloff(&mut self, value: ELoff) {
        self.number_u32(value.get());
    }

    pub fn ecoff(&mut self, value: ECoff) {
        self.number_u32(value.get());
    }

    pub fn oeoff(&mut self, value: OEoff) {
        self.number_u64(value.get());
    }

    pub fn oroff(&mut self, value: ORoff) {
        self.number_u64(value.get());
    }

    pub fn sroff(&mut self, value: SRoff) {
        self.number_u64(value.get());
    }

    pub fn oloff(&mut self, value: OLoff) {
        self.number_u64(value.get());
    }

    pub fn sloff(&mut self, value: SLoff) {
        self.number_u64(value.get());
    }

    pub fn ocoff(&mut self, value: OCoff) {
        self.number_u64(value.get());
    }

    pub fn scoff(&mut self, value: SCoff) {
        self.number_u64(value.get());
    }

    pub fn flag(&mut self, value: bool) {
        self.bytes.push(value as u8);
    }

    pub fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.bytes.extend_from_slice(value);
    }

    /// Embed an already serialized payload without flattening its leaves.
    pub fn message_bytes(&mut self, value: &MessageBytes) {
        self.number_u32(value.len() as u32);
        self.raw_message_bytes(value);
    }

    /// Copy serialized bytes whose length is supplied by the enclosing schema.
    pub fn raw_message_bytes(&mut self, value: &MessageBytes) {
        if let Err(e) = value.write_to(&mut self.bytes) {
            self.bytes.fail(e);
        }
    }

    /// Append tagged bytes whose nesting starts at the current writer depth.
    pub fn raw_message_depth(&mut self, value: &MessageBytes, depth: u8) {
        self.maximum_depth =
            self.maximum_depth.max(self.depth.saturating_add(depth.saturating_sub(1)));
        self.raw_message_bytes(value);
    }

    pub fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    /// Write a nested tagged structure while tracking its depth in the enclosing value.
    pub fn nested(&mut self, op: Op, write: impl FnOnce(&mut StructWriter)) {
        self.bytes.extend_from_slice(&op.code().to_le_bytes());
        self.depth = self.depth.checked_add(1).expect("serialization nesting depth overflow");
        self.maximum_depth = self.maximum_depth.max(self.depth);
        write(self);
        self.depth -= 1;
    }

    pub fn finish(self, op: Op) -> Arg {
        Arg::new(op, self.bytes.into(), self.maximum_depth)
    }
}

/// Read the self-contained payload of one tagged structure.
enum StructureInput<'a> {
    Slice(&'a [u8]),
    Payload(&'a MessagePayload),
    Message(&'a MessageBytes),
}
impl<'a> StructureInput<'a> {
    fn len(&self) -> usize {
        match self {
            Self::Slice(b) => b.len(),
            Self::Payload(b) => b.len(),
            Self::Message(b) => b.len(),
        }
    }
    fn chunk(&self, at: usize) -> &'a [u8] {
        match self {
            Self::Slice(b) => &b[at..],
            Self::Payload(b) => b.chunk(at),
            Self::Message(b) => b.chunk(at),
        }
    }
}
pub struct StructReader<'a> {
    bytes: StructureInput<'a>,
    current: &'a [u8],
    at: usize,
    depth: u8,
    maximum_depth: u8,
    depth_capture: Option<u8>,
    parses_remaining: isize,
}

impl<'a> StructReader<'a> {
    fn check_input(&self) -> Result<()> {
        match &self.bytes {
            StructureInput::Payload(bytes) => bytes.check()?,
            StructureInput::Message(bytes) => bytes.check()?,
            StructureInput::Slice(_) => {}
        }
        Ok(())
    }

    pub fn new(bytes: &'a [u8], maximum_depth: u8) -> StructReader<'a> {
        StructReader {
            bytes: StructureInput::Slice(bytes),
            current: bytes,
            at: 0,
            depth: 1,
            maximum_depth,
            depth_capture: None,
            parses_remaining: isize::try_from(bytes.len()).expect("slice length fits isize"),
        }
    }

    pub fn from_payload(bytes: &'a MessagePayload, maximum_depth: u8) -> StructReader<'a> {
        if let Some(bytes) = bytes.contiguous() {
            return Self::new(bytes, maximum_depth);
        }
        StructReader {
            bytes: StructureInput::Payload(bytes),
            current: bytes.chunk(0),
            at: 0,
            depth: 1,
            maximum_depth,
            depth_capture: None,
            parses_remaining: isize::try_from(bytes.len()).expect("payload length fits isize"),
        }
    }

    pub fn from_message(bytes: &'a MessageBytes, maximum_depth: u8) -> StructReader<'a> {
        StructReader {
            bytes: StructureInput::Message(bytes),
            current: bytes.chunk(0),
            at: 0,
            depth: 1,
            maximum_depth,
            depth_capture: None,
            parses_remaining: isize::try_from(bytes.len()).expect("message length fits isize"),
        }
    }

    /// Capture the maximum nesting depth reached by one decoder call.
    pub fn capture_depth<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<(T, u8)> {
        debug_assert!(self.depth_capture.is_none());
        self.depth_capture = Some(self.depth);
        let result = read(self);
        let maximum = self.depth_capture.take().unwrap_or(self.depth);
        result.map(|value| (value, maximum.saturating_sub(self.depth).saturating_add(1)))
    }

    /// Copy a previously read byte range without moving the reader.
    pub fn raw_range(&self, start: usize, length: usize) -> Result<MessageBytes> {
        if start.checked_add(length).is_none_or(|end| end > self.bytes.len()) {
            return Err(Error::new("structure range outside payload"));
        }
        self.check_input()?;
        let mut out = MessageBytes::new();
        let mut at = start;
        let end = start + length;
        while at < end {
            let chunk = self.bytes.chunk(at);
            if chunk.is_empty() {
                return Err(Error::new("structure range ends mid-segment"));
            }
            let n = chunk.len().min(end - at);
            out.extend_from_slice(&chunk[..n]);
            at += n;
        }
        out.check()?;
        Ok(out)
    }

    pub fn position(&self) -> usize {
        self.at
    }

    // Charge structural attempts, including failures, without charging leaf reads.
    fn count_parse(&mut self) -> Result<()> {
        self.parses_remaining = self.parses_remaining.saturating_sub(1);
        if self.parses_remaining < 0 {
            return Err(Error::new("too many parse routines for structure size"));
        }
        Ok(())
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        if self.current.len() >= N {
            let value = self.current[..N].try_into().unwrap();
            self.current = &self.current[N..];
            self.at += N;
            Ok(value)
        } else {
            let mut value = [0; N];
            self.read_into(&mut value)?;
            Ok(value)
        }
    }

    fn read_into(&mut self, mut out: &mut [u8]) -> Result<()> {
        self.check_input()?;
        if out.len() > self.remaining() {
            return Err(Error::new("structure payload ends mid-value"));
        }
        while !out.is_empty() {
            if self.current.is_empty() {
                self.current = self.bytes.chunk(self.at);
            }
            let n = self.current.len().min(out.len());
            out[..n].copy_from_slice(&self.current[..n]);
            self.current = &self.current[n..];
            self.at += n;
            out = &mut out[n..];
        }
        Ok(())
    }

    pub fn number_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_array::<8>()?))
    }

    pub fn number_u8(&mut self) -> Result<u8> {
        Ok(self.read_array::<1>()?[0])
    }

    pub fn number_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }

    pub fn number_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    pub fn dn_id(&mut self) -> Result<DnId> {
        Ok(DnId::new(self.number_u64()?))
    }

    pub fn sn_id(&mut self) -> Result<SnId> {
        Ok(SnId::new(self.number_u64()?))
    }

    pub fn en_id(&mut self) -> Result<EnId> {
        Ok(EnId::new(self.number_u64()?))
    }

    pub fn disk_id(&mut self) -> Result<DiskId> {
        Ok(DiskId::new(self.number_u16()?))
    }

    pub fn range_id(&mut self) -> Result<RangeId> {
        Ok(RangeId::new(self.number_u64()?))
    }

    pub fn object_id(&mut self) -> Result<ObjectId> {
        Ok(ObjectId::new(self.number_u32()?))
    }

    pub fn stream_id(&mut self) -> Result<StreamId> {
        Ok(StreamId::new(self.number_u32()?))
    }

    pub fn extent_id(&mut self) -> Result<ExtentId> {
        Ok(ExtentId::new(self.number_u32()?))
    }

    pub fn scn(&mut self) -> Result<crate::scn::Scn> {
        Ok(crate::scn::Scn::new(self.number_u64()?))
    }

    pub fn time_sec(&mut self) -> Result<TimeSec> {
        Ok(TimeSec::new(self.number_u64()?))
    }

    pub fn eroff(&mut self) -> Result<ERoff> {
        Ok(ERoff::new(self.number_u32()?))
    }

    pub fn eloff(&mut self) -> Result<ELoff> {
        Ok(ELoff::new(self.number_u32()?))
    }

    pub fn ecoff(&mut self) -> Result<ECoff> {
        Ok(ECoff::new(self.number_u32()?))
    }

    pub fn oeoff(&mut self) -> Result<OEoff> {
        Ok(OEoff::new(self.number_u64()?))
    }

    pub fn oroff(&mut self) -> Result<ORoff> {
        Ok(ORoff::new(self.number_u64()?))
    }

    pub fn sroff(&mut self) -> Result<SRoff> {
        Ok(SRoff::new(self.number_u64()?))
    }

    pub fn oloff(&mut self) -> Result<OLoff> {
        Ok(OLoff::new(self.number_u64()?))
    }

    pub fn sloff(&mut self) -> Result<SLoff> {
        Ok(SLoff::new(self.number_u64()?))
    }

    pub fn ocoff(&mut self) -> Result<OCoff> {
        Ok(OCoff::new(self.number_u64()?))
    }

    pub fn scoff(&mut self) -> Result<SCoff> {
        Ok(SCoff::new(self.number_u64()?))
    }

    /// Allocate a collection whose count came from this structure.
    pub fn checked_vec<T, N>(&self, count: N) -> Result<Vec<T>>
    where
        N: TryInto<usize>,
    {
        checked_vec(count, self.bytes.len() - self.at)
    }

    pub fn flag(&mut self) -> Result<bool> {
        Ok(self.read_array::<1>()?[0] != 0)
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = u32::from_le_bytes(self.read_array::<4>()?) as usize;
        if length > self.remaining() {
            return Err(Error::new("structure payload ends mid-value"));
        }
        let mut bytes = vec![0; length];
        self.read_into(&mut bytes)?;
        Ok(bytes)
    }

    pub fn skip_bytes(&mut self) -> Result<()> {
        let length = u32::from_le_bytes(self.read_array::<4>()?) as usize;
        if length > self.remaining() {
            return Err(Error::new("structure payload ends mid-value"));
        }
        self.check_input()?;
        let mut left = length;
        while left != 0 {
            if self.current.is_empty() {
                self.current = self.bytes.chunk(self.at);
            }
            let n = self.current.len().min(left);
            if n == 0 {
                return Err(Error::new("structure payload ends mid-value"));
            }
            self.current = &self.current[n..];
            self.at += n;
            left -= n;
        }
        Ok(())
    }

    /// Read an owned serialized field without flattening large fields.
    pub fn raw_message_bytes(&mut self, length: usize) -> Result<MessageBytes> {
        self.check_input()?;
        if length > self.remaining() {
            return Err(Error::new("structure payload ends mid-value"));
        }
        let mut out = MessageBytes::new();
        let mut left = length;
        while left != 0 {
            if self.current.is_empty() {
                self.current = self.bytes.chunk(self.at);
            }
            let n = self.current.len().min(left);
            out.write_all(&self.current[..n])?;
            self.current = &self.current[n..];
            self.at += n;
            left -= n;
        }
        Ok(out)
    }

    pub fn text(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).context("text in structure payload")
    }

    pub fn tag(&mut self, expected: Op, what: &str) -> Result<()> {
        let actual = Op::from_code(u16::from_le_bytes(self.read_array::<2>()?))?;
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
        self.count_parse()?;
        self.tag(expected, what)?;
        let depth =
            self.depth.checked_add(1).ok_or_else(|| Error::new("recursion depth overflow"))?;
        if depth > self.maximum_depth {
            return Err(Error::new("message recursion depth exceeded"));
        }
        self.depth = depth;
        if let Some(maximum) = &mut self.depth_capture {
            *maximum = (*maximum).max(depth);
        }
        let result = read(self);
        self.depth -= 1;
        result
    }

    /// Decode a tagged union while accounting for nesting depth.
    pub fn nested_any<T>(&mut self, read: impl FnOnce(Op, &mut Self) -> Result<T>) -> Result<T> {
        self.count_parse()?;
        let op = Op::from_code(self.number_u16()?)?;
        let depth =
            self.depth.checked_add(1).ok_or_else(|| Error::new("recursion depth overflow"))?;
        if depth > self.maximum_depth {
            return Err(Error::new("message recursion depth exceeded"));
        }
        self.depth = depth;
        if let Some(maximum) = &mut self.depth_capture {
            *maximum = (*maximum).max(depth);
        }
        let result = read(op, self);
        self.depth -= 1;
        result
    }
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    pub fn done(&self) -> Result<()> {
        self.check_input()?;
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
    let [arg, rest @ ..] = args else {
        return Err(Error::new(format!("{what} is not self-contained")));
    };
    if arg.op != expected {
        return Err(Error::new(format!("expected {expected}, got {}", arg.op)));
    }
    if arg.is_group() {
        return Err(Error::new(format!("{what} is not self-contained")));
    }
    arg.bytes().check()?;
    Ok((StructReader::from_payload(arg.bytes(), arg.depth), rest))
}

impl std::fmt::Display for Arg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_group() {
            write!(f, "<{}: {} arguments>", self.op, self.parts().len())
        } else {
            write!(f, "<{}: {} bytes>", self.op, self.bytes().len())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Req {
    /// Chosen by the caller, echoed by the response. Unique among that caller's
    /// outstanding requests, and nothing more.
    pub id: u32,
    pub payload: Arg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resp {
    pub id: u32,
    pub status: Status,
    pub payload: Arg,
}

impl Req {
    pub fn new(op: Op) -> Req {
        Req { id: 0, payload: StructWriter::new().finish(op) }
    }

    pub fn op(&self) -> Op {
        self.payload.op
    }

    pub fn args(&self) -> &[Arg] {
        if self.payload.is_group() {
            self.payload.parts()
        } else {
            std::slice::from_ref(&self.payload)
        }
    }

    pub fn into_args(self) -> Vec<Arg> {
        if self.payload.is_group() { self.payload.into_parts() } else { vec![self.payload] }
    }

    pub fn from_args(op: Op, args: Vec<Arg>) -> Req {
        let payload = if args.len() == 1 && args[0].op == op {
            args.into_iter().next().unwrap()
        } else {
            Arg::group(op, args)
        };
        Req { id: 0, payload }
    }

    fn write_field(mut self, write: impl FnOnce(&mut StructWriter)) -> Req {
        assert!(!self.payload.is_group(), "positional fields need a structure payload");
        let op = self.payload.op;
        let maximum_depth = self.payload.depth;
        let mut writer =
            StructWriter { bytes: self.payload.into_bytes().into(), depth: 1, maximum_depth };
        write(&mut writer);
        self.payload = writer.finish(op);
        self
    }

    pub fn id(mut self, id: u32) -> Req {
        self.id = id;
        self
    }

    pub fn text(self, text: impl Into<String>) -> Req {
        let text = text.into();
        self.write_field(|writer| writer.text(&text))
    }

    pub fn number(self, n: u64) -> Req {
        self.write_field(|writer| writer.number_u64(n))
    }

    pub fn flag(self, b: bool) -> Req {
        self.write_field(|writer| writer.flag(b))
    }

    pub fn bytes(self, bytes: impl Into<MessageBytes>) -> Req {
        let bytes = bytes.into();
        self.write_field(|writer| writer.message_bytes(&bytes))
    }

    /// Put this command's self-contained payload in the request. The request's
    /// `Op` is the command tag; the payload is positional and parsed by the
    /// command, not by generic wire code.
    pub fn structure(mut self, payload: StructWriter) -> Req {
        let op = self.op();
        assert!(!self.payload.is_group() && self.payload.bytes().len() == 0);
        self.payload = payload.finish(op);
        self
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        self.encode_to(out).expect("writing a message into Vec");
    }

    fn encode_to(&self, out: &mut impl Write) -> std::io::Result<()> {
        self.encode_with_id(self.id, out)
    }

    fn encode_with_id(&self, id: u32, out: &mut impl Write) -> std::io::Result<()> {
        out.write_all(&id.to_le_bytes())?;
        out.write_all(&[message_depth(std::slice::from_ref(&self.payload))])?;
        encode_arg(&self.payload, out)
    }

    #[cfg(test)]
    fn decode(payload: &[u8]) -> Result<Req> {
        Self::decode_from(payload, payload.len())
    }

    fn decode_from(input: impl BufRead, length: usize) -> Result<Req> {
        let mut reader = Cursor::new(input, length);
        let id = reader.u32()?;
        let maximum_depth = reader.depth()?;
        let payload = reader.arg(maximum_depth)?;
        if reader.remaining != 0 {
            return Err(Error::new("trailing bytes in frame"));
        }
        Ok(Req { id, payload })
    }
}

impl Resp {
    pub fn new(status: Status, payload: Arg) -> Resp {
        Resp { id: 0, status, payload }
    }

    pub fn id(mut self, id: u32) -> Resp {
        self.id = id;
        self
    }

    pub fn from_args(status: Status, args: Vec<Arg>) -> Resp {
        let group_op = match args.first().map(|arg| arg.op) {
            Some(Op::BatchResp) => Some(Op::BatchInfoResp),
            Some(Op::ExtentRunResp) => Some(Op::ListExtentsResp),
            Some(Op::ShapeResp) => Some(Op::ObjectInfoResp),
            _ => None,
        };
        let payload = match (group_op, args.len()) {
            (Some(op), _) => Arg::group(op, args),
            (None, 0) => StructWriter::new().finish(Op::EmptyResp),
            (None, 1) => args.into_iter().next().unwrap(),
            (None, _) => Arg::group(Op::CompoundResp, args),
        };
        Resp { id: 0, status, payload }
    }

    pub fn group(status: Status, op: Op, args: Vec<Arg>) -> Resp {
        Resp { id: 0, status, payload: Arg::group(op, args) }
    }

    pub fn args(&self) -> &[Arg] {
        if self.payload.is_group() {
            self.payload.parts()
        } else {
            std::slice::from_ref(&self.payload)
        }
    }

    pub fn into_args(self) -> Vec<Arg> {
        if self.payload.is_group() { self.payload.into_parts() } else { vec![self.payload] }
    }

    pub fn ok() -> Resp {
        Resp { id: 0, status: Status::Ok, payload: StructWriter::new().finish(Op::EmptyResp) }
    }

    pub fn text(text: impl Into<String>) -> Resp {
        let text = text.into();
        let mut payload = StructWriter::new();
        payload.text(&text);
        Resp { id: 0, status: Status::Ok, payload: payload.finish(Op::TextResp) }
    }

    pub fn error(status: Status, message: impl Into<String>) -> Resp {
        let message = message.into();
        let mut payload = StructWriter::new();
        payload.text(&message);
        Resp { id: 0, status, payload: payload.finish(Op::TextResp) }
    }

    pub fn flag(b: bool) -> Resp {
        let mut payload = StructWriter::new();
        payload.flag(b);
        Resp { id: 0, status: Status::Ok, payload: payload.finish(Op::FlagResp) }
    }

    pub fn bytes(bytes: impl Into<MessageBytes>) -> Resp {
        let mut payload = StructWriter::new();
        payload.message_bytes(&bytes.into());
        Resp { id: 0, status: Status::Ok, payload: payload.finish(Op::BytesResp) }
    }

    pub fn number(status: Status, number: u64) -> Resp {
        let mut payload = StructWriter::new();
        payload.number_u64(number);
        Resp { id: 0, status, payload: payload.finish(Op::NumberResp) }
    }

    /// Answer this request, whenever the answer is ready.
    pub fn to(mut self, request: &Req) -> Resp {
        self.id = request.id;
        self
    }

    /// The single text argument, if the response is one message.
    pub fn message(&self) -> Option<String> {
        let (mut payload, rest) = structure(self.args(), Op::TextResp, "text response").ok()?;
        if !rest.is_empty() {
            return None;
        }
        let text = payload.text().ok()?;
        payload.done().ok()?;
        Some(text)
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        self.encode_to(out).expect("writing a message into Vec");
    }

    fn encode_to(&self, out: &mut impl Write) -> std::io::Result<()> {
        encode_header(self.id, self.status as u16, std::slice::from_ref(&self.payload), out)?;
        // The status is not a schema tag: each response structure carries the
        // Op it was serialized with, independently of the request's Op.
        encode_arg(&self.payload, out)
    }

    #[cfg(test)]
    fn decode(payload: &[u8]) -> Result<Resp> {
        Self::decode_from(payload, payload.len())
    }

    fn decode_from(input: impl BufRead, length: usize) -> Result<Resp> {
        let mut reader = Cursor::new(input, length);
        let (id, status, maximum_depth) = reader.header(Status::from_code)?;
        let payload = reader.arg(maximum_depth)?;
        if reader.remaining != 0 {
            return Err(Error::new("trailing bytes in frame"));
        }
        Ok(Resp { id, status, payload })
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
    /// Optional expected extent record count; required when writing a secondary instance.
    pub record_offset: Option<ERoff>,
    /// The **(EnId, DiskId)** pairs holding it, primary first.
    pub instances: Vec<(EnId, DiskId)>,
    /// The block as stored: compressed, unless compressing it did not help.
    pub bytes: Vec<u8>,
    /// Compressed pieces belonging to this atomic append. Empty means the
    /// legacy single-piece fields above are being used.
    pub pieces: Vec<AppendPiece>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendPiece {
    pub holds: crate::block::Holds,
    pub crc: u64,
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
        payload.sn_id(self.sn);
        payload.eloff(self.logical);
        payload.eroff(self.first_record);
        payload.eroff(self.records);
        payload.extent_id(self.extent);
        payload.time_sec(self.created);
        payload.eloff(self.length);
        payload.finish(Op::ReceiptResp)
    }

    pub fn read(args: &[Arg]) -> Result<EnReceipt> {
        let (mut payload, rest) = structure(args, Op::ReceiptResp, "an EN append receipt")?;
        let receipt = EnReceipt {
            sn: payload.sn_id()?,
            logical: payload.eloff()?,
            first_record: payload.eroff()?,
            records: payload.eroff()?,
            extent: payload.extent_id()?,
            created: payload.time_sec()?,
            length: payload.eloff()?,
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

/// The SN key answer. `next` is supplied whenever a planned key change is
/// known, so an EN can cache the current key only until that exact second.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnKey {
    pub current: [u8; 32],
    pub next: Option<(TimeSec, [u8; 32])>,
}

impl SnKey {
    pub fn write(self) -> Arg {
        let mut payload = StructWriter::new();
        payload.bytes(&self.current);
        payload.flag(self.next.is_some());
        if let Some((at, key)) = self.next {
            payload.time_sec(at);
            payload.bytes(&key);
        }
        payload.finish(Op::SnKeyResp)
    }

    pub fn read(args: &[Arg]) -> Result<SnKey> {
        let (mut payload, rest) = structure(args, Op::SnKeyResp, "an SN key response")?;
        let current: [u8; 32] = payload
            .bytes()?
            .try_into()
            .map_err(|_| Error::new("SN returned a current key of the wrong size"))?;
        let next = if payload.flag()? {
            let at = payload.time_sec()?;
            let key: [u8; 32] = payload
                .bytes()?
                .try_into()
                .map_err(|_| Error::new("SN returned a next key of the wrong size"))?;
            Some((at, key))
        } else {
            None
        };
        if !rest.is_empty() {
            return Err(Error::new("an SN key response has trailing arguments"));
        }
        payload.done()?;
        Ok(SnKey { current, next })
    }
}

impl Receipt {
    pub fn write(self) -> Arg {
        let mut payload = StructWriter::new();
        payload.stream_id(self.stream);
        payload.sroff(self.record);
        payload.oroff(self.object_record);
        payload.eroff(self.records);
        payload.extent_id(self.extent);
        payload.time_sec(self.created);
        payload.eloff(self.offset);
        payload.eloff(self.length);
        payload.finish(Op::ReceiptResp)
    }

    pub fn read(args: &[Arg]) -> Result<Receipt> {
        let (mut payload, rest) = structure(args, Op::ReceiptResp, "an append receipt")?;
        let receipt = Receipt {
            stream: payload.stream_id()?,
            record: payload.sroff()?,
            object_record: payload.oroff()?,
            records: payload.eroff()?,
            extent: payload.extent_id()?,
            created: payload.time_sec()?,
            offset: payload.eloff()?,
            length: payload.eloff()?,
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
        let request = Req::new(op);
        let mut payload = StructWriter::new();
        let (kind, width) = self.framing.wire();
        payload.en_id(en);
        payload.disk_id(disk);
        payload.sn_id(self.sn);
        payload.extent_id(self.extent);
        payload.time_sec(self.created);
        payload.number_u8(kind);
        payload.number_u32(width);
        payload.number_u64(self.crc);
        payload.ecoff(self.holds.compressed);
        payload.eloff(self.holds.logical);
        payload.eroff(self.holds.records);
        payload.flag(self.logical_offset.is_some());
        payload.eloff(self.logical_offset.unwrap_or(ELoff::ZERO));
        payload.flag(self.record_offset.is_some());
        payload.eroff(self.record_offset.unwrap_or(ERoff::ZERO));
        payload.number_u32(self.instances.len() as u32);
        for (en, disk) in &self.instances {
            payload.en_id(*en);
            payload.disk_id(*disk);
        }
        let pieces = if self.pieces.is_empty() {
            vec![AppendPiece { holds: self.holds, crc: self.crc, bytes: self.bytes.clone() }]
        } else {
            self.pieces.clone()
        };
        payload.number_u32(pieces.len() as u32);
        for piece in pieces {
            piece.holds.write(&mut payload);
            payload.number_u64(piece.crc);
            payload.bytes(&piece.bytes);
        }
        request.structure(payload)
    }

    pub fn of(request: &Req) -> Result<(EnId, DiskId, AppendBlock)> {
        let (mut payload, rest) = structure(&request.args(), request.op(), "an append request")?;
        if !rest.is_empty() {
            return Err(Error::new("an append block has arguments after its payload"));
        }
        let en = payload.en_id()?;
        let disk = payload.disk_id()?;
        let sn = payload.sn_id()?;
        let extent = payload.extent_id()?;
        let created = payload.time_sec()?;
        let kind = payload.number_u8()?;
        let width = payload.number_u32()?;
        let crc = payload.number_u64()?;
        let _legacy_holds = crate::block::Holds {
            compressed: payload.ecoff()?,
            logical: payload.eloff()?,
            records: payload.eroff()?,
        };
        let has_logical = payload.flag()?;
        let logical = payload.eloff()?;
        let has_record = payload.flag()?;
        let record = payload.eroff()?;
        let count = payload.number_u32()?;
        let mut instances = payload.checked_vec(count)?;
        for _ in 0..count {
            instances.push((payload.en_id()?, payload.disk_id()?));
        }
        let count = payload.number_u32()?;
        if count == 0 {
            return Err(Error::new("an append block has no pieces"));
        }
        let mut pieces = Vec::with_capacity(count as usize);
        for _ in 0..count {
            pieces.push(AppendPiece {
                holds: crate::block::Holds::read(&mut payload)?,
                crc: payload.number_u64()?,
                bytes: payload.bytes()?,
            });
        }
        payload.done()?;
        let mut pieces_iter = pieces.iter();
        let first = pieces_iter.next().expect("piece count checked").holds;
        let holds = pieces_iter.try_fold(first, |total, piece| -> Result<crate::block::Holds> {
            Ok(crate::block::Holds {
                compressed: total.compressed.plus(piece.holds.compressed)?,
                logical: total.logical.plus(piece.holds.logical)?,
                records: total.records.plus(piece.holds.records)?,
            })
        })?;
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
                logical_offset: has_logical.then_some(logical),
                record_offset: has_record.then_some(record),
                bytes: pieces.first().map_or_else(Vec::new, |piece| piece.bytes.clone()),
                pieces,
            },
        ))
    }
}

fn encode_header(id: u32, kind: u16, args: &[Arg], out: &mut impl Write) -> std::io::Result<()> {
    out.write_all(&id.to_le_bytes())?;
    out.write_all(&kind.to_le_bytes())?;
    out.write_all(&[message_depth(args)])
}

fn encode_args(args: &[Arg], out: &mut impl Write) -> std::io::Result<()> {
    out.write_all(&(args.len() as u16).to_le_bytes())?;
    for arg in args {
        encode_arg(arg, out)?;
    }
    Ok(())
}

fn encode_arg(arg: &Arg, out: &mut impl Write) -> std::io::Result<()> {
    out.write_all(&arg.op.code().to_le_bytes())?;
    if arg.is_group() {
        encode_args(arg.parts(), out)?;
    } else {
        out.write_all(&(arg.bytes().len() as u32).to_le_bytes())?;
        arg.bytes().write_to(out)?;
    }
    Ok(())
}

fn message_depth(args: &[Arg]) -> u8 {
    args.iter().fold(1, |depth, arg| {
        if arg.is_group() {
            depth.max(message_depth(arg.parts()).saturating_add(1))
        } else {
            depth.max(arg.depth)
        }
    })
}

struct Cursor<R> {
    input: R,
    remaining: usize,
}
impl<R: BufRead> Cursor<R> {
    fn new(input: R, remaining: usize) -> Self {
        Self { input, remaining }
    }

    fn header<T>(&mut self, decode_kind: impl FnOnce(u16) -> Result<T>) -> Result<(u32, T, u8)> {
        let id = self.u32()?;
        let kind = decode_kind(self.u16()?)?;
        let maximum_depth = self.depth()?;
        Ok((id, kind, maximum_depth))
    }

    fn depth(&mut self) -> Result<u8> {
        let maximum_depth = self.u8()?;
        if maximum_depth == 0 {
            return Err(Error::new("message recursion depth is zero"));
        }
        if maximum_depth > MAX_RECURSION_DEPTH {
            return Err(Error::new("message recursion depth exceeds limit"));
        }
        Ok(maximum_depth)
    }

    fn take(&mut self, n: usize) -> Result<MessagePayload> {
        if n > self.remaining {
            return Err(Error::new("frame ends mid-value"));
        }
        if n <= buffer::pool_config().small_message_bytes {
            let mut bytes = Vec::with_capacity(n);
            let mut left = n;
            while left != 0 {
                let chunk = self.input.fill_buf()?;
                if chunk.is_empty() {
                    return Err(Error::new("frame ends mid-value"));
                }
                let count = chunk.len().min(left);
                bytes.extend_from_slice(&chunk[..count]);
                self.input.consume(count);
                left -= count;
            }
            self.remaining -= n;
            return Ok(MessagePayload::from_small(bytes));
        }
        let mut bytes = MessageBytes::new();
        let mut left = n;
        while left != 0 {
            let chunk = self.input.fill_buf()?;
            if chunk.is_empty() {
                return Err(Error::new("frame ends mid-value"));
            }
            let count = chunk.len().min(left);
            bytes.write_all(&chunk[..count])?;
            self.input.consume(count);
            left -= count;
        }
        self.remaining -= n;
        Ok(bytes.into())
    }

    #[inline]
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let current = self.input.fill_buf()?;
        if current.len().min(self.remaining) >= N {
            let value = current[..N].try_into().unwrap();
            self.input.consume(N);
            self.remaining -= N;
            Ok(value)
        } else {
            if N > self.remaining {
                return Err(Error::new("frame ends mid-value"));
            }
            let mut value = [0; N];
            self.input.read_exact(&mut value)?;
            self.remaining -= N;
            Ok(value)
        }
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn arg(&mut self, maximum_depth: u8) -> Result<Arg> {
        let op = Op::from_code(self.u16()?)?;
        if op.is_group() {
            if maximum_depth <= 1 {
                return Err(Error::new("message recursion depth exceeds limit"));
            }
            let count = self.u16()?;
            let mut args = checked_vec(count, self.remaining)?;
            for _ in 0..count {
                args.push(self.arg(maximum_depth - 1)?);
            }
            Ok(Arg::group(op, args))
        } else {
            let len = self.u32()? as usize;
            Ok(Arg::new(op, self.take(len)?, maximum_depth))
        }
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
    stream: crate::machine::Stream,
    seal: Seal,
    sealed: MessageBuffer,
    keep: u32,
}

impl Wire {
    /// Create an empty batch sharing this connection's process-wide pool.
    pub fn new_buffer(&self) -> MessageBuffer {
        MessageBuffer::with_retained(self.sealed.bytes.pool(), self.keep)
    }

    /// Seal and send a batch, splitting large batches at message boundaries.
    /// Success resets the batch, retaining only its bounded reusable capacity.
    pub fn send(&mut self, plain: &mut MessageBuffer) -> Result<()> {
        let mut at = plain.bytes.start();
        while at < plain.bytes.end() {
            let end = segment_end(plain, at)?;
            self.sealed.clear();
            self.seal.seal_segmented(&mut plain.bytes, at..end, &mut self.sealed.bytes)?;
            self.sealed.write_to(&mut self.stream)?;
            self.sealed.clear();
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
    stream: crate::machine::Stream,
    seal: Seal,
    open: MessageBuffer,
    sealed: MessageBuffer,
    sealed_scratch: Vec<u8>,
    at: u64,
    /// When the segment now being read arrived. A batch arrives at one moment, so
    /// this is one clock read for however many messages it holds — and it is the
    /// honest start for a duration, being when the bytes got here rather than
    /// when a thread got to them.
    arrived: Instant,
    may_be_raft: bool,
}

impl Reader {
    pub fn peer_key(&self) -> Option<&[u8]> {
        self.stream.peer_key.as_deref()
    }
    pub fn more(&self) -> bool {
        self.at < self.open.bytes.end()
    }

    pub fn arrived(&self) -> Instant {
        self.arrived
    }

    /// The next message, reading and opening a segment if none is in hand.
    fn next(&mut self) -> Result<pool::SegmentedReader<'_, 'static>> {
        if !self.more() {
            self.fill()?;
        }
        self.open.bytes.discard_before(self.at).context("discard consumed messages")?;
        let mut reader = self.open.bytes.reader();
        let length = reader.read_u32_le()? as u64;
        if length > MAX_FRAME_BYTES as u64 {
            return Err(Error::new("frame exceeds maximum size"));
        }
        if length > reader.remaining() {
            return Err(Error::new("a message longer than its segment"));
        }
        let start = self.at + 4;
        self.at = start + length;
        Ok(self.open.bytes.reader_range(start..self.at)?)
    }

    fn fill(&mut self) -> Result<()> {
        let mut header = [0u8; seal::HEADER];
        self.stream.read_exact(&mut header)?;
        self.arrived = Instant::now();
        let length = u32::from_le_bytes(header[..4].try_into().expect("length")) as usize;
        if length > MAX_SEGMENT_BYTES + seal::OVERHEAD {
            return Err(Error::new(format!("peer announced a {length}-byte segment")));
        }
        self.sealed.receive(&mut self.stream, length)?;
        self.open.clear();
        self.at = 0;
        let first = self.sealed.bytes.chunk(0).unwrap_or(&[]);
        let mut open =
            |sealed: &[u8]| self.seal.open_segmented(&header, sealed, &mut self.open.bytes);
        let result = if first.len() >= length {
            open(&first[..length])
        } else if pool::SegmentedBuffer::alias_supported() {
            self.sealed
                .bytes
                .with_contiguous_read(0..length as u64, open)
                .context("sealed input alias")
                .and_then(|result| result)
        } else {
            self.sealed.copy_to(0..length as u64, &mut self.sealed_scratch)?;
            open(&self.sealed_scratch)
        };
        self.sealed.clear();
        if result.is_err() {
            self.open.clear();
        }
        result
    }

    pub fn request(&mut self) -> Result<Req> {
        let reader = self.next()?;
        let length = reader.remaining() as usize;
        let result = Req::decode_from(reader, length);
        if self.may_be_raft {
            if result.as_ref().is_ok_and(|request| request.op() == Op::Raft) {
                self.open.set_retained_leaves(RAFT_KEEP);
                self.sealed.set_retained_leaves(RAFT_KEEP);
            }
            // The first request identifies this accepted connection's purpose.
            self.may_be_raft = false;
        }
        self.discard_consumed("discard consumed request")?;
        result
    }

    pub fn response(&mut self) -> Result<Resp> {
        self.decode_next("discard consumed response", |reader, length| {
            Resp::decode_from(reader, length)
        })
    }

    fn decode_next<T>(
        &mut self,
        what: &str,
        decode: impl FnOnce(pool::SegmentedReader<'_, 'static>, usize) -> Result<T>,
    ) -> Result<T> {
        let reader = self.next()?;
        let length = reader.remaining() as usize;
        let result = decode(reader, length);
        self.discard_consumed(what)?;
        result
    }

    fn discard_consumed(&mut self, what: &str) -> Result<()> {
        if self.at == self.open.bytes.end() {
            // The whole batch has been consumed. Reset to offset zero before
            // pruning so its retained first leaf needs no tree walk.
            self.open.clear();
            self.at = 0;
        } else {
            self.open.bytes.discard_before(self.at).context(what)?;
        }
        Ok(())
    }
}

/// How much of a batch fits in one segment, cut at a message boundary. At least
/// one message always goes, since a message cannot exceed a segment.
fn segment_end(plain: &MessageBuffer, start: u64) -> Result<u64> {
    segment_end_with_limit(plain, start, MAX_SEGMENT_BYTES as u64)
}
fn segment_end_with_limit(plain: &MessageBuffer, start: u64, limit: u64) -> Result<u64> {
    let mut at = start;
    while at < plain.bytes.end() {
        let mut reader = plain.bytes.reader_range(at..plain.bytes.end())?;
        let length = reader.read_u32_le()? as u64;
        if length > MAX_FRAME_BYTES as u64 {
            return Err(Error::new("frame exceeds maximum size"));
        }
        let next = at.checked_add(4 + length).ok_or_else(|| Error::new("frame offset overflow"))?;
        if next > plain.bytes.end() {
            return Err(Error::new("a message longer than its batch"));
        }
        if next - start > limit && at > start {
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
    filling: Mutex<MessageBuffer>,
    ready: Condvar,
    open: AtomicBool,
}

impl Outbox {
    /// Start a sender on `wire`, and return the box to append to.
    pub fn start(wire: Wire) -> Arc<Outbox> {
        let outbox = Arc::new(Outbox {
            filling: Mutex::new(wire.new_buffer()),
            ready: Condvar::new(),
            open: AtomicBool::new(true),
        });
        let sending = Arc::clone(&outbox);
        std::thread::spawn(move || sending.run(wire));
        outbox
    }

    /// Append a preframed message, copying it under the lock.
    pub fn put(&self, framed: &MessageBytes) -> Result<()> {
        let mut filling = self.filling.lock().expect("outbox");
        let woke = filling.is_empty();
        filling.append_payload(framed)?;
        drop(filling);
        // Waking is only needed when the sender has nothing else to find.
        if woke {
            self.ready.notify_one();
        }
        Ok(())
    }

    /// Encode directly into the filling buffer, avoiding a frame staging copy.
    /// The Outbox lock stays held for serialization.
    pub(crate) fn put_request(&self, request: &Req, id: u32) -> Result<()> {
        let mut filling = self.filling.lock().expect("outbox");
        let woke = filling.is_empty();
        put_request_with_id(&mut *filling, request, id)?;
        drop(filling);
        if woke {
            self.ready.notify_one();
        }
        Ok(())
    }

    pub fn close(&self) {
        self.open.store(false, Ordering::SeqCst);
        self.ready.notify_one();
    }

    fn run(&self, mut wire: Wire) {
        let mut batch = wire.new_buffer();
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
pub fn put_request(plain: &mut impl FrameBuffer, request: &Req) -> Result<()> {
    plain.append_message(request)
}

struct ReqWithId<'a> {
    request: &'a Req,
    id: u32,
}

impl Message for ReqWithId<'_> {
    fn encode_into(&self, out: &mut impl Write) -> std::io::Result<()> {
        self.request.encode_with_id(self.id, out)
    }
}

pub(crate) fn put_request_with_id(
    plain: &mut impl FrameBuffer,
    request: &Req,
    id: u32,
) -> Result<()> {
    plain.append_message(&ReqWithId { request, id })
}

pub fn put_response(plain: &mut impl FrameBuffer, response: &Resp) -> Result<()> {
    plain.append_message(response)
}

/// The two halves of one connection, for a caller that uses both from one thread.
pub struct Link {
    pub reader: Reader,
    pub wire: Wire,
    pub plain: MessageBuffer,
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
    connect_with_keep(stream, config, 1)
}

pub(crate) fn connect_peer(stream: TcpStream, config: &Messaging, raft: bool) -> Result<Link> {
    connect_with_keep(stream, config, if raft { RAFT_KEEP } else { 1 })
}

fn connect_with_keep(stream: TcpStream, config: &Messaging, keep: u32) -> Result<Link> {
    let pool = buffer::shared_pool(config.buffer_pool)?;
    stream.set_nodelay(true)?;
    let stream = crate::machine::Stream::new(stream, config, false)?;
    let mut prefix = [0u8; 8];
    Rng::new().fill_bytes(&mut prefix);
    let mut writing = stream.try_clone()?;
    writing.write_all(&prefix)?;
    crate::ring::connect(&mut writing, config)?;
    Ok(Link {
        reader: Reader {
            stream: stream.try_clone()?,
            seal: Seal::new(config, prefix, 1)?,
            open: MessageBuffer::with_retained(pool, keep),
            sealed: MessageBuffer::with_retained(pool, keep),
            sealed_scratch: Vec::new(),
            at: 0,
            arrived: Instant::now(),
            may_be_raft: false,
        },
        wire: Wire {
            stream: writing,
            seal: Seal::new(config, prefix, 0)?,
            sealed: MessageBuffer::with_retained(pool, keep),
            keep,
        },
        plain: MessageBuffer::with_retained(pool, keep),
    })
}

/// The accepting side takes the prefix it is given.
pub fn accept(stream: TcpStream, config: &Messaging) -> Result<(Reader, Wire)> {
    let pool = buffer::shared_pool(config.buffer_pool)?;
    stream.set_nodelay(true)?;
    let stream = crate::machine::Stream::new(stream, config, true)?;
    let mut prefix = [0u8; 8];
    let mut reading = stream.try_clone()?;
    reading.read_exact(&mut prefix)?;
    crate::ring::accept(&mut reading, config)?;
    Ok((
        Reader {
            stream: reading,
            seal: Seal::new(config, prefix, 0)?,
            open: MessageBuffer::new(pool),
            sealed: MessageBuffer::new(pool),
            sealed_scratch: Vec::new(),
            at: 0,
            arrived: Instant::now(),
            may_be_raft: config.local_ring.is_some(),
        },
        Wire {
            stream,
            seal: Seal::new(config, prefix, 1)?,
            sealed: MessageBuffer::new(pool),
            keep: 1,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn ordinary_args_do_not_carry_a_separate_parts_vec() {
        assert!(std::mem::size_of::<Arg>() <= 48);
    }

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
    fn request_id_can_be_overridden_without_cloning_payload() {
        let request = Req::new(Op::AppendRecord).id(7).text("payload");
        let mut plain = Vec::new();
        put_request_with_id(&mut plain, &request, 29).unwrap();
        let decoded = Req::decode(&plain[4..]).unwrap();
        assert_eq!(decoded, request.id(29));
    }

    #[test]
    fn a_command_can_use_its_op_as_a_self_contained_payload_tag() {
        let mut payload = StructWriter::new();
        payload.number_u64(7);
        payload.number_u64(42);
        let request = Req::new(Op::SnMasterKey).structure(payload);
        assert_eq!(round_trip_request(&request), request);
        let mut plain = Vec::new();
        put_request(&mut plain, &request).unwrap();
        assert_eq!(u16::from_le_bytes(plain[9..11].try_into().unwrap()), Op::SnMasterKey.code());
        assert_eq!(
            encoded_len(&request),
            encoded_len(&Req::new(Op::SnMasterKey).number(7).number(42))
        );
    }

    #[test]
    fn structure_tags_survive_a_different_request_op() {
        let mut payload = StructWriter::new();
        payload.number_u64(42);
        let mut request = Req::new(Op::Ping).id(123).structure(payload);
        request = Req::from_args(Op::CompoundReq, request.into_args()).id(123);

        let mut plain = Vec::new();
        put_request(&mut plain, &request).unwrap();
        let body = &plain[4..];
        assert_eq!(u16::from_le_bytes(body[5..7].try_into().unwrap()), Op::CompoundReq.code());
        assert_eq!(u16::from_le_bytes(body[7..9].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(body[9..11].try_into().unwrap()), Op::Ping.code());
        assert_eq!(Req::decode(body).unwrap(), request);

        let response = Resp::ok().to(&request);
        let mut plain = Vec::new();
        put_response(&mut plain, &response).unwrap();
        let body = &plain[4..];
        assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), request.id);
        assert_eq!(u16::from_le_bytes(body[7..9].try_into().unwrap()), Op::EmptyResp.code());
        assert_eq!(u32::from_le_bytes(body[9..13].try_into().unwrap()), 0);
        assert_eq!(Resp::decode(body).unwrap(), response);
    }

    #[test]
    fn compound_responses_have_one_outer_schema_tag() {
        let response = Resp::group(
            Status::Ok,
            Op::ListExtentsResp,
            vec![
                StructWriter::new().finish(Op::ExtentRunResp),
                StructWriter::new().finish(Op::NumberResp),
            ],
        )
        .id(123);
        let mut plain = Vec::new();
        put_response(&mut plain, &response).unwrap();
        let body = &plain[4..];
        assert_eq!(u16::from_le_bytes(body[7..9].try_into().unwrap()), Op::ListExtentsResp.code());
        assert_eq!(u16::from_le_bytes(body[9..11].try_into().unwrap()), 2);
        assert_eq!(Resp::decode(body).unwrap(), response);

        // The wrapper is one additional parser level, even when its children
        // are simple leaves.
        let mut malformed = body.to_vec();
        malformed[6] = 1;
        assert!(Resp::decode(&malformed).is_err());

        let batch_header = StructWriter::new().finish(Op::BatchResp);
        let batch = Resp::from_args(Status::Ok, vec![batch_header]);
        assert_eq!(batch.payload.op, Op::BatchInfoResp);
        assert!(batch.payload.is_group());
    }

    #[test]
    fn a_message_carries_the_depth_seen_while_serializing() {
        let mut payload = StructWriter::new();
        crate::range::Range::from(ORoff::new(7)).write_nested(&mut payload);
        let request = Req::new(Op::ReadRecord).structure(payload);
        let mut plain = Vec::new();
        put_request(&mut plain, &request).unwrap();
        assert_eq!(plain[8], 2);
    }

    #[test]
    fn a_nested_structure_may_not_exceed_the_message_depth() {
        let mut payload = StructWriter::new();
        crate::range::Range::from(ORoff::new(7)).write_nested(&mut payload);
        let mut arg = payload.finish(Op::ReadRecord);
        arg.depth = 1;
        let request = Req { id: 0, payload: arg };
        let (mut reader, rest) = structure(request.args(), Op::ReadRecord, "a request").unwrap();
        assert!(rest.is_empty());
        assert!(crate::range::Range::read_nested(&mut reader).is_err());
    }

    #[test]
    fn requests_and_responses_enforce_the_same_advertised_depth_limits() {
        for depth in [0, MAX_RECURSION_DEPTH + 1] {
            let mut request = Vec::new();
            put_request(&mut request, &Req::new(Op::Ping)).unwrap();
            request[8] = depth;
            assert!(Req::decode(&request[4..]).is_err());

            let mut response = Vec::new();
            put_response(&mut response, &Resp::ok()).unwrap();
            response[10] = depth;
            assert!(Resp::decode(&response[4..]).is_err());
        }
    }

    #[test]
    fn leaf_reads_only_check_bounds() {
        let mut reader = StructReader::new(&[7], 1);
        reader.parses_remaining = -1;
        assert_eq!(reader.number_u8().unwrap(), 7);
        assert_eq!(reader.number_u8().unwrap_err().to_string(), "structure payload ends mid-value");
        assert_eq!(reader.parses_remaining, -1);
        assert!(reader.read_array::<0>().unwrap().is_empty());
    }

    #[test]
    fn structural_retries_keep_their_work_charge() {
        let bytes = Op::Ping.code().to_le_bytes();
        for any in [false, true] {
            let mut reader = StructReader::new(&bytes, 2);
            let mut calls = 0;
            for _ in 0..bytes.len() {
                // Simulate speculative retry without refunding the work budget.
                reader.at = 0;
                let mut read = |_: &mut StructReader<'_>| -> Result<()> {
                    calls += 1;
                    Err(Error::new("failed attempt"))
                };
                let result = if any {
                    reader.nested_any(|_, r| read(r))
                } else {
                    reader.nested(Op::Ping, "test", read)
                };
                assert_eq!(result.unwrap_err().to_string(), "failed attempt");
                assert_eq!(reader.depth, 1);
            }
            reader.at = 0;
            let result = if any {
                reader.nested_any(|_, _| panic!("budget must stop dispatch"))
            } else {
                reader.nested(Op::Ping, "test", |_| panic!("budget must stop dispatch"))
            };
            let result: Result<()> = result;
            assert_eq!(
                result.unwrap_err().to_string(),
                "too many parse routines for structure size"
            );
            assert_eq!(calls, bytes.len());
        }
    }

    #[test]
    fn a_shape_is_one_self_contained_wire_argument() {
        let shape = crate::info::Shape {
            streams: StreamId::new(3),
            given: StreamId::new(2),
            extents: OEoff::new(7),
            entries: 9,
            next: crate::offset::ORoff::new(11),
            records: crate::offset::ORoff::NONE,
            logical: crate::offset::OLoff::NONE,
            compressed: crate::offset::OCoff::NONE,
        };
        let compact = Req::from_args(Op::CompoundReq, shape.args()).id(7);
        assert_eq!(compact.args().len(), 1, "the shape is one self-contained value");
        assert_eq!(round_trip_request(&compact).args(), compact.args());
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
            record_offset: None,
            instances: vec![
                (EnId::new(2), DiskId::new(0)),
                (EnId::new(3), DiskId::new(0)),
                (EnId::new(0), DiskId::new(0)),
                (EnId::new(1), DiskId::new(0)),
            ],
            bytes: b"squeezed".to_vec(),
            pieces: vec![AppendPiece {
                holds: crate::block::Holds {
                    compressed: crate::offset::ECoff::new(8 + crate::block::APPEND_OVERHEAD as u32),
                    logical: crate::offset::ELoff::new(10),
                    records: crate::offset::ERoff::new(2),
                },
                crc: 0xae8b_1486_0a79_9888,
                bytes: b"squeezed".to_vec(),
            }],
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
            record_offset: Some(ERoff::new(9)),
            ..block.clone()
        };
        let (read_en, read_disk, read_block) =
            AppendBlock::of(&onward.request(en, disk, Op::WriteAppend)).unwrap();
        assert_eq!(read_en, en);
        assert_eq!(read_disk, disk);
        assert_eq!(read_block, onward);

        let holds = crate::block::Holds { compressed: crate::offset::ECoff::ZERO, ..block.holds };
        let bare = AppendBlock {
            holds,
            pieces: vec![AppendPiece {
                holds,
                crc: 0xae8b_1486_0a79_9888,
                bytes: b"squeezed".to_vec(),
            }],
            ..block
        };
        let (_, _, read) = AppendBlock::of(&bare.request(en, disk, Op::AppendExtent)).unwrap();
        assert_eq!(read.holds.compressed, crate::offset::ECoff::ZERO);
        assert_eq!(
            read.holds.logical,
            crate::offset::ELoff::new(10),
            "what it holds survives either way"
        );
    }

    #[test]
    fn multiple_append_pieces_survive_the_wire() {
        let first = crate::block::Holds {
            compressed: crate::offset::ECoff::new(50),
            logical: crate::offset::ELoff::new(12),
            records: crate::offset::ERoff::new(2),
        };
        let second = crate::block::Holds {
            compressed: crate::offset::ECoff::new(60),
            logical: crate::offset::ELoff::new(16),
            records: crate::offset::ERoff::new(3),
        };
        let block = AppendBlock {
            sn: SnId::new(3),
            extent: ExtentId::new(17),
            created: TimeSec::new(1_000_000),
            framing: crate::framing::Framing::Lf,
            crc: 99,
            holds: crate::block::Holds {
                compressed: crate::offset::ECoff::new(110),
                logical: crate::offset::ELoff::new(28),
                records: crate::offset::ERoff::new(5),
            },
            logical_offset: Some(crate::offset::ELoff::new(7)),
            record_offset: Some(ERoff::new(2)),
            instances: Vec::new(),
            bytes: b"one".to_vec(),
            pieces: vec![
                AppendPiece { holds: first, crc: 11, bytes: b"one".to_vec() },
                AppendPiece { holds: second, crc: 22, bytes: b"two".to_vec() },
            ],
        };
        let (_, _, read) =
            AppendBlock::of(&block.request(EnId::new(2), DiskId::new(1), Op::WriteAppend)).unwrap();
        assert_eq!(read, block);
        assert_eq!(read.pieces.len(), 2);
        assert_eq!(read.holds.compressed, crate::offset::ECoff::new(110));
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

        let mut plain = Vec::new();
        frame_into(&mut plain, |buf| {
            Resp::ok().encode(buf);
            buf.push(0xff);
        })
        .unwrap();
        assert!(Resp::decode(&plain[4..]).is_err());
    }
}
