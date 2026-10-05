//! SN, the Stream Node: the extents of a stream and the instances of an extent.
//!
//! A stream is named by the range that owns it, so SN looks one up by
//! `(RangeId, StreamId)` and allocates neither. An extent is named here: this SN, an
//! id it hands out, and the second it was created in, which is what selects the
//! key it is encrypted under.
//!
//! **The SN names an extent and chooses its instances, and then gets out of the
//! way.** An append goes from the FE to the primary EN and is not seen here, so
//! this is asked once per extent rather than once per write.
//!
//! State is in memory. Stream and extent state share one SN-wide lock so
//! request handling and Raft apply can synchronize.
//! Deleted extent slots remain tombstones; recycling them safely is a later step.

use common::config::{SNConfig, ServiceKind};
use common::deletion::{
    Delete, Extras, Flags, SharedExtras, Target, bad, forward, read_soft_deleted,
};
use common::ids::{DiskId, EnId, ExtentId, RangeId, SnDiskId, SnId, StreamId};
use common::info::{ExtentRow, ExtentRun, Listing, StreamName, Totals};
use common::offset::{ECoff, ELoff, ERoff, SCoff, SLoff, SRoff};
use common::range::Filled;
use common::server::{self, Process, Reply, Service};
use common::time::{TimeIntSec64, TimeSec};
use common::{Arg, Op, Req, Resp, Status, StructReader, StructWriter, structure};
use common::{Error, Result};
use pool::Pool;
use raft::runtime::{Host, Settings as RaftSettings};
use std::collections::HashMap;
use std::mem::align_of;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const SERVES: &[Op] = &[
    Op::Raft,
    Op::SoftDeleteStream,
    Op::DeleteStream,
    Op::CreateStream,
    Op::ConcatStreams,
    Op::SealStream,
    Op::OpenExtent,
    Op::LocateRecord,
    Op::SealExtent,
    Op::ListExtents,
];

fn stream_name(payload: &mut StructReader<'_>) -> common::Result<StreamName> {
    Ok(StreamName {
        range: payload.range_id()?,
        stream: payload.stream_id()?,
        created: payload.time_sec()?,
    })
}

#[repr(C)]
struct Stream {
    /// Next stream in its hash bucket while live, or next pool slot while free.
    link: u32,
    /// Full key for chain lookup; live bucket chains are ordered by this name.
    name: StreamName,
    deletion: Flags,
    /// The SN that created this stream's extents, which is part of their name
    /// (§4.6.3) and what §7.3.3 requires key derivation to use — **the creating SN,
    /// never the serving one**, because a reassigned stream keeps its old extents'
    /// keys.
    ///
    /// ⚠️ **Held per stream rather than per extent, on §4.1.2's rule.** The two differ
    /// only after a reassignment, which does not exist yet, so per-extent storage
    /// would spend 4 bytes on every one of §14.1's 1.7 × 10^9 extent records — **6.8 GB
    /// of an SN's memory** — for a fact that is almost always the stream's own. When
    /// reassignment arrives, the exceptions want an override out of line, not a field
    /// inline on the rule.
    creator: SnId,
    /// This stream's sealed extents, oldest first, each with the record count it
    /// came to. In record order, because a stream only ever appends — which is what
    /// makes finding the extent holding a record a search rather than a scan.
    sealed: Vec<ExtentId>,
    /// The extent appends are landing in, once anything has asked for one.
    open: Option<ExtentId>,
    /// Records in the sealed extents, which is where the open extent's numbering
    /// starts from. **Stream-local**, which is the space above the extent's and below
    /// the object's — see `offset` for why all three are separate types.
    sealed_records: SRoff,
    /// Sealed: no more appends and no more concatenates, so the object this belongs
    /// to needs another stream for anything further.
    ///
    /// That is what sealed *means*; filling up is only why it happens here. An
    /// object is append-only, so only its last stream may take an append — every
    /// stream before it is sealed because appending to the middle of an object would
    /// renumber everything after it.
    sealed_stream: bool,
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        Self {
            link: StreamId::NONE.get(),
            name: self.name,
            deletion: self.deletion,
            creator: self.creator,
            sealed: self.sealed.clone(),
            open: self.open,
            sealed_records: self.sealed_records,
            sealed_stream: self.sealed_stream,
        }
    }
}

impl Stream {
    fn new(created: TimeSec, creator: SnId) -> Self {
        Self::with_flags(
            StreamName { range: RangeId::NONE, stream: StreamId::NONE, created },
            creator,
            Flags::live(),
        )
    }

    fn blank() -> Self {
        Self::with_flags(StreamName::default(), SnId::FIRST, Flags::default())
    }

    fn with_flags(name: StreamName, creator: SnId, deletion: Flags) -> Self {
        Self {
            link: StreamId::NONE.get(),
            name,
            deletion,
            creator,
            sealed: Vec::new(),
            open: None,
            sealed_records: SRoff::ZERO,
            sealed_stream: false,
        }
    }

    /// Initialize a new slot while preserving its pool link.
    fn set(&mut self, name: StreamName, mut stream: Stream) {
        self.name = name;
        self.deletion = stream.deletion;
        self.creator = stream.creator;
        self.sealed = std::mem::take(&mut stream.sealed);
        self.open = stream.open;
        self.sealed_records = stream.sealed_records;
        self.sealed_stream = stream.sealed_stream;
        self.deletion.set_valid();
    }

    fn clear(&mut self) {
        self.link = StreamId::NONE.get();
        self.name = StreamName::default();
        self.deletion = Flags::default();
        self.creator = SnId::FIRST;
        self.sealed = Vec::new();
        self.open = None;
        self.sealed_records = SRoff::ZERO;
        self.sealed_stream = false;
    }

    /// Each sealed extent with the stream-local number of its first record — the running
    /// total of everything before it.
    ///
    /// ⚠️ **This is the cost of not storing a position** (§17.29.1): finding an extent is
    /// a scan with a running sum rather than a search over stored keys. It was already a
    /// scan, and a stream holds a handful of extents in the expected case (§14.1.1's
    /// 128 MB against 32 MB extents), so nothing here got slower. A stream with millions
    /// of extents would want a sparse checkpoint every *k* extents, which is one `u64`
    /// per *k* rather than per extent (#107).
    fn sealed_from<'a>(
        &'a self,
        extents: &'a [Extent],
    ) -> impl Iterator<Item = (SRoff, &'a Extent)> {
        self.sealed.iter().map(|id| &extents[id.get() as usize]).scan(SRoff::ZERO, |at, sealed| {
            let first = *at;
            *at = at.plus(SRoff::from_extent(sealed.records)).expect("stream record position fits");
            Some((first, sealed))
        })
    }
}

const MAX_STREAMS: u32 = u32::MAX;
type StreamTable = pool::LinearPool<
    'static,
    Stream,
    { align_of::<Stream>() },
    { std::mem::offset_of!(Stream, link) },
>;

#[inline]
fn stream_hash(name: &StreamName) -> u64 {
    let mut words = [name.range.get(), name.stream.number(), name.created.number(), 0];
    spookyhash::short_end(&mut words);
    words[0]
}

impl pool::LinearEntry for Stream {
    type Key = StreamName;

    fn key(&self) -> Self::Key {
        self.name
    }

    fn hash(key: &Self::Key) -> u64 {
        stream_hash(key)
    }

    fn next(&self) -> u32 {
        self.link
    }

    fn set_next(&mut self, next: u32) {
        self.link = next;
    }

    fn init(&mut self, key: Self::Key, value: Self) {
        self.set(key, value);
    }

    fn clear(&mut self) {
        Stream::clear(self);
    }
}

// SAFETY: a fresh slot receives an empty, invalid Stream with its typed link sentinel.
unsafe impl pool::PoolItem for Stream {
    unsafe fn init(slot: *mut Self) {
        // SAFETY: the pool provides aligned, fresh storage for one Stream.
        unsafe { slot.write(Stream::blank()) };
    }
}

// SAFETY: the stream link is the successor index in the pool/hash chain, and
// every allocation starts with an invalid stream and no successor.
unsafe impl pool::LockedItem for Stream {
    const LINK_NONE: u32 = StreamId::NONE.get();

    unsafe fn on_alloc(slot: *mut Self) {
        // SAFETY: the pool calls this for an initialized, exclusively owned slot.
        unsafe {
            (*slot).link = StreamId::NONE.get();
            (*slot).deletion = Flags::default();
        }
    }
}

/// Where an extent is, under the name this SN gave it, and what it holds.
///
/// ⚠️ **No first record here** (Asserted, Bob, 2026-08-20), and the reason is not that it
/// was redundant: *"you can concatenate the same extent to multiple streams"*. One extent
/// has **as many first-records as there are streams referencing it**, so the field had no
/// single correct value to hold — it recorded whichever stream happened to create it and
/// was wrong for every other (§17.29.1).
#[repr(C)]
#[derive(Debug)]
struct Extent {
    deletion: Flags,
    logical: ELoff,
    compressed: ECoff,
    id: ExtentId,
    records: ERoff,
    created: TimeSec,
    disks: Vec<SnDiskId>,
}

impl Extent {
    fn blank() -> Self {
        Self {
            deletion: Flags::default(),
            logical: ELoff::ZERO,
            compressed: ECoff::ZERO,
            id: ExtentId::NONE,
            records: ERoff::ZERO,
            created: TimeSec::EPOCH,
            disks: Vec::new(),
        }
    }

    fn snapshot(&self) -> ExtentData {
        ExtentData {
            deletion: self.deletion,
            logical: self.logical,
            compressed: self.compressed,
            id: self.id,
            records: self.records,
            created: self.created,
            disks: self.disks.clone(),
        }
    }

    fn from_data(data: ExtentData) -> Self {
        Self {
            deletion: data.deletion,
            logical: data.logical,
            compressed: data.compressed,
            id: data.id,
            records: data.records,
            created: data.created,
            disks: data.disks,
        }
    }

    fn holds(&self, first: SRoff, record: SRoff) -> bool {
        let records = SRoff::from_extent(self.records);
        record >= first && record < first.plus(records).expect("stream record position fits")
    }

    fn init(&mut self) {
        self.deletion = Flags::default();
    }
}

// SAFETY: init writes a complete Extent into fresh slot storage before it is used.
unsafe impl pool::PoolItem for Extent {
    unsafe fn init(slot: *mut Self) {
        // SAFETY: the pool provides aligned, fresh storage for one Extent.
        unsafe {
            slot.write(Self::blank());
            (*slot).init();
        }
    }
}

type ExtentPool = Pool<Extent, { align_of::<Extent>() }>;

/// A stable-index table backed by typed pool slots.
struct ExtentTable {
    pool: ExtentPool,
    base: NonNull<Extent>,
    /// Initialized pool prefix, including slots currently free in the bitmap.
    /// This can lag `pool.next` while a growth range is being initialized.
    len: AtomicUsize,
    max: u32,
}

// SAFETY: base points into the owned pool's stable mapping; SN accesses the
// table under its shared state RwLock, len is atomic, and Extents are Send + Sync.
unsafe impl Send for ExtentTable {}
unsafe impl Sync for ExtentTable {}

impl ExtentTable {
    fn new(max_extents: u32) -> pool::Result<Self> {
        let pool = ExtentPool::new(max_extents, 0)?;
        let base = pool.base_ptr();
        let len = pool.issued_blocks() as usize;
        Ok(Self { pool, base, len: AtomicUsize::new(len), max: max_extents })
    }

    fn alloc_id(&self) -> pool::Result<ExtentId> {
        let block = self.pool.alloc_with_len(&self.len)?;
        let id = ExtentId::new(block.index());
        // The action owns this claim until it is applied or rolled back.
        std::mem::forget(block);
        Ok(id)
    }

    fn is_free(&self, id: ExtentId) -> pool::Result<bool> {
        self.pool.is_free(id.get())
    }

    fn reserve_id(&mut self, id: ExtentId) -> pool::Result<()> {
        let block = self.pool.reserve_with_len(id.get(), &self.len)?;
        std::mem::forget(block);
        Ok(())
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    fn release_id(&mut self, id: ExtentId) -> pool::Result<()> {
        self.pool.release(id.get())
    }

    fn is_live(&self, id: ExtentId) -> bool {
        let index = id.get() as usize;
        index < self.len() && self[index].deletion.is_valid()
    }

    fn check_vacant(&self, id: ExtentId) -> Result<()> {
        let index = id.get() as usize;
        if id.is_none() || index >= self.max as usize {
            return Err(Error::new("extent slot is out of range"));
        }
        if index < self.len() {
            // Every slot in the published prefix is initialized, including
            // slots the bitmap currently considers free.
            let flags = unsafe { (*self.base.as_ptr().add(index)).deletion };
            if flags.is_valid() {
                return Err(Error::new("extent slot is already occupied"));
            }
            if flags.bits() != 0 {
                return Err(Error::new("extent slot flags are not clear"));
            }
        }
        Ok(())
    }

    /// Write an extent into a slot whose bitmap claim is already held.
    fn insert_reserved(&mut self, wanted: ExtentId, extent: Extent) -> Result<ExtentId> {
        self.check_vacant(wanted)?;
        if self.pool.is_free(wanted.get()).map_err(|error| Error::new(error.to_string()))? {
            return Err(Error::new("reserved extent slot is free"));
        }
        let index = wanted.get() as usize;
        let pointer = unsafe { self.base.as_ptr().add(index) };
        // SAFETY: exact reservation initialized this stable pool slot and it is
        // not currently a live extent.
        if !extent.deletion.is_valid() {
            return Err(Error::new("extent action is missing its valid flag"));
        }
        unsafe {
            let slot = &mut *pointer;
            let mut extent = extent;
            extent.id = wanted;
            *slot = extent;
        }
        Ok(wanted)
    }

    #[cfg(test)]
    fn insert(&mut self, extent: Extent) -> Result<ExtentId> {
        let id = self
            .alloc_id()
            .map_err(|error| Error::new(format!("allocate SN extent slot: {error}")))?;
        self.insert_reserved(id, extent)
    }

    fn reset(&mut self) {
        *self = Self::new(self.max).expect("rebuilding SN extent table");
    }
}

impl Deref for ExtentTable {
    type Target = [Extent];

    fn deref(&self) -> &Self::Target {
        // SAFETY: len covers only initialized, stationary slots in the pool.
        unsafe { std::slice::from_raw_parts(self.base.as_ptr(), self.len()) }
    }
}

impl DerefMut for ExtentTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: SN's state write lock serializes mutation and slots are stable.
        let len = self.len();
        unsafe { std::slice::from_raw_parts_mut(self.base.as_ptr(), len) }
    }
}

impl Drop for ExtentTable {
    fn drop(&mut self) {
        for index in 0..self.pool.issued_blocks() as usize {
            // PoolItem::init initialized every issued slot.
            // Use raw pointers so the slice does not retain references to values
            // after their individual lifetimes end.
            unsafe { std::ptr::drop_in_place(self.base.as_ptr().add(index)) };
        }
    }
}
/// Persistent extent fields copied into Raft action payloads and held until replies are built.
/// Later lifecycle changes are separate Raft actions that update these flags.
#[derive(Clone, Debug)]
struct ExtentData {
    deletion: Flags,
    logical: ELoff,
    compressed: ECoff,
    /// The slot index in this SN's extent pool. The full extent name also includes
    /// the SN and creation second, but Raft members must agree on this slot.
    id: ExtentId,
    /// Records it holds, and **zero until it is sealed**, because until then the count
    /// grows with every append. `ERoff`, so it cannot be handed to anything counting a
    /// stream's or an object's records — and 32-bit because an extent seals at 32 MB
    /// compressed, so even one-byte records do not reach 4.3 × 10^9.
    records: ERoff,
    /// The second it was made in, which selects its key (§7.3.3).
    created: TimeSec,
    /// The **disks** holding its instances as indexes into the SN's disk table, primary first.
    disks: Vec<SnDiskId>,
}

struct CreateCmd {
    range: RangeId,
    first: StreamId,
    count: u64,
    created: TimeSec,
    creator: SnId,
}

struct ConcatCmd {
    destination: StreamName,
    creator: SnId,
    extents: Vec<ExtentId>,
    records: SRoff,
    require_exists: bool,
    require_absent: bool,
}

struct OpenCmd {
    name: StreamName,
    extent: ExtentData,
}

struct SealCmd {
    name: StreamName,
    id: ExtentId,
    came_to: Filled,
    per_stream: u64,
    next: Option<ExtentData>,
}

struct SealStreamCmd {
    name: StreamName,
    id: ExtentId,
    came_to: Filled,
}

struct DeleteCmd {
    name: StreamName,
    retention: Option<i64>,
    hard: bool,
    now: TimeSec,
    maximum: i64,
}

struct DeleteDoneCmd {
    name: StreamName,
    now: TimeSec,
}

#[derive(Clone)]
struct DeleteTask {
    id: ExtentId,
    created: TimeSec,
    disks: Vec<SnDiskId>,
    interval: TimeIntSec64,
    hard: bool,
}

#[derive(Clone)]
struct PendingDelete {
    name: StreamName,
    hard: bool,
    tasks: Vec<DeleteTask>,
}

type ExtentActionKey = (u16, Vec<u8>);

fn extent_action_key(action: &raft::Action) -> ExtentActionKey {
    (action.op.code(), action.fields.clone())
}

fn release_extent_id(table: &mut ExtentTable, id: ExtentId) {
    if table.is_live(id) {
        common::logn!("SN extent slot rollback failed", "extent {:?} is live", id);
        return;
    }
    match table.is_free(id) {
        Ok(true) => {}
        Ok(false) => {
            if let Err(error) = table.release_id(id) {
                common::logn!("SN extent slot rollback failed", "extent {:?}, error {}", id, error);
            }
        }
        Err(error) => {
            common::logn!("SN extent slot rollback failed", "extent {:?}, error {}", id, error)
        }
    }
}

/// Claim one exact slot before changing any application state. On secondaries,
/// `reserve_id` claims the replicated index; the primary proves its earlier
/// allocation is still held. Any impossible mismatch is logged and fails the
/// whole action before its other fields are applied.
fn claim_extent_id(
    table: &mut ExtentTable,
    id: ExtentId,
    local_id: Option<ExtentId>,
) -> Result<()> {
    if let Some(local) = local_id {
        if local != id {
            common::logn!(
                "SN extent slot claim failed",
                "action extent {:?}, local extent {:?}",
                id,
                local
            );
            return Err(Error::new("local extent reservation does not match Raft action"));
        }
        match table.is_free(id) {
            Ok(false) => {}
            Ok(true) => {
                common::logn!("SN extent slot claim failed", "primary slot {:?} is free", id);
                return Err(Error::new("primary extent slot is free"));
            }
            Err(error) => {
                common::logn!(
                    "SN extent slot claim failed",
                    "primary slot {:?}, error {}",
                    id,
                    error
                );
                return Err(Error::new(error.to_string()));
            }
        }
    } else if let Err(error) = table.reserve_id(id) {
        common::logn!("SN extent slot claim failed", "secondary slot {:?}, error {}", id, error);
        return Err(Error::new(error.to_string()));
    }

    if let Err(error) = table.check_vacant(id) {
        if local_id.is_none() {
            release_extent_id(table, id);
        }
        common::logn!("SN extent slot claim failed", "occupied slot {:?}, error {}", id, error);
        return Err(Error::new(error.to_string()));
    }
    Ok(())
}

fn write_name(w: &mut StructWriter, name: StreamName) {
    w.range_id(name.range);
    w.stream_id(name.stream);
    w.time_sec(name.created);
}

fn read_name(r: &mut StructReader<'_>) -> Result<StreamName> {
    Ok(StreamName { range: r.range_id()?, stream: r.stream_id()?, created: r.time_sec()? })
}

fn write_extent(w: &mut StructWriter, extent: &ExtentData) {
    w.number_u8(extent.deletion.bits());
    w.extent_id(extent.id);
    w.eloff(extent.logical);
    w.ecoff(extent.compressed);
    w.eroff(extent.records);
    w.time_sec(extent.created);
    w.number_u64(extent.disks.len() as u64);
    for disk in &extent.disks {
        w.number_u32(disk.get());
    }
}

fn read_extent(r: &mut StructReader<'_>) -> Result<ExtentData> {
    let deletion = Flags::from_bits(r.number_u8()?)?;
    let id = r.extent_id()?;
    let logical = r.eloff()?;
    let compressed = r.ecoff()?;
    let records = r.eroff()?;
    let created = r.time_sec()?;
    let count =
        usize::try_from(r.number_u64()?).map_err(|_| Error::new("extent disk count overflow"))?;
    if count > r.remaining() / 4 {
        return Err(Error::new("extent disk count exceeds action"));
    }
    let mut disks = Vec::with_capacity(count);
    for _ in 0..count {
        disks.push(SnDiskId::new(r.number_u32()?));
    }
    Ok(ExtentData { deletion, logical, compressed, id, records, created, disks })
}

fn write_filled(w: &mut StructWriter, filled: Filled) {
    w.eroff(filled.records);
    w.eloff(filled.logical);
    w.ecoff(filled.compressed);
}

fn read_filled(r: &mut StructReader<'_>) -> Result<Filled> {
    Ok(Filled { records: r.eroff()?, logical: r.eloff()?, compressed: r.ecoff()? })
}

macro_rules! sn_wire {
    ($type:ty, $tag:ident, $write:expr, $read:expr) => {
        impl raft::codec::Wire for $type {
            fn write(&self, w: &mut StructWriter) {
                w.nested(Op::$tag, |w| ($write)(self, w));
            }
            fn read(r: &mut StructReader<'_>) -> Result<Self> {
                r.nested(Op::$tag, stringify!($type), |r| ($read)(r))
            }
        }
    };
}

sn_wire!(
    CreateCmd,
    SnCreate,
    |v: &CreateCmd, w: &mut StructWriter| {
        w.range_id(v.range);
        w.stream_id(v.first);
        w.number_u64(v.count);
        w.time_sec(v.created);
        w.sn_id(v.creator);
    },
    |r: &mut StructReader<'_>| Ok(CreateCmd {
        range: r.range_id()?,
        first: r.stream_id()?,
        count: r.number_u64()?,
        created: r.time_sec()?,
        creator: r.sn_id()?,
    })
);

sn_wire!(
    ConcatCmd,
    SnConcat,
    |v: &ConcatCmd, w: &mut StructWriter| {
        write_name(w, v.destination);
        w.sn_id(v.creator);
        w.number_u64(v.extents.len() as u64);
        for id in &v.extents {
            w.extent_id(*id);
        }
        w.number_u64(v.records.number());
        w.flag(v.require_exists);
        w.flag(v.require_absent);
    },
    |r: &mut StructReader<'_>| {
        let destination = read_name(r)?;
        let creator = r.sn_id()?;
        let count = usize::try_from(r.number_u64()?)
            .map_err(|_| Error::new("concat extent count overflow"))?;
        if count > r.remaining() / 4 {
            return Err(Error::new("concat extent count exceeds action"));
        }
        let mut extents = Vec::with_capacity(count);
        for _ in 0..count {
            extents.push(r.extent_id()?);
        }
        Ok(ConcatCmd {
            destination,
            creator,
            extents,
            records: SRoff::of_number(r.number_u64()?)?,
            require_exists: r.flag()?,
            require_absent: r.flag()?,
        })
    }
);

sn_wire!(
    OpenCmd,
    SnOpen,
    |v: &OpenCmd, w: &mut StructWriter| {
        write_name(w, v.name);
        write_extent(w, &v.extent);
    },
    |r: &mut StructReader<'_>| Ok(OpenCmd { name: read_name(r)?, extent: read_extent(r)? })
);

sn_wire!(
    SealCmd,
    SnSeal,
    |v: &SealCmd, w: &mut StructWriter| {
        write_name(w, v.name);
        w.extent_id(v.id);
        write_filled(w, v.came_to);
        w.number_u64(v.per_stream);
        w.flag(v.next.is_some());
        if let Some(next) = &v.next {
            write_extent(w, next);
        }
    },
    |r: &mut StructReader<'_>| {
        let name = read_name(r)?;
        let id = r.extent_id()?;
        let came_to = read_filled(r)?;
        let per_stream = r.number_u64()?;
        let next = if r.flag()? { Some(read_extent(r)?) } else { None };
        Ok(SealCmd { name, id, came_to, per_stream, next })
    }
);

sn_wire!(
    SealStreamCmd,
    SnSealStream,
    |v: &SealStreamCmd, w: &mut StructWriter| {
        write_name(w, v.name);
        w.extent_id(v.id);
        write_filled(w, v.came_to);
    },
    |r: &mut StructReader<'_>| Ok(SealStreamCmd {
        name: read_name(r)?,
        id: r.extent_id()?,
        came_to: read_filled(r)?,
    })
);

sn_wire!(
    DeleteCmd,
    SnDelete,
    |v: &DeleteCmd, w: &mut StructWriter| {
        write_name(w, v.name);
        w.flag(v.retention.is_some());
        if let Some(retention) = v.retention {
            w.number_u64(retention as u64);
        }
        w.flag(v.hard);
        w.time_sec(v.now);
        w.number_u64(v.maximum as u64);
    },
    |r: &mut StructReader<'_>| {
        let name = read_name(r)?;
        let retention = if r.flag()? { Some(r.number_u64()? as i64) } else { None };
        Ok(DeleteCmd {
            name,
            retention,
            hard: r.flag()?,
            now: r.time_sec()?,
            maximum: r.number_u64()? as i64,
        })
    }
);

sn_wire!(
    DeleteDoneCmd,
    SnDeleteDone,
    |v: &DeleteDoneCmd, w: &mut StructWriter| {
        write_name(w, v.name);
        w.time_sec(v.now);
    },
    |r: &mut StructReader<'_>| Ok(DeleteDoneCmd { name: read_name(r)?, now: r.time_sec()? })
);

fn sn_action<T: raft::codec::Wire>(op: Op, value: &T) -> Result<raft::Action> {
    Ok(raft::Action::new(op, raft::codec::encode(value)?))
}

/// An extent **as one stream holds it**: the extent, and where its records start in that
/// stream.
///
/// ⚠️ **The pair exists because the position is not the extent's** (Asserted, Bob,
/// 2026-08-20): *"you can concatenate the same extent to multiple streams"*, so one extent
/// has as many first-records as there are streams referencing it, and none of them is its
/// own (§17.29.1). Anything answering a question about an extent *in a stream* answers with
/// this; anything about the extent itself answers with `Extent`.
#[derive(Clone, Debug)]
#[cfg(test)]
struct Placed {
    extent: ExtentData,
    first: SRoff,
}

/// A stream that will hold no more extents, and how many records it came to. Not a
/// failure: whoever asked needs another stream, and the DN is what makes those.
#[derive(Debug)]
#[cfg(test)]
struct Full {
    records: SRoff,
    /// Set when the stream could not be extended for an ordinary reason instead.
    why: Option<Error>,
}

#[cfg(test)]
impl Full {
    fn records(records: SRoff) -> Full {
        Full { records, why: None }
    }
}

#[cfg(test)]
impl From<Error> for Full {
    fn from(why: Error) -> Full {
        Full { records: SRoff::ZERO, why: Some(why) }
    }
}

use std::cell::RefCell;

thread_local! {
    /// One generator per worker thread, so choosing disks needs no lock.
    static RNG: RefCell<random::Rng> = RefCell::new(random::Rng::new());
}

/// Instances for one extent, taken randomly from the available disks.
fn pick_disks(total_disks: usize, want: usize) -> Result<Vec<SnDiskId>> {
    if total_disks < want {
        return Err(Error::new(format!(
            "an extent wants {want} instances, and there are only {total_disks} disks",
        )));
    }
    let mut picked = Vec::with_capacity(want);
    RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        while picked.len() < want {
            let id = SnDiskId::new((rng.next_u64() % total_disks as u64) as u32);
            if !picked.contains(&id) {
                picked.push(id);
            }
        }
    });
    Ok(picked)
}

struct SNState {
    /// Keyed by the whole name, `(RangeId, StreamId, CreateSec)`, not just the id (#109).
    /// A re-minted id in a later second is a different key, so stale references cannot
    /// silently find the new stream (`A-NAME-IS-NEVER-REMINTED-IN-ITS-OWN-SECOND`).
    /// Streams are keyed by their full name; IDs remain stable indexes in the extent table.
    stream_map: StreamTable,
    /// Stable extent indexes, protected by the same lock as the stream table.
    extent_table: ExtentTable,
    /// Side metadata whose lifetime follows its owning stream or extent.
    retired_streams: std::collections::HashSet<StreamName>,
    stream_extras: Extras<StreamName>,
    extent_extras: SharedExtras<(ExtentId, TimeSec)>,
    /// In-flight EN deletion work, keyed by stream name for batched Raft entries.
    pending_deletes: HashMap<StreamName, PendingDelete>,
    /// Primary-only waiters for an extent that is being created by Raft.
    /// The global lock protects the map; each value has a lock-free waiter stack.
    pending_opens: HashMap<StreamName, PendingOpen>,
    streams: u64,
    extents: u64,
    sealed: u64,
    sealed_streams: u64,
}

impl SNState {
    fn new(max_extents: u32) -> Result<Self> {
        let chunks = common::message_pool()
            .map_err(|error| Error::new(format!("get segmented-buffer pool: {error}")))?;
        let stream_map = StreamTable::with_buckets(chunks, MAX_STREAMS, 0)
            .map_err(|error| Error::new(format!("reserve SN stream table: {error}")))?;
        let extent_table = ExtentTable::new(max_extents)
            .map_err(|error| Error::new(format!("reserve SN extent table: {error}")))?;
        Ok(Self {
            stream_map,
            extent_table,
            retired_streams: std::collections::HashSet::new(),
            stream_extras: Extras::default(),
            extent_extras: Extras::default(),
            pending_deletes: HashMap::new(),
            pending_opens: HashMap::new(),
            streams: 0,
            extents: 0,
            sealed: 0,
            sealed_streams: 0,
        })
    }
}

enum OpenWaiter {
    Reply(Reply),
    Forward(SyncSender<Resp>),
}

impl OpenWaiter {
    fn send(self, response: Resp) {
        match self {
            Self::Reply(reply) => reply.send(response),
            Self::Forward(sender) => {
                let _ = sender.send(response);
            }
        }
    }
}

struct OpenDone {
    waiters: Vec<OpenWaiter>,
    extent: Option<(ExtentData, SRoff)>,
}

struct OpenNode {
    next: *mut OpenNode,
    waiter: OpenWaiter,
}

// The pointer is published only after the node is initialized. The global write
// lock drains it only after all readers that can push have left.
unsafe impl Send for OpenNode {}

struct PendingOpen {
    head: AtomicPtr<OpenNode>,
}

impl PendingOpen {
    fn new(waiter: OpenWaiter) -> Self {
        let pending = Self { head: AtomicPtr::new(std::ptr::null_mut()) };
        pending.push(waiter);
        pending
    }

    /// Push is lock-free. Callers hold the SN global read lock, which prevents the
    /// Raft writer from removing this entry while a push is in progress.
    fn push(&self, waiter: OpenWaiter) {
        let node = Box::into_raw(Box::new(OpenNode { next: std::ptr::null_mut(), waiter }));
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            // SAFETY: this node is still private until its successful CAS.
            unsafe { (*node).next = head };
            match self.head.compare_exchange_weak(head, node, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Only call while holding the SN global write lock, after all pushers leave.
    fn take(&self) -> Vec<OpenWaiter> {
        let mut node = self.head.swap(std::ptr::null_mut(), Ordering::AcqRel);
        let mut waiters = Vec::new();
        while !node.is_null() {
            // SAFETY: the exclusive global lock excludes pushers and other drainers;
            // this list was allocated by push and each node is reclaimed once.
            let boxed = unsafe { Box::from_raw(node) };
            node = boxed.next;
            waiters.push(boxed.waiter);
        }
        waiters
    }
}

impl Default for PendingOpen {
    fn default() -> Self {
        Self { head: AtomicPtr::new(std::ptr::null_mut()) }
    }
}

impl Drop for PendingOpen {
    fn drop(&mut self) {
        for waiter in self.take() {
            waiter.send(Resp::error(Status::Internal, "SN stopped before opening the extent"));
        }
    }
}

enum OpenCheck {
    Answer(Resp),
    Extent(ExtentData, SRoff),
    Create,
}

fn open_check(state: &SNState, name: StreamName) -> OpenCheck {
    let Some(stream) = state.stream_map.get(&name) else {
        return OpenCheck::Answer(Resp::error(Status::NotFound, format!("no {name}")));
    };
    if !stream.deletion.readable(false) {
        return OpenCheck::Answer(bad("stream is deleted"));
    }
    if let Some(id) = stream.open {
        return OpenCheck::Extent(
            state.extent_table[id.get() as usize].snapshot(),
            stream.sealed_records,
        );
    }
    if stream.sealed_stream {
        return OpenCheck::Answer(Resp::number(Status::Sealed, stream.sealed_records.number()));
    }
    OpenCheck::Create
}

fn check_delete(
    state: &SNState,
    name: StreamName,
    retention: Option<TimeIntSec64>,
    hard: bool,
    now: TimeSec,
) -> Result<()> {
    let stream = state.stream_map.get(&name).ok_or_else(|| Error::new("no stream"))?;
    let flags = stream.deletion;
    if flags.retired() {
        return Ok(());
    }
    let old_until = state.stream_extras.get(&name).map(|extra| extra.retain_until);
    match (retention, hard) {
        (Some(interval), true) if !flags.soft_deleted() => {
            if interval.get() >= 0 {
                return Err(Error::new("combined deletion requires expired retention"));
            }
        }
        (Some(_), true) | (None, true) => {
            if !flags.soft_deleted()
                || !flags.extra()
                || !old_until.is_some_and(|until| until < now)
            {
                return Err(Error::new(
                    "hard delete requires soft deletion with expired retention",
                ));
            }
        }
        _ => {}
    }

    let first_soft = retention.is_some() && !flags.soft_deleted();
    let first_hard = hard && !flags.deleted();
    let add = retention.unwrap_or(TimeIntSec64::MIN);
    let mut copies = HashMap::<ExtentId, u32>::new();
    for id in stream.sealed.iter().copied().chain(stream.open) {
        let count = copies.entry(id).or_default();
        *count =
            count.checked_add(1).ok_or_else(|| Error::new("extent reference count overflow"))?;
    }
    for (id, count) in copies {
        let extent = state
            .extent_table
            .get(id.get() as usize)
            .ok_or_else(|| Error::new("stream names an unknown extent"))?;
        let key = (extent.id, extent.created);
        let (soft, retained) = state.extent_extras.counts(&key, extent.deletion);
        let soft_after = if first_soft {
            if extent.deletion.soft_deleted() || soft < count {
                return Err(Error::new("live reference count underflow"));
            }
            soft - count
        } else {
            soft
        };
        if first_hard {
            if extent.deletion.deleted() || retained < count || retained - count < soft_after {
                return Err(Error::new("retained reference count underflow"));
            }
            if retained == count {
                let expired = if first_soft && soft_after == 0 {
                    let previous = state
                        .extent_extras
                        .get(&key)
                        .map_or(i64::MIN, |extra| i64::from(extra.references.max_retention.get()));
                    previous.max(add.get()) < 0
                } else {
                    state.extent_extras.get(&key).is_some_and(|extra| extra.retain_until < now)
                };
                if !expired {
                    return Err(Error::new(
                        "hard delete requires soft deletion with expired retention",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn apply_sn_action(state: &mut SNState, index: u64, action: &raft::Action) -> Result<()> {
    let mut local_used = false;
    apply_sn_slot(state, index, action, None, &mut local_used)
}

fn apply_sn_slot(
    state: &mut SNState,
    _index: u64,
    action: &raft::Action,
    local_id: Option<ExtentId>,
    local_used: &mut bool,
) -> Result<()> {
    match action.op {
        Op::SnCreate => {
            let cmd: CreateCmd = raft::codec::decode(&action.fields)?;
            let end = cmd
                .first
                .number()
                .checked_add(cmd.count)
                .ok_or_else(|| Error::new("stream id run overflow"))?;
            if end > u64::from(StreamId::MAX.get()) + 1 {
                return Err(Error::new("stream id run exceeds stream id space"));
            }
            let SNState { stream_map, retired_streams, streams, .. } = state;
            let mut added = Vec::new();
            for offset in 0..cmd.count {
                let stream_id = StreamId::of_number(cmd.first.number() + offset)?;
                let name = StreamName { range: cmd.range, stream: stream_id, created: cmd.created };
                if retired_streams.contains(&name) {
                    for name in added.iter().rev() {
                        assert!(stream_map.remove(name), "rollback lost inserted stream");
                    }
                    return Err(Error::new("stream identity has been retired"));
                }
                let (stream, inserted) = match stream_map
                    .find_or_create(name, |name| Stream::new(name.created, cmd.creator))
                {
                    Ok(result) => result,
                    Err(error) => {
                        for name in added.iter().rev() {
                            assert!(stream_map.remove(name), "rollback lost inserted stream");
                        }
                        return Err(Error::new(format!("insert SN stream: {error}")));
                    }
                };
                if inserted {
                    if added.len() == added.capacity() {
                        if let Err(error) = added.try_reserve(1) {
                            assert!(stream_map.remove(&name), "rollback lost new stream");
                            for name in added.iter().rev() {
                                assert!(stream_map.remove(name), "rollback lost inserted stream");
                            }
                            return Err(Error::new(format!("reserve SN stream batch: {error}")));
                        }
                    }
                    added.push(name);
                } else if !stream.deletion.readable(false) {
                    for name in added.iter().rev() {
                        assert!(stream_map.remove(name), "rollback lost inserted stream");
                    }
                    return Err(Error::new("stream is deleted"));
                }
            }
            *streams += added.len() as u64;
            Ok(())
        }
        Op::SnConcat => {
            let cmd: ConcatCmd = raft::codec::decode(&action.fields)?;
            let SNState {
                stream_map,
                extent_table,
                extent_extras,
                retired_streams,
                streams,
                sealed,
                ..
            } = state;
            if retired_streams.contains(&cmd.destination) {
                return Err(Error::new("destination stream identity has been retired"));
            }
            let map = stream_map;
            if cmd.require_absent && map.contains_key(&cmd.destination) {
                return Err(Error::new("concat destination already exists"));
            }
            if cmd.require_exists && !map.contains_key(&cmd.destination) {
                return Err(Error::new("concat destination does not exist"));
            }
            if map.get(&cmd.destination).is_some_and(|stream| {
                stream.sealed_stream || stream.open.is_some() || !stream.sealed.is_empty()
            }) {
                return Err(Error::new("concat destination is not empty and open"));
            }
            let inserted = !map.contains_key(&cmd.destination);
            if inserted {
                let (_, created) = map
                    .find_or_create(cmd.destination, |name| Stream::new(name.created, cmd.creator))
                    .map_err(|error| Error::new(format!("insert SN stream: {error}")))?;
                assert!(created, "SN concat destination appeared under the exclusive lock");
            }
            let mut retained: Vec<ExtentId> = Vec::new();
            for &id in &cmd.extents {
                let extent = extent_table
                    .get_mut(id.get() as usize)
                    .ok_or_else(|| Error::new("concat names an unknown extent"))?;
                if let Err(error) =
                    extent_extras.retain((extent.id, extent.created), &mut extent.deletion)
                {
                    for prior in retained {
                        let extent = &mut extent_table[prior.get() as usize];
                        extent_extras
                            .release(&(extent.id, extent.created), &mut extent.deletion)
                            .expect("rollback retained extent");
                    }
                    if inserted {
                        map.remove(&cmd.destination);
                    }
                    return Err(error);
                }
                retained.push(id);
            }
            let destination = map.get_mut(&cmd.destination).expect("concat destination exists");
            destination.sealed = cmd.extents;
            destination.sealed_records = cmd.records;
            if inserted {
                *streams += 1;
            }
            *sealed += destination.sealed.len() as u64;
            Ok(())
        }
        Op::SnOpen => {
            let cmd: OpenCmd = raft::codec::decode(&action.fields)?;
            if cmd.extent.id.is_none() {
                return Err(Error::new("open action has no extent id"));
            }
            let SNState { stream_map, extent_table, extents, .. } = state;
            let held = stream_map
                .get_mut(&cmd.name)
                .ok_or_else(|| Error::new("open action names no stream"))?;
            if !held.deletion.readable(false) || held.sealed_stream {
                return Err(Error::new("open action names a deleted or sealed stream"));
            }
            if held.open.is_some() {
                return Ok(());
            }
            let id = cmd.extent.id;
            claim_extent_id(extent_table, id, local_id)?;
            if let Err(error) = extent_table.insert_reserved(id, Extent::from_data(cmd.extent)) {
                if local_id.is_none() {
                    release_extent_id(extent_table, id);
                }
                return Err(Error::new(error.to_string()));
            }
            held.open = Some(id);
            *extents += 1;
            *local_used = true;
            Ok(())
        }
        Op::SnSeal => {
            let cmd: SealCmd = raft::codec::decode(&action.fields)?;
            let SNState { stream_map, extent_table, extents, sealed, sealed_streams, .. } = state;
            let held = stream_map
                .get_mut(&cmd.name)
                .ok_or_else(|| Error::new("seal action names no stream"))?;
            if held.open != Some(cmd.id) {
                return Ok(());
            }
            if !extent_table.is_live(cmd.id) {
                return Err(Error::new("seal action names an invalid extent slot"));
            }
            let sealed_records =
                held.sealed_records.plus(SRoff::from_extent(cmd.came_to.records))?;
            let is_full = held.sealed.len() as u64 + 1 >= cmd.per_stream;
            let next_id = if let Some(next) = cmd.next {
                if is_full {
                    return Err(Error::new("seal action creates an extent for a full stream"));
                }
                let id = next.id;
                claim_extent_id(extent_table, id, local_id)?;
                if let Err(error) = extent_table.insert_reserved(id, Extent::from_data(next)) {
                    if local_id.is_none() {
                        release_extent_id(extent_table, id);
                    }
                    return Err(Error::new(error.to_string()));
                }
                *local_used = true;
                Some(id)
            } else if local_id.is_some() {
                return Err(Error::new("local extent reservation is absent from seal action"));
            } else {
                None
            };
            held.open = None;
            held.sealed_records = sealed_records;
            let data = &mut extent_table[cmd.id.get() as usize];
            data.records = cmd.came_to.records;
            data.logical = cmd.came_to.logical;
            data.compressed = cmd.came_to.compressed;
            held.sealed.push(cmd.id);
            *sealed += 1;
            if is_full {
                held.sealed_stream = true;
                *sealed_streams += 1;
            } else if let Some(next_id) = next_id {
                held.open = Some(next_id);
                *extents += 1;
            }
            Ok(())
        }
        Op::SnSealStream => {
            let cmd: SealStreamCmd = raft::codec::decode(&action.fields)?;
            let SNState { stream_map, extent_table, sealed, sealed_streams, .. } = state;
            let held = stream_map
                .get_mut(&cmd.name)
                .ok_or_else(|| Error::new("seal-stream action names no stream"))?;
            if held.sealed_stream {
                return Ok(());
            }
            if held.open != Some(cmd.id) {
                return Err(Error::new("seal-stream action names a stale extent"));
            }
            held.sealed_records =
                held.sealed_records.plus(SRoff::from_extent(cmd.came_to.records))?;
            let data = &mut extent_table[cmd.id.get() as usize];
            data.records = cmd.came_to.records;
            data.logical = cmd.came_to.logical;
            data.compressed = cmd.came_to.compressed;
            held.open = None;
            held.sealed.push(cmd.id);
            held.sealed_stream = true;
            *sealed += 1;
            *sealed_streams += 1;
            Ok(())
        }
        Op::SnDelete => {
            let cmd: DeleteCmd = raft::codec::decode(&action.fields)?;
            let now = cmd.now;
            let maximum = TimeIntSec64::new(cmd.maximum);
            let SNState {
                stream_map,
                extent_table,
                retired_streams,
                stream_extras,
                extent_extras,
                pending_deletes,
                ..
            } = state;
            let Some(stream) = stream_map.get_mut(&cmd.name) else {
                if retired_streams.contains(&cmd.name) {
                    pending_deletes.insert(
                        cmd.name,
                        PendingDelete { name: cmd.name, hard: cmd.hard, tasks: Vec::new() },
                    );
                    return Ok(());
                }
                return Err(Error::new("delete action names no stream"));
            };
            let first_hard = cmd.hard && !stream.deletion.deleted();
            let first = stream_extras.transition(
                cmd.name,
                &mut stream.deletion,
                now,
                cmd.retention.map(TimeIntSec64::new),
                cmd.hard,
                maximum,
            )?;
            let interval = stream_extras.remaining(&cmd.name, now)?;
            let ids: Vec<_> = stream.sealed.iter().copied().chain(stream.open).collect();
            for id in &ids {
                let extent = &mut extent_table[id.get() as usize];
                let key = (extent.id, extent.created);
                if first {
                    extent_extras.release_soft(
                        key,
                        &mut extent.deletion,
                        now,
                        cmd.retention.map(TimeIntSec64::new).unwrap_or(interval),
                        maximum,
                    )?;
                }
                if let Some(retention) = cmd.retention {
                    extent_extras.observe_retention(
                        key,
                        &mut extent.deletion,
                        TimeIntSec64::new(retention),
                        maximum,
                    )?;
                }
                if first_hard {
                    extent_extras.release_hard(&key, &mut extent.deletion)?;
                }
            }
            let mut seen = std::collections::HashSet::new();
            let mut tasks = Vec::new();
            for id in ids {
                if !seen.insert(id) {
                    continue;
                }
                let extent = &mut extent_table[id.get() as usize];
                if !extent.deletion.soft_deleted() || extent.deletion.retired() {
                    continue;
                }
                let key = (extent.id, extent.created);
                let hard = cmd.hard && extent.deletion.deleted();
                if hard {
                    extent_extras.delete(&key, &mut extent.deletion, now)?;
                }
                tasks.push(DeleteTask {
                    id,
                    created: extent.created,
                    disks: extent.disks.clone(),
                    interval: extent_extras.remaining(&key, now)?,
                    hard,
                });
            }
            pending_deletes
                .insert(cmd.name, PendingDelete { name: cmd.name, hard: cmd.hard, tasks });
            Ok(())
        }
        Op::SnDeleteDone => {
            let cmd: DeleteDoneCmd = raft::codec::decode(&action.fields)?;
            let SNState {
                stream_map,
                extent_table,
                retired_streams,
                stream_extras,
                extent_extras,
                pending_deletes,
                streams,
                extents,
                ..
            } = state;
            let pending = pending_deletes
                .remove(&cmd.name)
                .ok_or_else(|| Error::new("delete completion has no pending work"))?;
            if pending.hard {
                for task in pending.tasks.iter().filter(|task| task.hard) {
                    let extent = &mut extent_table[task.id.get() as usize];
                    assert!(extent_extras.reclaim(
                        &(task.id, task.created),
                        &mut extent.deletion,
                        cmd.now
                    ));
                    extent.disks.clear();
                    *extents -= 1;
                }
                let map = stream_map;
                let stream = map
                    .get_mut(&pending.name)
                    .ok_or_else(|| Error::new("hard delete completion has no stream"))?;
                assert!(stream_extras.reclaim(&pending.name, &mut stream.deletion, cmd.now));
                map.remove(&pending.name);
                retired_streams.insert(pending.name);
                *streams -= 1;
            }
            Ok(())
        }
        _ => Err(Error::new(format!("unknown SN Raft action {}", action.op))),
    }
}

impl raft::execution::Application for SNState {
    fn reset(&mut self) {
        // The committed action history is the source of truth during a Raft restore.
        self.stream_map.clear();
        self.extent_table.reset();
        self.retired_streams.clear();
        self.stream_extras = Extras::default();
        self.extent_extras = Extras::default();
        self.pending_deletes.clear();
        // pending_opens is process-local and must survive replay until SnOpen applies.
        self.streams = 0;
        self.extents = 0;
        self.sealed = 0;
        self.sealed_streams = 0;
    }

    fn reclaim(&mut self) {
        self.stream_map.reclaim();
    }
}

struct SN {
    common: raft::runtime::Runtime<SNState>,
    config: SNConfig,
    deleting: Mutex<()>,
    /// Primary-owned pool slots keyed by the exact serialized action that uses them.
    pending_extents: Mutex<HashMap<ExtentActionKey, ExtentId>>,
    /// Maximum sealed extents one stream may hold, including extent references made by
    /// concatenation rather than by appending.
    per_stream: usize,
    /// This SN's own number, which every extent it creates records as its creator.
    ///
    /// ⚠️ **Held here rather than threaded through**, because §8.1.6.1 flags `close`'s
    /// parameter list as the worst run of same-typed arguments in the code and passing
    /// this down would have made it one longer. An SN's identity is a property of the
    /// SN, not an argument to its methods.
    mine: SnId,
    /// All disks across all ENs that this SN can see.
    disks: Mutex<Vec<(EnId, DiskId)>>,
    gc_stop: AtomicBool,
    gc_thread: Mutex<Option<JoinHandle<()>>>,
    streams: AtomicU64,
    extents: AtomicU64,
    sealed: AtomicU64,
    sealed_streams: AtomicU64,
    #[cfg(test)]
    _test_dir: Option<tempfile::TempDir>,
}

impl Deref for SN {
    type Target = raft::runtime::Runtime<SNState>;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl Service for SN {
    fn start(self: &Arc<Self>) -> Result<()> {
        self.start_gc()?;
        if let Err(error) = self.start_raft() {
            let _ = self.stop_gc();
            return Err(error);
        }
        Ok(())
    }

    fn stop(&self, _process: &Process) -> Result<()> {
        self.stop_gc()?;
        self.stop_raft()
    }

    fn serves(&self) -> &'static [Op] {
        SERVES
    }

    fn workers(&self) -> usize {
        self.config.knobs.workers
    }

    fn defers(&self, op: Op) -> bool {
        SERVES.contains(&op)
    }

    fn inline(&self, op: Op) -> bool {
        matches!(op, Op::Ping | Op::Status | Op::Shutdown | Op::Sites)
    }

    fn handle(self: &Arc<Self>, process: &Arc<Process>, request: Req, reply: Reply) {
        if request.op() == Op::OpenExtent {
            if let Some(answer) = self.forward(&request) {
                reply.send(answer);
            } else {
                self.open_to(process, &request, OpenWaiter::Reply(reply));
            }
        } else {
            reply.send(self.answer(process, &request));
        }
    }

    fn answer(&self, process: &Process, request: &Req) -> Resp {
        if self.forwardable(request.op()) {
            if let Some(answer) = self.forward(request) {
                return answer;
            }
        }
        self.answer_local(process, request)
    }

    fn status(&self, _process: &Process) -> Vec<String> {
        vec![
            format!("streams {}", self.streams.load(Ordering::Relaxed)),
            format!("extents {}", self.extents.load(Ordering::Relaxed)),
            format!("sealed_extents {}", self.sealed.load(Ordering::Relaxed)),
            format!("sealed_streams {}", self.sealed_streams.load(Ordering::Relaxed)),
            format!("stream_share {}", self.config.stream_share),
            self.raft_status(),
        ]
    }
}

impl SN {
    fn answer_local(&self, process: &Process, request: &Req) -> Resp {
        match request.op() {
            Op::Raft => self.raft_rpc(process, request),
            Op::SoftDeleteStream | Op::DeleteStream => self.delete_stream(process, request),
            Op::CreateStream => self.create(request),
            Op::ConcatStreams => self.concat_streams(request),
            Op::SealStream => self.seal_stream(request),
            Op::OpenExtent => self.open_extent(process, request),
            Op::LocateRecord => self.locate(request),
            Op::ListExtents => self.list(request),
            Op::SealExtent => self.seal(process, request),
            _ => server::unbuilt(process, SERVES, request),
        }
    }

    fn queue_action(&self, action: raft::Action) -> Result<raft::runtime::ClientTicket> {
        self.common.queue_client(action)
    }

    fn propose_action(&self, action: raft::Action) -> Result<u64> {
        self.queue_action(action)?.wait()
    }

    fn commit_action(&self, action: raft::Action) -> Result<u64> {
        let index = self.propose_action(action)?;
        self.wait_applied(index)?;
        Ok(index)
    }

    fn remember_ext(&self, action: &raft::Action, id: ExtentId) -> Result<()> {
        let key = extent_action_key(action);
        let mut pending =
            self.pending_extents.lock().map_err(|_| Error::new("pending extent lock poisoned"))?;
        if pending.contains_key(&key) {
            return Err(Error::new("extent action already has a local reservation"));
        }
        pending.insert(key, id);
        Ok(())
    }

    /// Caller holds the global write lock before taking the pending map lock,
    /// matching Raft apply's lock order.
    fn cancel_ext_action(&self, key: &ExtentActionKey) {
        let mut state = self.global_lock.write();
        let id = self.pending_extents.lock().expect("pending extents").remove(key);
        if let Some(id) = id {
            release_extent_id(&mut state.extent_table, id);
        }
    }

    fn release_extent_id(&self, id: ExtentId) {
        let mut state = self.global_lock.write();
        release_extent_id(&mut state.extent_table, id);
    }

    fn commit_ext_action(&self, action: raft::Action, id: ExtentId) -> Result<u64> {
        let key = extent_action_key(&action);
        if let Err(error) = self.remember_ext(&action, id) {
            self.release_extent_id(id);
            return Err(error);
        }
        let ticket = match self.queue_action(action) {
            Ok(ticket) => ticket,
            Err(error) => {
                self.cancel_ext_action(&key);
                return Err(error);
            }
        };
        let index = match ticket.wait() {
            Ok(index) => index,
            Err(error) => {
                self.cancel_ext_action(&key);
                return Err(error);
            }
        };
        // Once proposed, the action may still apply after a wait error, so keep
        // its slot reserved until apply consumes or rejects the exact action.
        self.wait_applied(index)?;
        Ok(index)
    }

    fn stop_raft(&self) -> Result<()> {
        self.snapshot_stopping.store(true, Ordering::SeqCst);
        self.wake_execution();
        loop {
            let handles = std::mem::take(
                &mut *self
                    .raft_workers
                    .lock()
                    .map_err(|_| Error::new("Raft worker lock poisoned"))?,
            );
            if handles.is_empty() {
                break;
            }
            for handle in &handles {
                handle.thread().unpark();
            }
            for handle in handles {
                handle.join().map_err(|_| Error::new("Raft worker panicked"))?;
            }
        }
        if let Some(fence) = self.io_fence.get() {
            fence.wait_all()?;
        }
        Ok(())
    }

    fn start_gc(self: &Arc<Self>) -> Result<()> {
        let mut slot = self.gc_thread.lock().expect("GC thread");
        if slot.is_some() {
            return Err(Error::new("SN GC thread is already running"));
        }
        self.gc_stop.store(false, Ordering::Release);
        let sn = Arc::downgrade(self);
        let worker = thread::Builder::new()
            .name("sn-gc".into())
            .spawn(move || {
                loop {
                    let Some(sn) = sn.upgrade() else { break };
                    if sn.gc_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let guard = sn.global_lock.read();
                    thread::sleep(Duration::from_millis(10));
                    drop(guard);
                    if sn.gc_stop.load(Ordering::Acquire) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            })
            .map_err(|error| Error::new(format!("starting SN GC thread: {error}")))?;
        *slot = Some(worker);
        Ok(())
    }

    fn stop_gc(&self) -> Result<()> {
        self.gc_stop.store(true, Ordering::Release);
        let worker = self.gc_thread.lock().expect("GC thread").take();
        if let Some(worker) = worker {
            worker.thread().unpark();
            worker.join().map_err(|_| Error::new("SN GC thread panicked"))?;
        }
        Ok(())
    }

    fn delete_stream(&self, process: &Process, request: &Req) -> Resp {
        let deletion =
            Delete::of_with_limit(request, process.cluster.max_soft_delete_retention_secs)
                .map_err(bad);
        let deletion = match deletion {
            Ok(deletion) => deletion,
            Err(error) => return error,
        };
        let Target::Stream(name) = deletion.target else {
            return bad("expected a stream");
        };
        let _serial = self.deleting.lock().expect("deletion");
        // A previous request may have committed its intent and then lost contact with an
        // EN. Finish that durable outbox before creating a fresh intent.
        loop {
            let pending = {
                let state = self.global_lock.read();
                state.pending_deletes.get(&name).cloned()
            };
            let Some(pending) = pending else { break };
            if let Err(error) = self.finish_delete(process, name, pending) {
                return error;
            }
        }

        let now = TimeSec::now();
        {
            let state = self.global_lock.read();
            if state.retired_streams.contains(&name) {
                return Resp::ok();
            }
            let Some(stream) = state.stream_map.get(&name) else {
                return Resp::error(Status::NotFound, "no stream");
            };
            if stream.deletion.retired() {
                return Resp::ok();
            }
            if let Err(error) = check_delete(&state, name, deletion.retention, deletion.hard, now) {
                return bad(error);
            }
        }

        let cmd = DeleteCmd {
            name,
            retention: deletion.retention.map(TimeIntSec64::get),
            hard: deletion.hard,
            now,
            maximum: process.cluster.max_soft_delete_retention_secs.get(),
        };
        let action = match sn_action(Op::SnDelete, &cmd) {
            Ok(action) => action,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        match self.commit_action(action) {
            Ok(_) => {}
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        }
        let pending = self.global_lock.read().pending_deletes.get(&name).cloned();
        let Some(pending) = pending else {
            return Resp::error(Status::Internal, "Raft applied deletion without an outbox");
        };
        if let Err(error) = self.finish_delete(process, name, pending) {
            return error;
        }
        Resp::ok()
    }

    fn finish_delete(
        &self,
        process: &Process,
        name: StreamName,
        pending: PendingDelete,
    ) -> std::result::Result<(), Resp> {
        for task in &pending.tasks {
            for &disk in &task.disks {
                let (en, disk) = self.disk_at(disk);
                let target = Target::Instance {
                    en,
                    disk,
                    sn: self.mine,
                    extent: task.id,
                    created: task.created,
                };
                forward(
                    process,
                    &format!("EN{en}"),
                    Delete { target: target.clone(), retention: Some(task.interval), hard: false },
                )?;
                if task.hard {
                    forward(
                        process,
                        &format!("EN{en}"),
                        Delete { target, retention: None, hard: true },
                    )?;
                }
            }
        }
        let action = sn_action(Op::SnDeleteDone, &DeleteDoneCmd { name, now: TimeSec::now() })
            .map_err(|error| Resp::error(Status::Internal, error.to_string()))?;
        self.commit_action(action)
            .map(|_| ())
            .map_err(|error| Resp::error(Status::Internal, error.to_string()))
    }

    fn disk_at(&self, id: SnDiskId) -> (EnId, DiskId) {
        self.disks.lock().expect("disks")[id.get() as usize]
    }

    fn extent_args(&self, extent: &Extent, first: SRoff) -> Vec<Arg> {
        self.extent_fields_args(extent.id, extent.created, extent.records, &extent.disks, first)
    }

    fn extent_data_args(&self, extent: &ExtentData, first: SRoff) -> Vec<Arg> {
        self.extent_fields_args(extent.id, extent.created, extent.records, &extent.disks, first)
    }

    fn extent_fields_args(
        &self,
        id: ExtentId,
        created: TimeSec,
        records: ERoff,
        extent_disks: &[SnDiskId],
        first: SRoff,
    ) -> Vec<Arg> {
        let disks = self.disks.lock().expect("disks");
        let mut payload = StructWriter::new();
        payload.extent_id(id);
        payload.time_sec(created);
        payload.sroff(first);
        payload.eroff(records);
        payload.number_u32(extent_disks.len() as u32);
        for sn_disk in extent_disks {
            let (en, disk) = disks[sn_disk.get() as usize];
            payload.en_id(en);
            payload.disk_id(disk);
        }
        vec![payload.finish(Op::ExtentResp)]
    }

    fn with_runtime(
        config: SNConfig,
        per_stream: usize,
        mine: SnId,
        disks: Vec<(EnId, DiskId)>,
        path: &std::path::Path,
        runtime: RaftSettings,
    ) -> Result<SN> {
        runtime.validate(&config.raft)?;
        let mut initial = raft::State::initial(
            runtime.ring.clone(),
            if runtime.identities.is_empty() {
                Default::default()
            } else {
                runtime.members.clone()
            },
            runtime.identities.clone(),
        )?;
        initial.ignore_ring_identity = runtime.ignore_ring_identity;
        let journal = raft::journal::Journal::open(path, initial, 3, &runtime.name)?;
        let id =
            journal.state.membership().slot(&journal.identity.current).unwrap_or(raft::UNASSIGNED);
        let mut node = raft::Node::new_with_log2(id, journal, config.raft.log2_entries)?;
        node.merge_history_scn()?;
        node.identity = Some(node.journal.identity.current.clone());
        node.lease = raft::lease::Lease::configured(&config.raft);
        if node.state().membership().members().len() == 1
            && node.state().membership().members().contains(&id)
        {
            node.campaign()?;
        }
        let options = raft::runtime::Options {
            raft: config.raft.clone(),
            snapshot_min_log_bytes: 1024 * 1024,
            snapshot_interval_secs: 86_400,
            allow_stale_reads: config.knobs.allow_stale_reads,
        };
        let state = SNState::new(config.max_extents)?;
        let sn = SN {
            common: raft::runtime::Runtime::with_state(runtime, node, options, state),
            config,
            deleting: Mutex::new(()),
            pending_extents: Mutex::new(HashMap::new()),
            per_stream,
            mine,
            disks: Mutex::new(disks),
            gc_stop: AtomicBool::new(true),
            gc_thread: Mutex::new(None),
            streams: AtomicU64::new(0),
            extents: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            sealed_streams: AtomicU64::new(0),
            #[cfg(test)]
            _test_dir: None,
        };
        sn.ready.store(true, Ordering::SeqCst);
        let commit = sn.on_raft(|host, node| {
            host.refresh_application(node)?;
            Ok(node.state().commit)
        })?;
        while sn.common.applied() < commit {
            sn.execute_pending()?;
        }
        Ok(sn)
    }

    #[cfg(test)]
    fn new(
        config: SNConfig,
        per_stream: usize,
        mine: SnId,
        disks: Vec<(EnId, DiskId)>,
    ) -> Result<SN> {
        let settings = RaftSettings::testing(&config.raft);
        let directory = tempfile::tempdir()
            .map_err(|error| Error::new(format!("create SN test directory: {error}")))?;
        raft::journal::provision_test_founder(
            directory.path(),
            &settings.ring,
            &settings.name,
            settings.identities[&0].identity.clone(),
        )?;
        let mut sn =
            Self::with_runtime(config, per_stream, mine, disks, directory.path(), settings)?;
        sn._test_dir = Some(directory);
        Ok(sn)
    }

    /// `(RangeId, first StreamId, count, creation second)`. A DN creating many
    /// objects at once names a run of streams in one request, and they share the second —
    /// so it is the consecutive ids that keep the names apart (#110).
    fn create(&self, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::CreateStream, "a create-stream request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        if !rest.is_empty() {
            return Resp::error(
                Status::BadRequest,
                "a create-stream request has trailing arguments",
            );
        }
        let range = match payload.range_id() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let first = match payload.stream_id() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let count = match payload.number_u64() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let created = match payload.time_sec() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        if count == 0 {
            return Resp::ok();
        }

        let end = match first.number().checked_add(count) {
            Some(end) if end <= u64::from(StreamId::MAX.get()) + 1 => end,
            _ => return bad("stream id run exceeds stream id space"),
        };
        let _serial = self.deleting.lock().expect("deletion");
        {
            let state = self.global_lock.read();
            let mut missing = false;
            for number in first.number()..end {
                let stream = StreamId::new(number as u32);
                let name = StreamName { range, stream, created };
                if state.retired_streams.contains(&name) {
                    return bad("stream identity has been retired");
                }
                match state.stream_map.get(&name) {
                    Some(stream) if !stream.deletion.readable(false) => {
                        return bad("stream is deleted");
                    }
                    Some(_) => {}
                    None => missing = true,
                }
            }
            if !missing {
                return Resp::ok();
            }
        }
        let action = match sn_action(
            Op::SnCreate,
            &CreateCmd { range, first, count, created, creator: self.mine },
        ) {
            Ok(action) => action,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        let ticket = match self.queue_action(action) {
            Ok(ticket) => ticket,
            Err(error) => {
                common::logn!("SN create queue failed", "error {}", error);
                return Resp::error(Status::Internal, error.to_string());
            }
        };
        drop(_serial);
        let index = match ticket.wait() {
            Ok(index) => index,
            Err(error) => {
                common::logn!("SN create proposal failed", "error {}", error);
                return Resp::error(Status::Internal, error.to_string());
            }
        };
        match self.wait_applied(index) {
            Ok(_) => Resp::ok(),
            Err(error) => {
                common::logn!("SN create did not apply", "index {},error {}", index, error);
                Resp::error(Status::Internal, error.to_string())
            }
        }
    }

    /// Copy two sealed streams' extent references into an already-created destination
    /// stream. The destination remains open, so a caller can append after the copied
    /// data. Extents themselves are immutable references here; no EN data is copied.
    fn concat_streams(&self, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::ConcatStreams, "a concat-streams request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        if !rest.is_empty() {
            return Resp::error(
                Status::BadRequest,
                "a concat-streams request has trailing arguments",
            );
        }
        let destination = match stream_name(&mut payload) {
            Ok(name) => name,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let left = match stream_name(&mut payload) {
            Ok(name) => name,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let right = match stream_name(&mut payload) {
            Ok(name) => name,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let require_exists = match payload.flag() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let require_absent = match payload.flag() {
            Ok(value) => value,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }
        if require_exists && require_absent {
            return Resp::error(
                Status::BadRequest,
                "concat-streams cannot require both an existing and an absent destination",
            );
        }
        if destination == left || destination == right {
            return Resp::error(Status::BadRequest, "concat-streams needs a distinct destination");
        }
        if destination.range != left.range || destination.range != right.range {
            return Resp::error(Status::BadRequest, "concat-streams streams have different ranges");
        }
        if self.global_lock.read().retired_streams.contains(&destination) {
            return bad("destination stream identity has been retired");
        }

        let _serial = self.deleting.lock().expect("deletion");
        {
            let state = self.global_lock.read();
            let map = &state.stream_map;
            if let Some(destination_stream) = map.get(&destination) {
                if !destination_stream.deletion.readable(false)
                    || destination_stream.sealed_stream
                    || destination_stream.open.is_some()
                    || !destination_stream.sealed.is_empty()
                {
                    return Resp::error(
                        Status::BadRequest,
                        "destination stream is not empty and open",
                    );
                }
            } else if require_exists {
                return Resp::error(Status::NotFound, format!("no {destination}"));
            } else if require_absent {
                // The destination is absent, which is exactly what this flag requires.
            }
        }
        // Copy source state before taking the global write lock for the destination.
        let left_stream = match self.global_lock.read().stream_map.get(&left) {
            Some(stream) => stream.clone(),
            None => return Resp::error(Status::NotFound, format!("no {left}")),
        };
        let right_stream = match self.global_lock.read().stream_map.get(&right) {
            Some(stream) => stream.clone(),
            None => return Resp::error(Status::NotFound, format!("no {right}")),
        };
        if !left_stream.deletion.readable(false) || !right_stream.deletion.readable(false) {
            return bad("cannot concatenate deleted streams");
        }
        if !left_stream.sealed_stream
            || left_stream.open.is_some()
            || !right_stream.sealed_stream
            || right_stream.open.is_some()
        {
            return Resp::error(Status::BadRequest, "only sealed streams may be concatenated");
        }
        let extent_count = left_stream
            .sealed
            .len()
            .checked_add(right_stream.sealed.len())
            .ok_or_else(|| Error::new("concat-streams extent reference count overflow"));
        let extent_count = match extent_count {
            Ok(count) if count <= self.per_stream && (count as u64) <= ExtentId::MAX.number() => {
                count
            }
            Ok(_) => {
                return Resp::error(
                    Status::BadRequest,
                    "concat-streams would exceed the per-stream extent limit",
                );
            }
            _ => {
                return Resp::error(
                    Status::BadRequest,
                    "concat-streams would exceed the extent-reference limit",
                );
            }
        };
        let records = match left_stream.sealed_records.plus(right_stream.sealed_records) {
            Ok(records) => records,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };

        // Clone only after all validation has succeeded. The source streams remain
        // sealed and independently usable; the destination receives its own list.
        let mut copied = Vec::with_capacity(extent_count);
        copied.extend(left_stream.sealed.iter().cloned());
        copied.extend(right_stream.sealed.iter().cloned());
        {
            let state = self.global_lock.read();
            let mut copies = HashMap::<ExtentId, u32>::new();
            for &id in &copied {
                let count = copies.entry(id).or_default();
                *count = match count.checked_add(1) {
                    Some(count) => count,
                    None => return bad("concat extent reference count overflow"),
                };
            }
            for (id, add) in copies {
                let Some(extent) = state.extent_table.get(id.get() as usize) else {
                    return bad("concat names an unknown extent");
                };
                if extent.deletion.soft_deleted() || extent.deletion.deleted() {
                    return bad("concat names a deleted extent");
                }
                let refs =
                    state.extent_extras.counts(&(extent.id, extent.created), extent.deletion);
                if refs.0 == 0 || refs.1 == 0 || u32::MAX - refs.0 < add || u32::MAX - refs.1 < add
                {
                    return bad("concat extent reference count overflow or retired");
                }
            }
        }
        let action = match sn_action(
            Op::SnConcat,
            &ConcatCmd {
                destination,
                creator: self.mine,
                extents: copied,
                records,
                require_exists,
                require_absent,
            },
        ) {
            Ok(action) => action,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        match self.commit_action(action) {
            Ok(_) => Resp::ok(),
            Err(error) => Resp::error(Status::Internal, error.to_string()),
        }
    }

    /// A stream's name, and back comes the extent to append to.
    fn open_extent(&self, process: &Process, request: &Req) -> Resp {
        let (send, receive) = sync_channel(1);
        self.open_to(process, request, OpenWaiter::Forward(send));
        receive.recv().unwrap_or_else(|_| {
            Resp::error(Status::Internal, "SN stopped before answering the append")
        })
    }

    fn extent_reply(&self, extent: ExtentData, first: SRoff) -> Resp {
        Resp::from_args(Status::Ok, self.extent_data_args(&extent, first)).id(0)
    }

    fn open_to(&self, process: &Process, request: &Req, waiter: OpenWaiter) {
        let (mut payload, rest) =
            match structure(&request.args(), Op::OpenExtent, "an open-extent request") {
                Ok(value) => value,
                Err(error) => {
                    return waiter.send(Resp::error(Status::BadRequest, error.to_string()));
                }
            };
        if !rest.is_empty() {
            return waiter.send(Resp::error(
                Status::BadRequest,
                "an open-extent request has trailing arguments",
            ));
        }
        let name = match stream_name(&mut payload).and_then(|name| payload.done().map(|()| name)) {
            Ok(name) => name,
            Err(error) => return waiter.send(Resp::error(Status::BadRequest, error.to_string())),
        };

        // Existing extents and pending creates are checked under the same read lock.
        // A pending entry cannot be removed while this waiter is being pushed.
        {
            let state = self.global_lock.read();
            match open_check(&state, name) {
                OpenCheck::Answer(answer) => {
                    drop(state);
                    return waiter.send(answer);
                }
                OpenCheck::Extent(extent, first) => {
                    drop(state);
                    return waiter.send(self.extent_reply(extent, first));
                }
                OpenCheck::Create => {}
            }
            if let Some(pending) = state.pending_opens.get(&name) {
                pending.push(waiter);
                return;
            }
        }

        // Elect one appender to create the entry. Other appends can join it as soon
        // as it is visible, even while the elected appender waits for the mutation
        // turn or the Raft commit.
        {
            let mut state = self.global_lock.write();
            match open_check(&state, name) {
                OpenCheck::Answer(answer) => {
                    drop(state);
                    return waiter.send(answer);
                }
                OpenCheck::Extent(extent, first) => {
                    drop(state);
                    return waiter.send(self.extent_reply(extent, first));
                }
                OpenCheck::Create => {}
            }
            if let Some(pending) = state.pending_opens.get(&name) {
                pending.push(waiter);
                return;
            }
            state.pending_opens.insert(name, PendingOpen::new(waiter));
        }

        let _serial = self.deleting.lock().expect("SN mutation");
        let id = {
            let state = self.global_lock.read();
            match open_check(&state, name) {
                OpenCheck::Answer(answer) => {
                    drop(state);
                    self.fail_open(name, answer);
                    return;
                }
                OpenCheck::Extent(extent, first) => {
                    drop(state);
                    self.finish_open(name, self.extent_reply(extent, first));
                    return;
                }
                OpenCheck::Create => {}
            }
            match state.extent_table.alloc_id() {
                Ok(id) => id,
                Err(error) => {
                    drop(state);
                    self.fail_open(
                        name,
                        Resp::error(Status::Internal, format!("SN extent table is full: {error}")),
                    );
                    return;
                }
            }
        };

        let assigned = process.assigned_of_kind(ServiceKind::En);
        if !assigned.is_empty() {
            let mut disks = self.disks.lock().expect("disks");
            disks.clear();
            for identity in assigned {
                let en = EnId::new(
                    process.cluster.service(&identity).map(|entry| entry.number()).unwrap_or(0),
                );
                for disk in 0..5 {
                    disks.push((en, DiskId::new(disk)));
                }
            }
        }
        let disks = match pick_disks(
            self.disks.lock().expect("disks").len(),
            process.cluster.extents.instances,
        ) {
            Ok(disks) => disks,
            Err(error) => {
                self.release_extent_id(id);
                self.fail_open(name, Resp::error(Status::Internal, error.to_string()));
                return;
            }
        };
        let extent = ExtentData {
            deletion: Flags::live(),
            logical: ELoff::ZERO,
            compressed: ECoff::ZERO,
            id,
            records: ERoff::ZERO,
            created: TimeSec::now(),
            disks,
        };
        let action = match sn_action(Op::SnOpen, &OpenCmd { name, extent }) {
            Ok(action) => action,
            Err(error) => {
                self.release_extent_id(id);
                self.fail_open(name, Resp::error(Status::Internal, error.to_string()));
                return;
            }
        };
        if let Err(error) = self.commit_ext_action(action, id) {
            self.fail_open(name, Resp::error(Status::Internal, error.to_string()));
        }
    }

    /// Remove a failed create while excluding appenders that could join its waiters.
    fn fail_open(&self, name: StreamName, answer: Resp) {
        self.finish_open(name, answer);
    }

    fn finish_open(&self, name: StreamName, answer: Resp) {
        let waiters = {
            let mut state = self.global_lock.write();
            state.pending_opens.remove(&name).map(|pending| pending.take()).unwrap_or_default()
        };
        for waiter in waiters {
            waiter.send(answer.clone());
        }
    }

    /// `(RangeId, StreamId, record)`, and back comes the extent whose numbering
    /// starts at or before that record. Whether the record is actually there is
    /// the EN's to say: an SN keeps its byte and record totals lazily, so it
    /// routes a read and does not bound it.
    fn locate(&self, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::LocateRecord, "a locate-record request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        let name = match stream_name(&mut payload) {
            Ok(name) => name,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let record = match payload.sroff() {
            Ok(record) => record,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let include = match read_soft_deleted(&mut payload, rest) {
            Ok(value) => value,
            Err(error) => return bad(error),
        };
        let state = self.global_lock.read();
        let map = &state.stream_map;
        let Some(held) = map.get(&name) else {
            return Resp::error(Status::NotFound, format!("no {name}"));
        };
        if !held.deletion.readable(include) {
            return Resp::error(Status::NotFound, "stream is deleted");
        }
        let extents = &state.extent_table;
        if held
            .sealed
            .iter()
            .chain(held.open.iter())
            .any(|id| extents[id.get() as usize].deletion.deleted())
        {
            return Resp::error(Status::NotFound, "extent is deleted");
        }
        // The sealed extents are in record order, so this is a walk over a sorted run,
        // accumulating each extent's start as it goes (§17.29.1).
        if let Some((first, sealed)) =
            held.sealed_from(&extents).find(|(first, s)| s.holds(*first, record))
        {
            return Resp::from_args(Status::Ok, self.extent_args(sealed, first)).id(0);
        }
        match &held.open {
            // How far the open extent reaches is not known here — an SN keeps its
            // record totals lazily — so anything at or past its first record is
            // routed to it and the EN says whether it is there.
            Some(extent) if record >= held.sealed_records => Resp::from_args(
                Status::Ok,
                self.extent_args(&extents[extent.get() as usize], held.sealed_records),
            )
            .id(0),
            Some(_) | None => Resp::error(
                Status::NotFound,
                format!("{name} has no extent holding record {record}"),
            ),
        }
    }

    /// `list-extents`: runs of extents for **several** streams in one ask (§9.1.3.7).
    ///
    /// **Complete in counts and partial in detail** (§9.1.3.2): each stream's extent
    /// total is this SN's own, so it costs nothing and is always right, while the rows
    /// stop at the limit and `next` says where to carry on.
    ///
    /// ⚠️ **The limit is shared across the whole ask**, spent on the streams in the
    /// order given. A stream reached after it runs out is still answered — with
    /// `given` zero and `next` where it was asked to start — so a caller learns the
    /// count and where to resume rather than being left to infer that it was skipped.
    ///
    /// ⚠️ **Record numbers here are stream-local**, which is what an SN knows. The DN
    /// retains them alongside each stream's object-relative base.
    fn list(&self, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::ListExtents, "a list-extents request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        let asked = match Listing::read_payload(&mut payload) {
            Ok(asked) => asked,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let include = match read_soft_deleted(&mut payload, rest) {
            Ok(value) => value,
            Err(error) => return bad(error),
        };
        let mut args = Vec::new();
        let mut room = asked.limit;
        let state = self.global_lock.read();
        for (name, from) in &asked.at {
            let map = &state.stream_map;
            let Some(held) = map.get(name) else {
                return Resp::error(Status::NotFound, format!("no {name}"));
            };
            if !held.deletion.readable(include) {
                return Resp::error(Status::NotFound, "stream is deleted");
            }
            let (run, rows) =
                self.run_of(&state.extent_table, held, name.stream, *from, room, asked.instances);
            room = room.saturating_sub(rows.len() as u64);
            args.extend(run.args());
            for row in rows {
                args.extend(row.args());
            }
        }
        Resp::group(Status::Ok, Op::ListExtentsResp, args)
    }

    /// One stream's run, up to `room` rows.
    fn run_of(
        &self,
        extents: &ExtentTable,
        held: &Stream,
        stream: StreamId,
        from: SRoff,
        room: u64,
        instances: bool,
    ) -> (ExtentRun, Vec<ExtentRow>) {
        // The sealed extents are in record order, so the first to describe is the last
        // one starting at or before `from` — the same rule locate uses to pick one.
        let first = if held.open.is_some() && from >= held.sealed_records {
            held.sealed.len()
        } else if from == SRoff::ZERO {
            0
        } else {
            held.sealed_from(&extents).filter(|(at, _)| *at <= from).count().saturating_sub(1)
        };
        let total = held.sealed.len() as u64 + u64::from(held.open.is_some());

        let mut rows: Vec<ExtentRow> = held
            .sealed_from(&extents)
            .skip(first)
            .take(room as usize)
            .map(|(at, sealed)| ExtentRow {
                sn: held.creator,
                id: sealed.id,
                created: sealed.created,
                first_record: at,
                records: sealed.records,
                logical: sealed.logical,
                length: sealed.compressed,
                disks: if instances {
                    sealed.disks.iter().map(|sn_disk| self.disk_at(*sn_disk)).collect()
                } else {
                    Vec::new()
                },
            })
            .collect();

        // Then the one still open, if there is room and everything sealed is described.
        // ⚠️ Its counts are UNKNOWN rather than zero: how far it reaches is not knowable
        // here, and a sealed extent holding nothing would be a real zero (§8.1.6.3).
        let sealed_seen = first + rows.len();
        if (rows.len() as u64) < room
            && sealed_seen == held.sealed.len()
            && let Some(open) = &held.open
        {
            let open = &extents[open.get() as usize];
            rows.push(ExtentRow {
                sn: held.creator,
                id: open.id,
                created: open.created,
                first_record: held.sealed_records,
                records: ERoff::NONE,
                logical: ELoff::NONE,
                length: ECoff::NONE,
                disks: if instances {
                    open.disks.iter().map(|sn_disk| self.disk_at(*sn_disk)).collect()
                } else {
                    Vec::new()
                },
            });
        }

        // Where to carry on: the first record of the next extent not described.
        let after = first + rows.len();
        let next = match held.sealed.get(after) {
            Some(_) => held.sealed_from(&extents).nth(after).map_or(SRoff::NONE, |(at, _)| at),
            None if after < total as usize => {
                held.open.as_ref().map_or(SRoff::NONE, |_| held.sealed_records)
            }
            None => SRoff::NONE,
        };
        // What the stream has come to, which only the SN can say: it owns the extent list.
        //
        // ⚠️ **Over the sealed extents, and the open one contributes nothing.** How far the
        // open extent reaches is not knowable here (§6.3), and adding a zero for it would
        // make the total read as complete when it is a stream short of its tail. So a
        // growing stream's totals are *behind*, which is exactly the staleness the DN's
        // cache is allowed (§17.25.1) — and a sealed stream has no open extent, so its
        // totals are exact.
        let mut totals = Totals {
            extents: ExtentId::of_number(total).expect("stream extent count fits"),
            logical: SLoff::ZERO,
            compressed: SCoff::ZERO,
        };
        for id in &held.sealed {
            let sealed = &extents[id.get() as usize];
            totals.logical =
                totals.logical.plus(SLoff::from_extent(sealed.logical)).expect("stream bytes fit");
            totals.compressed = totals
                .compressed
                .plus(SCoff::from_extent(sealed.compressed))
                .expect("stream bytes fit");
        }
        (
            ExtentRun {
                stream,
                totals,
                given: ExtentId::of_number(rows.len() as u64).expect("extent row count fits"),
                next,
            },
            rows,
        )
    }

    /// A stream's name, the sealed extent and what it came to: record it and open the next
    /// one, which is one round trip rather than two because whoever saw the seal wants both.
    fn seal_reply(&self, name: StreamName, id: ExtentId) -> Option<Resp> {
        let state = self.global_lock.read();
        let Some(stream) = state.stream_map.get(&name) else {
            return Some(Resp::error(Status::NotFound, format!("no {name}")));
        };
        if !stream.deletion.readable(false) {
            return Some(bad("stream is deleted"));
        }
        if stream.open == Some(id) {
            return None;
        }
        if stream.sealed_stream {
            return Some(Resp::number(Status::Sealed, stream.sealed_records.number()));
        }
        let Some(open) = stream.open else {
            return Some(Resp::error(Status::BadRequest, "stream has no open extent"));
        };
        let extent = state.extent_table[open.get() as usize].snapshot();
        let first = stream.sealed_records;
        drop(state);
        Some(Resp::from_args(Status::Ok, self.extent_data_args(&extent, first)).id(0))
    }

    fn seal(&self, process: &Process, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::SealExtent, "a seal-extent request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        if !rest.is_empty() {
            return Resp::error(Status::BadRequest, "a seal-extent request has trailing arguments");
        }
        let name = match stream_name(&mut payload) {
            Ok(name) => name,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let id = match payload.extent_id() {
            Ok(id) => id,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let came_to = match (payload.eroff(), payload.eloff(), payload.ecoff()) {
            (Ok(records), Ok(logical), Ok(compressed)) => Filled { records, logical, compressed },
            _ => {
                return Resp::error(Status::BadRequest, "seal-extent has invalid extent totals");
            }
        };
        if let Err(e) = payload.done() {
            return Resp::error(Status::BadRequest, e.to_string());
        }

        if let Some(answer) = self.seal_reply(name, id) {
            return answer;
        }
        let _serial = self.deleting.lock().expect("SN mutation");
        if let Some(answer) = self.seal_reply(name, id) {
            return answer;
        }
        let (first, next_result) = {
            let state = self.global_lock.read();
            let stream = &state.stream_map[&name];
            if stream.sealed_records.plus(SRoff::from_extent(came_to.records)).is_err() {
                return Resp::error(Status::BadRequest, "seal-extent record total overflows");
            }
            let is_full = stream.sealed.len() + 1 >= process.cluster.extents.per_stream;
            (stream.sealed_records, (!is_full).then(|| state.extent_table.alloc_id()))
        };

        let mut next = None;
        let mut next_error = None;
        let mut next_id = None;
        if let Some(allocated) = next_result {
            match allocated {
                Ok(id) => match pick_disks(
                    self.disks.lock().expect("disks").len(),
                    process.cluster.extents.instances,
                ) {
                    Ok(disks) => {
                        next_id = Some(id);
                        next = Some(ExtentData {
                            deletion: Flags::live(),
                            logical: ELoff::ZERO,
                            compressed: ECoff::ZERO,
                            id,
                            records: ERoff::ZERO,
                            created: TimeSec::now(),
                            disks,
                        });
                    }
                    Err(error) => {
                        self.release_extent_id(id);
                        next_error = Some(error.to_string());
                    }
                },
                Err(error) => {
                    next_error = Some(format!("SN extent table is full: {error}"));
                }
            }
        }
        let action = match sn_action(
            Op::SnSeal,
            &SealCmd {
                name,
                id,
                came_to,
                per_stream: process.cluster.extents.per_stream as u64,
                next,
            },
        ) {
            Ok(action) => action,
            Err(error) => {
                if let Some(id) = next_id {
                    self.release_extent_id(id);
                }
                return Resp::error(Status::Internal, error.to_string());
            }
        };
        let result = match next_id {
            Some(id) => self.commit_ext_action(action, id),
            None => self.commit_action(action),
        };
        if let Err(error) = result {
            return Resp::error(Status::Internal, error.to_string());
        }

        let state = self.global_lock.read();
        let Some(stream) = state.stream_map.get(&name) else {
            return Resp::error(Status::Internal, "Raft removed stream while sealing extent");
        };
        if stream.sealed_stream {
            return Resp::number(Status::Sealed, stream.sealed_records.number());
        }
        let Some(open) = stream.open else {
            return Resp::error(
                Status::Internal,
                next_error.unwrap_or_else(|| {
                    "Raft sealed extent without opening its successor".to_string()
                }),
            );
        };
        Resp::from_args(
            Status::Ok,
            self.extent_args(&state.extent_table[open.get() as usize], first),
        )
        .id(0)
    }

    fn seal_stream_reply(&self, name: StreamName, id: ExtentId, came_to: Filled) -> Option<Resp> {
        let state = self.global_lock.read();
        let Some(stream) = state.stream_map.get(&name) else {
            return Some(Resp::error(Status::NotFound, format!("no {name}")));
        };
        if !stream.deletion.readable(false) {
            return Some(bad("stream is deleted"));
        }
        if stream.sealed_stream {
            return Some(Resp::number(Status::Ok, stream.sealed_records.number()));
        }
        if stream.open != Some(id) {
            return Some(Resp::error(Status::BadRequest, "seal-stream named a stale extent"));
        }
        if stream.sealed_records.plus(SRoff::from_extent(came_to.records)).is_err() {
            return Some(Resp::error(Status::BadRequest, "seal-stream record total overflows"));
        }
        None
    }

    fn seal_stream(&self, request: &Req) -> Resp {
        let (mut payload, rest) =
            match structure(&request.args(), Op::SealStream, "a seal-stream request") {
                Ok(value) => value,
                Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
            };
        let name = match stream_name(&mut payload) {
            Ok(name) => name,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let id = match payload.extent_id() {
            Ok(id) => id,
            Err(e) => return Resp::error(Status::BadRequest, e.to_string()),
        };
        let came_to = match (payload.eroff(), payload.eloff(), payload.ecoff()) {
            (Ok(records), Ok(logical), Ok(compressed)) => Filled { records, logical, compressed },
            _ => return Resp::error(Status::BadRequest, "seal-stream has invalid extent totals"),
        };
        if !rest.is_empty() || payload.done().is_err() {
            return Resp::error(Status::BadRequest, "seal-stream has trailing arguments");
        }

        if let Some(answer) = self.seal_stream_reply(name, id, came_to) {
            return answer;
        }
        let _serial = self.deleting.lock().expect("SN mutation");
        if let Some(answer) = self.seal_stream_reply(name, id, came_to) {
            return answer;
        }
        let action = match sn_action(Op::SnSealStream, &SealStreamCmd { name, id, came_to }) {
            Ok(action) => action,
            Err(error) => return Resp::error(Status::Internal, error.to_string()),
        };
        if let Err(error) = self.commit_action(action) {
            return Resp::error(Status::Internal, error.to_string());
        }
        Resp::number(Status::Ok, self.global_lock.read().stream_map[&name].sealed_records.number())
    }

    /// Seal the stream's open extent and name the next one, both under the shared
    /// lock, so a request that finds the extent already sealed gets the extent that
    /// replaced it rather than an error.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn close(
        &self,
        name: StreamName,
        id: ExtentId,
        came_to: Filled,
        second: TimeSec,
        instances: usize,
        per_stream: usize,
    ) -> std::result::Result<Placed, Full> {
        let mut state = self.global_lock.write();
        let SNState { stream_map, extent_table, extents, sealed, sealed_streams, .. } = &mut *state;
        let map = stream_map;
        let Some(held) = map.get_mut(&name) else {
            return Err(Full::from(Error::new(format!("no {name}"))));
        };
        // Only whoever names the open extent seals it. Two appends can find the same
        // extent full at once, and the second one has nothing left to do.
        if !held.deletion.readable(false) {
            return Err(Full::from(Error::new("stream is deleted")));
        }
        if held.open == Some(id) {
            let extent = held.open.take().expect("an open extent");
            // ⚠️ Accumulated rather than computed from a stored base, which is the same
            // change one level down: the running total is the stream's (§17.29.1).
            held.sealed_records = held
                .sealed_records
                .plus(SRoff::from_extent(came_to.records))
                .expect("stream record position fits");
            {
                let data = &mut extent_table[extent.get() as usize];
                data.records = came_to.records;
                data.logical = came_to.logical;
                data.compressed = came_to.compressed;
            }
            held.sealed.push(extent);
            *sealed += 1;
            self.sealed.fetch_add(1, Ordering::Relaxed);
            if held.sealed.len() >= per_stream {
                held.sealed_stream = true;
                *sealed_streams += 1;
                self.sealed_streams.fetch_add(1, Ordering::Relaxed);
            }
        }
        if held.sealed_stream {
            return Err(Full::records(held.sealed_records));
        }
        self.appendable(extent_table, held, extents, second, instances).map_err(Full::from)
    }

    /// The stream's open extent, allocating one if it has none.
    #[cfg(test)]
    fn open(
        &self,
        name: StreamName,
        second: TimeSec,
        instances: usize,
    ) -> std::result::Result<Placed, Full> {
        let mut state = self.global_lock.write();
        let SNState { stream_map, extent_table, extents, .. } = &mut *state;
        let map = stream_map;
        let Some(held) = map.get_mut(&name) else {
            return Err(Full::from(Error::new(format!("no {name}"))));
        };
        if held.sealed_stream {
            return Err(Full::records(held.sealed_records));
        }
        self.appendable(extent_table, held, extents, second, instances).map_err(Full::from)
    }

    /// The extent this stream can be appended to, named here if there is not one.
    ///
    /// **The shared state write lock is held across allocation, so requests for one new
    /// extent produce one extent**: whoever gets the lock does the work and the rest
    /// wait and are handed the same answer. That is what stops a burst of
    /// appends to a fresh stream — or to one that has just sealed — from opening an
    /// extent apiece.
    ///
    /// ⚠️ When opening an extent has to be recorded durably, this becomes a wait on
    /// somebody else's round trip, and blocking a worker per waiter would be the wrong
    /// shape — the waiters must attach to the one in flight instead.
    #[cfg(test)]
    fn appendable(
        &self,
        extents: &mut ExtentTable,
        held: &mut Stream,
        count: &mut u64,
        second: TimeSec,
        instances: usize,
    ) -> Result<Placed> {
        if !held.deletion.readable(false) {
            return Err(Error::new("stream is deleted"));
        }
        // Where the open extent starts is the stream's running total, whether the extent
        // was already there or is being made now.
        let first = held.sealed_records;
        if let Some(id) = held.open {
            return Ok(Placed { extent: extents[id.get() as usize].snapshot(), first });
        }
        let disks = pick_disks(self.disks.lock().expect("disks").len(), instances)?;
        let extent = ExtentData {
            deletion: Flags::live(),
            id: ExtentId::NONE,
            created: second,
            records: ERoff::ZERO,
            logical: ELoff::ZERO,
            compressed: ECoff::ZERO,
            disks,
        };
        let id = extents
            .insert(Extent::from_data(extent))
            .map_err(|error| Error::new(format!("allocate SN extent slot: {error}")))?;
        let extent = extents[id.get() as usize].snapshot();
        held.open = Some(id);
        *count += 1;
        self.extents.fetch_add(1, Ordering::Relaxed);
        Ok(Placed { extent, first })
    }
}

impl Host for SN {
    type Application = SNState;
    type Done = OpenDone;

    fn apply_client(
        &self,
        state: &mut SNState,
        context: raft::execution::Context<'_>,
        action: &raft::Action,
        done: &mut Vec<Self::Done>,
    ) -> Result<()> {
        let local_id = if matches!(action.op, Op::SnOpen | Op::SnSeal) {
            self.pending_extents
                .lock()
                .map_err(|_| Error::new("pending extent lock poisoned"))?
                .remove(&extent_action_key(action))
        } else {
            None
        };
        let mut local_used = false;
        let result = apply_sn_slot(state, context.index, action, local_id, &mut local_used);
        if let Some(id) = local_id {
            if result.is_err() || !local_used {
                release_extent_id(&mut state.extent_table, id);
            }
        }
        result?;
        if action.op == Op::SnOpen {
            let command: OpenCmd = raft::codec::decode(&action.fields)?;
            if let Some(pending) = state.pending_opens.remove(&command.name) {
                let stream = state.stream_map.get(&command.name);
                let extent = stream.and_then(|stream| {
                    stream.open.map(|id| {
                        (state.extent_table[id.get() as usize].snapshot(), stream.sealed_records)
                    })
                });
                done.push(OpenDone { waiters: pending.take(), extent });
            }
        }
        Ok(())
    }

    fn finish_apply(&self, state: &SNState) {
        self.streams.store(state.streams, Ordering::Relaxed);
        self.extents.store(state.extents, Ordering::Relaxed);
        self.sealed.store(state.sealed, Ordering::Relaxed);
        self.sealed_streams.store(state.sealed_streams, Ordering::Relaxed);
    }

    fn complete_apply(&self, opened: Vec<Self::Done>) {
        // Do not write responses while holding the application write lock.
        for OpenDone { waiters, extent } in opened {
            let answer = extent.map_or_else(
                || Resp::error(Status::Internal, "Raft opened no extent"),
                |(extent, first)| self.extent_reply(extent, first),
            );
            for waiter in waiters {
                waiter.send(answer.clone());
            }
        }
    }

    fn prepare_primary(&self, _node: &mut raft::Node) -> Result<()> {
        Ok(())
    }

    fn checkpoint_actions(
        &self,
        state: &raft::State,
        _application: &Self::Application,
    ) -> Result<Vec<raft::Entry>> {
        Ok(state.capture().application)
    }

    fn peer_message(&self) -> Option<raft::Action> {
        None
    }

    fn receive_client(&self, _process: &Process, _action: raft::Action) -> Result<Resp> {
        Err(Error::new("SN has no Raft client actions yet"))
    }

    fn dispatch_forwarded(&self, process: &Process, request: &Req) -> Resp {
        self.answer_local(process, request)
    }

    fn forwardable(&self, op: Op) -> bool {
        matches!(
            op,
            Op::SoftDeleteStream
                | Op::DeleteStream
                | Op::CreateStream
                | Op::ConcatStreams
                | Op::SealStream
                | Op::OpenExtent
                | Op::SealExtent
        )
    }

    fn registered_endpoint(&self, _name: &str) -> Option<std::net::SocketAddr> {
        None
    }
}

fn main() {
    let result = server::main(|process| {
        let config = SNConfig::load(&process.config.service_config)?;
        let runtime = RaftSettings::for_process(process, &config.raft)?;
        SN::with_runtime(
            config,
            process.cluster.extents.per_stream,
            SnId::new(process.entry.number()),
            Vec::new(),
            &process.config.data_dir.join("raft"),
            runtime,
        )
    });
    if let Err(e) = result {
        eprintln!("sn: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    fn counts(extras: &SharedExtras<(ExtentId, TimeSec)>, extent: &Extent) -> (u32, u32) {
        extras.counts(&(extent.id, extent.created), extent.deletion)
    }

    fn retain(extras: &mut SharedExtras<(ExtentId, TimeSec)>, extent: &mut Extent) {
        extras.retain((extent.id, extent.created), &mut extent.deletion).unwrap();
    }

    #[test]
    fn shared_extent_waits_for_retained_streams_after_all_live_references_are_gone() {
        use common::time::TimeIntSec64;
        let sn = sn_with_stream(0, 7);
        let source = named(0, 7);
        let other = named(0, 9);
        let placed = sn.open(source, TimeSec::new(1_000_000), 4).unwrap();
        let id = placed.extent.id;
        {
            let mut state = sn.global_lock.write();
            state.streams += 1;
            let snapshot = state.stream_map[&source].clone();
            put_stream(&mut state.stream_map, other, snapshot);
        }
        sn.streams.fetch_add(1, Ordering::Relaxed);
        {
            let mut state = sn.global_lock.write();
            let SNState { extent_table, extent_extras, .. } = &mut *state;
            retain(extent_extras, &mut extent_table[id.get() as usize]);
            // No network in this unit test; the live test verifies instance fanout.
            extent_table[id.get() as usize].disks.clear();
        }
        let process = testing::process();
        let request = |name, retention, hard| {
            Delete { target: Target::Stream(name), retention, hard }.request().unwrap()
        };
        let before = sn.on_raft(|_, node| Ok(node.state().commit)).unwrap();
        assert_eq!(
            sn.delete_stream(&process, &request(source, None, true)).status,
            Status::BadRequest
        );
        assert_eq!(sn.on_raft(|_, node| Ok(node.state().commit)).unwrap(), before);
        assert_eq!(
            sn.delete_stream(&process, &request(source, Some(TimeIntSec64::MIN), false)).status,
            Status::Ok
        );
        assert_eq!(
            sn.delete_stream(&process, &request(other, Some(TimeIntSec64::MIN), true)).status,
            Status::Ok
        );
        {
            let state = sn.global_lock.read();
            let SNState { extent_table, extent_extras, .. } = &*state;
            let table = extent_table;
            let extent = &table[id.get() as usize];
            assert_eq!(counts(extent_extras, extent).0, 0);
            assert_eq!(counts(extent_extras, extent).1, 1);
            assert!(extent.deletion.soft_deleted());
            assert!(!extent.deletion.deleted());
        }
        assert_eq!(sn.delete_stream(&process, &request(source, None, true)).status, Status::Ok);
        assert_eq!(sn.delete_stream(&process, &request(source, None, true)).status, Status::Ok);
        let state = sn.global_lock.read();
        let SNState { extent_table, extent_extras, .. } = &*state;
        let table = extent_table;
        assert!(table[id.get() as usize].deletion.retired());
        assert_eq!(counts(extent_extras, &table[id.get() as usize]).1, 0);
    }
    #[test]
    fn concat_rolls_back_reference_increments_on_overflow() {
        let sn = sn_with_stream(0, 7);
        seal_for_concat(&sn, named(0, 7));
        {
            let mut state = sn.global_lock.write();
            let SNState { extent_table, extent_extras, .. } = &mut *state;
            retain(extent_extras, &mut extent_table[0]);
            extent_extras
                .get(&(extent_table[0].id, extent_table[0].created))
                .unwrap()
                .references
                .hard
                .store(u32::MAX - 1, Ordering::Relaxed);
        }
        let before = sn.on_raft(|_, node| Ok(node.state().commit)).unwrap();
        let response = sn.concat_streams(&concat_request(named(0, 9), named(0, 7), named(0, 7)));
        assert_eq!(response.status, Status::BadRequest);
        assert_eq!(sn.on_raft(|_, node| Ok(node.state().commit)).unwrap(), before);
        let state = sn.global_lock.read();
        let SNState { extent_table, extent_extras, .. } = &*state;
        let table = extent_table;
        assert_eq!(table.len(), 1);
        assert_eq!(counts(extent_extras, &table[0]).1, u32::MAX - 1);
        assert_eq!(table[0].records, ERoff::new(12));
    }

    #[test]
    fn extent_reference_counts_are_atomic() {
        let extent = &common::deletion::ReferenceCounts::default();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(move || {
                    for _ in 0..1000 {
                        extent.retain().unwrap();
                        assert!(!extent.release().unwrap());
                    }
                });
            }
        });
        assert_eq!(extent.hard.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn extent_layout() {
        assert_eq!(std::mem::size_of::<Extent>(), 56);
        assert_eq!(align_of::<Extent>(), 8);
        assert!(!Flags::default().is_valid());
    }

    #[test]
    fn spare_invalid() {
        let mut config = testing::sn_config();
        config.max_extents = 2;
        let sn = SN::new(config, 10, SnId::FIRST, default_disks()).unwrap();
        let mut stream = Stream::new(TimeSec::new(100), SnId::FIRST);
        {
            let mut state = sn.global_lock.write();
            let SNState { extent_table, extents, .. } = &mut *state;
            sn.appendable(extent_table, &mut stream, extents, TimeSec::new(100), 4).unwrap();
        }

        let state = sn.global_lock.read();
        let table = &state.extent_table;
        assert!(table[0].deletion.is_valid());
        assert_eq!(table.pool.issued_blocks(), 1);
        let spare = table.pool.alloc().unwrap();
        assert_eq!(spare.index(), 1);
        assert_eq!(table.pool.issued_blocks(), 2);
        let pointer = spare.as_ptr().cast::<Extent>();
        assert!(!unsafe { (*pointer).deletion.is_valid() });
        // Model the extent's delete routine clearing Valid before the pool
        // sees the block. Pool reuse must leave it alone.
        unsafe {
            (*pointer).deletion.set_valid();
            (*pointer).deletion.clear_valid();
        }
        assert!(!unsafe { (*pointer).deletion.is_valid() });
        drop(spare);
        let reused = state.extent_table.pool.alloc().unwrap();
        assert_eq!(reused.index(), 1);
        assert!(!unsafe { (*reused.as_ptr().cast::<Extent>()).deletion.is_valid() });
    }

    #[test]
    fn big_map() {
        let table = ExtentTable::new(1_073_741_824).unwrap();
        assert_eq!(table.pool.block_bytes(), std::mem::size_of::<Extent>());
        assert_eq!(table.pool.issued_blocks(), 1);
        assert!(!unsafe { (*table.base.as_ptr()).deletion.is_valid() });
    }

    #[test]
    fn extent_rows_keep_extent_measures_while_stream_totals_widen() {
        let sn = SN::new(testing::sn_config(), 10, SnId::FIRST, Vec::new()).unwrap();
        sn.global_lock
            .write()
            .extent_table
            .insert(Extent::from_data(ExtentData {
                deletion: Flags::live(),
                id: ExtentId::FIRST,
                records: ERoff::new(7),
                created: TimeSec::new(100),
                disks: Vec::new(),
                logical: ELoff::MAX,
                compressed: ECoff::new(123),
            }))
            .unwrap();
        let extent = ExtentId::FIRST;
        let mut stream = Stream::new(TimeSec::new(100), SnId::FIRST);
        stream.sealed = vec![extent.clone(), extent];
        stream.sealed_records = SRoff::new(14);
        stream.sealed_stream = true;
        let state = sn.global_lock.read();
        let (run, rows) =
            sn.run_of(&state.extent_table, &stream, StreamId::FIRST, SRoff::ZERO, 10, false);
        assert_eq!(run.totals.extents, ExtentId::new(2));
        assert_eq!(run.given, ExtentId::new(2));
        assert_eq!(run.totals.logical, SLoff::new(2 * ELoff::MAX.number()));
        assert_eq!(run.totals.compressed, SCoff::new(246));
        for row in rows {
            assert_eq!(
                (row.records, row.logical, row.length),
                (ERoff::new(7), ELoff::MAX, ECoff::new(123))
            );
        }
    }

    #[test]
    fn listing_continuation_advances_through_sealed_extents_and_into_the_open_one() {
        let sn = SN::new(testing::sn_config(), 10, SnId::FIRST, Vec::new()).unwrap();
        let mut ids = Vec::new();
        for _ in 0..4 {
            ids.push(
                sn.global_lock
                    .write()
                    .extent_table
                    .insert(Extent::from_data(ExtentData {
                        deletion: Flags::live(),
                        id: ExtentId::FIRST,
                        records: ERoff::new(7),
                        created: TimeSec::new(100),
                        disks: Vec::new(),
                        logical: ELoff::new(100),
                        compressed: ECoff::new(80),
                    }))
                    .unwrap(),
            );
        }
        let mut stream = Stream::new(TimeSec::new(100), SnId::FIRST);
        stream.sealed = vec![ids[1], ids[2]];
        stream.open = Some(ids[3]);
        stream.sealed_records = SRoff::new(14);
        let mut from = SRoff::ZERO;
        for (id, first, next) in [
            (ids[1], SRoff::ZERO, SRoff::new(7)),
            (ids[2], SRoff::new(7), SRoff::new(14)),
            (ids[3], SRoff::new(14), SRoff::NONE),
        ] {
            let state = sn.global_lock.read();
            let (run, rows) =
                sn.run_of(&state.extent_table, &stream, StreamId::FIRST, from, 1, false);
            assert_eq!(run.totals.extents, ExtentId::new(3));
            assert_eq!(rows.len(), 1);
            assert_eq!((rows[0].id, rows[0].first_record), (id, first));
            assert_eq!(run.next, next);
            from = run.next;
        }
        let state = sn.global_lock.read();
        let (run, rows) =
            sn.run_of(&state.extent_table, &stream, StreamId::FIRST, SRoff::new(7), 0, false);
        assert!(rows.is_empty());
        assert_eq!(run.totals.extents, ExtentId::new(3));
        assert_eq!(run.next, SRoff::new(7));
    }

    /// What an extent came to, for a test that does not care about the difference between
    /// a logical length and a compressed one. Anything testing #94's distinction says both.
    fn came(records: u64, compressed: u64) -> Filled {
        Filled {
            records: ERoff::new(records as u32),
            logical: ELoff::new(compressed as u32),
            compressed: ECoff::new(compressed as u32),
        }
    }

    use std::mem::size_of;

    #[test]
    fn extent_references_are_four_bytes() {
        assert_eq!(size_of::<ExtentId>(), 4);
        assert_eq!(size_of::<Extent>(), 56);
    }

    use super::*;
    use common::ids::RangeId;
    use common::offset::{ECoff, ELoff};
    use common::testing;

    /// Extents enough that a stream does not seal in the tests that are not about
    /// a stream sealing.
    const ROOMY: usize = 1000;

    /// A stream, so there is something to open an extent on.
    /// The name every test in here uses, so the second is one thing rather than sprinkled.
    const MADE: TimeSec = TimeSec::new(1_000_000);

    fn named(range: u64, stream: u32) -> StreamName {
        StreamName { range: RangeId::new(range), stream: StreamId::new(stream), created: MADE }
    }

    fn stream_table(max: u32) -> StreamTable {
        StreamTable::new(common::message_pool().unwrap(), max).unwrap()
    }

    fn empty_stream() -> Stream {
        Stream::new(MADE, SnId::FIRST)
    }

    fn put_stream(table: &mut StreamTable, name: StreamName, stream: Stream) {
        let (_, inserted) = table.find_or_create(name, |_| stream).unwrap();
        assert!(inserted);
    }

    fn apply_create(state: &mut SNState, first: u32, count: u64) -> Result<()> {
        let action = sn_action(
            Op::SnCreate,
            &CreateCmd {
                range: RangeId::new(42),
                first: StreamId::new(first),
                count,
                created: MADE,
                creator: SnId::FIRST,
            },
        )?;
        let mut local_used = false;
        apply_sn_slot(state, 0, &action, None, &mut local_used)
    }

    #[test]
    fn stream_create_keeps_existing_and_is_idempotent() {
        let mut state = SNState::new(8).unwrap();
        let existing = named(42, 2);
        put_stream(&mut state.stream_map, existing, empty_stream());
        state.stream_map.get_mut(&existing).unwrap().sealed_stream = true;
        state.streams = 1;

        apply_create(&mut state, 1, 3).unwrap();
        assert_eq!(state.streams, 3);
        assert_eq!(state.stream_map.len(), 3);
        assert!(state.stream_map.get(&existing).unwrap().sealed_stream);

        apply_create(&mut state, 1, 3).unwrap();
        assert_eq!(state.streams, 3);
        assert_eq!(state.stream_map.len(), 3);
        assert!(state.stream_map.get(&existing).unwrap().sealed_stream);
    }

    #[test]
    fn stream_create_rolls_back_on_late_retired_name() {
        let mut state = SNState::new(8).unwrap();
        state.retired_streams.insert(named(42, 3));

        assert!(apply_create(&mut state, 1, 3).is_err());
        assert_eq!(state.streams, 0);
        assert_eq!(state.stream_map.len(), 0);
        assert!(!state.stream_map.contains_key(&named(42, 1)));
        assert!(!state.stream_map.contains_key(&named(42, 2)));
    }

    #[test]
    fn stream_table_reuses_retired_slots() {
        let mut table = stream_table(8);
        let first = named(0, 1);
        let second = named(0, 2);
        put_stream(&mut table, first, empty_stream());
        assert_eq!(table.get(&first).unwrap().name.created, MADE);
        assert!(table.get(&first).unwrap().deletion.is_valid());
        assert_eq!(table.get(&first).unwrap().deletion.bits(), Flags::live().bits());
        assert_eq!(table.get(&first).unwrap().link, StreamId::NONE.get());

        assert!(table.remove(&first));
        assert!(!table.contains_key(&first));
        table.reclaim();
        put_stream(&mut table, second, empty_stream());

        assert!(table.get(&second).is_some());
        assert!(table.get(&second).unwrap().deletion.is_valid());
        assert_eq!(table.get(&second).unwrap().deletion.bits(), Flags::live().bits());
        assert_eq!(table.get(&second).unwrap().link, StreamId::NONE.get());
    }

    #[test]
    fn stream_chains() {
        let mut table = stream_table(128);
        let left = named(10, 7);
        let bucket = stream_hash(&left) & 63;
        let right = (10_000..20_000)
            .map(|range| named(range, 7))
            .find(|name| stream_hash(name) & 63 == bucket)
            .expect("find a stream-name hash collision");
        assert_eq!(stream_hash(&right) & 63, bucket);
        put_stream(&mut table, right, empty_stream());
        put_stream(&mut table, left, empty_stream());
        assert_eq!(table.get(&left).unwrap().name, left);
        assert_eq!(table.get(&right).unwrap().name, right);

        let names: Vec<_> = (0..65).map(|id| named(12, id)).collect();
        for name in &names {
            put_stream(&mut table, *name, empty_stream());
        }
        for name in &names {
            assert!(table.contains_key(name));
        }
        for name in &names {
            assert!(table.remove(name));
        }
        assert!(table.get(&left).is_some());
        assert!(table.get(&right).is_some());
        assert!(table.remove(&left));
        assert!(table.remove(&right));
        assert!(!table.contains_key(&left));
    }

    #[test]
    fn stream_batch_inserts_and_rolls_back() {
        let mut table = stream_table(128);
        let left = named(10, 7);
        let bucket = stream_hash(&left) & 63;
        let right = (10_000..20_000)
            .map(|range| named(range, 7))
            .find(|name| stream_hash(name) & 63 == bucket)
            .expect("find a stream-name hash collision");
        put_stream(&mut table, right, empty_stream());
        put_stream(&mut table, left, empty_stream());

        let mid: Vec<_> = (11..right.range.get())
            .map(|range| named(range, 7))
            .filter(|name| *name != named(500, 7) && stream_hash(name) & 63 == bucket)
            .take(2)
            .collect();
        assert_eq!(mid.len(), 2);
        assert!(left < mid[0] && mid[0] < mid[1] && mid[1] < right);

        let mut names: Vec<_> = (0..65).map(|id| named(500, id)).collect();
        names.extend(mid);
        for name in &names {
            put_stream(&mut table, *name, empty_stream());
        }

        assert_eq!(table.len(), 69);
        for name in &names {
            assert!(table.contains_key(name));
        }

        for name in names.iter().rev() {
            assert!(table.remove(name));
        }
        assert_eq!(table.len(), 2);
        for name in &names {
            assert!(!table.contains_key(name));
        }
        assert!(table.contains_key(&left));
        assert!(table.contains_key(&right));
    }

    #[test]
    fn hash_spread() {
        let mut buckets = std::collections::HashSet::new();
        for range in 0..64 {
            for stream in 1..=4 {
                buckets.insert(stream_hash(&named(range, stream)) & 63);
            }
        }
        assert!(buckets.len() >= 58, "only {} of 64 buckets used", buckets.len());
    }

    #[test]
    fn extent_init_clears_all_flags() {
        let mut extent = Extent::blank();
        extent.deletion = Flags::from_bits(15).unwrap();
        extent.init();
        assert_eq!(extent.deletion.bits(), 0);
    }

    #[test]
    fn extent_slot_requires_clear_flags() {
        let table = ExtentTable::new(8).unwrap();
        let id = table.alloc_id().unwrap();
        assert!(table.check_vacant(id).is_ok());

        // A slot with non-valid deletion bits is still not in the free state.
        unsafe {
            (*table.base.as_ptr().add(id.get() as usize)).deletion = Flags::vacant();
        }
        assert!(table.check_vacant(id).is_err());
    }

    #[test]
    fn extent_len_includes_free_pool_slots() {
        let table = ExtentTable::new(512).unwrap();
        for _ in 0..130 {
            table.alloc_id().unwrap();
        }

        assert_eq!(table.len(), table.pool.issued_blocks() as usize);
        let free = (0..table.len())
            .find(|&index| table.pool.is_free(index as u32).unwrap())
            .expect("the pool refills with a free slot");
        assert_eq!(table[free].deletion.bits(), 0);
    }

    #[test]
    fn same_extent_slot() {
        let name = named(3, 7);
        let mut primary = SNState::new(8).unwrap();
        let mut secondary = SNState::new(8).unwrap();
        put_stream(&mut primary.stream_map, name, empty_stream());
        put_stream(&mut secondary.stream_map, name, empty_stream());

        let id = primary.extent_table.alloc_id().unwrap();
        assert_eq!(primary.extent_table.len(), primary.extent_table.pool.issued_blocks() as usize);
        assert_eq!(
            secondary.extent_table.len(),
            secondary.extent_table.pool.issued_blocks() as usize
        );
        assert_eq!(primary.extent_table[id.get() as usize].deletion.bits(), 0);
        assert_eq!(secondary.extent_table[id.get() as usize].deletion.bits(), 0);
        let action = sn_action(
            Op::SnOpen,
            &OpenCmd {
                name,
                extent: ExtentData {
                    deletion: Flags::live(),
                    logical: ELoff::ZERO,
                    compressed: ECoff::ZERO,
                    id,
                    records: ERoff::ZERO,
                    created: MADE,
                    disks: Vec::new(),
                },
            },
        )
        .unwrap();

        let mut used = false;
        apply_sn_slot(&mut primary, 1, &action, Some(id), &mut used).unwrap();
        apply_sn_action(&mut secondary, 1, &action).unwrap();

        assert!(used);
        assert_eq!(primary.stream_map[&name].open, Some(id));
        assert_eq!(secondary.stream_map[&name].open, Some(id));
        assert!(primary.extent_table.is_live(id));
        assert!(secondary.extent_table.is_live(id));
        assert_eq!(primary.extent_table[id.get() as usize].deletion.bits(), Flags::live().bits());
        assert_eq!(secondary.extent_table[id.get() as usize].deletion.bits(), Flags::live().bits());
        assert!(!primary.extent_table.is_free(id).unwrap());
        assert!(!secondary.extent_table.is_free(id).unwrap());
    }

    #[test]
    fn extent_flags_round_trip_in_open_action() {
        let name = named(3, 70);
        let id = ExtentId::FIRST;
        let mut deletion = Flags::live();
        let mut extras = Extras::<_>::default();
        extras
            .soft_delete(
                (id, MADE),
                &mut deletion,
                MADE,
                TimeIntSec64::ZERO,
                TimeIntSec64::new(100),
            )
            .unwrap();
        let action = sn_action(
            Op::SnOpen,
            &OpenCmd {
                name,
                extent: ExtentData {
                    deletion,
                    logical: ELoff::ZERO,
                    compressed: ECoff::ZERO,
                    id,
                    records: ERoff::ZERO,
                    created: MADE,
                    disks: Vec::new(),
                },
            },
        )
        .unwrap();

        let decoded: OpenCmd = raft::codec::decode(&action.fields).unwrap();
        assert_eq!(decoded.extent.deletion.bits(), deletion.bits());
        assert!(decoded.extent.deletion.is_valid());
        assert!(decoded.extent.deletion.soft_deleted());
        assert!(Flags::from_bits(0x10).is_err());

        let mut truncated = action;
        truncated.fields.pop();
        assert!(raft::codec::decode::<OpenCmd>(&truncated.fields).is_err());
    }

    #[test]
    fn seal_slot_conflict() {
        let name = named(3, 8);
        let mut state = SNState::new(8).unwrap();
        put_stream(&mut state.stream_map, name, empty_stream());
        let current = ExtentId::FIRST;
        let open = sn_action(
            Op::SnOpen,
            &OpenCmd {
                name,
                extent: ExtentData {
                    deletion: Flags::live(),
                    logical: ELoff::ZERO,
                    compressed: ECoff::ZERO,
                    id: current,
                    records: ERoff::ZERO,
                    created: MADE,
                    disks: Vec::new(),
                },
            },
        )
        .unwrap();
        apply_sn_action(&mut state, 1, &open).unwrap();
        let taken = ExtentId::new(1);
        state.extent_table.reserve_id(taken).unwrap();

        let seal = sn_action(
            Op::SnSeal,
            &SealCmd {
                name,
                id: current,
                came_to: came(7, 90),
                per_stream: 10,
                next: Some(ExtentData {
                    deletion: Flags::live(),
                    logical: ELoff::ZERO,
                    compressed: ECoff::ZERO,
                    id: taken,
                    records: ERoff::ZERO,
                    created: MADE,
                    disks: Vec::new(),
                }),
            },
        )
        .unwrap();

        assert!(apply_sn_action(&mut state, 2, &seal).is_err());
        assert_eq!(state.stream_map[&name].open, Some(current));
        assert!(state.stream_map[&name].sealed.is_empty());
        assert_eq!(state.extent_table[current.get() as usize].records, ERoff::ZERO);
        assert!(!state.extent_table.is_free(taken).unwrap());
        assert!(!state.extent_table.is_live(taken));
    }

    fn default_disks() -> Vec<(EnId, DiskId)> {
        let mut disks = Vec::new();
        for en in 0..4 {
            for disk in 0..5 {
                disks.push((EnId::new(en), DiskId::new(disk)));
            }
        }
        disks
    }

    fn sn_with_stream(range: u64, stream: u32) -> SN {
        let sn = SN::new(
            testing::sn_config(),
            testing::extents().per_stream,
            SnId::new(0),
            default_disks(),
        )
        .unwrap();
        let made = sn.create(&Req::new(Op::CreateStream).structure({
            let mut payload = StructWriter::new();
            payload.range_id(RangeId::new(range));
            payload.stream_id(StreamId::new(stream));
            payload.number_u64(1);
            payload.time_sec(TimeSec::new(1_000_000));
            payload
        }));
        assert_eq!(made.status, Status::Ok);
        sn
    }

    #[test]
    fn single_member_raft_runs_without_sn_actions() {
        let sn = Arc::new(SN::new(testing::sn_config(), 10, SnId::FIRST, Vec::new()).unwrap());
        sn.start().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut ready = false;
        while std::time::Instant::now() < deadline {
            let (role, commit) =
                sn.on_raft(|_, node| Ok((node.role, node.state().commit))).unwrap();
            let applied = sn.common.applied();
            if role == raft::Role::Leader && commit != 0 && applied >= commit {
                ready = true;
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let status = sn.raft_status();
        sn.stop_gc().unwrap();
        sn.stop_raft().unwrap();
        assert!(ready, "single-member SN Raft did not apply its no-op entry: {status}");
    }

    fn add_stream(sn: &SN, range: u64, stream: u32) {
        let made = sn.create(&Req::new(Op::CreateStream).structure({
            let mut payload = StructWriter::new();
            payload.range_id(RangeId::new(range));
            payload.stream_id(StreamId::new(stream));
            payload.number_u64(1);
            payload.time_sec(MADE);
            payload
        }));
        assert_eq!(made.status, Status::Ok);
    }

    #[test]
    fn extent_actions_replay_from_checkpoint_history() {
        let name = named(3, 19);
        let sn = sn_with_stream(name.range.number(), name.stream.get());
        let process = testing::process();
        let open_req = Req::new(Op::OpenExtent).structure({
            let mut payload = StructWriter::new();
            payload.range_id(name.range);
            payload.stream_id(name.stream);
            payload.time_sec(name.created);
            payload
        });

        assert_eq!(sn.open_extent(&process, &open_req).status, Status::Ok);
        let id = sn.global_lock.read().stream_map[&name].open.unwrap();
        let commit = sn.on_raft(|_, node| Ok(node.state().commit)).unwrap();
        assert_eq!(sn.open_extent(&process, &open_req).status, Status::Ok);
        assert_eq!(sn.on_raft(|_, node| Ok(node.state().commit)).unwrap(), commit);

        let seal = |op: Op, id: ExtentId, filled: Filled| {
            Req::new(op).structure({
                let mut payload = StructWriter::new();
                payload.range_id(name.range);
                payload.stream_id(name.stream);
                payload.time_sec(name.created);
                payload.extent_id(id);
                payload.eroff(filled.records);
                payload.eloff(filled.logical);
                payload.ecoff(filled.compressed);
                payload
            })
        };
        assert_eq!(sn.seal(&process, &seal(Op::SealExtent, id, came(7, 90))).status, Status::Ok);
        let next = sn.global_lock.read().stream_map[&name].open.unwrap();
        assert_eq!(sn.seal_stream(&seal(Op::SealStream, next, came(3, 50))).status, Status::Ok);

        let expected = sn.global_lock.read();
        let stream = &expected.stream_map[&name];
        assert!(stream.sealed_stream);
        assert_eq!(stream.sealed.len(), 2);
        assert_eq!(expected.extent_table.len(), 2);
        let history = sn.on_raft(|_, node| Ok(node.state().capture().application)).unwrap();
        drop(expected);

        let mut replay = SNState::new(sn.config.max_extents).unwrap();
        for entry in history {
            if matches!(&entry.command, raft::Command::Client(_)) {
                let actions = entry.client_actions().unwrap();
                for action in actions.iter() {
                    apply_sn_action(&mut replay, entry.index, action).unwrap();
                }
            }
        }
        let restored = &replay.stream_map[&name];
        assert!(restored.sealed_stream);
        assert_eq!(restored.sealed, vec![ExtentId::FIRST, ExtentId::new(1)]);
        assert_eq!(restored.sealed_records, SRoff::new(10));
        assert_eq!(replay.extent_table.len(), 2);
        assert_eq!(replay.extent_table[0].records, ERoff::new(7));
        assert_eq!(replay.extent_table[1].records, ERoff::new(3));
    }

    #[test]
    fn wait_push() {
        let name = named(1, 2);
        let state = Arc::new(parking_lot::RwLock::new(SNState::new(1).unwrap()));
        state.write().pending_opens.insert(name, PendingOpen::default());
        let mut receivers = Vec::new();
        let mut workers = Vec::new();
        for _ in 0..8 {
            let mut senders = Vec::new();
            for _ in 0..64 {
                let (sender, receive) = sync_channel(1);
                senders.push(sender);
                receivers.push(receive);
            }
            let state = Arc::clone(&state);
            workers.push(thread::spawn(move || {
                for sender in senders {
                    let held = state.read();
                    held.pending_opens
                        .get(&name)
                        .expect("pending stream")
                        .push(OpenWaiter::Forward(sender));
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let waiters = {
            let mut held = state.write();
            held.pending_opens.remove(&name).unwrap().take()
        };
        assert_eq!(waiters.len(), receivers.len());
        for waiter in waiters {
            waiter.send(Resp::ok());
        }
        for receive in receivers {
            assert_eq!(receive.recv().unwrap().status, Status::Ok);
        }
    }

    #[test]
    fn wait_fanout() {
        let name = named(3, 21);
        let sn = sn_with_stream(name.range.number(), name.stream.get());
        let (one, got_one) = sync_channel(1);
        let (two, got_two) = sync_channel(1);
        {
            let mut state = sn.global_lock.write();
            let waiters = PendingOpen::default();
            waiters.push(OpenWaiter::Forward(one));
            waiters.push(OpenWaiter::Forward(two));
            state.pending_opens.insert(name, waiters);
        }
        let extent = ExtentData {
            deletion: Flags::live(),
            id: ExtentId::FIRST,
            created: MADE,
            records: ERoff::ZERO,
            logical: ELoff::ZERO,
            compressed: ECoff::ZERO,
            disks: vec![SnDiskId::new(0); testing::extents().instances],
        };
        let action = sn_action(Op::SnOpen, &OpenCmd { name, extent }).unwrap();
        sn.commit_action(action).unwrap();
        let one = got_one.recv_timeout(Duration::from_secs(1)).unwrap();
        let two = got_two.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(one.status, Status::Ok);
        assert_eq!(one.payload, two.payload);
        assert!(!sn.global_lock.read().pending_opens.contains_key(&name));
    }

    fn concat_request(destination: StreamName, left: StreamName, right: StreamName) -> Req {
        concat_request_with_flags(destination, left, right, false, false)
    }

    fn concat_request_with_flags(
        destination: StreamName,
        left: StreamName,
        right: StreamName,
        require_exists: bool,
        require_absent: bool,
    ) -> Req {
        Req::new(Op::ConcatStreams).structure({
            let mut payload = StructWriter::new();
            for name in [destination, left, right] {
                payload.range_id(name.range);
                payload.stream_id(name.stream);
                payload.time_sec(name.created);
            }
            payload.flag(require_exists);
            payload.flag(require_absent);
            payload
        })
    }

    fn seal_for_concat(sn: &SN, name: StreamName) {
        let extent = sn.open(name, MADE, testing::extents().instances).unwrap();
        let sealed =
            sn.close(name, extent.extent.id, came(12, 400), MADE, testing::extents().instances, 1);
        assert!(sealed.is_err(), "a per-stream limit of one seals the source");
    }

    #[test]
    fn self_concat_copies_extent_references_and_leaves_destination_open() {
        let sn = sn_with_stream(0, 7);
        seal_for_concat(&sn, named(0, 7));

        let answer = sn.concat_streams(&concat_request(named(0, 9), named(0, 7), named(0, 7)));
        assert_eq!(answer.status, Status::Ok, "{}", answer.message().unwrap_or_default());

        let id = {
            let state = sn.global_lock.read();
            let destination = &state.stream_map[&named(0, 9)];
            assert_eq!(destination.sealed.len(), 2, "A || A has two extent references");
            assert_eq!(destination.sealed_records, SRoff::new(24));
            assert!(destination.open.is_none(), "concatenation does not open an extent");
            assert!(!destination.sealed_stream, "the result remains appendable");
            assert_eq!(
                destination.sealed[0], destination.sealed[1],
                "self-concat shares the same immutable extent"
            );
            destination.sealed[0]
        };
        let mut state = sn.global_lock.write();
        let SNState { extent_table, extent_extras, .. } = &mut *state;
        let extents = extent_table;
        assert_eq!(extents.len(), 1);
        let extent = &mut extents[id.get() as usize];
        assert_eq!(counts(extent_extras, extent).1, 3);
        let extras = extent_extras;
        let key = (extent.id, extent.created);
        assert!(!extras.release(&key, &mut extent.deletion).unwrap());
        assert!(!extras.release(&key, &mut extent.deletion).unwrap());
        assert!(!extent.deletion.extra());
        assert!(extras.get(&key).is_none());
        assert_eq!(extras.counts(&key, extent.deletion), (1, 1));
        assert!(extras.release(&key, &mut extent.deletion).unwrap());
        assert!(extras.release(&key, &mut extent.deletion).is_err());
    }

    #[test]
    fn stream_concat_rejects_the_configured_per_stream_limit() {
        let mut sn = sn_with_stream(0, 7);
        sn.per_stream = 1;
        seal_for_concat(&sn, named(0, 7));
        let answer = sn.concat_streams(&concat_request(named(0, 9), named(0, 7), named(0, 7)));
        assert_eq!(answer.status, Status::BadRequest);
        assert_eq!(
            answer.message().as_deref(),
            Some("concat-streams would exceed the per-stream extent limit")
        );
    }

    #[test]
    fn stream_concat_requires_sealed_sources() {
        let sn = sn_with_stream(0, 7);

        let answer = sn.concat_streams(&concat_request(named(0, 9), named(0, 7), named(0, 7)));
        assert_eq!(answer.status, Status::BadRequest, "{}", answer.message().unwrap_or_default());
        assert!(answer.message().unwrap_or_default().contains("sealed"));
    }

    #[test]
    fn stream_concat_rejects_a_nonempty_destination() {
        let sn = sn_with_stream(0, 7);
        add_stream(&sn, 0, 9);
        seal_for_concat(&sn, named(0, 7));
        seal_for_concat(&sn, named(0, 9));

        let answer = sn.concat_streams(&concat_request(named(0, 9), named(0, 7), named(0, 7)));
        assert_eq!(answer.status, Status::BadRequest);
        assert!(answer.message().unwrap_or_default().contains("destination"));
    }

    #[test]
    fn stream_concat_destination_flags_control_creation() {
        let sn = sn_with_stream(0, 7);
        seal_for_concat(&sn, named(0, 7));

        let missing_required = sn.concat_streams(&concat_request_with_flags(
            named(0, 9),
            named(0, 7),
            named(0, 7),
            true,
            false,
        ));
        assert_eq!(missing_required.status, Status::NotFound);

        let created = sn.concat_streams(&concat_request_with_flags(
            named(0, 9),
            named(0, 7),
            named(0, 7),
            false,
            true,
        ));
        assert_eq!(created.status, Status::Ok);

        let existing_required_absent = sn.concat_streams(&concat_request_with_flags(
            named(0, 9),
            named(0, 7),
            named(0, 7),
            false,
            true,
        ));
        assert_eq!(existing_required_absent.status, Status::BadRequest);
    }

    #[test]
    fn stream_concat_rejects_conflicting_destination_flags() {
        let sn = sn_with_stream(0, 7);
        let answer = sn.concat_streams(&concat_request_with_flags(
            named(0, 9),
            named(0, 7),
            named(0, 7),
            true,
            true,
        ));
        assert_eq!(answer.status, Status::BadRequest);
    }

    #[test]
    fn a_stream_opens_one_extent_however_often_it_is_asked() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let again = sn.open(named(0, 7), TimeSec::new(1_000_001), want).unwrap();
        assert_eq!(first.extent.id, again.extent.id, "the second ask should find the open extent");
        assert_eq!(first.extent.created, again.extent.created);
        assert_eq!(sn.extents.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn extent_cap() {
        let mut config = testing::sn_config();
        config.max_extents = 1;
        let sn = SN::new(config, 10, SnId::FIRST, default_disks()).unwrap();
        let mut stream = Stream::new(TimeSec::new(100), SnId::FIRST);
        let first = {
            let mut state = sn.global_lock.write();
            let SNState { extent_table, extents, .. } = &mut *state;
            sn.appendable(extent_table, &mut stream, extents, TimeSec::new(100), 4).unwrap()
        };
        assert_eq!(first.extent.id, ExtentId::FIRST);
        assert_eq!(sn.config.max_extents, 1);
        assert_eq!(sn.global_lock.read().extent_table.len(), 1);
        stream.open = None;
        let result = {
            let mut state = sn.global_lock.write();
            let SNState { extent_table, extents, .. } = &mut *state;
            sn.appendable(extent_table, &mut stream, extents, TimeSec::new(101), 4)
        };
        assert!(result.is_err());
        assert_eq!(sn.global_lock.read().extent_table.len(), 1);
    }

    #[test]
    fn an_extent_on_a_stream_that_does_not_exist_is_refused() {
        let sn = SN::new(
            testing::sn_config(),
            testing::extents().per_stream,
            SnId::new(0),
            default_disks(),
        )
        .unwrap();
        assert!(sn.open(named(0, 7), TimeSec::new(1_000_000), 4).is_err());
    }

    /// Sealing records what the extent came to and names the next one, whose
    /// numbering starts where the last one stopped.
    #[test]
    fn sealing_starts_the_next_extent_where_the_last_one_stopped() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        // ⚠️ Asserted on the *stream*, because an extent no longer carries where it starts
        // (§17.29.1) — which is the point: the running total is the stream's, and testing it
        // there tests the thing that is actually maintained.
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        assert_eq!(sn.starts_at(0, 7), SRoff::new(0));

        // Twelve records in it, and 400 bytes.
        let second = sn
            .close(
                named(0, 7),
                first.extent.id,
                came(12, 400),
                TimeSec::new(1_000_001),
                want,
                ROOMY,
            )
            .unwrap();
        assert_ne!(second.extent.id, first.extent.id, "a fresh extent");
        assert_eq!(sn.starts_at(0, 7), SRoff::new(12), "numbering carries on");

        let third = sn
            .close(named(0, 7), second.extent.id, came(5, 90), TimeSec::new(1_000_002), want, ROOMY)
            .unwrap();
        assert_eq!(sn.starts_at(0, 7), SRoff::new(17));
        assert_eq!(third.extent.records, ERoff::ZERO, "and the fresh one holds nothing yet");
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 2);

        // And the positions the stream hands out are the running sum of what came before.
        assert_eq!(sn.firsts(0, 7), vec![SRoff::ZERO, SRoff::new(12)], "one per sealed extent");
    }

    /// Where the open extent starts, for a test.
    impl SN {
        fn starts_at(&self, range: u64, stream: u32) -> SRoff {
            self.global_lock.read().stream_map[&named(range, stream)].sealed_records
        }

        /// The stream-local first record of every sealed extent, recomputed by summing.
        fn firsts(&self, range: u64, stream: u32) -> Vec<SRoff> {
            let state = self.global_lock.read();
            state.stream_map[&named(range, stream)]
                .sealed_from(&state.extent_table)
                .map(|(at, _)| at)
                .collect()
        }
    }

    /// ⚠️ **The spot the `args(0)` bug was at** (§17.29.5). `open-extent` answers with the
    /// extent *and where it starts in this stream*, and the second one is easy to get wrong
    /// by passing a literal: the same argument used to mean "how many records" and now means
    /// "from which record", so `0` stayed correct-looking and became wrong. Nothing at this
    /// tier caught it, because `open` itself was right — the mistake was in the answer.
    #[test]
    fn the_open_extent_answers_with_where_it_starts_in_the_stream() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        assert_eq!(first.first, SRoff::ZERO, "the first extent starts the stream");

        // Seal it with twelve records, so the next one does not start at zero.
        sn.close(named(0, 7), first.extent.id, came(12, 400), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        let next = sn.open(named(0, 7), TimeSec::new(1_000_002), want).unwrap();
        assert_eq!(next.first, SRoff::new(12), "and the next carries on from what came before");

        // And the answer on the wire says so, which is the argument that was wrong: third
        // field is the first record, fourth is the count.
        let answer = sn.extent_data_args(&next.extent, next.first);
        let (mut payload, rest) = structure(&answer, Op::ExtentResp, "an extent").unwrap();
        assert!(rest.is_empty());
        let _id = payload.extent_id().unwrap();
        let _created = payload.time_sec().unwrap();
        let at = payload.sroff().unwrap();
        let records = payload.eroff().unwrap();
        assert_eq!((at, records), (SRoff::new(12), ERoff::ZERO), "starts at 12, holds nothing yet");
    }

    /// An extent already sealed by somebody else does not fail: the caller wanted
    /// somewhere to append, and the extent that replaced it is the answer. The shared
    /// state write lock makes concurrent seal requests serialize on that stream.
    #[test]
    fn sealing_an_extent_that_is_already_gone_gives_the_one_that_replaced_it() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(4, 100), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        // A request that saw the seal a moment later, naming the extent that is now
        // sealed, and one naming an extent this stream never had.
        assert_eq!(
            sn.close(
                named(0, 7),
                first.extent.id,
                came(4, 100),
                TimeSec::new(1_000_002),
                want,
                ROOMY
            )
            .unwrap()
            .extent
            .id,
            second.extent.id
        );
        assert_eq!(
            sn.close(
                named(0, 7),
                ExtentId::new(999),
                came(0, 0),
                TimeSec::new(1_000_003),
                want,
                ROOMY
            )
            .unwrap()
            .extent
            .id,
            second.extent.id
        );
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 1, "sealed once");
        assert_eq!(sn.extents.load(Ordering::Relaxed), 2, "two extents, not four");
    }

    /// Two appends can find the same extent full at once, so sealing it twice must
    /// not open two extents. The second one gets whatever is open now.
    #[test]
    fn sealing_the_same_extent_twice_opens_one_extent() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(4, 100), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        let repeat = sn
            .close(named(0, 7), first.extent.id, came(4, 100), TimeSec::new(1_000_002), want, ROOMY)
            .unwrap();
        assert_eq!(repeat.extent.id, second.extent.id, "the extent that is open now");
        assert_eq!(sn.sealed.load(Ordering::Relaxed), 1, "sealed once");
    }

    /// A stream seals when it has all the extents it is allowed, and then it will
    /// take no more — which is the object's problem, not this stream's.
    #[test]
    fn a_stream_seals_once_it_has_its_extents() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        // Room for two extents, so the second seal fills the stream.
        let two = 2;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(10, 100), TimeSec::new(1_000_001), want, two)
            .unwrap();
        assert_eq!(sn.starts_at(0, 7), SRoff::new(10));

        let full = sn
            .close(named(0, 7), second.extent.id, came(7, 70), TimeSec::new(1_000_002), want, two)
            .expect_err("the stream should be full");
        assert!(full.why.is_none(), "full is not a failure");
        assert_eq!(full.records, SRoff::new(17), "what the whole stream came to");
        assert_eq!(sn.sealed_streams.load(Ordering::Relaxed), 1);

        // And it stays sealed: nothing opens another extent on it.
        let again = sn.open(named(0, 7), TimeSec::new(1_000_003), want).expect_err("sealed");
        assert_eq!(again.records, SRoff::new(17));
        assert!(again.why.is_none());
    }

    /// A record in a sealed extent is found there; one past everything sealed goes to
    /// the open extent, because how far that reaches is the EN's to say.
    #[test]
    fn a_record_is_found_in_the_extent_holding_it() {
        let sn = sn_with_stream(0, 7);
        let want = testing::extents().instances;
        let first = sn.open(named(0, 7), TimeSec::new(1_000_000), want).unwrap();
        let second = sn
            .close(named(0, 7), first.extent.id, came(3, 60), TimeSec::new(1_000_001), want, ROOMY)
            .unwrap();
        let third = sn
            .close(named(0, 7), second.extent.id, came(2, 40), TimeSec::new(1_000_002), want, ROOMY)
            .unwrap();

        let holds = |record: u64| {
            // The whole name, built from the shape rather than three numbers in a row (#109).
            let mut payload = StructWriter::new();
            payload.range_id(RangeId::new(0));
            payload.stream_id(StreamId::new(7));
            payload.time_sec(MADE);
            payload.sroff(SRoff::new(record));
            let ask = Req::new(Op::LocateRecord).structure(payload);
            let answer = sn.locate(&ask);
            assert_eq!(answer.status, Status::Ok, "record {record}");
            let (mut payload, rest) =
                structure(answer.args(), Op::ExtentResp, "an extent").expect("an extent");
            assert!(rest.is_empty());
            payload.extent_id().expect("an extent id").number()
        };
        // Records 0 to 2 are in the first extent, 3 and 4 in the second, and
        // anything from 5 on is routed to the one still open.
        assert_eq!(holds(0), first.extent.id.number());
        assert_eq!(holds(2), first.extent.id.number());
        assert_eq!(holds(3), second.extent.id.number());
        assert_eq!(holds(4), second.extent.id.number());
        assert_eq!(holds(5), third.extent.id.number());
        assert_eq!(holds(500), third.extent.id.number(), "the EN says whether it is really there");
    }

    /// A stream with no extent at all has nowhere to look.
    #[test]
    fn a_record_in_a_stream_with_no_extent_is_not_found() {
        let sn = sn_with_stream(0, 7);
        let answer = sn.locate(&{
            let mut payload = StructWriter::new();
            payload.range_id(RangeId::new(0));
            payload.stream_id(StreamId::new(7));
            payload.time_sec(MADE);
            payload.sroff(SRoff::ZERO);
            Req::new(Op::LocateRecord).structure(payload)
        });
        assert_eq!(answer.status, Status::NotFound);
    }

    #[test]
    fn instances_are_four_distinct_disks_chosen_randomly() {
        for _ in 0..20 {
            let picked = pick_disks(7, testing::extents().instances).unwrap();
            assert_eq!(picked.len(), testing::extents().instances);
            let mut sorted = picked.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), testing::extents().instances, "{picked:?} repeats a disk");
        }
    }

    /// Fewer ENs than an extent has instances cannot be papered over by repeating
    /// one: four instances on three machines is three failure domains.
    #[test]
    fn too_few_ens_is_an_error_not_a_repeat() {
        assert!(pick_disks(3, testing::extents().instances).is_err());
    }

    #[test]
    fn disks_are_chosen_from_all_available_nodes_and_disks() {
        // 10 disks total (e.g., 2 ENs with 5 disks each)
        let picked = pick_disks(10, 4).unwrap();
        assert_eq!(picked.len(), 4);
        let mut sorted = picked.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 4, "should be distinct");
        // Verify they are within range
        for id in picked {
            assert!(id.get() < 10);
        }
    }

    /// The four ENs in the default roster are enough for an extent.
    #[test]
    fn the_test_roster_has_enough_disks_for_an_extent() {
        let cluster = testing::cluster();
        let mut disks = Vec::new();
        for en in cluster.services_of_kind(ServiceKind::En) {
            let en_id = EnId::new(en.number());
            for d in 0..5 {
                disks.push((en_id, DiskId::new(d)));
            }
        }
        assert!(disks.len() >= testing::extents().instances);
        assert!(pick_disks(disks.len(), testing::extents().instances).is_ok());
    }
}
