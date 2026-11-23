//! SN application checkpoint codec.
use super::extent_tree::DiskExtentTree;
use super::{
    DeleteTask, DiskUsage, EnDiskInfo, EnDiskStatus, EnInfo, Extent, ExtentData, ExtentTable,
    MAX_STREAMS, PendingDelete, SNState, Stream,
};
use common::deletion::{Extra, Flags, ReferenceCounts, SharedExtras};
use common::ids::{DiskId, EnId, ExtentId, RangeId, SnDiskId, SnEnDiskId, SnId, StreamId};
use common::info::StreamName;
use common::offset::{ECoff, ELoff, ERoff, SRoff};
use common::time::{TimeIntSec32, TimeIntSec64, TimeSec};
use common::{Error, Op, Result};
use raft::codec;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

const REF_CHUNK: usize = 16 * 1024;
const CHUNK_BYTES: usize = 2 * 1024 * 1024;
const CHUNK_MAX: usize = 4 * 1024 * 1024;
const CHUNK_ITEMS: usize = 64 * 1024;

#[derive(Clone, Copy)]
struct Range {
    start: u64,
    len: u64,
}
raft::wire_struct!(Range, SnCkRange, {start,len});

struct Footer {
    raft: raft::checkpoint::RaftStatus,
    sequence: u64,
    second: u64,
    streams: u64,
    stream_buckets: u64,
    extents: u64,
    extent_slots: ExtentId,
    ens: Range,
    en_states: Range,
    disk_usage: Range,
    disk_usage_built_at: Option<u64>,
    disk_extent_entries: u64,
    disk_extent_rows: Range,
    disk_extent_chunks: Vec<raft::checkpoint::ChunkPos>,
    extent_rows: Range,
    extent_chunks: Vec<raft::checkpoint::ChunkPos>,
    stream_rows: Range,
    stream_chunks: Vec<raft::checkpoint::ChunkPos>,
    retired: Range,
    pending: Range,
}
impl codec::Wire for Footer {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.nested(Op::SnCkFooter, |writer| {
            self.raft.write(writer);
            self.sequence.write(writer);
            self.second.write(writer);
            self.streams.write(writer);
            self.stream_buckets.write(writer);
            self.extents.write(writer);
            writer.extent_id(self.extent_slots);
            self.ens.write(writer);
            self.en_states.write(writer);
            self.disk_usage.write(writer);
            self.disk_usage_built_at.write(writer);
            self.disk_extent_entries.write(writer);
            self.disk_extent_rows.write(writer);
            self.disk_extent_chunks.write(writer);
            self.extent_rows.write(writer);
            self.extent_chunks.write(writer);
            self.stream_rows.write(writer);
            self.stream_chunks.write(writer);
            self.retired.write(writer);
            self.pending.write(writer);
        });
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        reader.nested(Op::SnCkFooter, "Footer", |reader| {
            Ok(Self {
                raft: codec::Wire::read(reader)?,
                sequence: codec::Wire::read(reader)?,
                second: codec::Wire::read(reader)?,
                streams: codec::Wire::read(reader)?,
                stream_buckets: codec::Wire::read(reader)?,
                extents: codec::Wire::read(reader)?,
                extent_slots: reader.extent_id()?,
                ens: codec::Wire::read(reader)?,
                en_states: codec::Wire::read(reader)?,
                disk_usage: codec::Wire::read(reader)?,
                disk_usage_built_at: codec::Wire::read(reader)?,
                disk_extent_entries: codec::Wire::read(reader)?,
                disk_extent_rows: codec::Wire::read(reader)?,
                disk_extent_chunks: codec::Wire::read(reader)?,
                extent_rows: codec::Wire::read(reader)?,
                extent_chunks: codec::Wire::read(reader)?,
                stream_rows: codec::Wire::read(reader)?,
                stream_chunks: codec::Wire::read(reader)?,
                retired: codec::Wire::read(reader)?,
                pending: codec::Wire::read(reader)?,
            })
        })
    }
}

struct EnRow {
    id: u64,
    disks: Vec<u64>,
}
raft::wire_struct!(EnRow, SnCkEn, {id,disks});

struct EnDiskRow {
    id: u64,
    total_bytes: f64,
    used_bytes: f64,
    status: u8,
}
impl codec::Wire for EnDiskRow {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.nested(Op::SnCkEnDiskDouble, |writer| {
            writer.number_u64(self.id);
            writer.number_f64(self.total_bytes);
            writer.number_f64(self.used_bytes);
            writer.number_u8(self.status);
        });
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        reader.nested_any(|op, reader| {
            let id = reader.number_u64()?;
            let (total_bytes, used_bytes) = match op {
                // Keep accepting checkpoints written before disk-space values
                // moved to doubles.
                Op::SnCkEnDisk => (reader.number_u64()? as f64, reader.number_u64()? as f64),
                Op::SnCkEnDiskDouble => (reader.number_f64()?, reader.number_f64()?),
                _ => return Err(Error::new("invalid SN checkpoint EN disk row")),
            };
            let status = reader.number_u8()?;
            if !total_bytes.is_finite()
                || total_bytes < 0.0
                || !used_bytes.is_finite()
                || used_bytes < 0.0
            {
                return Err(Error::new("invalid SN checkpoint EN disk space value"));
            }
            Ok(Self { id, total_bytes, used_bytes, status })
        })
    }
}

struct DiskUsageEnRow {
    en: u64,
    disks: Vec<DiskUsageDiskRow>,
}
raft::wire_struct!(DiskUsageEnRow, SnCkDiskUsageEn, {en,disks});

struct DiskUsageDiskRow {
    disk: u64,
    cumulative_usage: f64,
    cumulative_total_space: f64,
    cumulative_adjusted_free_space: f64,
}
impl codec::Wire for DiskUsageDiskRow {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.nested(Op::SnCkDiskUsageDiskDouble, |writer| {
            writer.number_u64(self.disk);
            writer.number_u64(self.cumulative_usage.to_bits());
            writer.number_u64(self.cumulative_total_space.to_bits());
            writer.number_u64(self.cumulative_adjusted_free_space.to_bits());
        });
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        reader.nested_any(|op, reader| {
            let disk = reader.number_u64()?;
            let (cumulative_usage, cumulative_total_space, cumulative_adjusted_free_space) =
                match op {
                    // Retain the previous integer representation while existing
                    // checkpoints written with it are still in use.
                    Op::SnCkDiskUsageDisk => (
                        reader.number_u64()? as f64,
                        reader.number_u64()? as f64,
                        reader.number_u64()? as f64,
                    ),
                    Op::SnCkDiskUsageDiskDouble => (
                        f64::from_bits(reader.number_u64()?),
                        f64::from_bits(reader.number_u64()?),
                        f64::from_bits(reader.number_u64()?),
                    ),
                    _ => return Err(Error::new("invalid SN checkpoint disk usage row")),
                };
            if !cumulative_usage.is_finite()
                || cumulative_usage < 0.0
                || !cumulative_total_space.is_finite()
                || cumulative_total_space < 0.0
                || !cumulative_adjusted_free_space.is_finite()
                || cumulative_adjusted_free_space < 0.0
            {
                return Err(Error::new("invalid SN checkpoint disk usage value"));
            }
            Ok(Self {
                disk,
                cumulative_usage,
                cumulative_total_space,
                cumulative_adjusted_free_space,
            })
        })
    }
}

struct EnStateRow {
    id: u64,
    last_disk_sync: Option<u64>,
    last_extent_sync: Option<u64>,
    disks: Vec<EnDiskRow>,
    retired_disks: Vec<u64>,
}
raft::wire_struct!(EnStateRow, SnCkEnState, {id,last_disk_sync,last_extent_sync,disks,retired_disks});

struct ExtentIndex(u32);

impl codec::Wire for ExtentIndex {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.number_u32(self.0);
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        Ok(Self(reader.number_u32()?))
    }
}

struct DiskExtentIndexRow {
    en: u64,
    disk: u64,
    ids: Vec<ExtentIndex>,
}
raft::wire_struct!(DiskExtentIndexRow, SnCkDiskExtentIndex, {en,disk,ids});

#[derive(Clone, Copy, Debug)]
enum DiskExtentMismatchKind {
    FooterEntryCount,
    EmptyTree,
    UnsortedIds,
    UnknownDisk,
    NoneExtentId,
    MissingExtent,
    WrongDisk,
    DuplicateExtentId,
    DuplicateTree,
    ExtentNotIndexed,
}

#[derive(Clone, Copy, Debug)]
struct DiskExtentMismatch {
    kind: DiskExtentMismatchKind,
    disk: Option<(u64, u16)>,
    extent: Option<u32>,
}

impl DiskExtentMismatch {
    fn new(
        kind: DiskExtentMismatchKind,
        disk: Option<(EnId, DiskId)>,
        extent: Option<u32>,
    ) -> Self {
        Self { kind, disk: disk.map(|(en, disk)| (en.get(), disk.get())), extent }
    }
}

struct DiskExtentIndexRange {
    disks: Vec<(EnId, DiskId)>,
}

struct ExtExtra {
    until: u64,
    soft: u64,
    hard: u64,
    max_retention: u64,
}
raft::wire_struct!(ExtExtra, SnCkExtExtra, {until,soft,hard,max_retention});

struct ExtentRow {
    id: u64,
    flags: u8,
    sealed: bool,
    logical: u64,
    compressed: u64,
    records: u64,
    created: u64,
    disks: Vec<DiskIndex>,
    extra: Option<ExtExtra>,
}
raft::wire_struct!(ExtentRow, SnCkExtent, {id,flags,sealed,logical,compressed,records,created,disks,extra});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiskIndex(u32);

impl codec::Wire for DiskIndex {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.number_u32(self.0);
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        Ok(Self(reader.number_u32()?))
    }
}

struct StreamHead {
    range: u64,
    stream: u64,
    created: u64,
    flags: u8,
    creator: u64,
    sealed_len: u64,
    open: Option<u64>,
    sealed_records: u64,
    sealed_stream: bool,
    retain_until: Option<u64>,
}
raft::wire_struct!(StreamHead, SnCkStream, {
    range,stream,created,flags,creator,sealed_len,open,sealed_records,sealed_stream,retain_until
});

struct Refs {
    ids: Vec<u64>,
}
raft::wire_struct!(Refs, SnCkRefs, { ids });

struct NameRow {
    range: u64,
    stream: u64,
    created: u64,
}
raft::wire_struct!(NameRow, SnCkRetired, {range,stream,created});

struct PendingHead {
    name: NameRow,
    hard: bool,
    task_count: u64,
}
raft::wire_struct!(PendingHead, SnCkPending, {name,hard,task_count});

struct TaskRow {
    id: u64,
    created: u64,
    disks: Vec<DiskIndex>,
    interval: u64,
    hard: bool,
}
raft::wire_struct!(TaskRow, SnCkTask, {id,created,disks,interval,hard});

fn encode_piece<T: codec::Wire>(value: &T) -> Result<Vec<u8>> {
    let mut writer = common::StructWriter::new();
    value.write(&mut writer);
    let arg = writer.finish(Op::Raft);
    if arg.depth == 0 || arg.depth > common::MAX_RECURSION_DEPTH {
        return Err(Error::new("SN checkpoint nesting exceeds limit"));
    }
    let mut bytes = vec![arg.depth];
    arg.into_bytes().write_to(&mut bytes)?;
    Ok(bytes)
}

fn decode_piece<T: codec::Wire>(bytes: &[u8]) -> Result<T> {
    let (&depth, bytes) =
        bytes.split_first().ok_or_else(|| Error::new("empty SN checkpoint piece"))?;
    if depth == 0 || depth > common::MAX_RECURSION_DEPTH || isize::try_from(bytes.len()).is_err() {
        return Err(Error::new("invalid SN checkpoint piece limits"));
    }
    let mut reader = common::StructReader::new(bytes, depth);
    let value = T::read(&mut reader)?;
    reader.done()?;
    Ok(value)
}

fn write_piece<T: codec::Wire>(file: &mut impl Write, value: &T) -> Result<()> {
    let bytes = encode_piece(value)?;
    let len = u64::try_from(bytes.len()).map_err(|_| Error::new("SN checkpoint row too large"))?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&bytes)?;
    Ok(())
}

fn encode_chunk<T: codec::Wire>(op: Op, rows: &[T]) -> Result<Vec<u8>> {
    let count =
        u64::try_from(rows.len()).map_err(|_| Error::new("SN checkpoint chunk too large"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&op.code().to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    for row in rows {
        write_piece(&mut bytes, row)?;
    }
    Ok(bytes)
}

fn decode_chunk<T: codec::Wire>(
    chunk: raft::checkpoint::ChunkPos,
    bytes: &[u8],
    op: Op,
) -> Result<Vec<T>> {
    let mut input = Cursor::new(bytes);
    let mut tag = [0; 2];
    input.read_exact(&mut tag)?;
    if u16::from_le_bytes(tag) != op.code() {
        return Err(Error::new(format!("wrong SN checkpoint chunk {op}")));
    }
    let count = read_u64(&mut input)?;
    if count != chunk.items {
        return Err(Error::new("SN checkpoint chunk item count mismatch"));
    }
    let count =
        usize::try_from(count).map_err(|_| Error::new("SN checkpoint chunk is too large"))?;
    let mut left =
        chunk.plain.checked_sub(10).ok_or_else(|| Error::new("truncated SN checkpoint chunk"))?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| Error::new("SN checkpoint chunk allocation failed"))?;
    for _ in 0..count {
        rows.push(read_piece(&mut input, &mut left)?);
    }
    if left != 0 {
        return Err(Error::new("trailing SN checkpoint chunk data"));
    }
    Ok(rows)
}

fn check_chunks(range: Range, chunks: &[raft::checkpoint::ChunkPos]) -> Result<()> {
    let start =
        range.start.checked_add(10).ok_or_else(|| Error::new("SN checkpoint range overflow"))?;
    let end = checked_end(range)?;
    let mut physical = chunks.to_vec();
    physical.sort_unstable_by_key(|chunk| chunk.start);
    let mut at = start;
    for chunk in &physical {
        let chunk_end = chunk
            .start
            .checked_add(chunk.stored)
            .ok_or_else(|| Error::new("SN checkpoint chunk range overflow"))?;
        if chunk.start != at
            || chunk_end > end
            || chunk.plain < 10
            || chunk.stored == 0
            || chunk.stored > chunk.plain
            || chunk.span == 0
            || chunk.items == 0
        {
            return Err(Error::new("invalid SN checkpoint chunk positions"));
        }
        at = chunk_end;
    }
    if at != end {
        return Err(Error::new("SN checkpoint chunks do not fill their section"));
    }
    let mut logical = chunks.to_vec();
    logical.sort_unstable_by_key(|chunk| chunk.first);
    let mut prior_end = 0u64;
    for chunk in logical {
        let chunk_end = chunk
            .first
            .checked_add(chunk.span)
            .ok_or_else(|| Error::new("SN checkpoint logical chunk range overflow"))?;
        if chunk.first < prior_end {
            return Err(Error::new("overlapping SN checkpoint chunks"));
        }
        prior_end = chunk_end;
    }
    Ok(())
}

fn section(file: &mut impl Write, op: Op, count: u64) -> Result<()> {
    file.write_all(&op.code().to_le_bytes())?;
    file.write_all(&count.to_le_bytes())?;
    Ok(())
}

fn section_start(file: &mut (impl Write + Seek), op: Op, count: u64) -> Result<u64> {
    let at = file.stream_position()?;
    section(file, op, count)?;
    Ok(at)
}

fn range(file: &mut (impl Write + Seek), start: u64) -> Result<Range> {
    let end = file.stream_position()?;
    Ok(Range { start, len: end - start })
}

fn name_row(name: StreamName) -> NameRow {
    NameRow {
        range: name.range.get(),
        stream: name.stream.number(),
        created: name.created.number(),
    }
}

fn of_name(row: NameRow) -> Result<StreamName> {
    Ok(StreamName {
        range: RangeId::new(row.range),
        stream: StreamId::of_number(row.stream)?,
        created: TimeSec::new(row.created),
    })
}

fn write_ens(file: &mut (impl Write + Seek), disks: &[(EnId, DiskId)]) -> Result<Range> {
    let groups = disks
        .iter()
        .enumerate()
        .filter(|(at, (en, _))| *at == 0 || disks[*at - 1].0 != *en)
        .count();
    let start = section_start(file, Op::SnCkEns, groups as u64)?;
    let mut at = 0;
    while at < disks.len() {
        let en = disks[at].0;
        let mut values = Vec::new();
        while at < disks.len() && disks[at].0 == en {
            values.push(u64::from(disks[at].1.get()));
            at += 1;
        }
        write_piece(file, &EnRow { id: en.get(), disks: values })?;
    }
    range(file, start)
}

fn write_en_states(file: &mut (impl Write + Seek), state: &SNState) -> Result<Range> {
    let mut ens: Vec<_> = state.ens.iter().collect();
    ens.sort_unstable_by_key(|(id, _)| **id);
    let start = section_start(file, Op::SnCkEnStates, ens.len() as u64)?;
    for (&id, en) in ens {
        let mut disks: Vec<_> = en.disks.iter().collect();
        disks.sort_unstable_by_key(|(disk, _)| **disk);
        let disks = disks
            .into_iter()
            .map(|(&id, info)| EnDiskRow {
                id: u64::from(id.get()),
                total_bytes: info.total_bytes,
                used_bytes: info.used_bytes,
                status: info.status as u8,
            })
            .collect();
        let mut retired_disks: Vec<_> =
            en.retired_disks.iter().map(|id| u64::from(id.get())).collect();
        retired_disks.sort_unstable();
        write_piece(
            file,
            &EnStateRow {
                id: id.get(),
                last_disk_sync: en.last_disk_sync.map(TimeSec::number),
                last_extent_sync: en.last_extent_sync.map(TimeSec::number),
                disks,
                retired_disks,
            },
        )?;
    }
    range(file, start)
}

fn write_disk_usage(file: &mut (impl Write + Seek), state: &SNState) -> Result<Range> {
    let groups = state
        .disk_usage
        .iter()
        .enumerate()
        .filter(|(at, row)| *at == 0 || state.disk_usage[*at - 1].id.en() != row.id.en())
        .count();
    let start = section_start(file, Op::SnCkDiskUsage, groups as u64)?;
    let mut at = 0;
    while at < state.disk_usage.len() {
        let en = state.disk_usage[at].id.en();
        let mut disks = Vec::new();
        while at < state.disk_usage.len() && state.disk_usage[at].id.en() == en {
            let row: DiskUsage = state.disk_usage[at];
            disks.push(DiskUsageDiskRow {
                disk: u64::from(row.id.disk().get()),
                cumulative_usage: row.cumulative_usage,
                cumulative_total_space: row.cumulative_total_space,
                cumulative_adjusted_free_space: row.cumulative_adjusted_free_space,
            });
            at += 1;
        }
        write_piece(file, &DiskUsageEnRow { en: en.get(), disks })?;
    }
    range(file, start)
}

fn disk_indices(ids: &[SnDiskId], roster: &[(EnId, DiskId)]) -> Result<Vec<DiskIndex>> {
    ids.iter()
        .map(|id| {
            if roster.get(id.get() as usize).is_none() {
                return Err(Error::new("SN checkpoint disk index is outside its roster"));
            }
            Ok(DiskIndex(id.get()))
        })
        .collect()
}

fn reindex_disks(roster: Vec<(EnId, DiskId)>) -> Result<(Vec<(EnId, DiskId)>, Vec<SnDiskId>)> {
    if roster.len() > u32::MAX as usize {
        return Err(Error::new("SN checkpoint EN roster is too large"));
    }

    let mut indexed: Vec<_> = roster.into_iter().enumerate().collect();
    indexed.sort_unstable_by_key(|(_, (en, disk))| (en.get(), disk.get()));

    let mut disks = Vec::with_capacity(indexed.len());
    let mut old_to_new = vec![SnDiskId::NONE; indexed.len()];
    let mut previous = None;
    for (new_at, (old_at, name)) in indexed.into_iter().enumerate() {
        if previous == Some(name) {
            return Err(Error::new("duplicate SN checkpoint EN disk"));
        }
        previous = Some(name);
        let new_id = SnDiskId::new(
            u32::try_from(new_at)
                .map_err(|_| Error::new("SN checkpoint EN roster is too large"))?,
        );
        old_to_new[old_at] = new_id;
        disks.push(name);
    }
    Ok((disks, old_to_new))
}

fn remap_disks(ids: Vec<DiskIndex>, old_to_new: &[SnDiskId]) -> Result<Vec<SnDiskId>> {
    ids.into_iter()
        .map(|disk| {
            old_to_new
                .get(disk.0 as usize)
                .copied()
                .filter(|id| *id != SnDiskId::NONE)
                .ok_or_else(|| Error::new("SN checkpoint disk index is outside its roster"))
        })
        .collect()
}

fn ext_extra(
    extras: &SharedExtras<(ExtentId, TimeSec)>,
    extent: &Extent,
) -> Result<Option<ExtExtra>> {
    let extra = extras.get(&(extent.id, extent.created));
    if extent.deletion.extra() != extra.is_some() {
        return Err(Error::new("SN extent extra flag does not match its checkpoint data"));
    }
    Ok(extra.map(|extra| ExtExtra {
        until: extra.retain_until.number(),
        soft: u64::from(extra.references.soft.load(Ordering::Acquire)),
        hard: u64::from(extra.references.hard.load(Ordering::Acquire)),
        max_retention: i64::from(extra.references.max_retention.get()) as u64,
    }))
}

fn write_extents(
    file: &mut File,
    state: &SNState,
    disks: &[(EnId, DiskId)],
    workers: usize,
) -> Result<(Range, Vec<raft::checkpoint::ChunkPos>)> {
    let table = &state.extent_table;
    let extras = &state.extent_extras;
    let count = table.iter().filter(|extent| extent.deletion.is_valid()).count();
    if count as u64 != state.extents {
        return Err(Error::new("SN extent count does not match its table"));
    }
    let start = file.stream_position()?;
    section(file, Op::SnCkExtents, count as u64)?;
    let data_start = file.stream_position()?;
    let per_chunk = (CHUNK_BYTES / std::mem::size_of::<Extent>().max(1)).clamp(1, CHUNK_ITEMS);
    let mut ranges = Vec::new();
    for first in (0..table.len()).step_by(per_chunk) {
        let end = (first + per_chunk).min(table.len());
        if table[first..end].iter().any(|extent| extent.deletion.is_valid()) {
            ranges.push((first as u64, (end - first) as u64));
        }
    }
    let (chunks, end) = raft::checkpoint::write_chunks(
        file,
        data_start,
        workers,
        "SN extents",
        &ranges,
        CHUNK_MAX,
        CHUNK_ITEMS as u64,
        |first, span| {
            let first =
                usize::try_from(first).map_err(|_| Error::new("extent chunk index overflow"))?;
            let end = first
                .checked_add(
                    usize::try_from(span)
                        .map_err(|_| Error::new("extent chunk length overflow"))?,
                )
                .filter(|end| *end <= table.len())
                .ok_or_else(|| Error::new("extent chunk exceeds the table"))?;
            let mut rows = Vec::new();
            for extent in table[first..end].iter().filter(|extent| extent.deletion.is_valid()) {
                rows.push(ExtentRow {
                    id: u64::from(extent.id.get()),
                    flags: extent.deletion.bits(),
                    sealed: extent.sealed,
                    logical: extent.logical.number(),
                    compressed: extent.compressed.number(),
                    records: extent.records.number(),
                    created: extent.created.number(),
                    disks: disk_indices(&extent.disks, disks)?,
                    extra: ext_extra(extras, extent)?,
                });
            }
            let items = rows.len() as u64;
            Ok((items, encode_chunk(Op::SnCkExtChunk, &rows)?))
        },
    )?;
    file.seek(SeekFrom::Start(end))?;
    Ok((range(file, start)?, chunks))
}

fn write_disk_extent_indexes(
    file: &mut File,
    state: &SNState,
    workers: usize,
) -> Result<(Range, Vec<raft::checkpoint::ChunkPos>, u64)> {
    let indexes = &state.disk_extent_indexes;
    let mut disks: Vec<_> = indexes
        .ens
        .iter()
        .flat_map(|(&en, en_indexes)| en_indexes.disks.keys().map(move |&disk| (en, disk)))
        .collect();
    disks.sort_unstable();

    let mut ranges = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    let mut entries = 0u64;
    for disk in disks {
        let tree = indexes.tree(disk).expect("disk index collected from tree map");
        if tree.is_empty() {
            return Err(Error::new("SN disk extent index contains an empty tree"));
        }
        let tree_bytes = usize::try_from(tree.len())
            .ok()
            .and_then(|count| count.checked_mul(5))
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| Error::new("SN disk extent tree chunk size overflow"))?;
        if !current.is_empty() && current_bytes.saturating_add(tree_bytes) > CHUNK_BYTES {
            ranges.push(DiskExtentIndexRange { disks: std::mem::take(&mut current) });
            current_bytes = 0;
        }
        current.push(disk);
        current_bytes = current_bytes.saturating_add(tree_bytes);
        if current_bytes >= CHUNK_BYTES {
            ranges.push(DiskExtentIndexRange { disks: std::mem::take(&mut current) });
            current_bytes = 0;
        }
        entries = entries
            .checked_add(u64::from(tree.len()))
            .ok_or_else(|| Error::new("SN disk extent index count overflow"))?;
    }
    if !current.is_empty() {
        ranges.push(DiskExtentIndexRange { disks: current });
    }

    let start = file.stream_position()?;
    section(file, Op::SnCkDiskExtents, ranges.len() as u64)?;
    let data_start = file.stream_position()?;
    let chunk_ranges: Vec<_> = (0..ranges.len()).map(|part| (part as u64, 1)).collect();
    let (chunks, end) = raft::checkpoint::write_chunks(
        file,
        data_start,
        workers,
        "SN disk extent indexes",
        &chunk_ranges,
        CHUNK_MAX,
        CHUNK_ITEMS as u64,
        |first, span| {
            if span != 1 {
                return Err(Error::new("SN disk extent index chunk was split"));
            }
            let at = usize::try_from(first)
                .map_err(|_| Error::new("SN disk extent chunk index overflow"))?;
            let chunk_range = ranges
                .get(at)
                .ok_or_else(|| Error::new("SN disk extent chunk is outside its table"))?;
            let mut rows = Vec::with_capacity(chunk_range.disks.len());
            for &disk in &chunk_range.disks {
                let tree = indexes.tree(disk).ok_or_else(|| {
                    Error::new("SN disk extent tree disappeared during checkpoint")
                })?;
                let mut ids = Vec::with_capacity(tree.len() as usize);
                tree.for_each(&indexes.pool, |id| {
                    ids.push(ExtentIndex(id.get()));
                    true
                });
                if ids.len() != tree.len() as usize {
                    return Err(Error::new("SN disk extent tree changed during checkpoint"));
                }
                rows.push(DiskExtentIndexRow {
                    en: u64::from(disk.0.get()),
                    disk: u64::from(disk.1.get()),
                    ids,
                });
            }
            let count = rows.len() as u64;
            Ok((count, encode_chunk(Op::SnCkDiskExtentChunk, &rows)?))
        },
    )?;
    file.seek(SeekFrom::Start(end))?;
    Ok((range(file, start)?, chunks, entries))
}

fn stream_head(stream: &Stream, extra: Option<&Extra<()>>) -> Result<StreamHead> {
    if stream.deletion.extra() != extra.is_some() {
        return Err(Error::new("SN stream extra flag does not match its checkpoint data"));
    }
    Ok(StreamHead {
        range: stream.name.range.get(),
        stream: stream.name.stream.number(),
        created: stream.name.created.number(),
        flags: stream.deletion.bits(),
        creator: stream.creator.get(),
        sealed_len: u64::try_from(stream.sealed.len())
            .map_err(|_| Error::new("SN checkpoint stream extent list is too large"))?,
        open: stream.open.map(|id| u64::from(id.get())),
        sealed_records: stream.sealed_records.number(),
        sealed_stream: stream.sealed_stream,
        retain_until: extra.map(|extra| extra.retain_until.number()),
    })
}

fn write_stream(file: &mut impl Write, stream: &Stream, extra: Option<&Extra<()>>) -> Result<()> {
    write_piece(file, &stream_head(stream, extra)?)?;
    for ids in stream.sealed.chunks(REF_CHUNK) {
        let row = Refs { ids: ids.iter().map(|id| u64::from(id.get())).collect() };
        write_piece(file, &row)?;
    }
    Ok(())
}

fn encode_stream_chunk(state: &SNState, first: u64, span: u64) -> Result<(u64, Vec<u8>)> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&Op::SnCkStrChunk.code().to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    let mut items = 0u64;
    let end = first.checked_add(span).ok_or_else(|| Error::new("SN stream chunk overflow"))?;
    if span == 0 || end > state.stream_map.bucket_count() {
        return Err(Error::new("SN stream chunk is outside the hash table"));
    }
    for bucket in first..end {
        let count_at = bytes.len();
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let mut count = 0u64;
        let mut result = Ok(());
        state.stream_map.for_each_bucket(bucket, |stream| {
            if result.is_err() {
                return;
            }
            if let Err(error) =
                write_stream(&mut bytes, stream, state.stream_extras.get(&stream.name))
            {
                result = Err(error);
                return;
            }
            count = match count.checked_add(1) {
                Some(count) => count,
                None => {
                    result = Err(Error::new("SN checkpoint stream count overflow"));
                    return;
                }
            };
        });
        result?;
        bytes[count_at..count_at + 8].copy_from_slice(&count.to_le_bytes());
        items = items
            .checked_add(count)
            .ok_or_else(|| Error::new("SN checkpoint stream count overflow"))?;
    }
    bytes[2..10].copy_from_slice(&items.to_le_bytes());
    Ok((items, bytes))
}

fn write_streams(
    file: &mut File,
    state: &SNState,
    workers: usize,
) -> Result<(Range, Vec<raft::checkpoint::ChunkPos>)> {
    if state.stream_map.len() != state.streams {
        return Err(Error::new("SN stream count does not match its hash table"));
    }
    let buckets = state.stream_map.bucket_count();
    let start = file.stream_position()?;
    section(file, Op::SnCkStreams, state.streams)?;
    let data_start = file.stream_position()?;
    let item_bytes = std::mem::size_of::<Stream>().saturating_add(64).max(1);
    let target_items = (CHUNK_BYTES / item_bytes).clamp(1, CHUNK_ITEMS) as u64;
    let avg_items = state.streams.div_ceil(buckets).max(1);
    let per_chunk = (target_items / avg_items).max(1);
    let mut ranges = Vec::new();
    let mut first = 0u64;
    while first < buckets {
        let span = per_chunk.min(buckets - first);
        ranges.push((first, span));
        first += span;
    }
    let (chunks, end) = raft::checkpoint::write_chunks(
        file,
        data_start,
        workers,
        "SN streams",
        &ranges,
        CHUNK_MAX,
        CHUNK_ITEMS as u64,
        |first, span| encode_stream_chunk(state, first, span),
    )?;
    file.seek(SeekFrom::Start(end))?;
    let rows = range(file, start)?;
    Ok((rows, chunks))
}

fn write_retired(file: &mut (impl Write + Seek), state: &SNState) -> Result<Range> {
    let start = section_start(file, Op::SnCkRetired, state.retired_streams.len() as u64)?;
    for &name in &state.retired_streams {
        write_piece(file, &name_row(name))?;
    }
    range(file, start)
}

fn write_pending(
    file: &mut (impl Write + Seek),
    state: &SNState,
    disks: &[(EnId, DiskId)],
) -> Result<Range> {
    let start = section_start(file, Op::SnCkPending, state.pending_deletes.len() as u64)?;
    for pending in state.pending_deletes.values() {
        write_piece(
            file,
            &PendingHead {
                name: name_row(pending.name),
                hard: pending.hard,
                task_count: pending.tasks.len() as u64,
            },
        )?;
        for task in &pending.tasks {
            write_piece(
                file,
                &TaskRow {
                    id: u64::from(task.id.get()),
                    created: task.created.number(),
                    disks: disk_indices(&task.disks, disks)?,
                    interval: task.interval.get() as u64,
                    hard: task.hard,
                },
            )?;
        }
    }
    range(file, start)
}

/// Write the application state at `snapshot` to an empty checkpoint file.
pub(super) fn write(
    file: &mut File,
    raft_status: &raft::checkpoint::RaftStatus,
    state: &SNState,
    disks: &[(EnId, DiskId)],
    sequence: u64,
    second: u64,
    workers: usize,
) -> Result<()> {
    if file.seek(SeekFrom::End(0))? != 0 {
        return Err(Error::new("SN checkpoint target is not empty"));
    }
    let ens = write_ens(file, disks)?;
    let en_states = write_en_states(file, state)?;
    let disk_usage = write_disk_usage(file, state)?;
    let (disk_extent_rows, disk_extent_chunks, disk_extent_entries) =
        write_disk_extent_indexes(file, state, workers)?;
    let (extent_rows, extent_chunks) = write_extents(file, state, disks, workers)?;
    let (stream_rows, stream_chunks) = write_streams(file, state, workers)?;
    let stream_buckets = state.stream_map.bucket_count();
    let retired = write_retired(file, state)?;
    let pending = write_pending(file, state, disks)?;
    let footer = encode_piece(&Footer {
        raft: raft_status.clone(),
        sequence,
        second,
        streams: state.streams,
        stream_buckets,
        extents: state.extents,
        extent_slots: ExtentId::of_number(state.extent_table.len() as u64)?,
        ens,
        en_states,
        disk_usage,
        disk_usage_built_at: state.disk_usage_built_at.map(TimeSec::number),
        disk_extent_entries,
        disk_extent_rows,
        disk_extent_chunks,
        extent_rows,
        extent_chunks,
        stream_rows,
        stream_chunks,
        retired,
        pending,
    })?;
    raft::checkpoint::write_footer(file, Op::SnCkFooter, &footer)?;
    Ok(())
}

/// Replace Raft's generic empty base snapshot with an empty SN checkpoint.
pub(super) fn write_bootstrap(
    path: &Path,
    raft_status: &raft::checkpoint::RaftStatus,
    state: &SNState,
    disks: &[(EnId, DiskId)],
    sequence: u64,
    second: u64,
    workers: usize,
) -> Result<u64> {
    let parent =
        path.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp = parent.join(format!(".sn-ck-{}-{stamp}.tmp", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        write(&mut file, raft_status, state, disks, sequence, second, workers)?;
        file.sync_all()?;
        let bytes = file.metadata()?.len();
        drop(file);
        std::fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(bytes)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn checked_end(range: Range) -> Result<u64> {
    range.start.checked_add(range.len).ok_or_else(|| Error::new("SN checkpoint range overflow"))
}

fn read_public(file: &mut (impl Read + Seek)) -> Result<(Footer, u64)> {
    let fixed = raft::checkpoint::read_footer(file)?;
    if fixed.op != Op::SnCkFooter {
        return Err(Error::new("wrong SN checkpoint footer"));
    }
    let footer: Footer = decode_piece(&fixed.bytes)?;
    footer.raft.validate()?;
    check_chunks(footer.disk_extent_rows, &footer.disk_extent_chunks)?;
    check_chunks(footer.extent_rows, &footer.extent_chunks)?;
    check_chunks(footer.stream_rows, &footer.stream_chunks)?;
    let ranges = [
        footer.ens,
        footer.en_states,
        footer.disk_usage,
        footer.disk_extent_rows,
        footer.extent_rows,
        footer.stream_rows,
        footer.retired,
        footer.pending,
    ];
    let mut end = 0;
    for part in ranges {
        if part.start != end || part.len < 10 {
            return Err(Error::new("invalid SN checkpoint section ranges"));
        }
        end = checked_end(part)?;
    }
    if end != fixed.start {
        return Err(Error::new("SN checkpoint sections do not reach the footer"));
    }
    Ok((footer, fixed.start))
}

/// Read an SN checkpoint into Raft state; only the empty bootstrap is generic.
pub(super) fn read_raft(
    path: &Path,
    entry: u64,
    ring: &str,
    ignore: bool,
) -> Result<(raft::State, u64, u64, u64)> {
    let mut file = File::open(path)?;
    let is_sn =
        raft::checkpoint::read_footer(&mut file).is_ok_and(|footer| footer.op == Op::SnCkFooter);
    if !is_sn {
        if entry != 0 {
            return Err(Error::new("SN checkpoint is missing its application footer"));
        }
        return raft::journal::read_checkpoint(path, entry, ring, ignore);
    }
    let (footer, _) = read_public(&mut file)?;
    let status = footer.raft;
    if status.index != entry || (!ignore && status.ring != ring) {
        return Err(Error::new("SN checkpoint ring or sequence mismatch"));
    }
    let state = restore_raft(status, ring, ignore)?;
    let length = file.metadata()?.len();
    Ok((state, footer.sequence, footer.second, length))
}

fn restore_raft(
    status: raft::checkpoint::RaftStatus,
    ring: &str,
    ignore: bool,
) -> Result<raft::State> {
    let mut state = raft::State::initial(
        status.ring.clone(),
        status.membership.old.clone(),
        status.membership.identities.clone(),
    )?;
    let snapshot = status.snapshot();
    state.generation = 0;
    state.max_scn = status.max_scn;
    state.founders = status.founders;
    state.contacts = status.contacts;
    state.retirements = status.retirements;
    state.term = status.hard_term;
    state.vote = status.vote;
    state.vote_scn = status.vote_scn;
    state.snapshot = snapshot;
    state.commit = state.snapshot.index;
    state.ignore_ring_identity = ignore;
    if ignore {
        state.ring = ring.to_owned();
        state.snapshot.ring = ring.to_owned();
    }
    state.check_ring(&state.snapshot)?;
    state.validate()?;
    Ok(state)
}

struct SectionRead {
    count: u64,
    left: u64,
}

fn read_section(file: &mut File, range: Range, op: Op) -> Result<SectionRead> {
    file.seek(SeekFrom::Start(range.start))?;
    let mut tag = [0; 2];
    file.read_exact(&mut tag)?;
    if u16::from_le_bytes(tag) != op.code() {
        return Err(Error::new(format!("wrong SN checkpoint section {op}")));
    }
    let count = read_u64(file)?;
    Ok(SectionRead { count, left: range.len - 10 })
}

fn read_u64(file: &mut impl Read) -> Result<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_piece<T: codec::Wire>(file: &mut impl Read, left: &mut u64) -> Result<T> {
    *left = left.checked_sub(8).ok_or_else(|| Error::new("truncated SN checkpoint row"))?;
    let size = read_u64(file)?;
    *left = left.checked_sub(size).ok_or_else(|| Error::new("truncated SN checkpoint row"))?;
    let size = usize::try_from(size).map_err(|_| Error::new("SN checkpoint row too large"))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|_| Error::new("SN checkpoint allocation failed"))?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)?;
    decode_piece(&bytes)
}

fn read_ens(file: &mut File, range: Range) -> Result<Vec<(EnId, DiskId)>> {
    let mut section = read_section(file, range, Op::SnCkEns)?;
    let mut disks = Vec::new();
    for _ in 0..section.count {
        let row: EnRow = read_piece(file, &mut section.left)?;
        let en = EnId::new(row.id);
        for disk in row.disks {
            disks.push((en, DiskId::of_number(disk)?));
        }
    }
    if section.left != 0 || disks.len() > u32::MAX as usize {
        return Err(Error::new("invalid SN checkpoint EN roster"));
    }
    Ok(disks)
}

fn read_en_states(
    file: &mut File,
    range: Range,
    state: &mut SNState,
    disk_roster: &[(EnId, DiskId)],
) -> Result<()> {
    let mut section = read_section(file, range, Op::SnCkEnStates)?;
    let known_disks: HashSet<_> = disk_roster.iter().copied().collect();
    for _ in 0..section.count {
        let row: EnStateRow = read_piece(file, &mut section.left)?;
        let id = EnId::of_number(row.id)?;
        let mut en = EnInfo {
            last_disk_sync: row.last_disk_sync.map(TimeSec::new),
            last_extent_sync: row.last_extent_sync.map(TimeSec::new),
            ..EnInfo::default()
        };
        for disk in row.disks {
            let disk_id = DiskId::of_number(disk.id)?;
            if !known_disks.contains(&(id, disk_id)) {
                return Err(Error::new("SN checkpoint EN disk state is outside the disk roster"));
            }
            let info = EnDiskInfo {
                total_bytes: disk.total_bytes,
                used_bytes: disk.used_bytes,
                status: EnDiskStatus::from_code(disk.status)?,
            };
            if en.disks.insert(disk_id, info).is_some() {
                return Err(Error::new("duplicate SN checkpoint EN disk state"));
            }
        }
        for disk in row.retired_disks {
            let disk_id = DiskId::of_number(disk)?;
            if !known_disks.contains(&(id, disk_id)) {
                return Err(Error::new("SN checkpoint retired disk is outside the disk roster"));
            }
            if en.disks.contains_key(&disk_id) || !en.retired_disks.insert(disk_id) {
                return Err(Error::new("duplicate or active SN checkpoint retired disk"));
            }
        }
        if state.ens.insert(id, en).is_some() {
            return Err(Error::new("duplicate SN checkpoint EN state"));
        }
    }
    if section.left != 0 {
        return Err(Error::new("trailing SN checkpoint EN state data"));
    }
    Ok(())
}

fn read_disk_usage(
    file: &mut File,
    range: Range,
    built_at: Option<u64>,
    state: &mut SNState,
    disk_roster: &[(EnId, DiskId)],
) -> Result<()> {
    let mut section = read_section(file, range, Op::SnCkDiskUsage)?;
    if built_at.is_none() && section.count != 0 {
        return Err(Error::new("SN checkpoint disk usage has no build timestamp"));
    }
    let known_disks: HashSet<_> = disk_roster.iter().copied().collect();
    let mut previous_en = None;
    let mut previous_id = None;
    let mut stored_cumulative_usage = 0.0;
    let mut stored_cumulative_total_space = 0.0;
    let mut stored_cumulative_adjusted_free_space = 0.0;
    let mut cumulative_usage = 0.0;
    let mut cumulative_total_space = 0.0;
    let mut cumulative_adjusted_free_space = 0.0;
    let mut available_disks = 0;
    for _ in 0..section.count {
        let row: DiskUsageEnRow = read_piece(file, &mut section.left)?;
        let en = EnId::of_number(row.en)?;
        if row.disks.is_empty() || previous_en.is_some_and(|previous| previous >= en) {
            return Err(Error::new("invalid SN checkpoint disk usage EN ordering"));
        }
        previous_en = Some(en);
        for disk_row in row.disks {
            let disk = DiskId::of_number(disk_row.disk)?;
            let id = SnEnDiskId::new(en, disk);
            if !known_disks.contains(&(en, disk))
                || previous_id.is_some_and(|previous| previous >= id)
            {
                return Err(Error::new("invalid SN checkpoint disk usage disk ordering"));
            }
            if disk_row.cumulative_usage < stored_cumulative_usage
                || disk_row.cumulative_total_space < stored_cumulative_total_space
                || disk_row.cumulative_adjusted_free_space < stored_cumulative_adjusted_free_space
            {
                return Err(Error::new("SN checkpoint disk usage cumulatives decrease"));
            }

            // Older checkpoint tables may include unavailable disks. Filter
            // them using the restored EN state and rebuild cumulatives over
            // the remaining disks only.
            let usage = disk_row.cumulative_usage - stored_cumulative_usage;
            let total_space = disk_row.cumulative_total_space - stored_cumulative_total_space;
            let adjusted_free =
                disk_row.cumulative_adjusted_free_space - stored_cumulative_adjusted_free_space;
            stored_cumulative_usage = disk_row.cumulative_usage;
            stored_cumulative_total_space = disk_row.cumulative_total_space;
            stored_cumulative_adjusted_free_space = disk_row.cumulative_adjusted_free_space;

            let ready = state
                .ens
                .get(&en)
                .and_then(|en| en.disks.get(&disk))
                .is_some_and(|disk| disk.status == EnDiskStatus::Ready);
            if ready {
                // A ready disk keeps a one-byte minimum weight, even when the
                // checkpoint predates that minimum or reports no free space.
                let adjusted_free = adjusted_free.max(1.0);
                available_disks += 1;
                cumulative_usage += usage;
                cumulative_total_space += total_space;
                cumulative_adjusted_free_space += adjusted_free;
                state.disk_usage.push(DiskUsage {
                    id,
                    cumulative_usage,
                    cumulative_total_space,
                    cumulative_adjusted_free_space,
                });
            }
            previous_id = Some(id);
        }
    }
    if section.left != 0 {
        return Err(Error::new("trailing SN checkpoint disk usage data"));
    }
    state.disk_usage_built_at = built_at.map(TimeSec::new);
    state.disk_usage_available_disks = available_disks;
    Ok(())
}

fn restore_extent(
    table: &ExtentTable,
    row: ExtentRow,
    table_len: usize,
    old_to_new: &[SnDiskId],
) -> Result<Option<((ExtentId, TimeSec), Extra<ReferenceCounts>)>> {
    if row.id >= table_len as u64 {
        return Err(Error::new("SN checkpoint extent is beyond the table length"));
    }
    let id = ExtentId::of_number(row.id)?;
    let flags = Flags::from_bits(row.flags)?;
    if !flags.is_valid() || row.extra.is_some() != flags.extra() {
        return Err(Error::new("invalid SN checkpoint extent flags"));
    }
    let disks = remap_disks(row.disks, old_to_new)?;
    let created = TimeSec::new(row.created);
    let extent = ExtentData {
        deletion: flags,
        sealed: row.sealed,
        logical: ELoff::of_number(row.logical)?,
        compressed: ECoff::of_number(row.compressed)?,
        id,
        records: ERoff::of_number(row.records)?,
        created,
        disks,
    };
    table.restore(id, Extent::from_data(extent))?;
    let extra = if let Some(extra) = row.extra {
        let soft =
            u32::try_from(extra.soft).map_err(|_| Error::new("checkpoint soft refs overflow"))?;
        let hard =
            u32::try_from(extra.hard).map_err(|_| Error::new("checkpoint hard refs overflow"))?;
        let max_retention = extra.max_retention as i64;
        let max_retention = i32::try_from(max_retention)
            .map_err(|_| Error::new("checkpoint retention overflow"))?;
        if soft > hard {
            return Err(Error::new("invalid checkpoint extent reference counts"));
        }
        let value = Extra {
            retain_until: TimeSec::new(extra.until),
            references: ReferenceCounts {
                soft: AtomicU32::new(soft),
                hard: AtomicU32::new(hard),
                max_retention: TimeIntSec32::new(max_retention),
            },
        };
        Some(((id, created), value))
    } else {
        None
    };
    Ok(extra)
}

fn read_extents(
    file: &mut File,
    range: Range,
    chunks: &[raft::checkpoint::ChunkPos],
    state: &mut SNState,
    table_len: usize,
    old_to_new: &[SnDiskId],
    expected: u64,
    workers: usize,
) -> Result<()> {
    let section = read_section(file, range, Op::SnCkExtents)?;
    check_chunks(range, chunks)?;
    let items = chunks.iter().try_fold(0u64, |sum, chunk| {
        sum.checked_add(chunk.items)
            .ok_or_else(|| Error::new("SN checkpoint extent count overflow"))
    })?;
    if items != section.count || section.count != expected {
        return Err(Error::new("SN checkpoint extent count does not match its chunks or footer"));
    }
    if chunks.iter().any(|chunk| chunk.items > CHUNK_ITEMS as u64 && chunk.span > 1) {
        return Err(Error::new("SN checkpoint extent chunk exceeds its item limit"));
    }
    let extras = Mutex::new(Vec::new());
    let table = &state.extent_table;
    raft::checkpoint::read_chunks(&*file, chunks, workers, "SN extents", |chunk, bytes| {
        let chunk_end = chunk
            .first
            .checked_add(chunk.span)
            .filter(|end| *end <= table_len as u64)
            .ok_or_else(|| Error::new("SN checkpoint extent chunk exceeds its table"))?;
        let rows: Vec<ExtentRow> = decode_chunk(chunk, &bytes, Op::SnCkExtChunk)?;
        let mut found = Vec::new();
        for row in rows {
            if row.id < chunk.first || row.id >= chunk_end {
                return Err(Error::new("SN checkpoint extent is outside its chunk range"));
            }
            if let Some(extra) = restore_extent(table, row, table_len, old_to_new)? {
                found.push(extra);
            }
        }
        extras.lock().map_err(|_| Error::new("SN checkpoint extra queue poisoned"))?.extend(found);
        Ok(())
    })?;
    let extras =
        extras.into_inner().map_err(|_| Error::new("SN checkpoint extra queue poisoned"))?;
    for (key, value) in extras {
        if state.extent_extras.restore(key, value).is_some() {
            return Err(Error::new("duplicate SN checkpoint extent extra"));
        }
    }
    Ok(())
}

fn read_disk_extent_indexes(
    file: &mut File,
    range: Range,
    chunks: &[raft::checkpoint::ChunkPos],
    state: &mut SNState,
    disk_roster: &[(EnId, DiskId)],
    expected_entries: u64,
    workers: usize,
) -> Result<Vec<DiskExtentMismatch>> {
    let section = read_section(file, range, Op::SnCkDiskExtents)?;
    check_chunks(range, chunks)?;
    if section.count != chunks.len() as u64
        || chunks.iter().any(|chunk| chunk.items == 0 || chunk.items > CHUNK_ITEMS as u64)
    {
        return Err(Error::new("SN disk extent index count does not match its chunks"));
    }

    let mut mismatches = Vec::new();
    let known_disks: HashSet<_> = disk_roster.iter().copied().collect();

    // A checkpoint chunk contains whole disk trees. Its worker owns each tree
    // from creation through completion, so no tree mutex is needed. Workers
    // share only the central allocator lock when they need an extra node batch.
    let (sender, receiver) = std::sync::mpsc::channel();
    {
        let tree_pool = &state.disk_extent_indexes.pool;
        let extent_table = &state.extent_table;
        raft::checkpoint::read_chunks(
            &*file,
            chunks,
            workers,
            "SN disk extent indexes",
            |chunk, bytes| {
                let rows: Vec<DiskExtentIndexRow> =
                    decode_chunk(chunk, &bytes, Op::SnCkDiskExtentChunk)?;
                if rows.is_empty() {
                    return Err(Error::new("SN disk extent chunk contains no trees"));
                }
                for row in rows {
                    let disk_name = (EnId::of_number(row.en)?, DiskId::of_number(row.disk)?);
                    let mut row_mismatches = Vec::new();
                    if row.ids.is_empty() {
                        row_mismatches.push(DiskExtentMismatch::new(
                            DiskExtentMismatchKind::EmptyTree,
                            Some(disk_name),
                            None,
                        ));
                    }
                    if row.ids.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                        row_mismatches.push(DiskExtentMismatch::new(
                            DiskExtentMismatchKind::UnsortedIds,
                            Some(disk_name),
                            None,
                        ));
                    }
                    if !known_disks.contains(&disk_name) {
                        row_mismatches.push(DiskExtentMismatch::new(
                            DiskExtentMismatchKind::UnknownDisk,
                            Some(disk_name),
                            None,
                        ));
                    }
                    let mut tree = DiskExtentTree::new();
                    let estimate = DiskExtentTree::checkpoint_node_estimate(row.ids.len() as u64)?;
                    let mut nodes = tree_pool.reserve_nodes(estimate)?;
                    for item in &row.ids {
                        if item.0 == ExtentId::NONE.get() {
                            row_mismatches.push(DiskExtentMismatch::new(
                                DiskExtentMismatchKind::NoneExtentId,
                                Some(disk_name),
                                Some(item.0),
                            ));
                            continue;
                        }
                        match extent_table
                            .get(item.0 as usize)
                            .filter(|extent| extent.deletion.is_valid())
                        {
                            None => row_mismatches.push(DiskExtentMismatch::new(
                                DiskExtentMismatchKind::MissingExtent,
                                Some(disk_name),
                                Some(item.0),
                            )),
                            Some(extent)
                                if !extent.disks.iter().any(|sn_disk| {
                                    disk_roster
                                        .get(sn_disk.get() as usize)
                                        .is_some_and(|name| *name == disk_name)
                                }) =>
                            {
                                row_mismatches.push(DiskExtentMismatch::new(
                                    DiskExtentMismatchKind::WrongDisk,
                                    Some(disk_name),
                                    Some(item.0),
                                ));
                            }
                            Some(_) => {}
                        }
                        if !tree.insert_with_reservation(
                            tree_pool,
                            ExtentId::new(item.0),
                            &mut nodes,
                        )? {
                            row_mismatches.push(DiskExtentMismatch::new(
                                DiskExtentMismatchKind::DuplicateExtentId,
                                Some(disk_name),
                                Some(item.0),
                            ));
                        }
                    }
                    tree_pool.return_nodes(&mut nodes)?;
                    let row_entries = u64::try_from(row.ids.len())
                        .map_err(|_| Error::new("SN disk extent index row is too large"))?;
                    sender
                        .send((disk_name, tree, row_mismatches, row_entries))
                        .map_err(|_| Error::new("SN disk extent index result receiver closed"))?;
                }
                Ok(())
            },
        )?;
    }
    drop(sender);

    let mut disks_seen = HashSet::<(EnId, DiskId)>::with_capacity(disk_roster.len());
    let mut actual_entries = 0u64;
    for (disk, mut tree, mut row_mismatches, row_entries) in receiver {
        actual_entries = actual_entries
            .checked_add(row_entries)
            .ok_or_else(|| Error::new("SN disk extent index entry count overflow"))?;
        mismatches.append(&mut row_mismatches);
        if !disks_seen.insert(disk) {
            mismatches.push(DiskExtentMismatch::new(
                DiskExtentMismatchKind::DuplicateTree,
                Some(disk),
                None,
            ));
            tree.clear(&mut state.disk_extent_indexes.pool);
            continue;
        }
        state.disk_extent_indexes.insert_tree(disk, tree)?;
    }
    if actual_entries != expected_entries {
        mismatches.push(DiskExtentMismatch::new(
            DiskExtentMismatchKind::FooterEntryCount,
            None,
            None,
        ));
    }
    Ok(mismatches)
}

/// Compare the extent rows back to their per-disk trees using the same
/// ExtentId ranges as the checkpoint chunks. Each worker scans disjoint ranges
/// and returns its own inconsistency list.
fn check_extent_chunks_against_disk_indexes(
    state: &SNState,
    chunks: &[raft::checkpoint::ChunkPos],
    disk_roster: &[(EnId, DiskId)],
    workers: usize,
) -> Result<Vec<DiskExtentMismatch>> {
    if workers == 0 {
        return Err(Error::new("SN checkpoint validation needs at least one worker"));
    }
    let table_len = state.extent_table.len();
    for chunk in chunks {
        let end = chunk
            .first
            .checked_add(chunk.span)
            .filter(|end| *end <= table_len as u64)
            .ok_or_else(|| Error::new("SN extent validation chunk exceeds its table"))?;
        if chunk.span == 0 || end <= chunk.first {
            return Err(Error::new("SN extent validation chunk is empty"));
        }
    }

    let next = AtomicUsize::new(0);
    let worker_count = workers.min(chunks.len());
    if worker_count == 0 {
        return Ok(Vec::new());
    }
    std::thread::scope(|scope| -> Result<Vec<DiskExtentMismatch>> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let next = &next;
            let chunks = chunks;
            let table = &state.extent_table;
            let indexes = &state.disk_extent_indexes;
            handles.push(scope.spawn(move || -> Result<Vec<DiskExtentMismatch>> {
                let mut mismatches = Vec::new();
                loop {
                    let chunk_at = next.fetch_add(1, Ordering::Relaxed);
                    let Some(chunk) = chunks.get(chunk_at) else { break };
                    let first = usize::try_from(chunk.first)
                        .map_err(|_| Error::new("SN extent validation offset is too large"))?;
                    let end = usize::try_from(chunk.first + chunk.span)
                        .map_err(|_| Error::new("SN extent validation limit is too large"))?;
                    for extent in &table[first..end] {
                        if !extent.deletion.is_valid() {
                            continue;
                        }
                        for &sn_disk in &extent.disks {
                            let disk =
                                *disk_roster.get(sn_disk.get() as usize).ok_or_else(|| {
                                    Error::new("SN extent disk index is outside its roster")
                                })?;
                            if !indexes.contains(disk, extent.id) {
                                mismatches.push(DiskExtentMismatch::new(
                                    DiskExtentMismatchKind::ExtentNotIndexed,
                                    Some(disk),
                                    Some(extent.id.get()),
                                ));
                            }
                        }
                    }
                }
                Ok(mismatches)
            }));
        }

        let mut mismatches = Vec::new();
        for handle in handles {
            let mut worker_mismatches = handle
                .join()
                .map_err(|_| Error::new("SN checkpoint extent validation worker panicked"))??;
            mismatches.append(&mut worker_mismatches);
        }
        Ok(mismatches)
    })
}

fn verify_or_repair_disk_extent_indexes(
    state: &mut SNState,
    chunks: &[raft::checkpoint::ChunkPos],
    disk_roster: &[(EnId, DiskId)],
    workers: usize,
    repair: bool,
    mut mismatches: Vec<DiskExtentMismatch>,
) -> Result<()> {
    mismatches.extend(check_extent_chunks_against_disk_indexes(
        state,
        chunks,
        disk_roster,
        workers,
    )?);
    if mismatches.is_empty() {
        return Ok(());
    }

    common::logw!(
        "SN checkpoint disk extent index inconsistent",
        "count {}, first {:?}, disk {:?}, extent {:?}, repair {}",
        mismatches.len(),
        mismatches[0].kind,
        mismatches[0].disk,
        mismatches[0].extent,
        repair
    );
    if !repair {
        return Err(Error::new(format!(
            "SN checkpoint EN disk extent indexes contain {} inconsistency(s); first {:?} disk {:?} extent {:?}",
            mismatches.len(),
            mismatches[0].kind,
            mismatches[0].disk,
            mismatches[0].extent
        )));
    }

    // Extent rows are the source of truth for this derived reverse index.
    // Complete both parallel scans before rebuilding it single-threadedly.
    let SNState { extent_table, disk_extent_indexes, .. } = state;
    disk_extent_indexes.clear();
    for extent in extent_table.iter().filter(|extent| extent.deletion.is_valid()) {
        for &sn_disk in &extent.disks {
            let disk = *disk_roster
                .get(sn_disk.get() as usize)
                .ok_or_else(|| Error::new("SN extent disk index is outside its roster"))?;
            if !disk_extent_indexes.insert_one(disk, extent.id)? {
                return Err(Error::new("SN extent index repair found a duplicate instance"));
            }
        }
    }
    Ok(())
}

fn read_u64_left(file: &mut impl Read, left: &mut u64) -> Result<u64> {
    *left = left.checked_sub(8).ok_or_else(|| Error::new("truncated SN checkpoint bucket"))?;
    read_u64(file)
}

fn read_stream(
    file: &mut impl Read,
    left: &mut u64,
) -> Result<(Stream, Option<(StreamName, Extra<()>)>)> {
    let row: StreamHead = read_piece(file, left)?;
    let name = of_name(NameRow { range: row.range, stream: row.stream, created: row.created })?;
    let flags = Flags::from_bits(row.flags)?;
    if !flags.is_valid() || row.retain_until.is_some() != flags.extra() {
        return Err(Error::new("invalid SN checkpoint stream flags"));
    }
    let sealed_len = usize::try_from(row.sealed_len)
        .map_err(|_| Error::new("SN checkpoint stream extent list is too large"))?;
    let mut sealed = Vec::new();
    sealed
        .try_reserve_exact(sealed_len)
        .map_err(|_| Error::new("SN checkpoint stream extent allocation failed"))?;
    let mut received = 0usize;
    while received < sealed_len {
        let refs: Refs = read_piece(file, left)?;
        if refs.ids.is_empty() || refs.ids.len() > sealed_len - received {
            return Err(Error::new("invalid SN checkpoint stream extent chunk"));
        }
        received += refs.ids.len();
        for id in refs.ids {
            sealed.push(ExtentId::of_number(id)?);
        }
    }
    let mut stream = Stream::with_flags(name, SnId::new(row.creator), flags);
    stream.sealed = sealed;
    stream.open = row.open.map(ExtentId::of_number).transpose()?;
    stream.sealed_records = SRoff::of_number(row.sealed_records)?;
    stream.sealed_stream = row.sealed_stream;
    let extra = row
        .retain_until
        .map(|until| (name, Extra { retain_until: TimeSec::new(until), references: () }));
    Ok((stream, extra))
}

fn read_streams(
    file: &mut File,
    range: Range,
    chunks: &[raft::checkpoint::ChunkPos],
    state: &mut SNState,
    expected: u64,
    workers: usize,
) -> Result<()> {
    let section = read_section(file, range, Op::SnCkStreams)?;
    check_chunks(range, chunks)?;
    let items = chunks.iter().try_fold(0u64, |sum, chunk| {
        sum.checked_add(chunk.items)
            .ok_or_else(|| Error::new("SN checkpoint stream count overflow"))
    })?;
    if items != section.count || items != expected {
        return Err(Error::new("SN checkpoint stream count does not match its chunks"));
    }
    if chunks.iter().any(|chunk| chunk.items > CHUNK_ITEMS as u64 && chunk.span > 1) {
        return Err(Error::new("SN checkpoint stream chunk exceeds its item limit"));
    }
    let mut slots = state
        .stream_map
        .begin_restore(
            usize::try_from(items)
                .map_err(|_| Error::new("SN checkpoint stream table is too large"))?,
        )
        .map_err(|error| Error::new(format!("reserve SN checkpoint streams: {error}")))?;
    let mut ordered = chunks.to_vec();
    ordered.sort_unstable_by_key(|chunk| chunk.first);
    let mut starts = std::collections::HashMap::with_capacity(ordered.len());
    let mut slot_at = 0usize;
    for chunk in &ordered {
        starts.insert(chunk.first, slot_at);
        slot_at = slot_at
            .checked_add(
                usize::try_from(chunk.items)
                    .map_err(|_| Error::new("SN checkpoint stream chunk is too large"))?,
            )
            .ok_or_else(|| Error::new("SN checkpoint stream slot range overflow"))?;
    }
    if slot_at != slots.len() {
        return Err(Error::new("SN checkpoint stream slot count mismatch"));
    }
    let extras = Mutex::new(Vec::new());
    let restored = AtomicU64::new(0);
    let table = &state.stream_map;
    raft::checkpoint::read_chunks(&*file, chunks, workers, "SN streams", |chunk, bytes| {
        let chunk_end = chunk
            .first
            .checked_add(chunk.span)
            .filter(|end| *end <= table.bucket_count())
            .ok_or_else(|| Error::new("SN checkpoint stream chunk exceeds its hash table"))?;
        if bytes.len() < 10 || chunk.plain < 10 {
            return Err(Error::new("truncated SN checkpoint stream chunk"));
        }
        let mut input = Cursor::new(bytes.as_slice());
        let mut tag = [0; 2];
        input.read_exact(&mut tag)?;
        if u16::from_le_bytes(tag) != Op::SnCkStrChunk.code() {
            return Err(Error::new("wrong SN checkpoint stream chunk"));
        }
        let count = read_u64(&mut input)?;
        if count != chunk.items {
            return Err(Error::new("SN checkpoint stream chunk item count mismatch"));
        }
        let mut left = chunk.plain - 10;
        let mut at = *starts
            .get(&chunk.first)
            .ok_or_else(|| Error::new("SN checkpoint stream chunk has no slot range"))?;
        let slot_end = at
            .checked_add(
                usize::try_from(chunk.items)
                    .map_err(|_| Error::new("SN checkpoint stream chunk is too large"))?,
            )
            .ok_or_else(|| Error::new("SN checkpoint stream slot range overflow"))?;
        let mut loaded = 0u64;
        let mut found = Vec::new();
        for bucket in chunk.first..chunk_end {
            let bucket_count = read_u64_left(&mut input, &mut left)?;
            let bucket_count = usize::try_from(bucket_count)
                .map_err(|_| Error::new("SN checkpoint bucket is too large"))?;
            let mut made = 0usize;
            // SAFETY: chunk ranges and assigned slots are disjoint across workers.
            let mut next_error = None;
            let got = unsafe {
                table.restore_bucket_with(bucket, || {
                    if made == bucket_count || next_error.is_some() {
                        return Ok(None);
                    }
                    let (stream, extra) = match read_stream(&mut input, &mut left) {
                        Ok(value) => value,
                        Err(error) => {
                            next_error = Some(error);
                            return Ok(None);
                        }
                    };
                    if at >= slot_end {
                        next_error =
                            Some(Error::new("SN checkpoint stream chunk exceeded its slot range"));
                        return Ok(None);
                    }
                    let Some(&index) = slots.get(at) else {
                        next_error = Some(Error::new("SN checkpoint stream slot is missing"));
                        return Ok(None);
                    };
                    at += 1;
                    made += 1;
                    if let Some(extra) = extra {
                        found.push(extra);
                    }
                    Ok(Some((index, stream)))
                })
            }
            .map_err(|error| Error::new(format!("restore SN stream bucket: {error}")))?;
            if let Some(error) = next_error {
                return Err(error);
            }
            if got != bucket_count as u64 {
                return Err(Error::new("SN checkpoint bucket count mismatch"));
            }
            loaded = loaded
                .checked_add(got)
                .ok_or_else(|| Error::new("SN checkpoint stream count overflow"))?;
        }
        if left != 0 || loaded != chunk.items || at != slot_end {
            return Err(Error::new("trailing or missing SN checkpoint stream data"));
        }
        extras.lock().map_err(|_| Error::new("SN checkpoint extra queue poisoned"))?.extend(found);
        restored.fetch_add(loaded, Ordering::Relaxed);
        Ok(())
    })?;
    let loaded = restored.load(Ordering::Relaxed);
    state
        .stream_map
        .finish_restore(loaded)
        .map_err(|error| Error::new(format!("finish SN stream restore: {error}")))?;
    let extras =
        extras.into_inner().map_err(|_| Error::new("SN checkpoint extra queue poisoned"))?;
    for (name, value) in extras {
        if state.stream_extras.restore(name, value).is_some() {
            return Err(Error::new("duplicate SN checkpoint stream extra"));
        }
    }
    slots.clear();
    Ok(())
}

fn read_retired(file: &mut File, range: Range, state: &mut SNState) -> Result<()> {
    let mut section = read_section(file, range, Op::SnCkRetired)?;
    for _ in 0..section.count {
        let row: NameRow = read_piece(file, &mut section.left)?;
        if !state.retired_streams.insert(of_name(row)?) {
            return Err(Error::new("duplicate retired SN checkpoint stream"));
        }
    }
    if section.left != 0 {
        return Err(Error::new("trailing SN checkpoint retired-stream data"));
    }
    Ok(())
}

fn read_pending(
    file: &mut File,
    range: Range,
    state: &mut SNState,
    old_to_new: &[SnDiskId],
) -> Result<()> {
    let mut section = read_section(file, range, Op::SnCkPending)?;
    for _ in 0..section.count {
        let row: PendingHead = read_piece(file, &mut section.left)?;
        let name = of_name(row.name)?;
        let count = usize::try_from(row.task_count)
            .map_err(|_| Error::new("SN checkpoint pending task count overflow"))?;
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(count)
            .map_err(|_| Error::new("SN checkpoint pending task allocation failed"))?;
        for _ in 0..count {
            let task: TaskRow = read_piece(file, &mut section.left)?;
            tasks.push(DeleteTask {
                id: ExtentId::of_number(task.id)?,
                created: TimeSec::new(task.created),
                disks: remap_disks(task.disks, old_to_new)?,
                interval: TimeIntSec64::new(task.interval as i64),
                hard: task.hard,
            });
        }
        let pending = PendingDelete { name, hard: row.hard, tasks };
        if state.pending_deletes.insert(name, pending).is_some() {
            return Err(Error::new("duplicate SN checkpoint pending delete"));
        }
    }
    if section.left != 0 {
        return Err(Error::new("trailing SN checkpoint pending-delete data"));
    }
    Ok(())
}

pub(super) struct Restored {
    pub state: SNState,
    pub disks: Vec<(EnId, DiskId)>,
    pub index: u64,
}

/// Read the application image after Raft has selected this checkpoint.
pub(super) fn read_state(path: &Path, max_extents: u32) -> Result<Restored> {
    read_state_with_workers(path, max_extents, 16, false)
}

pub(super) fn read_state_with_workers(
    path: &Path,
    max_extents: u32,
    workers: usize,
    repair_checkpoint_indexes: bool,
) -> Result<Restored> {
    let mut file = File::open(path)?;
    let fixed = raft::checkpoint::read_footer(&mut file)?;
    if fixed.op != Op::SnCkFooter {
        return Err(Error::new("SN checkpoint is missing its application footer"));
    }
    let (footer, _) = read_public(&mut file)?;
    let (disks, old_to_new) = reindex_disks(read_ens(&mut file, footer.ens)?)?;
    if footer.streams > u64::from(MAX_STREAMS) {
        return Err(Error::new("SN checkpoint stream table exceeds its limit"));
    }
    let stream_buckets = u32::try_from(footer.stream_buckets)
        .map_err(|_| Error::new("SN checkpoint stream bucket count is out of range"))?;
    if stream_buckets < 256 || stream_buckets > MAX_STREAMS || stream_buckets % 256 != 0 {
        return Err(Error::new("SN checkpoint stream bucket count is out of range"));
    }
    let mut state = SNState::with_stream_buckets(max_extents, stream_buckets)?;
    read_en_states(&mut file, footer.en_states, &mut state, &disks)?;
    read_disk_usage(&mut file, footer.disk_usage, footer.disk_usage_built_at, &mut state, &disks)?;
    let extent_slots = usize::try_from(footer.extent_slots.get())
        .map_err(|_| Error::new("SN checkpoint extent table is too large"))?;
    if extent_slots == 0 || extent_slots > max_extents as usize {
        return Err(Error::new("SN checkpoint extent table length is out of range"));
    }
    state.extent_table.finish_restore(extent_slots)?;
    read_extents(
        &mut file,
        footer.extent_rows,
        &footer.extent_chunks,
        &mut state,
        extent_slots,
        &old_to_new,
        footer.extents,
        workers,
    )?;
    // Each disk's extent list is stored independently so checkpoint workers can
    // read the reverse indexes without deriving them from the parallel extents.
    let index_mismatches = read_disk_extent_indexes(
        &mut file,
        footer.disk_extent_rows,
        &footer.disk_extent_chunks,
        &mut state,
        &disks,
        footer.disk_extent_entries,
        workers,
    )?;
    verify_or_repair_disk_extent_indexes(
        &mut state,
        &footer.extent_chunks,
        &disks,
        workers,
        repair_checkpoint_indexes,
        index_mismatches,
    )?;
    for (&en_id, en) in &state.ens {
        for (&disk_id, info) in &en.disks {
            if info.status == EnDiskStatus::Deleted
                && !state.disk_extent_indexes.has_instances((en_id, disk_id))
            {
                return Err(Error::new("SN checkpoint deleted disk has no indexed instances"));
            }
        }
        for &disk_id in &en.retired_disks {
            if state.disk_extent_indexes.has_instances((en_id, disk_id)) {
                return Err(Error::new("SN checkpoint retired disk still has indexed instances"));
            }
        }
    }
    read_streams(
        &mut file,
        footer.stream_rows,
        &footer.stream_chunks,
        &mut state,
        footer.streams,
        workers,
    )?;
    if state.stream_map.len() != footer.streams {
        return Err(Error::new("SN checkpoint stream table length mismatch"));
    }
    read_retired(&mut file, footer.retired, &mut state)?;
    read_pending(&mut file, footer.pending, &mut state, &old_to_new)?;
    state.sealed = u64::try_from(
        state
            .extent_table
            .iter()
            .filter(|extent| extent.deletion.is_valid() && extent.sealed)
            .count(),
    )
    .map_err(|_| Error::new("SN checkpoint sealed extent count overflow"))?;
    let mut sealed_streams = 0u64;
    let mut sealed_streams_overflow = false;
    state.stream_map.for_each(|stream| {
        if stream.sealed_stream {
            if let Some(count) = sealed_streams.checked_add(1) {
                sealed_streams = count;
            } else {
                sealed_streams_overflow = true;
            }
        }
    });
    if sealed_streams_overflow {
        return Err(Error::new("SN checkpoint sealed stream count overflow"));
    }
    state.sealed_streams = sealed_streams;
    state.streams = footer.streams;
    state.extents = footer.extents;
    Ok(Restored { state, disks, index: footer.raft.index })
}

pub(super) fn is_sn_ckpt(path: &Path) -> Result<bool> {
    let mut file = File::open(path)?;
    Ok(raft::checkpoint::read_footer(&mut file).is_ok_and(|footer| footer.op == Op::SnCkFooter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_index_reindexes() {
        let old = [(EnId::new(9), DiskId::new(2)), (EnId::new(4), DiskId::new(7))];
        let indexes = disk_indices(&[SnDiskId::new(0), SnDiskId::new(1)], &old).unwrap();
        let encoded = codec::encode(&indexes).unwrap();
        assert_eq!(codec::decode::<Vec<DiskIndex>>(&encoded).unwrap(), indexes);

        let (new, old_to_new) = reindex_disks(old.to_vec()).unwrap();
        assert_eq!(new, [old[1], old[0]]);
        let restored = remap_disks(indexes, &old_to_new).unwrap();
        assert_eq!(restored, [SnDiskId::new(1), SnDiskId::new(0)]);
        assert_eq!(new[restored[0].get() as usize], old[0]);
        assert_eq!(new[restored[1].get() as usize], old[1]);

        assert!(remap_disks(vec![DiskIndex(2)], &old_to_new).is_err());
    }
}
