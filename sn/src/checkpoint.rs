//! SN application checkpoint codec.
use super::{DeleteTask, Extent, ExtentData, PendingDelete, SNState, Stream};
use common::deletion::{Extra, Flags, ReferenceCounts};
use common::ids::{DiskId, EnId, ExtentId, RangeId, SnDiskId, SnId, StreamId};
use common::info::StreamName;
use common::offset::{ECoff, ELoff, ERoff, SRoff};
use common::time::{TimeIntSec32, TimeIntSec64, TimeSec};
use common::{Error, Op, Result};
use raft::codec;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

const REF_CHUNK: usize = 16 * 1024;

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
    extents: u64,
    ens: Range,
    extent_rows: Range,
    stream_rows: Range,
    retired: Range,
    pending: Range,
}
raft::wire_struct!(Footer, SnCkFooter, {
    raft,sequence,second,streams,extents,ens,extent_rows,stream_rows,retired,pending
});

struct EnRow {
    id: u64,
    disks: Vec<u64>,
}
raft::wire_struct!(EnRow, SnCkEn, {id,disks});

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
    disks: Vec<DiskRef>,
    extra: Option<ExtExtra>,
}
raft::wire_struct!(ExtentRow, SnCkExtent, {id,flags,sealed,logical,compressed,records,created,disks,extra});

struct DiskRef {
    en: u64,
    disk: u64,
}
raft::wire_struct!(DiskRef, SnCkDiskRef, {en,disk});

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
    disks: Vec<DiskRef>,
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

fn disk_refs(ids: &[SnDiskId], roster: &[(EnId, DiskId)]) -> Result<Vec<DiskRef>> {
    ids.iter()
        .map(|id| {
            let (en, disk) = roster
                .get(id.get() as usize)
                .ok_or_else(|| Error::new("SN checkpoint disk index is outside its roster"))?;
            Ok(DiskRef { en: en.get(), disk: u64::from(disk.get()) })
        })
        .collect()
}

fn disk_map(
    roster: &[(EnId, DiskId)],
) -> Result<std::collections::HashMap<(EnId, DiskId), SnDiskId>> {
    if roster.len() > u32::MAX as usize {
        return Err(Error::new("SN checkpoint EN roster is too large"));
    }
    let mut result = std::collections::HashMap::with_capacity(roster.len());
    for (at, &(en, disk)) in roster.iter().enumerate() {
        let id = SnDiskId::new(at as u32);
        if result.insert((en, disk), id).is_some() {
            return Err(Error::new("duplicate SN checkpoint EN disk"));
        }
    }
    Ok(result)
}

fn sn_disks(
    refs: Vec<DiskRef>,
    roster: &std::collections::HashMap<(EnId, DiskId), SnDiskId>,
) -> Result<Vec<SnDiskId>> {
    refs.into_iter()
        .map(|disk| {
            let key = (EnId::of_number(disk.en)?, DiskId::of_number(disk.disk)?);
            roster
                .get(&key)
                .copied()
                .ok_or_else(|| Error::new("SN checkpoint extent names an unknown EN disk"))
        })
        .collect()
}

fn ext_extra(state: &SNState, extent: &Extent) -> Result<Option<ExtExtra>> {
    let extra = state.extent_extras.get(&(extent.id, extent.created));
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
    file: &mut (impl Write + Seek),
    state: &SNState,
    disks: &[(EnId, DiskId)],
) -> Result<Range> {
    let count = state.extent_table.iter().filter(|extent| extent.deletion.is_valid()).count();
    let start = section_start(file, Op::SnCkExtents, count as u64)?;
    for extent in state.extent_table.iter().filter(|extent| extent.deletion.is_valid()) {
        let row = ExtentRow {
            id: u64::from(extent.id.get()),
            flags: extent.deletion.bits(),
            sealed: extent.sealed,
            logical: extent.logical.number(),
            compressed: extent.compressed.number(),
            records: extent.records.number(),
            created: extent.created.number(),
            disks: disk_refs(&extent.disks, disks)?,
            extra: ext_extra(state, extent)?,
        };
        write_piece(file, &row)?;
    }
    range(file, start)
}

fn write_streams(file: &mut (impl Write + Seek), state: &SNState) -> Result<Range> {
    let start = section_start(file, Op::SnCkStreams, state.stream_map.len())?;
    let mut result = Ok(());
    state.stream_map.for_each(|stream| {
        if result.is_err() {
            return;
        }
        let extra = state.stream_extras.get(&stream.name);
        if stream.deletion.extra() != extra.is_some() {
            result = Err(Error::new("SN stream extra flag does not match its checkpoint data"));
            return;
        }
        let head = StreamHead {
            range: stream.name.range.get(),
            stream: stream.name.stream.number(),
            created: stream.name.created.number(),
            flags: stream.deletion.bits(),
            creator: stream.creator.get(),
            sealed_len: stream.sealed.len() as u64,
            open: stream.open.map(|id| u64::from(id.get())),
            sealed_records: stream.sealed_records.number(),
            sealed_stream: stream.sealed_stream,
            retain_until: extra.map(|extra| extra.retain_until.number()),
        };
        if let Err(error) = write_piece(file, &head) {
            result = Err(error);
            return;
        }
        for ids in stream.sealed.chunks(REF_CHUNK) {
            let row = Refs { ids: ids.iter().map(|id| u64::from(id.get())).collect() };
            if let Err(error) = write_piece(file, &row) {
                result = Err(error);
                return;
            }
        }
    });
    result?;
    range(file, start)
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
                    disks: disk_refs(&task.disks, disks)?,
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
    file: &mut (impl Write + Seek),
    raft_status: &raft::checkpoint::RaftStatus,
    state: &SNState,
    disks: &[(EnId, DiskId)],
    sequence: u64,
    second: u64,
) -> Result<()> {
    if file.seek(SeekFrom::End(0))? != 0 {
        return Err(Error::new("SN checkpoint target is not empty"));
    }
    let mut output = BufWriter::new(file);
    let ens = write_ens(&mut output, disks)?;
    let extent_rows = write_extents(&mut output, state, disks)?;
    let stream_rows = write_streams(&mut output, state)?;
    let retired = write_retired(&mut output, state)?;
    let pending = write_pending(&mut output, state, disks)?;
    let footer = encode_piece(&Footer {
        raft: raft_status.clone(),
        sequence,
        second,
        streams: state.streams,
        extents: state.extents,
        ens,
        extent_rows,
        stream_rows,
        retired,
        pending,
    })?;
    raft::checkpoint::write_footer(&mut output, Op::SnCkFooter, &footer)?;
    output.flush()?;
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
        write(&mut file, raft_status, state, disks, sequence, second)?;
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
    let ranges =
        [footer.ens, footer.extent_rows, footer.stream_rows, footer.retired, footer.pending];
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

fn restore_extent(
    state: &mut SNState,
    row: ExtentRow,
    disks: &std::collections::HashMap<(EnId, DiskId), SnDiskId>,
) -> Result<()> {
    let id = ExtentId::of_number(row.id)?;
    let flags = Flags::from_bits(row.flags)?;
    if !flags.is_valid() || row.extra.is_some() != flags.extra() {
        return Err(Error::new("invalid SN checkpoint extent flags"));
    }
    let disks = sn_disks(row.disks, disks)?;
    let extent = ExtentData {
        deletion: flags,
        sealed: row.sealed,
        logical: ELoff::of_number(row.logical)?,
        compressed: ECoff::of_number(row.compressed)?,
        id,
        records: ERoff::of_number(row.records)?,
        created: TimeSec::new(row.created),
        disks,
    };
    state
        .extent_table
        .reserve_id(id)
        .map_err(|error| Error::new(format!("reserve checkpoint extent {}: {error}", id.get())))?;
    state.extent_table.insert_reserved(id, Extent::from_data(extent))?;
    if let Some(extra) = row.extra {
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
        if state.extent_extras.restore((id, TimeSec::new(row.created)), value).is_some() {
            return Err(Error::new("duplicate SN checkpoint extent extra"));
        }
    }
    Ok(())
}

fn read_extents(
    file: &mut File,
    range: Range,
    state: &mut SNState,
    disks: &std::collections::HashMap<(EnId, DiskId), SnDiskId>,
) -> Result<()> {
    let mut section = read_section(file, range, Op::SnCkExtents)?;
    for _ in 0..section.count {
        let row: ExtentRow = read_piece(file, &mut section.left)?;
        restore_extent(state, row, disks)?;
    }
    if section.left != 0 {
        return Err(Error::new("trailing SN checkpoint extent data"));
    }
    Ok(())
}

fn read_streams(file: &mut File, range: Range, state: &mut SNState) -> Result<()> {
    let mut section = read_section(file, range, Op::SnCkStreams)?;
    for _ in 0..section.count {
        let row: StreamHead = read_piece(file, &mut section.left)?;
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
            let refs: Refs = read_piece(file, &mut section.left)?;
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
        let sealed_count = u64::try_from(stream.sealed.len())
            .map_err(|_| Error::new("SN checkpoint sealed extent count overflow"))?;
        let sealed_stream = stream.sealed_stream;
        let (_, inserted) = state
            .stream_map
            .find_or_create(&name, |_| stream)
            .map_err(|error| Error::new(format!("restore SN stream: {error}")))?;
        if !inserted {
            return Err(Error::new("duplicate SN checkpoint stream"));
        }
        state.sealed = state
            .sealed
            .checked_add(sealed_count)
            .ok_or_else(|| Error::new("SN checkpoint sealed extent count overflow"))?;
        state.sealed_streams = state
            .sealed_streams
            .checked_add(u64::from(sealed_stream))
            .ok_or_else(|| Error::new("SN checkpoint sealed stream count overflow"))?;
        if let Some(until) = row.retain_until {
            let extra = Extra { retain_until: TimeSec::new(until), references: () };
            if state.stream_extras.restore(name, extra).is_some() {
                return Err(Error::new("duplicate SN checkpoint stream extra"));
            }
        }
    }
    if section.left != 0 {
        return Err(Error::new("trailing SN checkpoint stream data"));
    }
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
    disks: &std::collections::HashMap<(EnId, DiskId), SnDiskId>,
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
                disks: sn_disks(task.disks, disks)?,
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
    let mut file = File::open(path)?;
    let fixed = raft::checkpoint::read_footer(&mut file)?;
    if fixed.op != Op::SnCkFooter {
        return Err(Error::new("SN checkpoint is missing its application footer"));
    }
    let (footer, _) = read_public(&mut file)?;
    let disks = read_ens(&mut file, footer.ens)?;
    let disk_map = disk_map(&disks)?;
    let mut state = SNState::new(max_extents)?;
    read_extents(&mut file, footer.extent_rows, &mut state, &disk_map)?;
    read_streams(&mut file, footer.stream_rows, &mut state)?;
    read_retired(&mut file, footer.retired, &mut state)?;
    read_pending(&mut file, footer.pending, &mut state, &disk_map)?;
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
    fn disk_ref_reindexes() {
        let old = [(EnId::new(4), DiskId::new(7)), (EnId::new(9), DiskId::new(2))];
        let refs = disk_refs(&[SnDiskId::new(0), SnDiskId::new(1)], &old).unwrap();
        assert_eq!(refs[0].en, 4);
        assert_eq!(refs[0].disk, 7);
        assert_eq!(refs[1].en, 9);
        assert_eq!(refs[1].disk, 2);

        let new = [old[1], old[0]];
        let restored = sn_disks(refs, &disk_map(&new).unwrap()).unwrap();
        assert_eq!(restored, [SnDiskId::new(1), SnDiskId::new(0)]);
    }
}
