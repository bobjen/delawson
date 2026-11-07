//! DN application checkpoint codec.
use super::{DNState, Object, Stream};
use common::deletion::{Extra, Extras, Flags, ReferenceCounts, SharedExtras};
use common::framing::Framing;
use common::ids::{ExtentId, ObjectId, RangeId, SnId, StreamId};
use common::offset::{SCoff, SLoff, SRoff};
use common::time::{TimeIntSec32, TimeSec};
use common::{Error, Op, Result};
use raft::codec;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

const REF_CHUNK: usize = 2048;
const CHUNK_BYTES: usize = 2 * 1024 * 1024;
const CHUNK_MAX: usize = 4 * 1024 * 1024;
const CHUNK_ITEMS: usize = 64 * 1024;

#[derive(Clone, Copy)]
struct Range {
    start: u64,
    len: u64,
}
raft::wire_struct!(Range, DnCkSpan, {start,len});

struct Footer {
    raft: raft::checkpoint::RaftStatus,
    sequence: u64,
    second: u64,
    range_meta: Range,
    streams: Range,
    stream_chunks: Vec<raft::checkpoint::ChunkPos>,
    objects: Range,
    object_chunks: Vec<raft::checkpoint::ChunkPos>,
    object_extras: Range,
    stream_extras: Range,
}
raft::wire_struct!(Footer, DnCkFooter, {
    raft,sequence,second,range_meta,streams,stream_chunks,objects,object_chunks,object_extras,stream_extras
});

struct RangeMeta {
    range: RangeId,
    stream_slots: StreamId,
    object_buckets: u64,
    next_object: ObjectId,
    next_stream: StreamId,
}

impl codec::Wire for RangeMeta {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.nested(Op::DnCkRange, |writer| {
            writer.range_id(self.range);
            writer.stream_id(self.stream_slots);
            writer.number_u64(self.object_buckets);
            writer.object_id(self.next_object);
            writer.stream_id(self.next_stream);
        });
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        reader.nested(Op::DnCkRange, "RangeMeta", |reader| {
            Ok(Self {
                range: reader.range_id()?,
                stream_slots: reader.stream_id()?,
                object_buckets: reader.number_u64()?,
                next_object: reader.object_id()?,
                next_stream: reader.stream_id()?,
            })
        })
    }
}

struct StreamRow {
    id: u64,
    flags: u8,
    records: u64,
    created: u64,
    logical: u64,
    compressed: u64,
    sn: u64,
    extents: u64,
}
raft::wire_struct!(StreamRow, DnCkStream, {
    id,flags,records,created,logical,compressed,sn,extents
});

struct ObjectRow {
    name: String,
    flags: u8,
    purge_only: bool,
    id: u64,
    framing: u8,
    width: u64,
    stream_count: u64,
    keyed: bool,
    created: u64,
}
raft::wire_struct!(ObjectRow, DnCkObject, {
    name,flags,purge_only,id,framing,width,stream_count,keyed,created
});

struct RefRow {
    object: u64,
    created: u64,
    at: u64,
    streams: Vec<u64>,
}
raft::wire_struct!(RefRow, DnCkObjRefs, {object,created,at,streams});

type ObjKey = (ObjectId, TimeSec);

struct ObjExtra {
    object: u64,
    created: u64,
    until: u64,
}
raft::wire_struct!(ObjExtra, DnCkObjExtra, {object,created,until});

struct StreamExtra {
    stream: u64,
    created: u64,
    until: u64,
    max_retention: u64,
}
raft::wire_struct!(StreamExtra, DnCkStrExtra, {stream,created,until,max_retention});

#[derive(Clone, Copy)]
pub(super) struct ObjExtraOut {
    object: u64,
    created: u64,
    until: u64,
}

#[derive(Clone, Copy)]
pub(super) struct StreamExtraOut {
    stream: u64,
    created: u64,
    until: u64,
    max_retention: u64,
}

pub(super) struct Restored {
    pub state: DNState,
    pub object_extras: Extras<(ObjectId, TimeSec)>,
    pub stream_extras: SharedExtras<(StreamId, TimeSec)>,
    pub next_object: u64,
    pub next_stream: u64,
    pub index: u64,
}

pub(super) fn capture_extras(
    objects: &Extras<(ObjectId, TimeSec)>,
    streams: &SharedExtras<(StreamId, TimeSec)>,
) -> (Vec<ObjExtraOut>, Vec<StreamExtraOut>) {
    let mut object_rows: Vec<_> = objects
        .iter()
        .map(|(&(object, created), extra)| ObjExtraOut {
            object: object.number(),
            created: created.number(),
            until: extra.retain_until.number(),
        })
        .collect();
    object_rows.sort_unstable_by_key(|row| (row.object, row.created));
    let mut stream_rows: Vec<_> = streams
        .iter()
        .map(|(&(stream, created), extra)| StreamExtraOut {
            stream: stream.number(),
            created: created.number(),
            until: extra.retain_until.number(),
            max_retention: u64::from(extra.references.max_retention.get() as u32),
        })
        .collect();
    stream_rows.sort_unstable_by_key(|row| (row.stream, row.created));
    (object_rows, stream_rows)
}

fn encode_piece<T: codec::Wire>(value: &T) -> Result<Vec<u8>> {
    let mut writer = common::StructWriter::new();
    value.write(&mut writer);
    let arg = writer.finish(Op::Raft);
    if arg.depth == 0 || arg.depth > common::MAX_RECURSION_DEPTH {
        return Err(Error::new("DN checkpoint nesting exceeds limit"));
    }
    let mut bytes = vec![arg.depth];
    arg.into_bytes().write_to(&mut bytes)?;
    Ok(bytes)
}

fn decode_piece<T: codec::Wire>(bytes: &[u8]) -> Result<T> {
    let (&depth, bytes) =
        bytes.split_first().ok_or_else(|| Error::new("empty DN checkpoint piece"))?;
    if depth == 0 || depth > common::MAX_RECURSION_DEPTH || isize::try_from(bytes.len()).is_err() {
        return Err(Error::new("invalid DN checkpoint piece limits"));
    }
    let mut reader = common::StructReader::new(bytes, depth);
    let value = T::read(&mut reader)?;
    reader.done()?;
    Ok(value)
}

fn write_piece(file: &mut impl Write, value: &impl codec::Wire) -> Result<()> {
    let bytes = encode_piece(value)?;
    let size = u64::try_from(bytes.len()).map_err(|_| Error::new("DN checkpoint row too large"))?;
    file.write_all(&size.to_le_bytes())?;
    file.write_all(&bytes)?;
    Ok(())
}

fn section(file: &mut impl Write, op: Op, count: u64) -> Result<()> {
    file.write_all(&op.code().to_le_bytes())?;
    file.write_all(&count.to_le_bytes())?;
    Ok(())
}

fn section_start(file: &mut (impl Write + Seek), op: Op, count: u64) -> Result<u64> {
    let start = file.stream_position()?;
    section(file, op, count)?;
    Ok(start)
}

fn section_range(file: &mut (impl Write + Seek), start: u64) -> Result<Range> {
    Ok(Range { start, len: file.stream_position()? - start })
}

fn object_row(object: &Object) -> ObjectRow {
    let (framing, width) = object.framing.wire();
    ObjectRow {
        name: object.name.to_string(),
        flags: object.deletion.bits(),
        purge_only: object.purge_only,
        id: object.id.number(),
        framing,
        width: u64::from(width),
        stream_count: object.streams.len() as u64,
        keyed: object.keyed,
        created: object.created.number(),
    }
}

fn stream_row(stream: &Stream) -> StreamRow {
    StreamRow {
        id: stream.id.number(),
        flags: stream.deletion.bits(),
        records: stream.records.number(),
        created: stream.created.number(),
        logical: stream.logical.number(),
        compressed: stream.compressed.number(),
        sn: stream.sn.number(),
        extents: stream.extents.number(),
    }
}

fn saved_stream(at: u64, stream: &Stream) -> bool {
    stream.id.number() == at && !stream.deletion.retired()
}

fn write_stream_chunks(
    file: &File,
    start: u64,
    state: &DNState,
    slots: u64,
    workers: usize,
) -> Result<(Vec<raft::checkpoint::ChunkPos>, u64)> {
    let per_chunk =
        (CHUNK_BYTES / std::mem::size_of::<Stream>().max(1)).clamp(1, CHUNK_ITEMS) as u64;
    let mut ranges = Vec::new();
    let mut first = 0u64;
    while first < slots {
        let span = per_chunk.min(slots - first);
        ranges.push((first, span));
        first += span;
    }
    raft::checkpoint::write_chunks(
        file,
        start,
        workers,
        "DN streams",
        &ranges,
        CHUNK_MAX,
        CHUNK_ITEMS as u64,
        |first, span| {
            let end = first
                .checked_add(span)
                .filter(|end| *end <= slots)
                .ok_or_else(|| Error::new("DN stream checkpoint chunk is outside its table"))?;
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&Op::DnCkStrChunk.code().to_le_bytes());
            bytes.extend_from_slice(&0u64.to_le_bytes());
            let mut count = 0u64;
            for at in first..end {
                let stream = &state.stream_table[at as usize];
                if saved_stream(at, stream) {
                    write_piece(&mut bytes, &stream_row(stream))?;
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::new("too many DN streams in checkpoint chunk"))?;
                }
            }
            bytes[2..10].copy_from_slice(&count.to_le_bytes());
            Ok((count, bytes))
        },
    )
}

fn write_object_chunks(
    file: &File,
    start: u64,
    state: &DNState,
    buckets: u64,
    object_count: u64,
    workers: usize,
) -> Result<(Vec<raft::checkpoint::ChunkPos>, u64)> {
    let target_items =
        (CHUNK_BYTES / std::mem::size_of::<Object>().max(1)).clamp(1, CHUNK_ITEMS) as u64;
    let per_bucket = object_count.div_ceil(buckets.max(1)).max(1);
    let bucket_span = target_items.div_ceil(per_bucket).max(1);
    let mut ranges = Vec::new();
    let mut first = 0u64;
    while first < buckets {
        let span = bucket_span.min(buckets - first);
        ranges.push((first, span));
        first += span;
    }
    raft::checkpoint::write_chunks(
        file,
        start,
        workers,
        "DN objects",
        &ranges,
        CHUNK_MAX,
        CHUNK_ITEMS as u64,
        |first, span| {
            let end = first
                .checked_add(span)
                .filter(|end| *end <= buckets)
                .ok_or_else(|| Error::new("DN object checkpoint chunk is outside its table"))?;
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&Op::DnCkObjChunk.code().to_le_bytes());
            bytes.extend_from_slice(&0u64.to_le_bytes());
            let mut count = 0u64;
            for bucket in first..end {
                let at = bytes.len();
                bytes.extend_from_slice(&0u64.to_le_bytes());
                let mut bucket_count = 0u64;
                let mut result = Ok(());
                state.object_table.for_each_bucket(bucket, |object| {
                    if result.is_err() {
                        return;
                    }
                    result = write_piece(&mut bytes, &object_row(object));
                    for (chunk, streams) in object.streams.chunks(REF_CHUNK).enumerate() {
                        if result.is_err() {
                            break;
                        }
                        let row = RefRow {
                            object: object.id.number(),
                            created: object.created.number(),
                            at: (chunk * REF_CHUNK) as u64,
                            streams: streams.iter().map(|stream| stream.number()).collect(),
                        };
                        result = write_piece(&mut bytes, &row);
                    }
                    if result.is_ok() {
                        bucket_count = match bucket_count.checked_add(1) {
                            Some(next) => next,
                            None => {
                                result =
                                    Err(Error::new("too many DN objects in checkpoint bucket"));
                                bucket_count
                            }
                        };
                    }
                });
                result?;
                bytes[at..at + 8].copy_from_slice(&bucket_count.to_le_bytes());
                count = count
                    .checked_add(bucket_count)
                    .ok_or_else(|| Error::new("too many DN objects in checkpoint chunk"))?;
            }
            bytes[2..10].copy_from_slice(&count.to_le_bytes());
            Ok((count, bytes))
        },
    )
}

/// Write a complete DN checkpoint to an empty target.
pub(super) fn write(
    file: &mut File,
    raft_status: &raft::checkpoint::RaftStatus,
    state: &DNState,
    range: RangeId,
    next_object: u64,
    next_stream: u64,
    object_extras: &[ObjExtraOut],
    stream_extras: &[StreamExtraOut],
    sequence: u64,
    second: u64,
    workers: usize,
) -> Result<()> {
    raft_status.validate()?;
    if file.seek(SeekFrom::End(0))? != 0 {
        return Err(Error::new("DN checkpoint target is not empty"));
    }
    if next_object > ObjectId::NONE.number() || next_stream > StreamId::NONE.number() {
        return Err(Error::new("DN id high-water exceeds its identifier range"));
    }
    let stream_slots = StreamId::of_number(
        u64::try_from(state.stream_table.len)
            .map_err(|_| Error::new("DN stream table is too large"))?,
    )?;
    let object_count = state.object_table.len();

    let meta_at = section_start(file, Op::DnCkMeta, 1)?;
    write_piece(
        file,
        &RangeMeta {
            range,
            stream_slots,
            object_buckets: state.object_table.bucket_count(),
            next_object: ObjectId::of_number(next_object)?,
            next_stream: StreamId::of_number(next_stream)?,
        },
    )?;
    let range_meta = section_range(file, meta_at)?;

    let streams_at = section_start(file, Op::DnCkStreams, 0)?;
    let stream_data = file.stream_position()?;
    let (stream_chunks, stream_end) =
        write_stream_chunks(file, stream_data, state, u64::from(stream_slots.get()), workers)?;
    let stream_count = stream_chunks.iter().try_fold(0u64, |sum, chunk| {
        sum.checked_add(chunk.items).ok_or_else(|| Error::new("too many DN streams"))
    })?;
    file.seek(SeekFrom::Start(streams_at + 2))?;
    file.write_all(&stream_count.to_le_bytes())?;
    file.seek(SeekFrom::Start(stream_end))?;
    let streams = Range { start: streams_at, len: stream_end - streams_at };

    let object_buckets = state.object_table.bucket_count();
    let objects_at = section_start(file, Op::DnCkObjects, object_count)?;
    let object_data = file.stream_position()?;
    let (object_chunks, object_end) =
        write_object_chunks(file, object_data, state, object_buckets, object_count, workers)?;
    file.seek(SeekFrom::Start(object_end))?;
    let objects = Range { start: objects_at, len: object_end - objects_at };

    let mut output = BufWriter::new(&mut *file);

    let object_extras_at = section_start(&mut output, Op::DnCkObjX, object_extras.len() as u64)?;
    for row in object_extras {
        write_piece(
            &mut output,
            &ObjExtra { object: row.object, created: row.created, until: row.until },
        )?;
    }
    let object_extras_range = section_range(&mut output, object_extras_at)?;

    let stream_extras_at = section_start(&mut output, Op::DnCkStrX, stream_extras.len() as u64)?;
    for row in stream_extras {
        write_piece(
            &mut output,
            &StreamExtra {
                stream: row.stream,
                created: row.created,
                until: row.until,
                max_retention: row.max_retention,
            },
        )?;
    }
    let stream_extras_range = section_range(&mut output, stream_extras_at)?;

    let footer = encode_piece(&Footer {
        raft: raft_status.clone(),
        sequence,
        second,
        range_meta,
        streams,
        stream_chunks,
        objects,
        object_chunks,
        object_extras: object_extras_range,
        stream_extras: stream_extras_range,
    })?;
    raft::checkpoint::write_footer(&mut output, Op::DnCkFooter, &footer)?;
    output.flush()?;
    Ok(())
}

/// Replace the generic empty Raft base checkpoint with an empty DN checkpoint.
pub(super) fn write_bootstrap(
    path: &Path,
    status: &raft::checkpoint::RaftStatus,
    state: &DNState,
    range: RangeId,
    sequence: u64,
    workers: usize,
) -> Result<u64> {
    let parent =
        path.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp = parent.join(format!(".dn-ck-{}-{stamp}.tmp", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        write(
            &mut file,
            status,
            state,
            range,
            0,
            0,
            &[],
            &[],
            sequence,
            TimeSec::now().number(),
            workers,
        )?;
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

fn range_end(range: Range) -> Result<u64> {
    range.start.checked_add(range.len).ok_or_else(|| Error::new("DN checkpoint range overflow"))
}

fn read_public(file: &mut (impl Read + Seek)) -> Result<(Footer, u64)> {
    let fixed = raft::checkpoint::read_footer(file)?;
    if fixed.op != Op::DnCkFooter {
        return Err(Error::new("wrong DN checkpoint footer"));
    }
    let footer: Footer = decode_piece(&fixed.bytes)?;
    footer.raft.validate()?;
    let ranges = [
        footer.range_meta,
        footer.streams,
        footer.objects,
        footer.object_extras,
        footer.stream_extras,
    ];
    let mut end = 0;
    for range in ranges {
        if range.start != end || range.len < 10 {
            return Err(Error::new("invalid DN checkpoint section ranges"));
        }
        end = range_end(range)?;
    }
    if end != fixed.start {
        return Err(Error::new("DN checkpoint sections do not reach the footer"));
    }
    Ok((footer, fixed.start))
}

fn restore_raft(
    status: raft::checkpoint::RaftStatus,
    ring: &str,
    ignore: bool,
) -> Result<raft::State> {
    let snapshot = status.snapshot();
    let mut state = raft::State::initial(
        status.ring.clone(),
        status.membership.old.clone(),
        status.membership.identities.clone(),
    )?;
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

pub(super) fn read_raft(
    path: &Path,
    entry: u64,
    ring: &str,
    ignore: bool,
) -> Result<(raft::State, u64, u64, u64)> {
    let mut file = File::open(path)?;
    let is_dn =
        raft::checkpoint::read_footer(&mut file).is_ok_and(|footer| footer.op == Op::DnCkFooter);
    if !is_dn {
        if entry != 0 {
            return Err(Error::new("DN checkpoint is missing its application footer"));
        }
        return raft::journal::read_checkpoint(path, entry, ring, ignore);
    }
    let (footer, _) = read_public(&mut file)?;
    if footer.raft.index != entry || (!ignore && footer.raft.ring != ring) {
        return Err(Error::new("DN checkpoint ring or sequence mismatch"));
    }
    let state = restore_raft(footer.raft, ring, ignore)?;
    let length = file.metadata()?.len();
    Ok((state, footer.sequence, footer.second, length))
}

struct SectionRead {
    count: u64,
    left: u64,
}

fn read_section(file: &mut File, range: Range, op: Op, plain: Option<u64>) -> Result<SectionRead> {
    if range.len < 10 {
        return Err(Error::new("truncated DN checkpoint section"));
    }
    file.seek(SeekFrom::Start(range.start))?;
    let mut tag = [0; 2];
    file.read_exact(&mut tag)?;
    if u16::from_le_bytes(tag) != op.code() {
        return Err(Error::new(format!("wrong DN checkpoint section {op}")));
    }
    let count = read_u64(file)?;
    let left = range.len - 10;
    if count > plain.unwrap_or(left) / 9 {
        return Err(Error::new("DN checkpoint row count exceeds section length"));
    }
    Ok(SectionRead { count, left })
}

fn plain_chunk_bytes(chunks: &[raft::checkpoint::ChunkPos]) -> Result<u64> {
    chunks.iter().try_fold(0u64, |sum, chunk| {
        sum.checked_add(chunk.plain)
            .ok_or_else(|| Error::new("DN checkpoint plain chunk size overflow"))
    })
}

fn read_u64(file: &mut impl Read) -> Result<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_piece<T: codec::Wire>(file: &mut impl Read, left: &mut u64) -> Result<T> {
    *left = left.checked_sub(8).ok_or_else(|| Error::new("truncated DN checkpoint row"))?;
    let size = read_u64(file)?;
    *left = left.checked_sub(size).ok_or_else(|| Error::new("truncated DN checkpoint row"))?;
    let size = usize::try_from(size).map_err(|_| Error::new("DN checkpoint row too large"))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|_| Error::new("DN checkpoint allocation failed"))?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)?;
    decode_piece(&bytes)
}

fn read_meta(file: &mut File, range: Range) -> Result<RangeMeta> {
    let mut section = read_section(file, range, Op::DnCkMeta, None)?;
    if section.count != 1 {
        return Err(Error::new("DN checkpoint needs one range record"));
    }
    let meta = read_piece(file, &mut section.left)?;
    if section.left != 0 {
        return Err(Error::new("trailing DN range metadata"));
    }
    Ok(meta)
}

fn check_chunks(
    range: Range,
    chunks: &[raft::checkpoint::ChunkPos],
    limit: u64,
    count: u64,
    label: &str,
) -> Result<()> {
    let start = range
        .start
        .checked_add(10)
        .ok_or_else(|| Error::new(format!("DN checkpoint {label} range overflow")))?;
    let end = range_end(range)?;
    let mut physical = chunks.to_vec();
    physical.sort_unstable_by_key(|chunk| chunk.start);
    let mut at = start;
    let mut items = 0u64;
    for chunk in &physical {
        let chunk_end = chunk
            .start
            .checked_add(chunk.stored)
            .ok_or_else(|| Error::new(format!("DN checkpoint {label} chunk range overflow")))?;
        if chunk.start != at
            || chunk_end > end
            || chunk.plain < 10
            || chunk.stored == 0
            || chunk.stored > chunk.plain
            || chunk.span == 0
            || chunk.items == 0
        {
            return Err(Error::new(format!("invalid DN checkpoint {label} chunk positions")));
        }
        at = chunk_end;
        items = items
            .checked_add(chunk.items)
            .ok_or_else(|| Error::new(format!("DN checkpoint {label} chunk count overflow")))?;
    }
    if at != end || items != count {
        return Err(Error::new(format!("DN checkpoint {label} chunks do not match their section")));
    }
    let mut logical = chunks.to_vec();
    logical.sort_unstable_by_key(|chunk| chunk.first);
    let mut prior_end = 0u64;
    for chunk in logical {
        let chunk_end =
            chunk.first.checked_add(chunk.span).filter(|end| *end <= limit).ok_or_else(|| {
                Error::new(format!("DN checkpoint {label} chunk exceeds its table"))
            })?;
        if chunk.first < prior_end {
            return Err(Error::new(format!("overlapping DN checkpoint {label} chunks")));
        }
        prior_end = chunk_end;
    }
    Ok(())
}

fn read_streams(
    file: &mut File,
    range: Range,
    chunks: &[raft::checkpoint::ChunkPos],
    state: &mut DNState,
    len: StreamId,
    workers: usize,
) -> Result<()> {
    let section = read_section(file, range, Op::DnCkStreams, Some(plain_chunk_bytes(chunks)?))?;
    check_chunks(range, chunks, u64::from(len.get()), section.count, "stream")?;
    state.stream_table.prepare_restore(len.get() as usize)?;
    let table = &state.stream_table;
    raft::checkpoint::read_chunks(&*file, chunks, workers, "DN streams", |chunk, bytes| {
        let chunk_end = chunk
            .first
            .checked_add(chunk.span)
            .ok_or_else(|| Error::new("DN checkpoint stream chunk range overflow"))?;
        if bytes.len() as u64 != chunk.plain {
            return Err(Error::new("DN checkpoint stream chunk length mismatch"));
        }
        let mut input = Cursor::new(bytes.as_slice());
        let mut tag = [0; 2];
        input.read_exact(&mut tag)?;
        if u16::from_le_bytes(tag) != Op::DnCkStrChunk.code() {
            return Err(Error::new("wrong DN checkpoint stream chunk"));
        }
        let count = read_u64(&mut input)?;
        if count != chunk.items {
            return Err(Error::new("DN checkpoint stream chunk item count mismatch"));
        }
        let mut left = chunk.plain - 10;
        let mut previous = None;
        for _ in 0..count {
            let row: StreamRow = read_piece(&mut input, &mut left)?;
            if row.id < chunk.first
                || row.id >= chunk_end
                || previous.is_some_and(|id| id >= row.id)
            {
                return Err(Error::new("invalid DN checkpoint stream order"));
            }
            previous = Some(row.id);
            let id = StreamId::of_number(row.id)?;
            let deletion = Flags::from_bits(row.flags)?;
            if deletion.retired() {
                return Err(Error::new("retired stream appears in DN checkpoint"));
            }
            let stream = Stream {
                id,
                deletion,
                records: SRoff::new(row.records),
                created: TimeSec::new(row.created),
                logical: SLoff::new(row.logical),
                compressed: SCoff::new(row.compressed),
                sn: SnId::of_number(row.sn)?,
                extents: ExtentId::of_number(row.extents)?,
            };
            // SAFETY: chunk coverage is disjoint and each row is restricted to
            // its chunk's slot interval; the table is unpublished during restore.
            unsafe { table.restore_slot(stream)? };
        }
        if left != 0 {
            return Err(Error::new("trailing DN checkpoint stream chunk data"));
        }
        Ok(())
    })?;
    state.stream_table.finish_restore(len.get() as usize)?;
    Ok(())
}

fn read_u64_left(file: &mut impl Read, left: &mut u64) -> Result<u64> {
    *left = left.checked_sub(8).ok_or_else(|| Error::new("truncated DN checkpoint bucket"))?;
    read_u64(file)
}

fn read_object(file: &mut impl Read, left: &mut u64, streams: &[Stream]) -> Result<Object> {
    let row: ObjectRow = read_piece(file, left)?;
    let id = ObjectId::of_number(row.id)?;
    if id.is_none() {
        return Err(Error::new("DN checkpoint object uses the no-object id"));
    }
    let created = TimeSec::new(row.created);
    let width =
        u32::try_from(row.width).map_err(|_| Error::new("invalid DN object framing width"))?;
    let framing = Framing::of_wire(row.framing, width)?;
    let count = usize::try_from(row.stream_count)
        .map_err(|_| Error::new("DN object stream list is too large"))?;
    let mut references = Vec::new();
    references
        .try_reserve_exact(count)
        .map_err(|_| Error::new("DN object stream allocation failed"))?;
    while references.len() < count {
        let refs: RefRow = read_piece(file, left)?;
        let at =
            usize::try_from(refs.at).map_err(|_| Error::new("DN reference offset too large"))?;
        if refs.object != id.number()
            || refs.created != created.number()
            || at != references.len()
            || refs.streams.is_empty()
            || refs.streams.len() > REF_CHUNK
            || refs.streams.len() > count - references.len()
        {
            return Err(Error::new("invalid DN object stream-reference chunk"));
        }
        for number in refs.streams {
            let stream = StreamId::of_number(number)?;
            let index = stream.get() as usize;
            if index >= streams.len() {
                return Err(Error::new(format!(
                    "DN object {id} references stream {stream} beyond {} slots",
                    streams.len()
                )));
            }
            if streams[index].id != stream {
                return Err(Error::new(format!(
                    "DN object {id} references stream {stream}, but that slot has id {}",
                    streams[index].id
                )));
            }
            references.push(stream);
        }
    }
    Ok(Object {
        link: ObjectId::NONE.get(),
        name: row.name.into_boxed_str(),
        deletion: Flags::from_bits(row.flags)?,
        purge_only: row.purge_only,
        id,
        framing,
        streams: references,
        localflags: AtomicU8::new(0),
        keyed: row.keyed,
        created,
    })
}

fn read_objects(
    file: &mut File,
    range: Range,
    chunks: &[raft::checkpoint::ChunkPos],
    state: &mut DNState,
    workers: usize,
) -> Result<HashMap<ObjKey, bool>> {
    let section = read_section(file, range, Op::DnCkObjects, Some(plain_chunk_bytes(chunks)?))?;
    let count = usize::try_from(section.count)
        .map_err(|_| Error::new("DN checkpoint object count is too large"))?;
    let buckets = state.object_table.bucket_count();
    check_chunks(range, chunks, buckets, section.count, "object")?;
    if chunks.iter().any(|chunk| chunk.items > CHUNK_ITEMS as u64 && chunk.span > 1) {
        return Err(Error::new("DN checkpoint object chunk exceeds its item limit"));
    }
    let slots = state
        .object_table
        .begin_restore(count)
        .map_err(|error| Error::new(format!("reserve DN checkpoint objects: {error}")))?;
    let mut ordered = chunks.to_vec();
    ordered.sort_unstable_by_key(|chunk| chunk.first);
    let mut starts = HashMap::with_capacity(ordered.len());
    let mut slot_at = 0usize;
    for chunk in &ordered {
        starts.insert(chunk.first, slot_at);
        slot_at = slot_at
            .checked_add(
                usize::try_from(chunk.items)
                    .map_err(|_| Error::new("DN checkpoint object chunk is too large"))?,
            )
            .ok_or_else(|| Error::new("DN checkpoint object slot range overflow"))?;
    }
    if slot_at != slots.len() {
        return Err(Error::new("DN checkpoint object slot count mismatch"));
    }
    let identities = Mutex::new(HashMap::<ObjKey, bool>::new());
    identities
        .lock()
        .map_err(|_| Error::new("DN checkpoint object index poisoned"))?
        .try_reserve(count)
        .map_err(|error| Error::new(format!("reserve DN checkpoint object index: {error}")))?;
    let restored = AtomicU64::new(0);
    let table = &state.object_table;
    let stream_table = &state.stream_table;
    raft::checkpoint::read_chunks(&*file, chunks, workers, "DN objects", |chunk, bytes| {
        let chunk_end = chunk
            .first
            .checked_add(chunk.span)
            .filter(|end| *end <= buckets)
            .ok_or_else(|| Error::new("DN checkpoint object chunk exceeds its table"))?;
        if bytes.len() as u64 != chunk.plain {
            return Err(Error::new("DN checkpoint object chunk length mismatch"));
        }
        let mut input = Cursor::new(bytes.as_slice());
        let mut tag = [0; 2];
        input.read_exact(&mut tag)?;
        if u16::from_le_bytes(tag) != Op::DnCkObjChunk.code() {
            return Err(Error::new("wrong DN checkpoint object chunk"));
        }
        let count = read_u64(&mut input)?;
        if count != chunk.items {
            return Err(Error::new("DN checkpoint object chunk item count mismatch"));
        }
        let mut left = chunk.plain - 10;
        let mut next_slot = *starts
            .get(&chunk.first)
            .ok_or_else(|| Error::new("DN checkpoint object chunk has no slot range"))?;
        let slot_end = next_slot
            .checked_add(
                usize::try_from(chunk.items)
                    .map_err(|_| Error::new("DN checkpoint object chunk is too large"))?,
            )
            .ok_or_else(|| Error::new("DN checkpoint object slot range overflow"))?;
        let mut loaded = 0u64;
        let mut keys = Vec::new();
        for bucket in chunk.first..chunk_end {
            let bucket_count = read_u64_left(&mut input, &mut left)?;
            if bucket_count > count - loaded {
                return Err(Error::new("DN checkpoint bucket count exceeds its chunk"));
            }
            let mut values = Vec::new();
            values
                .try_reserve_exact(
                    usize::try_from(bucket_count)
                        .map_err(|_| Error::new("DN checkpoint bucket item count is too large"))?,
                )
                .map_err(|_| Error::new("DN checkpoint bucket allocation failed"))?;
            for _ in 0..bucket_count {
                let object = read_object(&mut input, &mut left, stream_table)?;
                keys.push(((object.id, object.created), object.deletion.extra()));
                values.push(object);
            }
            let before = next_slot;
            let mut values = values.into_iter();
            // SAFETY: this chunk owns a disjoint bucket interval and the slot
            // indexes assigned to it by the checkpoint item ranges.
            let inserted = unsafe {
                table.restore_bucket_with(bucket, || {
                    let Some(object) = values.next() else { return Ok(None) };
                    let index = slots[next_slot];
                    next_slot += 1;
                    Ok(Some((index, object)))
                })
            }
            .map_err(|error| Error::new(format!("restore DN object bucket: {error}")))?;
            if inserted != bucket_count || next_slot - before != bucket_count as usize {
                return Err(Error::new("DN checkpoint object bucket count mismatch"));
            }
            loaded = loaded
                .checked_add(bucket_count)
                .ok_or_else(|| Error::new("DN checkpoint object count overflow"))?;
        }
        if left != 0 || loaded != chunk.items || next_slot != slot_end {
            return Err(Error::new("incomplete DN checkpoint object chunk"));
        }
        let mut all =
            identities.lock().map_err(|_| Error::new("DN checkpoint object index poisoned"))?;
        for (key, extra) in keys {
            if all.insert(key, extra).is_some() {
                return Err(Error::new("duplicate DN checkpoint object"));
            }
        }
        restored.fetch_add(loaded, Ordering::Relaxed);
        Ok(())
    })?;
    if restored.load(Ordering::Relaxed) != section.count {
        return Err(Error::new("DN checkpoint restored object count mismatch"));
    }
    state
        .object_table
        .finish_restore(section.count)
        .map_err(|error| Error::new(format!("finish DN object restore: {error}")))?;
    identities.into_inner().map_err(|_| Error::new("DN checkpoint object index poisoned"))
}

fn read_object_extras(
    file: &mut File,
    range: Range,
    refs: &mut HashMap<ObjKey, bool>,
) -> Result<Extras<(ObjectId, TimeSec)>> {
    let mut extras = Extras::default();
    let mut section = read_section(file, range, Op::DnCkObjX, None)?;
    for _ in 0..section.count {
        let row: ObjExtra = read_piece(file, &mut section.left)?;
        let key = (ObjectId::of_number(row.object)?, TimeSec::new(row.created));
        let extra = refs
            .get_mut(&key)
            .ok_or_else(|| Error::new("DN checkpoint extra names an unknown object"))?;
        if !*extra {
            return Err(Error::new("DN object extra flag does not match checkpoint data"));
        }
        *extra = false;
        extras.restore(key, Extra { retain_until: TimeSec::new(row.until), references: () });
    }
    if section.left != 0 {
        return Err(Error::new("trailing DN object extras"));
    }
    if refs.values().any(|extra| *extra) {
        return Err(Error::new("DN object extra is missing from checkpoint"));
    }
    Ok(extras)
}

fn read_stream_extras(
    file: &mut File,
    range: Range,
    state: &DNState,
) -> Result<SharedExtras<(StreamId, TimeSec)>> {
    let mut extras = SharedExtras::default();
    let mut seen = HashSet::new();
    let mut counts = HashMap::<(StreamId, TimeSec), (u32, u32)>::new();
    let mut count_overflow = false;
    state.object_table.for_each(|object| {
        for id in &object.streams {
            let Some(index) =
                usize::try_from(id.get()).ok().filter(|index| *index < state.stream_table.len)
            else {
                count_overflow = true;
                continue;
            };
            let stream = &state.stream_table[index];
            if stream.id != *id {
                count_overflow = true;
                continue;
            }
            let count = counts.entry((*id, stream.created)).or_default();
            if !object.deletion.soft_deleted() {
                if let Some(value) = count.0.checked_add(1) {
                    count.0 = value
                } else {
                    count_overflow = true;
                }
            }
            if !object.deletion.deleted() {
                if let Some(value) = count.1.checked_add(1) {
                    count.1 = value
                } else {
                    count_overflow = true;
                }
            }
        }
    });
    if count_overflow {
        return Err(Error::new("DN stream reference count overflow or invalid reference"));
    }
    let mut section = read_section(file, range, Op::DnCkStrX, None)?;
    for _ in 0..section.count {
        let row: StreamExtra = read_piece(file, &mut section.left)?;
        let key = (StreamId::of_number(row.stream)?, TimeSec::new(row.created));
        if !seen.insert(key) {
            return Err(Error::new("duplicate DN stream extra"));
        }
        let index = key.0.get() as usize;
        if index >= state.stream_table.len {
            return Err(Error::new("DN stream extra names an unknown stream"));
        }
        let stream = &state.stream_table[index];
        if stream.id != key.0 || stream.created != key.1 || !stream.deletion.extra() {
            return Err(Error::new("DN stream extra flag does not match checkpoint data"));
        }
        let (soft, hard) = counts.get(&key).copied().unwrap_or_default();
        let retention = u32::try_from(row.max_retention)
            .map_err(|_| Error::new("DN retention value overflow"))? as i32;
        if soft > hard {
            return Err(Error::new("invalid DN stream reference counts"));
        }
        extras.restore(
            key,
            Extra {
                retain_until: TimeSec::new(row.until),
                references: ReferenceCounts {
                    soft: AtomicU32::new(soft),
                    hard: AtomicU32::new(hard),
                    max_retention: TimeIntSec32::new(retention),
                },
            },
        );
    }
    if section.left != 0 {
        return Err(Error::new("trailing DN stream extras"));
    }
    for (index, stream) in state.stream_table.iter().enumerate() {
        if stream.id.get() as usize != index {
            continue;
        }
        let key = (stream.id, stream.created);
        let found = seen.contains(&key);
        if stream.deletion.extra() != found {
            return Err(Error::new("DN stream extra is missing from checkpoint"));
        }
        let (soft, hard) = counts.get(&key).copied().unwrap_or_default();
        if (soft == 0) != stream.deletion.soft_deleted() || (hard == 0) != stream.deletion.deleted()
        {
            return Err(Error::new("DN stream flags disagree with object references"));
        }
        if !found
            && (soft != u32::from(!stream.deletion.soft_deleted())
                || hard != u32::from(!stream.deletion.deleted()))
        {
            return Err(Error::new("DN shared stream is missing reference-count metadata"));
        }
    }
    Ok(extras)
}

/// Read the application image after Raft selected this checkpoint.
pub(super) fn read_state(path: &Path, expected: RangeId, workers: usize) -> Result<Restored> {
    let mut file = File::open(path)?;
    let (footer, _) = read_public(&mut file)?;
    let meta = read_meta(&mut file, footer.range_meta)?;
    if meta.range != expected {
        return Err(Error::new("DN checkpoint range mismatch"));
    }
    let buckets = u32::try_from(meta.object_buckets)
        .map_err(|_| Error::new("DN checkpoint object bucket count is too large"))?;
    let mut state = DNState::with_object_buckets(buckets)?;
    if state.object_table.bucket_count() != meta.object_buckets {
        return Err(Error::new("DN checkpoint object bucket count is invalid"));
    }
    read_streams(
        &mut file,
        footer.streams,
        &footer.stream_chunks,
        &mut state,
        meta.stream_slots,
        workers,
    )?;
    let mut refs =
        read_objects(&mut file, footer.objects, &footer.object_chunks, &mut state, workers)?;
    let object_extras = read_object_extras(&mut file, footer.object_extras, &mut refs)?;
    let stream_extras = read_stream_extras(&mut file, footer.stream_extras, &state)?;

    let object_floor = refs.keys().map(|(id, _)| id.get() as u64 + 1).max().unwrap_or(0);
    let stream_floor = meta.stream_slots.number();
    if meta.next_object.number() < object_floor || meta.next_stream.number() < stream_floor {
        return Err(Error::new("DN checkpoint id high-water precedes live metadata"));
    }
    Ok(Restored {
        state,
        object_extras,
        stream_extras,
        next_object: meta.next_object.number(),
        next_stream: meta.next_stream.number(),
        index: footer.raft.index,
    })
}

pub(super) fn is_dn_ckpt(path: &Path) -> Result<bool> {
    let mut file = File::open(path)?;
    Ok(raft::checkpoint::read_footer(&mut file).is_ok_and(|footer| footer.op == Op::DnCkFooter))
}
