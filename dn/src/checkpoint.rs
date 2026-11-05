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
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicU32};

const REF_CHUNK: usize = 2048;

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
    objects: Range,
    object_refs: Range,
    object_extras: Range,
    stream_extras: Range,
}
raft::wire_struct!(Footer, DnCkFooter, {
    raft,sequence,second,range_meta,streams,objects,object_refs,object_extras,stream_extras
});

struct RangeMeta {
    range: RangeId,
    stream_slots: StreamId,
    next_object: ObjectId,
    next_stream: StreamId,
}

impl codec::Wire for RangeMeta {
    fn write(&self, writer: &mut common::StructWriter) {
        writer.nested(Op::DnCkRange, |writer| {
            writer.range_id(self.range);
            writer.stream_id(self.stream_slots);
            writer.object_id(self.next_object);
            writer.stream_id(self.next_stream);
        });
    }

    fn read(reader: &mut common::StructReader<'_>) -> Result<Self> {
        reader.nested(Op::DnCkRange, "RangeMeta", |reader| {
            Ok(Self {
                range: reader.range_id()?,
                stream_slots: reader.stream_id()?,
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

/// Write a complete DN checkpoint to an empty target.
pub(super) fn write(
    file: &mut (impl Write + Seek),
    raft_status: &raft::checkpoint::RaftStatus,
    state: &DNState,
    range: RangeId,
    next_object: u64,
    next_stream: u64,
    object_extras: &[ObjExtraOut],
    stream_extras: &[StreamExtraOut],
    sequence: u64,
    second: u64,
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
    let mut stream_count = 0u64;
    for (at, stream) in state.stream_table.iter().enumerate() {
        if stream.id.get() as usize == at {
            stream_count =
                stream_count.checked_add(1).ok_or_else(|| Error::new("too many DN streams"))?;
        }
    }
    let object_count = state.object_table.len();
    let mut ref_count = 0u64;
    let mut count_overflow = false;
    state.object_table.for_each(|object| {
        match ref_count.checked_add(object.streams.len().div_ceil(REF_CHUNK) as u64) {
            Some(next) => ref_count = next,
            None => count_overflow = true,
        }
    });
    if count_overflow {
        return Err(Error::new("too many DN object stream-reference chunks"));
    }

    let mut output = BufWriter::new(file);
    let meta_at = section_start(&mut output, Op::DnCkMeta, 1)?;
    write_piece(
        &mut output,
        &RangeMeta {
            range,
            stream_slots,
            next_object: ObjectId::of_number(next_object)?,
            next_stream: StreamId::of_number(next_stream)?,
        },
    )?;
    let range_meta = section_range(&mut output, meta_at)?;

    let streams_at = section_start(&mut output, Op::DnCkStreams, stream_count)?;
    for (at, stream) in state.stream_table.iter().enumerate() {
        if stream.id.get() as usize != at {
            continue;
        }
        write_piece(
            &mut output,
            &StreamRow {
                id: stream.id.number(),
                flags: stream.deletion.bits(),
                records: stream.records.number(),
                created: stream.created.number(),
                logical: stream.logical.number(),
                compressed: stream.compressed.number(),
                sn: stream.sn.number(),
                extents: stream.extents.number(),
            },
        )?;
    }
    let streams = section_range(&mut output, streams_at)?;

    let objects_at = section_start(&mut output, Op::DnCkObjects, object_count)?;
    let mut write_result = Ok(());
    state.object_table.for_each(|object| {
        if write_result.is_ok() {
            write_result = write_piece(&mut output, &object_row(object));
        }
    });
    write_result?;
    let objects = section_range(&mut output, objects_at)?;

    let refs_at = section_start(&mut output, Op::DnCkRefs, ref_count)?;
    write_result = Ok(());
    state.object_table.for_each(|object| {
        for (chunk, streams) in object.streams.chunks(REF_CHUNK).enumerate() {
            if write_result.is_err() {
                break;
            }
            let row = RefRow {
                object: object.id.number(),
                created: object.created.number(),
                at: (chunk * REF_CHUNK) as u64,
                streams: streams.iter().map(|stream| stream.number()).collect(),
            };
            write_result = write_piece(&mut output, &row);
        }
    });
    write_result?;
    let object_refs = section_range(&mut output, refs_at)?;

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
        objects,
        object_refs,
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
        write(&mut file, status, state, range, 0, 0, &[], &[], sequence, TimeSec::now().number())?;
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
        footer.object_refs,
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

fn read_section(file: &mut File, range: Range, op: Op) -> Result<SectionRead> {
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
    if count > left / 9 {
        return Err(Error::new("DN checkpoint row count exceeds section length"));
    }
    Ok(SectionRead { count, left })
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
    let mut section = read_section(file, range, Op::DnCkMeta)?;
    if section.count != 1 {
        return Err(Error::new("DN checkpoint needs one range record"));
    }
    let meta = read_piece(file, &mut section.left)?;
    if section.left != 0 {
        return Err(Error::new("trailing DN range metadata"));
    }
    Ok(meta)
}

fn read_streams(file: &mut File, range: Range, state: &mut DNState, len: StreamId) -> Result<()> {
    let mut section = read_section(file, range, Op::DnCkStreams)?;
    let mut previous = None;
    for _ in 0..section.count {
        let row: StreamRow = read_piece(file, &mut section.left)?;
        if row.id >= u64::from(len.get()) || previous.is_some_and(|id| id >= row.id) {
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
        state
            .stream_table
            .install(stream)
            .map_err(|error| Error::new(format!("restore DN stream {id}: {error}")))?;
    }
    if section.left != 0 {
        return Err(Error::new("trailing DN checkpoint streams"));
    }
    state.stream_table.finish_restore(len.get() as usize)?;
    Ok(())
}

struct RefProgress {
    name: String,
    next: usize,
    total: usize,
}

fn read_objects(
    file: &mut File,
    range: Range,
    state: &mut DNState,
) -> Result<HashMap<(ObjectId, TimeSec), RefProgress>> {
    let mut section = read_section(file, range, Op::DnCkObjects)?;
    let count = usize::try_from(section.count)
        .map_err(|_| Error::new("DN checkpoint object count is too large"))?;
    state
        .object_table
        .try_reserve(count)
        .map_err(|error| Error::new(format!("reserve DN checkpoint object table: {error}")))?;
    let mut refs = HashMap::new();
    let mut names = HashSet::new();
    refs.try_reserve(count)
        .map_err(|error| Error::new(format!("reserve DN checkpoint object index: {error}")))?;
    names
        .try_reserve(count)
        .map_err(|error| Error::new(format!("reserve DN checkpoint object names: {error}")))?;
    for _ in 0..section.count {
        let row: ObjectRow = read_piece(file, &mut section.left)?;
        let id = ObjectId::of_number(row.id)?;
        if id.is_none() {
            return Err(Error::new("DN checkpoint object uses the no-object id"));
        }
        let created = TimeSec::new(row.created);
        let key = (id, created);
        if !names.insert(row.name.clone()) || refs.contains_key(&key) {
            return Err(Error::new("duplicate DN checkpoint object"));
        }
        let width =
            u32::try_from(row.width).map_err(|_| Error::new("invalid DN object framing width"))?;
        let framing = Framing::of_wire(row.framing, width)?;
        let count = usize::try_from(row.stream_count)
            .map_err(|_| Error::new("DN object stream list is too large"))?;
        let mut streams = Vec::new();
        streams
            .try_reserve_exact(count)
            .map_err(|_| Error::new("DN object stream allocation failed"))?;
        let object = Object {
            link: ObjectId::NONE.get(),
            name: row.name.clone().into_boxed_str(),
            deletion: Flags::from_bits(row.flags)?,
            purge_only: row.purge_only,
            id,
            framing,
            streams,
            localflags: AtomicU8::new(0),
            keyed: row.keyed,
            created,
        };
        if state
            .object_table
            .insert_value(&row.name, object)
            .map_err(|error| Error::new(format!("restore DN object: {error}")))?
            .is_some()
        {
            return Err(Error::new("duplicate DN checkpoint object path"));
        }
        refs.insert(key, RefProgress { name: row.name, next: 0, total: count });
    }
    if section.left != 0 {
        return Err(Error::new("trailing DN checkpoint objects"));
    }
    Ok(refs)
}

fn read_refs(
    file: &mut File,
    range: Range,
    state: &mut DNState,
    refs: &mut HashMap<(ObjectId, TimeSec), RefProgress>,
) -> Result<()> {
    let mut section = read_section(file, range, Op::DnCkRefs)?;
    for _ in 0..section.count {
        let row: RefRow = read_piece(file, &mut section.left)?;
        let key = (ObjectId::of_number(row.object)?, TimeSec::new(row.created));
        let progress = refs
            .get_mut(&key)
            .ok_or_else(|| Error::new("DN checkpoint references an unknown object"))?;
        let at =
            usize::try_from(row.at).map_err(|_| Error::new("DN reference offset too large"))?;
        if at != progress.next || row.streams.is_empty() || row.streams.len() > REF_CHUNK {
            return Err(Error::new("invalid DN object stream-reference chunk"));
        }
        let end = at
            .checked_add(row.streams.len())
            .ok_or_else(|| Error::new("DN object stream count overflow"))?;
        if end > progress.total {
            return Err(Error::new("DN object stream references exceed declared length"));
        }
        let object = state
            .object_table
            .get_mut(&progress.name)
            .ok_or_else(|| Error::new("DN checkpoint object disappeared"))?;
        for number in row.streams {
            let id = StreamId::of_number(number)?;
            let index = id.get() as usize;
            if index >= state.stream_table.len || state.stream_table[index].id != id {
                return Err(Error::new("DN object references a missing stream"));
            }
            object.streams.push(id);
        }
        progress.next = end;
    }
    if section.left != 0 || refs.values().any(|progress| progress.next != progress.total) {
        return Err(Error::new("incomplete DN object stream references"));
    }
    Ok(())
}

fn read_object_extras(
    file: &mut File,
    range: Range,
    state: &DNState,
    refs: &HashMap<(ObjectId, TimeSec), RefProgress>,
) -> Result<Extras<(ObjectId, TimeSec)>> {
    let mut extras = Extras::default();
    let mut seen = HashSet::new();
    let mut section = read_section(file, range, Op::DnCkObjX)?;
    for _ in 0..section.count {
        let row: ObjExtra = read_piece(file, &mut section.left)?;
        let key = (ObjectId::of_number(row.object)?, TimeSec::new(row.created));
        if !seen.insert(key) {
            return Err(Error::new("duplicate DN object extra"));
        }
        let progress = refs
            .get(&key)
            .ok_or_else(|| Error::new("DN checkpoint extra names an unknown object"))?;
        let object = state
            .object_table
            .get(&progress.name)
            .ok_or_else(|| Error::new("DN checkpoint extra names an unknown object"))?;
        if !object.deletion.extra() {
            return Err(Error::new("DN object extra flag does not match checkpoint data"));
        }
        extras.restore(key, Extra { retain_until: TimeSec::new(row.until), references: () });
    }
    if section.left != 0 {
        return Err(Error::new("trailing DN object extras"));
    }
    let mut mismatch = false;
    state.object_table.for_each(|object| {
        if object.deletion.extra() != seen.contains(&(object.id, object.created)) {
            mismatch = true;
        }
    });
    if mismatch {
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
    let mut section = read_section(file, range, Op::DnCkStrX)?;
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
pub(super) fn read_state(path: &Path, expected: RangeId) -> Result<Restored> {
    let mut file = File::open(path)?;
    let (footer, _) = read_public(&mut file)?;
    let meta = read_meta(&mut file, footer.range_meta)?;
    if meta.range != expected {
        return Err(Error::new("DN checkpoint range mismatch"));
    }
    let mut state = DNState::new()?;
    read_streams(&mut file, footer.streams, &mut state, meta.stream_slots)?;
    let mut refs = read_objects(&mut file, footer.objects, &mut state)?;
    read_refs(&mut file, footer.object_refs, &mut state, &mut refs)?;
    let object_extras = read_object_extras(&mut file, footer.object_extras, &state, &refs)?;
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
