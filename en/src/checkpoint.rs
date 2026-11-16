//! Per-disk EN checkpoint state.
use super::*;

const VERSION: u16 = 1;

pub(super) fn decode(data: &[u8], stage: Option<Arc<journal::DiskFiles>>) -> Result<Extents> {
    let mut input = ReadCheckpoint { bytes: data, at: 0 };
    if input.u16()? != VERSION {
        return Err(Error::new("unsupported EN checkpoint state version"));
    }
    let extent_count = input.count("EN checkpoint extents")?;
    let mut extents = Extents::new();
    extents.stage = stage;
    let mut marks = Vec::with_capacity(extent_count);
    for _ in 0..extent_count {
        input.op(Op::EnCkExtent)?;
        let state = input.extent_state()?;
        let written = input.mark()?;
        let durable = input.mark()?;
        let place = extents.apply_state(state)?;
        if place + 1 != extents.extents.len() {
            return Err(Error::new("duplicate extent in EN checkpoint"));
        }
        marks.push(((state.sn, state.id, state.created), Marks { written, durable }));
    }
    for (key, value) in marks {
        extents.marks.insert(key, value);
    }

    let append_count = input.count("EN checkpoint appends")?;
    for _ in 0..append_count {
        input.op(Op::EnCkAppend)?;
        let place = input.u32()? as usize;
        if place >= extents.extents.len() {
            return Err(Error::new("EN checkpoint append has an invalid extent index"));
        }
        let offset = ECoff::new(input.u32()?);
        let logical = ELoff::new(input.u32()?);
        let first_record = ERoff::new(input.u32()?);
        let scn = input.option_u64()?.map(Scn::new);
        let stage_ref = input.stage_ref()?;
        let holds = input.holds()?;
        let written = input.flag()?;
        let quorum = input.flag()?;
        let block_count = input.count("EN checkpoint append blocks")?;
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            let logical = ELoff::new(input.u32()?);
            let first_record = ERoff::new(input.u32()?);
            let holds = input.holds()?;
            let crc = input.u64()?;
            let framing = Framing::of_wire(input.u8()?, input.u32()?)?;
            let sealed = input.vec()?;
            let block_stage = input.stage_ref()?;
            let stage_tag = input.take(16)?.try_into().unwrap();
            let stage_ad = if input.flag()? { Some(Arc::<[u8]>::from(input.vec()?)) } else { None };
            let stage_timestamp = input.option_u64()?.map(TimeSec::new);
            let stage_part = if input.flag()? { Some((input.u32()?, input.u32()?)) } else { None };
            blocks.push(Encrypted {
                logical,
                first_record,
                holds,
                crc,
                framing,
                sealed,
                stage: block_stage,
                stage_tag,
                stage_ad,
                stage_timestamp,
                stage_part,
            });
        }
        let append = Append {
            extent: place,
            offset,
            logical,
            first_record,
            scn,
            stage: stage_ref,
            holds,
            written,
            quorum,
            blocks,
        };
        if let (Some(scn), Some(stage_ref)) = (append.scn, append.stage) {
            extents.index_staged_append(place, stage_ref, scn, logical, first_record)?;
        }
        extents.appends.push(append);
    }
    input.done()?;

    for extent in &extents.extents {
        let key = (extent.sn_id, extent.id, extent.created);
        extents.marks.entry(key).or_default();
    }
    Ok(extents)
}

pub(super) fn encode(extents: &Extents) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    put_count(&mut bytes, extents.extents.len(), "EN checkpoint extents")?;
    for extent in &extents.extents {
        bytes.extend_from_slice(&Op::EnCkExtent.code().to_le_bytes());
        let key = (extent.sn_id, extent.id, extent.created);
        put_extent_state(
            &mut bytes,
            ExtentState {
                sn: extent.sn_id,
                id: extent.id,
                created: extent.created,
                deletion: extent.deletion,
                sealed: extent.sealed,
                open_record: extent.open_record,
                end: extent.end,
                logical_end: extent.logical_end,
                records: extent.records,
                retain_until: extents
                    .extras
                    .get(&key)
                    .map_or(TimeSec::EPOCH, |extra| extra.retain_until),
            },
        );
        let marks = extents.marks.get(&key);
        put_mark(&mut bytes, marks.map_or(Mark::default(), |marks| marks.written));
        put_mark(&mut bytes, marks.map_or(Mark::default(), |marks| marks.durable));
    }

    put_count(&mut bytes, extents.appends.len(), "EN checkpoint appends")?;
    for append in &extents.appends {
        if !append.written || append.stage.is_none() || append.scn.is_none() {
            return Err(Error::new("EN checkpoint contains an append without durable stage data"));
        }
        bytes.extend_from_slice(&Op::EnCkAppend.code().to_le_bytes());
        put_u32(
            &mut bytes,
            u32::try_from(append.extent).map_err(|_| Error::new("EN extent index exceeds u32"))?,
        );
        put_u32(&mut bytes, append.offset.get());
        put_u32(&mut bytes, append.logical.get());
        put_u32(&mut bytes, append.first_record.get());
        put_option_u64(&mut bytes, append.scn.map(Scn::number));
        put_stage_ref(&mut bytes, append.stage);
        put_holds(&mut bytes, append.holds);
        bytes.push(u8::from(append.written));
        bytes.push(u8::from(append.quorum));
        put_count(&mut bytes, append.blocks.len(), "EN checkpoint append blocks")?;
        for block in &append.blocks {
            if block.stage.is_none()
                || block.stage_ad.is_none()
                || block.stage_timestamp.is_none()
                || block.stage_part.is_none()
            {
                return Err(Error::new("EN checkpoint append block is missing its stage locator"));
            }
            put_u32(&mut bytes, block.logical.get());
            put_u32(&mut bytes, block.first_record.get());
            put_holds(&mut bytes, block.holds);
            put_u64(&mut bytes, block.crc);
            let (kind, width) = block.framing.wire();
            bytes.push(kind);
            put_u32(&mut bytes, width);
            put_vec(&mut bytes, &block.sealed)?;
            put_stage_ref(&mut bytes, block.stage);
            bytes.extend_from_slice(&block.stage_tag);
            match &block.stage_ad {
                Some(ad) => {
                    bytes.push(1);
                    put_vec(&mut bytes, ad)?;
                }
                None => bytes.push(0),
            }
            put_option_u64(&mut bytes, block.stage_timestamp.map(TimeSec::number));
            match block.stage_part {
                Some((at, len)) => {
                    bytes.push(1);
                    put_u32(&mut bytes, at);
                    put_u32(&mut bytes, len);
                }
                None => bytes.push(0),
            }
        }
    }
    Ok(bytes)
}

pub(super) fn write_next(storage: &Arc<journal::DiskFiles>) -> Result<()> {
    let boundary = storage.rotate_for_checkpoint()?;
    let previous = storage.latest_checkpoint();
    let mut max_scn = previous.as_ref().and_then(|checkpoint| checkpoint.max_scn);
    let mut state = if let Some(previous) = previous {
        decode(&previous.data, Some(Arc::clone(storage)))?
    } else {
        let mut empty = Extents::new();
        empty.stage = Some(Arc::clone(storage));
        empty
    };
    for record in storage.records_since_checkpoint(boundary.cursor)? {
        if max_scn.is_none_or(|max| record.scn.compare(max).is_gt()) {
            max_scn = Some(record.scn);
        }
        match record.op {
            Op::EnJAppend => state.restore_append(record)?,
            Op::EnJExtent | Op::EnJDelete | Op::EnJSeal => state.restore_state(record)?,
            Op::EnJDurable => state.restore_durable(record)?,
            op => return Err(Error::new(format!("unsupported EN journal operation {op}"))),
        }
    }
    let checkpoint = storage.publish_checkpoint(boundary, max_scn, encode(&state)?)?;
    logi!(
        "EN disk checkpoint written",
        "disk {},generation {},journal {},end {},bytes {}",
        storage.disk_id(),
        checkpoint.generation,
        checkpoint.cursor.file,
        checkpoint.cursor.end,
        checkpoint.size
    );
    Ok(())
}

fn put_mark(bytes: &mut Vec<u8>, mark: Mark) {
    put_u32(bytes, mark.logical.get());
    put_u32(bytes, mark.records.get());
}

fn put_stage_ref(bytes: &mut Vec<u8>, stage: Option<journal::StageRef>) {
    match stage {
        Some(stage) => {
            bytes.push(1);
            bytes.extend_from_slice(&stage.file.get().to_le_bytes());
            put_u32(bytes, stage.offset);
            put_u32(bytes, stage.len);
        }
        None => bytes.push(0),
    }
}

fn put_option_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            put_u64(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn put_count(bytes: &mut Vec<u8>, value: usize, what: &str) -> Result<()> {
    put_u32(bytes, u32::try_from(value).map_err(|_| Error::new(format!("too many {what}")))?);
    Ok(())
}

fn put_vec(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_count(bytes, value.len(), "checkpoint byte strings")?;
    bytes.extend_from_slice(value);
    Ok(())
}

struct ReadCheckpoint<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> ReadCheckpoint<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end =
            self.at.checked_add(len).ok_or_else(|| Error::new("EN checkpoint offset overflow"))?;
        let value = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| Error::new("truncated EN checkpoint state"))?;
        self.at = end;
        Ok(value)
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

    fn op(&mut self, expected: Op) -> Result<()> {
        let got = Op::from_code(self.u16()?)?;
        if got == expected {
            Ok(())
        } else {
            Err(Error::new(format!("expected {expected} in EN checkpoint, got {got}")))
        }
    }

    fn count(&mut self, what: &str) -> Result<usize> {
        let count = self.u32()? as usize;
        if count > self.bytes.len() {
            return Err(Error::new(format!("invalid {what} count in EN checkpoint")));
        }
        Ok(count)
    }

    fn flag(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::new("invalid EN checkpoint flag")),
        }
    }

    fn option_u64(&mut self) -> Result<Option<u64>> {
        if self.flag()? { Ok(Some(self.u64()?)) } else { Ok(None) }
    }

    fn vec(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn holds(&mut self) -> Result<Holds> {
        Ok(Holds {
            compressed: common::offset::ECoff::new(self.u32()?),
            logical: ELoff::new(self.u32()?),
            records: ERoff::new(self.u32()?),
        })
    }

    fn stage_ref(&mut self) -> Result<Option<journal::StageRef>> {
        if !self.flag()? {
            return Ok(None);
        }
        let id = u16::from_le_bytes(self.take(2)?.try_into().unwrap());
        Ok(Some(journal::StageRef {
            file: journal::StageId::from_number(id)?,
            offset: self.u32()?,
            len: self.u32()?,
        }))
    }

    fn mark(&mut self) -> Result<Mark> {
        Ok(Mark { logical: ELoff::new(self.u32()?), records: ERoff::new(self.u32()?) })
    }

    fn extent_state(&mut self) -> Result<ExtentState> {
        let mut row = JournalRead::new(&self.bytes[self.at..]);
        let state = read_extent_state(&mut row)?;
        self.at = self
            .at
            .checked_add(row.at)
            .ok_or_else(|| Error::new("EN checkpoint offset overflow"))?;
        Ok(state)
    }

    fn done(self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::new("trailing bytes in EN checkpoint"))
        }
    }
}
