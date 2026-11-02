//! KS application checkpoint codec.
use super::{KeyHistory, KsState, MasterEpoch, Peer, TimeSec};
use common::scn::Scn;
use common::{Error, Op, Result};
use raft::codec;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Clone)]
pub(super) struct Mark {
    pub raft: raft::checkpoint::RaftStatus,
    pub sequence: u64,
    pub second: u64,
}

impl Mark {
    fn validate(&self) -> Result<()> {
        self.raft.validate()
    }

    pub(super) fn capture(
        state: &raft::State,
        snapshot: &raft::Snapshot,
        sequence: u64,
        second: u64,
    ) -> Result<Self> {
        let mark = Self {
            raft: raft::checkpoint::RaftStatus::capture(state, snapshot)?,
            sequence,
            second,
        };
        mark.validate()?;
        Ok(mark)
    }
}

/// Read a KS checkpoint into Raft state, or use the generic format for older files.
pub(super) fn read_raft(
    path: &Path,
    entry: u64,
    ring: &str,
    ignore: bool,
) -> Result<(raft::State, u64, u64, u64)> {
    let mut file = File::open(path)?;
    let is_ks =
        raft::checkpoint::read_footer(&mut file).is_ok_and(|footer| footer.op == Op::KsCkFooter);
    if !is_ks {
        return raft::journal::read_checkpoint(path, entry, ring, ignore);
    }
    let public = read_public(&mut file)?;
    let mark = public.mark;
    let status = mark.raft;
    if status.index != entry || (!ignore && status.ring != ring) {
        return Err(Error::new("KS checkpoint ring or sequence mismatch"));
    }
    let second = mark.second;
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
    let length = file.metadata()?.len();
    Ok((state, mark.sequence, second, length))
}

pub(super) fn read_public_path(path: &Path) -> Result<Option<Public>> {
    let mut file = File::open(path)?;
    let footer = match raft::checkpoint::read_footer(&mut file) {
        Ok(footer) => footer,
        Err(_) => return Ok(None),
    };
    if footer.op != Op::KsCkFooter {
        return Ok(None);
    }
    file.rewind()?;
    read_public(&mut file).map(Some)
}

#[derive(Clone, Copy)]
struct Range {
    start: u64,
    len: u64,
}
raft::wire_struct!(Range, KsCkRange, {start,len});
struct KsFooter {
    raft: raft::checkpoint::RaftStatus,
    sequence: u64,
    second: u64,
    verifier: Option<[u8; 32]>,
    peers: Range,
    keys: Range,
}
raft::wire_struct!(KsFooter, KsCkFooter, {raft,sequence,second,verifier,peers,keys});
struct PeerRow {
    identity: String,
    kind: String,
    address: String,
    domain: Vec<String>,
    media_class: Option<String>,
    ring_identity: Option<String>,
}
raft::wire_struct!(PeerRow, KsCkPeer, {identity,kind,address,domain,media_class,ring_identity});
impl PeerRow {
    fn from_peer(peer: &Peer) -> Self {
        Self {
            identity: peer.identity.clone(),
            kind: peer.kind.as_str().into(),
            address: peer.address.to_string(),
            domain: peer.domain.clone(),
            media_class: peer.media_class.clone(),
            ring_identity: peer.ring_identity.as_ref().map(ToString::to_string),
        }
    }
    fn into_peer(self) -> Result<Peer> {
        Ok(Peer {
            identity: self.identity,
            kind: common::config::ServiceKind::parse(&self.kind)?,
            address: self
                .address
                .parse()
                .map_err(|_| Error::new("invalid checkpoint peer address"))?,
            domain: self.domain,
            media_class: self.media_class,
            ring_identity: self
                .ring_identity
                .map(common::ring::RingIdentity::try_from)
                .transpose()?,
        })
    }
}
struct Epoch {
    // Effective until the next epoch's start; the last epoch is open-ended.
    since: u64,
    key: [u8; 32],
}
raft::wire_struct!(Epoch, KsCkEpoch, {since,key});
struct KeyRows {
    rows: Vec<Epoch>,
}
raft::wire_struct!(KeyRows, KsCkKeyChunk, { rows });
struct Sealed {
    nonce: [u8; 32],
    ciphertext: Vec<u8>,
    tag: [u8; 32],
}
raft::wire_struct!(Sealed, KsCkSeal, {nonce,ciphertext,tag});

pub(super) struct Public {
    pub mark: Mark,
    pub verifier: Option<[u8; 32]>,
    pub peers: BTreeMap<String, Peer>,
    keys: Range,
    ad: Vec<u8>,
}

fn associated(mark: &Mark, verifier: &Option<[u8; 32]>, peers: &[u8; 32]) -> Result<Vec<u8>> {
    let mut ad = b"DLAW-KS-CHECKPOINT-1".to_vec();
    ad.extend(encode_piece(&mark.raft)?);
    ad.extend_from_slice(&mark.sequence.to_le_bytes());
    ad.extend_from_slice(&mark.second.to_le_bytes());
    ad.extend(encode_piece(verifier)?);
    ad.extend_from_slice(peers);
    Ok(ad)
}

// Checkpoint pieces are not network frames and have no network frame size limit.
fn encode_piece<T: codec::Wire>(value: &T) -> Result<Vec<u8>> {
    let mut writer = common::StructWriter::new();
    value.write(&mut writer);
    let arg = writer.finish(Op::Raft);
    if arg.depth > common::MAX_RECURSION_DEPTH {
        return Err(Error::new("checkpoint nesting exceeds limit"));
    }
    let mut bytes = vec![arg.depth];
    arg.into_bytes().write_to(&mut bytes)?;
    Ok(bytes)
}

fn decode_piece<T: codec::Wire>(bytes: &[u8]) -> Result<T> {
    let (&depth, bytes) =
        bytes.split_first().ok_or_else(|| Error::new("empty checkpoint piece"))?;
    if depth == 0 || depth > common::MAX_RECURSION_DEPTH || isize::try_from(bytes.len()).is_err() {
        return Err(Error::new("invalid checkpoint piece limits"));
    }
    let mut reader = common::StructReader::new(bytes, depth);
    let value = T::read(&mut reader)?;
    reader.done()?;
    Ok(value)
}

fn write_peers(file: &mut impl Write, state: &KsState) -> Result<(u64, [u8; 32])> {
    let mut hash = Sha256::new();
    let mut len = 0u64;
    let mut write = |bytes: &[u8]| -> Result<()> {
        file.write_all(bytes)?;
        hash.update(bytes);
        len = len
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::new("checkpoint length overflow"))?;
        Ok(())
    };
    write(&Op::KsCkPeers.code().to_le_bytes())?;
    write(
        &u64::try_from(state.registrations.peers.len())
            .map_err(|_| Error::new("too many checkpoint peers"))?
            .to_le_bytes(),
    )?;
    for peer in state.registrations.peers.values() {
        let bytes = encode_piece(&PeerRow::from_peer(peer))?;
        let size =
            u64::try_from(bytes.len()).map_err(|_| Error::new("checkpoint peer too large"))?;
        write(&size.to_le_bytes())?;
        write(&bytes)?;
    }
    Ok((len, hash.finalize().into()))
}

fn read_part(
    file: &mut impl Read,
    remaining: &mut u64,
    hash: &mut Sha256,
    bytes: &mut [u8],
) -> Result<()> {
    *remaining = remaining
        .checked_sub(bytes.len() as u64)
        .ok_or_else(|| Error::new("truncated checkpoint peers"))?;
    file.read_exact(bytes)?;
    hash.update(&*bytes);
    Ok(())
}

fn read_peers(
    file: &mut (impl Read + Seek),
    range: Range,
) -> Result<(BTreeMap<String, Peer>, [u8; 32])> {
    file.seek(SeekFrom::Start(range.start))?;
    let mut input = BufReader::new(file);
    let mut remaining = range.len;
    let mut hash = Sha256::new();
    let mut header = [0u8; 10];
    read_part(&mut input, &mut remaining, &mut hash, &mut header)?;
    if u16::from_le_bytes(header[..2].try_into().unwrap()) != Op::KsCkPeers.code() {
        return Err(Error::new("wrong checkpoint peers tag"));
    }
    let count = u64::from_le_bytes(header[2..].try_into().unwrap());
    if count > remaining / 8 {
        return Err(Error::new("invalid checkpoint peer count"));
    }
    let mut peers = BTreeMap::new();
    for _ in 0..count {
        let mut size = [0; 8];
        read_part(&mut input, &mut remaining, &mut hash, &mut size)?;
        let size = u64::from_le_bytes(size);
        if size > remaining {
            return Err(Error::new("invalid checkpoint peer length"));
        }
        let size = usize::try_from(size).map_err(|_| Error::new("checkpoint peer too large"))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(|_| Error::new("checkpoint allocation failed"))?;
        bytes.resize(size, 0);
        read_part(&mut input, &mut remaining, &mut hash, &mut bytes)?;
        let row: PeerRow = decode_piece(&bytes)?;
        let peer = row.into_peer()?;
        if peers.insert(peer.identity.clone(), peer).is_some() {
            return Err(Error::new("duplicate checkpoint peer"));
        }
    }
    if remaining != 0 {
        return Err(Error::new("trailing checkpoint peer data"));
    }
    Ok((peers, hash.finalize().into()))
}

fn end(range: Range) -> Result<u64> {
    range.start.checked_add(range.len).ok_or_else(|| Error::new("checkpoint range overflow"))
}

// About 1.44 MiB of serialized key records per full encryption group.
pub(super) const KEY_GROUP: usize = 32 * 1024;

fn key_nonce(mark: &Mark, random: &[u8; 32], index: u64) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"DLAW-KS-CHECKPOINT-NONCE-1");
    hash.update(mark.raft.term.to_le_bytes());
    hash.update(mark.raft.index.to_le_bytes());
    hash.update(mark.raft.scn.number().to_le_bytes());
    hash.update(random);
    hash.update(index.to_le_bytes());
    hash.finalize().into()
}

fn key_ad(base: &[u8], count: u64, group: u64, index: u64) -> Vec<u8> {
    let mut ad = base.to_vec();
    ad.extend(count.to_le_bytes());
    ad.extend(group.to_le_bytes());
    ad.extend(index.to_le_bytes());
    ad
}

fn write_keys(
    file: &mut impl Write,
    history: &KeyHistory,
    key: &[u8; 32],
    mark: &Mark,
    random: [u8; 32],
    ad: &[u8],
    group: usize,
) -> Result<u64> {
    let count =
        u64::try_from(history.epochs.len()).map_err(|_| Error::new("too many checkpoint keys"))?;
    file.write_all(&Op::KsCkKeys.code().to_le_bytes())?;
    file.write_all(&count.to_le_bytes())?;
    let group_num = u64::try_from(group).map_err(|_| Error::new("invalid checkpoint key group"))?;
    file.write_all(&group_num.to_le_bytes())?;
    file.write_all(&random)?;
    let mut length = 50u64;
    let groups = history.epochs.len().div_ceil(group).max(1);
    for index in 0..groups {
        let start = index * group;
        let end = start.saturating_add(group).min(history.epochs.len());
        let rows = history.epochs[start..end]
            .iter()
            .map(|epoch| Epoch { since: epoch.since.number(), key: epoch.key })
            .collect();
        let plain = encode_piece(&KeyRows { rows })?;
        let chunk_nonce = key_nonce(mark, &random, index as u64);
        let (ciphertext, tag) = aegis::aegis256::Aegis256::<32>::new(key, &chunk_nonce)
            .encrypt(&plain, &key_ad(ad, count, group_num, index as u64));
        let bytes = encode_piece(&Sealed { nonce: chunk_nonce, ciphertext, tag })?;
        let size =
            u64::try_from(bytes.len()).map_err(|_| Error::new("checkpoint key chunk too large"))?;
        file.write_all(&size.to_le_bytes())?;
        file.write_all(&bytes)?;
        length = length
            .checked_add(8)
            .and_then(|n| n.checked_add(size))
            .ok_or_else(|| Error::new("checkpoint length overflow"))?;
    }
    Ok(length)
}

fn key_read(file: &mut impl Read, remaining: &mut u64, bytes: &mut [u8]) -> Result<()> {
    *remaining = remaining
        .checked_sub(bytes.len() as u64)
        .ok_or_else(|| Error::new("truncated checkpoint keys"))?;
    file.read_exact(bytes)?;
    Ok(())
}

/// Write a complete checkpoint to an empty target. The caller supplies fresh random bytes.
pub(super) fn write(
    file: &mut (impl Write + Seek),
    mark: &Mark,
    state: &KsState,
    verifier: Option<[u8; 32]>,
    key: &[u8; 32],
    random: [u8; 32],
) -> Result<()> {
    write_with(file, mark, state, verifier, key, random, KEY_GROUP)
}

fn write_with(
    file: &mut (impl Write + Seek),
    mark: &Mark,
    state: &KsState,
    verifier: Option<[u8; 32]>,
    key: &[u8; 32],
    random: [u8; 32],
    group: usize,
) -> Result<()> {
    mark.validate()?;
    if group == 0 {
        return Err(Error::new("checkpoint key group must be positive"));
    }
    if verifier.as_ref().is_some_and(|value| !common::shamir::matches(key, value)) {
        return Err(Error::new("checkpoint unlock verifier mismatch"));
    }
    if file.seek(SeekFrom::End(0))? != 0 {
        return Err(Error::new("checkpoint target is not empty"));
    }
    if state.registrations.peers.iter().any(|(name, peer)| name != &peer.identity) {
        return Err(Error::new("checkpoint peer identity mismatch"));
    }
    let mut last = None;
    for epoch in &state.keys.epochs {
        let since = epoch.since.number();
        if last.is_some_and(|old| old >= since) {
            return Err(Error::new("invalid checkpoint key order"));
        }
        last = Some(since);
    }
    let mut output = BufWriter::new(file);
    let (peer_len, peer_hash) = write_peers(&mut output, state)?;
    let ad = associated(mark, &verifier, &peer_hash)?;
    let key_len = write_keys(&mut output, &state.keys, key, mark, random, &ad, group)?;
    let peers = Range { start: 0, len: peer_len };
    let keys = Range { start: end(peers)?, len: key_len };
    let footer = encode_piece(&KsFooter {
        raft: mark.raft.clone(),
        sequence: mark.sequence,
        second: mark.second,
        verifier,
        peers,
        keys,
    })?;
    raft::checkpoint::write_footer(&mut output, Op::KsCkFooter, &footer)?;
    output.flush()?;
    Ok(())
}

/// Read public state without an unlocking key. `read_keys` authenticates it.
pub(super) fn read_public(file: &mut (impl Read + Seek)) -> Result<Public> {
    let footer = raft::checkpoint::read_footer(file)?;
    if footer.op != Op::KsCkFooter {
        return Err(Error::new("wrong KS checkpoint footer"));
    }
    let variable_start = footer.start;
    let footer: KsFooter = decode_piece(&footer.bytes)?;
    let mark = Mark { raft: footer.raft, sequence: footer.sequence, second: footer.second };
    mark.validate()?;
    if footer.peers.start != 0
        || end(footer.peers)? != footer.keys.start
        || end(footer.keys)? != variable_start
    {
        return Err(Error::new("invalid KS checkpoint ranges"));
    }
    let (peers, peer_hash) = read_peers(file, footer.peers)?;
    let ad = associated(&mark, &footer.verifier, &peer_hash)?;
    Ok(Public { mark, verifier: footer.verifier, peers, keys: footer.keys, ad })
}

/// Authenticate and decode the master-key piece.
pub(super) fn read_keys(
    file: &mut (impl Read + Seek),
    public: &Public,
    key: &[u8; 32],
) -> Result<KeyHistory> {
    if public.verifier.as_ref().is_some_and(|value| !common::shamir::matches(key, value)) {
        return Err(Error::new("checkpoint unlock verifier mismatch"));
    }
    file.seek(SeekFrom::Start(public.keys.start))?;
    let mut input = BufReader::new(file);
    let mut remaining = public.keys.len;
    let mut header = [0u8; 50];
    key_read(&mut input, &mut remaining, &mut header)?;
    if u16::from_le_bytes(header[..2].try_into().unwrap()) != Op::KsCkKeys.code() {
        return Err(Error::new("wrong checkpoint keys tag"));
    }
    let count = u64::from_le_bytes(header[2..10].try_into().unwrap());
    let group = u64::from_le_bytes(header[10..18].try_into().unwrap());
    if group == 0 {
        return Err(Error::new("invalid checkpoint key group"));
    }
    let random: [u8; 32] = header[18..].try_into().unwrap();
    let groups = count.div_ceil(group).max(1);
    if groups > remaining / 8 {
        return Err(Error::new("invalid checkpoint key count"));
    }
    let mut history = KeyHistory::default();
    for index in 0..groups {
        let mut encoded_size = [0u8; 8];
        key_read(&mut input, &mut remaining, &mut encoded_size)?;
        let size = u64::from_le_bytes(encoded_size);
        let expected = count.saturating_sub(index * group).min(group);
        // Each tagged epoch has fixed fields; allow framing and authentication overhead.
        let limit = expected
            .checked_mul(64)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(|| Error::new("checkpoint key chunk too large"))?;
        if size > limit || size > remaining {
            return Err(Error::new("invalid checkpoint key chunk length"));
        }
        let size =
            usize::try_from(size).map_err(|_| Error::new("checkpoint key chunk too large"))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(|_| Error::new("checkpoint allocation failed"))?;
        bytes.resize(size, 0);
        key_read(&mut input, &mut remaining, &mut bytes)?;
        let sealed: Sealed = decode_piece(&bytes)?;
        if sealed.nonce != key_nonce(&public.mark, &random, index) {
            return Err(Error::new("invalid checkpoint key nonce"));
        }
        let plain = aegis::aegis256::Aegis256::<32>::new(key, &sealed.nonce)
            .decrypt(&sealed.ciphertext, &sealed.tag, &key_ad(&public.ad, count, group, index))
            .map_err(|_| Error::new("checkpoint key authentication failed"))?;
        let rows: KeyRows = decode_piece(&plain)?;
        if rows.rows.len() as u64 != expected {
            return Err(Error::new("invalid checkpoint key chunk count"));
        }
        for epoch in rows.rows {
            if history.epochs.last().is_some_and(|last| last.since.number() >= epoch.since) {
                return Err(Error::new("invalid checkpoint key order"));
            }
            history.epochs.push(MasterEpoch { since: TimeSec::new(epoch.since), key: epoch.key });
        }
    }
    if remaining != 0 {
        return Err(Error::new("trailing checkpoint key data"));
    }
    Ok(history)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::ServiceKind;
    use std::io::Cursor;

    fn sample() -> (Mark, KsState) {
        let ring = "01".repeat(64);
        let state = raft::State::initial(
            ring.clone(),
            raft::raft::tests::members(1),
            raft::raft::tests::names(),
        )
        .unwrap();
        let mark = Mark {
            raft: raft::checkpoint::RaftStatus {
                ring,
                index: 3,
                term: 2,
                scn: Scn::new(42),
                hard_term: 2,
                vote: Some(0),
                vote_scn: Some(Scn::new(43)),
                primary: Some(0),
                membership: state.snapshot.membership.clone(),
                founders: state.founders.clone(),
                contacts: state.contacts.clone(),
                retirements: state.retirements.clone(),
                checkpoint_start: None,
                max_scn: Some(Scn::new(43)),
            },
            sequence: 5,
            second: 100,
        };
        let mut state = KsState::default();
        state.keys.epochs.push(MasterEpoch { since: TimeSec::new(10), key: [11; 32] });
        state.keys.epochs.push(MasterEpoch { since: TimeSec::new(20), key: [22; 32] });
        for (name, port) in [("SN0", 9001), ("DN0", 9002)] {
            let peer = Peer {
                identity: name.into(),
                kind: if name == "SN0" { ServiceKind::Sn } else { ServiceKind::Dn },
                address: format!("127.0.0.1:{port}").parse().unwrap(),
                domain: if name == "SN0" { vec!["rack".into(), "machine".into()] } else { vec![] },
                media_class: if name == "SN0" { Some("ssd".into()) } else { None },
                ring_identity: Some(common::ring::RingIdentity::try_from("02".repeat(64)).unwrap()),
            };
            state.registrations.peers.insert(peer.identity.clone(), peer);
        }
        (mark, state)
    }

    #[test]
    fn nonce_depends_on_position_random_and_chunk() {
        let (mark, _) = sample();
        let random = [7; 32];
        let original = key_nonce(&mark, &random, 0);
        assert_eq!(original, key_nonce(&mark, &random, 0));
        let mut changed = mark.clone();
        changed.raft.term += 1;
        assert_ne!(original, key_nonce(&changed, &random, 0));
        changed = mark.clone();
        changed.raft.index += 1;
        assert_ne!(original, key_nonce(&changed, &random, 0));
        changed = mark.clone();
        changed.raft.scn = Scn::new(changed.raft.scn.number() + 1);
        assert_ne!(original, key_nonce(&changed, &random, 0));
        assert_ne!(original, key_nonce(&mark, &[8; 32], 0));
        assert_ne!(original, key_nonce(&mark, &random, 1));
    }

    #[test]
    fn checkpoint_round_trip_in_memory() {
        let (mark, state) = sample();
        let key = [37; 32];
        let verifier = Some(common::shamir::verifier(&key));
        let mut file = Cursor::new(Vec::new());
        write(&mut file, &mark, &state, verifier, &key, [7; 32]).unwrap();
        let public = read_public(&mut file).unwrap();
        assert_eq!(public.mark.raft.ring, mark.raft.ring);
        assert_eq!(
            (
                public.mark.sequence,
                public.mark.second,
                public.mark.raft.index,
                public.mark.raft.term,
                public.mark.raft.scn
            ),
            (5, 100, 3, 2, Scn::new(42))
        );
        assert_eq!(public.mark.raft.primary, Some(0));
        assert_eq!(public.mark.raft.membership, mark.raft.membership);
        assert_eq!(public.mark.raft.founders, mark.raft.founders);
        assert_eq!(public.mark.raft.contacts, mark.raft.contacts);
        assert_eq!(public.mark.raft.retirements, mark.raft.retirements);
        assert_eq!(public.mark.raft.max_scn, mark.raft.max_scn);
        assert_eq!(public.verifier, verifier);
        assert_eq!(public.peers, state.registrations.peers);
        let keys = read_keys(&mut file, &public, &key).unwrap();
        assert_eq!(keys.epochs.len(), 2);
        for (actual, expected) in keys.epochs.iter().zip(&state.keys.epochs) {
            assert_eq!(actual.since, expected.since);
            assert_eq!(actual.key, expected.key);
        }
        assert!(read_keys(&mut file, &public, &[38; 32]).is_err());
    }

    #[test]
    fn checkpoint_loads_into_raft_state() {
        let (mark, state) = sample();
        let path = tempfile::NamedTempFile::new().unwrap();
        let key = [37; 32];
        let mut file = File::create(path.path()).unwrap();
        write(&mut file, &mark, &state, None, &key, [7; 32]).unwrap();
        drop(file);

        let (restored, sequence, second, length) =
            read_raft(path.path(), mark.raft.index, &mark.raft.ring, false).unwrap();
        assert_eq!((sequence, second), (mark.sequence, mark.second));
        assert_eq!(length, std::fs::metadata(path.path()).unwrap().len());
        assert_eq!(restored.snapshot.index, mark.raft.index);
        assert_eq!(restored.snapshot.term, mark.raft.term);
        assert_eq!(restored.snapshot.scn, mark.raft.scn);
        assert_eq!(restored.snapshot.membership, mark.raft.membership);
        assert_eq!(restored.term, mark.raft.hard_term);
        assert_eq!(restored.vote, mark.raft.vote);
        assert_eq!(restored.vote_scn, mark.raft.vote_scn);
        assert_eq!(restored.commit, mark.raft.index);
        assert!(restored.snapshot.application.is_empty());
    }

    #[test]
    fn public_peer_tampering_fails_key_authentication() {
        let (mark, state) = sample();
        let key = [37; 32];
        let mut file = Cursor::new(Vec::new());
        write(&mut file, &mark, &state, None, &key, [7; 32]).unwrap();
        let mut bytes = file.into_inner();
        let addr = b"127.0.0.1:9001";
        let at = bytes.windows(addr.len()).position(|part| part == addr).unwrap();
        bytes[at + addr.len() - 1] = b'3';
        let mut file = Cursor::new(bytes);
        let public = read_public(&mut file).unwrap();
        assert!(read_keys(&mut file, &public, &key).is_err());
    }

    #[test]
    fn rejects_bad_ranges_and_truncation() {
        let (mark, state) = sample();
        let mut file = Cursor::new(Vec::new());
        write(&mut file, &mark, &state, None, &[37; 32], [7; 32]).unwrap();
        let bytes = file.into_inner();
        for n in 0..bytes.len() {
            assert!(read_public(&mut Cursor::new(&bytes[..n])).is_err(), "truncation {n}");
        }
        let mut source = Cursor::new(&bytes);
        let end = raft::checkpoint::read_footer(&mut source).unwrap();
        let mut footer: KsFooter = codec::decode(&end.bytes).unwrap();
        footer.keys.start += 1;
        let mut altered = Cursor::new(bytes[..end.start as usize].to_vec());
        altered.set_position(end.start);
        raft::checkpoint::write_footer(
            &mut altered,
            Op::KsCkFooter,
            &codec::encode(&footer).unwrap(),
        )
        .unwrap();
        assert!(read_public(&mut altered).is_err());
    }

    #[test]
    fn empty_state_and_invalid_inputs() {
        let (mark, mut state) = sample();
        let key = [37; 32];
        state.keys.epochs.clear();
        state.registrations.peers.clear();
        let mut file = Cursor::new(Vec::new());
        write(&mut file, &mark, &state, None, &key, [7; 32]).unwrap();
        let public = read_public(&mut file).unwrap();
        assert!(public.peers.is_empty());
        assert!(read_keys(&mut file, &public, &key).unwrap().epochs.is_empty());
        assert!(write(&mut file, &mark, &state, None, &key, [7; 32]).is_err());
        let mut empty = Cursor::new(Vec::new());
        assert!(write(&mut empty, &mark, &state, Some([0; 32]), &key, [7; 32]).is_err());
        assert!(empty.into_inner().is_empty());
        let mut empty = Cursor::new(Vec::new());
        assert!(write_with(&mut empty, &mark, &state, None, &key, [7; 32], 0).is_err());
        assert!(empty.into_inner().is_empty());

        let (mark, mut state) = sample();
        state.keys.epochs.swap(0, 1);
        let mut empty = Cursor::new(Vec::new());
        assert!(write(&mut empty, &mark, &state, None, &key, [7; 32]).is_err());
        assert!(empty.into_inner().is_empty());
    }

    #[test]
    fn rejects_corrupt_sealed_keys() {
        let (mark, state) = sample();
        let key = [37; 32];
        let mut file = Cursor::new(Vec::new());
        write(&mut file, &mark, &state, None, &key, [7; 32]).unwrap();
        let public = read_public(&mut file).unwrap();
        let at = (end(public.keys).unwrap() - 1) as usize;
        let mut bytes = file.into_inner();
        bytes[at] ^= 1;
        let mut file = Cursor::new(bytes);
        let public = read_public(&mut file).unwrap();
        assert!(read_keys(&mut file, &public, &key).is_err());
    }

    #[test]
    fn multiple_key_chunks_round_trip_and_cannot_be_reordered() {
        let (mark, mut state) = sample();
        const TEST_GROUP: usize = 4;
        state.keys.epochs = (0..TEST_GROUP * 2 + 1)
            .map(|index| MasterEpoch {
                since: TimeSec::new(index as u64 + 1),
                key: [index as u8; 32],
            })
            .collect();
        let key = [37; 32];
        let mut file = Cursor::new(Vec::new());
        write_with(&mut file, &mark, &state, None, &key, [7; 32], TEST_GROUP).unwrap();
        let public = read_public(&mut file).unwrap();
        let keys = read_keys(&mut file, &public, &key).unwrap();
        assert_eq!(keys.epochs.len(), state.keys.epochs.len());
        for (actual, expected) in keys.epochs.iter().zip(&state.keys.epochs) {
            assert_eq!(actual.since, expected.since);
            assert_eq!(actual.key, expected.key);
        }
        let mut bytes = file.into_inner();
        let group_at = public.keys.start as usize + 10;
        assert_eq!(u64::from_le_bytes(bytes[group_at..group_at + 8].try_into().unwrap()), 4);
        let mut changed = bytes.clone();
        changed[group_at..group_at + 8].copy_from_slice(&5u64.to_le_bytes());
        let mut changed = Cursor::new(changed);
        let changed_public = read_public(&mut changed).unwrap();
        assert!(read_keys(&mut changed, &changed_public, &key).is_err());
        let first = public.keys.start as usize + 50;
        let first_len = u64::from_le_bytes(bytes[first..first + 8].try_into().unwrap()) as usize;
        let second = first + 8 + first_len;
        let second_len = u64::from_le_bytes(bytes[second..second + 8].try_into().unwrap()) as usize;
        assert_eq!(first_len, second_len);
        let first_chunk = bytes[first + 8..second].to_vec();
        let second_chunk = bytes[second + 8..second + 8 + second_len].to_vec();
        bytes[first + 8..second].copy_from_slice(&second_chunk);
        bytes[second + 8..second + 8 + second_len].copy_from_slice(&first_chunk);
        let mut file = Cursor::new(bytes);
        let public = read_public(&mut file).unwrap();
        assert!(read_keys(&mut file, &public, &key).is_err());
    }

    #[test]
    fn default_key_group_exceeds_one_mib() {
        // Each tagged epoch contains an eight-byte start and a 32-byte key.
        assert!(KEY_GROUP * (2 + 8 + 4 + 32) >= 1024 * 1024);
    }

    #[test]
    fn process_records_are_streamed() {
        struct Counting {
            data: Cursor<Vec<u8>>,
            max_read: usize,
            reads: usize,
        }
        impl Read for Counting {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                self.max_read = self.max_read.max(bytes.len());
                self.reads += 1;
                self.data.read(bytes)
            }
        }
        impl Seek for Counting {
            fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
                self.data.seek(pos)
            }
        }
        let (_, mut state) = sample();
        let peer = state.registrations.peers.values().next().unwrap().clone();
        state.registrations.peers.clear();
        for index in 0..1024 {
            let mut item = peer.clone();
            item.identity = format!("SN{index}");
            item.domain = vec!["x".repeat(1024)];
            state.registrations.peers.insert(item.identity.clone(), item);
        }
        let mut output = Cursor::new(Vec::new());
        let (len, hash) = write_peers(&mut output, &state).unwrap();
        assert!(len > 1024 * 1024);
        let mut input = Counting { data: output, max_read: 0, reads: 0 };
        let (peers, read_hash) = read_peers(&mut input, Range { start: 0, len }).unwrap();
        assert_eq!(peers, state.registrations.peers);
        assert_eq!(read_hash, hash);
        assert!(input.max_read <= 8192);
        assert!(input.reads < 1024);
    }
}
