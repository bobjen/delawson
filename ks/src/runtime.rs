//! KS action encryption and application; consensus and transport live in raft.
use super::*;
use common::{Error, Result};
use raft::runtime::Host;
pub use raft::runtime::Settings;
use raft::{Command, Entry, Node, Role, State};
use std::time::Duration;
pub fn needs_request_worker(request: &Req) -> bool {
    request.op() == Op::Register || raft::runtime::needs_request_worker(request)
}
/// Public routing projection, retained even when this member is locked/cold.
#[derive(Default)]
pub struct Registrations {
    pub peers: BTreeMap<String, Peer>,
    pub(super) applied: u64,
}
impl raft::execution::Application for Registrations {
    fn applied(&self) -> u64 {
        self.applied
    }
    fn set_applied(&mut self, index: u64) {
        self.applied = index;
    }
}
fn registration_action(peer: &Peer) -> raft::Action {
    let common::wire::Arg::Struct(_, bytes, depth) = peer.request().payload else { unreachable!() };
    let mut fields = vec![depth];
    bytes.write_to(&mut fields).expect("registration serialization");
    raft::Action::new(Op::Register, fields)
}
fn registration_peer(action: &raft::Action) -> Result<Peer> {
    let (&depth, bytes) =
        action.fields.split_first().ok_or_else(|| Error::new("empty registration action"))?;
    if depth == 0
        || depth > common::wire::MAX_RECURSION_DEPTH
        || bytes.len() >= common::wire::MAX_FRAME_BYTES
    {
        return Err(Error::new("invalid registration action limits"));
    }
    Peer::from_args(&[common::wire::Arg::Struct(Op::Register, bytes.to_vec().into(), depth)])
}
fn apply_registration(
    state: &mut Registrations,
    _context: raft::execution::Context<'_>,
    action: &raft::Action,
) -> Result<()> {
    match action.op {
        Op::Register => {
            let peer = registration_peer(action)?;
            state.peers.insert(peer.identity.clone(), peer);
            Ok(())
        }
        Op::KsKey => Ok(()), // Opaque ciphertext is not needed for routing.
        _ => Err(Error::new("unknown KS action tag")),
    }
}
fn has_keys(state: &State) -> bool {
    state.snapshot.application.iter().chain(state.log.iter().filter(|e| e.index <= state.commit))
        .any(|e| matches!(&e.command, Command::Client(actions) if actions.iter().any(|a| a.op == Op::KsKey)))
}
struct KeyFields {
    key: [u8; 32],
    since: u64,
}
struct KeyEnvelope {
    nonce: [u8; 32],
    ciphertext: Vec<u8>,
    tag: [u8; 32],
}
raft::wire_struct!(KeyFields, KsKey, {key,since});
raft::wire_struct!(KeyEnvelope, KsEnvelope, {nonce,ciphertext,tag});
fn ad(ring: &str, index: u64, term: u64) -> Vec<u8> {
    [b"DLAW-RAFT-KEY-2".as_slice(), ring.as_bytes(), &index.to_le_bytes(), &term.to_le_bytes()]
        .concat()
}
pub fn seal_key(
    key: &[u8; 32],
    ring: &str,
    index: u64,
    term: u64,
    epoch: &MasterEpoch,
) -> Result<Vec<u8>> {
    let nonce = random_master_key()?;
    let plain = raft::codec::encode(&KeyFields { key: epoch.key, since: epoch.since.number() })?;
    let (ciphertext, tag) =
        aegis::aegis256::Aegis256::<32>::new(key, &nonce).encrypt(&plain, &ad(ring, index, term));
    raft::codec::encode(&KeyEnvelope { nonce, ciphertext, tag })
}
fn apply_action(
    history: &mut KeyHistory,
    key: &[u8; 32],
    context: raft::execution::Context<'_>,
    action: &raft::Action,
) -> Result<()> {
    // KS dispatches encrypted key actions; routing has its own always-loaded projection.
    match action.op {
        Op::Register => registration_peer(action).map(|_| ()),
        Op::KsKey => {
            let envelope: KeyEnvelope = raft::codec::decode(&action.fields)?;
            let plain = aegis::aegis256::Aegis256::<32>::new(key, &envelope.nonce)
                .decrypt(
                    &envelope.ciphertext,
                    &envelope.tag,
                    &ad(context.ring, context.index, context.term),
                )
                .map_err(|_| Error::new("KS key authentication failed"))?;
            let fields: KeyFields = raft::codec::decode(&plain)?;
            let epoch = MasterEpoch { key: fields.key, since: TimeSec::new(fields.since) };
            if history.epochs.last().is_some_and(|last| last.since >= epoch.since) {
                return Err(Error::new("KS key activation order is invalid"));
            }
            history.epochs.push(epoch);
            Ok(())
        }
        _ => Err(Error::new("unknown KS action tag")),
    }
}
pub fn decode_history(state: &State, key: &[u8; 32]) -> Result<KeyHistory> {
    let mut history = KeyHistory::default();
    apply_history(state, key, &mut history)?;
    Ok(history)
}
fn apply_history(state: &State, key: &[u8; 32], history: &mut KeyHistory) -> Result<()> {
    let global = RwLock::new(std::mem::take(history));
    let progress = Mutex::new(raft::execution::Cursor::default());
    let result = raft::execution::execute(
        &global,
        &progress,
        state,
        Duration::MAX,
        |history, ctx, action| apply_action(history, key, ctx, action),
    );
    *history = global.into_inner().map_err(|_| Error::new("global lock poisoned"))?;
    result.map(|_| ())
}

impl Ks {
    pub fn propose_registration(&self, peer: Peer) -> Result<()> {
        let _proposal = self.proposing.lock().map_err(|_| Error::new("proposal lock poisoned"))?;
        let deadline = std::time::Instant::now()
            + Duration::from_millis(self.config.raft.election_max_ms.saturating_mul(2));
        // Routing is available while locked, so admission uses metadata progress,
        // not decrypted master-key application progress.
        loop {
            let commit = self.on_raft(|host, node| {
                if node.role != Role::Leader || node.journal.failed {
                    return Err(Error::new("Raft is not a healthy leader"));
                }
                host.refresh_application(node)?;
                Ok((node.can_admit_with_policy(lease::now()?, false, false)
                    && host.metadata_generation.load(Ordering::SeqCst) == node.state().generation)
                    .then_some(node.state().commit))
            })?;
            if commit.is_some_and(|index| self.peers.read().is_ok_and(|p| p.applied >= index)) {
                break;
            }
            if !self.execution_started.load(Ordering::SeqCst) {
                self.execute_pending()?;
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::new("registration quorum timeout"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let (identical, applied) = {
            let routes = self.peers.read().map_err(|_| Error::new("registrations poisoned"))?;
            (routes.peers.get(&peer.identity) == Some(&peer), routes.applied)
        };
        if identical
            && self.on_raft(move |_, node| {
                Ok(node.state().last() == applied
                    && node.state().commit == applied
                    && !node.journal.failed
                    && node.can_admit_with_policy(lease::now()?, false, false))
            })?
        {
            return Ok(());
        }
        // Do not deduplicate against old applied state while a prior timed-out
        // registration could still commit a different endpoint.
        let action = registration_action(&peer);
        let index = self.on_raft(move |host, node| {
            let index = node.propose(Command::Client(vec![action]))?;
            host.refresh_application(node)?;
            Ok(index)
        })?;
        loop {
            self.on_raft(|host, node| {
                if node.role != Role::Leader || node.journal.failed {
                    return Err(Error::new(
                        "registration leadership changed; outcome may be unknown",
                    ));
                }
                host.refresh_application(node)
            })?;
            if self.peers.read().map_err(|_| Error::new("registrations poisoned"))?.applied >= index
            {
                break;
            }
            if !self.execution_started.load(Ordering::SeqCst) {
                self.execute_pending()?;
            }
            if let Some(error) = self
                .execution_error
                .lock()
                .map_err(|_| Error::new("execution error lock poisoned"))?
                .as_ref()
            {
                return Err(Error::new(error.clone()));
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::new("registration quorum timeout; outcome may be unknown"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        logn!("registered a peer", "identity {},at {}", peer.identity, peer.address);
        Ok(())
    }
    pub fn initial_key(&self, node: &mut Node) -> Result<()> {
        if node.role != Role::Leader
            || node.state().last() != node.state().commit
            || has_keys(node.state())
        {
            return Ok(());
        }
        let Some(key) = *self.unlock_key.lock().map_err(|_| Error::new("unlock lock poisoned"))?
        else {
            return Ok(());
        };
        let epoch = MasterEpoch { key: random_master_key()?, since: TimeSec::now() };
        let index =
            node.state().last().checked_add(1).ok_or_else(|| Error::new("Raft index overflow"))?;
        let sealed = seal_key(&key, &node.state().ring, index, node.state().term, &epoch)?;
        node.propose(Command::Client(vec![raft::Action::new(Op::KsKey, sealed)]))?;
        Ok(())
    }
    pub fn propose_key(&self, epoch: MasterEpoch) -> Result<u64> {
        let index = self.on_raft(move |host, node| {
            if node.state().last() != node.state().commit {
                return Err(Error::new(
                    "KS has an outstanding Raft proposal; retry after it commits",
                ));
            }
            let key = host
                .unlock_key
                .lock()
                .map_err(|_| Error::new("unlock lock poisoned"))?
                .ok_or_else(|| Error::new("KS is locked"))?;
            let history = decode_history(node.state(), &key)?;
            if history.epochs.last().is_some_and(|last| last.since >= epoch.since) {
                return Err(Error::new(
                    "new key activation must be later than the last recorded activation",
                ));
            }
            let index = node
                .state()
                .last()
                .checked_add(1)
                .ok_or_else(|| Error::new("Raft index overflow"))?;
            let sealed = seal_key(&key, &node.state().ring, index, node.state().term, &epoch)?;
            let index =
                node.propose(Command::Client(vec![raft::Action::new(Op::KsKey, sealed)]))?;
            host.refresh_application(&node)?;
            Ok(index)
        })?;
        self.wait_committed(index)?;
        Ok(index)
    }
}
impl Host for Ks {
    type Application = KeyHistory;
    fn has_metadata(&self) -> bool {
        true
    }
    fn apply_metadata(&self, state: &raft::execution::Work) -> Result<bool> {
        raft::execution::execute_blocking(
            &self.peers,
            &self.peer_cursor,
            state,
            Duration::from_millis(self.config.raft.execution_lock_ms),
            apply_registration,
        )
    }
    fn apply_client(&self, state: &raft::execution::Work) -> Result<()> {
        let key = self
            .unlock_key
            .lock()
            .map_err(|_| Error::new("unlock lock poisoned"))?
            .ok_or_else(|| Error::new("KS locked"))?;
        raft::execution::execute_blocking(
            &self.global_lock,
            &self.execution,
            state,
            Duration::from_millis(self.config.raft.execution_lock_ms),
            |history, ctx, action| apply_action(history, &key, ctx, action),
        )
        .map(|_| ())
    }
    fn checkpoint_actions(&self, state: &State, _history: &KeyHistory) -> Result<Vec<Entry>> {
        Ok(state.capture().application)
    }
    fn prepare_primary(&self, node: &mut Node) -> Result<()> {
        self.initial_key(node)
    }
    fn peer_message(&self) -> Option<raft::Action> {
        let key = (*self.unlock_key.lock().ok()?)?;
        Some(raft::Action::new(Op::Unlock, key.to_vec()))
    }
    fn receive_client(&self, _process: &Process, action: raft::Action) -> Result<Resp> {
        match action.op {
            Op::Unlock => {
                let key: [u8; 32] =
                    action.fields.try_into().map_err(|_| Error::new("invalid unlock key"))?;
                if self.unlock_verifier.as_ref().is_some_and(|v| !common::shamir::matches(&key, v))
                {
                    return Err(Error::new("wrong ring unlock key"));
                }
                self.open_keys(key)?;
                Ok(Resp::ok())
            }
            _ => Err(Error::new("unknown KS peer action")),
        }
    }
    fn dispatch_forwarded(&self, process: &Process, request: &Req) -> Resp {
        if request.op() == Op::Register {
            self.register_request(process, request, false)
        } else {
            self.client_request(process, request, false)
        }
    }
    fn forwardable(&self, op: Op) -> bool {
        matches!(op, Op::Register | Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers)
    }
    fn registered_endpoint(&self, name: &str) -> Option<std::net::SocketAddr> {
        self.peers.read().ok()?.peers.get(name).map(|peer| peer.address)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn peer(port: u16) -> Peer {
        Peer {
            identity: "KS0-member1".into(),
            kind: common::config::ServiceKind::Ks,
            address: ([127, 0, 0, 1], port).into(),
            domain: vec!["dc0".into(), "box0".into()],
            media_class: Some("ssd".into()),
            ring_identity: Some(common::ring::RingIdentity::try_from("01".repeat(64)).unwrap()),
        }
    }
    fn routes(state: &State) -> Registrations {
        let routes = RwLock::new(Registrations::default());
        let cursor = Mutex::new(raft::execution::Cursor::default());
        raft::execution::execute(&routes, &cursor, state, Duration::MAX, apply_registration)
            .unwrap();
        routes.into_inner().unwrap()
    }
    #[test]
    fn registrations_replay_only_committed_changes_without_unlock_material() {
        let mut state =
            State::initial("01".repeat(64), raft::tests::members(3), raft::tests::names()).unwrap();
        state.term = 1;
        state.log.push(Entry {
            scn: common::scn::Scn::new(1),
            index: 1,
            term: 1,
            command: Command::Client(vec![registration_action(&peer(9001))]),
        });
        state.log.push(Entry {
            scn: common::scn::Scn::new(2),
            index: 2,
            term: 1,
            command: Command::Client(vec![registration_action(&peer(9002))]),
        });
        state.commit = 1;
        let before_commit = routes(&state);
        let key_present = has_keys(&state);
        assert_eq!(before_commit.peers["KS0-member1"], peer(9001));
        assert!(!key_present); // A registration must not suppress initial-key creation.
        let snapshot = state.capture();
        state.apply(&raft::Record::Install(snapshot)).unwrap();
        // Snapshot plus a later journal registration survives a binary round trip.
        state.commit = 2;
        let saved = raft::codec::encode(&state).unwrap();
        let recovered: State = raft::codec::decode(&saved).unwrap();
        let after_commit = routes(&recovered);
        assert_eq!(after_commit.peers["KS0-member1"], peer(9002));
        assert_eq!(after_commit.applied, 2);
        // Routing and key application can replay the same mixed checkpoint independently.
        let key = [55; 32];
        let epoch = MasterEpoch { key: [89; 32], since: TimeSec::new(100) };
        let sealed = seal_key(&key, &state.ring, 3, 1, &epoch).unwrap();
        state.log.push(Entry {
            scn: common::scn::Scn::new(3),
            index: 3,
            term: 1,
            command: Command::Client(vec![raft::Action::new(Op::KsKey, sealed)]),
        });
        state.commit = 3;
        let snapshot = state.capture();
        state.apply(&raft::Record::Install(snapshot)).unwrap();
        let locked = routes(&state);
        let unlocked = decode_history(&state, &key).unwrap();
        assert_eq!(locked.peers["KS0-member1"], peer(9002));
        assert_eq!(unlocked.epochs[0].key, epoch.key);
        assert!(has_keys(&state));
    }
    #[test]
    fn registration_decoder_rejects_malformed_actions() {
        let mut action = registration_action(&peer(9001));
        action.fields.pop();
        let malformed = registration_peer(&action).is_err();
        let worker = needs_request_worker(&peer(9001).request());
        assert!(malformed);
        assert!(worker);
    }
    #[test]
    fn one_entry_dispatches_multiple_encrypted_key_actions() {
        let key = [19; 32];
        let mut state =
            State::initial("01".repeat(64), raft::tests::members(1), raft::tests::names()).unwrap();
        state.term = 1;
        let actions = (1..=2)
            .map(|n| {
                let epoch = MasterEpoch { key: [200 + n as u8; 32], since: TimeSec::new(n) };
                raft::Action::new(Op::KsKey, seal_key(&key, &state.ring, 1, 1, &epoch).unwrap())
            })
            .collect();
        state.log.push(Entry {
            scn: common::scn::Scn::new(1),
            index: 1,
            term: 1,
            command: Command::Client(actions),
        });
        state.commit = 1;
        let history = decode_history(&state, &key).unwrap();
        assert_eq!(history.epochs.len(), 2);
        assert_eq!(history.applied, 1);
        let persisted = raft::codec::encode(&state).unwrap();
        assert!(!persisted.windows(32).any(|bytes| bytes == [201; 32] || bytes == [202; 32]));
    }
    #[test]
    fn debugging_foreign_checkpoint_preserves_key_binding_but_not_override() {
        let key = [42; 32];
        let mut source =
            State::initial("02".repeat(64), raft::tests::members(1), raft::tests::names()).unwrap();
        source.apply(&raft::Record::Hard { term: 1, vote: None }).unwrap();
        let epoch = MasterEpoch { key: [7; 32], since: TimeSec::new(1) };
        let sealed = seal_key(&key, &source.ring, 1, 1, &epoch).unwrap();
        source
            .apply(&raft::Record::Entries {
                from: 1,
                entries: vec![Entry {
                    scn: common::scn::Scn::new(1),
                    index: 1,
                    term: 1,
                    command: Command::Client(vec![raft::Action::new(Op::KsKey, sealed)]),
                }],
            })
            .unwrap();
        source.apply(&raft::Record::Commit(1)).unwrap();
        let mut target =
            State::initial("01".repeat(64), raft::tests::members(1), raft::tests::names()).unwrap();
        target.ignore_ring_identity = true;
        target.apply(&raft::Record::Hard { term: 1, vote: None }).unwrap();
        target.apply(&raft::Record::Install(source.capture())).unwrap();
        let history = decode_history(&target, &key).unwrap();
        let wrong_key = decode_history(&target, &[43; 32]).is_err();
        let decoded: State = raft::codec::decode(&raft::codec::encode(&target).unwrap()).unwrap();
        assert_eq!(history.epochs[0].key, epoch.key);
        assert!(wrong_key);
        assert!(!decoded.ignore_ring_identity);
    }

    #[test]
    fn application_catches_up_from_snapshot_then_only_decodes_new_commits() {
        let key = [7; 32];
        let mut state =
            State::initial("01".repeat(64), raft::tests::members(3), raft::tests::names()).unwrap();
        state.term = 1;
        for index in 1..=3 {
            let epoch = MasterEpoch { since: TimeSec::new(index), key: [index as u8; 32] };
            let encrypted = seal_key(&key, &state.ring, index, 1, &epoch).unwrap();
            state.log.push(Entry {
                scn: common::scn::Scn::new(index),
                index,
                term: 1,
                command: Command::Client(vec![raft::Action::new(Op::KsKey, encrypted)]),
            });
        }
        state.commit = 2;
        let mut history = KeyHistory::default();
        apply_history(&state, &key, &mut history).unwrap();
        let committed_count = history.epochs.len();
        let checkpoint = state.capture();
        state.apply(&raft::Record::Install(checkpoint)).unwrap();
        let mut promoted = KeyHistory::default();
        apply_history(&state, &key, &mut promoted).unwrap();
        let promoted_count = promoted.epochs.len();
        // Already applied ciphertext need not be decrypted again.
        state.snapshot.application[0].command =
            Command::Client(vec![raft::Action::new(Op::KsKey, vec![])]);
        state.commit = 3;
        apply_history(&state, &key, &mut history).unwrap();
        let final_count = history.epochs.len();
        let final_index = history.applied;
        assert_eq!(committed_count, 2);
        assert_eq!(promoted_count, 2);
        assert_eq!(final_count, 3);
        assert_eq!(final_index, 3);
    }
}
