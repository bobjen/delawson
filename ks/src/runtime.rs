//! KS action encryption and application; consensus and transport live in raft.
use super::*;
use common::{Error, Result};
use raft::runtime::Host;
pub use raft::runtime::Settings;
use raft::{Command, Entry, Node, Role, State};
use std::time::Duration;
pub fn needs_request_worker(request: &Req) -> bool {
    raft::runtime::needs_request_worker(request)
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
    // The only client action dispatch: Raft never interprets KS action contents.
    match action.op {
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
    pub fn initial_key(&self, node: &mut Node) -> Result<()> {
        if node.role != Role::Leader
            || node.state().last() != node.state().commit
            || node.state().has_actions()
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
        self.client_request(process, request, false)
    }
    fn forwardable(&self, op: Op) -> bool {
        matches!(op, Op::SnMasterKey | Op::RotateMasterKey | Op::RaftMembers)
    }
    fn registered_endpoint(&self, name: &str) -> Option<std::net::SocketAddr> {
        self.peers.lock().ok()?.get(name).map(|peer| peer.address)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

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
        state.log.push(Entry { index: 1, term: 1, command: Command::Client(actions) });
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
