//! Shared action iteration and global-lock budgeting. Clients dispatch action tags.
use crate::{Action, Command, Entry, EntryLog, Snapshot, State};
use common::{Error, Result};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A detached, immutable view of committed work. No consensus lock is retained.
pub struct Work {
    pub generation: u64,
    pub ring: String,
    pub snapshot: Arc<Snapshot>,
    pub entries: EntryLog,
    pub commit: u64,
}
pub trait Committed {
    fn generation(&self) -> u64;
    fn ring(&self) -> &str;
    fn snapshot(&self) -> &Snapshot;
    fn entries(&self) -> &EntryLog;
    fn commit(&self) -> u64;
}
impl Committed for State {
    fn generation(&self) -> u64 {
        self.generation
    }
    fn ring(&self) -> &str {
        &self.ring
    }
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    fn entries(&self) -> &EntryLog {
        &self.log
    }
    fn commit(&self) -> u64 {
        self.commit
    }
}
impl Committed for Work {
    fn generation(&self) -> u64 {
        self.generation
    }
    fn ring(&self) -> &str {
        &self.ring
    }
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    fn entries(&self) -> &EntryLog {
        &self.entries
    }
    fn commit(&self) -> u64 {
        self.commit
    }
}
pub trait Application: Send + Sync {
    /// Reset application state before rebuilding it from the committed history.
    fn reset(&mut self);
    /// Reclaim application-owned state while the global lock is still exclusive.
    fn reclaim(&mut self) {}
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub applied: u64,
    pub complete: bool,
}
#[derive(Default)]
pub struct Cursor {
    generation: u64,
    restoring: Option<u64>,
    entry: u64,
    action: usize,
    applied: u64,
    decoded: Option<(u64, Vec<Action>)>,
}
impl Cursor {
    pub fn new(generation: u64, applied: u64) -> Self {
        Self { generation, applied, ..Self::default() }
    }
}
#[derive(Clone, Copy)]
pub struct Context<'a> {
    pub ring: &'a str,
    pub index: u64,
    pub term: u64,
}
/// Execute only committed work. A budget expiry yields the global write lock and
/// returns to the runtime, allowing journaling and checkpoint readers to proceed.
/// The next call reacquires the lock and resumes between actions, never repeating
/// a successful action in a partly executed entry.
pub fn execute<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    applied: &AtomicU64,
    state: &impl Committed,
    budget: Duration,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    execute_with_lock(
        global,
        progress,
        applied,
        state,
        budget,
        false,
        false,
        |data| data.reset(),
        |_| true,
        apply,
        |_| (),
    )
    .map(|(progress, _)| progress.complete)
}
/// The application worker can sleep on the global lock without delaying consensus.
pub fn execute_blocking<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    applied: &AtomicU64,
    state: &impl Committed,
    budget: Duration,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    exec_progress(global, progress, applied, state, budget, apply).map(|progress| progress.complete)
}
pub fn exec_progress<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    applied: &AtomicU64,
    state: &impl Committed,
    budget: Duration,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<Progress> {
    exec_report(global, progress, applied, state, budget, apply, |_| ())
        .map(|(progress, ())| progress)
}
pub fn exec_report<S: Application, T>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    applied: &AtomicU64,
    state: &impl Committed,
    budget: Duration,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
    finish: impl FnOnce(&S) -> T,
) -> Result<(Progress, T)>
where
    T: Default,
{
    execute_with_lock(
        global,
        progress,
        applied,
        state,
        budget,
        true,
        false,
        |data| data.reset(),
        |_| true,
        apply,
        finish,
    )
    .map(|(progress, result)| (progress, result.unwrap_or_default()))
}
pub fn execute_state<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    applied: &AtomicU64,
    state: &impl Committed,
    budget: Duration,
    blocking: bool,
    reset: impl Fn(&mut S),
    has_action: impl Fn(&Action) -> bool,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    execute_with_lock(
        global,
        progress,
        applied,
        state,
        budget,
        blocking,
        true,
        reset,
        has_action,
        apply,
        |_| (),
    )
    .map(|(progress, _)| progress.complete)
}
fn execute_with_lock<S: Application, T>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    applied_index: &AtomicU64,
    state: &impl Committed,
    budget: Duration,
    blocking: bool,
    filter_pending: bool,
    reset: impl Fn(&mut S),
    has_action: impl Fn(&Action) -> bool,
    mut apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
    finish: impl FnOnce(&S) -> T,
) -> Result<(Progress, Option<T>)> {
    let mut cursor = progress.lock().map_err(|_| Error::new("execution cursor poisoned"))?;
    let published = applied_index.load(Ordering::Acquire);
    let mut ready_actions = VecDeque::new();
    if cursor.generation == state.generation() {
        cursor.applied = cursor.applied.max(published);
    }
    if cursor.generation == state.generation()
        && cursor.restoring.is_none()
        && cursor.applied >= state.snapshot().index
    {
        let needs_apply = state
            .entries()
            .iter()
            .filter(|entry| entry.index > cursor.applied && entry.index <= state.commit())
            .try_fold(false, |found, entry| -> Result<bool> {
                if !matches!(entry.command, Command::Client(_)) {
                    return Ok(found);
                }
                let needs_apply = if !filter_pending {
                    true
                } else if let Some((_, actions)) =
                    cursor.decoded.as_ref().filter(|(index, _)| *index == entry.index)
                {
                    !found && actions.iter().any(&has_action)
                } else {
                    let actions = entry.client_actions()?;
                    let needs_apply = !found && actions.iter().any(&has_action);
                    ready_actions.push_back((entry.index, actions));
                    needs_apply
                };
                Ok(found || needs_apply)
            })?;
        if !needs_apply {
            cursor.applied = state.commit();
            applied_index.store(cursor.applied, Ordering::Release);
            return Ok((Progress { applied: cursor.applied, complete: true }, None));
        }
    }
    let mut data = if blocking {
        global.write()
    } else if let Some(data) = global.try_write() {
        data
    } else {
        return Ok((Progress { applied: cursor.applied, complete: false }, None));
    };
    let result = (|| -> Result<bool> {
        if cursor.generation != state.generation() {
            reset(&mut *data);
            *cursor = Cursor { generation: state.generation(), ..Cursor::default() };
        }
        if cursor.applied == state.commit() && cursor.restoring.is_none() {
            return Ok(true);
        }
        if cursor.applied < state.snapshot().index
            && cursor.restoring != Some(state.snapshot().index)
        {
            reset(&mut *data);
            cursor.restoring = Some(state.snapshot().index);
            cursor.entry = 0;
            cursor.action = 0;
            cursor.applied = 0;
        }
        let began = Instant::now();
        let mut run = |entry: &Entry,
                       restoring: bool,
                       data: &mut S,
                       cursor: &mut Cursor|
         -> Result<bool> {
            if restoring && entry.index < cursor.entry {
                return Ok(true);
            }
            if cursor.entry != entry.index {
                cursor.entry = entry.index;
                cursor.action = 0;
                cursor.decoded = None;
            }
            match &entry.command {
                Command::Client(_) => {
                    if cursor.decoded.as_ref().map(|(index, _)| *index) != Some(entry.index) {
                        let actions = match ready_actions.front() {
                            Some((index, _)) if *index == entry.index => {
                                ready_actions.pop_front().unwrap().1
                            }
                            _ => entry.client_actions()?,
                        };
                        cursor.decoded = Some((entry.index, actions.into_owned()));
                    }
                    let length = cursor.decoded.as_ref().unwrap().1.len();
                    while cursor.action < length {
                        let action = &cursor.decoded.as_ref().unwrap().1[cursor.action];
                        apply(
                            data,
                            Context { ring: state.ring(), index: entry.index, term: entry.term },
                            action,
                        )?;
                        cursor.action += 1;
                        if began.elapsed() >= budget {
                            return Ok(false);
                        }
                    }
                    cursor.decoded = None;
                }
                Command::Noop
                | Command::Primary(_)
                | Command::Members(_)
                | Command::CheckpointStart(_) => {}
            }
            if !restoring {
                cursor.applied = entry.index;
                common::logn!("Raft entry applied", "index {},term {}", entry.index, entry.term);
            }
            Ok(true)
        };
        if cursor.restoring.is_some() {
            for entry in &state.snapshot().application {
                if !run(entry, true, &mut *data, &mut cursor)? {
                    return Ok(false);
                }
            }
            cursor.applied = state.snapshot().index;
            cursor.restoring = None;
            cursor.entry = 0;
            cursor.action = 0;
        }
        for entry in state.entries().iter().filter(|entry| entry.index <= state.commit()) {
            if entry.index > cursor.applied && !run(entry, false, &mut *data, &mut cursor)? {
                return Ok(false);
            }
        }
        cursor.applied = state.commit();
        Ok(true)
    })();
    let applied_now = cursor.applied;
    (&mut *data).reclaim();
    let report = if result.is_ok() { Some(finish(&data)) } else { None };
    applied_index.store(applied_now, Ordering::Release);
    drop(data);
    result.map(|complete| (Progress { applied: applied_now, complete }, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Op;
    #[derive(Default)]
    struct Values {
        values: Vec<u8>,
    }
    impl Application for Values {
        fn reset(&mut self) {
            *self = Self::default();
        }
    }
    fn state() -> State {
        let mut state = State::initial(
            "01".repeat(64),
            crate::raft::tests::members(1),
            crate::raft::tests::names(),
        )
        .unwrap();
        state.term = 1;
        state.log = (1..=2)
            .map(|index| Entry {
                scn: common::scn::Scn::new(index),
                index,
                term: 1,
                command: Command::Client(
                    (1..=3).map(|n| Action::new(Op::BytesResp, vec![n])).collect(),
                ),
                entry_buf: None,
            })
            .collect();
        state.commit = 1;
        state
    }
    fn apply(data: &mut Values, _: Context<'_>, action: &Action) -> Result<()> {
        data.values.push(action.fields[0]);
        Ok(())
    }
    #[test]
    fn new_history_generation_resets_applied_state_even_when_commit_regresses() {
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        let applied = AtomicU64::new(0);
        let mut old = state();
        old.commit = 2;
        execute_blocking(&global, &cursor, &applied, &old, Duration::MAX, apply).unwrap();
        let mut replacement = state();
        replacement.generation = 1;
        let mut first = replacement.log[0].clone();
        first.command = Command::Client(vec![Action::new(Op::BytesResp, vec![9])]);
        replacement.log.set(0, first);
        execute_blocking(&global, &cursor, &applied, &replacement, Duration::MAX, apply).unwrap();
        let data = global.read();
        assert_eq!(applied.load(Ordering::Acquire), 1);
        assert_eq!(data.values, vec![9]);
    }
    #[test]
    fn no_action_commit_advances_without_the_global_lock() {
        let mut committed = state();
        let mut first = committed.log[0].clone();
        first.command = Command::Noop;
        committed.log.set(0, first);
        committed.log.truncate(1);
        committed.commit = 1;

        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        let applied = AtomicU64::new(0);
        let _guard = global.write();

        assert!(execute(&global, &cursor, &applied, &committed, Duration::ZERO, apply).unwrap());
        assert_eq!(applied.load(Ordering::Acquire), 1);
        assert_eq!(cursor.lock().unwrap().applied, 1);
    }
    #[test]
    fn blocked_client_action_does_not_block_heartbeat_or_commit() {
        use std::sync::mpsc;
        let node = crate::raft::tests::node(0, crate::raft::tests::members(2));
        let owner = std::sync::Arc::new(crate::owner::Owner::new(node));
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let consensus =
            owner.start(move |_| !stopping.load(std::sync::atomic::Ordering::SeqCst)).unwrap();
        let (entered, entering) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let application = std::thread::spawn(move || {
            let global = RwLock::new(Values::default());
            let progress = Mutex::new(Cursor::default());
            let applied = AtomicU64::new(0);
            let mut first = true;
            execute_blocking(
                &global,
                &progress,
                &applied,
                &state(),
                Duration::MAX,
                |data, ctx, action| {
                    if first {
                        first = false;
                        entered.send(()).unwrap();
                        released.recv().unwrap();
                    }
                    apply(data, ctx, action)
                },
            )
            .unwrap()
        });
        entering.recv_timeout(Duration::from_secs(5)).unwrap();
        let other = owner.clone();
        let (finished, finish) = mpsc::sync_channel(1);
        let query = std::thread::spawn(move || {
            let result = other.call(|node| {
                let answer = node.receive(
                    &1,
                    &crate::Rpc::Append {
                        term: 1,
                        previous: 0,
                        previous_term: 0,
                        previous_scn: common::scn::Scn::new(0),
                        entries: vec![],
                        commit: 0,
                    },
                )?;
                let appended = node.receive(
                    &1,
                    &crate::Rpc::Append {
                        term: 1,
                        previous: 0,
                        previous_term: 0,
                        previous_scn: common::scn::Scn::new(0),
                        entries: vec![Entry {
                            scn: common::scn::Scn::new(1),
                            index: 1,
                            term: 1,
                            command: Command::Noop,
                            entry_buf: None,
                        }]
                        .into(),
                        commit: 1,
                    },
                )?;
                Ok((answer.success, appended.success && node.state().commit == 1))
            });
            finished.send(result).unwrap();
        });
        let result = finish.recv_timeout(Duration::from_secs(5));
        // Always release the blocked application before asserting the result.
        release.send(()).unwrap();
        let applied = application.join().unwrap();
        query.join().unwrap();
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        consensus.join().unwrap();
        assert_eq!(result.unwrap().unwrap(), (true, true));
        assert!(applied);
    }
    #[test]
    fn yields_between_actions_and_does_not_apply_uncommitted_entries() {
        let mut state = state();
        let first = crate::codec::encode(&state.log[0]).unwrap();
        state.log.set(0, crate::codec::decode(&first).unwrap());
        assert!(matches!(&state.log[0].command, Command::Client(actions) if actions.is_empty()));
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        let applied = AtomicU64::new(0);
        for expected in 1..=3 {
            let finished =
                execute(&global, &cursor, &applied, &state, Duration::ZERO, apply).unwrap();
            assert!(!finished);
            let checkpoint_read = global.read();
            assert_eq!(checkpoint_read.values.len(), expected);
            let blocked =
                execute(&global, &cursor, &applied, &state, Duration::MAX, apply).unwrap();
            assert!(!blocked);
        }
        let finished = execute(&global, &cursor, &applied, &state, Duration::MAX, apply).unwrap();
        let data = global.read();
        assert!(finished);
        assert_eq!(data.values, vec![1, 2, 3]);
        assert_eq!(applied.load(Ordering::Acquire), 1);
    }
    #[test]
    fn snapshot_restore_resumes_between_actions() {
        let mut state = state();
        state.apply(&crate::Record::Install(state.capture())).unwrap();
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        let applied = AtomicU64::new(0);
        for _ in 0..3 {
            let finished =
                execute(&global, &cursor, &applied, &state, Duration::ZERO, apply).unwrap();
            assert!(!finished);
        }
        let finished = execute(&global, &cursor, &applied, &state, Duration::MAX, apply).unwrap();
        assert!(finished);
        assert_eq!(global.read().values, vec![1, 2, 3]);
    }
    #[test]
    fn failed_action_does_not_repeat_successful_predecessors() {
        let state = state();
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        let applied = AtomicU64::new(0);
        let failed =
            execute(&global, &cursor, &applied, &state, Duration::MAX, |data, ctx, action| {
                if action.fields == [2] {
                    return Err(Error::new("client failure"));
                }
                apply(data, ctx, action)
            });
        assert!(failed.is_err());
        let finished = execute(&global, &cursor, &applied, &state, Duration::MAX, apply).unwrap();
        assert!(finished);
        assert_eq!(global.read().values, vec![1, 2, 3]);
    }
}
