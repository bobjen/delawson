//! Shared action iteration and global-lock budgeting. Clients dispatch action tags.
use crate::{Action, Command, Entry, Snapshot, State};
use common::{Error, Result};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A detached, immutable view of committed work. No consensus lock is retained.
pub struct Work {
    pub generation: u64,
    pub ring: String,
    pub snapshot: Arc<Snapshot>,
    pub entries: Vec<Entry>,
    pub commit: u64,
}
pub trait Committed {
    fn generation(&self) -> u64;
    fn ring(&self) -> &str;
    fn snapshot(&self) -> &Snapshot;
    fn entries(&self) -> &[Entry];
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
    fn entries(&self) -> &[Entry] {
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
    fn entries(&self) -> &[Entry] {
        &self.entries
    }
    fn commit(&self) -> u64 {
        self.commit
    }
}
pub trait Application: Default + Send + Sync {
    fn applied(&self) -> u64;
    fn set_applied(&mut self, index: u64);
    /// Reclaim application-owned state while the global lock is still exclusive.
    fn reclaim(&mut self) {}
    fn reset(&mut self) {
        *self = Self::default();
    }
}
#[derive(Default)]
pub struct Cursor {
    generation: u64,
    restoring: Option<u64>,
    entry: u64,
    action: usize,
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
    state: &impl Committed,
    budget: Duration,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    execute_state(
        global,
        progress,
        state,
        budget,
        false,
        |data| data.applied(),
        |data, index| data.set_applied(index),
        |data| data.reset(),
        apply,
    )
}
/// The application worker can sleep on the global lock without delaying consensus.
pub fn execute_blocking<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    state: &impl Committed,
    budget: Duration,
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    execute_state(
        global,
        progress,
        state,
        budget,
        true,
        |data| data.applied(),
        |data, index| data.set_applied(index),
        |data| data.reset(),
        apply,
    )
}
pub fn execute_state<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    state: &impl Committed,
    budget: Duration,
    blocking: bool,
    applied: impl Fn(&S) -> u64,
    set_applied: impl Fn(&mut S, u64),
    reset: impl Fn(&mut S),
    apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    execute_with_lock(global, progress, state, budget, blocking, applied, set_applied, reset, apply)
}
fn execute_with_lock<S: Application>(
    global: &RwLock<S>,
    progress: &Mutex<Cursor>,
    state: &impl Committed,
    budget: Duration,
    blocking: bool,
    applied: impl Fn(&S) -> u64,
    set_applied: impl Fn(&mut S, u64),
    reset: impl Fn(&mut S),
    mut apply: impl FnMut(&mut S, Context<'_>, &Action) -> Result<()>,
) -> Result<bool> {
    let mut cursor = progress.lock().map_err(|_| Error::new("execution cursor poisoned"))?;
    let mut data = if blocking {
        global.write()
    } else if let Some(data) = global.try_write() {
        data
    } else {
        return Ok(false);
    };
    let result = (|| -> Result<bool> {
        if cursor.generation != state.generation() {
            reset(&mut *data);
            *cursor = Cursor { generation: state.generation(), ..Cursor::default() };
        }
        if applied(&data) == state.commit() && cursor.restoring.is_none() {
            return Ok(true);
        }
        if applied(&data) < state.snapshot().index
            && cursor.restoring != Some(state.snapshot().index)
        {
            reset(&mut *data);
            cursor.restoring = Some(state.snapshot().index);
            cursor.entry = 0;
            cursor.action = 0;
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
            }
            match &entry.command {
                Command::Client(actions) => {
                    while cursor.action < actions.len() {
                        apply(
                            data,
                            Context { ring: state.ring(), index: entry.index, term: entry.term },
                            &actions[cursor.action],
                        )?;
                        cursor.action += 1;
                        if began.elapsed() >= budget {
                            return Ok(false);
                        }
                    }
                }
                Command::Noop | Command::Members(_) | Command::CheckpointStart(_) => {}
            }
            if !restoring {
                set_applied(data, entry.index);
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
            set_applied(&mut *data, state.snapshot().index);
            cursor.restoring = None;
            cursor.entry = 0;
            cursor.action = 0;
        }
        for entry in state.entries().iter().filter(|entry| entry.index <= state.commit()) {
            if entry.index > applied(&data) && !run(entry, false, &mut *data, &mut cursor)? {
                return Ok(false);
            }
        }
        set_applied(&mut *data, state.commit());
        Ok(true)
    })();
    (&mut *data).reclaim();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Op;
    #[derive(Default)]
    struct Values {
        applied: u64,
        values: Vec<u8>,
    }
    impl Application for Values {
        fn applied(&self) -> u64 {
            self.applied
        }
        fn set_applied(&mut self, index: u64) {
            self.applied = index;
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
        let mut old = state();
        old.commit = 2;
        execute_blocking(&global, &cursor, &old, Duration::MAX, apply).unwrap();
        let mut replacement = state();
        replacement.generation = 1;
        replacement.log[0].command = Command::Client(vec![Action::new(Op::BytesResp, vec![9])]);
        execute_blocking(&global, &cursor, &replacement, Duration::MAX, apply).unwrap();
        let data = global.read();
        assert_eq!(data.applied, 1);
        assert_eq!(data.values, vec![9]);
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
            let mut first = true;
            execute_blocking(&global, &progress, &state(), Duration::MAX, |data, ctx, action| {
                if first {
                    first = false;
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                }
                apply(data, ctx, action)
            })
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
                        }],
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
        let state = state();
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        for expected in 1..=3 {
            let finished = execute(&global, &cursor, &state, Duration::ZERO, apply).unwrap();
            assert!(!finished);
            let checkpoint_read = global.read();
            assert_eq!(checkpoint_read.values.len(), expected);
            let blocked = execute(&global, &cursor, &state, Duration::MAX, apply).unwrap();
            assert!(!blocked);
        }
        let finished = execute(&global, &cursor, &state, Duration::MAX, apply).unwrap();
        let data = global.read();
        assert!(finished);
        assert_eq!(data.values, vec![1, 2, 3]);
        assert_eq!(data.applied, 1);
    }
    #[test]
    fn snapshot_restore_resumes_between_actions() {
        let mut state = state();
        state.apply(&crate::Record::Install(state.capture())).unwrap();
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        for _ in 0..3 {
            let finished = execute(&global, &cursor, &state, Duration::ZERO, apply).unwrap();
            assert!(!finished);
        }
        let finished = execute(&global, &cursor, &state, Duration::MAX, apply).unwrap();
        assert!(finished);
        assert_eq!(global.read().values, vec![1, 2, 3]);
    }
    #[test]
    fn failed_action_does_not_repeat_successful_predecessors() {
        let state = state();
        let global = RwLock::new(Values::default());
        let cursor = Mutex::new(Cursor::default());
        let failed = execute(&global, &cursor, &state, Duration::MAX, |data, ctx, action| {
            if action.fields == [2] {
                return Err(Error::new("client failure"));
            }
            apply(data, ctx, action)
        });
        assert!(failed.is_err());
        let finished = execute(&global, &cursor, &state, Duration::MAX, apply).unwrap();
        assert!(finished);
        assert_eq!(global.read().values, vec![1, 2, 3]);
    }
}
