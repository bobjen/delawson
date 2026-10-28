//! Parallel disk work with prefix-ordered completion. Waiting for a fence is
//! only allowed outside raft-main.
use common::{Error, Result};
use std::collections::{BTreeSet, VecDeque};
use std::fs::File;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Instant;

const WORKERS: usize = 4;
type Job = Box<dyn FnOnce() -> Result<Work> + Send>;

type AfterSync = Box<dyn FnOnce(u64, usize) -> Result<()> + Send>;

pub struct SyncRequest {
    file: Arc<File>,
    after: AfterSync,
}
impl SyncRequest {
    pub fn new(
        file: Arc<File>,
        after: impl FnOnce(u64, usize) -> Result<()> + Send + 'static,
    ) -> Self {
        Self { file, after: Box::new(after) }
    }
    pub(crate) fn run(self) -> Result<()> {
        let start = Instant::now();
        self.file.sync_all()?;
        let sync_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        (self.after)(sync_ns, 0)
    }
}

enum Work {
    Complete,
    Sync(SyncRequest),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Positional journal writes may run alongside one another.
    Parallel,
    /// Identity updates wait for the prior prefix, but do not stop later writes.
    Identity,
    /// File replacement and recovery work waits for and blocks all other work.
    Ordered,
}

struct Task {
    ticket: u64,
    kind: Kind,
    job: Job,
}

struct PendingSync {
    ticket: u64,
    request: SyncRequest,
}
type FenceCallback = Box<dyn FnOnce(Result<()>) + Send + 'static>;
struct FenceWaiter {
    ticket: u64,
    done: FenceCallback,
}

#[derive(Default)]
struct Progress {
    submitted: u64,
    completed: u64,
    finished: BTreeSet<u64>,
    failed: Option<String>,
    tasks: VecDeque<Task>,
    syncs: VecDeque<PendingSync>,
    fence_waiters: Vec<FenceWaiter>,
    running: usize,
    ordered_active: bool,
    shutdown: bool,
}

struct Shared {
    progress: Mutex<Progress>,
    wake: Condvar,
    sync_wake: Condvar,
    wake_owner: Box<dyn Fn() + Send + Sync>,
}

#[derive(Clone)]
pub struct Fence {
    shared: Arc<Shared>,
    ticket: u64,
}
impl Fence {
    pub fn wait(&self) -> Result<()> {
        let mut progress =
            self.shared.progress.lock().map_err(|_| Error::new("journal completion poisoned"))?;
        while progress.completed < self.ticket && progress.failed.is_none() {
            progress = self
                .shared
                .wake
                .wait(progress)
                .map_err(|_| Error::new("journal completion poisoned"))?;
        }
        if let Some(error) = &progress.failed {
            return Err(Error::new(error));
        }
        Ok(())
    }
    pub fn wait_all(&self) -> Result<()> {
        let ticket = self
            .shared
            .progress
            .lock()
            .map_err(|_| Error::new("journal completion poisoned"))?
            .submitted;
        Self { shared: self.shared.clone(), ticket }.wait()
    }
    pub fn completed(&self) -> Result<u64> {
        let progress =
            self.shared.progress.lock().map_err(|_| Error::new("journal completion poisoned"))?;
        if let Some(error) = &progress.failed {
            return Err(Error::new(error));
        }
        Ok(progress.completed)
    }
    /// Run `done` when this prefix is durable, without tying up a waiting thread.
    pub fn then(self, done: impl FnOnce(Result<()>) + Send + 'static) {
        let mut done: Option<FenceCallback> = Some(Box::new(done));
        let ready = {
            let mut progress = self.shared.progress.lock().expect("journal completion");
            if let Some(error) = &progress.failed {
                Some((done.take().expect("fence callback"), Err(Error::new(error))))
            } else if progress.completed >= self.ticket {
                Some((done.take().expect("fence callback"), Ok(())))
            } else {
                progress.fence_waiters.push(FenceWaiter {
                    ticket: self.ticket,
                    done: done.take().expect("fence callback"),
                });
                None
            }
        };
        if let Some((done, result)) = ready {
            run_fence_callback(done, result);
        }
    }
}

fn run_fence_callback(done: FenceCallback, result: Result<()>) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| done(result)));
}

fn take_fence_callbacks(progress: &mut Progress) -> Vec<(FenceCallback, Result<()>)> {
    let failure = progress.failed.clone();
    let mut ready = Vec::new();
    let mut at = 0;
    while at < progress.fence_waiters.len() {
        let waiter = &progress.fence_waiters[at];
        if let Some(error) = &failure {
            let waiter = progress.fence_waiters.swap_remove(at);
            ready.push((waiter.done, Err(Error::new(error))));
        } else if waiter.ticket <= progress.completed {
            let waiter = progress.fence_waiters.swap_remove(at);
            ready.push((waiter.done, Ok(())));
        } else {
            at += 1;
        }
    }
    ready
}

pub struct Read<T>(mpsc::Receiver<Result<T>>);
impl<T> Read<T> {
    pub fn wait(self) -> Result<T> {
        self.0.recv().map_err(|_| Error::new("I/O result lost"))?
    }
    pub fn ready(result: Result<T>) -> Self {
        let (send, receive) = mpsc::channel();
        let _ = send.send(result);
        Self(receive)
    }
}

pub struct Queue {
    shared: Arc<Shared>,
    _workers: Vec<std::thread::JoinHandle<()>>,
}
impl Queue {
    pub fn new(wake_owner: impl Fn() + Send + Sync + 'static) -> Result<Self> {
        let shared = Arc::new(Shared {
            progress: Mutex::new(Progress::default()),
            wake: Condvar::new(),
            sync_wake: Condvar::new(),
            wake_owner: Box::new(wake_owner),
        });
        let mut workers = Vec::with_capacity(WORKERS);
        for at in 0..WORKERS {
            let thread_shared = shared.clone();
            match std::thread::Builder::new()
                .name(format!("raft-io-{at}"))
                .spawn(move || worker(thread_shared))
            {
                Ok(thread) => workers.push(thread),
                Err(error) => {
                    shared.progress.lock().unwrap().shutdown = true;
                    shared.wake.notify_all();
                    shared.sync_wake.notify_all();
                    return Err(error.into());
                }
            }
        }
        let thread_shared = shared.clone();
        match std::thread::Builder::new()
            .name("raft-sync".into())
            .spawn(move || sync_worker(thread_shared))
        {
            Ok(thread) => workers.push(thread),
            Err(error) => {
                shared.progress.lock().unwrap().shutdown = true;
                shared.wake.notify_all();
                shared.sync_wake.notify_all();
                return Err(error.into());
            }
        }
        Ok(Self { shared, _workers: workers })
    }
    pub fn submit(&mut self, job: impl FnOnce() -> Result<()> + Send + 'static) -> Result<u64> {
        self.submit_kind(Kind::Parallel, job)
    }
    pub fn submit_identity(
        &mut self,
        job: impl FnOnce() -> Result<()> + Send + 'static,
    ) -> Result<u64> {
        self.submit_kind(Kind::Identity, job)
    }
    pub fn submit_ordered(
        &mut self,
        job: impl FnOnce() -> Result<()> + Send + 'static,
    ) -> Result<u64> {
        self.submit_kind(Kind::Ordered, job)
    }
    pub fn submit_kind(
        &mut self,
        kind: Kind,
        job: impl FnOnce() -> Result<()> + Send + 'static,
    ) -> Result<u64> {
        let mut progress =
            self.shared.progress.lock().map_err(|_| Error::new("journal completion poisoned"))?;
        if let Some(error) = &progress.failed {
            return Err(Error::new(error));
        }
        if progress.shutdown {
            return Err(Error::new("journal I/O queue stopped"));
        }
        let ticket =
            progress.submitted.checked_add(1).ok_or_else(|| Error::new("I/O ticket overflow"))?;
        progress.submitted = ticket;
        progress.tasks.push_back(Task {
            ticket,
            kind,
            job: Box::new(move || job().map(|()| Work::Complete)),
        });
        drop(progress);
        self.shared.wake.notify_all();
        Ok(ticket)
    }
    pub fn submit_sync_kind(
        &mut self,
        kind: Kind,
        job: impl FnOnce() -> Result<SyncRequest> + Send + 'static,
    ) -> Result<u64> {
        let mut progress =
            self.shared.progress.lock().map_err(|_| Error::new("journal completion poisoned"))?;
        if let Some(error) = &progress.failed {
            return Err(Error::new(error));
        }
        if progress.shutdown {
            return Err(Error::new("journal I/O queue stopped"));
        }
        let ticket =
            progress.submitted.checked_add(1).ok_or_else(|| Error::new("I/O ticket overflow"))?;
        progress.submitted = ticket;
        progress.tasks.push_back(Task {
            ticket,
            kind,
            job: Box::new(move || job().map(Work::Sync)),
        });
        drop(progress);
        self.shared.wake.notify_all();
        Ok(ticket)
    }
    pub fn read<T: Send + 'static>(
        &mut self,
        job: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<Read<T>> {
        let (send, receive) = mpsc::channel();
        self.submit_ordered(move || {
            let _ = send.send(job());
            Ok(())
        })?;
        Ok(Read(receive))
    }
    pub fn ticket(&self) -> u64 {
        self.shared.progress.lock().unwrap().submitted
    }
    pub fn fence(&self) -> Fence {
        let ticket = self.shared.progress.lock().unwrap().submitted;
        Fence { shared: self.shared.clone(), ticket }
    }
}
impl Drop for Queue {
    fn drop(&mut self) {
        if let Ok(mut progress) = self.shared.progress.lock() {
            progress.shutdown = true;
        }
        self.shared.wake.notify_all();
        self.shared.sync_wake.notify_all();
    }
}

fn worker(shared: Arc<Shared>) {
    loop {
        let task = {
            let mut progress = shared.progress.lock().unwrap();
            loop {
                if progress.failed.is_some() {
                    return;
                }
                if let Some(at) = next_task(&progress) {
                    let task = progress.tasks.remove(at).unwrap();
                    progress.running += 1;
                    if task.kind == Kind::Ordered {
                        progress.ordered_active = true;
                    }
                    break task;
                }
                if progress.shutdown && progress.tasks.is_empty() && progress.running == 0 {
                    return;
                }
                progress = shared.wake.wait(progress).unwrap();
            }
        };
        let ticket = task.ticket;
        let ordered = task.kind == Kind::Ordered;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task.job))
            .unwrap_or_else(|_| Err(Error::new("journal I/O worker panicked")));
        let mut progress = shared.progress.lock().unwrap();
        progress.running -= 1;
        let mut wake_sync = false;
        if ordered {
            progress.ordered_active = false;
        }
        match result {
            Ok(Work::Complete) => finish_ticket(&mut progress, ticket),
            Ok(Work::Sync(request)) => {
                if progress.failed.is_none() {
                    wake_sync = progress.syncs.is_empty();
                    progress.syncs.push_back(PendingSync { ticket, request });
                }
            }
            Err(error) => {
                if progress.failed.is_none() {
                    progress.failed = Some(error.to_string());
                }
                progress.tasks.clear();
                progress.syncs.clear();
                wake_sync = true;
            }
        }
        wake_sync |= progress.shutdown
            && progress.tasks.is_empty()
            && progress.running == 0;
        let callbacks = take_fence_callbacks(&mut progress);
        drop(progress);
        for (done, result) in callbacks {
            run_fence_callback(done, result);
        }
        shared.wake.notify_all();
        if wake_sync {
            shared.sync_wake.notify_one();
        }
        (shared.wake_owner)();
    }
}

fn sync_worker(shared: Arc<Shared>) {
    loop {
        let (file, batch) = {
            let mut progress = shared.progress.lock().unwrap();
            loop {
                if progress.failed.is_some() {
                    return;
                }
                if let Some(first) = progress.syncs.pop_front() {
                    let file = first.request.file.clone();
                    let mut batch = vec![first];
                    let mut remaining = VecDeque::new();
                    while let Some(next) = progress.syncs.pop_front() {
                        if Arc::ptr_eq(&file, &next.request.file) {
                            batch.push(next);
                        } else {
                            remaining.push_back(next);
                        }
                    }
                    progress.syncs = remaining;
                    break (file, batch);
                }
                if progress.shutdown && progress.tasks.is_empty() && progress.running == 0 {
                    return;
                }
                progress = shared.sync_wake.wait(progress).unwrap();
            }
        };

        let start = Instant::now();
        let result = file.sync_all().map_err(Error::from);
        let sync_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let count = batch.len();
        let result = result.and_then(|()| {
            let mut finished = Vec::with_capacity(count);
            for (at, pending) in batch.into_iter().enumerate() {
                let after = pending.request.after;
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| after(sync_ns, at)))
                    .unwrap_or_else(|_| Err(Error::new("journal sync worker panicked")))?;
                finished.push(pending.ticket);
            }
            let mut progress = shared.progress.lock().unwrap();
            for ticket in finished {
                finish_ticket(&mut progress, ticket);
            }
            Ok(())
        });
        let callbacks = {
            let mut progress = shared.progress.lock().unwrap();
            if let Err(error) = result {
                if progress.failed.is_none() {
                    progress.failed = Some(error.to_string());
                }
                progress.tasks.clear();
                progress.syncs.clear();
            }
            take_fence_callbacks(&mut progress)
        };
        for (done, result) in callbacks {
            run_fence_callback(done, result);
        }
        shared.wake.notify_all();
        (shared.wake_owner)();
    }
}

fn finish_ticket(progress: &mut Progress, ticket: u64) {
    progress.finished.insert(ticket);
    while let Some(next) = progress.completed.checked_add(1) {
        if !progress.finished.remove(&next) {
            break;
        }
        progress.completed = next;
    }
}

fn next_task(progress: &Progress) -> Option<usize> {
    if progress.ordered_active || progress.running >= WORKERS {
        return None;
    }
    for (at, task) in progress.tasks.iter().enumerate() {
        match task.kind {
            Kind::Parallel => return Some(at),
            Kind::Identity => {
                if progress.completed >= task.ticket - 1 {
                    return Some(at);
                }
            }
            Kind::Ordered => {
                if progress.completed >= task.ticket - 1 && progress.running == 0 {
                    return Some(at);
                }
                // An ordered operation is a barrier even while it is waiting.
                return None;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn completion_wakes_a_sleeping_owner_without_waiting_for_its_timer() {
        let owner = Arc::new(crate::owner::Owner::new(()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let (ticked, ticks) = mpsc::channel();
        let thread = owner
            .start_with_interval(Duration::from_secs(60), move |_| {
                let _ = ticked.send(());
                !stopping.load(std::sync::atomic::Ordering::SeqCst)
            })
            .unwrap();
        ticks.recv().unwrap();
        let waking = owner.clone();
        let mut io = Queue::new(move || waking.wake()).unwrap();
        io.submit(|| Ok(())).unwrap();
        // This is a liveness check, not a latency benchmark. The timer is a minute away.
        let woke = ticks.recv_timeout(Duration::from_secs(5)).is_ok();
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        owner.wake();
        thread.join().unwrap();
        assert!(woke);
    }

    #[test]
    fn identity_waits_for_prefix_but_does_not_block_later_writes() {
        let mut io = Queue::new(|| {}).unwrap();
        let (release_first, wait_first) = mpsc::channel();
        let (first_started, first_seen) = mpsc::channel();
        io.submit(move || {
            first_started.send(()).unwrap();
            wait_first.recv().unwrap();
            Ok(())
        })
        .unwrap();
        first_seen.recv_timeout(Duration::from_secs(5)).unwrap();

        let (release_identity, wait_identity) = mpsc::channel();
        let (identity_started, identity_seen) = mpsc::channel();
        io.submit_identity(move || {
            identity_started.send(()).unwrap();
            wait_identity.recv().unwrap();
            Ok(())
        })
        .unwrap();

        let (later_started, later_seen) = mpsc::channel();
        io.submit(move || {
            later_started.send(()).unwrap();
            Ok(())
        })
        .unwrap();
        later_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(identity_seen.try_recv().is_err());

        release_first.send(()).unwrap();
        identity_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        release_identity.send(()).unwrap();
        io.fence().wait().unwrap();
    }

    #[test]
    fn ordered_work_is_a_barrier_for_later_writes() {
        let mut io = Queue::new(|| {}).unwrap();
        let (release_first, wait_first) = mpsc::channel();
        let (first_started, first_seen) = mpsc::channel();
        io.submit(move || {
            first_started.send(()).unwrap();
            wait_first.recv().unwrap();
            Ok(())
        })
        .unwrap();
        first_seen.recv_timeout(Duration::from_secs(5)).unwrap();

        let (release_ordered, wait_ordered) = mpsc::channel();
        let (ordered_started, ordered_seen) = mpsc::channel();
        io.submit_ordered(move || {
            ordered_started.send(()).unwrap();
            wait_ordered.recv().unwrap();
            Ok(())
        })
        .unwrap();
        let (later_started, later_seen) = mpsc::channel();
        io.submit(move || {
            later_started.send(()).unwrap();
            Ok(())
        })
        .unwrap();
        assert!(later_seen.recv_timeout(Duration::from_millis(50)).is_err());

        release_first.send(()).unwrap();
        ordered_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(later_seen.try_recv().is_err());
        release_ordered.send(()).unwrap();
        later_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        io.fence().wait().unwrap();
    }

    #[test]
    fn failed_io_fences_successors() {
        let mut io = Queue::new(|| {}).unwrap();
        io.submit(|| Err(Error::new("injected disk failure"))).unwrap();
        assert!(io.fence().wait().is_err());
        assert!(io.submit(|| Ok(())).is_err());
    }
}
