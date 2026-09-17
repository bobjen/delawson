//! Ordered disk work. Waiting for a fence is only allowed outside raft-main.
use common::{Error, Result};
use std::sync::{Arc, Condvar, Mutex, mpsc};

type Job = Box<dyn FnOnce() -> Result<()> + Send>;
#[derive(Default)]
struct Progress {
    submitted: u64,
    completed: u64,
    failed: Option<String>,
}
#[derive(Clone)]
pub struct Fence {
    shared: Arc<(Mutex<Progress>, Condvar)>,
    ticket: u64,
}
impl Fence {
    pub fn wait(&self) -> Result<()> {
        let (lock, wake) = &*self.shared;
        let mut progress = lock.lock().map_err(|_| Error::new("journal completion poisoned"))?;
        while progress.completed < self.ticket && progress.failed.is_none() {
            progress =
                wake.wait(progress).map_err(|_| Error::new("journal completion poisoned"))?;
        }
        if let Some(error) = &progress.failed {
            return Err(Error::new(error));
        }
        Ok(())
    }
    pub fn wait_all(&self) -> Result<()> {
        let ticket =
            self.shared.0.lock().map_err(|_| Error::new("journal completion poisoned"))?.submitted;
        Self { shared: self.shared.clone(), ticket }.wait()
    }
    pub fn completed(&self) -> Result<u64> {
        let progress =
            self.shared.0.lock().map_err(|_| Error::new("journal completion poisoned"))?;
        if let Some(error) = &progress.failed {
            return Err(Error::new(error));
        }
        Ok(progress.completed)
    }
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
    sender: mpsc::Sender<(u64, Job)>,
    fence: Fence,
}
impl Queue {
    pub fn new(wake_owner: impl Fn() + Send + 'static) -> Result<Self> {
        let (sender, receive) = mpsc::channel::<(u64, Job)>();
        let fence = Fence {
            shared: Arc::new((Mutex::new(Progress::default()), Condvar::new())),
            ticket: 0,
        };
        let shared = fence.shared.clone();
        std::thread::Builder::new().name("raft-io".into()).spawn(move || {
            for (ticket, job) in receive {
                let failed = shared.0.lock().unwrap().failed.is_some();
                if failed {
                    continue;
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                    .unwrap_or_else(|_| Err(Error::new("journal I/O worker panicked")));
                let mut progress = shared.0.lock().unwrap();
                match result {
                    Ok(()) => progress.completed = ticket,
                    Err(error) => progress.failed = Some(error.to_string()),
                }
                drop(progress);
                shared.1.notify_all();
                wake_owner();
            }
        })?;
        Ok(Self { sender, fence })
    }
    pub fn submit(&mut self, job: impl FnOnce() -> Result<()> + Send + 'static) -> Result<u64> {
        self.fence.completed()?;
        let ticket =
            self.fence.ticket.checked_add(1).ok_or_else(|| Error::new("I/O ticket overflow"))?;
        self.sender
            .send((ticket, Box::new(job)))
            .map_err(|_| Error::new("journal I/O worker stopped"))?;
        self.fence
            .shared
            .0
            .lock()
            .map_err(|_| Error::new("journal completion poisoned"))?
            .submitted = ticket;
        self.fence.ticket = ticket;
        Ok(ticket)
    }
    pub fn read<T: Send + 'static>(
        &mut self,
        job: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<Read<T>> {
        let (send, receive) = mpsc::channel();
        self.submit(move || {
            let _ = send.send(job());
            Ok(())
        })?;
        Ok(Read(receive))
    }
    pub fn ticket(&self) -> u64 {
        self.fence.ticket
    }
    pub fn fence(&self) -> Fence {
        self.fence.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completion_wakes_a_sleeping_owner_without_waiting_for_its_timer() {
        let owner = Arc::new(crate::owner::Owner::new(()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let (ticked, ticks) = mpsc::channel();
        let thread = owner
            .start_with_interval(std::time::Duration::from_secs(60), move |_| {
                let _ = ticked.send(());
                !stopping.load(std::sync::atomic::Ordering::SeqCst)
            })
            .unwrap();
        ticks.recv().unwrap();
        let waking = owner.clone();
        let mut io = Queue::new(move || waking.wake()).unwrap();
        io.submit(|| Ok(())).unwrap();
        // This is a liveness check, not a latency benchmark. The timer is a minute away.
        let woke = ticks.recv_timeout(std::time::Duration::from_secs(5)).is_ok();
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        owner.wake();
        thread.join().unwrap();
        assert!(woke);
    }

    #[test]
    fn delayed_io_does_not_block_owner_and_failure_fences_successors() {
        let owner = Arc::new(crate::owner::Owner::new(0));
        let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = stopping.clone();
        let thread =
            owner.start(move |_| !stopped.load(std::sync::atomic::Ordering::SeqCst)).unwrap();
        let mut io = Queue::new(|| {}).unwrap();
        let (release, wait) = mpsc::channel();
        io.submit(move || {
            wait.recv().unwrap();
            Ok(())
        })
        .unwrap();
        let fence = io.fence();
        let (finished, completion) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            finished.send(fence.wait()).unwrap();
        });
        let result = owner
            .call(|value| {
                *value += 1;
                Ok(*value)
            })
            .unwrap();
        assert_eq!(result, 1);
        assert!(completion.try_recv().is_err());
        release.send(()).unwrap();
        completion.recv().unwrap().unwrap();
        waiter.join().unwrap();
        io.submit(|| Err(Error::new("injected disk failure"))).unwrap();
        let failed = io.fence().wait().is_err();
        assert!(failed);
        assert!(io.submit(|| Ok(())).is_err());
        stopping.store(true, std::sync::atomic::Ordering::SeqCst);
        owner.wake();
        thread.join().unwrap();
    }
}
