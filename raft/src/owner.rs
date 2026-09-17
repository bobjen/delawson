//! A single owner for consensus state. Calls carry owned inputs and return owned results.
use common::{Error, Result};
use std::sync::{Mutex, OnceLock, mpsc};
use std::time::Duration;

type Job<T> = Box<dyn FnOnce(&mut T) + Send>;
pub struct Owner<T> {
    initial: Mutex<Option<T>>,
    sender: OnceLock<mpsc::Sender<Job<T>>>,
}
impl<T: Send + 'static> Owner<T> {
    pub fn new(value: T) -> Self {
        Self { initial: Mutex::new(Some(value)), sender: OnceLock::new() }
    }
    pub fn initial_call<R>(&self, f: impl FnOnce(&mut T) -> Result<R>) -> Result<R> {
        let mut initial =
            self.initial.lock().map_err(|_| Error::new("Raft initialization poisoned"))?;
        f(initial.as_mut().ok_or_else(|| Error::new("Raft already running"))?)
    }
    pub fn call<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        if let Some(sender) = self.sender.get() {
            let (reply, receive) = mpsc::sync_channel(1);
            sender
                .send(Box::new(move |state| {
                    let _ = reply.send(f(state));
                }))
                .map_err(|_| Error::new("Raft loop stopped"))?;
            return receive.recv().map_err(|_| Error::new("Raft loop stopped without replying"))?;
        }
        let mut initial =
            self.initial.lock().map_err(|_| Error::new("Raft initialization poisoned"))?;
        f(initial.as_mut().ok_or_else(|| Error::new("Raft loop starting"))?)
    }
    pub fn start(
        &self,
        mut tick: impl FnMut(&mut T) -> bool + Send + 'static,
    ) -> Result<std::thread::JoinHandle<()>> {
        let mut initial =
            self.initial.lock().map_err(|_| Error::new("Raft initialization poisoned"))?;
        let (sender, receive) = mpsc::channel::<Job<T>>();
        if self.sender.get().is_some() {
            return Err(Error::new("Raft loop already started"));
        }
        let mut state = initial.take().ok_or_else(|| Error::new("missing Raft state"))?;
        let handle = std::thread::Builder::new().name("raft-main".into()).spawn(move || {
            let interval = Duration::from_millis(20);
            let mut deadline = std::time::Instant::now();
            loop {
                if std::time::Instant::now() >= deadline {
                    if !tick(&mut state) {
                        break;
                    }
                    deadline = std::time::Instant::now() + interval;
                }
                match receive
                    .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                {
                    Ok(job) => job(&mut state),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        })?;
        self.sender.set(sender).map_err(|_| Error::new("Raft loop already started"))?;
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn calls_share_one_owner_and_shutdown_rejects_more_work() {
        let owner = std::sync::Arc::new(Owner::new(Vec::new()));
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = stop.clone();
        let thread =
            owner.start(move |_| !stopped.load(std::sync::atomic::Ordering::SeqCst)).unwrap();
        let expected = thread.thread().id();
        let clients: Vec<_> = (0..4)
            .map(|id| {
                let owner = owner.clone();
                std::thread::spawn(move || {
                    owner
                        .call(move |state| {
                            state.push(id);
                            Ok(std::thread::current().id())
                        })
                        .unwrap()
                })
            })
            .collect();
        let actual: Vec<_> = clients.into_iter().map(|client| client.join().unwrap()).collect();
        let mut contents = owner.call(|state| Ok(state.clone())).unwrap();
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        thread.join().unwrap();
        let rejected = owner.call(|_| Ok(()));
        contents.sort();
        assert_eq!(contents, vec![0, 1, 2, 3]);
        assert!(actual.iter().all(|id| *id == expected));
        assert!(rejected.is_err());
    }
}
