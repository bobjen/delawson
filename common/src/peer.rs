//! One connection to another process, shared by every thread that needs it.
//!
//! Requests from many threads pile into one buffer and leave together, and a
//! reader thread hands each answer back to whoever asked by matching its id. That
//! is the whole reason the protocol carries ids: a service talking to another
//! service gets the same coalescing a client does, without a connection per
//! thread.
//!
//! Asking is asynchronous: `ask` hands over what to do with the answer and
//! returns, so no thread is held while another process works. `call` waits, on a
//! slot of its own so an answer wakes one thread rather than all of them, and is
//! for startup paths where there is nothing else to get on with.

use crate::config::Messaging;
use crate::wire::{self, Outbox, Req, Resp, put_request};
use crate::{Error, Result};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

struct Slot {
    answer: Mutex<Option<Resp>>,
    arrived: Condvar,
}

thread_local! {
    /// Framing scratch, so asking allocates nothing.
    static FRAME: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };

}

type Then = Box<dyn FnOnce(Resp) + Send>;

pub struct Peer {
    sending: Arc<Outbox>,
    waiting: Mutex<HashMap<u32, Then>>,
    next_id: AtomicU32,
    timeout: Duration,
    broken: AtomicBool,
    socket: TcpStream,
}

impl Peer {
    pub fn connect(
        address: SocketAddr,
        config: &Messaging,
        timeout: Duration,
    ) -> Result<Arc<Peer>> {
        let stream = TcpStream::connect_timeout(&address, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let control = stream.try_clone()?;
        let link = wire::connect(stream, config)?;
        control.set_read_timeout(None)?;
        let peer = Arc::new(Peer {
            sending: Outbox::start(link.wire),
            waiting: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(0),
            timeout,
            broken: AtomicBool::new(false),
            socket: control,
        });

        let reading = Arc::clone(&peer);
        let mut reader = link.reader;
        std::thread::spawn(move || {
            while let Ok(response) = reader.response() {
                reading.deliver(response);
            }
            // The connection is done; anyone still waiting will time out.
            reading.disconnect();
        });
        Ok(peer)
    }

    fn disconnect(&self) {
        self.broken.store(true, Ordering::SeqCst);
        self.sending.close();
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
    }
    fn deliver(&self, response: Resp) {
        let then = self.waiting.lock().expect("peer").remove(&response.id);
        if let Some(then) = then {
            then(response);
        }
    }

    /// Ask, and say what to do with the answer. Nothing waits: the answer is
    /// handled on this connection's reader thread, so whatever asked is free to
    /// get on with something else — a request waiting on another process must not
    /// hold a worker while it waits.
    pub fn ask(&self, request: &Req, then: impl FnOnce(Resp) + Send + 'static) -> Result<()> {
        self.submit(request, then).map(|_| ())
    }
    fn submit(&self, request: &Req, then: impl FnOnce(Resp) + Send + 'static) -> Result<u32> {
        if self.broken.load(Ordering::SeqCst) {
            return Err(Error::new("the connection to that process has ended"));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        self.waiting.lock().expect("peer").insert(id, Box::new(then));

        // Framed here, sent by this connection's sender along with whatever else
        // has arrived meanwhile.
        FRAME
            .with(|scratch| -> Result<()> {
                let mut framed = scratch.borrow_mut();
                framed.clear();
                put_request(&mut framed, &request.clone().id(id))?;
                self.sending.put(&framed);
                Ok(())
            })
            .inspect_err(|_| {
                self.waiting.lock().expect("peer").remove(&id);
            })
            .map(|_| id)
    }

    /// Ask and wait. For a startup path with nothing else to get on with; a
    /// request being served uses `ask`, or it holds a thread while another
    /// process works.
    pub fn call(&self, request: &Req) -> Result<Resp> {
        self.call_timeout(request, self.timeout)
    }
    pub fn call_timeout(&self, request: &Req, timeout: Duration) -> Result<Resp> {
        // A late reply after timeout must not complete this thread's next call.
        let slot = Arc::new(Slot { answer: Mutex::new(None), arrived: Condvar::new() });
        *slot.answer.lock().expect("slot") = None;
        let waiting = Arc::clone(&slot);
        let id = self.submit(request, move |response| {
            *waiting.answer.lock().expect("slot") = Some(response);
            waiting.arrived.notify_one();
        })?;

        let mut answer = slot.answer.lock().expect("slot");
        while answer.is_none() {
            let (next, timed_out) = slot.arrived.wait_timeout(answer, timeout).expect("slot");
            answer = next;
            if answer.is_none() && timed_out.timed_out() {
                self.waiting.lock().expect("peer").remove(&id);
                self.disconnect();
                return Err(Error::new(format!("no answer to {} in time", request.op)));
            }
        }
        Ok(answer.take().expect("an answer"))
    }
}

/// The connections a process holds to other processes: a fan of them per peer,
/// opened when first needed and used in turn. One connection would put every
/// request to that peer through one read loop on the far side.
pub struct Peers {
    config: Messaging,
    timeout: Duration,
    open: Mutex<HashMap<String, Vec<Arc<Peer>>>>,
    turn: AtomicU32,
}

impl Peers {
    pub fn new(config: Messaging, timeout: Duration) -> Peers {
        Peers { config, timeout, open: Mutex::new(HashMap::new()), turn: AtomicU32::new(0) }
    }

    /// The connection to `identity`, opening it if this is the first need for it.
    /// `find` is only called then, since it costs a lookup at KS.
    pub fn get(
        &self,
        identity: &str,
        find: impl FnOnce() -> Result<SocketAddr>,
    ) -> Result<Arc<Peer>> {
        self.get_refresh(identity, |_| find())
    }
    pub fn get_refresh(
        &self,
        identity: &str,
        find: impl FnOnce(bool) -> Result<SocketAddr>,
    ) -> Result<Arc<Peer>> {
        let turn = self.turn.fetch_add(1, Ordering::Relaxed) as usize;
        let mut reconnect = false;
        if let Some(fan) = self.open.lock().expect("peers").get(identity) {
            reconnect = true;
            let peer = &fan[turn % fan.len()];
            if !peer.broken.load(Ordering::SeqCst) {
                return Ok(Arc::clone(peer));
            }
        }

        let address = find(reconnect)?;
        let mut config = self.config.clone();
        config.expected_peer = Some(identity.into());
        let mut fan = Vec::with_capacity(self.config.peer_conns.max(1));
        for _ in 0..self.config.peer_conns.max(1) {
            fan.push(Peer::connect(address, &config, self.timeout)?);
        }
        let peer = Arc::clone(&fan[turn % fan.len()]);
        self.open.lock().expect("peers").insert(identity.to_string(), fan);
        Ok(peer)
    }
}
