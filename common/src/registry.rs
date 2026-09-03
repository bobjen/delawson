//! What a process tells KS about itself, and what KS tells everyone else.
//!
//! KS is the only thing in a cluster with a known address. Everything else is
//! handed a port by whatever starts it, announces itself here, and is found this
//! way.

use crate::config::ServiceKind;
use crate::wire::{Arg, Op, Request, Response, Status, StructWriter, structure};
use crate::{Error, Result};
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub identity: String,
    pub kind: ServiceKind,
    pub address: SocketAddr,
    pub domain: Vec<String>,
    pub media_class: Option<String>,
}

impl Peer {
    /// The registration request this peer sends to KS.
    pub fn request(&self) -> Request {
        let mut payload = StructWriter::new();
        payload.text(&self.identity);
        payload.text(self.kind.as_str());
        payload.text(&self.address.to_string());
        payload.text(&self.domain.join("/"));
        payload.text(self.media_class.as_deref().unwrap_or_default());
        Request::new(Op::Register).structure(payload)
    }

    pub fn from_args(args: &[Arg]) -> Result<Peer> {
        let (mut payload, rest) = structure(args, Op::Register, "a peer registration")?;
        if !rest.is_empty() {
            return Err(Error::new("a peer registration has trailing arguments"));
        }
        let identity = payload.text()?;
        let kind = ServiceKind::parse(&payload.text()?)?;
        let address = payload.text()?;
        let domain = payload.text()?;
        let media_class = payload.text()?;
        payload.done()?;
        Ok(Peer {
            identity,
            kind,
            address: address
                .parse()
                .map_err(|_| Error::new(format!("{address:?} is not an address")))?,
            domain: domain.split('/').map(str::to_string).collect(),
            media_class: Some(media_class).filter(|s| !s.is_empty()),
        })
    }

    /// One line, as `laws registry` prints it.
    pub fn line(&self) -> String {
        format!(
            "{:<6} {:<3} {} {}{}",
            self.identity,
            self.kind,
            self.address,
            self.domain.join("/"),
            self.media_class.as_ref().map(|m| format!(" {m}")).unwrap_or_default()
        )
    }
}

/// Every peer KS knows, one text argument each.
pub fn response(peers: impl IntoIterator<Item = Peer>) -> Response {
    Response {
        id: 0,
        status: Status::Ok,
        args: peers.into_iter().map(|peer| Arg::Text(peer.line())).collect(),
    }
}
