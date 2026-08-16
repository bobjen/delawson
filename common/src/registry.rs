//! What a process tells KS about itself, and what KS tells everyone else.
//!
//! KS is the only thing in a cluster with a known address. Everything else is
//! handed a port by whatever starts it, announces itself here, and is found this
//! way.

use crate::config::ServiceKind;
use crate::wire::{Arg, Op, Request, Response, Status};
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
        Request::new(Op::Register)
            .text(&self.identity)
            .text(self.kind.as_str())
            .text(self.address.to_string())
            .text(self.domain.join("/"))
            .text(self.media_class.clone().unwrap_or_default())
    }

    pub fn from_args(args: &[Arg]) -> Result<Peer> {
        let text = |i: usize| match args.get(i) {
            Some(Arg::Text(text)) => Ok(text.as_str()),
            _ => Err(Error::new("a peer is five text arguments")),
        };
        let address = text(2)?;
        Ok(Peer {
            identity: text(0)?.to_string(),
            kind: ServiceKind::parse(text(1)?)?,
            address: address
                .parse()
                .map_err(|_| Error::new(format!("{address:?} is not an address")))?,
            domain: text(3)?.split('/').map(str::to_string).collect(),
            media_class: Some(text(4)?.to_string()).filter(|s| !s.is_empty()),
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
