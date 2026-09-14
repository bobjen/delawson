//! What a process tells KS about itself, and what KS tells everyone else.
//!
//! KS is the only thing in a cluster with a known address. Everything else is
//! handed a port by whatever starts it, announces itself here, and is found this
//! way.

use crate::config::ServiceKind;
use crate::wire::{Arg, Op, Req, Resp, Status, StructWriter, structure};
use crate::{Error, Result};
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub identity: String,
    pub kind: ServiceKind,
    pub address: SocketAddr,
    pub domain: Vec<String>,
    pub media_class: Option<String>,
    pub ring_identity: Option<crate::ring::RingIdentity>,
}

impl Peer {
    pub fn check_ring(
        &self,
        expected: Option<&crate::ring::RingIdentity>,
        ignore: bool,
    ) -> Result<()> {
        if !ignore
            && matches!(self.kind, ServiceKind::Ks | ServiceKind::Sn | ServiceKind::Dn)
            && (self.ring_identity.is_none() || self.ring_identity.as_ref() != expected)
        {
            return Err(Error::new("registration ring identity mismatch"));
        }
        Ok(())
    }

    /// The registration request this peer sends to KS.
    pub fn request(&self) -> Req {
        let mut payload = StructWriter::new();
        payload.text(&self.identity);
        payload.text(self.kind.as_str());
        payload.text(&self.address.to_string());
        payload.text(&self.domain.join("/"));
        payload.text(self.media_class.as_deref().unwrap_or_default());
        payload.text(&self.ring_identity.as_ref().map(ToString::to_string).unwrap_or_default());
        Req::new(Op::Register).structure(payload)
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
        let ring = payload.text()?;
        let ring_identity =
            if ring.is_empty() { None } else { Some(crate::ring::RingIdentity::try_from(ring)?) };
        payload.done()?;
        Ok(Peer {
            ring_identity,
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

/// Every peer KS knows, in one tagged response payload.
pub fn response(peers: impl IntoIterator<Item = Peer>) -> Resp {
    let peers: Vec<_> = peers.into_iter().collect();
    let mut payload = StructWriter::new();
    payload.number_u64(peers.len() as u64);
    for peer in peers {
        payload.text(&peer.line());
    }
    Resp { id: 0, status: Status::Ok, args: vec![payload.finish(Op::RegistryResp)] }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_checks_universal_identity_despite_matching_routing_label() {
        for kind in [ServiceKind::Ks, ServiceKind::Sn, ServiceKind::Dn] {
            let expected = crate::ring::RingIdentity::try_from("01".repeat(64)).unwrap();
            let mut peer = Peer {
                identity: format!("{}0", kind.as_str().to_uppercase()),
                kind,
                address: "127.0.0.1:1".parse().unwrap(),
                domain: vec![],
                media_class: None,
                ring_identity: Some(expected.clone()),
            };
            let roundtrip = Peer::from_args(&peer.request().args).unwrap();
            let accepted = roundtrip.check_ring(Some(&expected), false).is_ok();
            peer.ring_identity =
                Some(crate::ring::RingIdentity::try_from("02".repeat(64)).unwrap());
            let rejected = peer.check_ring(Some(&expected), false).is_err();
            let debug = peer.check_ring(Some(&expected), true).is_ok();
            peer.ring_identity = None;
            let missing = peer.check_ring(None, false).is_err();
            assert!(accepted && rejected && debug && missing);
        }
    }
}
