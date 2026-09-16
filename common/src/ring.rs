//! Universe-wide ring identity. Compact KS/SN/DN numbers are only routing labels.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// 512 random bits, encoded as hexadecimal in configuration and JSON metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RingIdentity([u8; 64]);
impl TryFrom<String> for RingIdentity {
    type Error = Error;
    fn try_from(text: String) -> Result<Self> {
        if text.len() != 128 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::new("ring identity must contain exactly 64 hexadecimal bytes"));
        }
        let mut bytes = [0; 64];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
                .map_err(|_| Error::new("invalid ring identity"))?;
        }
        Ok(Self(bytes))
    }
}
impl From<RingIdentity> for String {
    fn from(id: RingIdentity) -> String {
        id.to_string()
    }
}
impl std::fmt::Display for RingIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
/// Validate the ring once, before this stream can carry ordinary requests.
/// Machine TLS (when configured) authenticates the host; this binds its process.
pub(crate) fn connect(
    stream: &mut (impl std::io::Read + std::io::Write),
    config: &crate::config::Messaging,
) -> Result<()> {
    let expected = config.expected_peer.as_ref().and_then(|id| config.ring_identities.get(id));
    let Some(expected) = expected else {
        stream.write_all(&[0])?;
        return Ok(());
    };
    stream.write_all(&[1])?;
    stream.write_all(&expected.0)?;
    stream.flush()?;
    let mut response = [0; 65];
    stream.read_exact(&mut response)?;
    if response[0] != 1 || (!config.ignore_ring_identity && response[1..] != expected.0) {
        return Err(Error::new("connection ring identity mismatch"));
    }
    Ok(())
}
pub(crate) fn accept(
    stream: &mut (impl std::io::Read + std::io::Write),
    config: &crate::config::Messaging,
) -> Result<()> {
    let mut mode = [0];
    stream.read_exact(&mut mode)?;
    if mode[0] == 0 {
        if config.local_ring.is_some() && !config.ignore_ring_identity {
            return Err(Error::new("connection is missing expected ring identity"));
        }
        return Ok(());
    }
    if mode[0] != 1 {
        return Err(Error::new("invalid ring handshake"));
    }
    let mut expected = [0; 64];
    stream.read_exact(&mut expected)?;
    let Some(local) = &config.local_ring else {
        stream.write_all(&[0; 65])?;
        stream.flush()?;
        return Err(Error::new("connection expected a ring at a non-ring endpoint"));
    };
    let accepted = config.ignore_ring_identity || expected == local.0;
    stream.write_all(&[u8::from(accepted)])?;
    stream.write_all(&local.0)?;
    stream.flush()?;
    if !accepted {
        return Err(Error::new("connection ring identity mismatch"));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Stream {
        input: std::io::Cursor<Vec<u8>>,
        output: Vec<u8>,
    }
    impl Stream {
        fn new(input: Vec<u8>) -> Self {
            Self { input: std::io::Cursor::new(input), output: vec![] }
        }
    }
    impl std::io::Read for Stream {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            std::io::Read::read(&mut self.input, bytes)
        }
    }
    impl std::io::Write for Stream {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn new_connection_checks_ring_before_consuming_any_request() {
        let expected = RingIdentity([1; 64]);
        let foreign = RingIdentity([2; 64]);
        let mut config = crate::testing::messaging();
        config.local_ring = Some(expected.clone());
        let hello = [vec![1], expected.0.to_vec(), b"ordinary request".to_vec()].concat();
        let mut same = Stream::new(hello);
        let accepted = accept(&mut same, &config).is_ok();
        let consumed = same.input.position();
        let mut wrong = Stream::new([vec![1], foreign.0.to_vec()].concat());
        let rejected = accept(&mut wrong, &config).is_err();
        config.expected_peer = Some("SN0".into());
        config.ring_identities.insert("SN0".into(), expected);
        let mut server_reply = Stream::new(same.output);
        let client_accepted = connect(&mut server_reply, &config).is_ok();
        let mut swapped = Stream::new([vec![1], foreign.0.to_vec()].concat());
        let client_rejected = connect(&mut swapped, &config).is_err();
        assert!(accepted && rejected && client_accepted && client_rejected);
        assert_eq!(consumed, 65);
    }
    #[test]
    fn non_ring_connections_need_no_reply_and_rings_reject_missing_identity() {
        let mut config = crate::testing::messaging();
        let mut stream = Stream::new(vec![]);
        connect(&mut stream, &config).unwrap();
        let mut accepted = Stream::new(stream.output);
        accept(&mut accepted, &config).unwrap();
        assert!(accepted.output.is_empty());
        config.local_ring = Some(RingIdentity([1; 64]));
        let rejected = accept(&mut Stream::new(vec![0]), &config).is_err();
        config.ignore_ring_identity = true;
        let debug = accept(&mut Stream::new(vec![1; 65]), &config).is_ok();
        assert!(rejected && debug);
    }
    #[test]
    fn exact_length_and_canonical_hex() {
        let id = RingIdentity::try_from("AB".repeat(64)).unwrap();
        let short = RingIdentity::try_from("ab".repeat(63)).is_err();
        let invalid = RingIdentity::try_from("xy".repeat(64)).is_err();
        assert_eq!(id.to_string(), "ab".repeat(64));
        assert!(short && invalid);
    }
}

/// Persistent voting identity of one ring member, independent of its endpoint.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MemberIdentity(String);
impl MemberIdentity {
    pub fn generate() -> Result<Self> {
        use std::io::Read;
        let mut bytes = [0u8; 64];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        Self::try_from(bytes.iter().map(|b| format!("{b:02x}")).collect::<String>())
    }
}
impl TryFrom<String> for MemberIdentity {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Ok(Self(RingIdentity::try_from(value)?.to_string()))
    }
}
impl From<MemberIdentity> for String {
    fn from(value: MemberIdentity) -> String {
        value.0
    }
}
impl std::fmt::Display for MemberIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod member_identity_tests {
    use super::*;
    #[test]
    fn generated_member_ids_are_valid_distinct_and_canonical() {
        let first = MemberIdentity::generate().unwrap();
        let second = MemberIdentity::generate().unwrap();
        let parsed = MemberIdentity::try_from(first.to_string().to_uppercase()).unwrap();
        let short = MemberIdentity::try_from("ab".repeat(63));
        let invalid = MemberIdentity::try_from("zz".repeat(64));
        assert_ne!(first, second);
        assert_eq!(first.to_string().len(), 128);
        assert_eq!(first, parsed);
        assert!(short.is_err());
        assert!(invalid.is_err());
    }
}
