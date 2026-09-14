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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_length_and_canonical_hex() {
        let id = RingIdentity::try_from("AB".repeat(64)).unwrap();
        let short = RingIdentity::try_from("ab".repeat(63)).is_err();
        let invalid = RingIdentity::try_from("xy".repeat(64)).is_err();
        assert_eq!(id.to_string(), "ab".repeat(64));
        assert!(short && invalid);
    }
}
