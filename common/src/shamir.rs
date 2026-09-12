//! Shamir sharing of 256-bit generated secrets, bytewise in GF(256).
//! Field polynomial x^8+x^4+x^3+x+1 (0x11b); x=0 is reserved for the secret.
use crate::{Error, Result};
use sha2::{Digest, Sha256};
use std::io::Read;

pub fn random_key() -> Result<[u8; 32]> {
    let mut key = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut key)?;
    Ok(key)
}

fn mul(mut a: u8, mut b: u8) -> u8 {
    let mut out = 0;
    for _ in 0..8 {
        out ^= a & 0u8.wrapping_sub(b & 1);
        a = (a << 1) ^ (0x1b & 0u8.wrapping_sub(a >> 7));
        b >>= 1;
    }
    out
}
fn inverse(a: u8) -> u8 {
    let mut out = 1;
    for bit in (0..8).rev() {
        out = mul(out, out);
        if (254 >> bit) & 1 != 0 {
            out = mul(out, a);
        }
    }
    out
}

#[derive(Clone)]
pub struct Share {
    pub set: [u8; 32],
    pub threshold: u8,
    pub x: u8,
    pub value: [u8; 32],
}

pub fn split(secret: &[u8; 32], threshold: u8, count: u8) -> Result<Vec<Share>> {
    if threshold == 0 || count < threshold {
        return Err(Error::new("invalid share threshold/count"));
    }
    let set = random_key()?;
    let mut coefficients = vec![*secret];
    for _ in 1..threshold {
        coefficients.push(random_key()?);
    }
    Ok((1..=count)
        .map(|x| {
            let mut value = [0; 32];
            for j in 0..32 {
                for c in coefficients.iter().rev() {
                    value[j] = mul(value[j], x) ^ c[j];
                }
            }
            Share { set, threshold, x, value }
        })
        .collect())
}

pub fn recover(shares: &[Share]) -> Result<[u8; 32]> {
    let first = shares.first().ok_or_else(|| Error::new("no shares"))?;
    if first.threshold == 0 || shares.len() < first.threshold as usize {
        return Err(Error::new("insufficient shares"));
    }
    let mut seen = [false; 256];
    for s in shares {
        if s.set != first.set || s.threshold != first.threshold || s.x == 0 || seen[s.x as usize] {
            return Err(Error::new("mixed, duplicate, or invalid shares"));
        }
        seen[s.x as usize] = true;
    }
    let basis = &shares[..first.threshold as usize];
    let evaluate = |x| {
        let mut value = [0; 32];
        for (i, s) in basis.iter().enumerate() {
            let mut weight = 1;
            for (j, other) in basis.iter().enumerate() {
                if i != j {
                    weight = mul(weight, mul(x ^ other.x, inverse(s.x ^ other.x)));
                }
            }
            for k in 0..32 {
                value[k] ^= mul(s.value[k], weight);
            }
        }
        value
    };
    for s in &shares[first.threshold as usize..] {
        if evaluate(s.x) != s.value {
            return Err(Error::new("inconsistent shares"));
        }
    }
    Ok(evaluate(0))
}

pub fn verifier(key: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"DeLawson unlock verifier v1\0");
    hash.update(key);
    hash.finalize().into()
}
pub fn matches(key: &[u8; 32], expected: &[u8; 32]) -> bool {
    verifier(key).iter().zip(expected).fold(0u8, |d, (a, b)| d | (a ^ b)) == 0
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn unhex<const N: usize>(text: &str) -> Result<[u8; N]> {
    let text = text.trim();
    if text.len() != N * 2 || !text.is_ascii() {
        return Err(Error::new("invalid hex length or encoding"));
    }
    let mut out = [0; N];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::new("invalid hex"))?;
    }
    Ok(out)
}
impl Share {
    pub fn encode(&self) -> String {
        format!("dls1:{}:{}:{}:{}", hex(&self.set), self.threshold, self.x, hex(&self.value))
    }
    pub fn decode(text: &str) -> Result<Self> {
        let parts: Vec<_> = text.trim().split(':').collect();
        if parts.len() != 5 || parts[0] != "dls1" {
            return Err(Error::new("invalid share format"));
        }
        Ok(Self {
            set: unhex(parts[1])?,
            threshold: parts[2].parse().map_err(|_| Error::new("invalid threshold"))?,
            x: parts[3].parse().map_err(|_| Error::new("invalid share index"))?,
            value: unhex(parts[4])?,
        })
    }
}
