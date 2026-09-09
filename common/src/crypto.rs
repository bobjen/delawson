use crate::ids::{ExtentId, SnId};
use crate::time::TimeSec;
use crate::{Error, Result};
use aegis::aegis128x2::Aegis128X2;
use hkdf::Hkdf;
use sha2::Sha256;

/// The initial master key, 256 bits. KS assigns master keys effective times;
/// this key is the initial epoch.
pub const MASTER_KEY: [u8; 32] = [
    0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
    0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
];

/// SnMasterKey request AD, for SN keys derived from a master-key epoch.
pub const MASTER_KEY_AD: &[u8] = b"sn-master-key";

pub fn sn_key(master_key: &[u8; 32], sn_id: SnId, day_start: TimeSec) -> [u8; 32] {
    let mut info = [0u8; 16];
    info[..8].copy_from_slice(&sn_id.get().to_le_bytes());
    info[8..].copy_from_slice(&day_start.number().to_le_bytes());
    let hk = Hkdf::<Sha256>::new(None, master_key);
    let mut okm = [0u8; 32];
    hk.expand(&info, &mut okm).expect("32 bytes is valid for HKDF-SHA256");
    okm
}

pub fn extent_key(
    sn_daily_key: &[u8; 32],
    sn_id: SnId,
    extent_id: ExtentId,
    ts: TimeSec,
) -> [u8; 32] {
    let mut info = [0u8; 20];
    info[..8].copy_from_slice(&sn_id.get().to_le_bytes());
    info[8..12].copy_from_slice(&extent_id.get().to_le_bytes());
    info[12..20].copy_from_slice(&ts.number().to_le_bytes());
    let hk = Hkdf::<Sha256>::new(None, sn_daily_key);
    let mut okm = [0u8; 32];
    hk.expand(&info, &mut okm).expect("32 bytes is valid for HKDF-SHA256");
    okm
}

pub fn derive_block_secrets(extent_key: &[u8; 32], offset: u64) -> ([u8; 16], [u8; 16]) {
    let mut info = [0u8; 8];
    info.copy_from_slice(&offset.to_le_bytes());
    let hk = Hkdf::<Sha256>::new(None, extent_key);
    let mut okm = [0u8; 32];
    hk.expand(&info, &mut okm).expect("32 bytes is valid for HKDF-SHA256");
    let mut key = [0u8; 16];
    let mut nonce = [0u8; 16];
    key.copy_from_slice(&okm[..16]);
    nonce.copy_from_slice(&okm[16..32]);
    (key, nonce)
}

pub fn encrypt_block(
    key: &[u8; 16],
    nonce: &[u8; 16],
    ad: &[u8],
    body: &[u8],
) -> (Vec<u8>, [u8; 16]) {
    let cipher = Aegis128X2::<16>::new(key, nonce);
    cipher.encrypt(body, ad)
}

pub fn decrypt_block(
    key: &[u8; 16],
    nonce: &[u8; 16],
    ad: &[u8],
    sealed: &[u8],
    tag: &[u8; 16],
) -> Result<Vec<u8>> {
    let cipher = Aegis128X2::<16>::new(key, nonce);
    cipher.decrypt(sealed, tag, ad).map_err(|e| Error::new(format!("decryption failed: {e}")))
}
