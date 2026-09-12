//! Local recovery ceremony. Secret contents never appear in command arguments.
use common::{Error, Result, shamir};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

fn write(path: &Path, text: &str) -> Result<()> {
    let mut file =
        std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
    writeln!(file, "{text}")?;
    file.sync_all()?;
    Ok(())
}
pub fn generate(dir: &Path, direct: bool, threshold: u8, count: u8) -> Result<()> {
    let key = shamir::random_key()?;
    let shares = if direct { Vec::new() } else { shamir::split(&key, threshold, count)? };
    std::fs::DirBuilder::new().mode(0o700).create(dir)?;
    write(&dir.join("verifier"), &shamir::hex(&shamir::verifier(&key)))?;
    if direct {
        write(&dir.join("unlock-key"), &shamir::hex(&key))?;
    }
    for share in shares {
        write(&dir.join(format!("share-{:02}", share.x)), &share.encode())?;
    }
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}
pub fn key(key_file: &Option<PathBuf>, shares: &[PathBuf]) -> Result<[u8; 32]> {
    match (key_file, shares.is_empty()) {
        (Some(path), true) => shamir::unhex(&std::fs::read_to_string(path)?),
        (None, false) => shamir::recover(
            &shares
                .iter()
                .map(|p| shamir::Share::decode(&std::fs::read_to_string(p)?))
                .collect::<Result<Vec<_>>>()?,
        ),
        _ => Err(Error::new("supply either --key-file or --share files")),
    }
}
pub fn reconstruct(shares: &[PathBuf], output: &Path) -> Result<()> {
    write(output, &shamir::hex(&key(&None, shares)?))
}
