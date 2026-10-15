//! Fixed-size pools and index structures.
use std::ptr::NonNull;

mod segmented;
pub use segmented::*;
mod linear;
pub use linear::{LinearEntry, LinearHash};
mod bitmap;
pub use bitmap::*;
#[cfg(test)]
mod concurrency;
mod locked;
pub use locked::*;

const NONE: u32 = u32::MAX;

#[derive(Debug)]
pub struct Error(String);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Initialization hook for fresh slots. Byte arrays intentionally do nothing.
/// The pool calls this once when it first assigns raw storage to a slot; it does
/// not reset items when their blocks are returned or reused.
///
/// # Safety
/// `init` must be safe to call once on a fresh slot and must prepare any typed
/// state that callers expect to read. It must not panic.
pub unsafe trait PoolItem: Sized {
    /// # Safety
    /// `slot` is aligned writable storage for one fresh `Self` value.
    unsafe fn init(_slot: *mut Self) {}
}

unsafe impl<const N: usize> PoolItem for [u8; N] {}

struct Mapping {
    raw: NonNull<u8>,
    base: NonNull<u8>,
    len: usize,
}

impl Mapping {
    fn new(bytes: usize, align: usize) -> Result<Self> {
        let len = bytes
            .checked_add(align - 1)
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or_else(|| Error::new("block pool mapping is too large"))?;
        // Reserve address space now, while leaving physical backing to first touch.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(Error::new(format!(
                "reserve block pool: {}",
                std::io::Error::last_os_error()
            )));
        }
        let Some(raw) = NonNull::new(raw.cast::<u8>()) else {
            // Rust references cannot represent a mapping at address zero.
            unsafe { libc::munmap(raw, len) };
            return Err(Error::new("block pool mapped at address zero"));
        };
        let offset = raw.as_ptr().align_offset(align);
        // SAFETY: len includes align - 1 bytes of alignment slack.
        let base = unsafe { NonNull::new_unchecked(raw.as_ptr().add(offset)) };
        Ok(Self { raw, base, len })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this owns the original mapping, including alignment slack.
        unsafe { libc::munmap(self.raw.as_ptr().cast(), self.len) };
    }
}
