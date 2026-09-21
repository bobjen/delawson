//! Process-wide fixed-size blocks. The backend currently uses malloc/free.
use crate::{Error, Result};
use std::alloc::{Layout, handle_alloc_error};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::OnceLock;

pub const DEFAULT_BYTES: usize = 64 * 1024;
static GLOBAL: OnceLock<Pool> = OnceLock::new();

pub(crate) fn default_bytes() -> usize {
    DEFAULT_BYTES
}

/// Set the size before the first use. Repeating the same setting is harmless.
pub fn initialize(block_bytes: usize) -> Result<()> {
    let pool = Pool::new(block_bytes)?;
    if GLOBAL.get_or_init(|| pool).block_bytes != block_bytes {
        return Err(Error::new("block pool already initialized with another size"));
    }
    Ok(())
}

/// Keep this reference when allocating repeatedly; initialization is checked here,
/// not in Pool::alloc or Block::drop.
#[inline]
pub fn global() -> &'static Pool {
    GLOBAL.get_or_init(|| Pool { block_bytes: DEFAULT_BYTES })
}

pub struct Pool {
    block_bytes: usize,
}

impl Pool {
    fn new(block_bytes: usize) -> Result<Self> {
        if block_bytes == 0 || Layout::from_size_align(block_bytes, 1).is_err() {
            return Err(Error::new("block_bytes must be positive and fit in isize"));
        }
        Ok(Self { block_bytes })
    }

    #[inline]
    pub fn block_bytes(&self) -> usize {
        self.block_bytes
    }

    /// Allocate uninitialized bytes with malloc's fundamental-type alignment.
    /// Allocation failure follows Rust's allocation error handler.
    #[inline]
    pub fn alloc(&self) -> Block {
        // SAFETY: the pool size is nonzero and fits a Rust allocation.
        let ptr = unsafe { libc::malloc(self.block_bytes) }.cast::<MaybeUninit<u8>>();
        let ptr = NonNull::new(ptr).unwrap_or_else(|| {
            handle_alloc_error(Layout::from_size_align(self.block_bytes, 1).unwrap())
        });
        Block { ptr, len: self.block_bytes }
    }
}

/// Exclusive ownership of one block. Drop returns it to the pool backend.
/// The bytes remain MaybeUninit until the caller has initialized them.
pub struct Block {
    ptr: NonNull<MaybeUninit<u8>>,
    len: usize,
}

// SAFETY: the allocation is exclusively owned; moving it transfers that ownership.
unsafe impl Send for Block {}
// SAFETY: shared access only exposes shared bytes or a raw pointer. Mutation
// through the raw pointer remains the caller's unsafe obligation.
unsafe impl Sync for Block {}

impl Block {
    #[inline]
    pub fn as_slice(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: malloc supplies len live bytes, and self keeps them allocated.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: the mutable borrow provides exclusive access to the allocation.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr().cast()
    }
}

/// Explicit alternative to dropping the owning handle.
#[inline]
pub fn free(block: Block) {
    drop(block);
}

impl Drop for Block {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: this pointer came from malloc and ownership prevents double free.
        unsafe { libc::free(self.ptr.as_ptr().cast()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_are_independent_and_can_be_freed_on_another_thread() {
        for size in [DEFAULT_BYTES, 4 * 1024 * 1024] {
            let pool = Pool::new(size).unwrap();
            let mut first = pool.alloc();
            let mut second = pool.alloc();
            assert_ne!(first.as_ptr(), second.as_ptr());
            first.as_mut_slice().fill(MaybeUninit::new(0x5a));
            second.as_mut_slice().fill(MaybeUninit::new(0xa5));
            std::thread::spawn(move || {
                assert_eq!(first.as_slice().len(), size);
                for byte in first.as_slice() {
                    // SAFETY: every byte was initialized before moving the block.
                    assert_eq!(unsafe { byte.assume_init() }, 0x5a);
                }
                free(first);
            })
            .join()
            .unwrap();
            for byte in second.as_slice() {
                // SAFETY: every byte was initialized above.
                assert_eq!(unsafe { byte.assume_init() }, 0xa5);
            }
        }
    }

    #[test]
    fn rejects_invalid_sizes() {
        assert!(Pool::new(0).is_err());
        assert!(Pool::new(usize::MAX).is_err());
    }

    #[test]
    fn global_size_cannot_change() {
        initialize(DEFAULT_BYTES).unwrap();
        initialize(DEFAULT_BYTES).unwrap();
        assert!(initialize(4 * 1024 * 1024).is_err());
        assert_eq!(global().block_bytes(), DEFAULT_BYTES);
        free(global().alloc());
    }
}
