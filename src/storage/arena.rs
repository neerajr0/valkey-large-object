//! Arena-based buffer pool (Approach B).
//!
//! A single large mmap'd segment registered as one iovec with io_uring.
//! Sub-allocation via talc arena allocator with 4KB alignment.
//! ReadFixed uses buf_index=0 with addr=segment_base+offset.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Mutex;

use talc::{ClaimOnOom, Span, Talc};

use super::engine::PinnedBuffer;

/// Arena-backed buffer pool. One contiguous mmap'd region, sub-allocated by talc.
pub struct ArenaPool {
    /// Base pointer of the mmap'd region (4KB-aligned, page-locked).
    base: *mut u8,
    /// Total size of the mmap'd region.
    total_size: usize,
    /// Size of each allocation (fixed, like BufferPool).
    buf_size: usize,
    /// The talc allocator instance, protected by mutex.
    allocator: Mutex<Talc<ClaimOnOom>>,
    /// Span covering the entire mmap region.
    _span: Span,
    /// Leaked PinnedBuffer backing the single iovec registration.
    /// We need this for the Buffer type's &'static PinnedBuffer requirement.
    /// In arena mode, ALL Buffers share this single PinnedBuffer (buf_index=0).
    pinned_backing: &'static PinnedBuffer,
}

// SAFETY: ArenaPool's mmap memory is stable for module lifetime and accessed through Mutex.
unsafe impl Send for ArenaPool {}
unsafe impl Sync for ArenaPool {}

impl ArenaPool {
    /// Create a new ArenaPool with `total_size` bytes mmap'd.
    /// `buf_size` is the fixed allocation size per buffer (must be 4KB-aligned).
    pub fn new(buf_size: usize, total_size: usize) -> Self {
        assert!(buf_size >= 4096 && buf_size % 4096 == 0);
        assert!(total_size >= buf_size);

        // Allocate via ValkeyAlloc (global allocator) so used_memory tracks it.
        // ValkeyAlloc → zmalloc → jemalloc → mmap internally for large allocs.
        // Returns contiguous, page-aligned memory for sizes > ~2MB.
        let layout = Layout::from_size_align(total_size, 4096)
            .expect("invalid arena layout");
        // SAFETY: layout is valid (total_size > 0, alignment is power of 2).
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!base.is_null(), "ValkeyAlloc failed for arena segment");

        // Create the talc span and allocator.
        // SAFETY: base is valid mmap'd memory of total_size bytes.
        let span = Span::from_base_size(base, total_size);

        // SAFETY: span is valid (covers our mmap'd memory).
        let talc = Talc::new(unsafe { ClaimOnOom::new(span) });

        // Create a single PinnedBuffer that represents the entire mmap region.
        // This is used for io_uring registration (one iovec entry = the whole arena).
        // We leak it to get &'static lifetime.
        // SAFETY: base is valid for total_size bytes and will be leaked (never freed by Box).
        let pinned_backing = Box::leak(Box::new(unsafe { PinnedBuffer::from_raw(base, total_size) }));

        Self {
            base,
            total_size,
            buf_size,
            allocator: Mutex::new(talc),
            _span: span,
            pinned_backing,
        }
    }

    /// Allocate a buffer from the arena. Returns None if arena is full.
    pub fn get(&self) -> Option<ArenaBuffer> {
        let layout = Layout::from_size_align(self.buf_size, 4096).ok()?;
        let mut alloc = self.allocator.lock().unwrap();
        // SAFETY: Layout is valid (checked above). talc returns aligned memory from our span.
        let ptr = unsafe { alloc.malloc(layout) };
        match ptr {
            Ok(nn) => {
                let offset = nn.as_ptr() as usize - self.base as usize;
                Some(ArenaBuffer {
                    ptr: nn.as_ptr(),
                    offset,
                    size: self.buf_size,
                })
            }
            Err(_) => None,
        }
    }

    /// Free a buffer back to the arena.
    pub fn put_back(&self, buf: &ArenaBuffer) {
        let layout = Layout::from_size_align(self.buf_size, 4096).unwrap();
        let mut alloc = self.allocator.lock().unwrap();
        // SAFETY: ptr was allocated by this talc instance with the same layout.
        unsafe {
            alloc.free(NonNull::new_unchecked(buf.ptr), layout);
        }
    }

    /// Get the base pointer (for io_uring iovec registration).
    pub fn base_ptr(&self) -> *mut u8 {
        self.base
    }

    /// Get the total arena size.
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Get the single PinnedBuffer representing the entire arena (for iovec registration).
    pub fn pinned_backing(&self) -> &'static PinnedBuffer {
        self.pinned_backing
    }

    /// Get buf_size.
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }
}

impl Drop for ArenaPool {
    fn drop(&mut self) {
        // SAFETY: Deallocating memory we allocated via std::alloc::alloc_zeroed.
        let layout = Layout::from_size_align(self.total_size, 4096).unwrap();
        unsafe {
            std::alloc::dealloc(self.base, layout);
        }
    }
}

/// An allocated region within the arena.
pub struct ArenaBuffer {
    pub ptr: *mut u8,
    pub offset: usize,
    pub size: usize,
}

// ─── Adapter: ArenaBuffer → Buffer ──────────────────────────────────────────
//
// The existing pipeline uses Buffer (which wraps &'static PinnedBuffer + idx).
// For arena mode, we create a Buffer with buf_index=0 pointing to the arena's
// single registered iovec. The ptr() will return the sub-allocated address.
// io_uring ReadFixed will use buf_index=0 with the ptr address.
//
// However, Buffer's ptr() returns pinned.as_mut_ptr() which is the segment base,
// not the sub-allocated address. We need a different approach.
//
// Solution: We'll use a thin wrapper in the storage engine that handles both modes.
// For arena mode, the uring submission will use the offset within the single iovec.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_alloc_and_free() {
        let pool = ArenaPool::new(4096, 4096 * 10);
        let buf = pool.get().expect("alloc should succeed");
        assert_eq!(buf.size, 4096);
        assert!(buf.offset < 4096 * 10);
        pool.put_back(&buf);
    }

    #[test]
    fn test_arena_exhaustion() {
        // 2 buffers worth of space (with some overhead for talc metadata)
        let pool = ArenaPool::new(4096, 4096 * 4);
        let mut bufs = Vec::new();
        // Should get at least 1 buffer, maybe 2-3 depending on talc overhead
        for _ in 0..10 {
            match pool.get() {
                Some(b) => bufs.push(b),
                None => break,
            }
        }
        assert!(!bufs.is_empty());
        let count = bufs.len();
        // Free all
        for b in &bufs {
            pool.put_back(b);
        }
        // Should be able to allocate the same count again
        for _ in 0..count {
            assert!(pool.get().is_some());
        }
    }

    #[test]
    fn test_arena_alignment() {
        let pool = ArenaPool::new(4096, 4096 * 20);
        for _ in 0..5 {
            let buf = pool.get().unwrap();
            assert_eq!(buf.ptr as usize % 4096, 0, "allocation not 4KB-aligned");
            pool.put_back(&buf);
        }
    }
}
