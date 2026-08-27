//! Segment — a contiguous registered memory region.
//!
//! Allocated via std::alloc::alloc_zeroed (ValkeyAlloc/zmalloc) so Valkey's
//! used_memory correctly reflects the allocation. Registered with io_uring
//! (one iovec entry) and EFA (one fi_mr_reg call).
//!
//! talc sub-allocates within segments. Individual talc.malloc/free calls
//! produce zero change to used_memory — only segment creation/destruction does.

use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// A contiguous registered memory region.
pub struct Segment {
    /// Base pointer (4KB-aligned, allocated via ValkeyAlloc).
    pub base: *mut u8,
    /// Total size in bytes.
    pub size: usize,
    /// Index in the io_uring iovec registration array (for ReadFixed/WriteFixed).
    pub iovec_index: u16,
    /// Number of live allocations from this segment.
    /// +1 on talc alloc, -1 on talc free. When 0 + draining → safe to release.
    pub refcount: AtomicU32,
    /// When true, no new allocations from this segment. Set during shrink/drain.
    pub draining: AtomicBool,
}

impl Segment {
    /// Allocate a new segment via ValkeyAlloc (alloc_zeroed).
    /// Visible in Valkey's used_memory immediately.
    /// Allocate a new segment. iovec_index is assigned later via append_iovec().
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("invalid segment layout");
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!base.is_null(), "segment allocation failed (out of memory)");

        Self {
            base,
            size,
            iovec_index: 0, // assigned by append_iovec() after creation
            refcount: AtomicU32::new(0),
            draining: AtomicBool::new(false),
        }
    }

    /// Check if safe to release (draining + no live allocations).
    pub fn is_releasable(&self) -> bool {
        self.draining.load(Ordering::Acquire) && self.refcount.load(Ordering::Acquire) == 0
    }

    pub fn inc_ref(&self) {
        self.refcount.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ref(&self) {
        self.refcount.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get iovec for io_uring registration.
    pub fn iovec(&self) -> libc::iovec {
        libc::iovec {
            iov_base: self.base as *mut libc::c_void,
            iov_len: self.size,
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, 4096).expect("Segment layout");
        unsafe { std::alloc::dealloc(self.base, layout) };
    }
}

// SAFETY: Segment memory is stable for its lifetime. Accessed through Mutex<Talc>.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}
