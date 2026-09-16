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

use talc::Span;

/// A contiguous registered memory region.
pub struct Segment {
    /// Base pointer (4KB-aligned, allocated via ValkeyAlloc).
    pub base: *mut u8,
    /// Total size in bytes.
    pub size: usize,
    /// Index into the sparse iovec table (io_uring ReadFixed/WriteFixed) and
    /// into the SegmentPool's `segments: Vec<Option<Segment>>` vector.
    /// Write-once at creation; immutable for the segment's lifetime (holes model).
    pub iovec_index: u16,
    /// The exact Span returned by talc.claim() at creation time.
    /// Required for talc.truncate() during drain — talc word-aligns the span
    /// inward and truncate requires the exact recorded value.
    pub claim_span: Span,
    /// Number of live allocations from this segment.
    /// +1 on talc alloc, -1 on talc free. When 0 + draining → safe to release.
    pub refcount: AtomicU32,
    /// When true, no new promotions target this segment. Set during shrink/drain.
    /// GET handlers check this before acquiring Arc<ObjectContext> on this segment;
    /// if draining they defer to NVMe so no new Arc refs are acquired.
    pub draining: AtomicBool,
}

impl Segment {
    /// Allocate a new segment via ValkeyAlloc (alloc_zeroed).
    /// Visible in Valkey's used_memory immediately.
    /// `iovec_index` and `claim_span` are set after claiming in talc (see SegmentPool::new / expand).
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("invalid segment layout");
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!base.is_null(), "segment allocation failed (out of memory)");

        Self {
            base,
            size,
            iovec_index: 0,            // set by caller after talc.claim()
            claim_span: Span::empty(), // set by caller after talc.claim()
            refcount: AtomicU32::new(0),
            draining: AtomicBool::new(false),
        }
    }

    /// Check if safe to release (draining + no live allocations).
    pub fn is_releasable(&self) -> bool {
        // Acquire pairs with Release in dec_ref: when we see refcount == 0,
        // all buffer writes from prior users are guaranteed visible, making
        // it safe to deallocate the segment.
        self.draining.load(Ordering::Acquire) && self.refcount.load(Ordering::Acquire) == 0
    }

    pub fn inc_ref(&self) {
        // Relaxed is fine since we are claiming the segment before doing any work,
        // so there are no prior writes that need to be visible to others.
        self.refcount.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ref(&self) {
        // Release ensures any data written to this segment's buffers is visible
        // before another thread sees refcount == 0 and deallocates the segment.
        self.refcount.fetch_sub(1, Ordering::Release);
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
