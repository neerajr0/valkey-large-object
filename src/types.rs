//! Shared types used across storage and transport layers.

use std::alloc::Layout;

/// Raw kernel-pinned memory slot. Fixed array, registered with IORING_REGISTER_BUFFERS
/// and fi_mr_reg. Never moves, never reallocated. Lives for module lifetime.
/// Transport layer uses this directly for DMA operations.
///
/// Memory is 4KB-aligned (required for O_DIRECT) and owned via Box<[u8]>.
pub struct PinnedBuffer {
    mem: Box<[u8]>,
}

impl PinnedBuffer {
    /// Allocate a new 4KB-aligned, zeroed buffer of `size` bytes.
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("invalid buffer layout");
        // SAFETY: layout is valid (size > 0, alignment is power of 2).
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        // SAFETY: ptr is valid, aligned, zeroed, and allocated with the given layout.
        // We transfer ownership to Box which will dealloc with the global allocator.
        // Note: Box::from_raw uses Layout::for_value which may differ from our layout.
        // We use Vec::from_raw_parts to preserve the exact allocation.
        let mem = unsafe { Vec::from_raw_parts(ptr, size, size) }.into_boxed_slice();
        Self { mem }
    }

    /// Raw pointer to buffer memory. Used by io_uring SQE submission and EFA DMA.
    pub fn as_ptr(&self) -> *const u8 {
        self.mem.as_ptr()
    }

    /// Mutable raw pointer. Used by io_uring ReadFixed (kernel writes into this).
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.mem.as_ptr() as *mut u8
    }

    /// Buffer length in bytes.
    pub fn len(&self) -> usize {
        self.mem.len()
    }

    /// Slice view of the buffer contents.
    pub fn as_slice(&self) -> &[u8] {
        &self.mem
    }

    /// Mutable slice view.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.mem
    }
}
