//! Shared types used across storage and transport layers.
//!
//! PoolBuffer is the one type both storage and transport need — it lives here
//! to avoid circular dependencies.

/// Shared buffer descriptor — both storage and transport speak this language.
/// Pool-allocated, 4KB-aligned, stable for module lifetime.
/// Registered with both io_uring (IORING_REGISTER_BUFFERS) and EFA (fi_mr_reg).
pub struct PoolBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    /// Registered buffer index — set at pool creation, used for ReadFixed/WriteFixed.
    /// Eliminates the O(N) buf_index_for() linear scan on every I/O op.
    pub idx: u16,
}

// SAFETY: PoolBuffer is a descriptor. The underlying memory is pool-allocated,
// page-aligned, and never moved or reallocated for the module's lifetime.
// Access is synchronized by the pool free-list (only one owner at a time).
unsafe impl Send for PoolBuffer {}
unsafe impl Sync for PoolBuffer {}
