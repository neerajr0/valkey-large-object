//! ObjectContext, StreamingContext, and SegmentBuffer.
//!
//! These are the runtime companions to LoValue. Not serialized — rebuilt on load,
//! evicted independently of commands.
//!
//! - ObjectContext: lives in DRAMPool HashMap, long-lived, complete object.
//! - StreamingContext: lives on a tokio task, short-lived, partial buffer window.
//! - SegmentBuffer: a slice within a registered segment (DRAMPool or NVMePool).
//!
//! NOTE: CRC32c is NOT stored on either context struct. It is a local variable
//! in the tokio task that performs the SET. The task computes the rolling CRC as
//! chunks arrive (`let mut crc: u32 = 0`) and compares against the client-provided
//! value on completion. Neither ObjectContext nor StreamingContext needs CRC state.

use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

// ─── SegmentBuffer ───────────────────────────────────────────────────────────

/// A buffer that is a sub-allocation within a registered segment.
/// Segment-agnostic: works for both DRAMPool and NVMePool segments.
/// `segment_idx` identifies which registered iovec entry (io_uring buf_index).
#[derive(Debug, Clone)]
pub struct SegmentBuffer {
    /// Which segment this slice lives in (local index into the owning pool's segments vec).
    /// NOT the global io_uring iovec index - that is on Segment.iovec_index.
    pub segment_idx: u8,
    /// Byte offset within that segment.
    pub offset: u64,
    /// Size of this buffer allocation.
    pub len: u32,
}

impl super::TryClone for SegmentBuffer {
    fn try_clone(&self) -> Option<Self> {
        let pool = crate::storage::get_dram_pool();
        let new_buf = pool.alloc(self.len as usize)?;
        let src_ptr = pool.buffer_ptr(self);
        let dst_ptr = pool.buffer_ptr(&new_buf);
        // SAFETY: src and dst are non-overlapping regions within pool segment(s).
        unsafe {
            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, self.len as usize);
        }
        Some(new_buf)
    }
}

// ─── Object State ────────────────────────────────────────────────────────────

/// Atomic state for ObjectContext. `#[repr(u8)]` for use with AtomicU8.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    /// All buffers filled, object is servable.
    Ready = 0,
    /// Promotion in progress — NVMe ReadFixed filling buffers.
    Filling = 1,
}

// ─── ObjectContext ───────────────────────────────────────────────────────────

/// Long-lived runtime state for a cached object in DRAMPool.
/// ALL N buffers for the entire object are allocated upfront from DRAMPool segment.
/// Stored in: `RwLock<HashMap<ObjectId, Arc<ObjectContext>>>`
/// Buffers are automatically returned to DRAMPool when the last Arc drops.
#[derive(Debug)]
pub struct ObjectContext {
    /// Ordered chunks. 1 for small objects, N for large.
    pub buffers: Vec<SegmentBuffer>,
    /// Total object size (sum of all buffer lens).
    pub total_len: u64,
    /// Filling→Ready transition via mark_ready() with Release ordering.
    /// Readers use is_ready() with Acquire ordering — guarantees visibility
    /// of the NVMe read data that was written before mark_ready().
    state: AtomicU8,
    /// Chunks completed during promotion (only meaningful when state == Filling).
    chunks_ready: AtomicU32,
    /// Total chunks for this object (used by streaming/chunking path).
    #[allow(dead_code)]
    total_chunks: u32,
}

impl ObjectContext {
    /// Create a new ObjectContext in Ready state (e.g., DRAM-only SET).
    pub fn new_ready(buffers: Vec<SegmentBuffer>, total_len: u64) -> Self {
        Self {
            buffers,
            total_len,
            state: AtomicU8::new(ObjectState::Ready as u8),
            chunks_ready: AtomicU32::new(0),
            total_chunks: 0,
        }
    }

    /// Create a new ObjectContext in Filling state (promotion path).
    pub fn new_filling(buffers: Vec<SegmentBuffer>, total_len: u64, total_chunks: u32) -> Self {
        Self {
            buffers,
            total_len,
            state: AtomicU8::new(ObjectState::Filling as u8),
            chunks_ready: AtomicU32::new(0),
            total_chunks,
        }
    }

    /// Check if the object is fully ready for serving.
    /// Uses Acquire ordering: if this returns true, all data written
    /// before mark_ready() is guaranteed visible to this thread.
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == ObjectState::Ready as u8
    }

    /// Transition from Filling to Ready. Called by the tokio task
    /// after NVMe ReadFixed completes successfully.
    /// Uses Release ordering: all preceding writes (the NVMe read data
    /// in the buffer) are visible to any thread that later sees is_ready() == true.
    pub fn mark_ready(&self) {
        assert!(
            !self.is_ready(),
            "mark_ready called on an already Ready ObjectContext"
        );
        self.state
            .store(ObjectState::Ready as u8, Ordering::Release);
    }

    /// Get the number of chunks ready (contiguous from offset 0).
    pub fn chunks_ready(&self) -> u32 {
        if self.is_ready() {
            self.buffers.len() as u32
        } else {
            self.chunks_ready.load(Ordering::Acquire)
        }
    }

    /// Advance chunks_ready after a batch completes. Called from tokio promotion task.
    pub fn advance_chunks_ready(&self, batch_size: u32) {
        assert!(
            !self.is_ready(),
            "advance_chunks_ready called on Ready ObjectContext"
        );
        self.chunks_ready.fetch_add(batch_size, Ordering::Release);
    }
}

impl Drop for ObjectContext {
    fn drop(&mut self) {
        // Guard: pool may not be initialized in unit tests.
        if let Some(dram_pool) = super::DRAM_POOL.get() {
            for buf in &self.buffers {
                dram_pool.free(buf);
            }
        }
    }
}

impl super::TryClone for ObjectContext {
    /// Deep-copies all buffers into new DRAMPool allocations.
    /// Returns Some(new Ready ObjectContext) on success.
    /// Returns None if object is Filling (incomplete) or pool is full.
    fn try_clone(&self) -> Option<Self> {
        // Cannot copy an object that is still being promoted (buffers incomplete).
        if !self.is_ready() {
            return None;
        }
        let mut new_buffers = Vec::with_capacity(self.buffers.len());
        for buf in &self.buffers {
            new_buffers.push(buf.try_clone()?);
        }
        Some(Self::new_ready(new_buffers, self.total_len))
    }
}

// ─── StreamingContext ────────────────────────────────────────────────────────

/// Short-lived runtime state for a transient I/O operation on NVMePool.
/// Rotating window of X buffers, reused across batches.
/// Owned by a single tokio task — no Arc needed.
/// Buffers are automatically returned to NVMePool on drop.
#[derive(Debug)]
pub struct StreamingContext {
    /// Rotating buffer window (max X = batch size).
    pub buffers: Vec<SegmentBuffer>,
    /// Total object size being transferred.
    pub total_len: u64,
    /// Number of chunks completed.
    pub chunks_completed: u32,
    /// Total chunks needed for the full object.
    pub total_chunks: u32,
}

impl Drop for StreamingContext {
    fn drop(&mut self) {
        // Guard: pool may not be initialized in unit tests.
        if let Some(nvme_pool) = super::NVME_POOL.get() {
            for buf in &self.buffers {
                nvme_pool.free(buf);
            }
        }
    }
}

impl StreamingContext {
    /// Create a new StreamingContext for a transient NVMe I/O operation (GET or SET).
    pub fn new(buffers: Vec<SegmentBuffer>, total_len: u64, total_chunks: u32) -> Self {
        Self {
            buffers,
            total_len,
            chunks_completed: 0,
            total_chunks,
        }
    }

    /// Number of buffers (batch size / pipeline depth).
    pub fn batch_size(&self) -> usize {
        self.buffers.len()
    }

    /// Advance progress after a chunk completes.
    pub fn advance(&mut self) {
        self.chunks_completed += 1;
    }

    /// Check if the entire object has been transferred.
    pub fn is_complete(&self) -> bool {
        self.chunks_completed == self.total_chunks
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_context_ready() {
        let bufs = vec![
            SegmentBuffer {
                segment_idx: 0,
                offset: 0,
                len: 1024,
            },
            SegmentBuffer {
                segment_idx: 0,
                offset: 1024,
                len: 1024,
            },
        ];
        let ctx = ObjectContext::new_ready(bufs, 2048);
        assert!(ctx.is_ready());
        assert_eq!(ctx.chunks_ready(), 2);
    }

    #[test]
    fn test_object_context_filling() {
        let bufs = vec![
            SegmentBuffer {
                segment_idx: 0,
                offset: 0,
                len: 8_000_000,
            },
            SegmentBuffer {
                segment_idx: 0,
                offset: 8_000_000,
                len: 8_000_000,
            },
            SegmentBuffer {
                segment_idx: 1,
                offset: 0,
                len: 8_000_000,
            },
        ];
        let ctx = ObjectContext::new_filling(bufs, 24_000_000, 3);
        assert!(!ctx.is_ready());
        assert_eq!(ctx.chunks_ready(), 0);

        ctx.advance_chunks_ready(2);
        assert_eq!(ctx.chunks_ready(), 2);

        ctx.advance_chunks_ready(1);
        assert_eq!(ctx.chunks_ready(), 3);
    }

    #[test]
    fn test_streaming_context_progress() {
        let bufs = vec![
            SegmentBuffer {
                segment_idx: 0,
                offset: 0,
                len: 8_000_000,
            },
            SegmentBuffer {
                segment_idx: 0,
                offset: 8_000_000,
                len: 8_000_000,
            },
        ];
        let mut ctx = StreamingContext::new(bufs, 50_000_000, 4);
        assert!(!ctx.is_complete());
        assert_eq!(ctx.batch_size(), 2);

        ctx.advance();
        assert!(!ctx.is_complete());

        ctx.advance();
        ctx.advance();
        assert!(!ctx.is_complete());

        ctx.advance();
        assert!(ctx.is_complete());
    }
}
