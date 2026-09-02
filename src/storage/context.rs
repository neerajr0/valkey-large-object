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
use std::sync::Arc;

use super::object_file::ObjectFile;

// ─── SegmentBuffer ───────────────────────────────────────────────────────────

/// A buffer that is a sub-allocation within a registered segment.
/// Segment-agnostic: works for both DRAMPool and NVMePool segments.
/// `segment_idx` identifies which registered iovec entry (io_uring buf_index).
#[derive(Debug, Clone)]
pub struct SegmentBuffer {
    /// Which segment this slice lives in (local index into the owning pool's segments vec).
    /// NOT the global io_uring iovec index - that is on Segment.iovec_index.
    pub segment_idx: u16,
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
    /// Strong `Arc<ObjectFile>` — Tiered mode only (`None` in Dram mode and for a
    /// context created Ready).
    ///
    /// Set while `Filling` and is not dropped until the ObjectContext itself is
    /// dropped. Once updated to a `Ready` state, the benign ObjectFile reference is
    /// only dropped by the ObjectContext drop which is triggered by `lo_free`.
    #[allow(dead_code)]
    file: Option<Arc<ObjectFile>>,
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
            file: None,
        }
    }

    /// Create a new ObjectContext in Filling state (Tiered promotion path). Holds a
    /// strong `Arc<ObjectFile>` for the duration of the fill (`None` is accepted for
    /// tests).
    pub fn new_filling(
        buffers: Vec<SegmentBuffer>,
        total_len: u64,
        total_chunks: u32,
        file: Option<Arc<ObjectFile>>,
    ) -> Self {
        Self {
            buffers,
            total_len,
            state: AtomicU8::new(ObjectState::Filling as u8),
            chunks_ready: AtomicU32::new(0),
            total_chunks,
            file,
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
    /// Strong `Arc<ObjectFile>` held for a tiered transient READ (keeps the file
    /// linked + the fd valid for the read's duration). `None` on the SET write
    /// path (the new file has no committed `ObjectFile` until commit).
    #[allow(dead_code)]
    file: Option<Arc<ObjectFile>>,
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
    /// `file` is `Some` for a tiered read (pins the object's file for the read),
    /// `None` for a SET write.
    pub fn new(
        buffers: Vec<SegmentBuffer>,
        total_len: u64,
        total_chunks: u32,
        file: Option<Arc<ObjectFile>>,
    ) -> Self {
        Self {
            buffers,
            total_len,
            chunks_completed: 0,
            total_chunks,
            file,
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

    // The EFA serve reads a DRAM buffer whose lifetime IS the ObjectContext's
    // (ObjectContext::Drop frees the buffer). The fix clones the Arc<ObjectContext>
    // into the async RDMA write task, so a concurrent free (lo_free's remove_object
    // dropping the DRAM map's Arc — the sole strong ref to the context) must NOT drop
    // the context — hence must not free the buffer — until the transfer completes and the
    // task's clone drops. This is the unit-level proof of that deferral; the real RDMA
    // transfer can't be driven from a unit test, and the buffer free itself routes
    // through the global DRAM_POOL (absent here), so we observe the owner via a Weak.
    #[test]
    fn test_serve_pin_defers_buffer_free_until_transfer_done() {
        let bufs = vec![SegmentBuffer {
            segment_idx: 0,
            offset: 0,
            len: 1024,
        }];
        let ctx = Arc::new(ObjectContext::new_ready(bufs, 1024));
        // Weak observes whether the context (and thus its buffer) has been dropped.
        let weak = Arc::downgrade(&ctx);
        // The EFA write task's captured clone — what the fix adds.
        let serve_pin = Arc::clone(&ctx);

        // Concurrent free lands mid-transfer: drop every ref except the serve pin.
        drop(ctx);
        assert!(
            weak.upgrade().is_some(),
            "buffer owner must stay alive while the serve is in flight"
        );
        assert_eq!(Arc::strong_count(&serve_pin), 1);

        // Transfer completes -> task's clone drops -> context drops -> buffer freed.
        drop(serve_pin);
        assert!(
            weak.upgrade().is_none(),
            "buffer owner must drop (buffer freed) once the transfer completes"
        );
    }

    // A Filling context pins the ObjectFile; mark_ready() only flips the state
    // atomic and CANNOT null the Option, so a promoted Ready context keeps a benign
    // redundant ref that is released on drop, with no cycle. Strong counts are not
    // observable from an integration test.
    #[test]
    fn test_promoted_ready_retains_file_ref_until_drop() {
        use crate::data_type::ObjectId;
        let file = Arc::new(ObjectFile::new_cold(ObjectId(21)));
        let bufs = vec![SegmentBuffer {
            segment_idx: 0,
            offset: 0,
            len: 1024,
        }];
        let ctx = ObjectContext::new_filling(bufs, 1024, 1, Some(Arc::clone(&file)));
        assert_eq!(Arc::strong_count(&file), 2, "Filling context pins the file");

        ctx.mark_ready();
        assert!(ctx.is_ready());
        assert_eq!(
            Arc::strong_count(&file),
            2,
            "promoted Ready context still holds the benign ref"
        );

        drop(ctx);
        assert_eq!(
            Arc::strong_count(&file),
            1,
            "dropping the context releases the ref (no cycle keeps it alive)"
        );
    }

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
        let ctx = ObjectContext::new_filling(bufs, 24_000_000, 3, None);
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
        let mut ctx = StreamingContext::new(bufs, 50_000_000, 4, None);
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
