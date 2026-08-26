//! ObjectContext, StreamingContext, and SegmentBuffer.
//!
//! These are the runtime companions to LoValue. Not serialized — rebuilt on load,
//! evicted independently of commands.
//!
//! - ObjectContext: lives in DRAMPool HashMap, long-lived, complete object.
//! - StreamingContext: lives on a tokio task, short-lived, partial buffer window.
//! - SegmentBuffer: a slice within a registered segment (DRAMPool or NVMePool).

use std::sync::atomic::{AtomicU32, Ordering};

// ─── SegmentBuffer ───────────────────────────────────────────────────────────

/// A buffer that is a sub-allocation within a registered segment.
/// Segment-agnostic: works for both DRAMPool and NVMePool segments.
/// `segment_idx` identifies which registered iovec entry (io_uring buf_index).
#[derive(Debug, Clone)]
pub struct SegmentBuffer {
    /// Which segment this slice lives in (index into the io_uring iovec array).
    pub segment_idx: u8,
    /// Byte offset within that segment.
    pub offset: u64,
    /// Size of this buffer allocation.
    pub len: u32,
}

// ─── ObjectState ─────────────────────────────────────────────────────────────

/// State of an ObjectContext in DRAMPool.
#[derive(Debug)]
pub enum ObjectState {
    /// All buffers filled, object is servable.
    Ready,
    /// Promotion in progress — batched ReadFixed filling buffers.
    /// `chunks_ready` advances by batch_size atomically after each batch completes.
    Filling { chunks_ready: AtomicU32, total: u32 },
}

// ─── ObjectContext ───────────────────────────────────────────────────────────

/// Long-lived runtime state for a cached object in DRAMPool.
/// ALL N buffers for the entire object are allocated upfront from DRAMPool segment.
/// Stored in: `RwLock<HashMap<ObjectId, Arc<ObjectContext>>>`
#[derive(Debug)]
pub struct ObjectContext {
    /// Ordered chunks. 1 for small objects, N for large.
    pub buffers: Vec<SegmentBuffer>,
    /// Total object size (sum of all buffer lens).
    pub total_len: u64,
    /// Current state: Ready (servable) or Filling (promotion in progress).
    pub state: ObjectState,
}

impl ObjectContext {
    /// Create a new ObjectContext in Ready state (e.g., DRAM-only SET).
    pub fn new_ready(buffers: Vec<SegmentBuffer>, total_len: u64) -> Self {
        Self {
            buffers,
            total_len,
            state: ObjectState::Ready,
        }
    }

    /// Create a new ObjectContext in Filling state (promotion path).
    pub fn new_filling(buffers: Vec<SegmentBuffer>, total_len: u64, total_chunks: u32) -> Self {
        Self {
            buffers,
            total_len,
            state: ObjectState::Filling {
                chunks_ready: AtomicU32::new(0),
                total: total_chunks,
            },
        }
    }

    /// Check if the object is fully ready for serving.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, ObjectState::Ready)
    }

    /// Get the number of chunks ready (contiguous from offset 0).
    pub fn chunks_ready(&self) -> u32 {
        match &self.state {
            ObjectState::Ready => self.buffers.len() as u32,
            ObjectState::Filling { chunks_ready, .. } => chunks_ready.load(Ordering::Acquire),
        }
    }

    /// Advance chunks_ready after a batch completes. Called from tokio promotion task.
    pub fn advance_chunks_ready(&self, batch_size: u32) {
        if let ObjectState::Filling { chunks_ready, .. } = &self.state {
            chunks_ready.fetch_add(batch_size, Ordering::Release);
        }
    }
}

// ─── StreamingContext ────────────────────────────────────────────────────────

/// Short-lived runtime state for a transient I/O operation on NVMePool.
/// Rotating window of X buffers, reused across batches.
/// Owned by a single tokio task — no Arc needed.
#[derive(Debug)]
pub struct StreamingContext {
    /// Rotating buffer window (max X = batch size).
    pub buffers: Vec<SegmentBuffer>,
    /// Total object size being transferred.
    pub total_len: u64,
    /// Progress cursor: bytes completed so far.
    pub bytes_completed: u64,
    /// Rolling CRC32c hasher for SET verification. None for GET.
    pub crc_state: u32,
    /// Whether CRC is being tracked (SET = true, GET = false).
    pub track_crc: bool,
}

impl StreamingContext {
    /// Create a new StreamingContext for a SET operation (CRC tracked).
    pub fn new_for_set(buffers: Vec<SegmentBuffer>, total_len: u64) -> Self {
        Self {
            buffers,
            total_len,
            bytes_completed: 0,
            crc_state: 0,
            track_crc: true,
        }
    }

    /// Create a new StreamingContext for a GET operation (no CRC).
    pub fn new_for_get(buffers: Vec<SegmentBuffer>, total_len: u64) -> Self {
        Self {
            buffers,
            total_len,
            bytes_completed: 0,
            crc_state: 0,
            track_crc: false,
        }
    }

    /// Number of buffers (batch size / pipeline depth).
    pub fn batch_size(&self) -> usize {
        self.buffers.len()
    }

    /// Advance progress after a batch completes.
    pub fn advance(&mut self, bytes: u64) {
        self.bytes_completed += bytes;
    }

    /// Check if the entire object has been transferred.
    pub fn is_complete(&self) -> bool {
        self.bytes_completed >= self.total_len
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
        let mut ctx = StreamingContext::new_for_set(bufs, 50_000_000);
        assert!(!ctx.is_complete());
        assert_eq!(ctx.batch_size(), 2);

        ctx.advance(16_000_000);
        assert!(!ctx.is_complete());

        ctx.advance(16_000_000);
        ctx.advance(16_000_000);
        ctx.advance(2_000_000);
        assert!(ctx.is_complete());
    }
}
