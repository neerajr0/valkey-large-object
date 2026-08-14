//! Buffer ownership and pool management.
//!
//! Buffer: owned handle wrapping &'static PinnedBuffer. Moves through the pipeline.
//! BufferPool: holds available Buffers. get() pops, Drop pushes back.

use std::sync::Mutex;
use super::engine::PinnedBuffer;

/// Owned buffer handle. Wraps a &'static PinnedBuffer + registered index.
/// Holding this = exclusive access to the underlying pinned memory.
/// Move between layers freely. Drop returns it to the pool.
pub struct Buffer {
    pinned: &'static PinnedBuffer,
    idx: u16,
}

impl Buffer {
    pub(crate) fn from_pinned(pinned: &'static PinnedBuffer, idx: u16) -> Self {
        Self { pinned, idx }
    }

    pub fn ptr(&self) -> *mut u8 { self.pinned.as_mut_ptr() }
    pub fn len(&self) -> usize { self.pinned.len() }
    pub fn idx(&self) -> u16 { self.idx }
    pub fn pinned(&self) -> &'static PinnedBuffer { self.pinned }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        crate::storage::return_buffer(self.pinned, self.idx);
    }
}

/// The buffer pool. Holds available Buffers in a Vec behind a Mutex.
/// get() pops one out. Drop pushes it back.
pub struct BufferPool {
    pool: Mutex<Vec<Buffer>>,
}

impl BufferPool {
    pub fn new() -> Self {
        Self { pool: Mutex::new(Vec::new()) }
    }

    /// Fill the pool with Buffers pointing to the given PinnedBuffers.
    pub fn fill(&self, pinned_buffers: &'static [PinnedBuffer]) {
        let mut pool = self.pool.lock().unwrap();
        for (i, pb) in pinned_buffers.iter().enumerate() {
            pool.push(Buffer::from_pinned(pb, i as u16));
        }
    }

    /// Take a buffer from the pool. Returns None if exhausted.
    pub fn get(&self) -> Option<Buffer> {
        self.pool.lock().unwrap().pop()
    }

    /// Return a buffer to the pool (called by Buffer::Drop).
    pub fn put_back(&self, pinned: &'static PinnedBuffer, idx: u16) {
        self.pool.lock().unwrap().push(Buffer::from_pinned(pinned, idx));
    }
}
