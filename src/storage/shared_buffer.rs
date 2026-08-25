//! SharedBuffer: Arc-wrapped Buffer for zero-copy fan-out.
//!
//! When multiple consumers need the same data (read coalescing, broadcast),
//! SharedBuffer provides shared read access to a single pool buffer.
//! The underlying Buffer returns to the pool when the last SharedBuffer clone drops.
//!
//! Thread safety: Clone is cheap (Arc increment). All clones share read-only access
//! to the same pinned memory. The data is immutable after NVMe read completion.

use std::sync::Arc;

use super::buffer::Buffer;

/// Internal wrapper holding the owned Buffer + valid data length.
/// Buffer's Drop impl returns it to the pool when this struct drops
/// (which happens when the last Arc reference is released).
struct BufferInner {
    buf: Buffer,
    /// Actual bytes of valid data in the buffer (may be < buf.len()).
    data_len: u64,
}

/// Shared read-only access to a pool buffer.
///
/// Created by wrapping a Buffer after NVMe read completion.
/// Clone is cheap (Arc refcount increment). The underlying pinned memory
/// returns to the buffer pool when the last SharedBuffer is dropped.
///
/// Multiple concurrent fi_write operations from the same SharedBuffer are safe:
/// the local buffer is read-only during fi_write (libfabric guarantee).
#[derive(Clone)]
pub struct SharedBuffer {
    inner: Arc<BufferInner>,
}

impl SharedBuffer {
    /// Wrap a Buffer after NVMe read completion.
    /// `data_len` is the number of valid bytes (bytes_read from io_uring CQE).
    pub fn new(buf: Buffer, data_len: u64) -> Self {
        Self {
            inner: Arc::new(BufferInner { buf, data_len }),
        }
    }

    /// Pointer to the underlying pinned memory. Valid for `data_len()` bytes.
    /// The memory is read-only after construction — do not write through this pointer.
    pub fn ptr(&self) -> *const u8 {
        self.inner.buf.ptr() as *const u8
    }

    /// Number of valid data bytes in the buffer.
    pub fn data_len(&self) -> u64 {
        self.inner.data_len
    }

    /// Buffer index (for io_uring registered buffer ops, fi_write local_desc, etc.)
    pub fn idx(&self) -> u16 {
        self.inner.buf.idx()
    }

    /// Read the valid data as a byte slice.
    ///
    /// After NVMe read completion, the buffer content is immutable —
    /// concurrent reads from multiple threads via Arc clones are safe.
    pub fn as_slice(&self) -> &[u8] {
        &self.inner.buf.pinned().as_slice()[..self.inner.data_len as usize]
    }

    /// Number of active references to this shared buffer (for metrics/debugging).
    #[allow(dead_code)]
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }
}

// BufferInner's Drop runs Buffer's Drop which returns it to the pool.
// No manual Drop implementation needed.

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! NOTE: Tests use `std::mem::forget` on SharedBuffer/Buffer values because Buffer's
    //! Drop impl calls into the global STORAGE static (to return the buffer to the pool).
    //! In unit tests STORAGE is never initialized, so we prevent Drop from running.
    //! In production, Drop works correctly because STORAGE is initialized at module load.

    use super::*;
    use crate::storage::buffer::BufferPool;
    use crate::storage::engine::PinnedBuffer;

    /// Leak PinnedBuffers to get &'static refs for test Buffers.
    fn leak_pinned(count: usize, size: usize) -> &'static [PinnedBuffer] {
        let buffers: Vec<PinnedBuffer> = (0..count).map(|_| PinnedBuffer::new(size)).collect();
        Box::leak(buffers.into_boxed_slice())
    }

    /// Create a BufferPool with N buffers of given size.
    fn make_pool(count: usize, size: usize) -> &'static BufferPool {
        let pinned = leak_pinned(count, size);
        let pool = BufferPool::new();
        {
            let mut inner = pool.pool.lock().unwrap();
            for (i, pb) in pinned.iter().enumerate() {
                inner.push(Buffer::from_pinned(pb, i as u16));
            }
        }
        Box::leak(Box::new(pool))
    }

    #[test]
    fn test_shared_buffer_data_access() {
        let pool = make_pool(1, 4096);
        let buf = pool.get().unwrap();

        // Write known pattern into the buffer before wrapping.
        unsafe {
            std::ptr::write_bytes(buf.ptr(), 0xAB, 100);
        }

        let shared = SharedBuffer::new(buf, 100);

        // Verify data accessible via as_slice.
        let slice = shared.as_slice();
        assert_eq!(slice.len(), 100);
        assert!(slice.iter().all(|&b| b == 0xAB));

        // Verify data_len and idx.
        assert_eq!(shared.data_len(), 100);

        std::mem::forget(shared);
    }

    #[test]
    fn test_shared_buffer_clone_shares_data() {
        let pool = make_pool(1, 4096);
        let buf = pool.get().unwrap();

        unsafe {
            std::ptr::write_bytes(buf.ptr(), 0xCD, 256);
        }

        let shared = SharedBuffer::new(buf, 256);
        let clone1 = shared.clone();
        let clone2 = shared.clone();

        // All clones see the same data.
        assert_eq!(shared.as_slice(), clone1.as_slice());
        assert_eq!(shared.as_slice(), clone2.as_slice());
        assert_eq!(shared.ptr(), clone1.ptr());
        assert_eq!(shared.ptr(), clone2.ptr());

        std::mem::forget(shared);
        std::mem::forget(clone1);
        std::mem::forget(clone2);
    }

    #[test]
    fn test_shared_buffer_refcount() {
        let pool = make_pool(1, 4096);
        let buf = pool.get().unwrap();
        let shared = SharedBuffer::new(buf, 64);

        assert_eq!(shared.ref_count(), 1);

        let clone1 = shared.clone();
        assert_eq!(shared.ref_count(), 2);
        assert_eq!(clone1.ref_count(), 2);

        let clone2 = shared.clone();
        assert_eq!(shared.ref_count(), 3);

        std::mem::forget(shared);
        std::mem::forget(clone1);
        std::mem::forget(clone2);
    }

    #[test]
    fn test_shared_buffer_send_across_threads() {
        use std::thread;

        let pool = make_pool(1, 4096);
        let buf = pool.get().unwrap();

        unsafe {
            std::ptr::write_bytes(buf.ptr(), 0xEF, 128);
        }

        let shared = SharedBuffer::new(buf, 128);
        let clone_for_thread = shared.clone();

        let handle = thread::spawn(move || {
            // Verify data accessible from another thread.
            let slice = clone_for_thread.as_slice();
            assert_eq!(slice.len(), 128);
            assert!(slice.iter().all(|&b| b == 0xEF));
            std::mem::forget(clone_for_thread);
        });

        handle.join().unwrap();
        std::mem::forget(shared);
    }
}
