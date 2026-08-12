//! Storage Layer — buffer pool + NVMe I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crate::data_type::ObjectId;
use crate::transport::PoolBuffer;

pub mod pool;
pub mod uring;

// ─── Error Types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum StorageError {
    IoError { code: i32 },
    PoolExhausted,
    ObjectTooLarge,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError { code } => write!(f, "I/O error (code {})", code),
            Self::PoolExhausted => write!(f, "buffer pool exhausted"),
            Self::ObjectTooLarge => write!(f, "object exceeds buffer size"),
        }
    }
}

// ─── Storage Trait (from interface doc) ──────────────────────────────────────

/// Storage trait — buffer pool + NVMe I/O.
/// All methods operate on ObjectId, never on Valkey keys.
pub trait Storage: Send + Sync {
    // ─── Buffer Pool ─────────────────────────────────────────────────────

    /// Get a buffer from the pool. Returns None if pool exhausted.
    fn pool_get(&self) -> Option<PoolBuffer>;

    /// Return a buffer to the pool.
    fn pool_put(&self, buf: PoolBuffer);

    /// Pin buffer — prevents eviction/reuse during in-flight DMA or io_uring op.
    fn pin(&self, buf: &PoolBuffer);

    /// Unpin buffer — allows eviction/reuse.
    fn unpin(&self, buf: &PoolBuffer);

    /// Pool buffer size (all buffers are this fixed size).
    fn pool_buf_size(&self) -> usize;

    // ─── Registration ────────────────────────────────────────────────────

    /// Register pool buffers with io_uring (IORING_REGISTER_BUFFERS).
    fn register_buffers(&self) -> Result<(), StorageError>;

    /// Deregister pool buffers from io_uring.
    fn deregister_buffers(&self) -> Result<(), StorageError>;

    // ─── NVMe I/O ───────────────────────────────────────────────────────

    /// Read object bytes from NVMe into buf. Async via io_uring ReadFixed.
    /// object_id maps directly to file path: {data_dir}/{oid:016x}.dat
    fn read_into(
        &self,
        object_id: ObjectId,
        buf: &mut PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<u64, StorageError>) + Send>,
    );

    /// Write buf to NVMe as a new object. Async via io_uring.
    /// Returns (ObjectId, crc32c) via callback.
    /// Atomicity: O_TMPFILE → write → linkat.
    fn write_new(
        &self,
        buf: &PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<(ObjectId, u32), StorageError>) + Send>,
    );

    /// Delete an object file from NVMe. Called on key deletion or eviction.
    fn delete(&self, object_id: ObjectId);
}

// ─── Global Storage Instance ─────────────────────────────────────────────────

use std::sync::OnceLock;
static STORAGE: OnceLock<pool::PoolStorage> = OnceLock::new();

pub fn get() -> &'static pool::PoolStorage {
    STORAGE.get().expect("storage not initialized")
}

/// Convenience: delete an object's NVMe file.
pub fn delete(object_id: crate::data_type::ObjectId) {
    get().delete_file(object_id);
}

pub fn init(buf_size: usize, buf_count: usize, data_dir: &str) {
    let storage = pool::PoolStorage::new(buf_size, buf_count, data_dir);
    STORAGE.set(storage).ok();
}

pub fn register_buffers() {
    // TODO: Implement io_uring IORING_REGISTER_BUFFERS for pool buffers.
    // This pins pages in kernel for ReadFixed/WriteFixed zero-copy DMA.
    let _ = get().register_buffers();
}

pub fn deregister_buffers() {
    let _ = get().deregister_buffers();
}

pub fn shutdown() {
    // TODO: Drain in-flight io_uring ops, release ring, unmap buffers.
}

/// Return PoolBuffer descriptors for transport layer to fi_mr_reg.
pub fn pool_buffer_descriptors() -> Vec<PoolBuffer> {
    get().buffer_descriptors()
}
