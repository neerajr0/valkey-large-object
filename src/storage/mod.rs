//! Storage Layer — buffer pool + NVMe I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crate::data_type::ObjectId;
pub use buffer::Buffer;
use engine::PinnedBuffer;

pub mod arena;
pub mod buffer;
pub mod engine;
pub mod fd_pool;
pub mod uring;

// ─── Callback Type Aliases ────────────────────────────────────────────────────

/// Callback for read completion: (buffer returned, bytes_read or error).
pub type ReadCallback = Box<dyn FnOnce(Buffer, Result<u64, StorageError>) + Send>;

/// Callback for write completion: (buffer returned, (ObjectId, crc32c) or error).
pub type WriteCallback = Box<dyn FnOnce(Buffer, Result<(ObjectId, u32), StorageError>) + Send>;

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

// ─── NvmeEngine Trait ────────────────────────────────────────────────────────

/// NvmeEngine trait — abstraction over the io_uring submission path.
/// Implemented by UringNvmeEngine (production) and SyncNvmeEngine (tests).
pub trait NvmeEngine: Send + Sync {
    fn submit(&self, req: uring::IoRequest);
    /// Signal the engine to stop accepting work and exit its poller loop.
    /// Does not block — the poller thread exits asynchronously.
    fn signal_shutdown(&self);
}

// ─── Storage Trait (from interface doc) ──────────────────────────────────────

/// Storage trait — buffer pool + NVMe I/O.
/// All methods operate on ObjectId, never on Valkey keys.
pub trait Storage: Send + Sync {
    // ─── Buffer Pool ─────────────────────────────────────────────────────

    /// Get a buffer from the pool. Returns None if pool exhausted.
    /// The returned Buffer is owned — Drop returns it to the pool automatically.
    fn pool_get(&self) -> Option<Buffer>;

    /// Pool buffer size (all buffers are this fixed size).
    fn pool_buf_size(&self) -> usize;

    // ─── Registration ────────────────────────────────────────────────────

    /// Register pool buffers with io_uring (IORING_REGISTER_BUFFERS).
    fn register_buffers(&self) -> Result<(), StorageError>;

    /// Deregister pool buffers from io_uring.
    fn deregister_buffers(&self) -> Result<(), StorageError>;

    // ─── NVMe I/O ───────────────────────────────────────────────────────

    /// Read object from NVMe into buf. Async via io_uring ReadFixed.
    /// Takes Buffer by value (ownership transfers to storage during I/O).
    /// Returns (Buffer, bytes_read) in callback — caller gets buf back.
    fn read_into(&self, object_id: ObjectId, buf: Buffer, len: u64, on_complete: ReadCallback);

    /// Write buf to NVMe as a new object. Async via io_uring.
    /// Takes Buffer by value. Returns (Buffer, ObjectId, crc32c) via callback.
    fn write_new(&self, buf: Buffer, len: u64, on_complete: WriteCallback);

    /// Delete an object file from NVMe. Called on key deletion or eviction.
    fn delete(&self, object_id: ObjectId);
}

// ─── Global Storage Instance ─────────────────────────────────────────────────

use std::sync::OnceLock;
static STORAGE: OnceLock<engine::StorageEngine> = OnceLock::new();

/// Arena storage instance (used when pool-mode=arena).
static ARENA_STORAGE: OnceLock<engine::ArenaStorageEngine> = OnceLock::new();

/// Dynamic storage instance (used when pool-mode=dynamic).
static DYNAMIC_STORAGE: OnceLock<engine::DynamicStorageEngine> = OnceLock::new();

/// Which mode is active.
static POOL_MODE: OnceLock<PoolMode> = OnceLock::new();

#[derive(Clone, Copy, PartialEq)]
pub enum PoolMode {
    BufPool,
    Arena,
    /// Dynamic: buffers allocated on demand from heap (ValkeyAlloc).
    /// No io_uring pre-registration (uses plain read/write, not ReadFixed).
    /// EFA: single registered staging buffer for fi_write (memcpy on EFA path).
    Dynamic,
    /// Dynamic without EFA registration — isolates alloc cost from fi_mr_reg cost.
    DynamicNoEfa,
}

pub fn pool_mode() -> PoolMode {
    *POOL_MODE.get().unwrap_or(&PoolMode::BufPool)
}

pub fn get() -> &'static dyn Storage {
    match pool_mode() {
        PoolMode::BufPool => STORAGE.get().expect("storage not initialized") as &dyn Storage,
        PoolMode::Arena => {
            ARENA_STORAGE.get().expect("arena storage not initialized") as &dyn Storage
        }
        PoolMode::Dynamic | PoolMode::DynamicNoEfa => {
            DYNAMIC_STORAGE.get().expect("dynamic storage not initialized") as &dyn Storage
        }
    }
}

/// Called by Buffer::drop() to return a buffer to the pool.
pub fn return_buffer(pinned: &'static engine::PinnedBuffer, idx: u16) {
    match pool_mode() {
        PoolMode::BufPool => {
            if let Some(storage) = STORAGE.get() {
                storage.buffer_pool().put_back(pinned, idx);
            }
        }
        PoolMode::Arena => {
            if let Some(storage) = ARENA_STORAGE.get() {
                // In arena mode, pinned.as_mut_ptr() is the sub-allocated address.
                storage.return_buffer_by_ptr(pinned.as_mut_ptr());
            }
        }
        PoolMode::Dynamic | PoolMode::DynamicNoEfa => {
            // In dynamic mode, the buffer was heap-allocated and fi_mr_reg'd.
            // The MR handle was forgotten in pool_get() — on real EFA hardware,
            // we'd need to track it to call fi_mr_dereg here.
            // TODO: Store MR handle in a side-map keyed by ptr, retrieve and drop here.
            // For now, the registration leaks until process exit (acceptable for benchmark).
            //
            // Free the buffer memory.
            // SAFETY: We leaked this PinnedBuffer in pool_get(). Reconstruct and drop.
            unsafe {
                let _ = Box::from_raw(pinned as *const engine::PinnedBuffer as *mut engine::PinnedBuffer);
            }
        }
    }
}

/// Convenience: delete an object's NVMe file.
pub fn delete(object_id: crate::data_type::ObjectId) {
    get().delete(object_id);
}

pub fn init(buf_size: usize, buf_count: usize, data_dir: &str) {
    let mode_str = crate::pool_mode();
    let mode = if mode_str == "arena" {
        PoolMode::Arena
    } else if mode_str == "dynamic" {
        PoolMode::Dynamic
    } else if mode_str == "dynamic-noefa" {
        PoolMode::DynamicNoEfa
    } else {
        PoolMode::BufPool
    };
    POOL_MODE.set(mode).ok();

    match mode {
        PoolMode::BufPool => {
            let storage = engine::StorageEngine::new(buf_size, buf_count, data_dir);
            STORAGE.set(storage).ok();
            // Fill the pool now that StorageEngine is in the static OnceLock.
            STORAGE.get().unwrap().init_pool();
        }
        PoolMode::Arena => {
            let total_size = buf_size * buf_count;
            let storage = engine::ArenaStorageEngine::new(buf_size, total_size, data_dir);
            ARENA_STORAGE.set(storage).ok();
        }
        PoolMode::Dynamic | PoolMode::DynamicNoEfa => {
            let storage = engine::DynamicStorageEngine::new(buf_size, data_dir);
            DYNAMIC_STORAGE.set(storage).ok();
        }
    }
}

/// Shutdown: drain in-flight ops, close fds, clean up files.
pub fn shutdown() {
    match pool_mode() {
        PoolMode::BufPool => {
            if let Some(storage) = STORAGE.get() {
                storage.signal_shutdown();
            }
        }
        PoolMode::Arena => {
            if let Some(storage) = ARENA_STORAGE.get() {
                storage.signal_shutdown();
            }
        }
        PoolMode::Dynamic | PoolMode::DynamicNoEfa => {
            if let Some(storage) = DYNAMIC_STORAGE.get() {
                storage.signal_shutdown();
            }
        }
    }
}

pub fn register_buffers() {
    let _ = get().register_buffers();
}

pub fn deregister_buffers() {
    let _ = get().deregister_buffers();
}

/// Return Buffer descriptors for transport layer to fi_mr_reg.
pub fn pinned_buffers() -> &'static [PinnedBuffer] {
    match pool_mode() {
        PoolMode::BufPool => STORAGE.get().unwrap().buffer_descriptors(),
        PoolMode::Arena => {
            let arena = ARENA_STORAGE.get().unwrap();
            std::slice::from_ref(arena.pinned_backing())
        }
        PoolMode::Dynamic => {
            // Dynamic mode: register a single staging buffer for EFA fi_write.
            let dynamic = DYNAMIC_STORAGE.get().unwrap();
            std::slice::from_ref(dynamic.staging_buffer())
        }
        PoolMode::DynamicNoEfa => {
            // No EFA registration at all — return empty slice.
            &[]
        }
    }
}
