//! Storage Layer — DRAMPool + NVMePool + io_uring I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crate::data_type::ObjectId;

pub mod context;
pub mod dram_pool;
pub mod fd_pool;
pub mod nvme_pool;
pub mod segment;
pub mod uring;

// Re-exports for convenience.
pub use context::{ObjectContext, ObjectState, SegmentBuffer, StreamingContext};
pub use dram_pool::DRAMPool;
pub use fd_pool::FdPool;
pub use nvme_pool::NVMePool;

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
            Self::ObjectTooLarge => write!(f, "object exceeds max size"),
        }
    }
}

// ─── Global Pool Instances ───────────────────────────────────────────────────

use std::sync::OnceLock;

static DRAM_POOL: OnceLock<DRAMPool> = OnceLock::new();
static NVME_POOL: OnceLock<NVMePool> = OnceLock::new();
static FD_POOL: OnceLock<FdPool> = OnceLock::new();

pub fn get_dram_pool() -> &'static DRAMPool {
    DRAM_POOL.get().expect("DRAMPool not initialized")
}

pub fn get_nvme_pool() -> &'static NVMePool {
    NVME_POOL.get().expect("NVMePool not initialized")
}

pub fn get_fd_pool() -> &'static FdPool {
    FD_POOL.get().expect("FdPool not initialized")
}

// ─── Initialization ──────────────────────────────────────────────────────────

/// Initialize both pools. Called at module startup.
/// `nvme_segment_size`: total NVMePool segment size (buf_count * buf_size).
/// `dram_segment_size`: total DRAMPool segment size (configurable, larger).
/// `data_dir`: NVMe file storage directory.
pub fn init(nvme_segment_size: usize, dram_segment_size: usize, _data_dir: &str) {
    // NVMePool: 1 segment, fixed at startup. buf_index starts at 0.
    let nvme_pool = NVMePool::new(1, nvme_segment_size, 0);
    NVME_POOL.set(nvme_pool).ok();

    // DRAMPool: 1 segment initially. buf_index starts after NVMePool segments.
    let nvme_seg_count = get_nvme_pool().segments.len() as u16;
    let dram_pool = DRAMPool::new(1, dram_segment_size, nvme_seg_count);
    DRAM_POOL.set(dram_pool).ok();

    // FdPool: caches open file descriptors for NVMe object files.
    // Currently basic HashMap; will be upgraded to Arc<FdEntry> + LFRU eviction.
    FD_POOL.set(FdPool::new()).ok();
}

/// Register ALL segments (both pools) with io_uring as one combined iovec array.
/// Must be called after init().
pub fn register_buffers() {
    let mut iovecs = get_nvme_pool().iovecs();
    iovecs.extend(get_dram_pool().iovecs());

    // Create io_uring engine with combined iovecs.
    let engine = uring::UringNvmeEngine::new(iovecs);
    uring::set_engine(engine);
}

/// Shutdown: signal io_uring poller to exit.
pub fn shutdown() {
    uring::shutdown();
}

/// Get combined iovecs for transport registration (fi_mr_reg per segment).
pub fn all_segment_slices() -> Vec<&'static [u8]> {
    let mut slices = Vec::new();
    for seg in &get_nvme_pool().segments {
        slices.push(unsafe { std::slice::from_raw_parts(seg.base, seg.size) });
    }
    for seg in &get_dram_pool().segments {
        slices.push(unsafe { std::slice::from_raw_parts(seg.base, seg.size) });
    }
    slices
}

/// Delete an object's NVMe file. Called from free callback.
pub fn delete_file(object_id: ObjectId) {
    let dir = crate::data_dir();
    let path = object_id.file_path(&dir);
    let _ = std::fs::remove_file(&path);
}
