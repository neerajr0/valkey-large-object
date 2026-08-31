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
pub mod segment_pool;
pub mod uring;

// Re-exports for convenience.
pub use context::{ObjectContext, SegmentBuffer, StreamingContext};

/// O_DIRECT / io_uring alignment requirement (XFS default block size).
/// Both buffer address and I/O length must be multiples of this.
pub const IO_ALIGN: usize = 4096;

/// Round up to IO_ALIGN boundary. Used by the allocator (buffer size)
/// and the uring layer (I/O length) to satisfy O_DIRECT requirements.
pub fn align_up(n: usize) -> usize {
    (n + IO_ALIGN - 1) & !(IO_ALIGN - 1)
}
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

use std::sync::{Mutex, OnceLock};

/// Global iovec registry. Segments append here at creation time.
/// Array position = iovec_index used by io_uring ReadFixed/WriteFixed.
/// register_buffers() passes this directly to the kernel — no reordering.
/// Stored as (ptr, len) pairs because libc::iovec contains raw pointers (not Send).
static IOVECS: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());

/// Called by SegmentPool::new() when creating each segment.
/// Returns the assigned iovec_index (= current array length before push).
pub fn append_iovec(iov: libc::iovec) -> u16 {
    let mut iovecs = IOVECS.lock().expect("IOVECS lock unavailable");
    let idx = iovecs.len() as u16;
    iovecs.push((iov.iov_base as usize, iov.iov_len));
    idx
}

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

/// Initialize pools based on operating mode.
/// Creation order doesn't matter — iovec indices are assigned via global registry.
pub fn init(
    mode: crate::OperatingMode,
    dram_segment_count: usize,
    dram_segment_size: usize,
    nvme_staging_size: usize,
    _nvme_dir: &str,
) {
    // NVMePool + FdPool: only needed in Tiered mode.
    if mode == crate::OperatingMode::Tiered {
        let nvme_pool = NVMePool::new(1, nvme_staging_size);
        NVME_POOL.set(nvme_pool).ok();

        FD_POOL.set(FdPool::new()).ok();
    }

    // DRAMPool: always needed (both modes).
    let dram_pool = DRAMPool::new(dram_segment_count, dram_segment_size);
    DRAM_POOL.set(dram_pool).ok();
}

/// Register ALL segments with io_uring. Uses the global IOVECS vec built during init.
/// Array position = iovec_index, guaranteed by append_iovec() at creation time.
pub fn register_buffers() {
    let pairs = IOVECS.lock().expect("IOVECS lock unavailable").clone();
    let iovecs: Vec<libc::iovec> = pairs
        .iter()
        .map(|&(ptr, len)| libc::iovec {
            iov_base: ptr as *mut libc::c_void,
            iov_len: len,
        })
        .collect();
    let engine = uring::UringNvmeEngine::new(iovecs);
    uring::set_engine(engine);
}

/// Shutdown: signal io_uring poller to exit.
pub fn shutdown() {
    uring::shutdown();
}

/// Reset the NVMe object directory (Tiered mode only): delete it and everything
/// under it, then recreate it empty. `nvme-dir` is a dedicated, module-owned
/// directory (see the `nvme-dir` config docs), so wiping it is safe. A no-op
/// in Dram mode, which never touches disk.
///
/// Called both to reclaim a previous run's leftovers at startup and to clear
/// this instance's files at shutdown. Returns `Ok(())` once nvme-dir exists and
/// is empty (or immediately, in Dram mode); `Err` if nvme-dir is unset in Tiered
/// mode, or the directory could not be removed or recreated.
pub fn validate_and_clean_nvme_dir(mode: crate::OperatingMode, dir: &str) -> std::io::Result<()> {
    if mode != crate::OperatingMode::Tiered {
        return Ok(());
    }
    if dir.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "nvme-dir is required in Tiered operating mode",
        ));
    }
    // remove_dir_all errors if `dir` is absent — but "absent" is already the
    // state we want, so treat NotFound as success.
    if let Err(e) = std::fs::remove_dir_all(dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(e);
        }
    }
    std::fs::create_dir_all(dir)
}

/// Get combined iovecs for transport registration (fi_mr_reg per segment).
pub fn all_segment_slices() -> Vec<&'static [u8]> {
    let mut slices = Vec::new();
    if let Some(nvme_pool) = NVME_POOL.get() {
        for seg in nvme_pool.segments() {
            slices.push(unsafe { std::slice::from_raw_parts(seg.base, seg.size) });
        }
    }
    for seg in get_dram_pool().segments() {
        slices.push(unsafe { std::slice::from_raw_parts(seg.base, seg.size) });
    }
    slices
}

/// Delete an object's NVMe file. Called from free callback.
pub fn delete_file(object_id: ObjectId) {
    let dir = crate::nvme_dir();
    let path = object_id.file_path(&dir);
    let _ = std::fs::remove_file(&path);
}
