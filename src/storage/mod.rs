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
pub fn cleanup_nvme_dir(mode: crate::OperatingMode, dir: &str) -> std::io::Result<()> {
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

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperatingMode;

    #[test]
    fn test_cleanup_nvme_dir_wipes_and_recreates() {
        // Unique temp nvme-dir without relying on external crates.
        let base = std::env::temp_dir();
        let dir = base.join(format!("bigobj_cleanup_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap();

        // nvme-dir is exclusively the module's, so cleanup removes everything in
        // it, name-agnostically — object files, tmp scratch, anything.
        std::fs::write(dir.join("0000000000000001.dat"), b"a").unwrap();
        std::fs::write(dir.join("0000000000000002.dat.tmp"), b"b").unwrap();
        std::fs::write(dir.join("whatever"), b"c").unwrap();

        cleanup_nvme_dir(OperatingMode::Tiered, dir_str).unwrap();
        assert!(dir.exists(), "nvme-dir should be recreated empty");
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "nvme-dir should be empty after cleanup"
        );

        // Idempotent: a second cleanup leaves an empty dir.
        cleanup_nvme_dir(OperatingMode::Tiered, dir_str).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);

        // A missing dir is a safe no-op that recreates it.
        let missing = base.join(format!("bigobj_cleanup_missing_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&missing);
        cleanup_nvme_dir(OperatingMode::Tiered, missing.to_str().unwrap()).unwrap();
        assert!(missing.exists(), "missing dir should be created");

        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::remove_dir_all(&missing).unwrap();
    }

    #[test]
    fn test_cleanup_nvme_dir_dram_is_noop() {
        // In Dram mode cleanup never touches disk: existing contents survive,
        // and even an unset (empty) path is accepted without error.
        let dir = std::env::temp_dir().join(format!("bigobj_cleanup_dram_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("keep.dat"), b"x").unwrap();

        cleanup_nvme_dir(OperatingMode::Dram, dir.to_str().unwrap()).unwrap();
        assert!(
            dir.join("keep.dat").exists(),
            "Dram cleanup must not delete files"
        );
        cleanup_nvme_dir(OperatingMode::Dram, "").unwrap();

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_cleanup_nvme_dir_tiered_requires_dir() {
        // Tiered mode with an unset nvme-dir is a misconfiguration, surfaced as
        // an error so startup can refuse to load rather than start dirty.
        assert!(cleanup_nvme_dir(OperatingMode::Tiered, "").is_err());
    }
}
