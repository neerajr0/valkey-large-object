//! Storage Layer — DRAMPool + NVMePool + io_uring I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

pub mod context;
pub mod dram_pool;
pub mod fd_pool;
pub mod nvme;
pub mod nvme_pool;
pub mod object_file;
pub mod scaling;
pub mod segment;
pub mod segment_pool;
pub mod uring;

// Re-exports for convenience.
pub use context::{ObjectContext, SegmentBuffer, StreamingContext};
pub use object_file::ObjectFile;

// Re-exports from nvme.rs
pub use nvme::{
    object_disk_len, open_nvme_file_for_write, read_and_verify_file_header,
    validate_and_clean_nvme_dir, write_file_header, FileHeader, FILE_HEADER_MAGIC,
    FILE_HEADER_SIZE, FILE_HEADER_VERSION, FILE_HEADER_WIRE_LEN,
};

// Re-export for crate-internal use only.
pub(crate) use nvme::warn_failed_unlink;

pub use dram_pool::DRAMPool;
pub use fd_pool::FdPool;
pub use nvme_pool::NVMePool;

/// O_DIRECT / io_uring alignment requirement (XFS default block size).
/// Both buffer address and I/O length must be multiples of this.
pub const IO_ALIGN: usize = 4096;

/// Round up to IO_ALIGN boundary. Used by the allocator (buffer size)
/// and the uring layer (I/O length) to satisfy O_DIRECT requirements.
pub fn align_up(n: usize) -> usize {
    (n + IO_ALIGN - 1) & !(IO_ALIGN - 1)
}

// ─── TryClone Trait ──────────────────────────────────────────────────────────

/// Fallible deep-copy. Like Clone but returns None on failure modes when not possible.
pub trait TryClone: Sized {
    fn try_clone(&self) -> Option<Self>;
}

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

/// Global sparse iovec table. Slot `i` = iovec_index for io_uring ReadFixed/WriteFixed.
/// `None` = empty slot (no page pinned, no buffer registered at this index).
/// `Some((ptr, len))` = live segment registered at this index.
///
/// Grows as segments are added via `append_iovec`. Bounded by `u16::MAX` (65535)
/// since iovec_index is u16 — in practice a handful of entries.
/// Per-slot updates via `clear_iovec` mirror the io_uring sparse table model:
/// nulling a slot costs nothing (no page pinning for null entries).
static IOVECS: Mutex<Vec<Option<(usize, usize)>>> = Mutex::new(Vec::new());

/// Called by SegmentPool when creating each segment.
/// Fills the first `None` hole in the sparse table (or appends if no hole).
/// This matches the "first None hole, else append" policy used by
/// `SegmentPool::expand` when placing the new segment in `slots`, so the
/// returned `iovec_index` always equals the segment's slot index. Callers
/// depend on `segment.iovec_index == slot_idx`; using a different policy here
/// would silently violate that invariant when the two Vecs have holes in
/// different positions.
///
/// Only invoked from the main event-loop thread — no cross-thread contention
/// over which hole to fill.
pub fn append_iovec(iov: libc::iovec) -> u16 {
    let mut iovecs = IOVECS.lock().expect("IOVECS lock unavailable");
    let entry = Some((iov.iov_base as usize, iov.iov_len));
    match iovecs.iter().position(|s| s.is_none()) {
        Some(i) => {
            iovecs[i] = entry;
            u16::try_from(i).expect("iovec index overflow (>65535)")
        }
        None => {
            let i = iovecs.len();
            iovecs.push(entry);
            u16::try_from(i).expect("iovec index overflow (>65535)")
        }
    }
}

/// Called by SegmentPool during segment drain completion.
/// Nulls the sparse slot so the io_uring registration can be cleared.
pub fn clear_iovec(iovec_index: u16) {
    let mut iovecs = IOVECS.lock().expect("IOVECS lock unavailable");
    let idx = iovec_index as usize;
    if idx < iovecs.len() {
        iovecs[idx] = None;
    }
}

pub(super) static DRAM_POOL: OnceLock<DRAMPool> = OnceLock::new();
pub(super) static NVME_POOL: OnceLock<NVMePool> = OnceLock::new();
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

/// Initialize storage layer: validate config, create pools, spawn io_uring poller (Tiered only).
/// All OnceLock statics are set at the very end after everything succeeds.
/// On failure, local variables drop naturally — no cleanup needed, module load retryable.
/// Returns Ok(summary string) on success, Err(message) on validation/environment failure.
pub fn init(mode: crate::OperatingMode, nvme_dir: &str) -> Result<String, String> {
    let dram_seg_size = crate::dram_segment_size();
    let dram_max = crate::dram_maxmemory();
    let nvme_staging = crate::nvme_staging_size();
    // DRAMPool segment count: if maxmemory=0, start with 1 segment (grow later).
    // Otherwise pre-allocate maxmemory / segment_size segments.
    let dram_segment_count = if dram_max == 0 {
        1
    } else {
        ((dram_max as usize) / dram_seg_size).max(1)
    };
    // Total registered iovecs (DRAM + NVMe) must fit in u16 for io_uring IORING_REGISTER_BUFFERS.
    //
    // NVMe staging is split into uniform `segment_size` segments (the io_uring/EFA
    // per-buffer cap is 1 GiB, and segment-size is bounded to ≤1 GiB). Ceiling division
    // so total NVMe staging capacity is never less than the requested nvme-staging-size
    // (floor would under-provision: e.g. 100MB staging / 64MB segment = 1 segment = 64MB,
    // 36MB short).
    let nvme_segments: usize = if mode == crate::OperatingMode::Tiered {
        (nvme_staging.div_ceil(dram_seg_size)).max(1)
    } else {
        0
    };
    let total_segments = dram_segment_count + nvme_segments;
    if total_segments > u16::MAX as usize + 1 {
        return Err(format!(
            "too many segments ({} DRAM + {} NVMe = {}). \
             Max {} (io_uring iovec_index is u16). \
             Increase segment-size or decrease dram-maxmemory",
            dram_segment_count,
            nvme_segments,
            total_segments,
            u16::MAX as usize + 1,
        ));
    }
    // ── Create all resources as locals (no OnceLock yet) ──
    // NVMePool + FdPool: only needed in Tiered mode.
    let nvme_pool = if mode == crate::OperatingMode::Tiered {
        Some(NVMePool::new(nvme_segments, dram_seg_size))
    } else {
        None
    };
    let fd_pool = if mode == crate::OperatingMode::Tiered {
        Some(FdPool::new())
    } else {
        None
    };
    // DRAMPool: always needed (both modes).
    let dram_pool = DRAMPool::new(dram_segment_count, dram_seg_size);
    // io_uring NVMe engine: only in Tiered mode. Ring creation + buffer registration
    // happen on this (main) thread so failures return Err, not panic in the poller.
    let nvme_engine = if mode == crate::OperatingMode::Tiered {
        let pairs = IOVECS.lock().expect("IOVECS lock unavailable").clone();
        let iovecs: Vec<libc::iovec> = pairs
            .iter()
            .filter_map(|opt| {
                opt.map(|(ptr, len)| libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: len,
                })
            })
            .collect();
        let engine = uring::UringNvmeEngine::new(iovecs).map_err(|e| {
            // Engine failed — clear IOVECS so a retry starts fresh.
            IOVECS.lock().expect("IOVECS lock unavailable").clear();
            format!("io_uring engine: {}", e)
        })?;
        Some(engine)
    } else {
        None
    };
    // ── All succeeded — commit to globals. No failure possible after this point. ──
    if let Some(pool) = nvme_pool {
        if NVME_POOL.set(pool).is_err() {
            panic!("NVMePool already initialized");
        }
    }
    if let Some(pool) = fd_pool {
        if FD_POOL.set(pool).is_err() {
            panic!("FdPool already initialized");
        }
    }
    if DRAM_POOL.set(dram_pool).is_err() {
        panic!("DRAMPool already initialized");
    }
    if let Some(engine) = nvme_engine {
        uring::set_nvme_engine(engine);
    }
    Ok(format!(
        "mode={:?} nvme_dir={} dram_segments={}x{}MB nvme_staging={}MB",
        mode,
        nvme_dir,
        dram_segment_count,
        dram_seg_size / (1024 * 1024),
        nvme_staging / (1024 * 1024),
    ))
}

/// Get combined iovecs for transport registration (fi_mr_reg per segment).
pub fn all_segment_slices() -> Vec<&'static [u8]> {
    let mut slices = Vec::new();
    if let Some(nvme_pool) = NVME_POOL.get() {
        nvme_pool.with_live_segment_slices(|base, size| {
            slices.push(unsafe { std::slice::from_raw_parts(base, size) });
        });
    }
    get_dram_pool().with_live_segment_slices(|base, size| {
        slices.push(unsafe { std::slice::from_raw_parts(base, size) });
    });
    slices
}

// ─── ChunkBuilder ────────────────────────────────────────────────────────────

/// One region of client-visible memory for a chunk (EFA transfer target).
/// In the single-address case, every chunk has exactly one region.
/// In the multi-address case, chunks straddling a client buffer boundary
/// will have two regions.
#[derive(Debug, Clone)]
pub struct ClientRegion {
    pub remote_addr: u64,
    pub len: usize,
}

/// Builds chunk descriptors for an object. Encapsulates chunking geometry
/// and client address mapping. Decoupled from buffer ownership — callers
/// index into their own buffer collections using chunk indices from this builder.
pub struct ChunkBuilder {
    obj_len: u64,
    chunk_size: usize,
    total_chunks: u32,
    /// Per-chunk client address mappings. Populated by map_client_addresses().
    mappings: Vec<Vec<ClientRegion>>,
}

impl ChunkBuilder {
    /// Create a new ChunkBuilder.
    /// Panics on zero obj_len or zero chunk_size (callers must reject these earlier).
    pub fn new(obj_len: u64, chunk_size: usize) -> Self {
        debug_assert!(obj_len > 0, "ChunkBuilder: obj_len must be > 0");
        debug_assert!(chunk_size > 0, "ChunkBuilder: chunk_size must be > 0");
        let total_chunks = obj_len.div_ceil(chunk_size as u64) as u32;
        Self {
            obj_len,
            chunk_size,
            total_chunks,
            mappings: Vec::new(),
        }
    }

    pub fn total_chunks(&self) -> u32 {
        self.total_chunks
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Data length for chunk i (last chunk may be shorter).
    pub fn data_len(&self, i: u32) -> usize {
        debug_assert!(
            i < self.total_chunks,
            "chunk index {} >= total_chunks {}",
            i,
            self.total_chunks
        );
        if i == self.total_chunks - 1 {
            let rem = (self.obj_len % self.chunk_size as u64) as usize;
            if rem == 0 {
                self.chunk_size
            } else {
                rem
            }
        } else {
            self.chunk_size
        }
    }

    /// Compute client address mapping for all chunks (single-pass).
    /// Returns Err if total client address space is insufficient.
    pub fn map_client_addresses(
        &mut self,
        client_addrs: &[(u64, usize)],
    ) -> Result<(), &'static str> {
        let mut mappings = Vec::with_capacity(self.total_chunks as usize);
        let mut current_addr_idx: usize = 0;
        let mut current_offset: usize = 0;
        for chunk_idx in 0..self.total_chunks {
            let mut remaining = self.data_len(chunk_idx);
            let mut regions = Vec::new();
            // Consume client address space until this chunk is fully covered.
            while remaining > 0 {
                // No more client addresses — total address space is insufficient.
                if current_addr_idx >= client_addrs.len() {
                    return Err("insufficient client address space");
                }
                // Bytes remaining in the current client address region.
                let avail = client_addrs[current_addr_idx].1 - current_offset;
                // Current region exhausted — advance to the next one.
                if avail == 0 {
                    current_addr_idx += 1;
                    current_offset = 0;
                    continue;
                }
                // Take as much as we need (or as much as is available).
                let take = remaining.min(avail);
                regions.push(ClientRegion {
                    remote_addr: client_addrs[current_addr_idx].0 + current_offset as u64,
                    len: take,
                });
                current_offset += take;
                remaining -= take;
            }
            mappings.push(regions);
        }
        self.mappings = mappings;
        Ok(())
    }

    /// Get the client regions for chunk i. Panics if map_client_addresses not called.
    pub fn client_regions(&self, i: u32) -> &[ClientRegion] {
        assert!(
            !self.mappings.is_empty(),
            "client_regions called before map_client_addresses"
        );
        &self.mappings[i as usize]
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── ChunkBuilder: geometry ──────────────────────────────────────────

    #[test]
    fn test_chunk_builder_single_chunk_exact() {
        // obj_len == chunk_size → exactly 1 full chunk.
        let b = ChunkBuilder::new(4096, 4096);
        assert_eq!(b.total_chunks(), 1);
        assert_eq!(b.data_len(0), 4096);
    }

    #[test]
    fn test_chunk_builder_single_byte() {
        // Smallest possible object: 1 byte → 1 chunk of 1 byte.
        let b = ChunkBuilder::new(1, 4096);
        assert_eq!(b.total_chunks(), 1);
        assert_eq!(b.data_len(0), 1);
    }

    #[test]
    fn test_chunk_builder_exact_multiple() {
        // obj_len is an exact multiple of chunk_size → all chunks are full.
        let b = ChunkBuilder::new(16384, 4096);
        assert_eq!(b.total_chunks(), 4);
        for i in 0..4 {
            assert_eq!(b.data_len(i), 4096);
        }
    }

    #[test]
    fn test_chunk_builder_partial_last_chunk() {
        // obj_len = 3 * chunk_size + 1 → last chunk has 1 byte.
        let b = ChunkBuilder::new(12289, 4096);
        assert_eq!(b.total_chunks(), 4);
        assert_eq!(b.data_len(0), 4096);
        assert_eq!(b.data_len(1), 4096);
        assert_eq!(b.data_len(2), 4096);
        assert_eq!(b.data_len(3), 1);
    }

    #[test]
    fn test_chunk_builder_large_object() {
        // 50 MB object with 8 MB chunks → 7 chunks, last has 50%8=2 MB.
        let obj_len = 50 * 1024 * 1024u64;
        let chunk_size = 8 * 1024 * 1024;
        let b = ChunkBuilder::new(obj_len, chunk_size);
        assert_eq!(b.total_chunks(), 7);
        for i in 0..6 {
            assert_eq!(b.data_len(i), chunk_size);
        }
        assert_eq!(b.data_len(6), 2 * 1024 * 1024);
    }

    // ─── ChunkBuilder: map_client_addresses ──────────────────────────────

    #[test]
    fn test_map_single_contiguous_address() {
        // Single client address covering the full object.
        let mut b = ChunkBuilder::new(8193, 4096);
        assert_eq!(b.total_chunks(), 3);
        b.map_client_addresses(&[(0x1000, 8193)]).unwrap();
        // Chunk 0: one region covering full chunk_size.
        let r0 = b.client_regions(0);
        assert_eq!(r0.len(), 1);
        assert_eq!(r0[0].remote_addr, 0x1000);
        assert_eq!(r0[0].len, 4096);
        // Chunk 1: one region covering full chunk_size.
        let r1 = b.client_regions(1);
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].remote_addr, 0x1000 + 4096);
        assert_eq!(r1[0].len, 4096);
        // Chunk 2: one region covering partial last chunk (1 byte).
        let r2 = b.client_regions(2);
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].remote_addr, 0x1000 + 8192);
        assert_eq!(r2[0].len, 1);
    }

    #[test]
    fn test_map_multiple_addresses_chunk_straddling() {
        // Two client addresses: first covers 5000 bytes, second covers 3193.
        // Object is 8193 bytes with chunk_size=4096 → 3 chunks.
        // Chunk 0 (4096 bytes): entirely in address 0.
        // Chunk 1 (4096 bytes): 904 bytes from address 0 + 3192 bytes from address 1.
        // Chunk 2 (1 byte): from address 1.
        let mut b = ChunkBuilder::new(8193, 4096);
        b.map_client_addresses(&[(0x1000, 5000), (0x2000, 3193)])
            .unwrap();
        // Chunk 0: single region.
        let r0 = b.client_regions(0);
        assert_eq!(r0.len(), 1);
        assert_eq!(r0[0].remote_addr, 0x1000);
        assert_eq!(r0[0].len, 4096);
        // Chunk 1: straddles two addresses.
        let r1 = b.client_regions(1);
        assert_eq!(r1.len(), 2);
        assert_eq!(r1[0].remote_addr, 0x1000 + 4096); // remaining 904 bytes of addr 0
        assert_eq!(r1[0].len, 904);
        assert_eq!(r1[1].remote_addr, 0x2000); // 3192 bytes from addr 1
        assert_eq!(r1[1].len, 3192);
        // Chunk 2: single region from address 1.
        let r2 = b.client_regions(2);
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].remote_addr, 0x2000 + 3192);
        assert_eq!(r2[0].len, 1);
    }

    #[test]
    fn test_map_insufficient_address_space() {
        // Client address space is smaller than obj_len.
        let mut b = ChunkBuilder::new(8192, 4096);
        let result = b.map_client_addresses(&[(0x1000, 4096)]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "insufficient client address space");
    }

    #[test]
    #[should_panic(expected = "client_regions called before map_client_addresses")]
    fn test_client_regions_panics_without_mapping() {
        let b = ChunkBuilder::new(4096, 4096);
        let _ = b.client_regions(0);
    }
}
