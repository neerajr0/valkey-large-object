//! ValkeyLargeObj: Large Object Module + Transport Crate
//!
//! Architecture (from interface doc):
//!   Data Type (commands, LoValue, keyspace)
//!       ↓ calls
//!   Storage (buffer pool, io_uring, NVMe files)
//!       ↓ passes buffers to
//!   Transport (EFA, fi_write/fi_read)
//!
//! Commands: LO.HELLO, LO.GET, LO.SET
//! Deletion: native Valkey DEL triggers module free callback.

// ─── Initialization Order ────────────────────────────────────────────────────
//
// Module init proceeds in strict order. Commands are safe to call ONLY after
// all steps complete:
//
//   1. transport::init()       — discover EFA devices, create fabric/domain.
//   2. storage::init(nvme_size, dram_size, data_dir)
//                              — allocate pool segments, create DRAMPool + NVMePool.
//                              — scan data_dir for existing .dat files to
//                                recover OID counter (avoids OID collision).
//   3. storage::register_buffers()
//                              — IORING_REGISTER_BUFFERS pins pool pages for
//                                ReadFixed/WriteFixed zero-copy I/O.
//   4. transport::register_buffers()
//                              — fi_mr_reg same pool buffers with EFA domains
//                                for RDMA fi_write/fi_read.
//
// After step 4, commands (LO.GET, LO.SET, LO.HELLO) may execute safely.
// ─────────────────────────────────────────────────────────────────────────────

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Mutex;

use valkey_module::configuration::ConfigurationFlags;
use valkey_module::{valkey_module, Context, Status, ValkeyString};

use tokio::runtime::Runtime;

pub mod commands;
pub mod data_type;
pub mod engine;
pub mod errors;
pub mod storage;
pub mod transport;

use crate::data_type::LO_TYPE;
use crate::engine::OperatingMode;

pub const MODULE_NAME: &str = "largeobj";
pub const MODULE_VERSION: i32 = 1;

// ─── Module Configurations (ValkeyModule Config API) ─────────────────────────

lazy_static::lazy_static! {
    /// Data directory for NVMe object files. Required. Immutable after load.
    static ref CFG_DATA_DIR: Mutex<String> = Mutex::new(String::new());

    /// NVMe pool size in bytes. This is the total segment memory for NVMe I/O buffers.
    /// Used in Tiered mode for read/write staging. Default: 512MB.
    /// Immutable after load. Supports memory notation via module args (e.g., "512mb").
    static ref CFG_NVME_POOL_SIZE: AtomicI64 = AtomicI64::new(512 * 1024 * 1024);

    /// DRAM pool size in bytes. This is the total segment memory for the DRAM cache.
    /// Used in both modes: DRAM-only stores objects here permanently,
    /// Tiered mode uses it as a promotion cache. Default: 512MB.
    /// Immutable after load. Supports memory notation via module args (e.g., "2gb").
    static ref CFG_DRAM_POOL_SIZE: AtomicI64 = AtomicI64::new(512 * 1024 * 1024);

    /// Maximum total bytes on NVMe. 0 = unlimited. Supports memory notation (e.g. "10gb").
    static ref CFG_MAX_BYTES: AtomicI64 = AtomicI64::new(0);

    /// Number of tokio worker threads for transport CQ polling. Immutable after load.
    static ref CFG_TRANSPORT_THREADS: AtomicI64 = AtomicI64::new(2);

    /// Bench mode: LO.GET TCP path replies with size integer instead of bulk value bytes.
    /// For benchmarking NVMe read throughput without TCP output buffer overhead.
    static ref CFG_BENCH_MODE: AtomicBool = AtomicBool::new(false);

    /// Direct I/O mode: when enabled, file opens use O_DIRECT to bypass the kernel page cache.
    ///
    /// WHY: Large objects (4KB-50MB) would thrash the page cache if buffered. O_DIRECT ensures
    /// NVMe reads/writes go straight to/from our pre-aligned pool buffers without kernel copies.
    ///
    /// WHEN TO DISABLE: Set to "no" when O_DIRECT writes fail with EINVAL on the target
    /// environment. Known case: ASAN builds where the sanitizer's allocator interacts
    /// differently with io_uring O_DIRECT buffer alignment validation.
    ///
    /// DEFAULT: yes (production path — always use O_DIRECT on XFS/NVMe instance store).
    static ref CFG_DIRECT_IO: AtomicBool = AtomicBool::new(true);

    /// Operating mode. Immutable after module load.
    /// - Tiered (0): objects persist on NVMe, DRAMPool is a read cache with promotion.
    /// - DramOnly (1): all objects live exclusively in DRAMPool. No NVMe. Fastest reads.
    static ref CFG_OPERATING_MODE: Mutex<OperatingMode> = Mutex::new(OperatingMode::Tiered);
}

// ─── Global Runtime ──────────────────────────────────────────────────────────

/// Tokio runtime — owned by the module, handle passed to transport crate.
static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();

pub fn runtime_handle() -> &'static tokio::runtime::Handle {
    RUNTIME.get().expect("runtime not initialized").handle()
}

// ─── Config Accessors ────────────────────────────────────────────────────────

pub fn data_dir() -> String {
    CFG_DATA_DIR.lock().unwrap().clone()
}

pub fn nvme_pool_size() -> usize {
    CFG_NVME_POOL_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn dram_pool_size() -> usize {
    CFG_DRAM_POOL_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_bytes() -> u64 {
    CFG_MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn transport_threads() -> usize {
    CFG_TRANSPORT_THREADS.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn bench_mode() -> bool {
    CFG_BENCH_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn direct_io() -> bool {
    CFG_DIRECT_IO.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn operating_mode() -> OperatingMode {
    *CFG_OPERATING_MODE.lock().unwrap()
}

// ─── Module Lifecycle ────────────────────────────────────────────────────────

fn initialize(ctx: &Context, _args: &[ValkeyString]) -> Status {
    // Configs are already populated by the valkey_module! macro via module_args_as_configuration.
    let dir = data_dir();
    if dir.is_empty() {
        ctx.log_warning("largeobj: data-dir is required");
        return Status::Err;
    }

    // Ensure data directory exists.
    if let Err(e) = std::fs::create_dir_all(&dir) {
        ctx.log_warning(&format!("largeobj: failed to create data-dir: {}", e));
        return Status::Err;
    }

    // Step 0: Create tokio runtime (module owns it, transport borrows handle).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(transport_threads())
        .thread_name("lo-transport")
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    RUNTIME.set(rt).ok();

    // Step 1: Transport::init() — discover EFA devices (may fail gracefully).
    transport::init();

    // Step 2: Initialize DRAMPool + NVMePool with configured sizes.
    let mode = operating_mode();
    let nvme_size = nvme_pool_size();
    let dram_size = dram_pool_size();
    storage::init(nvme_size, dram_size, &dir);

    // Step 3: Register all segments with io_uring.
    storage::register_buffers();

    // Step 4: Transport::register_buffers() — fi_mr_reg per segment.
    let slices = storage::all_segment_slices();
    let slice_refs: Vec<&[u8]> = slices.to_vec();
    transport::register_buffers(&slice_refs);

    ctx.log_notice(&format!(
        "largeobj: initialized mode={:?} data_dir={} nvme_pool={}MB dram_pool={}MB transport_threads={}",
        mode,
        dir,
        nvme_size / (1024 * 1024),
        dram_size / (1024 * 1024),
        transport_threads(),
    ));

    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    transport::deregister_buffers();
    transport::shutdown();
    storage::shutdown();
    Status::Ok
}

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [LO_TYPE],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["LO.HELLO", commands::lo_hello, "write", 0, 0, 0],
        ["LO.GET", commands::lo_get, "readonly", 1, 1, 1],
        ["LO.SET", commands::lo_set, "write deny-oom", 1, 1, 1],
    ],
    configurations: [
        i64: [
            ["nvme-pool-size", &*CFG_NVME_POOL_SIZE, 536_870_912, 1_048_576, 1_099_511_627_776,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["dram-pool-size", &*CFG_DRAM_POOL_SIZE, 536_870_912, 1_048_576, 1_099_511_627_776,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["max-bytes", &*CFG_MAX_BYTES, 0, 0, i64::MAX,
             ConfigurationFlags::MEMORY, None, None],
            ["transport-threads", &*CFG_TRANSPORT_THREADS, 2, 1, 32,
             ConfigurationFlags::IMMUTABLE, None, None],
        ],
        string: [
            ["data-dir", &*CFG_DATA_DIR, "", ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [
            ["bench-mode", &*CFG_BENCH_MODE, false, ConfigurationFlags::DEFAULT, None],
            ["direct-io", &*CFG_DIRECT_IO, true, ConfigurationFlags::IMMUTABLE, None],
        ],
        enum: [
            ["operating-mode", &*CFG_OPERATING_MODE, OperatingMode::Tiered,
             ConfigurationFlags::IMMUTABLE, None],
        ],
        module_args_as_configuration: true,
    ]
}
