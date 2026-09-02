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
//   2. storage::init(mode, dram_segment_count, dram_seg_size, nvme_staging, nvme_dir)
//                              — allocate pool segments, create DRAMPool + NVMePool.
//      (In Tiered mode, nvme_dir is reset beforehand — created if absent, wiped
//       clean of any object files a previous run left behind after an unclean exit.)
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
use valkey_module_macros::shutdown_event_handler;

use tokio::runtime::Runtime;

pub mod commands;
pub mod data_type;
pub mod engine;
pub mod errors;
pub mod storage;
pub mod transport;

use crate::data_type::LO_TYPE;

use valkey_module::enum_configuration;

enum_configuration! {
    /// Operating mode — set via `operating-mode` module enum config.
    /// Dram (default): all objects live exclusively in DRAMPool. No NVMe.
    /// Tiered: objects persist on NVMe, DRAMPool is a read cache with promotion.
    #[derive(Debug, PartialEq, Eq, Copy)]

    pub enum OperatingMode {
        Dram = 0,
        Tiered = 1,
    }
}

pub const MODULE_NAME: &str = "largeobj";
pub const MODULE_VERSION: i32 = 1;

// ─── Module Configurations (ValkeyModule Config API) ─────────────────────────

lazy_static::lazy_static! {
    /// Data directory for NVMe object files. Required. Immutable after load.
    static ref CFG_NVME_DIR: Mutex<String> = Mutex::new(String::new());

    /// Size of the single NVMe staging segment (DRAM for I/O buffers).
    /// Used in Tiered mode for read/write staging. Default: 64MB.
    /// Immutable after load. Always 1 segment of this size.
    static ref CFG_NVME_STAGING_SIZE: AtomicI64 = AtomicI64::new(64 * 1024 * 1024);

    /// Total DRAM budget for cached objects. 0 = no limit (grow on demand).
    /// In Dram mode: all objects live here. In Tiered mode: promotion cache.
    static ref CFG_DRAM_MAXMEMORY: AtomicI64 = AtomicI64::new(0);

    /// Size of each DRAMPool segment. Growth unit when dram-maxmemory=0.
    /// Default: 64MB. Immutable after load.
    static ref CFG_DRAM_SEGMENT_SIZE: AtomicI64 = AtomicI64::new(64 * 1024 * 1024);

    /// Max disk usage in nvme-dir. Default: 10GB.
    static ref CFG_NVME_MAXMEMORY: AtomicI64 = AtomicI64::new(10 * 1024 * 1024 * 1024);

    /// Number of tokio worker threads for transport CQ polling. Immutable after load.
    static ref CFG_WORKER_THREADS: AtomicI64 = AtomicI64::new(2);

    /// Max object size eligible for DRAMPool promotion (Tiered mode).
    /// Objects larger than this skip promotion and are always served from NVMe.
    /// Default: 256MB. Supports memory notation (e.g., "256mb").
    static ref CFG_MAX_PROMOTE_SIZE: AtomicI64 = AtomicI64::new(256 * 1024 * 1024);

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
    /// - Dram (0): all objects live exclusively in DRAMPool. No NVMe. Fastest reads.
    /// - Tiered (1): objects persist on NVMe, DRAMPool is a read cache with promotion.
    static ref CFG_OPERATING_MODE: Mutex<OperatingMode> = Mutex::new(OperatingMode::Dram);
}

// ─── Global Runtime ──────────────────────────────────────────────────────────

/// Tokio runtime — owned by the module, handle passed to transport crate.
static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();

pub fn runtime_handle() -> &'static tokio::runtime::Handle {
    RUNTIME.get().expect("runtime not initialized").handle()
}

// ─── Config Accessors ────────────────────────────────────────────────────────

pub fn nvme_dir() -> String {
    CFG_NVME_DIR
        .lock()
        .expect("CFG_NVME_DIR lock unavailable")
        .clone()
}

pub fn nvme_staging_size() -> usize {
    CFG_NVME_STAGING_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn dram_maxmemory() -> u64 {
    CFG_DRAM_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn dram_segment_size() -> usize {
    CFG_DRAM_SEGMENT_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn nvme_maxmemory() -> u64 {
    CFG_NVME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn worker_threads() -> usize {
    CFG_WORKER_THREADS.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_promote_size() -> u64 {
    CFG_MAX_PROMOTE_SIZE.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn bench_mode() -> bool {
    CFG_BENCH_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn direct_io() -> bool {
    CFG_DIRECT_IO.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn operating_mode() -> OperatingMode {
    *CFG_OPERATING_MODE
        .lock()
        .expect("CFG_OPERATING_MODE lock unavailable")
}

// ─── Module Lifecycle ────────────────────────────────────────────────────────

fn initialize(ctx: &Context, _args: &[ValkeyString]) -> Status {
    // Any panic on any thread (main, tokio, io_uring poller) aborts the server.
    // We are a no-panic codebase — a panic means a bug, not a recoverable condition.
    std::panic::set_hook(Box::new(|info| {
        valkey_module::logging::log_warning(format!(
            "largeobj: fatal panic — aborting server: {}",
            info
        ));
        std::process::abort();
    }));

    // Configs are already populated by the valkey_module! macro via module_args_as_configuration.
    let mode = operating_mode();
    let dir = nvme_dir();

    // Reset nvme-dir before use (Tiered mode only; a no-op in Dram, which never
    // touches disk): reclaim any object files a previous run left behind after an
    // unclean exit — a hard crash / SIGKILL never reaches our shutdown handler.
    // If the reset fails we can't guarantee a clean slate, so refuse to load
    // rather than start dirty.
    if let Err(e) = storage::validate_and_clean_nvme_dir(mode, &dir) {
        ctx.log_warning(&format!(
            "largeobj: startup failed to reset nvme-dir {}: {}; aborting module load",
            dir, e
        ));
        return Status::Err;
    }

    // Step 0: Create tokio runtime (module owns it, transport borrows handle).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .thread_name("largeobj")
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    if RUNTIME.set(rt).is_err() {
        panic!("Runtime already initialized");
    }

    // Step 1: Transport::init() — discover EFA devices (may fail gracefully).
    transport::init();

    // Step 2: Initialize DRAMPool + NVMePool with configured sizes.
    let dram_seg_size = dram_segment_size();
    let dram_max = dram_maxmemory();
    let nvme_staging = nvme_staging_size();

    // DRAMPool segment count: if maxmemory=0, start with 1 segment (grow later).
    // Otherwise pre-allocate maxmemory / segment_size segments.
    let dram_segment_count = if dram_max == 0 {
        1
    } else {
        ((dram_max as usize) / dram_seg_size).max(1)
    };

    // Total registered iovecs (DRAM + NVMe) must fit in u16 for io_uring IORING_REGISTER_BUFFERS.
    // NVMe staging is always 1 segment in Tiered mode, 0 in Dram mode.
    let nvme_segments: usize = if mode == OperatingMode::Tiered { 1 } else { 0 };
    let total_segments = dram_segment_count + nvme_segments;
    if total_segments > u16::MAX as usize + 1 {
        ctx.log_warning(&format!(
            "largeobj: too many segments ({} DRAM + {} NVMe = {}). \
             Max {} (io_uring iovec_index is u16). \
             Increase dram-segment-size or decrease dram-maxmemory",
            dram_segment_count,
            nvme_segments,
            total_segments,
            u16::MAX as usize + 1,
        ));
        return Status::Err;
    }

    storage::init(mode, dram_segment_count, dram_seg_size, nvme_staging, &dir);

    // Step 3: Register all segments with io_uring.
    storage::register_buffers();

    // Step 4: Transport::register_buffers() — fi_mr_reg per segment.
    let slices = storage::all_segment_slices();
    let slice_refs: Vec<&[u8]> = slices.to_vec();
    if let Err(e) = transport::register_buffers(&slice_refs) {
        ctx.log_warning(&format!("largeobj: EFA buffer registration failed: {}", e));
        return Status::Err;
    }

    ctx.log_notice(&format!(
        "largeobj: initialized mode={:?} nvme_dir={} dram_segments={}x{}MB nvme_staging={}MB",
        mode,
        dir,
        dram_segment_count,
        dram_seg_size / (1024 * 1024),
        nvme_staging / (1024 * 1024),
    ));

    Status::Ok
}

/// MODULE UNLOAD entry point.
///
/// NOTE: modules that define data types cannot be unloaded by Valkey Core, so
/// this function is not called (this module defines LO_TYPE). All teardown
/// therefore lives in `on_server_shutdown`, which fires on graceful server
/// shutdown.
fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

/// Tear down on graceful server shutdown (SIGINT / SIGTERM / SHUTDOWN command):
/// release transport resources, stop the io_uring poller, and — in Tiered mode —
/// wipe nvme-dir so object files don't accumulate on the SSD across server
/// lifetimes. Logs on cleanup failure but never blocks shutdown. A hard crash
/// (SIGKILL / SIGSEGV / power loss) never reaches this handler; those leftovers
/// are reclaimed by the startup reset in `initialize`.
#[shutdown_event_handler]
fn on_server_shutdown(ctx: &Context, _subevent: u64) {
    transport::deregister_buffers();
    transport::shutdown();
    storage::shutdown(); // stop the io_uring poller
    let dir = nvme_dir();
    if let Err(e) = storage::validate_and_clean_nvme_dir(operating_mode(), &dir) {
        ctx.log_warning(&format!(
            "largeobj: shutdown cleanup failed to reset nvme-dir {}: {}",
            dir, e
        ));
    }
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
            ["dram-maxmemory", &*CFG_DRAM_MAXMEMORY, 0, 0, i64::MAX,
             ConfigurationFlags::MEMORY, None, None],
            ["dram-segment-size", &*CFG_DRAM_SEGMENT_SIZE, 67_108_864, 1_048_576, i64::MAX,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["nvme-staging-size", &*CFG_NVME_STAGING_SIZE, 67_108_864, 1_048_576, i64::MAX,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["nvme-maxmemory", &*CFG_NVME_MAXMEMORY, 10_737_418_240, 1_048_576, i64::MAX,
             ConfigurationFlags::MEMORY, None, None],
            ["worker-threads", &*CFG_WORKER_THREADS, 2, 1, 32,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["max-promote-size", &*CFG_MAX_PROMOTE_SIZE, 268_435_456, 0, 1_099_511_627_776,
             ConfigurationFlags::MEMORY, None, None],
        ],
        string: [
            ["nvme-dir", &*CFG_NVME_DIR, "", ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [
            ["bench-mode", &*CFG_BENCH_MODE, false, ConfigurationFlags::DEFAULT, None],
            ["direct-io", &*CFG_DIRECT_IO, true, ConfigurationFlags::IMMUTABLE, None],
        ],
        enum: [
            ["operating-mode", &*CFG_OPERATING_MODE, OperatingMode::Dram,
             ConfigurationFlags::IMMUTABLE, None],
        ],
        module_args_as_configuration: true,
    ]
}
