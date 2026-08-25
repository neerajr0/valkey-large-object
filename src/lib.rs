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
//   2. storage::init(buf_size, buf_count, fd_pool_capacity, data_dir)
//                              — allocate pool buffers, create StorageEngine.
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
pub mod errors;
pub mod storage;
pub mod transport;

use crate::data_type::LO_TYPE;

pub const MODULE_NAME: &str = "largeobj";
pub const MODULE_VERSION: i32 = 1;

// ─── Module Configurations (ValkeyModule Config API) ─────────────────────────

lazy_static::lazy_static! {
    /// Data directory for NVMe object files. Required. Immutable after load.
    static ref CFG_DATA_DIR: Mutex<String> = Mutex::new(String::new());

    /// Buffer pool slot size in bytes. Must be 4KB-aligned. Immutable after load.
    /// Default: 4MB (suitable for KV cache chunks).
    static ref CFG_POOL_BUF_SIZE: AtomicI64 = AtomicI64::new(4 * 1024 * 1024);

    /// Number of buffer pool slots. Immutable after load.
    /// Default: 512 (512 * 4MB = 2GB pool).
    static ref CFG_POOL_BUF_COUNT: AtomicI64 = AtomicI64::new(512);

    /// Maximum total bytes on NVMe. 0 = unlimited. Supports memory notation (e.g. "10gb").
    static ref CFG_MAX_BYTES: AtomicI64 = AtomicI64::new(0);

    /// Number of tokio worker threads for transport CQ polling. Immutable after load.
    static ref CFG_TRANSPORT_THREADS: AtomicI64 = AtomicI64::new(2);

    /// Max number of read fds the fd-pool caches. Immutable after load.
    /// 0 = auto: derive from the process RLIMIT_NOFILE soft limit at load (see
    /// `fd_pool_capacity`). Set a positive value to pin the cap explicitly.
    static ref CFG_FD_POOL_SIZE: AtomicI64 = AtomicI64::new(0);

    /// Bench mode: LO.GET TCP path replies with size integer instead of bulk value bytes.
    /// For benchmarking NVMe read throughput without TCP output buffer overhead.
    static ref CFG_BENCH_MODE: AtomicBool = AtomicBool::new(false);

    /// Direct I/O mode: when enabled, file opens use O_DIRECT to bypass the kernel page cache.
    ///
    /// WHY: Large objects (4KB-50MB) would thrash the page cache if buffered. O_DIRECT ensures
    /// NVMe reads/writes go straight to/from our pre-aligned pool buffers without kernel copies.
    /// Our PinnedBuffer allocations are 4KB-aligned, satisfying O_DIRECT alignment requirements.
    ///
    /// WHEN TO DISABLE: Set to "no" when O_DIRECT writes fail with EINVAL on the target
    /// environment. Known case: ASAN builds with GCC standalone toolchains where the sanitizer's
    /// allocator interacts differently with io_uring O_DIRECT buffer alignment validation.
    /// Not needed on production (XFS/NVMe instance store) or standard CI (ubuntu-latest).
    ///
    /// DEFAULT: yes (production path — always use O_DIRECT on XFS/NVMe instance store).
    static ref CFG_DIRECT_IO: AtomicBool = AtomicBool::new(true);
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

pub fn pool_buf_size() -> usize {
    CFG_POOL_BUF_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn pool_buf_count() -> usize {
    CFG_POOL_BUF_COUNT.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_bytes() -> u64 {
    CFG_MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn transport_threads() -> usize {
    CFG_TRANSPORT_THREADS.load(std::sync::atomic::Ordering::Relaxed) as usize
}

/// Resolve the effective fd-pool capacity.
///
/// If `fd-pool-size` is configured (> 0) that value wins. Otherwise (0 = auto) we size
/// the pool from what the *core* says is available, so the read-fd cache can never push
/// total open fds past `RLIMIT_NOFILE`:
///
///   free   = nofile_soft − maxclients − CONFIG_FDSET_INCR
///   pool   = 75% of free   (the remaining 25% covers the module's own transient fds:
///                           LO.SET write fds, io_uring rings, EFA transport)
///
/// `maxclients` is read live from the core (INFO `clients`); Valkey has already
/// reconciled it against the OS limit in `adjustOpenFilesLimit()` and reserves
/// `CONFIG_FDSET_INCR` (32 + 96) more for its event loop. Reserving that budget means
/// the pool only claims fds Valkey's own clients/internals won't. Falls back to
/// [`storage::fd_pool::DEFAULT_CAPACITY`] if the OS limit can't be read or is unbounded.
pub fn fd_pool_capacity() -> usize {
    use crate::storage::fd_pool::DEFAULT_CAPACITY;

    let configured = CFG_FD_POOL_SIZE.load(std::sync::atomic::Ordering::Relaxed);
    if configured > 0 {
        return configured as usize;
    }

    const FLOOR: u64 = 1024;
    // Valkey keeps CONFIG_FDSET_INCR (CONFIG_MIN_RESERVED_FDS 32 + 96) fds beyond
    // maxclients for its event loop; mirror that reservation here.
    const VALKEY_FDSET_INCR: u64 = 128;
    // Valkey's default maxclients — used only if the live value can't be read, so we
    // still reserve a client budget rather than risk EMFILE.
    const DEFAULT_MAXCLIENTS: u64 = 10_000;

    let soft = match nofile_soft_limit() {
        Some(n) => n,
        None => return DEFAULT_CAPACITY, // unreadable or unlimited → conservative fallback
    };

    // Reserve the fds the core has already carved out for client connections + its own
    // event loop; the pool may use only what's left.
    let reserved = valkey_maxclients()
        .unwrap_or(DEFAULT_MAXCLIENTS)
        .saturating_add(VALKEY_FDSET_INCR);
    let free = soft.saturating_sub(reserved);

    // 75% of the remaining fds, floored so tiny limits still cache something.
    let usable = (free / 4 * 3).max(FLOOR);
    usable as usize
}

/// Live `maxclients` from the core (INFO `clients` section). Valkey has already
/// clamped this against `RLIMIT_NOFILE` in `adjustOpenFilesLimit()`, so it is the
/// authoritative count of fds reserved for client connections. `None` if the field
/// can't be read. Safe crate wrapper — no `unsafe` in our code.
fn valkey_maxclients() -> Option<u64> {
    valkey_module::ServerInfo::new("clients").field_unsigned("maxclients")
}

/// Read the open-file soft limit (`RLIMIT_NOFILE`) from `/proc/self/limits`.
///
/// Pure safe std I/O — no `libc`/`unsafe`. The module is Linux-only (io_uring), so
/// `/proc/self/limits` is always available. Returns `None` if the file can't be read,
/// the line is missing, or the limit is `unlimited`.
fn nofile_soft_limit() -> Option<u64> {
    // Format (whitespace-padded columns):
    //   Limit             Soft Limit   Hard Limit   Units
    //   Max open files    1024         1048576      files
    let contents = std::fs::read_to_string("/proc/self/limits").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("Max open files") {
            // Soft limit is the first token after the label.
            let soft = rest.split_whitespace().next()?;
            if soft == "unlimited" {
                return None;
            }
            return soft.parse::<u64>().ok();
        }
    }
    None
}

pub fn bench_mode() -> bool {
    CFG_BENCH_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn direct_io() -> bool {
    CFG_DIRECT_IO.load(std::sync::atomic::Ordering::Relaxed)
}

// ─── Config Validators ───────────────────────────────────────────────────────

use valkey_module::configuration::ConfigurationContext;
use valkey_module::configuration::ConfigurationValue;
use valkey_module::ValkeyError;

fn validate_pool_buf_size<T: ConfigurationValue<i64>>(
    config_ctx: &ConfigurationContext,
    _name: &str,
    val: &'static T,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx);
    if v < 4096 {
        return Err(ValkeyError::Str("pool-buf-size must be at least 4096"));
    }
    if !(v as usize).is_multiple_of(4096) {
        return Err(ValkeyError::Str("pool-buf-size must be 4KB aligned"));
    }
    Ok(())
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

    // Step 2+3: Storage allocates buffer pool + register with io_uring.
    let fd_cap = fd_pool_capacity();
    storage::init(pool_buf_size(), pool_buf_count(), fd_cap, &dir);
    storage::register_buffers();

    // Step 4: Transport::register_buffers() — fi_mr_reg same buffers.
    let pinned = storage::pinned_buffers();
    let slices: Vec<&[u8]> = pinned.iter().map(|pb| pb.as_slice()).collect();
    transport::register_buffers(&slices);

    ctx.log_notice(&format!(
        "largeobj: initialized data_dir={} pool={}x{}={:.0}MB transport_threads={} fd_pool_capacity={}",
        dir,
        pool_buf_count(),
        pool_buf_size(),
        (pool_buf_count() * pool_buf_size()) as f64 / (1024.0 * 1024.0),
        transport_threads(),
        fd_cap,
    ));

    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    transport::deregister_buffers();
    transport::shutdown();
    storage::deregister_buffers();
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
            ["pool-buf-size", &*CFG_POOL_BUF_SIZE, 4_194_304, 4096, 1_073_741_824,
             ConfigurationFlags::IMMUTABLE, None, Some(Box::new(validate_pool_buf_size::<AtomicI64>))],
            ["pool-buf-count", &*CFG_POOL_BUF_COUNT, 512, 1, 65536,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["max-bytes", &*CFG_MAX_BYTES, 0, 0, i64::MAX,
             ConfigurationFlags::MEMORY, None, None],
            ["transport-threads", &*CFG_TRANSPORT_THREADS, 2, 1, 32,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["fd-pool-size", &*CFG_FD_POOL_SIZE, 0, 0, i64::MAX,
             ConfigurationFlags::IMMUTABLE, None, None],
        ],
        string: [
            ["data-dir", &*CFG_DATA_DIR, "", ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [
            ["bench-mode", &*CFG_BENCH_MODE, false, ConfigurationFlags::DEFAULT, None],
            ["direct-io", &*CFG_DIRECT_IO, true, ConfigurationFlags::IMMUTABLE, None],
        ],
        enum: [],
        module_args_as_configuration: true,
    ]
}
