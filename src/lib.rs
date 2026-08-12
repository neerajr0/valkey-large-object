//! valkey-bigobj: NVMe-backed storage module for large immutable objects (KV cache tensors).

use valkey_module::{valkey_module, Context, Status, ValkeyString};

pub mod commands;
pub mod data_type;
pub mod storage;
pub mod uring_engine;

use crate::data_type::BIGOBJ_TYPE;
use crate::storage::engine;

pub const MODULE_NAME: &str = "bigobj";
pub const MODULE_VERSION: i32 = 1;

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    let mut max_bytes: u64 = 1024 * 1024 * 1024;
    let mut data_dir = String::new();
    let mut pool_buf_size: usize = 65536;  // 64KB default
    let mut pool_buf_count: usize = 1024;
    let mut keep_read_fds: bool = true;    // hold 1 read fd per object (default)

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].to_string_lossy();
        match arg.as_ref() {
            "max-bytes" => {
                i += 1;
                if i < args.len() {
                    if let Ok(v) = args[i].to_string_lossy().parse::<u64>() {
                        max_bytes = v;
                    }
                }
            }
            "data-dir" => {
                i += 1;
                if i < args.len() {
                    data_dir = args[i].to_string_lossy().to_string();
                }
            }
            "pool-buf-size" => {
                i += 1;
                if i < args.len() {
                    if let Ok(v) = args[i].to_string_lossy().parse::<usize>() {
                        pool_buf_size = v;
                    }
                }
            }
            "pool-buf-count" => {
                i += 1;
                if i < args.len() {
                    if let Ok(v) = args[i].to_string_lossy().parse::<usize>() {
                        pool_buf_count = v;
                    }
                }
            }
            "keep-read-fds" => {
                i += 1;
                if i < args.len() {
                    let v = args[i].to_string_lossy();
                    keep_read_fds = matches!(v.as_ref(), "1" | "true" | "yes");
                }
            }
            _ => {}
        }
        i += 1;
    }

    if data_dir.is_empty() {
        ctx.log_warning("bigobj: data-dir is required");
        return Status::Err;
    }

    // Validate pool-buf-size is 4KB aligned
    if pool_buf_size % 4096 != 0 {
        ctx.log_warning("bigobj: pool-buf-size must be 4KB aligned");
        return Status::Err;
    }

    // Initialize storage engine.
    let dir = std::path::PathBuf::from(&data_dir);
    engine::init_engine(dir, max_bytes, keep_read_fds);

    // Initialize io_uring poller with configured buffer pool.
    uring_engine::init(pool_buf_size, pool_buf_count);

    ctx.log_notice(&format!(
        "bigobj: initialized data_dir={} max_bytes={} pool_buf_size={} pool_buf_count={} keep_read_fds={} (pool={}MB)",
        data_dir, max_bytes, pool_buf_size, pool_buf_count, keep_read_fds,
        (pool_buf_size * pool_buf_count) / (1024 * 1024)
    ));

    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [BIGOBJ_TYPE],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["BO.SET", commands::bo_set, "write deny-oom", 1, 1, 1],
        ["BO.GET", commands::bo_get, "readonly", 1, 1, 1],
        ["BO.DEL", commands::bo_del, "write", 1, 1, 1],
        ["BO.EXISTS", commands::bo_exists, "readonly fast", 1, 1, 1],
        ["BO.LEN", commands::bo_len, "readonly fast", 1, 1, 1],
        ["BO.INFO", commands::bo_info, "readonly", 0, 0, 0],
        ["BO.EVICT", commands::bo_evict, "write", 1, 1, 1],
    ],
}
