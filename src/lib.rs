//! valkey-bigobj: NVMe-backed storage module for large immutable objects (KV cache tensors).
//!
//! Single module architecture:
//!   - data_type: Valkey data type registration (BoValue in keyspace)
//!   - storage/nvme: NVMe file I/O with pre-opened fd pool
//!   - storage/engine: thin wrapper (object store + NVMe backend)
//!   - uring_engine: io_uring poller for async reads
//!   - commands: BO.SET/GET/DEL/EXISTS/LEN/INFO/EVICT

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
    let mut max_bytes: u64 = 1024 * 1024 * 1024; // 1 GB default
    let mut data_dir = String::new();

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
            _ => {}
        }
        i += 1;
    }

    if data_dir.is_empty() {
        ctx.log_warning("bigobj: data-dir is required");
        return Status::Err;
    }

    // Initialize storage engine (NVMe backend + object store).
    let dir = std::path::PathBuf::from(&data_dir);
    engine::init_engine(dir.clone(), max_bytes);

    // Initialize the io_uring poller thread.
    uring_engine::init();

    ctx.log_notice(&format!(
        "bigobj: initialized data_dir={} max_bytes={}",
        data_dir, max_bytes
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
