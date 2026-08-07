//! valkey-bigobj: NVMe/DRAM tiered storage module for large immutable objects.
//!
//! Architecture:
//!   - Module B (this): owns the data type + storage engine + shared API export
//!   - Module A (vrdma/transport): discovers this API via GetSharedAPI, handles
//!     RDMA reads (pin/unpin/memory_region) and replication (subscribe to mutations)
//!
//! v1: DRAM-only mode. All objects in memory. NVMe path is a future addition.

use valkey_module::{
    valkey_module, Context, Status, ValkeyResult, ValkeyString,
};

pub mod commands;
pub mod data_type;
pub mod storage;
pub mod threadpool;

use crate::data_type::BIGOBJ_TYPE;
use crate::storage::engine::{self, EngineConfig, StorageMode};
use crate::storage::export::API_TABLE;
use crate::storage::api::BIGOBJ_STORAGE_API_NAME;

pub const MODULE_NAME: &str = "bigobj";
pub const MODULE_VERSION: i32 = 1;

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    // Parse module load args: mode=dram|nvme max-bytes=N data-dir=PATH
    let mut mode = StorageMode::DramOnly;
    let mut max_bytes: u64 = 1024 * 1024 * 1024; // 1 GB default
    let mut data_dir = std::path::PathBuf::from("/tmp/bigobj-data");

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].to_string_lossy();
        match arg.as_ref() {
            "mode" => {
                i += 1;
                if i < args.len() {
                    match args[i].to_string_lossy().as_ref() {
                        "dram" => mode = StorageMode::DramOnly,
                        "nvme" => mode = StorageMode::NvmeTiered,
                        _ => {
                            ctx.log_warning("bigobj: invalid mode, use 'dram' or 'nvme'");
                            return Status::Err;
                        }
                    }
                }
            }
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
                    data_dir = std::path::PathBuf::from(args[i].to_string_lossy().to_string());
                }
            }
            _ => {}
        }
        i += 1;
    }

    // Initialize the storage engine.
    engine::init_engine(EngineConfig { mode, max_bytes, data_dir: data_dir.clone() });

    // Initialize the worker thread pool (500 threads for blocking I/O).
    threadpool::init_pool(500);

    // Export the shared API so the transport module can discover it.
    // Safety: API_TABLE is a static with 'static lifetime.
    let api_ptr = &API_TABLE as *const _ as *const std::os::raw::c_void;
    let api_name = BIGOBJ_STORAGE_API_NAME.as_ptr() as *const std::os::raw::c_char;

    // Call ValkeyModule_ExportSharedAPI(ctx, name, ptr)
    // This is available via the raw module API.
    unsafe {
        let raw_ctx = ctx.get_raw();
        let export_fn = valkey_module::raw::RedisModule_ExportSharedAPI.unwrap();
        let result = export_fn(raw_ctx, api_name, api_ptr as *mut std::os::raw::c_void);
        if result != 0 {
            ctx.log_warning("bigobj: failed to export shared API");
            return Status::Err;
        }
    }

    ctx.log_notice(&format!(
        "bigobj: initialized mode={:?} max_bytes={}",
        mode, max_bytes
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
        ["BO.GETRANGE", commands::bo_getrange, "readonly", 1, 1, 1],
    ],
}
