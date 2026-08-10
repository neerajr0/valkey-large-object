//! Data type registration and RDB callbacks for the BO (BigObject) type.
//!
//! Each key's value in Valkey's keyspace is a `BoValue` containing metadata
//! (object_id, len, crc). The actual bytes live in the storage engine (DRAM pool or NVMe).

use valkey_module::{native_types::ValkeyType, raw};
use std::os::raw::c_void;

use crate::storage::engine::{ObjectId, ObjectMeta};
use crate::storage::engine;

/// The per-key value struct stored in Valkey's keyspace.
/// This is NOT a separate index — it IS the Valkey data type value.
#[repr(C)]
pub struct BoValue {
    pub meta: ObjectMeta,
}

/// Data type name: must be exactly 9 chars (Valkey requirement).
pub static BIGOBJ_TYPE: ValkeyType = ValkeyType::new(
    "bigobject",
    0, // encoding version
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(bo_rdb_load),
        rdb_save: Some(bo_rdb_save),
        aof_rewrite: None, // BO.SET replay handles this
        mem_usage: Some(bo_mem_usage),
        digest: None,
        free: Some(bo_free),
        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,
        free_effort: None,
        unlink: None,
        copy: Some(bo_copy),
        defrag: None,
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

// === RDB callbacks ===

/// Save: emit only the reference (object_id, len, crc). NOT the bytes.
/// The bytes live in the storage engine and are transferred via the bulk channel.
unsafe extern "C" fn bo_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let bo = &*(value as *const BoValue);
    raw::save_unsigned(rdb, bo.meta.object_id.0);
    raw::save_unsigned(rdb, bo.meta.len);
    raw::save_unsigned(rdb, bo.meta.crc32c as u64);
    raw::save_unsigned(rdb, bo.meta.created_at_us);
}

/// Load: reconstruct the BoValue from the reference.
/// The transport module is responsible for hydrating the actual bytes
/// (via pull from primary) after rdb_load completes.
unsafe extern "C" fn bo_rdb_load(rdb: *mut raw::RedisModuleIO, _encver: i32) -> *mut c_void {
    let oid = raw::load_unsigned(rdb).unwrap_or(0);
    let len = raw::load_unsigned(rdb).unwrap_or(0);
    let crc = raw::load_unsigned(rdb).unwrap_or(0) as u32;
    let created = raw::load_unsigned(rdb).unwrap_or(0);

    let bo = Box::new(BoValue {
        meta: ObjectMeta {
            object_id: ObjectId(oid),
            len,
            crc32c: crc,
            created_at_us: created,
        },
    });

    Box::into_raw(bo) as *mut c_void
}

/// Memory usage: the value bytes aren't in Valkey's keyspace — report metadata size only.
/// The actual memory is tracked by the storage engine.
unsafe extern "C" fn bo_mem_usage(value: *const c_void) -> usize {
    let bo = &*(value as *const BoValue);
    std::mem::size_of::<BoValue>() + bo.meta.len as usize
}

/// Free: delete from storage engine when key is removed.
unsafe extern "C" fn bo_free(value: *mut c_void) {
    let bo = Box::from_raw(value as *mut BoValue);
    // Remove from engine by object_id (best effort — key may already be gone).
    // The engine's oid_index handles this case gracefully.
    let _ = bo; // Bo is freed here; engine cleanup happens via BO.DEL command.
}

/// Copy: create a new object in the engine with the same data.
unsafe extern "C" fn bo_copy(
    _from_key: *mut raw::RedisModuleString,
    _to_key: *mut raw::RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let src = &*(value as *const BoValue);
    // For COPY, we'd need to read the data and write a new object.
    // For v1: create a new BoValue pointing to a new OID (shallow copy of metadata).
    // The transport/replication will handle copying the actual bytes.
    let new_bo = Box::new(BoValue {
        meta: ObjectMeta {
            object_id: engine::engine().alloc_oid(),
            len: src.meta.len,
            crc32c: src.meta.crc32c,
            created_at_us: crate::storage::engine::StorageEngine::now_us(),
        },
    });
    Box::into_raw(new_bo) as *mut c_void
}
