//! Data Type Layer — LoValue stored in Valkey keyspace.
//!
//! The data type layer owns:
//! - LoValue struct in keyspace (accessed via ValkeyModule_OpenKey)
//! - OID generation (monotonic counter)
//! - RDB callbacks (save/load references)
//! - TIERING.REF replication (TODO)
//! - Native Valkey DEL triggers free callback → deletes NVMe file.

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::native_types::ValkeyType;
use valkey_module::raw;

// ─── ObjectId ────────────────────────────────────────────────────────────────

/// ObjectId IS the file path: deterministic mapping OID → "{data_dir}/{oid:016x}.dat"
/// No lookup table. Compact u64 safe for replication streams, RDB, and LoValue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);

/// Monotonic OID counter. Each node generates unique IDs independently.
static OID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ObjectId {
    pub fn next() -> Self {
        Self(OID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Deterministic file path from OID.
    pub fn file_path(&self, data_dir: &str) -> String {
        format!("{}/{:016x}.dat", data_dir, self.0)
    }

    /// Initialize OID counter to at least `val`.
    /// Used at startup after scanning data_dir for existing .dat files.
    /// Ensures new OIDs never collide with on-disk objects surviving a restart.
    pub fn init_counter(val: u64) {
        OID_COUNTER.fetch_max(val, Ordering::Relaxed);
    }
}

// ─── LoValue ─────────────────────────────────────────────────────────────────

/// LoValue — the Valkey data type value struct, stored in Valkey's keyspace.
/// Accessed via ValkeyModule_OpenKey → ModuleTypeGetValue on the main thread.
/// This is NOT in the storage layer. Command handlers read this to get file info
/// before calling storage.
#[derive(Debug, Clone)]
pub struct LoValue {
    pub object_id: ObjectId, // monotonic per-node OID (used as filename)
    pub len: u64,            // object size in bytes
    pub crc32c: u32,         // integrity checksum (verified on replication pull)
}

// ─── RDB Callbacks ───────────────────────────────────────────────────────────

unsafe extern "C" fn lo_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut std::ffi::c_void) {
    // SAFETY: value is a valid LoValue pointer created by us in rdb_load or set_value.
    // rdb is a valid RedisModuleIO context provided by the engine during RDB save.
    let lo = &*(value as *const LoValue);
    raw::save_unsigned(rdb, lo.object_id.0);
    raw::save_unsigned(rdb, lo.len);
    raw::save_unsigned(rdb, lo.crc32c as u64);
}

unsafe extern "C" fn lo_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    _encver: i32,
) -> *mut std::ffi::c_void {
    // SAFETY: rdb is a valid RedisModuleIO context provided by the engine during RDB load.
    // We allocate LoValue on the heap and return ownership to the engine.
    let oid = raw::load_unsigned(rdb).unwrap_or(0);
    let len = raw::load_unsigned(rdb).unwrap_or(0);
    let crc = raw::load_unsigned(rdb).unwrap_or(0) as u32;

    // Update OID counter to avoid collisions after RDB load.
    OID_COUNTER.fetch_max(oid + 1, Ordering::Relaxed);

    let lo = Box::new(LoValue {
        object_id: ObjectId(oid),
        len,
        crc32c: crc,
    });
    Box::into_raw(lo) as *mut std::ffi::c_void
}

/// Free callback — triggered by native Valkey DEL.
/// Deletes the NVMe file for this object.
unsafe extern "C" fn lo_free(value: *mut std::ffi::c_void) {
    // SAFETY: value is a valid LoValue pointer that we previously returned from
    // rdb_load or set_value. We take ownership back and drop it after deleting the file.
    let lo = Box::from_raw(value as *mut LoValue);
    // Delete NVMe file via storage layer.
    crate::storage::delete(lo.object_id);
}

// ─── Type Registration ───────────────────────────────────────────────────────

pub static LO_TYPE: ValkeyType = ValkeyType::new(
    "largeob-k", // 9 char type name
    0,           // encoding version
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(lo_rdb_load),
        rdb_save: Some(lo_rdb_save),
        aof_rewrite: None,          // TODO
        free: Some(lo_free),
        mem_usage: None,            // TODO
        digest: None,               // TODO
        aux_load: None,             // TODO
        aux_save: None,             // TODO
        aux_save2: None,
        aux_save_triggers: 0,
        free_effort: None,          // TODO
        unlink: None,
        copy: None,                 // TODO
        defrag: None,
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);
