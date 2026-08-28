//! Data Type Layer — LoValue stored in Valkey keyspace.
//!
//! The data type layer owns:
//! - LoValue struct in keyspace (accessed via ValkeyModule_OpenKey)
//! - OID generation (monotonic counter)
//! - RDB callbacks (save/load references) (TODO)
//! - Replication (TODO)
//! - Native Valkey DEL triggers free callback → deletes NVMe file.
//! - Callbacks: MEMORY USAGE, FREE EFFORT, COPY, DEBUG DIGEST.

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::digest::Digest;
use valkey_module::native_types::ValkeyType;
use valkey_module::raw;

// ─── ObjectId ────────────────────────────────────────────────────────────────

/// ObjectId IS the file path: deterministic mapping OID → "{nvme_dir}/{oid:016x}.dat"
/// No lookup table. Compact u64 safe for replication streams, RDB, and LoValue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);

/// Monotonic OID counter.
/// TODO: Not yet unique per node. Currently a plain counter starting at 1.
/// Two nodes in a cluster will generate colliding OIDs.
/// Fix: hash VM_GetMyClusterID() to 16 bits, OR into top bits of counter.
/// Requires raw FFI call (no safe wrapper in valkey-module crate yet).
static OID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ObjectId {
    pub fn next() -> Self {
        Self(OID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Deterministic file path from OID.
    pub fn file_path(&self, nvme_dir: &str) -> String {
        format!("{}/{:016x}.dat", nvme_dir, self.0)
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

// ─── LoValue Helper Methods ──────────────────────────────────────────────────

impl LoValue {
    /// Reports memory usage in bytes: in-memory struct size + value size.
    /// Used by `MEMORY USAGE <key>`.
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<LoValue>() + self.len as usize
    }

    /// Returns 0 to signal Valkey to ALWAYS free asynchronously (BIO thread).
    ///
    /// Per the Module API contract: returning 0 guarantees async free.
    pub fn free_effort(&self) -> usize {
        0
    }

    /// Deep-copy: allocates a new OID and copies the NVMe file.
    /// Returns None if the file copy fails (e.g., source file missing).
    ///
    /// Thread-safety note: This runs on the main thread (COPY command handler).
    /// The source file is safe to read because:
    /// - All commands (read/copy/delete) execute on the main thread.
    /// - The core guarantees a key exists when COPY is dispatched — a DEL after
    ///   COPY is sequenced by the event loop, so the source file cannot vanish
    ///   mid-copy.
    ///
    pub fn create_copy(&self, data_dir: &str) -> Option<LoValue> {
        let new_oid = ObjectId::next();
        let src_path = self.object_id.file_path(data_dir);
        let dst_path = new_oid.file_path(data_dir);

        match std::fs::copy(&src_path, &dst_path) {
            Ok(_) => Some(LoValue {
                object_id: new_oid,
                len: self.len,
                crc32c: self.crc32c,
            }),
            Err(_) => None,
        }
    }
}

// ─── Callbacks ───────────────────────────────────────────────────────────────

/// Free callback — triggered by native Valkey DEL.
///
/// TODO (object lifecycle): Not concurrency-safe. In-flight GETs or promotions may hold
/// Arc<ObjectContext> clones or have io_uring SQEs referencing the fd. Requires refcounted
/// teardown — only free buffers/close fd when last reference drops. Same mechanism needed
/// for LO.SET overwrite (see engine.rs) and for module eviction.
unsafe extern "C" fn lo_free(value: *mut std::ffi::c_void) {
    let lo = Box::from_raw(value as *mut LoValue);
    crate::storage::get_dram_pool().remove_object(&lo.object_id);
    // FdPool and NVMe files only exist in Tiered mode.
    if crate::operating_mode() == crate::OperatingMode::Tiered {
        crate::storage::get_fd_pool().remove(lo.object_id);
        crate::storage::delete_file(lo.object_id);
    }
}

/// MEMORY USAGE callback.
/// Reports struct overhead + full on-disk object size for capacity planning.
unsafe extern "C" fn lo_mem_usage(value: *const std::ffi::c_void) -> usize {
    let val = &*(value as *const LoValue);
    val.memory_usage()
}

/// FREE EFFORT callback.
/// Always returns 0 to force asynchronous free via BIO thread.
/// This keeps unlink(2) off the main event-loop thread.
/// See LoValue::free_effort() for full rationale.
unsafe extern "C" fn lo_free_effort(
    _key: *mut raw::RedisModuleString,
    value: *const std::ffi::c_void,
) -> usize {
    let val = &*(value as *const LoValue);
    val.free_effort()
}

/// COPY callback.
/// Deep-copies the NVMe file with a fresh OID. Returns null on failure.
unsafe extern "C" fn lo_copy(
    _from_key: *mut raw::RedisModuleString,
    _to_key: *mut raw::RedisModuleString,
    value: *const std::ffi::c_void,
) -> *mut std::ffi::c_void {
    let src = &*(value as *const LoValue);
    match src.create_copy(&crate::nvme_dir()) {
        Some(new_val) => Box::into_raw(Box::new(new_val)) as *mut std::ffi::c_void,
        None => std::ptr::null_mut(),
    }
}

/// DEBUG DIGEST callback.
/// Feeds object_id, len, and crc32c into the digest for integrity verification.
unsafe extern "C" fn lo_digest(md: *mut raw::RedisModuleDigest, value: *mut std::ffi::c_void) {
    let mut dig = Digest::new(md);
    let val = &*(value as *const LoValue);
    dig.add_long_long(val.object_id.0 as i64);
    dig.add_long_long(val.len as i64);
    dig.add_long_long(val.crc32c as i64);
    dig.end_sequence();
}

// ─── Type Registration ───────────────────────────────────────────────────────

pub static LO_TYPE: ValkeyType = ValkeyType::new(
    "largeob-k", // 9 char type name
    0,           // encoding version
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: None,    // TODO
        rdb_save: None,    // TODO
        aof_rewrite: None, // TODO
        free: Some(lo_free),
        mem_usage: Some(lo_mem_usage),
        digest: Some(lo_digest),
        aux_load: None, // TODO
        aux_save: None, // TODO
        aux_save2: None,
        aux_save_triggers: 0,
        free_effort: Some(lo_free_effort),
        unlink: None,
        copy: Some(lo_copy),
        defrag: None, // TODO
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_oid_monotonic() {
        let a = ObjectId::next();
        let b = ObjectId::next();
        let c = ObjectId::next();
        assert!(b.0 > a.0);
        assert!(c.0 > b.0);
    }

    #[test]
    fn test_oid_file_path_format() {
        let oid = ObjectId(0xff);
        assert_eq!(oid.file_path("/data"), "/data/00000000000000ff.dat");

        let oid2 = ObjectId(1);
        assert_eq!(
            oid2.file_path("/mnt/bigobj"),
            "/mnt/bigobj/0000000000000001.dat"
        );
    }

    // ─── memory_usage tests ──────────────────────────────────────────────

    #[test]
    fn test_memory_usage() {
        let val = LoValue {
            object_id: ObjectId(1),
            len: 4_194_304, // 4MB
            crc32c: 0,
        };
        assert_eq!(
            val.memory_usage(),
            std::mem::size_of::<LoValue>() + 4_194_304
        );
    }

    #[test]
    fn test_memory_usage_zero() {
        let val = LoValue {
            object_id: ObjectId(1),
            len: 0,
            crc32c: 0,
        };
        assert_eq!(val.memory_usage(), std::mem::size_of::<LoValue>());
    }

    // ─── free_effort tests ───────────────────────────────────────────────

    #[test]
    fn test_free_effort_always_zero() {
        // free_effort always returns 0 (async free) regardless of object size.
        let small = LoValue {
            object_id: ObjectId(1),
            len: 512,
            crc32c: 0,
        };
        assert_eq!(small.free_effort(), 0);

        let large = LoValue {
            object_id: ObjectId(2),
            len: 100 * 1024 * 1024,
            crc32c: 0,
        };
        assert_eq!(large.free_effort(), 0);

        let zero = LoValue {
            object_id: ObjectId(3),
            len: 0,
            crc32c: 0,
        };
        assert_eq!(zero.free_effort(), 0);
    }

    // ─── create_copy tests ───────────────────────────────────────────────

    #[test]
    fn test_copy_success() {
        let tmp = std::env::temp_dir().join("lo_test_copy_success");
        std::fs::create_dir_all(&tmp).unwrap();

        let oid = ObjectId::next();
        let src_path = oid.file_path(tmp.to_str().unwrap());
        // Create source file with known content.
        let content = b"hello large object world";
        let mut f = std::fs::File::create(&src_path).unwrap();
        f.write_all(content).unwrap();

        let val = LoValue {
            object_id: oid,
            len: content.len() as u64,
            crc32c: 0xDEAD,
        };

        let copy = val.create_copy(tmp.to_str().unwrap()).unwrap();

        // New OID must differ.
        assert_ne!(copy.object_id, val.object_id);
        // Metadata preserved.
        assert_eq!(copy.len, val.len);
        assert_eq!(copy.crc32c, val.crc32c);
        // New file exists with same content.
        let dst_path = copy.object_id.file_path(tmp.to_str().unwrap());
        let read_back = std::fs::read(&dst_path).unwrap();
        assert_eq!(read_back, content);

        // Cleanup.
        let _ = std::fs::remove_file(&src_path);
        let _ = std::fs::remove_file(&dst_path);
        let _ = std::fs::remove_dir(&tmp);
    }

    #[test]
    fn test_copy_missing_source() {
        let tmp = std::env::temp_dir().join("lo_test_copy_missing");
        std::fs::create_dir_all(&tmp).unwrap();

        let val = LoValue {
            object_id: ObjectId(0xDEADBEEF),
            len: 1024,
            crc32c: 0,
        };

        let result = val.create_copy(tmp.to_str().unwrap());
        assert!(result.is_none());

        let _ = std::fs::remove_dir(&tmp);
    }

    #[test]
    fn test_copy_different_oid() {
        let tmp = std::env::temp_dir().join("lo_test_copy_diff_oid");
        std::fs::create_dir_all(&tmp).unwrap();

        let oid = ObjectId::next();
        let src_path = oid.file_path(tmp.to_str().unwrap());
        std::fs::write(&src_path, b"data").unwrap();

        let val = LoValue {
            object_id: oid,
            len: 4,
            crc32c: 0,
        };

        let copy = val.create_copy(tmp.to_str().unwrap()).unwrap();
        assert_ne!(copy.object_id, val.object_id);

        // Cleanup.
        let _ = std::fs::remove_file(&src_path);
        let dst_path = copy.object_id.file_path(tmp.to_str().unwrap());
        let _ = std::fs::remove_file(&dst_path);
        let _ = std::fs::remove_dir(&tmp);
    }
}
