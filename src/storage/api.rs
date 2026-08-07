//! Shared API types for cross-module communication.
//!
//! This defines the C-ABI-stable interface that the transport module (vrdma)
//! discovers via `ValkeyModule_GetSharedAPI("bigobj_storage_v1")`.
//!
//! The storage module exports this via `ValkeyModule_ExportSharedAPI`.

use std::os::raw::c_void;

/// Per-node monotonic object identity.
/// Global uniqueness = (node_id from module state, object_id).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);

/// Object metadata — everything about an object EXCEPT the bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ObjectMeta {
    pub object_id: ObjectId,
    pub len: u64,
    pub crc32c: u32,
    pub created_at_us: u64,
}

/// Result of a sync get operation.
#[repr(C)]
pub enum SyncGetResult {
    /// Value is in buffer pool — pointer valid until handle is released.
    Present {
        data: *const u8,
        len: u64,
        meta: ObjectMeta,
        handle: *mut c_void, // opaque ValueHandle; caller MUST call release_handle
    },
    /// Key does not exist.
    NotFound,
    /// Value is on NVMe, not in buffer pool. Caller must use async path.
    NeedsAsync,
    /// Internal error.
    Error { code: i32 },
}

/// Result of a sync get-by-object-id (used by transport for pull requests).
#[repr(C)]
pub enum SyncGetByIdResult {
    /// Object found and resident in buffer pool.
    Present {
        data: *const u8,
        len: u64,
        meta: ObjectMeta,
        handle: *mut c_void,
    },
    /// Object was deleted/superseded — tell the requestor "GONE".
    Gone,
    /// Object exists but is on NVMe; use async path.
    NeedsAsync,
    /// Internal error.
    Error { code: i32 },
}

/// Async completion callback signature.
/// Called on the storage module's io_uring reaper thread (NVMe mode)
/// or inline (DRAM-only mode).
///
/// `user_data` is the opaque pointer the caller passed when initiating the operation.
/// `data` points to a 4KiB-aligned buffer (valid for RDMA). Length in `len`.
/// `handle` MUST be released by the caller via `release_handle` after use.
/// On error: data is null, len is 0, error_code is non-zero.
pub type AsyncReadCallback = extern "C" fn(
    user_data: *mut c_void,
    data: *const u8,
    len: u64,
    meta: ObjectMeta,
    handle: *mut c_void,
    error_code: i32,
);

/// Callback for async write completion.
/// `new_oid` is the assigned object ID. `superseded` is the previous OID (0 if none).
pub type AsyncWriteCallback = extern "C" fn(
    user_data: *mut c_void,
    new_oid: ObjectId,
    superseded: ObjectId, // ObjectId(0) means no prior object
    error_code: i32,
);

/// Mutation event delivered to subscribers.
#[repr(C)]
#[derive(Debug, Clone)]
pub enum Mutation {
    Created {
        key_ptr: *const u8,
        key_len: u64,
        meta: ObjectMeta,
        superseded: ObjectId, // ObjectId(0) if no prior
    },
    Deleted {
        key_ptr: *const u8,
        key_len: u64,
        oid: ObjectId,
    },
    Evicted {
        key_ptr: *const u8,
        key_len: u64,
        oid: ObjectId,
    },
}

/// Mutation subscription callback.
pub type MutationCallback = extern "C" fn(user_data: *mut c_void, event: *const Mutation);

/// Handle to a snapshot (frozen view at fork time).
/// Opaque — transport uses the function table to iterate/get from it.
pub type SnapshotHandle = *mut c_void;

/// Iterator over snapshot entries. Opaque.
pub type SnapshotIterHandle = *mut c_void;

/// The exported function table — this IS the shared API contract.
///
/// Version 1. Transport calls `GetSharedAPI("bigobj_storage_v1")` and casts
/// to `*const BigObjStorageApiV1`.
#[repr(C)]
pub struct BigObjStorageApiV1 {
    // === Sync reads (never block; return NeedsAsync if IO required) ===
    pub get_sync: extern "C" fn(key: *const u8, key_len: u64) -> SyncGetResult,
    pub get_by_id_sync: extern "C" fn(oid: ObjectId) -> SyncGetByIdResult,

    // === Async reads (always work; callback on reaper or inline) ===
    pub get_async: extern "C" fn(
        key: *const u8,
        key_len: u64,
        cb: AsyncReadCallback,
        user_data: *mut c_void,
    ),
    pub get_by_id_async: extern "C" fn(
        oid: ObjectId,
        cb: AsyncReadCallback,
        user_data: *mut c_void,
    ),

    // === Release a handle returned by sync or async reads ===
    pub release_handle: extern "C" fn(handle: *mut c_void),

    // === Writes ===
    /// Sync write — only for DRAM-only mode or small values in buffer pool.
    /// Returns the new ObjectId or error. Data is copied.
    pub put_sync: extern "C" fn(
        key: *const u8,
        key_len: u64,
        value: *const u8,
        value_len: u64,
    ) -> SyncGetByIdResult, // reusing enum shape: Present means success

    /// Async write — streams to NVMe via io_uring.
    pub put_async: extern "C" fn(
        key: *const u8,
        key_len: u64,
        value: *const u8,
        value_len: u64,
        cb: AsyncWriteCallback,
        user_data: *mut c_void,
    ),

    // === Delete ===
    /// Returns the ObjectId that was deleted, or ObjectId(0) if not found.
    pub delete: extern "C" fn(key: *const u8, key_len: u64) -> ObjectId,

    // === Metadata ===
    pub get_meta: extern "C" fn(key: *const u8, key_len: u64) -> ObjectMeta, // oid=0 if not found
    pub get_meta_by_id: extern "C" fn(oid: ObjectId) -> ObjectMeta,

    // === Pin / Unpin (for RDMA: keep buffer stable during DMA) ===
    /// Pin prevents eviction/free of this handle's backing memory.
    /// Caller MUST call unpin after DMA completes.
    pub pin: extern "C" fn(handle: *mut c_void) -> i32,   // 0 = success
    pub unpin: extern "C" fn(handle: *mut c_void) -> i32,  // 0 = success

    /// Get raw memory region for RDMA. Only valid while pinned.
    /// Returns pointer + length. Pointer is 4KiB-aligned.
    pub memory_region: extern "C" fn(
        handle: *mut c_void,
        out_data: *mut *const u8,
        out_len: *mut u64,
    ) -> i32, // 0 = success

    // === Mutation subscription (transport observes writes/deletes) ===
    pub subscribe: extern "C" fn(cb: MutationCallback, user_data: *mut c_void) -> u64, // subscriber_id
    pub unsubscribe: extern "C" fn(subscriber_id: u64),

    // === Snapshot (frozen view for full sync RDB) ===
    pub snapshot_create: extern "C" fn() -> SnapshotHandle,
    pub snapshot_iter_create: extern "C" fn(snap: SnapshotHandle) -> SnapshotIterHandle,
    /// Returns 0 when exhausted. Fills out_meta.
    pub snapshot_iter_next: extern "C" fn(
        iter: SnapshotIterHandle,
        out_key: *mut *const u8,
        out_key_len: *mut u64,
        out_meta: *mut ObjectMeta,
    ) -> i32,
    pub snapshot_iter_destroy: extern "C" fn(iter: SnapshotIterHandle),
    pub snapshot_destroy: extern "C" fn(snap: SnapshotHandle),
    /// Get value from snapshot by object_id (for pull during sync).
    pub snapshot_get_by_id: extern "C" fn(
        snap: SnapshotHandle,
        oid: ObjectId,
        cb: AsyncReadCallback,
        user_data: *mut c_void,
    ),

    // === Stats ===
    pub buffer_pool_hit_rate: extern "C" fn() -> f64,
    pub disk_used_bytes: extern "C" fn() -> u64,
    pub disk_free_bytes: extern "C" fn() -> u64,
    pub object_count: extern "C" fn() -> u64,
}

/// API name constant used by both sides.
pub const BIGOBJ_STORAGE_API_NAME: &str = "bigobj_storage_v1\0";
