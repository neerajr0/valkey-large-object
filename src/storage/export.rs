//! C ABI function implementations that are exported via ExportSharedAPI.
//! Each function delegates to the StorageEngine singleton.

use std::os::raw::c_void;
use std::slice;

use super::api::*;
use super::engine;

// === Sync reads ===

extern "C" fn api_get_sync(key: *const u8, key_len: u64) -> SyncGetResult {
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    engine::engine().get_sync(key)
}

extern "C" fn api_get_by_id_sync(oid: ObjectId) -> SyncGetByIdResult {
    engine::engine().get_by_id_sync(oid)
}

// === Async reads (delegate to engine — inline for DRAM, io_uring for NVMe) ===

extern "C" fn api_get_async(
    key: *const u8,
    key_len: u64,
    cb: AsyncReadCallback,
    user_data: *mut c_void,
) {
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    engine::engine().get_async(key, cb, user_data);
}

extern "C" fn api_get_by_id_async(
    oid: ObjectId,
    cb: AsyncReadCallback,
    user_data: *mut c_void,
) {
    engine::engine().get_by_id_async(oid, cb, user_data);
}

// === Release ===

extern "C" fn api_release_handle(handle: *mut c_void) {
    let hid = handle as u64;
    engine::engine().release_handle(hid);
}

// === Writes ===

extern "C" fn api_put_sync(
    key: *const u8,
    key_len: u64,
    value: *const u8,
    value_len: u64,
) -> SyncGetByIdResult {
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    let value = unsafe { slice::from_raw_parts(value, value_len as usize) };
    let (meta, _superseded) = engine::engine().put(key, value);

    // Return success via the Present variant (data is not returned for put).
    SyncGetByIdResult::Present {
        data: std::ptr::null(),
        len: 0,
        meta,
        handle: std::ptr::null_mut(),
    }
}

extern "C" fn api_put_async(
    key: *const u8,
    key_len: u64,
    value: *const u8,
    value_len: u64,
    cb: AsyncWriteCallback,
    user_data: *mut c_void,
) {
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    let value = unsafe { slice::from_raw_parts(value, value_len as usize) };
    let (meta, superseded) = engine::engine().put(key, value);
    // In DRAM-only mode, complete inline.
    cb(user_data, meta.object_id, superseded, 0);
}

// === Delete ===

extern "C" fn api_delete(key: *const u8, key_len: u64) -> ObjectId {
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    engine::engine().delete(key)
}

// === Metadata ===

extern "C" fn api_get_meta(key: *const u8, key_len: u64) -> ObjectMeta {
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    engine::engine().get_meta(key)
}

extern "C" fn api_get_meta_by_id(oid: ObjectId) -> ObjectMeta {
    match engine::engine().get_by_id_sync(oid) {
        SyncGetByIdResult::Present { meta, handle, .. } => {
            engine::engine().release_handle(handle as u64);
            meta
        }
        _ => ObjectMeta { object_id: ObjectId(0), len: 0, crc32c: 0, created_at_us: 0 },
    }
}

// === Pin / Unpin / Memory Region ===

extern "C" fn api_pin(handle: *mut c_void) -> i32 {
    engine::engine().pin_handle(handle as u64)
}

extern "C" fn api_unpin(handle: *mut c_void) -> i32 {
    engine::engine().unpin_handle(handle as u64)
}

extern "C" fn api_memory_region(
    handle: *mut c_void,
    out_data: *mut *const u8,
    out_len: *mut u64,
) -> i32 {
    match engine::engine().memory_region(handle as u64) {
        Some((ptr, len)) => {
            unsafe {
                *out_data = ptr;
                *out_len = len;
            }
            0
        }
        None => -1,
    }
}

// === Subscriptions ===

extern "C" fn api_subscribe(cb: MutationCallback, user_data: *mut c_void) -> u64 {
    engine::engine().subscribe(cb, user_data)
}

extern "C" fn api_unsubscribe(subscriber_id: u64) {
    engine::engine().unsubscribe(subscriber_id);
}

// === Snapshot (simplified: DRAM snapshot = Vec<(key, meta)>) ===

extern "C" fn api_snapshot_create() -> SnapshotHandle {
    let snap = engine::engine().create_snapshot();
    Box::into_raw(Box::new(snap)) as *mut c_void
}

extern "C" fn api_snapshot_iter_create(snap: SnapshotHandle) -> SnapshotIterHandle {
    let snap_ref = unsafe { &*(snap as *const Vec<(Vec<u8>, ObjectMeta)>) };
    let iter = Box::new(SnapshotIter { items: snap_ref, pos: 0 });
    Box::into_raw(iter) as *mut c_void
}

struct SnapshotIter {
    items: *const Vec<(Vec<u8>, ObjectMeta)>,
    pos: usize,
}

extern "C" fn api_snapshot_iter_next(
    iter: SnapshotIterHandle,
    out_key: *mut *const u8,
    out_key_len: *mut u64,
    out_meta: *mut ObjectMeta,
) -> i32 {
    let it = unsafe { &mut *(iter as *mut SnapshotIter) };
    let items = unsafe { &*it.items };
    if it.pos >= items.len() {
        return 0; // exhausted
    }
    let (ref key, meta) = items[it.pos];
    unsafe {
        *out_key = key.as_ptr();
        *out_key_len = key.len() as u64;
        *out_meta = meta;
    }
    it.pos += 1;
    1 // has more
}

extern "C" fn api_snapshot_iter_destroy(iter: SnapshotIterHandle) {
    unsafe { drop(Box::from_raw(iter as *mut SnapshotIter)); }
}

extern "C" fn api_snapshot_destroy(snap: SnapshotHandle) {
    unsafe { drop(Box::from_raw(snap as *mut Vec<(Vec<u8>, ObjectMeta)>)); }
}

extern "C" fn api_snapshot_get_by_id(
    _snap: SnapshotHandle,
    oid: ObjectId,
    cb: AsyncReadCallback,
    user_data: *mut c_void,
) {
    // Delegate to main engine — snapshot holds metadata only, data is in the live store.
    api_get_by_id_async(oid, cb, user_data);
}

// === Stats ===

extern "C" fn api_buffer_pool_hit_rate() -> f64 {
    1.0 // DRAM-only: always hits
}

extern "C" fn api_disk_used_bytes() -> u64 {
    0 // DRAM-only: no disk
}

extern "C" fn api_disk_free_bytes() -> u64 {
    0 // DRAM-only: no disk
}

extern "C" fn api_object_count() -> u64 {
    engine::engine().object_count()
}

// === The static API table ===

pub static API_TABLE: BigObjStorageApiV1 = BigObjStorageApiV1 {
    get_sync: api_get_sync,
    get_by_id_sync: api_get_by_id_sync,
    get_async: api_get_async,
    get_by_id_async: api_get_by_id_async,
    release_handle: api_release_handle,
    put_sync: api_put_sync,
    put_async: api_put_async,
    delete: api_delete,
    get_meta: api_get_meta,
    get_meta_by_id: api_get_meta_by_id,
    pin: api_pin,
    unpin: api_unpin,
    memory_region: api_memory_region,
    subscribe: api_subscribe,
    unsubscribe: api_unsubscribe,
    snapshot_create: api_snapshot_create,
    snapshot_iter_create: api_snapshot_iter_create,
    snapshot_iter_next: api_snapshot_iter_next,
    snapshot_iter_destroy: api_snapshot_iter_destroy,
    snapshot_destroy: api_snapshot_destroy,
    snapshot_get_by_id: api_snapshot_get_by_id,
    buffer_pool_hit_rate: api_buffer_pool_hit_rate,
    disk_used_bytes: api_disk_used_bytes,
    disk_free_bytes: api_disk_free_bytes,
    object_count: api_object_count,
};
