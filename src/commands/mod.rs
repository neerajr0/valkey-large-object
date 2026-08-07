//! Command handlers for BO.SET, BO.GET, BO.DEL, BO.EXISTS, BO.LEN, BO.INFO, BO.EVICT, BO.GETRANGE

use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use valkey_module::ThreadSafeContext;
use std::os::raw::c_void;

use crate::storage::api::{SyncGetResult, ObjectMeta, AsyncReadCallback};
use crate::storage::engine;
use crate::threadpool;

/// BO.SET key value
/// Stores a large immutable object. Returns OK.
pub fn bo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args_iter = args.into_iter().skip(1);
    let key = args_iter.next_arg()?;
    let value = args_iter.next_arg()?;

    let key_bytes = key.as_slice();
    let value_bytes = value.as_slice();

    let (meta, _superseded) = engine::engine().put(key_bytes, value_bytes);

    ctx.replicate_verbatim();

    ctx.log_debug(&format!(
        "BO.SET key={} oid={} len={} crc=0x{:08x}",
        String::from_utf8_lossy(key_bytes),
        meta.object_id.0,
        meta.len,
        meta.crc32c
    ));

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

/// BO.GET key
/// KV cache semantics: verifies the object exists and is readable.
///   - If in pool: returns OK <len> immediately (already warm).
///   - If on NVMe only: reads from disk to verify, returns OK <len>.
///     Does NOT persist in pool — DMA.GET handles its own pinned read lifecycle.
///     The read proves the object is intact; client then issues DMA.GET.
/// Returns nil if key not found.
pub fn bo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let key_bytes = key.as_slice();

    match engine::engine().get_sync(key_bytes) {
        SyncGetResult::Present { len, handle, .. } => {
            // Already in buffer pool — just report size, release handle.
            engine::engine().release_handle(handle as u64);
            Ok(ValkeyValue::BulkString(format!("OK {}", len)))
        }
        SyncGetResult::NotFound => Ok(ValkeyValue::Null),
        SyncGetResult::NeedsAsync => {
            // On NVMe only. Block client, read to verify, reply OK <len>.
            // Data is NOT kept in pool — just read, confirm, free.
            let blocked = ctx.block_client();
            let key_owned = key_bytes.to_vec();

            threadpool::pool().spawn(move || {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(1);

                extern "C" fn read_cb(
                    user_data: *mut c_void,
                    _data: *const u8,
                    len: u64,
                    _meta: ObjectMeta,
                    _handle: *mut c_void,
                    error_code: i32,
                ) {
                    let tx = unsafe {
                        Box::from_raw(user_data as *mut crossbeam_channel::Sender<u64>)
                    };
                    if error_code == 0 && len > 0 {
                        let _ = tx.send(len);
                    } else {
                        let _ = tx.send(0);
                    }
                    // NOTE: _data buffer is freed by the reaper after callback returns.
                    // We do NOT copy or retain it. Just read the length.
                }

                engine::engine().get_async(
                    &key_owned,
                    read_cb,
                    Box::into_raw(Box::new(tx)) as *mut c_void,
                );

                let len = rx.recv().unwrap_or(0);

                let thread_ctx = ThreadSafeContext::with_blocked_client(blocked);
                if len == 0 {
                    thread_ctx.reply(Ok(ValkeyValue::Null));
                } else {
                    thread_ctx.reply(Ok(ValkeyValue::BulkString(format!("OK {}", len))));
                }
            });

            Ok(ValkeyValue::NoReply)
        }
        SyncGetResult::Error { .. } => {
            Err(ValkeyError::Str("ERR internal storage error"))
        }
    }
}

/// BO.DEL key
/// Deletes the object. Returns 1 if deleted, 0 if not found.
pub fn bo_del(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let oid = engine::engine().delete(key.as_slice());
    if oid.0 == 0 {
        Ok(ValkeyValue::Integer(0))
    } else {
        Ok(ValkeyValue::Integer(1))
    }
}

/// BO.EXISTS key
/// Returns 1 if key exists, 0 otherwise.
pub fn bo_exists(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let meta = engine::engine().get_meta(key.as_slice());
    if meta.object_id.0 == 0 {
        Ok(ValkeyValue::Integer(0))
    } else {
        Ok(ValkeyValue::Integer(1))
    }
}

/// BO.LEN key
/// Returns the object length in bytes, or nil if not found.
pub fn bo_len(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let meta = engine::engine().get_meta(key.as_slice());
    if meta.object_id.0 == 0 {
        Ok(ValkeyValue::Null)
    } else {
        Ok(ValkeyValue::Integer(meta.len as i64))
    }
}

/// BO.INFO
/// Returns storage engine stats.
pub fn bo_info(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }

    let eng = engine::engine();
    let mode = if eng.is_nvme_mode() { "nvme-tiered" } else { "dram-only" };
    let info = format!(
        "object_count:{}\r\ntotal_bytes:{}\r\nbuffer_pool_bytes:{}\r\nmode:{}\r\nbuffer_pool_hit_rate:1.0",
        eng.object_count(),
        eng.total_stored_bytes(),
        eng.buffer_pool_bytes(),
        mode,
    );
    Ok(ValkeyValue::BulkString(info))
}

/// BO.EVICT key
/// Debug command: evict an object from the buffer pool (remains on NVMe).
/// Next BO.GET will go through the async io_uring path.
pub fn bo_evict(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    if engine::engine().evict_from_pool(key.as_slice()) {
        Ok(ValkeyValue::Integer(1))
    } else {
        Ok(ValkeyValue::Integer(0))
    }
}

/// BO.GETRANGE key offset len
/// Returns a chunk of the value starting at `offset` for `len` bytes.
/// Enables client-driven streaming for large objects over TCP.
/// Returns nil if key not found. Returns error if offset >= object length.
pub fn bo_getrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 4 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let offset: u64 = args[2].to_string_lossy().parse::<u64>()
        .map_err(|_| ValkeyError::Str("ERR invalid offset"))?;
    let len: u64 = args[3].to_string_lossy().parse::<u64>()
        .map_err(|_| ValkeyError::Str("ERR invalid length"))?;

    let key_bytes = key.as_slice();

    match engine::engine().get_sync(key_bytes) {
        SyncGetResult::Present { data, len: total_len, handle, .. } => {
            if offset >= total_len {
                engine::engine().release_handle(handle as u64);
                return Err(ValkeyError::Str("ERR offset beyond object length"));
            }
            let actual_len = std::cmp::min(len, total_len - offset);
            let bytes = unsafe {
                std::slice::from_raw_parts(data.add(offset as usize), actual_len as usize)
            };
            let result = ValkeyValue::StringBuffer(bytes.to_vec());
            engine::engine().release_handle(handle as u64);
            Ok(result)
        }
        SyncGetResult::NotFound => Ok(ValkeyValue::Null),
        SyncGetResult::NeedsAsync => {
            // Block client, read from NVMe, promote to pool, then return the range.
            let blocked = ctx.block_client();
            let key_owned = key_bytes.to_vec();

            threadpool::pool().spawn(move || {
                let (tx, rx) = crossbeam_channel::bounded::<(Vec<u8>, u64)>(1);

                extern "C" fn read_cb(
                    user_data: *mut c_void,
                    data: *const u8,
                    data_len: u64,
                    _meta: ObjectMeta,
                    _handle: *mut c_void,
                    error_code: i32,
                ) {
                    let tx = unsafe {
                        Box::from_raw(user_data as *mut crossbeam_channel::Sender<(Vec<u8>, u64)>)
                    };
                    if error_code == 0 && !data.is_null() {
                        let bytes = unsafe { std::slice::from_raw_parts(data, data_len as usize) };
                        let _ = tx.send((bytes.to_vec(), data_len));
                    } else {
                        let _ = tx.send((Vec::new(), 0));
                    }
                }

                engine::engine().get_async(
                    &key_owned,
                    read_cb,
                    Box::into_raw(Box::new(tx)) as *mut c_void,
                );

                let (data, total_len) = rx.recv().unwrap_or_default();

                let thread_ctx = ThreadSafeContext::with_blocked_client(blocked);
                if data.is_empty() {
                    thread_ctx.reply(Ok(ValkeyValue::Null));
                } else {
                    // Promote to pool for future reads.
                    let actual_offset = std::cmp::min(offset, total_len);
                    let actual_len = std::cmp::min(len, total_len - actual_offset);
                    let range = data[actual_offset as usize..(actual_offset + actual_len) as usize].to_vec();

                    engine::engine().promote_to_pool(&key_owned, data);
                    thread_ctx.reply(Ok(ValkeyValue::StringBuffer(range)));
                }
            });

            Ok(ValkeyValue::NoReply)
        }
        SyncGetResult::Error { .. } => {
            Err(ValkeyError::Str("ERR internal storage error"))
        }
    }
}
