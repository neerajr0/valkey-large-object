//! Command handlers for BO.SET, BO.GET, BO.DEL, BO.EXISTS, BO.LEN, BO.INFO, BO.EVICT

use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::storage::engine;
use crate::uring_engine;

/// BO.SET key value
pub fn bo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args_iter = args.into_iter().skip(1);
    let key = args_iter.next_arg()?;
    let value = args_iter.next_arg()?;

    let meta = engine::engine().put(key.as_slice(), value.as_slice());

    ctx.log_debug(&format!(
        "BO.SET oid={} len={} crc=0x{:08x}",
        meta.object_id.0, meta.len, meta.crc32c
    ));

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

/// BO.GET key
/// If in buffer pool: reply OK <len> immediately.
/// If on NVMe: BlockClient -> io_uring read -> UnblockClient with OK <len>.
pub fn bo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let key_bytes = key.as_slice();

    match engine::engine().get_status(key_bytes) {
        None => Ok(ValkeyValue::Null),
        Some((meta, _in_pool)) => {
            // Always go through io_uring for benchmarking purposes.
            // In production: check in_pool and return immediately if true.
            let fd = engine::engine().get_read_fd(meta.object_id);
            if fd.is_none() {
                return Ok(ValkeyValue::Null);
            }

            let blocked = ctx.block_client();
            let client_data = Box::into_raw(Box::new(uring_engine::ClientData {
                blocked_client: blocked,
                object_len: meta.len,
            }));

            uring_engine::engine().submit(uring_engine::ReadRequest {
                fd: fd.unwrap(),
                len: meta.len,
                client_data,
            });

            Ok(ValkeyValue::NoReply)
        }
    }
}

/// BO.DEL key
pub fn bo_del(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    if engine::engine().delete(key.as_slice()) {
        Ok(ValkeyValue::Integer(1))
    } else {
        Ok(ValkeyValue::Integer(0))
    }
}

/// BO.EXISTS key
pub fn bo_exists(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    if engine::engine().exists(key.as_slice()) {
        Ok(ValkeyValue::Integer(1))
    } else {
        Ok(ValkeyValue::Integer(0))
    }
}

/// BO.LEN key
pub fn bo_len(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    match engine::engine().get_meta(key.as_slice()) {
        Some(meta) => Ok(ValkeyValue::Integer(meta.len as i64)),
        None => Ok(ValkeyValue::Null),
    }
}

/// BO.INFO
pub fn bo_info(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }
    let eng = engine::engine();
    let info = format!(
        "object_count:{}\r\ntotal_bytes:{}\r\nbuffer_pool_bytes:{}\r\nmode:nvme",
        eng.object_count(),
        eng.total_stored_bytes(),
        eng.buffer_pool_bytes(),
    );
    Ok(ValkeyValue::BulkString(info))
}

/// BO.EVICT key
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
