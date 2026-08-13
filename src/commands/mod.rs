//! Command handlers for BO.SET, BO.GET, BO.DEL, BO.EXISTS, BO.LEN, BO.INFO, BO.EVICT

use std::os::unix::io::RawFd;

use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::storage::engine;
use crate::uring_engine;

/// Block the client and hand a ready fd to the io_uring read pipeline.
/// `close_fd_after` is Some when the fd was opened on demand for this GET (the
/// poller closes it after the read) and None when it is a pooled fd.
fn block_and_read(ctx: &Context, len: u64, fd: RawFd, close_fd_after: Option<RawFd>) {
    let blocked = ctx.block_client();
    let client_data = Box::into_raw(Box::new(uring_engine::ClientData {
        blocked_client: blocked,
        object_len: len,
        close_fd_after,
    }));
    uring_engine::engine().submit(uring_engine::ReadRequest {
        fd,
        len,
        client_data,
    });
}

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

    // Always go through io_uring for benchmarking purposes.
    // In production: check in_pool and return immediately if true.
    let (meta, _in_pool) = match engine::engine().get_status(key_bytes) {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    // Fast path: fd already pooled (keep_read_fds mode) — no open() needed.
    if let Some(fd) = engine::engine().get_read_fd(meta.object_id) {
        block_and_read(ctx, meta.len, fd, None);
        return Ok(ValkeyValue::NoReply);
    }

    // Non-pooling: an fd must be opened on demand. The shard dir is chosen by the
    // key's Valkey hash slot.
    let slot = crate::slot::key_hash_slot(key_bytes);

    // Preferred (open-threads > 0): offload the potentially-blocking open() to the
    // worker pool. Block the client FIRST — the worker unblocks it on open failure.
    if crate::open_pool::enabled() {
        let blocked = ctx.block_client();
        let client_data = Box::into_raw(Box::new(uring_engine::ClientData {
            blocked_client: blocked,
            object_len: meta.len,
            close_fd_after: None, // the worker fills this in once it has the fd
        }));
        crate::open_pool::pool().submit(crate::open_pool::OpenRequest {
            slot,
            oid: meta.object_id,
            len: meta.len,
            client_data,
        });
        return Ok(ValkeyValue::NoReply);
    }

    // Fallback (open-threads 0): open inline on the main thread (original behavior).
    match engine::engine().open_read_fd_ondemand(slot, meta.object_id) {
        Some(fd) => {
            block_and_read(ctx, meta.len, fd, Some(fd));
            Ok(ValkeyValue::NoReply)
        }
        None => Ok(ValkeyValue::Null),
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
