//! Command Engine — routes GET/SET through the correct path based on
//! operating mode (DRAM-only vs Tiered) and transport (TCP vs EFA).
//!
//! Architecture (STORAGE_DESIGN.md §9.2):
//!   TCP GET, DRAMPool hit       → serve inline (no tokio)
//!   TCP GET, DRAMPool miss      → tokio task (Tiered: NVMe read; DRAM-only: impossible)
//!   TCP SET, DRAM-only          → inline (alloc + memcpy, no NVMe)
//!   TCP SET, Tiered             → tokio task (NVMe write)
//!   EFA anything                → tokio task
//!
//! Promotion (Tiered GET miss):
//!   If admission policy says yes → alloc ObjectContext in DRAMPool,
//!   ReadFixed directly into DRAMPool buffers, mark Filling→Ready.
//!   Concurrent GETs coalesce on Filling ObjectContext.

use std::sync::Arc;

use valkey_module::{ValkeyError, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::storage::{self, uring, ObjectContext};
use crate::transport::Session;
use crate::OperatingMode;

// ─── Transport context passed to engine ──────────────────────────────────────

pub enum Transport {
    Tcp,
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

// ─── Engine Result ────────────────────────────────────────────────────────────

/// Result of an engine dispatch. Command handler matches on this.
pub enum EngineResult {
    /// Sync path completed — return this value directly to Valkey.
    Sync(Result<ValkeyValue, ValkeyError>),
    /// Async path — client is blocked, reply will come from tokio task.
    Async,
}

// ─── GET Engine ──────────────────────────────────────────────────────────────

/// Execute LO.GET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_get(
    ctx: &valkey_module::Context,
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
) -> EngineResult {
    let mode = crate::operating_mode();

    match (mode, &transport) {
        (OperatingMode::DramOnly, Transport::Tcp) => {
            // Fully sync — serve from DRAMPool, return directly.
            EngineResult::Sync(serve_get_dram_tcp(object_id, obj_len))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            match mode {
                OperatingMode::DramOnly => {
                    execute_get_dram_only(object_id, obj_len, transport, blocked_client);
                }
                OperatingMode::Tiered => {
                    execute_get_tiered(object_id, obj_len, transport, blocked_client);
                }
            }
            EngineResult::Async
        }
    }
}

/// Sync DRAM-only TCP GET: serve object data directly from DRAMPool.
fn serve_get_dram_tcp(object_id: ObjectId, obj_len: u64) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            let mut data = Vec::with_capacity(obj_len as usize);
            for buf in &obj_ctx.buffers {
                let ptr = dram_pool.buffer_ptr(buf);
                let slice = unsafe { std::slice::from_raw_parts(ptr, buf.len as usize) };
                data.extend_from_slice(slice);
            }
            data.truncate(obj_len as usize);
            Ok(ValkeyValue::StringBuffer(data))
        }
        Some(_) => Err(ValkeyError::Str("ERR object not ready")),
        None => Err(ValkeyError::Str("ERR object not in DRAM cache")),
    }
}

/// DRAM-only GET: object MUST be in DRAMPool. If not found → key doesn't exist
/// (shouldn't happen — LoValue exists implies ObjectContext exists in DRAM-only mode).
fn execute_get_dram_only(
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            // Serve from DRAMPool.
            serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx);
        }
        Some(_obj_ctx) => {
            // Filling state in DRAM-only mode — shouldn't happen (SET is synchronous).
            // But handle gracefully: return error.
            thread_ctx.reply(Err(ValkeyError::Str("ERR object not ready")));
        }
        None => {
            // Shouldn't happen: LoValue exists but no ObjectContext in DRAM-only mode.
            thread_ctx.reply(Err(ValkeyError::Str("ERR object not in DRAM cache")));
        }
    }
}

/// Tiered GET: check DRAMPool → try promote → fall back to NVMe.
fn execute_get_tiered(
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();

    // ─── DRAMPool hit ────────────────────────────────────────────────────
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx);
            return;
        }
        // Filling state: promotion in progress.
        // TODO: coalesce — register as waiter on this ObjectContext.
        // For now: fall through to NVMe read.
    }

    // ─── Try DRAMPool promotion ──────────────────────────────────────────
    // If pool has space and object is eligible, read directly into DRAMPool.
    if let Some(obj_ctx) = dram_pool.try_promote_object(object_id, obj_len) {
        let seg_buf = &obj_ctx.buffers[0];
        let buf_ptr_usize = dram_pool.buffer_ptr(seg_buf) as usize;
        let read_op = uring::UringOp {
            iovec_index: dram_pool.segments()[seg_buf.segment_idx as usize].iovec_index,
            buf_ptr: buf_ptr_usize as *mut u8,
            file_offset: 0,
            len: obj_len,
        };

        let fd_pool = storage::get_fd_pool();
        let fd = match fd_pool.get_or_open(object_id, &crate::data_dir()) {
            Some(fd) => fd,
            None => {
                dram_pool.remove_object(&object_id);
                dram_pool.free(seg_buf);
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                return;
            }
        };

        // Read from NVMe directly into DRAMPool buffer, then serve.
        crate::runtime_handle().spawn(async move {
            let result = uring::submit_read(fd, &read_op).await;
            let thread_ctx =
                valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

            match result {
                Ok(Ok(_)) => {
                    // Serve from DRAMPool (object is now cached).
                    let dram_pool = storage::get_dram_pool();
                    let obj_ctx = dram_pool.get_object(&object_id).unwrap();
                    serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx);
                }
                _ => {
                    // Read failed — remove entry, free buffer.
                    storage::get_dram_pool().remove_object(&object_id);
                    storage::get_dram_pool().free(&obj_ctx.buffers[0]);
                    thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                }
            }
        });
        return;
    }

    // ─── NVMePool fallback (DRAMPool full) ───────────────────────────────
    // Transient read: alloc NVMePool buffer, serve, free.
    let nvme_pool = storage::get_nvme_pool();
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
            return;
        }
    };

    let stream_ctx = storage::StreamingContext::new(vec![seg_buf], obj_len, 1);

    let fd_pool = storage::get_fd_pool();
    let fd = match fd_pool.get_or_open(object_id, &crate::data_dir()) {
        Some(fd) => fd,
        None => {
            nvme_pool.free(&stream_ctx.buffers[0]);
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
            return;
        }
    };

    let buf_ptr_usize = nvme_pool.buffer_ptr(&stream_ctx.buffers[0]) as usize;
    // Single-chunk today: one UringOp for the entire object.
    // Streaming PR will iterate stream_ctx.buffers and submit per-chunk ops in a loop.
    let read_op = uring::UringOp {
        iovec_index: nvme_pool.segments()[stream_ctx.buffers[0].segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr_usize as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    // Spawn tokio task for NVMe read + serve (no caching — transient).
    crate::runtime_handle().spawn(async move {
        let read_result = uring::submit_read(fd, &read_op).await;
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

        match read_result {
            Ok(Ok(_bytes_read)) => {
                match transport {
                    Transport::Tcp => {
                        let data = unsafe {
                            std::slice::from_raw_parts(buf_ptr_usize as *const u8, obj_len as usize)
                                .to_vec()
                        };
                        thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
                    }
                    Transport::Efa {
                        session,
                        rkey,
                        remote_addr,
                    } => {
                        let (efa_tx, efa_rx) = tokio::sync::oneshot::channel();
                        let buf_ptr = buf_ptr_usize as *mut u8;
                        session.write(
                            buf_ptr,
                            obj_len as usize,
                            rkey,
                            remote_addr,
                            Box::new(move |_ptr, result| {
                                let _ = efa_tx.send(result);
                            }),
                        );
                        match efa_rx.await {
                            Ok(Ok(())) => {
                                thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
                            }
                            _ => {
                                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_EFA_WRITE)));
                            }
                        }
                    }
                }
                storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
            }
            Ok(Err(e)) => {
                storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
                thread_ctx.reply(Err(ValkeyError::String(format!(
                    "{}: {}",
                    errors::ERR_NVME_READ,
                    e
                ))));
            }
            Err(_) => {
                storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
            }
        }
    });
}

// ─── SET Engine ──────────────────────────────────────────────────────────────

/// Execute LO.SET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_set(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data_source: DataSource,
) -> EngineResult {
    let mode = crate::operating_mode();

    match (mode, &data_source) {
        (OperatingMode::DramOnly, DataSource::Tcp(data)) => {
            // Fully sync — alloc + memcpy + create LoValue inline.
            EngineResult::Sync(serve_set_dram_tcp(ctx, key_name, obj_len, data))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            let key_name_bytes = key_name.as_slice().to_vec();
            match mode {
                OperatingMode::DramOnly => {
                    execute_set_dram_only(key_name_bytes, obj_len, data_source, blocked_client);
                }
                OperatingMode::Tiered => {
                    execute_set_tiered(key_name_bytes, obj_len, data_source, blocked_client);
                }
            }
            EngineResult::Async
        }
    }
}

/// Sync DRAM-only TCP SET: alloc + memcpy + create LoValue on main thread.
fn serve_set_dram_tcp(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data: &[u8],
) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();

    let seg_buf = match dram_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => return Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)),
    };

    let buf_ptr = dram_pool.buffer_ptr(&seg_buf);
    let copy_len = data.len().min(obj_len as usize);
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };

    let crc = crc32c::crc32c(unsafe { std::slice::from_raw_parts(buf_ptr, obj_len as usize) });

    let object_id = ObjectId::next();
    let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
    dram_pool.insert_object(object_id, obj_ctx);

    let key = ctx.open_key_writable(key_name);
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
    };
    key.set_value(&LO_TYPE, lo_value)
        .map_err(|_| ValkeyError::Str("ERR failed to set key"))?;

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

pub enum DataSource {
    /// TCP: data is inline bytes from the RESP command args.
    Tcp(Vec<u8>),
    /// EFA: data pulled from client GPU via session.read.
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

/// DRAM-only SET: alloc in DRAMPool, fill, create ObjectContext + LoValue.
/// No NVMe. Synchronous for TCP, tokio for EFA.
fn execute_set_dram_only(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();

    // Invalidate existing entry if present (LO.SET overwrites).
    // ObjectId from existing key would be needed here — for now, skip (new key path).
    // TODO: look up existing LoValue to get old object_id and remove from DRAMPool.

    // Alloc from DRAMPool (this IS the final storage).
    let seg_buf = match dram_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
            return;
        }
    };

    let buf_ptr = dram_pool.buffer_ptr(&seg_buf);
    let buf_ptr_usize = buf_ptr as usize;

    match data_source {
        DataSource::Tcp(data) => {
            // TCP: memcpy inline, synchronous.
            let copy_len = data.len().min(obj_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };

            // Compute CRC.
            let crc =
                crc32c::crc32c(unsafe { std::slice::from_raw_parts(buf_ptr, obj_len as usize) });

            // Create ObjectContext (Ready — data is complete).
            let object_id = ObjectId::next();
            let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
            dram_pool.insert_object(object_id, obj_ctx);

            // Create LoValue in keyspace.
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            {
                let ctx = thread_ctx.lock();
                let key_str = ctx.create_string(key_name);
                let key = ctx.open_key_writable(&key_str);
                let lo_value = LoValue {
                    object_id,
                    len: obj_len,
                    crc32c: crc,
                };
                key.set_value(&LO_TYPE, lo_value).unwrap();
            }
            thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: transport.read into DRAMPool buffer via tokio task.
            crate::runtime_handle().spawn(async move {
                let (efa_tx, efa_rx) = tokio::sync::oneshot::channel();
                let ptr = buf_ptr_usize as *mut u8;
                session.read(
                    ptr,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                    Box::new(move |_ptr, result| {
                        let _ = efa_tx.send(result);
                    }),
                );

                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                match efa_rx.await {
                    Ok(Ok(())) => {
                        let crc = crc32c::crc32c(unsafe {
                            std::slice::from_raw_parts(buf_ptr_usize as *const u8, obj_len as usize)
                        });
                        let object_id = ObjectId::next();
                        let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
                        storage::get_dram_pool().insert_object(object_id, obj_ctx);

                        {
                            let ctx = thread_ctx.lock();
                            let key_str = ctx.create_string(key_name);
                            let key = ctx.open_key_writable(&key_str);
                            let lo_value = LoValue {
                                object_id,
                                len: obj_len,
                                crc32c: crc,
                            };
                            key.set_value(&LO_TYPE, lo_value).unwrap();
                        }
                        thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
                    }
                    _ => {
                        storage::get_dram_pool().free(&seg_buf);
                        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_EFA_READ)));
                    }
                }
            });
        }
    }
}

/// Tiered SET: write to NVMe (invalidate DRAMPool entry if exists).
fn execute_set_tiered(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
) {
    let nvme_pool = storage::get_nvme_pool();

    // Alloc NVMePool buffer for the write.
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
            return;
        }
    };

    // StreamingContext owns the NVMePool buffer for this SET operation.
    // Single buffer today; multi-batch streaming adds more buffers here.
    let stream_ctx = storage::StreamingContext::new(vec![seg_buf], obj_len, 1);

    let buf_ptr = nvme_pool.buffer_ptr(&stream_ctx.buffers[0]);
    let buf_ptr_usize = buf_ptr as usize;

    match data_source {
        DataSource::Tcp(data) => {
            // TCP: memcpy into NVMePool buffer, then write to NVMe.
            let copy_len = data.len().min(obj_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write(buf_ptr_usize, obj_len, stream_ctx, blocked_client, key_name)
                    .await;
            });
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: transport.read into NVMePool buffer, then write to NVMe.
            crate::runtime_handle().spawn(async move {
                let (efa_tx, efa_rx) = tokio::sync::oneshot::channel();
                let ptr = buf_ptr_usize as *mut u8;
                session.read(
                    ptr,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                    Box::new(move |_ptr, result| {
                        let _ = efa_tx.send(result);
                    }),
                );

                match efa_rx.await {
                    Ok(Ok(())) => {
                        do_tiered_nvme_write(
                            buf_ptr_usize,
                            obj_len,
                            stream_ctx,
                            blocked_client,
                            key_name,
                        )
                        .await;
                    }
                    _ => {
                        storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
                        let thread_ctx =
                            valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_EFA_READ)));
                    }
                }
            });
        }
    }
}

/// Shared Tiered NVMe write: CRC → open tmp → WriteFixed → rename → create LoValue.
/// Must be called from within a tokio task (awaits io_uring write).
async fn do_tiered_nvme_write(
    buf_ptr_usize: usize,
    obj_len: u64,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
    key_name: Vec<u8>,
) {
    let buf_ptr = buf_ptr_usize as *mut u8;
    let crc = crc32c::crc32c(unsafe { std::slice::from_raw_parts(buf_ptr, obj_len as usize) });

    let object_id = ObjectId::next();
    let dir = crate::data_dir();
    let file_path = object_id.file_path(&dir);
    let c_path = std::ffi::CString::new(file_path.as_str()).unwrap();
    let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        write_flags |= libc::O_DIRECT;
    }
    let fd = unsafe { libc::open(c_path.as_ptr(), write_flags, 0o644) };
    if fd < 0 {
        storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        return;
    }

    let nvme_pool = storage::get_nvme_pool();
    // Single-chunk today: one UringOp for the entire object.
    // Streaming PR will iterate stream_ctx.buffers and submit per-chunk ops in a loop.
    let write_op = uring::UringOp {
        iovec_index: nvme_pool.segments()[stream_ctx.buffers[0].segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr_usize as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    let write_result = uring::submit_write(fd, &write_op).await;
    unsafe { libc::close(fd) };

    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

    match write_result {
        Ok(Ok(())) => {
            {
                let ctx = thread_ctx.lock();
                let key_str = ctx.create_string(key_name);
                let key = ctx.open_key_writable(&key_str);
                let lo_value = LoValue {
                    object_id,
                    len: obj_len,
                    crc32c: crc,
                };
                key.set_value(&LO_TYPE, lo_value).unwrap();
            }
            storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
            thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
        }
        Ok(Err(e)) => {
            let _ = std::fs::remove_file(&file_path);
            storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
            thread_ctx.reply(Err(ValkeyError::String(format!(
                "{}: {}",
                errors::ERR_NVME_WRITE,
                e
            ))));
        }
        Err(_) => {
            let _ = std::fs::remove_file(&file_path);
            storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        }
    }
}

// ─── Serve from DRAMPool ─────────────────────────────────────────────────────

fn serve_from_dram(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &Arc<ObjectContext>,
    obj_len: u64,
    transport: Transport,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
) {
    match transport {
        Transport::Tcp => {
            // Collect data from DRAMPool buffers.
            let mut data = Vec::with_capacity(obj_len as usize);
            for buf in &obj_ctx.buffers {
                let ptr = dram_pool.buffer_ptr(buf);
                let slice = unsafe { std::slice::from_raw_parts(ptr, buf.len as usize) };
                data.extend_from_slice(slice);
            }
            data.truncate(obj_len as usize);
            thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
        }
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: write from DRAMPool buffer to client GPU.
            // Must spawn tokio task for the async EFA write.
            let buf = &obj_ctx.buffers[0]; // Single-chunk for now.
            let buf_ptr = dram_pool.buffer_ptr(buf) as usize;
            crate::runtime_handle().spawn(async move {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let ptr = buf_ptr as *mut u8;
                session.write(
                    ptr,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                    Box::new(move |_ptr, result| {
                        let _ = tx.send(result);
                    }),
                );
                match rx.await {
                    Ok(Ok(())) => thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64))),
                    _ => thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_EFA_WRITE))),
                }
            });
        }
    }
}
