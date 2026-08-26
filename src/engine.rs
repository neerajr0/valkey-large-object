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

use valkey_module::{enum_configuration, ValkeyError, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::storage::{self, uring, ObjectContext};
use crate::transport::Session;

// ─── Operating Mode ──────────────────────────────────────────────────────────

enum_configuration! {
    /// Operating mode — set via `operating-mode` module enum config.
    /// Tiered (0): objects persist on NVMe, DRAMPool is a read cache with promotion.
    /// DramOnly (1): all objects live exclusively in DRAMPool. No NVMe. Fastest reads.
    #[derive(Debug, PartialEq, Eq, Copy)]
    pub enum OperatingMode {
        Tiered = 0,
        DramOnly = 1,
    }
}

// ─── Transport context passed to engine ──────────────────────────────────────

pub enum Transport {
    Tcp,
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

// ─── GET Engine ──────────────────────────────────────────────────────────────

/// Execute LO.GET with mode + transport routing.
/// Called from the command handler after parsing args.
/// `blocked_client` is used for async paths (tokio spawn).
pub fn execute_get(
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let mode = crate::operating_mode();

    match mode {
        OperatingMode::DramOnly => {
            execute_get_dram_only(object_id, obj_len, transport, blocked_client);
        }
        OperatingMode::Tiered => {
            execute_get_tiered(object_id, obj_len, transport, blocked_client);
        }
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

/// Tiered GET: check DRAMPool → if miss, read from NVMe (with optional promotion).
fn execute_get_tiered(
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();

    // ─── DRAMPool hit check ──────────────────────────────────────────────
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx);
            return;
        }
        // Filling state: promotion in progress.
        // TODO: coalesce — register as waiter on this ObjectContext.
        // For now: fall through to NVMe read (duplicate read / buffers).
    }

    // ─── DRAMPool miss: read from NVMe ───────────────────────────────────
    let nvme_pool = storage::get_nvme_pool();
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
            return;
        }
    };

    // StreamingContext owns the NVMePool buffer for this GET operation.
    // Single buffer today; multi-batch streaming adds more buffers here.
    let stream_ctx = storage::StreamingContext::new_for_get(vec![seg_buf], obj_len);

    // Get fd from FdPool (cached) or open fresh.
    let fd_pool = storage::get_fd_pool();
    let fd = match fd_pool.get(object_id) {
        Some(cached_fd) => cached_fd,
        None => {
            let dir = crate::data_dir();
            let path = object_id.file_path(&dir);
            let c_path = std::ffi::CString::new(path).unwrap();
            let mut flags = libc::O_RDONLY;
            if crate::direct_io() {
                flags |= libc::O_DIRECT;
            }
            let new_fd = unsafe { libc::open(c_path.as_ptr(), flags) };
            if new_fd < 0 {
                nvme_pool.free(&stream_ctx.buffers[0]);
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                return;
            }
            // Cache the fd for future reads.
            fd_pool.insert(object_id, new_fd);
            new_fd
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

    // Spawn tokio task for NVMe read + serve + optional promotion.
    crate::runtime_handle().spawn(async move {
        // submit_read takes *mut u8 — construct from usize at call site (no raw ptr across await).
        let read_result = uring::submit_read(fd, &read_op).await;
        // fd is NOT closed here — FdPool owns it for future reads.

        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        let buf_ptr = buf_ptr_usize as *mut u8;

        match read_result {
            Ok(Ok(_bytes_read)) => {
                // Serve the data.
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
                        // EFA: write buf → client GPU via oneshot bridge.
                        let (efa_tx, efa_rx) = tokio::sync::oneshot::channel();
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

                // ─── Promotion decision ──────────────────────────────────
                // TODO: Use HeavyKeeper or access-count policy to decide.
                // For now: always promote (fill DRAMPool on every miss).
                promote_to_dram(object_id, obj_len);

                // Free NVMePool buffer (served, promotion uses separate ReadFixed).
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
pub fn execute_set(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
) {
    let mode = crate::operating_mode();

    match mode {
        OperatingMode::DramOnly => {
            execute_set_dram_only(key_name, obj_len, data_source, blocked_client);
        }
        OperatingMode::Tiered => {
            execute_set_tiered(key_name, obj_len, data_source, blocked_client);
        }
    }
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
            let oid = ObjectId::next();
            let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
            dram_pool.insert_object(oid, obj_ctx);

            // Create LoValue in keyspace.
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            {
                let ctx = thread_ctx.lock();
                let key_str = ctx.create_string(key_name);
                let key = ctx.open_key_writable(&key_str);
                let lo_value = LoValue {
                    object_id: oid,
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
                        let oid = ObjectId::next();
                        let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
                        storage::get_dram_pool().insert_object(oid, obj_ctx);

                        {
                            let ctx = thread_ctx.lock();
                            let key_str = ctx.create_string(key_name);
                            let key = ctx.open_key_writable(&key_str);
                            let lo_value = LoValue {
                                object_id: oid,
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
    let stream_ctx = storage::StreamingContext::new_for_set(vec![seg_buf], obj_len);

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

    let oid = ObjectId::next();
    let dir = crate::data_dir();
    let file_path = oid.file_path(&dir);
    let tmp_path = format!("{}.tmp", file_path);
    let c_tmp = std::ffi::CString::new(tmp_path.as_str()).unwrap();
    let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        write_flags |= libc::O_DIRECT;
    }
    let fd = unsafe { libc::open(c_tmp.as_ptr(), write_flags, 0o644) };
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
            if std::fs::rename(&tmp_path, &file_path).is_ok() {
                // Cache the fd in FdPool for future reads.
                let c_file = std::ffi::CString::new(file_path.as_str()).unwrap();
                let mut read_flags = libc::O_RDONLY;
                if crate::direct_io() {
                    read_flags |= libc::O_DIRECT;
                }
                let read_fd = unsafe { libc::open(c_file.as_ptr(), read_flags) };
                if read_fd >= 0 {
                    storage::get_fd_pool().insert(oid, read_fd);
                }

                {
                    let ctx = thread_ctx.lock();
                    let key_str = ctx.create_string(key_name);
                    let key = ctx.open_key_writable(&key_str);
                    let lo_value = LoValue {
                        object_id: oid,
                        len: obj_len,
                        crc32c: crc,
                    };
                    key.set_value(&LO_TYPE, lo_value).unwrap();
                }
                storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
                thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
            } else {
                let _ = std::fs::remove_file(&tmp_path);
                storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
            }
        }
        Ok(Err(e)) => {
            let _ = std::fs::remove_file(&tmp_path);
            storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
            thread_ctx.reply(Err(ValkeyError::String(format!(
                "{}: {}",
                errors::ERR_NVME_WRITE,
                e
            ))));
        }
        Err(_) => {
            let _ = std::fs::remove_file(&tmp_path);
            storage::get_nvme_pool().free(&stream_ctx.buffers[0]);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        }
    }
}

// ─── Promotion (Tiered mode: NVMe → DRAMPool) ───────────────────────────────

/// Promote an object to DRAMPool by reading from NVMe directly into DRAMPool buffers.
/// No memcpy. No NVMePool involvement. ReadFixed lands in final DRAMPool location.
/// Called after serving a GET miss (fire-and-forget — doesn't block the reply).
fn promote_to_dram(object_id: ObjectId, obj_len: u64) {
    let dram_pool = storage::get_dram_pool();

    // Don't promote if already cached (hit or Filling).
    if dram_pool.contains_object(&object_id) {
        return;
    }

    // Don't promote objects above the threshold.
    // TODO: configurable dram-pool-max-object-size. For now: 256MB.
    if obj_len > 256 * 1024 * 1024 {
        return;
    }

    // Alloc in DRAMPool (this IS the final storage for the cached copy).
    let seg_buf = match dram_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => return, // DRAMPool full — skip promotion silently.
    };

    let buf_ptr = dram_pool.buffer_ptr(&seg_buf) as usize;
    let promote_op = uring::UringOp {
        iovec_index: dram_pool.segments()[seg_buf.segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    // Insert as Filling (visible but not yet servable).
    let obj_ctx = Arc::new(ObjectContext::new_filling(
        vec![seg_buf.clone()],
        obj_len,
        1,
    ));
    dram_pool.insert_object(object_id, obj_ctx.clone());

    // Get fd from FdPool (cached) or open fresh for promotion read.
    let fd_pool = storage::get_fd_pool();
    let fd = match fd_pool.get(object_id) {
        Some(cached_fd) => cached_fd,
        None => {
            let dir = crate::data_dir();
            let path = object_id.file_path(&dir);
            let c_path = std::ffi::CString::new(path).unwrap();
            let mut flags = libc::O_RDONLY;
            if crate::direct_io() {
                flags |= libc::O_DIRECT;
            }
            let new_fd = unsafe { libc::open(c_path.as_ptr(), flags) };
            if new_fd < 0 {
                // Failed to open — remove the Filling entry and free buffer.
                dram_pool.remove_object(&object_id);
                dram_pool.free(&seg_buf);
                return;
            }
            fd_pool.insert(object_id, new_fd);
            new_fd
        }
    };

    // Spawn promotion read (fire-and-forget — doesn't block the original GET reply).
    crate::runtime_handle().spawn(async move {
        let result = uring::submit_read(fd, &promote_op).await;
        // fd is NOT closed here — FdPool owns it for future reads.

        match result {
            Ok(Ok(_)) => {
                // Promotion complete: advance to Ready.
                obj_ctx.advance_chunks_ready(1);
                // TODO: wake coalesced waiters here.
            }
            _ => {
                // Promotion failed — remove entry and free buffer.
                storage::get_dram_pool().remove_object(&object_id);
                storage::get_dram_pool().free(&seg_buf);
            }
        }
    });
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
