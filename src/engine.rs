//! Command Engine — routes GET/SET through the correct path based on
//! operating mode (DRAM-only vs Tiered) and transport (TCP vs EFA).
//!
//! Architecture:
//!   TCP GET, DRAMPool hit       → serve inline (no tokio)
//!   TCP GET, DRAMPool miss      → tokio task (Tiered: NVMe read; DRAM-only: impossible)
//!   TCP SET, DRAM-only          → inline (alloc + memcpy, no NVMe)
//!   TCP SET, Tiered             → tokio task (NVMe write)
//!   EFA anything                → tokio task
//!
//! Streaming:
//!   All paths use multi-buffer chunked I/O via ChunkIterator. NVMe paths
//!   write FileHeader at offset 0, data at offset 4096+. SET computes CRC
//!   incrementally per chunk; GET verifies CRC via FileHeader comparison only
//!   (no rolling hash on the read path).
//!
//! Promotion (Tiered GET miss):
//!   If admission policy says yes → alloc ObjectContext in DRAMPool,
//!   ReadFixed directly into DRAMPool buffers, mark Filling→Ready.
//!   Concurrent GETs coalesce on Filling ObjectContext.

use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use valkey_module::{ValkeyError, ValkeyValue, VALKEY_OK};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::info;
use crate::storage::{self, nvme, ChunkIterator, ObjectContext, ObjectFile};
use crate::transport::Session;
use crate::OperatingMode;

// ─── In-flight pin invariant ─────────────────────────────────────────────────
//
// A DEL / overwrite / expiry / eviction / flush runs `lo_free`, which drops the
// keyspace's refs — the DRAM map's `Arc<ObjectContext>` and `LoValue.file` — at
// any await point of an in-flight request. So any async request that reads or
// writes keyspace-reachable backing state across an `.await` MUST hold its own
// clone of that state for the whole operation.
//
// Must pin `Arc<ObjectContext>` (owns the DRAM buffer; its Drop frees it):
//   - Every DRAM serve that transfers a map-resident object — `serve_from_dram`'s
//     EFA path, and `do_tiered_promote_and_serve_tcp/efa`. (TCP serves copy
//     synchronously with no await via `collect_dram_bytes`, so no pin is needed.)
//   - The promotion read, whose target buffer lives in the Filling `ObjectContext`
//     already inserted in the map — the task moves that Arc in for the read.
//   NOT needed on SET: the buffer is private until `set_value` + `insert_object`
//   commit it, so no concurrent free can reach it.
//
// Must pin `Arc<ObjectFile>` (the object's on-disk existence; its Drop unlinks). The
// open fd is a separate `Arc<OwnedFd>` from `ensure_open`, held for the read's duration:
//   - Every Tiered request that READS the object: the NVMe promotion read
//     (`do_tiered_promote_and_serve_tcp/efa`), the transient NVMe read
//     (`do_tiered_nvme_read_and_serve_tcp/efa`), and — by the blanket rule — the
//     DRAM serve that follows a promotion. The `ObjectFile` pin is held for the
//     whole request, read plus transfer, via `_keep_alive = (file, fd)`.
//   NOT needed in Dram mode (there is no `ObjectFile`), and NOT on the SET write
//   path: the `ObjectFile` is created at commit via `set_finalize`, never read
//   during the write. An overwritten old `ObjectFile` is protected by refcount
//   on the replaced `LoValue` (via `lo_free`), not by the writer.
// ─────────────────────────────────────────────────────────────────────────────

// ─── Transport context passed to engine ──────────────────────────────────────

pub enum Transport {
    Tcp,
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

/// Direction for EFA multi-address transfers.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum EfaDirection {
    Read,
    Write,
}

/// Resolved object identity for GET operations — the subset of LoValue fields
/// needed by async read tasks.
struct GetObjectInfo {
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
}

/// Object identity for SET operations — shared fields passed to async write tasks.
struct SetObjectInfo {
    object_id: ObjectId,
    obj_len: u64,
    key_name: Vec<u8>,
}

// ─── Engine Result ────────────────────────────────────────────────────────────

/// Result of an engine dispatch. Command handler matches on this.
pub enum EngineResult {
    /// Sync path completed — return this value directly to Valkey.
    Sync(Result<ValkeyValue, ValkeyError>),
    /// Async path — client is blocked, reply will come from tokio task.
    Async,
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Increment a metric counter and reply with an error.
/// Consolidates the most common error-reply pattern in the engine.
pub(crate) fn reply_err(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    metric: &std::sync::atomic::AtomicU64,
    err: ValkeyError,
) {
    metric.fetch_add(1, Ordering::Relaxed);
    thread_ctx.reply(Err(err));
}

/// Map a `stream::StreamError` to its metric + reply string and send the error.
/// One place, so every path that drives `stream::run` reports failures identically.
fn reply_stream_err(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    e: crate::stream::StreamError,
) {
    use crate::stream::StreamError::*;
    let (metric, err): (&std::sync::atomic::AtomicU64, &str) = match e {
        NvmeRead => (&info::NVME_READ_ERRORS, errors::ERR_NVME_READ),
        NvmeWrite => (&info::NVME_WRITE_ERRORS, errors::ERR_NVME_WRITE),
        EfaRead => (&info::EFA_READ_ERRORS, errors::ERR_EFA_READ),
        EfaWrite => (&info::EFA_WRITE_ERRORS, errors::ERR_EFA_WRITE),
    };
    reply_err(thread_ctx, metric, ValkeyError::Str(err));
}

/// Test hook: pause between NVMe write completion and set_finalize to allow
/// integration tests to inject a DEL and deterministically exercise the
/// delete-during-SET race. Controlled by `test-pause-before-finalize-set-ms`
/// config. 0 = disabled (production default).
async fn test_pause_before_finalize() {
    let pause_ms = crate::test_pause_before_finalize_set_ms();
    if pause_ms > 0 {
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(pause_ms));
        })
        .await
        .ok();
    }
}

/// Collect all DRAMPool buffers into a contiguous Vec for TCP reply.
fn collect_dram_bytes(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &ObjectContext,
    obj_len: u64,
    chunk_iter: &mut ChunkIterator,
) -> Vec<u8> {
    chunk_iter.reset_cursor();
    let mut data = Vec::with_capacity(obj_len as usize);
    while let Some(chunk) = chunk_iter.next_chunk() {
        let buf = &obj_ctx.buffers[chunk.buffer_idx];
        let ptr = dram_pool.buffer_ptr(buf);
        let slice = unsafe { std::slice::from_raw_parts(ptr, chunk.user_data_len) };
        data.extend_from_slice(slice);
    }
    data
}

/// Outcome of `set_finalize` — distinguishes a successful write from a stale discard.
enum SetFinalizeOutcome {
    /// Value was written and attached to the key.
    ValueSet,
    /// A newer version already existed; this write was silently discarded.
    StaleDiscarded,
}

/// Version check + set_value on the async SET path.
/// On success the `ObjectFile` is moved into the `LoValue` and lives with the key.
/// On stale or error the `ObjectFile` drops, which removes the file and releases
/// the NVMe disk budget automatically.
fn set_finalize(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    key_name: &[u8],
    object_file: Arc<ObjectFile>,
    obj_len: u64,
    crc: u32,
) -> Result<SetFinalizeOutcome, ValkeyError> {
    let object_id = object_file.object_id();
    let disk_len = object_file.disk_len();
    let file_path = object_id.file_path(&crate::nvme_dir());
    let ctx = thread_ctx.lock();
    let key_str = ctx.create_string(key_name.to_vec());
    let key = ctx.open_key_writable(&key_str);
    if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
        if existing.object_id > object_id {
            // ObjectFile drops here — removes file + releases disk budget.
            return Ok(SetFinalizeOutcome::StaleDiscarded);
        }
    }
    let on_disk = std::fs::metadata(&file_path)
        .unwrap_or_else(|e| {
            panic!(
                "NVMe accounting: cannot stat object {:?} at {} \
                 to verify write size: {e}",
                object_id, file_path
            )
        })
        .len();
    assert_eq!(
        on_disk, disk_len,
        "NVMe accounting: object {:?} on disk is {on_disk} B but we \
         reserved {} B — write path and accounting have diverged",
        object_id, disk_len
    );
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
        file: Some(object_file),
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        // set_value failed — LoValue dropped, ObjectFile drops, cleanup automatic.
        return Err(ValkeyError::Str(errors::ERR_SET_VALUE));
    }
    Ok(SetFinalizeOutcome::ValueSet)
}

// ═══════════════════════════════════════════════════════════════════════════════
// GET Engine
// ═══════════════════════════════════════════════════════════════════════════════

/// Execute LO.GET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_get(
    ctx: &valkey_module::Context,
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
    file: Option<Arc<ObjectFile>>,
    transport: Transport,
) -> EngineResult {
    let mode = crate::operating_mode();
    match (mode, &transport) {
        (OperatingMode::Dram, Transport::Tcp) => {
            // Fully sync — serve from DRAMPool, return directly.
            EngineResult::Sync(serve_get_dram_tcp(object_id, obj_len))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            match mode {
                OperatingMode::Dram => {
                    execute_get_dram_efa(object_id, obj_len, crc32c, transport, blocked_client);
                }
                OperatingMode::Tiered => {
                    let file =
                        file.expect("Tiered GET: LoValue.file must be Some (created at commit)");
                    execute_get_tiered(object_id, obj_len, crc32c, file, transport, blocked_client);
                }
            }
            EngineResult::Async
        }
    }
}

// ─── DRAM-only TCP GET ────────────────────────────────────────────

/// Sync DRAM-only TCP GET: serve object data directly from DRAMPool.
/// Multi-buffer: collect_dram_bytes iterates all buffers, copying up to obj_len total.
fn serve_get_dram_tcp(object_id: ObjectId, obj_len: u64) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            if crate::bench_mode() {
                Ok(ValkeyValue::Integer(obj_len as i64))
            } else {
                let mut chunk_iter =
                    ChunkIterator::new(obj_len, crate::chunk_size(), obj_ctx.buffers.len(), None);
                Ok(ValkeyValue::StringBuffer(collect_dram_bytes(
                    dram_pool,
                    &obj_ctx,
                    obj_len,
                    &mut chunk_iter,
                )))
            }
        }
        Some(_) => {
            panic!("DRAM-only GET: object in Filling state — SET is synchronous, this is a bug")
        }
        None => panic!("DRAM-only GET: LoValue exists but ObjectContext missing — logic bug"),
    }
}

// ─── DRAM-only EFA GET ────────────────────────────────────────────

/// DRAM-only EFA GET: object MUST be in DRAMPool. If not found → key doesn't exist
/// (shouldn't happen — LoValue exists implies ObjectContext exists in DRAM-only mode).
fn execute_get_dram_efa(
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            // Serve from DRAMPool.
            serve_from_dram(
                dram_pool, &obj_ctx, obj_len, crc32c, transport, thread_ctx, None,
            );
        }
        Some(_obj_ctx) => {
            // TODO: Replace with waiter registration on the watch channel (coalescing).
            todo!("DRAM-only GET: object in Filling state. Needs Request Coalescing");
        }
        None => panic!("DRAM-only GET: LoValue exists but ObjectContext missing — logic bug"),
    }
}

// ─── Tiered GET ──────────────────────────────────────────────

/// Tiered GET: check DRAMPool → try promote → fall back to NVMe.
/// `file` pins the object's `ObjectFile` (existence) for the whole GET operation; the
/// open fd is a separate `Arc<OwnedFd>` obtained via `ensure_open`.
fn execute_get_tiered(
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
    file: Arc<ObjectFile>,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    // ─── DRAMPool hit ────────────────────────────────────────────────────
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            serve_from_dram(
                dram_pool,
                &obj_ctx,
                obj_len,
                crc32c,
                transport,
                thread_ctx,
                Some(file),
            );
            return;
        }
        // Filling state: promotion in progress.
        // TODO: coalesce — register as waiter on this ObjectContext.
        // For now: fall through to NVMe read.
    }
    // ─── Try DRAMPool promotion ──────────────────────────────────────────
    // If pool has space and object is eligible, read directly into DRAMPool.
    if let Some(obj_ctx) = dram_pool.try_promote_object(object_id, obj_len) {
        let fd_pool = storage::get_fd_pool();
        let fd = match file.ensure_open(fd_pool, &crate::nvme_dir()) {
            Some(fd) => fd,
            None => {
                // remove_object drops the map's Arc; obj_ctx drops at end of scope
                // → ObjectContext::Drop frees the buffer automatically.
                dram_pool.remove_object(&object_id);
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                reply_err(
                    &thread_ctx,
                    &info::NVME_READ_ERRORS,
                    ValkeyError::Str(errors::ERR_NVME_READ),
                );
                return;
            }
        };
        let raw_fd = fd.as_raw_fd();
        // All N DRAMPool buffers are allocated upfront by try_promote_object.
        // max_sqes_per_batch throttles how many ReadFixed SQEs we submit per
        // io_uring_submit() call — it does NOT control buffer count.
        let max_sqes_per_batch = crate::max_buffers_per_op();
        match &transport {
            Transport::Tcp => {
                crate::runtime_handle().spawn(async move {
                    let _keep_alive = (file, fd);
                    do_tiered_promote_and_serve_tcp(
                        GetObjectInfo {
                            object_id,
                            obj_len,
                            crc32c,
                        },
                        obj_ctx,
                        raw_fd,
                        blocked_client,
                        max_sqes_per_batch,
                    )
                    .await;
                });
            }
            Transport::Efa {
                session,
                rkey,
                remote_addr,
            } => {
                let session = session.clone();
                let efa_addrs = single_efa_addrs(*rkey, *remote_addr, obj_len);
                crate::runtime_handle().spawn(async move {
                    let _keep_alive = (file, fd);
                    do_tiered_promote_and_serve_efa(
                        GetObjectInfo {
                            object_id,
                            obj_len,
                            crc32c,
                        },
                        obj_ctx,
                        raw_fd,
                        blocked_client,
                        max_sqes_per_batch,
                        session,
                        efa_addrs,
                    )
                    .await;
                });
            }
        }
        return;
    }
    // ─── NVMePool fallback (promotion skipped) ───────────────────────────
    // Reaches here when try_promote_object returns None: pool full, object
    // exceeds max-promote-size, or another GET is already promoting this OID.
    // Future: LRFU admission policy may also reject promotion here.
    let nvme_pool = storage::get_nvme_pool();
    let max_buffers = crate::max_buffers_per_op();
    let min_buffers = crate::min_buffers_per_op();
    let buffers = match nvme_pool.alloc_window(obj_len as usize, max_buffers, min_buffers) {
        Some(bufs) => bufs,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::NVME_BUFFER_EXHAUSTED,
                ValkeyError::Str(errors::ERR_INSUFFICIENT_NVME_BUFFERS),
            );
            return;
        }
    };
    let stream_ctx = storage::StreamingContext::new(buffers);
    let fd_pool = storage::get_fd_pool();
    let fd = match file.ensure_open(fd_pool, &crate::nvme_dir()) {
        Some(fd) => fd,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::NVME_READ_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_READ),
            );
            return;
        }
    };
    let raw_fd = fd.as_raw_fd();
    match &transport {
        Transport::Tcp => {
            crate::runtime_handle().spawn(async move {
                // StreamingContext owns the NVMe buffers (freed on drop). ObjectFile
                // pin and open fd are held alive for the read's duration.
                let _keep_alive = (file, fd);
                do_tiered_nvme_read_and_serve_tcp(
                    object_id,
                    obj_len,
                    crc32c,
                    stream_ctx,
                    raw_fd,
                    blocked_client,
                )
                .await;
            });
        }
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            let session = session.clone();
            let efa_addrs = single_efa_addrs(*rkey, *remote_addr, obj_len);
            crate::runtime_handle().spawn(async move {
                // StreamingContext owns the NVMe buffers (freed on drop). ObjectFile
                // pin and open fd are held alive for the read's duration.
                let _keep_alive = (file, fd);
                do_tiered_nvme_read_and_serve_efa(
                    GetObjectInfo {
                        object_id,
                        obj_len,
                        crc32c,
                    },
                    stream_ctx,
                    raw_fd,
                    blocked_client,
                    session,
                    efa_addrs,
                )
                .await;
            });
        }
    }
}

/// TCP promotion — batched ReadFixed into DRAMPool, then accumulate and reply.
async fn do_tiered_promote_and_serve_tcp(
    get_info: GetObjectInfo,
    obj_ctx: Arc<ObjectContext>,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
    max_sqes_per_batch: usize,
) {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    let chunk_iter = ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), None);
    let hdr_buf = &obj_ctx.buffers[0];
    let job = crate::stream::StreamJob {
        fd,
        obj_len,
        chunk_size,
        object_id,
        crc32c_expected,
        hdr_iovec: dram_pool.iovec_index_for_buf(hdr_buf),
        hdr_ptr: dram_pool.buffer_ptr(hdr_buf) as usize,
        batch_width: max_sqes_per_batch,
        header_write: None,
    };
    // Promotion reads the NVMe file INTO the DRAM promotion buffers (pool=dram),
    // then the target serves them; ProgressHook marks the DRAM entry Ready.
    let source = crate::stream::NvmeSource {
        buffers: &obj_ctx.buffers,
        pool: &dram_pool,
    };
    let target = crate::stream::TcpReplyTarget::new(obj_len, crate::bench_mode());
    let progress = crate::stream::ProgressHook {
        obj_ctx: &obj_ctx,
        dram_pool,
        object_id,
    };
    match crate::stream::run(&job, chunk_iter, &source, &target, Some(&progress), |_| {
        crc32c_expected
    })
    .await
    {
        Ok(done) => match done.target_err {
            Some(e) => reply_stream_err(&thread_ctx, e),
            None => {
                thread_ctx.reply(Ok(target.into_reply(obj_len)));
            }
        },
        Err(e) => reply_stream_err(&thread_ctx, e),
    }
}

/// EFA promotion — batched ReadFixed into DRAMPool + parallelized EFA write per batch.
async fn do_tiered_promote_and_serve_efa(
    get_info: GetObjectInfo,
    obj_ctx: Arc<ObjectContext>,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
    max_sqes_per_batch: usize,
    session: Arc<Session>,
    efa_addrs: Vec<storage::ClientEFAAddress>,
) {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    let chunk_iter =
        ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), Some(efa_addrs));
    let hdr_buf = &obj_ctx.buffers[0];
    let job = crate::stream::StreamJob {
        fd,
        obj_len,
        chunk_size,
        object_id,
        crc32c_expected,
        hdr_iovec: dram_pool.iovec_index_for_buf(hdr_buf),
        hdr_ptr: dram_pool.buffer_ptr(hdr_buf) as usize,
        batch_width: max_sqes_per_batch,
        header_write: None,
    };
    // Promotion reads the NVMe file INTO the DRAM promotion buffers (pool=dram),
    // then the target serves them; ProgressHook marks the DRAM entry Ready.
    let source = crate::stream::NvmeSource {
        buffers: &obj_ctx.buffers,
        pool: &dram_pool,
    };
    let target = crate::stream::EfaTarget { session };
    let progress = crate::stream::ProgressHook {
        obj_ctx: &obj_ctx,
        dram_pool,
        object_id,
    };
    match crate::stream::run(&job, chunk_iter, &source, &target, Some(&progress), |_| {
        crc32c_expected
    })
    .await
    {
        Ok(done) => match done.target_err {
            Some(e) => reply_stream_err(&thread_ctx, e),
            // Main's EFA GET replies the bare object CRC (not an [obj_len, crc] array).
            None => {
                thread_ctx.reply(Ok(ValkeyValue::Integer(crc32c_expected as i64)));
            }
        },
        Err(e) => reply_stream_err(&thread_ctx, e),
    }
}

/// TCP serve-and-discard — batched ReadFixed from NVMe, accumulate into Vec.
async fn do_tiered_nvme_read_and_serve_tcp(
    object_id: ObjectId,
    obj_len: u64,
    crc32c_expected: u32,
    stream_ctx: storage::StreamingContext,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
) {
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, None);
    let hdr_buf = &stream_ctx.buffers[0];
    let job = crate::stream::StreamJob {
        fd,
        obj_len,
        chunk_size,
        object_id,
        crc32c_expected,
        hdr_iovec: nvme_pool.iovec_index_for_buf(hdr_buf),
        hdr_ptr: nvme_pool.buffer_ptr(hdr_buf) as usize,
        batch_width: batch_size,
        header_write: None,
    };
    let source = crate::stream::NvmeSource {
        buffers: &stream_ctx.buffers,
        pool: &nvme_pool,
    };
    let target = crate::stream::TcpReplyTarget::new(obj_len, crate::bench_mode());
    // StreamingContext dropped on return → NVMe buffers freed.
    match crate::stream::run(&job, chunk_iter, &source, &target, None, |_| {
        crc32c_expected
    })
    .await
    {
        Ok(_) => {
            thread_ctx.reply(Ok(target.into_reply(obj_len)));
        }
        Err(e) => reply_stream_err(&thread_ctx, e),
    }
}

/// EFA serve-and-discard — batched ReadFixed from NVMe, parallel EFA write per batch.
async fn do_tiered_nvme_read_and_serve_efa(
    get_info: GetObjectInfo,
    stream_ctx: storage::StreamingContext,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
    session: Arc<Session>,
    efa_addrs: Vec<storage::ClientEFAAddress>,
) {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, Some(efa_addrs));
    let hdr_buf = &stream_ctx.buffers[0];
    let job = crate::stream::StreamJob {
        fd,
        obj_len,
        chunk_size,
        object_id,
        crc32c_expected,
        hdr_iovec: nvme_pool.iovec_index_for_buf(hdr_buf),
        hdr_ptr: nvme_pool.buffer_ptr(hdr_buf) as usize,
        batch_width: batch_size,
        header_write: None,
    };
    let source = crate::stream::NvmeSource {
        buffers: &stream_ctx.buffers,
        pool: &nvme_pool,
    };
    let target = crate::stream::EfaTarget { session };
    match crate::stream::run(&job, chunk_iter, &source, &target, None, |_| {
        crc32c_expected
    })
    .await
    {
        Ok(_) => {
            thread_ctx.reply(Ok(ValkeyValue::Integer(crc32c_expected as i64)));
        }
        Err(e) => reply_stream_err(&thread_ctx, e),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SET Engine
// ═══════════════════════════════════════════════════════════════════════════════

/// Execute LO.SET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_set(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data_source: DataSource,
) -> EngineResult {
    let mode = crate::operating_mode();
    // Assign object_id at command dispatch time (main thread) — establishes
    // ordering by arrival, not completion. Used for version checks on async paths.
    let object_id = ObjectId::next();
    match (mode, &data_source) {
        (OperatingMode::Dram, DataSource::Tcp(data)) => {
            // Fully sync — alloc + memcpy + create LoValue inline.
            EngineResult::Sync(serve_set_dram_tcp(ctx, key_name, obj_len, data, object_id))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            let key_name_bytes = key_name.as_slice().to_vec();
            match mode {
                OperatingMode::Dram => {
                    execute_set_dram_efa(
                        key_name_bytes,
                        obj_len,
                        data_source,
                        blocked_client,
                        object_id,
                    );
                }
                OperatingMode::Tiered => {
                    execute_set_tiered(
                        key_name_bytes,
                        obj_len,
                        data_source,
                        blocked_client,
                        object_id,
                    );
                }
            }
            EngineResult::Async
        }
    }
}

// ─── DRAM-only TCP SET ───────────────────────────────────────────────────────

/// Sync DRAM-only TCP SET: chunked alloc + chunked memcpy + create LoValue.
fn serve_set_dram_tcp(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data: &[u8],
    object_id: ObjectId,
) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, u32::MAX as usize, None);
    let buffers = match dram_pool.alloc_exact(obj_len as usize) {
        Some(bufs) => bufs,
        None => {
            // Reactive expansion: pool exhausted — try adding one segment, then retry.
            if dram_pool.try_expand(ctx).is_none() {
                info::DRAM_POOL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                return Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED));
            }
            match dram_pool.alloc_exact(obj_len as usize) {
                Some(bufs) => bufs,
                None => {
                    info::DRAM_POOL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                    return Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED));
                }
            }
        }
    };
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    while let Some(chunk) = chunk_iter.next_chunk() {
        let src_offset = chunk.index as usize * chunk_size;
        let src = &data[src_offset..src_offset + chunk.user_data_len];
        let buf = &buffers[chunk.buffer_idx];
        let dst = dram_pool.buffer_ptr(buf);
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, chunk.user_data_len) };
        digest.update(src);
    }
    let crc = digest.finalize() as u32;
    // set_value BEFORE insert_object — sync path, no version check needed
    // (single-threaded main thread, our object_id is always the latest).
    // If set_value fails, only the buffers need freeing — no map entry to undo.
    let key = ctx.open_key_writable(key_name);
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
        file: None,
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        dram_pool.free_n(&buffers);
        info::SET_VALUE_FAILURES.fetch_add(1, Ordering::Relaxed);
        return Err(ValkeyError::Str(errors::ERR_SET_VALUE));
    }
    let obj_ctx = Arc::new(ObjectContext::new_ready(buffers));
    dram_pool.insert_object(object_id, obj_ctx);
    VALKEY_OK
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

// ─── DRAM-only EFA SET ───────────────────────────────────────────────────────

/// DRAM-only EFA SET: chunked alloc in DRAMPool, parallel EFA read + post-hoc CRC, create LoValue.
fn execute_set_dram_efa(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    // Overwriting a key is safe: the winning commit's set_value fires lo_free on the
    // replaced LoValue, dropping its Arc<ObjectContext> (the DRAMPool entry). Dram mode
    // has no file, so there is no fd or .dat to tear down here.
    // DRAMPool::alloc_exact: all-or-nothing.
    let buffers = match dram_pool.alloc_exact(obj_len as usize) {
        Some(bufs) => bufs,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::DRAM_POOL_EXHAUSTED,
                ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED),
            );
            return;
        }
    };
    match data_source {
        DataSource::Tcp(_) => unreachable!("Dram+TCP SET routed to sync path"),
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA SET: parallel reads into all buffers, then sequential CRC pass.
            crate::runtime_handle().spawn(async move {
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                let dram_pool = storage::get_dram_pool();
                let efa_addrs = single_efa_addrs(rkey, remote_addr, obj_len);
                let chunk_iter =
                    ChunkIterator::new(obj_len, chunk_size, buffers.len(), Some(efa_addrs));
                // Dram EFA SET: EFA-read every chunk into the DRAM buffers via the
                // ONE streaming driver (source=EFA client, target=DRAM resident).
                let job = crate::stream::StreamJob {
                    fd: -1,
                    obj_len,
                    chunk_size,
                    object_id,
                    crc32c_expected: 0,
                    hdr_iovec: 0,
                    hdr_ptr: 0,
                    batch_width: buffers.len(),
                    header_write: None,
                };
                let source = crate::stream::EfaSource {
                    session,
                    buffers: &buffers,
                    pool: &dram_pool,
                };
                let target = crate::stream::DramTarget;
                let crc = match crate::stream::run(&job, chunk_iter, &source, &target, None, |ci| {
                    ci.combine_checksums()
                })
                .await
                {
                    Ok(done) => done.crc,
                    Err(e) => {
                        reply_stream_err(&thread_ctx, e);
                        dram_pool.free_n(&buffers);
                        return;
                    }
                };
                // Insert ObjectContext BEFORE set_value so the key is never visible
                // without its ObjectContext. On discard, remove the entry —
                // ObjectContext::Drop returns buffers to DRAMPool automatically.
                let obj_ctx = Arc::new(ObjectContext::new_ready(buffers));
                dram_pool.insert_object(object_id, obj_ctx);
                {
                    let ctx = thread_ctx.lock();
                    let key_str = ctx.create_string(key_name.clone());
                    let key = ctx.open_key_writable(&key_str);
                    if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
                        if existing.object_id > object_id {
                            // Stale write — a newer SET already completed.
                            info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
                            dram_pool.remove_object(&object_id);
                            thread_ctx.reply(VALKEY_OK);
                            return;
                        }
                    }
                    let lo_value = LoValue {
                        object_id,
                        len: obj_len,
                        crc32c: crc,
                        file: None,
                    };
                    if key.set_value(&LO_TYPE, lo_value).is_err() {
                        dram_pool.remove_object(&object_id);
                        reply_err(
                            &thread_ctx,
                            &info::SET_VALUE_FAILURES,
                            ValkeyError::Str(errors::ERR_SET_VALUE),
                        );
                        return;
                    }
                }
                thread_ctx.reply(VALKEY_OK);
            });
        }
    }
}

// ─── Tiered SET ──────────────────────────────────────────────────────────────

/// Tiered SET: streaming batch write to NVMe via NVMePool buffer window.
fn execute_set_tiered(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let max_buffers = crate::max_buffers_per_op();
    let min_buffers = crate::min_buffers_per_op();
    let nvme_pool = storage::get_nvme_pool();
    let buffers = match nvme_pool.alloc_window(obj_len as usize, max_buffers, min_buffers) {
        Some(bufs) => bufs,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::NVME_BUFFER_EXHAUSTED,
                ValkeyError::Str(errors::ERR_INSUFFICIENT_NVME_BUFFERS),
            );
            return;
        }
    };
    let stream_ctx = storage::StreamingContext::new(buffers);
    match data_source {
        DataSource::Tcp(data) => {
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write_tcp(
                    data,
                    SetObjectInfo {
                        object_id,
                        obj_len,
                        key_name,
                    },
                    stream_ctx,
                    blocked_client,
                )
                .await;
            });
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            let efa_addrs = single_efa_addrs(rkey, remote_addr, obj_len);
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write_efa(
                    SetObjectInfo {
                        object_id,
                        obj_len,
                        key_name,
                    },
                    stream_ctx,
                    blocked_client,
                    session,
                    efa_addrs,
                )
                .await;
            });
        }
    }
}

/// TCP Tiered SET — CRC computed incrementally, data written in batches, FileHeader last.
async fn do_tiered_nvme_write_tcp(
    data: Vec<u8>,
    set_info: SetObjectInfo,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
) {
    let SetObjectInfo {
        object_id,
        obj_len,
        key_name,
    } = set_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, None);
    let disk_len = storage::object_disk_len(&mut chunk_iter);
    if !nvme::try_reserve_nvme_disk_usage(disk_len) {
        reply_err(
            &thread_ctx,
            &info::NVME_CAPACITY_EXCEEDED,
            ValkeyError::Str(errors::ERR_NVME_CAPACITY_EXCEEDED),
        );
        return;
    }
    // FdPool not used on SET: this write fd is short-lived and never cached.
    // FdPool caches read fds lazily on first GET via ensure_open.
    let dir = crate::nvme_dir();
    let file_path = object_id.file_path(&dir);
    let fd = match storage::open_nvme_file_for_write(&file_path) {
        Ok(fd) => fd,
        Err(_e) => {
            nvme::decrease_nvme_disk_usage(disk_len);
            reply_err(
                &thread_ctx,
                &info::NVME_WRITE_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_WRITE),
            );
            return;
        }
    };
    // ObjectFile owns cleanup from here: Drop removes the file and releases disk budget.
    let object_file = Arc::new(ObjectFile::new(object_id, disk_len));
    let job = crate::stream::StreamJob {
        fd: fd.as_raw_fd(),
        obj_len,
        chunk_size,
        object_id,
        crc32c_expected: 0,
        hdr_iovec: 0,
        hdr_ptr: 0,
        batch_width: batch_size,
        header_write: Some(crate::stream::HeaderWrite {
            hdr_buf: &stream_ctx.buffers[0],
            pool: nvme_pool,
        }),
    };
    let source = crate::stream::TcpInlineSource {
        data: &data,
        buffers: &stream_ctx.buffers,
        pool: &nvme_pool,
    };
    let target = crate::stream::NvmeTarget {
        buffers: &stream_ctx.buffers,
        pool: nvme_pool,
    };
    // TCP object CRC is CRC32C over the whole inline payload, in object order.
    let crc = match crate::stream::run(&job, chunk_iter, &source, &target, None, |_| {
        crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, &data) as u32
    })
    .await
    {
        // On error the fd + object_file drop on return: file unlinked, disk released.
        Ok(done) => done.crc,
        Err(e) => {
            reply_stream_err(&thread_ctx, e);
            return;
        }
    };
    test_pause_before_finalize().await;
    match set_finalize(&thread_ctx, &key_name, object_file, obj_len, crc) {
        Ok(SetFinalizeOutcome::ValueSet) => {
            thread_ctx.reply(VALKEY_OK);
        }
        Ok(SetFinalizeOutcome::StaleDiscarded) => {
            info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
            thread_ctx.reply(VALKEY_OK);
        }
        Err(e) => {
            reply_err(&thread_ctx, &info::SET_VALUE_FAILURES, e);
        }
    }
}

/// EFA Tiered SET — parallel EFA read per batch + post-hoc CRC + batched NVMe write.
async fn do_tiered_nvme_write_efa(
    set_info: SetObjectInfo,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
    session: Arc<Session>,
    efa_addrs: Vec<storage::ClientEFAAddress>,
) {
    let SetObjectInfo {
        object_id,
        obj_len,
        key_name,
    } = set_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, Some(efa_addrs));
    let disk_len = storage::object_disk_len(&mut chunk_iter);
    if !nvme::try_reserve_nvme_disk_usage(disk_len) {
        reply_err(
            &thread_ctx,
            &info::NVME_CAPACITY_EXCEEDED,
            ValkeyError::Str(errors::ERR_NVME_CAPACITY_EXCEEDED),
        );
        return;
    }
    // FdPool not used on SET: this write fd is short-lived and never cached.
    // FdPool caches read fds lazily on first GET via ensure_open.
    let dir = crate::nvme_dir();
    let file_path = object_id.file_path(&dir);
    let fd = match storage::open_nvme_file_for_write(&file_path) {
        Ok(fd) => fd,
        Err(_e) => {
            nvme::decrease_nvme_disk_usage(disk_len);
            reply_err(
                &thread_ctx,
                &info::NVME_WRITE_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_WRITE),
            );
            return;
        }
    };
    // ObjectFile owns cleanup from here: Drop removes the file and releases disk budget.
    let object_file = Arc::new(ObjectFile::new(object_id, disk_len));
    let job = crate::stream::StreamJob {
        fd: fd.as_raw_fd(),
        obj_len,
        chunk_size,
        object_id,
        crc32c_expected: 0,
        hdr_iovec: 0,
        hdr_ptr: 0,
        batch_width: batch_size,
        header_write: Some(crate::stream::HeaderWrite {
            hdr_buf: &stream_ctx.buffers[0],
            pool: nvme_pool,
        }),
    };
    let source = crate::stream::EfaSource {
        session,
        buffers: &stream_ctx.buffers,
        pool: &nvme_pool,
    };
    let target = crate::stream::NvmeTarget {
        buffers: &stream_ctx.buffers,
        pool: nvme_pool,
    };
    // EFA object CRC combines the per-chunk transport CRCs the driver recorded.
    let crc = match crate::stream::run(&job, chunk_iter, &source, &target, None, |ci| {
        ci.combine_checksums()
    })
    .await
    {
        // On error the fd + object_file drop on return: file unlinked, disk released.
        Ok(done) => done.crc,
        Err(e) => {
            reply_stream_err(&thread_ctx, e);
            return;
        }
    };
    test_pause_before_finalize().await;
    match set_finalize(&thread_ctx, &key_name, object_file, obj_len, crc) {
        Ok(SetFinalizeOutcome::ValueSet) => {
            thread_ctx.reply(VALKEY_OK);
        }
        Ok(SetFinalizeOutcome::StaleDiscarded) => {
            info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
            thread_ctx.reply(VALKEY_OK);
        }
        Err(e) => {
            reply_err(&thread_ctx, &info::SET_VALUE_FAILURES, e);
        }
    }
}

// ─── Serve from DRAMPool ─────────────────────────────────────────────────────

/// Serve a Ready ObjectContext from DRAMPool. TCP accumulates into Vec;
/// EFA writes per-chunk to client GPU (parallelized).
/// On the EFA path, `obj_ctx` and `file` are pinned for the async transfer's duration.
fn serve_from_dram(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &Arc<ObjectContext>,
    obj_len: u64,
    crc32c: u32,
    transport: Transport,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    file: Option<Arc<ObjectFile>>,
) {
    match transport {
        Transport::Tcp => {
            if crate::bench_mode() {
                thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
            } else {
                let mut chunk_iter =
                    ChunkIterator::new(obj_len, crate::chunk_size(), obj_ctx.buffers.len(), None);
                thread_ctx.reply(Ok(ValkeyValue::StringBuffer(collect_dram_bytes(
                    dram_pool,
                    obj_ctx,
                    obj_len,
                    &mut chunk_iter,
                ))));
            }
        }
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: write every DRAM buffer to the client via the ONE streaming
            // driver (source=DRAM resident, target=EFA client).
            let obj_ctx = obj_ctx.clone();
            crate::runtime_handle().spawn(async move {
                let _keep_alive = (&obj_ctx, file);
                let dram_pool = storage::get_dram_pool();
                let chunk_size = crate::chunk_size();
                let efa_addrs = single_efa_addrs(rkey, remote_addr, obj_len);
                let chunk_iter =
                    ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), Some(efa_addrs));
                let job = crate::stream::StreamJob {
                    fd: -1,
                    obj_len,
                    chunk_size,
                    object_id: ObjectId(0), // unused: no fd/header on Dram GET
                    crc32c_expected: crc32c,
                    hdr_iovec: 0,
                    hdr_ptr: 0,
                    batch_width: obj_ctx.buffers.len(),
                    header_write: None,
                };
                let source = crate::stream::DramSource {
                    buffers: &obj_ctx.buffers,
                    pool: dram_pool,
                };
                let target = crate::stream::EfaTarget { session };
                match crate::stream::run(&job, chunk_iter, &source, &target, None, |_| crc32c).await
                {
                    Ok(_) => {
                        thread_ctx.reply(Ok(ValkeyValue::Integer(crc32c as i64)));
                    }
                    Err(e) => reply_stream_err(&thread_ctx, e),
                }
            });
        }
    }
}

// ─── EFA Transport Helpers ───────────────────────────────────────────────────

/// Wrap a single contiguous EFA address as a ClientEFAAddress list.
/// Temporary: once multi-address support lands, callers will receive
/// Vec<ClientEFAAddress> directly from the transport layer. For now, we
/// perform the transformation to Vec in this function.
fn single_efa_addrs(rkey: u64, remote_addr: u64, obj_len: u64) -> Vec<storage::ClientEFAAddress> {
    vec![(remote_addr, obj_len as usize, rkey)]
}

/// EFA transfer for a chunk's addresses. Each address is (remote_addr, len, rkey).
/// Fires all sub-transfers in parallel via FuturesUnordered. Returns the combined
/// transport checksum (CRC32C for reads, 0 for writes).
pub(crate) async fn efa_transfer_addrs(
    session: &Arc<Session>,
    buf_ptr: usize,
    addrs: &[storage::ClientEFAAddress],
    direction: EfaDirection,
) -> Result<u32, ValkeyError> {
    // TODO: Track specific EFA error types (e.g. timeout, connection reset) before
    // collapsing to the generic ERR_EFA_READ/ERR_EFA_WRITE reply string.
    let err_str = match direction {
        EfaDirection::Write => errors::ERR_EFA_WRITE,
        EfaDirection::Read => errors::ERR_EFA_READ,
    };
    let mut indexed_futures = FuturesUnordered::new();
    let mut buf_offset = 0usize;
    let mut sub_lens: Vec<usize> = Vec::with_capacity(addrs.len());
    for (i, &(addr, len, rkey)) in addrs.iter().enumerate() {
        let transfer = match direction {
            EfaDirection::Write => {
                session.write((buf_ptr + buf_offset) as *mut u8, len, rkey, addr)
            }
            EfaDirection::Read => session.read((buf_ptr + buf_offset) as *mut u8, len, rkey, addr),
        }
        .map_err(|_| ValkeyError::Str(err_str))?;
        sub_lens.push(len);
        indexed_futures.push(async move { (i, transfer.await) });
        buf_offset += len;
    }
    let mut results: Vec<Option<u32>> = vec![None; addrs.len()];
    while let Some((idx, (outcome, _operand))) = indexed_futures.next().await {
        let done = outcome.map_err(|_| ValkeyError::Str(err_str))?;
        results[idx] = match direction {
            // SET path: transport must provide a checksum for CRC combination.
            EfaDirection::Read => Some(
                done.checksum
                    .expect("EFA Read completion missing checksum — transport must provide CRC"),
            ),
            // GET path: checksum not needed (already stored in FileHeader).
            EfaDirection::Write => Some(0),
        };
    }
    // GET (Write) path: callers ignore the returned CRC — skip combination.
    if matches!(direction, EfaDirection::Write) {
        return Ok(0);
    }
    let mut combined = results[0].expect("EFA transfer result missing") as u64;
    for i in 1..results.len() {
        combined = crc_fast::checksum_combine(
            crc_fast::CrcAlgorithm::Crc32Iscsi,
            combined,
            results[i].expect("EFA transfer result missing") as u64,
            sub_lens[i] as u64,
        );
    }
    Ok(combined as u32)
}
