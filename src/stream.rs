//! Unified streaming I/O driver for the tiered GET/SET paths.
//!
//! The six `do_tiered_*` task bodies were one batch loop copy-pasted across a
//! 2 (read/write) × 2 (tcp/efa) × 2 (dram/nvme) matrix. This module factors that
//! loop into ONE read driver + ONE write driver, parameterized by three seams:
//!
//!   ChunkPlan  — which pool backs the buffers (DRAM ObjectContext vs NVMe window)
//!   ReadSink   — what happens to each completed READ chunk (TCP accumulate vs EFA write)
//!   WriteSource— where each WRITE chunk's bytes come from (TCP memcpy vs EFA read)
//!
//! plus two policy knobs the read paths differ on:
//!   ProgressHook       — DRAM promotion advances chunks_ready / marks Ready; streaming does not
//!   EfaErrorPolicy     — promotion keeps filling DRAM for coalesced waiters on EFA failure;
//!                        streaming aborts immediately
//!
//! Behaviour is preserved 1:1 with the original six functions. The one intentional
//! unification: both TCP read paths accumulate into the reply Vec during the loop
//! (the old promote path re-collected at the end via collect_dram_bytes — identical
//! bytes in identical order).

// The pool/transport seams are internal traits used only within this crate; the
// raw-pointer buffer accessors and async trait methods are idiomatic here.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(async_fn_in_trait)]

use std::os::unix::io::RawFd;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use valkey_module::{ValkeyError, ValkeyValue};

use crate::data_type::ObjectId;
use crate::engine::{efa_transfer_addrs, reply_err, EfaDirection};
use crate::errors;
use crate::info;
use crate::storage::{self, uring, ChunkIterator, ObjectContext, SegmentBuffer};
use crate::transport::Session;

// ─── Seam 1: which pool backs the chunk buffers ──────────────────────────────

pub trait ChunkPlan {
    fn buffer_ptr(&self, buffer_idx: usize) -> *mut u8;
    fn iovec_index(&self, buffer_idx: usize) -> u16;
}

pub struct NvmePlan<'a> {
    pub buffers: &'a [SegmentBuffer],
    pub pool: &'static storage::NVMePool,
}
impl ChunkPlan for NvmePlan<'_> {
    fn buffer_ptr(&self, i: usize) -> *mut u8 {
        self.pool.buffer_ptr(&self.buffers[i])
    }
    fn iovec_index(&self, i: usize) -> u16 {
        self.pool.iovec_index_for_buf(&self.buffers[i])
    }
}

pub struct DramPlan<'a> {
    pub buffers: &'a [SegmentBuffer],
    pub pool: &'static storage::DRAMPool,
}
impl ChunkPlan for DramPlan<'_> {
    fn buffer_ptr(&self, i: usize) -> *mut u8 {
        self.pool.buffer_ptr(&self.buffers[i])
    }
    fn iovec_index(&self, i: usize) -> u16 {
        self.pool.iovec_index_for_buf(&self.buffers[i])
    }
}

// ─── Generic EFA transfer over generic storage ───────────────────────────────
//
// ONE fan-out-and-drain primitive backs every EFA path. `EfaBatch` accumulates
// per-chunk `efa_transfer_addrs` futures and drains them with first-error
// semantics, recording each chunk's transport CRC (reads) on the iterator so a
// caller can `combine_checksums()`. Every EFA site pushes through this — the two
// whole-object DRAM paths via `efa_transfer_all` below, and the tiered
// `EfaReadSink` / `EfaWriteSource` per batch — so the fan-out logic exists once.

/// Accumulates EFA chunk transfers and drains them once, first-error wins.
/// Records read CRCs on the iterator via `record_checksum` at drain time.
struct EfaBatch {
    in_flight:
        FuturesUnordered<futures::future::BoxFuture<'static, Result<(u32, u32), ValkeyError>>>,
    direction: EfaDirection,
}
impl EfaBatch {
    fn new(direction: EfaDirection) -> Self {
        Self {
            in_flight: FuturesUnordered::new(),
            direction,
        }
    }
    /// Fire one chunk's EFA transfer. `buf_ptr` must stay valid until `drain`.
    fn push(
        &mut self,
        chunk_index: u32,
        buf_ptr: usize,
        addrs: Vec<storage::ClientEFAAddress>,
        session: Arc<Session>,
    ) {
        let direction = self.direction;
        self.in_flight.push(Box::pin(async move {
            let crc = efa_transfer_addrs(&session, buf_ptr, &addrs, direction).await?;
            Ok((chunk_index, crc))
        }));
    }
    /// Drain all in-flight transfers; on a read, record each CRC on `chunk_iter`
    /// (pass `Some`). A write-only transfer records nothing and passes `None`.
    /// Returns the first transport error if any transfer failed.
    async fn drain(
        &mut self,
        mut chunk_iter: Option<&mut ChunkIterator>,
    ) -> Result<(), ValkeyError> {
        let mut first_err: Option<ValkeyError> = None;
        while let Some(result) = self.in_flight.next().await {
            match result {
                Ok((chunk_index, crc)) if self.direction == EfaDirection::Read => {
                    if let Some(ci) = chunk_iter.as_deref_mut() {
                        ci.record_checksum(chunk_index, crc);
                    }
                }
                Ok(_) => {}
                Err(e) if first_err.is_none() => first_err = Some(e),
                Err(_) => {}
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

// The whole-object EFA loop for the two DRAM-EFA paths, whose buffers are all
// allocated upfront so one fan-out over every chunk suffices. Thin wrapper over
// `EfaBatch` — the same primitive the tiered drivers use per batch.
//
// Callers: `serve_from_dram` (Write, no CRC needed) and `execute_set_dram_efa`
// (Read, CRC recorded then combined). Storage is abstracted by `ChunkPlan`, so it
// works identically over a DRAMPool object window or an NVMePool streaming window.
pub async fn efa_transfer_all<P: ChunkPlan>(
    chunk_iter: &mut ChunkIterator,
    plan: &P,
    session: &Arc<Session>,
    direction: EfaDirection,
) -> Result<(), ValkeyError> {
    let mut batch = EfaBatch::new(direction);
    while let Some(chunk) = chunk_iter.next_chunk() {
        let buf_ptr = plan.buffer_ptr(chunk.buffer_idx) as usize;
        let addrs = chunk.addrs.clone().expect("EFA chunk missing addrs");
        batch.push(chunk.index, buf_ptr, addrs, session.clone());
    }
    batch.drain(Some(chunk_iter)).await
}

// ─── Read-path policy knobs ──────────────────────────────────────────────────

/// What to do on an EFA write failure mid-read.
#[derive(Clone, Copy, PartialEq)]
pub enum EfaErrorPolicy {
    /// Streaming GET: reply the error and stop immediately.
    Abort,
    /// DRAM promotion GET: latch the error but keep reading so the DRAM copy
    /// still fills for coalesced waiters; reply the error after the loop.
    ContinueFilling,
}

/// Optional DRAM-promotion side effects run around the batch loop.
pub struct ProgressHook<'a> {
    pub obj_ctx: &'a Arc<ObjectContext>,
    /// Called with the object id to evict the half-filled DRAM entry on NVMe error.
    pub dram_pool: &'static storage::DRAMPool,
}

// ─── Seam 2: what happens to each completed READ chunk ───────────────────────

pub enum SinkOutcome {
    Ok,
    TransportErr(ValkeyError),
}

pub trait ReadSink {
    fn on_chunk(
        &mut self,
        buf_ptr: *mut u8,
        len: usize,
        addrs: Option<&[storage::ClientEFAAddress]>,
    );
    async fn drain_batch(&mut self) -> SinkOutcome;
    fn success_reply(self, obj_len: u64, crc: u32) -> ValkeyValue;
}

/// TCP read: accumulate chunk bytes into one contiguous reply buffer.
pub struct TcpReadSink {
    reply: Vec<u8>,
    bench: bool,
}
impl TcpReadSink {
    pub fn new(obj_len: u64, bench: bool) -> Self {
        Self {
            reply: if bench {
                Vec::new()
            } else {
                Vec::with_capacity(obj_len as usize)
            },
            bench,
        }
    }
}
impl ReadSink for TcpReadSink {
    fn on_chunk(
        &mut self,
        buf_ptr: *mut u8,
        len: usize,
        _addrs: Option<&[storage::ClientEFAAddress]>,
    ) {
        if !self.bench {
            // SAFETY: buffer holds `len` bytes just written by the completed ReadFixed.
            let slice = unsafe { std::slice::from_raw_parts(buf_ptr, len) };
            self.reply.extend_from_slice(slice);
        }
    }
    async fn drain_batch(&mut self) -> SinkOutcome {
        SinkOutcome::Ok
    }
    fn success_reply(self, obj_len: u64, _crc: u32) -> ValkeyValue {
        if self.bench {
            ValkeyValue::Integer(obj_len as i64)
        } else {
            ValkeyValue::StringBuffer(self.reply)
        }
    }
}

/// EFA read: fi_write each chunk to the client, drain per batch (buffers reused).
/// The fan-out/drain is the shared `EfaBatch` primitive, same as `efa_transfer_all`.
pub struct EfaReadSink {
    session: Arc<Session>,
    batch: EfaBatch,
}
impl EfaReadSink {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            batch: EfaBatch::new(EfaDirection::Write),
        }
    }
}
impl ReadSink for EfaReadSink {
    fn on_chunk(
        &mut self,
        buf_ptr: *mut u8,
        _len: usize,
        addrs: Option<&[storage::ClientEFAAddress]>,
    ) {
        let addrs = addrs.expect("EFA chunk missing addrs").to_vec();
        // Serving to the client is a Write transfer with no CRC to record, so the
        // chunk index is unused on drain; 0 is a placeholder.
        self.batch
            .push(0, buf_ptr as usize, addrs, self.session.clone());
    }
    async fn drain_batch(&mut self) -> SinkOutcome {
        match self.batch.drain(None).await {
            Ok(()) => SinkOutcome::Ok,
            Err(e) => SinkOutcome::TransportErr(e),
        }
    }
    fn success_reply(self, _obj_len: u64, crc: u32) -> ValkeyValue {
        // Main's EFA GET replies the bare object CRC (the client already knows the
        // length it requested). NOT an [obj_len, crc] array.
        ValkeyValue::Integer(crc as i64)
    }
}

// ─── The READ driver: one batch loop for all four GET paths ──────────────────

pub struct ReadJob {
    pub fd: RawFd,
    pub obj_len: u64,
    pub chunk_size: usize,
    pub crc32c_expected: u32,
    pub object_id: ObjectId,
    pub hdr_iovec: u16,
    pub hdr_ptr: usize,
    pub efa_error_policy: EfaErrorPolicy,
    /// SQEs submitted per batch. Promotion throttles to max_buffers_per_op even
    /// though all buffers are allocated upfront; streaming uses its window width.
    pub batch_width: usize,
}

/// Returns Ok(reply) on success; Err(()) means an error reply was already sent.
pub async fn stream_read<P: ChunkPlan, S: ReadSink>(
    job: ReadJob,
    mut chunk_iter: ChunkIterator,
    plan: &P,
    mut sink: S,
    progress: Option<ProgressHook<'_>>,
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
) -> Result<ValkeyValue, ()> {
    let ReadJob {
        fd,
        obj_len,
        chunk_size,
        crc32c_expected,
        object_id,
        hdr_iovec,
        hdr_ptr,
        efa_error_policy,
        batch_width,
    } = job;

    // Pre-loop: read and verify FileHeader. Panics on corrupt data.
    // TODO: Parallelize header and data read submission. Currently serialized
    // because buffers[0] is shared between the header read and chunk 0's data
    // read — submitting both concurrently causes the data read to overwrite
    // header bytes before validation.
    storage::read_and_verify_file_header(
        fd,
        hdr_iovec,
        hdr_ptr,
        object_id,
        obj_len,
        crc32c_expected,
    )
    .await;

    let total_chunks = chunk_iter.total_chunks();
    let window = batch_width;
    let mut transport_err: Option<ValkeyError> = None;
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = window.min((total_chunks - chunks_done) as usize);
        let batch_start = chunks_done;
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter.next_chunk().unwrap();
            ops.push(uring::UringOp {
                iovec_index: plan.iovec_index(chunk.buffer_idx),
                buf_ptr: plan.buffer_ptr(chunk.buffer_idx),
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let mut completions = uring::into_completions(uring::submit_read_batch(fd, ops));
        while let Some((batch_idx, result)) = completions.next().await {
            if result.is_err() {
                // Drain so kernel ops finish before buffers free (unchanged).
                while completions.next().await.is_some() {}
                if let Some(p) = &progress {
                    p.dram_pool.remove_object(&object_id);
                }
                reply_err(
                    thread_ctx,
                    &info::NVME_READ_ERRORS,
                    ValkeyError::Str(errors::ERR_NVME_READ),
                );
                return Err(());
            }
            // ContinueFilling: once an EFA error is latched, keep reading but stop feeding the sink.
            if transport_err.is_none() {
                let chunk = chunk_iter.peek_chunk(batch_start + batch_idx as u32);
                sink.on_chunk(
                    plan.buffer_ptr(chunk.buffer_idx),
                    chunk.user_data_len,
                    chunk.addrs.as_deref(),
                );
            }
        }
        if transport_err.is_none() {
            if let SinkOutcome::TransportErr(e) = sink.drain_batch().await {
                match efa_error_policy {
                    EfaErrorPolicy::Abort => {
                        reply_err(thread_ctx, &info::EFA_WRITE_ERRORS, e);
                        return Err(());
                    }
                    EfaErrorPolicy::ContinueFilling => {
                        transport_err = Some(e); // latch, keep filling DRAM
                    }
                }
            }
        }
        if let Some(p) = &progress {
            p.obj_ctx.advance_chunks_ready(batch_count as u32);
            // TODO: obj_ctx.notify_progress() for coalesced waiters.
        }
        chunks_done += batch_count as u32;
    }
    if let Some(p) = &progress {
        p.obj_ctx.mark_ready();
    }
    if let Some(e) = transport_err {
        reply_err(thread_ctx, &info::EFA_WRITE_ERRORS, e);
        return Err(());
    }
    Ok(sink.success_reply(obj_len, crc32c_expected))
}

// ═══════════════════════════════════════════════════════════════════════════════
// WRITE driver — one skeleton for the two Tiered NVMe-write SET paths
// ═══════════════════════════════════════════════════════════════════════════════
//
// SET has four paths (2 modes × 2 transports). Only the two Tiered ones write an
// NVMe file, so only they share this reserve/open→WriteFixed→FileHeader skeleton:
//   Tiered TCP → do_tiered_nvme_write_tcp (TcpWriteSource)
//   Tiered EFA → do_tiered_nvme_write_efa (EfaWriteSource)
// The two Dram-mode SET paths deliberately do NOT use this driver — they have no
// fd/file/FileHeader: Dram TCP is a synchronous memcpy (serve_set_dram_tcp), and
// Dram EFA (execute_set_dram_efa) shares only the transport loop, via efa_transfer_all.
//
// Shared skeleton: per-batch (fill buffers + WriteFixed + drain) → object CRC →
// write FileHeader. The only per-transport differences live behind WriteSource:
//   TCP: memcpy inline data into buf + rolling digest, submit the batch, await it.
//   EFA: parallel fi_read into buf, submit each chunk's NVMe write as its read
//        completes, record the chunk's CRC; the object CRC is combined at the end.
//
// Cleanup model matches main: the caller holds an `Arc<ObjectFile>` whose Drop
// removes the file and releases the NVMe disk budget, and an owned `fd` that closes
// on drop. So the driver does NO manual teardown — on any error it just returns the
// failure, and the caller's early `return` drops the ObjectFile + fd, cleaning up
// automatically. On success the driver returns the object CRC; the caller runs
// `set_finalize`, which consumes the ObjectFile into the committed LoValue.

use std::os::unix::io::AsRawFd;

/// Which failure a write batch hit — selects the caller's error reply + metric.
pub enum WriteError {
    /// A chunk's NVMe WriteFixed failed.
    NvmeWrite,
    /// A chunk's EFA fi_read from the client failed (EFA path only).
    EfaRead,
}

/// Per-batch work + object-CRC, the two things the two write paths differ on.
pub trait WriteSource {
    /// Fill this batch's buffers and run their NVMe writes to completion.
    /// Advances `chunk_iter` by `batch_count`.
    async fn process_batch(
        &mut self,
        fd: RawFd,
        chunk_iter: &mut ChunkIterator,
        batch_count: usize,
        chunk_size: usize,
    ) -> Result<(), WriteError>;

    /// Object CRC after all batches. `chunk_iter` carries the per-chunk EFA CRCs.
    fn object_crc(&self, chunk_iter: &ChunkIterator) -> u32;
}

/// TCP write: memcpy inline data into pool buffers, rolling CRC, batched WriteFixed.
pub struct TcpWriteSource<'a> {
    pub data: &'a [u8],
    pub buffers: &'a [SegmentBuffer],
    pub pool: &'static storage::NVMePool,
    pub digest: crc_fast::Digest,
}
impl WriteSource for TcpWriteSource<'_> {
    async fn process_batch(
        &mut self,
        fd: RawFd,
        chunk_iter: &mut ChunkIterator,
        batch_count: usize,
        chunk_size: usize,
    ) -> Result<(), WriteError> {
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let src_offset = chunk.index as usize * chunk_size;
            let src = &self.data[src_offset..src_offset + chunk.user_data_len];
            let buf = &self.buffers[chunk.buffer_idx];
            let dst = self.pool.buffer_ptr(buf);
            // SAFETY: dst is this chunk's pool buffer (>= user_data_len); src is in-bounds.
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, chunk.user_data_len) };
            self.digest.update(src);
            ops.push(uring::UringOp {
                iovec_index: self.pool.iovec_index_for_buf(buf),
                buf_ptr: dst,
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let receivers = uring::submit_write_batch(fd, ops);
        uring::await_batch(receivers, uring::UringDirection::Write)
            .await
            .map_err(|_| WriteError::NvmeWrite)
    }
    fn object_crc(&self, _chunk_iter: &ChunkIterator) -> u32 {
        self.digest.clone().finalize() as u32
    }
}

/// EFA write: parallel fi_read into buffers, submit each chunk's NVMe write as its
/// read completes, record per-chunk CRC; object CRC is combined from the iterator.
pub struct EfaWriteSource<'a> {
    pub session: Arc<Session>,
    pub buffers: &'a [SegmentBuffer],
    pub pool: &'static storage::NVMePool,
}
impl WriteSource for EfaWriteSource<'_> {
    async fn process_batch(
        &mut self,
        fd: RawFd,
        chunk_iter: &mut ChunkIterator,
        batch_count: usize,
        chunk_size: usize,
    ) -> Result<(), WriteError> {
        // Interleaved EFA read → NVMe write: as each EFA read completes, immediately
        // submit that chunk's NVMe write to overlap network and disk I/O.
        let mut efa_futures = FuturesUnordered::new();
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let chunk_index = chunk.index;
            let buf = &self.buffers[chunk.buffer_idx];
            let buf_ptr = self.pool.buffer_ptr(buf) as usize;
            let addrs_owned = chunk.addrs.clone().expect("chunk missing EFA addrs");
            let session = self.session.clone();
            efa_futures.push(async move {
                let crc =
                    efa_transfer_addrs(&session, buf_ptr, &addrs_owned, EfaDirection::Read).await?;
                Ok::<_, ValkeyError>((chunk_index, crc))
            });
        }
        let mut receivers = Vec::new();
        while let Some(result) = efa_futures.next().await {
            match result {
                Ok((chunk_index, crc)) => {
                    chunk_iter.record_checksum(chunk_index, crc);
                    let chunk = chunk_iter.peek_chunk(chunk_index);
                    let buf = &self.buffers[chunk.buffer_idx];
                    receivers.push(uring::submit_write(
                        fd,
                        uring::UringOp {
                            iovec_index: self.pool.iovec_index_for_buf(buf),
                            buf_ptr: self.pool.buffer_ptr(buf),
                            file_offset: storage::FILE_HEADER_SIZE
                                + chunk.index as u64 * chunk_size as u64,
                            len: chunk.user_data_len as u64,
                        },
                    ));
                }
                Err(_) => return Err(WriteError::EfaRead),
            }
        }
        for rx in receivers {
            match rx.await {
                Ok(Ok(())) => {}
                _ => return Err(WriteError::NvmeWrite),
            }
        }
        Ok(())
    }
    fn object_crc(&self, chunk_iter: &ChunkIterator) -> u32 {
        chunk_iter.combine_checksums()
    }
}

/// Runs the batch loop + object CRC + FileHeader write. `fd` is an owned handle
/// (its `.as_raw_fd()` is submitted); the caller keeps ownership so it closes on
/// drop. This driver does NO file/disk teardown — on any error it returns the
/// `WriteError` and the caller's `Arc<ObjectFile>` Drop unlinks the file and
/// releases the disk budget. On success it returns the object CRC to commit.
#[allow(clippy::too_many_arguments)]
pub async fn stream_write<F: AsRawFd, S: WriteSource>(
    fd: &F,
    obj_len: u64,
    chunk_size: usize,
    object_id: ObjectId,
    hdr_buf: &SegmentBuffer,
    nvme_pool: &'static storage::NVMePool,
    batch_width: usize,
    mut chunk_iter: ChunkIterator,
    mut source: S,
) -> Result<u32, WriteError> {
    let raw = fd.as_raw_fd();
    let total_chunks = chunk_iter.total_chunks();
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = batch_width.min((total_chunks - chunks_done) as usize);
        source
            .process_batch(raw, &mut chunk_iter, batch_count, chunk_size)
            .await?;
        chunks_done += batch_count as u32;
    }
    let crc = source.object_crc(&chunk_iter);
    if storage::write_file_header(raw, object_id, obj_len, crc, hdr_buf, nvme_pool)
        .await
        .is_err()
    {
        return Err(WriteError::NvmeWrite);
    }
    Ok(crc)
}
