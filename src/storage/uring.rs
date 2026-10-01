//! io_uring Engine — registered segments, ReadFixed/WriteFixed, CQ poller thread.
//!
//! Only used in Tiered mode. Dram-only mode has no io_uring engine.
//!
//! Architecture (split poller — one independent engine PER POOL):
//!   Caller: submit(PoolId, IoRequest) via that pool's channel → returns immediately
//!   Poller thread (one per pool): owns its OWN io_uring ring + its OWN registered-
//!                  buffer table, submits ReadFixed/WriteFixed, polls its CQ, sends
//!                  the completion result via a oneshot channel.
//!
//! There are TWO engines: one for the DRAM pool, one for the NVMe staging pool.
//! Each registers ONLY its own pool's segments (IORING_REGISTER_BUFFERS is
//! per-ring-fd, so the two tables are fully independent), and `iovec_index` is
//! POOL-LOCAL — an index into that ring's own table. Every op references exactly
//! one registered buffer (its `iovec_index` + `buf_ptr`) and a raw file fd +
//! offset; no op spans both pools, so each op routes cleanly to the ring that
//! owns its buffer's pool (`PoolId`).
//!
//! Only the DRAM pool expands/shrinks, so only the DRAM ring ever rebuilds its
//! table (`submit_reregister(PoolId::Dram)`); the NVMe ring is fixed-size after
//! startup and never rebuilds, so a DRAM-ring swap never stalls the NVMe serving
//! path.
//!
//! Segments are registered with IORING_REGISTER_BUFFERS at startup (per ring).
//! ReadFixed/WriteFixed use buf_index (pool-local segment index) + offset within
//! segment.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};
use tokio::sync::oneshot;

use super::StorageError;

// ─── Pool identity ───────────────────────────────────────────────────────────

/// Which pool (and therefore which io_uring ring + registered-buffer table) an
/// op or a reregister targets. This is the whole generic seam: one `UringEngine`
/// type, two instances distinguished by this field — no trait, no dyn. The
/// poller reads it to know which pool's segments to rebuild on a swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolId {
    Dram,
    Nvme,
}

// ─── Request Types ───────────────────────────────────────────────────────────

/// A single buffer operation descriptor for io_uring ReadFixed/WriteFixed.
/// Constructed from ObjectContext or StreamingContext + their owning pool.
#[derive(Debug)]
pub struct UringOp {
    /// Segment's position in the registered iovec array (IORING_REGISTER_BUFFERS).
    pub iovec_index: u16,
    /// Pointer to the buffer within the segment (absolute address).
    pub buf_ptr: *mut u8,
    /// Offset within the NVMe file.
    pub file_offset: u64,
    /// Number of bytes to read/write.
    pub len: u64,
    /// Whether this op's buffer segment is registered in the io_uring kernel
    /// table. `true` → ReadFixed/WriteFixed (fast path, segment in the
    /// IORING_REGISTER_BUFFERS set); `false` → plain Read/Write (a segment
    /// added after startup that is not yet kernel-registered). Set by the caller
    /// from the owning segment's registration flag at construction time.
    pub use_fixed: bool,
}

// SAFETY: buf_ptr points to segment memory that is stable for module lifetime.
unsafe impl Send for UringOp {}

/// I/O request — internal transport to the poller thread.
/// Contains the oneshot sender directly — no callback boxing.
enum IoRequest {
    Read {
        fd: RawFd,
        op: UringOp,
        tx: oneshot::Sender<Result<u64, StorageError>>,
    },
    Write {
        fd: RawFd,
        op: UringOp,
        tx: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Rebuild THIS ring's fixed-buffer table densely from its OWN pool's
    /// currently-live segments (5.10 has no sparse tables / per-slot updates, so
    /// the only primitive is a whole-table swap and the array must have no
    /// holes). The poller sets `registration_pending`, forces ops issued during
    /// the window onto the non-fixed path, waits for THIS ring's in-flight FIXED
    /// ops to drain, then `unregister_buffers` + `register_buffers(rebuilt)`.
    /// Only this ring is affected — the other pool's ring keeps serving. Fire-
    /// and-forget: sent by expand()/release() after a segment is added/removed
    /// (DRAM ring only in practice).
    Reregister,
}

// SAFETY: IoRequest contains raw pointers (inside UringOp) referring to segment-allocated memory
// that is stable for module lifetime. oneshot::Sender is Send. Only sent across a
// bounded channel to the single poller thread.
unsafe impl Send for IoRequest {}

// ─── Pending Operation Tracking ──────────────────────────────────────────────

enum PendingOp {
    Read {
        tx: oneshot::Sender<Result<u64, StorageError>>,
        /// Expected byte count for this I/O op. Short reads are rejected.
        expected_bytes: u64,
        /// True if issued as ReadFixed — counts against `fixed_in_flight`, which
        /// gates the whole-table registration swap (see `poller_loop`).
        fixed: bool,
    },
    Write {
        tx: oneshot::Sender<Result<(), StorageError>>,
        /// Expected byte count for this I/O op. Short writes are rejected.
        expected_bytes: u64,
        /// True if issued as WriteFixed — counts against `fixed_in_flight`.
        fixed: bool,
    },
}

impl PendingOp {
    /// True if this op was issued on the fixed (ReadFixed/WriteFixed) path.
    fn is_fixed(&self) -> bool {
        match self {
            PendingOp::Read { fixed, .. } | PendingOp::Write { fixed, .. } => *fixed,
        }
    }

    /// Send an error to the waiting caller. Used when submit fails fatally.
    fn send_error(self, code: i32) {
        match self {
            PendingOp::Read { tx, .. } => {
                let _ = tx.send(Err(StorageError::IoError { code }));
            }
            PendingOp::Write { tx, .. } => {
                let _ = tx.send(Err(StorageError::IoError { code }));
            }
        }
    }
}

// ─── Per-Pool Engines ────────────────────────────────────────────────────────

/// One engine per pool: the DRAM ring and the NVMe ring are fully independent
/// (separate ring fd, separate registered-buffer table, separate poller thread).
static DRAM_ENGINE: OnceLock<UringEngine> = OnceLock::new();
static NVME_ENGINE: OnceLock<UringEngine> = OnceLock::new();

/// Install the engine for `pool`. Panics if already set (init runs once).
pub fn set_engine(pool: PoolId, engine: UringEngine) {
    let slot = match pool {
        PoolId::Dram => &DRAM_ENGINE,
        PoolId::Nvme => &NVME_ENGINE,
    };
    if slot.set(engine).is_err() {
        panic!("{:?} engine already initialized", pool);
    }
}

fn engine(pool: PoolId) -> Option<&'static UringEngine> {
    match pool {
        PoolId::Dram => DRAM_ENGINE.get(),
        PoolId::Nvme => NVME_ENGINE.get(),
    }
}

/// Submit an IoRequest to the poller thread of `pool`'s ring.
/// Returns SendError with the request back on failure (channel disconnected)
/// so the caller can extract the oneshot sender and fire an explicit error.
fn submit(pool: PoolId, req: IoRequest) -> Result<(), crossbeam_channel::SendError<IoRequest>> {
    match engine(pool) {
        Some(engine) => engine.tx.send(req),
        None => Err(crossbeam_channel::SendError(req)),
    }
}

// ─── Async submit helpers ────────────────────────────────────────────────────
//
// Create a oneshot channel, send the tx inside the IoRequest to the pool's poller.
// Poller fires tx.send() on CQE completion. Caller awaits rx.

/// Submit a ReadFixed to `pool`'s ring and return a oneshot receiver.
/// If the poller is dead, sends an explicit error on the oneshot.
pub fn submit_read(
    pool: PoolId,
    fd: RawFd,
    op: UringOp,
) -> oneshot::Receiver<Result<u64, StorageError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(crossbeam_channel::SendError(IoRequest::Read { tx, .. })) =
        submit(pool, IoRequest::Read { fd, op, tx })
    {
        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
    }
    rx
}

/// Submit a WriteFixed to `pool`'s ring and return a oneshot receiver.
/// If the poller is dead, sends an explicit error on the oneshot.
pub fn submit_write(
    pool: PoolId,
    fd: RawFd,
    op: UringOp,
) -> oneshot::Receiver<Result<(), StorageError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(crossbeam_channel::SendError(IoRequest::Write { tx, .. })) =
        submit(pool, IoRequest::Write { fd, op, tx })
    {
        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
    }
    rx
}

/// Ask `pool`'s poller to rebuild + re-register its fixed-buffer table after a
/// segment was added (expand) or removed (release). Fire-and-forget, no reply:
/// the swap runs on that ring's poller once its in-flight fixed ops drain, and
/// touches ONLY that ring — the other pool's ring keeps serving uninterrupted.
/// No-op if that pool's engine is down (Dram mode / not yet initialized) — the
/// fixed path is unused there anyway. Only ever called for `PoolId::Dram` today
/// (the NVMe pool is fixed-size and never rebuilds after startup).
pub fn submit_reregister(pool: PoolId) {
    let _ = submit(pool, IoRequest::Reregister);
}

// ─── UringEngine ─────────────────────────────────────────────────────────────

pub struct UringEngine {
    tx: Sender<IoRequest>,
    shutdown: Arc<AtomicBool>,
    _poller: Option<thread::JoinHandle<()>>,
}

impl Drop for UringEngine {
    fn drop(&mut self) {
        // Set shutdown flag BEFORE tx drops. This ensures the poller sees
        // shutdown=true when the channel disconnects, and exits cleanly
        // instead of panicking on unexpected disconnect.
        // Fires on: (1) init failure (local engine dropped), (2) process exit.
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl UringEngine {
    /// Create an engine for `pool`: init its io_uring ring + register its pool's
    /// buffers on the calling thread, then spawn a CQ poller owning that ring.
    /// Returns Err if the kernel doesn't support io_uring or buffer registration
    /// fails. `pool` is captured so the poller's registration swaps rebuild the
    /// correct pool's table.
    pub fn new(pool: PoolId, iovecs: Vec<libc::iovec>) -> Result<Self, String> {
        // Create ring on main thread — fail gracefully instead of panicking.
        let ring =
            io_uring::IoUring::new(256).map_err(|e| format!("io_uring init failed: {}", e))?;
        // Register buffers on main thread.
        if !iovecs.is_empty() {
            unsafe { ring.submitter().register_buffers(&iovecs) }
                .map_err(|e| format!("IORING_REGISTER_BUFFERS failed: {}", e))?;
        }
        let (tx, rx) = bounded::<IoRequest>(4096);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        // Pass the fully initialized ring to the poller thread.
        let poller = thread::Builder::new()
            .name(format!("lo-uring-poller-{:?}", pool))
            .spawn(move || {
                Self::poller_loop(pool, rx, shutdown_clone, ring);
            })
            .expect("failed to spawn io_uring poller thread");
        Ok(Self {
            tx,
            shutdown,
            _poller: Some(poller),
        })
    }

    /// The CQ poller loop — owns one pool's io_uring ring (received fully
    /// initialized). `pool` selects which pool's table a registration swap
    /// rebuilds.
    fn poller_loop(
        pool: PoolId,
        rx: Receiver<IoRequest>,
        shutdown: Arc<AtomicBool>,
        mut ring: io_uring::IoUring,
    ) {
        let mut pending: HashMap<u64, PendingOp> = HashMap::new();
        let mut next_token: u64 = 1;
        let mut channel_alive = true;
        let mut submit_error: Option<i32> = None;
        // Whole-table registration swap state (5.10 — see IoRequest::Reregister).
        // While `registration_pending`, ops are issued NON-fixed (kill-switch); the
        // swap fires once `fixed_in_flight` (in-flight ReadFixed/WriteFixed) hits 0.
        let mut registration_pending = false;
        let mut fixed_in_flight: usize = 0;
        loop {
            // Exit when shutdown requested and all in-flight ops are drained.
            if shutdown.load(Ordering::Relaxed) && pending.is_empty() {
                break;
            }
            // Registration swap: once all in-flight FIXED ops have drained, rebuild
            // the dense buffer table from live segments and re-register it. Ops
            // issued during the window went non-fixed, so their (possibly stale)
            // iovec_index is never used — only pre-window fixed ops had to drain.
            if registration_pending && fixed_in_flight == 0 {
                let iovecs = super::rebuild_dense_iovecs_for(pool);
                unsafe {
                    let _ = ring.submitter().unregister_buffers();
                    if !iovecs.is_empty() {
                        ring.submitter()
                            .register_buffers(&iovecs)
                            .expect("largeobj: io_uring re-register_buffers failed");
                    }
                }
                registration_pending = false;
            }
            // Channel disconnected without shutdown flag = bug. The sender lives
            // in an OnceLock for the entire process lifetime. If it's gone without
            // shutdown being set, something is seriously wrong.
            if !channel_alive && pending.is_empty() {
                if !shutdown.load(Ordering::Relaxed) {
                    panic!(
                        "largeobj: io_uring poller channel disconnected unexpectedly \
                         (shutdown flag not set)"
                    );
                }
                break;
            }
            // Phase 1: Drain channel → build SQEs.
            // Track which tokens belong to this batch so Phase 3b only errors
            // ops from this batch, not in-flight ops from previous iterations
            // whose buffers the kernel may still be accessing.
            let batch_start_token = next_token;
            let mut batch = 0;
            while batch < 64 {
                // If nothing is pending and this is the first iteration, block until
                // a request arrives. Zero CPU when idle, instant wakeup on work.
                // Once we have at least one pending op or one batched SQE, use try_recv
                // to drain without blocking.
                let req = if pending.is_empty() && batch == 0 {
                    match rx.recv() {
                        Ok(req) => req,
                        Err(_) => {
                            channel_alive = false;
                            break;
                        }
                    }
                } else {
                    match rx.try_recv() {
                        Ok(req) => req,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            channel_alive = false;
                            break;
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                    }
                };
                let token = next_token;
                next_token += 1;
                let (sqe, op) = match req {
                    IoRequest::Reregister => {
                        // Enter the swap window. Ops keep flowing but are forced
                        // non-fixed below until `fixed_in_flight` drains and the
                        // top-of-loop swap re-registers the table. No SQE to build.
                        registration_pending = true;
                        continue;
                    }
                    IoRequest::Read { fd, op, tx } => {
                        let read_len = super::align_up(op.len as usize) as u32;
                        // Kill-switch: while a swap is pending, issue non-fixed so a
                        // stale iovec_index against the about-to-change table is never used.
                        let issue_fixed = op.use_fixed && !registration_pending;
                        let sqe = if issue_fixed {
                            io_uring::opcode::ReadFixed::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr,
                                read_len,
                                op.iovec_index,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        } else {
                            io_uring::opcode::Read::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr,
                                read_len,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        };
                        (
                            sqe,
                            PendingOp::Read {
                                tx,
                                expected_bytes: op.len,
                                fixed: issue_fixed,
                            },
                        )
                    }
                    IoRequest::Write { fd, op, tx } => {
                        let write_len = super::align_up(op.len as usize) as u32;
                        let issue_fixed = op.use_fixed && !registration_pending;
                        let sqe = if issue_fixed {
                            io_uring::opcode::WriteFixed::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr as *const u8,
                                write_len,
                                op.iovec_index,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        } else {
                            io_uring::opcode::Write::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr as *const u8,
                                write_len,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        };
                        (
                            sqe,
                            PendingOp::Write {
                                tx,
                                expected_bytes: op.len,
                                fixed: issue_fixed,
                            },
                        )
                    }
                };
                let op_is_fixed = op.is_fixed();
                unsafe {
                    if ring.submission().is_full() {
                        let _ = ring.submit();
                    }
                    if ring.submission().push(&sqe).is_err() {
                        // SQ full even after flush — error the caller directly.
                        op.send_error(libc::EAGAIN);
                    } else {
                        pending.insert(token, op);
                        if op_is_fixed {
                            fixed_in_flight += 1;
                        }
                    }
                }
                batch += 1;
            }
            // Phase 2: Submit + wait. Retry on EINTR (max 3 attempts).
            if !pending.is_empty() {
                for _ in 0..3 {
                    match ring.submit_and_wait(1) {
                        Ok(_) => break,
                        // Signal interrupted — normal, retry immediately.
                        Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                        // Kernel backpressure — reap CQEs in Phase 3, retry next loop.
                        // Self-resolving: kernel is actively processing, just needs time.
                        Err(ref e)
                            if e.raw_os_error() == Some(libc::EBUSY)
                                || e.raw_os_error() == Some(libc::EAGAIN) =>
                        {
                            break;
                        }
                        // Kernel can't allocate internal resources. Not self-resolving —
                        // reaping CQEs won't free the right memory. Flag it; Phase 3
                        // will error all remaining pending ops after reaping what it can.
                        Err(ref e) if e.raw_os_error() == Some(libc::ENOMEM) => {
                            submit_error = Some(libc::ENOMEM);
                            break;
                        }
                        // Unrecoverable: EFAULT (bad pointer), EINVAL (bad SQE),
                        // EBADF (ring dead), EPERM (environment broken).
                        Err(ref e) => {
                            panic!("largeobj: io_uring submit_and_wait unrecoverable: {}", e);
                        }
                    }
                }
            } else if shutdown.load(Ordering::Relaxed) {
                break;
            }
            // Phase 3: Reap CQEs → send results on oneshot channels.
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }
            for (token, result) in completed {
                if let Some(op) = pending.remove(&token) {
                    if op.is_fixed() {
                        fixed_in_flight -= 1;
                    }
                    match op {
                        PendingOp::Read {
                            tx, expected_bytes, ..
                        } => {
                            if result >= 0 && result as u64 >= expected_bytes {
                                let _ = tx.send(Ok(result as u64));
                            } else if result < 0 {
                                let _ = tx.send(Err(StorageError::IoError { code: -result }));
                            } else {
                                // Short read.
                                let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                            }
                        }
                        PendingOp::Write {
                            tx, expected_bytes, ..
                        } => {
                            if result >= 0 && result as u64 >= expected_bytes {
                                let _ = tx.send(Ok(()));
                            } else if result < 0 {
                                let _ = tx.send(Err(StorageError::IoError { code: -result }));
                            } else {
                                // Short write.
                                let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                            }
                        }
                    }
                }
            }
            // Phase 3b: If submit failed fatally (ENOMEM), error ops from this
            // batch only. In-flight ops from previous iterations stay in pending —
            // their buffers are still being accessed by the kernel via DMA, and
            // erroring them would let callers free buffers mid-I/O (corruption).
            if let Some(code) = submit_error.take() {
                let batch_tokens: Vec<u64> = pending
                    .keys()
                    .filter(|&&t| t >= batch_start_token)
                    .copied()
                    .collect();
                for token in batch_tokens {
                    if let Some(op) = pending.remove(&token) {
                        if op.is_fixed() {
                            fixed_in_flight -= 1;
                        }
                        op.send_error(code);
                    }
                }
            }
            // Phase 4: CQ overflow detection — if the kernel dropped completions,
            // pending ops will never complete and tasks will hang forever.
            if ring.completion().overflow() > 0 {
                panic!(
                    "largeobj: io_uring CQ overflow detected ({} dropped). \
                     Pending ops will never complete. Aborting.",
                    ring.completion().overflow()
                );
            }
        }
    }
}
