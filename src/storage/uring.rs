//! io_uring Engine — registered buffers, ReadFixed/WriteFixed, CQ poller thread.
//!
//! Architecture:
//!   Main thread:   submit(IoRequest) via channel → returns immediately.
//!   Poller thread: owns the io_uring ring, submits ReadFixed/WriteFixed, polls the
//!                  completion queue, and fires the completion callback.
//!
//! The io_uring protocol bookkeeping (correlating completions back to their request
//! via a token, pushing submission-queue entries, and reaping the completion queue)
//! is encapsulated in `PollerRing`, so `poller_loop` reads as a short description of
//! the poll cycle rather than a wall of ring manipulation.
//!
//! Key optimization: buffers are registered with IORING_REGISTER_BUFFERS at startup.
//! Reads use the ReadFixed opcode — pages are pinned ONCE, eliminating the per-read
//! gup_fast_fallback cost.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};

use super::buffer::Buffer;
use crate::storage::{NvmeEngine, StorageError};

// ─── Tunables ────────────────────────────────────────────────────────────────

/// io_uring submission/completion queue depth.
const RING_DEPTH: u32 = 256;
/// Max requests pulled from the channel before yielding to submit + reap.
const SUBMIT_BATCH: usize = 64;
/// Bounded channel capacity between submitters and the poller thread.
const CHANNEL_CAPACITY: usize = 4096;
/// How often an idle poller wakes to observe the shutdown flag.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// O_DIRECT requires read lengths aligned to the block size (4 KiB).
const DIRECT_IO_ALIGN: u64 = 4096;

/// Sentinel error code used when an op fails before it ever reaches the kernel
/// (engine not ready, submission queue could not accept it, init failure).
const ERR_NOT_SUBMITTED: i32 = -1;

// ─── Request Type ──────────────────────────────────────────────────────────────

/// Completion callbacks — fired from the CQ poller thread.
pub type ReadCallback = Box<dyn FnOnce(Buffer, Result<u64, StorageError>) + Send>;
pub type WriteCallback = Box<dyn FnOnce(Buffer, Result<(), StorageError>) + Send>;

/// A unit of I/O handed to the engine. The owned `buf` travels through the kernel
/// op and is handed back to the caller in `on_complete`, whose Drop returns it to
/// the pool. This single type is used for both the submission and in-flight stages;
/// the poller stores it in `PollerRing::pending` until the matching completion.
pub enum IoRequest {
    Read {
        fd: RawFd,
        buf: Buffer, // owned buffer — travels through the io_uring pipeline
        len: u64,
        on_complete: ReadCallback,
    },
    Write {
        fd: RawFd,
        buf: Buffer,
        len: u64,
        on_complete: WriteCallback,
    },
}

// SAFETY: IoRequest contains raw pointers (inside Buffer) and boxed closures.
// The pointers refer to pool-allocated buffers that are stable for the module's
// lifetime. The closures are Send. The enum is only sent across a bounded channel
// to the poller thread, which is the sole consumer.
unsafe impl Send for IoRequest {}

impl IoRequest {
    /// Build the submission-queue entry for this request, tagged with `token` so the
    /// completion can be correlated back. Uses the *Fixed opcodes when buffers are
    /// registered, falling back to plain Read/Write otherwise.
    fn build_sqe(&self, token: u64, use_fixed: bool) -> io_uring::squeue::Entry {
        use io_uring::{opcode, types::Fd};

        let entry = match *self {
            IoRequest::Read { fd, ref buf, len, .. } => {
                let read_len = align_up(len, DIRECT_IO_ALIGN) as u32;
                if use_fixed {
                    opcode::ReadFixed::new(Fd(fd), buf.ptr(), read_len, buf.idx())
                        .offset(0)
                        .build()
                } else {
                    opcode::Read::new(Fd(fd), buf.ptr(), read_len).offset(0).build()
                }
            }
            IoRequest::Write { fd, ref buf, len, .. } => {
                let write_len = len as u32;
                if use_fixed {
                    opcode::WriteFixed::new(Fd(fd), buf.ptr() as *const u8, write_len, buf.idx())
                        .offset(0)
                        .build()
                } else {
                    opcode::Write::new(Fd(fd), buf.ptr() as *const u8, write_len)
                        .offset(0)
                        .build()
                }
            }
        };
        entry.user_data(token)
    }

    /// Fire the completion callback with the kernel's result. `raw_result` is the
    /// io_uring convention: bytes transferred when `>= 0`, else a negative errno.
    fn complete(self, raw_result: i32) {
        match self {
            IoRequest::Read { buf, on_complete, .. } => {
                let res = if raw_result >= 0 {
                    Ok(raw_result as u64)
                } else {
                    Err(StorageError::IoError { code: -raw_result })
                };
                on_complete(buf, res);
            }
            IoRequest::Write { buf, on_complete, len, .. } => {
                let res = if raw_result < 0 {
                    Err(StorageError::IoError { code: -raw_result })
                } else if (raw_result as u64) >= len {
                    Ok(())
                } else {
                    // Short write.
                    Err(StorageError::IoError { code: ERR_NOT_SUBMITTED })
                };
                on_complete(buf, res);
            }
        }
    }

    /// Fire the completion callback with an error, for ops that fail before (or
    /// instead of) reaching the kernel. Returns the buffer to the pool.
    fn fail(self, err: StorageError) {
        match self {
            IoRequest::Read { buf, on_complete, .. } => on_complete(buf, Err(err)),
            IoRequest::Write { buf, on_complete, .. } => on_complete(buf, Err(err)),
        }
    }
}

// ─── PollerRing: owns all io_uring bookkeeping ───────────────────────────────

/// Wraps the io_uring ring together with the token↔request correlation map.
/// Everything the kernel-facing protocol needs — assigning tokens, pushing SQEs,
/// reaping CQEs — lives here so the poll loop can stay declarative.
struct PollerRing {
    ring: io_uring::IoUring,
    pending: HashMap<u64, IoRequest>,
    next_token: u64,
    use_fixed: bool,
}

impl PollerRing {
    /// Create the ring and register buffers for fixed I/O. `use_fixed` records
    /// whether IORING_REGISTER_BUFFERS succeeded; if it did not, ops fall back to
    /// plain Read/Write.
    fn new(depth: u32, iovecs: &[libc::iovec]) -> std::io::Result<Self> {
        let ring = io_uring::IoUring::new(depth)?;

        let use_fixed = if iovecs.is_empty() {
            false
        } else {
            // SAFETY: iovecs point to pool-allocated, page-aligned memory that is
            // stable for the module's lifetime. The kernel pins these pages for
            // zero-copy I/O.
            match unsafe { ring.submitter().register_buffers(iovecs) } {
                Ok(()) => true,
                Err(_) => {
                    eprintln!("largeobj: IORING_REGISTER_BUFFERS failed, using regular Read/Write");
                    false
                }
            }
        };

        Ok(Self {
            ring,
            pending: HashMap::new(),
            next_token: 1,
            use_fixed,
        })
    }

    /// True when no ops are in flight — there is nothing to reap.
    fn is_idle(&self) -> bool {
        self.pending.is_empty()
    }

    /// Queue a request for submission. On success it is tracked in `pending` until
    /// its completion is reaped; if the submission queue cannot accept it (even
    /// after flushing), the op is failed rather than silently dropped, so its buffer
    /// returns to the pool and its callback fires.
    fn submit(&mut self, req: IoRequest) {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        let sqe = req.build_sqe(token, self.use_fixed);

        // Flush already-queued SQEs to make room if the queue is full.
        if self.ring.submission().is_full() {
            let _ = self.ring.submit();
        }

        // SAFETY: the SQE references stable pool memory, and the submission queue is
        // only accessed from this single poller thread.
        let pushed = unsafe { self.ring.submission().push(&sqe).is_ok() };
        if pushed {
            self.pending.insert(token, req);
        } else {
            req.fail(StorageError::IoError { code: ERR_NOT_SUBMITTED });
        }
    }

    /// Submit all queued SQEs and block until at least `want` completions arrive.
    fn submit_and_wait(&mut self, want: usize) {
        let _ = self.ring.submit_and_wait(want);
    }

    /// Reap every available completion and fire its callback.
    fn reap_completions(&mut self) {
        // Collect first so the completion-queue borrow ends before we touch `pending`.
        let mut done: Vec<(u64, i32)> = Vec::new();
        for cqe in self.ring.completion() {
            done.push((cqe.user_data(), cqe.result()));
        }
        for (token, result) in done {
            if let Some(req) = self.pending.remove(&token) {
                req.complete(result);
            }
        }
    }
}

// ─── UringNvmeEngine ─────────────────────────────────────────────────────────

pub struct UringNvmeEngine {
    tx: Sender<IoRequest>,
    shutdown: Arc<AtomicBool>,
    poller: Option<thread::JoinHandle<()>>,
}

impl NvmeEngine for UringNvmeEngine {
    fn submit(&self, req: IoRequest) {
        self.tx.send(req).ok();
    }
}

impl UringNvmeEngine {
    /// Create the engine and spawn the CQ poller thread.
    /// `iovecs` are the pool buffers to register with the kernel.
    pub fn new(iovecs: Vec<libc::iovec>) -> Self {
        let (tx, rx) = bounded::<IoRequest>(CHANNEL_CAPACITY);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        // Convert to Send-safe (ptr as usize, len) pairs for the thread boundary.
        let buf_info: Vec<(usize, usize)> = iovecs
            .iter()
            .map(|iov| (iov.iov_base as usize, iov.iov_len))
            .collect();

        let poller = thread::Builder::new()
            .name("lo-uring-poller".into())
            .spawn(move || {
                // SAFETY: Reconstruct iovecs inside the poller thread from the
                // (ptr, len) pairs. The underlying memory is pool-allocated and
                // stable for the module's lifetime.
                let iovecs: Vec<libc::iovec> = buf_info
                    .iter()
                    .map(|&(ptr, len)| libc::iovec {
                        iov_base: ptr as *mut libc::c_void,
                        iov_len: len,
                    })
                    .collect();
                Self::poller_loop(rx, shutdown_clone, iovecs);
            })
            .expect("failed to spawn io_uring poller thread");

        Self {
            tx,
            shutdown,
            poller: Some(poller),
        }
    }

    /// Shutdown the engine. Signals the poller to drain in-flight ops then exit.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.poller.take() {
            handle.join().ok();
        }
    }

    /// The CQ poller loop. Owns the ring via `PollerRing` and cycles through:
    /// fill the submission queue from the channel, submit + wait for a completion,
    /// then reap completions and fire callbacks. When nothing is in flight it blocks
    /// on the channel (rather than busy-spinning) until work arrives or shutdown.
    fn poller_loop(rx: Receiver<IoRequest>, shutdown: Arc<AtomicBool>, iovecs: Vec<libc::iovec>) {
        let mut ring = match PollerRing::new(RING_DEPTH, &iovecs) {
            Ok(ring) => ring,
            Err(e) => {
                eprintln!("largeobj: io_uring init failed: {}", e);
                // Fallback: drain requests with errors so callers never hang.
                Self::error_drain_loop(rx, shutdown);
                return;
            }
        };

        loop {
            if shutdown.load(Ordering::Relaxed) && ring.is_idle() {
                break;
            }

            // Nothing in flight → no completions to service, so block on the channel
            // (waking periodically to observe shutdown) instead of busy-polling.
            if ring.is_idle() {
                match rx.recv_timeout(SHUTDOWN_POLL_INTERVAL) {
                    Ok(req) => ring.submit(req),
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }

            // Drain a batch of additional requests without blocking.
            let mut batch = 0;
            while batch < SUBMIT_BATCH {
                match rx.try_recv() {
                    Ok(req) => {
                        ring.submit(req);
                        batch += 1;
                    }
                    Err(_) => break,
                }
            }

            // Submission may have failed for everything we pulled; don't wait on a
            // completion that will never come.
            if ring.is_idle() {
                continue;
            }

            ring.submit_and_wait(1);
            ring.reap_completions();
        }
    }

    /// Used when the ring cannot be initialized: fail every incoming request so
    /// callers get their buffer back and an error instead of hanging.
    fn error_drain_loop(rx: Receiver<IoRequest>, shutdown: Arc<AtomicBool>) {
        loop {
            match rx.recv_timeout(SHUTDOWN_POLL_INTERVAL) {
                Ok(req) => req.fail(StorageError::IoError { code: ERR_NOT_SUBMITTED }),
                Err(RecvTimeoutError::Timeout) => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

/// Round `n` up to the next multiple of `align` (a power of two).
fn align_up(n: u64, align: u64) -> u64 {
    (n + align - 1) & !(align - 1)
}
