//! io_uring Engine — registered segments, ReadFixed/WriteFixed, CQ poller thread.
//!
//! Architecture:
//!   Caller: submit(IoRequest) via channel → returns immediately
//!   Poller thread: owns io_uring ring, submits ReadFixed/WriteFixed, polls CQ,
//!                  sends completion result via oneshot channel.
//!
//! Segments are registered with IORING_REGISTER_BUFFERS at startup.
//! ReadFixed/WriteFixed use buf_index (segment index) + offset within segment.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};
use tokio::sync::oneshot;

use super::StorageError;

// ─── NVMe Disk Usage Tracking ────────────────────────────────────────────────

/// Tracks total NVMe disk usage in bytes. Incremented on file creation, decremented on deletion.
static NVME_DISK_USAGE: AtomicU64 = AtomicU64::new(0);

/// Increment NVMe disk usage after a file is created.
pub fn increase_nvme_disk_usage(bytes: u64) {
    NVME_DISK_USAGE.fetch_add(bytes, Ordering::Relaxed);
}

/// Decrement NVMe disk usage after a file is deleted.
pub fn decrease_nvme_disk_usage(bytes: u64) {
    NVME_DISK_USAGE.fetch_sub(bytes, Ordering::Relaxed);
}

/// Atomically check and reserve NVMe disk capacity.
/// Returns true on success (capacity reserved), false if it would exceed nvme-maxmemory.
/// Thread-safe: uses fetch_update to avoid check-then-act TOCTOU race.
pub fn try_reserve_nvme_capacity(bytes: u64) -> bool {
    let max = crate::nvme_maxmemory();
    if max == 0 {
        // Unlimited — just increment.
        increase_nvme_disk_usage(bytes);
        return true;
    }
    NVME_DISK_USAGE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            if used + bytes <= max {
                Some(used + bytes)
            } else {
                None
            }
        })
        .is_ok()
}

// ─── Request Types ───────────────────────────────────────────────────────────

/// A single buffer operation descriptor for io_uring ReadFixed/WriteFixed.
/// Constructed from ObjectContext or StreamingContext + their owning pool.
/// TODO: Multi-buffer batch support (STORAGE_DESIGN.md §7.3).
#[derive(Clone)]
pub struct UringOp {
    /// Segment's position in the registered iovec array (IORING_REGISTER_BUFFERS).
    pub iovec_index: u16,
    /// Pointer to the buffer within the segment (absolute address).
    pub buf_ptr: *mut u8,
    /// Offset within the NVMe file.
    pub file_offset: u64,
    /// Number of bytes to read/write.
    pub len: u64,
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
    },
    Write {
        tx: oneshot::Sender<Result<(), StorageError>>,
        /// Expected byte count for this I/O op. Short writes are rejected.
        expected_bytes: u64,
    },
}

// ─── Global Engine ───────────────────────────────────────────────────────────

static ENGINE: OnceLock<UringNvmeEngine> = OnceLock::new();

pub fn set_engine(engine: UringNvmeEngine) {
    ENGINE.set(engine).ok();
}

/// Submit an IoRequest to the poller thread.
/// Returns SendError with the request back on failure (channel disconnected)
/// so the caller can extract the oneshot sender and fire an explicit error.
fn submit(req: IoRequest) -> Result<(), crossbeam_channel::SendError<IoRequest>> {
    match ENGINE.get() {
        Some(engine) => engine.tx.send(req),
        None => Err(crossbeam_channel::SendError(req)),
    }
}

// ─── Async submit helpers ────────────────────────────────────────────────────
//
// Create a oneshot channel, send the tx inside the IoRequest to the poller.
// Poller fires tx.send() on CQE completion. Caller awaits rx.

/// Submit a ReadFixed and return a oneshot receiver.
/// If the poller is dead, sends an explicit error on the oneshot.
pub fn submit_read(fd: RawFd, op: &UringOp) -> oneshot::Receiver<Result<u64, StorageError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(crossbeam_channel::SendError(IoRequest::Read { tx, .. })) = submit(IoRequest::Read {
        fd,
        op: op.clone(),
        tx,
    }) {
        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
    }
    rx
}

/// Submit a WriteFixed and return a oneshot receiver.
/// If the poller is dead, sends an explicit error on the oneshot.
pub fn submit_write(fd: RawFd, op: &UringOp) -> oneshot::Receiver<Result<(), StorageError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(crossbeam_channel::SendError(IoRequest::Write { tx, .. })) =
        submit(IoRequest::Write {
            fd,
            op: op.clone(),
            tx,
        })
    {
        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
    }
    rx
}

pub fn shutdown() {
    if let Some(engine) = ENGINE.get() {
        engine.shutdown.store(true, Ordering::Relaxed);
    }
}

// ─── Batch submit helpers ────────────────────────────────────────────────────

/// Submit multiple ReadFixed ops. Returns one receiver per op.
pub fn submit_read_batch(
    fd: RawFd,
    ops: &[UringOp],
) -> Vec<oneshot::Receiver<Result<u64, StorageError>>> {
    ops.iter().map(|op| submit_read(fd, op)).collect()
}

/// Submit multiple WriteFixed ops. Returns one receiver per op.
pub fn submit_write_batch(
    fd: RawFd,
    ops: &[UringOp],
) -> Vec<oneshot::Receiver<Result<(), StorageError>>> {
    ops.iter().map(|op| submit_write(fd, op)).collect()
}

/// Await all read receivers. Drains every receiver before returning so in-flight
/// io_uring ops complete before callers free buffers. Returns first error.
pub async fn await_read_batch(
    receivers: Vec<oneshot::Receiver<Result<u64, StorageError>>>,
) -> Result<(), StorageError> {
    let mut first_err: Option<StorageError> = None;
    for rx in receivers {
        match rx.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(StorageError::IoError { code: libc::EIO });
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Await all write receivers. Drains every receiver before returning so in-flight
/// io_uring ops complete before callers free buffers. Returns first error.
pub async fn await_write_batch(
    receivers: Vec<oneshot::Receiver<Result<(), StorageError>>>,
) -> Result<(), StorageError> {
    let mut first_err: Option<StorageError> = None;
    for rx in receivers {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(StorageError::IoError { code: libc::EIO });
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ─── UringNvmeEngine ─────────────────────────────────────────────────────────

pub struct UringNvmeEngine {
    tx: Sender<IoRequest>,
    shutdown: Arc<AtomicBool>,
    _poller: Option<thread::JoinHandle<()>>,
}

impl UringNvmeEngine {
    /// Create engine and spawn CQ poller thread.
    /// `iovecs` are the registered segments (combined DRAMPool + NVMePool).
    pub fn new(iovecs: Vec<libc::iovec>) -> Self {
        let (tx, rx) = bounded::<IoRequest>(4096);
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
            _poller: Some(poller),
        }
    }

    /// The CQ poller loop — owns the io_uring ring.
    fn poller_loop(rx: Receiver<IoRequest>, shutdown: Arc<AtomicBool>, iovecs: Vec<libc::iovec>) {
        let mut ring = match io_uring::IoUring::new(256) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("largeobj: io_uring init failed: {}", e);
                Self::error_drain_loop(rx, shutdown);
                return;
            }
        };

        let use_fixed = if !iovecs.is_empty() {
            unsafe { ring.submitter().register_buffers(&iovecs) }.is_ok()
        } else {
            false
        };

        if !use_fixed && !iovecs.is_empty() {
            eprintln!("largeobj: IORING_REGISTER_BUFFERS failed, using regular Read/Write");
        }

        let mut pending: HashMap<u64, PendingOp> = HashMap::new();
        let mut next_token: u64 = 1;

        loop {
            if shutdown.load(Ordering::Relaxed) && pending.is_empty() {
                break;
            }

            // Phase 1: Drain channel → build SQEs.
            let mut batch = 0;
            while batch < 64 {
                match rx.try_recv() {
                    Ok(req) => {
                        let token = next_token;
                        next_token += 1;

                        let (sqe, op) = match req {
                            IoRequest::Read { fd, op, tx } => {
                                let read_len = super::align_up(op.len as usize) as u32;
                                let sqe = if use_fixed {
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
                                    },
                                )
                            }
                            IoRequest::Write { fd, op, tx } => {
                                let write_len = super::align_up(op.len as usize) as u32;
                                let sqe = if use_fixed {
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
                                    },
                                )
                            }
                        };

                        unsafe {
                            if ring.submission().is_full() {
                                let _ = ring.submit();
                            }
                            if ring.submission().push(&sqe).is_err() {
                                // SQ full even after flush — send error on oneshot.
                                match op {
                                    PendingOp::Read { tx, .. } => {
                                        let _ = tx.send(Err(StorageError::IoError {
                                            code: libc::EAGAIN,
                                        }));
                                    }
                                    PendingOp::Write { tx, .. } => {
                                        let _ = tx.send(Err(StorageError::IoError {
                                            code: libc::EAGAIN,
                                        }));
                                    }
                                }
                            } else {
                                pending.insert(token, op);
                            }
                        }
                        batch += 1;
                    }
                    Err(_) => break,
                }
            }

            // Phase 2: Submit + wait. Retry on EINTR (max 3 attempts).
            if !pending.is_empty() {
                for _ in 0..3 {
                    match ring.submit_and_wait(1) {
                        Ok(_) => break,
                        Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                        Err(_) => break,
                    }
                }
            } else if shutdown.load(Ordering::Relaxed) {
                break;
            } else {
                thread::sleep(std::time::Duration::from_micros(50));
                continue;
            }

            // Phase 3: Reap CQEs → send results on oneshot channels.
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }

            for (token, result) in completed {
                if let Some(op) = pending.remove(&token) {
                    match op {
                        PendingOp::Read { tx, expected_bytes } => {
                            if result >= 0 && result as u64 >= expected_bytes {
                                let _ = tx.send(Ok(result as u64));
                            } else if result < 0 {
                                let _ = tx.send(Err(StorageError::IoError { code: -result }));
                            } else {
                                // Short read.
                                let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                            }
                        }
                        PendingOp::Write { tx, expected_bytes } => {
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
        }
    }

    fn error_drain_loop(rx: Receiver<IoRequest>, shutdown: Arc<AtomicBool>) {
        loop {
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(req) => match req {
                    IoRequest::Read { tx, .. } => {
                        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                    }
                    IoRequest::Write { tx, .. } => {
                        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                    }
                },
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
}
