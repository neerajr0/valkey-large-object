//! io_uring Engine — registered segments, ReadFixed/WriteFixed, CQ poller thread.
//!
//! Architecture:
//!   Caller: submit(IoRequest) via channel → returns immediately
//!   Poller thread: owns io_uring ring, submits ReadFixed/WriteFixed, polls CQ,
//!                  fires completion callback from CQ thread.
//!
//! Segments are registered with IORING_REGISTER_BUFFERS at startup.
//! ReadFixed/WriteFixed use buf_index (segment index) + offset within segment.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};

use super::StorageError;

// ─── NVMe Disk Usage Tracking ────────────────────────────────────────────────

/// Tracks total NVMe disk usage in bytes. Incremented on file creation, decremented on deletion.
static NVME_DISK_USAGE: AtomicI64 = AtomicI64::new(0);

/// Adjust NVMe disk usage by delta bytes (positive on create, negative on delete).
pub fn adjust_nvme_disk_usage(delta: i64) {
    NVME_DISK_USAGE.fetch_add(delta, Ordering::Relaxed);
}

/// Returns true if writing `obj_len` bytes would stay within nvme-maxmemory.
/// Returns true if nvme-maxmemory is 0 (unlimited).
pub fn check_nvme_capacity(obj_len: u64) -> bool {
    let max = crate::nvme_maxmemory();
    if max == 0 {
        return true;
    }
    let used = NVME_DISK_USAGE.load(Ordering::Relaxed).max(0) as u64;
    used + obj_len <= max
}

// ─── Request Types ───────────────────────────────────────────────────────────

/// A single buffer operation descriptor for io_uring ReadFixed/WriteFixed.
/// Constructed from ObjectContext or StreamingContext + their owning pool.
/// Multi-buffer batch support: pass a Vec<UringOp> to submit_batch (future).
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

/// Completion callback: (result or error). Caller retains buffer ownership.
type ReadCallback = Box<dyn FnOnce(Result<u64, StorageError>) + Send>;
type WriteCallback = Box<dyn FnOnce(Result<(), StorageError>) + Send>;

/// I/O request — internal transport to the poller thread. Not exposed externally.
enum IoRequest {
    Read {
        fd: RawFd,
        op: UringOp,
        on_complete: ReadCallback,
    },
    Write {
        fd: RawFd,
        op: UringOp,
        on_complete: WriteCallback,
    },
}

// SAFETY: IoRequest contains raw pointers (inside UringOp) referring to segment-allocated memory
// that is stable for module lifetime. Closures are Send. Only sent across a
// bounded channel to the single poller thread.
unsafe impl Send for IoRequest {}

// ─── Pending Operation Tracking ──────────────────────────────────────────────

enum PendingOp {
    Read {
        on_complete: ReadCallback,
    },
    Write {
        on_complete: WriteCallback,
        len: u64,
    },
}

// ─── Global Engine ───────────────────────────────────────────────────────────

static ENGINE: OnceLock<UringNvmeEngine> = OnceLock::new();

pub fn set_engine(engine: UringNvmeEngine) {
    ENGINE.set(engine).ok();
}

fn submit(req: IoRequest) {
    if let Some(engine) = ENGINE.get() {
        engine.tx.send(req).ok();
    }
}

// ─── Oneshot-bridged async submit helpers ────────────────────────────────────
//
// These wrap the callback-based submit() with a tokio oneshot channel.
// The io_uring callback fires tx.send(), the tokio task awaits rx.
// io_uring remains callback-based internally — this is just the bridge.

use tokio::sync::oneshot;

/// Submit a ReadFixed and return a oneshot receiver.
/// The tokio task awaits this receiver. io-poller fires it on CQE completion.
pub fn submit_read(
    fd: std::os::unix::io::RawFd,
    op: &UringOp,
) -> oneshot::Receiver<Result<u64, super::StorageError>> {
    let (tx, rx) = oneshot::channel();
    submit(IoRequest::Read {
        fd,
        op: op.clone(),
        on_complete: Box::new(move |result| {
            let _ = tx.send(result);
        }),
    });
    rx
}

/// Submit a WriteFixed and return a oneshot receiver.
pub fn submit_write(
    fd: std::os::unix::io::RawFd,
    op: &UringOp,
) -> oneshot::Receiver<Result<(), super::StorageError>> {
    let (tx, rx) = oneshot::channel();
    submit(IoRequest::Write {
        fd,
        op: op.clone(),
        on_complete: Box::new(move |result| {
            let _ = tx.send(result);
        }),
    });
    rx
}

pub fn shutdown() {
    if let Some(engine) = ENGINE.get() {
        engine.shutdown.store(true, Ordering::Relaxed);
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
                            IoRequest::Read {
                                fd,
                                op,
                                on_complete,
                            } => {
                                let read_len = Self::align_up(op.len) as u32;
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
                                (sqe, PendingOp::Read { on_complete })
                            }
                            IoRequest::Write {
                                fd,
                                op,
                                on_complete,
                            } => {
                                let write_len = Self::align_up(op.len) as u32;
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
                                        on_complete,
                                        len: op.len,
                                    },
                                )
                            }
                        };

                        unsafe {
                            if ring.submission().is_full() {
                                let _ = ring.submit();
                            }
                            if ring.submission().push(&sqe).is_err() {
                                // SQ full even after flush — fire error callback.
                                match op {
                                    PendingOp::Read { on_complete } => {
                                        on_complete(Err(StorageError::IoError {
                                            code: -libc::EAGAIN,
                                        }));
                                    }
                                    PendingOp::Write { on_complete, .. } => {
                                        on_complete(Err(StorageError::IoError {
                                            code: -libc::EAGAIN,
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

            // Phase 3: Reap CQEs → fire callbacks.
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }

            for (token, result) in completed {
                if let Some(op) = pending.remove(&token) {
                    match op {
                        PendingOp::Read { on_complete } => {
                            if result >= 0 {
                                on_complete(Ok(result as u64));
                            } else {
                                on_complete(Err(StorageError::IoError { code: -result }));
                            }
                        }
                        PendingOp::Write { on_complete, len } => {
                            if result >= 0 && result as u64 >= len {
                                on_complete(Ok(()));
                            } else if result < 0 {
                                on_complete(Err(StorageError::IoError { code: -result }));
                            } else {
                                on_complete(Err(StorageError::IoError { code: -1 }));
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
                    IoRequest::Read { on_complete, .. } => {
                        on_complete(Err(StorageError::IoError { code: -1 }));
                    }
                    IoRequest::Write { on_complete, .. } => {
                        on_complete(Err(StorageError::IoError { code: -1 }));
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

    fn align_up(n: u64) -> u64 {
        (n + 4095) & !4095
    }
}
