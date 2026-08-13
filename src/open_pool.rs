//! Open-offload worker pool.
//!
//! In non-pooling mode (`keep-read-fds 0`), BO.GET must `open()` the object's file
//! on demand. Doing that inline on the Valkey main thread blocks the event loop on
//! a cold inode/directory fault from NVMe — see FD_OVERHEAD_TEST.md: sustained-cold
//! collapses to ~7K RPS with the main thread spending ~85% of wall-clock inside
//! `openat`. This pool offloads the `open()` onto N worker threads so the main
//! thread never blocks on it.
//!
//! Pipeline:
//!   main thread ─[open chan]─► open worker: open() ─[read chan]─► poller ─► UnblockClient
//!
//! The open workers are the ONLY new piece; after opening they hand the fd to the
//! existing `ReadEngine` exactly like `bo_get` does today. Enabled via the
//! `open-threads N` module arg (N = 0 disables the pool → `open()` stays inline on
//! the main thread, the original behavior / control arm).
//!
//! Ownership: `client_data` is a raw `*mut ClientData` handed off along the
//! pipeline (main thread → open worker → poller). Exactly one stage owns it at a
//! time, and it is freed exactly once by whichever stage terminates the command —
//! the poller on read completion, or this pool on open failure.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};

use crate::storage::engine::{self, ObjectId};
use crate::uring_engine::{self, ClientData, ReadRequest};

/// A job for the open pool: open this object's file, then start its read.
pub struct OpenRequest {
    /// Valkey hash slot of the key — selects the shard directory.
    pub slot: u16,
    pub oid: ObjectId,
    pub len: u64,
    /// The already-blocked client, carried through to the read stage.
    pub client_data: *mut ClientData,
}
// Safe: the pointer has a single owner in flight (see module ownership note).
unsafe impl Send for OpenRequest {}

pub struct OpenPool {
    tx: Sender<OpenRequest>,
    _workers: Vec<thread::JoinHandle<()>>,
    _shutdown: Arc<AtomicBool>,
}

static mut OPEN_POOL: Option<OpenPool> = None;

/// Initialize the pool with `n` worker threads. Call once at startup, only when
/// `n > 0` (the caller gates on it).
pub fn init(n: usize) {
    unsafe {
        OPEN_POOL = Some(OpenPool::new(n));
    }
}

/// Whether the open pool is active (`open-threads > 0`). When false, `bo_get`
/// opens inline on the main thread (original behavior).
pub fn enabled() -> bool {
    unsafe { OPEN_POOL.is_some() }
}

pub fn pool() -> &'static OpenPool {
    unsafe { OPEN_POOL.as_ref().expect("OpenPool not initialized") }
}

impl OpenPool {
    fn new(n: usize) -> Self {
        // Bounded: if every worker stalls on a cold open and this fills, the main
        // thread's send() blocks — natural backpressure (a limit, not a bug).
        let (tx, rx) = bounded::<OpenRequest>(8192);
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut workers = Vec::with_capacity(n);
        for i in 0..n {
            let rx: Receiver<OpenRequest> = rx.clone(); // clone → SPMC fan-out to N workers
            let sd = shutdown.clone();
            let handle = thread::Builder::new()
                .name(format!("bigobj-open-{i}")) // named → visible in perf / top -H
                .spawn(move || Self::worker_loop(rx, sd))
                .expect("Failed to spawn open worker");
            workers.push(handle);
        }

        Self {
            tx,
            _workers: workers,
            _shutdown: shutdown,
        }
    }

    /// Submit an open job. Called from the Valkey main thread. Non-blocking until
    /// the channel fills (then it applies backpressure).
    pub fn submit(&self, req: OpenRequest) {
        self.tx.send(req).ok();
    }

    fn worker_loop(rx: Receiver<OpenRequest>, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(req) => {
                    // The blocking part — off the main thread, parallel across workers.
                    match engine::engine().open_read_fd_ondemand(req.slot, req.oid) {
                        Some(fd) => {
                            // Record the fd to close after the read, then hand off
                            // to the existing read pipeline (read channel is now MPSC).
                            unsafe {
                                (*req.client_data).close_fd_after = Some(fd);
                            }
                            uring_engine::engine().submit(ReadRequest {
                                fd,
                                len: req.len,
                                client_data: req.client_data,
                            });
                        }
                        None => {
                            // Open failed (missing file / EMFILE). We own the client
                            // here — reply Null and unblock, or it would hang forever.
                            let cd = unsafe { Box::from_raw(req.client_data) };
                            let ctx = valkey_module::ThreadSafeContext::with_blocked_client(
                                cd.blocked_client,
                            );
                            ctx.reply(Ok(valkey_module::ValkeyValue::Null));
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(_) => break, // channel disconnected
            }
        }
    }
}
