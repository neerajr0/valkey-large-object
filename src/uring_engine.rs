//! Direct io_uring architecture: one channel hop, one poller thread.
//!
//! Main thread: BlockClient → send ReadRequest to channel → return immediately
//! Poller thread: owns io_uring ring. Drains channel → submits to SQ → polls CQ → UnblockClient
//!
//! vs old architecture (4 hops):
//!   main → channel → worker → channel → reaper → io_uring → channel → worker → unblock
//! vs this (1 hop):
//!   main → channel → poller(io_uring submit+poll) → unblock

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Sender, Receiver, bounded};

/// A read request from the main thread to the poller.
pub struct ReadRequest {
    pub fd: RawFd,
    pub len: u64,
    pub client_data: *mut ClientData, // heap-allocated, poller owns and frees
}

/// Data associated with a blocked client, passed through the ring.
pub struct ClientData {
    pub blocked_client: valkey_module::BlockedClient,
    pub object_len: u64,
}

unsafe impl Send for ReadRequest {}
unsafe impl Send for ClientData {}

/// Pending read in the io_uring.
struct PendingRead {
    buf: *mut u8,
    layout: std::alloc::Layout,
    actual_len: u64,
    client_data: *mut ClientData,
}

/// The io_uring read engine.
pub struct ReadEngine {
    tx: Sender<ReadRequest>,
    shutdown: Arc<AtomicBool>,
    _poller: Option<thread::JoinHandle<()>>,
    pub submitted: AtomicU64,
}

/// Callback to unblock a client with the result.
/// Called from the poller thread via ThreadSafeContext.

static mut READ_ENGINE: Option<ReadEngine> = None;

pub fn init() {
    unsafe {
        READ_ENGINE = Some(ReadEngine::new());
    }
}

pub fn engine() -> &'static ReadEngine {
    unsafe { READ_ENGINE.as_ref().expect("ReadEngine not initialized") }
}

fn align_up(n: u64) -> u64 {
    (n + 4095) & !4095
}

impl ReadEngine {
    fn new() -> Self {
        // Bounded channel — main thread never blocks (4096 slots)
        let (tx, rx) = bounded::<ReadRequest>(4096);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let poller = thread::Builder::new()
            .name("bigobj-uring-poller".into())
            .spawn(move || {
                Self::poller_loop(rx, shutdown_clone);
            })
            .expect("Failed to spawn poller thread");

        Self {
            tx,
            shutdown,
            _poller: Some(poller),
            submitted: AtomicU64::new(0),
        }
    }

    /// Submit a read request. Called from Valkey main thread. Non-blocking.
    pub fn submit(&self, req: ReadRequest) {
        self.tx.send(req).ok();
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    /// The poller loop: owns the io_uring ring, submits reads, polls completions, unblocks clients.
    fn poller_loop(rx: Receiver<ReadRequest>, shutdown: Arc<AtomicBool>) {
        let mut ring = match io_uring::IoUring::new(256) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("bigobj: io_uring init failed: {}. Falling back to sync poller.", e);
                Self::sync_fallback_loop(rx, shutdown);
                return;
            }
        };

        let mut pending: HashMap<u64, PendingRead> = HashMap::new();
        let mut next_token: u64 = 1;

        loop {
            if shutdown.load(Ordering::Relaxed) && pending.is_empty() {
                break;
            }

            // Phase 1: Drain channel → submit to SQ (batch up to 64)
            let mut batch = 0;
            while batch < 64 {
                match rx.try_recv() {
                    Ok(req) => {
                        let aligned_len = align_up(req.len) as usize;
                        let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
                        let buf = unsafe { std::alloc::alloc_zeroed(layout) };
                        if buf.is_null() {
                            // OOM — unblock with failure
                            let cd = unsafe { Box::from_raw(req.client_data) };
                            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(cd.blocked_client);
                            thread_ctx.reply(Ok(valkey_module::ValkeyValue::Null));
                            continue;
                        }

                        let token = next_token;
                        next_token += 1;

                        pending.insert(token, PendingRead {
                            buf,
                            layout,
                            actual_len: req.len,
                            client_data: req.client_data,
                        });

                        let sqe = io_uring::opcode::Read::new(
                            io_uring::types::Fd(req.fd),
                            buf,
                            aligned_len as u32,
                        )
                        .offset(0)
                        .build()
                        .user_data(token);

                        unsafe {
                            if ring.submission().is_full() {
                                ring.submit().ok();
                            }
                            ring.submission().push(&sqe).ok();
                        }
                        batch += 1;
                    }
                    Err(_) => break, // channel empty
                }
            }

            // Phase 2: Submit + wait for at least 1 completion (or timeout)
            if !pending.is_empty() {
                ring.submit_and_wait(1).ok();
            } else if shutdown.load(Ordering::Relaxed) {
                break;
            } else {
                // Nothing pending, nothing in channel — brief sleep
                thread::sleep(std::time::Duration::from_micros(50));
                continue;
            }

            // Phase 3: Reap completions → UnblockClient
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }

            for (token, result) in completed {
                if let Some(pr) = pending.remove(&token) {
                    let success = result >= 0;
                    // Free the read buffer
                    unsafe { std::alloc::dealloc(pr.buf, pr.layout); }
                    // Unblock the client via ThreadSafeContext
                    let cd = unsafe { Box::from_raw(pr.client_data) };
                    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(cd.blocked_client);
                    if success {
                        thread_ctx.reply(Ok(valkey_module::ValkeyValue::BulkString(
                            format!("OK {}", pr.actual_len)
                        )));
                    } else {
                        thread_ctx.reply(Ok(valkey_module::ValkeyValue::Null));
                    }
                }
            }
        }
    }

    /// Sync fallback if io_uring isn't available.
    fn sync_fallback_loop(rx: Receiver<ReadRequest>, shutdown: Arc<AtomicBool>) {
        loop {
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(req) => {
                    let aligned_len = align_up(req.len) as usize;
                    let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
                    let buf = unsafe { std::alloc::alloc_zeroed(layout) };
                    let success = if !buf.is_null() {
                        let n = unsafe { libc::pread(req.fd, buf as *mut libc::c_void, aligned_len, 0) };
                        unsafe { std::alloc::dealloc(buf, layout); }
                        n >= 0
                    } else {
                        false
                    };
                    let cd = unsafe { Box::from_raw(req.client_data) };
                    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(cd.blocked_client);
                    if success {
                        thread_ctx.reply(Ok(valkey_module::ValkeyValue::BulkString(
                            format!("OK {}", req.len)
                        )));
                    } else {
                        thread_ctx.reply(Ok(valkey_module::ValkeyValue::Null));
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if shutdown.load(Ordering::Relaxed) { break; }
                }
                Err(_) => break,
            }
        }
    }
}
