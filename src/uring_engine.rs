//! Direct io_uring architecture with pre-allocated buffer pool.
//!
//! Main thread: BlockClient → send ReadRequest to channel → return immediately
//! Poller thread: owns io_uring ring + buffer pool. Grabs buffer from pool,
//!   submits to SQ, polls CQ, returns buffer to pool, UnblockClient.
//!
//! Buffer pool: fixed number of 4KB-aligned buffers allocated at startup.
//! No malloc/free on the hot path for objects ≤ buffer size.
//! Objects larger than buffer size fall back to per-request allocation.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Sender, Receiver, bounded};

// ─── Buffer Pool ────────────────────────────────────────────────────────────

/// Pre-allocated buffer pool. Buffers are 4KB-aligned for O_DIRECT.
struct BufferPool {
    buffers: Vec<*mut u8>,
    buf_size: usize,
    layout: std::alloc::Layout,
}

unsafe impl Send for BufferPool {}

impl BufferPool {
    /// Allocate `count` buffers of `buf_size` bytes (must be 4KB-aligned).
    fn new(count: usize, buf_size: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(buf_size, 4096).unwrap();
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            let ptr = unsafe { std::alloc::alloc(layout) };
            if !ptr.is_null() {
                buffers.push(ptr);
            }
        }
        Self { buffers, buf_size, layout }
    }

    /// Get a buffer from the pool. Returns None if pool is exhausted.
    fn get(&mut self) -> Option<*mut u8> {
        self.buffers.pop()
    }

    /// Return a buffer to the pool.
    fn put(&mut self, buf: *mut u8) {
        self.buffers.push(buf);
    }

    /// Buffer size this pool provides.
    fn buf_size(&self) -> usize {
        self.buf_size
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        for buf in self.buffers.drain(..) {
            unsafe { std::alloc::dealloc(buf, self.layout); }
        }
    }
}

// ─── Public types ───────────────────────────────────────────────────────────

/// A read request from the main thread to the poller.
pub struct ReadRequest {
    pub fd: RawFd,
    pub len: u64,
    pub client_data: *mut ClientData,
}

/// Data associated with a blocked client.
pub struct ClientData {
    pub blocked_client: valkey_module::BlockedClient,
    pub object_len: u64,
}

unsafe impl Send for ReadRequest {}
unsafe impl Send for ClientData {}

// ─── Internals ──────────────────────────────────────────────────────────────

/// Pending read in the io_uring.
struct PendingRead {
    buf: *mut u8,
    buf_from_pool: bool,  // true = return to pool on completion; false = dealloc
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

static mut READ_ENGINE: Option<ReadEngine> = None;

pub fn init(buf_size: usize, buf_count: usize) {
    unsafe {
        READ_ENGINE = Some(ReadEngine::new(buf_size, buf_count));
    }
}

pub fn engine() -> &'static ReadEngine {
    unsafe { READ_ENGINE.as_ref().expect("ReadEngine not initialized") }
}

fn align_up(n: u64) -> u64 {
    (n + 4095) & !4095
}

/// Pool config passed at init from module configs.
static mut POOL_BUF_SIZE: usize = 65536;
static mut POOL_BUF_COUNT: usize = 1024;

impl ReadEngine {
    fn new(buf_size: usize, buf_count: usize) -> Self {
        unsafe {
            POOL_BUF_SIZE = buf_size;
            POOL_BUF_COUNT = buf_count;
        }

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

    /// The poller loop with buffer pool.
    fn poller_loop(rx: Receiver<ReadRequest>, shutdown: Arc<AtomicBool>) {
        let mut ring = match io_uring::IoUring::new(256) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("bigobj: io_uring init failed: {}. Falling back to sync poller.", e);
                Self::sync_fallback_loop(rx, shutdown);
                return;
            }
        };

        // Pre-allocate buffer pool (owned by the poller thread — no sharing needed).
        let pool_size = unsafe { POOL_BUF_SIZE };
        let pool_count = unsafe { POOL_BUF_COUNT };
        let mut pool = BufferPool::new(pool_count, pool_size);

        let mut pending: HashMap<u64, PendingRead> = HashMap::new();
        let mut next_token: u64 = 1;

        loop {
            if shutdown.load(Ordering::Relaxed) && pending.is_empty() {
                break;
            }

            // Phase 1: Drain channel → allocate buffer → submit to SQ
            let mut batch = 0;
            while batch < 64 {
                match rx.try_recv() {
                    Ok(req) => {
                        let aligned_len = align_up(req.len) as usize;

                        // Try to get buffer from pool (fast path, no malloc).
                        let (buf, from_pool, layout) = if aligned_len <= pool.buf_size() {
                            match pool.get() {
                                Some(b) => (b, true, std::alloc::Layout::from_size_align(pool.buf_size(), 4096).unwrap()),
                                None => {
                                    // Pool exhausted — fall back to alloc.
                                    let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
                                    let b = unsafe { std::alloc::alloc(layout) };
                                    if b.is_null() {
                                        let cd = unsafe { Box::from_raw(req.client_data) };
                                        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(cd.blocked_client);
                                        thread_ctx.reply(Ok(valkey_module::ValkeyValue::Null));
                                        continue;
                                    }
                                    (b, false, layout)
                                }
                            }
                        } else {
                            // Object larger than pool buffer — per-request alloc.
                            let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
                            let b = unsafe { std::alloc::alloc(layout) };
                            if b.is_null() {
                                let cd = unsafe { Box::from_raw(req.client_data) };
                                let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(cd.blocked_client);
                                thread_ctx.reply(Ok(valkey_module::ValkeyValue::Null));
                                continue;
                            }
                            (b, false, layout)
                        };

                        let token = next_token;
                        next_token += 1;

                        pending.insert(token, PendingRead {
                            buf,
                            buf_from_pool: from_pool,
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
                    Err(_) => break,
                }
            }

            // Phase 2: Submit + wait
            if !pending.is_empty() {
                ring.submit_and_wait(1).ok();
            } else if shutdown.load(Ordering::Relaxed) {
                break;
            } else {
                thread::sleep(std::time::Duration::from_micros(50));
                continue;
            }

            // Phase 3: Reap completions → return buffer → UnblockClient
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }

            for (token, result) in completed {
                if let Some(pr) = pending.remove(&token) {
                    let success = result >= 0;

                    // Return buffer to pool or free it.
                    if pr.buf_from_pool {
                        pool.put(pr.buf); // O(1), no syscall
                    } else {
                        unsafe { std::alloc::dealloc(pr.buf, pr.layout); }
                    }

                    // Unblock client.
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
        let pool_size = unsafe { POOL_BUF_SIZE };
        let pool_count = unsafe { POOL_BUF_COUNT };
        let mut pool = BufferPool::new(pool_count, pool_size);

        loop {
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(req) => {
                    let aligned_len = align_up(req.len) as usize;

                    let (buf, from_pool, layout) = if aligned_len <= pool.buf_size() {
                        match pool.get() {
                            Some(b) => (b, true, std::alloc::Layout::from_size_align(pool.buf_size(), 4096).unwrap()),
                            None => {
                                let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
                                let b = unsafe { std::alloc::alloc(layout) };
                                (b, false, layout)
                            }
                        }
                    } else {
                        let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
                        let b = unsafe { std::alloc::alloc(layout) };
                        (b, false, layout)
                    };

                    let success = if !buf.is_null() {
                        let n = unsafe { libc::pread(req.fd, buf as *mut libc::c_void, aligned_len, 0) };
                        n >= 0
                    } else {
                        false
                    };

                    if from_pool {
                        pool.put(buf);
                    } else if !buf.is_null() {
                        unsafe { std::alloc::dealloc(buf, layout); }
                    }

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
