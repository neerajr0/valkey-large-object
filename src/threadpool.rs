//! Fixed-size thread pool where each worker owns an io_uring ring.
//! Workers do inline io_uring reads — no channel hop to a separate reaper.
//! This eliminates 3 out of 4 channel transitions in the read path:
//!   Old: main → channel → worker → channel → reaper → io_uring → channel → worker → UnblockClient
//!   New: main → channel → worker → io_uring (inline) → UnblockClient

use crossbeam_channel::{Sender, Receiver, bounded};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A fixed-size thread pool for blocking NVMe I/O.
/// Each worker thread has its own io_uring instance for inline reads.
pub struct ThreadPool {
    sender: Sender<Job>,
    shutdown: Arc<AtomicBool>,
    _workers: Vec<thread::JoinHandle<()>>,
}

/// Global thread pool instance.
static mut POOL: Option<ThreadPool> = None;

pub fn init_pool(size: usize) {
    unsafe {
        POOL = Some(ThreadPool::new(size));
    }
}

pub fn pool() -> &'static ThreadPool {
    unsafe { POOL.as_ref().expect("ThreadPool not initialized") }
}

/// Per-worker io_uring context for inline reads.
/// Each worker can submit and complete reads without cross-thread communication.
pub struct WorkerUring {
    ring: io_uring::IoUring,
}

impl WorkerUring {
    fn new() -> Option<Self> {
        // Each worker gets a small ring (32 entries — only 1 in-flight per job)
        io_uring::IoUring::new(32).ok().map(|ring| Self { ring })
    }

    /// Perform a blocking O_DIRECT read using io_uring.
    /// Returns the data read, or None on failure.
    /// The fd must already be open (from the fd pool).
    pub fn read_file(&mut self, fd: RawFd, len: u64) -> Option<Vec<u8>> {
        let aligned_len = align_up(len) as usize;
        let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
        let buf_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if buf_ptr.is_null() {
            return None;
        }

        // Submit read
        let read_op = io_uring::opcode::Read::new(
            io_uring::types::Fd(fd),
            buf_ptr,
            aligned_len as u32,
        )
        .offset(0)
        .build()
        .user_data(1);

        unsafe {
            self.ring.submission().push(&read_op).ok()?;
        }

        // Submit and wait for completion
        self.ring.submit_and_wait(1).ok()?;

        // Reap
        let cqe = self.ring.completion().next()?;
        let result = cqe.result();

        if result >= 0 {
            // Success — copy actual data (not aligned padding) into a Vec
            let data = unsafe { std::slice::from_raw_parts(buf_ptr, len as usize) }.to_vec();
            unsafe { std::alloc::dealloc(buf_ptr, layout); }
            Some(data)
        } else {
            unsafe { std::alloc::dealloc(buf_ptr, layout); }
            None
        }
    }

    /// Perform a blocking read and return just the length (no data copy).
    /// Used by BO.GET which only needs confirmation, not the actual bytes.
    pub fn read_verify(&mut self, fd: RawFd, len: u64) -> bool {
        let aligned_len = align_up(len) as usize;
        let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
        let buf_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if buf_ptr.is_null() {
            return false;
        }

        let read_op = io_uring::opcode::Read::new(
            io_uring::types::Fd(fd),
            buf_ptr,
            aligned_len as u32,
        )
        .offset(0)
        .build()
        .user_data(1);

        let ok = unsafe { self.ring.submission().push(&read_op).is_ok() }
            && self.ring.submit_and_wait(1).is_ok()
            && self.ring.completion().next().map(|c| c.result() >= 0).unwrap_or(false);

        unsafe { std::alloc::dealloc(buf_ptr, layout); }
        ok
    }
}

fn align_up(n: u64) -> u64 {
    (n + 4095) & !4095
}

/// Thread-local io_uring ring (one per worker).
thread_local! {
    static WORKER_URING: std::cell::RefCell<Option<WorkerUring>> = std::cell::RefCell::new(None);
}

/// Get or initialize the thread-local io_uring ring.
pub fn with_worker_uring<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut WorkerUring) -> R,
{
    WORKER_URING.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            *borrow = WorkerUring::new();
        }
        borrow.as_mut().map(f)
    })
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let (sender, receiver): (Sender<Job>, Receiver<Job>) = bounded(size * 4);
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(size);

        for i in 0..size {
            let rx = receiver.clone();
            let shut = shutdown.clone();

            let handle = thread::Builder::new()
                .name(format!("bigobj-worker-{}", i))
                .spawn(move || {
                    // Initialize thread-local io_uring on first use (lazy in with_worker_uring)
                    while !shut.load(Ordering::Relaxed) {
                        match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                            Ok(job) => job(),
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                            Err(_) => break,
                        }
                    }
                })
                .expect("Failed to spawn worker thread");

            workers.push(handle);
        }

        Self { sender, shutdown, _workers: workers }
    }

    /// Submit a job to the pool.
    pub fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).ok();
    }
}
