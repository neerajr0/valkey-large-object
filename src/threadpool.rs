//! Tokio-based thread pool for blocking NVMe I/O.
//! Uses tokio::task::spawn_blocking which dispatches to a dedicated blocking thread pool.
//! Each blocking task gets its own thread-local io_uring ring for inline reads.

use std::os::unix::io::RawFd;
use tokio::runtime::Runtime;

/// Global tokio runtime.
static mut RUNTIME: Option<Runtime> = None;

pub fn init_pool(max_threads: usize) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)  // async workers (minimal — we mostly use blocking)
        .max_blocking_threads(max_threads)  // blocking threads for io_uring reads
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");
    unsafe { RUNTIME = Some(rt); }
}

pub fn runtime() -> &'static Runtime {
    unsafe { RUNTIME.as_ref().expect("Tokio runtime not initialized") }
}

/// Spawn a blocking task on the tokio blocking thread pool.
/// Each blocking thread has its own io_uring ring (thread-local).
pub fn spawn_blocking<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    runtime().spawn(async move {
        tokio::task::spawn_blocking(f).await.ok();
    });
}

/// Per-worker io_uring context for inline reads.
pub struct WorkerUring {
    ring: io_uring::IoUring,
}

impl WorkerUring {
    fn new() -> Option<Self> {
        io_uring::IoUring::new(32).ok().map(|ring| Self { ring })
    }

    /// Blocking O_DIRECT read using io_uring. Returns true if read succeeded.
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

    /// Blocking O_DIRECT read, returns the data.
    pub fn read_file(&mut self, fd: RawFd, len: u64) -> Option<Vec<u8>> {
        let aligned_len = align_up(len) as usize;
        let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
        let buf_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if buf_ptr.is_null() {
            return None;
        }

        let read_op = io_uring::opcode::Read::new(
            io_uring::types::Fd(fd),
            buf_ptr,
            aligned_len as u32,
        )
        .offset(0)
        .build()
        .user_data(1);

        unsafe {
            if self.ring.submission().push(&read_op).is_err() {
                std::alloc::dealloc(buf_ptr, layout);
                return None;
            }
        }

        if self.ring.submit_and_wait(1).is_err() {
            unsafe { std::alloc::dealloc(buf_ptr, layout); }
            return None;
        }

        let ok = self.ring.completion().next().map(|c| c.result() >= 0).unwrap_or(false);
        if ok {
            let data = unsafe { std::slice::from_raw_parts(buf_ptr, len as usize) }.to_vec();
            unsafe { std::alloc::dealloc(buf_ptr, layout); }
            Some(data)
        } else {
            unsafe { std::alloc::dealloc(buf_ptr, layout); }
            None
        }
    }
}

fn align_up(n: u64) -> u64 {
    (n + 4095) & !4095
}

thread_local! {
    static WORKER_URING: std::cell::RefCell<Option<WorkerUring>> = std::cell::RefCell::new(None);
}

/// Access the thread-local io_uring ring (initialized on first use).
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
