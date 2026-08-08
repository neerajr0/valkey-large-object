//! Tokio runtime for async NVMe I/O.
//! Pattern matches ElastiCacheRedisIAM: handle.spawn(async { blocking_io; unblock_client; })
//! Worker threads do blocking pread (O_DIRECT) directly — no spawn_blocking indirection.
//! With 500 worker threads, a 500μs pread block per task is acceptable.

use std::os::unix::io::RawFd;
use tokio::runtime::Runtime;

/// Global tokio runtime.
static mut RUNTIME: Option<Runtime> = None;

pub fn init_pool(max_threads: usize) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(max_threads)  // all workers can do blocking pread
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");
    unsafe { RUNTIME = Some(rt); }
}

pub fn runtime() -> &'static Runtime {
    unsafe { RUNTIME.as_ref().expect("Tokio runtime not initialized") }
}

/// Spawn an async task on the tokio runtime.
/// The closure runs directly on a tokio worker thread.
/// For our use case: the closure does a blocking pread (~500μs) then unblocks the client.
/// Same pattern as ElastiCacheRedisIAM which does blocking UDS I/O in spawned tasks.
pub fn spawn<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    runtime().handle().spawn(async move {
        f();
    });
}

/// Perform a blocking O_DIRECT pread. Called directly inside the async task.
/// No io_uring, no spawn_blocking — just a syscall.
/// Returns true if read succeeded, false otherwise.
pub fn direct_read_verify(fd: RawFd, len: u64) -> bool {
    let aligned_len = align_up(len) as usize;
    let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
    let buf_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if buf_ptr.is_null() {
        return false;
    }

    let n = unsafe {
        libc::pread(fd, buf_ptr as *mut libc::c_void, aligned_len, 0)
    };

    unsafe { std::alloc::dealloc(buf_ptr, layout); }
    n >= 0
}

/// Perform a blocking O_DIRECT pread and return the data.
pub fn direct_read_data(fd: RawFd, len: u64) -> Option<Vec<u8>> {
    let aligned_len = align_up(len) as usize;
    let layout = std::alloc::Layout::from_size_align(aligned_len, 4096).unwrap();
    let buf_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if buf_ptr.is_null() {
        return None;
    }

    let n = unsafe {
        libc::pread(fd, buf_ptr as *mut libc::c_void, aligned_len, 0)
    };

    if n >= 0 {
        let data = unsafe { std::slice::from_raw_parts(buf_ptr, len as usize) }.to_vec();
        unsafe { std::alloc::dealloc(buf_ptr, layout); }
        Some(data)
    } else {
        unsafe { std::alloc::dealloc(buf_ptr, layout); }
        None
    }
}

fn align_up(n: u64) -> u64 {
    (n + 4095) & !4095
}
