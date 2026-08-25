//! Buffer Pool + io_uring NVMe I/O.
//!
//! Buffer pool: fixed-size, 4KB-aligned, dual-registered (io_uring + EFA).
//! io_uring: ReadFixed/WriteFixed with registered buffers.

use std::alloc::Layout;
use std::os::unix::io::RawFd;
use std::sync::OnceLock;

use super::buffer::{Buffer, BufferPool};
use crate::data_type::ObjectId;

use super::fd_pool::FdPool;
use super::uring::{IoRequest, UringNvmeEngine};
use super::{NvmeEngine, Storage, StorageError};

// ─── PinnedBuffer ────────────────────────────────────────────────────────────

/// 4KB-aligned kernel-pinned memory. Registered with IORING_REGISTER_BUFFERS
/// and fi_mr_reg. Never moves, never reallocated. Lives for module lifetime.
pub struct PinnedBuffer {
    mem: Box<[u8]>,
}

impl PinnedBuffer {
    /// Allocate a new 4KB-aligned, zeroed buffer of `size` bytes.
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("invalid buffer layout");
        // SAFETY: layout is valid (size > 0, alignment is power of 2).
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        // SAFETY: ptr is valid, aligned, zeroed. Vec takes ownership of the allocation.
        let mem = unsafe { Vec::from_raw_parts(ptr, size, size) }.into_boxed_slice();
        Self { mem }
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.mem.as_ptr() as *mut u8
    }
    pub fn len(&self) -> usize {
        self.mem.len()
    }
    pub fn is_empty(&self) -> bool {
        self.mem.is_empty()
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.mem
    }
}

// ─── StorageEngine ───────────────────────────────────────────────────────────

// Look at thread safety of this when we are re-entering this on the completion callback.
pub struct StorageEngine {
    buf_size: usize,
    #[allow(dead_code)]
    buf_count: usize,
    data_dir: String,
    /// The buffer pool. Buffers taken out via get(), returned via Drop.
    pool: BufferPool,
    /// Fixed array of kernel-pinned memory. Registered with io_uring at startup.
    pinned_buffers: Vec<PinnedBuffer>,
    /// io_uring engine (initialized once, never changes). Trait object for testability.
    uring: OnceLock<Box<dyn NvmeEngine>>,
    /// Fd pool: ObjectId → pre-opened read fd.
    fd_pool: FdPool,
}

impl StorageEngine {
    pub fn new(buf_size: usize, buf_count: usize, fd_pool_capacity: usize, data_dir: &str) -> Self {
        let mut pinned_buffers = Vec::with_capacity(buf_count);

        for _i in 0..buf_count {
            pinned_buffers.push(PinnedBuffer::new(buf_size));
        }

        Self {
            buf_size,
            buf_count,
            data_dir: data_dir.to_string(),
            pool: BufferPool::new(),
            pinned_buffers,
            uring: OnceLock::new(),
            fd_pool: FdPool::with_capacity(fd_pool_capacity),
        }
    }

    /// Fill the buffer pool with Buffers. Must be called after StorageEngine is placed in OnceLock.
    pub fn init_pool(&'static self) {
        self.pool.fill(&self.pinned_buffers);
    }

    /// Access the buffer pool (for Drop return path).
    pub fn buffer_pool(&self) -> &BufferPool {
        &self.pool
    }

    /// Return all buffer descriptors for transport registration (fi_mr_reg).
    /// Return buffer pointer/len info for transport registration (fi_mr_reg).
    pub fn buffer_descriptors(&self) -> &[PinnedBuffer] {
        &self.pinned_buffers
    }

    /// Delete convenience method (called from data_type free callback).
    pub fn delete_file(&self, object_id: ObjectId) {
        self.delete(object_id);
    }

    /// Signal the io_uring poller thread to exit. Non-blocking.
    /// The poller drains pending ops then terminates, allowing process exit.
    pub fn signal_shutdown(&self) {
        if let Some(engine) = self.uring.get() {
            engine.signal_shutdown();
        }
    }

    /// Open a read fd for an object. Uses O_DIRECT when direct-io config is enabled.
    /// On failure returns the OS error code (errno) so the caller can surface it.
    fn open_read_fd(&self, oid: ObjectId) -> Result<RawFd, i32> {
        let path = oid.file_path(&self.data_dir);
        let c_path = std::ffi::CString::new(path).map_err(|_| libc::EINVAL)?;
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        // SAFETY: c_path is a valid null-terminated C string, flags are valid POSIX.
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            Ok(fd)
        } else {
            // Capture errno via the safe wrapper immediately, before any other libc
            // call on this thread can clobber it.
            Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(-1))
        }
    }
}

impl Storage for StorageEngine {
    // Note: Check for a cleaner way to pass on the popped object.
    fn pool_get(&self) -> Option<Buffer> {
        self.pool.get()
    }

    fn pool_buf_size(&self) -> usize {
        self.buf_size
    }

    fn register_buffers(&self) -> Result<(), StorageError> {
        let iovecs: Vec<libc::iovec> = self
            .pinned_buffers
            .iter()
            .map(|pb| libc::iovec {
                iov_base: pb.as_mut_ptr() as *mut libc::c_void,
                iov_len: pb.len(),
            })
            .collect();

        let engine: Box<dyn NvmeEngine> = Box::new(UringNvmeEngine::new(iovecs));
        self.uring
            .set(engine)
            .map_err(|_| StorageError::IoError { code: -1 })?;
        Ok(())
    }

    fn deregister_buffers(&self) -> Result<(), StorageError> {
        // OnceLock: engine lives for module lifetime. Shutdown handled in Drop.
        Ok(())
    }

    fn read_into(
        &self,
        object_id: ObjectId,
        buf: Buffer,
        len: u64,
        on_complete: super::ReadCallback,
    ) {
        // Acquire an fd guard from the LFU pool (open + insert on miss). The guard
        // holds an in-flight reference on the fd so a concurrent evict/delete can't
        // close it out from under this read.
        let guard = match self.fd_pool.acquire(object_id) {
            Some(g) => g,
            None => match self.open_read_fd(object_id) {
                Ok(fd) => self.fd_pool.insert(object_id, fd),
                Err(code) => {
                    on_complete(buf, Err(StorageError::IoError { code }));
                    return;
                }
            },
        };
        let fd = guard.fd();

        // Engine must be initialized (set once at module load via register_buffers).
        let engine = match self.uring.get() {
            Some(e) => e,
            None => {
                on_complete(buf, Err(StorageError::IoError { code: -1 }));
                return;
            }
        };

        // Move the guard into the completion closure so the fd stays open for the
        // whole read; it is released (in-flight refcount decremented) once the read
        // completes on the poller thread. This is the in-flight reference count in
        // action, and the extension point for read coalescing.
        let req = IoRequest::Read {
            fd,
            buf,
            len,
            on_complete: Box::new(move |buf, result| {
                on_complete(buf, result);
                drop(guard);
            }),
        };

        engine.submit(req);
    }

    fn write_new(&self, buf: Buffer, len: u64, on_complete: super::WriteCallback) {
        let oid = ObjectId::next();
        let final_path = oid.file_path(&self.data_dir);
        let tmp_path = format!("{}.tmp", final_path);

        // Open tmp file for write. Uses O_DIRECT when direct-io config is enabled.
        let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
        if crate::direct_io() {
            write_flags |= libc::O_DIRECT;
        }
        // SAFETY: CString is valid, flags are standard POSIX, mode 0o644 is safe.
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(tmp_path.as_str()).unwrap().as_ptr(),
                write_flags,
                0o644,
            )
        };
        if fd < 0 {
            // Capture errno via the safe wrapper before any other libc call clobbers it.
            let code = std::io::Error::last_os_error().raw_os_error().unwrap_or(-1);
            on_complete(buf, Err(StorageError::IoError { code }));
            return;
        }

        // Compute crc32c before submitting write.
        // SAFETY: buf.ptr() is a valid pool buffer pointer, len bytes are within allocation.
        let slice = unsafe { std::slice::from_raw_parts(buf.ptr(), len as usize) };
        let crc = crc32c::crc32c(slice);

        // Engine must be initialized.
        let engine = match self.uring.get() {
            Some(e) => e,
            None => {
                // SAFETY: fd is a valid file descriptor we just opened.
                unsafe { libc::close(fd) };
                let _ = std::fs::remove_file(&tmp_path);
                on_complete(buf, Err(StorageError::IoError { code: -1 }));
                return;
            }
        };

        let tmp_path_for_cb = tmp_path.clone();
        let final_path_for_cb = final_path.clone();

        let req = IoRequest::Write {
            fd,
            buf,
            len,
            on_complete: Box::new(move |buf, result| {
                // SAFETY: fd is the valid descriptor we opened above. Closed exactly once here.
                unsafe { libc::close(fd) };

                match result {
                    Ok(()) => {
                        // Atomic rename: tmp → final.
                        if std::fs::rename(&tmp_path_for_cb, &final_path_for_cb).is_ok() {
                            // The fd pool is populated lazily on the read path; we
                            // deliberately do not warm it from the write path (that's
                            // speculative work for an object that may never be read).
                            on_complete(buf, Ok((oid, crc)));
                        } else {
                            let _ = std::fs::remove_file(&tmp_path_for_cb);
                            on_complete(buf, Err(StorageError::IoError { code: -1 }));
                        }
                    }
                    Err(e) => {
                        let _ = std::fs::remove_file(&tmp_path_for_cb);
                        on_complete(buf, Err(e));
                    }
                }
            }),
        };

        engine.submit(req);
    }

    fn delete(&self, object_id: ObjectId) {
        // Drop the fd from the pool (an in-flight read keeps it open until it
        // completes), then unlink. Unlinking an open file is fine on Linux.
        self.fd_pool.remove(object_id);
        let path = object_id.file_path(&self.data_dir);
        let _ = std::fs::remove_file(&path);
    }
}

// No manual Drop needed — PinnedBuffer owns Box<[u8]> which deallocates automatically.
