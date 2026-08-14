//! Buffer Pool + io_uring NVMe I/O.
//!
//! Buffer pool: fixed-size, 4KB-aligned, dual-registered (io_uring + EFA).
//! io_uring: ReadFixed/WriteFixed with registered buffers.

use std::os::unix::io::RawFd;
use std::sync::OnceLock;


use crate::data_type::ObjectId;
use crate::types::PinnedBuffer;
use super::buffer::{Buffer, BufferPool};

use super::fd_pool::FdPool;
use super::uring::{IoRequest, UringNvmeEngine};
use super::{NvmeEngine, Storage, StorageError};

// ─── Pool Storage ────────────────────────────────────────────────────────────

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
    pub fn new(buf_size: usize, buf_count: usize, data_dir: &str) -> Self {
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
            fd_pool: FdPool::new(),
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

    /// Open a read fd for an object (O_RDONLY | O_DIRECT).
    fn open_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        let path = oid.file_path(&self.data_dir);
        let c_path = std::ffi::CString::new(path).ok()?;
        // SAFETY: c_path is a valid null-terminated C string. O_RDONLY|O_DIRECT are valid flags.
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_DIRECT) };
        if fd >= 0 {
            Some(fd)
        } else {
            // Fallback without O_DIRECT (e.g., tmpfs for testing).
            // SAFETY: Same as above, just without O_DIRECT.
            let fd2 = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
            if fd2 >= 0 { Some(fd2) } else { None }
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
        self.uring.set(engine).map_err(|_| StorageError::IoError { code: -1 })?;
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
        on_complete: Box<dyn FnOnce(Buffer, Result<u64, StorageError>) + Send>,
    ) {
        // Get fd from pool (or open if miss).
        let fd = match self.fd_pool.get(object_id) {
            Some(fd) => fd,
            None => {
                match self.open_read_fd(object_id) {
                    Some(fd) => {
                        self.fd_pool.insert(object_id, fd);
                        fd
                    }
                    None => {
                        on_complete(buf, Err(StorageError::IoError {
                            // SAFETY: __errno_location returns a valid pointer to thread-local errno.
                            code: unsafe { *libc::__errno_location() },
                        }));
                        return;
                    }
                }
            }
        };

        // Engine must be initialized (set once at module load via register_buffers).
        let engine = match self.uring.get() {
            Some(e) => e,
            None => {
                on_complete(buf, Err(StorageError::IoError { code: -1 }));
                return;
            }
        };

        let req = IoRequest::Read {
            fd,
            buf,
            len,
            on_complete,
        };

        engine.submit(req);
    }

    fn write_new(
        &self,
        buf: Buffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(ObjectId, u32), StorageError>) + Send>,
    ) {
        let oid = ObjectId::next();
        let final_path = oid.file_path(&self.data_dir);
        let tmp_path = format!("{}.tmp", final_path);

        // Open tmp file for O_DIRECT write.
        // SAFETY: CString is valid, flags are standard POSIX, mode 0o644 is safe.
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(tmp_path.as_str()).unwrap().as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_DIRECT,
                0o644,
            )
        };
        if fd < 0 {
            on_complete(buf, Err(StorageError::IoError {
                // SAFETY: __errno_location returns a valid pointer to thread-local errno.
                code: unsafe { *libc::__errno_location() },
            }));
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
        // Close fd first (fd pool), then unlink file.
        self.fd_pool.remove(object_id);
        let path = object_id.file_path(&self.data_dir);
        let _ = std::fs::remove_file(&path);
    }
}

// No manual Drop needed — PinnedBuffer owns Box<[u8]> which deallocates automatically.
