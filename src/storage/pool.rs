//! Buffer Pool + io_uring NVMe I/O.
//!
//! Buffer pool: fixed-size, 4KB-aligned, dual-registered (io_uring + EFA).
//! io_uring: ReadFixed/WriteFixed with registered buffers.

use std::os::unix::io::RawFd;
use std::sync::OnceLock;

use crossbeam_queue::ArrayQueue;

use crate::data_type::ObjectId;
use crate::types::PoolBuffer;

use super::fd_pool::FdPool;
use super::uring::{IoRequest, UringNvmeEngine};
use super::{NvmeEngine, Storage, StorageError};

// ─── Pool Storage ────────────────────────────────────────────────────────────

pub struct PoolStorage {
    buf_size: usize,
    #[allow(dead_code)]
    buf_count: usize,
    data_dir: String,
    /// Lock-free free list of pool buffer indices.
    free_list: ArrayQueue<usize>,
    /// All pool buffers. Stable for lifetime of module.
    buffers: Vec<PoolBuffer>,
    /// io_uring engine (initialized once, never changes). Trait object for testability.
    uring: OnceLock<Box<dyn NvmeEngine>>,
    /// Fd pool: ObjectId → pre-opened read fd.
    fd_pool: FdPool,
}

// SAFETY: PoolStorage is accessed from multiple threads (main thread + io_uring callbacks).
// Interior sync is provided by: ArrayQueue (lock-free), OnceLock (write-once), FdPool (RwLock).
// buffers Vec is never mutated after construction.
unsafe impl Send for PoolStorage {}
unsafe impl Sync for PoolStorage {}

impl PoolStorage {
    pub fn new(buf_size: usize, buf_count: usize, data_dir: &str) -> Self {
        let mut buffers = Vec::with_capacity(buf_count);
        let free_list = ArrayQueue::new(buf_count);

        for i in 0..buf_count {
            let layout =
                std::alloc::Layout::from_size_align(buf_size, 4096).expect("invalid layout");
            // SAFETY: Layout is valid (size > 0, alignment is power of 2).
            // alloc_zeroed returns a valid pointer or null (checked below).
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            if ptr.is_null() {
                panic!("largeobj: failed to allocate pool buffer {}", i);
            }
            buffers.push(PoolBuffer { ptr, len: buf_size, idx: i as u16 });
            free_list.push(i).unwrap();
        }

        Self {
            buf_size,
            buf_count,
            data_dir: data_dir.to_string(),
            free_list,
            buffers,
            uring: OnceLock::new(),
            fd_pool: FdPool::new(),
        }
    }

    /// Return all buffer descriptors for transport registration (fi_mr_reg).
    pub fn buffer_descriptors(&self) -> Vec<PoolBuffer> {
        self.buffers
            .iter()
            .map(|b| PoolBuffer { ptr: b.ptr, len: b.len, idx: b.idx })
            .collect()
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

impl Storage for PoolStorage {
    fn pool_get(&self) -> Option<PoolBuffer> {
        self.free_list.pop().map(|idx| PoolBuffer {
            ptr: self.buffers[idx].ptr,
            len: self.buffers[idx].len,
            idx: idx as u16,
        })
    }

    fn pool_put(&self, buf: PoolBuffer) {
        // Use the idx field directly — O(1) instead of O(N) linear scan.
        self.free_list.push(buf.idx as usize).ok();
    }

    fn pin(&self, _ptr: *mut u8) {
        // TODO: Pin bitmap for eviction policy. Currently pool_get removal acts as implicit pin.
    }

    fn unpin(&self, _ptr: *mut u8) {
        // TODO: Clear pin in bitmap.
    }

    fn pool_buf_size(&self) -> usize {
        self.buf_size
    }

    fn register_buffers(&self) -> Result<(), StorageError> {
        let iovecs: Vec<libc::iovec> = self
            .buffers
            .iter()
            .map(|b| libc::iovec {
                iov_base: b.ptr as *mut libc::c_void,
                iov_len: b.len,
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
        buf: PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(PoolBuffer, Result<u64, StorageError>) + Send>,
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

        // Use idx directly from the buffer — no linear scan needed.
        let buf_index = buf.idx;
        let buf_ptr = buf.ptr as usize;
        let buf_len = buf.len;

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
            buf_ptr,
            buf_index,
            len,
            on_complete: Box::new(move |result| {
                // SAFETY: buf_ptr came from a pool-allocated buffer. The pool guarantees
                // this memory is valid for the module lifetime. We reconstruct the PoolBuffer
                // to return ownership to the caller via the callback.
                let buf = PoolBuffer { ptr: buf_ptr as *mut u8, len: buf_len, idx: buf_index };
                on_complete(buf, result);
            }),
        };

        engine.submit(req);
    }

    fn write_new(
        &self,
        buf: PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(PoolBuffer, Result<(ObjectId, u32), StorageError>) + Send>,
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

        // Use idx directly from the buffer.
        let buf_index = buf.idx;
        let buf_ptr = buf.ptr as usize;
        let buf_len = buf.len;

        // Compute crc32c before submitting write.
        // SAFETY: buf_ptr is a valid pool buffer pointer, len bytes are within allocation.
        let slice = unsafe { std::slice::from_raw_parts(buf_ptr as *const u8, len as usize) };
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
            buf_ptr,
            buf_index,
            len,
            on_complete: Box::new(move |result| {
                // SAFETY: fd is the valid descriptor we opened above. Closed exactly once here.
                unsafe { libc::close(fd) };
                // SAFETY: Reconstruct PoolBuffer — same invariant as read_into callback.
                let buf = PoolBuffer { ptr: buf_ptr as *mut u8, len: buf_len, idx: buf_index };

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

impl Drop for PoolStorage {
    fn drop(&mut self) {
        for buf in &self.buffers {
            let layout = std::alloc::Layout::from_size_align(self.buf_size, 4096).unwrap();
            // SAFETY: Each buffer was allocated with this exact layout in new().
            // We dealloc each exactly once during drop.
            unsafe { std::alloc::dealloc(buf.ptr, layout) };
        }
    }
}
