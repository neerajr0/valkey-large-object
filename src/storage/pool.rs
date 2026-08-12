//! Buffer Pool + Fd Pool + io_uring NVMe I/O.
//!
//! Buffer pool: fixed-size, 4KB-aligned, dual-registered (io_uring + EFA).
//! Fd pool: open once per object on write, reuse on every read, close on delete.
//! io_uring: ReadFixed/WriteFixed with registered buffers.

use std::collections::{HashMap, VecDeque};
use std::os::unix::io::RawFd;
use std::sync::{Mutex, RwLock};

use crate::data_type::ObjectId;
use crate::transport::PoolBuffer;

use super::uring::{IoRequest, UringEngine};
use super::{Storage, StorageError};

// ─── Fd Pool ─────────────────────────────────────────────────────────────────

/// Pre-opened file descriptor pool. Open once per object (on write), reuse on reads.
/// Saves open()/close() syscalls on the hot read path.
struct FdPool {
    fds: RwLock<HashMap<u64, RawFd>>,
}

impl FdPool {
    fn new() -> Self {
        Self {
            fds: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, oid: ObjectId) -> Option<RawFd> {
        self.fds.read().unwrap().get(&oid.0).copied()
    }

    fn insert(&self, oid: ObjectId, fd: RawFd) {
        self.fds.write().unwrap().insert(oid.0, fd);
    }

    fn remove(&self, oid: ObjectId) {
        if let Some(fd) = self.fds.write().unwrap().remove(&oid.0) {
            unsafe { libc::close(fd) };
        }
    }
}

impl Drop for FdPool {
    fn drop(&mut self) {
        for (_, fd) in self.fds.write().unwrap().drain() {
            unsafe { libc::close(fd) };
        }
    }
}

// ─── Pool Storage ────────────────────────────────────────────────────────────

pub struct PoolStorage {
    buf_size: usize,
    #[allow(dead_code)]
    buf_count: usize,
    data_dir: String,
    /// Free list of pool buffer indices.
    free_list: Mutex<VecDeque<usize>>,
    /// All pool buffers. Stable for lifetime of module.
    buffers: Vec<PoolBuffer>,
    /// io_uring engine (owns the ring + poller thread).
    uring: Mutex<Option<UringEngine>>,
    /// Fd pool: ObjectId → pre-opened read fd.
    fd_pool: FdPool,
}

unsafe impl Send for PoolStorage {}
unsafe impl Sync for PoolStorage {}

impl PoolStorage {
    pub fn new(buf_size: usize, buf_count: usize, data_dir: &str) -> Self {
        let mut buffers = Vec::with_capacity(buf_count);
        let mut free_list = VecDeque::with_capacity(buf_count);

        for i in 0..buf_count {
            let layout =
                std::alloc::Layout::from_size_align(buf_size, 4096).expect("invalid layout");
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            if ptr.is_null() {
                panic!("largeobj: failed to allocate pool buffer {}", i);
            }
            buffers.push(PoolBuffer { ptr, len: buf_size });
            free_list.push_back(i);
        }

        Self {
            buf_size,
            buf_count,
            data_dir: data_dir.to_string(),
            free_list: Mutex::new(free_list),
            buffers,
            uring: Mutex::new(None),
            fd_pool: FdPool::new(),
        }
    }

    /// Return all buffer descriptors for transport registration (fi_mr_reg).
    pub fn buffer_descriptors(&self) -> Vec<PoolBuffer> {
        self.buffers
            .iter()
            .map(|b| PoolBuffer { ptr: b.ptr, len: b.len })
            .collect()
    }

    /// Delete convenience method (called from data_type free callback).
    pub fn delete_file(&self, object_id: ObjectId) {
        self.delete(object_id);
    }

    /// Find the registered buffer index for a given pointer.
    fn buf_index_for(&self, ptr: *mut u8) -> u16 {
        self.buffers
            .iter()
            .position(|b| b.ptr == ptr)
            .unwrap_or(0) as u16
    }

    /// Open a read fd for an object (O_RDONLY | O_DIRECT).
    fn open_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        let path = oid.file_path(&self.data_dir);
        let c_path = std::ffi::CString::new(path).ok()?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_DIRECT) };
        if fd >= 0 {
            Some(fd)
        } else {
            // Fallback without O_DIRECT (e.g., tmpfs for testing).
            let fd2 = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
            if fd2 >= 0 { Some(fd2) } else { None }
        }
    }
}

impl Storage for PoolStorage {
    fn pool_get(&self) -> Option<PoolBuffer> {
        let mut fl = self.free_list.lock().unwrap();
        fl.pop_front().map(|idx| PoolBuffer {
            ptr: self.buffers[idx].ptr,
            len: self.buffers[idx].len,
        })
    }

    fn pool_put(&self, buf: PoolBuffer) {
        if let Some(idx) = self.buffers.iter().position(|b| b.ptr == buf.ptr) {
            let mut fl = self.free_list.lock().unwrap();
            fl.push_back(idx);
        }
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

        let engine = UringEngine::new(iovecs);
        *self.uring.lock().unwrap() = Some(engine);
        Ok(())
    }

    fn deregister_buffers(&self) -> Result<(), StorageError> {
        if let Some(mut engine) = self.uring.lock().unwrap().take() {
            engine.shutdown();
        }
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
                            code: unsafe { *libc::__errno_location() },
                        }));
                        return;
                    }
                }
            }
        };

        let buf_index = self.buf_index_for(buf.ptr);
        let buf_ptr = buf.ptr as usize;
        let buf_len = buf.len;

        // Check engine availability before moving on_complete.
        if self.uring.lock().unwrap().is_none() {
            on_complete(buf, Err(StorageError::IoError { code: -1 }));
            return;
        }

        let req = IoRequest::Read {
            fd,
            buf_ptr,
            buf_index,
            len,
            on_complete: Box::new(move |result| {
                let buf = PoolBuffer { ptr: buf_ptr as *mut u8, len: buf_len };
                on_complete(buf, result);
            }),
        };

        self.uring.lock().unwrap().as_ref().unwrap().submit(req);
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
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(tmp_path.as_str()).unwrap().as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_DIRECT,
                0o644,
            )
        };
        if fd < 0 {
            on_complete(buf, Err(StorageError::IoError {
                code: unsafe { *libc::__errno_location() },
            }));
            return;
        }

        let buf_index = self.buf_index_for(buf.ptr);
        let buf_ptr = buf.ptr as usize;
        let buf_len = buf.len;

        // Compute crc32c before submitting write.
        let slice = unsafe { std::slice::from_raw_parts(buf_ptr as *const u8, len as usize) };
        let crc = crc32c::crc32c(slice);

        // Check engine availability before moving on_complete.
        if self.uring.lock().unwrap().is_none() {
            unsafe { libc::close(fd) };
            let _ = std::fs::remove_file(&tmp_path);
            on_complete(buf, Err(StorageError::IoError { code: -1 }));
            return;
        }

        let tmp_path_for_cb = tmp_path.clone();
        let final_path_for_cb = final_path.clone();

        let req = IoRequest::Write {
            fd,
            buf_ptr,
            buf_index,
            len,
            on_complete: Box::new(move |result| {
                unsafe { libc::close(fd) };
                let buf = PoolBuffer { ptr: buf_ptr as *mut u8, len: buf_len };

                match result {
                    Ok(()) => {
                        // Atomic rename: tmp → final.
                        if std::fs::rename(&tmp_path_for_cb, &final_path_for_cb).is_ok() {
                            // Open read fd and store in fd pool for future reads.
                            // (We can't access self.fd_pool from here since we're in a
                            //  moved closure. The fd pool insert happens externally after
                            //  the caller processes the callback.)
                            // TODO: Pass fd_pool reference or use a channel to notify.
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

        if let Some(engine) = self.uring.lock().unwrap().as_ref() {
            engine.submit(req);
        } else {
            unsafe { libc::close(fd) };
            let _ = std::fs::remove_file(&tmp_path);
            // Engine unavailable — shouldn't happen (initialized before any writes).
        }
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
            unsafe { std::alloc::dealloc(buf.ptr, layout) };
        }
    }
}
