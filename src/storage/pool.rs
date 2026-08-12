//! Buffer Pool — fixed-size, 4KB-aligned buffers for NVMe DMA.
//!
//! Dual-registered: io_uring (for NVMe ReadFixed/WriteFixed) + EFA (fi_mr_reg).
//! Same physical pages, no conflicts.

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::data_type::ObjectId;
use crate::transport::PoolBuffer;

use super::uring::{IoRequest, UringEngine};
use super::{Storage, StorageError};

/// Pool storage implementation.
pub struct PoolStorage {
    buf_size: usize,
    buf_count: usize,
    data_dir: String,
    /// Free list of pool buffer indices.
    free_list: Mutex<VecDeque<usize>>,
    /// All pool buffers. Stable for lifetime of module.
    buffers: Vec<PoolBuffer>,
    /// io_uring engine (owns the ring + poller thread).
    uring: Mutex<Option<UringEngine>>,
}

// Safety: buffers are allocated once and stable. Access via free_list is mutex-protected.
unsafe impl Send for PoolStorage {}
unsafe impl Sync for PoolStorage {}

impl PoolStorage {
    pub fn new(buf_size: usize, buf_count: usize, data_dir: &str) -> Self {
        let mut buffers = Vec::with_capacity(buf_count);
        let mut free_list = VecDeque::with_capacity(buf_count);

        for i in 0..buf_count {
            // Allocate 4KB-aligned buffer for O_DIRECT compatibility.
            let layout = std::alloc::Layout::from_size_align(buf_size, 4096)
                .expect("invalid layout");
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
        }
    }

    /// Return all buffer descriptors for transport registration (fi_mr_reg).
    pub fn buffer_descriptors(&self) -> Vec<PoolBuffer> {
        self.buffers
            .iter()
            .map(|b| PoolBuffer { ptr: b.ptr, len: b.len })
            .collect()
    }

    /// Delete an object file from NVMe. Public convenience method.
    pub fn delete_file(&self, object_id: crate::data_type::ObjectId) {
        self.delete(object_id);
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
        // Find index by pointer.
        if let Some(idx) = self.buffers.iter().position(|b| b.ptr == buf.ptr) {
            let mut fl = self.free_list.lock().unwrap();
            fl.push_back(idx);
        }
    }

    fn pin(&self, _buf: &PoolBuffer) {
        // TODO: Mark buffer as pinned (prevent eviction during in-flight I/O).
        // For now, pool_get already removes from free list which prevents reuse.
    }

    fn unpin(&self, _buf: &PoolBuffer) {
        // TODO: Clear pin flag. Currently a no-op since pool_put handles return.
    }

    fn pool_buf_size(&self) -> usize {
        self.buf_size
    }

    fn register_buffers(&self) -> Result<(), StorageError> {
        // Build iovecs from our pool buffers and create the UringEngine.
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
        buf: &mut PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<u64, StorageError>) + Send>,
    ) {
        let path = object_id.file_path(&self.data_dir);

        // Open file for O_DIRECT read.
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(path).unwrap().as_ptr(),
                libc::O_RDONLY | libc::O_DIRECT,
            )
        };
        if fd < 0 {
            on_complete(Err(StorageError::IoError {
                code: unsafe { *libc::__errno_location() },
            }));
            return;
        }

        // Find this buffer's registered index.
        let buf_index = self
            .buffers
            .iter()
            .position(|b| b.ptr == buf.ptr)
            .unwrap_or(0) as u16;

        let fd_copy = fd;
        let req = IoRequest::Read {
            fd,
            buf_ptr: buf.ptr as usize,
            buf_index,
            len,
            on_complete: Box::new(move |result| {
                // Close fd after read completes.
                unsafe { libc::close(fd_copy) };
                on_complete(result);
            }),
        };

        if let Some(engine) = self.uring.lock().unwrap().as_ref() {
            engine.submit(req);
        } else {
            unsafe { libc::close(fd) };
            // Can't call on_complete here — it's moved into the req.
            // This path shouldn't happen (uring is initialized before any reads).
        }
    }

    fn write_new(
        &self,
        buf: &PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<(ObjectId, u32), StorageError>) + Send>,
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
            on_complete(Err(StorageError::IoError {
                code: unsafe { *libc::__errno_location() },
            }));
            return;
        }

        // Find this buffer's registered index.
        let buf_index = self
            .buffers
            .iter()
            .position(|b| b.ptr == buf.ptr)
            .unwrap_or(0) as u16;

        // Compute crc32c before submitting write (buffer contents are ready).
        let slice = unsafe { std::slice::from_raw_parts(buf.ptr, len as usize) };
        let crc = crc32c::crc32c(slice);

        let tmp_path_clone = tmp_path.clone();
        let final_path_clone = final_path.clone();

        let req = IoRequest::Write {
            fd,
            buf_ptr: buf.ptr as usize,
            buf_index,
            len,
            on_complete: Box::new(move |result| {
                unsafe { libc::close(fd) };
                match result {
                    Ok(()) => {
                        // Atomic rename: tmp → final.
                        if std::fs::rename(&tmp_path_clone, &final_path_clone).is_ok() {
                            on_complete(Ok((oid, crc)));
                        } else {
                            let _ = std::fs::remove_file(&tmp_path_clone);
                            on_complete(Err(StorageError::IoError { code: -1 }));
                        }
                    }
                    Err(e) => {
                        let _ = std::fs::remove_file(&tmp_path_clone);
                        on_complete(Err(e));
                    }
                }
            }),
        };

        if let Some(engine) = self.uring.lock().unwrap().as_ref() {
            engine.submit(req);
        } else {
            unsafe { libc::close(fd) };
            let _ = std::fs::remove_file(&tmp_path);
            // Can't call on_complete — moved into req. This path shouldn't happen.
        }
    }

    fn delete(&self, object_id: ObjectId) {
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
