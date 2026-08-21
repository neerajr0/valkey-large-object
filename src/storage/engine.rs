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

    /// Wrap an existing raw pointer as a PinnedBuffer (for arena mmap region).
    /// SAFETY: ptr must be valid for `len` bytes and must outlive this PinnedBuffer.
    /// The caller is responsible for not freeing the memory (e.g., mmap region managed externally).
    pub unsafe fn from_raw(ptr: *mut u8, len: usize) -> Self {
        // Create a Vec that "owns" this memory. We rely on the ArenaPool's Drop
        // to munmap, so we must NOT let this Vec deallocate. We'll use ManuallyDrop
        // in the containing structure OR just leak this PinnedBuffer.
        // Since arena PinnedBuffer is always leaked (Box::leak), Vec::drop never runs.
        let mem = Vec::from_raw_parts(ptr, len, len).into_boxed_slice();
        Self { mem }
    }

    /// Wrap a raw pointer as a non-owning PinnedBuffer (for arena sub-allocations).
    /// SAFETY: same as from_raw. The caller MUST ensure this is leaked (Box::leak)
    /// so Vec::drop never runs and doesn't try to free the memory.
    pub unsafe fn from_raw_noown(ptr: *mut u8, len: usize) -> Self {
        let mem = Vec::from_raw_parts(ptr, len, len).into_boxed_slice();
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
    pub fn new(buf_size: usize, buf_count: usize, data_dir: &str) -> Self {
        let mut pinned_buffers = Vec::with_capacity(buf_count);

        for _i in 0..buf_count {
            let pb = PinnedBuffer::new(buf_size);
            pinned_buffers.push(pb);
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

    /// Signal the io_uring poller thread to exit. Non-blocking.
    /// The poller drains pending ops then terminates, allowing process exit.
    pub fn signal_shutdown(&self) {
        if let Some(engine) = self.uring.get() {
            engine.signal_shutdown();
        }
    }

    /// Open a read fd for an object. Uses O_DIRECT when direct-io config is enabled.
    fn open_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        let path = oid.file_path(&self.data_dir);
        let c_path = std::ffi::CString::new(path).ok()?;
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        // SAFETY: c_path is a valid null-terminated C string, flags are valid POSIX.
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            Some(fd)
        } else {
            None
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
                        on_complete(
                            buf,
                            Err(StorageError::IoError {
                                // SAFETY: __errno_location returns a valid pointer to thread-local errno.
                                code: unsafe { *libc::__errno_location() },
                            }),
                        );
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
            on_complete(
                buf,
                Err(StorageError::IoError {
                    // SAFETY: __errno_location returns a valid pointer to thread-local errno.
                    code: unsafe { *libc::__errno_location() },
                }),
            );
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

// ─── ArenaStorageEngine (pool-mode=arena) ────────────────────────────────────

use super::arena::ArenaPool;

/// Arena-based storage engine. Uses talc sub-allocation from a single mmap'd segment.
/// One iovec entry registered with io_uring (buf_index=0).
pub struct ArenaStorageEngine {
    buf_size: usize,
    data_dir: String,
    arena: ArenaPool,
    /// io_uring engine (initialized once).
    uring: OnceLock<Box<dyn NvmeEngine>>,
    /// Fd pool.
    fd_pool: FdPool,
}

impl ArenaStorageEngine {
    pub fn new(buf_size: usize, total_size: usize, data_dir: &str) -> Self {
        Self {
            buf_size,
            data_dir: data_dir.to_string(),
            arena: ArenaPool::new(buf_size, total_size),
            uring: OnceLock::new(),
            fd_pool: FdPool::new(),
        }
    }

    /// Get the single PinnedBuffer (whole arena segment) for registration.
    pub fn pinned_backing(&self) -> &'static PinnedBuffer {
        self.arena.pinned_backing()
    }

    /// Return a buffer by idx (called from Buffer::drop in arena mode).
    /// In arena mode, idx=0 always (for io_uring). We identify the allocation by ptr.
    pub fn return_buffer_by_idx(&self, _idx: u16) {
        // In arena mode, the buffer return is handled via the ptr address.
        // We don't use idx for tracking in arena mode.
        // The actual deallocation happens when we know the ptr.
        // With the current design, Buffer::drop calls return_buffer(pinned, idx).
        // pinned.as_mut_ptr() gives us the sub-allocated ptr.
        // We cannot reach pinned here — we'll handle it differently.
    }

    /// Return a buffer by its raw pointer (arena mode).
    pub fn return_buffer_by_ptr(&self, ptr: *mut u8) {
        let ab = super::arena::ArenaBuffer {
            ptr,
            offset: 0,
            size: self.buf_size,
        };
        self.arena.put_back(&ab);
    }

    pub fn signal_shutdown(&self) {
        if let Some(engine) = self.uring.get() {
            engine.signal_shutdown();
        }
    }

    fn open_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        let path = oid.file_path(&self.data_dir);
        let c_path = std::ffi::CString::new(path).ok()?;
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            Some(fd)
        } else {
            None
        }
    }
}

impl Storage for ArenaStorageEngine {
    fn pool_get(&self) -> Option<Buffer> {
        let ab = self.arena.get()?;
        // Create a leaked PinnedBuffer wrapper pointing to the sub-allocated address.
        // idx=0 for io_uring ReadFixed (single registered iovec).
        // SAFETY: ab.ptr is within the arena mmap region which lives for module lifetime.
        let wrapper = unsafe { Box::leak(Box::new(PinnedBuffer::from_raw_noown(ab.ptr, self.buf_size))) };
        Some(Buffer::from_pinned(wrapper, 0))
    }

    fn pool_buf_size(&self) -> usize {
        self.buf_size
    }

    fn register_buffers(&self) -> Result<(), StorageError> {
        // Register the single arena segment as one iovec.
        let iovecs = vec![libc::iovec {
            iov_base: self.arena.base_ptr() as *mut libc::c_void,
            iov_len: self.arena.total_size(),
        }];

        let engine: Box<dyn NvmeEngine> = Box::new(UringNvmeEngine::new(iovecs));
        self.uring
            .set(engine)
            .map_err(|_| StorageError::IoError { code: -1 })?;
        Ok(())
    }

    fn deregister_buffers(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn read_into(
        &self,
        object_id: ObjectId,
        buf: Buffer,
        len: u64,
        on_complete: super::ReadCallback,
    ) {
        let fd = match self.fd_pool.get(object_id) {
            Some(fd) => fd,
            None => match self.open_read_fd(object_id) {
                Some(fd) => {
                    self.fd_pool.insert(object_id, fd);
                    fd
                }
                None => {
                    on_complete(
                        buf,
                        Err(StorageError::IoError {
                            code: unsafe { *libc::__errno_location() },
                        }),
                    );
                    return;
                }
            },
        };

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

    fn write_new(&self, buf: Buffer, len: u64, on_complete: super::WriteCallback) {
        let oid = ObjectId::next();
        let final_path = oid.file_path(&self.data_dir);
        let tmp_path = format!("{}.tmp", final_path);

        let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
        if crate::direct_io() {
            write_flags |= libc::O_DIRECT;
        }
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(tmp_path.as_str()).unwrap().as_ptr(),
                write_flags,
                0o644,
            )
        };
        if fd < 0 {
            on_complete(
                buf,
                Err(StorageError::IoError {
                    code: unsafe { *libc::__errno_location() },
                }),
            );
            return;
        }

        let slice = unsafe { std::slice::from_raw_parts(buf.ptr(), len as usize) };
        let crc = crc32c::crc32c(slice);

        let engine = match self.uring.get() {
            Some(e) => e,
            None => {
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
                unsafe { libc::close(fd) };
                match result {
                    Ok(()) => {
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
        self.fd_pool.remove(object_id);
        let path = object_id.file_path(&self.data_dir);
        let _ = std::fs::remove_file(&path);
    }
}

// ─── DynamicStorageEngine (pool-mode=dynamic) ────────────────────────────────

/// Dynamic buffer allocation: allocates a fresh 4KB-aligned buffer per request
/// from the heap (ValkeyAlloc). No pre-registration with io_uring (uses plain
/// read/write, not ReadFixed). EFA: a single staging buffer is pre-registered
/// for fi_mr_reg; data is memcpy'd to staging before fi_write.
///
/// This mode exists as a baseline comparison — it shows the cost of not
/// pre-registering buffers with io_uring.
pub struct DynamicStorageEngine {
    buf_size: usize,
    data_dir: String,
    /// io_uring engine — uses plain read/write (no registered buffers).
    uring: OnceLock<Box<dyn NvmeEngine>>,
    /// Fd pool.
    fd_pool: FdPool,
    /// A single pre-registered staging buffer for EFA fi_write.
    /// When an EFA client requests data, we memcpy from the dynamic buffer
    /// to this staging buffer, then fi_write from staging.
    staging: &'static PinnedBuffer,
}

impl DynamicStorageEngine {
    pub fn new(buf_size: usize, data_dir: &str) -> Self {
        // Allocate one staging buffer for EFA (registered with transport).
        let staging = Box::leak(Box::new(PinnedBuffer::new(buf_size)));

        Self {
            buf_size,
            data_dir: data_dir.to_string(),
            uring: OnceLock::new(),
            fd_pool: FdPool::new(),
            staging,
        }
    }

    /// Get the staging buffer for transport registration (fi_mr_reg).
    pub fn staging_buffer(&self) -> &'static PinnedBuffer {
        self.staging
    }

    pub fn signal_shutdown(&self) {
        if let Some(engine) = self.uring.get() {
            engine.signal_shutdown();
        }
    }

    fn open_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        let path = oid.file_path(&self.data_dir);
        let c_path = std::ffi::CString::new(path).ok()?;
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd >= 0 {
            Some(fd)
        } else {
            None
        }
    }
}

impl Storage for DynamicStorageEngine {
    fn pool_get(&self) -> Option<Buffer> {
        // Allocate a fresh buffer from the heap (ValkeyAlloc via global allocator).
        let pinned = Box::leak(Box::new(PinnedBuffer::new(self.buf_size)));

        // EFA registration: fi_mr_reg on this buffer so fi_write can use it directly.
        // This is the REAL per-request registration cost. On EFA hardware, fi_mr_reg
        // pins pages (get_user_pages) + programs NIC MR table (~1-5ms total).
        // The RegisteredMr handle is stored alongside the buffer — dropping it
        // calls fi_mr_dereg when the buffer is returned.
        //
        // NOTE: On non-EFA machines (feature "efa" disabled), this is a no-op.
        let _mr = crate::transport::register_single_buffer(
            pinned.as_mut_ptr(),
            pinned.len(),
        );
        // TODO: Store the MR handle so it lives as long as the buffer.
        // For now we leak it (registration stays active until process exit).
        // Proper fix: attach MR to Buffer or store in a side map.
        if let Ok(mr) = _mr {
            std::mem::forget(mr); // Keep registered until buffer is freed
        }

        // idx=u16::MAX signals "dynamic" — no io_uring registered index.
        Some(Buffer::from_pinned(pinned, u16::MAX))
    }

    fn pool_buf_size(&self) -> usize {
        self.buf_size
    }

    fn register_buffers(&self) -> Result<(), StorageError> {
        // Dynamic mode: NO io_uring buffer registration.
        // We still create the io_uring ring for plain read/write ops.
        // Pass empty iovecs — UringNvmeEngine handles this by using non-fixed ops.
        let engine: Box<dyn NvmeEngine> = Box::new(UringNvmeEngine::new(Vec::new()));
        self.uring
            .set(engine)
            .map_err(|_| StorageError::IoError { code: -1 })?;
        Ok(())
    }

    fn deregister_buffers(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn read_into(
        &self,
        object_id: ObjectId,
        buf: Buffer,
        len: u64,
        on_complete: super::ReadCallback,
    ) {
        let fd = match self.fd_pool.get(object_id) {
            Some(fd) => fd,
            None => match self.open_read_fd(object_id) {
                Some(fd) => {
                    self.fd_pool.insert(object_id, fd);
                    fd
                }
                None => {
                    on_complete(
                        buf,
                        Err(StorageError::IoError {
                            code: unsafe { *libc::__errno_location() },
                        }),
                    );
                    return;
                }
            },
        };

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

    fn write_new(&self, buf: Buffer, len: u64, on_complete: super::WriteCallback) {
        let oid = ObjectId::next();
        let final_path = oid.file_path(&self.data_dir);
        let tmp_path = format!("{}.tmp", final_path);

        let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
        if crate::direct_io() {
            write_flags |= libc::O_DIRECT;
        }
        let fd = unsafe {
            libc::open(
                std::ffi::CString::new(tmp_path.as_str()).unwrap().as_ptr(),
                write_flags,
                0o644,
            )
        };
        if fd < 0 {
            on_complete(
                buf,
                Err(StorageError::IoError {
                    code: unsafe { *libc::__errno_location() },
                }),
            );
            return;
        }

        let slice = unsafe { std::slice::from_raw_parts(buf.ptr(), len as usize) };
        let crc = crc32c::crc32c(slice);

        let engine = match self.uring.get() {
            Some(e) => e,
            None => {
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
                unsafe { libc::close(fd) };
                match result {
                    Ok(()) => {
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
        self.fd_pool.remove(object_id);
        let path = object_id.file_path(&self.data_dir);
        let _ = std::fs::remove_file(&path);
    }
}
