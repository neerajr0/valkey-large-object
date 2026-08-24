//! Buffer Pool + io_uring NVMe I/O.
//!
//! Buffer pool: fixed-size, 4KB-aligned, dual-registered (io_uring + EFA).
//! io_uring: ReadFixed/WriteFixed with registered buffers.

use std::alloc::Layout;
use std::os::unix::io::RawFd;
use std::sync::OnceLock;

use super::buffer::{Buffer, BufferPool};
use crate::data_type::ObjectId;

use super::coalescing::{CoalesceResult, ReadCoalesceResult, ReadCoalescingMap, WaiterCallback};
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
    /// Per-key read coalescing map. Deduplicates concurrent NVMe reads for the same object_id.
    /// Uses the generic CoalescingMap specialized for read operations.
    coalescing: ReadCoalescingMap,
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
            coalescing: ReadCoalescingMap::new(),
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
    fn open_read_fd(&self, object_id: ObjectId) -> Option<RawFd> {
        let path = object_id.file_path(&self.data_dir);
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

    /// Coalesced read: deduplicates concurrent NVMe reads for the same ObjectId.
    ///
    /// - First request for an object_id becomes the leader: acquires a buffer, submits io_uring read.
    /// - Subsequent requests for the same object_id (while leader is in-flight): become waiters,
    ///   no buffer acquired, callback stored. At leader completion, data is memcpy'd to
    ///   a just-in-time acquired buffer per waiter.
    ///
    /// The command handler calls this instead of pool_get() + read_into() directly.
    /// Buffer acquisition is internal — callers never call pool_get() for reads.
    ///
    /// The coalescing map is generic — the read-specific completion logic (buffer acquire +
    /// memcpy from leader buffer) is defined here as a closure passed to `complete()`,
    /// keeping the coalescing infrastructure reusable for other operations.
    pub fn read_into_coalesced(
        &'static self,
        object_id: ObjectId,
        len: u64,
        cb: WaiterCallback<ReadCoalesceResult>,
    ) -> Result<(), StorageError> {
        match self.coalescing.try_join_or_lead(object_id, cb) {
            CoalesceResult::Waiter => Ok(()), // callback registered, no I/O needed
            CoalesceResult::Leader(leader_cb) => {
                // Leader: acquire buffer and submit NVMe read.
                let buf = match self.pool_get() {
                    Some(b) => b,
                    None => {
                        // Can't even start the read — remove the map entry and notify waiters.
                        // No waiters can exist yet in practice (we just inserted the
                        // entry on this single-threaded main thread), but drain safely.
                        let dropped = self.coalescing.remove_and_notify(object_id, || {
                            (None, Err(StorageError::PoolExhausted))
                        });
                        if dropped > 0 {
                            eprintln!(
                                "largeobj: coalescing leader pool_get failed, notified {} waiters for object_id={}",
                                dropped, object_id.0
                            );
                        }
                        leader_cb((None, Err(StorageError::PoolExhausted)));
                        return Ok(()); // Error delivered via callback, not Result
                    }
                };

                // Wrap the io_uring completion to fan out to waiters, then fire leader cb.
                let io_cb: super::ReadCallback = Box::new(move |buf, result| {
                    // Fan out to waiters via the generic complete() interface.
                    // The read-specific logic (buffer acquire + memcpy) is defined
                    // here as the completion closure — not inside the coalescing map.
                    match &result {
                        Ok(bytes_read) => {
                            let len = *bytes_read as usize;
                            let src_ptr = buf.ptr();
                            self.coalescing.complete(object_id, || {
                                // Per-waiter: acquire buffer, memcpy leader data.
                                match self.pool.get() {
                                    Some(waiter_buf) => {
                                        // SAFETY: Both pointers are valid pool buffers with
                                        // capacity >= len. leader_buf was just filled by NVMe
                                        // DMA (L1/L2 hot). waiter_buf is a distinct pool slot
                                        // (no aliasing). len <= pool_buf_size.
                                        unsafe {
                                            std::ptr::copy_nonoverlapping(
                                                src_ptr,
                                                waiter_buf.ptr(),
                                                len,
                                            );
                                        }
                                        (Some(waiter_buf), Ok(*bytes_read))
                                    }
                                    None => {
                                        // Pool exhausted at fan-out time.
                                        (None, Err(StorageError::PoolExhausted))
                                    }
                                }
                            });
                        }
                        Err(_) => {
                            // Leader NVMe read failed — propagate error to all waiters.
                            self.coalescing.complete(object_id, || {
                                (None, Err(StorageError::IoError { code: -1 }))
                            });
                        }
                    }

                    // Fire leader's own callback.
                    match result {
                        Ok(bytes_read) => leader_cb((Some(buf), Ok(bytes_read))),
                        Err(e) => {
                            // Leader also gets the error. Drop buf (returns to pool).
                            drop(buf);
                            leader_cb((None, Err(e)));
                        }
                    }
                });

                // Submit the actual io_uring read (existing path).
                self.read_into(object_id, buf, len, io_cb);
                Ok(())
            }
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
        let object_id = ObjectId::next();
        let final_path = object_id.file_path(&self.data_dir);
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
                            on_complete(buf, Ok((object_id, crc)));
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
