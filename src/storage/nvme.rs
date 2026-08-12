//! NVMe file-backed storage: write via O_DIRECT + fallocate.
//!
//! File layout: {data_dir}/{object_id_hex}.dat
//! Pre-opened fd pool: after write, a read fd is kept open for the io_uring poller.
//! Reads are handled by uring_engine.rs (not here).

use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use super::engine::ObjectId;

/// 4 KiB alignment for O_DIRECT.
const ALIGN: u64 = 4096;

fn align_up(n: u64) -> u64 {
    (n + ALIGN - 1) & !(ALIGN - 1)
}

/// Pre-opened file descriptor pool.
struct FdPool {
    fds: RwLock<HashMap<u64, RawFd>>,
}

impl FdPool {
    fn new() -> Self {
        Self { fds: RwLock::new(HashMap::new()) }
    }

    fn get(&self, oid: ObjectId) -> Option<RawFd> {
        self.fds.read().unwrap().get(&oid.0).copied()
    }

    fn insert(&self, oid: ObjectId, fd: RawFd) {
        self.fds.write().unwrap().insert(oid.0, fd);
    }

    fn remove(&self, oid: ObjectId) {
        if let Some(fd) = self.fds.write().unwrap().remove(&oid.0) {
            unsafe { libc::close(fd); }
        }
    }
}

impl Drop for FdPool {
    fn drop(&mut self) {
        for (_, fd) in self.fds.write().unwrap().drain() {
            unsafe { libc::close(fd); }
        }
    }
}

/// NVMe storage backend — write + fd pool management.
/// Reads are done by uring_engine via the pre-opened fds.
pub struct NvmeBackend {
    data_dir: PathBuf,
    fd_pool: FdPool,
    /// If true (default), a read fd is opened at write time and held in the pool
    /// forever (no open() on the GET hot path — costs 1 fd + pinned inode per
    /// object). If false, no fd is pooled; GET opens on demand and closes after
    /// the read. This flag is the A/B switch for the fd-overhead experiment.
    keep_read_fds: bool,
}

impl NvmeBackend {
    pub fn new(data_dir: &Path, _reaper_count: usize, keep_read_fds: bool) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            fd_pool: FdPool::new(),
            keep_read_fds,
        })
    }

    fn obj_path(&self, oid: ObjectId) -> PathBuf {
        self.data_dir.join(format!("{:016x}.dat", oid.0))
    }

    fn open_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        let path = self.obj_path(oid);
        let c_path = CString::new(path.to_str()?).ok()?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_DIRECT, 0) };
        if fd < 0 {
            // Fallback without O_DIRECT
            let fd2 = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY, 0) };
            if fd2 < 0 { return None; }
            return Some(fd2);
        }
        Some(fd)
    }

    /// Open a fresh read fd WITHOUT pooling it. Caller owns it and must close it
    /// (used in keep_read_fds=false mode: open per GET, close after the read).
    pub fn open_read_fd_ondemand(&self, oid: ObjectId) -> Option<RawFd> {
        self.open_read_fd(oid)
    }

    /// Write an object to NVMe. Opens a read fd and stores in pool.
    pub fn write_object(&self, oid: ObjectId, data: &[u8]) -> io::Result<()> {
        let path = self.obj_path(oid);
        let aligned_size = align_up(data.len() as u64) as usize;
        let c_path = CString::new(path.to_str().unwrap()).unwrap();

        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_DIRECT | libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC,
                0o644,
            )
        };
        if fd < 0 {
            return self.write_buffered(oid, data);
        }

        let ret = unsafe { libc::fallocate(fd, 0, 0, aligned_size as libc::off_t) };
        if ret != 0 {
            unsafe { libc::close(fd); }
            return self.write_buffered(oid, data);
        }

        let layout = std::alloc::Layout::from_size_align(aligned_size, ALIGN as usize).unwrap();
        let buf_ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if buf_ptr.is_null() {
            unsafe { libc::close(fd); }
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "alloc failed"));
        }
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, data.len()); }

        let written = unsafe { libc::pwrite(fd, buf_ptr as *const libc::c_void, aligned_size, 0) };
        unsafe { std::alloc::dealloc(buf_ptr, layout); }
        unsafe { libc::close(fd); }

        if written < 0 {
            return Err(io::Error::last_os_error());
        }

        // Open read fd and store in pool (only in keep_read_fds mode).
        if self.keep_read_fds {
            if let Some(read_fd) = self.open_read_fd(oid) {
                self.fd_pool.insert(oid, read_fd);
            }
        }

        Ok(())
    }

    /// Fallback write without O_DIRECT.
    fn write_buffered(&self, oid: ObjectId, data: &[u8]) -> io::Result<()> {
        let path = self.obj_path(oid);
        fs::write(&path, data)?;
        if self.keep_read_fds {
            if let Some(read_fd) = self.open_read_fd(oid) {
                self.fd_pool.insert(oid, read_fd);
            }
        }
        Ok(())
    }

    /// Delete an object file and close its pooled fd.
    pub fn delete_object(&self, oid: ObjectId) {
        self.fd_pool.remove(oid);
        let path = self.obj_path(oid);
        let _ = fs::remove_file(&path);
    }

    /// Get pre-opened fd from the pool.
    pub fn fd_pool_get(&self, oid: ObjectId) -> Option<RawFd> {
        self.fd_pool.get(oid)
    }
}
