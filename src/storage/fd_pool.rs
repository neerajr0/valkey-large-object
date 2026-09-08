//! File Descriptor Pool — owns and caches open read fds as `Arc<OwnedFd>`.
//!
//! Each object's read fd is opened lazily on the first GET (via `ObjectFile::ensure_open`,
//! which calls `get_or_open` here) and cached as a strong `Arc<OwnedFd>` keyed by
//! `ObjectId`. Every later GET reuses it; every in-flight reader holds its own clone.
//! Because the fd is an `Arc<OwnedFd>`, it closes itself (RAII) once the last ref — the
//! pool's cache entry plus any reader clones — is gone.
//!
//! The pool earns its keep for three jobs:
//!   1. **Cache open read fds for reuse** — avoid a fresh `open()` on every GET.
//!   2. **Serialize the lazy first-open** — two concurrent first-GETs on the same cold
//!      object must not both `open()` and leak an fd; the write lock is the point.
//!   3. **Own the fd independently of the `ObjectFile` handle** — holding a strong ref
//!      here means a future evictor can drop the pool's ref without impacting inflight
//!      readers that may still hold a strong reference to the fd.
//!
//! `remove` drops the pool's strong ref. Today it is called only from
//! `ObjectFile::Drop` (on delete); the fd closes once this ref and all reader clones
//! are gone. TODO: a future cold-fd evictor will also call it to reclaim descriptors
//! under fd pressure while the file still exists, after which the next GET reopens.

use std::collections::HashMap;
use std::os::unix::io::{FromRawFd, OwnedFd};
use std::sync::{Arc, RwLock};

use crate::data_type::ObjectId;

pub struct FdPool {
    fds: RwLock<HashMap<ObjectId, Arc<OwnedFd>>>,
}

impl Default for FdPool {
    fn default() -> Self {
        Self::new()
    }
}

impl FdPool {
    pub fn new() -> Self {
        Self {
            fds: RwLock::new(HashMap::new()),
        }
    }

    /// Return the cached read fd for `object_id`, opening + caching it if not present.
    /// A clone of `Arc<OwnedFd>` is returned to the caller. The strong reference can be
    /// used to prevent the underlying fd from being closed during inflight read requests.
    /// Returns `None` only on a genuine `open()` failure.
    pub fn get_or_open(&self, object_id: ObjectId, dir: &str) -> Option<Arc<OwnedFd>> {
        // Fast path: shared read lock, clone the cached handle if present.
        let cached = self
            .fds
            .read()
            .expect("FdPool.fds lock unavailable")
            .get(&object_id)
            .cloned();
        if let Some(fd) = cached {
            return Some(fd);
        }

        // Slow path: serialize opens through the write lock.
        let mut fds = self.fds.write().expect("FdPool.fds lock unavailable");

        // Re-check under the lock: another caller may have opened it meanwhile.
        if let Some(fd) = fds.get(&object_id).cloned() {
            return Some(fd);
        }

        let path = object_id.file_path(dir);
        let c_path = std::ffi::CString::new(path).expect("file_path null");
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        // SAFETY: c_path is a valid NUL-terminated path; open returns a fd or -1.
        let raw = unsafe { libc::open(c_path.as_ptr(), flags) };
        if raw < 0 {
            return None;
        }
        let fd = Arc::new(unsafe { OwnedFd::from_raw_fd(raw) });
        fds.insert(object_id, Arc::clone(&fd));
        Some(fd)
    }

    /// Drop the pool's strong ref to `object_id`'s fd. The fd closes once this ref and all
    /// in-flight reader clones are gone. Called from `ObjectFile::Drop` (on delete).
    pub fn remove(&self, object_id: ObjectId) {
        // Only hold the write lock for remove(). Once the lock is released we can drop
        // the strong reference which may trigger the drop().
        let removed = self
            .fds
            .write()
            .expect("FdPool.fds lock unavailable")
            .remove(&object_id);
        drop(removed);
    }

    /// Number of cached fds. Test/introspection helper.
    pub fn len(&self) -> usize {
        self.fds.read().expect("FdPool.fds lock unavailable").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// An fd closes when its cache entry and all reader clones drop. No `Drop for FdPool`:
// it is a process-lifetime static whose fds are reclaimed by process exit. During
// normal operation `ObjectFile::Drop` calls `remove(object_id)`, which drops the pool's
// entry for that object; the fd then closes once the last reader clone is gone too.

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ObjectId;
    use std::os::unix::io::AsRawFd;

    // get_or_open uses O_DIRECT when direct_io() is set and can fail on some
    // filesystems (e.g. tmpfs). When the open fails we skip the fd-dependent
    // assertions — the Python integration tests cover the real NVMe path.

    #[test]
    fn test_remove_is_idempotent() {
        let pool = FdPool::new();
        // Removing an unregistered object_id is a harmless no-op.
        pool.remove(ObjectId(999));
        assert!(pool.is_empty());
    }

    #[test]
    fn test_get_or_open_caches_and_reuses() {
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let object_id = ObjectId(0x5151);
        let path = object_id.file_path(dir);
        std::fs::write(&path, b"hello").unwrap();

        let pool = FdPool::new();
        if let Some(fd1) = pool.get_or_open(object_id, dir) {
            assert_eq!(pool.len(), 1, "open registers exactly one cached fd");
            // Second call reuses the cached handle (a clone of the same Arc).
            let fd2 = pool.get_or_open(object_id, dir).expect("cached fd");
            assert_eq!(fd1.as_raw_fd(), fd2.as_raw_fd());
            assert!(
                Arc::ptr_eq(&fd1, &fd2),
                "reuse returns clones of the same Arc"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reader_clone_keeps_fd_open_across_remove() {
        // The honor rule at the refcount level: a reader holding an Arc<OwnedFd> clone
        // keeps the fd open even after the pool drops its own ref (as ObjectFile::Drop
        // does on delete); the fd closes only when the last clone goes away.
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let object_id = ObjectId(0x6262);
        let path = object_id.file_path(dir);
        std::fs::write(&path, b"world").unwrap();

        let pool = FdPool::new();
        if let Some(reader) = pool.get_or_open(object_id, dir) {
            let raw = reader.as_raw_fd();
            assert_eq!(pool.len(), 1);

            // Pool drops its ref; the reader clone is still alive, so fd stays open.
            pool.remove(object_id);
            assert_eq!(pool.len(), 0);
            // SAFETY: fcntl on the fd; valid because the reader clone holds it open.
            assert_ne!(
                unsafe { libc::fcntl(raw, libc::F_GETFD) },
                -1,
                "fd must stay open while a reader clone is alive"
            );

            // Last clone drops -> OwnedFd::drop closes the fd.
            drop(reader);
            // SAFETY: fcntl on the now-closed fd returns -1 with EBADF, no crash.
            assert_eq!(
                unsafe { libc::fcntl(raw, libc::F_GETFD) },
                -1,
                "fd must be closed once the last clone drops"
            );
        }

        let _ = std::fs::remove_file(&path);
    }
}
