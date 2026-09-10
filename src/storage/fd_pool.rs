//! File Descriptor Pool — owns and caches open read fds as `Arc<OwnedFd>`, keyed by `ObjectId`.
//!
//! A read fd is opened lazily on the first GET (via `ObjectFile::ensure_open` →
//! `get_or_open`) and cached for reuse; every later GET and in-flight reader gets a clone.
//! Being an `Arc<OwnedFd>`, it closes itself (RAII) once the last ref — the cache entry
//! plus any reader clones — is gone. An fd is transient metadata, never persisted on the
//! `LoValue` data type; it lives only in this pool, decoupled from object lifetime.
//!
//! The pool earns its keep for three jobs:
//!   1. **Reuse** — avoid a fresh `open()` on every GET.
//!   2. **Serialize the lazy first-open** — the write lock stops two concurrent first-GETs
//!      on a cold object from both `open()`-ing and leaking an fd.
//!   3. **Own the fd independently of `ObjectFile`** — a future evictor can drop the pool's
//!      ref to reclaim a cold fd without disturbing in-flight readers that still hold one.
//!
//! `remove` drops the pool's ref; today only `ObjectFile::Drop` (on delete) calls it. There
//! is no `Drop for FdPool` — it is a process-lifetime static, so any fds still cached at
//! teardown are reclaimed by process exit. TODO: a cold-fd evictor will also call `remove`
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
    /// A clone of `Arc<OwnedFd>` is returned to the caller. The reference can be
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

    /// Drop the pool's ref to `object_id`'s fd. The fd closes once this ref and all
    /// in-flight reader clones are gone. Called from `ObjectFile::Drop` (on delete).
    pub fn remove(&self, object_id: ObjectId) {
        // Only hold the write lock for remove(). Once the lock is released we can drop
        // the reference which may trigger the drop().
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
