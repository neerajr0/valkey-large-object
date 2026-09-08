//! File Descriptor Pool — the read-fd cache: an index of `object_id → Weak<ObjectFile>`.
//!
//! The pool does **not** own fds: each open read fd lives inside its `ObjectFile`
//! (an `AtomicI32` slot) and is closed by that `ObjectFile`'s `Drop` on delete
//! (object_file.rs). The pool earns its keep for two jobs:
//!   1. **Cache open read fds for reuse** — a cold object's read fd is opened lazily
//!      on the first GET and then reused by every later GET (no repeated `open()`),
//!      staying open until the object is deleted. TODO: in the future we will close
//!      cold fds for items that are in DRAM to relieve descriptor pressure.
//!   2. **Serialize the lazy first-open** — two concurrent first-GETs on the same
//!      cold object must not both `open()` and leak an fd; the write lock is the
//!      serialization point.
//!
//! `remove` is **deregister-only** (closes nothing). Today it is called only from
//! `ObjectFile::Drop` (on delete). TODO: a future fd-cache eviction path will also
//! remove an entry to close an fd *while its file still exists* — reclaiming
//! descriptors under fd pressure — after which the next GET reopens lazily.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::{Arc, RwLock, Weak};

use super::object_file::ObjectFile;
use crate::data_type::ObjectId;

pub struct FdPool {
    fds: RwLock<HashMap<ObjectId, Weak<ObjectFile>>>,
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

    /// Return a usable read fd for `objfile`. Reuses existing fd if available, otherwise,
    /// opens a new fd.
    ///
    /// The caller holds a strong `Arc<ObjectFile>`, so the file is guaranteed still
    /// linked, so the `open()` cannot spuriously `ENOENT`. Returns `None` only on
    /// a genuine `open()` failure. Direct-io is IMMUTABLE, so an fd opened here stays
    /// valid until the object is deleted (or TODO a future evictor closes it in place,
    /// after which the next call reopens).
    pub fn ensure_open(&self, objfile: &Arc<ObjectFile>, dir: &str) -> Option<RawFd> {
        // Fast path, lock-free: already open.
        let fd = objfile.fd();
        if fd >= 0 {
            return Some(fd);
        }

        // Slow path: serialize opens through the write lock.
        let mut fds = self.fds.write().expect("FdPool.fds lock unavailable");

        // Re-check under the lock: another caller may have opened it meanwhile.
        let fd = objfile.fd();
        if fd >= 0 {
            return Some(fd);
        }

        let path = objfile.object_id().file_path(dir);
        let c_path = std::ffi::CString::new(path).expect("file_path null");
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        // SAFETY: c_path is a valid NUL-terminated path; open returns a fd or -1.
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd < 0 {
            return None;
        }
        // Publish the fd into the shared slot and register the Weak.
        objfile.store_fd(fd);
        fds.insert(objfile.object_id(), Arc::downgrade(objfile));
        Some(fd)
    }

    /// Deregister an object's `Weak` entry. **Closes nothing** — the fd is closed by
    /// `ObjectFile::Drop`. Called from that `Drop`. Idempotent: a missing entry (an
    /// object whose fd never opened, so never registered) is a no-op.
    pub fn remove(&self, oid: ObjectId) {
        self.fds
            .write()
            .expect("FdPool.fds lock unavailable")
            .remove(&oid);
    }

    /// Number of registered handles (open or recently-dropped-but-not-yet-pruned).
    /// Test/introspection helper.
    pub fn len(&self) -> usize {
        self.fds.read().expect("FdPool.fds lock unavailable").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// No `Drop for FdPool`: the pool owns no fds. Every fd is owned by its `ObjectFile`
// and closed by `ObjectFile::Drop`. At shutdown, live `LoValue`s (and their
// `ObjectFile`s) are freed by the keyspace teardown, closing their fds then.

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ObjectId;

    #[test]
    fn test_ensure_open_fast_path_does_not_register() {
        // When the fd slot is already populated, ensure_open takes the lock-free
        // fast path: it returns the cached fd and does NOT register a Weak.
        let pool = FdPool::new();
        let of = Arc::new(ObjectFile::new_cold(ObjectId(1), 0));
        of.store_fd(4242); // pretend an fd is already open
        assert_eq!(pool.ensure_open(&of, "/nonexistent"), Some(4242));
        assert_eq!(pool.len(), 0, "fast path must not register a Weak");
    }

    #[test]
    fn test_remove_is_deregister_only_and_idempotent() {
        let pool = FdPool::new();
        // Removing an unregistered oid is a harmless no-op.
        pool.remove(ObjectId(999));
        assert!(pool.is_empty());
    }

    #[test]
    fn test_ensure_open_registers_and_remove_does_not_close() {
        // Bonus end-to-end coverage of the slow (open) path. O_DIRECT can fail on
        // some filesystems (e.g. tmpfs); if the open fails we can't exercise this
        // branch here — the Python integration tests cover the real NVMe path.
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let oid = ObjectId(0x5151);
        let path = oid.file_path(dir);
        std::fs::write(&path, b"hello").unwrap();

        let pool = FdPool::new();
        let of = Arc::new(ObjectFile::new_cold(oid, 0));

        if let Some(fd1) = pool.ensure_open(&of, dir) {
            assert_eq!(pool.len(), 1, "slow path registers exactly one Weak");
            // Second call hits the fast path and returns the same fd.
            let fd2 = pool.ensure_open(&of, dir).expect("cached fd");
            assert_eq!(fd1, fd2);
            assert_eq!(of.fd(), fd1);

            // remove() deregisters but must NOT close the fd.
            pool.remove(oid);
            assert_eq!(pool.len(), 0);
            // SAFETY: fcntl on the fd; still open because remove closes nothing.
            let r = unsafe { libc::fcntl(fd1, libc::F_GETFD) };
            assert_ne!(r, -1, "remove must NOT close the fd");
            // SAFETY: close the fd we opened, cleaning up the test.
            unsafe { libc::close(fd1) };
        }

        let _ = std::fs::remove_file(&path);
    }
}
