//! ObjectFile — the reference-counted file handle owned by `LoValue`.
//!
//! An `ObjectFile` bundles one Tiered-mode object's *on-disk existence* with its
//! *open read fd* behind a single `Arc`. The keyspace `LoValue` holds a strong
//! reference, and so does every in-flight request that resolved the key. The fd is
//! closed and the NVMe file unlinked **only when the last strong reference drops**
//! (`ObjectFile::Drop`). The teardown syscalls run inline on whichever thread dropped
//! the last ref. If the drop occurs on the main thread, the deletion is handed to
//! the tokio blocking pool.
//!
//! Safety of deletion under a concurrent read (the "honor rule") rests on two facts,
//! and neither pins teardown to a particular thread:
//!   1. Reference counting — a reader pins its own `Arc`, so the file cannot be
//!      closed or unlinked while that reader is alive, no matter which thread drops
//!      the last remaining ref (a tokio worker, a lazyfree BIO thread, or the main
//!      thread on a synchronous free).
//!   2. The keyspace lookup and the keyspace removal are both serialized on the main
//!      event-loop thread, so a reader either resolves the key *before* it is
//!      unlinked — taking a pin that outlives the delete — or *after*, seeing it
//!      already gone. There is no interleaving that yields a half-freed object. This
//!      holds for async/lazyfree deletes too: Valkey unlinks the key on the main
//!      thread and only frees the value object off-thread afterward.
//!
//! The `FdPool` holds only a `Weak<ObjectFile>` (fd_pool.rs): it can upgrade to
//! reuse a cached fd, but it never counts toward existence and can neither keep a
//! file alive nor unlink one.
//!
//! `ObjectFile` is Tiered-mode-only (DRAM-only mode has no NVMe file). It has no
//! serialized form; it is reconstructed cold (fd = -1) on load and its fd opens
//! lazily on the first GET.

use std::sync::atomic::{AtomicI32, Ordering};

use crate::data_type::ObjectId;

// ─── ObjectFile ────────────────────────────────────────────────────────────────

/// Runtime handle for one object's NVMe file and its open read fd.
/// Always held behind an `Arc`; its `Drop` closes the fd + unlinks the file once,
/// when the last strong reference goes away.
#[derive(Debug)]
pub struct ObjectFile {
    /// Identity; the file path is *derived* (`ObjectId::file_path`), never stored.
    object_id: ObjectId,
    /// Open read descriptor as an interior-mutable slot. `-1` = not currently open.
    /// Atomic to support lazy open, a future in-place evictor, and close-once (`swap`).
    fd: AtomicI32,
    /// True on-disk size, used for NVMe utilization accounting.
    disk_len: u64,
}

impl ObjectFile {
    /// Construct a cold handle: the file already exists on NVMe, but no read fd is
    /// open yet. The fd opens lazily on the first GET via `FdPool::ensure_open`.
    /// `disk_len` is the true on-disk size; `Drop` releases exactly that many bytes.
    pub fn new_cold(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            fd: AtomicI32::new(-1),
            disk_len,
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Load the current fd (`-1` if not open). Uses Acquire so a reader that sees a
    /// valid fd also sees the `open()` that produced it.
    pub fn fd(&self) -> i32 {
        self.fd.load(Ordering::Acquire)
    }

    /// Publish a freshly-opened fd into the slot. Called by `FdPool::ensure_open`
    /// under the pool write lock.
    pub(super) fn store_fd(&self, fd: i32) {
        self.fd.store(fd, Ordering::Release);
    }
}

impl Drop for ObjectFile {
    fn drop(&mut self) {
        // Runs once, when the last strong ref drops: a completed deletion with no
        // remaining readers. Always means "object gone, nobody reading — close +
        // unlink."

        // 1. Deregister the (now-dangling) Weak from the fd pool. Cheap, inline.
        //    Guarded: the pool may be uninitialized in unit tests that build an
        //    ObjectFile without module init. In production an ObjectFile only ever
        //    exists in Tiered mode (Dram mode never constructs one), and there the
        //    fd pool is always initialized.
        if let Some(pool) = super::FD_POOL.get() {
            pool.remove(self.object_id);
        }

        // 2. Take the fd (swap makes close-once race-free vs. a future evictor).
        let fd = self.fd.swap(-1, Ordering::AcqRel);

        // 3. close() + unlink() are blocking syscalls. Drop ⟺ deletion, so the unlink
        //    is unconditional; object_ids are never reused, so an as-yet-unlinked file
        //    can't be mistaken for a live object in the meantime.
        let object_id = self.object_id;
        let disk_len = self.disk_len;
        let teardown = move || {
            if fd >= 0 {
                // SAFETY: fd was opened by us via libc::open and swapped out here
                // exactly once; no other thread can close the same descriptor.
                unsafe { libc::close(fd) };
            }
            let path = object_id.file_path(&crate::nvme_dir());
            let _ = std::fs::remove_file(&path);
            // Release exactly what create added — no stat, so it can't drift.
            crate::storage::uring::decrease_nvme_disk_usage(disk_len);
        };

        // If the drop is fired on the main thread, we attempt to hand off
        // the operation to a tokio runtime to reduce main thread contention.
        if crate::is_main_thread() {
            match crate::runtime_handle_opt() {
                // Fire-and-forget on the blocking pool (the JoinHandle is dropped).
                Some(handle) => {
                    handle.spawn_blocking(teardown);
                }
                // Runtime somehow gone on the main thread (pre-init/shutdown edge):
                // inline is the least-bad fallback.
                None => teardown(),
            }
        } else {
            teardown();
        }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // The honor rule at the refcount level: while ANY strong ref is alive the fd
    // stays open; only the LAST drop runs teardown. Integration cannot force this
    // mid-flight race deterministically — this is the unit-level proof.
    #[test]
    fn test_teardown_deferred_until_last_ref() {
        let tmpl = std::ffi::CString::new("/tmp/objfile_defer_XXXXXX").unwrap();
        let mut buf = tmpl.into_bytes_with_nul();
        // SAFETY: mkstemp takes a mutable C-string template and returns a valid fd.
        let fd = unsafe { libc::mkstemp(buf.as_mut_ptr() as *mut libc::c_char) };
        assert!(fd >= 0, "mkstemp failed");
        let path = std::ffi::CStr::from_bytes_with_nul(&buf)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Keyspace ref (LoValue.file) + a reader pin (cloned in lo_get before async).
        let keyspace_ref = std::sync::Arc::new(ObjectFile::new_cold(ObjectId(11), 0));
        keyspace_ref.store_fd(fd);
        let reader_pin = std::sync::Arc::clone(&keyspace_ref);
        assert_eq!(std::sync::Arc::strong_count(&keyspace_ref), 2);

        // Keyspace ref drops first (e.g. DEL removed the key): must NOT close the fd.
        drop(keyspace_ref);
        std::thread::sleep(std::time::Duration::from_millis(30));
        // SAFETY: fcntl on the fd; still valid because the reader pin holds it alive.
        assert_ne!(
            unsafe { libc::fcntl(fd, libc::F_GETFD) },
            -1,
            "fd must stay open while a reader still holds a ref"
        );

        // Last ref (the reader) drops -> Drop closes the fd (inline in tests).
        drop(reader_pin);
        for _ in 0..100 {
            // SAFETY: fcntl on a (possibly closed) fd returns -1/EBADF, no crash.
            if unsafe { libc::fcntl(fd, libc::F_GETFD) } == -1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // SAFETY: fcntl on the now-closed fd returns -1 with EBADF.
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_GETFD) },
            -1,
            "fd must be closed once the last ref drops"
        );

        // Drop unlinked the OID-derived path (not our temp path); clean up ours.
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_object_file_cold() {
        let of = ObjectFile::new_cold(ObjectId(7), 0);
        assert_eq!(of.object_id(), ObjectId(7));
        assert_eq!(of.fd(), -1);
    }

    #[test]
    fn test_drop_closes_fd_and_unlinks() {
        // Create a real temp file and hand its fd to an ObjectFile via store_fd.
        let tmpl = std::ffi::CString::new("/tmp/objfile_test_XXXXXX").unwrap();
        let mut buf = tmpl.into_bytes_with_nul();
        // SAFETY: mkstemp takes a mutable C-string template and returns a valid fd.
        let fd = unsafe { libc::mkstemp(buf.as_mut_ptr() as *mut libc::c_char) };
        assert!(fd >= 0, "mkstemp failed");
        let path = std::ffi::CStr::from_bytes_with_nul(&buf)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(std::path::Path::new(&path).exists());

        let of = ObjectFile::new_cold(ObjectId(9), 0);
        of.store_fd(fd);
        assert_eq!(of.fd(), fd);

        // Dropping closes the fd (inline in tests — no runtime). The unlink targets
        // the OID-derived path (not our temp path), so we verify fd closure directly.
        drop(of);

        // Drop runs teardown inline in tests (no runtime); poll to be safe.
        for _ in 0..100 {
            // SAFETY: fcntl on any int is safe; returns -1/EBADF once closed.
            let r = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            if r == -1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // SAFETY: fcntl on a (possibly closed) fd returns -1 with EBADF, no crash.
        let r = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_eq!(r, -1, "fd should be closed after ObjectFile::Drop");

        // Clean up our temp file (Drop unlinked the OID path, not this one).
        let _ = std::fs::remove_file(&path);
    }
}
