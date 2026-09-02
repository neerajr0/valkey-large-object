//! ObjectFile — the reference-counted file handle owned by `LoValue`.
//!
//! An `ObjectFile` bundles one Tiered-mode object's *on-disk existence* with its
//! *open read fd* behind a single `Arc`. The keyspace `LoValue` holds a strong
//! reference, and so does every in-flight request that resolved the key. The fd is
//! closed and the NVMe file unlinked **only when the last strong reference drops**
//! (`ObjectFile::Drop`), and that teardown always runs off the event loop (via the
//! teardown worker below) — never inline on whichever thread dropped the ref.
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
use std::sync::mpsc;
use std::sync::OnceLock;

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
}

impl ObjectFile {
    /// Construct a cold handle: the file already exists on NVMe, but no read fd is
    /// open yet. The fd opens lazily on the first GET via `FdPool::ensure_open`.
    pub fn new_cold(object_id: ObjectId) -> Self {
        Self {
            object_id,
            fd: AtomicI32::new(-1),
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

        // 3. Dispatch the blocking teardown OFF the main thread. Drop ⟺ deletion, so
        //    the unlink is unconditional. object_ids are never reused, so a file that is
        //    not yet unlinked can never be mistaken for a live object in the meantime.
        let object_id = self.object_id;
        get_teardown_worker().enqueue(move || {
            if fd >= 0 {
                // SAFETY: fd was opened by us via libc::open and swapped out here
                // exactly once; no other thread can close the same descriptor.
                unsafe { libc::close(fd) };
            }
            let path = object_id.file_path(&crate::nvme_dir());
            let _ = std::fs::remove_file(&path);
        });
    }
}

// ─── Teardown worker ─────────────────────────────────────────────────────────

/// A single background thread that runs `ObjectFile::Drop`'s blocking teardown
/// (close + unlink) off the main event-loop thread. `close()`/`unlink()` are passive
/// low-priority cleanup, so any off-MT thread is fine; a dedicated worker keeps the
/// syscalls off the event loop regardless of which thread `Drop` fired on (a
/// synchronous `DEL` frees on the main thread; a last-in-flight-reader `Drop` runs
/// on a tokio worker).
pub struct TeardownWorker {
    tx: mpsc::Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl TeardownWorker {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Box<dyn FnOnce() + Send + 'static>>();
        std::thread::Builder::new()
            .name("largeobj-teardown".to_string())
            .spawn(move || {
                // Runs until the process exits (the sender lives for the module's
                // lifetime, so recv() only errors at shutdown).
                while let Ok(job) = rx.recv() {
                    job();
                }
            })
            .expect("failed to spawn largeobj-teardown thread");
        Self { tx }
    }

    /// Enqueue a teardown job. Non-blocking; the job runs on the worker thread.
    pub fn enqueue<F: FnOnce() + Send + 'static>(&self, job: F) {
        // If the worker has gone away (only at shutdown), the drop is a no-op; the
        // process is exiting and nvme-dir is wiped by the shutdown handler anyway.
        let _ = self.tx.send(Box::new(job));
    }
}

static TEARDOWN_WORKER: OnceLock<TeardownWorker> = OnceLock::new();

/// Global teardown worker, initialized (thread spawned) on first use.
pub fn get_teardown_worker() -> &'static TeardownWorker {
    TEARDOWN_WORKER.get_or_init(TeardownWorker::new)
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_file_cold() {
        let of = ObjectFile::new_cold(ObjectId(7));
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

        let of = ObjectFile::new_cold(ObjectId(9));
        of.store_fd(fd);
        assert_eq!(of.fd(), fd);

        // Dropping closes the fd (via the teardown worker). The unlink targets the
        // OID-derived path (not our temp path), so we verify fd closure directly.
        drop(of);

        // Give the teardown worker a moment to run the close.
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
