//! ObjectFile — the reference-counted existence handle owned by `LoValue`.
//!
//! An `ObjectFile` represents one Tiered-mode object's *on-disk existence*: its
//! identity (`ObjectId`, from which the file path is derived) and the NVMe bytes it
//! accounts for. Objects are immutable and versioned: each write or copy creates a
//! new `ObjectFile` with a monotonically-increasing `ObjectId` (a new version),
//! and `LoValue` always references the latest one. The keyspace `LoValue` holds an
//! `Arc<ObjectFile>`, and so does every in-flight request that resolved the key. When
//! the last reference drops (`ObjectFile::Drop`) that version is gone: we deregister
//! its read fd from the `FdPool` and unlink the NVMe file.
//!
//! Safety of deletion under a concurrent read rests on two facts:
//!   1. Reference counting on two independent Arcs — a reader pins `Arc<ObjectFile>`
//!      (existence) AND holds an `Arc<OwnedFd>` clone (the open fd). Neither the file
//!      nor its fd can be reclaimed while that reader is alive, no matter which thread
//!      drops the last ObjectFile ref (a tokio worker, a lazyfree BIO thread, or the
//!      main thread on a synchronous free).
//!   2. The keyspace lookup and removal are both serialized on the main event-loop
//!      thread, so a reader either resolves the key *before* it is unlinked — taking
//!      pins that outlive the delete — or *after*, seeing it already gone. This holds
//!      for async/lazyfree deletes too: Valkey unlinks the key on the main thread and
//!      frees the value object off-thread afterward.
//!
//! Teardown (pool deregister + unlink) runs inline on whichever thread dropped the
//! last ref, except on the main event-loop thread, where blocking would stall the
//! server, so it is handed to the tokio worker pool (see `crate::is_main_thread`).
//!
//! The handle also owns `disk_len` which is its charge against `nvme-maxmemory`. The 
//! release in `Drop` accounts for freeing this charge in most cases. `DiskReservation`,
//! can cause the reservation to be free'd before the `Drop` is processed and it is
//! important to not double count the free. The current use-case for this is evictions.
//!
//! `ObjectFile` is Tiered-mode-only (DRAM-only mode has no NVMe file). It has no
//! serialized form; on load a handle is reconstructed for the existing file and its
//! fd opens lazily on the first GET.

use std::os::unix::io::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::fd_pool::FdPool;
use super::Crc;
use crate::data_type::ObjectId;

// ─── ObjectFile ────────────────────────────────────────────────────────────────

/// Existence handle for one version of an object's NVMe file. Always held behind an
/// `Arc`; its
/// `Drop` deregisters the read fd from the pool and unlinks the file once, when the
/// last reference goes away.
#[derive(Debug)]
pub struct ObjectFile {
    /// Identity; the file path is *derived* (`ObjectId::file_path`), never stored.
    object_id: ObjectId,
    /// True on-disk size, used for NVMe utilization accounting.
    disk_len: u64,
    /// Whether `Drop` still owes `disk_len` back to the ledger.
    owes_ledger: AtomicBool,
}

impl ObjectFile {
    /// Construct the handle for a newly committed object version whose file already
    /// exists on NVMe. No read fd is open yet — it opens lazily on the first GET via
    /// `ensure_open`. `disk_len` is the true on-disk size; `Drop` releases exactly
    /// that many bytes if not already foregone (`owes_ledger` is `true`).
    pub fn new(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            disk_len,
            owes_ledger: AtomicBool::new(true),
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// On-disk bytes this version is charged for — what releasing it is worth.
    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Hand this handle's ledger obligation to the caller: `Drop` will still unlink the file,
    /// but will not credit `disk_len`. Only `DiskReservation::commit` calls this, and only
    /// after folding the bytes into its own single ledger update.
    fn forgo_ledger(&self) {
        self.owes_ledger.store(false, Ordering::Release);
    }

    /// Returns a cloned `Arc<OwnedFd>`. Calls into the `FdPool`, which owns the fd and
    /// caches it for reuse. The returned reference should be used to protect the
    /// fd from being closed while there are inflight read requests.
    pub fn ensure_open(&self, pool: &FdPool, dir: &str) -> Option<Arc<OwnedFd>> {
        pool.get_or_open(self.object_id, dir)
    }

    /// Copy this file into a new object version: allocates a fresh `ObjectId`, writes a
    /// header carrying the new OID with this object's `len`/`crc32c`, then copies the
    /// payload past the header byte-for-byte. `fsync`s before returning so the file is
    /// durable before it is exposed to O_DIRECT reads via io_uring.
    ///
    /// Reserves `disk_len` against nvme-maxmemory up front; the returned handle's `Drop`
    /// releases it. Returns `None` if the reservation or any I/O fails (COPY then fails
    /// the command rather than aborting the node), leaving no partial file behind.
    pub fn copy(&self, len: u64, crc32c: Crc) -> Option<ObjectFile> {
        let dir = crate::nvme_dir();
        let disk_len = self.disk_len;
        if !super::nvme::try_reserve_nvme_disk_usage(disk_len) {
            return None;
        }
        let new_oid = ObjectId::next();
        let dst_path = new_oid.file_path(&dir);
        match self.copy_file(&dst_path, new_oid, len, crc32c) {
            Ok(()) => Some(ObjectFile::new(new_oid, disk_len)),
            Err(e) => {
                let _ = std::fs::remove_file(&dst_path);
                super::nvme::decrease_nvme_disk_usage(disk_len);
                valkey_module::logging::log_warning(format!(
                    "largeobj: Tiered COPY {:?} -> {new_oid:?} failed: {e}",
                    self.object_id
                ));
                None
            }
        }
    }

    /// Header write + payload copy for `copy`. Buffered I/O on purpose: this runs off
    /// the io_uring path, and the destination is not yet visible to any reader.
    fn copy_file(
        &self,
        dst_path: &str,
        new_oid: ObjectId,
        len: u64,
        crc32c: Crc,
    ) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let mut src = std::fs::File::open(self.object_id.file_path(&crate::nvme_dir()))?;
        let mut dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dst_path)?;
        dst.write_all(&super::FileHeader::new(new_oid, len, crc32c).to_page())?;
        src.seek(SeekFrom::Start(super::FILE_HEADER_SIZE))?;
        std::io::copy(&mut src, &mut dst)?;
        dst.sync_all()
    }
}

impl Drop for ObjectFile {
    fn drop(&mut self) {
        // Runs once, when the last ref drops: a completed deletion with no
        // remaining ObjectFile refs. Means "object gone" — deregister the fd and
        // unlink the file.
        let object_id = self.object_id;

        // If the deleted bytes have already been accoutned for (ex. Evictions)
        // we need to prevent double accounting those free'd bytes.
        if self.owes_ledger.swap(false, Ordering::AcqRel) {
            super::nvme::decrease_nvme_disk_usage(self.disk_len);
        }

        // Deregistering drops the pool's Arc<OwnedFd>; if no in-flight reader
        // holds a clone, the fd's OwnedFd closes at this time.
        let teardown = move || {
            if let Some(pool) = super::FD_POOL.get() {
                pool.remove(object_id);
            }
            let path = object_id.file_path(&crate::nvme_dir());
            if let Err(e) = std::fs::remove_file(&path) {
                super::warn_failed_unlink("teardown", &path, &e);
            }
        };

        // The main event-loop thread must be kept syscall-free. Hand the operation
        // off to the tokio pool if the drop() is invoked from the main thread.
        if crate::is_main_thread() {
            crate::runtime_handle().spawn(async move { teardown() });
        } else {
            teardown();
        }
    }
}

// ─── DiskReservation ───────────────────────────────────────────────────────────

/// One Tiered SET's claim on the `nvme-maxmemory` budget: created on the main thread,
/// carried into the write task, settled there.
///
/// Isolation: whatever pays for `disk_len` — spare budget or victims already out of the
/// keyspace — is decided on the event loop and travels with the write, so the task cannot
/// fail for capacity and no concurrent SET can spend what this holds. The cost is that a
/// pending claim's bytes are invisible to the next SET, which frees its own victims instead
/// of waiting; at a full cache that is one victim per newcomer either way.
///
/// Atomicity: victims are held as `ObjectFile` handles rather than a byte total, which keeps
/// their `Drop` — and so the `unlink(2)` — inside `commit`, before the new file is written and
/// before their bytes are released to anyone else. A reservation dropped instead of committed
/// undoes itself, because the handles still owe their bytes.
pub struct DiskReservation {
    /// The version these bytes are for.
    object_id: ObjectId,
    /// Bytes this reservation is for — the new object's on-disk size.
    disk_len: u64,
    /// Objects gone from the keyspace whose bytes pay for `disk_len`, held as handles so that
    /// dropping them is both the release and the `unlink(2)`.
    victims: Vec<Arc<ObjectFile>>,
    /// Whether `disk_len` was charged to the ledger.
    charged: bool,
}

impl DiskReservation {
    /// The budget had room: `disk_len` is charged already and nothing was destroyed.
    pub fn charged(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            disk_len,
            victims: Vec::new(),
            charged: true,
        }
    }

    /// The budget was full: `victims` are gone from the keyspace and their still-charged
    /// bytes, which the caller has verified cover `disk_len`, are what will pay for it.
    pub fn paid_by(object_id: ObjectId, disk_len: u64, victims: Vec<Arc<ObjectFile>>) -> Self {
        Self {
            object_id,
            disk_len,
            victims,
            charged: false,
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Release the victims. Next, charge our own bytes minus the victims to the ledger in one
    /// transaction. This is done to protect the write task by providing accounting isolation
    /// against competing writes. Free'd victims are marked as no longer being owed to the ledger
    /// for when they are later dropped.
    pub fn commit(&mut self) {
        let mut freed = 0;
        for victim in &self.victims {
            freed += victim.disk_len();
            victim.forgo_ledger();
        }
        // in-progress note: if this was coded better I think victims.clear() would make
        // owes_ledger redundant because they'd all get dropped here. It would cut down on
        // code complexity considerably.
        self.victims.clear();
        let charge = if self.charged { 0 } else { self.disk_len };
        super::nvme::exchange_nvme_disk_usage(freed, charge);
        self.charged = true;
    }

    /// Hand the charged bytes to the new version's handle. From here the `ObjectFile` owes
    /// the release, which is where every other delete path already expects it to live.
    pub fn into_object_file(mut self) -> ObjectFile {
        self.charged = false;
        ObjectFile::new(self.object_id, self.disk_len)
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        // The write never got as far as an `ObjectFile`, so give the budget back. Victims
        // drop with `self.victims` and release their own — and they stay destroyed, because
        // the keyspace lost them before this reservation existed and no reply promised
        // otherwise.
        if self.charged {
            super::nvme::decrease_nvme_disk_usage(self.disk_len);
        }
    }
}

// ─── Unit Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::nvme::{accounting_test_lock, nvme_disk_usage};

    const DISK_LEN: u64 = 64 * 1024;

    /// Make a handle for `oid` droppable: point `nvme-dir` at a writable directory and put
    /// the file there, so the `unlink(2)` succeeds. Otherwise `Drop` logs the failure, and
    /// logging aborts outside a server — `log_internal`'s `cfg!(test)` escape is compiled
    /// into the *valkey-module* crate, which is not built as a test.
    fn place_file_for(oid: ObjectId) {
        let dir = std::env::temp_dir().join("bigobj-object-file-tests");
        std::fs::create_dir_all(&dir).expect("temp dir must be creatable");
        let dir = dir.to_str().expect("temp path must be UTF-8").to_string();
        crate::set_nvme_dir_for_test(&dir);
        std::fs::write(oid.file_path(&dir), b"x").expect("stand-in object file");
    }

    fn file_with_real_path(oid: ObjectId) -> ObjectFile {
        place_file_for(oid);
        ObjectFile::new(oid, DISK_LEN)
    }

    /// Stand in for the SET that built a file: charge the ledger, then hand back the handle
    /// that owes it. Every `ObjectFile` exists only after a successful reservation.
    fn charged_file(oid: ObjectId) -> ObjectFile {
        super::super::nvme::increase_nvme_disk_usage(DISK_LEN);
        file_with_real_path(oid)
    }

    /// `Drop` is the only thing that credits the ledger, so this is the whole of the release
    /// path. Without it every DEL, overwrite and expiry would leak budget until the ledger was
    /// full of objects that no longer exist.
    #[test]
    fn drop_releases_the_charge() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();

        drop(charged_file(ObjectId(u64::MAX - 1)));

        assert_eq!(nvme_disk_usage(), base, "Drop alone must return the bytes");
    }

    /// The whole point of the type: a victim's bytes pay for its replacement, and the ledger
    /// ends up charged for exactly one object rather than two or zero.
    #[test]
    fn commit_spends_victims_on_the_newcomer() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();

        let victim = Arc::new(charged_file(ObjectId(u64::MAX - 2)));
        assert_eq!(nvme_disk_usage(), base + DISK_LEN);

        let mut res = DiskReservation::paid_by(ObjectId(u64::MAX - 3), DISK_LEN, vec![victim]);
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "claiming a victim must not move the ledger — its bytes are still charged, \
             which is what stops another SET from spending them"
        );

        res.commit();
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "one object out, one object in"
        );

        // Nothing wrote a file, so the reservation dies still owing the bytes.
        drop(res);
        assert_eq!(nvme_disk_usage(), base, "an abandoned commit leaks nothing");
    }

    /// `commit` settles the victims' bytes itself, so a handle that outlives it — another `Arc`
    /// still held somewhere — must credit nothing when it finally drops. Without the obligation
    /// flag this is a double-free: the ledger drifts down by one object per eviction until
    /// `decrease` underflows and aborts the node.
    #[test]
    fn a_victim_outliving_commit_does_not_credit_twice() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();

        let victim = Arc::new(charged_file(ObjectId(u64::MAX - 8)));
        let held = Arc::clone(&victim);

        let mut res = DiskReservation::paid_by(ObjectId(u64::MAX - 9), DISK_LEN, vec![victim]);
        place_file_for(ObjectId(u64::MAX - 9));
        res.commit();
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "one object out, one in — netted, never passing through base"
        );

        drop(held);
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "the late drop unlinks and nothing more; commit already paid"
        );

        drop(res.into_object_file());
        assert_eq!(nvme_disk_usage(), base);
    }

    /// A reservation dropped before `commit` — the EFA read failed, say — must undo itself.
    /// The victims are still destroyed, because the keyspace lost them before the reservation
    /// existed, but their bytes go back to the ledger rather than being lost to it.
    #[test]
    fn drop_before_commit_returns_everything() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();

        let victim = Arc::new(charged_file(ObjectId(u64::MAX - 4)));
        drop(DiskReservation::paid_by(
            ObjectId(u64::MAX - 5),
            DISK_LEN,
            vec![victim],
        ));
        assert_eq!(nvme_disk_usage(), base, "victim bytes returned, none taken");

        // The other constructor: budget had room, so the bytes are charged up front and the
        // drop has to give them back itself.
        super::super::nvme::increase_nvme_disk_usage(DISK_LEN);
        drop(DiskReservation::charged(ObjectId(u64::MAX - 6), DISK_LEN));
        assert_eq!(nvme_disk_usage(), base, "un-reserved on the way out");
    }

    /// The handoff. After `into_object_file` the bytes stay charged and the *handle* owes
    /// them — if the reservation's `Drop` also released, the new object would be accounted
    /// for by nobody and the ledger would drift down by one object per SET.
    #[test]
    fn into_object_file_transfers_the_charge() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();

        // `charged` means the caller's `try_reserve` already succeeded, so charge first.
        let oid = ObjectId(u64::MAX - 7);
        super::super::nvme::increase_nvme_disk_usage(DISK_LEN);
        let mut res = DiskReservation::charged(oid, DISK_LEN);
        res.commit();

        place_file_for(oid);
        let file = res.into_object_file();
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "the object is charged, exactly once"
        );

        drop(file);
        assert_eq!(
            nvme_disk_usage(),
            base,
            "and the handle is what releases it"
        );
    }
}
