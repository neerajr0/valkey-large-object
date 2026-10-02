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
//! The handle also owns `disk_len`, its charge against `nvme-maxmemory`, and `Drop` is
//! the only thing that credits it back — so DEL, overwrite, expiry, flush and eviction
//! each account for themselves exactly once. Eviction keeps that true by *moving* the
//! last handle into a `DiskReservation` (instead of cloning the `Arc`).
//!
//! `ObjectFile` is Tiered-mode-only (DRAM-only mode has no NVMe file). It has no
//! serialized form; on load a handle is reconstructed for the existing file and its
//! fd opens lazily on the first GET.

use std::os::unix::io::OwnedFd;
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
}

impl ObjectFile {
    /// Construct the handle for a newly committed object version whose file already
    /// exists on NVMe. No read fd is open yet — it opens lazily on the first GET via
    /// `ensure_open`. `disk_len` is the true on-disk size, and `Drop` releases exactly
    /// that many bytes.
    pub fn new(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            disk_len,
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// On-disk bytes this version is charged for — what releasing it is worth.
    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Returns a cloned `Arc<OwnedFd>`. Calls into the `FdPool`, which owns the fd and
    /// caches it for reuse. The returned reference should be used to protect the
    /// fd from being closed while there are inflight read requests.
    pub fn ensure_open(&self, pool: &FdPool, dir: &str) -> Option<Arc<OwnedFd>> {
        pool.get_or_open(self.object_id, dir)
    }

    /// Copy this file into the new object version `reservation` pays for: commits the
    /// reservation, writes a header carrying the new OID with this object's `len`/`crc32c`,
    /// then copies the payload past the header byte-for-byte. `fsync`s before returning so the
    /// file is durable before it is exposed to O_DIRECT reads via io_uring.
    ///
    /// The returned handle's `Drop` releases the reservation's bytes. Returns `None` if any I/O
    /// fails (COPY then fails the command rather than aborting the node), leaving no partial
    /// file behind and the bytes returned to the budget.
    pub fn copy(
        &self,
        mut reservation: DiskReservation,
        len: u64,
        crc32c: Crc,
    ) -> Option<ObjectFile> {
        let new_oid = reservation.object_id();
        let dst_path = new_oid.file_path(&crate::nvme_dir());
        reservation.commit();
        match self.copy_file(&dst_path, new_oid, len, crc32c) {
            Ok(()) => Some(reservation.into_object_file()),
            Err(e) => {
                let _ = std::fs::remove_file(&dst_path);
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
        let disk_len = self.disk_len;

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
            // Release exactly what create added — no stat, so it can't drift.
            crate::storage::nvme::decrease_nvme_disk_usage(disk_len);
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
/// Ordering: victims are owned outright — `eviction::take_file` takes the handle out of the
/// keyspace. `commit` drops them, so the victims' files are gone from the directory before
/// the new one is created, and a reservation dropped instead of committed undoes itself.
pub struct DiskReservation {
    /// The version these bytes are for.
    object_id: ObjectId,
    /// Bytes this reservation is for — the new object's on-disk size.
    disk_len: u64,
    payment: Payment,
}

/// How `disk_len` is paid for, and so who owes the ledger credit.
enum Payment {
    /// Charged to the ledger, by a successful `try_reserve` or by `commit`. Dropping the
    /// reservation credits it back.
    Charged,
    /// Not charged yet. Objects gone from the keyspace, their bytes still charged, pay for
    /// `disk_len` on `commit`. Dropping them returns their own credit and calls `unlink(2)`.
    PaidBy(Vec<ObjectFile>),
    /// The new `ObjectFile` owes the credit.
    HandedOff,
}

impl DiskReservation {
    /// The budget had room: `disk_len` is charged already and nothing was destroyed.
    pub fn charged(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            disk_len,
            payment: Payment::Charged,
        }
    }

    /// The budget was full: `victims` are gone from the keyspace and their still-charged
    /// bytes, which the caller has verified cover `disk_len`, are what will pay for it.
    pub fn paid_by(object_id: ObjectId, disk_len: u64, victims: Vec<ObjectFile>) -> Self {
        Self {
            object_id,
            disk_len,
            payment: Payment::PaidBy(victims),
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Charge our own bytes, then drop the victims. `Drop` runs in-line and is responsible for
    /// unlinking victim files and crediting their `disk_len` before creating the new file.
    ///
    /// Charging first is what keeps a competing SET's `try_reserve` isolated: the ledger briefly
    /// over-counts by briefly including both the new file and victims whose files are being unlinked.
    pub fn commit(&mut self) {
        if let Payment::PaidBy(victims) = std::mem::replace(&mut self.payment, Payment::Charged) {
            super::nvme::increase_nvme_disk_usage(self.disk_len);
            drop(victims);
        }
    }

    /// Hand the charged bytes to the new version's handle. From here the `ObjectFile` owes
    /// the release, which is where every other delete path already expects it to live.
    pub fn into_object_file(mut self) -> ObjectFile {
        debug_assert!(matches!(self.payment, Payment::Charged), "commit first");
        self.payment = Payment::HandedOff;
        ObjectFile::new(self.object_id, self.disk_len)
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        // The write never got as far as an `ObjectFile`, so give the budget back. Victims
        // drop with `self.payment` and release their own — and they stay destroyed, because
        // the keyspace lost them before this reservation existed and no reply promised
        // otherwise.
        if matches!(self.payment, Payment::Charged) {
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

    /// The whole point of the type: a victim's bytes pay for its replacement, the ledger ends up
    /// charged for exactly one object rather than two or zero, and the victim's file is gone from
    /// the directory before `commit` returns so the caller writes into space that is actually free.
    /// That ordering is what owning the handles buys — a cloned `Arc` would leave the unlink to
    /// whenever the keyspace's own reference died on a lazyfree BIO thread.
    #[test]
    fn commit_spends_victims_on_the_newcomer() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();
        let victim_oid = ObjectId(u64::MAX - 8);
        let newcomer_oid = ObjectId(u64::MAX - 9);

        let victim = charged_file(victim_oid);
        let victim_path = victim_oid.file_path(&crate::nvme_dir());
        assert_eq!(nvme_disk_usage(), base + DISK_LEN);
        assert!(
            std::path::Path::new(&victim_path).exists(),
            "fixture must place the victim's file"
        );

        let mut res = DiskReservation::paid_by(newcomer_oid, DISK_LEN, vec![victim]);
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "claiming a victim must not move the ledger — its bytes are still charged, \
             which is what stops another SET from spending them"
        );

        res.commit();
        assert!(
            !std::path::Path::new(&victim_path).exists(),
            "commit must unlink the victim before the caller writes the new file"
        );
        assert_eq!(
            nvme_disk_usage(),
            base + DISK_LEN,
            "one object out, one in — the charge lands, the victim's credit comes back"
        );

        // After `into_object_file` the *handle* owes the bytes. If the reservation's `Drop` also
        // released, the new object would be accounted for by nobody and the ledger would drift down
        // by one object per SET. `Drop` on the handle is then the whole of the release path: without
        // it every DEL, overwrite and expiry would leak budget.
        place_file_for(newcomer_oid);
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

    /// A reservation that never hands its file over must undo itself: the EFA read failed, say, or
    /// nothing ever wrote the file. The victims are still destroyed, because the keyspace lost them
    /// before the reservation existed, but their bytes go back to the ledger rather than being lost
    /// to it. Both constructors, and both sides of `commit`.
    #[test]
    fn a_reservation_that_never_hands_over_its_file_undoes_itself() {
        let _g = accounting_test_lock();
        let base = nvme_disk_usage();

        let victim = charged_file(ObjectId(u64::MAX - 4));
        drop(DiskReservation::paid_by(
            ObjectId(u64::MAX - 5),
            DISK_LEN,
            vec![victim],
        ));
        assert_eq!(nvme_disk_usage(), base, "victim bytes returned, none taken");

        // `charged` means the caller's `try_reserve` already succeeded, so the bytes are charged up
        // front and the drop has to give them back itself.
        super::super::nvme::increase_nvme_disk_usage(DISK_LEN);
        drop(DiskReservation::charged(ObjectId(u64::MAX - 6), DISK_LEN));
        assert_eq!(nvme_disk_usage(), base, "un-reserved on the way out");

        // Past `commit` and still abandoned: nothing wrote a file, so the reservation dies owing
        // the bytes it charged and has to credit them itself.
        let victim = charged_file(ObjectId(u64::MAX - 2));
        let mut res = DiskReservation::paid_by(ObjectId(u64::MAX - 3), DISK_LEN, vec![victim]);
        res.commit();
        drop(res);
        assert_eq!(nvme_disk_usage(), base, "an abandoned commit leaks nothing");
    }
}
