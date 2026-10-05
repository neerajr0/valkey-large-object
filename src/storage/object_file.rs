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
//! The handle also owns `disk_len`, its charge against `nvme-maxmemory`. `release` credits it back
//! once: from `Drop`, or earlier from eviction via a `DiskReservation`, since a tombstoned key keeps
//! its reference (see `crate::eviction::tombstone`).
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
    /// Set by the first `release`, so the file is unlinked and its bytes credited once.
    released: AtomicBool,
}

impl ObjectFile {
    /// Construct the handle for a newly committed object version whose file already
    /// exists on NVMe. No read fd is open yet — it opens lazily on the first GET via
    /// `ensure_open`. `disk_len` is the true on-disk size; `Drop` releases exactly
    /// that many bytes.
    pub fn new(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            disk_len,
            released: AtomicBool::new(false),
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Deregister the fd, unlink the file and credit `disk_len`, once however often it is called.
    /// From the main thread the teardown is queued to the tokio pool, so it lands later.
    pub fn release(&self) {
        self.release_on(crate::is_main_thread());
    }

    /// `release` that always unlinks inline: COPY blocks the event loop on a whole-object copy anyway.
    pub fn release_blocking(&self) {
        self.release_on(false);
    }

    fn release_on(&self, defer: bool) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
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

        if defer {
            crate::runtime_handle().spawn(async move { teardown() });
        } else {
            teardown();
        }
    }

    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Returns a cloned `Arc<OwnedFd>`. Calls into the `FdPool`, which owns the fd and
    /// caches it for reuse. The returned reference should be used to protect the
    /// fd from being closed while there are inflight read requests.
    pub fn ensure_open(&self, pool: &FdPool, dir: &str) -> Option<Arc<OwnedFd>> {
        pool.get_or_open(self.object_id, dir)
    }

    /// Copy this file into the new object version `reservation` pays for: writes a
    /// header carrying the new OID with this object's `len`/`crc32c`, then copies the
    /// payload past the header byte-for-byte. `fsync`s before returning so the file is
    /// durable before it is exposed to O_DIRECT reads via io_uring.
    ///
    /// The returned handle's `Drop` releases the reserved bytes. Returns `None` if any I/O
    /// fails (COPY then fails the command rather than aborting the node), leaving no partial
    /// file behind.
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
        // The last ref dropped: the object is gone, unless eviction already released it.
        self.release();
    }
}

// ─── DiskReservation ───────────────────────────────────────────────────────────

/// One Tiered SET's claim on the `nvme-maxmemory` budget, made on the main thread and settled by
/// the write task: either budget that is already charged, or evicted victims whose still-charged
/// bytes pay on `commit`. Deciding that up front means the task cannot fail for capacity and no
/// concurrent SET can spend what this holds. The cost is that a pending claim is invisible to the
/// next SET, which evicts its own victims rather than wait.
///
/// `commit` releases the victims (their keys may still hold a reference, so dropping alone would
/// not), and a reservation dropped uncommitted releases them too: they stay destroyed.
pub struct DiskReservation {
    object_id: ObjectId,
    /// The new object's on-disk size.
    disk_len: u64,
    payment: Payment,
}

/// How `disk_len` is paid for, and so who owes the ledger credit.
enum Payment {
    /// Charged to the ledger; dropping the reservation credits it back.
    Charged,
    /// Not charged yet: the evicted victims, still charged, pay on `commit`.
    PaidBy(Vec<Arc<ObjectFile>>),
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

    /// The budget was full: `victims` are evicted, and their still-charged bytes (which the caller
    /// has verified cover `disk_len`) will pay for it.
    pub fn paid_by(object_id: ObjectId, disk_len: u64, victims: Vec<Arc<ObjectFile>>) -> Self {
        Self {
            object_id,
            disk_len,
            payment: Payment::PaidBy(victims),
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Charge the new file, then release the victims, unlinking inline. Charging first keeps a
    /// competing `try_reserve` honest: the ledger over-counts until the unlinks land.
    pub fn commit(&mut self) {
        if let Payment::PaidBy(victims) = std::mem::replace(&mut self.payment, Payment::Charged) {
            super::nvme::increase_nvme_disk_usage(self.disk_len);
            for victim in victims {
                victim.release_blocking();
            }
        }
    }

    /// Hand the charged bytes to the new version's handle, which now owes the release.
    pub fn into_object_file(mut self) -> ObjectFile {
        debug_assert!(matches!(self.payment, Payment::Charged), "commit first");
        self.payment = Payment::HandedOff;
        ObjectFile::new(self.object_id, self.disk_len)
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        // The write never produced an `ObjectFile`: give the budget back. Victims stay destroyed.
        match &self.payment {
            Payment::Charged => super::nvme::decrease_nvme_disk_usage(self.disk_len),
            Payment::PaidBy(victims) => victims.iter().for_each(|victim| victim.release()),
            Payment::HandedOff => {}
        }
    }
}
