//! DRAMPool — long-lived cached objects.
//!
//! SegmentPool + RwLock<HashMap<ObjectId, Arc<ObjectContext>>>.
//!
//! Expand: triggered reactively when alloc fails (pool full), or proactively
//! by the scaling cron when utilization exceeds the expand watermark.
//! Adds one `segment-size` segment via `try_expand()`.
//!
//! Shrink (Tiered-mode only): triggered proactively by the scaling cron when
//! `used_memory` approaches `maxmemory`. Picks the least-used segment, marks
//! it draining, and removes its cached objects from the HashMap so GET handlers
//! fall back to NVMe. The segment releases on the next cron tick when refcount hits 0.
//!
//! Shrink leaves the keyspace alone because the data is on NVMe. It is the only thing that
//! drops promoted copies: an arena too full to serve a promotion skips it instead
//! (`try_promote_object`), so nothing resident is ever given up for a cache fill.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::context::{ObjectContext, SegmentBuffer};
use super::segment_pool::SegmentPool;
use crate::data_type::ObjectId;

pub struct DRAMPool {
    pool: SegmentPool,
    /// Cached objects: ObjectId → Arc<ObjectContext>.
    /// RwLock: main thread reads (GET hit), tokio writes (promotion insert).
    objects: RwLock<HashMap<ObjectId, Arc<ObjectContext>>>,
    /// Cumulative count of successful expand operations since module load.
    pub expand_count: AtomicU64,
    /// Cumulative count of successful shrink operations since module load.
    pub shrink_count: AtomicU64,
}

impl DRAMPool {
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size, super::uring::PoolType::Dram),
            objects: RwLock::new(HashMap::new()),
            expand_count: AtomicU64::new(0),
            shrink_count: AtomicU64::new(0),
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate from the capacity the pool already has. Never expands, so it is also the
    /// question eviction asks: "is there room *now*".
    pub fn alloc_exact(&self, size: usize) -> Option<Vec<SegmentBuffer>> {
        self.pool.alloc_exact(size)
    }

    /// Allocate all buffers for an object, expanding once if needed.
    /// Try `alloc_exact`; on failure expand a single segment and retry.
    ///
    /// One expand suffices: an object is guaranteed <= `segment_size` (oversized
    /// ones are rejected at SET admission), so a fresh empty segment can hold it.
    /// If even a fresh segment can't (talc overhead on an object right at the
    /// boundary), no same-size segment can — so we return None rather than loop.
    ///
    /// Callers on the main thread pass their command `&Context`; callers on
    /// tokio workers pass `&Context::dummy()` (null ctx is accepted by
    /// RM_GetServerInfo for the memory watermark check).
    pub fn alloc_exact_or_expand(
        &self,
        ctx: &valkey_module::Context,
        obj_len: u64,
    ) -> Option<Vec<super::context::SegmentBuffer>> {
        if let Some(bufs) = self.pool.alloc_exact(obj_len as usize) {
            return Some(bufs);
        }
        // Existing segments are full for this object. Expand once (None if the
        // server maxmemory watermark would be crossed) and try the fresh segment.
        self.try_expand(ctx)?;
        self.pool.alloc_exact(obj_len as usize)
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
    }

    /// Whether the segment owning `buf` is registered in the io_uring kernel
    /// buffer table (picks fixed vs non-fixed I/O). See SegmentPool.
    pub fn is_buf_io_uring_registered(&self, buf: &SegmentBuffer) -> bool {
        self.pool.is_buf_io_uring_registered(buf)
    }

    /// Mark all current segments io_uring-registered (startup, post-register).
    pub fn mark_all_registered(&self) {
        self.pool.mark_all_registered();
    }

    /// Startup iovec snapshot for this pool's ring. See `SegmentPool::startup_iovecs`.
    pub fn startup_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.startup_iovecs()
    }

    /// See `SegmentPool::rebuild_dense_iovecs`. Called by this pool's io_uring
    /// engine on a re-registration.
    pub fn rebuild_dense_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.rebuild_dense_iovecs()
    }

    /// See `SegmentPool::io_uring_registered_count`.
    pub fn io_uring_registered_count(&self) -> usize {
        self.pool.io_uring_registered_count()
    }

    // ─── Object Map ──────────────────────────────────────────────────────────

    /// Lookup a cached object.
    ///
    /// Returns None if the object is not cached, or if its segment is draining
    /// (caller should fall back to NVMe to prevent new Arc refs on a draining segment).
    pub fn get_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        let arc = self
            .objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .get(oid)
            .cloned()?;

        // If any buffer of this object lives in a draining segment, refuse the
        // Arc — forces the caller to NVMe and lets the refcount drain to zero.
        let is_draining = self.pool.is_any_buffer_draining(&arc.buffers);
        if is_draining {
            return None;
        }
        Some(arc)
    }

    /// Insert an ObjectContext (promotion path).
    pub fn insert_object(&self, oid: ObjectId, ctx: Arc<ObjectContext>) {
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .insert(oid, ctx);
    }

    /// Remove an ObjectContext (free callback / eviction).
    pub fn remove_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .remove(oid)
    }

    /// True when someone outside the map holds this object — an in-flight transfer
    /// whose buffer the NIC is still reading. Eviction skips these: the memory is
    /// genuinely in use, so giving the entry up would free nothing.
    ///
    /// Reads through the guard rather than via `get_object`, which clones — and a
    /// clone is itself a reference, so it could never report anything but pinned.
    pub fn is_pinned(&self, oid: &ObjectId) -> bool {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .get(oid)
            .is_some_and(|arc| Arc::strong_count(arc) > 1)
    }

    /// Check if object exists (coalesce check — is promotion in progress?).
    pub fn contains_object(&self, oid: &ObjectId) -> bool {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .contains_key(oid)
    }

    /// Number of cached objects.
    pub fn object_count(&self) -> usize {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .len()
    }

    /// Try to allocate space and create an ObjectContext for this object.
    /// Returns None if pool is full or object exceeds max-promote-size.
    /// On success returns Arc<ObjectContext> in Filling state — caller reads
    /// NVMe data into the buffers, then calls mark_ready().
    /// Multi-buffer: allocates ceil(obj_len / chunk_size) buffers via
    /// alloc_exact_or_expand with all-or-nothing semantics.
    ///
    /// If we cannot expand (or promote into existing segments), returns None.
    /// Caller falls back to NVMe read (Tiered mode).
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // Don't promote objects above the configured threshold.
        if obj_len > crate::max_promote_size() {
            return None;
        }
        // All-or-nothing with reactive expansion via dummy context (promotion
        // runs on tokio workers — null ctx is accepted by RM_GetServerInfo).
        // Alloc BEFORE write lock — talc scan under memory pressure
        // won't block GET readers waiting on get_object().
        let dummy = valkey_module::Context::dummy();
        let buffers = self.alloc_exact_or_expand(&dummy, obj_len)?;
        // Atomic check-and-insert under write lock to prevent TOCTOU race
        // (concurrent GETs promoting the same OID simultaneously).
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        if objects.contains_key(&oid) {
            self.free_n(&buffers);
            return None;
        }
        // buf.len stays chunk_size for all buffers — must match alloc size for free().
        let obj_ctx = std::sync::Arc::new(super::context::ObjectContext::new_filling(buffers));
        objects.insert(oid, obj_ctx.clone());
        Some(obj_ctx)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    pub fn utilization_ratio(&self) -> f64 {
        self.pool.utilization_ratio()
    }

    /// Total allocated bytes across live segments. Used by INFO largeobj.
    pub fn allocated_bytes(&self) -> usize {
        self.pool.allocated_bytes()
    }

    /// Total free-gap count across live segments — the fragmentation signal.
    /// Used by INFO largeobj.
    pub fn fragment_count(&self) -> usize {
        self.pool.fragment_count()
    }

    /// Counts of (live, draining, unused) segments. Used by INFO largeobj.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        self.pool.segment_counts()
    }

    /// Call `f` with each segment's base pointer and size. Used for EFA registration.
    pub fn with_live_segment_slices<F>(&self, f: F)
    where
        F: FnMut(*const u8, usize),
    {
        self.pool.with_live_segment_slices(f);
    }

    /// Free segments that finished draining since the last scaling cron tick.
    ///
    /// A segment marked draining is freed asynchronously: its memory is not
    /// reclaimed until all in-flight Arc holders drop and refcount reaches 0.
    /// This function scans for segments where `draining && refcount == 0` and
    /// physically frees them: takes the Segment out of its slot, nulls this
    /// pool's io_uring iovec slot, and drops it. Each segment owns its
    /// own talc whose metadata lives inside the segment's memory, so dropping
    /// the Segment deallocates that memory and the talc vanishes with it — no
    /// `talc.truncate` is needed.
    ///
    /// Must be called from the Valkey main event-loop thread only.
    pub fn release_drained_segments(&self) {
        self.pool.release_all_releasable();
    }

    /// Add one segment to the pool, gated by the server-wide `maxmemory` (via
    /// `would_cross_memory_watermark`). When the server has no `maxmemory`
    /// configured (0), there is no ceiling and the pool grows on demand — the
    /// same unbounded behavior as core Valkey with `maxmemory 0`.
    ///
    /// Called reactively when alloc fails, or proactively when utilization > watermark.
    /// Returns the new iovec_index on success, `None` if the watermark would be
    /// crossed. Must be called on the main event-loop thread (reads server memory).
    pub fn try_expand(&self, ctx: &valkey_module::Context) -> Option<u16> {
        // Server-wide OOM guard: never grow into memory the shrink path would
        // immediately reclaim. No-op when the server has no maxmemory configured.
        if crate::would_cross_memory_watermark(ctx, self.pool.segment_size as u64) {
            return None;
        }

        let (idx, slice) = self.pool.expand()?;
        self.expand_count.fetch_add(1, Ordering::Relaxed);

        // Register the new segment with EFA (fatal on failure — see efa_register_segment).
        crate::efa_register_segment(slice);
        // Rebuild + re-register the DRAM ring's fixed-buffer table so the new
        // segment joins the fixed path (no-op in Dram mode). The iovecs table rebuild runs
        // on the DRAM poller only; the NVMe ring is untouched and keeps serving.
        super::uring::submit_reregister(super::uring::PoolType::Dram);
        Some(idx)
    }

    /// Mark the segment with the least cached bytes draining and remove its objects.
    ///
    /// Victim selection: the non-draining segment with the fewest allocated bytes.
    /// Uses the per-segment `allocated_bytes` counter — O(live segments), no HashMap scan.
    /// This minimises NVMe fallback work after eviction — clients re-read the least data.
    ///
    /// **Tiered mode:** always safe — data persists on NVMe; GETs fall back.
    /// **Dram mode:** only allowed when the victim segment has zero allocated bytes.
    ///   If it has live data, shrink is skipped — there is no NVMe fallback.
    ///
    /// Returns true if a victim was selected, false if nothing to shrink.
    pub fn try_shrink(&self) -> bool {
        let (victim_idx, victim_bytes) = match self.pool.find_shrink_victim() {
            Some(v) => v,
            None => return false,
        };

        if crate::operating_mode() == crate::OperatingMode::Dram && victim_bytes > 0 {
            // Can't evict — data would be lost with no NVMe fallback.
            // No unmark needed: segment was never marked draining.
            return false;
        }

        // Commit: mark draining only now that we know eviction is safe.
        self.pool.mark_segment_draining(victim_idx);

        // Remove cached objects on the victim segment from the HashMap.
        // Tiered: data persists on NVMe. Dram: verified empty above.
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .retain(|_oid, ctx| {
                ctx.buffers
                    .iter()
                    .all(|b| b.segment_idx as usize != victim_idx)
            });

        self.shrink_count.fetch_add(1, Ordering::Relaxed);
        true
    }
}
