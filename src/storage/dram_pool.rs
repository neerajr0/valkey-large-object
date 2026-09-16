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
//! fall back to NVMe. The segment releases event-driven when its refcount hits 0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::context::{ObjectContext, SegmentBuffer};
use super::segment::Segment;
use super::segment_pool::SegmentPool;
use crate::data_type::ObjectId;

pub struct DRAMPool {
    pool: SegmentPool,
    /// Cached objects: ObjectId → Arc<ObjectContext>.
    /// RwLock: main thread reads (GET hit), tokio writes (promotion insert).
    objects: RwLock<HashMap<ObjectId, Arc<ObjectContext>>>,
}

impl DRAMPool {
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size),
            objects: RwLock::new(HashMap::new()),
        }
    }

    // ─── Allocator (draining-aware) ──────────────────────────────────────────

    /// Allocate a buffer. Returns None if pool is exhausted or the segment
    /// talc selected is draining (in which case the caller should fall back
    /// to NVMe rather than promoting).
    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        self.pool.alloc(size)
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    /// Access segments (needed by engine for buf_index lookup).
    pub fn segments(&self) -> &[Option<Segment>] {
        &self.pool.segments
    }

    /// Return the io_uring iovec_index for the segment owning `buf`.
    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
    }

    // ─── Object Map ──────────────────────────────────────────────────────────

    /// Lookup a cached object.
    ///
    /// If the segment backing this object is draining, returns None so the
    /// caller defers to NVMe — prevents new Arc refs from accumulating on a
    /// draining segment and accelerates its refcount drain to zero.
    pub fn get_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        let arc = self
            .objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .get(oid)
            .cloned()?;

        // Check if the segment backing the first buffer of this ObjectContext
        // is draining. If so, refuse to hand out a new Arc reference.
        let seg_idx = arc.buffers.first().map(|b| b.segment_idx as usize);
        if let Some(idx) = seg_idx {
            if let Some(Some(seg)) = self.pool.segments.get(idx) {
                if seg.draining.load(std::sync::atomic::Ordering::Acquire) {
                    return None; // Caller falls back to NVMe
                }
            }
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
    /// Returns None if pool is full (after one expand attempt) or object exceeds
    /// max-promote-size.
    ///
    /// Reactive expansion: if the initial alloc fails (pool exhausted),
    /// attempt one expand, then retry. If expand also fails (at cap), return None.
    ///
    /// SAFETY: must be called from the Valkey main event-loop thread only.
    /// Expand mutates the segment vec, which is single-threaded by the event loop.
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // Don't promote objects above the configured threshold.
        if obj_len > crate::max_promote_size() {
            return None;
        }

        // First attempt.
        let seg_buf = match self.alloc(obj_len as usize) {
            Some(buf) => buf,
            None => {
                // Reactive expansion: pool exhausted — try adding one segment.
                // SAFETY: main-thread only (see fn doc).
                let self_mut: &mut Self = unsafe { &mut *std::ptr::NonNull::from(self).as_ptr() };
                self_mut.try_expand()?; // returns None if at cap
                                        // Retry alloc after expand.
                self.alloc(obj_len as usize)?
            }
        };

        // Atomic check-and-insert under write lock to prevent TOCTOU race
        // (concurrent GETs promoting the same OID simultaneously).
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        if objects.contains_key(&oid) {
            self.free(&seg_buf);
            return None;
        }
        let obj_ctx = std::sync::Arc::new(super::context::ObjectContext::new_filling(
            vec![seg_buf],
            obj_len,
            1, // TODO: Single chunk today; streaming will pass actual chunk count.
        ));
        objects.insert(oid, obj_ctx.clone());
        Some(obj_ctx)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Current allocation utilization ratio of the pool (0.0–1.0).
    /// Exposed so the timer can check whether a proactive expand is needed.
    pub fn utilization_ratio(&self) -> f64 {
        self.pool.utilization_ratio()
    }

    /// Complete the release of any draining segments whose refcount has reached 0.
    ///
    /// Called from the shrink timer (main thread) each tick. Segments are marked
    /// draining by `try_shrink()`; their refcount drains as existing Arc holders
    /// finish. When `is_releasable()` is true, this completes the release:
    /// talc.truncate → clear sparse slot → dealloc.
    ///
    /// SAFETY: must be called from the Valkey main event-loop thread only.
    pub fn complete_drained_segments(&mut self) {
        self.pool.release_all_releasable();
    }

    /// Expand: add one segment to the pool.
    /// Returns the new iovec_index on success, None if at segment capacity.
    ///
    /// Called reactively when alloc fails, or proactively when utilization > 80%.
    ///
    /// SAFETY: must be called from the Valkey main event-loop thread only.
    pub fn try_expand(&mut self) -> Option<u16> {
        // Enforce the dram-maxmemory cap (if set > 0).
        let dram_max = crate::dram_maxmemory();
        if dram_max > 0 {
            let current_bytes = self.pool.live_segment_count() * self.pool.segment_size;
            if current_bytes as u64 >= dram_max {
                return None; // At configured cap — don't expand
            }
        }

        self.pool.expand()
    }

    /// Shrink (Tiered-mode only): mark the least-used segment as draining and
    /// remove its cached objects from the HashMap so GET handlers fall back to NVMe.
    ///
    /// The segment is released asynchronously: when all existing Arc holders drop
    /// their references, the last `free()` call sees `is_releasable()` and
    /// completes the release (talc.truncate + dealloc).
    ///
    /// Returns true if a victim was selected, false if nothing to shrink.
    ///
    /// SAFETY: must be called from the Valkey main event-loop thread only.
    /// Shrink: mark the least-used segment as draining and remove its cached
    /// objects from the HashMap so GET handlers fall back to NVMe (Tiered) or
    /// stop acquiring new Arc refs (both modes).
    ///
    /// **Dram mode:** only allowed when the victim segment has no live objects.
    /// Dropping a segment with live LO data would be data loss. If the victim
    /// has live objects, shrink is blocked — there is no NVMe fallback.
    ///
    /// **Tiered mode:** always safe — data persists on NVMe; dropping a cached
    /// segment just means GETs fall back to NVMe reads.
    ///
    /// Returns true if a victim was selected, false if nothing to shrink.
    ///
    /// SAFETY: must be called from the Valkey main event-loop thread only.
    pub fn try_shrink(&mut self) -> bool {
        let victim_idx = match self.pool.mark_drain_victim() {
            Some(i) => i,
            None => return false,
        };

        // In Dram mode, the DRAMPool IS the data — we can only drain a segment
        // if it has no live objects. Check the victim segment before proceeding.
        if crate::operating_mode() == crate::OperatingMode::Dram {
            let victim_has_objects = {
                let objects = self
                    .objects
                    .read()
                    .expect("DRAMPool.objects lock unavailable");
                objects.values().any(|ctx| {
                    ctx.buffers
                        .first()
                        .map(|b| b.segment_idx as usize == victim_idx)
                        .unwrap_or(false)
                })
            };
            if victim_has_objects {
                // Can't evict — unmark draining and bail.
                if let Some(Some(seg)) = self.pool.segments.get(victim_idx) {
                    seg.draining
                        .store(false, std::sync::atomic::Ordering::Release);
                }
                return false;
            }
        }

        // Remove cached objects on the victim segment from the HashMap.
        // Tiered: objects persist on NVMe; a later GET re-reads them.
        // Dram: checked above — no objects in this segment.
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        objects.retain(|_oid, ctx| {
            ctx.buffers
                .first()
                .map(|b| b.segment_idx as usize != victim_idx)
                .unwrap_or(true)
        });

        true
    }
}
