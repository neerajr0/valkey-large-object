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
            pool: SegmentPool::new(segment_count, segment_size),
            objects: RwLock::new(HashMap::new()),
            expand_count: AtomicU64::new(0),
            shrink_count: AtomicU64::new(0),
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        self.pool.alloc(size)
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
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
    ///
    /// Reactive expansion: if alloc fails (pool exhausted), attempts one expand
    /// then retries. Returns None if at cap or object exceeds max-promote-size.
    pub fn try_promote_object(&self, oid: ObjectId, obj_len: u64) -> Option<Arc<ObjectContext>> {
        if obj_len > crate::max_promote_size() {
            return None;
        }

        let seg_buf = match self.alloc(obj_len as usize) {
            Some(buf) => buf,
            None => {
                // Reactive expansion: try adding one segment, then retry alloc.
                self.try_expand()?;
                self.alloc(obj_len as usize)?
            }
        };

        // Check-and-insert under write lock to prevent TOCTOU race
        // (concurrent GETs promoting the same OID simultaneously — only one wins).
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        if objects.contains_key(&oid) {
            self.free(&seg_buf);
            return None;
        }
        let obj_ctx = Arc::new(ObjectContext::new_filling(
            vec![seg_buf],
            obj_len,
            1, // TODO: Single chunk today; streaming will pass actual chunk count.
        ));
        objects.insert(oid, obj_ctx.clone());
        Some(obj_ctx)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    pub fn utilization_ratio(&self) -> f64 {
        self.pool.utilization_ratio()
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

    /// Complete the release of any draining segments whose refcount has reached 0.
    ///
    /// Called from the scaling cron each tick. Segments are marked draining by
    /// `try_shrink()`; their refcount drains as existing Arc holders finish.
    /// When `is_releasable()` is true, completes the release:
    /// talc.truncate → clear sparse slot → dealloc.
    pub fn complete_drained_segments(&self) {
        self.pool.release_all_releasable();
    }

    /// Add one segment to the pool, respecting the dram-maxmemory cap.
    ///
    /// Called reactively when alloc fails, or proactively when utilization > watermark.
    /// Returns the new iovec_index on success, None if at cap.
    pub fn try_expand(&self) -> Option<u16> {
        let dram_max = crate::dram_maxmemory();
        if dram_max > 0 {
            let current_bytes = self.pool.live_segment_count() * self.pool.segment_size;
            if current_bytes as u64 >= dram_max {
                return None;
            }
        }
        let result = self.pool.expand();
        if result.is_some() {
            self.expand_count.fetch_add(1, Ordering::Relaxed);
        }
        result
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
        let (victim_idx, victim_bytes) = match self.pool.select_and_drain_victim() {
            Some(v) => v,
            None => return false,
        };

        if crate::operating_mode() == crate::OperatingMode::Dram && victim_bytes > 0 {
            // Can't evict — data would be lost with no NVMe fallback.
            // Unmark draining since we're aborting.
            self.pool.unmark_draining(victim_idx);
            return false;
        }

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
