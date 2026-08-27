//! DRAMPool — long-lived cached objects.
//!
//! SegmentPool + RwLock<HashMap<ObjectId, Arc<ObjectContext>>>.
//! Uses alloc_checked (draining-aware). Segments can expand/shrink.

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

    // ─── Allocator (draining-aware) ──────────────────────────────────────

    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        self.pool.alloc_checked(size)
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    /// Access segments (needed by engine for buf_index lookup).
    pub fn segments(&self) -> &[Segment] {
        &self.pool.segments
    }

    // ─── Object Map ──────────────────────────────────────────────────────

    /// Lookup a cached object. Returns Arc clone (safe to use after releasing lock).
    pub fn get_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .get(oid)
            .cloned()
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
    /// Returns None if pool is full or object exceeds max-promote-size.
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // Don't promote objects above the configured threshold.
        if obj_len > crate::max_promote_size() {
            return None;
        }

        // Atomic check-and-insert under write lock to prevent TOCTOU race
        // (concurrent GETs promoting the same OID simultaneously).
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        if objects.contains_key(&oid) {
            return None;
        }

        let seg_buf = self.alloc(obj_len as usize)?;
        let obj_ctx = std::sync::Arc::new(super::context::ObjectContext::new_ready(
            vec![seg_buf],
            obj_len,
        ));
        objects.insert(oid, obj_ctx.clone());
        Some(obj_ctx)
    }
}
