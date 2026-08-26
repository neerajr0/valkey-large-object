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

    pub fn iovecs(&self) -> Vec<libc::iovec> {
        self.pool.iovecs()
    }

    /// Access segments (needed by engine for buf_index lookup).
    pub fn segments(&self) -> &[Segment] {
        &self.pool.segments
    }

    // ─── Object Map ──────────────────────────────────────────────────────

    /// Lookup a cached object. Returns Arc clone (safe to use after releasing lock).
    pub fn get_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        self.objects.read().unwrap().get(oid).cloned()
    }

    /// Insert an ObjectContext (promotion path).
    pub fn insert_object(&self, oid: ObjectId, ctx: Arc<ObjectContext>) {
        self.objects.write().unwrap().insert(oid, ctx);
    }

    /// Remove an ObjectContext (free callback / eviction).
    pub fn remove_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        self.objects.write().unwrap().remove(oid)
    }

    /// Check if object exists (coalesce check — is promotion in progress?).
    pub fn contains_object(&self, oid: &ObjectId) -> bool {
        self.objects.read().unwrap().contains_key(oid)
    }

    /// Number of cached objects.
    pub fn object_count(&self) -> usize {
        self.objects.read().unwrap().len()
    }
}
