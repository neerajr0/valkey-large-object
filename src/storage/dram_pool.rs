//! DRAMPool — long-lived cached objects.
//!
//! Owns: DRAMSegments + Mutex<Talc> + RwLock<HashMap<ObjectId, Arc<ObjectContext>>>
//! Segments can expand/shrink under memory pressure.

use std::alloc::Layout;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, RwLock};

use talc::{ClaimOnOom, Span, Talc};

use crate::data_type::ObjectId;
use super::context::{ObjectContext, SegmentBuffer};
use super::segment::Segment;

pub struct DRAMPool {
    /// Segments owned by this pool.
    pub segments: Vec<Segment>,
    /// talc allocator managing all DRAMPool segments.
    allocator: Mutex<Talc<ClaimOnOom>>,
    /// Cached objects: ObjectId → Arc<ObjectContext>.
    /// RwLock: main thread reads (GET hit), tokio writes (promotion insert).
    objects: RwLock<HashMap<ObjectId, Arc<ObjectContext>>>,
}

impl DRAMPool {
    /// Create a new DRAMPool with `segment_count` segments of `segment_size` bytes.
    /// `base_buf_index`: starting io_uring iovec index for these segments.
    pub fn new(segment_count: usize, segment_size: usize, base_buf_index: u16) -> Self {
        let mut segments = Vec::with_capacity(segment_count);
        for i in 0..segment_count {
            segments.push(Segment::new(segment_size, base_buf_index + i as u16));
        }

        // Create talc with first segment as initial span.
        let first_span = Span::from_base_size(segments[0].base, segments[0].size);
        let mut talc = Talc::new(unsafe { ClaimOnOom::new(first_span) });

        // Claim remaining segments.
        for seg in segments.iter().skip(1) {
            let span = Span::from_base_size(seg.base, seg.size);
            unsafe { talc.claim(span).expect("talc claim failed") };
        }

        Self {
            segments,
            allocator: Mutex::new(talc),
            objects: RwLock::new(HashMap::new()),
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────

    /// Allocate a buffer from DRAMPool. Returns None if exhausted.
    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        let layout = Layout::from_size_align(size, 4096).ok()?;
        let ptr = unsafe { self.allocator.lock().unwrap().malloc(layout) }.ok()?;
        let addr = ptr.as_ptr() as usize;

        let (seg_idx, offset) = self.find_segment(addr)?;
        if self.segments[seg_idx].draining.load(std::sync::atomic::Ordering::Acquire) {
            // Segment draining — free immediately, return None.
            unsafe { self.allocator.lock().unwrap().free(ptr, layout) };
            return None;
        }
        self.segments[seg_idx].inc_ref();

        Some(SegmentBuffer {
            segment_idx: seg_idx as u8,
            offset: offset as u64,
            len: size as u32,
        })
    }

    /// Free a buffer back to DRAMPool.
    pub fn free(&self, buf: &SegmentBuffer) {
        let seg = &self.segments[buf.segment_idx as usize];
        let ptr = unsafe { seg.base.add(buf.offset as usize) };
        let layout = Layout::from_size_align(buf.len as usize, 4096).unwrap();
        unsafe {
            self.allocator.lock().unwrap().free(NonNull::new_unchecked(ptr), layout);
        }
        seg.dec_ref();
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

    // ─── Segment Helpers ─────────────────────────────────────────────────

    /// Get absolute pointer for a SegmentBuffer.
    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        unsafe { self.segments[buf.segment_idx as usize].base.add(buf.offset as usize) }
    }

    /// Get iovecs for io_uring registration.
    pub fn iovecs(&self) -> Vec<libc::iovec> {
        self.segments.iter().map(|s| s.iovec()).collect()
    }

    fn find_segment(&self, addr: usize) -> Option<(usize, usize)> {
        for (i, seg) in self.segments.iter().enumerate() {
            let base = seg.base as usize;
            if addr >= base && addr < base + seg.size {
                return Some((i, addr - base));
            }
        }
        None
    }
}
