//! NVMePool — short-lived transient I/O buffers.
//!
//! Owns: NVMeSegments + Mutex<Talc>
//! Segments are fixed at startup, never resized.
//! StreamingContexts allocate from here and free on request completion.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Mutex;

use talc::{ClaimOnOom, Span, Talc};

use super::context::SegmentBuffer;
use super::segment::Segment;

pub struct NVMePool {
    /// Segments owned by this pool (fixed at startup).
    pub segments: Vec<Segment>,
    /// talc allocator managing all NVMePool segments.
    allocator: Mutex<Talc<ClaimOnOom>>,
}

impl NVMePool {
    /// Create a new NVMePool with `segment_count` segments of `segment_size` bytes.
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
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────

    /// Allocate a buffer from NVMePool. Returns None if exhausted.
    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        let layout = Layout::from_size_align(size, 4096).ok()?;
        let ptr = unsafe { self.allocator.lock().unwrap().malloc(layout) }.ok()?;
        let addr = ptr.as_ptr() as usize;

        let (seg_idx, offset) = self.find_segment(addr)?;
        self.segments[seg_idx].inc_ref();

        Some(SegmentBuffer {
            segment_idx: seg_idx as u8,
            offset: offset as u64,
            len: size as u32,
        })
    }

    /// Free a buffer back to NVMePool.
    pub fn free(&self, buf: &SegmentBuffer) {
        let seg = &self.segments[buf.segment_idx as usize];
        let ptr = unsafe { seg.base.add(buf.offset as usize) };
        let layout = Layout::from_size_align(buf.len as usize, 4096).unwrap();
        unsafe {
            self.allocator.lock().unwrap().free(NonNull::new_unchecked(ptr), layout);
        }
        seg.dec_ref();
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
