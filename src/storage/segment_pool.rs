//! SegmentPool — shared allocator over one or more Segments.
//!
//! Both NVMePool and DRAMPool delegate allocation/free to this struct.
//! Owns: Vec<Segment> + Mutex<Talc>.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Mutex;

use talc::{ClaimOnOom, Span, Talc};

use super::context::SegmentBuffer;
use super::segment::Segment;

pub struct SegmentPool {
    /// Segments owned by this pool.
    pub segments: Vec<Segment>,
    /// talc allocator managing all segments.
    allocator: Mutex<Talc<ClaimOnOom>>,
}

impl SegmentPool {
    /// Create a new SegmentPool with `segment_count` segments of `segment_size` bytes.
    /// Each segment's iovec_index is assigned by appending to the global IOVECS registry.
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let seg = Segment::new(segment_size); // iovec_index assigned below
            let idx = super::append_iovec(seg.iovec());
            let mut seg = seg;
            seg.iovec_index = idx;
            segments.push(seg);
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

    /// Allocate a buffer. Returns None if pool is exhausted.
    /// Size is rounded up to 4 KiB to match O_DIRECT / io_uring alignment —
    /// the uring layer rounds I/O lengths to 4 KiB, so the buffer must be
    /// at least that large to avoid writing past the allocation.
    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        let layout = Layout::from_size_align(super::align_up(size), super::IO_ALIGN).ok()?;
        let ptr = unsafe {
            self.allocator
                .lock()
                .expect("allocator lock unavailable")
                .malloc(layout)
        }
        .ok()?;
        let addr = ptr.as_ptr() as usize;

        let (seg_idx, offset) = self
            .find_segment(addr)
            .expect("talc returned ptr outside segments");
        self.segments[seg_idx].inc_ref();

        Some(SegmentBuffer {
            segment_idx: seg_idx as u8,
            offset: offset as u64,
            len: size as u32,
        })
    }

    /// Free a buffer back to the pool.
    pub fn free(&self, buf: &SegmentBuffer) {
        let seg = &self.segments[buf.segment_idx as usize];
        let ptr = unsafe { seg.base.add(buf.offset as usize) };
        let aligned_size = super::align_up(buf.len as usize);
        let layout =
            Layout::from_size_align(aligned_size, super::IO_ALIGN).expect("SegmentBuffer layout");
        unsafe {
            self.allocator
                .lock()
                .expect("allocator lock unavailable")
                .free(NonNull::new_unchecked(ptr), layout);
        }
        seg.dec_ref();
    }

    // ─── Segment Helpers ─────────────────────────────────────────────────

    /// Get absolute pointer for a SegmentBuffer.
    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        unsafe {
            self.segments[buf.segment_idx as usize]
                .base
                .add(buf.offset as usize)
        }
    }

    /// Given a raw pointer address, find which segment it belongs to.
    /// Returns (segment_index, offset_within_segment) for io_uring ReadFixed/WriteFixed.
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
