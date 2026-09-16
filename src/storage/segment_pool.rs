//! SegmentPool — shared allocator over one or more Segments.
//!
//! Both NVMePool and DRAMPool delegate allocation/free to this struct.
//!
//! ## Segment registry model (holes)
//!
//! `segments: Vec<Option<Segment>>` — each slot is either `Some(segment)` (live)
//! or `None` (empty/removed). This mirrors the io_uring sparse buffer table:
//! slot `i` here corresponds to slot `i` in the kernel's registered-buffer table.
//!
//! `iovec_index` on every Segment is its slot index, set write-once at birth and
//! **immutable for the segment's entire lifetime**. This eliminates the stale-index
//! race that swap-remove would introduce.
//!
//! Holes are transient: they exist only between a shrink and the next expand that
//! fills them. Steady-state growth (no shrinks) is fully dense.
//!
//! ## Draining / shrink protocol
//!
//! 1. Caller marks `segment.draining = true` (handled at DRAMPool level).
//! 2. GET handlers check draining before acquiring Arc<ObjectContext>; if draining
//!    they defer to NVMe, so no new Arc refs are acquired.
//! 3. Existing Arc holders drop naturally; each drop calls `pool.free()`.
//! 4. `free()` calls `segment.dec_ref()`, then checks `is_releasable()`.
//!    When `draining && refcount == 0`, the free path invokes `release_drained()`.
//! 5. `release_drained()`: talc.truncate → null the sparse slot → dealloc.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Mutex;

use talc::{ErrOnOom, Span, Talc};

use super::context::SegmentBuffer;
use super::segment::Segment;

pub struct SegmentPool {
    /// Segment slots. Slot `i` corresponds to iovec_index `i` in the sparse table.
    /// `None` = empty slot (no segment; sparse table slot is null).
    ///
    /// Mutated only from the Valkey main event-loop thread (expand/shrink), which
    /// is single-threaded. Read by alloc/free under the allocator Mutex.
    pub segments: Vec<Option<Segment>>,
    /// Single talc heap spanning all currently-claimed segments.
    /// Uses ErrOnOom — we manage expansion ourselves via `expand()`.
    allocator: Mutex<Talc<ErrOnOom>>,
    /// Size of each segment (uniform within a pool).
    pub segment_size: usize,
    /// Bytes currently allocated from this pool (sum of align_up(alloc_size)).
    /// Incremented on every successful alloc, decremented on free.
    /// Used to compute utilization = allocated_bytes / (live_segments * segment_size)
    /// for proactive expand decisions and observability.
    pub allocated_bytes: std::sync::atomic::AtomicUsize,
}

impl SegmentPool {
    /// Create a new SegmentPool with `segment_count` pre-allocated segments of
    /// `segment_size` bytes. Each segment's iovec_index = its position in the
    /// sparse slot table, registered via `super::append_iovec`.
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        assert!(
            segment_count >= 1,
            "SegmentPool requires at least 1 segment"
        );

        let mut segments: Vec<Option<Segment>> = Vec::with_capacity(segment_count);

        // Allocate all segments; assign iovec_index = slot position.
        for _ in 0..segment_count {
            let seg = Segment::new(segment_size);
            let idx = super::append_iovec(seg.iovec());
            let mut seg = seg;
            seg.iovec_index = idx;
            // claim_span assigned after talc construction below
            segments.push(Some(seg));
        }

        // Build talc with ErrOnOom — we manage expansion ourselves.
        let mut talc = Talc::new(ErrOnOom);

        // Claim all segments and record the exact Span talc stores internally.
        for seg_opt in segments.iter_mut() {
            let seg = seg_opt.as_mut().unwrap();
            let span = Span::from_base_size(seg.base, seg.size);
            let recorded = unsafe { talc.claim(span).expect("talc claim failed") };
            seg.claim_span = recorded;
        }

        Self {
            segments,
            allocator: Mutex::new(talc),
            segment_size,
            allocated_bytes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate a buffer. Returns None if pool is exhausted or all segments
    /// with capacity are draining (caller should fall back to NVMe).
    ///
    /// Size is rounded up to IO_ALIGN (4 KiB) for O_DIRECT / io_uring.
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

        let seg = self.segments[seg_idx].as_ref().unwrap();

        // If this segment is draining, free the allocation back and signal
        // the caller to fall back (None = pool unavailable for this object).
        if seg.draining.load(std::sync::atomic::Ordering::Acquire) {
            let aligned_size = super::align_up(size);
            let layout_free =
                Layout::from_size_align(aligned_size, super::IO_ALIGN).expect("layout");
            unsafe {
                self.allocator
                    .lock()
                    .expect("allocator lock unavailable")
                    .free(ptr, layout_free);
            }
            return None;
        }

        seg.inc_ref();
        self.allocated_bytes
            .fetch_add(super::align_up(size), std::sync::atomic::Ordering::Relaxed);
        Some(SegmentBuffer {
            segment_idx: seg_idx as u16,
            offset: offset as u64,
            len: size as u32,
        })
    }

    /// Free a buffer back to the pool.
    /// Decrements the segment refcount. If the segment becomes releasable
    /// (draining + refcount == 0), the cleanup is deferred to the next shrink
    /// timer tick rather than running inline — `release_drained` mutates the
    /// segment list and must only run on the main event-loop thread.
    pub fn free(&self, buf: &SegmentBuffer) {
        let seg_idx = buf.segment_idx as usize;
        let seg = self.segments[seg_idx]
            .as_ref()
            .expect("segment slot empty for live buffer — invariant broken");

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
        self.allocated_bytes.fetch_sub(
            super::align_up(buf.len as usize),
            std::sync::atomic::Ordering::Relaxed,
        );
        // Note: is_releasable() may now be true. The shrink timer on the main
        // thread will detect this on its next tick and call release_drained().
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Add a new segment to the pool. Finds the first `None` slot (or appends).
    /// Returns the assigned iovec_index on success, or None if the slot table
    /// would exceed u16::MAX.
    pub fn expand(&mut self) -> Option<u16> {
        let seg = Segment::new(self.segment_size);

        // Register in the sparse iovec table.
        let idx = super::append_iovec(seg.iovec());

        let mut seg = seg;
        seg.iovec_index = idx;

        // Claim the new segment in talc and record the exact Span.
        let span = Span::from_base_size(seg.base, seg.size);
        let recorded = unsafe {
            self.allocator
                .lock()
                .expect("allocator lock unavailable")
                .claim(span)
                .expect("talc claim failed on expand")
        };
        seg.claim_span = recorded;

        // Place in the first empty slot (or append).
        let slot = self.segments.iter().position(|s| s.is_none());
        match slot {
            Some(i) => {
                self.segments[i] = Some(seg);
            }
            None => {
                self.segments.push(Some(seg));
            }
        }

        Some(idx)
    }

    /// Pick the least-used segment as the shrink victim and mark it draining.
    /// Returns the victim's slot index, or None if no live segments or all
    /// segments already have live allocations that can't be trivially evicted
    /// (caller is responsible for evicting ObjectContexts before calling free()).
    ///
    /// "Least used" = segment with the lowest refcount among non-draining segments.
    pub fn mark_drain_victim(&self) -> Option<usize> {
        let (victim_idx, _) = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                opt.as_ref().and_then(|seg| {
                    if !seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                        Some((i, seg.refcount.load(std::sync::atomic::Ordering::Relaxed)))
                    } else {
                        None
                    }
                })
            })
            .min_by_key(|&(_, rc)| rc)?;

        // Mark draining so no new promotions land here.
        if let Some(Some(seg)) = self.segments.get(victim_idx) {
            seg.draining
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Some(victim_idx)
    }

    /// Scan all segments and release any that are draining with refcount == 0.
    /// Called from the shrink timer tick on the main thread.
    ///
    /// SAFETY: must be called from the Valkey main event-loop thread only.
    pub fn release_all_releasable(&mut self) {
        let releasable: Vec<usize> = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| opt.as_ref().filter(|seg| seg.is_releasable()).map(|_| i))
            .collect();
        for idx in releasable {
            self.release_drained(idx);
        }
    }

    /// Complete the drain of a segment: truncate its address range from talc,
    /// null the sparse slot, and dealloc its memory.
    /// Called automatically from `free()` when `is_releasable()` fires, or
    /// can be called explicitly once refcount reaches 0.
    ///
    /// SAFETY: caller must ensure refcount == 0 and draining == true.
    fn release_drained(&mut self, seg_idx: usize) {
        let seg = match self.segments[seg_idx].take() {
            Some(s) => s,
            None => return, // already released
        };

        // Remove the segment's address range from talc's free-lists.
        // Safe because refcount == 0 (no live allocations remain).
        unsafe {
            self.allocator
                .lock()
                .expect("allocator lock unavailable")
                .truncate(seg.claim_span, Span::empty());
        }

        // Clear the sparse iovec slot (null = free, no page pinning).
        super::clear_iovec(seg.iovec_index);

        // seg is dropped here → std::alloc::dealloc via Segment::drop.
        // used_memory decreases automatically (ValkeyAlloc tracks it).
        drop(seg);
    }

    // ─── Segment Helpers ─────────────────────────────────────────────────────

    /// Get absolute pointer for a SegmentBuffer.
    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        unsafe {
            self.segments[buf.segment_idx as usize]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken")
                .base
                .add(buf.offset as usize)
        }
    }

    /// Return the io_uring iovec_index for the segment owning `buf`.
    /// Panics if the segment slot is empty (should never happen for a live buffer).
    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.segments[buf.segment_idx as usize]
            .as_ref()
            .expect("segment slot empty for live buffer")
            .iovec_index
    }

    /// Utilization ratio: allocated_bytes / total_live_capacity.
    /// 0.0 = empty, 1.0 = fully allocated. Used for proactive expand decisions.
    pub fn utilization_ratio(&self) -> f64 {
        let live = self.live_segment_count();
        if live == 0 {
            return 0.0;
        }
        let capacity = live * self.segment_size;
        let allocated = self
            .allocated_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        (allocated as f64) / (capacity as f64)
    }

    /// Count of live (non-None, non-draining) segments.
    pub fn live_segment_count(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| {
                s.as_ref()
                    .map(|seg| !seg.draining.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(false)
            })
            .count()
    }

    /// Given a raw pointer address, find which segment it belongs to.
    /// Returns (slot_index, offset_within_segment).
    /// O(N) in segment count — used only on the alloc path, acceptable at
    /// ≤1024 segments (~tens of ns). Binary search on sorted bases is a
    /// future optimization if segment counts grow large.
    fn find_segment(&self, addr: usize) -> Option<(usize, usize)> {
        for (i, seg_opt) in self.segments.iter().enumerate() {
            if let Some(seg) = seg_opt {
                let base = seg.base as usize;
                if addr >= base && addr < base + seg.size {
                    return Some((i, addr - base));
                }
            }
        }
        None
    }
}
