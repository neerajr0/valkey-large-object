//! SegmentPool — collection of Segments, each with its own talc allocator.
//!
//! Both NVMePool and DRAMPool delegate allocation/free to this struct.
//!
//! ## Design
//!
//! `slots: Vec<Option<Segment>>` behind a `Mutex` — each slot is either
//! `Some(segment)` (live) or `None` (empty/removed). Slot index == iovec index
//! in the sparse io_uring buffer table. Segments have `iovec_index` set
//! write-once at birth.
//!
//! Each `Segment` owns its own `Talc<>` instance covering exactly its own
//! `[base, base+size)` range. There is NO shared allocator across segments.
//! `alloc_one` walks live non-draining segments in least-loaded-first order
//! under one state lock, prechecks each with `talc.get_allocated_span`, and
//! allocates from the first that passes — talc.malloc after a passing
//! precheck is guaranteed to succeed. Per-segment locking means concurrent
//! allocs on different segments never contend.
//!
//! ## Draining / shrink protocol
//!
//! 1. Caller marks `segment.draining = true` (via `mark_segment_draining`).
//! 2. Picker skips draining segments — no new allocations land there.
//! 3. GET handlers check draining before acquiring Arc<ObjectContext>; if draining
//!    they defer to NVMe, so no new Arc refs are acquired.
//! 4. Existing Arc holders drop naturally; each drop calls `pool.free()`.
//! 5. `free()` calls `segment.dec_ref()`.
//! 6. The scaling cron calls `release_all_releasable()` each tick, which drops
//!    Segments whose `is_releasable()` is true. Segment::drop deallocates the
//!    backing memory — its talc's metadata lived inside that memory, so no
//!    talc.truncate is needed.

use std::alloc::Layout;
use std::sync::Mutex;

use super::context::SegmentBuffer;
use super::segment::Segment;

/// Mutable segment state: just the slot vector. No shared allocator, no
/// reverse-lookup index — each segment carries its own talc, and the
/// segment_idx is known at alloc time (from the picker).
struct SegmentState {
    /// Segment slots. Slot `i` = iovec_index `i` in the sparse io_uring table.
    /// `None` = empty slot (hole from a previous drain, or unused capacity).
    slots: Vec<Option<Segment>>,
}

pub struct SegmentPool {
    state: Mutex<SegmentState>,
    /// Size of each segment (uniform within a pool).
    pub segment_size: usize,
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

        let mut slots: Vec<Option<Segment>> = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let mut seg = Segment::new(segment_size);
            let idx = super::append_iovec(seg.iovec());
            seg.iovec_index = idx;
            slots.push(Some(seg));
        }

        Self {
            state: Mutex::new(SegmentState { slots }),
            segment_size,
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate a single buffer. Delegates to `alloc_n(size, 1, 1)`.
    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        self.alloc_n(size, 1, 1).map(|mut v| v.remove(0))
    }

    /// Allocate up to `count` uniform buffers. Used directly for StreamingContext
    /// (rotating window of reusable buffers), and internally by `alloc_for_object`.
    ///
    /// Allocates up to `count` buffers of `chunk_size` each, requiring at least
    /// `min_required`. Each iteration walks the live non-draining segments in
    /// LEAST-LOADED-first order under one state lock, and for the first one
    /// that passes an exact talc `get_allocated_span` precheck, allocates from
    /// its own talc allocator. talc.malloc after a passing precheck is
    /// guaranteed to succeed. No retries, no poisoning.
    ///
    /// Returns `None` if fewer than `min_required` could be allocated (partial
    /// allocation freed internally). Callers never need cleanup logic.
    pub fn alloc_n(
        &self,
        chunk_size: usize,
        count: usize,
        min_required: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        let aligned_size = super::align_up(chunk_size);
        let layout = Layout::from_size_align(aligned_size, super::IO_ALIGN)
            .expect("alloc_n: invalid chunk_size layout");
        let mut buffers: Vec<SegmentBuffer> = Vec::with_capacity(count);

        for _ in 0..count {
            let Some(buf) = self.alloc_one(aligned_size, layout) else {
                break;
            };
            buffers.push(buf);
        }

        if buffers.len() < min_required {
            for buf in &buffers {
                self.free(buf);
            }
            return None;
        }
        Some(buffers)
    }

    /// Allocate all buffers for an ObjectContext (DRAM cache), where the last
    /// chunk may be smaller. All-or-nothing.
    /// Derives chunk geometry from `obj_len` and `crate::chunk_size()`.
    pub fn alloc_for_object(&self, obj_len: u64) -> Option<Vec<SegmentBuffer>> {
        let chunk_size = crate::chunk_size();
        let total_chunks = obj_len.div_ceil(chunk_size as u64) as u32;
        let n = total_chunks as usize;
        let last_chunk_size = {
            let rem = (obj_len % chunk_size as u64) as usize;
            if rem == 0 {
                chunk_size
            } else {
                rem
            }
        };
        let full_count = if n > 1 { n - 1 } else { 0 };
        // Allocate the first N-1 uniform-sized buffers (all-or-nothing).
        let mut buffers = self.alloc_n(chunk_size, full_count, full_count)?;
        // Allocate the last (possibly smaller) buffer.
        let aligned_last = super::align_up(last_chunk_size);
        let last_layout = Layout::from_size_align(aligned_last, super::IO_ALIGN)
            .expect("alloc_for_object: invalid last_chunk_size layout");
        let Some(last_buf) = self.alloc_one(aligned_last, last_layout) else {
            // All-or-nothing: free the uniform buffers we already got.
            for buf in &buffers {
                self.free(buf);
            }
            return None;
        };
        buffers.push(last_buf);
        Some(buffers)
    }

    /// One allocation: pick the least-loaded live, non-draining segment that
    /// clears the fast byte filter (single O(N) `min_by_key` pass, no sort, no
    /// candidate Vec), then exact-precheck it via `talc.get_allocated_span` and
    /// allocate. Returns None if there is no eligible segment, or the picked
    /// segment's exact precheck fails.
    ///
    /// Why not fall back to the next-least-loaded on a failed precheck: the fast
    /// filter already guaranteed `cur + aligned_size <= seg.size`, so the exact
    /// precheck can only fail by talc's per-chunk boundary-tag overhead tipping
    /// it over the edge. Since all segments are the same size, if the emptiest
    /// eligible segment can't fit the alloc by that overhead sliver, none can —
    /// the correct answer is "pool full", which the caller handles via reactive
    /// expand. A fallback loop would add machinery for a case that yields the
    /// same result.
    fn alloc_one(&self, aligned_size: usize, layout: Layout) -> Option<SegmentBuffer> {
        let st = self.state.lock().expect("state lock unavailable");

        // Single O(N) pass: least-loaded eligible segment. Done under the state
        // lock so slots stay stable through the subsequent malloc.
        let (_, seg_idx) = st
            .slots
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                let seg = opt.as_ref()?;
                if seg.draining.load(std::sync::atomic::Ordering::Acquire) {
                    return None;
                }
                let cur = seg
                    .allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                // Fast filter — allocated_bytes ignores talc's per-chunk overhead,
                // so the exact precheck below still runs on the winner.
                if cur + aligned_size > seg.size {
                    return None;
                }
                Some((cur, i))
            })
            .min_by_key(|&(cur, _)| cur)?;

        let seg = st.slots[seg_idx].as_ref().unwrap();
        let seg_base = seg.base;
        let mut talc = seg.talc.lock().expect("segment talc lock unavailable");
        // TODO: add a precheck that accounts for per-allocation overhead
        // (tag + alignment padding) to avoid the malloc attempt when the
        // segment is clearly full. For now, rely on malloc's own Err.
        let ptr = match unsafe { talc.malloc(layout) } {
            Ok(p) => p,
            Err(_) => return None,
        };
        let offset = ptr.as_ptr() as usize - seg_base as usize;
        seg.inc_ref();
        seg.allocated_bytes
            .fetch_add(aligned_size, std::sync::atomic::Ordering::Relaxed);
        drop(talc);
        drop(st);
        Some(SegmentBuffer {
            segment_idx: seg_idx as u16,
            offset: offset as u64,
            len: aligned_size as u32,
        })
    }

    /// Free a buffer back to its owning segment.
    /// Uses `buf.segment_idx` directly — no reverse lookup needed.
    pub fn free(&self, buf: &SegmentBuffer) {
        self.free_n(std::slice::from_ref(buf));
    }

    /// Free multiple buffers back to the pool.
    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        for buf in buffers {
            let seg_idx = buf.segment_idx as usize;
            // No align_up needed — buf.len is already the aligned size talc allocated.
            let layout = Layout::from_size_align(buf.len as usize, super::IO_ALIGN)
                .expect("SegmentBuffer layout");
            let st = self.state.lock().expect("state lock unavailable");
            let seg = st.slots[seg_idx]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken");
            let ptr = unsafe { seg.base.add(buf.offset as usize) };
            {
                let mut talc = seg.talc.lock().expect("segment talc lock unavailable");
                unsafe {
                    talc.free(std::ptr::NonNull::new_unchecked(ptr), layout);
                }
            }
            seg.dec_ref();
            seg.allocated_bytes
                .fetch_sub(buf.len as usize, std::sync::atomic::Ordering::Relaxed);
        }
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Add a new segment to the pool. Finds the first `None` slot (or appends).
    /// Each new Segment carries its own fresh talc allocator.
    ///
    /// Called from the main event-loop thread only (scaling cron or reactive expand).
    pub fn expand(&self) -> Option<u16> {
        let mut seg = Segment::new(self.segment_size);
        let idx = super::append_iovec(seg.iovec());
        seg.iovec_index = idx;

        let mut st = self.state.lock().expect("state lock unavailable");
        match st.slots.iter().position(|s| s.is_none()) {
            Some(i) => {
                st.slots[i] = Some(seg);
            }
            None => {
                st.slots.push(Some(seg));
            }
        };

        Some(idx)
    }

    /// Select the least-loaded non-draining segment as a shrink candidate.
    /// Returns `(slot_idx, allocated_bytes)` without marking the segment draining.
    pub fn find_shrink_victim(&self) -> Option<(usize, usize)> {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                opt.as_ref().and_then(|seg| {
                    if !seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                        Some((
                            i,
                            seg.allocated_bytes
                                .load(std::sync::atomic::Ordering::Relaxed),
                        ))
                    } else {
                        None
                    }
                })
            })
            .min_by_key(|&(_, bytes)| bytes)
    }

    /// Mark a specific segment as draining by slot index.
    /// After this, the picker skips this segment; existing allocations continue
    /// to be freed naturally, and once refcount==0 the segment is releasable.
    pub fn mark_segment_draining(&self, seg_idx: usize) {
        let st = self.state.lock().expect("state lock unavailable");
        if let Some(Some(seg)) = st.slots.get(seg_idx) {
            seg.draining
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Scan all segments and release any that are draining with refcount == 0.
    /// Called from the scaling cron on the main thread each tick.
    pub fn release_all_releasable(&self) {
        let releasable: Vec<usize> = {
            let st = self.state.lock().expect("state lock unavailable");
            st.slots
                .iter()
                .enumerate()
                .filter_map(|(i, opt)| opt.as_ref().filter(|seg| seg.is_releasable()).map(|_| i))
                .collect()
        };
        for idx in releasable {
            self.release_drained(idx);
        }
    }

    /// Complete the drain: pull the Segment out of its slot, clear iovec,
    /// drop it. Segment::drop deallocates the backing memory (its talc's
    /// metadata lived inside that memory and vanishes with it).
    fn release_drained(&self, seg_idx: usize) {
        let seg = {
            let mut st = self.state.lock().expect("state lock unavailable");
            let Some(seg) = st.slots[seg_idx].take() else {
                return; // already released
            };
            seg
        };
        super::clear_iovec(seg.iovec_index);
        // seg dropped here → Segment::drop → talc drops (no external state) →
        // std::alloc::dealloc frees backing memory.
    }

    // ─── Segment Helpers ─────────────────────────────────────────────────────

    /// Get absolute pointer for a SegmentBuffer.
    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        let st = self.state.lock().expect("state lock unavailable");
        unsafe {
            st.slots[buf.segment_idx as usize]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken")
                .base
                .add(buf.offset as usize)
        }
    }

    /// Return the io_uring iovec_index for the segment owning `buf`.
    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots[buf.segment_idx as usize]
            .as_ref()
            .expect("segment slot empty for live buffer")
            .iovec_index
    }

    /// Total allocated bytes across live (non-draining) segments.
    /// Sums the per-segment atomics directly — the exact figure INFO reports,
    /// with no ratio round-trip.
    pub fn allocated_bytes(&self) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .flatten()
            .filter(|seg| !seg.draining.load(std::sync::atomic::Ordering::Relaxed))
            .map(|seg| {
                seg.allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .sum()
    }

    /// Utilization ratio: allocated_bytes / total_live_capacity.
    /// 0.0 = empty, 1.0 = fully allocated.
    pub fn utilization_ratio(&self) -> f64 {
        let st = self.state.lock().expect("state lock unavailable");
        let mut live = 0usize;
        let mut allocated = 0usize;
        for seg in st.slots.iter().flatten() {
            if !seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                live += 1;
                allocated += seg
                    .allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
            }
        }
        if live == 0 {
            return 0.0;
        }
        let capacity = live * self.segment_size;
        (allocated as f64) / (capacity as f64)
    }

    /// Count of live (non-None, non-draining) segments.
    pub fn live_segment_count(&self) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .filter(|s| {
                s.as_ref()
                    .map(|seg| !seg.draining.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(false)
            })
            .count()
    }

    /// Counts of (live, draining, unused) segments.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        let st = self.state.lock().expect("state lock unavailable");
        let mut live = 0usize;
        let mut draining = 0usize;
        let mut unused = 0usize;
        for s in st.slots.iter() {
            match s {
                None => unused += 1,
                Some(seg) => {
                    if seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                        draining += 1;
                    } else {
                        live += 1;
                    }
                }
            }
        }
        (live, draining, unused)
    }

    /// Call `f` with each live segment's base pointer and size.
    /// Used by `all_segment_slices` for EFA registration.
    pub fn with_live_segment_slices<F>(&self, mut f: F)
    where
        F: FnMut(*const u8, usize),
    {
        let st = self.state.lock().expect("state lock unavailable");
        for s in st.slots.iter().flatten() {
            f(s.base, s.size);
        }
    }

    /// Returns true if any buffer in `bufs` lives in a currently-draining segment.
    pub fn is_any_buffer_draining(&self, bufs: &[SegmentBuffer]) -> bool {
        let st = self.state.lock().expect("state lock unavailable");
        bufs.iter().any(|b| {
            st.slots[b.segment_idx as usize]
                .as_ref()
                .map(|seg| seg.draining.load(std::sync::atomic::Ordering::Acquire))
                .unwrap_or(false)
        })
    }
}
