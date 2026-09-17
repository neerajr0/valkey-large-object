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
//! Allocations pick a segment via `pick_alloc_target`, then lock only that
//! segment's talc. Per-segment locking means concurrent allocs on different
//! segments never contend.
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

    /// Allocate up to `count` buffers of `chunk_size` each, requiring at least
    /// `min_required`. Each chunk picks a segment independently via the picker,
    /// then locks only that segment's talc for its own malloc.
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
            match self.try_alloc_one(chunk_size, aligned_size, layout) {
                Some(buf) => buffers.push(buf),
                None => break,
            }
        }

        if buffers.len() < min_required {
            // Rollback — free each partial buffer via its owning segment's talc.
            for buf in &buffers {
                self.free(buf);
            }
            return None;
        }
        Some(buffers)
    }

    /// Pick a target segment and allocate one chunk from it.
    /// Picker: least-loaded non-draining segment that has room.
    /// Returns None only when no segment can serve.
    fn try_alloc_one(
        &self,
        chunk_size: usize,
        aligned_size: usize,
        layout: Layout,
    ) -> Option<SegmentBuffer> {
        // Try up to 8 candidate segments (in case one loses to a race and its
        // talc.malloc fails despite the picker's optimistic capacity read).
        for _attempt in 0..8 {
            let seg_idx = self.pick_alloc_target(aligned_size)?;

            // Extract pointer we need to compute offset; do allocation under
            // the segment's own talc mutex.
            let st = self.state.lock().expect("state lock unavailable");
            let Some(seg) = st.slots.get(seg_idx).and_then(|s| s.as_ref()) else {
                // Segment was removed between picker and here; retry.
                drop(st);
                continue;
            };
            // Re-check draining under lock — the picker's atomic read may have
            // been stale.
            if seg.draining.load(std::sync::atomic::Ordering::Acquire) {
                drop(st);
                continue;
            }
            let seg_base = seg.base;
            let mut talc = seg.talc.lock().expect("segment talc lock unavailable");
            let ptr = match unsafe { talc.malloc(layout) } {
                Ok(p) => p,
                Err(_) => {
                    // This segment's talc rejected the alloc (fragmentation or
                    // near-full). Mark it saturated so picker skips it next time.
                    // Not a persistent poison — a future free() decrements
                    // allocated_bytes and it becomes eligible again.
                    seg.allocated_bytes
                        .fetch_max(self.segment_size, std::sync::atomic::Ordering::Relaxed);
                    drop(talc);
                    drop(st);
                    continue;
                }
            };
            let offset = ptr.as_ptr() as usize - seg_base as usize;
            seg.inc_ref();
            seg.allocated_bytes
                .fetch_add(aligned_size, std::sync::atomic::Ordering::Relaxed);
            drop(talc);
            drop(st);
            let _ = chunk_size; // used only via alignment above
            return Some(SegmentBuffer {
                segment_idx: seg_idx as u16,
                offset: offset as u64,
                len: chunk_size as u32,
            });
        }
        None
    }

    /// Picker: return the slot index of a segment that likely has room for
    /// `aligned_size` bytes. Strategy: LEAST-loaded non-draining segment first
    /// (spread allocations so no segment saturates prematurely, giving the
    /// shrink path more room). Returns None if no segment has capacity.
    fn pick_alloc_target(&self, aligned_size: usize) -> Option<usize> {
        let st = self.state.lock().expect("state lock unavailable");
        let mut best: Option<(usize, usize)> = None; // (allocated, idx)
        for (i, opt) in st.slots.iter().enumerate() {
            let Some(seg) = opt else { continue };
            if seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                continue;
            }
            let cur = seg
                .allocated_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            if cur + aligned_size > seg.size {
                continue;
            }
            match best {
                None => best = Some((cur, i)),
                Some((b, _)) if cur < b => best = Some((cur, i)),
                _ => {}
            }
        }
        best.map(|(_, i)| i)
    }

    /// Free a buffer back to its owning segment.
    /// Uses `buf.segment_idx` directly — no reverse lookup needed.
    pub fn free(&self, buf: &SegmentBuffer) {
        let seg_idx = buf.segment_idx as usize;
        let aligned_size = super::align_up(buf.len as usize);
        let layout =
            Layout::from_size_align(aligned_size, super::IO_ALIGN).expect("SegmentBuffer layout");

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
            .fetch_sub(aligned_size, std::sync::atomic::Ordering::Relaxed);
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
