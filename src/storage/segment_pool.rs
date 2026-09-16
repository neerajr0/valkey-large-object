//! SegmentPool — shared allocator over one or more Segments.
//!
//! Both NVMePool and DRAMPool delegate allocation/free to this struct.
//!
//! ## Segment registry model (holes)
//!
//! `segments: Mutex<Vec<Option<Segment>>>` — each slot is either `Some(segment)` (live)
//! or `None` (empty/removed). This mirrors the io_uring sparse buffer table:
//! slot `i` here corresponds to slot `i` in the kernel's registered-buffer table.
//!
//! The Mutex allows expand/shrink to mutate the segment list from the main thread
//! while alloc/free read it from tokio tasks, without unsafe casts.
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
//! 4. `free()` calls `segment.dec_ref()`.
//! 5. The scaling cron on the main thread calls `release_all_releasable()` each tick,
//!    which checks `is_releasable()` and completes: talc.truncate → clear slot → dealloc.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Mutex;

use talc::{ErrOnOom, Span, Talc};

use super::context::SegmentBuffer;
use super::segment::Segment;

/// Mutable segment state held behind one Mutex.
/// Grouping both fields avoids a second lock when alloc needs the sorted index.
struct SegmentState {
    /// Segment slots. Slot `i` = iovec_index `i` in the sparse io_uring table.
    /// `None` = empty slot (hole from a previous drain).
    slots: Vec<Option<Segment>>,
    /// Sorted index for O(log N) pointer → segment lookup on the alloc path.
    /// Each entry is `(base_address, slot_index)`, kept sorted by base_address.
    /// Updated on expand (insert in sorted position) and release_drained (remove).
    /// At steady state this Vec is tiny (single digits of entries) and always hot
    /// in L1 cache.
    sorted_bases: Vec<(usize, usize)>,
}

impl SegmentState {
    /// Binary-search the sorted index to find which slot owns `addr`.
    /// Returns `(slot_index, offset_within_segment)` or None.
    fn find_segment(&self, addr: usize) -> Option<(usize, usize)> {
        // Find the last entry whose base ≤ addr.
        let pos = self.sorted_bases.partition_point(|&(base, _)| base <= addr);
        if pos == 0 {
            return None;
        }
        let (base, slot_idx) = self.sorted_bases[pos - 1];
        let seg = self.slots[slot_idx].as_ref()?;
        if addr < base + seg.size {
            Some((slot_idx, addr - base))
        } else {
            None
        }
    }

    /// Insert a new entry into sorted_bases, preserving sort order.
    fn insert_sorted(&mut self, base: usize, slot_idx: usize) {
        let pos = self.sorted_bases.partition_point(|&(b, _)| b < base);
        self.sorted_bases.insert(pos, (base, slot_idx));
    }

    /// Remove the entry for `slot_idx` from sorted_bases.
    fn remove_sorted(&mut self, slot_idx: usize) {
        self.sorted_bases.retain(|&(_, i)| i != slot_idx);
    }
}

pub struct SegmentPool {
    /// Mutable segment state: slots + sorted index for pointer lookup.
    /// Single Mutex so alloc/free hold one lock for both.
    state: Mutex<SegmentState>,
    /// Single talc heap spanning all currently-claimed segments.
    /// Uses ErrOnOom — we manage expansion ourselves via `expand()`.
    allocator: Mutex<Talc<ErrOnOom>>,
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
            let seg = Segment::new(segment_size);
            let idx = super::append_iovec(seg.iovec());
            let mut seg = seg;
            seg.iovec_index = idx;
            slots.push(Some(seg));
        }

        let mut talc = Talc::new(ErrOnOom);

        // Claim all segments and record exact Span. Build sorted_bases in parallel.
        let mut sorted_bases: Vec<(usize, usize)> = Vec::with_capacity(segment_count);
        for (slot_idx, seg_opt) in slots.iter_mut().enumerate() {
            let seg = seg_opt.as_mut().unwrap();
            let span = Span::from_base_size(seg.base, seg.size);
            let recorded = unsafe { talc.claim(span).expect("talc claim failed") };
            seg.claim_span = recorded;
            sorted_bases.push((seg.base as usize, slot_idx));
        }
        // Bases are assigned by the OS allocator — sort by address.
        sorted_bases.sort_unstable_by_key(|&(base, _)| base);

        Self {
            state: Mutex::new(SegmentState {
                slots,
                sorted_bases,
            }),
            allocator: Mutex::new(talc),
            segment_size,
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate a single buffer. Delegates to `alloc_n(size, 1, 1)`.
    /// Returns None if pool is exhausted or the selected segment is draining.
    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        self.alloc_n(size, 1, 1).map(|mut v| v.remove(0))
    }

    /// Allocate up to `count` buffers of `chunk_size` each, requiring at least
    /// `min_required`. Holds the allocator lock for the entire batch so that
    /// partial rollback is atomic against concurrent allocations.
    ///
    /// Returns `None` if fewer than `min_required` could be allocated (partial
    /// allocation freed internally). Callers never need cleanup logic.
    ///
    /// Draining check: if malloc returns a pointer in a draining segment, that
    /// buffer is freed back and the loop breaks (treated as pool exhausted for
    /// this allocation).
    pub fn alloc_n(
        &self,
        chunk_size: usize,
        count: usize,
        min_required: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        let layout = Layout::from_size_align(super::align_up(chunk_size), super::IO_ALIGN)
            .expect("alloc_n: invalid chunk_size layout");
        let mut talc = self.allocator.lock().expect("allocator lock unavailable");
        let mut buffers = Vec::with_capacity(count);
        'outer: for _ in 0..count {
            let ptr = match unsafe { talc.malloc(layout) } {
                Ok(p) => p,
                Err(_) => break,
            };
            let addr = ptr.as_ptr() as usize;
            // Binary-search lookup under state lock — O(log N), cache-hot.
            let st = self.state.lock().expect("state lock unavailable");
            let (seg_idx, offset) = st
                .find_segment(addr)
                .expect("talc returned ptr outside segments");
            let seg = st.slots[seg_idx].as_ref().unwrap();
            if seg.draining.load(std::sync::atomic::Ordering::Acquire) {
                // Segment is being evicted — free this allocation and stop.
                drop(st);
                unsafe { talc.free(ptr, layout) };
                break 'outer;
            }
            seg.inc_ref();
            seg.allocated_bytes.fetch_add(
                super::align_up(chunk_size),
                std::sync::atomic::Ordering::Relaxed,
            );
            drop(st);
            buffers.push(SegmentBuffer {
                segment_idx: seg_idx as u16,
                offset: offset as u64,
                len: chunk_size as u32,
            });
        }
        if buffers.len() < min_required {
            // Still under the same allocator lock — rollback is atomic.
            for buf in &buffers {
                self.free_with_lock(&mut talc, buf, layout);
            }
            return None;
        }
        Some(buffers)
    }

    /// Free a single buffer under an already-held allocator lock (used by alloc_n rollback).
    /// Lock order: allocator lock is held by caller. We acquire state lock briefly
    /// to dec_ref — this is safe because alloc_n also acquires state inside allocator.
    fn free_with_lock(
        &self,
        talc: &mut talc::Talc<talc::ErrOnOom>,
        buf: &SegmentBuffer,
        layout: Layout,
    ) {
        // Get the base pointer and dec_ref under state lock.
        let ptr = {
            let st = self.state.lock().expect("state lock unavailable");
            let seg = st.slots[buf.segment_idx as usize]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken");
            let ptr = unsafe { seg.base.add(buf.offset as usize) };
            seg.dec_ref();
            seg.allocated_bytes.fetch_sub(
                super::align_up(buf.len as usize),
                std::sync::atomic::Ordering::Relaxed,
            );
            ptr
        };
        unsafe {
            talc.free(std::ptr::NonNull::new_unchecked(ptr), layout);
        }
    }

    /// Free a buffer back to the pool.
    /// Decrements the segment refcount. If the segment becomes releasable
    /// (draining + refcount == 0), the scaling cron detects it on the next tick
    /// via `release_all_releasable()` and completes the release — this deferred
    /// model keeps release off the tokio hot path and on the main thread only.
    pub fn free(&self, buf: &SegmentBuffer) {
        let seg_idx = buf.segment_idx as usize;
        let aligned_size = super::align_up(buf.len as usize);
        let layout =
            Layout::from_size_align(aligned_size, super::IO_ALIGN).expect("SegmentBuffer layout");

        // Extract the raw pointer and a borrow of the Segment while holding
        // the segments lock, then release the lock before calling talc.free
        // (which acquires the allocator lock). This preserves lock order:
        // segments → allocator is never held simultaneously.
        let ptr = {
            let st = self.state.lock().expect("state lock unavailable");
            let seg = st.slots[seg_idx]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken");
            unsafe { seg.base.add(buf.offset as usize) }
            // segs MutexGuard drops here
        };

        unsafe {
            self.allocator
                .lock()
                .expect("allocator lock unavailable")
                .free(NonNull::new_unchecked(ptr), layout);
        }

        // dec_ref is an atomic operation on the Segment — no segments lock needed.
        // The Segment at seg_idx is stable: only expand/shrink touch Vec structure,
        // and they only run on the main thread, never concurrently with free().
        {
            let st = self.state.lock().expect("state lock unavailable");
            let seg = st.slots[seg_idx]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken");
            seg.dec_ref();
            seg.allocated_bytes
                .fetch_sub(aligned_size, std::sync::atomic::Ordering::Relaxed);
        }
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Add a new segment to the pool. Finds the first `None` slot (or appends).
    /// Returns the assigned iovec_index on success.
    /// Returns None if the slot table would overflow u16::MAX (io_uring limit).
    ///
    /// Called from the main event-loop thread only (scaling cron or reactive expand).
    pub fn expand(&self) -> Option<u16> {
        let seg = Segment::new(self.segment_size);
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

        // Place in the first empty slot (or append). Update sorted index.
        let mut st = self.state.lock().expect("state lock unavailable");
        let slot_idx = match st.slots.iter().position(|s| s.is_none()) {
            Some(i) => {
                st.slots[i] = Some(seg);
                i
            }
            None => {
                let i = st.slots.len();
                st.slots.push(Some(seg));
                i
            }
        };
        let base = st.slots[slot_idx].as_ref().unwrap().base as usize;
        st.insert_sorted(base, slot_idx);

        Some(idx)
    }

    /// Select the least-loaded non-draining segment as a shrink candidate.
    /// Returns `(slot_idx, allocated_bytes)` without marking the segment draining.
    /// The caller decides whether to proceed (based on mode / bytes) and calls
    /// `mark_segment_draining` only if it will commit to the drain.
    pub fn find_shrink_victim(&self) -> Option<(usize, usize)> {
        let segs = self.state.lock().expect("state lock unavailable");
        segs.slots
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

    /// Returns the slot index of the live non-draining segment with the fewest
    /// allocated bytes. Returns None if no live segments exist.
    pub fn least_loaded_segment(&self) -> Option<usize> {
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
            .map(|(i, _)| i)
    }

    /// Returns the allocated bytes for a specific segment slot.
    pub fn segment_allocated_bytes(&self, seg_idx: usize) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots[seg_idx]
            .as_ref()
            .map(|seg| {
                seg.allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .unwrap_or(0)
    }

    /// Returns the slot indices of all live (non-None, non-draining) segments.
    /// Used by DRAMPool::try_shrink to select a victim based on cached bytes.
    /// Mark a specific segment as draining by slot index.
    /// After this, no new allocations land on this segment (alloc() returns None
    /// if it gets a pointer into a draining segment).
    /// The caller is responsible for removing cached objects from the HashMap.
    ///
    /// Called from the main event-loop thread only.
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

    /// Complete the drain of a segment: truncate its address range from talc,
    /// null the sparse slot, and dealloc its memory.
    ///
    /// Only called when `is_releasable()` is true (draining == true && refcount == 0).
    /// Safe because refcount == 0 guarantees no live application allocations remain.
    fn release_drained(&self, seg_idx: usize) {
        let seg = {
            let mut st = self.state.lock().expect("state lock unavailable");
            let seg = match st.slots[seg_idx].take() {
                Some(s) => s,
                None => return, // already released
            };
            st.remove_sorted(seg_idx);
            seg
        };

        unsafe {
            let mut talc = self.allocator.lock().expect("allocator lock unavailable");
            // talc requires new_heap to contain all allocated memory (including its own
            // bookkeeping bytes). Use get_allocated_span to find the minimal valid range.
            // For a fully empty segment this is the talc metadata at the base.
            // For a segment where all app allocs are freed, this is also the metadata.
            // Truncating to the allocated span disables future allocations from this
            // segment without panicking.
            let allocated = talc.get_allocated_span(seg.claim_span);
            talc.truncate(seg.claim_span, allocated);
        }

        super::clear_iovec(seg.iovec_index);
        // seg dropped here → std::alloc::dealloc via Segment::drop.
        // used_memory decreases automatically (ValkeyAlloc tracks it).
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
    /// One lock acquisition — live count and allocated bytes read together.
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
    /// Used by INFO largeobj and all_segment_slices — avoids exposing raw segment refs.
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
    /// Used by DRAMPool::get_object to refuse Arc refs on draining segments.
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
