# DRAM Cache: Arena Allocator Design (Approach B Deep-Dive)

**Date:** 2026-08-21  **Status:** Draft  **Author:** karsubba  
**Forked from:** DRAM_CACHE_DESIGN.md (Approach B exploration)

---

## 1. Context

From the parent design doc, Approach B uses a pre-registered contiguous arena with a slab allocator for exact-fit allocation. This doc explores the full design: allocator choice, multi-segment architecture, dynamic resizing, and defragmentation.

**Why Approach B over Approach A:**
- 500x object size range (15KB–8MB) → fixed classes waste up to 50% DRAM per object
- DRAM-only mode: memory efficiency is the only metric that matters
- Industry precedent: Mooncake, DOCA, SPDK all use pre-registered arenas

**What Approach B gives up vs Approach A:**
- No io_uring `ReadFixed` (arena too large for fixed-buffer registration) → plain read/write for NVMe I/O
- ~20% NVMe throughput loss (measured: 156K rps with ReadFixed vs ~130K without)
- Non-deterministic alloc latency (free-list search vs O(1) pop)

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ DramArena (one talc allocator instance, multiple segments)      │
│                                                                 │
│  Segment 0 (16GB)         Segment 1 (16GB)        Segment N    │
│  ┌─────────────────┐     ┌─────────────────┐     ┌────────┐   │
│  │ mmap'd          │     │ mmap'd          │     │        │   │
│  │ fi_mr_reg'd     │     │ fi_mr_reg'd     │     │  ...   │   │
│  │ rkey_0          │     │ rkey_1          │     │        │   │
│  │                 │     │                 │     │        │   │
│  │ [obj][obj][ free ]     │ [obj][ free ][obj]│     │        │   │
│  └─────────────────┘     └─────────────────┘     └────────┘   │
│                                                                 │
│  talc sees all segments as one logical address space            │
│  (multiple Spans, one allocator)                                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────┐
│ IoPool (separate)   │
│ io_uring registered │
│ EFA registered      │
│ fixed-size, small   │
│ transient only      │
└─────────────────────┘
```

**Two allocators coexist:**
1. `ValkeyAlloc` (zmalloc) — global allocator for all Rust heap allocations (HashMap, Vec metadata, etc.)
2. `talc` — private allocator for DRAM cache object data only, operating within pre-registered mmap segments

---

## 3. Allocator: talc

### Why talc

| Requirement | talc support |
|---|---|
| Per-object free | Yes (general-purpose malloc/free) |
| Variable sizes | Yes (any layout) |
| Pre-allocated region | Yes (takes `&mut [u8]` span) |
| Multiple spans | Yes (`Talc::claim()` adds spans) |
| Realloc | Yes (grows in-place if possible, else alloc+copy+free) |
| Thread safety | Yes (`Talck` = talc + lock) |
| Coalescing | Yes (adjacent free blocks merge automatically) |
| GlobalAlloc trait | Yes (but we use it as a private allocator, not global) |

### What talc handles (we don't write this):
- Free-list management across all spans
- Coalescing adjacent free blocks on free()
- Splitting blocks on alloc
- Best-fit search within spans
- Internal fragmentation minimization

### What we manage (talc doesn't do this):
- Segment lifecycle (mmap / munmap / fi_mr_reg / fi_mr_dereg)
- rkey lookup (which segment owns a pointer)
- Grow/shrink policy (when to add/remove segments)
- Drain + evacuation (moving objects before segment removal)
- Defragmentation strategy (segment-level rotation)

---

## 4. Object Storage Model

```rust
// Object metadata stored in main HashMap (via ValkeyAlloc)
struct CachedObject {
    segment_idx: u16,       // which segment this object lives in
    offset: u32,            // offset from segment base (supports up to 4GB per segment)
    len: u32,               // actual object length
}

// Global lookup
HashMap<ObjectId, CachedObject>   // allocated via ValkeyAlloc (normal heap)
```

**Why offsets, not raw pointers:**
- Survives segment base relocation (if ever needed)
- Smaller (u16 + u32 = 6 bytes vs 8 bytes for pointer)
- Safer (can't dereference stale pointer)

**Dereferencing:**
```rust
fn get_ptr(&self, obj: &CachedObject) -> *const u8 {
    let seg = &self.segments[obj.segment_idx as usize];
    unsafe { seg.base.add(obj.offset as usize) }
}

fn get_rkey(&self, obj: &CachedObject) -> u64 {
    self.segments[obj.segment_idx as usize].rkey
}
```

---

## 5. Dynamic Resizing

### 5.1 Growing (adding segments)

**Trigger:** Allocation fails (arena full) or proactive policy (utilization > 80%).

```
1. mmap(NULL, segment_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0)
2. fi_mr_reg(new_ptr, segment_size) → new rkey
3. talc.claim(Span::new(new_ptr, new_ptr + segment_size))
4. Add Segment to segments list
```

**Cost:** ~2-5ms (fi_mr_reg dominates). No disruption to existing I/O.

### 5.2 Shrinking (removing segments)

**Trigger:** Memory pressure from Valkey (used_memory approaching maxmemory) or proactive policy (segment utilization < 20%).

**Constraint:** Cannot deregister a segment while in-flight I/O or EFA ops reference buffers within it.

**Steps:**

```
1. Pick segment with lowest utilization (fewest live objects)
2. Mark segment as DRAINING:
   - talc stops allocating from this span (remove from allocator? or just bias away)
   - New allocs go to other segments
3. Wait for in-flight I/O targeting this segment to complete (~2-5ms)
4. Evacuate live objects:
   - For each live object in segment:
     a. Allocate new slot in another segment (via talc)
     b. memcpy(new_slot, old_slot, len)
     c. Update HashMap entry (new segment_idx, new offset)
     d. talc.free(old_slot)
5. fi_mr_dereg(segment.mr)
6. munmap(segment.base, segment.size)
7. Remove Segment from list
```

**Evacuation cost:** Proportional to live data in segment.
- Segment 30% full with 16GB size = ~5GB to copy = ~500ms at 10 GB/s memory bandwidth.
- Segment 5% full = ~800MB = ~80ms.

**Mitigation:** Only shrink segments below 20% utilization. Or: mark draining and let natural evictions empty it (zero-copy but slower — depends on workload).

### 5.3 DRAM-only mode shrinking constraint

In DRAM-only mode, objects exist ONLY in the arena. There is no NVMe copy.

- **Can shrink:** Free space within segments (uncommit pages via `madvise(MADV_DONTNEED)`)
- **Cannot shrink by eviction:** Evicting = deleting user data. Only Valkey's maxmemory eviction policy can trigger key deletion.
- **Can shrink by segment removal:** Only if objects are evacuated to other segments first (no data loss).

Flow under memory pressure (DRAM-only):
```
1. Valkey detects used_memory > maxmemory
2. Valkey evicts LO keys (fires our free callback)
3. free() returns object's arena slot to talc
4. If a segment drops below threshold → shrink it (steps above)
5. Physical memory released via munmap
```

### 5.4 IoPool resizing (DRAM+NVMe mode)

IoPool has two tiers:

```
IoPool
├─ Pinned tier: fixed count, io_uring + EFA registered, NEVER shrunk
│   (deregister too expensive, stalls all I/O)
└─ Elastic tier: heap-allocated, unregistered, expandable/shrinkable
    - Uses plain io_uring read/write (not ReadFixed)
    - EFA: must memcpy to pinned tier first
    - Allocated on demand when pinned tier exhausted
    - Freed under memory pressure
```

---

## 6. Defragmentation

### The Problem

After many alloc/free cycles with varied sizes, the arena develops external fragmentation: free space exists but is scattered in small non-contiguous blocks. A 4MB allocation may fail even though 20MB is free (but in 100 scattered 200KB blocks).

### What talc does automatically

- **Coalescing:** When you `free()` a block, talc merges it with adjacent free blocks. Two adjacent 200KB frees → one 400KB free block. This is the primary defrag mechanism and it's free.

### What talc cannot do

- **Compaction:** Moving live objects to consolidate free space. Impossible without invalidating all external pointers/offsets to those objects. talc cannot do this.

### Our defragmentation strategy: Segment-level rotation

Instead of compacting within a segment, we rotate at the segment level:

```
Fragmented segment (30% utilized, 70% free but scattered):
┌─────────────────────────────────────────────────┐
│ [obj][ free ][obj][ free ][ free ][obj][ free ] │  ← can't allocate 4MB contiguously
└─────────────────────────────────────────────────┘

Strategy: evacuate → deregister → replace with fresh segment
```

**Steps:**
1. **Monitor:** Track per-segment metrics:
   - `live_bytes / segment_size` (utilization)
   - `largest_free_block / segment_size` (fragmentation indicator)
   - If utilization is low BUT largest free block is small → fragmented
2. **Mark draining:** Stop new allocations from this segment
3. **Evacuate:** Move live objects to other (healthier) segments
4. **Reclaim:** Deregister + munmap the fragmented segment
5. **Replace:** mmap + register a fresh segment (100% contiguous free space)

**When to trigger:**
- Segment utilization < 30% AND largest free block < 50% of free space
- Or: allocation failed despite total free bytes being sufficient (fragmentation-caused OOM)

### Alternative: Generational drain (passive defrag)

Instead of active evacuation, simply stop allocating from the fragmented segment and wait for natural evictions/deletions to empty it:

```
1. Mark segment as DRAINING (no new allocs)
2. Existing objects remain accessible (reads/EFA serve from it normally)
3. As objects are evicted or DELeted, free() returns space
4. When segment reaches 0% utilization → deregister + munmap
5. Add fresh segment if capacity needed
```

**Advantage:** Zero memcpy cost. No disruption.
**Disadvantage:** Slow — depends on access patterns. A segment with cold but never-evicted objects may stay in draining state indefinitely.

**Hybrid approach:** Drain passively for N minutes. If still > 10% utilized after timeout, actively evacuate the remaining objects.

### Defrag cost summary

| Strategy | Copy cost | Disruption | Speed |
|----------|----------|------------|-------|
| Coalescing (talc auto) | 0 | 0 | Instant (on every free) |
| Passive drain | 0 | 0 | Slow (minutes to hours) |
| Active evacuation | memcpy all live objects | Per-object brief lock | Fast (~500ms for 5GB) |
| Hybrid (passive + timeout) | Partial memcpy | Minimal | Balanced |

---

## 7. Data Flows

### DRAM-only mode

```
LO.SET key len <payload>:
  1. slot = arena.alloc(len, align=4096)
  2. Copy payload into slot
  3. Store CachedObject{segment_idx, offset, len} in HashMap

LO.GET key [rkey remote_addr len]:
  4. Lookup CachedObject in HashMap
  5a. TCP: reply directly from arena slot (zero-copy to TCP buffer)
  5b. EFA: fi_write(arena_ptr, len, segment.rkey, ...) → zero-copy to client

DEL key:
  6. arena.free(slot, layout)
  7. Remove from HashMap
```

### DRAM+NVMe mode

```
LO.SET key len <payload>:
  1. Borrow IoPool buffer
  2. Copy payload into IoPool buffer
  3. io_uring write to NVMe (from IoPool buffer)
  4. Return IoPool buffer
  5. Optionally: arena.alloc + memcpy to cache (write-through)

LO.GET key (DRAM hit):
  6. Lookup CachedObject → found in arena
  7a. TCP: reply from arena slot
  7b. EFA: fi_write from arena slot (zero-copy)

LO.GET key (DRAM miss):
  8. Borrow IoPool buffer
  9. io_uring read from NVMe into IoPool buffer
  10. Reply (TCP or memcpy to IoPool then fi_write for EFA)
  11. Promote: arena.alloc(len) + memcpy from IoPool buffer to arena
  12. Return IoPool buffer
  13. Store CachedObject in HashMap
```

---

## 8. Comparison with Approach A (Fixed Classes)

| Aspect | Approach A (fixed classes) | Approach B (talc arena) |
|--------|:---:|:---:|
| DRAM waste | up to 50% per object | <5% (alignment padding only) |
| io_uring ReadFixed | yes | no (plain read/write) |
| NVMe throughput | ~156K rps (ReadFixed) | ~130K rps (plain read) |
| EFA zero-copy on hit | yes | yes |
| Promotion zero-copy | yes (keep buffer) | no (memcpy IoPool → arena) |
| Alloc speed | O(1) guaranteed | O(1) amortized, O(n) worst case |
| Fragmentation | impossible | possible (mitigated by segment rotation) |
| Defrag mechanism | N/A | segment drain + evacuation |
| Dynamic resize | add/remove buffers (trivial) | add/remove segments (drain required) |
| Code complexity | ~100 lines | ~315 lines |
| Best for | DRAM+NVMe (NVMe perf matters) | DRAM-only (memory efficiency matters) |

---

## 9. Open Questions

1. **talc multi-span behavior:** Does `talc.claim()` actually allow new spans after initial creation? Need to verify the crate's API.
2. **Segment size:** 16GB? 4GB? Smaller = more granular shrink, more segments to track, more rkeys. Larger = fewer segments but coarser shrink.
3. **EFA registration of overcommitted pages:** Does `fi_mr_reg` on a large mmap'd region pin ALL pages immediately? If yes, Option 3 (virtual overcommit) doesn't save physical memory. Must test on i8ge.
4. **Allocation bias away from draining segments:** talc may not natively support "don't allocate from span X." May need to wrap with our own segment preference logic.
5. **Per-object locking for evacuation:** During active evacuation, we memcpy + update HashMap. If a concurrent read hits the same object mid-move, it could read stale data. Need per-object or per-segment read lock during evacuation.
6. **io_uring alternative for arena:** Could we register sub-regions of the arena with io_uring (e.g., register the first 1000 × 8MB offsets as a virtual buffer table)? This would recover ReadFixed performance. Investigate `IORING_REGISTER_BUFFERS` with arena sub-slices.
