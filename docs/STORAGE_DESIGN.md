# Storage Design

**Date:** 2026-08-21  **Status:** Draft  **Author:** karsubba

---

## 1. Problem

The module stores large objects (15KB–8MB) and must serve them via two transports:
- **TCP:** standard RESP reply
- **EFA:** RDMA fi_write directly to client GPU memory

Two operating modes:
- **DRAM-only:** All objects live in DRAM. No NVMe. Fastest reads. Limited by DRAM capacity.
- **DRAM + NVMe:** All objects persist on NVMe. DRAM is a read cache (hot objects promoted on access). Larger capacity, slightly higher latency on cold reads.

---

## 2. Hard Constraints

These two constraints drive every design decision:

**1. EFA registration (fi_mr_reg):**
Any buffer used as source for `fi_write` must be pre-registered with the NIC. Registration pins physical pages and programs the NIC's translation table. Cost: ~1-5ms per call. Must be done at startup or on rare resize events — never on the data path.

**2. io_uring registration (IORING_REGISTER_BUFFERS):**
Any buffer used for `ReadFixed`/`WriteFixed` must be pre-registered with the kernel's io_uring ring. Enables kernel-bypass I/O (no per-op address translation). Cost: one-time at startup. Kernel overhead scales with buffer count — keep ≤1000 per registration.

**Consequence:** A buffer registered with both can serve NVMe I/O AND EFA transfers without copying. An unregistered buffer can only serve TCP replies.

### io_uring ReadFixed/WriteFixed Mechanics

`IORING_REGISTER_BUFFERS` takes an array of `iovec` structs. Each entry is one "buffer" from io_uring's perspective. `ReadFixed`/`WriteFixed` operations reference a buffer by its array index (`buf_index`) plus an offset and length within it.

**Critical insight:** Each iovec entry can be arbitrarily large. A 16GB segment is a valid single entry. You then use `buf_index=N` to select the segment and `offset` to target a specific location within it.

```
Registration:  [ iovec{segment0, 16GB}, iovec{segment1, 16GB}, iovec{segment2, 16GB} ]
                  buf_index=0              buf_index=1            buf_index=2

ReadFixed:  buf_index=1, offset=0x5000, len=4MB
            → reads 4MB from NVMe into segment1 at byte offset 0x5000
```

**Performance constraint:** The kernel's buffer lookup degrades with entry count. Measured on i8ge: 128 entries → 156K rps, 10000 entries → 52K rps. With segments (2-8 entries), this is not a concern.

**No alternative API:** There is no way to use `ReadFixed`/`WriteFixed` without `IORING_REGISTER_BUFFERS`. The registration is what gives the kernel pre-pinned page tables to avoid per-I/O `get_user_pages()`. Plain `read`/`write` ops work without registration but pay the page-pinning cost every time (~15-20% throughput loss).

**Resize implication:** Adding or removing a segment requires `IORING_UNREGISTER_BUFFERS` (drops all registrations) then `IORING_REGISTER_BUFFERS` with the new array. During this window (~μs), no `ReadFixed`/`WriteFixed` can be issued. In-flight ops already submitted are unaffected (kernel has their pages pinned). New submissions must wait or fall back to plain read/write.

---

## 3. Object Size Distribution

Modern hybrid-attention models produce diverse object types per prefix:

| Object type | Size per chunk | Growth |
|---|---|---|
| HCA compressed blocks (128x) | 15–50 KB | Linear with sequence, tiny |
| FP4 indexer state | 10–50 KB | Small metadata |
| CSA compressed KV (4x) | 0.5–1 MB | Linear ÷ 4 |
| Sliding window KV | 0.5–2 MB | Fixed (window size) |
| KDA recurrent state | 2–8 MB | Fixed (model dims) |
| Full-attention KV blocks | 0.5–4 MB | Linear with sequence |

**Range: 15KB to 8MB per stored object. 500x variance.**

The client (LMCache) stores each chunk as a separate key. Our module sees individual opaque blobs at varying sizes.

---

## 4. Storage Approaches

Two ways to handle the size variance. This is the fundamental design choice.

### 4.1 Approach A: Fixed-Size Buffer Classes

Pre-allocate buffers in 2-3 size classes. Each object goes in the smallest class that fits. Unused space in the buffer is DRAM padding (wasted memory, but not wasted on NVMe or EFA — those use exact `len`).

```
Class 1: 64KB buffers × 2000   (serves 15-64KB objects)
Class 2: 1MB buffers  × 500    (serves 65KB-1MB objects)
Class 3: 8MB buffers  × 250    (serves 1-8MB objects)
```

**How it works:**
- Each class is a Vec of pre-allocated, 4KB-aligned buffers
- All buffers registered with io_uring (`IORING_REGISTER_BUFFERS`) and EFA (`fi_mr_reg`) at startup
- Alloc = pop buffer index from class free list. O(1).
- Free = push buffer index back to free list. O(1).
- No fragmentation possible (fixed slots, never split or merged)

**Registration:**
- io_uring: Yes (each buffer is a registered fixed buffer — enables `ReadFixed`/`WriteFixed`)
- EFA: Yes (each buffer is within a registered region)

**Pros:**
- Zero-copy on ALL paths (NVMe read → keep buffer as cache → fi_write from same buffer)
- O(1) deterministic alloc/free
- No fragmentation, ever
- io_uring `ReadFixed` (fastest NVMe path — measured 156K rps vs 130K without)
- Simple implementation (~100 lines)

**Cons:**
- Up to 50% DRAM waste per object (500KB object in 1MB buffer = 500KB wasted)
- Fixed capacity per class decided at startup
- Cannot handle objects larger than largest class (reject with ERR)

### 4.2 Approach B: Arena with Slab Allocator (talc)

Allocate one or more large contiguous memory segments at startup. Register each segment with EFA. Sub-allocate exact-sized slots from the segments using a general-purpose allocator (talc).

```
┌────────────────────────────────────────────────────────────┐
│ Segment 0 (16GB, mmap'd, fi_mr_reg'd, rkey_0)            │
│ ┌──────┐┌─────────┐┌──┐┌─────────────┐┌──────┐ ...      │
│ │ 47KB ││  820KB  ││4K││    6.2MB    ││ 91KB │          │
│ └──────┘└─────────┘└──┘└─────────────┘└──────┘          │
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│ Segment 1 (16GB, mmap'd, fi_mr_reg'd, rkey_1)            │
│ ...                                                        │
└────────────────────────────────────────────────────────────┘
```

**How it works:**
- One `talc` allocator instance manages multiple segments (each added via `talc.claim(span)`)
- Alloc = `talc.malloc(Layout::from_size_align(len, 4096))`. Finds contiguous free block.
- Free = `talc.free(ptr, layout)`. Returns space, coalesces with adjacent free blocks.
- Realloc = `talc.realloc()`. Grows in-place if possible, else alloc+copy+free.
- Objects tracked as `(segment_idx, offset, len)` in HashMap — offsets, not raw pointers.

**Registration:**
- io_uring: Yes — register each segment as one large buffer (`buf_index = segment_idx`, use offset within it for each I/O). Enables `ReadFixed`/`WriteFixed`.
- EFA: Yes (entire segment registered as one region)

**Pros:**
- Near-zero DRAM waste (allocate exact bytes + 4KB alignment overhead)
- Handles any size up to segment capacity without class boundaries
- io_uring `ReadFixed`/`WriteFixed` via segment-as-buffer (same NVMe throughput as Approach A)
- EFA zero-copy on DRAM hit (fi_write from any offset within registered segment)
- NVMe read can land directly in arena slot (ReadFixed with offset) — zero-copy promotion possible
- Industry precedent (Mooncake, NVIDIA DOCA, SPDK)

**Cons:**
- Fragmentation possible after many alloc/free cycles with varied sizes
- Alloc is O(1) amortized but O(n) worst case on fragmented arena
- More complex implementation (~315 lines)
- Shrinking requires drain + evacuation (cannot simply return a buffer)
- Adding/removing segments requires `IORING_UNREGISTER_BUFFERS` + re-register (brief I/O pause)

### 4.3 Fragmentation in Approach B

**What causes it:** Interleaved alloc/free of varied sizes. Example: allocate [1MB][64KB][1MB][64KB], then free the 1MB blocks → two 1MB holes separated by 64KB live objects. Cannot allocate 2MB contiguously despite 2MB total free.

**What talc does automatically:**
- Coalescing: when you `free()` a block, talc merges it with adjacent free blocks. This is the primary repair mechanism and it's free.

**What talc cannot do:**
- Compaction (moving live objects to consolidate free space). Would invalidate all pointers/offsets.

**How we fix it — segment-level rotation:**
1. Monitor per-segment: `largest_free_block / total_free_bytes`. If ratio < 50% → fragmented.
2. Mark segment as DRAINING (no new allocs from it).
3. Passively wait for evictions/deletions to empty it, OR actively evacuate remaining objects to other segments (memcpy + update HashMap offsets).
4. Once empty: `fi_mr_dereg` + `munmap`. Replace with fresh segment.

**Mitigation — size-banded segments:**
Route small objects (≤1MB) to "small segments" and large objects (>1MB) to "large segments." Similar sizes in the same segment minimize fragmentation (same pattern jemalloc uses internally with arenas).

### 4.4 Comparison

| | Approach A (fixed classes) | Approach B (talc arena) |
|---|:---:|:---:|
| DRAM efficiency | ≤50% waste per class | ~95%+ efficient |
| io_uring ReadFixed | Yes | Yes (segment-as-buffer + offset) |
| NVMe read throughput | ~156K rps | ~156K rps (same — ReadFixed works) |
| EFA zero-copy on hit | Yes | Yes |
| Promotion zero-copy (NVMe → cache) | Yes (keep buffer) | Yes (ReadFixed into arena slot directly) |
| Alloc speed | O(1) guaranteed | O(1) amortized, O(n) worst |
| Fragmentation | Impossible | Possible (segment rotation fixes) |
| Defrag mechanism | N/A | Segment drain + evacuation |
| Code complexity | ~100 lines | ~315 lines |
| Best for | Simplicity, predictable latency | Memory efficiency, varied object sizes |

### 4.5 Common Behaviors (both approaches)

**NVMe write alignment:**
O_DIRECT requires write length to be a multiple of 512 bytes (sector size). Objects not naturally aligned are padded on disk: `ceil(len / 512) * 512`. Up to 511 bytes waste on disk for small odd-sized objects. Reads return only `len` bytes (stored in LoValue metadata). EFA `fi_write` has no alignment constraint — sends exact `len`.

**Max object size enforcement:**
Objects exceeding the largest supported size are rejected at `LO.SET` with `ERR object exceeds max buffer size`. No multi-buffer stitching, no fallback path. Client (LMCache) already chunks by layer/block and can chunk smaller. Module advertises max size via config.

---

## 5. Operating Modes

### 5.1 DRAM-Only Mode

All objects live exclusively in DRAM. No NVMe storage. Fastest possible reads. Capacity limited by available DRAM.

**With Approach A:**
```
LO.SET key len <payload>:
  1. Pop buffer from smallest fitting class free list
  2. Copy payload into buffer
  3. Store (class_idx, buf_idx, len) in HashMap

LO.GET key [rkey remote_addr len]:
  4. Lookup in HashMap → buffer pointer
  5a. TCP: reply from buffer
  5b. EFA: fi_write from buffer (zero-copy, buffer is registered)

DEL key:
  6. Push buffer back to class free list
  7. Remove from HashMap
```

**With Approach B:**
```
LO.SET key len <payload>:
  1. arena.alloc(len, align=4096) → (segment_idx, offset)
  2. Copy payload into arena slot
  3. Store CachedObject{segment_idx, offset, len} in HashMap

LO.GET key [rkey remote_addr len]:
  4. Lookup in HashMap → derive pointer from segment base + offset
  5a. TCP: reply from arena slot
  5b. EFA: fi_write(arena_ptr, len, segment.rkey) — zero-copy

DEL key:
  6. arena.free(ptr, layout)
  7. Remove from HashMap
```

**Key property:** Data exists ONLY in DRAM. Eviction = data loss = equivalent to DEL. Only Valkey's maxmemory eviction policy triggers this.

### 5.2 DRAM + NVMe Mode

All objects persist on NVMe (write-through). DRAM is a read cache — hot objects promoted on access.

**With Approach A:**
```
LO.SET key len <payload>:
  1. Pop buffer from class free list (this is IoPool — registered with io_uring + EFA)
  2. Copy payload into buffer
  3. io_uring WriteFixed to NVMe
  4. Decision: keep buffer as cache (promotion) OR return to free list
     - If cache: buffer stays held, tracked in HashMap as cached
     - If not: return buffer to free list after write completes

LO.GET key (DRAM hit):
  5. Lookup in HashMap → buffer is cached
  6a. TCP: reply from buffer
  6b. EFA: fi_write from buffer (zero-copy)

LO.GET key (DRAM miss):
  7. Pop buffer from class free list
  8. io_uring ReadFixed from NVMe into buffer
  9. Reply (TCP direct or EFA fi_write — zero-copy either way)
  10. Keep buffer as cache (promotion). Store in HashMap.

Eviction (under memory pressure):
  11. Pick victim (LRU/LFU)
  12. Push buffer back to free list. Data safe on NVMe.
  13. Remove from HashMap cache entry (keep NVMe reference)
```

**With Approach B:**
```
LO.SET key len <payload>:
  1. arena.alloc(len, align=4096) → (segment_idx, offset)
  2. Copy payload into arena slot
  3. io_uring WriteFixed to NVMe (buf_index=segment_idx, offset=obj_offset)
  4. Object lives in arena (cached) AND on NVMe (persisted)

LO.GET key (DRAM hit — in arena):
  6. Lookup in HashMap → CachedObject in arena
  7a. TCP: reply from arena slot
  7b. EFA: fi_write from arena slot (zero-copy, segment is EFA-registered)

LO.GET key (DRAM miss):
  8. arena.alloc(len) → new slot in arena
  9. io_uring ReadFixed from NVMe directly into arena slot (buf_index=segment_idx, offset=slot_offset)
  10. Reply (TCP from arena slot, or fi_write from arena slot for EFA) — zero-copy
  11. Object is now cached in arena. Store in HashMap.

Eviction:
  12. arena.free(slot). Data safe on NVMe.
  13. Remove cache entry from HashMap.
```

**Key property:** DRAM cache is expendable. Eviction is cheap (data persists on NVMe). Cache miss costs one NVMe read (~15μs).

---

## 6. Expanding and Shrinking

### 6.1 When to Expand

| Trigger | Action |
|---------|--------|
| Allocation fails (pool/arena full) | Add capacity immediately |
| Utilization > 80% sustained | Add capacity proactively |
| New LO keys being SET faster than evictions | Add capacity to reduce eviction rate |

### 6.2 When to Shrink

| Trigger | Action |
|---------|--------|
| Valkey `used_memory` approaching `maxmemory` | Shrink to give memory back |
| Module's DRAM usage disproportionately high vs Valkey's other data | Shrink |
| Sustained low utilization (<30% for >5 minutes) | Shrink to reduce waste |

### 6.3 How Expansion Works

**Approach A:**
- Allocate new buffers from ValkeyAlloc (zmalloc)
- New buffers are **unregistered** (elastic tier — cannot use ReadFixed or fi_write directly)
- Serve NVMe I/O via plain io_uring read/write (slower but functional)
- For EFA: memcpy from elastic buffer to a registered (pinned tier) buffer, then fi_write
- Cost: one allocation per buffer. No disruption to existing operations.

**Approach B:**
- `mmap` a new segment (e.g., 16GB)
- `fi_mr_reg(new_segment)` → new rkey (~2-5ms)
- `talc.claim(Span::new(base, base + size))` — adds segment to allocator
- New allocations can immediately use the new segment
- Cost: ~5ms for registration. No disruption.

### 6.4 How Shrinking Works

**Approach A:**

```
DRAM+NVMe mode:
  1. Evict cached objects (LFU/LRU) — data safe on NVMe
  2. Return freed buffers to ValkeyAlloc
  3. Pinned tier (registered) is NEVER shrunk — deregister is too expensive and stalls I/O
  4. Elastic tier (unregistered) buffers freed immediately

DRAM-only mode:
  1. Can only free UNUSED buffers (not holding live objects)
  2. Live objects ARE the data — freeing them = data loss
  3. Valkey's maxmemory eviction must delete LO keys first
  4. After key deletion → buffer returns to free list → can be freed to ValkeyAlloc
```

**Approach B:**

```
DRAM+NVMe mode:
  1. Pick segment with lowest utilization
  2. Mark segment DRAINING (no new allocations from it)
  3. Wait for in-flight I/O targeting this segment to complete (~2-5ms)
  4. Evacuate remaining live objects:
     - For each live object: alloc in another segment, memcpy, update HashMap
  5. fi_mr_dereg(segment) — releases NIC resources
  6. munmap(segment) — releases physical memory to OS
  Evacuation cost: proportional to live data. 5% utilized 16GB segment = ~800MB copy = ~80ms.

DRAM-only mode:
  1. Same as above, but evacuation is MANDATORY (cannot evict — data only exists here)
  2. Shrinking only reduces total segment count, never destroys data
  3. Must have enough capacity in remaining segments to hold evacuated objects
  4. If remaining segments too full to absorb: cannot shrink (reject the shrink request)
```

### 6.5 IoPool Resizing (DRAM+NVMe, both approaches)

The IoPool has two tiers:

```
IoPool
├─ Pinned tier (registered at startup, io_uring + EFA)
│   - Fixed count, NEVER shrunk
│   - Enables ReadFixed / fi_write source
│   - Sized for steady-state concurrent I/O (e.g., 128 buffers)
│
└─ Elastic tier (unregistered, heap-allocated via ValkeyAlloc)
    - Grows on demand when pinned tier exhausted
    - Shrinks under memory pressure (freed back to ValkeyAlloc)
    - Uses plain io_uring read/write (not ReadFixed — works but no kernel shortcut)
    - EFA: must memcpy to pinned tier buffer first, then fi_write
```

**Why two tiers:** The pinned tier gives maximum I/O performance. The elastic tier handles bursts without rejecting requests. Under sustained pressure, elastic buffers are freed first (cheapest to reclaim — no deregistration needed).

---

## 7. Recommendation

| Mode | Recommended approach | Why |
|------|---------------------|-----|
| **DRAM-only** | Approach B (arena) | Memory efficiency is the only metric. No NVMe, so ReadFixed doesn't matter. 50% waste from Approach A is unacceptable when DRAM IS the storage. |
| **DRAM+NVMe, EFA-heavy** | Approach A (fixed classes) | Zero-copy on every path. ReadFixed for NVMe. Fragmentation-free. EFA fi_write from same buffer that did the NVMe read. |
| **DRAM+NVMe, TCP-heavy** | Either (Approach A simpler) | TCP replies don't need registration. Approach A still wins on NVMe read throughput (ReadFixed). |
| **Hybrid deployment** | Approach A for IoPool + Approach B for DRAM cache | Best of both: ReadFixed NVMe I/O + memory-efficient DRAM cache. One memcpy on promotion (IoPool → arena). |

**Migration path:** Start with Approach A (simpler, zero-copy everywhere, works today). Move to Approach B for DRAM-only mode when memory efficiency becomes critical. The command interface is identical — the change is internal to the storage layer.

---

## 8. Open Questions

1. What size classes for Approach A? Need LMCache team input on their chunk sizes.
2. For Approach A: what free:cached ratio is safe? (reserve 20% for I/O, allow 80% for caching?)
3. For Approach B: segment size? 4GB (granular shrink) vs 16GB (fewer segments, less overhead)?
4. For Approach B: does `talc.claim()` support adding spans after initial creation? Must verify API.
5. For Approach B: does `fi_mr_reg` on overcommitted mmap pin all pages immediately? If yes, virtual overcommit trick doesn't save physical memory. Test on i8ge.
6. Shrink trigger: how does the module learn about Valkey memory pressure? `VM_GetServerInfo` polling? A callback from Valkey? Memory hooks?
7. Should we expose pool/arena stats via `LO.INFO` for observability?
