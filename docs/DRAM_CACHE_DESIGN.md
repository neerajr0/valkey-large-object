# DRAM Cache Design

**Date:** 2026-08-19  **Status:** Draft  **Author:** karsubba

---

## 1. Problem

The module needs a DRAM layer that serves hot objects without NVMe I/O. Two operating modes:

- **DRAM-only** — all objects live in DRAM. No NVMe.
- **DRAM + NVMe** — first write goes to NVMe. Subsequent reads served from DRAM if cached.

---

## 2. Constraints

1. **EFA registration:** Any buffer used for `fi_write`/`fi_read` must be pre-registered with EFA (`fi_mr_reg`). Registration is expensive (~ms, pins pages). Done at startup, not on the data path.

2. **io_uring registration:** Any buffer used for `ReadFixed`/`WriteFixed` must be pre-registered with io_uring (`IORING_REGISTER_BUFFERS`). Done once at startup. Kernel overhead scales with buffer count (keep ≤1000 per registration).

3. **A buffer registered with both** can serve NVMe I/O AND EFA transfers without copying.

4. **An unregistered buffer** can only serve the TCP reply path (no DMA, no fixed I/O).

---

## 3. Object Size Distribution

Modern hybrid-attention models produce diverse object types per prefix:

| Object type | Size per chunk | Growth |
|---|---|---|
| HCA compressed blocks (DeepSeek V4, 128x) | 15–50 KB | Linear with sequence, but tiny |
| FP4 indexer state (DeepSeek V4 CSA) | 10–50 KB | Small metadata |
| CSA compressed KV (DeepSeek V4, 4x) | 0.5–1 MB | Linear with sequence ÷ 4 |
| Sliding window KV | 0.5–2 MB | Fixed (window size) |
| KDA recurrent state (Kimi K3) | 2–8 MB | Fixed (model dimensions) |
| Full-attention KV (Kimi K3, every 4th layer) | 0.5–4 MB per block | Linear with sequence |

**Range: ~15 KB to ~8 MB per stored object.** This is a 500x range.

The client (LMCache) stores each type as a separate key. Our module sees individual opaque blobs at varying sizes.

---

## 4. Value Size Handling

Objects range from ~15KB to ~8MB. Buffers used for NVMe I/O and EFA must be pre-registered (Section 2). Two approaches to handling the mismatch between object size and buffer size:

### Approach A: Fixed-size buffer classes

Pre-allocate buffers in 2-3 size classes. Object goes in the smallest class that fits. Unused space in the buffer is padding (wasted DRAM, but not wasted on NVMe or network — those use exact `len`).

```
Class 1: 64KB buffers × 2000   (for 15-50KB objects)
Class 2: 1MB buffers × 500     (for 0.5-1MB objects)
Class 3: 8MB buffers × 250     (for 2-8MB objects)
```

| Aspect | Detail |
|--------|--------|
| DRAM waste | class_size - object_size per cached object (up to ~50% per class) |
| NVMe waste | none (writes exact `len` bytes) |
| EFA waste | none (fi_write sends exact `len` bytes) |
| Allocation speed | O(1) — pop from class free list |
| Fragmentation | none (fixed slots, no heap fragmentation) |
| Registration | each class registered once at startup (io_uring + EFA) |
| Complexity | low |

### Approach B: Slab allocator from pre-registered contiguous region

Allocate one large contiguous memory region at startup. Register the entire region once with EFA. Sub-allocate exact-sized slots from it using a slab allocator (size classes internally, but allocations are tight-fit).

```
┌────────────────────────────────────────────────────────┐
│ Arena (e.g., 128GB mmap, one fi_mr_reg)               │
│ ┌──────┐┌─────────┐┌──┐┌─────────────┐┌──────┐ ...   │
│ │ 47KB ││  820KB  ││4K││    6.2MB    ││ 91KB │       │
│ └──────┘└─────────┘└──┘└─────────────┘└──────┘       │
└────────────────────────────────────────────────────────┘
```

| Aspect | Detail |
|--------|--------|
| DRAM waste | near-zero (allocate exact bytes + small slab header overhead) |
| NVMe waste | none |
| EFA waste | none (fi_write from any offset in the registered region) |
| Allocation speed | O(1) amortized (slab free-list per size class) |
| Fragmentation | possible over time (external fragmentation from varied sizes) |
| Registration | one fi_mr_reg for entire arena. NOT io_uring registered (too large for fixed-buffer registration). |
| Complexity | moderate (slab allocator, compaction/defrag under churn) |

### Comparison

| | Approach A (fixed classes) | Approach B (slab arena) |
|---|:---:|:---:|
| DRAM efficiency | ≤50% waste per class | ~95%+ efficient |
| io_uring compatible | yes (each class is a registered fixed buffer) | no (arena too large, must use non-fixed Read/Write) |
| EFA compatible | yes (pre-registered) | yes (pre-registered region) |
| Zero-copy NVMe → cache | yes (read into buffer, keep it) | no (NVMe read must use separate IoPool buffer, then memcpy to arena) |
| Zero-copy cache → EFA | yes (fi_write from buffer) | yes (fi_write from arena offset) |
| Implementation | simple (fixed pools) | moderate (slab allocator, fragmentation management) |
| Industry precedent | SPDK buffer pools | Mooncake, NVIDIA DOCA |

### Key tradeoff

Approach A wastes DRAM (padding) but gets zero-copy on the NVMe→cache promotion path (same buffer stays as cache). Approach B saves DRAM but loses that zero-copy (NVMe reads cannot land directly in the arena because the arena is not io_uring-registered).

### Common behaviors (apply to both approaches)

- **Reject if too large:** Objects exceeding the largest supported size → `ERR object exceeds buffer size`. Client must chunk.
- **Exact NVMe writes:** Only `len` bytes written to disk, regardless of buffer/slot size.
- **Exact EFA sends:** Only `len` bytes sent via fi_write.
- **Client can query sizes:** Module can advertise supported size limits via `MODULE INFO` or config (optional, not required).

---

## 4.1 Common Challenges (apply to both approaches)

### NVMe write alignment

O_DIRECT requires write length to be a multiple of the sector size (512 bytes). If an object is 700 bytes, it cannot be written as-is — it must be padded to 1024 bytes (next 512 multiple).

In practice:
- KV cache chunks from LMCache are always multiples of `num_heads × head_dim × dtype_size` — typically 4KB+ and naturally aligned.
- Small objects (HCA compressed blocks, ~15KB) may need up to 511 bytes of padding on disk.
- The padded write length is `ceil(len / 512) * 512`.

On read, only `len` bytes are returned to the caller (the object's stored length, not the padded disk length). The module tracks `len` in the LoValue metadata.

EFA `fi_write` has no alignment constraint — sends exact `len` bytes regardless.

Summary:
- **NVMe:** writes `ceil(len / 512) * 512` bytes to disk. Up to 511 bytes padding. Reads back same padded length into buffer, replies with only `len`.
- **EFA:** sends exact `len`. No padding.
- **DRAM cache:** holds the buffer at class/slab size. Only `len` bytes are meaningful.

### Max object size enforcement

Both approaches have a hard ceiling on object size (largest buffer class in Approach A, or arena capacity in Approach B). When a client sends an object exceeding this limit:

**Behavior:** Reject at `LO.SET` time with `ERR object exceeds max buffer size`.

**Why not multi-buffer:** Stitching multiple buffers for one object requires scatter-gather I/O, multi-buffer fi_write coordination, and fragmented object tracking. Complexity not justified — the client (LMCache) already chunks by layer/block and can chunk smaller.

**Why not oversized fallback path:** Routing oversized objects through a non-registered heap allocation (TCP-only, no EFA, no ReadFixed) creates two code paths with different performance characteristics. Violates the unified-path principle.

**Client contract:** Module advertises its max size via config (`pool-buf-size` for Approach A, or a `max-object-size` config for Approach B). Client respects this and chunks objects that exceed it.

---


## 5. Pool Architecture Options

Given the size handling decision (multiple fixed-size classes, pre-registered), three architectures for the DRAM cache:

### Option 1: Two Separate Pools

One pool for NVMe I/O (registered, fixed-size, transient). One separate pool for DRAM cache (unregistered, variable-size, persistent).

```
┌──────────────────┐     ┌──────────────────┐
│ IoPool           │     │ DRAMPool         │
│ io_uring + EFA   │     │ NOT registered   │
│ fixed-size       │     │ variable-size    │
│ transient        │     │ persistent       │
└──────────────────┘     └──────────────────┘
```

**EFA on DRAM hit:** memcpy DRAMPool → IoPool → fi_write. One extra copy (DRAMPool is not registered).

**TCP on DRAM hit:** reply directly from DRAMPool. No copy.

**Promotion (miss → cache):** NVMe read into IoPool → memcpy to DRAMPool. One extra copy.

| Aspect | Detail |
|--------|--------|
| Registration | IoPool: io_uring + EFA. DRAMPool: none. |
| Object sizes | DRAMPool: any size (heap allocated). IoPool: fixed. |
| Memory commitment | IoPool: upfront. DRAMPool: lazy (allocate on demand). |
| EFA copy cost | 1 memcpy per DRAM-hit EFA request (~1μs/4KB, ~5ms/50MB) |
| Implementation | Simple: `HashMap<ObjectId, Vec<u8>>` |

### Option 2: Pre-registered DRAM Arena + Separate IoPool

One large contiguous region for DRAM cache, registered with EFA at startup. IoPool separate for NVMe I/O. Sub-allocate within arena using a slab allocator.

```
┌──────────────────┐     ┌────────────────────────────┐
│ IoPool           │     │ DRAM Arena                 │
│ io_uring + EFA   │     │ EFA registered (one region)│
│ fixed-size       │     │ slab sub-allocated         │
│ transient        │     │ persistent                 │
└──────────────────┘     └────────────────────────────┘
```

**EFA on DRAM hit:** fi_write directly from arena. Zero copy.

**TCP on DRAM hit:** reply directly. Zero copy.

**Promotion (miss → cache):** NVMe read into IoPool → memcpy to arena slot. One copy.

| Aspect | Detail |
|--------|--------|
| Registration | IoPool: io_uring + EFA. Arena: EFA only (too large for io_uring fixed-buffer registration). |
| Object sizes | Slab classes within arena (bounded waste). |
| Memory commitment | Arena: upfront (must mmap at startup). IoPool: upfront. |
| EFA copy cost | Zero on hit. One memcpy on promotion. |
| Implementation | Moderate: slab allocator (well-understood, jemalloc-like). |

### Option 3: One Unified Pool (Both Categories)

One pool of fixed-size buffers, all pre-registered with BOTH io_uring and EFA. Buffers serve two roles: transient (I/O) or persistent (cached). A buffer that completes an NVMe read stays held as cache.

```
┌───────────────────────────────────────────────┐
│ Unified Pool (per size class)                 │
│ ALL buffers: io_uring + EFA registered        │
│ Fixed-size per class                          │
│                                               │
│  ┌──── Free ────┐  ┌──── Cached ────┐        │
│  │ available    │  │ holding data   │        │
│  │ for I/O      │  │ (DRAM cache)   │        │
│  └──────────────┘  └────────────────┘        │
└───────────────────────────────────────────────┘
```

**EFA on DRAM hit:** fi_write directly from cached buffer. Zero copy.

**TCP on DRAM hit:** reply directly. Zero copy.

**Promotion (miss → cache):** NVMe read lands in buffer → keep it (don't return to free list). Zero copy.

**Eviction:** Move buffer from cached → free list. Zero copy.

| Aspect | Detail |
|--------|--------|
| Registration | All buffers: io_uring + EFA. One registration per size class at startup. |
| Object sizes | Fixed per class. Waste = class_size - object_size. |
| Memory commitment | All upfront (buffer_count × buffer_size per class). |
| EFA copy cost | Zero. Always. |
| Promotion cost | Zero (keep the buffer). |
| I/O starvation risk | Yes — cache competes with I/O for same buffers. Need backpressure. |
| Implementation | Moderate: track free vs cached sets per class. |

---

## 6. Comparison

| Aspect | Option 1 (two pools) | Option 2 (arena + IoPool) | Option 3 (unified pool) |
|--------|:---:|:---:|:---:|
| EFA on DRAM hit | memcpy (not zero-copy) | zero-copy | zero-copy |
| TCP on DRAM hit | zero-copy | zero-copy | zero-copy |
| Promotion cost | memcpy | memcpy | zero (keep buffer) |
| Eviction cost | free alloc | return slab slot | pointer move |
| Object size flexibility | any | slab classes | fixed per class |
| Memory commitment | partial lazy | arena upfront | all upfront |
| Registration | IoPool only | IoPool + arena (EFA) | all buffers (io_uring + EFA) |
| I/O starvation risk | none | none | yes (shared budget) |
| Implementation | simple | moderate (slab) | moderate (free/cached tracking) |
| Industry precedent | — | Mooncake, DOCA, SPDK | — |

---

## 7. Max Memory Handling

### DRAM over limit

| Mode | Option 1 | Option 2 | Option 3 |
|------|----------|----------|----------|
| DRAM-only | reject LO.SET | reject LO.SET | reject (pool exhausted) |
| DRAM+NVMe | evict from DRAM (data safe on NVMe) | evict from arena | evict cached buffer to free list |

### NVMe over limit (DRAM+NVMe only)

Reject new `LO.SET`. Cannot silently drop NVMe data.

### I/O exhaustion

Options 1 & 2: `ERR pool exhausted`. Independent of DRAM cache.

Option 3: Must evict a cached buffer to serve I/O. Eviction on hot path — latency cost.

---

## 8. Recommendation

**Option 3 (unified pool with multiple size classes)** for production:
- Zero-copy on every path (promotion, EFA hit, TCP hit, eviction)
- Simplest data flow (buffer never moves between pools)
- Multiple size classes handle the 15KB–8MB object range
- Industry constraint: keep each class ≤1000 buffers for io_uring

**Option 1 (two pools)** as stepping-stone implementation:
- Works without EFA (TCP-only deployment)
- No upfront memory commitment
- Simplest to build first

**Migration path:** Option 1 → Option 3 is internal (command interface unchanged). Build Option 1 now, measure, switch to Option 3 when EFA transport is real.

---

## 9. Open Questions

1. What size classes should we pre-configure? Need LMCache team input on their chunk sizes.
2. For Option 3: what free:cached ratio is safe? (e.g., reserve 20% for I/O, allow 80% for caching).
3. For Option 3: can we dynamically resize classes at runtime (e.g., `CONFIG SET`) or must they be fixed at startup? (Answer: fixed — registration is at startup only.)
4. Should we expose size classes to clients (Strategy C) or keep them internal?
