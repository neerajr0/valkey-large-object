# Storage Design

**Date:** 2026-08-21  **Status:** Draft  **Author:** @KarthikSubbarao

---

## 1. Problem

The module stores large objects (15KB to multi-GB (TBD)) and must serve them via two transports:
- **TCP:** standard RESP reply
- **EFA:** RDMA fi_write directly to client GPU memory

Two operating modes:
- **DRAM-only:** All objects live in DRAM. No NVMe. Fastest reads. Limited by DRAM capacity.
- **DRAM + NVMe:** All objects persist on NVMe. DRAM is a read cache (hot objects promoted on access). Larger capacity, slightly higher latency on cold reads.

---

## 2. Hard Constraints

These two constraints drive every design decision:

**1. EFA registration (fi_mr_reg):**
Any buffer used as source for `fi_write` must be pre-registered with the NIC. Registration pins physical pages and programs the NIC's translation table. Cost: ~300-500μs per call (measured ~333μs on i8ge EFA, not size-proportional). Must be done at startup or on rare resize events — never on the data path. Per-request registration destroys throughput by 45x (measured: 138K rps pre-registered vs 3K rps per-request at 4KB).

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

Object sizes are **unknown at design time**. The module stores opaque blobs from clients (LMCache, vLLM, custom inference frameworks). Sizes depend on model architecture, page size, attention type, and layer grouping — none of which we control.

**Known lower bound:** ~15KB (compressed attention blocks, small metadata).
**Known upper bound:** Unbounded in theory. KDA checkpoints ~150MB, full KV cache for long contexts can reach GBs. The module handles large objects via multi-buffer parallel I/O (§7).

Example ranges observed in hybrid-attention models:

| Object type | Typical size | Notes |
|---|---|---|
| Compressed attention blocks | 15–50 KB | Small, numerous |
| Metadata / indexer state | 10–50 KB | Small |
| Compressed KV (4x) | 0.5–1 MB | Common |
| Sliding window KV | 0.5–2 MB | Fixed per model |
| KDA recurrent state / checkpoints | 2–150 MB | Hot, good DRAMPool candidates |
| Full-attention KV blocks | 0.5 MB – multi-GB | Linear with context length |

**Variance: 1000x+ across object types.**

The client stores each chunk as a separate key. Our module sees individual opaque blobs at varying sizes.

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

### 4.2 Approach B: Arena with Slab Allocator (talc) [Recommended]

Allocate one or more large contiguous memory segments at startup. Register each segment with EFA. Sub-allocate exact-sized slots from the segments using a general-purpose allocator (talc).

```
┌────────────────────────────────────────────────────────────┐
│ Segment 0 (16GB, io_uring buf_index=0)                    │
│ ┌──────┐┌─────────┐┌──┐┌─────────────┐┌──────┐ ...      │
│ │ 47KB ││  820KB  ││4K││    6.2MB    ││ 91KB │          │
│ └──────┘└─────────┘└──┘└─────────────┘└──────┘          │
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│ Segment 1 (16GB, io_uring buf_index=1)                    │
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

**Mitigation strategy:** Open question. Separate segments per layer (§4.5) prevents the worst case (short-lived NVMePool churn fragmenting long-lived DRAMPool). Within each layer, coalescing may be sufficient — needs production data to determine if active mitigation is required. See §9 Open Questions. Other mitigations include (1) banding into segments based on value size (2) scaling out and scaling in to delete fragmented segments.

### 4.4 Comparison

| | Approach A (fixed classes) | Approach B (talc arena) |
|---|:---:|:---:|
| DRAM efficiency | ≤50% waste per class | ~95%+ efficient |
| io_uring ReadFixed | Yes | Yes (segment-as-buffer + offset) |
| NVMe read throughput | ~156K rps | ~156K rps (same — ReadFixed works) |
| EFA zero-copy on hit | Yes | Yes |
| Promotion zero-copy (NVMe → cache) | Yes (keep buffer) | Yes (ReadFixed into arena slot directly) |
| Alloc speed | O(1) guaranteed | O(1) amortized, O(n) worst |
| Fragmentation | Impossible | Possible (coalescing + separate segments mitigate) |
| Defrag mechanism | N/A | Open question (§9) |
| Code complexity | ~100 lines | ~315 lines |
| Best for | Simplicity, predictable latency | Memory efficiency, varied object sizes |

### 4.5 Decision: Approach B (Separate Segments Per Layer)

We use Approach B (talc arena). Object sizes are unknown at design time — talc provides exact-fit allocation regardless of what sizes production traffic produces.

**Two separate talc instances, each with their own segments:**

```
NVMePool:    Segment(s) (e.g., 2GB)   — own Mutex<Talc>, high churn, short-lived StreamingContexts
DRAMPool:    Segment(s) (e.g., 16GB)  — own Mutex<Talc>, low churn, long-lived ObjectContexts

io_uring registration: [iovec{NVMePool_seg, 2GB}, iovec{DRAMPool_seg, 16GB}] — all segments in one array
EFA registration:      fi_mr_reg per segment — enables fi_write from any buffer in either pool
```

**Both pools are io_uring registered (IORING_REGISTER_BUFFERS):**
- NVMePool: ReadFixed/WriteFixed for NVMe I/O staging (primary use case)
- DRAMPool: ReadFixed during promotion (NVMe → DRAMPool direct fill, §7.3.4). Without registration, promotion falls back to plain `read` (~15-20% slower per chunk — acceptable but suboptimal).

**Both pools are EFA registered (fi_mr_reg):**
- NVMePool: fi_write to client during serve-and-discard GET
- DRAMPool: fi_write to client from cached objects (the hot serving path)

**Why separate segments per layer:**
- Prevents lifetime-mixing fragmentation: NVMePool high-churn alloc/free cycles cannot create holes between long-lived DRAMPool objects
- Each layer's talc instance only sees objects of similar lifetime — fragmentation is self-healing (NVMePool: FIFO churn reclaims space naturally; DRAMPool: infrequent evictions don't leave Swiss-cheese)
- Independent sizing: NVMePool sized for max concurrent I/O, DRAMPool sized for working set
- Independent scaling: expand/shrink one layer without affecting the other

### 4.6 Common Requirements

**O_DIRECT alignment (NVMe mode only — does not apply to DRAM-only mode):**
O_DIRECT bypasses the kernel page cache for direct NVMe I/O. It imposes two constraints:
1. **Buffer address** must be 4KB-aligned (filesystem block size). Handled by `PinnedBuffer::new()` via `Layout::from_size_align(size, 4096)`.
2. **Write length** must be a multiple of 512 bytes (logical sector size). Objects not naturally aligned are padded on disk: `ceil(len / 512) * 512`. Up to 511 bytes waste on disk. Reads return only `len` bytes (stored in LoValue metadata).

**Both read and write paths must round up the I/O length.** The kernel rejects non-aligned lengths with `EINVAL`. The module handles this explicitly — O_DIRECT does not auto-pad. Current code (`uring.rs`) uses `align_up()` which rounds to 4096 — over-aligned but correct. Read path applies this; write path currently does not (BUG — masked because benchmark object sizes are naturally aligned). Must be fixed.

EFA `fi_write` has no alignment constraint — sends exact `len`.

Without O_DIRECT (DRAM-only mode, or `direct-io no`), neither constraint applies.

**Max object size enforcement:**
- **TCP:** Objects exceeding `lo-max-tcp-object-size` (default 256MB) are rejected — Valkey's querybuf cannot stream (§7.7).
- **EFA:** No hard max. Objects larger than a single buffer are handled via multi-buffer parallel I/O (§7.3) or streaming mode (§7.3). The module chunks internally using `lo-buffer-size`.
- **NVMe capacity:** Objects exceeding available NVMe space are rejected at `LO.SET`.

---

## 5. Operating Modes

Both approaches follow the same command-level flow. "Alloc" and "free" refer to `talc.alloc`/`talc.free` from either NVMePool (transient I/O) or DRAMPool (cached objects). The resulting memory is pre-registered with io_uring and EFA.

### 5.1 DRAM-Only Mode

All objects live exclusively in DRAM. No NVMe storage. Fastest possible reads. Capacity limited by available DRAM.

```
LO.SET key len <payload> [rkey remote_addr]:
  1. Alloc buffer (registered memory)
  2a. TCP: copy payload into buffer
  2b. EFA: fi_read from client GPU into buffer (zero-copy)
  3. Track buffer: key → buffer location + len

LO.GET key [rkey remote_addr len]:
  4. Lookup in HashMap → buffer pointer
  5a. TCP: reply from buffer
  5b. EFA: fi_write from buffer to client GPU (zero-copy)

DEL key:
  6. Free buffer (return to pool / arena)
  7. Untrack buffer / delete object
```

**Key property:** Data exists ONLY in DRAM. Eviction on the DRAMPool layer = data loss = equivalent to DEL. Only Valkey's maxmemory eviction policy triggers this.

### 5.2 DRAM + NVMe Mode

All objects persist on NVMe (write-through). DRAM is a read cache — hot objects promoted on GET only.

```
LO.SET key len <payload> [rkey remote_addr]:
  1. If key has existing DRAMPool entry: free that buffer (invalidate stale data)
  2. Alloc buffer (registered memory)
  3a. TCP: copy payload into buffer
  3b. EFA: fi_read from client GPU into buffer (zero-copy)
  4. io_uring WriteFixed to NVMe (O_DIRECT, 512-byte aligned length)
  5. Free buffer (no DRAMPool caching on write path)
  6. Track file in key/object: key → NVMe location only

LO.GET key [rkey remote_addr len] — DRAMPool hit:
  7. Lookup in HashMap → buffer is cached in DRAMPool
  8a. TCP: reply from buffer
  8b. EFA: fi_write from buffer (zero-copy)

LO.GET key [rkey remote_addr len] — DRAMPool miss:
  9. Alloc buffer from NVMePool (registered memory)
  10. io_uring ReadFixed from NVMe into NVMePool buffer (O_DIRECT)
  11a. TCP: reply from NVMePool buffer
  11b. EFA: fi_write from NVMePool buffer (zero-copy)
  12. Free NVMePool buffer
  13. If promotion policy says YES:
      - Alloc ObjectContext with N buffers directly in DRAMPool segment
      - ReadFixed from NVMe directly into DRAMPool buffers (no memcpy, no NVMePool involvement)
      - Mark ObjectContext as Filling → Ready when complete
      - Concurrent GETs coalesce on this ObjectContext (§7.3.5)

Eviction (DRAMPool pressure):
  13. Free buffer. Data safe on NVMe.
  14. Remove DRAMPool pointer from HashMap (keep NVMe reference)

DEL key:
  15. Free buffer (if cached in DRAM)
  16. Delete NVMe file
  17. Remove from HashMap
```

**Key properties:**
- SET always invalidates any stale DRAMPool entry then writes to NVMe. No caching on write path.
- DRAMPool is populated only on the GET path (promotion). Admission policy is a single decision point at step 13. Promotion reads directly into DRAMPool buffers via ReadFixed — no memcpy, no intermediate NVMePool buffer (§7.3.4).
- DRAMPool is expendable. Eviction is cheap (data persists on NVMe). Cache miss costs one NVMe read (~15μs on i8ge).

---

## 6. Data Type Struct and Object References

### 6.1 LoValue (Per-Key Metadata)

Stored in Valkey's keyspace via the module data type. One per LO key. ~20 bytes. Serialized to RDB.

```rust
pub struct LoValue {
    pub object_id: ObjectId,  // Monotonic per-node OID (used as NVMe filename)
    pub len: u64,             // Object size in bytes (exact)
    pub crc32c: u32,          // Integrity checksum (verified on replication pull)
}
```

Only durable, object-intrinsic data. No runtime state (fd, DRAM cache location, flags). Runtime references are in module-internal structures:
- **FdPool:** `HashMap<ObjectId, RawFd>` — rebuilt on load, not serialized
- **DRAMPool:** `HashMap<ObjectId, ObjectContext>` — buffers in DRAMPool segments, populated on GET hits, evicted independently
- **NVMePool inflight:** transient `StreamingContext` per in-flight request — buffers in NVMePool segments, dropped on completion
- **Allocators:** `Mutex<Talc>` per layer — `dram_pool_talc` for DRAMPool, `nvme_pool_talc` for NVMePool (§4.5)

### 6.2 ObjectContext, StreamingContext, and Buffer

Module-internal runtime companions to LoValue. Not serialized — rebuilt on load, evicted independently of commands.

```rust
struct Buffer {
    segment_idx: u8,       // Which segment this slice lives in (DRAMPool or NVMePool)
    offset: u64,           // Byte offset within that segment
    len: u32,              // This chunk's size
}
// Always within a registered segment → ReadFixed + EFA fi_write capable

struct ObjectContext {
    buffers: Vec<Buffer>,  // ALL chunks (complete object). Allocated from DRAMPool.
    total_len: u64,
    state: ObjectState,
}

enum ObjectState {
    Ready,                                          // Fully filled, servable
    Filling { chunks_ready: u32, total: u32 },      // Promotion in progress (§7.3.4)
}

struct StreamingContext {
    buffers: Vec<Buffer>,       // Rotating window of X buffers. Allocated from NVMePool.
    total_len: u64,
    bytes_completed: u64,       // Progress cursor
    crc_hasher: Option<Crc32c>, // For SET verification
}
```

**ObjectContext (DRAMPool — long-lived, complete):**
- ALL N buffers for the entire object allocated upfront from DRAMPool segment
- `state = Filling` during promotion (§7.3.4): batched ReadFixed fills buffers, `chunks_ready` advances per batch
- `state = Ready`: all buffers filled, object servable
- Coalesced waiters block on `Filling` state, wake when sufficient chunks are ready (§7.3.5)
- Stored in: `HashMap<ObjectId, ObjectContext>`

**StreamingContext (NVMePool — short-lived, partial window):**
- X buffers (batch size), reused across batches
- Used for: SET writes to NVMe, GET serve-and-discard (no promotion)
- Freed entirely after operation completes

**Buffer:**
- Segment-agnostic: works for both DRAMPool and NVMePool segments
- Same struct regardless of lifetime or pool
- `segment_idx` identifies which registered iovec entry (DRAMSegment or NVMeSegment)

### 6.3 NVMe File Reference

Each object is one file: `/data/lo-data/{oid:016x}.dat`

- fd opened at LO.SET, held in FdPool (`HashMap<ObjectId, RawFd>`)
- Lookup: `fd_pool.get(object_id)` → RawFd for io_uring submission
- File size = `ceil(len / 512) * 512` (O_DIRECT 512-byte write alignment padding)
- Actual object length stored in `LoValue.len` (not derived from file size)
- On DEL: `fd_pool.remove(oid)` closes fd, then `unlink()` deletes file

### 6.4 ObjectContext Lifetimes

ObjectContext exists in two layers with different lifetimes. Same struct, same Buffer type, but allocated from **separate talc instances in separate segments** (§4.5).

**DRAMPool (long-lived):**
- ObjectContext created on cache promotion (LO.GET hit policy admits it)
- Buffers allocated from DRAMPool segment(s) via `dram_pool_talc.lock().alloc()`
- Held in `HashMap<ObjectId, ObjectContext>` for the object's entire cached lifetime
- Buffers remain allocated and serve repeated LO.GET hits directly
- On DRAMPool eviction (policy-based — LRU/LFU/memory pressure): ObjectContext dropped → `dram_pool_talc.lock().free()` for each buffer
- Object survives on NVMe. Next GET is a cache miss (NVMePool serves it).

**NVMePool (short-lived):**
- StreamingContext created per in-flight I/O request
- Buffers allocated from NVMePool segment(s) via `nvme_pool_talc.lock().alloc()`
- On request completion: StreamingContext dropped → `nvme_pool_talc.lock().free()` for each buffer
- If promotion policy says yes: separate ReadFixed directly into DRAMPool buffers (§7.3.4). No memcpy from NVMePool. NVMePool buffers freed independently after serving the current request.

### 6.5 Relationship Diagram

Example: 50MB object cached in DRAMPool (long-lived). NVMePool would look the same structurally but with short-lived StreamingContexts in NVMePool segments.

```
Valkey keyspace                Module internals
──────────────                 ────────────────
key "obj-A"
  └─ LoValue {oid=42,         fd_pool.get(42) → RawFd → /data/lo-data/000000000000002a.dat (50MB)
       len=50MB,                                                ▲
       crc32c=0xAB12}                                           │  N buffers : 1 file
                                                                │  (parallel ReadFixed/WriteFixed
                               object_contexts[42] → ObjectContext    at different file offsets)
                                 buffers: [                     │
                                   buf[0] {seg=0, off=0x0000, 8MB}  ──→ file offset 0MB
                                   buf[1] {seg=0, off=0x80_0000, 8MB} → file offset 8MB
                                   buf[2] {seg=1, off=0x0000, 8MB}  ──→ file offset 16MB
                                   buf[3] {seg=0, off=0x100_0000, 8MB} → file offset 24MB
                                   buf[4] {seg=1, off=0x80_0000, 8MB} → file offset 32MB
                                   buf[5] {seg=0, off=0x180_0000, 8MB} → file offset 40MB
                                   buf[6] {seg=1, off=0x100_0000, 2MB} → file offset 48MB
                                 ]
                                 total_len: 50MB
                                                     │
                    ┌────────────────────────────────┘
                    ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ DRAMPool Segment 0 (16GB, io_uring buf_index=0)               │
  │ [...buf[0]...][...buf[1]...][...buf[3]...][...buf[5]...]    │
  └─────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────┐
  │ DRAMPool Segment 1 (16GB, io_uring buf_index=1)               │
  │ [...buf[2]...][...buf[4]...][...buf[6]...]                  │
  └─────────────────────────────────────────────────────────────┘
```

---


## 7. Large Object I/O: Multi-Buffer Parallel

Objects can be much larger than a single I/O buffer (e.g., 10GB object with 64MB buffers). The module handles this by streaming through multiple buffers in parallel — never allocating the full object in DRAM at once.

**Transport-dependent behavior:**
- **TCP:** Valkey's command dispatch accumulates the full payload in `querybuf` before calling the module handler. Replies use single-allocation `VM_ReplyWithStringBuffer`. There is no incremental/streaming API. **TCP rejects objects above a configurable max size** (e.g., 256MB). Multi-buffer parallel I/O applies only to EFA.
- **EFA:** The module controls chunk size via async transport.read/transport.write. Multi-buffer parallel I/O is the primary large object path.

### 7.1 NVMe Representation: Single File Per Object

Each object is one contiguous file on NVMe regardless of size:

```
Object "key123" (10GB):
  NVMe: /data/lo-data/00000042.dat   (10GB file, XFS extent-allocated)
  LoValue: {oid=42, len=10GB, crc32c=0xAB12}
```

**Why single file, not multiple:**
- io_uring parallelizes via offset within one fd — no need for multiple files
- NVMe controller sees LBAs, not files. Same parallelism either way.
- Atomicity: single unlink = atomic delete. No partial-object cleanup.
- Simpler fd management, RDB serialization, and error handling.
- mdraid0 stripes across all drives regardless of file count.

### 7.2 Buffer Size Translation (Client Args → Server Chunks)

The server decides chunk size — the client never specifies or sees it.

**Server config:** `lo-buffer-size` (e.g., 8MB). This determines the allocation unit for all I/O operations.

**LO.SET translation:**
```
Client sends:  LO.SET key 50MB <payload or rkey+addr+len>
Server sees:   total_len=50MB, chunk_size=8MB → N=7 chunks
Server does:   alloc 7 buffers from shared segments
               partition incoming data into 8MB pieces
               submit 7 parallel WriteFixed SQEs to NVMe
```

**LO.GET translation:**
```
Client sends:  LO.GET key [rkey addr 50MB]
Server sees:   LoValue.len=50MB, chunk_size=8MB → N=7 chunks
Server does:   alloc 4-8 buffers (pipeline depth)
               submit ReadFixed SQEs at file offsets 0, 8MB, 16MB, ...
               TCP: write each chunk to reply buffer sequentially (client sees one bulk string)
               EFA: transport.write each chunk to client at addr + i*chunk_size
```

**Key invariant:** The client provides `total_len` and a destination (TCP socket or EFA region). The server partitions into `ceil(total_len / lo-buffer-size)` internal operations. The last operation uses `len = total_len % lo-buffer-size` (partial chunk). O_DIRECT write path pads the final write to 512-byte boundary on disk (§4.6). The chunk boundary is invisible to the client protocol.

### 7.3 Chunked Streaming I/O

All I/O operations (SET and GET) use the same chunked streaming pattern. "Full pipeline" and "degraded streaming" are the same code path — the difference is how many buffers are available (max X vs min Y). There is no separate "parallel I/O" path.

#### 7.3.1 Core Pattern: Batched Submission with Oneshot Bridge

Every chunked I/O operation runs as a tokio task with X buffers (the batch/pipeline depth). The task submits a **single batch of X I/Os** to the io layer, awaits all completions, then reuses the buffers for the next batch.

```rust
async fn stream_batched(buffers: &mut [Buffer], fd: RawFd, total_len: u64, chunk_size: usize) {
    let x = buffers.len();  // batch size
    let total_chunks = ceil(total_len, chunk_size);
    
    for batch_start in (0..total_chunks).step_by(x) {
        let batch_end = min(batch_start + x, total_chunks);
        let batch_size = batch_end - batch_start;
        
        // One call to io layer — submits all X SQEs as a single batch
        let rx = io_layer.submit_batch(
            fd,
            &buffers[..batch_size],
            base_offset: batch_start * chunk_size,
            chunk_size,
        );  // internally: queues X SQEs → one io_uring_submit() syscall
        
        // Await batch completion (all X CQEs reaped)
        rx.await;
        
        // Batch complete: all X buffers are now filled (read) or flushed (write)
        // Process results, reuse all X buffers for next batch
    }
}
```

**Key invariant:** After awaiting a batch, buffers `[0..batch_size]` are ALL complete. Progress advances by `batch_size * chunk_size` bytes atomically. No partial-batch state. The io layer handles CQE ordering internally — the tokio task sees only "batch done" or "batch failed."

**Pipeline depth = batch size = X = number of buffers allocated for this operation.**

#### 7.3.2 Context Types

See §6.2 for full struct definitions. Summary:

- **StreamingContext** (NVMePool): rotating window of X buffers, used for SET writes and GET serve-and-discard. Short-lived.
- **ObjectContext** (DRAMPool): complete allocation of ALL N buffers, with `ObjectState` (`Ready` or `Filling{chunks_ready, total}`). Long-lived. Coalescing point for concurrent GETs during promotion.

#### 7.3.3 LO.SET Flows

**SET + EFA + NVMe mode:**
```
Main thread: validate, fallocate NVMe file, BlockedClient
Spawn tokio task with StreamingContext (X buffers from NVMePool):

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Fill X buffers from client via EFA
    for i in 0..batch_size:
      transport.read(client_addr + (batch_start+i)*chunk_size, buffer[i], chunk_size).await
      crc_hasher.update(buffer[i][..chunk_len])
    
    // Single batch submission to io layer (one io_uring_submit syscall)
    io_layer.submit_batch(fd, &buffers[..batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion
    
    // Batch done. All X buffers reusable for next batch.

  Finalize: verify CRC. Match → create LoValue, unblock OK. Mismatch → unlink, unblock ERR.
  Free all StreamingContext buffers back to NVMePool.
```

**SET + TCP + NVMe mode:**
```
Main thread: validate (payload already in querybuf), fallocate NVMe file, BlockedClient
Spawn tokio task with StreamingContext (X buffers from NVMePool):

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Fill X buffers from querybuf (memcpy — data already in memory)
    for i in 0..batch_size:
      memcpy(buffer[i], querybuf + (batch_start+i)*chunk_size, chunk_len)
      crc_hasher.update(buffer[i][..chunk_len])
    
    // Single batch submission + await
    io_layer.submit_batch(fd, &buffers[..batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion

  Finalize: verify CRC, create LoValue, unblock.
```

**SET + EFA + DRAM-only mode:**
```
Main thread: validate, BlockedClient
Spawn tokio task — alloc N buffers from DRAMPool segment (final storage):

  for i in 0..N:
    transport.read(client_addr + i*chunk_size, dram_buffers[i], chunk_size).await
    crc_hasher.update(dram_buffers[i])

  Verify CRC → create LoValue + ObjectContext{buffers, state=Ready}, unblock.
  (Buffers stay — they ARE the cached object. No free.)
```

**SET + TCP + DRAM-only mode:**
```
Main thread (synchronous — no tokio task needed):
  Alloc N buffers from DRAMPool segment
  for i in 0..N:
    memcpy(dram_buffers[i], querybuf + i*chunk_size, chunk_len)
    crc_hasher.update(dram_buffers[i])
  Verify CRC → create LoValue + ObjectContext{buffers, state=Ready}, reply OK.
```

#### 7.3.4 LO.GET Flows

**GET + DRAMPool hit (both transports):**
```
ObjectContext.state == Ready:
  EFA: transport.write(buffer[i], chunk_len, client_addr + i*chunk_size, rkey).await for each chunk
  TCP: VM_ReplyWithStringBuffer from ObjectContext buffers (or reject if > TCP threshold)
  No I/O. No StreamingContext. Direct serve from DRAMPool.
```

**GET + DRAMPool miss + NO promotion (serve and discard):**
```
Spawn tokio task with StreamingContext (X buffers from NVMePool):

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Single batch ReadFixed submission
    io_layer.submit_read_batch(fd, &buffers[..batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion
    
    // Batch done — all X buffers filled. Send to client:
    for i in 0..batch_size:
      EFA: transport.write(buffer[i], chunk_len, client_addr + (batch_start+i)*chunk_size, rkey).await
      TCP: write to reply buffer (if within TCP threshold)
    
    // All X buffers reusable for next batch.

  Unblock client. Free StreamingContext buffers.
```

**GET + DRAMPool miss + promotion (direct fill into DRAMPool):**
```
1. Alloc FULL ObjectContext in DRAMPool segment (N buffers for entire object)
2. Insert into DRAMPool HashMap with state = Filling{chunks_ready: 0, total: N}
3. Spawn tokio task — reads directly into DRAMPool buffers in batches:

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Single batch ReadFixed directly into DRAMPool buffers
    io_layer.submit_read_batch(fd, &dram_buffers[batch_start..batch_start+batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion
    
    // Batch complete — advance contiguous progress
    object_context.state = Filling{ chunks_ready: batch_start + batch_size, total: N }
    // Wake coalesced waiters: they can now serve bytes [0..(batch_start+batch_size)*chunk_size]

4. Set state = Ready. Wake all remaining waiters.
5. Serve the original request from the now-complete ObjectContext.
```

**Why batched for promotion:** `chunks_ready` advances by X at a time (one batch). After a batch completes, all chunks `[0..chunks_ready]` are contiguous and valid. Waiters can safely serve `[0..chunks_ready * chunk_size]` — no holes, no out-of-order risk. CQEs within a batch may arrive in any order; we wait for the whole batch before advancing.

#### 7.3.5 Coalescing During Promotion

When a GET arrives for a key whose ObjectContext is in `Filling` state:

```
GET arrives → lookup DRAMPool HashMap → ObjectContext exists, state = Filling:
  - Do NOT start a new NVMe read
  - Do NOT allocate NVMePool buffers
  - Register as a waiter on this ObjectContext
  - When state transitions to Ready (or enough chunks for this request): wake and serve
```

All concurrent GETs for the same key during promotion share the single ongoing fill. Zero duplicate NVMe reads. Zero wasted buffers.

This integrates with PR #42's CoalescingMap: the DRAMPool HashMap entry in `Filling` state IS the coalescing point. No separate singleflight structure needed for the promotion path.

#### 7.3.6 Buffer Budget

| Config | Default | Meaning |
|---|---|---|
| `lo-max-buffers-per-op` (X) | 8 | Max buffers per operation = batch size. All X submitted simultaneously. |
| `lo-streaming-min-buffers` (Y) | 2 | Min buffers to start (below = reject). Y=2 enables double-buffering. |
| `lo-max-streaming-ops` | 2 | Max concurrent streaming operations (prevents cascading) |

**X = batch size = pipeline depth.** Each iteration of the streaming loop submits X I/Os, awaits all X, then reuses all X for the next batch. Progress advances by X chunks atomically.

Decision logic on NVMePool alloc:
- Got X buffers → full pipeline speed (8 concurrent SQEs per batch)
- Got ≥ Y but < X → proceed at reduced batch size (degrades gracefully)
- Got < Y → reject with `ERR insufficient buffer capacity`

Pool exhaustion mid-stream: impossible. Buffers are allocated once at the start of the operation and reused across batches. The loop never allocates mid-flight.

#### 7.3.7 Data Correctness

- **CRC32c** computed incrementally during SET streaming, verified at end
- **LoValue** created ONLY after all chunks written AND CRC verified
- **Client disconnect mid-SET:** unlink partial file, no LoValue created
- **Server crash mid-SET:** orphan file without LoValue → reconciliation deletes
- **Invariant:** `LoValue exists ⟺ NVMe file is complete AND CRC-verified`
- **Promotion correctness:** ObjectContext in `Filling` state is visible but only servable up to `chunks_ready * chunk_size` bytes. `chunks_ready` advances by batch (X chunks at a time) — never partial batches. Waiters blocked on content beyond `chunks_ready` wait for the next batch to complete.

### 7.4 Chunk Size Selection

| Chunk size | Buffers for 10GB | SQE count | Tradeoff |
|---|---|---|---|
| 4MB | 2500 (sequential) | 2500 | Minimal pool usage, high SQE overhead |
| 64MB | 160 (sequential) | 160 | Good balance |
| 256MB | 40 (sequential) | 40 | Fewer SQEs, larger pool reservation |

With pipelining (4–8 buffers in flight), only 4–8 buffers are checked out at once regardless of object size. Total SQE count determines total I/O time; pipeline depth determines pool pressure.

**Recommended:** chunk_size = `lo-buffer-size` config value. No special "large object" buffer — reuse the same shared segment allocator. The chunking is purely an I/O scheduling pattern, not a storage decision.

### 7.5 DRAMPool for Large Objects

Large objects (>256MB) are **never promoted to DRAMPool**:
- Cost/benefit is poor (256MB DRAM for one key vs serving hundreds of smaller hot objects)
- Promotion threshold is configurable: `dram-cache-max-object-size` (default: 256MB)
- Objects above this threshold always read from NVMe via the parallel pipeline
- Objects below this threshold can be promoted to DRAMPool on repeated access (§5.2 step 13, §7.3.4)

### 7.6 EFA Transport for Large Objects

Two cases for how EFA handles large objects:

**Case 1: Client provides multiple address/len pairs in the command**

The command itself includes multiple regions. Server performs parallel transport.read/transport.write across all of them simultaneously:

```
LO.SET key <total_len> <n_regions> <rkey1 addr1 len1> <rkey2 addr2 len2> ...
LO.GET key <n_regions> <rkey1 addr1 len1> <rkey2 addr2 len2> ...
```

- Client has multiple GPU memory registrations (e.g., multi-GPU, or multiple buffers on one GPU)
- Server calls transport.read/transport.write in parallel across all provided regions
- Each region maps to one or more NVMe chunks
- Client controls the parallelism and memory layout explicitly

**Case 2: Client provides a single large address/len that exceeds comfortable buffer size**

The client provides one region larger than the server's buffer size. Two sub-options:

- **Reject:** Return ERR if `len > max_efa_transfer_size`. Simple, forces client to use Case 1.
- **Accept and split (preferred — product requirement):** Server internally splits the single large region into chunk-sized fi_write/fi_read calls at sequential offsets within the client's region:

```
Client provides: rkey=R, remote_addr=A, len=10GB
Server internally:
  transport.write(buf[0], chunk_size, A + 0*chunk_size, R).await
  transport.write(buf[1], chunk_size, A + 1*chunk_size, R).await
  transport.write(buf[2], chunk_size, A + 2*chunk_size, R).await
  ...
```

- Transparent to client — single registration, single addr, server handles the chunking
- Server pipelines: NVMe ReadFixed fills buffer[i], fi_write sends it, buffer returned to pool
- No API change from the small-object case — same command syntax, server detects large size and splits

**v1 decision:** Reject over TCP for large objects. Accept over EFA using Case 2 (server-side split) to meet the product requirement. Case 1 deferred to v2 if multi-GPU clients need explicit region control.

### 7.7 TCP Path: Large Object Rejection

Valkey's RESP command dispatch accumulates the full payload in `client->querybuf` before calling the module handler. Replies use single-allocation `VM_ReplyWithStringBuffer`. There is no incremental streaming API for either direction.

**Consequence:** A 10GB LO.SET over TCP requires 10GB in querybuf before the module even runs. This is untenable.

**v1 behavior:**
- `LO.SET` over TCP: reject with `ERR object exceeds max TCP size` if payload > `lo-max-tcp-object-size` (configurable, default 256MB)
- `LO.GET` over TCP: reject with same error if stored object size > threshold
- EFA clients are not subject to this limit — they use multi-buffer parallel I/O (Cases 1/2 above)

**Future (v2+):** If Valkey adds a streaming/incremental module API for reading from client socket and writing chunked replies, TCP could support larger objects. Until then, large objects require EFA.

---
## 8. Expanding and Shrinking of Segments

Expanding and shrinking applies only to **DRAMPool segments**. NVMePool segments are fixed at startup (sized for max concurrent I/O) and never resized — if NVMePool is exhausted, the module back-pressures new requests until buffers are freed.

This will be solved using a cron job from the Module that monitors memory usage using existing Module APIs.

### 8.1 When to Expand

| Trigger | Action |
|---------|--------|
| Allocation fails (no contiguous space in any segment) | Add segment immediately |
| Segment Memory Utilization > 80% sustained | Add segment proactively |

### 8.2 When to Shrink

| Trigger | Action |
|---------|--------|
| Valkey `used_memory` approaching `maxmemory` | Shrink to give memory back. There are caveats explained in sections below |

### 8.3 How Expansion Works

1. Allocate new segment (contiguous region, e.g., 16GB)
2. Register with io_uring: `IORING_UNREGISTER_BUFFERS` → append new iovec → `IORING_REGISTER_BUFFERS`
3. Add to allocator: `talc.claim(Span::new(base, base + size))`
4. New allocations can immediately use the new segment

Cost: ~2-10ms (Need to validate through tests) for the unregister/re-register cycle. In-flight ReadFixed/WriteFixed ops already submitted are unaffected (kernel has their pages pinned). New submissions wait briefly.

### 8.4 How Shrinking Works

**DRAM+NVMe mode:**
```
1. Pick segment with lowest utilization (live bytes allocated / segment capacity)
2. Mark segment DRAINING (no new allocations from it)
3. Wait for in-flight I/O targeting this segment to complete
4. Evict DRAMPool objects in this segment (data safe on NVMe)
   - Drop their ObjectContexts → Buffers logically freed
5. IORING_UNREGISTER_BUFFERS → remove segment from iovec array → IORING_REGISTER_BUFFERS
6. Release segment memory to OS
```
Eviction is cheap — objects survive on NVMe. Next GET is a DRAMPool miss.

**DRAM-only mode:**
```
1. Pick segment with lowest utilization (live bytes allocated / segment capacity)
2. Mark segment DRAINING
3. Wait for in-flight I/O to complete
4. Evacuate remaining live objects:
   - For each live object: alloc in another segment, memcpy, update ObjectContext
5. IORING_UNREGISTER → remove → IORING_REGISTER
6. Release segment memory to OS
```
Evacuation is mandatory — data only exists in DRAM. Cannot shrink if remaining segments are too full to absorb evacuated objects (reject the shrink request).

Evacuation cost: proportional to live data in segment. 5% utilized 16GB segment = ~800MB copy = ~80ms.

## 9. Open Questions

1. Segment size? 4GB (granular shrink) vs 16GB (fewer segments, less overhead)? Config knob — needs production data.
2. Should we use Scale Out and Scale In to handle overly fragmented segments? Requires live transition (drain + evacuate). May be over-engineering — talc free-coalescing may be sufficient. Needs tests to determine fragmentation rate in practice.
