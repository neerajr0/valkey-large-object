# Storage Design

**Date:** 2026-08-21  **Status:** Draft  **Author:** @KarthikSubbarao

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

### 4.2 Approach B: Arena with Slab Allocator (talc) [Recommended]

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
3. Passively wait for evictions (valkey core driven, ie, object free) / deletions to empty it, OR actively evacuate remaining objects to other segments (memcpy + update HashMap offsets).
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

### 4.5 Decision: Approach B (Separate Segments Per Layer)

We use Approach B (talc arena). Object sizes are unknown at design time — talc provides exact-fit allocation regardless of what sizes production traffic produces.

**Two separate talc instances, each with their own segments:**

```
IoPool:      Segment(s) (e.g., 2GB)   — own Mutex<Talc>, high churn, short-lived ObjectContexts
DRAMCache:   Segment(s) (e.g., 16GB)  — own Mutex<Talc>, low churn, long-lived ObjectContexts

io_uring registration: [iovec{IoPool_seg, 2GB}, iovec{DRAMCache_seg, 16GB}] — 2 entries
```

**Why separate segments per layer:**
- Prevents lifetime-mixing fragmentation: IoPool high-churn alloc/free cycles cannot create holes between long-lived DRAMCache objects
- Each layer's talc instance only sees objects of similar lifetime — fragmentation is self-healing (IoPool: FIFO churn reclaims space naturally; DRAMCache: infrequent evictions don't leave Swiss-cheese)
- Independent sizing: IoPool sized for max concurrent I/O, DRAMCache sized for working set
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
Objects exceeding the largest supported size are rejected at `LO.SET` with `ERR object exceeds max buffer size`. No multi-buffer stitching, no fallback path. Client (LMCache) already chunks by layer/block and can chunk smaller. Module advertises max size via config.

---

## 5. Operating Modes

Both approaches follow the same command-level flow. "Alloc" and "free" refer to `talc.alloc`/`talc.free` from the shared segment pool. The resulting memory is pre-registered with io_uring and EFA.

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

**Key property:** Data exists ONLY in DRAM. Eviction on the DRAMCache layer = data loss = equivalent to DEL. Only Valkey's maxmemory eviction policy triggers this.

### 5.2 DRAM + NVMe Mode

All objects persist on NVMe (write-through). DRAM is a read cache — hot objects promoted on GET only.

```
LO.SET key len <payload> [rkey remote_addr]:
  1. If key has existing DRAMCache entry: free that buffer (invalidate stale data)
  2. Alloc buffer (registered memory)
  3a. TCP: copy payload into buffer
  3b. EFA: fi_read from client GPU into buffer (zero-copy)
  4. io_uring WriteFixed to NVMe (O_DIRECT, 512-byte aligned length)
  5. Free buffer (no DRAMCache caching on write path)
  6. Track file in key/object: key → NVMe location only

LO.GET key [rkey remote_addr len] — DRAMCache hit:
  7. Lookup in HashMap → buffer is cached in DRAMCache
  8a. TCP: reply from buffer
  8b. EFA: fi_write from buffer (zero-copy)

LO.GET key [rkey remote_addr len] — DRAMCache miss:
  9. Alloc buffer (registered memory)
  10. io_uring ReadFixed from NVMe into buffer (O_DIRECT)
  11a. TCP: reply from buffer
  11b. EFA: fi_write from buffer (zero-copy)
  12. Promote to DRAMCache (based on access frequency / policy) + Track buffer in ObjectContext

Eviction (DRAMCache pressure):
  13. Free buffer. Data safe on NVMe.
  14. Remove DRAMCache pointer from HashMap (keep NVMe reference)

DEL key:
  15. Free buffer (if cached in DRAM)
  16. Delete NVMe file
  17. Remove from HashMap
```

**Key properties:**
- SET always invalidates any stale DRAMCache entry then writes to NVMe. No caching on write path.
- DRAMCache is populated only on the GET path (promotion). Admission policy is a single decision point at step 12.
- DRAMCache is expendable. Eviction is cheap (data persists on NVMe). Cache miss costs one NVMe read (~15μs on i8ge).

---

## 6. Large Object I/O: Multi-Buffer Parallel

Objects can be much larger than a single I/O buffer (e.g., 10GB object with 64MB buffers). The module handles this by streaming through multiple buffers in parallel — never allocating the full object in DRAM at once.

**Transport-dependent behavior:**
- **TCP:** Valkey's command dispatch accumulates the full payload in `querybuf` before calling the module handler. Replies use single-allocation `VM_ReplyWithStringBuffer`. There is no incremental/streaming API. **TCP rejects objects above a configurable max size** (e.g., 256MB). Multi-buffer parallel I/O applies only to EFA.
- **EFA:** The module controls chunk size via fi_read/fi_write. Multi-buffer parallel I/O is the primary large object path.

### 6.1 NVMe Representation: Single File Per Object

Each object is one contiguous file on NVMe regardless of size:

```
Object "key123" (10GB):
  NVMe: /data/lo-data/00000042.dat   (10GB file, XFS extent-allocated)
  LoValue: {oid=42, size=10GB, fd_idx=7}
```

**Why single file, not multiple:**
- io_uring parallelizes via offset within one fd — no need for multiple files
- NVMe controller sees LBAs, not files. Same parallelism either way.
- Atomicity: single unlink = atomic delete. No partial-object cleanup.
- Simpler fd management, RDB serialization, and error handling.
- mdraid0 stripes across all drives regardless of file count.

### 6.2 Buffer Size Translation (Client Args → Server Chunks)

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
               EFA: fi_write each chunk to client at addr + i*chunk_size
```

**Key invariant:** The client provides `total_len` and a destination (TCP socket or EFA region). The server partitions into `ceil(total_len / lo-buffer-size)` internal operations. The last operation uses `len = total_len % lo-buffer-size` (partial chunk). O_DIRECT write path pads the final write to 512-byte boundary on disk (§4.6). The chunk boundary is invisible to the client protocol.

### 6.3 Parallel Write (LO.SET via EFA)

For EFA, the module pulls data from client GPU memory in chunks using fi_read:

```
LO.SET key 10GB [rkey remote_addr len] (EFA path):
  1. Create/open NVMe file, fallocate(10GB)
  2. fi_read chunk from client GPU into registered buffer
  3. Submit N WriteFixed SQEs in parallel:
     SQE[0]: WriteFixed(fd, offset=0,             buf_idx=0, len=chunk_size)
     SQE[1]: WriteFixed(fd, offset=chunk_size,    buf_idx=1, len=chunk_size)
     SQE[2]: WriteFixed(fd, offset=2*chunk_size,  buf_idx=2, len=chunk_size)
     ...
  4. Reap CQEs. As each completes, return buffer to pool, fi_read next chunk.
  5. Fence: wait for ALL writes to complete before ACKing to client.
```

Pipeline depth (buffers in flight simultaneously) is bounded by available pool buffers and NVMe queue depth.

### 6.4 Parallel Read (LO.GET via EFA)

Module reads from NVMe in parallel chunks and writes to client GPU:

```
LO.GET key [rkey remote_addr len] (EFA path, cache miss):
  1. Lookup LoValue → fd, size
  2. Checkout N buffers from pool (e.g., 4 × chunk_size)
  3. Submit N ReadFixed SQEs in parallel:
     SQE[0]: ReadFixed(fd, offset=0,             buf_idx=0, len=chunk_size)
     SQE[1]: ReadFixed(fd, offset=chunk_size,    buf_idx=1, len=chunk_size)
     SQE[2]: ReadFixed(fd, offset=2*chunk_size,  buf_idx=2, len=chunk_size)
     SQE[3]: ReadFixed(fd, offset=3*chunk_size,  buf_idx=3, len=chunk_size)
  4. As each CQE completes:
     a. fi_write chunk to client GPU (at sequential offset within client's region)
     b. Return buffer to pool
     c. Submit next ReadFixed SQE for the next file offset
  5. Repeat until entire object transferred.
```

**Pipeline overlap:** Read and send happen concurrently. While buffer 0 is being sent to client, buffers 1-3 are being filled from NVMe. This keeps both NVMe bandwidth and network bandwidth saturated.

### 6.5 Chunk Size Selection

| Chunk size | Buffers for 10GB | SQE count | Tradeoff |
|---|---|---|---|
| 4MB | 2500 (sequential) | 2500 | Minimal pool usage, high SQE overhead |
| 64MB | 160 (sequential) | 160 | Good balance |
| 256MB | 40 (sequential) | 40 | Fewer SQEs, larger pool reservation |

With pipelining (4–8 buffers in flight), only 4–8 buffers are checked out at once regardless of object size. Total SQE count determines total I/O time; pipeline depth determines pool pressure.

**Recommended:** chunk_size = `lo-buffer-size` config value. No special "large object" buffer — reuse the same shared segment allocator. The chunking is purely an I/O scheduling pattern, not a storage decision.

### 6.6 DRAMCache for Large Objects

Large objects (>256MB) are **never promoted to DRAMCache**:
- Cost/benefit is poor (256MB DRAM for one key vs serving hundreds of smaller hot objects)
- Promotion threshold is configurable: `dram-cache-max-object-size` (default: 256MB)
- Objects above this threshold always read from NVMe via the parallel pipeline
- Objects below this threshold can be promoted to DRAMCache on repeated access (Section 5.2 step 12)

### 6.7 EFA Transport for Large Objects

Two cases for how EFA handles large objects:

**Case 1: Client provides multiple address/len pairs in the command**

The command itself includes multiple regions. Server performs parallel fi_read/fi_write across all of them simultaneously:

```
LO.SET key <total_len> <n_regions> <rkey1 addr1 len1> <rkey2 addr2 len2> ...
LO.GET key <n_regions> <rkey1 addr1 len1> <rkey2 addr2 len2> ...
```

- Client has multiple GPU memory registrations (e.g., multi-GPU, or multiple buffers on one GPU)
- Server fi_reads/fi_writes in parallel across all provided regions
- Each region maps to one or more NVMe chunks
- Client controls the parallelism and memory layout explicitly

**Case 2: Client provides a single large address/len that exceeds comfortable buffer size**

The client provides one region larger than the server's buffer size. Two sub-options:

- **Reject:** Return ERR if `len > max_efa_transfer_size`. Simple, forces client to use Case 1.
- **Accept and split (preferred — product requirement):** Server internally splits the single large region into chunk-sized fi_write/fi_read calls at sequential offsets within the client's region:

```
Client provides: rkey=R, remote_addr=A, len=10GB
Server internally:
  fi_write(buf[0], chunk_size, dest, A + 0*chunk_size, R, ...)
  fi_write(buf[1], chunk_size, dest, A + 1*chunk_size, R, ...)
  fi_write(buf[2], chunk_size, dest, A + 2*chunk_size, R, ...)
  ...
```

- Transparent to client — single registration, single addr, server handles the chunking
- Server pipelines: NVMe ReadFixed fills buffer[i], fi_write sends it, buffer returned to pool
- No API change from the small-object case — same command syntax, server detects large size and splits

**v1 decision:** Reject over TCP for large objects. Accept over EFA using Case 2 (server-side split) to meet the product requirement. Case 1 deferred to v2 if multi-GPU clients need explicit region control.

### 6.8 TCP Path: Large Object Rejection

Valkey's RESP command dispatch accumulates the full payload in `client->querybuf` before calling the module handler. Replies use single-allocation `VM_ReplyWithStringBuffer`. There is no incremental streaming API for either direction.

**Consequence:** A 10GB LO.SET over TCP requires 10GB in querybuf before the module even runs. This is untenable.

**v1 behavior:**
- `LO.SET` over TCP: reject with `ERR object exceeds max TCP size` if payload > `lo-max-tcp-object-size` (configurable, default 256MB)
- `LO.GET` over TCP: reject with same error if stored object size > threshold
- EFA clients are not subject to this limit — they use multi-buffer parallel I/O (Cases 1/2 above)

**Future (v2+):** If Valkey adds a streaming/incremental module API for reading from client socket and writing chunked replies, TCP could support larger objects. Until then, large objects require EFA.

---

## 7. Data Type Struct and Object References

### 7.1 LoValue (Per-Key Metadata)

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
- **DRAMCache:** `HashMap<ObjectId, ObjectContext>` — buffers in DRAMCache segments, populated on GET hits, evicted independently
- **IoPool inflight:** transient `ObjectContext` per in-flight request — buffers in IoPool segments, dropped on completion
- **Allocators:** `Mutex<Talc>` per layer — `dram_talc` for DRAMCache, `io_talc` for IoPool (§4.5)

### 7.2 ObjectContext (Per-Object Runtime State)

Module-internal runtime companion to LoValue. Tracks the object's live buffer locations and access metadata. Not serialized — rebuilt on load, evicted independently of commands.

```rust
struct Buffer {
    segment_idx: u8,       // Which pinned segment this slice lives in
    offset: u64,           // Byte offset within that segment
    len: u32,              // This chunk's size
}
// Always within a registered segment → ReadFixed + EFA fi_write capable

struct ObjectContext {
    buffers: Vec<Buffer>,  // Ordered chunks. 1 for small objects, N for large.
    total_len: u64,        // Sum of all buffer lens = object size
}
```

Stored in: `HashMap<ObjectId, ObjectContext>`

- **Small object (1MB):** `buffers = [Buffer{seg=0, offset=0x5000, len=1MB}]`
- **Large object (50MB):** `buffers = [Buffer{seg=0, ...}, Buffer{seg=1, ...}, ...]` — chunks may span multiple segments
- **Not cached (cold on NVMe):** No entry in HashMap. LO.GET allocates transient buffers via IoPool, reads from NVMe, serves, then either promotes to DRAMCache (inserts ObjectContext) or frees.

Used by both:
- **DRAMCache:** Serving hits directly from buffers
- **IoPool:** Parallel ReadFixed/WriteFixed and EFA fi_write across all chunks

### 7.3 NVMe File Reference

Each object is one file: `/data/lo-data/{oid:016x}.dat`

- fd opened at LO.SET, held in FdPool (`HashMap<ObjectId, RawFd>`)
- Lookup: `fd_pool.get(object_id)` → RawFd for io_uring submission
- File size = `ceil(len / 512) * 512` (O_DIRECT 512-byte write alignment padding)
- Actual object length stored in `LoValue.len` (not derived from file size)
- On DEL: `fd_pool.remove(oid)` closes fd, then `unlink()` deletes file

### 7.4 ObjectContext Lifetimes

ObjectContext exists in two layers with different lifetimes. Same struct, same Buffer type, but allocated from **separate talc instances in separate segments** (§4.5).

**DRAMCache (long-lived):**
- ObjectContext created on cache promotion (LO.GET hit policy admits it)
- Buffers allocated from DRAMCache segment(s) via `dram_talc.lock().alloc()`
- Held in `HashMap<ObjectId, ObjectContext>` for the object's entire cached lifetime
- Buffers remain allocated and serve repeated LO.GET hits directly
- On DRAMCache eviction (policy-based — LRU/LFU/memory pressure): ObjectContext dropped → `dram_talc.lock().free()` for each buffer
- Object survives on NVMe. Next GET is a cache miss (IoPool serves it).

**IoPool (short-lived):**
- ObjectContext created per in-flight I/O request (or coalesced group of requests for same object)
- Buffers allocated from IoPool segment(s) via `io_talc.lock().alloc()`
- On request completion: ObjectContext dropped → `io_talc.lock().free()` for each buffer
- If promotion policy says yes: data copied from IoPool buffer into a fresh DRAMCache allocation, then IoPool buffer freed. (Cannot transfer ownership across segments — different talc instances.)

### 7.5 Relationship Diagram

Example: 50MB object cached in DRAMCache (long-lived). IoPool would look the same structurally but with shorter-lived ObjectContexts in IoPool segments.

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
  │ DRAMCache Segment 0 (16GB, io_uring buf_index=0, dram_talc) │
  │ [...buf[0]...][...buf[1]...][...buf[3]...][...buf[5]...]    │
  └─────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────┐
  │ DRAMCache Segment 1 (16GB, io_uring buf_index=1, dram_talc) │
  │ [...buf[2]...][...buf[4]...][...buf[6]...]                  │
  └─────────────────────────────────────────────────────────────┘
```

---

## 8. Expanding and Shrinking of Segments

Expanding and shrinking applies only to **DRAMCache segments**. IoPool segments are fixed at startup (sized for max concurrent I/O) and never resized — if IoPool is exhausted, the module back-pressures new requests until buffers are freed.

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
4. Evict DRAMCache objects in this segment (data safe on NVMe)
   - Drop their ObjectContexts → Buffers logically freed
5. IORING_UNREGISTER_BUFFERS → remove segment from iovec array → IORING_REGISTER_BUFFERS
6. Release segment memory to OS
```
Eviction is cheap — objects survive on NVMe. Next GET is a DRAMCache miss.

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

1. Segment size? 4GB (granular shrink) vs 16GB (fewer segments, less overhead)?
2. Does `talc.claim()` support adding spans after initial creation? Must verify API.
3. Does `fi_mr_reg` on overcommitted mmap pin all pages immediately? If yes, virtual overcommit trick doesn't save physical memory. Test on i8ge.
4. Shrink trigger: how does the module learn about Valkey memory pressure? `VM_GetServerInfo` polling? A callback from Valkey? Memory hooks?
5. Should we expose pool/arena stats via `LO.INFO` for observability?
6. Should we use a Scale Out and Scale In to handle overly fragmented Segments? We will need a live transition. IMO, it might be over-engineering and we need tests to see how common fragmentation is in talc. free operations on talc already work to mitigate fragmentation
