# ValkeyLargeObj Module — Status

Single source of truth for what's done and what's remaining.

## Done

### Module Core
- Module registration, config API, lifecycle, tokio runtime
- LoValue data type, ObjectId, RDB save/load, free callback (DEL → delete NVMe)
- LO.HELLO, LO.GET (TCP + bench-mode), LO.SET (TCP) — full roundtrip working
- EFA paths stubbed (Session::write/read return Ok immediately)
- BlockClient/UnblockClient async chain, keyspace write on reply

### Storage
- Buffer pool: lock-free ArrayQueue, 4KB-aligned alloc, pool_get/pool_put
- PoolBuffer in `src/types.rs` with `idx: u16` (O(1) lookup)
- NvmeEngine trait (`storage/mod.rs`), UringNvmeEngine (`uring.rs`)
- io_uring: ring init, IORING_REGISTER_BUFFERS, ReadFixed/WriteFixed, CQ poller thread
- FdPool: open-once-per-object, reuse on reads, close on delete (`fd_pool.rs`)
- File management: tmp+rename atomicity, aligned reads, fallback paths
- OnceLock for engine (zero-cost after init)

### Code Quality
- `src/errors.rs` — centralized error constants
- `src/storage/fd_pool.rs` — extracted from pool.rs
- `// SAFETY:` on all unsafe blocks
- `Arc<Session>` — cloned before io_uring callback (no lock on poller thread)
- Lifecycle init-order docs in `lib.rs`
- `.gitignore`

### Testing & Benchmarks
- `build.sh` — cargo build + clone valkey from source + test framework + pytest
- Integration tests: 7 tests, valkey-bloom pattern (valkey-test-framework)
- `bench.sh` — per-size server restart, io-threads 8, taskset, --duration, c=750
- Benchmark results: 4KB=150K rps (io-threads 8), NVMe reads confirmed via /proc/diskstats

---

## Remaining — High Priority

| Item | Notes |
|------|-------|
| IORING_REGISTER_FILES | Pre-register fds with kernel. Eliminates fget/fput atomics per SQE. |
| Startup reconciliation | After crash: scan data_dir, delete orphaned .dat files not in keyspace. |
| Client disconnect cleanup | Remove EFA session from SESSIONS map on client disconnect. |
| Data type callbacks (copy, mem_usage) | Needed for COPY command, MEMORY USAGE. |

## Remaining — Medium Priority

| Item | Notes |
|------|-------|
| Multi-poller investigation | One io_uring ring may saturate on 16-drive i8g. Evaluate multiple rings. |
| O_TMPFILE atomic write | Replace tmp+rename with O_TMPFILE → linkat (no dir entry until commit). |
| Disk space accounting (max-bytes) | Enforce NVMe capacity limit, trigger eviction when approaching. |
| fallocate on write | Pre-allocate space to avoid extent allocation during write. |
| Pin/unpin tracking bitmap | Needed for buffer eviction to know which bufs are in-flight. |
| DRAM buffer pool eviction | LRU/clock when pool exhausted (future tiering layer). |
| INFO largeobj section | Module stats: objects stored, NVMe bytes, pool utilization, io_uring throughput. |

## Remaining — Lower Priority (do alongside feature work)

| Item | Notes |
|------|-------|
| Callback state machine | Replace nested closures with GetState/SetState enums + advance(). Do when replication needs to insert a step. See design below. |
| Keyspace write on main thread | Move keyspace mutation to reply_callback (blocked on valkeymodule-rs). |
| Graceful shutdown drain | Wire UringNvmeEngine::shutdown() into module deinit, drain in-flight ops. |
| Storage retryable error enum | EAGAIN/ring-full vs ENOENT/corruption. `is_retryable()` for back-pressure. |
| Unit tests | ObjectId, PoolBuffer lifecycle, FdPool, NvmeEngine mock. |
| io_uring SQ polling (SQPOLL) | Kernel-side submission polling. Burns a core but eliminates submit() syscall. |
| Multiple stripe directories | For multi-drive parallelism beyond LVM. |
| NVMe SMART monitoring | Periodic SMART data collection, early failure detection. |
| Oversized buffer path | One-off mmap + register for objects > pool_buf_size. |
| CI setup | Automated build + test on push. |

---

## Replication (not started — entire subsystem)

### Metadata Stream
- TIERING.REF command (propagates key+oid+len+crc via replication)
- Replication start/stop module hooks
- RDB load: build pull queue from refs
- Core state: VM_SetClusterFlags(NO_FAILOVER) while queue non-empty
- Core lag: VM_SetReplicationAckOffset — hold back ACK until hydration complete
- Cut-off period (grace before declaring pull failed)

### Data Pull Engine
- Pull queue (ObjectId queue from RDB refs + TIERING.REF stream)
- Pull engine thread (replica pulls via LO.GET from primary)
- Client pool for pull connections (auth/ACL/TLS)
- Congestion control (limit in-flight pulls)
- Queue drain detection → clear NO_FAILOVER, resume ACK offset
- Slot migration: OnSlotImportReadyCheck (return 0 until slot objects pulled)

---

## Transport (stubbed — replaced by libefa-rs crate)

All transport methods are stubs. Real implementation comes from external libefa-rs crate:
- EfaContext discovery (fi_getinfo, fi_fabric, fi_domain)
- Session creation (fi_endpoint, fi_av_insert)
- DMA write/read (fi_writemsg/fi_readmsg + CQ poll)
- Multi-EFA device load balancing
- Buffer dual-registration (io_uring + EFA on same pages)

---

## Callback State Machine Design (for replication phase)

**LO.GET states:**
```
TCP:   NvmeRead → Reply(bulk data)
EFA:   NvmeRead → EfaWrite → Reply(size integer)
```

**LO.SET states:**
```
TCP:   CopyInline → NvmeWrite → KeyspaceWrite → Replicate → Reply(OK)
EFA:   EfaRead → NvmeWrite → KeyspaceWrite → Replicate → Reply(OK)
```

**Structs:**
```rust
struct GetContext {
    buf: Option<PoolBuffer>,
    object_id: ObjectId,
    obj_len: u64,
    blocked_client: BlockedClient,
    session: Option<Arc<Session>>,   // None = TCP, Some = EFA
    efa_args: Option<(u32, u64)>,    // (region_idx, remote_offset)
}

struct SetContext {
    buf: Option<PoolBuffer>,
    key_name: Vec<u8>,
    obj_len: u64,
    blocked_client: BlockedClient,
    session: Option<Arc<Session>>,
    efa_args: Option<(u32, u64)>,
    object_id: Option<ObjectId>,     // set after NvmeWrite completes
    crc: Option<u32>,
}

enum GetState { NvmeRead, EfaWrite, Reply }
enum SetState { SourceData, NvmeWrite, KeyspaceWrite, Replicate, Reply }
```

**Transitions:**
```rust
fn advance_get(ctx: GetContext, state: GetState, result: Result<...>) {
    match state {
        NvmeRead => if ctx.session.is_some() { → EfaWrite } else { → Reply }
        EfaWrite → Reply
        Reply → unpin, pool_put, unblock_client
    }
}

fn advance_set(ctx: SetContext, state: SetState, result: Result<...>) {
    match state {
        SourceData → NvmeWrite  (EFA: buf filled from GPU. TCP: already filled.)
        NvmeWrite → KeyspaceWrite  (store oid+crc in ctx)
        KeyspaceWrite → Replicate  (open_key_writable, set_value)
        Replicate → Reply  (propagate TIERING.REF)
        Reply → unpin, pool_put, unblock_client(OK)
    }
}
```

**Assessment:** Current nesting is 2-3 levels (lo_get: 105 lines, lo_set: 116 lines). Readable today. The state machine adds ~100 lines for zero functional change. **Do alongside the first replication commit that inserts a step between NvmeWrite and Reply.**

---

## DRAM Tier (not started — future extension)

Buffer pool today is transient: GET reads NVMe → buf → reply → pool_put (buf contents discarded).
DRAM tier retains hot objects in pool buffers across requests — second GET for same key hits DRAM, not NVMe.

### Architecture

```
LO.GET key:
  1. pool.lookup(object_id) → HIT:  pin, reply from DRAM, unpin (no io_uring, no NVMe)
                            → MISS: pool.reserve() → io_uring read → reply → buf stays in pool (cached)
```

### Tasks

| Item | Notes |
|------|-------|
| Named buffers | Associate buf with object_id after read. Buf stays in pool keyed by OID. |
| Cache index (ObjectId → buf_idx) | O(1) lookup on GET hot path. HashMap or array if OIDs are dense. Main-thread only (no cross-thread sync for hits). |
| Eviction policy (LRU or clock) | When pool full and new object needs loading, evict coldest unpinned buffer. |
| Pin/unpin enforcement | Pin = buffer in-flight (io_uring read, EFA DMA, TCP reply). Cannot be evicted. Unpin = safe to evict. Bitmap or atomic per-buffer flag. |
| Admission policy | Not all objects should be cached. Options: cache-on-second-access, cache-if-size ≤ threshold, always-cache. |
| Invalidation on DEL/overwrite | LO.SET to existing key or DEL must remove the buffer from cache index. Free callback already deletes NVMe file — extend to clear cache entry. |
| Memory accounting | Report DRAM tier usage via INFO. Respect max-memory or dedicated pool-max-bytes config. |
| Warm-up on RDB load | Optionally pre-fill DRAM cache for first N objects during RDB load (configurable). |
| Interaction with eviction (NVMe tier) | If DRAM-cached object is evicted from NVMe tier (disk full), DRAM copy becomes authoritative until next write-back. |
| Metrics | Cache hit/miss ratio, eviction count, DRAM bytes used, hit latency vs miss latency. |

### Design Decisions (pending)

- **Pool size partitioning:** Fixed DRAM tier size (e.g., 50% of pool for cache, 50% for in-flight), or dynamic (any free buffer can become cache)?
- **Write-through vs write-back:** On LO.SET, always write to NVMe immediately (current behavior, DRAM is read cache only)? Or buffer writes in DRAM and flush lazily?
- **Consistency:** If same object is cached in DRAM and someone does LO.SET with new data, the DRAM copy must be invalidated atomically before the new write completes.
- **Interaction with replication:** Replica's DRAM cache is independent (populated by its own pull engine reads). No cache state in replication stream.
