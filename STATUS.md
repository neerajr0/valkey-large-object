# ValkeyLargeObj Module — Status & Remaining Work

## Legend
- ✅ Done (compiles, logic in place)
- 🟡 Stubbed (interface defined, placeholder impl)
- ❌ Not started

---

## Module Entry (`src/lib.rs`)

| Item | Status | Notes |
|------|--------|-------|
| Module registration (valkey_module! macro) | ✅ | LO.HELLO, LO.GET, LO.SET registered |
| Config via ValkeyModule Config API | ✅ | data-dir, pool-buf-size, pool-buf-count, max-bytes, transport-threads, bench-mode |
| Config validator (4KB alignment) | ✅ | Rejects at CONFIG SET time |
| module_args_as_configuration | ✅ | loadmodule args map to configs |
| Tokio runtime (module-owned) | ✅ | Spawns on init. Transport exposes async fn — internal CQ polling opaque. No handle passed. |
| Lifecycle: init → register → shutdown | ✅ | Follows interface doc steps 1-4, 11-12 |

---

## Data Type Layer (`src/data_type.rs`)

| Item | Status | Notes |
|------|--------|-------|
| LoValue struct (object_id, len, crc32c) | ✅ | |
| ObjectId (deterministic file path) | ✅ | `{data_dir}/{oid:016x}.dat` |
| OID monotonic counter | ✅ | AtomicU64, updated on RDB load |
| RDB save/load callbacks | ✅ | |
| Free callback (DEL → delete NVMe file) | ✅ | |
| Data type callbacks (copy, digest, mem_usage) | ❌ | Optional but good for production |

---

## Commands (`src/commands/mod.rs`)

| Item | Status | Notes |
|------|--------|-------|
| LO.HELLO — parse peer addr + regions, create session | ✅ | |
| LO.GET — EFA path (NVMe read → RDMA write) | 🟡 | Stubbed — EFA transport returns immediate Ok() |
| LO.GET — TCP path (NVMe read → bulk reply) | ✅ | NVMe read via io_uring → reply with bulk bytes. Bench-mode returns size only. |
| LO.SET — EFA path (RDMA read → NVMe write) | 🟡 | Stubbed — EFA transport returns immediate Ok() |
| LO.SET — TCP path (inline bulk → NVMe write) | ✅ | Copies arg bytes into pool buf, writes to NVMe, stores LoValue in keyspace |
| BlockClient/UnblockClient async chain | ✅ | ThreadSafeContext used for reply + keyspace write |
| Reply callback stores LoValue in keyspace (SET) | ✅ | Opens key writable, sets module type value |
| Bench-mode config (reply size only on GET) | ✅ | `bench-mode yes` loadarg — skips bulk copy, returns integer |
| Session store (per-client HashMap) | ✅ | |
| Client disconnect cleanup | ❌ | Should remove session on disconnect |

---

## Storage Layer (`src/storage/`)

### Buffer Pool (`pool.rs`)

| Item | Status | Notes |
|------|--------|-------|
| 4KB-aligned allocation | ✅ | `alloc_zeroed` with Layout alignment |
| Lock-free free list (crossbeam ArrayQueue) | ✅ | Replaced Mutex<VecDeque>. Zero contention between main thread and poller. |
| pool_get / pool_put | ✅ | Lock-free (ArrayQueue pop/push) |
| pin / unpin | 🟡 | API defined, currently no-op (free list removal acts as implicit pin) |
| Pin/unpin tracking bitmap | ❌ | Needed for eviction to know which bufs are safe |
| DRAM buffer pool eviction | ❌ | When pool exhausted, evict unpinned bufs (LRU or clock) |
| Pool stats (in-use count, hit rate) | ❌ | |
| Oversized buffer path (objects > pool_buf_size) | ❌ | One-off mmap + register for large objects |
| buf_index_for O(1) lookup | ❌ | Currently O(N) linear scan over all buffers to find index. Replace with HashMap<ptr,idx> or store index in PoolBuffer struct. ~5-10% overhead at 150K rps. |

### io_uring Engine (`uring.rs`)

| Item | Status | Notes |
|------|--------|-------|
| io_uring engine OnceLock (zero-cost access) | ✅ | Replaced Mutex<Option<UringEngine>>. Initialized once at module load, zero overhead on hot path. |
| io_uring ring init | ✅ | `IoUring::new(256)` |
| IORING_REGISTER_BUFFERS (pool buffers) | ✅ | Pins pages once at startup |
| ReadFixed opcode | ✅ | Uses registered buffer index |
| WriteFixed opcode | ✅ | |
| CQ poller thread | ✅ | Drains channel → submits SQEs → reaps CQEs → fires callbacks |
| Fallback to regular Read if register fails | ✅ | |
| Error drain loop (if io_uring unavailable) | ✅ | |
| Graceful shutdown | ✅ | Drains pending ops then exits |
| IORING_REGISTER_FILES (pre-registered fds) | ❌ | Kernel optimization: eliminates fget/fput atomics per SQE. Requires fd pool as prerequisite (fds must stay open). |
| Fd pool (open once per object, reuse on reads) | ✅ | HashMap<ObjectId, RawFd> + RwLock. Open once on first write, reuse on reads. |
| Multi-poller investigation | ❌ | One poller may saturate on multi-NVMe (16-drive i8g). Evaluate multiple rings. |
| O_TMPFILE atomic write | ❌ | Currently uses tmp+rename. O_TMPFILE → linkat is cleaner (no dir entry until commit). |
| io_uring SQ polling (IORING_SETUP_SQPOLL) | ❌ | Kernel-side submission polling — eliminates submit() syscall. Burns a core. |
| Batched submissions | 🟡 | Drains up to 64 per loop. Not adaptive. |
| Aligned read length (4KB ceiling for O_DIRECT) | ✅ | `align_up()` rounds to 4KB boundary |

### NVMe File Management

| Item | Status | Notes |
|------|--------|-------|
| Write: create file, write data, crc32c | ✅ | tmp + rename atomicity |
| Read: open file, submit ReadFixed | ✅ | |
| Delete: unlink file | ✅ | Via free callback |
| Disk space accounting (max-bytes enforcement) | ❌ | |
| Disk eviction policy | ❌ | When approaching max-bytes, evict coldest objects |
| Startup reconciliation (NVMe dir vs keyspace) | ❌ | Delete orphaned files after crash |
| fallocate on write (pre-allocate space) | ❌ | Avoids extent allocation during write |
| Multiple stripe directories | ❌ | Kevin's POC had this for multi-drive parallelism |
| NVMe SMART logging/health monitoring | ❌ | Periodic SMART data collection, early failure detection |

---

## Transport Layer (`src/transport/mod.rs`)

| Item | Status | Notes |
|------|--------|-------|
| PoolBuffer shared type | ✅ | `{ ptr: *mut u8, len: usize }` |
| EfaAddress type ([u8; 32]) | ✅ | |
| ClientRegion (rkey: u64, remote_addr, len) | ✅ | |
| TransportError enum | ✅ | |
| EfaContext struct + init | 🟡 | Stubbed — returns `available: false` on dev desktop |
| EfaContext::register_buffers (fi_mr_reg) | 🟡 | No-op when EFA unavailable |
| Session::new (fi_av_insert peer) | 🟡 | Stores regions, no actual fi_* calls |
| Session::write (fi_writemsg → CQ callback) | 🟡 | Immediate Ok(()) stub |
| Session::read (fi_readmsg → CQ callback) | 🟡 | Immediate Ok(()) stub |
| Session::close | 🟡 | No-op |
| server_addrs (fi_getname) | 🟡 | Returns empty vec |
| Actual libfabric FFI integration | ❌ | Kenny's transport crate will provide this |
| Multi-EFA device LB (best-of-two) | ❌ | |
| CQ poller tasks on tokio runtime | ❌ | |
| Dual-registration (same pages to io_uring + EFA) | 🟡 | Architecture defined, EFA side stubbed |

---

## Replication

### Replication Control (metadata stream)

| Item | Status | Notes |
|------|--------|-------|
| TIERING.REF command | ❌ | Propagates (key, oid, len, crc) via replication stream to replicas |
| Replication start/stop callbacks | ❌ | Module hooks for when replication begins/ends |
| RDB save: write refs (not data) | ✅ | RDB callbacks save oid/len/crc only |
| RDB load: build pull queue from refs | ❌ | On replica RDB load, queue all OIDs for pulling |
| Core replication state reflection | ❌ | VM_SetClusterFlags(NO_FAILOVER) while queue non-empty |
| Core replication lag reflection | ❌ | VM_SetReplicationAckOffset — hold back ACK so lag metric reflects pending pulls |
| Cut-off period | ❌ | Grace period before declaring pull failed / giving up |

### Replication Data Pull Engine

| Item | Status | Notes |
|------|--------|-------|
| Pull queue (ObjectId queue) | ❌ | Built from RDB refs + TIERING.REF stream |
| Pull engine thread/task | ❌ | Replica pulls objects from primary via LO.GET |
| Client pool for pull connections | ❌ | Pool of authenticated connections to primary |
| Auth/ACL/TLS for pull clients | ❌ | Replica→primary auth, encryption |
| Congestion control | ❌ | Backpressure — limit in-flight pulls to avoid overwhelming primary |
| Pull completion → clear queue entry | ❌ | On successful pull, write to NVMe + remove from queue |
| Queue drain detection | ❌ | When queue empty, clear NO_FAILOVER flag + resume ACK offset |
| Slot migration: OnSlotImportReadyCheck | ❌ | Return 0 until all objects for slot are pulled |

---

## Observability

| Item | Status | Notes |
|------|--------|-------|
| INFO largeobj section | ❌ | Module stats in INFO output |
| Objects stored count | ❌ | Total LoValues in keyspace |
| NVMe bytes used | ❌ | Total bytes on disk |
| Buffer pool utilization | ❌ | in-use / total buffers |
| io_uring submissions/completions | ❌ | Throughput counters |
| io_uring latency histogram | ❌ | p50/p99 read/write latency |
| EFA sessions active | ❌ | Count of LO.HELLO sessions |
| EFA bytes transferred | ❌ | RDMA read/write bytes |
| Pull queue depth | ❌ | Pending pulls on replica |
| Pull throughput (objects/sec, bytes/sec) | ❌ | |
| NVMe SMART health | ❌ | Periodic SMART data, early failure detection |

---

## Integration & Testing

| Item | Status | Notes |
|------|--------|-------|
| Builds clean (cargo build) | ✅ | Produces libvalkey_largeobj.so |
| Load into Valkey and run LO.SET/LO.GET | ✅ | Full roundtrip verified: SET writes to NVMe + keyspace, GET reads and returns bulk data |
| Bench-mode perf test (gp3 baseline) | ✅ | c=1: ~2K rps, c=8: ~4.3K rps (gp3 IOPS ceiling). Matches expected 52-74x slower than NVMe. |
| Basic integration test (TCP path) | ❌ | Automated test suite |
| EFA integration test (requires i8g with EFA ENI) | ❌ | |
| Benchmark on i8ge (compare to NVMEBenchmark) | ❌ | Expected: ~72K rps/c=1, ~166K rps/c=100 at 4KB |
| CI setup | ❌ | |

---

## Ownership

| Layer | Primary Owner | Helper/Next Stage |
|-------|--------------|-------------------|
| Module entry + config + lifecycle | Karthik | — |
| Data type + commands + BlockClient wiring | Karthik | — |
| Transport crate (libefa-rs) | Kenny | — |
| io_uring engine + NVMe storage | Karthik (scaffold) | **NVMe engineer** (productionize: REGISTER_FILES, fd pool, multi-poller, eviction, O_TMPFILE, reconciliation) |
| Replication (TIERING.REF, pull, hydration) | TBD | — |
| Slot migration | TBD | — |
