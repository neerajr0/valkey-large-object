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
| Config via ValkeyModule Config API | ✅ | data-dir, pool-buf-size, pool-buf-count, max-bytes, transport-threads |
| Config validator (4KB alignment) | ✅ | Rejects at CONFIG SET time |
| module_args_as_configuration | ✅ | loadmodule args map to configs |
| Tokio runtime (module-owned) | ✅ | Spawns on init, transport borrows handle |
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
| TIERING.REF replication | ❌ | Propagates OID/len/crc to replicas |
| Replica RDB load (pull queue build) | ❌ | |
| Slot migration hooks (OnSlotImportReadyCheck) | ❌ | |

---

## Commands (`src/commands/mod.rs`)

| Item | Status | Notes |
|------|--------|-------|
| LO.HELLO — parse peer addr + regions, create session | ✅ | |
| LO.GET — EFA path (NVMe read → RDMA write) | 🟡 | BlockClient wiring in progress, ownership fix needed |
| LO.GET — TCP path (NVMe read → bulk reply) | 🟡 | Same — branch at completion callback |
| LO.SET — EFA path (RDMA read → NVMe write) | 🟡 | Same |
| LO.SET — TCP path (inline bulk → NVMe write) | 🟡 | Copies arg bytes into pool buf, submits write |
| BlockClient/UnblockClient async chain | 🟡 | Structure in place, Rust ownership issue with PoolBuffer move being resolved |
| Reply callback stores LoValue in keyspace (SET) | 🟡 | Currently replies with metadata string, needs keyspace write |
| Session store (per-client HashMap) | ✅ | |
| Client disconnect cleanup | ❌ | Should remove session on disconnect |

---

## Storage Layer (`src/storage/`)

### Buffer Pool (`pool.rs`)

| Item | Status | Notes |
|------|--------|-------|
| 4KB-aligned allocation | ✅ | `alloc_zeroed` with Layout alignment |
| Free list (VecDeque<usize>) | ✅ | |
| pool_get / pool_put | ✅ | |
| pin / unpin | 🟡 | API defined, currently no-op (free list removal acts as implicit pin) |
| Pin/unpin tracking bitmap | ❌ | Needed for eviction to know which bufs are safe |
| DRAM buffer pool eviction | ❌ | When pool exhausted, evict unpinned bufs (LRU or clock) |
| Pool stats (in-use count, hit rate) | ❌ | |
| Oversized buffer path (objects > pool_buf_size) | ❌ | One-off mmap + register for large objects |

### io_uring Engine (`uring.rs`)

| Item | Status | Notes |
|------|--------|-------|
| io_uring ring init | ✅ | `IoUring::new(256)` |
| IORING_REGISTER_BUFFERS (pool buffers) | ✅ | Pins pages once at startup |
| ReadFixed opcode | ✅ | Uses registered buffer index |
| WriteFixed opcode | ✅ | |
| CQ poller thread | ✅ | Drains channel → submits SQEs → reaps CQEs → fires callbacks |
| Fallback to regular Read if register fails | ✅ | |
| Error drain loop (if io_uring unavailable) | ✅ | |
| Graceful shutdown | ✅ | Drains pending ops then exits |
| IORING_REGISTER_FILES (pre-registered fds) | ❌ | Eliminates fget/fput atomics per op. Fixed fd table + slot index. |
| Fd pool (open once per object, reuse on reads) | ❌ | Currently open/close per request |
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

## Integration & Testing

| Item | Status | Notes |
|------|--------|-------|
| Builds clean (cargo build) | ✅ | Produces libvalkey_largeobj.so |
| Load into Valkey and run LO.SET/LO.GET | ❌ | Needs BlockClient chain fixed first |
| Basic integration test (TCP path) | ❌ | |
| EFA integration test (requires i8g with EFA ENI) | ❌ | |
| Benchmark (compare to NVMEBenchmark branch) | ❌ | |
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
