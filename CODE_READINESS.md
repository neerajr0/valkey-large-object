# Code Readiness for External Contributions

Status of structural work needed before opening this repo to other engineers.

## Completed

| Item | What was done |
|------|--------------|
| PoolBuffer in wrong module | Moved to `src/types.rs` (shared between storage + transport) |
| buf_index_for O(N) scan | Added `idx: u16` to PoolBuffer. O(1) lookup everywhere. |
| No NvmeEngine trait | `trait NvmeEngine` in `storage/mod.rs`. `UringNvmeEngine` in `uring.rs` implements it. Pool calls via trait. |
| SESSIONS lock on poller thread | Changed to `Arc<Session>`, cloned before entering io_uring callback. |
| No centralized error strings | `src/errors.rs` with `pub const` literals. All commands reference constants. |
| No lifecycle documentation | Init-order comment block at top of `lib.rs`. |
| No test infrastructure | `build.sh` (cargo build + clone valkey + clone test framework + pytest). Tests in `tests/` following valkey-bloom pattern. |
| Mixed responsibility in pool.rs | `FdPool` extracted to `src/storage/fd_pool.rs`. |
| Unsafe code without safety comments | `// SAFETY:` above every unsafe block. |
| OID counter restart collision | Resolved by design: RDB load (`lo_rdb_load` → `fetch_max`) is the authoritative source. Files are deleted on clean shutdown. No filesystem scan needed. |

## Remaining (short-term)

### Callback nesting in command handlers
- LO.SET EFA path has 3-4 nested closures
- Blocks: replication propagation, tracing, retry logic
- **Fix:** State machine or pipeline with `RequestContext` struct. Do before replication work lands.
- **Note:** Currently in section "Longer-Term" because it's a major refactor best done alongside replication.

## Longer-Term (beyond initial contribution readiness)

### Keyspace write inside unblock_client (wrong thread)
- `ReplyData::SetOk` opens a writable key via `ThreadSafeContext` on the poller thread
- Mixes I/O completion with keyspace mutation
- **Blocked on:** valkeymodule-rs crate change — needs `reply_callback` support upstream

### Graceful shutdown / in-flight drain
- `storage::shutdown()` is a stub
- If server shuts down with io_uring ops in-flight, BlockedClients never get unblocked
- **Fix:** Wire `UringNvmeEngine::shutdown()` into module deinit. Drain pending → fire error callbacks.

### Metrics / observability hooks
- No counters for pool ops, io_uring submissions, completions, errors
- **Fix:** Atomic counters exposed via INFO section. Add incrementally.

### Unit tests
- ObjectId generation, PoolBuffer lifecycle, FdPool open/close/reuse, NvmeEngine mock

### Storage-layer retryable error enum
- io_uring transient failures (EAGAIN, fd exhaustion, ring full) vs permanent (ENOENT, corruption)
- **Fix:** Enum with `is_retryable()` for caller back-pressure logic.

---

## File Layout (current)

```
src/
  lib.rs              — module entry, config, lifecycle docs
  types.rs            — PoolBuffer (shared between storage + transport)
  errors.rs           — centralized error string constants
  data_type.rs        — LoValue, ObjectId, RDB callbacks, free callback
  commands/mod.rs     — LO.HELLO, LO.GET, LO.SET handlers
  storage/
    mod.rs            — Storage trait + NvmeEngine trait
    pool.rs           — buffer pool (ArrayQueue) + io dispatch
    fd_pool.rs        — pre-opened fd management
    uring.rs          — UringNvmeEngine (io_uring ring + poller thread)
  transport/mod.rs    — EFA stubs (replaced by libefa-rs crate later)
tests/
  conftest.py         — pytest setup, imports valkeytestframework
  valkey_largeobj_test_case.py  — base class (server spawn + module load)
  test_largeobj_basic.py        — 7 integration tests
build.sh              — cargo build + valkey clone + test framework + pytest
bench.sh              — performance benchmarks (fio + valkey-benchmark)
requirements.txt      — valkey + pytest
CODE_READINESS.md     — this file
STATUS.md             — component-level work tracker
```
