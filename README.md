# valkey-bigobj NVMe Benchmark Results

## Hardware

- **Instance:** i8ge.48xlarge (192 vCPU, 1.5 TiB RAM)
- **Storage:** 16 × 6.8 TB NVMe instance store drives, LVM-striped (110 TB total)
- **Filesystem:** XFS (agcount=256, noatime, discard, inode64)
- **Architecture:** aarch64 (Graviton)

## Module Architecture

- Single io_uring poller thread (256-depth ring)
- Pre-allocated buffer pool (configurable size/count via module load args)
- Pre-opened O_DIRECT fd pool (no `open()` syscall per read)
- `BO.GET` blocks client → submits to io_uring → unblocks with `OK <len>` (no value bytes over TCP)

## Raw NVMe Baseline (fio, 8 jobs × depth 64)

| Block Size | IOPS | Bandwidth | p50 Latency |
|---|---|---|---|
| 4 KB | **1,417,000** | 5.5 GB/s | 355 μs |
| 1 MB | **52,000** | 50.8 GB/s | 9.8 ms |
| 50 MB | **962** | 47.0 GB/s | 507 ms |

At 1 MB+, the array is bandwidth-saturated at ~50 GB/s. At 4 KB, it's IOPS-limited at 1.4M.

## Module Benchmark Results (valkey-benchmark)

| Object Size | Clients | RPS | p50 Latency | p99 Latency | Efficiency vs fio |
|---|---|---|---|---|---|
| 4 KB | 750 | **157,000** | 2.4 ms | 2.7 ms | 11% of raw IOPS |
| 1 MB | 750 | **27,350** | 27.4 ms | 32.3 ms | 53% of raw IOPS |
| 50 MB | 20 | **499** | 26.8 ms | 147 ms | 52% of raw IOPS |

## Analysis

### 4 KB: IOPS gap (157K vs 1.4M)

The module achieves 11% of raw NVMe IOPS at 4 KB. The bottleneck is **not the NVMe device** — it's Valkey's single-threaded event loop serializing `BlockClient → channel.send → UnblockClient` at ~160K ops/sec. Perf profiling confirms module code uses 1.2% CPU; the rest is Valkey TCP networking + epoll + blocked client management.

To close the gap: need multi-threaded command dispatch (Valkey doesn't support this for blocked commands today) or multiple poller threads to increase io_uring concurrency.

### 1 MB: Near-optimal

At 1 MB objects, the module achieves 53% of fio throughput. This translates to **27.4 GB/s effective read bandwidth** (27,350 × 1 MB). The overhead is the single io_uring poller processing one read at a time per SQE slot, plus Valkey event loop serialization.

### 50 MB: Bandwidth-limited

At 50 MB objects, the module achieves 52% of fio (499 vs 962 IOPS). Effective bandwidth: **24.9 GB/s**. The gap is primarily io_uring queue depth — our single poller submits reads sequentially, while fio uses 8 jobs × 64 depth = 512 concurrent reads.

## Bottleneck Summary

| Object Size | Primary Bottleneck | Secondary |
|---|---|---|
| 4 KB | Valkey main thread (BlockClient serialization) | Single io_uring poller |
| 1 MB | Single io_uring poller (queue depth) | Valkey main thread |
| 50 MB | io_uring queue depth + NVMe bandwidth | — |

## Configuration

```bash
# Module load args:
--loadmodule bigobj.so data-dir /mnt/bigobj-data pool-buf-size <bytes> pool-buf-count <n>

# Examples:
pool-buf-size 4096 pool-buf-count 2048      # 4KB objects, 8MB pool
pool-buf-size 1048576 pool-buf-count 128     # 1MB objects, 128MB pool
pool-buf-size 52428800 pool-buf-count 32     # 50MB objects, 1.6GB pool
```

## Next Steps

1. **Multiple io_uring poller threads** — Scale read parallelism to match fio's 8-job model
2. **Batch io_uring submissions** — Submit N reads per `io_uring_enter()` instead of draining one-at-a-time
3. **Investigate Valkey io-threads interaction** — io-threads help with network I/O but not BlockClient dispatch
4. **EFA/RDMA path (DMA.GET)** — Bypass TCP entirely for GPU-attached clients (LMCache)
5. **Larger object tests with real KV cache chunks** — 256KB–84MB per LMCache chunk depending on model and chunking config
