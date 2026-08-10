# valkey-bigobj NVMe Benchmark Results

## Hardware

- **Instance:** i8ge.48xlarge (192 vCPU, 1.5 TiB RAM)
- **Storage:** 16 × 6.8 TB NVMe instance store drives, LVM-striped (110 TB total)
- **Filesystem:** XFS (agcount=256, noatime, discard, inode64)
- **Architecture:** aarch64 (Graviton)

## Module Architecture

- Single io_uring poller thread (256-depth ring)
- **Registered buffers** (`IORING_REGISTER_BUFFERS` + `ReadFixed` opcode) — pages pinned once at startup, zero per-read page pinning
- Pre-opened O_DIRECT fd pool (no `open()` syscall per read)
- Configurable buffer pool size via module load args
- `BO.GET` blocks client → submits to io_uring → unblocks with `OK <len>` (no value bytes over TCP)

## Raw NVMe Baseline (fio, 8 jobs × depth 64, 16 drives striped)

| Block Size | IOPS | Bandwidth | p50 Latency |
|---|---|---|---|
| 4 KB | **1,417,000** | 5.5 GB/s | 355 μs |
| 1 MB | **52,000** | 50.8 GB/s | 9.8 ms |
| 50 MB | **962** | 47.0 GB/s | 507 ms |

At 1 MB+, the array is bandwidth-saturated at ~50 GB/s. At 4 KB, it's IOPS-limited at 1.4M.

## Module Benchmark Results (valkey-benchmark, 750 clients)

| Object Size | Clients | RPS | p50 Latency | Efficiency vs fio |
|---|---|---|---|---|
| 4 KB | 750 | **166,000** | 0.44 ms | 12% of raw IOPS |
| 1 MB | 750 | **47,200** | 15.2 ms | 91% of raw IOPS |
| 50 MB | 20 | **945** | 19.9 ms | 98% of raw IOPS |

## Analysis

### 4 KB: Valkey main thread limited

The module achieves 12% of raw NVMe IOPS at 4 KB. The bottleneck is Valkey's single-threaded event loop serializing `BlockClient → channel.send → UnblockClient` at ~166K ops/sec. Perf confirms: main thread at 87% CPU, poller at 53%. Module code (`get_status`) is 1% of total CPU.

### 1 MB: Near-optimal (91% of fio)

At 1 MB objects, the module achieves 91% of fio throughput (47.2K vs 52K IOPS). Effective read bandwidth: **47.2 GB/s**. The remaining 9% gap is Valkey event loop + channel overhead.

### 50 MB: Near-optimal (98% of fio)

At 50 MB objects, the module achieves 98% of fio (945 vs 962 IOPS). Effective bandwidth: **47.3 GB/s**. Virtually no software overhead at this object size — the NVMe bandwidth ceiling is the only limit.

## Perf Profile (after registered buffers optimization)

`gup_fast_fallback` (kernel page pinning) was **33% of poller CPU** before registered buffers. After: **completely eliminated** from all profiles.

### 4KB profile (top functions)
```
2.95%  bigobj-uring-po  [kernel] finish_task_switch    (idle/scheduling)
2.54%  valkey-server    [kernel] __wake_up_sync_key    (epoll wakeup on UnblockClient)
1.06%  valkey-server    libvalkey_bigobj.so  get_status (our code — 1%)
```

### 1MB profile (top functions)
```
7.05%  bigobj-uring-po  [kernel] blk_map_iter_next     (LVM stripe mapping)
4.81%  bigobj-uring-po  [kernel] __srcu_read_lock      (NVMe driver)
4.65%  bigobj-uring-po  [kernel] bio_split_io_at       (splitting I/O across stripes)
4.60%  bigobj-uring-po  [kernel] nvme_pci_setup_data_prp (DMA descriptor setup)
```

### 50MB profile (top functions)
```
12.29%  bigobj-uring-po  [kernel] blk_map_iter_next    (LVM stripe mapping)
8.50%   bigobj-uring-po  [kernel] __srcu_read_lock     (NVMe driver)
7.69%   bigobj-uring-po  [kernel] bio_split_io_at      (splitting I/O across stripes)
7.03%   bigobj-uring-po  [kernel] nvme_pci_setup_data_prp (DMA descriptor setup)
```

All top functions at 1MB/50MB are **irreducible kernel NVMe I/O costs** — not module overhead.

## Configuration

```bash
# Module load args:
--loadmodule bigobj.so data-dir /mnt/bigobj-data pool-buf-size <bytes> pool-buf-count <n>

# Examples:
pool-buf-size 4096 pool-buf-count 5000       # 4KB objects, 20MB pool
pool-buf-size 1048576 pool-buf-count 5000    # 1MB objects, 5GB pool
pool-buf-size 52428800 pool-buf-count 100    # 50MB objects, 5GB pool
```

## Bottleneck Summary

| Object Size | Primary Bottleneck | Module Overhead |
|---|---|---|
| 4 KB | Valkey main thread (166K cmd/s ceiling) | ~1% CPU |
| 1 MB | NVMe bandwidth (50 GB/s) | ~9% gap vs fio |
| 50 MB | NVMe bandwidth (50 GB/s) | ~2% gap vs fio |

## Key Optimization: Registered Buffers

Before: every io_uring read triggered `get_user_pages` (kernel page pinning) = **33% of poller CPU**.

After: `IORING_REGISTER_BUFFERS` pins pages once at startup. `ReadFixed` opcode skips per-read pinning entirely.

Impact: **+74% throughput at 1MB, +89% at 50MB.**

## Next Steps

1. **Multiple poller threads** — for 4KB workloads where main thread is the ceiling, parallelizing the poller won't help. But for mixed workloads with varied sizes, multiple pollers scale better.
2. **EFA/RDMA path (DMA.GET)** — bypass TCP entirely for GPU-attached clients (LMCache)
3. **Proper BlockClient reply callback** — replace ThreadSafeContext workaround with raw FFI reply_callback for architectural correctness
