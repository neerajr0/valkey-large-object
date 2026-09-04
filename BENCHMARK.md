# Benchmark Guide

## Overview

`bench.sh` measures LO.GET throughput across three operating modes:

| Mode | What it tests |
|------|---------------|
| **Dram** | All objects in DRAM. No NVMe. Baseline for command processing overhead. |
| **Tiered** | NVMe persistence + DRAM cache. First GET promotes from NVMe → DRAM. Subsequent GETs are cache hits. |
| **NVMe** | Pure NVMe reads. No DRAM cache (`max-promote-size 0`). Every GET reads from disk. |

## Quick Start

```bash
# Dram only (no NVMe needed)
./bench.sh --port 7380

# All 3 modes on NVMe stripe
./bench.sh --port 7380 --nvme-dir /mnt/bigobj-data/bench-test
```

## Requirements

- `valkey-server`, `valkey-cli`, `valkey-benchmark` in `$PATH`
- Module built: `cargo build --release`
- Python 3 (no pip packages)
- For Tiered/NVMe: O_DIRECT capable mount on real NVMe (XFS or ext4)
- For fio baselines: `fio` installed

## How It Works

For each mode × size combination:

1. **Kill stale server** on the port (graceful shutdown + force kill)
2. **Start fresh server** with mode-specific configs
3. **Wait for ready** (up to 30s PING retry — Tiered allocates 32GB+ on startup)
4. **Populate keys** via raw RESP (Python, no dependencies)
5. **Record disk reads** from `/sys/block/*/stat`
6. **Run `valkey-benchmark`** for `--duration` seconds
7. **Assert disk reads** — verify actual NVMe I/O happened
8. **Shutdown server** and wait for full exit

## bench-mode

The module is loaded with `bench-mode yes`. This makes LO.GET reply with an integer (the object size) instead of the actual bulk data. This isolates storage + io_uring throughput from TCP output buffer overhead. The full NVMe read still happens — only the reply is shortened.

## Key Format

`valkey-benchmark` replaces `__rand_int__` with a **zero-padded 12-digit** number. Example: `k:__rand_int__` with `-r 500` sends `k:000000000042`.

The populate script uses `f'k:{i:012d}'` to match. If these don't match, all GETs return nil and the benchmark measures nothing.

## Disk Read Assertions

For Tiered and NVMe modes, the script reads `/sys/block/<device>/stat` before and after the benchmark to count actual disk reads.

- **Tiered:** Expects ≥ `num_keys` reads (one NVMe→DRAM promotion per key on first access)
- **NVMe:** Expects >> `num_keys` reads (every GET reads from NVMe, no caching)
- **Dram:** No assertion (no disk I/O)

If the assertion fails, the script prints `FAIL`. This catches mismatched key formats, wrong mount points, or code bugs where the NVMe path is silently skipped.

## Sizing

### Keys
Scaled by object size to keep populate time reasonable:

| Object Size | Keys |
|-------------|------|
| ≤ 4MB | 500 |
| 4MB - 16MB | 100 |
| 16MB - 50MB | 50 |
| ≥ 50MB | 20 |

### NVMe Staging
Auto-calculated: `clients × object_size × 1.2` (20% headroom for talc metadata). Capped at 1GB — the kernel hard limit per registered buffer (`IORING_REGISTER_BUFFERS`). If staging would exceed 1GB, client count is automatically reduced.

### Clients
For large objects where staging cap reduces clients:

```
effective_clients = min(clients, 1GB / object_size)
```

Example: 50MB objects with 200 clients → `1GB / 50MB = 20` effective clients.

## Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | (required) | Valkey server port |
| `--nvme-dir` | (optional) | NVMe directory. If set, runs all 3 modes. If not, Dram only. |
| `--modes` | auto | Space-separated: `"Dram Tiered NVMe"`. Auto-detected from `--nvme-dir`. |
| `--sizes` | `"4KB 1MB 50MB"` | Object sizes. Valid: 4KB, 16KB, 50KB, 256KB, 1MB, 4MB, 16MB, 50MB. |
| `--clients` | 200 | Concurrent benchmark clients. |
| `--duration` | 10 | Seconds per benchmark run. |
| `--keys` | 500 | Base key count (scaled down for large objects). |
| `--skip-fio` | off | Skip fio baselines (run by default when `--nvme-dir` set). |
| `--dram-maxmemory` | 34359738368 (32GB) | DRAM budget in bytes. |
| `--dram-segment-size` | 67108864 (64MB) | Segment size in bytes. |
| `--nvme-maxmemory` | 107374182400 (100GB) | NVMe budget in bytes. |
| `--worker-threads` | 2 | Tokio worker threads. |

Config values are in **raw bytes** (the module config API doesn't accept `1gb` notation on module load args).

## Environment Overrides

| Variable | Description |
|----------|-------------|
| `MODULE_SO` | Path to module .so (default: `./target/release/libvalkey_largeobj.so`) |
| `VALKEY_SERVER` | Server binary (default: `valkey-server`) |
| `VALKEY_CLI` | CLI binary (default: `valkey-cli`) |
| `VALKEY_BENCH` | Benchmark binary (default: `valkey-benchmark`) |

## Common Pitfalls

### Wrong mount point
`/data` may not exist as a mount — it falls through to the root disk. Always verify with `df <path>`. The script warns if `--nvme-dir` is on the root disk.

### Stale server
If a previous run's server is still on the port, the benchmark connects to it (wrong data, wrong mode). The script kills stale servers before each test, but if the process takes >15s to die (32GB deallocation), it can still race. Kill manually with `pkill -9 -f valkey-server` before running.

### Startup time
Tiered mode with 32GB DRAM allocates 512 × 64MB segments on startup (~5 seconds). The script retries PING for up to 30 seconds.

### 1GB staging limit
`IORING_REGISTER_BUFFERS` has a kernel hard limit of 1GB per buffer. For 50MB objects with many clients, the script auto-reduces client count to fit within 1GB staging.

## Example Output

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Mode: NVMe
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ── 4KB (4096 bytes) ── [keys=500, clients=200, staging=64MB]
  Populated 500 keys (4KB) in 0.0s (12391 keys/s)
  DBSIZE: 500
  throughput summary: 159048.41 requests per second
          avg       min       p50       p95       p99       max
  Disk reads: 1590623
```

## Reference Results (i8ge.48xlarge, 16 NVMe striped)

### fio baseline (16 jobs × iodepth 128, O_DIRECT, io_uring)

| Size | IOPS | Bandwidth | Latency |
|------|------|-----------|---------|
| 4KB | 1,400K | 5.5 GB/s | ~1ms |
| 1MB | 51K | 50 GB/s | ~40ms |
| 50MB | 960 | 49 GB/s | ~1.9s |

### Module (200 clients, bench-mode, 10s duration)

| Mode | 4KB | 1MB | 50MB |
|------|-----|-----|------|
| Dram | ~150K rps | ~150K rps | ~150K rps |
| NVMe | ~159K rps | TBD | TBD |
