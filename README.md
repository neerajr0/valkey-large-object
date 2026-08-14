# ValkeyLargeObj

A Valkey module for storing large objects (KV cache tensors, embeddings, blobs) on NVMe with io_uring and optional EFA RDMA transport to GPU memory.

## Features

- **LO.SET key len [data]** — Write object to NVMe via io_uring (O_DIRECT, WriteFixed)
- **LO.GET key** — Read object from NVMe via io_uring (ReadFixed), reply as bulk string
- **LO.HELLO** — Establish EFA/RDMA session for GPU-direct DMA transfers
- **Native DEL** — Deletes NVMe file via module free callback
- **Bench mode** — `bench-mode yes` makes LO.GET return size integer (skips TCP bulk copy, isolates NVMe throughput)

## Build

```bash
# Requires Rust toolchain
cargo build --release
# Output: target/release/libvalkey_largeobj.so
```

## Run

```bash
valkey-server --port 7380 \
    --loadmodule ./target/release/libvalkey_largeobj.so \
        data-dir /mnt/bigobj-data \
        pool-buf-size 4096 \
        pool-buf-count 1000 \
        bench-mode no \
    --io-threads 8
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `data-dir` | (required) | Directory for NVMe .dat files. Must support O_DIRECT. |
| `pool-buf-size` | 4194304 (4MB) | Size of each buffer in the pool. Must be 4KB-aligned. |
| `pool-buf-count` | 512 | Number of pre-allocated buffers. Each registered with io_uring. |
| `bench-mode` | no | When yes, LO.GET returns integer size instead of bulk data. |
| `max-bytes` | 0 (unlimited) | Max NVMe bytes (eviction not implemented yet). |
| `transport-threads` | 2 | Threads for EFA transport runtime. |

## Test

```bash
# Builds module, clones valkey from source, clones test framework, runs pytest
./build.sh

# Build only
./build.sh build

# Test only (assumes already built)
./build.sh test

# Run specific test
TEST_PATTERN=test_lo_set_get_roundtrip ./build.sh test
```

## Benchmark

```bash
# Full run (fio baselines + module benchmark)
./bench.sh /mnt/bigobj-data 7380

# Module benchmark only (skip fio)
./bench.sh /mnt/bigobj-data 7380 --skip-fio
```

Benchmark uses per-size server restarts, io-threads 8, taskset pinning, 750 clients, 10s duration per size.

## Performance (i8ge.48xlarge, 16 NVMe striped)

| Object Size | Clients | RPS |
|-------------|---------|-----|
| 4KB | 750 | 150,000 |
| 1MB | 750 | 60,000 |
| 16MB | 750 | 3,900 |
| 50MB | 750 | 1,000 |

Raw NVMe baseline (fio): 1.4M IOPS at 4KB, 50 GB/s at 1MB+.

## License

BSD-3-Clause
