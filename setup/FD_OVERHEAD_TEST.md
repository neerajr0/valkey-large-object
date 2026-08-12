# File-Descriptor Overhead Test

**Goal:** Measure the memory overhead of the bigobj module keeping one open file
descriptor per stored object, and (eventually) compare open-fd vs closed-fd
behavior. Motivated by the backlog concern that holding millions of fds is not
scalable.

## Hardware / environment

- **Instance:** i8ge.48xlarge (aarch64 / Graviton), 1.5 TiB RAM
- **Storage:** 16 × NVMe instance-store, LVM 16-way striped (256K stripe), XFS,
  mounted at `/mnt/bigobj-data` (110 TB)
- **Module:** `bigobj` @ branch `NVMEBenchmark`, built `--release`

## What the module does (relevant to this test)

- One file per object: `/mnt/bigobj-data/{object_id:016x}.dat`
- On `BO.SET`: writes the file (O_DIRECT) **and** opens a read fd kept in an
  in-memory `FdPool` (`HashMap<oid, fd>`) — **never closed until `BO.DEL`**.
- On `BO.SET`: also keeps a full DRAM copy of the object (`Arc<Vec<u8>>`) in the
  buffer pool unless `BO.EVICT` is called. → ~40 GB DRAM for 10M × 4KB.
- No "close fds" mode exists yet (see A/B section).

## Key metric

The signal for fd overhead is **kernel slab**, specifically `SUnreclaim` in
`/proc/meminfo` and the `xfs_inode` / `filp` / `dentry` caches in
`/proc/slabinfo`. Open fds pin these (non-reclaimable). The DRAM object-copies
live in process RSS (userspace), separate from slab — so slab growth isolates
the fd/inode cost.

---

## Procedure

### Step 0 — Raise fd limits (else `open()` hits EMFILE and the module silently returns Null)

```bash
sudo sysctl -w fs.nr_open=20000000
sudo sysctl -w fs.file-max=20000000
```

### Step 1 — Start valkey + module with a high fd limit

```bash
sudo bash -c 'ulimit -n 20000000; exec /home/ec2-user/valkey/src/valkey-server --port 6399 \
  --loadmodule /home/ec2-user/bigobj/target/release/libvalkey_bigobj.so \
  data-dir /mnt/bigobj-data pool-buf-size 4096 pool-buf-count 5000 \
  --save "" --logfile /tmp/bigobj.log --daemonize yes'

PID=$(pgrep -o valkey-server)
sudo cat /proc/$PID/limits | grep 'open files'   # confirm ~20000000
```

### Measurement snapshot (run before and after each load)

```bash
PID=$(pgrep -o valkey-server)
echo "=== fds ===";        sudo ls /proc/$PID/fd | wc -l
echo "=== process RSS ==="; sudo grep VmRSS /proc/$PID/status
echo "=== kernel slab totals ==="; grep -E 'Slab|SReclaimable|SUnreclaim' /proc/meminfo
echo "=== inode/dentry/file caches ==="
sudo grep -E 'xfs_inode|^dentry|^filp|nvme' /proc/slabinfo | awk '{printf "%-20s active_objs=%-10s objsize=%s\n",$1,$2,$4}'
/home/ec2-user/valkey/src/valkey-cli -p 6399 BO.INFO
```

### Step 3 — Load keys (4 KB each)

```bash
# 1M smoke test:
/home/ec2-user/valkey/src/valkey-benchmark -p 6399 -n 1000000 -r 1000000 -c 50 \
  BO.SET bo:key:__rand_int__ "$(head -c 4096 /dev/zero | tr '\0' 'x')"

# 10M full run: bump -n and -r to 10000000
```

Note: `-r N` random keys → some collisions → slightly fewer than N distinct
objects/fds. Confirm actual count via `BO.INFO` `object_count`.

---

## Results

### Baseline (server idle, object_count = 0)

| Metric | Value |
|---|---|
| Open fds | _(pending — permission-fixed snapshot)_ |
| Process RSS | ~31 MB (`VmRSS: 31456 kB`) |
| Slab total | ~1.04 GB (`1040520 kB`) |
| SReclaimable | ~369 MB (`377812 kB`) |
| SUnreclaim | ~647 MB (`662708 kB`) |
| xfs_inode / filp / dentry | _(pending)_ |

### After 1M keys (4 KB) — `-r 1000000` random keys → 632,281 distinct

Raw snapshot:
```
fds:            632292
RSS:            2817520 kB  (~2.69 GB)
Slab:           3641524 kB  (~3.47 GB)
SReclaimable:   1897620 kB  (~1.81 GB)
SUnreclaim:     1743904 kB  (~1.66 GB)
xfs_inode  active_objs=780859   objsize=1024
filp       active_objs=645166   objsize=192
dentry     active_objs=1915057  objsize=192
BO.INFO:   object_count=632281  total_bytes=2589822976 (~2.41 GB)  buffer_pool_bytes=same
```

| Metric | Value | Δ vs baseline | Per-object |
|---|---|---|---|
| object_count | 632,281 | — | — |
| Open fds | 632,292 | +632,281 | **1 fd / object** ✅ |
| Process RSS | 2.69 GB | +2.66 GB | ~4.4 KB (≈ the 4 KB DRAM copy + map/meta) |
| Slab total | 3.47 GB | +2.48 GB | ~4.1 KB |
| **SUnreclaim (pinned)** | 1.66 GB | **+1.03 GB** | **~1.7 KB / fd (non-reclaimable)** |
| SReclaimable | 1.81 GB | +1.45 GB | ~2.4 KB (dentries, reclaimable) |
| xfs_inode | 780,859 objs | — | 1 KB each |
| filp (open files) | 645,166 objs | — | 192 B each ≈ fd count |

**Reading:** one fd per object confirmed. Fd-attributable pinned kernel memory
≈ **1.7 KB/object** (`SUnreclaim` delta). RSS is dominated by the 4 KB DRAM
copy per object, *separate* from slab — so slab cleanly isolates the fd cost.

### After dense load → 950,176 objects (`keep-read-fds 1`, same population as the throughput test below)

Raw snapshot:
```
object_count:   950176      total_bytes: 3891920896 (~3.62 GB DRAM copies)
fds:            950887
RSS:            4275116 kB  (~4.08 GB)
Slab:           4553028 kB  (~4.34 GB)
SReclaimable:   2472736 kB  (~2.36 GB)
SUnreclaim:     2080292 kB  (~1.98 GB)
xfs_inode  active_objs=958560   objsize=1024   (~937 MB)
filp       active_objs=964614   objsize=192    (~185 MB)
dentry     active_objs=3033744  objsize=192    (~579 MB)
```

| Metric | Value | Δ vs idle baseline | Per-object |
|---|---|---|---|
| object_count | 950,176 | — | — |
| Open fds | 950,887 | +950K | **1 fd / object** ✅ |
| Process RSS | 4.08 GB | +4.05 GB | ~4.3 KB (4 KB DRAM copy + map/meta) |
| **SUnreclaim (pinned)** | 1.98 GB | **+1.35 GB** | **~1.49 KB / fd (non-reclaimable)** |
| xfs_inode | 958,560 objs | — | ≈ object_count |
| filp (open files) | 964,614 objs | — | ≈ fd count |

**Two data points agree:** ~1.7 KB/fd at 632K and ~1.5 KB/fd at 950K pinned
kernel memory → roughly linear in object count. (Note: SUnreclaim survives
`drop_caches` precisely because open fds *pin* those inodes — that's the
non-reclaimable cost. Reclaimable dentries were dropped and repopulated.)

### Extrapolation to 10M (from the measured per-object rate)

Per-object costs measured above, ×10M objects:
- **Pinned kernel memory (SUnreclaim):** ~1.7 KB × 10M ≈ **~17 GB non-reclaimable**
- **Process RSS:** ~40 GB DRAM copies + ~4–5 GB fd-map/metadata ≈ **~45 GB**
- **Total slab:** ~4.1 KB × 10M ≈ **~41 GB** (of which ~17 GB unreclaimable)
- **Open fds:** ~10M (needs `fs.nr_open`/`ulimit -n` raised — done in Step 0)

Trivial on 1.5 TiB, but the ~17 GB of *pinned, non-reclaimable* kernel memory is
the scalability concern: it grows linearly with object count and can't be
reclaimed under pressure.

### After 10M keys (4 KB) — actual

_(pending — run 10M load)_

| Metric | Value | Δ vs baseline |
|---|---|---|
| object_count | | |
| Open fds | | |
| Process RSS | | |
| SUnreclaim | | |
| xfs_inode active_objs | | |

---

## A/B: open fds vs closed fds — throughput & latency

**Status: IMPLEMENTED** via a `keep-read-fds 0|1` module arg (default 1).
- `keep-read-fds 1` (today's behavior): read fd opened at write time, held in the
  pool forever (no `open()` on the GET hot path).
- `keep-read-fds 0`: no fd pooled. `bo_get` opens a fresh fd **on the main thread**
  per GET and flags it; the io_uring poller `close()`s it after the read completes.

Code touched: `lib.rs` (arg), `storage/engine.rs` + `storage/nvme.rs`
(`keep_read_fds` flag, `open_read_fd_ondemand`), `commands/mod.rs` (bo_get
fallback + close flag), `uring_engine.rs` (`ClientData.close_fd_after`, close at
all 3 completion sites).

### Hypothesis

At 4 KB objects the workload is **main-thread limited** (per Karthik's README,
~166K RPS). In `keep-read-fds 0` the `open()`+`close()` land on the main thread
per GET → expect **lower TPS and higher P50** vs `keep-read-fds 1`.

### Method (GET benchmark, 750 clients, 4 KB objects)

Load once (keys must exist), then benchmark GET under each mode. Restart the
server between modes with the flag flipped. Keys were loaded with
`bo:key:__rand_int__` and `-r 1000000`, so GET uses the same key space.

```bash
# start (arm A): keep-read-fds 1
sudo bash -c 'ulimit -n 20000000; exec /home/ec2-user/valkey/src/valkey-server --port 6399 \
  --loadmodule /home/ec2-user/bigobj/target/release/libvalkey_bigobj.so \
  data-dir /mnt/bigobj-data pool-buf-size 4096 pool-buf-count 5000 \
  keep-read-fds 1 --save "" --logfile /tmp/bigobj.log --daemonize yes'
# (load keys if starting fresh — see Step 3)

# GET benchmark:
/home/ec2-user/valkey/src/valkey-benchmark -p 6399 -c 750 --duration 60 \
  -r 1000000 BO.GET bo:key:__rand_int__

# then SHUTDOWN, restart with keep-read-fds 0, re-run the same GET benchmark.
```

### Results

Population: 950,176 objects (4 KB), 750 clients, 60s, cold cache, server pinned
0-31 / benchmark pinned 32-63.

| Arm | TPS (RPS) | avg | P50 | P95 | P99 | Open fds during GET |
|---|---|---|---|---|---|---|
| `keep-read-fds 1` (pooled) | **176,186** | 0.231 ms | **0.239 ms** | 0.303 ms | 0.343 ms | 950,887 (~object_count) |
| `keep-read-fds 0` (open per GET) | _(pending — Arm B)_ | | | | | ~tens (valkey only) |

**Baseline check:** Arm A's 176K RPS / 0.239 ms P50 matches (slightly beats)
Karthik's README 4 KB result (~166K RPS / ~0.44 ms P50). Baseline validated. ✅

For reference, the BO.SET load (write path, synchronous on main thread) ran at
~12,149 RPS / P50 4.16 ms — writes are much slower than reads here because
`BO.SET` does O_DIRECT write + fallocate + (in this arm) fd open, all on the
main thread.

---

## Observations / conclusions

_(to be filled in as results come in)_
