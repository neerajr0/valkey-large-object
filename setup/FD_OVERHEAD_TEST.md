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
| `keep-read-fds 0` (open per GET) | **118,601** | 5.694 ms | **4.751 ms** | 7.895 ms | 24.159 ms | ~780 (transient in-flight) |

**Effect of pooling: +48% throughput (176K vs 119K RPS), ~20× lower P50
(0.24 ms vs 4.75 ms), ~70× lower P99.** Both arms used the same ~950K-object
population, 750 clients, 60s, cold cache, server pinned 0-31 / bench 32-63.

**Sanity re-run of Arm A** (pooled, on the *experimental* binary rather than
origin-tip): 178,069 RPS, P50 0.239 ms, P99 0.327 ms, 951,096 fds, SUnreclaim
2.08 GB — matches the original Arm A within ~1%. Confirms the result is
reproducible and the experimental binary's `keep-read-fds 1` path is equivalent
to mainline.

Why Arm B is so much slower: `bo_get` performs `open()` (and the poller
`close()`) synchronously **on the main thread**, before blocking the client.
Post-`drop_caches`, many opens fault the inode in from NVMe, stalling the
single event loop per GET. Arm A's main thread only does a HashMap lookup for
the pooled fd, then hands off to the io_uring poller — it never blocks on I/O.

**Baseline check:** Arm A's 176K RPS / 0.239 ms P50 matches (slightly beats)
Karthik's README 4 KB result (~166K RPS / ~0.44 ms P50). Baseline validated. ✅

For reference, the BO.SET load (write path, synchronous on main thread) ran at
~12,149 RPS / P50 4.16 ms — writes are much slower than reads here because
`BO.SET` does O_DIRECT write + fallocate + (in this arm) fd open, all on the
main thread.

---

### Arm B memory (post-GET snapshot, `keep-read-fds 0`, 949,957 objects)

Apples-to-apples with Arm A's post-GET snapshot (both after `drop_caches` + GET).

```
object_count:   949957
fds:            11           (idle; ~780 transient during GET)
RSS:            4239224 kB   (~4.04 GB — same as Arm A; DRAM copies dominate)
Slab:           2055028 kB   (~1.96 GB)
SReclaimable:   1275592 kB
SUnreclaim:      779436 kB   (~0.74 GB)
xfs_inode  active_objs=956768   (reclaimable now — NOT pinned by open fds)
filp       active_objs=19362    (~3.7 MB vs Arm A's 185 MB)
dentry     active_objs=1022826
```

Note: the Arm B *post-load* snapshot (before GET) showed SUnreclaim 2.58 GB and
xfs_inode 1.56M — contaminated by the fresh 3M-write inode cache. The post-GET
numbers above (after drop_caches) are the clean comparison.

---

## Experiment 2: directory sharding (per-Valkey-slot subdirs)

Objects sharded into `{data_dir}/{slot:04x}/{oid}.dat`, slot = Valkey key hash
slot (CRC16 % 16384). Hypothesis: reduce single-directory inode-lock contention
so opens can parallelize. Toggle: `dir-shards N` (1 = flat, 16384 = per-slot).

### Result (non-pooling `keep-read-fds 0`, 950K × 4KB, 750c/60s, cold cache)

| Variant | RPS | P50 | avg | P99 |
|---|---|---|---|---|
| Flat (`dir-shards 1`) | 118,601 | 4.75 ms | 5.69 ms | 24.16 ms |
| Sharded (`dir-shards 16384`) | **106,859** | **5.54 ms** | 6.32 ms | 18.67 ms |
| (pooled baseline, ref) | 176,186 | 0.24 ms | — | — |

**Sharding alone slightly REGRESSED throughput (−10%) and median (+17%)** — as
predicted. With on-demand `open()` still **serialized on the main thread**, there
is no concurrent directory-lock contention to relieve, so sharding only adds
cost: (1) deeper path = extra directory lookup per open; (2) after drop_caches,
each cold GET now faults the shard-directory inode *and* the file inode (~2×
metadata faults), and the 16384 dir inodes are mostly cold too.

Nuance: **P99 improved** (18.7 vs 24.2 ms) — spreading files across XFS
allocation groups smooths worst-case metadata variance, but the added
path-resolution cost dominates the common case.

**Conclusion: sharding is an ENABLER for parallel opens, not a standalone win.**
It only pays off once opens run concurrently (io-wq async openat or a userspace
open-thread pool) — then the shards stop those parallel opens from re-serializing
on one directory lock. Next experiment: parallel opens, measured flat vs sharded.

### Isolation run + confound (IMPORTANT — earlier comparison is not trustworthy)

Re-ran non-pooling on the *sharding build* with `dir-shards 1` (flat path + CRC16,
no extra directory level):

| Run | Build | RPS | P50 |
|---|---|---|---|
| Old flat | pre-sharding | 118,601 | 4.75 ms |
| New, dir-shards 1 | sharding build | 108,596 | 5.23 ms |
| New, dir-shards 16384 | sharding build | 106,859 | 5.54 ms |

Findings:
- `dir-shards 1` (108K) ≈ `dir-shards 16384` (107K): the extra **directory level
  was NOT the cause** — the "path depth" hypothesis is falsified.
- The real gap is old-build (119K) → new-build (108K) at the *same* flat layout.
  Code-wise the only GET-path difference is the CRC16 slot compute (~0.15 µs ≈
  ~1.5%), which cannot explain ~8%.
- **Prime confound: orphaned `.dat` files accumulate across restart+reload cycles**
  (in-memory state is wiped on restart, but files persist and are never cleaned),
  bloating the flat root directory and slowing cold opens. Plus the non-pooling
  path is inherently noisier than pooled (disk-open-bound; pooled reproduced
  within 1%, these did not).

**→ The 119/108/107 spread is within the confound/noise band. The sharding
result is INCONCLUSIVE.** To attribute anything, control disk state:
`rm -rf /mnt/bigobj-data/*` between runs, and run each config 2–3× for a variance
band. Only then compare flat vs sharded (and later flat vs sharded under parallel
opens, which is where sharding should actually matter).

### Clean-workspace baseline (non-pooling, dir-shards 1) — TRUSTWORTHY

Workspace reformatted (`bench-reset.sh`) so only one run's ~950K files exist
(object_count 950,410 == dat files 950,410 — zero orphans). 3 GET runs:

| Run | RPS | P50 |
|---|---|---|
| 1 | 115,561 | 5.119 ms |
| 2 | 116,480 | 5.063 ms |
| 3 | 115,253 | 5.127 ms |

**Variance band: 115.3–116.5K RPS (~1% spread) — tight and reproducible.**
Memory: 11 fds, SUnreclaim 0.71 GB, filp 21,756 (non-pooling confirmed).

Verdict: the earlier "regression" (108.6K dirty run) was the **orphan confound**,
now removed. Clean non-pooling flat = ~116K, within ~2.5% of the pre-sharding
build (118.6K) — accounted for by the CRC16 slot compute (~1.5%) + single-run
noise. **No meaningful code regression.** This ~116K is the trustworthy
non-pooling-flat baseline for future clean comparisons (vs sharded, vs parallel
opens). Pooled remains ~176K — the ~34% gap is the real, structural cost of
open()-on-main-thread, unchanged by the sharding work.

### Clean flat vs sharded (both clean workspace) — does sharding help serially?

| Config (clean, non-pooling) | RPS | P50 |
|---|---|---|
| Flat (dir-shards 1), 3-run band | 115.3–116.5K | ~5.1 ms |
| Sharded 1024 (1 run) | 111,386 | 5.32 ms |
| Sharded 16384 (dirty, ignore) | ~107K | 5.54 ms |

**Sharding does NOT help serial opens — it slightly regresses (~4% at 1024),
monotonically worse with more shards (116K → 111K → ~107K).** This is the pure
directory-depth cost (extra path component + more cold shard-dir inode faults per
open), now visible without the orphan confound. With opens serial on the main
thread there is no concurrency for sharding to exploit, so it is neutral-to-
negative standalone. Caveat: 1024 is a single run vs flat's 3-run band; ~4% is
likely real (flat band ~1%) but 3 runs would confirm.

**Confirmed: sharding is only worthwhile paired with PARALLEL opens** (io-wq /
open-thread pool), where shards stop concurrent openers serializing on one
directory lock. That is the decisive next experiment. **(Now answered — see
Experiment 3: it does, dramatically, in the cold + parallel regime.)**

---

## Experiment 3: open-offload worker pool (`open-threads N`)

Motivated by the cold profile (`bench-perf-cold.sh`): with non-pooling, the
on-demand `open()` runs synchronously on the Valkey main thread and **blocks it**
on a cold inode/directory fault from NVMe. Sustained cold, throughput collapses to
~8K RPS with the main thread ~85% of wall-clock inside `openat` (and only ~25%
CPU — the rest is off-CPU, asleep on disk). On-CPU, the dominant leaf is
`xfs_dir2_node_lookup` (walking the directory B-tree of ~950K entries).

**Implementation** (`open-threads N` arg, `src/open_pool.rs`): in non-pooling mode,
`bo_get` blocks the client and hands the open to a pool of N worker threads via a
second crossbeam channel (SPMC). Each worker does the blocking `open()` off the
main thread, then submits the read to the existing io_uring poller (the read
channel becomes MPSC). `open-threads 0` = original inline behavior (control).
Pipeline: `main → [open chan] → open worker: open() → [read chan] → poller → UnblockClient`.

### Methodology note (IMPORTANT — a restart wipes the in-memory index)

A server restart wipes the **in-memory** object index (we run `--save ""`, and the
module does not rebuild the index from the `.dat` files on startup). The files
persist on disk, but `bo_get` gates on the in-memory index first — so after a
restart with **no reload**, every GET misses and returns Null *without opening a
file*. That measures null-reply throughput (~140K), not real cold GETs — the tell
is `openat` ≈ 0 in the trace. **Reload after every restart.** `bench-perf-cold.sh`
now aborts if `object_count == 0`. All numbers below are with a populated index
(real `openat` counts in the tens of thousands).

### Results (sustained cold via `drop_caches` every 1s, non-pooling, 950K × 4KB, 750c)

| Config (cold, non-pooling) | Cold RPS | P50 | avg openat | Bottleneck (from %CPU + on-CPU) |
|---|---|---|---|---|
| inline (`open-threads 0`) | 8,388 | 78 ms | 188 µs | main thread BLOCKED in `openat` (25.8% CPU, off-CPU); on-CPU `openat → xfs_dir2_node_lookup` |
| 1 worker, flat | 12,834 | 14 ms | 188 µs | the single worker (serial cold opens, 96% wall in `openat`); main freed |
| **16 workers, 1024 shards** | **112,837** | **4.9 ms** | **~100 µs** | **main thread — network egress `tcp_sendmsg` (101.5% CPU)** |
| *(ref) pooled* | *176,186* | *0.24 ms* | *—* | *main thread — network* |
| *(ref) warm non-pooling* | *~150K* | *—* | *—* | *main thread — network* |

**Findings:**
- **Offload works, provably.** Main-thread `openat` calls: 22,277 (inline) → 50
  (1 worker) → 46 (16 workers). Opens move onto the `bigobj-open-*` threads; the
  main thread's on-CPU profile flips from the cold-open path to the network path.
- **1 worker ≈ 1.5×** (8.4K → 12.8K): it relocates the block from the main thread
  to a single worker, which then serializes on ~188 µs cold opens (~5K/s ceiling).
- **16 workers + sharding ≈ 13.5×** (8.4K → 112.8K): parallel opens (16 workers,
  none pegged, ~23.5% CPU each) **plus** sharding shrinking the directory B-tree —
  per-open latency 188 µs → ~100 µs, because each shard dir holds ~930 files
  instead of 950K, making `xfs_dir2_node_lookup` cheap. **Sharding finally pays off
  — only here, in the cold + parallel regime** (warm or serial it was
  neutral-to-negative, per Experiment 2).
- **The bottleneck moved back to the main thread** (101.5% CPU, `tcp_sendmsg`) —
  the SAME ceiling as pooled/warm. The cold-open stall is fully escaped.
- **More than ~16 workers won't help:** workers are only ~23.5% busy; the main
  thread is saturated. Now network/main-thread-bound, not open-bound (~12 workers
  would sustain 112K: ~112K × 100 µs ≈ 11 worker-seconds/sec).
- **The poller rose to 52.4%** (was ~15%): now fed by 16 workers (MPSC), doing
  ~112K reads + closes/sec. Headroom remains, but it's the next thing to saturate.

### Bottom line

**Non-pooling + 16 open-workers + 1024 shards recovers ~113K RPS cold — ~64% of
pooled's 176K — WITHOUT pinning ~950K fds or ~2 GB of non-reclaimable kernel
memory, and without the cold-collapse.** Thesis validated: you can drop the fd pool
and stay fast, provided you (1) offload `open()` off the main thread, (2)
parallelize it across enough workers, and (3) shard directories so parallel cold
opens don't serialize on one directory B-tree/lock. The residual gap to pooled
(113K vs 176K) is main-thread network egress + per-GET offload overhead
(block/unblock + channel sends); the lever from here is Valkey `io-threads`
(offload the socket writes) — same conclusion as the warm profile.

Caveat: numbers are under `drop_caches` every 1s (aggressively cold). Real
production with a partly-warm cache sits higher, toward the ~150K warm figure.

---

## Observations / conclusions

**Memory cost of pooling fds (holding 1 read fd per object):**
- ~1 fd per object (950,887 vs 11 idle).
- **~1.24 GB pinned, non-reclaimable kernel memory** at 950K objects
  (SUnreclaim 1.98 GB pooled vs 0.74 GB not-pooled), ≈ **1.3–1.7 KB/fd**,
  linear in object count (agrees across 632K, 950K data points).
  Breakdown: `filp` (struct file) ~185 MB + pinned `xfs_inode` ~940 MB.
- The DRAM object-copy buffer pool (~40 GB at 10M × 4KB) is a *separate*,
  larger memory axis (userspace RSS), unaffected by the fd choice.

**Performance value of pooling fds:**
- **+48% GET throughput** (176K vs 119K RPS) and **~20× lower P50** (0.24 vs
  4.75 ms) at 4 KB objects, because pooling removes a synchronous
  `open()`/`close()` from the main-thread hot path.

**Conclusion / recommendation:**
Neither extreme is ideal. "Pool every fd forever" (current default) is fast but
costs ~1.7 KB pinned kernel memory per object and doesn't scale to 10s of
millions of objects (fd-limit + pinned-memory wall). "Never pool" is scalable but,
naively (inline open on the main thread), pays a large per-GET penalty that
*collapses* under a cold cache (~8K RPS — Experiment 3).
→ **Validated fix (Experiment 3): non-pooling + open-offload worker pool +
directory sharding.** Offloading `open()` to ~16 worker threads and sharding into
~1024 dirs recovers ~113K RPS *cold* (~64% of pooled) with only ~11 fds and
~0.74 GB kernel memory — decoupling throughput from the fd/pinned-memory wall. This
is the scalable path: keep the memory profile of "never pool" while approaching the
throughput of "pool everything."
→ A **bounded LRU fd cache** (hold the hot N fds, open-on-demand for the cold tail)
remains complementary — it would shrink the offload pool's work to the cold tail
only. The even-longer-term fix is a **slab/packed-file layout** (offset+len in
ObjectMeta) so object count is fully decoupled from fd count.
→ Remaining ceiling is the single-threaded network egress on the main thread
(`tcp_sendmsg`), unchanged across pooled/warm/offloaded — the next lever is Valkey
`io-threads`.
See the backlog doc's "Claude's TODOs" — this test quantifies all sides.
