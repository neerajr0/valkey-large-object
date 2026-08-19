# fd-pool benchmark / cap test

Scripts to validate the read-fd pool (`src/storage/fd_pool.rs`): under heavy
`LO.GET` traffic over a keyspace much larger than the pool cap, the number of
open fds stays bounded at ~`FD_CAP` (16384) and the pool evicts, instead of
opening one fd per object (~1M fds for a 1M keyspace).

## Design note (read this first)

On this branch the pool is populated **on reads only** — a cache miss opens an
`O_RDONLY` fd and caches it. **Writes do not populate the pool**: `LO.SET` opens
an `O_WRONLY` fd, writes, then closes and renames. So *pure* write traffic keeps
the fd count near zero. The ~16k plateau is therefore a **read-path** result;
the optional concurrent write stream in the cap test is there to show writes
*also* don't inflate the count, not to fill the pool.

The cap (`DEFAULT_CAPACITY = 16384`) is hard-coded in `fd_pool.rs`; there is no
config for it. `FD_CAP` in `bench-config.sh` is only used for the test assertion.

## Sequence

```bash
cd setup

# 0. Storage (once per boot on ephemeral NVMe — see notes below). DESTRUCTIVE.
./setup-storage.sh                 # dry run: prints which disks it WOULD wipe
sudo ./setup-storage.sh --yes      # execute: LVM-stripe all eligible NVMe → XFS → mount at DATA_DIR

# 1. Run the test
./bench-server.sh          # start valkey-server + module, ulimit -n raised above the cap
./bench-load.sh            # write ~1M dense keys via LO.SET (3x ops for ~95% coverage)
./fd-cap-test.sh           # GET workload + concurrent SET stream, samples peak fds
./bench-server.sh --stop   # shut down

# 2. Clean slate between runs (reformats XFS, keeps the LVM stripe)
./bench-reset.sh
```

### Storage, striping, and cleanup

- **`setup-storage.sh`** auto-discovers *empty, unclaimed, non-root* NVMe/SSD whole
  disks, LVM-stripes them (stripe count = #drives, `STRIPE_SIZE=256K`), makes an XFS
  filesystem (`noatime,discard,inode64`), and mounts at `DATA_DIR`. It is **DESTRUCTIVE**
  and therefore **dry-runs by default** — run with no args to see the plan, then
  `sudo ./setup-storage.sh --yes` to execute. It never touches the root disk, is
  idempotent (re-mounts an existing LV instead of rebuilding), and deliberately does
  *not* write `/etc/fstab` (ephemeral instance-store volumes vanish on stop — re-run
  after each boot). Override discovery with `--devices "/dev/nvme1n1 /dev/nvme2n1"`.
- **`bench-reset.sh`** clean-slates between configs by reformatting the XFS filesystem
  (fast even with millions of `.dat` files) while **preserving the LVM stripe** — so
  orphan objects from a prior write stream don't confound the next run. It reads `LV`
  and `MOUNT_OPTS` from `bench-config.sh` (defaults match `setup-storage.sh`).
- Requires `lvm2` + `xfsprogs`; `setup-storage.sh` prints the exact install command for
  your package manager if they're missing.

Everything is env-overridable (see `bench-config.sh`), e.g.:

```bash
CLIENTS=750 DURATION=60 OBJ_SIZE=4096 ./fd-cap-test.sh   # matches the 4KB/750-client run
WRITE_CLIENTS=0 ./fd-cap-test.sh                          # read-only (no write stream)
KEYSPACE=2000000 LOAD_OPS=6000000 ./bench-load.sh         # bigger keyspace
```

## What to look for

`fd-cap-test.sh` prints GET throughput + latency, optional SET throughput, the
**peak live fd count** sampled during the run, a **memory/slab snapshot**, then
`PASS`/`FAIL`:

- **PASS**: peak fds ≤ `FD_CAP + FD_HEADROOM` and far below `DBSIZE`. The pool
  bounded read fds to ~16k and evicted the rest, even while serving a 1M keyspace.
- Peak includes the bench client sockets (`CLIENTS + WRITE_CLIENTS`) and a few
  in-flight read fds, so it lands a bit above 16384 — the point is it's ~16k, not ~1M.

### Memory overhead — the real point of bounding the pool

Holding a read fd pins its inode in kernel memory, so the fd-overhead signal is
**pinned (non-reclaimable) kernel slab**: `SUnreclaim` in `/proc/meminfo` and the
`xfs_inode`/`filp` caches in `/proc/slabinfo` (~1.5 KB per fd). This is the
methodology from the old branch's [`FD_OVERHEAD_TEST.md`](https://github.com/KarthikSubbarao/ValkeyLargeObj/blob/experiment/keep-read-fds/setup/FD_OVERHEAD_TEST.md),
which measured the *unbounded* design (one fd held per object, never closed):

| Design | fds @ ~950K objects | pinned kernel mem (SUnreclaim Δ) |
|---|---|---|
| Unbounded (1 fd/object, old) | ~950K | **~1.35 GB** (~1.5 KB/fd, linear in object count) |
| **Bounded pool (this branch)** | **~16,384** | **~24 MB** (`FD_CAP` × ~1.5 KB, flat) |

`fd-cap-test.sh` runs a `drop_caches` before the snapshot (needs root) so the
`SUnreclaim` that *remains* is what the pool's open fds pin — it should track
`~FD_CAP`, not `object_count`. That flat ~24 MB (vs GBs growing linearly) is the
"less memory overhead" win: the bounded pool is exactly the *bounded LRU fd cache*
that `FD_OVERHEAD_TEST.md` recommended as the scalable fix. The throughput target
is the same 4 KB / 750-client run (~166K RPS, sub-ms p50) for the hot working set.

## Requirements / gotchas

- **`ulimit -n` must exceed the cap.** `bench-server.sh` raises the soft limit to
  `FD_LIMIT` (default 1048576); if the hard limit is lower it errors with how to fix
  it (`sudo prlimit` or run under sudo).
- Counting fds reads `/proc/<pid>/fd`; if the server runs as root the scripts fall
  back to `sudo`.
- `bench-mode yes` is set so `LO.GET` does the full NVMe read but replies with the
  size only — isolates the storage/fd path from TCP reply bandwidth.
- `data-dir` should be an O_DIRECT-capable mount (XFS/ext4 on NVMe). A concurrent
  write stream mints a new object file per SET (write-once), so `.dat` files
  accumulate for the duration — fine for a bounded test; run `./bench-reset.sh`
  between runs to clean-slate (reformats XFS, keeps the LVM stripe).

## Status / PR note

A merged setup + teardown mechanism for clean-slate testing does not exist on the
mainline branch yet. This `setup/` suite (storage provisioning via
`setup-storage.sh`, clean-slate via `bench-reset.sh`, plus the fd-cap harness) is
proposed as part of the fd-pool PR — carried over and adapted from the
`experiment/keep-read-fds` branch, updated to this branch's `LO.*` commands and
config. Raise it for review alongside the pool change.
