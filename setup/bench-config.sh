#!/usr/bin/env bash
# Shared config for the fd-pool benchmark scripts. Sourced by the others.
# Every value is overridable from the environment, e.g.:
#   OBJ_SIZE=4096 KEYSPACE=1000000 ./bench-load.sh
#   CLIENTS=750 DURATION=60 ./fd-cap-test.sh

# --- paths ---
: "${PORT:=6399}"
: "${DATA_DIR:=/mnt/bigobj-data}"        # must match setup-storage.sh MOUNT_POINT
# Log/pid in $HOME so a normal user can always write them (a shared /tmp path can
# be left root-owned by an earlier sudo run → "Can't open the log file").
: "${LOGFILE:=${HOME:-/tmp}/bigobj.log}"
: "${PIDFILE:=${HOME:-/tmp}/bigobj.pid}"
# LVM striped volume + mount opts (created by setup-storage.sh; used by bench-reset.sh).
: "${LV:=/dev/bigobj_vg/bigobj_lv}"
: "${MOUNT_OPTS:=noatime,discard,inode64}"

# module .so: default = ../target/release relative to this script
_CFG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${MODULE:=$(cd "$_CFG_DIR/.." && pwd)/target/release/libvalkey_largeobj.so}"

# valkey binaries (assumed on PATH; override to point at a source checkout)
: "${VALKEY_SERVER:=valkey-server}"
: "${VALKEY_CLI:=valkey-cli}"
: "${VALKEY_BENCH:=valkey-benchmark}"

# --- server / module config ---
: "${OBJ_SIZE:=4096}"          # object size in bytes (== pool-buf-size, 4KB-aligned)
: "${POOL_BUF_COUNT:=5000}"    # io_uring registered buffers (caps concurrent in-flight I/O)
: "${IO_THREADS:=8}"           # valkey io-threads (offload TCP reply encoding)
: "${FD_LIMIT:=1048576}"       # ulimit -n for the server (MUST exceed the fd-pool cap)
: "${SERVER_CPUS:=0-15}"       # taskset for valkey-server

# --- workload ---
: "${KEYSPACE:=1000000}"       # -r  distinct key range (must be >> FD_CAP to force eviction)
: "${LOAD_OPS:=3000000}"       # -n for load (3x keyspace => ~95% dense)
: "${LOAD_CLIENTS:=50}"
: "${CLIENTS:=750}"            # -c for the GET workload
: "${WRITE_CLIENTS:=50}"       # -c for the concurrent SET stream (0 = read-only test)
: "${DURATION:=60}"            # seconds for the fd-cap test window
: "${BENCH_CPUS:=16-31}"       # taskset for valkey-benchmark
: "${KEY_PREFIX:=lo:key:}"     # key namespace

# --- fd-pool cap (hard-coded in src/storage/fd_pool.rs: DEFAULT_CAPACITY) ---
# The test asserts the live fd count stays near this, not 1-per-object.
: "${FD_CAP:=16384}"
# Headroom above the cap for client sockets, in-flight read fds, log/listener, etc.
: "${FD_HEADROOM:=$((CLIENTS + WRITE_CLIENTS + 1024))}"

# --- derived handles ---
CLI="$VALKEY_CLI -p $PORT"
BENCH="$VALKEY_BENCH -p $PORT"

bc_echo() { echo "[bench] $*"; }

# Count open fds for the running server (works without sudo if you own the process;
# falls back to sudo for a root-owned server).
server_pid() { pgrep -o -f "valkey-server.*$PORT" || true; }
count_fds() {
  local pid="$1"
  ls "/proc/$pid/fd" 2>/dev/null | wc -l && return 0
  sudo ls "/proc/$pid/fd" 2>/dev/null | wc -l
}

# Memory / fd overhead snapshot. The overhead of holding read fds shows up as
# PINNED (non-reclaimable) kernel slab: SUnreclaim in /proc/meminfo and the
# xfs_inode / filp caches in /proc/slabinfo — open fds pin those inodes (~1.5
# KB/fd). With the bounded pool this should track ~FD_CAP, not object_count.
# (/proc/slabinfo is root-only, so those lines fall back to sudo.)
mem_fd_snapshot() {
  local pid="$1"
  echo "  open fds     : $(count_fds "$pid")"
  local rss
  rss="$( { grep VmRSS "/proc/$pid/status" 2>/dev/null || sudo grep VmRSS "/proc/$pid/status" 2>/dev/null; } | awk '{print $2" "$3}')"
  echo "  process RSS  : ${rss:-?}"
  grep -E '^(Slab|SReclaimable|SUnreclaim):' /proc/meminfo | awk '{printf "  %-13s: %s %s\n",$1,$2,$3}'
  { grep -E 'xfs_inode|^dentry |^filp ' /proc/slabinfo 2>/dev/null || sudo grep -E 'xfs_inode|^dentry |^filp ' /proc/slabinfo 2>/dev/null; } \
    | awk '{printf "  %-13s: active_objs=%s objsize=%s\n",$1,$3,$4}'
}
