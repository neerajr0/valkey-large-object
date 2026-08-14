#!/usr/bin/env bash
# Profile the COLD non-pooling GET path — where open() faults inodes from NVMe and
# BLOCKS the main thread. Unlike bench-perf.sh (which warms up and hides the cost),
# this SUSTAINS the cold state by dropping the inode/dentry cache on a loop for the
# whole window, so open() keeps faulting and the bottleneck stays on screen.
#
# Run against a NON-POOLING server:   ./bench-server.sh -k 0 -d 1 -o 1
# then populate:                       ./bench-load.sh
#
# Three views are captured:
#   1. per-thread %CPU — main thread should DROP (it's blocked on disk, off-CPU).
#   2. on-CPU perf     — the CPU slice of the cold open: xfs_iget / lookup_slow /
#                        path_openat / xfs_buf / io_schedule on the main thread.
#   3. perf trace -s   — THE money shot: openat syscall count + avg/max latency.
#                        Cold, openat avg should be milliseconds and dominate.
#
# Usage:  ./bench-perf-cold.sh [record_secs]   (default 15; trace window +5s)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

PERF_SECS="${1:-15}"
TRACE_SECS="${TRACE_SECS:-5}"
OUT="${PERF_OUT:-/tmp/bigobj-perf-cold.data}"
T0="/tmp/bigobj-perf-cold.cpu0"
T1="/tmp/bigobj-perf-cold.cpu1"
CLK="$(getconf CLK_TCK 2>/dev/null || echo 100)"
COLD_SECS="$((PERF_SECS + TRACE_SECS + 8))"   # keep dropping caches this long

command -v perf >/dev/null 2>&1 || { echo "[bench] perf not found (sudo dnf install -y perf)"; exit 1; }
PID="$(pgrep -o valkey-server || true)"
[ -n "$PID" ] || { echo "[bench] no valkey-server running — ./bench-server.sh -k 0 -d 1 -o 1 first"; exit 1; }

# Warn if the server is actually pooling (then there are no per-GET opens to see).
if grep -q 'keep_read_fds=true' <(grep 'bigobj: initialized' "$LOGFILE" 2>/dev/null | tail -1); then
  echo "[bench] WARNING: server is keep_read_fds=true (POOLING) — there are no per-GET open()s."
  echo "[bench]          restart non-pooling:  ./bench-server.sh -k 0 -d 1 -o 1"
fi

# GUARD: a server RESTART wipes the in-memory object index (we run --save "", and
# the module does not rebuild the index from the .dat files on startup). If the
# index is empty, EVERY GET misses and returns Null WITHOUT opening a file — you'd
# measure null-reply throughput, not real cold GETs. So reload after any restart.
OBJ_CNT="$($CLI BO.INFO 2>/dev/null | grep -o 'object_count:[0-9]*' | cut -d: -f2)"
if [ "${OBJ_CNT:-0}" -lt 1 ]; then
  echo "[bench] ERROR: object_count=${OBJ_CNT:-0} — the in-memory index is EMPTY." >&2
  echo "[bench]        A server restart wiped it. Run ./bench-load.sh, then re-run this." >&2
  exit 1
fi
bc_echo "object_count=$OBJ_CNT (index populated — GETs will hit real objects)"

snapshot_cpu() {
  local out="$1"; : > "$out"; local tid st comm ticks
  for tid in $(sudo ls "/proc/$PID/task" 2>/dev/null); do
    st="$(sudo cat "/proc/$PID/task/$tid/stat" 2>/dev/null)" || continue
    comm="$(sudo cat "/proc/$PID/task/$tid/comm" 2>/dev/null || echo '?')"
    ticks="$(printf '%s' "$st" | awk '{ line=$0; sub(/^.*\) /,"",line); n=split(line,b); print b[12]+b[13] }')"
    echo "$tid $comm ${ticks:-0}" >> "$out"
  done
}

# --- keep the cache COLD for the whole run (background, self-terminating) ---
bc_echo "starting cache-drop loop (every 1s for ${COLD_SECS}s) to sustain cold opens ..."
sudo sh -c "for i in \$(seq 1 $COLD_SECS); do sync; echo 3 > /proc/sys/vm/drop_caches; sleep 1; done" &
DROP_BG=$!

WORK_SECS="$((PERF_SECS + TRACE_SECS + 6))"
WORK_BG=""   # set below; declared here so the EXIT trap is safe under `set -u`
cleanup() {
  [ -n "${WORK_BG:-}" ] && kill "$WORK_BG" 2>/dev/null || true
  sudo kill "${DROP_BG:-}" 2>/dev/null || true
}
trap cleanup EXIT

# --- launch the GET workload in the background ---
bc_echo "GET workload: taskset -c $BENCH_CPUS $BENCH -c $CLIENTS --duration $WORK_SECS -r $KEYSPACE BO.GET bo:key:__rand_int__"
bc_echo "  (running in background; live output → /tmp/bigobj-perf-cold-bench.out)"
taskset -c "$BENCH_CPUS" $BENCH -c "$CLIENTS" --duration "$WORK_SECS" \
  -r "$KEYSPACE" BO.GET bo:key:__rand_int__ >/tmp/bigobj-perf-cold-bench.out 2>&1 &
WORK_BG=$!

sleep 1                      # let clients connect (drop loop keeps it cold)

# --- View 2 capture: on-CPU perf, NO warmup, sample the cold window ---
snapshot_cpu "$T0"
WALL_START="$SECONDS"
bc_echo "recording on-CPU perf (dwarf, ${PERF_SECS}s) while opens are cold ..."
sudo perf record --call-graph dwarf -F 999 -p "$PID" -o "$OUT" -- sleep "$PERF_SECS" || true
WALL="$((SECONDS - WALL_START))"; [ "$WALL" -gt 0 ] || WALL="$PERF_SECS"
snapshot_cpu "$T1"

# --- View 3 capture: syscall latency (the money shot) ---
echo
bc_echo "=== syscall latency on all engine threads (${TRACE_SECS}s) — look for 'openat' ==="
sudo perf trace -s -p "$PID" -- sleep "$TRACE_SECS" 2>&1 | \
  grep -iE 'syscall|openat|read|write|Summary|\([0-9]+\)' | head -40 || true

wait "$WORK_BG" 2>/dev/null || true
echo
bc_echo "cold workload result:"; sed -n '/Summary:/,$p' /tmp/bigobj-perf-cold-bench.out 2>/dev/null | head -8 || true

# --- View 1: per-thread %CPU (main thread should be LOW = blocked on disk) ---
echo
bc_echo "=== per-thread %CPU over ${WALL}s (main LOW => blocked in open(), off-CPU) ==="
awk -v clk="$CLK" -v wall="$WALL" '
  NR==FNR { c0[$1]=$3; next }
  { d=$3-c0[$1]; if (d<0) d=0; pct=(d/clk)/wall*100; printf "%7.1f  tid=%-8s %s\n", pct, $1, $2 }
' "$T0" "$T1" | sort -rn | head -20

# --- View 2 report: cold-open symbols on the main thread ---
echo
bc_echo "=== main-thread on-CPU hot functions (expect open/lookup/xfs/io_schedule) ==="
sudo perf report -i "$OUT" --stdio --comms=valkey-server 2>/dev/null | grep -vE '^#|^$' | head -35
echo
bc_echo "=== grep the profile for the open path ==="
sudo perf report -i "$OUT" --stdio 2>/dev/null | \
  grep -iE 'openat|path_openat|link_path_walk|walk_component|lookup_slow|xfs_iget|xfs_buf|xfs_lookup|io_schedule|iomap' | head -20 || \
  echo "  (no open-path symbols — cache may still be warming; raise record_secs or drop interval)"

echo
bc_echo "interactive: sudo perf report -i $OUT --comms=valkey-server"
