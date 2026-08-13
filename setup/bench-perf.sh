#!/usr/bin/env bash
# Profile the running valkey-server's engine threads while a workload runs, to
# locate the bottleneck — especially the non-pooling (keep-read-fds 0) GET path,
# where open() runs synchronously on the main thread and stalls the event loop.
#
# Two complementary views are captured, because the non-pooling stall is mostly
# OFF-CPU (the main thread sleeps in open() waiting on an NVMe inode fault):
#   1. per-thread CPU utilization (procfs) — cheap; in non-pooling mode the main
#      thread shows LOW %CPU (blocked on disk), which is itself the smoking gun.
#   2. perf record (on-CPU) — where CPU cycles actually go, per engine thread.
#
# The fd-recycling penalty lives on the GET path only: BO.SET never opens a read
# fd in keep-read-fds 0 mode, so profile a GET workload against a keep-read-fds 0
# server to see it. Start that server first:  ./bench-server.sh -k 0 -d <shards> -o 0
#
# Usage:
#   ./bench-perf.sh                # profile a GET workload (default), 20s window
#   ./bench-perf.sh get 30         # GET workload, 30s perf window
#   ./bench-perf.sh load 20        # profile the BO.SET write path instead
#
# Requires: perf (linux-tools). Server runs as root, so perf/procfs use sudo.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

MODE="${1:-get}"
PERF_SECS="${2:-20}"
OUT="${PERF_OUT:-/tmp/bigobj-perf.data}"
T0="/tmp/bigobj-perf.cpu0"
T1="/tmp/bigobj-perf.cpu1"
CLK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

command -v perf >/dev/null 2>&1 || {
  echo "[bench] perf not found. Install linux-tools, e.g.:"
  echo "        sudo dnf install -y perf   # or: sudo yum install -y perf / apt-get install -y linux-tools-\$(uname -r)"
  exit 1
}

PID="$(pgrep -o valkey-server || true)"
[ -n "$PID" ] || { echo "[bench] no valkey-server running — start one with ./bench-server.sh first"; exit 1; }

# --- per-thread CPU-time snapshot (tid comm utime+stime_ticks) ---
snapshot_cpu() {
  local out="$1"; : > "$out"
  local tid st comm ticks
  for tid in $(sudo ls "/proc/$PID/task" 2>/dev/null); do
    st="$(sudo cat "/proc/$PID/task/$tid/stat" 2>/dev/null)" || continue
    comm="$(sudo cat "/proc/$PID/task/$tid/comm" 2>/dev/null || echo '?')"
    # Strip "pid (comm) " prefix, then utime=field12 stime=field13 of the rest.
    ticks="$(printf '%s' "$st" | awk '{ line=$0; sub(/^.*\) /,"",line); n=split(line,b); print b[12]+b[13] }')"
    echo "$tid $comm ${ticks:-0}" >> "$out"
  done
}

bc_echo "engine threads (tid / comm) for pid $PID:"
snapshot_cpu "$T0"
awk '{ printf "  tid=%-8s %s\n",$1,$2 }' "$T0"

bc_echo "server config line:"
grep -E 'initialized' "$LOGFILE" 2>/dev/null | tail -1 || true

# Cold cache so we profile real (cold-inode) open behaviour, matching bench-get.
sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# --- launch the workload in the background (runs a bit longer than the window) ---
WORK_SECS="$((PERF_SECS + 8))"
case "$MODE" in
  get)
    bc_echo "starting GET workload ($CLIENTS clients, ${WORK_SECS}s) in background ..."
    taskset -c "$BENCH_CPUS" $BENCH -c "$CLIENTS" --duration "$WORK_SECS" \
      -r "$KEYSPACE" BO.GET bo:key:__rand_int__ >/tmp/bigobj-perf-bench.out 2>&1 &
    ;;
  load)
    bc_echo "starting LOAD workload (BO.SET) in background ..."
    val="$(head -c "$OBJ_SIZE" /dev/zero | tr '\0' 'x')"
    taskset -c "$BENCH_CPUS" $BENCH -n "$LOAD_OPS" -r "$KEYSPACE" -c "$LOAD_CLIENTS" \
      BO.SET bo:key:__rand_int__ "$val" >/tmp/bigobj-perf-bench.out 2>&1 &
    ;;
  *) echo "[bench] unknown mode '$MODE' (use: get | load)"; exit 1 ;;
esac
BENCH_BG=$!

# Let the workload reach steady state, then sample.
sleep 3
snapshot_cpu "$T0"          # re-baseline right before the window
WALL_START="$SECONDS"
bc_echo "recording perf (call-graph dwarf, -F 999, ${PERF_SECS}s) on all threads of pid $PID ..."
sudo perf record --call-graph dwarf -F 999 -p "$PID" -o "$OUT" -- sleep "$PERF_SECS" || true
WALL="$((SECONDS - WALL_START))"; [ "$WALL" -gt 0 ] || WALL="$PERF_SECS"
snapshot_cpu "$T1"

wait "$BENCH_BG" 2>/dev/null || true
echo
bc_echo "workload result:"; grep -E 'requests per second|throughput|Summary' -A1 /tmp/bigobj-perf-bench.out 2>/dev/null | tail -6 || true

# --- View 1: per-thread CPU utilization over the window (off-CPU detector) ---
echo
bc_echo "=== per-thread %CPU over the ${WALL}s window (LOW on main thread => blocked on I/O) ==="
awk -v clk="$CLK" -v wall="$WALL" '
  NR==FNR { c0[$1]=$3; next }
  { d=$3-c0[$1]; if (d<0) d=0; pct=(d/clk)/wall*100; printf "%7.1f  tid=%-8s %s\n", pct, $1, $2 }
' "$T0" "$T1" | sort -rn | head -20
echo "  (values are %CPU; 100% = one core fully busy. Sum across threads / cores.)"

# --- View 2: on-CPU perf, broken down per engine thread ---
echo
bc_echo "=== on-CPU samples per thread (which engine thread burned CPU) ==="
sudo perf report -i "$OUT" --stdio -s comm 2>/dev/null | grep -vE '^#|^$' | head -15
echo
bc_echo "=== hot functions per thread (comm / dso / symbol) ==="
sudo perf report -i "$OUT" --stdio -s comm,dso,symbol 2>/dev/null | grep -vE '^#|^$' | head -40

echo
bc_echo "deeper dives:"
echo "  interactive call graphs : sudo perf report -i $OUT"
echo "  syscall latency (off-CPU open() cost, ~5s, perturbs throughput):"
echo "      sudo perf trace -s -p $PID -- sleep 5"
echo "  (perf trace -s prints per-thread syscall count + total/avg/max latency —"
echo "   in non-pooling mode 'openat' should dominate the main thread's total time.)"
