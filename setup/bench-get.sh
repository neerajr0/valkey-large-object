#!/usr/bin/env bash
# Run the GET benchmark N times (default 3) with a cold cache before each, then
# print a memory/fd snapshot. Prints the Summary block (RPS + latency) per run.
#
# Arg: $1 = number of runs (default 3)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

RUNS="${1:-3}"

for r in $(seq 1 "$RUNS"); do
  bc_echo "=== GET run $r/$RUNS (cold cache, $CLIENTS clients, ${DURATION}s) ==="
  sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  taskset -c "$BENCH_CPUS" $BENCH -c "$CLIENTS" --duration "$DURATION" \
    -r "$KEYSPACE" BO.GET bo:key:__rand_int__ | sed -n '/Summary:/,$p'
done

echo
bc_echo "=== memory / fd snapshot ==="
PID="$(pgrep -o valkey-server || true)"
if [ -n "$PID" ]; then
  echo "object_count: $($CLI BO.INFO | tr ',' ' ')"
  echo "open fds    : $(sudo ls /proc/$PID/fd | wc -l)"
  echo "RSS         : $(sudo grep VmRSS /proc/$PID/status | awk '{print $2" "$3}')"
  grep -E 'Slab|SReclaimable|SUnreclaim' /proc/meminfo
  sudo grep -E 'xfs_inode|^dentry|^filp' /proc/slabinfo | \
    awk '{printf "%-16s active_objs=%s objsize=%s\n",$1,$3,$4}'
  echo "dat files   : $(sudo find "$DATA_DIR" -name '*.dat' | wc -l)"
else
  echo "(no valkey-server running)"
fi
