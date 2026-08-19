#!/usr/bin/env bash
# fd-pool cap test.
#
# Goal: prove the read-fd pool stays bounded at ~FD_CAP (16384) under heavy GET
# traffic over a keyspace FAR larger than the cap, instead of opening one fd per
# object (which for a 1M keyspace would be ~1M fds). Optionally runs a concurrent
# LO.SET stream to show constant writes do NOT inflate the fd count (writes open
# O_WRONLY, write, close+rename — they never populate the read pool on this branch).
#
# Reports: GET throughput + latency, (optional) SET throughput, and the PEAK live
# fd count sampled during the run, with a PASS/FAIL against FD_CAP + FD_HEADROOM.
#
# Preconditions: server running (./bench-server.sh) and keyspace loaded
# (./bench-load.sh). Read-only run:  WRITE_CLIENTS=0 ./fd-cap-test.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

$CLI ping >/dev/null 2>&1 || { echo "[bench] no server on :$PORT — run ./bench-server.sh first" >&2; exit 1; }
PID="$(server_pid)"
[ -n "$PID" ] || { echo "[bench] can't find valkey-server pid" >&2; exit 1; }

DBSIZE="$($CLI DBSIZE | awk '{print $NF}')"
[ "${DBSIZE:-0}" -ge "$FD_CAP" ] || bc_echo "WARN: DBSIZE=$DBSIZE < FD_CAP=$FD_CAP — load more keys (./bench-load.sh) so reads exceed the cap."

BASELINE_FDS="$(count_fds "$PID")"
bc_echo "pid=$PID  DBSIZE=$DBSIZE  idle fds=$BASELINE_FDS  (pool starts cold; writes don't fill it)"

# --- background fd sampler: records the peak live fd count -----------------------
PEAK_FILE="$(mktemp)"; echo 0 > "$PEAK_FILE"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-0.5}"
(
  peak=0
  while :; do
    n="$(count_fds "$PID")"; [ -n "$n" ] || n=0
    [ "$n" -gt "$peak" ] && { peak="$n"; echo "$peak" > "$PEAK_FILE"; }
    sleep "$SAMPLE_INTERVAL"
  done
) & SAMPLER=$!
cleanup() { kill "$SAMPLER" 2>/dev/null || true; kill "${WRITER:-}" 2>/dev/null || true; rm -f "$PEAK_FILE"; }
trap cleanup EXIT

# --- optional concurrent write stream (constant write traffic) -------------------
WRITE_OUT="$(mktemp)"
if [ "${WRITE_CLIENTS:-0}" -gt 0 ]; then
  bc_echo "starting concurrent LO.SET stream: $WRITE_CLIENTS clients for ${DURATION}s"
  val="$(head -c "$OBJ_SIZE" /dev/zero | tr '\0' 'w')"
  taskset -c "$BENCH_CPUS" $BENCH --duration "$DURATION" -c "$WRITE_CLIENTS" -r "$KEYSPACE" \
    LO.SET "${KEY_PREFIX}__rand_int__" "$OBJ_SIZE" "$val" >"$WRITE_OUT" 2>&1 & WRITER=$!
fi

# --- GET workload (the thing that fills and bounds the pool) ---------------------
GET_OUT="$(mktemp)"
bc_echo "starting LO.GET workload: $CLIENTS clients, ${DURATION}s, keyspace=$KEYSPACE ..."
taskset -c "$BENCH_CPUS" $BENCH --duration "$DURATION" -c "$CLIENTS" -r "$KEYSPACE" \
  LO.GET "${KEY_PREFIX}__rand_int__" >"$GET_OUT" 2>&1 || true

# Wait for the writer to finish its window too.
[ -n "${WRITER:-}" ] && wait "$WRITER" 2>/dev/null || true
kill "$SAMPLER" 2>/dev/null || true

PEAK_FDS="$(cat "$PEAK_FILE")"
DAT_FILES="$(find "$DATA_DIR" -maxdepth 1 -name '*.dat' 2>/dev/null | wc -l || echo '?')"

get_rps="$(grep -iE 'requests per second' "$GET_OUT" | grep -oE '[0-9]+\.?[0-9]*' | head -1)"
write_rps="$(grep -iE 'requests per second' "$WRITE_OUT" 2>/dev/null | grep -oE '[0-9]+\.?[0-9]*' | head -1)"

echo
echo "================ fd-cap test results ================"
echo "GET summary:"
sed -n '/Summary:/,$p' "$GET_OUT" | sed 's/^/    /'
if [ "${WRITE_CLIENTS:-0}" -gt 0 ]; then
  echo "SET (concurrent) throughput: ${write_rps:-?} req/s"
fi
echo "-----------------------------------------------------"
echo "keyspace (DBSIZE)     : $DBSIZE objects"
echo "on-disk .dat files    : $DAT_FILES"
echo "idle fds (baseline)   : $BASELINE_FDS"
echo "PEAK live fds         : $PEAK_FDS"
echo "fd-pool cap (const)   : $FD_CAP  (+ headroom $FD_HEADROOM for sockets/in-flight)"
echo "GET throughput        : ${get_rps:-?} req/s"

# Approx object fds = peak minus the client sockets we knowingly opened.
approx_obj_fds=$(( PEAK_FDS - CLIENTS - WRITE_CLIENTS ))
echo "approx object fds     : ~$approx_obj_fds  (peak minus $((CLIENTS + WRITE_CLIENTS)) bench sockets)"
echo "-----------------------------------------------------"
echo "memory / fd overhead (post-run; pinned slab should track ~FD_CAP, not DBSIZE):"
# Drop reclaimable caches so the SUnreclaim that REMAINS is what the open pool
# fds pin (the 1M written-file inodes are reclaimable — not pinned — since writes
# close their fds). Skips gracefully without root.
if [ "${DROP_CACHES:-1}" = "1" ]; then
  sync 2>/dev/null || true
  sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches' 2>/dev/null \
    || echo "  (drop_caches skipped — needs root; SUnreclaim still includes reclaimable inode cache)"
fi
mem_fd_snapshot "$PID"
echo "  expect ~$FD_CAP pinned fds => ~$((FD_CAP * 3 / 2 / 1024)) MB pinned kernel mem (~1.5KB/fd),"
echo "  vs ~1 fd/object (~$DBSIZE fds, GBs pinned) for an unbounded per-object pool."
echo "-----------------------------------------------------"
LIMIT=$(( FD_CAP + FD_HEADROOM ))
if [ "$PEAK_FDS" -le "$LIMIT" ]; then
  echo "RESULT: PASS — peak $PEAK_FDS <= cap+headroom $LIMIT, and nowhere near DBSIZE=$DBSIZE."
  echo "        The pool bounded read fds to ~$FD_CAP and evicted the rest."
else
  echo "RESULT: FAIL — peak $PEAK_FDS > cap+headroom $LIMIT. Pool not bounding fds as expected."
fi
echo "====================================================="
rm -f "$GET_OUT" "$WRITE_OUT"
