#!/usr/bin/env bash
# Populate a dense keyspace of OBJ_SIZE-byte objects via LO.SET.
# Loads LOAD_OPS ops over KEYSPACE distinct keys (default 3x => ~95% dense), so a
# later GET workload with the same -r hits an already-written object almost always.
#
# LO.SET syntax:  LO.SET <key> <len> <value-bytes>
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

$CLI ping >/dev/null 2>&1 || { echo "[bench] no server on :$PORT — run ./bench-server.sh first" >&2; exit 1; }

bc_echo "loading: $LOAD_OPS ops over keyspace=$KEYSPACE, obj-size=$OBJ_SIZE (${LOAD_CLIENTS} clients)"
val="$(head -c "$OBJ_SIZE" /dev/zero | tr '\0' 'x')"

taskset -c "$BENCH_CPUS" $BENCH -n "$LOAD_OPS" -r "$KEYSPACE" -c "$LOAD_CLIENTS" \
  LO.SET "${KEY_PREFIX}__rand_int__" "$OBJ_SIZE" "$val" \
  | grep -E 'throughput summary|requests per second' || true

bc_echo "DBSIZE: $($CLI DBSIZE | awk '{print $NF}')"
