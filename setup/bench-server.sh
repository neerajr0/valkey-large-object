#!/usr/bin/env bash
# Start valkey-server with the bigobj module. Kills any existing server first.
#
# Args (optional, override env/defaults):
#   $1 = keep-read-fds (0|1)
#   $2 = dir-shards     (1..16384)
# Examples:
#   ./bench-server.sh            # defaults (KEEP_READ_FDS=1 DIR_SHARDS=1)
#   ./bench-server.sh 0 16384    # non-pooling, per-slot sharding
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

KRF="${1:-$KEEP_READ_FDS}"
DS="${2:-$DIR_SHARDS}"

[ -f "$MODULE" ] || { echo "[bench] ERROR: module not found: $MODULE (cargo build --release?)" >&2; exit 1; }

bc_echo "stopping existing server (if any) ..."
$CLI SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

bc_echo "starting: keep-read-fds=$KRF dir-shards=$DS obj-size=$OBJ_SIZE cpus=$SERVER_CPUS"
sudo bash -c "ulimit -n $FD_LIMIT; exec taskset -c $SERVER_CPUS '$SERVER' --port $PORT \
  --loadmodule '$MODULE' \
  data-dir '$DATA_DIR' pool-buf-size $OBJ_SIZE pool-buf-count $POOL_BUF_COUNT \
  keep-read-fds $KRF dir-shards $DS \
  --save '' --logfile '$LOGFILE' --daemonize yes"
sleep 2

bc_echo "config line from $LOGFILE:"
grep -E 'initialized|keep_read_fds|dir_shards' "$LOGFILE" | tail -1
bc_echo "ping: $($CLI ping 2>&1)"
