#!/usr/bin/env bash
# Start valkey-server with the bigobj module. Kills any existing server first.
#
# Named flags (each overrides its bench-config.sh default / env var):
#   -k, --keep-read-fds <0|1>       0 = open-per-GET (non-pooling), 1 = pool fds
#   -d, --dir-shards    <1..16384>  1 = flat, 16384 = one dir per Valkey slot
#   -o, --open-threads  <N>         N worker threads for on-demand open()
#                                   (0 = inline on main thread; only used when
#                                    keep-read-fds=0)
#   -h, --help
#
# Examples:
#   ./bench-server.sh                                              # config defaults
#   ./bench-server.sh --keep-read-fds 0 --dir-shards 1 --open-threads 1
#   ./bench-server.sh -k 0 -d 1024 -o 1
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

KRF="$KEEP_READ_FDS"
DS="$DIR_SHARDS"
OT="$OPEN_THREADS"

usage() {
  grep -E '^#( |$)' "${BASH_SOURCE[0]}" | sed 's/^#\{1\} \{0,1\}//'
}
need_val() { [ -n "${2:-}" ] || { echo "[bench] $1 needs a value" >&2; exit 1; }; }

while [ $# -gt 0 ]; do
  case "$1" in
    -k|--keep-read-fds) need_val "$1" "${2:-}"; KRF="$2"; shift 2 ;;
    -d|--dir-shards)    need_val "$1" "${2:-}"; DS="$2";  shift 2 ;;
    -o|--open-threads)  need_val "$1" "${2:-}"; OT="$2";  shift 2 ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "[bench] unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

[ -f "$MODULE" ] || { echo "[bench] ERROR: module not found: $MODULE (cargo build --release?)" >&2; exit 1; }

bc_echo "stopping existing server (if any) ..."
$CLI SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

bc_echo "starting: keep-read-fds=$KRF dir-shards=$DS open-threads=$OT obj-size=$OBJ_SIZE cpus=$SERVER_CPUS"
sudo bash -c "ulimit -n $FD_LIMIT; exec taskset -c $SERVER_CPUS '$SERVER' --port $PORT \
  --loadmodule '$MODULE' \
  data-dir '$DATA_DIR' pool-buf-size $OBJ_SIZE pool-buf-count $POOL_BUF_COUNT \
  keep-read-fds $KRF dir-shards $DS open-threads $OT \
  --save '' --logfile '$LOGFILE' --daemonize yes"
sleep 2

bc_echo "config line from $LOGFILE:"
grep -E 'initialized' "$LOGFILE" | tail -1
bc_echo "ping: $($CLI ping 2>&1)"
