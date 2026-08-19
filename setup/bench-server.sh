#!/usr/bin/env bash
# Start valkey-server with the bigobj (largeobj) module for the fd-pool test.
# Kills any existing server on $PORT first.
#
# The ulimit -n MUST exceed the fd-pool cap (16384) plus client sockets, or the
# pool can't fill and the server hits EMFILE. We raise the soft limit to
# $FD_LIMIT; if the hard limit is lower, re-run under sudo or raise the hard
# limit (see the error message).
#
#   ./bench-server.sh            # start with config defaults / env overrides
#   ./bench-server.sh --stop     # just stop the running server
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

if [ "${1:-}" = "--stop" ]; then
  bc_echo "stopping server on :$PORT ..."
  $CLI SHUTDOWN NOSAVE 2>/dev/null || true
  exit 0
fi

[ -f "$MODULE" ] || { echo "[bench] ERROR: module not found: $MODULE (cargo build --release?)" >&2; exit 1; }
mkdir -p "$DATA_DIR"

bc_echo "stopping existing server (if any) ..."
$CLI SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

# Raise the fd soft limit for THIS shell; the server inherits it.
if ! ulimit -n "$FD_LIMIT" 2>/dev/null; then
  hard="$(ulimit -Hn)"
  echo "[bench] ERROR: can't raise 'ulimit -n' to $FD_LIMIT (hard limit=$hard)." >&2
  echo "[bench]        The fd-pool cap is $FD_CAP, so you need well above that." >&2
  echo "[bench]        Re-run under sudo, or: sudo prlimit --nofile=$FD_LIMIT --pid \$\$" >&2
  exit 1
fi
bc_echo "ulimit -n = $(ulimit -n)  (fd-pool cap = $FD_CAP)"

bc_echo "starting: obj-size=$OBJ_SIZE pool-buf-count=$POOL_BUF_COUNT io-threads=$IO_THREADS cpus=$SERVER_CPUS"
taskset -c "$SERVER_CPUS" "$VALKEY_SERVER" --port "$PORT" --daemonize yes \
  --logfile "$LOGFILE" --pidfile "$PIDFILE" \
  --loadmodule "$MODULE" data-dir "$DATA_DIR" \
    pool-buf-size "$OBJ_SIZE" \
    pool-buf-count "$POOL_BUF_COUNT" \
    bench-mode yes \
  --save '' --appendonly no --io-threads "$IO_THREADS"
sleep 2

bc_echo "init line from $LOGFILE:"
grep -E 'initialized' "$LOGFILE" | tail -1 || true
bc_echo "ping: $($CLI ping 2>&1)"
