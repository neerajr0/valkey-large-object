#!/bin/bash
# EFA Registration Benchmark — real fi_mr_reg cost across 3 pool modes.
#
# Usage: ./bench_efa.sh <data_dir> <port> <buf_size_bytes> [clients] [num_keys] [pool_count]
#
# Examples:
#   ./bench_efa.sh /data/lo-bench 6399 4096          # 4KB
#   ./bench_efa.sh /data/lo-bench 6399 1048576       # 1MB
#   ./bench_efa.sh /data/lo-bench 6399 52428800      # 50MB

set -e

DATA_DIR="${1:?Usage: $0 <data_dir> <port> <buf_size>}"
PORT="${2:?Usage: $0 <data_dir> <port> <buf_size>}"
BUF_SIZE="${3:?Usage: $0 <data_dir> <port> <buf_size>}"
CLIENTS="${4:-750}"
NUM_KEYS="${5:-500}"
BUFPOOL_COUNT="${6:-4096}"

MODULE_SO="$(pwd)/target/release/libvalkey_largeobj.so"
VALKEY_SERVER="../valkey/src/valkey-server"
VALKEY_CLI="../valkey/src/valkey-cli"
VALKEY_BENCH="../valkey/src/valkey-benchmark"

DURATION=10
IO_THREADS=8

# CPU pinning — i8ge (192 CPUs)
SERVER_CPUS="0-9"
BENCH_CPUS="12-31"

# Human-readable size
if [ "$BUF_SIZE" -ge 1048576 ]; then
    SIZE_LABEL="$((BUF_SIZE / 1048576))MB"
elif [ "$BUF_SIZE" -ge 1024 ]; then
    SIZE_LABEL="$((BUF_SIZE / 1024))KB"
else
    SIZE_LABEL="${BUF_SIZE}B"
fi

echo "=============================================="
echo "EFA Registration Benchmark"
echo "=============================================="
echo "DATA_DIR:       $DATA_DIR"
echo "PORT:           $PORT"
echo "BUF_SIZE:       $BUF_SIZE ($SIZE_LABEL)"
echo "BUFPOOL_COUNT:  $BUFPOOL_COUNT"
echo "CLIENTS:        $CLIENTS"
echo "DURATION:       ${DURATION}s"
echo "IO_THREADS:     $IO_THREADS"
echo "NUM_KEYS:       $NUM_KEYS"
echo "SERVER_CPUS:    $SERVER_CPUS"
echo "BENCH_CPUS:     $BENCH_CPUS"
echo "=============================================="
echo ""

for f in "$VALKEY_SERVER" "$VALKEY_CLI" "$VALKEY_BENCH" "$MODULE_SO"; do
    if [ ! -f "$f" ]; then echo "ERROR: $f not found"; exit 1; fi
done

cleanup() {
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

populate() {
    echo "  Populating $NUM_KEYS keys ($SIZE_LABEL each)..."
    python3 -c "
import socket, os, time
s = socket.socket(); s.connect(('127.0.0.1', $PORT))
s.setsockopt(6, 1, 1)
payload = os.urandom($BUF_SIZE)
len_str = b'$BUF_SIZE'
start = time.monotonic()
for i in range($NUM_KEYS):
    key = f'k:{i:012d}'.encode()
    cmd = f'*4\r\n\$6\r\nLO.SET\r\n\${len(key)}\r\n'.encode() + key + b'\r\n'
    cmd += f'\${len(len_str)}\r\n'.encode() + len_str + b'\r\n'
    cmd += f'\${len(payload)}\r\n'.encode() + payload + b'\r\n'
    s.sendall(cmd)
    s.recv(1024)
elapsed = time.monotonic() - start
s.close()
print(f'  Done: $NUM_KEYS keys in {elapsed:.1f}s ({$NUM_KEYS/elapsed:.0f} keys/s)')
"
}

run_mode() {
    local MODE=$1
    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Mode: $MODE"
    echo "═══════════════════════════════════════════════"

    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    taskset -c $SERVER_CPUS $VALKEY_SERVER --port $PORT --daemonize yes \
        --logfile "$DATA_DIR/bench-server.log" \
        --pidfile "$DATA_DIR/bench-server.pid" \
        --loadmodule "$MODULE_SO" data-dir "$DATA_DIR" \
            pool-mode $MODE \
            pool-buf-size $BUF_SIZE \
            pool-buf-count $BUFPOOL_COUNT \
            bench-mode yes \
            direct-io yes \
        --save "" \
        --appendonly no \
        --io-threads $IO_THREADS
    sleep 2

    if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
        echo "  ERROR: Server failed to start. Log:"
        tail -10 "$DATA_DIR/bench-server.log"
        return 1
    fi

    populate

    echo "  DBSIZE: $($VALKEY_CLI -p $PORT DBSIZE 2>/dev/null | awk '{print $NF}')"

    echo ""
    echo "  ── LO.GET c=$CLIENTS duration=${DURATION}s ──"
    taskset -c $BENCH_CPUS $VALKEY_BENCH -p $PORT \
        --duration $DURATION -c $CLIENTS -r $NUM_KEYS \
        -- LO.GET "k:__rand_int__" 2>&1 | grep -E "throughput summary|avg"

    RPS=$(taskset -c $BENCH_CPUS $VALKEY_BENCH -p $PORT \
        --duration $DURATION -c $CLIENTS -r $NUM_KEYS --csv \
        -- LO.GET "k:__rand_int__" 2>/dev/null | tail -1 | cut -d',' -f2 | tr -d '"')
    echo "  → $MODE: ${RPS} rps"
    echo "$MODE,$RPS" >> /tmp/efa_results.csv

    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
}

echo "mode,lo_get_rps" > /tmp/efa_results.csv

run_mode "bufpool"
run_mode "arena"
run_mode "dynamic"

echo ""
echo ""
echo "════════════════════════════════════════════════"
echo "         RESULTS ($SIZE_LABEL LO.GET, bench-mode)"
echo "════════════════════════════════════════════════"
column -t -s',' /tmp/efa_results.csv
echo "════════════════════════════════════════════════"
echo ""
echo "dynamic = fi_mr_reg every pool_get()"
echo "bufpool/arena = fi_mr_reg at startup only"
