#!/bin/bash
# EFA Registration Benchmark — real fi_mr_reg cost across 3 pool modes.
#
# bufpool: fi_mr_reg N buffers at startup. Per-request: zero cost.
# arena:   fi_mr_reg 1 segment at startup. Per-request: zero cost.
# dynamic: fi_mr_reg per buffer alloc (~1-5ms on EFA). Per-request cost.
#
# Uses same practices as bench.sh: taskset pinning, io-threads, bench-mode,
# high client count, duration-based, per-mode server restart.
#
# Usage: ./bench_efa.sh [data_dir] [port]
# Default: ./bench_efa.sh /tmp/lo-bench-data 6399

set -e

DATA_DIR="${1:-/tmp/lo-bench-data}"
PORT="${2:-6399}"

MODULE_SO="$(pwd)/target/release/libvalkey_largeobj.so"
VALKEY_SERVER="../valkey/src/valkey-server"
VALKEY_CLI="../valkey/src/valkey-cli"
VALKEY_BENCH="../valkey/src/valkey-benchmark"

# Benchmark params (matched to bench.sh)
BUF_SIZE=4096
BUFPOOL_COUNT=128
CLIENTS=750
DURATION=10
NUM_KEYS=500
IO_THREADS=8

# CPU pinning — i8ge has 192 CPUs (aarch64)
# Server: main thread + 8 io-threads + uring poller = 10 threads
# Bench: separate cores to avoid contention
SERVER_CPUS="0-9"
BENCH_CPUS="12-31"

echo "=============================================="
echo "EFA Registration Benchmark"
echo "=============================================="
echo "DATA_DIR:       $DATA_DIR"
echo "PORT:           $PORT"
echo "MODULE_SO:      $MODULE_SO"
echo "BUF_SIZE:       $BUF_SIZE (4KB)"
echo "BUFPOOL_COUNT:  $BUFPOOL_COUNT"
echo "CLIENTS:        $CLIENTS"
echo "DURATION:       ${DURATION}s"
echo "IO_THREADS:     $IO_THREADS"
echo "NUM_KEYS:       $NUM_KEYS"
echo "SERVER_CPUS:    $SERVER_CPUS"
echo "BENCH_CPUS:     $BENCH_CPUS"
echo "CPUs:           $NCPU"
echo "=============================================="
echo ""

# Verify binaries
for f in "$VALKEY_SERVER" "$VALKEY_CLI" "$VALKEY_BENCH" "$MODULE_SO"; do
    if [ ! -f "$f" ]; then echo "ERROR: $f not found"; exit 1; fi
done

cleanup() {
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# Populate keys via raw RESP (no pip)
populate() {
    echo "  Populating $NUM_KEYS keys (4KB each)..."
    python3 -c "
import socket, os, time
s = socket.socket(); s.connect(('127.0.0.1', $PORT))
s.setsockopt(6, 1, 1)
payload = os.urandom($BUF_SIZE)
start = time.monotonic()
for i in range($NUM_KEYS):
    key = f'k:{i:012d}'.encode()
    cmd = f'*3\r\n\$6\r\nLO.SET\r\n\${len(key)}\r\n'.encode() + key + b'\r\n'
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

    # Clean data dir
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    # Start server with taskset pinning, io-threads, bench-mode
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

    # Populate
    populate

    echo "  DBSIZE: $($VALKEY_CLI -p $PORT DBSIZE 2>/dev/null | awk '{print $NF}')"

    # LO.GET benchmark — duration-based, taskset-pinned clients
    echo ""
    echo "  ── LO.GET c=$CLIENTS duration=${DURATION}s ──"
    taskset -c $BENCH_CPUS $VALKEY_BENCH -p $PORT \
        --duration $DURATION -c $CLIENTS -r $NUM_KEYS \
        -- LO.GET "k:__rand_int__" 2>&1 | grep -E "throughput summary|avg"

    # Capture rps for summary
    RPS=$(taskset -c $BENCH_CPUS $VALKEY_BENCH -p $PORT \
        --duration $DURATION -c $CLIENTS -r $NUM_KEYS --csv \
        -- LO.GET "k:__rand_int__" 2>/dev/null | tail -1 | cut -d',' -f2 | tr -d '"')
    echo "  → $MODE: ${RPS} rps"
    echo "$MODE,$RPS" >> /tmp/efa_results.csv

    # Shutdown
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
}

# Results header
echo "mode,lo_get_rps" > /tmp/efa_results.csv

run_mode "bufpool"
run_mode "arena"
run_mode "dynamic"

echo ""
echo ""
echo "════════════════════════════════════════════════"
echo "         RESULTS (4KB LO.GET, bench-mode)"
echo "════════════════════════════════════════════════"
column -t -s',' /tmp/efa_results.csv
echo "════════════════════════════════════════════════"
echo ""
echo "dynamic = fi_mr_reg every pool_get() (~1-5ms on EFA)"
echo "bufpool/arena = fi_mr_reg at startup only (0ms per request)"
echo ""
echo "If dynamic ≈ bufpool, the EFA fi_mr_reg cost is too small to"
echo "measure at this concurrency (or the NVMe latency dominates)."
