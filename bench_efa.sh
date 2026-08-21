#!/bin/bash
# EFA Registration Benchmark — measures real fi_mr_reg impact across 3 pool modes.
#
# bufpool: fi_mr_reg N buffers at startup. Per-request: zero registration cost.
# arena:   fi_mr_reg 1 segment at startup. Per-request: zero registration cost.
# dynamic: fi_mr_reg per buffer alloc (~1-5ms each on EFA hardware). Per-request cost.
#
# Run on EFA machine: ec2-user@16.147.230.226
# Usage: ./bench_efa.sh [buf_size_kb] [num_keys] [clients]
#
# Requires:
#   - ../valkey/src/valkey-server
#   - ../valkey/src/valkey-benchmark
#   - ./target/release/libvalkey_largeobj.so

set -e

BUF_SIZE_KB=${1:-4}
NUM_KEYS=${2:-1000}
CLIENTS=${3:-50}
BUF_SIZE=$((BUF_SIZE_KB * 1024))
DURATION=10
PORT=6399
DATA_DIR="/tmp/lo-bench-data"
SERVER="../valkey/src/valkey-server"
BENCH="../valkey/src/valkey-benchmark"
MODULE="./target/release/libvalkey_largeobj.so"

BUFPOOL_COUNT=128
ARENA_TOTAL=$((BUFPOOL_COUNT * BUF_SIZE))

echo "=== EFA Registration Benchmark ==="
echo "Buffer size: ${BUF_SIZE_KB}KB | Keys: ${NUM_KEYS} | Clients: ${CLIENTS} | Duration: ${DURATION}s"
echo ""

for f in "$SERVER" "$BENCH" "$MODULE"; do
    if [ ! -f "$f" ]; then echo "ERROR: $f not found"; exit 1; fi
done

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# Populate keys via raw Python RESP (no pip needed).
populate() {
    local N=$1
    echo "  Populating $N keys (${BUF_SIZE_KB}KB each)..."
    python3 - "$PORT" "$N" "$BUF_SIZE" << 'PYEOF'
import socket, sys, os
port, n, size = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
payload = b"X" * size  # Deterministic fill
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", port))
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

# Pipeline in batches of 100
batch = 100
for start in range(0, n, batch):
    end = min(start + batch, n)
    buf = b""
    for i in range(start, end):
        key = f"key:{i:012d}".encode()
        buf += f"*3\r\n$6\r\nLO.SET\r\n${len(key)}\r\n".encode() + key + b"\r\n"
        buf += f"${len(payload)}\r\n".encode() + payload + b"\r\n"
    sock.sendall(buf)
    # Read responses
    resp = b""
    expected = end - start
    count = 0
    while count < expected:
        resp += sock.recv(65536)
        count = resp.count(b"\r\n")  # Each response has at least one \r\n

sock.close()
print(f"  Populated {n} keys")
PYEOF
}

run_mode() {
    local MODE=$1
    echo "--- Mode: $MODE ---"

    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    # Start server
    $SERVER \
        --port $PORT \
        --daemonize no \
        --loglevel warning \
        --save "" \
        --appendonly no \
        --io-threads 4 \
        --loadmodule "$MODULE" \
        --pool-mode "$MODE" \
        --pool-buf-size "$BUF_SIZE" \
        --pool-buf-count "$BUFPOOL_COUNT" \
        --arena-total-size "$ARENA_TOTAL" \
        --data-dir "$DATA_DIR" \
        --direct-io yes \
        > /tmp/valkey_bench_${MODE}.log 2>&1 &
    SERVER_PID=$!
    sleep 2

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  ERROR: Server failed to start. Check /tmp/valkey_bench_${MODE}.log"
        tail -10 /tmp/valkey_bench_${MODE}.log
        SERVER_PID=""
        return 1
    fi

    # Populate
    populate "$NUM_KEYS"

    # Benchmark LO.GET with valkey-benchmark
    echo "  Benchmarking LO.GET (${DURATION}s, ${CLIENTS} clients)..."
    $BENCH \
        -p $PORT \
        -c $CLIENTS \
        --threads 4 \
        -r "$NUM_KEYS" \
        --duration "$DURATION" \
        --csv \
        -- LO.GET "key:__rand_int__" \
        > /tmp/bench_${MODE}.csv 2>/dev/null

    # Extract rps from CSV output
    GET_RPS=$(grep -i "lo.get\|\"LO" /tmp/bench_${MODE}.csv | head -1 | cut -d',' -f2 | tr -d '"' || echo "0")
    if [ -z "$GET_RPS" ] || [ "$GET_RPS" = "0" ]; then
        # Try alternate CSV format
        GET_RPS=$(tail -1 /tmp/bench_${MODE}.csv | cut -d',' -f2 | tr -d '"')
    fi
    echo "  LO.GET: ${GET_RPS} rps"
    echo "$MODE,$BUF_SIZE_KB,$GET_RPS" >> /tmp/efa_results.csv

    # Stop server
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
    sleep 1
}

echo "mode,buf_size_kb,lo_get_rps" > /tmp/efa_results.csv

echo ""
echo "▶ bufpool — fi_mr_reg ${BUFPOOL_COUNT} buffers at startup"
run_mode "bufpool"

echo ""
echo "▶ arena — fi_mr_reg 1 segment at startup"
run_mode "arena"

echo ""
echo "▶ dynamic — fi_mr_reg PER BUFFER ALLOC"
run_mode "dynamic"

echo ""
echo "════════════════════════════════════════"
echo "         RESULTS"
echo "════════════════════════════════════════"
column -t -s',' /tmp/efa_results.csv
echo "════════════════════════════════════════"
echo ""
echo "dynamic = fi_mr_reg every pool_get() (~1-5ms on EFA hardware)"
echo "bufpool/arena = fi_mr_reg at startup only (0ms per request)"
