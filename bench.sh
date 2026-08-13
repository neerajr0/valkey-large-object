#!/bin/bash
# ValkeyLargeObj Benchmark Script
# Phase 1: fio baselines (raw disk).
# Phase 2: Python populates keys, valkey-benchmark measures LO.GET throughput.
#
# Design: LO.GET in bench-mode returns integer (size) — no bulk payload transfer.
# This isolates NVMe + io_uring + Valkey event loop overhead from network bandwidth.
# Python handles LO.SET population because valkey-benchmark can't pass >4KB inline payloads.
#
# Prerequisites:
#   - XFS/ext4 mount at DATA_DIR (O_DIRECT capable, ideally LVM-striped NVMe)
#   - valkey-server, valkey-cli, valkey-benchmark in PATH or env vars
#   - Module built: cargo build --release
#   - Python 3 (no pip packages needed — uses raw RESP sockets)
#
# Usage:
#   ./bench.sh <DATA_DIR> <PORT> [--skip-fio]
#
# Options:
#   --skip-fio   Skip Phase 1 (fio baseline), run only the module benchmark
#
# Example (i8ge — LVM-striped NVMe):
#   ./bench.sh /mnt/bigobj-data 7380
#   ./bench.sh /mnt/bigobj-data 7380 --skip-fio   # module only
#
# Example (dev desktop — gp3 EBS):
#   mkdir -p /tmp/lo-bench && ./bench.sh /tmp/lo-bench 7380

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <DATA_DIR> <PORT> [--skip-fio]"
    echo ""
    echo "  DATA_DIR    Path to the storage directory (O_DIRECT capable)"
    echo "  PORT        Valkey server port"
    echo "  --skip-fio  Skip fio baseline, run only module benchmark"
    echo ""
    echo "Examples:"
    echo "  ./bench.sh /mnt/bigobj-data 7380             # full run"
    echo "  ./bench.sh /mnt/bigobj-data 7380 --skip-fio  # module only"
    exit 1
fi

DATA_DIR="$1"
PORT="$2"
SKIP_FIO=0
if [ "${3:-}" = "--skip-fio" ]; then
    SKIP_FIO=1
fi
MODULE_SO="${MODULE_SO:-$(dirname $0)/target/release/libvalkey_largeobj.so}"
VALKEY_SERVER="${VALKEY_SERVER:-valkey-server}"
VALKEY_CLI="${VALKEY_CLI:-valkey-cli}"
VALKEY_BENCH="${VALKEY_BENCH:-valkey-benchmark}"

# Object sizes to test
SIZES_LABEL=("4KB" "1MB" "16MB" "50MB")
SIZES_BYTES=(4096 1048576 16777216 52428800)
SIZES_FIO=("4k" "1m" "16m" "50m")

# Concurrency levels per object size (pool has 10000 buffers)
CONC_4KB=750
CONC_1MB=750
CONC_16MB=750
CONC_50MB=750
CONCURRENCIES=($CONC_4KB $CONC_1MB $CONC_16MB $CONC_50MB)

# Benchmark parameters
DURATION=10
NUM_KEYS=500

echo "=============================================="
echo "ValkeyLargeObj Benchmark"
echo "=============================================="
echo "DATA_DIR:    $DATA_DIR"
echo "PORT:        $PORT"
echo "MODULE_SO:   $MODULE_SO"
echo "SIZES:       ${SIZES_LABEL[*]}"
echo "CONCURRENCY: ${CONCURRENCIES[*]}"
echo "DURATION:    ${DURATION}s"
echo "KEYS:        $NUM_KEYS"
echo "=============================================="
echo ""

mkdir -p "$DATA_DIR"

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 1: fio baseline (raw disk throughput)
# ──────────────────────────────────────────────────────────────────────────────

if [ $SKIP_FIO -eq 0 ]; then

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 1: fio baseline (O_DIRECT random read)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

FIO_FILE="$DATA_DIR/fio_testfile"

for i in "${!SIZES_LABEL[@]}"; do
    SIZE_LABEL="${SIZES_LABEL[$i]}"
    SIZE_FIO="${SIZES_FIO[$i]}"
    SIZE_BYTES="${SIZES_BYTES[$i]}"

    # Test file: 4GB minimum to defeat NVMe controller cache
    FILE_SIZE=4294967296

    echo ""
    echo "--- fio: $SIZE_LABEL random read (numjobs=16, iodepth=64, O_DIRECT, io_uring) ---"
    fio --name=randread_${SIZE_LABEL} \
        --filename="$FIO_FILE" \
        --size=${FILE_SIZE} \
        --bs=${SIZE_FIO} \
        --rw=randread \
        --ioengine=io_uring \
        --direct=1 \
        --iodepth=128 \
        --numjobs=16 \
        --runtime=10 \
        --time_based \
        --group_reporting \
        --output-format=terse \
        2>/dev/null | awk -F';' '{printf "  IOPS: %s  BW: %s KB/s  lat_avg: %s us\n", $8, $7, $16}'
done

rm -f "$FIO_FILE"

echo ""
echo ""

fi  # end SKIP_FIO

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 2: Module benchmark (Python populate → valkey-benchmark LO.GET)
# ──────────────────────────────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 2: module benchmark (bench-mode)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check module exists
if [ ! -f "$MODULE_SO" ]; then
    echo "ERROR: Module not found at $MODULE_SO"
    echo "Build with: cargo build --release"
    exit 1
fi

# ── Python helper: populate keys with LO.SET ──
POPULATE_PY="$DATA_DIR/.populate.py"
cat > "$POPULATE_PY" << 'PYTHON_EOF'
#!/usr/bin/env python3
"""Populate keys for LO.GET benchmark. Writes random data via LO.SET using raw RESP."""
import sys
import os
import time
import socket

def resp_command(*args):
    """Encode a RESP array command."""
    parts = [f"*{len(args)}\r\n"]
    for arg in args:
        if isinstance(arg, bytes):
            parts.append(f"${len(arg)}\r\n")
            return ("".join(parts)).encode() + arg + b"\r\n"
        else:
            s = str(arg)
            parts.append(f"${len(s)}\r\n{s}\r\n")
    return ("".join(parts)).encode()

def read_reply(sock):
    """Read one RESP reply (simple: +, -, :, $)."""
    line = b""
    while not line.endswith(b"\r\n"):
        line += sock.recv(1)
    line = line[:-2]
    if line[0:1] == b'+' or line[0:1] == b'-' or line[0:1] == b':':
        return line.decode()
    elif line[0:1] == b'$':
        n = int(line[1:])
        if n == -1:
            return None
        data = b""
        while len(data) < n + 2:
            data += sock.recv(n + 2 - len(data))
        return data[:-2]
    return line.decode()

port = int(sys.argv[1])
size_bytes = int(sys.argv[2])
num_keys = int(sys.argv[3])
key_prefix = sys.argv[4]

payload = os.urandom(size_bytes)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
sock.connect(('127.0.0.1', port))

start = time.monotonic()
for i in range(num_keys):
    key = f"{key_prefix}{i:012d}"
    cmd = resp_command("LO.SET", key, str(size_bytes), payload)
    sock.sendall(cmd)
    read_reply(sock)
elapsed = time.monotonic() - start

sock.close()
rps = num_keys / elapsed
print(f"  Populated {num_keys} keys ({size_bytes} bytes each) in {elapsed:.1f}s ({rps:.0f} keys/s)")
PYTHON_EOF

# For each object size
for i in "${!SIZES_LABEL[@]}"; do
    SIZE_LABEL="${SIZES_LABEL[$i]}"
    SIZE_BYTES="${SIZES_BYTES[$i]}"

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Object size: $SIZE_LABEL ($SIZE_BYTES bytes)"
    echo "═══════════════════════════════════════════════"

    # Start fresh server with pool-buf-size matching this object size
    $VALKEY_SERVER --port $PORT --daemonize yes \
        --logfile "$DATA_DIR/bench-server.log" \
        --pidfile "$DATA_DIR/bench-server.pid" \
        --loadmodule "$MODULE_SO" data-dir "$DATA_DIR" \
            pool-buf-size $SIZE_BYTES \
            pool-buf-count 1000 \
            bench-mode yes \
        --save "" \
        --appendonly no \
        --io-threads 1
    sleep 1
    if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
        echo "  ERROR: Server failed to start. Check $DATA_DIR/bench-server.log"
        tail -5 "$DATA_DIR/bench-server.log"
        continue
    fi

    # ── Populate keys with Python (handles arbitrary payload sizes) ──
    # Key format: "lo:4KB:000000000000" through "lo:4KB:000000000499"
    KEY_PREFIX="lo:${SIZE_LABEL}:"
    python3 "$POPULATE_PY" $PORT $SIZE_BYTES $NUM_KEYS "$KEY_PREFIX"
    echo "  DBSIZE: $($VALKEY_CLI -p $PORT DBSIZE 2>/dev/null | awk '{print $NF}')"

    # ── LO.SET benchmark (only 4KB — larger sizes can't pass inline to valkey-benchmark) ──
    # ── LO.GET benchmark (NVMe read + integer reply) ──
    echo ""
    echo "  ── LO.GET (NVMe read + integer reply) ──"
    c=${CONCURRENCIES[$i]}
    RPS=$($VALKEY_BENCH -p $PORT --duration $DURATION -c $c -r $NUM_KEYS --csv \
        -- LO.GET "${KEY_PREFIX}__rand_int__" 2>/dev/null \
        | grep -v "test" | tail -1 | cut -d',' -f2 | tr -d '"')
    printf "    c=%-4d  %10s rps\n" $c "${RPS:-FAILED}"

    # Shutdown server for this size
    echo ""
    echo "  Shutting down..."
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
done

# Cleanup
rm -f "$POPULATE_PY"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Done."
echo ""
echo "Compare LO.GET rps to fio IOPS at same object size."
echo "Ratio = overhead the module/Valkey event loop adds over raw disk."
