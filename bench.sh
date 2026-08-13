#!/bin/bash
# ValkeyLargeObj Benchmark Script
# Phase 1: fio baselines (raw disk, all 16 NVMe striped).
# Phase 2: Per-size server restart with matched pool-buf-size.
#           Python populates keys, valkey-benchmark measures LO.GET (10s duration).
#
# Design: LO.GET in bench-mode does full NVMe io_uring read but replies with
# integer size only (no bulk TCP copy). Isolates storage path from network BW.
#
# Prerequisites:
#   - XFS/ext4 mount at DATA_DIR (O_DIRECT capable, ideally LVM-striped NVMe)
#   - valkey-server, valkey-cli, valkey-benchmark in PATH
#   - Module built: cargo build --release
#   - Python 3 (no pip packages — uses raw RESP sockets)
#
# Usage:
#   ./bench.sh <DATA_DIR> <PORT> [--skip-fio]
#
# Examples:
#   ./bench.sh /mnt/bigobj-data 7380              # full run (fio + module)
#   ./bench.sh /mnt/bigobj-data 7380 --skip-fio   # module only

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <DATA_DIR> <PORT> [--skip-fio]"
    echo ""
    echo "  DATA_DIR    Path to storage (O_DIRECT capable, LVM-striped NVMe ideal)"
    echo "  PORT        Valkey server port"
    echo "  --skip-fio  Skip fio baseline, run only module benchmark"
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

# Benchmark parameters
CLIENTS=750
DURATION=10
NUM_KEYS=500
POOL_BUF_COUNT=1000
IO_THREADS=8

echo "=============================================="
echo "ValkeyLargeObj Benchmark"
echo "=============================================="
echo "DATA_DIR:       $DATA_DIR"
echo "PORT:           $PORT"
echo "MODULE_SO:      $MODULE_SO"
echo "SIZES:          ${SIZES_LABEL[*]}"
echo "CLIENTS:        $CLIENTS"
echo "DURATION:       ${DURATION}s"
echo "POOL_BUF_COUNT: $POOL_BUF_COUNT"
echo "IO_THREADS:     $IO_THREADS"
echo "KEYS:           $NUM_KEYS"
echo "=============================================="
echo ""

mkdir -p "$DATA_DIR"

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 1: fio baseline (raw disk throughput, all NVMe disks)
# ──────────────────────────────────────────────────────────────────────────────

if [ $SKIP_FIO -eq 0 ]; then

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 1: fio baseline (O_DIRECT random read)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

FIO_FILE="$DATA_DIR/fio_testfile"

for i in "${!SIZES_LABEL[@]}"; do
    SIZE_LABEL="${SIZES_LABEL[$i]}"
    SIZE_FIO="${SIZES_FIO[$i]}"

    # 4GB test file to defeat controller cache
    FILE_SIZE=4294967296

    echo ""
    echo "--- fio: $SIZE_LABEL random read (numjobs=16, iodepth=128, O_DIRECT, io_uring) ---"
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
# PHASE 2: Module benchmark (per-size server, io-threads, LO.GET duration-based)
# ──────────────────────────────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 2: module benchmark (io-threads=$IO_THREADS, bench-mode)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check module exists
if [ ! -f "$MODULE_SO" ]; then
    echo "ERROR: Module not found at $MODULE_SO"
    echo "Build with: cargo build --release"
    exit 1
fi

# For each object size: start fresh server with matched pool-buf-size
for i in "${!SIZES_LABEL[@]}"; do
    SIZE_LABEL="${SIZES_LABEL[$i]}"
    SIZE_BYTES="${SIZES_BYTES[$i]}"

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Object size: $SIZE_LABEL ($SIZE_BYTES bytes)"
    echo "═══════════════════════════════════════════════"

    # Clean data dir
    rm -f "$DATA_DIR"/*.dat "$DATA_DIR"/*.tmp

    # Start server with pool-buf-size matching object size
    taskset -c 0-9 $VALKEY_SERVER --port $PORT --daemonize yes \
        --logfile "$DATA_DIR/bench-server.log" \
        --pidfile "$DATA_DIR/bench-server.pid" \
        --loadmodule "$MODULE_SO" data-dir "$DATA_DIR" \
            pool-buf-size $SIZE_BYTES \
            pool-buf-count $POOL_BUF_COUNT \
            bench-mode yes \
        --save "" \
        --appendonly no \
        --io-threads $IO_THREADS
    sleep 1

    if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
        echo "  ERROR: Server failed to start. Check $DATA_DIR/bench-server.log"
        tail -5 "$DATA_DIR/bench-server.log"
        continue
    fi

    # Populate keys via raw RESP (zero dependencies)
    python3 -c "
import socket, os, time
def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        if isinstance(a, bytes):
            parts.append(f'\${len(a)}\r\n')
            return ''.join(parts).encode() + a + b'\r\n'
        s = str(a)
        parts.append(f'\${len(s)}\r\n{s}\r\n')
    return ''.join(parts).encode()
s = socket.socket(); s.connect(('127.0.0.1', $PORT))
s.setsockopt(6, 1, 1)
payload = os.urandom($SIZE_BYTES)
start = time.monotonic()
for i in range($NUM_KEYS):
    s.sendall(resp('LO.SET', f'k:{i:012d}', '$SIZE_BYTES', payload))
    s.recv(1024)
elapsed = time.monotonic() - start
s.close()
print(f'  Populated $NUM_KEYS keys ($SIZE_LABEL) in {elapsed:.1f}s ({$NUM_KEYS/elapsed:.0f} keys/s)')
"

    echo "  DBSIZE: $($VALKEY_CLI -p $PORT DBSIZE 2>/dev/null | awk '{print $NF}')"

    # LO.GET benchmark (duration-based, io-threads offload network)
    echo ""
    echo "  ── LO.GET c=$CLIENTS duration=${DURATION}s ──"
    taskset -c 12-31 $VALKEY_BENCH -p $PORT --duration $DURATION -c $CLIENTS -r $NUM_KEYS \
        -- LO.GET "k:__rand_int__" 2>&1 | grep -E "throughput summary|avg"
    echo ""

    # Shutdown server
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Done."
