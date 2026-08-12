#!/bin/bash
# ValkeyLargeObj Benchmark Script
# Runs fio baselines + valkey-benchmark for LO.SET/LO.GET at multiple object sizes.
#
# Prerequisites:
#   - XFS mount at $DATA_DIR (O_DIRECT capable)
#   - valkey-server, valkey-cli, valkey-benchmark in PATH or set below
#   - Module built: cargo build --release (use target/release/libvalkey_largeobj.so)
#
# Usage:
#   ./bench.sh [DATA_DIR] [PORT]
#
# Example (i8ge):
#   ./bench.sh /mnt/bigobj-data 7380
#
# Example (dev desktop):
#   mkdir -p /tmp/lo-bench && ./bench.sh /tmp/lo-bench 7380

set -e

DATA_DIR="${1:-/tmp/lo-bench}"
PORT="${2:-7380}"
MODULE_SO="${MODULE_SO:-$(dirname $0)/target/release/libvalkey_largeobj.so}"
VALKEY_SERVER="${VALKEY_SERVER:-valkey-server}"
VALKEY_CLI="${VALKEY_CLI:-valkey-cli}"
VALKEY_BENCH="${VALKEY_BENCH:-valkey-benchmark}"

# Object sizes to test
SIZES_LABEL=("4KB" "1MB" "16MB" "50MB")
SIZES_BYTES=(4096 1048576 16777216 52428800)
SIZES_FIO=("4k" "1m" "16m" "50m")

# Concurrency levels
CONCURRENCIES=(1 8 50 100)

# Number of ops per benchmark
NUM_OPS=10000
NUM_KEYS=500  # pre-populated keys per size

echo "=============================================="
echo "ValkeyLargeObj Benchmark"
echo "=============================================="
echo "DATA_DIR:   $DATA_DIR"
echo "PORT:       $PORT"
echo "MODULE_SO:  $MODULE_SO"
echo "SIZES:      ${SIZES_LABEL[*]}"
echo "CONCURRENCY: ${CONCURRENCIES[*]}"
echo "=============================================="
echo ""

mkdir -p "$DATA_DIR"

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 1: fio baseline (raw disk throughput)
# ──────────────────────────────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 1: fio baseline (O_DIRECT random read)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

FIO_FILE="$DATA_DIR/fio_testfile"

for i in "${!SIZES_LABEL[@]}"; do
    SIZE_LABEL="${SIZES_LABEL[$i]}"
    SIZE_FIO="${SIZES_FIO[$i]}"
    SIZE_BYTES="${SIZES_BYTES[$i]}"

    # Create test file (at least 1GB or 1000x object size, whichever is smaller)
    FILE_SIZE=$(( SIZE_BYTES * 500 ))
    if [ $FILE_SIZE -gt 1073741824 ]; then
        FILE_SIZE=1073741824
    fi
    if [ $FILE_SIZE -lt $SIZE_BYTES ]; then
        FILE_SIZE=$SIZE_BYTES
    fi

    echo ""
    echo "--- fio: $SIZE_LABEL random read (iodepth=64, O_DIRECT) ---"
    fio --name=randread_${SIZE_LABEL} \
        --filename="$FIO_FILE" \
        --size=${FILE_SIZE} \
        --bs=${SIZE_FIO} \
        --rw=randread \
        --ioengine=io_uring \
        --direct=1 \
        --iodepth=64 \
        --numjobs=1 \
        --runtime=10 \
        --time_based \
        --group_reporting \
        --output-format=terse \
        2>/dev/null | awk -F';' '{printf "  IOPS: %s  BW: %s KB/s  lat_avg: %s us\n", $8, $7, $16}'
done

rm -f "$FIO_FILE"

echo ""
echo ""

# ──────────────────────────────────────────────────────────────────────────────
# PHASE 2: valkey-benchmark (LO.SET + LO.GET with random keys)
# ──────────────────────────────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 2: valkey-benchmark (module, bench-mode)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check module exists
if [ ! -f "$MODULE_SO" ]; then
    echo "ERROR: Module not found at $MODULE_SO"
    echo "Build with: cargo build --release"
    exit 1
fi

# Start Valkey with module
echo "Starting Valkey on port $PORT..."
$VALKEY_SERVER --port $PORT --daemonize yes \
    --logfile "$DATA_DIR/bench-server.log" \
    --pidfile "$DATA_DIR/bench-server.pid" \
    --loadmodule "$MODULE_SO" data-dir "$DATA_DIR" \
        pool-buf-size 52428800 \
        pool-buf-count 128 \
        bench-mode yes \
    --save "" \
    --appendonly no

sleep 1

if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
    echo "ERROR: Server failed to start. Check $DATA_DIR/bench-server.log"
    tail -20 "$DATA_DIR/bench-server.log"
    exit 1
fi
echo "Server running."
echo ""

# Function to generate payload of given size
gen_payload() {
    python3 -c "import sys; sys.stdout.write('X' * $1)"
}

# For each object size
for i in "${!SIZES_LABEL[@]}"; do
    SIZE_LABEL="${SIZES_LABEL[$i]}"
    SIZE_BYTES="${SIZES_BYTES[$i]}"

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Object size: $SIZE_LABEL ($SIZE_BYTES bytes)"
    echo "═══════════════════════════════════════════════"

    # ── Populate keys for GET benchmark ──
    echo "  Populating $NUM_KEYS keys..."
    PAYLOAD=$(gen_payload $SIZE_BYTES)
    for k in $(seq 1 $NUM_KEYS); do
        $VALKEY_CLI -p $PORT LO.SET "${SIZE_LABEL}:key:$k" $SIZE_BYTES "$PAYLOAD" > /dev/null
    done
    echo "  Done. DBSIZE: $($VALKEY_CLI -p $PORT DBSIZE | awk '{print $2}')"

    # ── LO.SET benchmark (write path) ──
    echo ""
    echo "  ── LO.SET (write: client → NVMe) ──"
    for c in "${CONCURRENCIES[@]}"; do
        RESULT=$($VALKEY_BENCH -p $PORT -n $NUM_OPS -c $c -r $NUM_KEYS \
            -- LO.SET "__rand_key__" $SIZE_BYTES "$PAYLOAD" 2>&1 | grep "throughput\|avg")
        RPS=$(echo "$RESULT" | grep throughput | awk '{print $3}')
        LATENCY=$(echo "$RESULT" | grep avg | awk '{print $2}')
        printf "    c=%-4d  %10s rps  avg=%s ms\n" $c "$RPS" "$LATENCY"
    done

    # ── LO.GET benchmark (read path, bench-mode = reply size only) ──
    echo ""
    echo "  ── LO.GET (read: NVMe → reply size, bench-mode) ──"
    for c in "${CONCURRENCIES[@]}"; do
        RESULT=$($VALKEY_BENCH -p $PORT -n $NUM_OPS -c $c -r $NUM_KEYS \
            -- LO.GET "${SIZE_LABEL}:key:__rand_int__" 2>&1 | grep "throughput\|avg")
        RPS=$(echo "$RESULT" | grep throughput | awk '{print $3}')
        LATENCY=$(echo "$RESULT" | grep avg | awk '{print $2}')
        printf "    c=%-4d  %10s rps  avg=%s ms\n" $c "$RPS" "$LATENCY"
    done

    # Flush keys for this size to free disk
    echo ""
    echo "  Flushing keys..."
    $VALKEY_CLI -p $PORT FLUSHDB > /dev/null
done

# Shutdown
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Shutting down Valkey..."
$VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
echo "Done."
echo ""
echo "Results above. Compare LO.GET rps to fio IOPS for the same object size."
echo "The ratio shows how much overhead the module adds over raw disk."
