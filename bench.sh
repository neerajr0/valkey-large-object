#!/bin/bash
# ValkeyLargeObj Benchmark Script
#
# Supports Dram and Tiered operating modes with current config names.
# Per-size server restart. Python populates keys, valkey-benchmark measures LO.GET.
#
# bench-mode: LO.GET does full storage path but replies with integer size only
# (no TCP bulk copy). Isolates storage throughput from network bandwidth.
#
# Prerequisites:
#   - valkey-server, valkey-cli, valkey-benchmark in PATH
#   - Module built: cargo build --release
#   - Python 3 (no pip packages)
#   - For Tiered mode: O_DIRECT capable mount (XFS/ext4 on NVMe)
#   - For fio baselines: fio installed
#
# Usage:
#   ./bench.sh --mode <Dram|Tiered> --port <PORT> [options]
#
# Examples:
#   ./bench.sh --mode Dram --port 7380
#   ./bench.sh --mode Tiered --nvme-dir /mnt/nvme --port 7380
#   ./bench.sh --mode Tiered --nvme-dir /mnt/nvme --port 7380 --fio
#   ./bench.sh --mode Tiered --nvme-dir /mnt/nvme --port 7380 --sizes "4KB 50KB 1MB" --clients 500

set -e

# ─── Defaults ─────────────────────────────────────────────────────────────────

MODE="Dram"
PORT=""
NVME_DIR=""
RUN_FIO=0
CLIENTS=50
DURATION=10
NUM_KEYS=500
DRAM_MAXMEMORY="1073741824"       # 1GB
DRAM_SEGMENT_SIZE="67108864"      # 64MB
NVME_MAXMEMORY="10737418240"     # 10GB
NVME_STAGING_SIZE="67108864"      # 64MB
SIZES_STR="4KB 1MB"
WORKER_THREADS=2
NO_PROMOTE=0

# ─── Parse args ───────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)          MODE="$2"; shift 2 ;;
        --port)          PORT="$2"; shift 2 ;;
        --nvme-dir)      NVME_DIR="$2"; shift 2 ;;
        --fio)           RUN_FIO=1; shift ;;
        --clients)       CLIENTS="$2"; shift 2 ;;
        --duration)      DURATION="$2"; shift 2 ;;
        --keys)          NUM_KEYS="$2"; shift 2 ;;
        --sizes)         SIZES_STR="$2"; shift 2 ;;
        --dram-maxmemory)    DRAM_MAXMEMORY="$2"; shift 2 ;;
        --dram-segment-size) DRAM_SEGMENT_SIZE="$2"; shift 2 ;;
        --nvme-maxmemory)    NVME_MAXMEMORY="$2"; shift 2 ;;
        --worker-threads)    WORKER_THREADS="$2"; shift 2 ;;
        --no-promote)        NO_PROMOTE=1; shift ;;
        --help|-h)
            echo "Usage: $0 --mode <Dram|Tiered> --port <PORT> [options]"
            echo ""
            echo "Options:"
            echo "  --mode <Dram|Tiered>       Operating mode (default: Dram)"
            echo "  --port <PORT>              Valkey server port (required)"
            echo "  --nvme-dir <DIR>           NVMe directory (required for Tiered)"
            echo "  --fio                      Run fio baselines (Tiered only)"
            echo "  --clients <N>              Benchmark clients (default: 50)"
            echo "  --duration <SEC>           Duration per size (default: 10)"
            echo "  --keys <N>                 Number of keys to populate (default: 500)"
            echo "  --sizes <\"4KB 1MB ...\">    Object sizes to test (default: \"4KB 1MB\")"
            echo "  --dram-maxmemory <BYTES>   DRAM budget in bytes (default: 1073741824 = 1GB)"
            echo "  --dram-segment-size <BYTES> Segment size in bytes (default: 67108864 = 64MB)"
            echo "  --nvme-maxmemory <BYTES>   NVMe budget in bytes (default: 10737418240 = 10GB)"
            echo "  --worker-threads <N>       Tokio threads (default: 2)"
            echo "  --no-promote               Disable DRAM promotion on GET (Tiered: pure NVMe reads)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$PORT" ]; then
    echo "ERROR: --port is required"
    exit 1
fi

if [ "$MODE" = "Tiered" ] && [ -z "$NVME_DIR" ]; then
    echo "ERROR: --nvme-dir is required for Tiered mode"
    exit 1
fi

MODULE_SO="${MODULE_SO:-$(dirname "$0")/target/release/libvalkey_largeobj.so}"
VALKEY_SERVER="${VALKEY_SERVER:-valkey-server}"
VALKEY_CLI="${VALKEY_CLI:-valkey-cli}"
VALKEY_BENCH="${VALKEY_BENCH:-valkey-benchmark}"

# Parse sizes string into arrays
declare -a SIZES_LABEL
declare -a SIZES_BYTES
for s in $SIZES_STR; do
    SIZES_LABEL+=("$s")
    case "$s" in
        4KB)   SIZES_BYTES+=(4096) ;;
        16KB)  SIZES_BYTES+=(16384) ;;
        50KB)  SIZES_BYTES+=(51200) ;;
        256KB) SIZES_BYTES+=(262144) ;;
        1MB)   SIZES_BYTES+=(1048576) ;;
        4MB)   SIZES_BYTES+=(4194304) ;;
        16MB)  SIZES_BYTES+=(16777216) ;;
        50MB)  SIZES_BYTES+=(52428800) ;;
        *)     echo "Unknown size: $s (use 4KB, 16KB, 50KB, 256KB, 1MB, 4MB, 16MB, 50MB)"; exit 1 ;;
    esac
done

# ─── Header ───────────────────────────────────────────────────────────────────

echo "=============================================="
echo "ValkeyLargeObj Benchmark"
echo "=============================================="
echo "Mode:           $MODE"
echo "Port:           $PORT"
[ "$MODE" = "Tiered" ] && echo "NVMe dir:       $NVME_DIR"
echo "Module:         $MODULE_SO"
echo "Sizes:          ${SIZES_LABEL[*]}"
echo "Clients:        $CLIENTS"
echo "Duration:       ${DURATION}s"
echo "Keys:           $NUM_KEYS"
echo "DRAM maxmem:    $DRAM_MAXMEMORY ($((DRAM_MAXMEMORY / 1048576))MB)"
echo "DRAM segment:   $DRAM_SEGMENT_SIZE ($((DRAM_SEGMENT_SIZE / 1048576))MB)"
[ "$MODE" = "Tiered" ] && echo "NVMe maxmem:    $NVME_MAXMEMORY ($((NVME_MAXMEMORY / 1048576))MB)"
echo "Worker threads: $WORKER_THREADS"
[ $NO_PROMOTE -eq 1 ] && echo "Promotion:      DISABLED (pure NVMe reads)"
echo "=============================================="
echo ""

# ─── fio baseline (Tiered only) ──────────────────────────────────────────────

if [ $RUN_FIO -eq 1 ] && [ "$MODE" = "Tiered" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "fio baseline (O_DIRECT random read, io_uring)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    FIO_FILE="$NVME_DIR/fio_testfile"
    FIO_SIZE=4294967296  # 4GB

    for i in "${!SIZES_LABEL[@]}"; do
        LABEL="${SIZES_LABEL[$i]}"
        BYTES="${SIZES_BYTES[$i]}"
        echo ""
        echo "--- fio: $LABEL random read ---"
        fio --name="randread_${LABEL}" \
            --filename="$FIO_FILE" \
            --size=$FIO_SIZE \
            --bs=$BYTES \
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
fi

# ─── Check module ────────────────────────────────────────────────────────────

if [ ! -f "$MODULE_SO" ]; then
    echo "ERROR: Module not found at $MODULE_SO"
    echo "Build with: cargo build --release"
    exit 1
fi

# ─── Per-size benchmark ──────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Module benchmark (mode=$MODE, bench-mode=yes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for i in "${!SIZES_LABEL[@]}"; do
    LABEL="${SIZES_LABEL[$i]}"
    BYTES="${SIZES_BYTES[$i]}"

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  $LABEL ($BYTES bytes) — $MODE mode"
    echo "═══════════════════════════════════════════════"

    # Build module args
    MODULE_ARGS="operating-mode $MODE"
    MODULE_ARGS="$MODULE_ARGS dram-maxmemory $DRAM_MAXMEMORY"
    MODULE_ARGS="$MODULE_ARGS dram-segment-size $DRAM_SEGMENT_SIZE"
    MODULE_ARGS="$MODULE_ARGS worker-threads $WORKER_THREADS"
    MODULE_ARGS="$MODULE_ARGS bench-mode yes"
    if [ $NO_PROMOTE -eq 1 ]; then
        MODULE_ARGS="$MODULE_ARGS max-promote-size 0"
    fi

    if [ "$MODE" = "Tiered" ]; then
        MODULE_ARGS="$MODULE_ARGS nvme-dir $NVME_DIR"
        MODULE_ARGS="$MODULE_ARGS nvme-maxmemory $NVME_MAXMEMORY"
        MODULE_ARGS="$MODULE_ARGS nvme-staging-size $NVME_STAGING_SIZE"
        # Clean nvme-dir
        rm -rf "$NVME_DIR"
        mkdir -p "$NVME_DIR"
    fi

    # Start server
    $VALKEY_SERVER --port $PORT --daemonize yes \
        --logfile /tmp/bench-server-$PORT.log \
        --pidfile /tmp/bench-server-$PORT.pid \
        --loadmodule "$MODULE_SO" $MODULE_ARGS \
        --save "" \
        --appendonly no
    sleep 1

    if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
        echo "  ERROR: Server failed to start. Check /tmp/bench-server-$PORT.log"
        tail -5 /tmp/bench-server-$PORT.log 2>/dev/null
        continue
    fi

    # Populate keys via raw RESP
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
payload = os.urandom($BYTES)
start = time.monotonic()
for i in range($NUM_KEYS):
    s.sendall(resp('LO.SET', f'k:{i:012d}', payload))
    r = s.recv(1024)
elapsed = time.monotonic() - start
s.close()
print(f'  Populated $NUM_KEYS keys ($LABEL) in {elapsed:.1f}s ({$NUM_KEYS/elapsed:.0f} keys/s)')
"

    echo "  DBSIZE: $($VALKEY_CLI -p $PORT DBSIZE 2>/dev/null | awk '{print $NF}')"

    # LO.GET benchmark
    echo ""
    echo "  ── LO.GET c=$CLIENTS duration=${DURATION}s ──"
    $VALKEY_BENCH -p $PORT --duration $DURATION -c $CLIENTS -r $NUM_KEYS \
        -- LO.GET "k:__rand_int__" 2>&1 | tr '\r' '\n' | grep -E "throughput summary|avg.*min.*p50"
    echo ""

    # Shutdown
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Done."
