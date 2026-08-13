#!/usr/bin/env bash
# Shared config for the bigobj benchmark scripts. Sourced by the others.
# Every value is overridable from the environment, e.g.:
#   KEEP_READ_FDS=0 DIR_SHARDS=16384 ./bench-server.sh
#   OBJ_SIZE=1048576 ./bench-load.sh

# --- paths ---
: "${PORT:=6399}"
: "${DATA_DIR:=/mnt/bigobj-data}"
: "${VALKEY_SRC:=$HOME/valkey/src}"
: "${LV:=/dev/bigobj_vg/bigobj_lv}"        # logical volume (for reset/reformat)
: "${MOUNT_OPTS:=noatime,discard,inode64}"
: "${LOGFILE:=/tmp/bigobj.log}"

# module .so: default = ../target/release relative to this script
_CFG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${MODULE:=$(cd "$_CFG_DIR/.." && pwd)/target/release/libvalkey_bigobj.so}"

# --- server / module config ---
: "${OBJ_SIZE:=4096}"          # object size in bytes (also pool-buf-size)
: "${POOL_BUF_COUNT:=5000}"    # io_uring registered buffer count
: "${KEEP_READ_FDS:=1}"        # 1 = pool fds, 0 = open-per-GET
: "${DIR_SHARDS:=1}"           # 1 = flat, 16384 = per-Valkey-slot subdirs
: "${OPEN_THREADS:=0}"         # N open() worker threads (0 = inline; non-pooling only)
: "${FD_LIMIT:=20000000}"      # ulimit -n for the server
: "${SERVER_CPUS:=0-31}"       # taskset for valkey-server

# --- workload ---
: "${KEYSPACE:=1000000}"       # -r  (distinct key range)
: "${LOAD_OPS:=3000000}"       # -n for load (3x keyspace => ~95% dense)
: "${LOAD_CLIENTS:=50}"
: "${CLIENTS:=750}"            # -c for GET benchmark
: "${DURATION:=60}"            # --duration for GET benchmark
: "${BENCH_CPUS:=32-63}"       # taskset for valkey-benchmark

# --- derived handles ---
SERVER="$VALKEY_SRC/valkey-server"
CLI="$VALKEY_SRC/valkey-cli -p $PORT"
BENCH="$VALKEY_SRC/valkey-benchmark -p $PORT"

bc_echo() { echo "[bench] $*"; }
