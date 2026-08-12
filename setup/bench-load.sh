#!/usr/bin/env bash
# Populate a dense keyspace of OBJ_SIZE-byte objects via BO.SET.
# Loads LOAD_OPS ops over KEYSPACE distinct keys (default 3x => ~95% dense),
# so a subsequent GET benchmark with the same -r hits almost every time.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

bc_echo "loading: $LOAD_OPS ops, keyspace=$KEYSPACE, obj-size=$OBJ_SIZE"
val="$(head -c "$OBJ_SIZE" /dev/zero | tr '\0' 'x')"
$BENCH -n "$LOAD_OPS" -r "$KEYSPACE" -c "$LOAD_CLIENTS" \
  BO.SET bo:key:__rand_int__ "$val"

bc_echo "done. BO.INFO:"
$CLI BO.INFO
