#!/usr/bin/env bash
# Clean-slate the workspace by reformatting the XFS filesystem (fast, even with
# millions of files). Does NOT touch the LVM stripe — the volume group / striped
# logical volume persist; only the filesystem on top is re-created (mkfs auto-
# detects and re-applies the stripe alignment).
#
# Use between benchmark configs so accumulated orphan .dat files don't confound
# results.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/bench-config.sh"

bc_echo "shutting down any running server on :$PORT ..."
$CLI SHUTDOWN NOSAVE 2>/dev/null || true
sleep 1

bc_echo "unmounting $DATA_DIR (LVM stripe is preserved) ..."
sudo umount "$DATA_DIR" 2>/dev/null || true

bc_echo "reformatting $LV as XFS ..."
sudo mkfs.xfs -f "$LV"

bc_echo "remounting $DATA_DIR (opts: $MOUNT_OPTS) ..."
sudo mkdir -p "$DATA_DIR"
sudo mount -o "$MOUNT_OPTS" "$LV" "$DATA_DIR"
sudo chmod 1777 "$DATA_DIR"

bc_echo "clean. .dat files: $(sudo find "$DATA_DIR" -name '*.dat' | wc -l) (expect 0)"
