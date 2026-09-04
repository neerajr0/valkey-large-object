#!/bin/bash
# One-time NVMe stripe setup for i8ge.48xlarge (or similar multi-NVMe instances).
#
# Creates an LVM striped volume across all NVMe instance store drives,
# formats with XFS, and mounts at /mnt/bigobj-data.
#
# WARNING: This DESTROYS all data on the NVMe instance store drives.
# Only run once on a fresh instance. Not idempotent.
#
# Usage:
#   sudo ./setup-nvme.sh
#
# Prerequisites:
#   - Instance with NVMe instance store drives (e.g., i8ge.48xlarge)
#   - Root access
#   - xfsprogs, lvm2 installed

set -e

MOUNT_POINT="/mnt/bigobj-data"
VG_NAME="bigobj_vg"
LV_NAME="bigobj_lv"

# Find all NVMe instance store drives (exclude root volume which is typically nvme0n1 or small)
echo "Discovering NVMe instance store drives..."
DRIVES=()
for dev in /dev/nvme*n1; do
    # Skip root volume (usually < 100GB)
    SIZE_GB=$(lsblk -dn -o SIZE "$dev" 2>/dev/null | sed 's/[^0-9.]//g' | cut -d. -f1)
    if [ -n "$SIZE_GB" ] && [ "$SIZE_GB" -gt 100 ]; then
        # Skip if it has partitions (likely root)
        PARTS=$(lsblk -dn -o TYPE "$dev" 2>/dev/null | grep -c part || true)
        if [ "$PARTS" -eq 0 ] || ! lsblk "$dev" | grep -q "part"; then
            DRIVES+=("$dev")
        fi
    fi
done

if [ ${#DRIVES[@]} -eq 0 ]; then
    echo "ERROR: No NVMe instance store drives found."
    exit 1
fi

echo "Found ${#DRIVES[@]} NVMe drives: ${DRIVES[*]}"
echo ""
echo "WARNING: This will DESTROY all data on these drives."
read -p "Continue? [y/N] " -r
if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Create physical volumes
echo "Creating physical volumes..."
for dev in "${DRIVES[@]}"; do
    pvcreate -f "$dev"
done

# Create volume group
echo "Creating volume group $VG_NAME..."
vgcreate "$VG_NAME" "${DRIVES[@]}"

# Create striped logical volume (stripe across all drives)
STRIPE_COUNT=${#DRIVES[@]}
echo "Creating striped logical volume ($STRIPE_COUNT stripes)..."
lvcreate -l 100%FREE -i "$STRIPE_COUNT" -n "$LV_NAME" "$VG_NAME"

# Format with XFS
echo "Formatting with XFS..."
mkfs.xfs -f "/dev/$VG_NAME/$LV_NAME"

# Mount
echo "Mounting at $MOUNT_POINT..."
mkdir -p "$MOUNT_POINT"
mount -o noatime,discard "/dev/$VG_NAME/$LV_NAME" "$MOUNT_POINT"

# Add to fstab for persistence across reboots
if ! grep -q "$VG_NAME/$LV_NAME" /etc/fstab; then
    echo "/dev/$VG_NAME/$LV_NAME $MOUNT_POINT xfs noatime,discard 0 0" >> /etc/fstab
    echo "Added to /etc/fstab"
fi

# Set permissions
chown "$(logname 2>/dev/null || echo ec2-user)":"$(logname 2>/dev/null || echo ec2-user)" "$MOUNT_POINT"

echo ""
echo "Done. NVMe stripe ready at $MOUNT_POINT"
echo ""
df -h "$MOUNT_POINT"
echo ""
echo "Verify with: lsblk"
