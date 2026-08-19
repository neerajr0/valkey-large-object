#!/usr/bin/env bash
#
# setup-storage.sh — assemble local NVMe/SSD drives into a single LVM-striped
# XFS volume for valkey-bigobj.
#
# Platform-agnostic: this script has NO cloud-provider knowledge. It inspects
# the machine, auto-discovers drives that are SAFE to use, derives the stripe
# geometry from how many it finds, and builds the volume. Works the same on
# EC2 instance-store, bare metal, an on-prem box, or a local test rig.
#
# ── How drives are selected ───────────────────────────────────────────────────
# A block device is eligible ONLY if ALL of these hold:
#   - it is a whole disk (TYPE=disk), not a partition/LV/loop device
#   - it is NOT the disk that carries the root filesystem
#   - it has NO partitions or child devices
#   - it is NOT mounted anywhere
#   - it is NOT an existing LVM physical volume
#   - it is NOT active swap
#   - (optionally) it is non-rotational, i.e. an SSD/NVMe  (ONLY_SSD=1, default)
# This "empty, unclaimed, not-the-root-disk" heuristic is deliberately
# conservative: we would rather skip a usable disk than wipe one in use.
#
# ── Safety ────────────────────────────────────────────────────────────────────
# Assembling drives is DESTRUCTIVE (pvcreate wipes them). The script therefore
# PLANS by default and only executes with an explicit --yes. Run it once with no
# flags to see exactly which devices it would wipe.
#
# Usage:
#   ./setup-storage.sh                 # dry run: print the plan, wipe nothing
#   sudo ./setup-storage.sh --yes      # execute
#   sudo ./setup-storage.sh --yes --devices "/dev/nvme1n1 /dev/nvme2n1"   # override discovery
#
# Config: optional ./setup.conf (sourced if present) overrides any tunable below.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Tunables (override via ./setup.conf or environment) ───────────────────────
: "${VG_NAME:=bigobj_vg}"
: "${LV_NAME:=bigobj_lv}"
: "${MOUNT_POINT:=/mnt/bigobj-data}"
: "${MOUNT_OPTS:=noatime,discard,inode64}"
# Stripe size per drive. "auto" is a sensible default for large-object I/O.
: "${STRIPE_SIZE:=256K}"
# XFS allocation groups. "auto" lets mkfs.xfs decide (recommended); or pin a number.
: "${XFS_AGCOUNT:=auto}"
# Only consider non-rotational (SSD/NVMe) disks. Set 0 to allow spinning disks.
: "${ONLY_SSD:=1}"
# Minimum drives required to proceed. 1 = allow a single-drive (linear) volume.
: "${MIN_DRIVES:=1}"
# Optional explicit device list (space-separated). Overrides auto-discovery.
: "${DEVICES:=}"

[ -f "${SCRIPT_DIR}/setup.conf" ] && { echo "[setup] sourcing setup.conf"; source "${SCRIPT_DIR}/setup.conf"; }

# ── Args ──────────────────────────────────────────────────────────────────────
ASSUME_YES=0
while [ $# -gt 0 ]; do
    case "$1" in
        --yes|-y)       ASSUME_YES=1 ;;
        --devices)      shift; DEVICES="${1:-}" ;;
        --stripe-size)  shift; STRIPE_SIZE="${1:-}" ;;
        --mount-point)  shift; MOUNT_POINT="${1:-}" ;;
        --allow-hdd)    ONLY_SSD=0 ;;
        -h|--help)      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)              echo "[setup] unknown arg: $1" >&2; exit 2 ;;
    esac
    shift
done

log() { echo "[setup] $*"; }
die() { echo "[setup] ERROR: $*" >&2; exit 1; }

# ── Required tools ────────────────────────────────────────────────────────────
# lvm2 provides pv*/vg*/lv*; xfsprogs provides mkfs.xfs; util-linux provides
# lsblk/findmnt/mount (usually already present).
missing=()
for bin in lsblk findmnt pvcreate vgcreate lvcreate mkfs.xfs mount pvs; do
    command -v "${bin}" >/dev/null 2>&1 || missing+=("${bin}")
done
if [ "${#missing[@]}" -gt 0 ]; then
    # Suggest the right install command for the detected package manager.
    if command -v dnf >/dev/null 2>&1;   then install_cmd="sudo dnf install -y lvm2 xfsprogs"
    elif command -v yum >/dev/null 2>&1; then install_cmd="sudo yum install -y lvm2 xfsprogs"
    elif command -v apt-get >/dev/null 2>&1; then install_cmd="sudo apt-get update && sudo apt-get install -y lvm2 xfsprogs"
    elif command -v zypper >/dev/null 2>&1;  then install_cmd="sudo zypper install -y lvm2 xfsprogs"
    elif command -v pacman >/dev/null 2>&1;  then install_cmd="sudo pacman -S --noconfirm lvm2 xfsprogs"
    else install_cmd="install the 'lvm2' and 'xfsprogs' packages with your package manager"
    fi
    log "missing required tool(s): ${missing[*]}"
    log "install them and re-run:"
    log "    ${install_cmd}"
    die "prerequisites not met"
fi

# ── Identify the root disk (never touch it) ───────────────────────────────────
root_src="$(findmnt -no SOURCE / 2>/dev/null || true)"          # e.g. /dev/nvme0n1p1 or /dev/mapper/...
ROOT_DISK=""
if [ -n "${root_src}" ]; then
    # Walk up to the parent whole-disk (pkname of pkname, until TYPE=disk).
    ROOT_DISK="$(lsblk -no PKNAME "${root_src}" 2>/dev/null | head -1)"
    [ -z "${ROOT_DISK}" ] && ROOT_DISK="$(basename "${root_src}")"
    # If root_src was an LV/partition, PKNAME may itself be a partition; resolve to disk.
    while [ -n "${ROOT_DISK}" ] && [ "$(lsblk -dno TYPE "/dev/${ROOT_DISK}" 2>/dev/null)" != "disk" ]; do
        parent="$(lsblk -no PKNAME "/dev/${ROOT_DISK}" 2>/dev/null | head -1)"
        [ -z "${parent}" ] && break
        ROOT_DISK="${parent}"
    done
fi
log "root filesystem is on disk: ${ROOT_DISK:-<unknown>}"

# ── Is a device an existing LVM PV? ───────────────────────────────────────────
is_pv() { pvs --noheadings -o pv_name 2>/dev/null | tr -d ' ' | grep -qx "$1"; }

# ── Discover eligible drives ──────────────────────────────────────────────────
discover_drives() {
    local dev name type rota mnt children
    lsblk -dno NAME,TYPE,ROTA | while read -r name type rota; do
        dev="/dev/${name}"
        [ "${type}" = "disk" ] || continue                       # whole disks only
        [ "${name}" = "${ROOT_DISK}" ] && continue                # never the root disk
        [ "${ONLY_SSD}" = "1" ] && [ "${rota}" = "1" ] && continue   # SSD/NVMe only (default)
        # must have no children (partitions/LVs)
        children="$(lsblk -rno NAME "${dev}" | tail -n +2)"
        [ -n "${children}" ] && continue
        # must not be mounted anywhere
        mnt="$(lsblk -rno MOUNTPOINT "${dev}" | grep -v '^$' || true)"
        [ -n "${mnt}" ] && continue
        # must not already be an LVM PV
        is_pv "${dev}" && continue
        echo "${dev}"
    done | sort -V
}

if [ -n "${DEVICES}" ]; then
    log "using explicitly provided devices (discovery skipped)"
    # shellcheck disable=SC2206
    DRIVES=(${DEVICES})
else
    mapfile -t DRIVES < <(discover_drives)
fi

COUNT="${#DRIVES[@]}"

# ── Idempotency: already set up? ──────────────────────────────────────────────
if findmnt -rn --target "${MOUNT_POINT}" >/dev/null 2>&1; then
    log "already mounted at ${MOUNT_POINT} — nothing to do."
    findmnt -rn --target "${MOUNT_POINT}"; exit 0
fi
if [ -e "/dev/${VG_NAME}/${LV_NAME}" ]; then
    log "LV /dev/${VG_NAME}/${LV_NAME} exists but is not mounted — mounting it."
    [ "${ASSUME_YES}" = "1" ] || { log "(dry run) would mount existing LV. Re-run with --yes."; exit 0; }
    mkdir -p "${MOUNT_POINT}"; mount -o "${MOUNT_OPTS}" "/dev/${VG_NAME}/${LV_NAME}" "${MOUNT_POINT}"
    log "mounted."; exit 0
fi

# ── Derive geometry from what we found ────────────────────────────────────────
[ "${COUNT}" -ge "${MIN_DRIVES}" ] || die "found ${COUNT} eligible drive(s), need at least ${MIN_DRIVES}. (Nothing to stripe.)"

STRIPE_COUNT="${COUNT}"
if [ "${COUNT}" -eq 1 ]; then
    LV_TYPE_ARGS=()                                              # single drive → linear, no striping
    GEOMETRY_DESC="linear (single drive)"
else
    LV_TYPE_ARGS=(--type striped --stripes "${STRIPE_COUNT}" --stripesize "${STRIPE_SIZE}")
    GEOMETRY_DESC="${STRIPE_COUNT}-way striped, stripe size ${STRIPE_SIZE}"
fi

MKFS_ARGS=(-f)
[ "${XFS_AGCOUNT}" != "auto" ] && MKFS_ARGS+=(-d agcount="${XFS_AGCOUNT}")

# ── Show the plan ─────────────────────────────────────────────────────────────
echo
log "──────────────── PLAN ────────────────"
log "eligible drives (${COUNT}): ${DRIVES[*]}"
log "volume group:   ${VG_NAME}"
log "logical volume: ${LV_NAME}  (${GEOMETRY_DESC})"
log "filesystem:     XFS ${XFS_AGCOUNT/auto/(agcount auto)}"
log "mount:          ${MOUNT_POINT}  opts=${MOUNT_OPTS}"
log "root disk (protected, never touched): ${ROOT_DISK:-<unknown>}"
log "──────────────────────────────────────"
echo

if [ "${ASSUME_YES}" != "1" ]; then
    log "DRY RUN — nothing changed. The above ${COUNT} drive(s) WILL BE WIPED on execute."
    log "Re-run with --yes to proceed:  sudo $0 --yes"
    exit 0
fi

# ── Must be root to execute ───────────────────────────────────────────────────
[ "$(id -u)" -eq 0 ] || die "must run as root to execute (use sudo)"

# ── Build: PV → VG → LV → mkfs → mount ────────────────────────────────────────
log "creating physical volumes..."
pvcreate -ff -y "${DRIVES[@]}"

log "creating volume group ${VG_NAME}..."
vgcreate "${VG_NAME}" "${DRIVES[@]}"

log "creating logical volume ${LV_NAME} (${GEOMETRY_DESC})..."
lvcreate "${LV_TYPE_ARGS[@]}" --extents 100%FREE --name "${LV_NAME}" "${VG_NAME}"

LV_PATH="/dev/${VG_NAME}/${LV_NAME}"

log "formatting ${LV_PATH} as XFS..."
mkfs.xfs "${MKFS_ARGS[@]}" "${LV_PATH}"

log "mounting at ${MOUNT_POINT} (opts: ${MOUNT_OPTS})..."
mkdir -p "${MOUNT_POINT}"
mount -o "${MOUNT_OPTS}" "${LV_PATH}" "${MOUNT_POINT}"
chmod 1777 "${MOUNT_POINT}"

# NOTE: intentionally NOT writing /etc/fstab. On ephemeral local disks (e.g. EC2
# instance-store) the volume disappears on stop, and a persistent mount line can
# block boot. Re-run this script after each fresh boot instead.

log "done."
findmnt -rn --target "${MOUNT_POINT}"
df -h "${MOUNT_POINT}"
