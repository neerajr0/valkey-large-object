# bigobj storage setup

Assembles a machine's local NVMe/SSD drives into a single LVM-striped XFS
volume mounted at `/mnt/bigobj-data`, ready for the valkey-bigobj module's
`data-dir`.

**Platform-agnostic.** The script has no cloud-provider knowledge. It inspects
the machine, auto-discovers which drives are safe to use, derives the stripe
geometry from how many it finds, and builds the volume — the same way on EC2
instance-store, bare metal, on-prem, or a local test rig.

## Layout

```
setup/
├── setup-storage.sh    # generic; discovers drives and builds the volume
├── setup.conf          # OPTIONAL — override tunables (not required)
└── README.md
```

## How drives are chosen

A whole disk is eligible **only if all** of these hold — a deliberately
conservative "empty, unclaimed, not the root disk" heuristic:

- it's a whole disk (not a partition/LV/loop)
- it is **not** the disk carrying the root filesystem
- it has no partitions or child devices
- it is not mounted anywhere
- it is not an existing LVM physical volume
- it is not active swap
- it is non-rotational (SSD/NVMe) — unless you pass `--allow-hdd`

The stripe count is then just the number of eligible drives found (1 drive →
linear volume, no striping).

## Usage

Assembling drives is **destructive** (`pvcreate` wipes them), so the script
**plans by default** and only executes with `--yes`.

```bash
# 1. Dry run — prints exactly which drives it would wipe. Changes nothing:
./setup-storage.sh

# 2. Execute once the plan looks right:
sudo ./setup-storage.sh --yes

# Override discovery with an explicit device list:
sudo ./setup-storage.sh --yes --devices "/dev/nvme1n1 /dev/nvme2n1"

# Other flags: --stripe-size 512K   --mount-point /data   --allow-hdd
```

Idempotent: if `/mnt/bigobj-data` is already mounted (or the LV exists), it does
nothing destructive.

Requires `lvm2` and `xfsprogs`. If missing, the script detects your package
manager and prints the exact install command (e.g. `sudo dnf install -y lvm2
xfsprogs`, or `sudo apt-get install -y lvm2 xfsprogs`).

## Tunables (`setup.conf`, all optional)

Drop a `setup.conf` next to the script to override any default:

```bash
VG_NAME=bigobj_vg
LV_NAME=bigobj_lv
MOUNT_POINT=/mnt/bigobj-data
MOUNT_OPTS="noatime,discard,inode64"
STRIPE_SIZE=256K          # per-drive stripe chunk
XFS_AGCOUNT=auto          # or pin a number, e.g. 256
ONLY_SSD=1                # 0 to allow spinning disks
MIN_DRIVES=1              # refuse to run with fewer eligible drives
```

## Getting this onto a remote host

```bash
scp -r bigobj/setup user@host:~/bigobj-setup
ssh user@host './bigobj-setup/setup-storage.sh'          # dry run first
ssh user@host 'sudo ./bigobj-setup/setup-storage.sh --yes'
```

## ⚠️ Ephemeral local disks (e.g. EC2 instance-store)

If the drives are ephemeral (cloud instance-store), their contents do **not**
survive a stop/terminate. The script deliberately does **not** write an
`/etc/fstab` entry (a persistent mount line for a vanished volume can block
boot). Re-run the script after each fresh boot; the idempotency guard makes that
safe.
