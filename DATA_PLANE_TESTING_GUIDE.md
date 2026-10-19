# valkey-large-object — Data Plane Testing Guide

This guide walks through provisioning EC2 instances with EFA support,
installing Valkey with the `valkey-large-object` module, and running
functional test scripts that exercise all client-facing commands over
TCP and EFA RDMA. Covers both self-managed standalone instances and
DubDevo managed clusters.

---

## Architecture Overview

The module adds six commands to Valkey for storing and retrieving large objects
(KV cache tensors, embeddings, blobs). Two operating modes:

- **Dram** — objects live exclusively in a pre-allocated DRAM pool.
- **Tiered** — objects persist on NVMe; the DRAM pool acts as a read cache with
  automatic promotion.

| Command | Description |
|---------|-------------|
| `BLOB.TCP_SET key <data>` | Store object over TCP |
| `BLOB.TCP_GET key` | Retrieve object over TCP |
| `BLOB.RDMA_SET key total_len rkey addr len [...]` | Store object via RDMA |
| `BLOB.RDMA_GET key rkey addr len [...]` | Retrieve object via RDMA |
| `BLOB.RDMA_HELLO client_efa_addr_hex` | Establish RDMA session |
| `BLOB.INFO key [LEN\|CRC\|TIER]` | Object metadata |

Standard Valkey commands (`DEL`, `COPY`, `PEXPIRE`, `FLUSHALL`, `MEMORY USAGE`,
`CONFIG SET/GET`) work on module keys.

---

## 1. Infrastructure Provisioning

### Prerequisites

- AWS CLI v2 installed
- Credentials to your AWS account with permissions to launch EC2 instances:
  ```bash
  ada credentials update --account <ACCOUNT_ID> --role Admin --provider isengard --once
  ```

### 1.1 Create a Cluster Placement Group

Both instances must be in the same placement group for EFA connectivity.

```bash
aws ec2 create-placement-group \
  --group-name valkey-large-object-test \
  --strategy cluster \
  --region us-east-1
```

### 1.2 Create a Security Group

EFA RDMA requires a security group that allows **all traffic to and from itself**
— both inbound AND outbound. The default `0.0.0.0/0` outbound rule is **not
sufficient**; EFA uses link-local IPv6 addresses (`fe80::...`) and requires the
explicit self-referencing outbound rule for the return path.

```bash
VPC_ID=$(aws ec2 describe-vpcs --region us-east-1 \
  --filters "Name=isDefault,Values=true" \
  --query 'Vpcs[0].VpcId' --output text)

SG_ID=$(aws ec2 create-security-group \
  --group-name valkey-large-object-test-sg \
  --description "valkey-large-object test - allow all traffic within group" \
  --vpc-id "$VPC_ID" \
  --region us-east-1 \
  --query 'GroupId' --output text)

# Inbound: all traffic from the same group (required for EFA)
aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol -1 \
  --source-group "$SG_ID" \
  --region us-east-1

# Outbound: all traffic TO the same group (also required for EFA)
# Without this, RDMA_HELLO works (plain TCP) but RDMA_SET/GET hang with
# "No route to host (err=113)" in server logs.
aws ec2 authorize-security-group-egress \
  --group-id "$SG_ID" \
  --protocol -1 \
  --source-group "$SG_ID" \
  --region us-east-1

# SSH from anywhere
aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp \
  --port 22 \
  --cidr 0.0.0.0/0 \
  --region us-east-1
```

### 1.3 Create a Key Pair

```bash
aws ec2 create-key-pair \
  --key-name valkey-large-object-test-key \
  --key-type ed25519 \
  --query 'KeyMaterial' \
  --output text \
  --region us-east-1 > valkey-large-object-test-key.pem

chmod 400 valkey-large-object-test-key.pem
```

### 1.4 Find the Latest AL2023 arm64 AMI

```bash
AMI_ID=$(aws ec2 describe-images \
  --owners amazon \
  --filters "Name=name,Values=al2023-ami-*-kernel-6.1-arm64" \
            "Name=state,Values=available" \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
  --output text \
  --region us-east-1)
echo "AMI: $AMI_ID"
```

### 1.5 Pick a Subnet

Use a public subnet in `us-east-1a` (placement group and EFA require same AZ).

```bash
SUBNET_ID=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" \
            "Name=availability-zone,Values=us-east-1a" \
            "Name=map-public-ip-on-launch,Values=true" \
  --query 'Subnets[0].SubnetId' --output text \
  --region us-east-1)
echo "Subnet: $SUBNET_ID"
```

### 1.6 Launch Instances

Launch two instances with EFA network interfaces. Any NVMe-backed instance type
with EFA support works — `i8g.48xlarge` and `i8ge.48xlarge` are both tested.

**Important:** Not all EFA-enabled instance types support full RDMA read+write.
Some (e.g., `c7gn.16xlarge`) only support RDMA read, which is insufficient for
`BLOB.RDMA_SET`. Verify capabilities on a running instance:

```bash
export PATH=/opt/amazon/efa/bin:$PATH

# Read-only (should return exit 0):
fi_info -p efa -f efa-direct -t FI_EP_RDM \
  -c "FI_MSG|FI_RECV|FI_SEND|FI_READ|FI_REMOTE_READ"

# Read+write (must return exit 0 for full RDMA support):
fi_info -p efa -f efa-direct -t FI_EP_RDM \
  -c "FI_MSG|FI_RECV|FI_SEND|FI_READ|FI_REMOTE_READ|FI_WRITE|FI_REMOTE_WRITE"
```

| Instance Type | RDMA Capabilities |
|---------------|-------------------|
| `c7gn.16xlarge` | Read only ✗ |
| `c8g.24xlarge` | Read + Write ✓ |
| `c8g.48xlarge` | Read + Write ✓ |
| `i8g.48xlarge` | Read + Write ✓ |
| `i8ge.48xlarge` | Read + Write ✓ |

```bash
# Choose your instance type:
INSTANCE_TYPE="i8g.48xlarge"    # or i8ge.48xlarge, c8g.24xlarge
```

**Server:**

```bash
aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name valkey-large-object-test-key \
  --placement "GroupName=valkey-large-object-test" \
  --network-interfaces "DeviceIndex=0,SubnetId=$SUBNET_ID,Groups=[$SG_ID],InterfaceType=efa" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=valkey-large-object-server}]" \
  --region us-east-1
```

**Client:**

```bash
aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name valkey-large-object-test-key \
  --placement "GroupName=valkey-large-object-test" \
  --network-interfaces "DeviceIndex=0,SubnetId=$SUBNET_ID,Groups=[$SG_ID],InterfaceType=efa" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=valkey-large-object-client}]" \
  --region us-east-1
```

> **Note:** Both i8g and i8ge instances come with local NVMe instance storage.
> The NVMe device names and count may differ between types — use `lsblk` after
> launch to identify the instance-store drives. The root volume is 8 GB gp3 —
> keep heavy build artifacts on NVMe.

### 1.7 Record Instance IPs

```bash
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=valkey-large-object-server,valkey-large-object-client" \
            "Name=instance-state-name,Values=running" \
  --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],PrivateIpAddress,PublicIpAddress]' \
  --output table \
  --region us-east-1
```

Note the **server private IP** — the client uses it to connect on port 6379.

---

## 2. Server Setup

SSH into the **server** instance (or use SSM Session Manager).

```bash
ssh -i valkey-large-object-test-key.pem ec2-user@<SERVER_PUBLIC_IP>
```

### 2.1 Mount an NVMe Drive

The root volume is only 8 GB. Mount an NVMe instance-store drive first and use
it for all heavy artifacts (Rust toolchain, cargo registry, repo clone, build
output, and module data). Device names vary by instance type — use `lsblk`.

```bash
lsblk
# Pick the first unmounted instance-store drive (not nvme0n1, that's root)
NVME_DEV="/dev/nvme1n1"   # verify with lsblk
sudo mkfs.xfs "$NVME_DEV"
sudo mkdir -p /mnt/nvme0
sudo mount "$NVME_DEV" /mnt/nvme0
sudo chown -R ec2-user:ec2-user /mnt/nvme0
```

### 2.2 Install System Dependencies

```bash
# Valkey server
sudo yum install -y valkey

# Build tools for the module
sudo yum install -y gcc git

# EFA installer (provides libfabric with efa-direct support)
# The module's EfaDirect fabric provider requires libfabric-aws >= 2.6.0, which
# ships in EFA installer >= 1.50.0. Download the latest installer:
cd /mnt/nvme0
curl -O https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
tar xzf aws-efa-installer-latest.tar.gz
cd aws-efa-installer
sudo bash efa_installer.sh -y
cd ~

# If the installer fails (e.g. missing efa-config RPM for your OS version),
# install the required RPMs manually from the installer's RPMS directory:
#   cd /mnt/nvme0/aws-efa-installer/RPMS/ALINUX2023/aarch64   # adjust for your OS
#   sudo rpm -Uvh --nodeps \
#     rdma-core/rdma-core-64.0amzn0-*.rpm \
#     rdma-core/libibverbs-64.0amzn0-*.rpm \
#     rdma-core/librdmacm-64.0amzn0-*.rpm \
#     rdma-core/libibumad-64.0amzn0-*.rpm \
#     rdma-core/libibverbs-utils-64.0amzn0-*.rpm \
#     rdma-core/librdmacm-utils-64.0amzn0-*.rpm \
#     rdma-core/rdma-core-devel-64.0amzn0-*.rpm \
#     efa-driver/efa-3.3.0-*.rpm \
#     libfabric-aws-2.6.0amzn1.0-*.rpm \
#     libfabric-aws-devel-2.6.0amzn1.0-*.rpm

# Verify efa-direct is available:
export PATH=/opt/amazon/efa/bin:$PATH
fi_info -f efa-direct   # should show provider: efa, fabric: efa-direct

# Rust toolchain — install to NVMe to avoid filling the root volume
export RUSTUP_HOME=/mnt/nvme0/.rustup
export CARGO_HOME=/mnt/nvme0/.cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source /mnt/nvme0/.cargo/env
```

> **Tip:** Add these to `~/.bashrc` so they persist across SSH sessions:
> ```bash
> echo 'export RUSTUP_HOME=/mnt/nvme0/.rustup' >> ~/.bashrc
> echo 'export CARGO_HOME=/mnt/nvme0/.cargo' >> ~/.bashrc
> echo 'source /mnt/nvme0/.cargo/env' >> ~/.bashrc
> ```

### 2.3 Clone and Build the Module

```bash
cd /mnt/nvme0
git clone https://github.com/valkey-io/valkey-large-object.git
cd valkey-large-object

# Point pkg-config at the EFA-provided libfabric
export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig

export CARGO_TARGET_DIR=/mnt/nvme0/cargo-target
cargo build --release
```

The module shared library lands at:
```
/mnt/nvme0/cargo-target/release/libvalkey_large_object.so
```

### 2.4 Start Valkey with the Module

Create a data directory and start the server:

```bash
mkdir -p /mnt/nvme0/lo_data

LD_LIBRARY_PATH=/opt/amazon/efa/lib64 valkey-server \
  --loadmodule /mnt/nvme0/cargo-target/release/libvalkey_large_object.so \
    operating-mode Tiered \
    disk-dir /mnt/nvme0/lo_data \
    segment-size 1073741824 \
    disk-staging-size 1073741824 \
    disk-maxmemory 10737418240 \
    max-object-size 536870912 \
    max-promote-size 536870912 \
    chunk-size 8388608 \
    fabric-provider EfaDirect \
  --protected-mode no \
  --daemonize yes \
  --logfile /tmp/valkey.log \
  --save "" \
  --dir /mnt/nvme0
```

Configuration breakdown:

| Parameter | Value | Notes |
|-----------|-------|-------|
| `operating-mode` | `Tiered` | DRAM cache + NVMe persistence |
| `disk-dir` | `/mnt/nvme0/lo_data` | NVMe-backed directory for object files |
| `segment-size` | 1 GB | DRAM pool segment size |
| `disk-staging-size` | 1 GB | NVMe staging buffer (must be ≥ segment-size) |
| `disk-maxmemory` | 10 GB | Max NVMe disk usage |
| `max-object-size` | 512 MB | Largest allowed object |
| `max-promote-size` | 512 MB | Largest object promoted NVMe → DRAM |
| `chunk-size` | 8 MB | Streaming I/O chunk size |
| `fabric-provider` | `EfaDirect` | EFA hardware RDMA for DMA transfers |
| `--save ""` | — | Disables RDB persistence |
| `--dir /mnt/nvme0` | — | Server working directory on NVMe |

Verify it started:

```bash
tail -10 /tmp/valkey.log
valkey-cli PING          # → PONG
valkey-cli MODULE LIST   # → largeobj
```

### 2.5 Stopping the Server

```bash
valkey-cli SHUTDOWN NOSAVE
# or
pkill -f valkey-server
```

### 2.6 Restarting Clean

```bash
pkill -f valkey-server
sleep 1
rm -f /mnt/nvme0/lo_data/*.dat
# Re-run the valkey-server command from 2.4
```

---

## 3. Client Setup

SSH into the **client** instance.

```bash
ssh -i valkey-large-object-test-key.pem ec2-user@<CLIENT_PUBLIC_IP>
```

### 3.1 Mount NVMe and Install Dependencies

Mount NVMe first (same as server section 2.1) to keep the root volume clear:

```bash
lsblk
NVME_DEV="/dev/nvme1n1"   # verify with lsblk
sudo mkfs.xfs "$NVME_DEV"
sudo mkdir -p /mnt/nvme0
sudo mount "$NVME_DEV" /mnt/nvme0
sudo chown -R ec2-user:ec2-user /mnt/nvme0
```

```bash
# Valkey CLI (for manual interaction)
sudo yum install -y valkey python3-pip

# Python client library (for the test script)
pip3 install valkey

# For EFA RDMA testing (section 5): upgrade the EFA stack
# Follow the same EFA upgrade steps as the server (section 2.2), then
# install Rust to NVMe:
export RUSTUP_HOME=/mnt/nvme0/.rustup
export CARGO_HOME=/mnt/nvme0/.cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source /mnt/nvme0/.cargo/env
```

### 3.2 Verify Connectivity

```bash
valkey-cli -h <SERVER_PRIVATE_IP> PING
# → PONG
```

### 3.3 Deploy the Test Scripts

Copy the test scripts to NVMe. From your local machine:

```bash
scp -i valkey-large-object-test-key.pem functional_test.py efa_rdma_test.py \
  ec2-user@<CLIENT_PUBLIC_IP>:/mnt/nvme0/
```

---

## 4. Running the Functional Test

### Against Standalone Valkey (Tiered mode)

```bash
python3 /mnt/nvme0/functional_test.py --host <SERVER_PRIVATE_IP>
```

Expected output (66 checks, 0 failures):

```
Connecting to <SERVER_PRIVATE_IP>:6379...
Storage mode: Tiered (NVMe -> DRAM)

────────────────────────────────────────────────────────────
  1. Basic SET/GET roundtrip
────────────────────────────────────────────────────────────
  ✓ SET 1 byte
  ✓ GET 1 byte roundtrip
  ...
  ✓ SET 10 MB
  ✓ GET 10 MB roundtrip

  ...23 test sections...

════════════════════════════════════════════════════════════
  Results: 66 passed, 0 failed, 66 total
════════════════════════════════════════════════════════════
```

### Against ElastiCache Managed Cluster (Devo)

For cluster-mode enabled caches (e.g., Devo `cache.i8ge.48xlarge`), use the
`--cluster-mode` flag. This prefixes all keys with a hash tag `{t}` so multi-key
operations (COPY, concurrent tests) hash to the same slot.

```bash
python3 /mnt/nvme0/functional_test.py \
  --host <CLUSTER_CONFIG_ENDPOINT> \
  --port 6379 \
  --tls \
  --cluster-mode
```

Expected output (61 checks, 0 failures, 2 skipped):

```
Connecting to <CLUSTER_ENDPOINT>:6379 (TLS, cluster-mode)...
Storage mode: Dram-only (tier=b'dram')

...

════════════════════════════════════════════════════════════
  Results: 61 passed, 0 failed
════════════════════════════════════════════════════════════
```

The 2 skipped tests (13 and 22) are tiered-mode specific and are automatically
skipped when the script detects Dram-only mode.

### Command-Line Options

| Option | Description |
|--------|-------------|
| `--host HOST` | Server hostname or IP (default: `127.0.0.1`) |
| `--port PORT` | Server port (default: `6379`) |
| `--tls` | Enable TLS (required for Devo) |
| `--cluster-mode` | Use hash-tagged keys for cluster-mode compatibility |

### What the Test Script Covers

| # | Section | What it tests |
|---|---------|---------------|
| 1 | Basic SET/GET | Roundtrip at 1B, 4KB, 4KB+1, 8KB, 64KB, 1MB, 10MB |
| 2 | GET nonexistent | Returns nil |
| 3 | Overwrite | Second SET replaces the value |
| 4 | DEL | Removes key, GET returns nil |
| 5 | BLOB.INFO | Full array + individual LEN, CRC, TIER fields |
| 6 | BLOB.INFO errors | Nonexistent key, wrong type, bad field |
| 7 | WRONGTYPE | Module commands reject native string keys |
| 8 | Zero-length | Empty payload rejected |
| 9 | max-object-size | CONFIG SET enforcement, existing reads unaffected |
| 10 | COPY | Independent duplicate, CRC match, delete independence |
| 11 | TTL expiry | PEXPIRE removes the key |
| 12 | FLUSHALL | All module keys removed |
| 13 | Tiered promotion | SET → nvme tier, GET(s) → dram tier |
| 14 | Multiple objects | 20 concurrent keys round-trip |
| 15 | INFO section | `INFO largeobj` returns metrics |
| 16 | ACL categories | All 6 commands registered, -@largeobj blocks access |
| 17 | MEMORY USAGE | Reports non-zero after promotion |
| 18 | Config constraints | min-buffers-per-op > max rejected |
| 19 | Arity validation | Wrong arg count → error |
| 20 | Keyspace events | largeobj.create and largeobj.update fire |
| 21 | CRC on overwrite | CRC changes when value changes |
| 22 | disk-maxmemory | NVMe capacity gate enforced |
| 23 | Concurrent | 10 threads SET/GET without corruption |
| 24 | Steady-state | 4 clients × 50 ops mixed workload, data integrity |

---

## 5. EFA RDMA Transfer Validation

The functional test script (`functional_test.py`) exercises the TCP command
path. To validate that data actually moves over EFA RDMA, run `efa_rdma_test.py`
on the **client** instance. This uses `fabric_target` — a Rust binary in the repo
that acts as a passive libfabric peer with EFA-registered memory.

### 5.1 Build `fabric_target` on the Client

The client needs Rust, the upgraded EFA stack (same as section 2.2), and the
module repo. These should already be set up from section 3.1. Build the binary:

```bash
cd /mnt/nvme0
git clone https://github.com/valkey-io/valkey-large-object.git
cd valkey-large-object

export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
export CARGO_TARGET_DIR=/mnt/nvme0/cargo-target
cargo build --release --features test-harness --bin fabric_target
```

The binary lands at `/mnt/nvme0/cargo-target/release/fabric_target`.

### 5.2 Get the Server's Fabric Address

The server logs its EFA fabric address at startup. Retrieve it from the server
(run this command on the **server** instance, or via SSM):

```bash
grep "fabric service" /tmp/valkey.log
# Output: largeobj: fabric service 0 address <64-char hex>
```

If you only have access to the client, use `valkey-cli` to trigger the
address from the server log via SSM. It is printed once at startup and is stable for the lifetime
of the server process.

Copy the 64-character hex string — the RDMA test script needs it.

### 5.3 Run the EFA RDMA Test

```bash
# On the client instance
export LD_LIBRARY_PATH=/opt/amazon/efa/lib64:$LD_LIBRARY_PATH

python3 /mnt/nvme0/efa_rdma_test.py \
  --server-ip <SERVER_PRIVATE_IP> \
  --server-fabric-addr <SERVER_FABRIC_ADDRESS_HEX>
```

The script:
1. Starts `fabric_target --efa` as a passive peer with EFA-registered memory
2. Calls `BLOB.RDMA_HELLO` to establish an RDMA session
3. Calls `BLOB.RDMA_GET` — the server writes object data into the client's
   registered buffer via EFA RDMA; `fabric_target` verifies the bytes byte-by-byte
4. Calls `BLOB.RDMA_SET` — the server reads from the client's prefilled buffer
   via EFA RDMA; verified by reading the object back over TCP
5. Full roundtrip: SET via RDMA then GET via RDMA
6. Steady-state: 10 rounds of RDMA SET + GET with fresh sessions each round,
   verifying data integrity under sustained load

Expected output (14 checks, 0 failures):

```
EFA RDMA Transfer Test
  Server: <ip>:6379
  Fabric: fe80000000000000...

── RDMA GET test ──
  ✓ BLOB.RDMA_HELLO returns server addresses
  ✓ BLOB.RDMA_GET returns [len, crc]
  ✓   len=4096
  ✓ fabric_target verified payload
  ✓ Client-side CRC matches server CRC

── RDMA SET test ──
  ✓ BLOB.RDMA_HELLO (SET session)
  ✓ BLOB.RDMA_SET returns OK
  ✓ TCP readback matches pattern
  ✓ Server stored CRC matches client source CRC

── RDMA roundtrip test ──
  ✓ RDMA SET for roundtrip
  ✓ RDMA GET for roundtrip returns [4096, crc]
  ✓ Roundtrip: fabric_target verified payload
  ✓ Roundtrip: client CRC matches server CRC

── RDMA steady-state test ──
  ✓ 10 RDMA SET/GET rounds completed

══════════════════════════════════════════════════
  EFA RDMA: 14 passed, 0 failed
══════════════════════════════════════════════════
```

---

## 6. Manual Exploration with valkey-cli

From the client (or the server itself):

```bash
valkey-cli -h <SERVER_PRIVATE_IP>
```

```
# Store and retrieve
127.0.0.1:6379> BLOB.TCP_SET mykey "hello world"
OK
127.0.0.1:6379> BLOB.TCP_GET mykey
"hello world"

# Inspect metadata
127.0.0.1:6379> BLOB.INFO mykey
1) "len"
2) (integer) 11
3) "crc"
4) (integer) 2178490556
5) "tier"
6) nvme

# Trigger promotion (default promote-min-hits=2, so 2 GETs)
127.0.0.1:6379> BLOB.TCP_GET mykey
"hello world"
127.0.0.1:6379> BLOB.INFO mykey TIER
dram

# Module metrics
127.0.0.1:6379> INFO largeobj

# Runtime config tuning
127.0.0.1:6379> CONFIG SET largeobj.max-object-size 1048576
OK
127.0.0.1:6379> CONFIG SET largeobj.disk-maxmemory 5368709120
OK

# Cleanup
127.0.0.1:6379> DEL mykey
(integer) 1
```

---

## 7. Dram-Only Mode

To test without NVMe (simpler, no disk-dir needed):

```bash
LD_LIBRARY_PATH=/opt/amazon/efa/lib64 valkey-server \
  --loadmodule /mnt/nvme0/cargo-target/release/libvalkey_large_object.so \
    operating-mode Dram \
    segment-size 1073741824 \
    max-object-size 536870912 \
    chunk-size 8388608 \
    fabric-provider EfaDirect \
  --protected-mode no \
  --daemonize yes \
  --logfile /tmp/valkey.log \
  --save "" \
  --dir /mnt/nvme0
```

In Dram mode, `BLOB.INFO TIER` always returns `dram`, and there are no `.dat`
files on disk.

---

## 8. Troubleshooting

| Problem | Fix |
|---------|-----|
| `ERR segment-size is insufficient for max-promote-size` | `max-promote-size` must be ≤ `segment-size` minus talc overhead |
| `ERR disk-staging-size must be >= segment-size` | Set `disk-staging-size` ≥ `segment-size` |
| `ERR disk-maxmemory must be >= max-object-size` | In Tiered mode, `disk-maxmemory` must be ≥ `max-object-size` |
| `MISCONF ... unable to persist to disk` | The server can't write RDB. Start with `--save ""` to disable |
| `Failed to write PID file` | Non-critical warning — server still runs. Set `--dir` to a writable path |
| `fi_getinfo: No data available` for read+write caps | Instance type lacks RDMA write capability. Use c8g, i8g, or i8ge instance types |
| Port 6379 in use | `pkill -9 -f valkey-server` then restart |
| RDMA_HELLO works but RDMA_SET/GET hangs | Missing outbound self-referencing SG rule. Add egress `-1` to self (see section 1.2) |
| `No route to host (err=113)` in server logs | Same as above — EFA return path blocked |
| `CROSSSLOT` error on COPY/multi-key ops | Running against cluster-mode without `--cluster-mode` flag |

