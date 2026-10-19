# Data Plane Testing — Pentest Guide

This document supplements the [PenTest API spec doc](https://pippin.amazon.dev/docs/GN52p58qo3b9sN/pentest-api-spec-doc), which covers **control-plane** validation (CreateReplicationGroup, ModifyReplicationGroup, ACLs). This section covers **data-plane** validation: exercising the large-object commands over TCP and EFA RDMA against a live cluster.

---

## Overview

The large-object module exposes six commands:

| Command | Transport | Description |
|---------|-----------|-------------|
| `BLOB.TCP_SET key <data>` | TCP/TLS | Store object |
| `BLOB.TCP_GET key` | TCP/TLS | Retrieve object |
| `BLOB.RDMA_SET key total_len rkey addr len [...]` | EFA RDMA | Store via RDMA DMA |
| `BLOB.RDMA_GET key rkey addr len [...]` | EFA RDMA | Retrieve via RDMA DMA |
| `BLOB.RDMA_HELLO client_fabric_addr` | TCP/TLS | Establish RDMA session (control) |
| `BLOB.INFO key [LEN\|CRC\|TIER]` | TCP/TLS | Object metadata |

Standard Valkey commands (`DEL`, `COPY`, `PEXPIRE`, `FLUSHALL`, `MEMORY USAGE`) work on module keys. ACL categories `@largeobj`, `@read`, `@write` control access.

---

## Test Environments

Data-plane tests can run against two environments:

| Environment | Characteristics | Use for |
|-------------|-----------------|---------|
| **Self-managed EC2** | Full control over SG, ports, binary, config. Standalone Valkey. Tiered or Dram mode. | Detailed functional validation, RDMA debugging |
| **DubDevo managed cluster** | ElastiCache-managed. Cluster mode enabled. TLS optional. Dram-only mode. Limited port access (6379 only). | Production-like validation, cluster-mode testing |

The test scripts auto-detect the environment and adjust expectations accordingly.

---

## Test Scripts

Two test scripts live in the [valkey-large-object](https://github.com/neerajr0/valkey-large-object/tree/functional-testing) repository:

### `functional_test.py` — Full Functional Test (61–66 checks)

Exercises all TCP commands, standard Valkey integration, ACLs, concurrency, and config enforcement.

```bash
# Against self-managed standalone Valkey (Tiered mode):
python3 functional_test.py --host <SERVER_PRIVATE_IP>
# → 66 passed

# Against DubDevo managed cluster (Dram mode, cluster-mode enabled):
python3 functional_test.py \
  --host <CLUSTER_ENDPOINT> \
  --port 6379 \
  --tls \
  --cluster-mode
# → 61 passed (2 tiered-mode tests skipped, 3 tests N/A in cluster mode)
```

### `efa_rdma_test.py` — EFA RDMA Validation (14 checks)

Validates that data moves over EFA RDMA hardware using `fabric_target`, a passive libfabric peer with EFA-registered memory.

```bash
# Set FABRIC_TARGET if your binary is not at the default path
export FABRIC_TARGET=~/valkey-large-object/target/release/fabric_target

python3 efa_rdma_test.py \
  --server-ip <SERVER_PRIVATE_IP> \
  --server-fabric-addr <64_HEX_FROM_RDMA_HELLO>
# → 14 passed
```

---

## Quick Start: Testing Against DubDevo

If you already have a DubDevo large-object cluster (`cache.i8ge.48xlarge`) created per the control-plane pentest doc, run the data-plane tests from an EFA-capable client instance in the same VPC.

### 1. Launch a Client Instance

Use an instance type with full EFA RDMA capabilities. **Not all EFA-enabled instance types support RDMA read+write** — some only support read (e.g., `c7gn.16xlarge`). Verified working:

| Instance Type | RDMA Capabilities |
|---------------|-------------------|
| `c8g.24xlarge` | Read + Write ✓ |
| `c8g.48xlarge` | Read + Write ✓ |
| `i8g.48xlarge` | Read + Write ✓ |
| `i8ge.48xlarge` | Read + Write ✓ |

Verify capabilities on a running instance:

```bash
export PATH=/opt/amazon/efa/bin:$PATH
# Must return exit 0 for both:
fi_info -p efa -f efa-direct -t FI_EP_RDM \
  -c "FI_MSG|FI_RECV|FI_SEND|FI_READ|FI_REMOTE_READ"           # read-only
fi_info -p efa -f efa-direct -t FI_EP_RDM \
  -c "FI_MSG|FI_RECV|FI_SEND|FI_READ|FI_REMOTE_READ|FI_WRITE|FI_REMOTE_WRITE"  # read+write
```

### 2. Security Group Configuration

**Critical:** EFA requires a self-referencing security group with **both inbound AND outbound** rules. The default `0.0.0.0/0` outbound is **not sufficient** for EFA.

```bash
SG_ID=<your-security-group-id>

# Inbound: all traffic from self (standard EFA requirement)
aws ec2 authorize-security-group-ingress --region $AWS_REGION \
  --group-id $SG_ID \
  --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$SG_ID}]"

# Outbound: all traffic TO self (also required for EFA return path)
aws ec2 authorize-security-group-egress --region $AWS_REGION \
  --group-id $SG_ID \
  --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$SG_ID}]"
```

**Symptom if missing:** `BLOB.RDMA_HELLO` succeeds (it's plain TCP), but `BLOB.RDMA_SET`/`RDMA_GET` hang indefinitely. Server logs show `No route to host (err=113)`.

### 3. Install Dependencies on Client

```bash
# Python client
sudo yum install -y python3-pip
pip3 install valkey

# EFA stack (libfabric >= 2.6.0 required)
curl -O https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz
tar xzf aws-efa-installer-latest.tar.gz && cd aws-efa-installer
sudo bash efa_installer.sh -y

# Verify
export PATH=/opt/amazon/efa/bin:$PATH
fi_info -f efa-direct  # should show provider: efa
```

### 4. Get the Scripts

```bash
curl -O https://raw.githubusercontent.com/neerajr0/valkey-large-object/functional-testing/functional_test.py
curl -O https://raw.githubusercontent.com/neerajr0/valkey-large-object/functional-testing/efa_rdma_test.py
```

### 5. Run TCP Functional Test

```bash
# If cluster has TLS enabled:
python3 functional_test.py \
  --host <CLUSTER_CONFIG_ENDPOINT> \
  --port 6379 \
  --tls \
  --cluster-mode

# If cluster does NOT have TLS:
python3 functional_test.py \
  --host <CLUSTER_CONFIG_ENDPOINT> \
  --port 6379 \
  --cluster-mode
```

**Expected:** 61 passed, 0 failed (tiered-mode tests skipped for Dram-only DubDevo).

### 6. Run EFA RDMA Test

First, get the server's fabric address via `BLOB.RDMA_HELLO`. The HELLO command takes your client's fabric address and returns the server's.

```bash
# Build fabric_target (one-time)
sudo yum install -y gcc git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
git clone https://github.com/neerajr0/valkey-large-object.git
cd valkey-large-object
export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
cargo build --release --features test-harness --bin fabric_target

# Get your client fabric address
export LD_LIBRARY_PATH=/opt/amazon/efa/lib64:$LD_LIBRARY_PATH
./target/release/fabric_target --efa &
# Output: fabric_target: advertising <64-hex-client-addr>
kill %1

# Send HELLO to get server address
valkey-cli --tls -h <CLUSTER_NODE_ENDPOINT> -p 6379 \
  BLOB.RDMA_HELLO <64-hex-client-addr>
# Returns: <64-hex-server-addr>

# Run the RDMA test
export FABRIC_TARGET=~/valkey-large-object/target/release/fabric_target
python3 efa_rdma_test.py \
  --server-ip <CLUSTER_NODE_IP> \
  --server-fabric-addr <64-hex-server-addr>
```

**Expected:** 14 passed, 0 failed.

---

## Detailed Testing Guide

For step-by-step infrastructure provisioning, server setup, and detailed test coverage, see:

**[DATA_PLANE_TESTING_GUIDE.md](./DATA_PLANE_TESTING_GUIDE.md)**

This guide covers:
- EC2 instance provisioning (placement groups, EFA interfaces)
- Security group configuration with EFA requirements
- Server installation and module build
- Valkey configuration (Tiered vs Dram mode)
- Full test script documentation (what each of the 66 checks validates)
- EFA RDMA test setup with `fabric_target`
- Troubleshooting common issues

---

## What Pentesters Should Validate

### TCP Data Plane

| Test Area | Script Coverage | What to Look For |
|-----------|-----------------|------------------|
| Command arity | Tests 19, 6 | Wrong arg count rejected |
| Type isolation | Test 7 | Module commands reject native string keys |
| Zero-length objects | Test 8 | Empty payload rejected |
| Size limits | Tests 9, 22 | `max-object-size`, `disk-maxmemory` enforced |
| ACL enforcement | Test 16 | `@largeobj` category blocks when denied |
| Data integrity | Tests 1, 21, 23, 24 | CRC validation, concurrent access |

### EFA RDMA Data Plane

| Test Area | Script Coverage | What to Look For |
|-----------|-----------------|------------------|
| Session establishment | RDMA test 1 | HELLO returns valid server address |
| RDMA GET integrity | RDMA tests 2-5 | Data matches, CRC matches |
| RDMA SET integrity | RDMA tests 6-9 | Data persists correctly |
| Roundtrip | RDMA tests 10-13 | SET then GET returns same data |
| Sustained load | RDMA test 14 | 10 rounds without corruption |

### Security Group Attacks (EFA-specific)

| Attack | How to Test | Expected Result |
|--------|-------------|-----------------|
| Remove inbound self-ref | Delete the `-1/self` inbound rule | EFA should fail to establish; TCP still works |
| Remove outbound self-ref | Delete the `-1/self` outbound rule | RDMA hangs; HELLO works but GET/SET stall |
| Cross-AZ EFA | Client in different AZ than cluster | EFA unavailable (placement constraint) |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `CROSSSLOT` error on COPY | Running cluster-mode without `--cluster-mode` flag | Add `--cluster-mode` to test command |
| RDMA_HELLO works, RDMA_SET hangs | Missing outbound self-referencing SG rule | Add egress `-1` to self |
| `fi_getinfo: No data available` | Instance type lacks RDMA write capability | Use c8g or i8g instance type |
| Tests 13, 22 skipped on DubDevo | DubDevo runs Dram-only mode | Expected behavior — `--cluster-mode` auto-detects and skips |
