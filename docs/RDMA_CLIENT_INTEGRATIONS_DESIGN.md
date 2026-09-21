# Valkey large object RDMA client integrations design doc

## Table of Contents
* [valkey-glide python sdk](#valkey-glide-python-sdk)
  * [User experience](#user-experience)
    * [Installation](#installation)
    * [Usage](#usage)
  * [Implementation](#implementation)
    * [glide-rdma](#glide-rdma)
    * [glide-core](#glide-core)
    * [glide-ffi](#glide-ffi)
    * [Python SDK: glide-shared and glide-sync](#python-sdk-glide-shared-and-glide-sync) 
* [LMCache](#lmcache)
  * [User experience](#user-experience-1)
  * [Implementation](#implementation-1)
* [vLLM](#vllm)
  * [User experience](#user-experience-2)
    * [Most straightforward option: LMCache](#most-straightforward-option-lmcache)
    * [Direct integration: SecondaryTierManager](#direct-integration-secondarytiermanager)
  * [Implementation](#implementation-2)

---

# valkey-glide python sdk

## User experience

### Installation

Note: The valkey-glide maintainers probably have a more defined path for packaging and distribution of new features, but here we outline one possible path forward. Depending on what they ask for, the work may or may not fit into our timeline.

RDMA capability can be considered a “preview” feature until it’s more mature. People should be able to install valkey-glide normally without picking up extra dependencies (namely [libfabric](https://github.com/ofiwg/libfabric)) and opt in to using the RDMA capabilities as they wish. Note that the `efa-direct` option for RDMA can be run only on hosts with the special EFA hardware.

Pip supports installing from a [source distribution](https://packaging.python.org/en/latest/tutorials/installing-packages/#source-distributions-vs-wheels) (sdist) instead of a pre-built binary (wheel), so the RDMA-capable client can be made available only via sdist while in preview. These instructions assume valkey-glide will be installed on a host with an EFA and libfabric.

```bash
# Typical install for a prebuilt wheel, no libfabric
pip install valkey-glide-sync

# RDMA preview install builds from source with the `rdma` feature on

# Install build toolchain
sudo apt-get install -y build-essential pkg-config protobuf-compiler
curl https://sh.rustup.rs -sSf | sh -s -- -y && source "$HOME/.cargo/env"

export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
export LD_LIBRARY_PATH=/opt/amazon/efa/lib64:$LD_LIBRARY_PATH

# Complete the valkey-glide + RDMA build
GLIDE_SYNC_RDMA=1 RELEASE_MODE=1 \
	pip install --no-binary valkey-glide-sync valkey-glide-sync
```

### Usage

The example is for a non-cluster client, but cluster mode will be supported as well.

```bash
config = GlideClientConfiguration(
    addresses=[NodeAddress("...")],
    rdma=RdmaConfiguration(provider=EfaDirect()),
)
client = GlideClient.create(config)

# provide a window of the region with each get/set call
region = client.register_rdma_region(slab)

receipt = client.rdma_get(b"key", region.window(offset, capacity))
client.rdma_set(b"key", region.window(offset, length))

region.close()
```

## Implementation

### glide-rdma

New leaf crate to glide-core that contains the components needed for establishing the client host as a RDMA target, registering a block of memory the server will RDMA against, minting a key for the server to access that memory, and building the RESP commands glide-core sends to a valkey server node with the [ValkeyLargeObj module](https://github.com/KarthikSubbarao/ValkeyLargeObj) installed: `LO.HELLO`, `LO.GET`, and `LO.SET`. This crate depends on libfabric to open the fabric endpoint and register memory, but the server performs all of the actual data transfer.

- diagram of main glide-rdma components
    
    ```mermaid
    flowchart TB
      subgraph GCORE["glide-core — owns the connections, sends the commands"]
        direction TB
        CONN["GlideConnectionWithRdma<br/>one RESP connection, and the session opened on it"]
        PROTO["rdma::protocol<br/>glide-rdma's commands as redis-rs speaks them"]
      end

      subgraph WIRE["glide-rdma — wire format, builds without libfabric"]
        direction TB
        FC["FabricConfig + Provider<br/>which card to open and how"]
        CMD["RdmaCommand<br/>LO.HELLO / LO.GET / LO.SET, as a name and arguments"]
        ADV["RegionRef<br/>the remote key and address permitting one transfer"]
        HS["Handshake<br/>the fabric addresses the server answered with"]
        RCPT["ReadReceipt<br/>bytes moved, and the server's checksum when it sends one"]
        CK["checksum<br/>CRC-32c, the same one the server computes"]
      end

      subgraph DP["glide-rdma — data plane, requires libfabric"]
        direction TB
        FAB["RdmaFabric<br/>this host's endpoint and its address vector"]
        SESS["RdmaSession<br/>the server addresses one connection may be reached from"]
        BUF["RdmaBuffer<br/>pinned pages the server may read or write"]
        WIN["RegionWindow<br/>one slice of those pages, for one transfer"]
        EP["LibfabricEndpoint<br/>raw libfabric bring-up and teardown"]
        PROG["ProgressDriver<br/>polls for completions; every provider except efa-direct"]
      end

      CONN -->|"sends its commands through"| PROTO
      PROTO -->|builds| CMD
      PROTO -->|"parses the LO.HELLO reply into"| HS
      PROTO -->|"parses a transfer reply into"| RCPT
      RCPT -.->|"a read is verified against"| CK

      FC -->|"opens, once per client"| FAB
      FAB -.->|"handed to every connection"| CONN
      FAB -->|owns| EP
      FAB -->|drives| PROG
      FAB -->|"registers memory into"| BUF
      BUF -->|"slice(at, len)"| WIN
      WIN -->|"region_ref()"| ADV
      ADV -->|"two of a transfer's arguments"| CMD
      HS -->|"open_session mints"| SESS
      SESS -.->|"opened by the first transfer, held for the connection's life"| CONN
    ```
    

### glide-core

Build `glide-core` with large object RDMA capability using `--features rdma`. Alternately, `--features rdma-vendored` builds libfabric from source and allows `--all-features` to continue working in CI/CD.

The [server module lists `LO.HELLO` as a write command](https://github.com/KarthikSubbarao/ValkeyLargeObj/blob/main/src/lib.rs#L453), so RDMA transfers will be between only the client and primary nodes (in non-cluster mode, there’s only one; in cluster mode, there are multiple for the different shards), not replica nodes. Allows only one in-flight transfer per connection at a time with the RESP channel used as the control plane to coordinate with the valkey node. Should not prevent RESP command pipelining.

RDMA handshakes occur on the first transfer (`LO.GET` or `LO.SET` command). There should be only one `LO.HELLO` per RESP connection. The server module [tracks RDMA sessions by each connection's `client_id`](https://github.com/KarthikSubbarao/ValkeyLargeObj/blob/main/src/transport/session.rs#L88), so the client should also pair an RDMA session with its own RESP connection. This way, RDMA sessions are kept in sync whenever a RESP connection must be replaced or a new one must be created or removed due to cluster topology changes.

RDMA is not compatible with the other optional configurations for compression, `lazy_connect`, or `read_only`. 

### glide-ffi

Pass the new configuration and APIs through from glide-core to the python-sync SDK. Add a `rdma` feature to the crate as well.

### Python SDK: glide-shared and glide-sync

Update existing configuration objects to accept an optional RDMA configuration. Expose new APIs for `register_rdma_region`, `rdma_get`, `rdma_set`, and `rdma_checksum`.

Packaging changes will also vendor `glide-rdma` into the sdist so the `GLIDE_SYNC_RDMA=1 pip install` command in the User experience > Installation section will work.

---

# LMCache

Uses valkey-glide’s python client, glide-sync.

## User experience

The RDMA-capable adapter is dormant until the RDMA-capable valkey-glide client is installed/built and the adapter is named by the `--l2-adapter` argument.

Installation would look something like:

```bash
# Install build toolchain
sudo apt-get install -y build-essential pkg-config protobuf-compiler
curl https://sh.rustup.rs -sSf | sh -s -- -y && source "$HOME/.cargo/env"

uv venv --python 3.12 && source .venv/bin/activate
uv pip install lmcache

export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
export LD_LIBRARY_PATH=/opt/amazon/efa/lib64:$LD_LIBRARY_PATH

# Complete the valkey-glide + RDMA build
GLIDE_SYNC_RDMA=1 RELEASE_MODE=1 \
  uv pip install --no-binary-package valkey-glide-sync valkey-glide-sync
```

And running LMCache and vLLM with the adapter would look like:

```bash
lmcache server --host 0.0.0.0 --port 5555 \
	--chunk-size 512 --l1-size-gb 100 \
  --l2-adapter '{"type":"valkey_rdma",
                 "startup_nodes":"kv.internal:6379",
                 "num_workers":8,
                 "ttl_seconds":3600,
                 "rdma_provider":"efa-direct"}'

vllm serve Qwen/Qwen3-14B --kv-transfer-config \
  '{"kv_connector":"LMCacheMPConnector","kv_role":"kv_both",
    "kv_connector_extra_config":{"lmcache.mp.host":"127.0.0.1","lmcache.mp.port":5555}}'
```

## Implementation

Write a RDMA-capable L2 adapter based on the [existing valkey one](https://github.com/LMCache/LMCache/blob/dev/lmcache/v1/distributed/l2_adapters/valkey_l2_adapter.py).

A separate adapter file that subclasses the existing adapter may be nice for working out the rough edges we may not yet know about, but we can fold the RDMA parameters into the existing adapter instead if that makes more sense.

---

# vLLM

## User experience

### Most straightforward option: LMCache

Integrating with LMCache using RDMA-capable valkey-glide simply means using the L2 adapter written as part of the LMCache integration.

```bash
lmcache server --host 0.0.0.0 --port 5555 \
	--chunk-size 512 --l1-size-gb 100 \
  --l2-adapter '{"type":"valkey_rdma",
                 "startup_nodes":"kv.internal:6379",
                 "num_workers":8,
                 "ttl_seconds":3600,
                 "rdma_provider":"efa-direct"}'

vllm serve Qwen/Qwen3-14B --kv-transfer-config \
  '{"kv_connector":"LMCacheMPConnector","kv_role":"kv_both",
    "kv_connector_extra_config":{"lmcache.mp.host":"127.0.0.1","lmcache.mp.port":5555}}'
```

### Direct integration: SecondaryTierManager

vLLM appears to be working on implementing their own version of tiered KV caching called [kv_offload](https://github.com/vllm-project/vllm/tree/df42d112ee88dd4a9b64efbad55621af6a66a44b/vllm/v1/kv_offload) (see this [github issue](https://github.com/vllm-project/vllm/issues/38260) and [github PR](https://github.com/vllm-project/vllm/pull/40020)). We can integrate with this framework by writing an implementation of [SecondaryTierManager](https://github.com/vllm-project/vllm/blob/main/vllm/v1/kv_offload/tiering/base.py#L121) to provide Valkey with RDMA capabilities.

Once integrated, installation would look like:

```bash
# Install build toolchain
sudo apt-get install -y build-essential pkg-config protobuf-compiler
curl https://sh.rustup.rs -sSf | sh -s -- -y && source "$HOME/.cargo/env"

export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
export LD_LIBRARY_PATH=/opt/amazon/efa/lib64:$LD_LIBRARY_PATH

# Complete the valkey-glide + RDMA build
GLIDE_SYNC_RDMA=1 RELEASE_MODE=1 \
  pip install --no-binary-package valkey-glide-sync valkey-glide-sync
  
pip install vllm
```

And running it would look something like:

```bash
vllm serve <model> --kv-transfer-config '{
  "kv_connector": "OffloadingConnector",
  "kv_role": "kv_both",
  "kv_connector_extra_config": {
    "spec_name": "TieringOffloadingSpec",
    "cpu_bytes_to_use": 10737418240,
    "blocks_per_chunk": 4,
    "eviction_policy": "lru",
    "secondary_tiers": [{
      "type": "valkey_rdma",
      "host": "10.0.1.5", 
      "port": 6379, 
      "rdma_provider": "efa-direct",
      "num_workers": 8
    }]
  }}'
```

## Implementation

For the direct integration, we would subclass [SecondaryTierManager](https://github.com/vllm-project/vllm/blob/main/vllm/v1/kv_offload/tiering/base.py#L121) to create `ValkeyRdmaTierManager` that uses a pool of glide-sync clients to send `LO.*` commands. We can use the [file system](https://github.com/vllm-project/vllm/blob/df42d112ee88dd4a9b64efbad55621af6a66a44b/vllm/v1/kv_offload/tiering/fs/manager.py) and [object store](https://github.com/vllm-project/vllm/blob/df42d112ee88dd4a9b64efbad55621af6a66a44b/vllm/v1/kv_offload/tiering/obj/manager.py) secondary tier manager implementations as reference. Then [register](https://github.com/vllm-project/vllm/blob/main/vllm/v1/kv_offload/tiering/factory.py) the tier to make it available as an option to vLLM.