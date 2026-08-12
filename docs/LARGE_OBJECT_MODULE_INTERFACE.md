# Large Object Module + Transport Crate Interface Design

## Architecture

- **One `.so`** — single Valkey module artifact with three internal layers:
  - **Data Type** — Valkey keyspace integration: LoValue struct, commands (LO.HELLO, LO.GET, LO.SET), RDB callbacks, TIERING.REF, BlockClient management
  - **Storage** — Buffer pool + NVMe I/O: pool_get/put, pin/unpin, io_uring read/write, registered buffers, disk eviction
  - **Transport crate** (libefa-rs) — EFA/libfabric lifecycle, multi-device LB, completion handling. Cargo dependency, reusable by any Rust project.

```
Data Type (commands, LoValue, keyspace)
    ↓ calls
Storage (buffer pool, io_uring, NVMe files)
    ↓ passes buffers to
Transport (EFA, fi_write/fi_read)
```

Transport never calls storage or data type. Storage never touches Valkey keys.

---

## Lifecycle

```
Module OnLoad:
  1. Transport::init()              → discover EFA devices, create fi_fabric + fi_domain per device
  2. Storage allocates buffer pool  → fixed-size buffers, 4KB-aligned, owned by storage
  3. Storage::register_buffers()    → IORING_REGISTER_BUFFERS (kernel pins pages for NVMe DMA)
  4. Transport::register_buffers()  → fi_mr_reg same buffers across all EFA domains (NIC learns phys addrs)
     (Order between 3 and 4 does not matter — both independently pin pages via get_user_pages.
      Dual-registration on the same physical pages has no conflicts. Only concern: each registration
      counts against ulimit -l locked memory accounting, so 2GB pool = ~4GB locked memory reported.
      EC2 instances typically have unlimited memlock.)

LO.HELLO (per client connection):
  5. Session::new(client_regions)   → create fi_endpoint on each EFA device, insert client AV entries

LO.GET / LO.SET (per operation):
  6. storage.pin(buf)               → prevent eviction during DMA
  7. session.write/read(buf, ...)   → non-blocking, completion via callback
  8. On completion callback (fires on transport CQ thread or io_uring poller thread):
       storage.unpin(buf)
       storage.pool_put(buf)
       UnblockClient(bc, private_data)
  9. Reply callback fires on main thread:
       ctx.reply_ok() or ctx.reply_error()

Client disconnect:
  10. Session::close()              → destroy endpoints, remove AV entries

Module unload:
  11. Transport::deregister_buffers() → fi_mr_dereg
  12. Transport::shutdown()           → close domains, free global EFA state
```

---

## Data Type Layer (commands + keyspace)

The data type layer owns:
- `LoValue` struct in Valkey's keyspace (accessed via `ValkeyModule_OpenKey`)
- Command handlers (LO.HELLO, LO.GET, LO.SET)
- Native Valkey `DEL` triggers module free callback → deletes NVMe file
- OID generation (monotonic counter)
- RDB callbacks (save/load references)
- TIERING.REF replication
- BlockClient lifecycle

Command handlers read `LoValue` from keyspace to get `object_id` and `len`, then call storage for I/O. Storage never touches Valkey keys.

```rust
/// LoValue — the Valkey data type value struct, stored in Valkey's keyspace.
/// Accessed via ValkeyModule_OpenKey → ModuleTypeGetValue on the main thread.
/// This is NOT in the storage layer. Command handlers read this to get file info before calling storage.
#[derive(Debug, Clone)]
pub struct LoValue {
    pub object_id: ObjectId,   // monotonic per-node OID (used as filename)
    pub len: u64,              // object size in bytes
    pub crc32c: u32,           // integrity checksum (verified on replication pull)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);
// ObjectId IS the file path: deterministic mapping OID → "{data_dir}/{oid:016x}.dat"
// No lookup table. Compact u64 safe for replication streams, RDB, and LoValue.
```

---

## Storage Layer API (buffer pool + NVMe I/O)

Storage operates on **OIDs and file paths**, never on Valkey keys. The command handler resolves key → OID via the data type layer, then calls storage.

Uses `PoolBuffer` from the transport crate as the shared buffer descriptor.

```rust
/// Error types for storage operations.
#[derive(Debug)]
pub enum StorageError {
    IoError { code: i32 },     // io_uring read/write failed
    PoolExhausted,             // no free buffers available
    ObjectTooLarge,            // object exceeds pool buffer size - Debatable (see LO.GET/SET handling)
}

trait Storage {
    // ─── Buffer Pool ─────────────────────────────────────────────────────

    /// Get a buffer from the pool. Returns None if pool exhausted.
    /// Returned buffer is a registered slot (valid for io_uring ReadFixed and EFA fi_write).
    fn pool_get(&self) -> Option<PoolBuffer>;

    /// Return a buffer to the pool.
    fn pool_put(&self, buf: PoolBuffer);

    /// Pin buffer — prevents eviction/reuse during in-flight DMA or io_uring op.
    fn pin(&self, buf: &PoolBuffer);

    /// Unpin buffer — allows eviction/reuse.
    fn unpin(&self, buf: &PoolBuffer);

    /// Pool buffer size (all buffers are this fixed size).
    fn pool_buf_size(&self) -> usize;

    // ─── Registration ────────────────────────────────────────────────────

    /// Register pool buffers with io_uring (IORING_REGISTER_BUFFERS).
    /// Called once at startup. Pins pages for NVMe DMA.
    fn register_buffers(&self) -> Result<(), StorageError>;

    /// Deregister pool buffers from io_uring. Called at module unload.
    fn deregister_buffers(&self) -> Result<(), StorageError>;

    // ─── NVMe I/O ───────────────────────────────────────────────────────

    /// Read object bytes from NVMe into buf. Async via io_uring ReadFixed.
    /// object_id maps directly to a file path: {data_dir}/{oid:016x}.dat — no lookup needed.
    /// Calls on_complete from the io_uring poller thread.
    fn read_into(
        &self,
        object_id: ObjectId,
        buf: &mut PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<u64, StorageError>) + Send>,  // Ok(bytes_read)
    );

    /// Write buf to NVMe as a new object. Async via io_uring.
    /// Returns the new ObjectId + crc32c via callback (caller stores these in LoValue).
    /// Atomicity: O_TMPFILE → write → linkat (file invisible until complete).
    fn write_new(
        &self,
        buf: &PoolBuffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<(ObjectId, u32), StorageError>) + Send>,  // Ok((oid, crc32c))
    );

    /// Delete an object file from NVMe. Called on key deletion or eviction.
    fn delete(&self, object_id: ObjectId);
}
```

---

## Transport Crate API (libefa-rs)

```rust
/// Shared type: both storage and transport speak this language.
/// Defined in the transport crate; storage depends on it.
pub struct PoolBuffer {
    pub ptr: *mut u8,
    pub len: usize,
}

/// Error types for transport operations.
#[derive(Debug)]
pub enum TransportError {
    DeviceNotFound,            // EFA device not present (AMI issue)
    RegistrationFailed,        // fi_mr_reg failed (memory alignment, permissions)
    SessionCreateFailed,       // fi_endpoint or fi_av_insert failed
    WriteFailed { code: i32 }, // fi_write CQE returned error
    ReadFailed { code: i32 },  // fi_read CQE returned error
    RegionOutOfBounds,         // region_idx invalid or offset+len exceeds region
    Timeout,                   // CQ poll timed out (configurable deadline)
    SessionClosed,             // operation submitted on a closed/closing session
}

pub struct EfaContext { /* fi_fabric + fi_domain per device, fi_eq, registered MRs */ }

/// EFA endpoint address — 32 bytes, opaque to callers.
/// Contains GID (16B) + QPN (2B) + pad (2B) + QKEY (4B).
/// Obtained via fi_getname(). Exchanged during LO.HELLO so each side can fi_av_insert the peer.
pub struct EfaAddress(pub [u8; 32]);

impl EfaContext {
    /// Discover EFA devices, create fabric + domain per device.
    pub fn init() -> Result<Self, TransportError>;

    /// Number of EFA devices available.
    pub fn device_count(&self) -> usize;

    /// Register buffers with all EFA domains for zero-cost per-op DMA.
    /// Buffers must be 4KB-aligned.
    pub fn register_buffers(&self, bufs: &[PoolBuffer]) -> Result<(), TransportError>;

    /// Deregister previously registered buffers.
    pub fn deregister_buffers(&self) -> Result<(), TransportError>;

    /// Shutdown and release all EFA resources.
    pub fn shutdown(self);
}

/// Client-side memory region descriptor.
/// Received during LO.HELLO. Represents one contiguous registered region on the client
/// (typically one per GPU memory pool — 1 to 8 total, NOT per object).
pub struct ClientRegion {
    pub rkey: u64,           // remote key (fi_write takes uint64_t key; EFA uses 32-bit, zero-extended)
    pub remote_addr: u64,    // base virtual address of the region on the client
    pub len: u64,            // total length of the region
}

pub struct Session { /* endpoints per EFA device, AV entries, client regions, LB state */ }

impl Session {
    /// Create a session. Internally creates fi_endpoint on each EFA device,
    /// inserts peer address into each device's AV (fi_av_insert).
    /// client_regions are stored for per-op targeting (region_idx → rkey + remote_addr).
    pub fn new(ctx: &EfaContext, peer_addr: &EfaAddress, client_regions: &[ClientRegion]) -> Result<Self, TransportError>;

    /// Server addresses to return in LO.HELLO reply.
    pub fn server_addrs(&self) -> Vec<EfaAddress>;

    /// DMA write: server buffer → client region.
    /// Non-blocking. Internally load-balances across EFA devices (best-of-two on in-flight).
    /// region_idx selects which ClientRegion to target (resolves to rkey + base addr internally).
    /// Calls on_complete from the transport CQ thread when done.
    pub fn write(
        &self,
        buf: &PoolBuffer,
        len: usize,
        region_idx: u32,
        remote_offset: u64,
        on_complete: Box<dyn FnOnce(Result<(), TransportError>) + Send>,
    );

    /// DMA read: client region → server buffer.
    /// Non-blocking. Calls on_complete when data has arrived in buf.
    /// region_idx selects which ClientRegion to read from.
    pub fn read(
        &self,
        buf: &mut PoolBuffer,
        len: usize,
        region_idx: u32,
        remote_offset: u64,
        on_complete: Box<dyn FnOnce(Result<(), TransportError>) + Send>,
    );

    /// Tear down session. In-flight ops receive SessionClosed in their callbacks.
    pub fn close(self);
}
```

---

## Client Unblocking

The module uses `ValkeyModule_BlockClient` with a `reply_callback` function pointer:

```rust
// Main thread:
bc = ValkeyModule_BlockClient(ctx, Some(reply_fn), None, Some(free_fn), timeout_ms);

// Any background thread (io_uring poller, EFA CQ thread):
ValkeyModule_UnblockClient(bc, private_data_ptr);

// Main thread (next event loop tick):
reply_fn(ctx, private_data) → ctx.reply_ok() or ctx.reply_error(msg)
```

No `ThreadSafeContext` needed. The `BlockedClient` raw pointer is `Send` — it passes through async closure chains. The reply always executes on the main thread with a valid `ctx`.

---

## LO.GET Command Flow

```
(a) Main thread (command handler):
    key = args[1]
    region_idx = args[2]
    remote_offset = args[3]

    // Data type layer: read LoValue from Valkey keyspace
    lo_value = OpenKey(key) → ModuleTypeGetValue()
    if lo_value.is_none() { return reply_null() }
    // If object exceeds fixed pool buffer size, allocate a one-off buffer of exact size.
    // This buffer is still 4KB-aligned (for O_DIRECT) but not from the pool's free-list.
    // It must be individually registered with io_uring and EFA before use, and deregistered after.
    // TODO: implement oversized buffer path (pool_get_sized or direct mmap + register)
    // OR we will have to reject the command during the LO.SET itself to not allow divergence.
    buf = if lo_value.len <= storage.pool_buf_size() {
        storage.pool_get()          // fast path: pre-registered pool slot
    } else {
        storage.pool_get_oversized(lo_value.len)  // slow path: custom-sized allocation
    }
    if buf.is_none() { return reply_error("pool exhausted") }

    session = sessions.get(client_id)
    storage.pin(buf)
    bc = BlockClient(ctx, dma_get_reply_fn, free_fn)

    // Storage layer: read from NVMe by OID (not key)
    storage.read_into(lo_value.object_id, buf, lo_value.len, move |read_result| {
        match read_result {
            Err(e) => {
                storage.unpin(buf); storage.pool_put(buf);
                UnblockClient(bc, ErrResult(e));
            }
            Ok(bytes_read) => {
                // Transport layer: send to client GPU
                session.write(buf, bytes_read, region_idx, remote_offset, move |write_result| {
                    storage.unpin(buf);
                    storage.pool_put(buf);
                    UnblockClient(bc, write_result.into());
                });
            }
        }
    });
    return NoReply

(b) Main thread (reply callback):
    dma_get_reply_fn(ctx, private_data):
        match private_data { Ok → ctx.reply_ok(), Err(e) → ctx.reply_error(e) }
```

---

## LO.SET Command Flow

```
(a) Main thread (command handler):
    key = args[1]
    len = args[2]
    region_idx = args[3]
    remote_offset = args[4]

    // TODO: The alternative is to do an one-off allocation of a custom size, after
    // dual registering it. 
    if len > storage.pool_buf_size() { return reply_error("object exceeds buffer") }

    session = sessions.get(client_id)
    buf = storage.pool_get()
    storage.pin(buf)
    bc = BlockClient(ctx, dma_set_reply_fn, free_fn)

    // Transport layer: read from client GPU into buf
    session.read(buf, len, region_idx, remote_offset, move |read_result| {
        match read_result {
            Err(e) => {
                storage.unpin(buf); storage.pool_put(buf);
                UnblockClient(bc, ErrResult(e));
            }
            Ok(()) => {
                // Storage layer: write to NVMe (returns new OID + crc)
                storage.write_new(buf, len, move |write_result| {
                    storage.unpin(buf);
                    storage.pool_put(buf);
                    UnblockClient(bc, write_result.into());
                });
            }
        }
    });
    return NoReply

(b) Main thread (reply callback):
    dma_set_reply_fn(ctx, private_data):
        match private_data {
            Ok((oid, crc)) => {
                // Data type layer: create/update LoValue in keyspace
                set_lo_value(key, LoValue { object_id: oid, len, crc32c: crc });
                ctx.replicate("TIERING.REF", key, oid, len, crc);
                ctx.reply_ok();
            }
            Err(e) => ctx.reply_error(e)
        }
```

---

## TCP vs EFA Transport Paths

Large objects need to support both RDMA-capable clients (GPU inference with EFA) and regular TCP clients (general applications, debugging, migration tooling).

**Current thinking: same command, optional args determine transport.**

```
LO.GET key [region_idx remote_offset]
  - With args:    client has LO.HELLO session → NVMe read → RDMA write to client GPU
  - Without args: no session required → NVMe read → TCP bulk reply

LO.SET key len [region_idx remote_offset]
  - With args:    client has LO.HELLO session → RDMA read from client GPU → NVMe write
  - Without args: no session required → client sends bytes inline (TCP bulk) → NVMe write
```

Server knows if the connection has a DMA session. Trailing args give the client explicit control over GPU memory placement.

**Alternative: separate commands.**

```
LO.GET  key                           → always TCP reply
LO.DGET key region_idx remote_offset  → always RDMA (requires LO.HELLO)
```

Pros: no ambiguity, no arg-count dispatch. Cons: two commands for the same logical operation.

**Decision: TBD.** Starting with optional trailing args (fewer commands, one client library path). Can split later if the arg-count dispatch causes issues.

---

## Key Design Decisions

| Concern | Decision |
|---|---|
| Layer separation | Data type = keyspace + commands. Storage = pool + disk I/O (by OID, not key). Transport = EFA. Each layer has a clean API boundary. |
| Concurrency | All I/O ops (NVMe + EFA) are non-blocking with callbacks. No thread ever blocks on another subsystem. |
| Reply mechanism | `BlockClient` with `reply_callback`. `UnblockClient` called from any thread. Reply fires on main thread. No ThreadSafeContext needed. |
| Memory registration | Buffer pool dual-registered with io_uring + EFA once at startup. Same physical pages, no conflicts. Zero per-op registration cost. |
| rkey model | Client sends 1-8 `ClientRegion` descriptors during LO.HELLO. Per-op specifies `region_idx` + `remote_offset`. |
| Multi-EFA LB | Internal to `Session`. Best-of-two on in-flight count. Storage/data type unaware. |
| Buffer ownership | Storage owns + allocates. Transport reads/writes into them. Pin/unpin prevents eviction during DMA. |
| Error handling | Typed errors (`TransportError`, `StorageError`) propagated through callbacks to `UnblockClient` → reply callback → client. |
| Callback threading | Callbacks fire on io_uring poller or transport CQ thread. Never block — only unpin/pool_put/UnblockClient (all O(1)). |
| Storage addressing | Storage takes `ObjectId`, not Valkey keys. Key→OID resolution is the data type layer's job (reads LoValue from keyspace). |
