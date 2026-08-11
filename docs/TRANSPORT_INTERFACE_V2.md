# Transport Interface Design (Option 2: Storage Module + Transport Crate)

## Architecture

- **One `.so`** — single Valkey module artifact
- **Storage module** (bigobj) — owns data type, commands, buffer pool, NVMe I/O, pin/unpin, BlockClient
- **Transport crate** (libefa-rs) — Rust crate as a Cargo dependency. Owns EFA/libfabric lifecycle, multi-device load balancing, completion handling. Reusable by any Rust project.

Storage calls into transport. Transport never calls into storage. Transport operates only on buffers that storage provides.

---

## Lifecycle

```
Module OnLoad:
  1. Transport::init()              → discover EFA devices, create fi_fabric + fi_domain per device
  2. Storage allocates buffer pool  → fixed-size buffers, 4KB-aligned, owned by storage
  3. Transport::register_buffers()  → fi_mr_reg all pool buffers across all EFA domains (one-time)

DMA.HELLO (per client connection):
  4. Session::new(client_regions)   → create fi_endpoint on each EFA device, insert client AV entries

DMA.GET / DMA.SET (per operation):
  5. storage.pin(buf)               → prevent eviction during DMA
  6. session.write/read(buf, ...)   → non-blocking, completion via callback
  7. On completion callback (fires on transport CQ thread or io_uring poller thread):
       storage.unpin(buf)
       storage.pool_put(buf)
       UnblockClient(bc, private_data)
  8. Reply callback fires on main thread:
       ctx.reply_ok() or ctx.reply_error()

Client disconnect:
  8. Session::close()               → destroy endpoints, remove AV entries

Module unload:
  9. Transport::deregister_buffers() → fi_mr_dereg
  10. Transport::shutdown()           → close domains, free global EFA state
```

---

## Client Unblocking

The module uses `RedisModule_BlockClient` with a `reply_callback` function pointer:

```rust
// Main thread:
bc = RedisModule_BlockClient(ctx, Some(reply_fn), None, Some(free_fn), timeout_ms);

// Any background thread (io_uring poller, EFA CQ thread):
RedisModule_UnblockClient(bc, private_data_ptr);

// Main thread (next event loop tick):
reply_fn(ctx, private_data) → ctx.reply_ok() or ctx.reply_error(msg)
```

No `ThreadSafeContext` needed. The `BlockedClient` raw pointer is `Send` — it passes through async closure chains. The reply always executes on the main thread with a valid `ctx`.

---

## Types

```rust
/// Object metadata — stored in the Valkey data type value struct (BoValue in keyspace).
/// Returned by write operations so the command handler can update the keyspace entry.
#[derive(Debug, Clone, Copy)]
pub struct ObjectMeta {
    pub object_id: ObjectId,   // monotonic per-node OID
    pub len: u64,              // object size in bytes
    pub crc32c: u32,           // integrity checksum
    pub created_at_us: u64,    // microsecond timestamp
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);

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

/// Error types for storage operations.
#[derive(Debug)]
pub enum StorageError {
    NotFound,                  // key does not exist
    IoError { code: i32 },     // io_uring/pread failed
    PoolExhausted,             // no free buffers available
    ObjectTooLarge,            // object exceeds pool buffer size
}
```

---

## Transport Crate API (libefa-rs)

```rust
pub struct EfaContext { /* fi_fabric + fi_domain per device, fi_eq, registered MRs */ }

impl EfaContext {
    /// Discover EFA devices, create fabric + domain per device.
    /// Returns Err(TransportError::DeviceNotFound) if no EFA devices present.
    pub fn init() -> Result<Self, TransportError>;

    /// Number of EFA devices available.
    pub fn device_count(&self) -> usize;

    /// Register buffers with all EFA domains for zero-cost per-op DMA.
    /// Buffers must be 4KB-aligned. Returns Err(TransportError::RegistrationFailed) on failure.
    pub fn register_buffers(&self, bufs: &[(*mut u8, usize)]) -> Result<(), TransportError>;

    /// Deregister previously registered buffers.
    pub fn deregister_buffers(&self) -> Result<(), TransportError>;

    /// Shutdown and release all EFA resources.
    pub fn shutdown(self);
}

/// Client-side memory region descriptor.
/// Received during DMA.HELLO. Represents one contiguous registered region on the client
/// (typically one per GPU memory pool — 1 to 8 total, NOT per object).
pub struct ClientRegion {
    pub rkey: Vec<u8>,       // remote key for this region
    pub remote_addr: u64,    // base address of the region on the client
    pub len: u64,            // total length of the region
}

pub struct Session { /* endpoints per EFA device, AV entries, client regions, LB state */ }

impl Session {
    /// Create a session. Internally creates fi_endpoint on each EFA device,
    /// inserts client address into each device's AV.
    /// Returns Err(TransportError::SessionCreateFailed) if endpoint/AV setup fails.
    pub fn new(ctx: &EfaContext, client_regions: &[ClientRegion]) -> Result<Self, TransportError>;

    /// Server addresses to return in DMA.HELLO reply (client needs these to target the server).
    pub fn server_addrs(&self) -> Vec<Vec<u8>>;

    /// DMA write: server buffer → client region.
    /// Non-blocking. Internally load-balances across EFA devices (best-of-two on in-flight).
    /// Calls `on_complete` from the transport CQ thread when done.
    ///
    /// Errors delivered via on_complete:
    ///   TransportError::RegionOutOfBounds — region_idx invalid or offset+len > region.len
    ///   TransportError::WriteFailed — CQE error from fi_write
    ///   TransportError::Timeout — CQ poll exceeded deadline
    ///   TransportError::SessionClosed — session torn down before completion
    pub fn write(
        &self,
        local_buf: *const u8,
        len: u64,
        region_idx: u32,
        remote_offset: u64,
        on_complete: Box<dyn FnOnce(Result<(), TransportError>) + Send>,
    );

    /// DMA read: client region → server buffer.
    /// Non-blocking. Calls `on_complete` when data has arrived in local_buf.
    ///
    /// Same error set as write.
    pub fn read(
        &self,
        local_buf: *mut u8,
        len: u64,
        region_idx: u32,
        remote_offset: u64,
        on_complete: Box<dyn FnOnce(Result<(), TransportError>) + Send>,
    );

    /// Tear down session. Destroys endpoints, removes AV entries.
    /// In-flight operations receive TransportError::SessionClosed in their callbacks.
    pub fn close(self);
}
```

---

## Storage Module Interface (internal, not exported)

```rust
/// Storage trait — internal to bigobj module, called by command handlers.
/// Transport crate does NOT reference this.
trait Storage {
    /// Read object from NVMe into a pool buffer. Async via io_uring.
    /// Calls on_complete from the io_uring poller thread.
    /// Error: StorageError::NotFound if key doesn't exist.
    /// Error: StorageError::IoError if io_uring read fails.
    fn read_into(
        &self,
        key: &[u8],
        buf: *mut u8,
        buf_len: u64,
        on_complete: Box<dyn FnOnce(Result<u64, StorageError>) + Send>,  // Ok(bytes_read)
    );

    /// Write value to NVMe from buffer. Async via io_uring.
    /// Calls on_complete from the io_uring poller thread.
    /// Returns ObjectMeta (OID, len, crc) needed to update Valkey keyspace.
    fn write_from(
        &self,
        key: &[u8],
        buf: *const u8,
        len: u64,
        on_complete: Box<dyn FnOnce(Result<ObjectMeta, StorageError>) + Send>,
    );

    /// Get a buffer from the pool. Returns None if pool exhausted.
    fn pool_get(&self) -> Option<*mut u8>;

    /// Return a buffer to the pool.
    fn pool_put(&self, buf: *mut u8);

    /// Pin buffer — prevents eviction/free during DMA.
    fn pin(&self, buf: *mut u8);

    /// Unpin buffer — allows eviction/free.
    fn unpin(&self, buf: *mut u8);

    /// Get object metadata without reading data (for length/existence checks).
    fn get_meta(&self, key: &[u8]) -> Option<ObjectMeta>;

    /// Pool buffer size (all buffers in pool are this size).
    fn pool_buf_size(&self) -> usize;
}
```

---

## DMA.GET Command Flow

```
(a) Main thread (command handler):
    key = args[1]
    region_idx = args[2]
    remote_offset = args[3]

    meta = storage.get_meta(key)
    if meta.is_none() { return reply_null() }
    if meta.len > storage.pool_buf_size() { return reply_error("object exceeds buffer") }

    session = sessions.get(client_id)
    buf = storage.pool_get()    // None → reply_error("pool exhausted")
    storage.pin(buf)
    bc = BlockClient(ctx, dma_get_reply_fn, free_fn)

    storage.read_into(key, buf, meta.len, move |read_result| {
        match read_result {
            Err(e) => {
                storage.unpin(buf); storage.pool_put(buf);
                UnblockClient(bc, ErrResult(e));
            }
            Ok(bytes_read) => {
                session.write(buf, bytes_read, region_idx, remote_offset, move |write_result| {
                    storage.unpin(buf);
                    storage.pool_put(buf);
                    UnblockClient(bc, write_result.into());
                });
            }
        }
    });
    return NoReply

(b) Main thread (reply callback, next event loop tick):
    dma_get_reply_fn(ctx, private_data):
        match private_data { Ok → ctx.reply_ok(), Err(e) → ctx.reply_error(e) }
```

---

## DMA.SET Command Flow

```
(a) Main thread (command handler):
    key = args[1]
    len = args[2]
    region_idx = args[3]
    remote_offset = args[4]

    if len > storage.pool_buf_size() { return reply_error("object exceeds buffer") }

    session = sessions.get(client_id)
    buf = storage.pool_get()
    storage.pin(buf)
    bc = BlockClient(ctx, dma_set_reply_fn, free_fn)

    session.read(buf, len, region_idx, remote_offset, move |read_result| {
        match read_result {
            Err(e) => {
                storage.unpin(buf); storage.pool_put(buf);
                UnblockClient(bc, ErrResult(e));
            }
            Ok(()) => {
                storage.write_from(key, buf, len, move |write_result| {
                    storage.unpin(buf);
                    storage.pool_put(buf);
                    UnblockClient(bc, write_result.map(|_meta| ()).into());
                });
            }
        }
    });
    return NoReply

(b) Main thread (reply callback):
    dma_set_reply_fn(ctx, private_data):
        match private_data { Ok → ctx.reply_ok(), Err(e) → ctx.reply_error(e) }
```

---

## Key Design Decisions

| Concern | Decision |
|---|---|
| Concurrency | All I/O ops (NVMe + EFA) are non-blocking with callbacks. No thread ever blocks on another subsystem. |
| Reply mechanism | `BlockClient` with `reply_callback`. `UnblockClient` called from any thread. Reply fires on main thread. No ThreadSafeContext. |
| Memory registration | Storage buffer pool registered with EFA once at startup. Zero `fi_mr_reg` per operation. |
| rkey model | Client sends 1-8 `ClientRegion` descriptors during DMA.HELLO. Per-op specifies `region_idx` + `remote_offset`. |
| Multi-EFA LB | Internal to `Session`. Best-of-two on in-flight count. Storage unaware. |
| Lifecycle | init → register_buffers → Session::new → ops → Session::close → deregister → shutdown. |
| Buffer ownership | Storage owns + allocates. Transport reads/writes into them. Pin/unpin prevents eviction during DMA. |
| Shared buffers | Dual-registered: io_uring (`IORING_REGISTER_BUFFERS`) + EFA (`fi_mr_reg`). Zero copy between NVMe read and EFA write. |
| Error handling | Typed errors (`TransportError`, `StorageError`) propagated through callbacks to `UnblockClient` → reply callback → client. |
| Callback threading | Callbacks fire on io_uring poller or transport CQ thread. Never block — only unpin/pool_put/UnblockClient (all O(1)). |
