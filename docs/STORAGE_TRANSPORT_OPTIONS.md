# Storage & Transport Separation: Options

## Context

How should storage (NVMe I/O, buffer pool, pin/unpin) and transport (EFA/RDMA) relate structurally?

## Option 1: Single Module, Internal Boundary

One `.so`. Storage and transport are Rust mods (`mod storage`, `mod transport`) in the same crate.

### DMA.GET

```
(a) Command handler — main thread:
    session = transport::sessions::get(client_id)
    bc = block_client(ctx, dma_get_reply_callback)
    spawn → bg_dma_get(bc, key, session)

(b) Background thread:
    (ptr, len, handle) = storage::engine().read_blocking(key)
    storage::engine().pin(handle)
    transport::efa::write(ptr, len, session.remote_addr, session.rkey)
    storage::engine().unpin(handle)
    storage::engine().release(handle)
    UnblockClient(bc, result_ok)

(c) Reply callback — main thread, normal ctx:
    dma_get_reply_callback(ctx, private_data):
        ctx.reply_ok()
```

### DMA.SET

```
(a) Command handler — main thread:
    session = transport::sessions::get(client_id)
    buf = storage::engine().alloc_aligned_buffer(len)
    bc = block_client(ctx, dma_set_reply_callback)
    spawn → bg_dma_set(bc, key, buf, len, session)

(b) Background thread:
    transport::efa::read(buf, len, session.remote_addr, session.rkey)
    storage::engine().put_blocking(key, buf)
    UnblockClient(bc, result_ok)

(c) Reply callback — main thread, normal ctx:
    dma_set_reply_callback(ctx, private_data):
        ctx.reply_ok()
```

### Pros

- Direct `storage::` and `transport::` calls — zero indirection
- One `.so` to build, package, deploy and version
- No external contract. Avoids having to maintain a Module Interface, compatibility across future versions and avoiding breaking changes in the interface. Once we claim other Modules can use a Module Interface (even if we are exporting it), it needs to be maintained.
- Compile-time type safety (Rust traits)

### Cons

- EFA code not reusable outside without copy-paste
- Momento cannot release transport independently
- Extracting later requires moving files into a crate

### Interface

No external contract. Rust traits internal to one crate.

```rust
// Storage trait (transport calls this):
trait Storage {
    fn read_blocking(&self, key: &[u8]) -> Result<(Handle, *const u8, u64)>;
    fn pin(&self, handle: Handle);
    fn unpin(&self, handle: Handle);
    fn release(&self, handle: Handle);
    fn put_blocking(&self, key: &[u8], data: &[u8]) -> Result<ObjectMeta>;
    fn alloc_aligned_buffer(&self, len: u64) -> *mut u8;
}

// Transport trait (commands call this):
trait Transport {
    fn init(&mut self) -> Result<()>;
    fn session_setup(&self, client_id: u64, client_rkey: &[u8], client_remote_addr: u64) -> Result<SessionId>;
    fn write(&self, session_id: SessionId, local_ptr: *const u8, len: u64) -> Result<()>;
    fn read(&self, session_id: SessionId, local_buf: *mut u8, len: u64) -> Result<()>;
    fn teardown(&self, session_id: SessionId);
}
```

---

## Option 2: Single Module + EFA Crate [Recommended]

We have a Rust Crate that handles the transport EFA/libfabric logic. And we have a Storage Module that handles data type + storage logic + commands / blocking etc. This results in one single `.so` artifact and the EFA logic can be in a Rust crate which is re-usable across any project which wishes to do so.

- Transport EFA logic is a Rust crate (`libefa-rs`) as a Cargo dependency
- One `.so`

### DMA.GET

```
(a) Command handler — main thread:
    session = transport::sessions::get(client_id)
    bc = block_client(ctx, dma_get_reply_callback)
    spawn → bg_dma_get(bc, key, session)

(b) Background thread:
    (ptr, len, handle) = storage::engine().read_blocking(key)
    storage::engine().pin(handle)
    session.efa.write(ptr, len)              // libefa_rs::Session::write
    storage::engine().unpin(handle)
    storage::engine().release(handle)
    UnblockClient(bc, result_ok)

(c) Reply callback — main thread, normal ctx:
    dma_get_reply_callback(ctx, private_data):
        ctx.reply_ok()
```

### DMA.SET

```
(a) Command handler — main thread:
    session = transport::sessions::get(client_id)
    buf = storage::engine().alloc_aligned_buffer(len)
    bc = block_client(ctx, dma_set_reply_callback)
    spawn → bg_dma_set(bc, key, buf, len, session)

(b) Background thread:
    session.efa.read(buf, len)               // libefa_rs::Session::read
    storage::engine().put_blocking(key, buf)
    UnblockClient(bc, result_ok)

(c) Reply callback — main thread, normal ctx:
    dma_set_reply_callback(ctx, private_data):
        ctx.reply_ok()
```

### Pros

- `storage::` calls direct. `session.efa.*` calls into the crate — Rust type safety preserved.
- EFA crate reusable by any Rust project
- Still one `.so` — no deployment complexity
- Momento CAN release transport independently as a Rust crate which can be maintained and documented like any other Rust project
- We do not have to worry about maintaining inter module APIs and interfaces across Valkey versions

### Cons

- Two versioned things (module pins crate in Cargo.toml)
- The Rust Crate needs its maintenance (of interfaces) and publishing

### Interface

Crate handles the transport EFA/libfabric. BigObj Storage Module handles storage + commands.

```rust
// --- libefa-rs crate public API ---

pub struct EfaContext { /* fi_fabric, fi_domain, fi_eq */ }
impl EfaContext {
    pub fn new() -> Result<Self>;
}

pub struct Session { /* fi_endpoint, fi_av entry, client rkey+addr */ }
impl Session {
    pub fn new(ctx: &EfaContext, client_rkey: &[u8], client_remote_addr: u64) -> Result<Self>;
    pub fn server_addr(&self) -> &[u8];
    pub fn write(&self, local_ptr: *const u8, len: u64) -> Result<()>;
    pub fn read(&self, local_buf: *mut u8, len: u64) -> Result<()>;
    pub fn close(self);
}

// --- Storage trait (internal to module, same as Option 1) ---
trait Storage {
    fn read_blocking(&self, key: &[u8]) -> Result<(Handle, *const u8, u64)>;
    fn pin(&self, handle: Handle);
    fn unpin(&self, handle: Handle);
    fn release(&self, handle: Handle);
    fn put_blocking(&self, key: &[u8], data: &[u8]) -> Result<ObjectMeta>;
    fn alloc_aligned_buffer(&self, len: u64) -> *mut u8;
    fn free_buffer(&self, buf: *mut u8);
}
```

---

## Option 3: Two Modules — Transport Exports, BigObj Imports

Two `.so` files. Module A (vrdma) exports transport APIs. Module B (bigobj) owns ALL commands, data type, storage, and imports Module A's transport.

### Pros

- BigObj module owns all commands and logic — no split brain for data type ownership
- Transport module is a pure library (no Valkey command/data type knowledge)
- Transport module reusable by any other module that needs RDMA
- Momento can ship transport independently
- Storage calls are local (direct), only transport crosses FFI — clean separation

### Cons

- C ABI contract on the transport API (versioned function table). Publishing the transport + storage module is a one way door and we need to maintain its interface and the APIs it exports.
- Two `.so` to deploy, load ordering matters (vrdma first)
- Transport API type errors are runtime, not compile-time
- `transport_api.write()` blocks the bg thread inside Module A's code — Module A must be thread-safe

---

## Option 4: Two Modules — BigObj Exports Storage, Transport Owns Commands

Two `.so` files. Module A (bigobj) exports storage APIs. Module B (vrdma) owns DMA commands and imports storage.

### Pros

- Full org independence (separate repos, releases, teams)
- vrdma could serve other data types exporting the same storage API

### Cons

- DMA commands split from data type — two modules must agree on semantics
- C ABI contract on storage API (more complex: get_sync, get_async, pin, unpin, memory_region, put, alloc). Publishing the transport + storage module is a one way door and we need to maintain its interface and the APIs it exports.
- Cross-module async callbacks (Module B's callback runs on Module A's reaper thread)
- Channel + callback dance for async reads
- Two `.so` to deploy, load ordering matters
- Debugging: two module stacks interleave in crashes

---

## Comparison

| | Option 1 | Option 2 | Option 3 | Option 4 |
|---|---|---|---|---|
| Deployment | 1 .so | 1 .so | 2 .so | 2 .so |
| Who owns DMA commands | bigobj | bigobj | bigobj | vrdma |
| What crosses FFI | Nothing | Nothing (crate boundary) | Transport calls only | Storage calls only |
| Call complexity | Direct | Direct | transport_api.write() blocks — simple | More complex: storage_api.get_async() + callback + channel |
| Reusability | None | EFA crate | Transport module | Storage API |
| Org independence | Same release | Crate separate | Fully separate | Fully separate |
| ABI surface | None | None | Small (write/read/session) | Large (get/put/pin/unpin/alloc/async) |

## Recommendation

Start with **Option 2** if we are able to align on the interface of the RDMA transport layer Rust Crate. Otherwise, we should go with **Option 1**.
