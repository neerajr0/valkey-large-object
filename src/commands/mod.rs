//! Command Handlers — LO.HELLO, LO.GET, LO.SET
//!
//! Command flow (STORAGE_DESIGN.md §9.2):
//!   TCP GET, DRAMPool hit → Main thread only (no I/O)
//!   TCP GET, DRAMPool miss → Main → tokio task (future) / io-poller → reply
//!   TCP SET → Main → io-poller → reply
//!   EFA anything → Main → tokio task (future)
//!
//! LO.GET key [rkey remote_addr len]
//!   NVMe read path is the same for both transports. Branch at completion:
//!   - EFA: session.write(buf → client GPU)
//!   - TCP: reply with bulk string from buf
//!
//! LO.SET key len [rkey remote_addr]
//!   NVMe write path is the same for both transports. Source of bytes differs:
//!   - EFA: session.read(client GPU → buf) then NVMe write
//!   - TCP: bytes already inline in RESP args, copy into buf, then NVMe write

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::storage::{self, uring};
use crate::transport::{self, EfaAddress, Session};

// ─── Per-Client Session Store ────────────────────────────────────────────────
//
// Each EFA client calls LO.HELLO once to establish a session.
// The session holds fi_av_insert'd dest_fi_addr handles for the client.
// Session is keyed by Valkey client_id and cleaned up on disconnect.

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<u64, Arc<Session>>> = Mutex::new(HashMap::new());
}

// ─── LO.HELLO ────────────────────────────────────────────────────────────────
//
// Establishes an EFA session with the client.
// Client sends its EFA address (32 bytes hex). Server calls fi_av_insert on all
// N EFA devices and returns all N server EFA addresses.
// After HELLO, the client can use DMA variants of LO.GET/LO.SET.

pub fn lo_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let efa_ctx = transport::efa_context();
    if !efa_ctx.is_available() {
        return Err(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE));
    }

    // Parse client's EFA address from hex.
    let peer_hex = args[1].to_string_lossy();
    let peer_bytes =
        hex_decode(&peer_hex).map_err(|_| ValkeyError::Str(errors::ERR_INVALID_PEER_ADDR_HEX))?;
    if peer_bytes.len() != 32 {
        return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_LEN));
    }
    let mut addr = [0u8; 32];
    addr.copy_from_slice(&peer_bytes);
    let peer_addr = EfaAddress(addr);

    // Create session: fi_av_insert on all server EFA devices for this client.
    let session = Session::new(efa_ctx, &peer_addr)
        .map_err(|e| ValkeyError::String(format!("{}: {}", errors::ERR_SESSION_CREATE, e)))?;
    let server_addrs = session.server_addrs();

    // Store session keyed by client_id.
    let client_id = ctx.get_client_id();
    SESSIONS
        .lock()
        .unwrap()
        .insert(client_id, Arc::new(session));

    // Return all server EFA addresses (one per device) as hex strings.
    let reply: Vec<ValkeyValue> = server_addrs
        .iter()
        .map(|a| ValkeyValue::BulkString(hex_encode(&a.0)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── LO.GET ──────────────────────────────────────────────────────────────────
//
// Flow:
//   1. Lookup LoValue in keyspace → get object_id, len
//   2. Check DRAMPool for cache hit (Arc<ObjectContext> clone, serve directly)
//   3. On miss: alloc from NVMePool, ReadFixed from NVMe
//   4. On completion:
//      - TCP: reply with bulk string (obj_len bytes)
//      - EFA: session.write(buf → client GPU), reply with bytes_read integer
//   5. Free NVMePool buffer

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;

    // Parse optional EFA args: LO.GET key [rkey remote_addr]
    let efa_args = if args.len() >= 4 {
        let rkey: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        Some((rkey, remote_addr))
    } else {
        None
    };

    // Clone Arc<Session> BEFORE entering async path (avoid locking SESSIONS in callback).
    let client_id = ctx.get_client_id();
    let session_arc = if efa_args.is_some() {
        let sessions = SESSIONS.lock().unwrap();
        match sessions.get(&client_id) {
            Some(s) => Some(Arc::clone(s)),
            None => return Err(ValkeyError::Str(errors::ERR_NO_DMA_SESSION)),
        }
    } else {
        None
    };

    // ─── DRAMPool hit check ──────────────────────────────────────────────
    let dram_pool = storage::get_dram_pool();
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            // DRAMPool hit — serve directly from cached buffers.
            // TODO: For EFA, use transport.write from DRAMPool buffers (tokio task).
            // For now: TCP path only.
            let mut data = Vec::with_capacity(obj_len as usize);
            for buf in &obj_ctx.buffers {
                let ptr = dram_pool.buffer_ptr(buf);
                let slice = unsafe { std::slice::from_raw_parts(ptr, buf.len as usize) };
                data.extend_from_slice(slice);
            }
            data.truncate(obj_len as usize);
            return Ok(ValkeyValue::StringBuffer(data));
        }
        // ObjectContext in Filling state — promotion in progress.
        // TODO: coalesce on this ObjectContext (§7.3.5). For now, fall through to NVMe.
    }

    // ─── DRAMPool miss: read from NVMe via NVMePool ──────────────────────
    let nvme_pool = storage::get_nvme_pool();
    let seg_buf = nvme_pool
        .alloc(obj_len as usize)
        .ok_or(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED))?;

    // Open fd for this object (TODO: use FdPool with LFRU + Arc<FdEntry>).
    let dir = crate::data_dir();
    let path = object_id.file_path(&dir);
    let c_path = std::ffi::CString::new(path).unwrap();
    let mut flags = libc::O_RDONLY;
    if crate::direct_io() {
        flags |= libc::O_DIRECT;
    }
    let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
    if fd < 0 {
        nvme_pool.free(&seg_buf);
        return Err(ValkeyError::Str(errors::ERR_NVME_READ));
    }

    let buf_ptr = nvme_pool.buffer_ptr(&seg_buf);
    let buf_ptr_usize = buf_ptr as usize; // Cast to usize for Send across thread boundary.
    let buf_index = nvme_pool.segments[seg_buf.segment_idx as usize].buf_index;

    let blocked_client = ctx.block_client();

    // Submit ReadFixed to io_uring. Callback fires on io-poller thread.
    uring::submit(uring::IoRequest::Read {
        fd,
        buf_index,
        buf_ptr,
        file_offset: 0,
        len: obj_len,
        on_complete: Box::new(move |result| {
            unsafe { libc::close(fd) };
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

            match result {
                Ok(_bytes_read) => {
                    if let Some((_rkey, _remote_addr)) = efa_args {
                        // ─── EFA path: RDMA write buf → client GPU ───────────
                        // session_arc was cloned before entering this callback.
                        if let Some(session) = session_arc {
                            let buf_ptr_raw = buf_ptr_usize as *mut u8;
                            session.write(
                                buf_ptr_raw,
                                obj_len as usize,
                                _rkey,
                                _remote_addr,
                                Box::new(move |_buf_ptr, write_result| {
                                    storage::get_nvme_pool().free(&seg_buf);
                                    match write_result {
                                        Ok(()) => {
                                            thread_ctx.reply(Ok(ValkeyValue::Integer(
                                                obj_len as i64,
                                            )));
                                        }
                                        Err(e) => {
                                            thread_ctx.reply(Err(ValkeyError::String(format!(
                                                "{}: {}",
                                                errors::ERR_EFA_WRITE,
                                                e
                                            ))));
                                        }
                                    }
                                }),
                            );
                        } else {
                            storage::get_nvme_pool().free(&seg_buf);
                            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_SESSION_GONE)));
                        }
                    } else {
                        // ─── TCP path: reply with object data ─────────────────
                        // Use obj_len (true length), not _bytes_read (aligned).
                        let data = unsafe {
                            std::slice::from_raw_parts(
                                buf_ptr_usize as *const u8,
                                obj_len as usize,
                            )
                            .to_vec()
                        };
                        storage::get_nvme_pool().free(&seg_buf);
                        thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
                    }
                }
                Err(e) => {
                    storage::get_nvme_pool().free(&seg_buf);
                    thread_ctx.reply(Err(ValkeyError::String(format!(
                        "{}: {}",
                        errors::ERR_NVME_READ,
                        e
                    ))));
                }
            }
        }),
    });

    Ok(ValkeyValue::NoReply)
}

// ─── LO.SET ──────────────────────────────────────────────────────────────────
//
// Flow:
//   1. Allocate NVMePool buffer
//   2. Fill buffer:
//      - TCP: memcpy from inline RESP arg
//      - EFA: session.read(client GPU → buf)
//   3. Compute CRC32c
//   4. WriteFixed to NVMe (tmp file)
//   5. On completion: atomic rename, create LoValue in keyspace
//   6. Free NVMePool buffer

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = args[1].clone();
    let obj_len: u64 = args[2]
        .to_string_lossy()
        .parse()
        .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;

    // Parse optional EFA args: LO.SET key len [rkey remote_addr]
    let efa_args = if args.len() >= 5 {
        let rkey: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[4]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        Some((rkey, remote_addr))
    } else {
        None
    };

    // Clone Arc<Session> before async path.
    let client_id = ctx.get_client_id();
    let session_arc = if efa_args.is_some() {
        let sessions = SESSIONS.lock().unwrap();
        match sessions.get(&client_id) {
            Some(s) => Some(Arc::clone(s)),
            None => return Err(ValkeyError::Str(errors::ERR_NO_DMA_SESSION)),
        }
    } else {
        None
    };

    // Allocate from NVMePool.
    let nvme_pool = storage::get_nvme_pool();
    let seg_buf = nvme_pool
        .alloc(obj_len as usize)
        .ok_or(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED))?;

    let buf_ptr = nvme_pool.buffer_ptr(&seg_buf);
    let buf_ptr_usize = buf_ptr as usize; // Safe to send: stable segment memory for module lifetime.
    let blocked_client = ctx.block_client();

    if let Some((rkey, remote_addr)) = efa_args {
        // ─── EFA path: read from client GPU into NVMePool buffer, then write to NVMe ───
        if let Some(session) = session_arc {
            let key_for_reply = key_name.as_slice().to_vec();
            session.read(
                buf_ptr,
                obj_len as usize,
                rkey,
                remote_addr,
                Box::new(move |_buf_ptr, read_result| {
                    match read_result {
                        Ok(()) => {
                            // EFA read complete — data is in buf. Now write to NVMe.
                            let ptr = buf_ptr_usize as *mut u8;
                            do_nvme_write(
                                ptr, obj_len, &seg_buf, blocked_client, key_for_reply,
                            );
                        }
                        Err(e) => {
                            storage::get_nvme_pool().free(&seg_buf);
                            let thread_ctx =
                                valkey_module::ThreadSafeContext::with_blocked_client(
                                    blocked_client,
                                );
                            thread_ctx.reply(Err(ValkeyError::String(format!(
                                "{}: {}",
                                errors::ERR_EFA_READ,
                                e
                            ))));
                        }
                    }
                }),
            );
        } else {
            nvme_pool.free(&seg_buf);
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_SESSION_GONE)));
        }
    } else {
        // ─── TCP path: data is inline in args[3] ─────────────────────────────
        if args.len() > 3 {
            let data = args[3].as_slice();
            let copy_len = data.len().min(obj_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };
        }
        let key_for_reply = key_name.as_slice().to_vec();
        // buf_ptr is valid here — still on main thread, do_nvme_write captures as usize internally.
        do_nvme_write(buf_ptr, obj_len, &seg_buf, blocked_client, key_for_reply);
    }

    Ok(ValkeyValue::NoReply)
}

// ─── Shared NVMe Write Logic ─────────────────────────────────────────────────
//
// Used by both TCP and EFA SET paths after the buffer is filled.
// Computes CRC, opens tmp file, submits WriteFixed, on completion renames + creates LoValue.

fn do_nvme_write(
    buf_ptr: *mut u8,
    obj_len: u64,
    seg_buf: &storage::SegmentBuffer,
    blocked_client: valkey_module::BlockedClient,
    key_for_reply: Vec<u8>,
) {
    // Compute CRC over actual object bytes.
    let crc = crc32c::crc32c(unsafe { std::slice::from_raw_parts(buf_ptr, obj_len as usize) });

    // Open tmp file for write.
    let oid = ObjectId::next();
    let dir = crate::data_dir();
    let file_path = oid.file_path(&dir);
    let tmp_path = format!("{}.tmp", file_path);
    let c_tmp = std::ffi::CString::new(tmp_path.as_str()).unwrap();
    let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        write_flags |= libc::O_DIRECT;
    }
    let fd = unsafe { libc::open(c_tmp.as_ptr(), write_flags, 0o644) };
    if fd < 0 {
        storage::get_nvme_pool().free(seg_buf);
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        return;
    }

    let nvme_pool = storage::get_nvme_pool();
    let buf_index = nvme_pool.segments[seg_buf.segment_idx as usize].buf_index;
    let seg_buf_clone = seg_buf.clone();

    // Submit WriteFixed. Callback fires on io-poller thread.
    uring::submit(uring::IoRequest::Write {
        fd,
        buf_index,
        buf_ptr: buf_ptr as *const u8,
        file_offset: 0,
        len: obj_len,
        on_complete: Box::new(move |result| {
            unsafe { libc::close(fd) };
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

            match result {
                Ok(()) => {
                    // Atomic rename: tmp → final.
                    if std::fs::rename(&tmp_path, &file_path).is_ok() {
                        // Create LoValue in keyspace (requires lock on ThreadSafeContext).
                        {
                            let ctx = thread_ctx.lock();
                            let key_str = ctx.create_string(key_for_reply);
                            let key = ctx.open_key_writable(&key_str);
                            let lo_value = LoValue {
                                object_id: oid,
                                len: obj_len,
                                crc32c: crc,
                            };
                            key.set_value(&LO_TYPE, lo_value).unwrap();
                        }
                        storage::get_nvme_pool().free(&seg_buf_clone);
                        thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
                    } else {
                        let _ = std::fs::remove_file(&tmp_path);
                        storage::get_nvme_pool().free(&seg_buf_clone);
                        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
                    }
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp_path);
                    storage::get_nvme_pool().free(&seg_buf_clone);
                    thread_ctx.reply(Err(ValkeyError::String(format!(
                        "{}: {}",
                        errors::ERR_NVME_WRITE,
                        e
                    ))));
                }
            }
        }),
    });
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
