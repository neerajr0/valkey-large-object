//! Command Handlers — LO.HELLO, LO.GET, LO.SET
//!
//! Follows the interface doc command flows exactly.
//! LO.GET key [region_idx remote_offset] — RDMA if args, TCP if none.
//! LO.SET key len [region_idx remote_offset] — DMA or TCP upload → NVMe.
//! LO.HELLO — establish EFA session, exchange addresses + memory regions.

use std::collections::HashMap;
use std::sync::Mutex;

use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::storage::{self, Storage, StorageError};
use crate::transport::{self, ClientRegion, EfaAddress, PoolBuffer, Session, TransportError};

// ─── Per-Client Session Store ────────────────────────────────────────────────

lazy_static::lazy_static! {
    /// Active DMA sessions keyed by client ID.
    static ref SESSIONS: Mutex<HashMap<u64, Session>> = Mutex::new(HashMap::new());
}

// ─── LO.HELLO ────────────────────────────────────────────────────────────────
//
// Establishes EFA session. Exchanges:
//   Client → Server: peer EFA address + memory regions (rkey, remote_addr, len)
//   Server → Client: server EFA addresses
//
// LO.HELLO <peer_addr_hex> <num_regions> [rkey remote_addr len] ...

pub fn lo_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let efa_ctx = transport::efa_context();
    if !efa_ctx.is_available() {
        return Err(valkey_module::ValkeyError::Str("ERR EFA unavailable on this instance"));
    }

    // Parse peer address (hex-encoded 32 bytes = 64 hex chars).
    let peer_hex = args[1].to_string_lossy();
    let peer_bytes = hex_decode(&peer_hex).map_err(|_| {
        valkey_module::ValkeyError::Str("ERR invalid peer address hex")
    })?;
    if peer_bytes.len() != 32 {
        return Err(valkey_module::ValkeyError::Str("ERR peer address must be 32 bytes"));
    }
    let mut addr = [0u8; 32];
    addr.copy_from_slice(&peer_bytes);
    let peer_addr = EfaAddress(addr);

    // Parse number of regions.
    let num_regions: usize = args[2].to_string_lossy().parse().map_err(|_| {
        valkey_module::ValkeyError::Str("ERR invalid num_regions")
    })?;

    // Parse regions: each is (rkey, remote_addr, len).
    let expected_args = 3 + num_regions * 3;
    if args.len() < expected_args {
        return Err(valkey_module::ValkeyError::Str("ERR insufficient region args"));
    }

    let mut regions = Vec::with_capacity(num_regions);
    for i in 0..num_regions {
        let base = 3 + i * 3;
        let rkey: u64 = args[base].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid rkey")
        })?;
        let remote_addr: u64 = args[base + 1].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid remote_addr")
        })?;
        let len: u64 = args[base + 2].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid region len")
        })?;
        regions.push(ClientRegion {
            rkey,
            remote_addr,
            len,
        });
    }

    // Create session (fi_av_insert + store regions).
    let session = Session::new(efa_ctx, &peer_addr, regions).map_err(|e| {
        valkey_module::ValkeyError::String(format!("ERR session create: {}", e))
    })?;

    // Reply with server addresses.
    let server_addrs = session.server_addrs();

    // Store session keyed by client ID.
    let client_id = ctx.get_client_id();
    SESSIONS.lock().unwrap().insert(client_id, session);

    // Reply: array of hex-encoded server addresses.
    let reply: Vec<ValkeyValue> = server_addrs
        .iter()
        .map(|a| ValkeyValue::BulkString(hex_encode(&a.0)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── LO.GET ──────────────────────────────────────────────────────────────────
//
// LO.GET key [region_idx remote_offset]
//   With args:    NVMe read → RDMA write to client GPU
//   Without args: NVMe read → TCP bulk reply

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key_name = &args[1];

    // Open key, get LoValue from keyspace.
    let key = ctx.open_key(key_name);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;

    // Determine path: EFA (with args) or TCP (without).
    if args.len() >= 4 {
        // ─── EFA Path ────────────────────────────────────────────────────
        let region_idx: u32 = args[2].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid region_idx")
        })?;
        let remote_offset: u64 = args[3].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid remote_offset")
        })?;

        let client_id = ctx.get_client_id();
        let sessions = SESSIONS.lock().unwrap();
        if !sessions.contains_key(&client_id) {
            return Err(valkey_module::ValkeyError::Str(
                "ERR no DMA session (call LO.HELLO first)",
            ));
        }
        drop(sessions);

        // Get buffer from pool.
        let storage = storage::get();
        let mut buf = storage.pool_get().ok_or_else(|| {
            valkey_module::ValkeyError::Str("ERR pool exhausted")
        })?;

        storage.pin(&buf);

        // BlockClient → submit NVMe read → on completion, submit EFA write → UnblockClient.
        // TODO: Implement BlockClient + async callback chain as described in interface doc.
        //   bc = BlockClient(ctx, reply_fn, free_fn)
        //   storage.read_into(object_id, &mut buf, obj_len, move |read_result| {
        //       match read_result {
        //           Ok(bytes_read) => {
        //               session.write(&buf, bytes_read, region_idx, remote_offset, move |write_result| {
        //                   storage.unpin(&buf); storage.pool_put(buf);
        //                   UnblockClient(bc, write_result);
        //               });
        //           }
        //           Err(e) => { storage.unpin(&buf); storage.pool_put(buf); UnblockClient(bc, Err(e)); }
        //       }
        //   });
        //   return NoReply
        //
        // Stub: synchronous path for initial compilation.
        storage.unpin(&buf);
        storage.pool_put(buf);
        Ok(ValkeyValue::SimpleStringStatic("OK"))
    } else {
        // ─── TCP Path ────────────────────────────────────────────────────
        // NVMe read → TCP bulk reply (standard RESP).
        // TODO: BlockClient + async NVMe read, reply with bulk string.
        //   For now, stub with key existence confirmation.
        Ok(ValkeyValue::Integer(obj_len as i64))
    }
}

// ─── LO.SET ──────────────────────────────────────────────────────────────────
//
// LO.SET key len [region_idx remote_offset]
//   With args:    RDMA read from client GPU → NVMe write
//   Without args: TCP bulk upload → NVMe write

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key_name = &args[1];
    let obj_len: u64 = args[2].to_string_lossy().parse().map_err(|_| {
        valkey_module::ValkeyError::Str("ERR invalid len")
    })?;

    let storage = storage::get();

    // Check size against pool buffer.
    if obj_len > storage.pool_buf_size() as u64 {
        return Err(valkey_module::ValkeyError::Str("ERR object exceeds buffer size"));
    }

    if args.len() >= 5 {
        // ─── EFA Path ────────────────────────────────────────────────────
        let region_idx: u32 = args[3].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid region_idx")
        })?;
        let remote_offset: u64 = args[4].to_string_lossy().parse().map_err(|_| {
            valkey_module::ValkeyError::Str("ERR invalid remote_offset")
        })?;

        let client_id = ctx.get_client_id();
        let sessions = SESSIONS.lock().unwrap();
        if !sessions.contains_key(&client_id) {
            return Err(valkey_module::ValkeyError::Str(
                "ERR no DMA session (call LO.HELLO first)",
            ));
        }
        drop(sessions);

        let mut buf = storage.pool_get().ok_or_else(|| {
            valkey_module::ValkeyError::Str("ERR pool exhausted")
        })?;

        storage.pin(&buf);

        // BlockClient → EFA read from client → NVMe write → UnblockClient.
        // TODO: Implement BlockClient + async callback chain as described in interface doc.
        //   bc = BlockClient(ctx, reply_fn, free_fn)
        //   session.read(&mut buf, obj_len as usize, region_idx, remote_offset, move |read_result| {
        //       match read_result {
        //           Ok(()) => {
        //               storage.write_new(&buf, obj_len, move |write_result| {
        //                   storage.unpin(&buf); storage.pool_put(buf);
        //                   match write_result {
        //                       Ok((oid, crc)) => {
        //                           // UnblockClient with (oid, crc) — reply callback writes LoValue to keyspace.
        //                           UnblockClient(bc, Ok((oid, obj_len, crc)));
        //                       }
        //                       Err(e) => UnblockClient(bc, Err(e)),
        //                   }
        //               });
        //           }
        //           Err(e) => { storage.unpin(&buf); storage.pool_put(buf); UnblockClient(bc, Err(e)); }
        //       }
        //   });
        //   return NoReply
        //
        // Stub: synchronous for initial compilation.
        storage.unpin(&buf);
        storage.pool_put(buf);
        Ok(ValkeyValue::SimpleStringStatic("OK"))
    } else {
        // ─── TCP Path ────────────────────────────────────────────────────
        // Client sends bytes inline via RESP bulk string.
        // TODO: Read bulk payload from client, write to NVMe, store LoValue.
        //
        // For now, create LoValue with a stub OID.
        let oid = ObjectId::next();
        let crc = 0u32; // TODO: compute from received bytes

        let lo = LoValue {
            object_id: oid,
            len: obj_len,
            crc32c: crc,
        };

        // Store in keyspace.
        let key = ctx.open_key_writable(key_name);
        key.set_value(&LO_TYPE, lo)?;

        Ok(ValkeyValue::SimpleStringStatic("OK"))
    }
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
