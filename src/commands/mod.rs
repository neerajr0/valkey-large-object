//! Command Handlers — thin layer that parses args and dispatches to engine.
//!
//! LO.HELLO: EFA session establishment
//! LO.GET key [rkey remote_addr]: engine::execute_get
//! LO.SET key len <data|rkey remote_addr>: engine::execute_set

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, LO_TYPE};
use crate::engine::{self, DataSource, Transport};
use crate::errors;
use crate::transport::{self, EfaAddress, Session};

// ─── Per-Client Session Store ────────────────────────────────────────────────

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<u64, Arc<Session>>> = Mutex::new(HashMap::new());
}

// ─── LO.HELLO ────────────────────────────────────────────────────────────────
//
// Establishes an EFA session with the client.
// Client sends its EFA address (32 bytes hex). Server calls fi_av_insert on all
// N EFA devices and returns all N server EFA addresses.

pub fn lo_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let efa_ctx = transport::efa_context();
    if !efa_ctx.is_available() {
        return Err(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE));
    }

    let peer_hex = args[1].to_string_lossy();
    let peer_bytes =
        hex_decode(&peer_hex).map_err(|_| ValkeyError::Str(errors::ERR_INVALID_PEER_ADDR_HEX))?;
    if peer_bytes.len() != 32 {
        return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_LEN));
    }
    let mut addr = [0u8; 32];
    addr.copy_from_slice(&peer_bytes);
    let peer_addr = EfaAddress(addr);

    let session = Session::new(efa_ctx, &peer_addr)
        .map_err(|e| ValkeyError::String(format!("{}: {}", errors::ERR_SESSION_CREATE, e)))?;
    let server_addrs = session.server_addrs();

    let client_id = ctx.get_client_id();
    SESSIONS
        .lock()
        .unwrap()
        .insert(client_id, Arc::new(session));

    let reply: Vec<ValkeyValue> = server_addrs
        .iter()
        .map(|a| ValkeyValue::BulkString(hex_encode(&a.0)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── LO.GET ──────────────────────────────────────────────────────────────────
//
// Parse args → resolve key → determine transport → dispatch to engine.

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    // Lookup LoValue in keyspace.
    let key = ctx.open_key(&args[1]);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;

    // Determine transport: EFA if rkey+remote_addr provided, else TCP.
    let transport = if args.len() >= 4 {
        let rkey: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let client_id = ctx.get_client_id();
        let session = SESSIONS
            .lock()
            .unwrap()
            .get(&client_id)
            .ok_or(ValkeyError::Str(errors::ERR_NO_DMA_SESSION))?
            .clone();
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        }
    } else {
        Transport::Tcp
    };

    // Block client and dispatch to engine.
    let blocked_client = ctx.block_client();
    engine::execute_get(object_id, obj_len, transport, blocked_client);

    Ok(ValkeyValue::NoReply)
}

// ─── LO.SET ──────────────────────────────────────────────────────────────────
//
// Parse args → determine data source → dispatch to engine.

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = args[1].as_slice().to_vec();
    let obj_len: u64 = args[2]
        .to_string_lossy()
        .parse()
        .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;

    // Determine data source: EFA if rkey+remote_addr provided, else TCP (inline data).
    let data_source = if args.len() >= 5 {
        // EFA path: LO.SET key len rkey remote_addr
        let rkey: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[4]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let client_id = ctx.get_client_id();
        let session = SESSIONS
            .lock()
            .unwrap()
            .get(&client_id)
            .ok_or(ValkeyError::Str(errors::ERR_NO_DMA_SESSION))?
            .clone();
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        }
    } else if args.len() >= 4 {
        // TCP path: LO.SET key len <data>
        let data = args[3].as_slice().to_vec();
        DataSource::Tcp(data)
    } else {
        return Err(ValkeyError::WrongArity);
    };

    // Block client and dispatch to engine.
    let blocked_client = ctx.block_client();
    engine::execute_set(key_name, obj_len, data_source, blocked_client);

    Ok(ValkeyValue::NoReply)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
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
