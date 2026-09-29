//! Command Handlers — thin layer that parses args and dispatches to engine.
//!
//! LO.HELLO: EFA session establishment
//! LO.GET key [rkey addr]: legacy single-region EFA
//! LO.GET key [n_regions rkey1 addr1 len1 ...]: multi-region EFA
//! LO.SET key <data>                                     (TCP): engine::execute_set
//! LO.SET key total_len rkey addr                        (EFA legacy): engine::execute_set
//! LO.SET key total_len n_regions rkey1 addr1 len1 ...   (EFA): engine::execute_set
//! LO.INFO key [LEN|CRC|TIER]: metadata from LoValue, no engine call

use std::sync::Arc;

use dma_libfabric_protocol::{decode_hex, encode_hex};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, LO_TYPE};
use crate::engine::{self, DataSource, Transport};
use crate::errors;
use crate::storage::ClientEFAAddress;
use crate::transport::config::FabricProvider;
use crate::transport::{self, session, Session};

/// Upper bound on client memory regions in one EFA command. Checked before deriving
/// the required arg count, which keeps that arithmetic from overflowing.
const MAX_EFA_REGIONS: usize = 256;

/// The EFA session the client previously established with LO.HELLO.
fn efa_session(ctx: &Context) -> Result<Arc<Session>, ValkeyError> {
    session::lookup(ctx.get_client_id()).ok_or(ValkeyError::Str(errors::ERR_NO_DMA_SESSION))
}

/// Parse the client's memory regions from an EFA command's tail: `args[start_idx]` is
/// `n_regions`, followed by exactly that many `(rkey, addr, len)` triples.
fn parse_efa_regions(
    args: &[ValkeyString],
    start_idx: usize,
    required_len: u64,
) -> Result<Vec<ClientEFAAddress>, ValkeyError> {
    let fields: Vec<String> = args[start_idx..]
        .iter()
        .map(|arg| arg.to_string_lossy())
        .collect();
    parse_region_fields(&fields, required_len)
}

/// `fields[0]` is `n_regions`, the rest are its triples. Regions may cover more than
/// `required_len`; only a shortfall is an error. Takes stringified fields so the
/// parsing contract is testable without a server.
fn parse_region_fields<S: AsRef<str>>(
    fields: &[S],
    required_len: u64,
) -> Result<Vec<ClientEFAAddress>, ValkeyError> {
    let n_regions: usize = fields
        .first()
        .ok_or(ValkeyError::Str(errors::ERR_INVALID_NUM_REGIONS))?
        .as_ref()
        .parse()
        .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_NUM_REGIONS))?;
    if 0 == n_regions || n_regions > MAX_EFA_REGIONS {
        return Err(ValkeyError::Str(errors::ERR_INVALID_NUM_REGIONS));
    }
    let needed = 1 + n_regions * 3;
    if fields.len() < needed {
        return Err(ValkeyError::Str(errors::ERR_INSUFFICIENT_REGION_ARGS));
    }
    // Ignoring trailing args would transfer against a region set the client did not send.
    if fields.len() > needed {
        return Err(ValkeyError::WrongArity);
    }
    let mut addrs = Vec::with_capacity(n_regions);
    let mut total_addr_len: u64 = 0;
    for i in 0..n_regions {
        let base = 1 + i * 3;
        let rkey: u64 = fields[base]
            .as_ref()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let addr: u64 = fields[base + 1]
            .as_ref()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let len: u64 = fields[base + 2]
            .as_ref()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REGION_LEN))?;
        // A zero-length region can never absorb bytes; ChunkIterator would skip past it.
        if 0 == len {
            return Err(ValkeyError::Str(errors::ERR_INVALID_REGION_LEN));
        }
        addrs.push((addr, len as usize, rkey));
        total_addr_len = total_addr_len.saturating_add(len);
    }
    if total_addr_len < required_len {
        return Err(ValkeyError::Str(errors::ERR_INSUFFICIENT_ADDR_SPACE));
    }
    Ok(addrs)
}

// ─── LO.HELLO ────────────────────────────────────────────────────────────────
//
// Establishes a fabric session with the client.
// Client sends its fabric address as hex, opaque to us and in the provider's own format. The
// server inserts it into each domain's address vector and returns an address per server,
// so both sides hold each other before the first transfer.

pub fn lo_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let Some(fabric) = transport::fabric() else {
        return Err(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE));
    };

    let peer_address = decode_hex(args[1].as_slice())
        .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_PEER_ADDR_HEX))?;
    // An EFA address is exactly 32 bytes; a tcp one is a sockaddr, opaque beyond being non-empty.
    match crate::fabric_provider() {
        FabricProvider::EfaDirect if peer_address.len() != 32 => {
            return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_LEN));
        }
        FabricProvider::Emulated if peer_address.is_empty() => {
            return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_EMPTY));
        }
        FabricProvider::EfaDirect | FabricProvider::Emulated => {}
    }

    let client_id = ctx.get_client_id();
    // One endpoint per connection. A second HELLO would hold the old address-vector entry while
    // inserting the new one; when the client's old endpoint has died and the new one reuses its
    // QPN, efa-direct cannot represent both (vdma/.claude/open_issue.md). Reconnect instead.
    if session::lookup(client_id).is_some() {
        return Err(ValkeyError::Str(errors::ERR_DMA_SESSION_EXISTS));
    }
    fabric
        .add_peer(client_id, &peer_address)
        .map_err(|e| ValkeyError::String(format!("{}: {}", errors::ERR_SESSION_CREATE, e)))?;
    session::insert(client_id, Session::new(client_id, peer_address));

    let reply: Vec<ValkeyValue> = fabric
        .local_addresses()
        .map(|address| ValkeyValue::BulkString(encode_hex(address)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── LO.GET ──────────────────────────────────────────────────────────────────
//
// Parse args → resolve key → determine transport → dispatch to engine.

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Strict arity, decided before the key lookup so a malformed EFA call cannot be
    // answered as if it were a TCP GET (or as a missing key).
    //   2 args: TCP          — LO.GET key
    //   4 args: legacy EFA   — LO.GET key rkey addr           (single region, no per-region len)
    //  ≥5 args: multi-region — LO.GET key n_regions rkey1 addr1 len1 ...
    let efa = match args.len() {
        2 => false,
        4 => true,
        len if len >= 5 => true,
        _ => return Err(ValkeyError::WrongArity),
    };

    // Lookup LoValue in keyspace.
    let key = ctx.open_key(&args[1]);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;
    let crc32c = lo_value.crc32c;

    // Pin the file to protect it from asynchronous deletion in tiered mode.
    let file = lo_value.file.clone();

    // EFA syntax (two forms):
    //   Legacy:       LO.GET key <rkey> <addr>                         (4 args)
    //   Multi-region: LO.GET key <n_regions> <rkey1> <addr1> <len1> ...  (≥5 args)
    // Legacy has no per-region len, so obj_len is used as the region length — the
    // server trusts the client registered at least that many bytes.
    let transport = if efa {
        let addrs = if args.len() == 4 {
            let fields = [
                "1".to_string(),
                args[2].to_string_lossy(),
                args[3].to_string_lossy(),
                obj_len.to_string(),
            ];
            parse_region_fields(&fields, obj_len)?
        } else {
            parse_efa_regions(&args, 2, obj_len)?
        };
        let session = efa_session(ctx)?;
        Transport::Efa { session, addrs }
    } else {
        Transport::Tcp
    };

    // Dispatch to engine — it decides sync vs async internally.
    match engine::execute_get(ctx, object_id, obj_len, crc32c, file, transport) {
        engine::EngineResult::Sync(result) => result,
        engine::EngineResult::Async => Ok(ValkeyValue::NoReply),
    }
}

// ─── LO.SET ──────────────────────────────────────────────────────────────────
//
// Parse args → determine data source → dispatch to engine.

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Strict arity.
    //   3 args: TCP          — LO.SET key <data>
    //   5 args: legacy EFA   — LO.SET key total_len rkey addr          (single region, no per-region len)
    //  ≥6 args: multi-region — LO.SET key total_len n_regions rkey1 addr1 len1 ...
    // 4 args is rejected: it is neither a valid TCP call (which is exactly 3) nor a
    // valid EFA call, so falling through to TCP would store the numeric `total_len`
    // as the object payload.
    let efa = match args.len() {
        3 => false,
        5 => true,
        len if len >= 6 => true,
        _ => return Err(ValkeyError::WrongArity),
    };

    // Determine data source and obj_len based on transport.
    let (obj_len, data_source) = if efa {
        // EFA path (two forms):
        //   Legacy:       LO.SET key total_len rkey addr                     (5 args)
        //   Multi-region: LO.SET key total_len n_regions rkey1 addr1 len1 ...  (≥6 args)
        // total_len is required in both — server needs to know how many bytes to fi_read
        // from the client.
        let obj_len: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;
        let addrs = if args.len() == 5 {
            // Legacy: no per-region len, so total_len doubles as the region length.
            let fields = [
                "1".to_string(),
                args[3].to_string_lossy(),
                args[4].to_string_lossy(),
                obj_len.to_string(),
            ];
            parse_region_fields(&fields, obj_len)?
        } else {
            parse_efa_regions(&args, 3, obj_len)?
        };
        let session = efa_session(ctx)?;
        (obj_len, DataSource::Efa { session, addrs })
    } else {
        // TCP path: LO.SET key <data>
        // data.len() IS the authoritative length. No user-provided len needed.
        let data = args[2].as_slice().to_vec();
        let obj_len = data.len() as u64;
        (obj_len, DataSource::Tcp(data))
    };

    // Dispatch to engine — it decides sync vs async internally.
    match engine::execute_set(ctx, &args[1], obj_len, data_source) {
        engine::EngineResult::Sync(result) => result,
        engine::EngineResult::Async => Ok(ValkeyValue::NoReply),
    }
}

// ─── LO.INFO ─────────────────────────────────────────────────────────────────
//
// LO.INFO key [LEN | CRC | TIER]
//
// Parse args → resolve key → return metadata field or all fields as array.

pub fn lo_info(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if !(2..=3).contains(&args.len()) {
        return Err(ValkeyError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    let value = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Err(ValkeyError::Str(errors::ERR_NOT_FOUND)),
    };

    let len = ValkeyValue::Integer(value.len as i64);
    let crc = ValkeyValue::Integer(i64::from(value.crc32c));
    let tier = ValkeyValue::SimpleStringStatic(value.tier().as_str());

    if args.len() == 2 {
        return Ok(ValkeyValue::Array(vec![
            ValkeyValue::SimpleStringStatic("len"),
            len,
            ValkeyValue::SimpleStringStatic("crc"),
            crc,
            ValkeyValue::SimpleStringStatic("tier"),
            tier,
        ]));
    }

    match args[2].to_string_lossy().to_uppercase().as_str() {
        "LEN" => Ok(len),
        "CRC" => Ok(crc),
        "TIER" => Ok(tier),
        _ => Err(ValkeyError::Str(errors::ERR_INVALID_INFO_FIELD)),
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// The error string a rejection carries, so cases can assert the client contract.
    fn reject(fields: &[&str], required_len: u64) -> String {
        match parse_region_fields(fields, required_len) {
            Ok(addrs) => panic!("expected rejection, parsed {addrs:?}"),
            Err(ValkeyError::Str(message)) => message.to_string(),
            Err(ValkeyError::WrongArity) => "WrongArity".to_string(),
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn regions_parse_in_client_order() {
        // ClientEFAAddress is (addr, len, rkey) — not the arg order, which is rkey first.
        assert_eq!(
            parse_region_fields(&["1", "999", "4096", "8192"], 8192).unwrap(),
            vec![(4096, 8192, 999)]
        );
        // Order is load-bearing: ChunkIterator consumes regions front to back, so the
        // object's first bytes land in the first region.
        assert_eq!(
            parse_region_fields(&["2", "11", "1000", "1024", "22", "9000", "3072"], 4096).unwrap(),
            vec![(1000, 1024, 11), (9000, 3072, 22)]
        );
        // A zero addr is a valid base: without FI_MR_VIRT_ADDR it is an offset into the
        // region, so it must not be read as unset.
        assert_eq!(
            parse_region_fields(&["1", "7", "0", "4096"], 4096).unwrap(),
            vec![(0, 4096, 7)]
        );
        let mut at_cap = vec![MAX_EFA_REGIONS.to_string()];
        for i in 0..MAX_EFA_REGIONS {
            at_cap.extend(["7".to_string(), (i * 4096).to_string(), "4096".to_string()]);
        }
        assert_eq!(
            parse_region_fields(&at_cap, 4096 * MAX_EFA_REGIONS as u64)
                .unwrap()
                .len(),
            MAX_EFA_REGIONS
        );
    }

    #[test]
    fn address_space_is_validated_against_the_object() {
        // Exact fit and surplus both pass; a client may advertise whole buffers and let
        // the reply's obj_len bound the meaningful bytes.
        assert!(parse_region_fields(&["1", "7", "64", "4096"], 4096).is_ok());
        assert_eq!(
            parse_region_fields(&["1", "7", "64", "1048576"], 4096).unwrap(),
            vec![(64, 1048576, 7)]
        );
        assert_eq!(
            reject(&["1", "7", "64", "4095"], 4096),
            errors::ERR_INSUFFICIENT_ADDR_SPACE
        );
        // Summed across regions, still one byte short.
        assert_eq!(
            reject(&["2", "7", "64", "2048", "8", "9000", "2047"], 4096),
            errors::ERR_INSUFFICIENT_ADDR_SPACE
        );
    }

    #[test]
    fn bad_region_counts_are_rejected() {
        assert_eq!(reject(&["0"], 4096), errors::ERR_INVALID_NUM_REGIONS);
        assert_eq!(reject(&["two"], 4096), errors::ERR_INVALID_NUM_REGIONS);
        assert_eq!(reject(&["-1"], 4096), errors::ERR_INVALID_NUM_REGIONS);
        assert_eq!(
            reject(&[] as &[&str], 4096),
            errors::ERR_INVALID_NUM_REGIONS
        );
        assert_eq!(
            reject(&[(MAX_EFA_REGIONS + 1).to_string().as_str()], 4096),
            errors::ERR_INVALID_NUM_REGIONS
        );
        // The cap is checked before `1 + n_regions * 3`, so a count this large cannot
        // wrap that into a small length and admit a huge allocation.
        assert_eq!(
            reject(&[usize::MAX.to_string().as_str()], 4096),
            errors::ERR_INVALID_NUM_REGIONS
        );
        assert_eq!(
            reject(&["99999999999999999999999"], 4096),
            errors::ERR_INVALID_NUM_REGIONS
        );
    }

    #[test]
    fn region_arg_count_must_match_the_declared_count() {
        // Two declared, one supplied.
        assert_eq!(
            reject(&["2", "7", "64", "4096"], 4096),
            errors::ERR_INSUFFICIENT_REGION_ARGS
        );
        // A truncated triple.
        assert_eq!(
            reject(&["1", "7", "64"], 4096),
            errors::ERR_INSUFFICIENT_REGION_ARGS
        );
        assert_eq!(
            reject(&["1", "7", "64", "4096", "extra"], 4096),
            "WrongArity"
        );
    }

    #[test]
    fn legacy_single_region_parses_via_synthesized_fields() {
        // Legacy syntax: the caller builds ["1", rkey, addr, obj_len] and feeds it to
        // parse_region_fields. This is the same path lo_get/lo_set take for 4-arg GET
        // and 5-arg SET.
        assert_eq!(
            parse_region_fields(&["1", "999", "4096", "8192"], 8192).unwrap(),
            vec![(4096, 8192, 999)]
        );
        // A zero addr is valid (offset-based addressing without FI_MR_VIRT_ADDR).
        assert_eq!(
            parse_region_fields(&["1", "7", "0", "4096"], 4096).unwrap(),
            vec![(0, 4096, 7)]
        );
        // Malformed rkey or addr in the synthesized fields is caught by the same
        // validation that handles multi-region triples.
        assert_eq!(
            reject(&["1", "notakey", "0", "4096"], 4096),
            errors::ERR_INVALID_RKEY
        );
        assert_eq!(
            reject(&["1", "7", "notanaddr", "4096"], 4096),
            errors::ERR_INVALID_REMOTE_ADDR
        );
    }

    #[test]
    fn arity_gaps_are_still_rejected() {
        // The arity gap that remains after legacy support:
        //   GET: 3 args (cmd + key + one_extra) is neither TCP (2) nor legacy (4) nor multi (>=5)
        //   SET: 4 args (cmd + key + len + one_extra) is neither TCP (3) nor legacy (5) nor multi (>=6)
        // We can't call lo_get/lo_set without a server, but the arity table is:
        //   GET: 2=TCP, 4=legacy, >=5=multi, else WrongArity
        //   SET: 3=TCP, 5=legacy, >=6=multi, else WrongArity
        // The multi-region parser still rejects trailing args past the declared count.
        assert_eq!(
            reject(&["1", "7", "64", "4096", "extra"], 4096),
            "WrongArity"
        );
    }

    #[test]
    fn malformed_triple_fields_are_rejected_by_field() {
        assert_eq!(
            reject(&["1", "notakey", "64", "4096"], 4096),
            errors::ERR_INVALID_RKEY
        );
        assert_eq!(
            reject(&["1", "7", "notanaddr", "4096"], 4096),
            errors::ERR_INVALID_REMOTE_ADDR
        );
        assert_eq!(
            reject(&["1", "7", "64", "notalen"], 4096),
            errors::ERR_INVALID_REGION_LEN
        );
        assert_eq!(
            reject(&["1", "7", "64", "0"], 4096),
            errors::ERR_INVALID_REGION_LEN
        );
        assert_eq!(
            reject(&["2", "7", "64", "0", "8", "9000", "4096"], 4096),
            errors::ERR_INVALID_REGION_LEN
        );
    }
}
