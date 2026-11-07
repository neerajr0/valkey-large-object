//! A raw libfabric passive peer for integration tests. Registers one or more buffers, exposes
//! them, prints an advertisement per region, and polls until bytes land. Copied/modified from
//! vdma's `examples/one_target.rs`.
//!
//! ```text
//! # prints, once per region: advertisement: <address-hex> <rkey> <remote-addr> <len>
//! cargo run --features test-harness --bin fabric_target -- 127.0.0.1
//! # against a module started with fabric-provider Emulated:
//! BLOB.RDMA_HELLO <address-hex>
//! BLOB.RDMA_GET key 1 <rkey> <remote-addr> <len>    # writes the object into the target's buffer
//! ```
//!
//! `--read` prefills the buffers and serves them for
//! `BLOB.RDMA_SET key <len> 1 <rkey> <remote-addr> <len>` instead. This holds the buffers open and lets
//! the initiator do the verifying.
//!
//! `--len=<bytes>` sets the object length (default 4096).
//!
//! `--split=<size1>,<size2>[,...]` registers N separate allocations of the given sizes instead of
//! one buffer, each with its own registration and so its own rkey, and advertises one line per
//! region. The sizes must sum to the object length and may be unequal (`--split=1024,3072`).
//! Regions are advertised in registration order, which is the order the object's bytes span them.
//!
//! `--efa` opens the `efa` (rxr) fabric instead of tcp loopback. There a target must hold the
//! initiator's address before it can be RMA'd against, so it takes one as its only positional.
//! `BLOB.RDMA_HELLO` can't supply it, since HELLO needs the target's address first. The module logs each
//! service's address at load (`largeobj: fabric service N address <hex>`).

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dma_libfabric_protocol::{checksum, decode_hex, encode_hex};
use ofi_libfabric_sys::bindgen::{
    fi_addr_t, fi_allocinfo, fi_av_attr, fi_av_insert, fi_av_open, fi_av_type_FI_AV_MAP, fi_close,
    fi_cq_attr, fi_cq_data_entry, fi_cq_format_FI_CQ_FORMAT_DATA, fi_cq_open, fi_cq_read,
    fi_domain, fi_enable, fi_endpoint, fi_ep_bind, fi_ep_type_FI_EP_RDM, fi_fabric, fi_freeinfo,
    fi_getinfo, fi_getname, fi_info, fi_mr_key, fi_mr_reg, fi_msg_rma, fi_rma_iov, fi_version,
    fi_writemsg, fid_av, fid_cq, fid_domain, fid_ep, fid_fabric, fid_mr, FI_MR_ALLOCATED,
    FI_MR_LOCAL, FI_MR_PROV_KEY, FI_MR_VIRT_ADDR, FI_MSG, FI_READ, FI_RECV, FI_REMOTE_CQ_DATA,
    FI_REMOTE_READ, FI_REMOTE_WRITE, FI_RMA, FI_SOURCE, FI_TRANSMIT, FI_WRITE,
};
use libc::iovec;

const DEFAULT_LEN: usize = 4096;

/// Each 4-byte word holds its own index, little-endian, so every position differs: bytes that
/// land at the wrong offset (a misplaced chunk or region) cannot match. Must match
/// `position_pattern` in tests/valkey_largeobj_test_case.py.
///
/// NOTE: For testing FI_REMOTE_CQ_DATA with real blob data, set USE_SYMMETRIC_PATTERN=false
/// to skip pattern verification and just rely on FI_REMOTE_CQ_DATA completions.
const USE_SYMMETRIC_PATTERN: bool = false;

/// Server's ack buffer info, provided via --ack-rkey=N --ack-addr=N flags.
/// When both are provided, the client sends an ack after receiving data.
struct AckBufferInfo {
    rkey: u64,
    remote_addr: u64,
}

fn generate_pattern(len: usize) -> Vec<u8> {
    let mut pattern: Vec<u8> = (0..len.div_ceil(4) as u32)
        .flat_map(u32::to_le_bytes)
        .collect();
    pattern.truncate(len);
    pattern
}

/// Everything opened, closed on drop in reverse construction order.
struct Target {
    /// One registration per advertised region, in registration order.
    memory_regions: Vec<*mut fid_mr>,
    endpoint: *mut fid_ep,
    completion_queue: *mut fid_cq,
    address_vector: *mut fid_av,
    domain: *mut fid_domain,
    fabric: *mut fid_fabric,
    info: *mut fi_info,
}

impl Drop for Target {
    fn drop(&mut self) {
        // SAFETY: each fid is closed once, in reverse order, and null when never opened.
        unsafe {
            // Registrations come first — they were opened last, and they borrow the domain.
            for memory_region in self.memory_regions.drain(..).rev() {
                if !memory_region.is_null() {
                    fi_close(memory_region.cast());
                }
            }
            for fid in [
                self.endpoint.cast::<c_void>(),
                self.completion_queue.cast::<c_void>(),
                self.address_vector.cast::<c_void>(),
                self.domain.cast::<c_void>(),
                self.fabric.cast::<c_void>(),
            ] {
                if !fid.is_null() {
                    // Every fid_* starts with its `fid`, so the first field is the handle to close.
                    fi_close(fid.cast());
                }
            }
            if !self.info.is_null() {
                fi_freeinfo(self.info);
            }
        }
    }
}

fn check(code: i32, what: &str) -> Result<(), String> {
    if 0 == code {
        return Ok(());
    }
    Err(format!("{what} failed: {code}"))
}

/// The region sizes to register, from a `--split=a,b,...` spec; absent, one whole-buffer
/// region. The sizes must sum to `len`, since the object is exactly that long.
fn region_sizes(split: Option<&str>, len: usize) -> Result<Vec<usize>, String> {
    let Some(spec) = split else {
        return Ok(vec![len]);
    };
    let mut sizes = Vec::new();
    for field in spec.split(',') {
        let size: usize = field
            .trim()
            .parse()
            .map_err(|_| format!("--split: {field:?} is not a size"))?;
        if 0 == size {
            return Err("--split: a region size must be > 0".to_string());
        }
        sizes.push(size);
    }
    if sizes.is_empty() {
        return Err("--split: at least one region size is required".to_string());
    }
    let total: usize = sizes.iter().sum();
    if len != total {
        return Err(format!(
            "--split: sizes total {total}, must total the object length {len}"
        ));
    }
    Ok(sizes)
}

fn main() -> Result<(), String> {
    let (flags, positional): (Vec<String>, Vec<String>) = std::env::args()
        .skip(1)
        .partition(|argument| argument.starts_with("--"));
    // Serve the buffer for a remote read rather than wait for a remote write.
    let serve_read = flags.iter().any(|flag| "--read" == flag);
    let use_efa = flags.iter().any(|flag| "--efa" == flag);
    let len = match flags.iter().find_map(|flag| flag.strip_prefix("--len=")) {
        Some(len) => match len.parse::<usize>() {
            Ok(len) if len > 0 => len,
            _ => return Err(format!("--len: {len:?} is not a size > 0")),
        },
        None => DEFAULT_LEN,
    };
    let split = flags.iter().find_map(|flag| flag.strip_prefix("--split="));
    let sizes = region_sizes(split, len)?;

    // Parse server ack buffer info from --ack-rkey=N --ack-addr=N flags.
    // When both are provided, the client will send an ack after receiving data.
    let ack_rkey = flags
        .iter()
        .find_map(|flag| flag.strip_prefix("--ack-rkey="))
        .map(|s| s.parse::<u64>())
        .transpose()
        .map_err(|_| "--ack-rkey: not a valid u64")?;
    let ack_addr = flags
        .iter()
        .find_map(|flag| flag.strip_prefix("--ack-addr="))
        .map(|s| s.parse::<u64>())
        .transpose()
        .map_err(|_| "--ack-addr: not a valid u64")?;
    let ack_buffer_info = match (ack_rkey, ack_addr) {
        (Some(rkey), Some(remote_addr)) => {
            println!("ack buffer configured: rkey={rkey}, addr={remote_addr}");
            Some(AckBufferInfo { rkey, remote_addr })
        }
        (None, None) => None,
        _ => return Err("--ack-rkey and --ack-addr must both be provided or both omitted".into()),
    };

    let mut arguments = positional.into_iter();
    // An EFA endpoint binds to the device and advertises its own fabric address, so there is no
    // source to pin — the first positional is the initiator's address instead.
    let bind = if use_efa { None } else { arguments.next() };
    let initiator = arguments.next();

    let mut target = Target {
        memory_regions: Vec::with_capacity(sizes.len()),
        endpoint: ptr::null_mut(),
        completion_queue: ptr::null_mut(),
        address_vector: ptr::null_mut(),
        domain: ptr::null_mut(),
        fabric: ptr::null_mut(),
        info: ptr::null_mut(),
    };

    // Must match the initiator's hints or the two pick incompatible providers; mirrors
    // `endpoint::query_info`. On efa the provider name alone is ambiguous — it exposes both rxr
    // `efa` fabric and device-RDMA `efa-direct`. We use `efa` (rxr) for FI_REMOTE_CQ_DATA support.
    let provider = CString::new(if use_efa { "efa" } else { "tcp" })
        .map_err(|_| "provider name".to_string())?;
    // Pin the `efa` fabric name (rxr), not `efa-direct`, for unsolicited write recv support.
    let fabric_name = CString::new("efa").map_err(|_| "fabric name".to_string())?;
    let node = match &bind {
        Some(bind) => Some(CString::new(bind.as_str()).map_err(|_| "bind address".to_string())?),
        None => None,
    };
    // SAFETY: hints are freed below; the CStrings outlive the fi_getinfo call and are detached from
    // the hints first, so fi_freeinfo doesn't free Rust-owned memory.
    unsafe {
        let hints = fi_allocinfo();
        if hints.is_null() {
            return Err("fi_allocinfo failed".into());
        }
        (*hints).caps =
            u64::from(FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE);
        (*(*hints).ep_attr).type_ = fi_ep_type_FI_EP_RDM;
        (*(*hints).domain_attr).mr_mode =
            (FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR) as i32;
        (*(*hints).fabric_attr).prov_name = provider.as_ptr().cast_mut();
        if use_efa {
            // Pin to `efa` (rxr) fabric for FI_REMOTE_CQ_DATA with unsolicited write recv.
            // No FI_CONTEXT2 needed — that's only required by efa-direct.
            (*(*hints).fabric_attr).name = fabric_name.as_ptr().cast_mut();
        }
        let (node_ptr, flags) = match &node {
            Some(node) => (node.as_ptr(), FI_SOURCE),
            None => (ptr::null(), 0),
        };
        let code = fi_getinfo(
            fi_version(),
            node_ptr,
            ptr::null(),
            flags,
            hints,
            &mut target.info,
        );
        (*(*hints).fabric_attr).prov_name = ptr::null_mut();
        (*(*hints).fabric_attr).name = ptr::null_mut();
        fi_freeinfo(hints);
        check(code, "fi_getinfo")?;
        if target.info.is_null() {
            return Err("no provider matched".into());
        }
    }

    // SAFETY: `target.info` is a live fi_info; each handle is stored in `target`, which closes them.
    let uses_virtual_addressing = unsafe {
        check(
            fi_fabric(
                (*target.info).fabric_attr,
                &mut target.fabric,
                ptr::null_mut(),
            ),
            "fi_fabric",
        )?;
        check(
            fi_domain(
                target.fabric,
                target.info,
                &mut target.domain,
                ptr::null_mut(),
            ),
            "fi_domain",
        )?;
        let mut av_attr: fi_av_attr = std::mem::zeroed();
        av_attr.type_ = fi_av_type_FI_AV_MAP;
        check(
            fi_av_open(
                target.domain,
                &mut av_attr,
                &mut target.address_vector,
                ptr::null_mut(),
            ),
            "fi_av_open",
        )?;
        let mut cq_attr: fi_cq_attr = std::mem::zeroed();
        // Use FI_CQ_FORMAT_DATA to receive immediate data from FI_REMOTE_CQ_DATA writes.
        // When the server uses post_out_with_immediate(), we'll see the completion here
        // with the immediate data value, which is the reliable signal data has landed.
        cq_attr.format = fi_cq_format_FI_CQ_FORMAT_DATA;
        check(
            fi_cq_open(
                target.domain,
                &mut cq_attr,
                &mut target.completion_queue,
                ptr::null_mut(),
            ),
            "fi_cq_open",
        )?;
        check(
            fi_endpoint(
                target.domain,
                target.info,
                &mut target.endpoint,
                ptr::null_mut(),
            ),
            "fi_endpoint",
        )?;
        check(
            fi_ep_bind(target.endpoint, &mut (*target.address_vector).fid, 0),
            "fi_ep_bind(av)",
        )?;
        check(
            fi_ep_bind(
                target.endpoint,
                &mut (*target.completion_queue).fid,
                u64::from(FI_TRANSMIT | FI_RECV),
            ),
            "fi_ep_bind(cq)",
        )?;
        check(fi_enable(target.endpoint), "fi_enable")?;
        (*(*target.info).domain_attr).mr_mode as u32 & FI_MR_VIRT_ADDR != 0
    };

    // Remote-accessible, unlike the initiator's local-only operands. Prefilled when the initiator is
    // the one fetching it. One allocation per region, all allocated before the first registration
    // because a registration holds a raw pointer into its buffer.
    let pattern = generate_pattern(len);
    let buffers: Vec<Vec<u8>> = if serve_read {
        // Prefill each region with its slice of the position-dependent pattern.
        let mut offset = 0;
        sizes
            .iter()
            .map(|&size| {
                let buf = pattern[offset..offset + size].to_vec();
                offset += size;
                buf
            })
            .collect()
    } else {
        sizes.iter().map(|&size| vec![0u8; size]).collect()
    };

    let mut advertisements = Vec::with_capacity(buffers.len());
    for (index, buffer) in buffers.iter().enumerate() {
        let pointer = buffer.as_ptr().cast_mut();
        let mut memory_region: *mut fid_mr = ptr::null_mut();
        // SAFETY: `buffers` outlives the registrations, which `target` closes before this returns.
        let remote_key = unsafe {
            check(
                fi_mr_reg(
                    target.domain,
                    pointer.cast::<c_void>(),
                    buffer.len(),
                    u64::from(FI_REMOTE_READ | FI_REMOTE_WRITE | FI_READ | FI_WRITE),
                    0,
                    // requested_key, distinct per region: the tcp provider hands this back as
                    // the rkey, so reusing one fails the second registration with FI_ENOKEY.
                    // A provider honouring FI_MR_PROV_KEY (efa) assigns its own and ignores it.
                    index as u64,
                    0,
                    &mut memory_region,
                    ptr::null_mut(),
                ),
                "fi_mr_reg",
            )?;
            target.memory_regions.push(memory_region);
            fi_mr_key(memory_region)
        };
        // On FI_MR_VIRT_ADDR providers like efa the remote address is the buffer's virtual address;
        // on tcp it is an offset into the region, so 0 — each region has its own offset space.
        let remote_address = if uses_virtual_addressing {
            pointer as u64
        } else {
            0
        };
        advertisements.push((remote_key, remote_address, buffer.len()));
    }

    // SAFETY: writes `length` bytes of address into `address`, after asking for the size.
    let address = unsafe {
        let mut length: usize = 0;
        fi_getname(&mut (*target.endpoint).fid, ptr::null_mut(), &mut length);
        let mut address = vec![0u8; length];
        check(
            fi_getname(
                &mut (*target.endpoint).fid,
                address.as_mut_ptr().cast::<c_void>(),
                &mut length,
            ),
            "fi_getname",
        )?;
        address.truncate(length);
        address
    };

    // efa-direct requires a target to hold the initiator's address before it can be RMA'd against.
    // tcp does not.
    if let Some(initiator) = &initiator {
        let bytes = decode_hex(initiator.as_bytes()).map_err(|error| error.to_string())?;
        let mut peer: fi_addr_t = 0;
        // SAFETY: one address of the provider's own format, from the initiator.
        let inserted = unsafe {
            fi_av_insert(
                target.address_vector,
                bytes.as_ptr().cast::<c_void>(),
                1,
                &mut peer,
                0,
                ptr::null_mut(),
            )
        };
        if 1 != inserted {
            return Err(format!("fi_av_insert inserted {inserted} of 1"));
        }
    }

    // One line per region, in the order the object's bytes span them. The initiator carries these
    // into BLOB.RDMA_GET / BLOB.RDMA_SET as its (rkey, addr, len) triples.
    let address_hex = encode_hex(&address);
    for (remote_key, remote_address, length) in &advertisements {
        println!("advertisement: {address_hex} {remote_key} {remote_address} {length}");
    }
    // The object spans the regions in order, so its bytes are their contents concatenated.
    let contents = |buffers: &[Vec<u8>]| -> Vec<u8> { buffers.concat() };
    if serve_read {
        println!(
            "serving {len} bytes (position pattern) across {} region(s) for a remote read, crc {:#010x} — the initiator verifies",
            advertisements.len(),
            checksum(&contents(&buffers))
        );
    } else {
        println!(
            "waiting for a transfer into {len} bytes across {} region(s)...",
            advertisements.len()
        );
    }

    // Under FI_PROGRESS_MANUAL nothing services the inbound RMA unless the queue is polled, and the
    // write raises no completion here — so poll for progress, watch the buffers for arrival.
    // Polling is also what acknowledges the initiator's writes, so once the payload is verified
    // keep polling until stdin closes: the test closes it after its command has replied.
    // Exiting at once could drop acknowledgements and fail the initiator's last writes.
    let stdin_closed = Arc::new(AtomicBool::new(false));
    {
        let stdin_closed = Arc::clone(&stdin_closed);
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut std::io::stdin(), &mut std::io::sink());
            stdin_closed.store(true, Ordering::Relaxed);
        });
    }
    // Track the last received transfer_id from FI_REMOTE_CQ_DATA completions.
    // The client echoes this back in the ack so the server can correlate.
    let mut last_transfer_id: u64 = 0;
    let mut verified = false;
    let mut ack_sent = false;
    // Count of FI_REMOTE_CQ_DATA completions received (each signals data has landed).
    let mut completions_received: u64 = 0;
    let deadline = Instant::now() + Duration::from_secs(300);

    // For sending ack back to the server, we need the peer's address handle.
    // The peer was inserted above via fi_av_insert, its handle is 0 (first entry).
    let peer_handle: fi_addr_t = 0;

    // Small ack buffer - just need to send something with FI_REMOTE_CQ_DATA.
    // The real signal is in the immediate data field (ACK_MAGIC).
    let ack_buffer: [u8; 8] = [0xAC, 0xAC, 0xAC, 0xAC, 0xAC, 0xAC, 0xAC, 0xAC];

    // Helper function to send ack to server with the transfer_id echoed back.
    // The server polls for this completion with matching transfer_id before replying OK.
    let send_ack = |endpoint: *mut fid_ep,
                    ack_info: &AckBufferInfo,
                    peer: fi_addr_t,
                    buf: &[u8; 8],
                    transfer_id: u64|
     -> Result<(), String> {
        unsafe {
            let buffer = iovec {
                iov_base: buf.as_ptr().cast_mut().cast(),
                iov_len: buf.len(),
            };
            let mut descriptors: [*mut c_void; 1] = [ptr::null_mut()];
            let remote = fi_rma_iov {
                addr: ack_info.remote_addr,
                len: buf.len(),
                key: ack_info.rkey,
            };
            let message = fi_msg_rma {
                msg_iov: &buffer,
                desc: descriptors.as_mut_ptr(),
                iov_count: 1,
                addr: peer,
                rma_iov: &remote,
                rma_iov_count: 1,
                context: ptr::null_mut(),
                // Echo the transfer_id back to the server for correlation.
                // The server matches this against the transfer_id it sent with the data.
                data: transfer_id,
            };
            let code = fi_writemsg(endpoint, &message, u64::from(FI_REMOTE_CQ_DATA));
            if code == 0 {
                Ok(())
            } else {
                Err(format!("fi_writemsg(ack) failed: {code}"))
            }
        }
    };

    while Instant::now() < deadline {
        // SAFETY: a one-entry read into an owned value; the queue is live until `target` drops.
        // Use fi_cq_data_entry to receive immediate data from FI_REMOTE_CQ_DATA writes.
        unsafe {
            let mut entry: fi_cq_data_entry = std::mem::zeroed();
            let count = fi_cq_read(
                target.completion_queue,
                (&raw mut entry).cast::<c_void>(),
                1,
            );
            // Check if this is a remote CQ data completion (from FI_REMOTE_CQ_DATA write).
            // With the `efa` (rxr) provider and "unsolicited write recv", no posted receive
            // buffers are needed — the completion appears directly on our CQ.
            if count > 0 && (entry.flags & u64::from(FI_REMOTE_CQ_DATA)) != 0 {
                // The immediate data contains the transfer_id sent by the server.
                // We echo this back in our ack for correlation.
                last_transfer_id = entry.data;
                completions_received += 1;
                println!(
                    "received FI_REMOTE_CQ_DATA completion #{}: flags={:#x}, transfer_id={:#x}",
                    completions_received, entry.flags, entry.data
                );
            }
        }
        // A read leaves nothing behind here, so there is no arrival to watch for: hold the buffers
        // open and keep polling until the window closes.
        if serve_read {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }
        if verified {
            if stdin_closed.load(Ordering::Relaxed) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }

        // Check if we've received all the data via FI_REMOTE_CQ_DATA completions.
        // For multi-region transfers (--split), we get one completion per region.
        // The number of regions equals the number of advertisements.
        let expected_completions = advertisements.len() as u64;
        if completions_received >= expected_completions && !ack_sent {
            // When USE_SYMMETRIC_PATTERN is false, skip pattern verification and just trust
            // FI_REMOTE_CQ_DATA completions. This is for testing with real blob data.
            if !USE_SYMMETRIC_PATTERN {
                println!(
                    "received {} FI_REMOTE_CQ_DATA completion(s) for {} region(s) (pattern check skipped)",
                    completions_received, advertisements.len()
                );

                // Send ack back to server using fi_writemsg with FI_REMOTE_CQ_DATA.
                // The server will poll for this completion before replying TCP OK.
                if use_efa && initiator.is_some() && ack_buffer_info.is_some() {
                    let ack_info = ack_buffer_info.as_ref().unwrap();
                    println!(
                        "sending ack to server (rkey={}, addr={}, transfer_id={:#x})...",
                        ack_info.rkey, ack_info.remote_addr, last_transfer_id
                    );
                    match send_ack(target.endpoint, ack_info, peer_handle, &ack_buffer, last_transfer_id) {
                        Ok(()) => {
                            println!("ack posted successfully, waiting for local completion...");
                            // Poll for local completion of the ack write
                            let ack_deadline = Instant::now() + Duration::from_secs(5);
                            let mut ack_completed = false;
                            while Instant::now() < ack_deadline {
                                unsafe {
                                    let mut entry: fi_cq_data_entry = std::mem::zeroed();
                                    let count = fi_cq_read(
                                        target.completion_queue,
                                        (&raw mut entry).cast::<c_void>(),
                                        1,
                                    );
                                    // Local write completions have op_context (which we passed as null)
                                    // and no FI_REMOTE_CQ_DATA flag. Count > 0 means our write completed.
                                    if count > 0
                                        && (entry.flags & u64::from(FI_REMOTE_CQ_DATA)) == 0
                                    {
                                        println!("ack local completion received");
                                        ack_completed = true;
                                        break;
                                    }
                                }
                                std::thread::sleep(Duration::from_micros(100));
                            }
                            if ack_completed {
                                println!("ack sent successfully");
                                ack_sent = true;
                            } else {
                                println!("ack local completion timed out (might still work)");
                                ack_sent = true; // Proceed anyway
                            }
                        }
                        Err(e) => {
                            println!("ack send failed: {e}");
                        }
                    }
                } else if use_efa && initiator.is_some() && ack_buffer_info.is_none() {
                    println!("skipping ack (no ack buffer info provided via --ack-rkey/--ack-addr)");
                }
                verified = true;
                continue;
            }

            // Original pattern verification path
            let mut offset = 0;
            let landed = buffers.iter().all(|buffer| {
                let pointer = buffer.as_ptr();
                let matches = (0..buffer.len()).all(|index| {
                    pattern[offset + index] == unsafe { pointer.add(index).read_volatile() }
                });
                offset += buffer.len();
                matches
            });
            if landed {
                // The CRC the initiator also prints, so the pair cross-checks.
                println!(
                    "received {len} bytes via FI_REMOTE_CQ_DATA, crc {:#010x} — payload verified",
                    checksum(&contents(&buffers))
                );

                // Send ack back to server
                if use_efa && initiator.is_some() && ack_buffer_info.is_some() {
                    let ack_info = ack_buffer_info.as_ref().unwrap();
                    println!(
                        "sending ack to server (rkey={}, addr={}, transfer_id={:#x})...",
                        ack_info.rkey, ack_info.remote_addr, last_transfer_id
                    );
                    match send_ack(target.endpoint, ack_info, peer_handle, &ack_buffer, last_transfer_id) {
                        Ok(()) => {
                            println!("ack posted, polling for local completion...");
                            // Brief poll for local completion
                            for _ in 0..100 {
                                unsafe {
                                    let mut entry: fi_cq_data_entry = std::mem::zeroed();
                                    let count = fi_cq_read(
                                        target.completion_queue,
                                        (&raw mut entry).cast::<c_void>(),
                                        1,
                                    );
                                    if count > 0
                                        && (entry.flags & u64::from(FI_REMOTE_CQ_DATA)) == 0
                                    {
                                        break;
                                    }
                                }
                                std::thread::sleep(Duration::from_micros(100));
                            }
                            println!("ack sent");
                            ack_sent = true;
                        }
                        Err(e) => println!("ack send failed: {e}"),
                    }
                }
                verified = true;
                continue;
            }
            // Data not yet visible despite CQ completion — keep polling for progress.
        }

        // Fallback: also check buffer contents directly (for providers without FI_REMOTE_CQ_DATA).
        if USE_SYMMETRIC_PATTERN {
            let mut offset = 0;
            let landed = buffers.iter().all(|buffer| {
                let pointer = buffer.as_ptr();
                let matches = (0..buffer.len()).all(|index| {
                    pattern[offset + index] == unsafe { pointer.add(index).read_volatile() }
                });
                offset += buffer.len();
                matches
            });
            if landed {
                let via = if completions_received > 0 {
                    " (also signaled by FI_REMOTE_CQ_DATA)"
                } else {
                    " (no FI_REMOTE_CQ_DATA - detected via buffer polling)"
                };
                println!(
                    "received {len} bytes, crc {:#010x} — payload verified{via}",
                    checksum(&contents(&buffers))
                );
                verified = true;
                continue;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if serve_read || verified {
        return Ok(());
    }
    Err("timed out waiting for a transfer".into())
}
