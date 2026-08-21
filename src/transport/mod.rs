//! Transport Layer — EFA/libfabric lifecycle + memory registration.
//!
//! Real fi_mr_reg/fi_mr_dereg via libfabric FFI.
//! On machines without EFA, gracefully falls back to unavailable mode.
//!
//! The module uses this for:
//! 1. Startup: register all buffer pool / arena memory with EFA
//! 2. Per-request (dynamic mode only): register/deregister per buffer
//! 3. DMA sessions: fi_av_insert, fi_write, fi_read

use std::sync::{Mutex, OnceLock};

use crate::storage::Buffer;

// ─── libfabric FFI bindings ──────────────────────────────────────────────────

/// Raw libfabric types and functions.
/// These are the minimal bindings needed for memory registration.
/// On EFA-enabled machines, link with -lfabric.
#[allow(non_camel_case_types, dead_code)]
mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    // Opaque handle types
    pub type fid_domain = c_void;
    pub type fid_fabric = c_void;
    pub type fid_mr = c_void;
    pub type fid_av = c_void;
    pub type fid_ep = c_void;
    pub type fid_cq = c_void;
    pub type fid_eq = c_void;

    pub type fi_info = c_void;

    // fi_mr_attr for fi_mr_regattr
    #[repr(C)]
    pub struct fi_mr_attr {
        pub mr_iov: *const iovec,
        pub iov_count: usize,
        pub access: u64,
        pub offset: u64,
        pub requested_key: u64,
        pub context: *mut c_void,
        pub auth_key_size: usize,
        pub auth_key: *const u8,
        pub iface: u32,       // fi_hmem_iface
        pub device: c_int,    // union { reserved, cuda, ze, neuron }
        pub hmem_data: *const c_void,
    }

    #[repr(C)]
    pub struct iovec {
        pub iov_base: *mut c_void,
        pub iov_len: usize,
    }

    // Access flags
    pub const FI_SEND: u64 = 1 << 0;
    pub const FI_RECV: u64 = 1 << 1;
    pub const FI_READ: u64 = 1 << 2;
    pub const FI_WRITE: u64 = 1 << 3;
    pub const FI_REMOTE_READ: u64 = 1 << 4;
    pub const FI_REMOTE_WRITE: u64 = 1 << 5;

    // Capabilities
    pub const FI_RMA: u64 = 1 << 8;
    pub const FI_MSG: u64 = 1 << 1;

    // Endpoint type
    pub const FI_EP_RDM: u32 = 1;

    // MR mode bits
    pub const FI_MR_LOCAL: u64 = 1 << 0;
    pub const FI_MR_VIRT_ADDR: u64 = 1 << 1;
    pub const FI_MR_ALLOCATED: u64 = 1 << 2;
    pub const FI_MR_PROV_KEY: u64 = 1 << 3;

    // Return codes
    pub const FI_SUCCESS: c_int = 0;

    // Real libfabric FFI — only linked when feature "efa" is enabled.
    // On EFA machines: cargo build --features efa (links -lfabric).
    #[cfg(feature = "efa")]
    #[link(name = "fabric")]
    extern "C" {
        // Discovery
        pub fn fi_getinfo(
            version: u32,
            node: *const c_char,
            service: *const c_char,
            flags: u64,
            hints: *const fi_info,
            info: *mut *mut fi_info,
        ) -> c_int;

        pub fn fi_freeinfo(info: *mut fi_info);

        // Fabric + Domain
        pub fn fi_fabric(
            attr: *mut c_void, // fi_fabric_attr*
            fabric: *mut *mut fid_fabric,
            context: *mut c_void,
        ) -> c_int;

        pub fn fi_domain(
            fabric: *mut fid_fabric,
            info: *mut fi_info,
            domain: *mut *mut fid_domain,
            context: *mut c_void,
        ) -> c_int;

        // Memory Registration
        pub fn fi_mr_reg(
            domain: *mut fid_domain,
            buf: *const c_void,
            len: usize,
            access: u64,
            offset: u64,
            requested_key: u64,
            flags: u64,
            mr: *mut *mut fid_mr,
            context: *mut c_void,
        ) -> c_int;

        pub fn fi_mr_desc(mr: *mut fid_mr) -> *mut c_void;
        pub fn fi_mr_key(mr: *mut fid_mr) -> u64;

        pub fn fi_close(fid: *mut c_void) -> c_int;

        // fi_version
        pub fn fi_version() -> u32;
    }

    /// fi_mr_dereg is fi_close on the MR's fid.
    #[cfg(feature = "efa")]
    pub unsafe fn fi_mr_dereg(mr: *mut fid_mr) -> c_int {
        fi_close(mr)
    }

    // Stub implementations when EFA feature is disabled (no libfabric linkage).
    #[cfg(not(feature = "efa"))]
    pub unsafe fn fi_mr_dereg(_mr: *mut fid_mr) -> c_int { 0 }
}

// ─── EFA Types ───────────────────────────────────────────────────────────────

/// EFA endpoint address — 32 bytes, opaque to callers.
#[derive(Clone)]
pub struct EfaAddress(pub [u8; 32]);

/// Client-side memory region descriptor.
#[derive(Debug, Clone)]
pub struct ClientRegion {
    pub rkey: u64,
    pub remote_addr: u64,
    pub len: u64,
}

// ─── Error Types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TransportError {
    DeviceNotFound,
    RegistrationFailed,
    SessionCreateFailed,
    WriteFailed { code: i32 },
    ReadFailed { code: i32 },
    RegionOutOfBounds,
    Timeout,
    SessionClosed,
    Unavailable,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeviceNotFound => write!(f, "EFA device not found"),
            Self::RegistrationFailed => write!(f, "fi_mr_reg failed"),
            Self::SessionCreateFailed => write!(f, "session create failed"),
            Self::WriteFailed { code } => write!(f, "fi_write failed ({})", code),
            Self::ReadFailed { code } => write!(f, "fi_read failed ({})", code),
            Self::RegionOutOfBounds => write!(f, "region index out of bounds"),
            Self::Timeout => write!(f, "CQ poll timeout"),
            Self::SessionClosed => write!(f, "session closed"),
            Self::Unavailable => write!(f, "EFA unavailable"),
        }
    }
}

// ─── Memory Registration Handle ─────────────────────────────────────────────

/// A registered memory region. Wraps fi_mr handle.
/// Drop calls fi_mr_dereg (fi_close on the MR fid).
pub struct RegisteredMr {
    mr: *mut ffi::fid_mr,
    pub desc: *mut std::os::raw::c_void,
    pub rkey: u64,
    pub addr: *const u8,
    pub len: usize,
}

// SAFETY: RegisteredMr is only accessed through EfaContext which is behind a Mutex.
unsafe impl Send for RegisteredMr {}
unsafe impl Sync for RegisteredMr {}

impl Drop for RegisteredMr {
    fn drop(&mut self) {
        #[cfg(feature = "efa")]
        {
            if !self.mr.is_null() {
                unsafe { ffi::fi_mr_dereg(self.mr) };
            }
        }
    }
}

// ─── EfaContext ──────────────────────────────────────────────────────────────

/// Global EFA context — fabric + domain per device, registered MRs.
pub struct EfaContext {
    available: bool,
    device_count: usize,
    /// libfabric domain handle (needed for fi_mr_reg).
    /// NULL if EFA unavailable.
    domain: *mut ffi::fid_domain,
    /// Fabric handle.
    fabric: *mut ffi::fid_fabric,
    /// All registered MRs (for bulk startup registration).
    registered_mrs: Mutex<Vec<RegisteredMr>>,
    /// Next MR key to request.
    next_key: Mutex<u64>,
}

// SAFETY: EfaContext fields are protected by Mutex where needed.
// domain/fabric are only used for fi_mr_reg which is thread-safe per libfabric spec.
unsafe impl Send for EfaContext {}
unsafe impl Sync for EfaContext {}

impl EfaContext {
    /// Discover EFA devices, create fabric + domain.
    pub fn new() -> Result<Self, TransportError> {
        // Try to call fi_getinfo to discover EFA provider.
        // If libfabric is not linked or no EFA device, return unavailable.
        let domain: *mut ffi::fid_domain = std::ptr::null_mut();
        let fabric: *mut ffi::fid_fabric = std::ptr::null_mut();

        // Attempt EFA discovery. This will fail gracefully on non-EFA machines
        // because fi_getinfo returns an error when no provider matches.
        let available = unsafe { Self::try_init_efa() };

        match available {
            Some((fab, dom, dev_count)) => Ok(Self {
                available: true,
                device_count: dev_count,
                domain: dom,
                fabric: fab,
                registered_mrs: Mutex::new(Vec::new()),
                next_key: Mutex::new(0),
            }),
            None => Ok(Self {
                available: false,
                device_count: 0,
                domain,
                fabric,
                registered_mrs: Mutex::new(Vec::new()),
                next_key: Mutex::new(0),
            }),
        }
    }

    /// Attempt to initialize EFA. Returns None if unavailable.
    unsafe fn try_init_efa() -> Option<(*mut ffi::fid_fabric, *mut ffi::fid_domain, usize)> {
        // Check if libfabric is loadable (weak linking or dlopen approach).
        // For now, we use direct linking — if libfabric is not present,
        // the binary won't load (linker error at build time).
        //
        // On EFA-enabled machines: link with -lfabric (add to build.rs).
        // On dev desktops: compile with feature flag `efa` disabled.

        #[cfg(not(feature = "efa"))]
        {
            return None;
        }

        #[cfg(feature = "efa")]
        {
            use std::ffi::CString;
            use std::ptr;

            let version = ffi::fi_version();
            let provider = CString::new("efa").unwrap();

            // TODO: Build hints struct with:
            //   ep_type = FI_EP_RDM
            //   caps = FI_MSG | FI_RMA
            //   mode = FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY
            let hints: *const ffi::fi_info = ptr::null();

            let mut info: *mut ffi::fi_info = ptr::null_mut();
            let ret = ffi::fi_getinfo(
                version,
                ptr::null(),
                ptr::null(),
                0,
                hints,
                &mut info,
            );
            if ret != ffi::FI_SUCCESS || info.is_null() {
                return None;
            }

            // Create fabric
            let mut fabric: *mut ffi::fid_fabric = ptr::null_mut();
            // fi_fabric takes fi_fabric_attr which is the first field of fi_info
            let ret = ffi::fi_fabric(
                info as *mut std::os::raw::c_void, // fabric_attr is first field
                &mut fabric,
                ptr::null_mut(),
            );
            if ret != ffi::FI_SUCCESS {
                ffi::fi_freeinfo(info);
                return None;
            }

            // Create domain
            let mut domain: *mut ffi::fid_domain = ptr::null_mut();
            let ret = ffi::fi_domain(fabric, info, &mut domain, ptr::null_mut());
            if ret != ffi::FI_SUCCESS {
                ffi::fi_close(fabric as *mut std::os::raw::c_void);
                ffi::fi_freeinfo(info);
                return None;
            }

            // Count devices (walk the fi_info linked list)
            let mut dev_count = 0;
            let mut cur = info;
            while !cur.is_null() {
                dev_count += 1;
                // fi_info->next is a linked list — for now assume 1
                break;
            }

            ffi::fi_freeinfo(info);
            Some((fabric, domain, dev_count.max(1)))
        }
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn device_count(&self) -> usize {
        self.device_count
    }

    /// Register a buffer with EFA (fi_mr_reg).
    /// Returns a RegisteredMr handle. Caller owns it — drop to deregister.
    ///
    /// This is the REAL fi_mr_reg call. On EFA hardware it takes ~1-5ms
    /// (pins pages via get_user_pages + programs NIC MR table).
    pub fn register_buffer(&self, buf: *const u8, len: usize) -> Result<RegisteredMr, TransportError> {
        if !self.available {
            // No EFA — return a dummy MR (no-op registration).
            return Ok(RegisteredMr {
                mr: std::ptr::null_mut(),
                desc: std::ptr::null_mut(),
                rkey: 0,
                addr: buf,
                len,
            });
        }

        #[cfg(feature = "efa")]
        {
            let mut key = self.next_key.lock().unwrap();
            let requested_key = *key;
            *key += 1;
            drop(key);

            let access = ffi::FI_SEND | ffi::FI_RECV | ffi::FI_READ | ffi::FI_WRITE
                | ffi::FI_REMOTE_READ | ffi::FI_REMOTE_WRITE;

            let mut mr: *mut ffi::fid_mr = std::ptr::null_mut();

            let ret = unsafe {
                ffi::fi_mr_reg(
                    self.domain,
                    buf as *const std::os::raw::c_void,
                    len,
                    access,
                    0,
                    requested_key,
                    0,
                    &mut mr,
                    std::ptr::null_mut(),
                )
            };

            if ret != ffi::FI_SUCCESS {
                return Err(TransportError::RegistrationFailed);
            }

            let desc = unsafe { ffi::fi_mr_desc(mr) };
            let rkey = unsafe { ffi::fi_mr_key(mr) };

            Ok(RegisteredMr {
                mr,
                desc,
                rkey,
                addr: buf,
                len,
            })
        }

        #[cfg(not(feature = "efa"))]
        {
            // Without EFA feature, return dummy (no-op).
            Ok(RegisteredMr {
                mr: std::ptr::null_mut(),
                desc: std::ptr::null_mut(),
                rkey: 0,
                addr: buf,
                len,
            })
        }
    }

    /// Register multiple buffers at startup (bulk registration).
    /// Stores MR handles internally. Used by bufpool and arena modes.
    pub fn register_buffers(&self, bufs: &[&[u8]]) -> Result<(), TransportError> {
        if !self.available {
            return Ok(());
        }

        let mut mrs = self.registered_mrs.lock().unwrap();
        for buf in bufs {
            let mr = self.register_buffer(buf.as_ptr(), buf.len())?;
            mrs.push(mr);
        }
        Ok(())
    }

    /// Deregister all bulk-registered MRs.
    pub fn deregister_buffers(&self) -> Result<(), TransportError> {
        if !self.available {
            return Ok(());
        }
        let mut mrs = self.registered_mrs.lock().unwrap();
        mrs.clear(); // Drop triggers fi_mr_dereg on each
        Ok(())
    }

    pub fn shutdown(&self) {
        // Deregister all MRs first.
        let _ = self.deregister_buffers();
        // fi_close domain + fabric.
        #[cfg(feature = "efa")]
        if self.available {
            unsafe {
                if !self.domain.is_null() {
                    ffi::fi_close(self.domain as *mut std::os::raw::c_void);
                }
                if !self.fabric.is_null() {
                    ffi::fi_close(self.fabric as *mut std::os::raw::c_void);
                }
            }
        }
    }
}

// ─── Per-request registration (for dynamic mode) ─────────────────────────────

/// Register a single buffer with EFA. Returns handle for fi_write use.
/// Caller must hold this handle until the fi_write completes, then drop to deregister.
///
/// This is the expensive per-request path. On real EFA hardware (~1-5ms per call).
pub fn register_single_buffer(buf: *const u8, len: usize) -> Result<RegisteredMr, TransportError> {
    efa_context().register_buffer(buf, len)
}

/// Deregister by dropping the RegisteredMr handle (Drop impl calls fi_close).
/// Explicit function for clarity — just drops the handle.
pub fn deregister_single_buffer(mr: RegisteredMr) {
    drop(mr);
}

// ─── Session ─────────────────────────────────────────────────────────────────

/// Per-client DMA session. Created during LO.HELLO.
pub struct Session {
    pub client_regions: Vec<ClientRegion>,
    // TODO: dest_fi_addr handles per EFA device (from fi_av_insert)
}

impl Session {
    pub fn new(
        _ctx: &EfaContext,
        _peer_addr: &EfaAddress,
        client_regions: Vec<ClientRegion>,
    ) -> Result<Self, TransportError> {
        // TODO: fi_av_insert(peer_addr) on all N devices
        Ok(Self { client_regions })
    }

    pub fn server_addrs(&self) -> Vec<EfaAddress> {
        vec![]
    }

    pub fn write(
        &self,
        buf: Buffer,
        _len: usize,
        region_idx: u32,
        _remote_offset: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    ) {
        if region_idx as usize >= self.client_regions.len() {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }
        // TODO: fi_writemsg using buf.ptr() as local source, region rkey + addr as dest.
        on_complete(buf, Ok(()));
    }

    pub fn read(
        &self,
        buf: Buffer,
        _len: usize,
        region_idx: u32,
        _remote_offset: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    ) {
        if region_idx as usize >= self.client_regions.len() {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }
        // TODO: fi_readmsg using buf.ptr() as local dest, region rkey + addr as source.
        on_complete(buf, Ok(()));
    }

    pub fn close(self) {
        // TODO: fi_close endpoints, remove AV entries.
    }
}

// ─── Global Transport State ──────────────────────────────────────────────────

static EFA_CTX: OnceLock<EfaContext> = OnceLock::new();

pub fn init() {
    match EfaContext::new() {
        Ok(ctx) => {
            EFA_CTX.set(ctx).ok();
        }
        Err(_) => {
            EFA_CTX
                .set(EfaContext {
                    available: false,
                    device_count: 0,
                    domain: std::ptr::null_mut(),
                    fabric: std::ptr::null_mut(),
                    registered_mrs: Mutex::new(Vec::new()),
                    next_key: Mutex::new(0),
                })
                .ok();
        }
    }
}

pub fn efa_context() -> &'static EfaContext {
    EFA_CTX.get().expect("transport not initialized")
}

pub fn register_buffers(bufs: &[&[u8]]) {
    let _ = efa_context().register_buffers(bufs);
}

pub fn deregister_buffers() {
    let _ = efa_context().deregister_buffers();
}

pub fn shutdown() {
    if let Some(ctx) = EFA_CTX.get() {
        ctx.shutdown();
    }
}
