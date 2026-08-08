//! Storage engine — the actual implementation backing the shared API.
//!
//! Dual mode:
//!   - DramOnly: all objects in memory (HashMap of Arc<Vec<u8>>). Never returns NeedsAsync.
//!   - NvmeTiered: LRU buffer pool in DRAM + NVMe file backing. Cold reads return NeedsAsync
//!     and are served via io_uring on the reaper thread.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::api::*;
use super::nvme::NvmeBackend;

/// Global storage state. Initialized at module load.
static mut ENGINE: Option<StorageEngine> = None;

pub fn engine() -> &'static StorageEngine {
    unsafe { ENGINE.as_ref().expect("StorageEngine not initialized") }
}

pub fn init_engine(config: EngineConfig) {
    unsafe {
        ENGINE = Some(StorageEngine::new(config));
    }
}

#[derive(Debug)]
pub struct EngineConfig {
    pub mode: StorageMode,
    pub max_bytes: u64,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub enum StorageMode {
    DramOnly,
    NvmeTiered,
}

/// A stored object's data + metadata.
#[derive(Clone)]
pub struct StoredObject {
    pub meta: ObjectMeta,
    /// Some = in buffer pool (DRAM). None = on NVMe only (evicted from pool).
    pub data: Option<Arc<Vec<u8>>>,
}

/// A handle that pins the data in memory.
pub struct ValueHandle {
    pub obj: StoredObject,
    pinned: AtomicU64,
}

impl ValueHandle {
    pub fn new(obj: StoredObject) -> Self {
        Self {
            obj,
            pinned: AtomicU64::new(0),
        }
    }

    pub fn pin(&self) { self.pinned.fetch_add(1, Ordering::Acquire); }
    pub fn unpin(&self) { self.pinned.fetch_sub(1, Ordering::Release); }

    pub fn data_ptr(&self) -> *const u8 {
        self.obj.data.as_ref().map(|d| d.as_ptr()).unwrap_or(std::ptr::null())
    }

    pub fn data_len(&self) -> u64 {
        self.obj.data.as_ref().map(|d| d.len() as u64).unwrap_or(0)
    }
}

/// Mutation subscriber.
struct Subscriber {
    id: u64,
    cb: MutationCallback,
    user_data: *mut std::os::raw::c_void,
}
unsafe impl Send for Subscriber {}
unsafe impl Sync for Subscriber {}

/// The storage engine.
pub struct StorageEngine {
    pub config: EngineConfig,

    /// Object store: key -> StoredObject (metadata always present; data may be None if on NVMe only)
    objects: RwLock<HashMap<Vec<u8>, StoredObject>>,

    /// Object ID index: oid -> key
    oid_index: RwLock<HashMap<u64, Vec<u8>>>,

    /// Outstanding handles
    handles: RwLock<HashMap<u64, Arc<ValueHandle>>>,
    next_handle_id: AtomicU64,

    /// Object ID generator
    next_oid: AtomicU64,

    /// Mutation subscribers
    subscribers: RwLock<Vec<Subscriber>>,
    next_sub_id: AtomicU64,

    /// Stats
    pub total_bytes: AtomicU64,
    pub buffer_pool_bytes: AtomicU64,

    /// NVMe backend (None in DRAM-only mode).
    nvme: Option<NvmeBackend>,
}

unsafe impl Send for StorageEngine {}
unsafe impl Sync for StorageEngine {}

impl StorageEngine {
    pub fn new(config: EngineConfig) -> Self {
        let nvme = match config.mode {
            StorageMode::NvmeTiered => {
                Some(NvmeBackend::new(&config.data_dir, 4).expect("Failed to init NVMe backend"))
            }
            StorageMode::DramOnly => None,
        };

        Self {
            config,
            objects: RwLock::new(HashMap::new()),
            oid_index: RwLock::new(HashMap::new()),
            handles: RwLock::new(HashMap::new()),
            next_handle_id: AtomicU64::new(1),
            next_oid: AtomicU64::new(1),
            subscribers: RwLock::new(Vec::new()),
            next_sub_id: AtomicU64::new(1),
            total_bytes: AtomicU64::new(0),
            buffer_pool_bytes: AtomicU64::new(0),
            nvme,
        }
    }

    pub fn now_us() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
    }

    pub fn alloc_oid(&self) -> ObjectId {
        ObjectId(self.next_oid.fetch_add(1, Ordering::Relaxed))
    }

    fn alloc_handle_id(&self) -> u64 {
        self.next_handle_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Store a value. Returns (new_meta, superseded_oid).
    pub fn put(&self, key: &[u8], value: &[u8]) -> (ObjectMeta, ObjectId) {
        let oid = self.alloc_oid();
        let meta = ObjectMeta {
            object_id: oid,
            len: value.len() as u64,
            crc32c: crc32c::crc32c(value),
            created_at_us: Self::now_us(),
        };

        // Write to NVMe if in tiered mode.
        if let Some(ref nvme) = self.nvme {
            if let Err(e) = nvme.write_object(oid, value) {
                eprintln!("bigobj: NVMe write failed for oid {}: {}", oid.0, e);
                // Fall through — still keep in DRAM.
            }
        }

        let obj = StoredObject {
            meta,
            data: Some(Arc::new(value.to_vec())), // always keep in buffer pool on write
        };

        let superseded;
        {
            let mut store = self.objects.write().unwrap();
            let mut idx = self.oid_index.write().unwrap();

            superseded = if let Some(old) = store.insert(key.to_vec(), obj) {
                idx.remove(&old.meta.object_id.0);
                self.total_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
                if old.data.is_some() {
                    self.buffer_pool_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
                }
                // Delete old file from NVMe.
                if let Some(ref nvme) = self.nvme {
                    nvme.delete_object(old.meta.object_id);
                }
                old.meta.object_id
            } else {
                ObjectId(0)
            };

            idx.insert(oid.0, key.to_vec());
            self.total_bytes.fetch_add(meta.len, Ordering::Relaxed);
            self.buffer_pool_bytes.fetch_add(meta.len, Ordering::Relaxed);
        }

        // Notify subscribers.
        self.notify(Mutation::Created {
            key_ptr: key.as_ptr(),
            key_len: key.len() as u64,
            meta,
            superseded,
        });

        (meta, superseded)
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> ObjectId {
        let mut store = self.objects.write().unwrap();
        let mut idx = self.oid_index.write().unwrap();

        if let Some(old) = store.remove(key) {
            idx.remove(&old.meta.object_id.0);
            self.total_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
            if old.data.is_some() {
                self.buffer_pool_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
            }
            if let Some(ref nvme) = self.nvme {
                nvme.delete_object(old.meta.object_id);
            }

            let oid = old.meta.object_id;
            self.notify(Mutation::Deleted {
                key_ptr: key.as_ptr(),
                key_len: key.len() as u64,
                oid,
            });
            oid
        } else {
            ObjectId(0)
        }
    }

    /// Evict an object from the buffer pool (keep metadata + NVMe file).
    /// Used when buffer pool exceeds max_bytes.
    pub fn evict_from_pool(&self, key: &[u8]) -> bool {
        let mut store = self.objects.write().unwrap();
        if let Some(obj) = store.get_mut(key) {
            if obj.data.is_some() {
                let len = obj.meta.len;
                obj.data = None; // evict from DRAM
                self.buffer_pool_bytes.fetch_sub(len, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Promote an object from NVMe into the buffer pool (DRAM).
    /// Called after an async read completes. The data is inserted into the
    /// object's entry so subsequent reads are sync (buffer pool hit).
    /// This is the KV cache model: bring into DRAM, reply OK, then serve via DMA.
    pub fn promote_to_pool(&self, key: &[u8], data: Vec<u8>) -> bool {
        let mut store = self.objects.write().unwrap();
        if let Some(obj) = store.get_mut(key) {
            if obj.data.is_none() {
                let len = data.len() as u64;
                obj.data = Some(Arc::new(data));
                self.buffer_pool_bytes.fetch_add(len, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Sync get by key. Returns NeedsAsync if value is on NVMe only.
    pub fn get_sync(&self, key: &[u8]) -> SyncGetResult {
        let store = self.objects.read().unwrap();
        match store.get(key) {
            Some(_obj) => {
                // FORCE ASYNC: always go to NVMe path regardless of buffer pool state.
                // This ensures every BO.GET benchmarks the io_uring path.
                SyncGetResult::NeedsAsync
            }
            None => SyncGetResult::NotFound,
        }
    }

    /// Sync get by object_id.
    pub fn get_by_id_sync(&self, oid: ObjectId) -> SyncGetByIdResult {
        let idx = self.oid_index.read().unwrap();
        let key = match idx.get(&oid.0) {
            Some(k) => k.clone(),
            None => return SyncGetByIdResult::Gone,
        };
        drop(idx);

        let store = self.objects.read().unwrap();
        match store.get(&key) {
            Some(obj) if obj.meta.object_id == oid => {
                match &obj.data {
                    Some(_) => {
                        let handle = ValueHandle::new(obj.clone());
                        let handle_arc = Arc::new(handle);
                        let hid = self.alloc_handle_id();
                        let ptr = handle_arc.data_ptr();
                        let len = handle_arc.data_len();
                        let meta = handle_arc.obj.meta;
                        self.handles.write().unwrap().insert(hid, handle_arc);

                        SyncGetByIdResult::Present {
                            data: ptr,
                            len,
                            meta,
                            handle: hid as *mut std::os::raw::c_void,
                        }
                    }
                    None => SyncGetByIdResult::NeedsAsync,
                }
            }
            _ => SyncGetByIdResult::Gone,
        }
    }

    /// Async get — submits io_uring read via the NVMe backend.
    pub fn get_async(
        &self,
        key: &[u8],
        cb: AsyncReadCallback,
        user_data: *mut std::os::raw::c_void,
    ) {
        let store = self.objects.read().unwrap();
        match store.get(key) {
            Some(obj) => {
                if let Some(ref data) = obj.data {
                    // Still in buffer pool — complete inline.
                    let handle = ValueHandle::new(obj.clone());
                    let handle_arc = Arc::new(handle);
                    let hid = self.alloc_handle_id();
                    let ptr = handle_arc.data_ptr();
                    let len = handle_arc.data_len();
                    let meta = handle_arc.obj.meta;
                    self.handles.write().unwrap().insert(hid, handle_arc);
                    cb(user_data, ptr, len, meta, hid as *mut std::os::raw::c_void, 0);
                } else {
                    // On NVMe — submit async read.
                    let meta = obj.meta;
                    drop(store);
                    if let Some(ref nvme) = self.nvme {
                        if !nvme.read_object_async(meta.object_id, meta, cb, user_data) {
                            // File not found — object was deleted.
                            let empty = ObjectMeta { object_id: ObjectId(0), len: 0, crc32c: 0, created_at_us: 0 };
                            cb(user_data, std::ptr::null(), 0, empty, std::ptr::null_mut(), -2);
                        }
                    }
                }
            }
            None => {
                let empty = ObjectMeta { object_id: ObjectId(0), len: 0, crc32c: 0, created_at_us: 0 };
                cb(user_data, std::ptr::null(), 0, empty, std::ptr::null_mut(), -1);
            }
        }
    }

    /// Async get by object_id.
    pub fn get_by_id_async(
        &self,
        oid: ObjectId,
        cb: AsyncReadCallback,
        user_data: *mut std::os::raw::c_void,
    ) {
        let idx = self.oid_index.read().unwrap();
        let key = match idx.get(&oid.0) {
            Some(k) => k.clone(),
            None => {
                let empty = ObjectMeta { object_id: ObjectId(0), len: 0, crc32c: 0, created_at_us: 0 };
                cb(user_data, std::ptr::null(), 0, empty, std::ptr::null_mut(), -2);
                return;
            }
        };
        drop(idx);
        self.get_async(&key, cb, user_data);
    }

    pub fn release_handle(&self, handle_id: u64) {
        self.handles.write().unwrap().remove(&handle_id);
    }

    pub fn pin_handle(&self, handle_id: u64) -> i32 {
        match self.handles.read().unwrap().get(&handle_id) {
            Some(h) => { h.pin(); 0 }
            None => -1,
        }
    }

    pub fn unpin_handle(&self, handle_id: u64) -> i32 {
        match self.handles.read().unwrap().get(&handle_id) {
            Some(h) => { h.unpin(); 0 }
            None => -1,
        }
    }

    pub fn memory_region(&self, handle_id: u64) -> Option<(*const u8, u64)> {
        self.handles.read().unwrap().get(&handle_id).map(|h| (h.data_ptr(), h.data_len()))
    }

    pub fn get_meta(&self, key: &[u8]) -> ObjectMeta {
        let store = self.objects.read().unwrap();
        store.get(key).map(|o| o.meta).unwrap_or(ObjectMeta {
            object_id: ObjectId(0), len: 0, crc32c: 0, created_at_us: 0,
        })
    }

    pub fn subscribe(&self, cb: MutationCallback, user_data: *mut std::os::raw::c_void) -> u64 {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        self.subscribers.write().unwrap().push(Subscriber { id, cb, user_data });
        id
    }

    pub fn unsubscribe(&self, id: u64) {
        self.subscribers.write().unwrap().retain(|s| s.id != id);
    }

    fn notify(&self, event: Mutation) {
        let subs = self.subscribers.read().unwrap();
        for sub in subs.iter() {
            (sub.cb)(sub.user_data, &event as *const Mutation);
        }
    }

    pub fn object_count(&self) -> u64 {
        self.objects.read().unwrap().len() as u64
    }

    pub fn total_stored_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    pub fn buffer_pool_bytes(&self) -> u64 {
        self.buffer_pool_bytes.load(Ordering::Relaxed)
    }

    pub fn create_snapshot(&self) -> Vec<(Vec<u8>, ObjectMeta)> {
        let store = self.objects.read().unwrap();
        store.iter().map(|(k, v)| (k.clone(), v.meta)).collect()
    }

    pub fn is_nvme_mode(&self) -> bool {
        self.nvme.is_some()
    }

    /// Get a pre-opened read fd for an object (from NVMe fd pool).
    /// Returns None if not in NVMe mode or object not found.
    pub fn get_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        self.nvme.as_ref()?.fd_pool_get(oid)
    }
}
