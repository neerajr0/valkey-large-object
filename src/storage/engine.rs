//! Storage engine — manages objects on NVMe with an in-memory metadata index.
//!
//! Valkey keyspace holds the BoValue (oid, len, crc). This engine holds:
//!   - Object data on NVMe files (via NvmeBackend)
//!   - OID-to-key reverse index (for BO.PULL by OID)
//!   - Buffer pool (data in DRAM for hot objects; None = on NVMe only)

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::nvme::NvmeBackend;

/// Object identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);

/// Object metadata.
#[derive(Debug, Clone, Copy)]
pub struct ObjectMeta {
    pub object_id: ObjectId,
    pub len: u64,
    pub crc32c: u32,
    pub created_at_us: u64,
}

/// A stored object — metadata always present, data optional (buffer pool).
#[derive(Clone)]
pub struct StoredObject {
    pub meta: ObjectMeta,
    pub data: Option<Arc<Vec<u8>>>,
}

/// Global storage engine.
static mut ENGINE: Option<StorageEngine> = None;

pub fn engine() -> &'static StorageEngine {
    unsafe { ENGINE.as_ref().expect("StorageEngine not initialized") }
}

pub fn init_engine(data_dir: PathBuf, max_bytes: u64) {
    unsafe {
        ENGINE = Some(StorageEngine::new(data_dir, max_bytes));
    }
}

pub struct StorageEngine {
    /// Object store: key -> StoredObject
    objects: RwLock<HashMap<Vec<u8>, StoredObject>>,
    /// Reverse index: oid -> key (for BO.PULL)
    oid_index: RwLock<HashMap<u64, Vec<u8>>>,
    /// OID generator
    next_oid: AtomicU64,
    /// Stats
    pub total_bytes: AtomicU64,
    pub buffer_pool_bytes: AtomicU64,
    /// NVMe backend
    nvme: NvmeBackend,
    /// Config
    pub max_bytes: u64,
}

unsafe impl Send for StorageEngine {}
unsafe impl Sync for StorageEngine {}

impl StorageEngine {
    fn new(data_dir: PathBuf, max_bytes: u64) -> Self {
        let nvme = NvmeBackend::new(&data_dir, 4)
            .expect("Failed to init NVMe backend");

        Self {
            objects: RwLock::new(HashMap::new()),
            oid_index: RwLock::new(HashMap::new()),
            next_oid: AtomicU64::new(1),
            total_bytes: AtomicU64::new(0),
            buffer_pool_bytes: AtomicU64::new(0),
            nvme,
            max_bytes,
        }
    }

    pub fn now_us() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
    }

    pub fn alloc_oid(&self) -> ObjectId {
        ObjectId(self.next_oid.fetch_add(1, Ordering::Relaxed))
    }

    /// Store a value. Writes to NVMe + keeps in buffer pool.
    pub fn put(&self, key: &[u8], value: &[u8]) -> ObjectMeta {
        let oid = self.alloc_oid();
        let meta = ObjectMeta {
            object_id: oid,
            len: value.len() as u64,
            crc32c: crc32c::crc32c(value),
            created_at_us: Self::now_us(),
        };

        // Write to NVMe.
        if let Err(e) = self.nvme.write_object(oid, value) {
            eprintln!("bigobj: NVMe write failed for oid {}: {}", oid.0, e);
        }

        let obj = StoredObject {
            meta,
            data: Some(Arc::new(value.to_vec())),
        };

        let mut store = self.objects.write().unwrap();
        let mut idx = self.oid_index.write().unwrap();

        // Remove old object if key exists.
        if let Some(old) = store.insert(key.to_vec(), obj) {
            idx.remove(&old.meta.object_id.0);
            self.total_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
            if old.data.is_some() {
                self.buffer_pool_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
            }
            self.nvme.delete_object(old.meta.object_id);
        }

        idx.insert(oid.0, key.to_vec());
        self.total_bytes.fetch_add(meta.len, Ordering::Relaxed);
        self.buffer_pool_bytes.fetch_add(meta.len, Ordering::Relaxed);

        meta
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> bool {
        let mut store = self.objects.write().unwrap();
        let mut idx = self.oid_index.write().unwrap();

        if let Some(old) = store.remove(key) {
            idx.remove(&old.meta.object_id.0);
            self.total_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
            if old.data.is_some() {
                self.buffer_pool_bytes.fetch_sub(old.meta.len, Ordering::Relaxed);
            }
            self.nvme.delete_object(old.meta.object_id);
            true
        } else {
            false
        }
    }

    /// Check if key exists and return whether it needs async read.
    /// Returns: Some(meta) if exists, None if not found.
    /// The bool indicates if it's in buffer pool (true) or needs NVMe (false).
    pub fn get_status(&self, key: &[u8]) -> Option<(ObjectMeta, bool)> {
        let store = self.objects.read().unwrap();
        store.get(key).map(|obj| (obj.meta, obj.data.is_some()))
    }

    /// Get metadata only.
    pub fn get_meta(&self, key: &[u8]) -> Option<ObjectMeta> {
        let store = self.objects.read().unwrap();
        store.get(key).map(|obj| obj.meta)
    }

    /// Get pre-opened read fd for an object.
    pub fn get_read_fd(&self, oid: ObjectId) -> Option<RawFd> {
        self.nvme.fd_pool_get(oid)
    }

    /// Evict from buffer pool (keep on NVMe).
    pub fn evict_from_pool(&self, key: &[u8]) -> bool {
        let mut store = self.objects.write().unwrap();
        if let Some(obj) = store.get_mut(key) {
            if obj.data.is_some() {
                self.buffer_pool_bytes.fetch_sub(obj.meta.len, Ordering::Relaxed);
                obj.data = None;
                return true;
            }
        }
        false
    }

    /// Check if key exists.
    pub fn exists(&self, key: &[u8]) -> bool {
        self.objects.read().unwrap().contains_key(key)
    }

    /// Object count.
    pub fn object_count(&self) -> u64 {
        self.objects.read().unwrap().len() as u64
    }

    /// Total stored bytes (all objects, pool + NVMe).
    pub fn total_stored_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Bytes currently in buffer pool.
    pub fn buffer_pool_bytes(&self) -> u64 {
        self.buffer_pool_bytes.load(Ordering::Relaxed)
    }

    /// Is NVMe mode (always true now).
    pub fn is_nvme_mode(&self) -> bool {
        true
    }
}
