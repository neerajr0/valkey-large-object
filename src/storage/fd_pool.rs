//! File Descriptor Pool — bounded, sampled-LFU cache of pre-opened object fds.
//!
//! Open once per object (lazily, on read miss), reuse on every read,
//! close on delete. Saves open()/close() syscalls on the hot read path.
//!
//! ## Bounded + sampled LFU
//!
//! fds are a finite process resource (ulimit -n), so the pool is capacity-bounded.
//! When full, eviction samples a small constant window of entries and drops the
//! least-frequently-used *idle* fd — best-effort. Every access bumps a per-fd
//! frequency counter and the counter is halved when an entry is sampled for eviction
//! (aging), to factor in decay of cooled down items. A rotating cursor traverses
//! the lfu to prevent re-inspection of the same entries.
//!
//! ## In-flight reference counting
//!
//! The fd is closed *exactly once*, by `FdHandle::drop`, only when the last strong
//! reference is released. Eviction/removal just drops the map's reference; if a read
//! is still in flight the fd survives until that read completes and drops its guard.
//! `Arc::strong_count - 1` is therefore the in-flight count for an object.

use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use indexmap::IndexMap;

use crate::data_type::ObjectId;

/// Cap on the number of *cached* fds. Keep it under `ulimit -n` with headroom for
/// Valkey's own fds. Soft bound: in-flight fds (evicted or deleted mid-read) stay
/// open until the read completes, so the live fd count can briefly exceed this.
pub const DEFAULT_CAPACITY: usize = 100_000;

/// LFU eviction sample size (similar to Valkey's `maxmemory-samples`).
const EVICT_SAMPLE_SIZE: usize = 8;

/// Owns a `RawFd` and closes it exactly once, when the last reference drops.
struct FdHandle {
    fd: RawFd,
}

/// Drop runs once, when the final Arc is released, so we close once.
impl Drop for FdHandle {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

/// A holder for an `Arc<FdHandle>` reference, held for the duration of one request.
/// Keeping it alive keeps the fd open; dropping it releases the reference.
#[derive(Clone)]
pub struct FdGuard(Arc<FdHandle>);

impl FdGuard {
    /// The raw fd. Valid for as long as this guard (or any clone) is alive.
    #[inline]
    pub fn fd(&self) -> RawFd {
        self.0.fd
    }
}

/// One cached fd plus its LFU frequency counter.
struct FdEntry {
    handle: Arc<FdHandle>,
    /// Bumped on every acquire and halved when sampled for eviction (aging).
    freq: AtomicU64,
}

/// Lock-protected pool state: the fd map plus the rotating eviction-sample cursor.
/// Both are only mutated under the pool's `RwLock` lock for insertions and evictions.
struct PoolInner {
    /// `IndexMap` (not `HashMap`) so eviction can sample by position — `get_index`.
    map: IndexMap<u64, FdEntry>,
    /// Cursor to track eviction sample window.
    cursor: usize,
}

pub struct FdPool {
    capacity: usize,
    inner: RwLock<PoolInner>,
}

impl Default for FdPool {
    fn default() -> Self {
        Self::new()
    }
}

impl FdPool {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: RwLock::new(PoolInner {
                map: IndexMap::new(),
                cursor: 0,
            }),
        }
    }

    /// Fast path: if `object_id` is cached, record an access (LFU bump) and return a
    /// guard that keeps the fd open for the duration of the caller's I/O. Returns
    /// `None` on a cache miss — the caller opens an fd and calls [`FdPool::insert`].
    ///
    /// Only takes a shared read lock, so concurrent reads of different (or the same)
    /// object don't contend beyond the atomic frequency bump.
    pub fn acquire(&self, object_id: ObjectId) -> Option<FdGuard> {
        let inner = self.inner.read().unwrap();
        let entry = inner.map.get(&object_id.0)?;
        entry.freq.fetch_add(1, Ordering::Relaxed);
        Some(FdGuard(entry.handle.clone()))
    }

    /// Insert a freshly-opened fd for `object_id` and return a guard for immediate
    /// use. Samples an eviction victim first if the pool is at capacity.
    pub fn insert(&self, object_id: ObjectId, fd: RawFd) -> FdGuard {
        let mut inner = self.inner.write().unwrap();

        if let Some(entry) = inner.map.get(&object_id.0) {
            // Lost the open race — close our redundant fd, reuse the winner.
            // Winner is the attempt that acquired the write lock first.
            // SAFETY: `fd` was just opened here and not yet shared, so closing it is sound.
            unsafe { libc::close(fd) };
            entry.freq.fetch_add(1, Ordering::Relaxed);
            return FdGuard(entry.handle.clone());
        }

        if inner.map.len() >= self.capacity {
            Self::evict_sample(&mut inner);
        }

        let handle = Arc::new(FdHandle { fd });
        let guard = FdGuard(handle.clone());
        inner.map.insert(
            object_id.0,
            FdEntry {
                handle,
                freq: AtomicU64::new(1),
            },
        );
        guard
    }

    /// Drop `object_id` from the pool (on object delete). The fd is closed once the
    /// last in-flight guard is released — a read already in flight keeps the fd valid
    /// until it completes.
    pub fn remove(&self, object_id: ObjectId) {
        // swap_remove is O(1); it moves the last entry into the freed slot (order
        // isn't meaningful here). Dropping the entry drops the map's Arc<FdHandle>;
        // the fd closes now if idle, or when the last in-flight guard drops otherwise.
        self.inner.write().unwrap().map.swap_remove(&object_id.0);
    }

    /// Current number of cached fds.
    #[allow(dead_code)] // forward-looking: INFO largeobj stats
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().map.len()
    }

    #[allow(dead_code)] // forward-looking: INFO largeobj stats
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Evict one entry via sampled (approximate) LFU. Caller holds the write lock.
    ///
    /// Inspects up to `EVICT_SAMPLE_SIZE` entries starting at `inner.cursor`, and
    /// evicts an *idle* entry (`strong_count == 1`) with the lowest frequency among
    /// them. This is a best-effort O(K) operation and is inspired by Valkey evictions.
    ///
    /// The cursor advances past the sampled window each call so successive evictions
    /// sweep the map rather than repeatedly inspecting the same entries.
    ///
    /// If the whole sample is in flight there is no victim and this is a no-op; the
    /// caller inserts anyway and the map briefly exceeds capacity (see `DEFAULT_CAPACITY`).
    fn evict_sample(inner: &mut PoolInner) {
        let len = inner.map.len();
        if len == 0 {
            return;
        }
        let sample = EVICT_SAMPLE_SIZE.min(len);

        let mut victim: Option<(usize, u64)> = None; // (index, freq)
        for i in 0..sample {
            let idx = (inner.cursor + i) % len;
            let (_, entry) = inner.map.get_index(idx).expect("idx < len");
            if Arc::strong_count(&entry.handle) != 1 {
                continue; // in flight — never evict, don't decay
            }
            let freq = entry.freq.load(Ordering::Relaxed);
            // Aging: halve the sampled entry's frequency
            entry.freq.store(freq >> 1, Ordering::Relaxed);
            let take = match victim {
                None => true,
                Some((_, best)) => freq < best,
            };
            if take {
                victim = Some((idx, freq));
            }
        }

        inner.cursor = (inner.cursor + sample) % len;

        if let Some((idx, _)) = victim {
            // Idle by construction, so this closes the fd immediately. swap_remove
            // moves the last entry into `idx` — fine for best-effort sampling.
            inner.map.swap_remove_index(idx);
        }
    }
}

// No manual Drop: dropping the map drops each Arc<FdHandle>, which closes every idle
// fd exactly once. fds still in flight at shutdown close when their last guard drops.

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ObjectId;

    #[test]
    fn test_fd_pool_insert_acquire_remove() {
        let pool = FdPool::new();
        let oid = ObjectId(42);

        // Open a real temp file to get a valid fd.
        let tmp = std::ffi::CString::new("/tmp/fdpool_test_XXXXXX").unwrap();
        let mut buf = tmp.into_bytes_with_nul();
        // SAFETY: mkstemp takes a mutable C string template, returns a valid fd.
        let fd = unsafe { libc::mkstemp(buf.as_mut_ptr() as *mut libc::c_char) };
        assert!(fd >= 0, "mkstemp failed");

        // Insert returns a guard for immediate use; the cached fd is retrievable.
        let guard = pool.insert(oid, fd);
        assert_eq!(guard.fd(), fd);
        assert_eq!(pool.len(), 1);

        // A subsequent acquire hits the cache and returns the same fd.
        let hit = pool.acquire(oid).expect("cached fd");
        assert_eq!(hit.fd(), fd);

        // Drop all outstanding guards so remove can close the fd (no in-flight ref).
        drop(guard);
        drop(hit);

        // Remove drops the map's reference; with no in-flight guard the fd closes now.
        pool.remove(oid);
        assert!(pool.acquire(oid).is_none());

        // Verify fd is actually closed: fcntl should fail with EBADF.
        // SAFETY: fcntl on a closed fd returns -1 (does not crash).
        let ret = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_eq!(ret, -1, "fd should be closed after remove");

        // Clean up the temp file.
        let path = std::ffi::CStr::from_bytes_with_nul(&buf).unwrap();
        // SAFETY: path is a valid C string from mkstemp.
        unsafe { libc::unlink(path.as_ptr()) };
    }
}
