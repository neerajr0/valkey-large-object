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

/// Fallback cap on the number of *cached* fds.
///
/// The real cap is normally derived at load from the process `RLIMIT_NOFILE` soft
/// limit (see `crate::fd_pool_capacity`), which is the actual ceiling on open fds.
/// This constant is only used when that query fails or the limit is unbounded
/// (`RLIM_INFINITY`). 100k is a deliberately conservative floor: it sits well under
/// the ulimits we run with (production NVMe instance stores raise `nofile` to ~1M),
/// while still caching enough fds to keep the read path warm.
///
/// Soft bound: in-flight fds (evicted or deleted mid-read) stay open until the read
/// completes, so the live fd count can briefly exceed the cap.
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

    /// Current number of cached fds. Test-only for now; promote to a public accessor
    /// when INFO largeobj stats actually need it.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.read().unwrap().map.len()
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

    /// Open a throwaway read fd (real, valid, cheap to close). `/dev/null` gives us as
    /// many distinct fds as we need without touching the filesystem under test.
    fn open_null() -> RawFd {
        let path = std::ffi::CString::new("/dev/null").unwrap();
        // SAFETY: constant valid C path, O_RDONLY is a valid flag.
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        assert!(fd >= 0, "open /dev/null failed");
        fd
    }

    #[test]
    fn test_fd_pool_insert_acquire_remove() {
        let pool = FdPool::new();
        let oid = ObjectId(42);

        // Use a pipe so closure can be verified race-free: the read end reports EOF
        // only once every write-end fd is closed. That depends on the pipe's open
        // file description, not the fd *number*, so it is immune to fd-number reuse
        // by other tests running on parallel threads.
        let mut fds = [0 as RawFd; 2];
        // SAFETY: pipe() fills a 2-element array with valid fds when it returns 0.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "pipe failed");
        let (read_end, write_end) = (fds[0], fds[1]);
        // Non-blocking read end so the EOF check can't hang if the close didn't happen.
        // SAFETY: read_end is a valid fd; setting O_NONBLOCK on it is sound.
        unsafe { libc::fcntl(read_end, libc::F_SETFL, libc::O_NONBLOCK) };

        // Insert returns a guard for immediate use; the cached fd is retrievable.
        let guard = pool.insert(oid, write_end);
        assert_eq!(guard.fd(), write_end);
        assert_eq!(pool.len(), 1);

        // A subsequent acquire hits the cache and returns the same fd.
        let hit = pool.acquire(oid).expect("cached fd");
        assert_eq!(hit.fd(), write_end);

        // Drop all outstanding guards so remove can close the fd (no in-flight ref).
        drop(guard);
        drop(hit);

        // Remove drops the map's reference; with no in-flight guard the fd closes now.
        pool.remove(oid);
        assert!(pool.acquire(oid).is_none());

        // The only write end is now closed, so the read end must observe EOF (read
        // returns 0). Had the pool failed to close it, read would return -1/EAGAIN.
        let mut byte = 0u8;
        // SAFETY: read_end is a valid fd; reading one byte into a local is sound.
        let n = unsafe { libc::read(read_end, &mut byte as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(
            n, 0,
            "write end should be closed after remove (expected EOF)"
        );

        // SAFETY: read_end is still open and owned by this test.
        unsafe { libc::close(read_end) };
    }

    #[test]
    fn test_eviction_bounds_capacity() {
        // With all guards dropped, every entry is idle, so inserting past capacity
        // always finds a victim and the cached count never exceeds the cap.
        let pool = FdPool::with_capacity(4);
        for i in 0..20u64 {
            drop(pool.insert(ObjectId(i), open_null()));
            assert!(pool.len() <= 4, "cached fds exceeded capacity at i={}", i);
        }
        assert_eq!(pool.len(), 4);
    }

    #[test]
    fn test_inflight_fd_is_never_evicted() {
        let pool = FdPool::with_capacity(2);

        // A is held (in flight): its guard keeps a strong ref alive.
        let a = pool.insert(ObjectId(1), open_null());
        // B is idle.
        drop(pool.insert(ObjectId(2), open_null()));

        // Inserting C is at capacity → eviction must skip the in-flight A and drop B.
        drop(pool.insert(ObjectId(3), open_null()));

        assert!(
            pool.acquire(ObjectId(1)).is_some(),
            "in-flight A was evicted"
        );
        assert!(
            pool.acquire(ObjectId(2)).is_none(),
            "idle B should be evicted"
        );
        assert!(
            pool.acquire(ObjectId(3)).is_some(),
            "freshly inserted C missing"
        );

        drop(a);
    }

    #[test]
    fn test_lfu_evicts_least_frequently_used() {
        let pool = FdPool::with_capacity(2);

        // Two idle entries; warm up A so it has a higher access frequency than B.
        drop(pool.insert(ObjectId(1), open_null()));
        drop(pool.insert(ObjectId(2), open_null()));
        for _ in 0..10 {
            drop(pool.acquire(ObjectId(1)));
        }

        // Inserting C evicts the coldest sampled idle entry — that's B, not A.
        drop(pool.insert(ObjectId(3), open_null()));

        assert!(pool.acquire(ObjectId(1)).is_some(), "hot A should survive");
        assert!(
            pool.acquire(ObjectId(2)).is_none(),
            "cold B should be evicted"
        );
        assert!(
            pool.acquire(ObjectId(3)).is_some(),
            "new C should be present"
        );
    }
}
