//! Per-key read coalescing (singleflight pattern).
//!
//! Deduplicates concurrent NVMe reads for the same ObjectId.
//! Only the leader acquires a buffer and submits an io_uring read.
//! Waiters register callbacks and receive memcpy from the leader's buffer at completion.
//!
//! Lives entirely in the storage layer — invisible to transport (libefa-rs) and
//! command handlers. The Storage trait signature does not change.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::data_type::ObjectId;
use crate::storage::buffer::BufferPool;
use crate::storage::Buffer;
use crate::storage::StorageError;

// ─── Callback Type ───────────────────────────────────────────────────────────

/// Callback type for the coalesced read path.
///
/// Receives `Option<Buffer>`:
///   - `Some(buf)` on success — buf contains the object data (via NVMe DMA or memcpy).
///   - `None` on error — leader NVMe read failed, or pool exhausted at fan-out time.
///
/// The existing `ReadCallback` (which always requires a Buffer) remains unchanged
/// for internal I/O paths (io_uring submission, write_new, etc.).
pub type CoalescedReadCallback = Box<dyn FnOnce(Option<Buffer>, Result<u64, StorageError>) + Send>;

// ─── Coalesce Result ─────────────────────────────────────────────────────────

/// Result of attempting to coalesce a read via `try_join_or_lead`.
pub enum CoalesceResult {
    /// Caller is the leader — must acquire a buffer and submit the NVMe read.
    /// The `CoalescedReadCallback` is returned so the caller can wrap it into
    /// the leader's io_uring completion path.
    Leader(CoalescedReadCallback),
    /// Caller joined as a waiter — no buffer needed, callback stored in map.
    Waiter,
}

// ─── Internal State ──────────────────────────────────────────────────────────

/// State for one in-flight coalesced read.
struct CoalescedRead {
    /// Callbacks waiting for this read to complete (waiters only).
    /// The leader's callback is NOT stored here — it travels with the io_uring request.
    waiters: Vec<CoalescedReadCallback>,
}

// ─── CoalescingMap ───────────────────────────────────────────────────────────

/// The coalescing map. Keyed by ObjectId.
/// An entry exists IFF a read for that OID is currently in-flight.
///
/// Accessed from:
///   - Main thread: try_join_or_lead() at command dispatch time.
///   - io_uring poller thread: complete() at NVMe read completion time.
///
/// Lock hold time: ~50-100ns (HashMap lookup + Vec push or drain).
pub struct CoalescingMap {
    map: Mutex<HashMap<ObjectId, CoalescedRead>>,
}

impl Default for CoalescingMap {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalescingMap {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// Try to join an existing in-flight read, or become the leader.
    ///
    /// - If OID is already in the map: registers `cb` as a waiter, returns `Waiter`.
    /// - If OID is not in the map: inserts a new entry (empty waiter list),
    ///   returns `Leader(cb)` so the caller can use it in the io_uring completion path.
    pub fn try_join_or_lead(&self, oid: ObjectId, cb: CoalescedReadCallback) -> CoalesceResult {
        let mut map = self.map.lock().unwrap();
        if let Some(entry) = map.get_mut(&oid) {
            entry.waiters.push(cb);
            CoalesceResult::Waiter
        } else {
            map.insert(
                oid,
                CoalescedRead {
                    waiters: Vec::new(),
                },
            );
            CoalesceResult::Leader(cb)
        }
    }

    /// Called when the leader's NVMe read completes (success or failure).
    ///
    /// Drains all waiters for this OID:
    ///   - On success: acquires a buffer per waiter, memcpy from leader_buf, fires cb(Some(buf), Ok(n)).
    ///   - On leader failure: fires cb(None, Err(e)) for each waiter.
    ///   - On pool exhaustion during fan-out: fires cb(None, Err(PoolExhausted)) for that waiter.
    ///
    /// Removes the OID from the map after draining.
    ///
    /// Returns the number of waiters served (for future metrics).
    pub fn complete(
        &self,
        oid: ObjectId,
        leader_buf: &Buffer,
        result: &Result<u64, StorageError>,
        pool: &BufferPool,
    ) -> usize {
        let waiters = {
            let mut map = self.map.lock().unwrap();
            match map.remove(&oid) {
                Some(entry) => entry.waiters,
                None => return 0,
            }
        };

        let waiter_count = waiters.len();

        match result {
            Ok(bytes_read) => {
                let len = *bytes_read as usize;
                for cb in waiters {
                    match pool.get() {
                        Some(waiter_buf) => {
                            // SAFETY: Both pointers are valid pool buffers with capacity >= len.
                            // leader_buf was just filled by NVMe DMA (L1/L2 hot).
                            // waiter_buf is a distinct pool slot (no aliasing).
                            // len <= pool_buf_size (enforced at DMA.SET time).
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    leader_buf.ptr(),
                                    waiter_buf.ptr(),
                                    len,
                                );
                            }
                            cb(Some(waiter_buf), Ok(*bytes_read));
                        }
                        None => {
                            // Pool exhausted at fan-out time — fail this waiter.
                            cb(None, Err(StorageError::PoolExhausted));
                        }
                    }
                }
            }
            Err(_e) => {
                // Leader NVMe read failed — propagate error to all waiters.
                // Waiters never acquired a buffer, so pass None.
                for cb in waiters {
                    cb(None, Err(StorageError::IoError { code: -1 }));
                }
            }
        }

        waiter_count
    }

    /// Check if an OID currently has an in-flight read (for testing/metrics).
    #[cfg(test)]
    pub fn is_inflight(&self, oid: ObjectId) -> bool {
        self.map.lock().unwrap().contains_key(&oid)
    }

    /// Remove an entry without completing it (used when leader fails to acquire a buffer).
    /// Returns the number of waiters that were dropped.
    pub fn remove_without_complete(&self, oid: ObjectId) -> usize {
        let mut map = self.map.lock().unwrap();
        match map.remove(&oid) {
            Some(entry) => entry.waiters.len(),
            None => 0,
        }
    }

    /// Number of OIDs currently in-flight (for metrics).
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::PinnedBuffer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Helper: leak PinnedBuffers to get &'static refs for test Buffers.
    fn leak_pinned(count: usize, size: usize) -> &'static [PinnedBuffer] {
        let buffers: Vec<PinnedBuffer> = (0..count).map(|_| PinnedBuffer::new(size)).collect();
        Box::leak(buffers.into_boxed_slice())
    }

    /// Helper: create a BufferPool with N buffers of given size.
    fn make_pool(count: usize, size: usize) -> &'static BufferPool {
        let pinned = leak_pinned(count, size);
        let pool = BufferPool::new();
        // Manually fill without going through StorageEngine.
        {
            let mut inner = pool.pool.lock().unwrap();
            for (i, pb) in pinned.iter().enumerate() {
                inner.push(Buffer::from_pinned(pb, i as u16));
            }
        }
        Box::leak(Box::new(pool))
    }

    #[test]
    fn test_leader_when_no_inflight() {
        let map = CoalescingMap::new();
        let oid = ObjectId(1);

        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        let result = map.try_join_or_lead(oid, cb);

        assert!(matches!(result, CoalesceResult::Leader(_)));
        assert!(map.is_inflight(oid));
    }

    #[test]
    fn test_waiter_when_inflight_exists() {
        let map = CoalescingMap::new();
        let oid = ObjectId(2);

        // First call: becomes leader.
        let cb1: CoalescedReadCallback = Box::new(|_buf, _result| {});
        let result1 = map.try_join_or_lead(oid, cb1);
        assert!(matches!(result1, CoalesceResult::Leader(_)));

        // Second call: becomes waiter.
        let cb2: CoalescedReadCallback = Box::new(|_buf, _result| {});
        let result2 = map.try_join_or_lead(oid, cb2);
        assert!(matches!(result2, CoalesceResult::Waiter));
    }

    #[test]
    fn test_multiple_waiters() {
        let map = CoalescingMap::new();
        let oid = ObjectId(3);

        // Leader
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid, cb);

        // 10 waiters
        for _ in 0..10 {
            let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
            let result = map.try_join_or_lead(oid, cb);
            assert!(matches!(result, CoalesceResult::Waiter));
        }

        // Verify map has the entry
        assert!(map.is_inflight(oid));
    }

    #[test]
    fn test_complete_fires_all_waiters() {
        let map = CoalescingMap::new();
        let oid = ObjectId(4);
        let pool = make_pool(10, 4096);

        // Leader
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid, cb);

        // 3 waiters with counters
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = counter.clone();
            let cb: CoalescedReadCallback = Box::new(move |buf, result| {
                assert!(buf.is_some(), "waiter should receive a buffer");
                assert!(result.is_ok(), "waiter should receive Ok");
                assert_eq!(result.unwrap(), 100);
                c.fetch_add(1, Ordering::Relaxed);
                // Forget buffer to avoid Drop calling into uninitialized STORAGE.
                std::mem::forget(buf);
            });
            map.try_join_or_lead(oid, cb);
        }

        // Simulate leader completion: create a "leader buffer" with known data.
        let leader_pinned = Box::leak(Box::new(PinnedBuffer::new(4096)));
        // Write a pattern into leader buf.
        unsafe {
            std::ptr::write_bytes(leader_pinned.as_mut_ptr(), 0xAB, 100);
        }
        let leader_buf = Buffer::from_pinned(leader_pinned, 99);

        let served = map.complete(oid, &leader_buf, &Ok(100), pool);

        assert_eq!(served, 3);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert!(!map.is_inflight(oid));

        // Forget leader buf to avoid Drop into uninitialized STORAGE.
        std::mem::forget(leader_buf);
    }

    #[test]
    fn test_complete_on_error_propagates() {
        let map = CoalescingMap::new();
        let oid = ObjectId(5);
        let pool = make_pool(5, 4096);

        // Leader
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid, cb);

        // 2 waiters
        let error_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let c = error_count.clone();
            let cb: CoalescedReadCallback = Box::new(move |buf, result| {
                assert!(buf.is_none(), "waiter should NOT receive a buffer on error");
                assert!(result.is_err(), "waiter should receive Err");
                c.fetch_add(1, Ordering::Relaxed);
            });
            map.try_join_or_lead(oid, cb);
        }

        // Leader failed
        let leader_pinned = Box::leak(Box::new(PinnedBuffer::new(4096)));
        let leader_buf = Buffer::from_pinned(leader_pinned, 99);

        let served = map.complete(
            oid,
            &leader_buf,
            &Err(StorageError::IoError { code: -5 }),
            pool,
        );

        assert_eq!(served, 2);
        assert_eq!(error_count.load(Ordering::Relaxed), 2);
        assert!(!map.is_inflight(oid));

        std::mem::forget(leader_buf);
    }

    #[test]
    fn test_different_oids_independent() {
        let map = CoalescingMap::new();
        let oid1 = ObjectId(10);
        let oid2 = ObjectId(20);

        // Leader for oid1
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid1, cb);

        // Leader for oid2
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid2, cb);

        // Waiter joins oid1
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        let result = map.try_join_or_lead(oid1, cb);
        assert!(matches!(result, CoalesceResult::Waiter));

        // oid2 still independent
        assert!(map.is_inflight(oid1));
        assert!(map.is_inflight(oid2));
    }

    #[test]
    fn test_map_empty_after_complete() {
        let map = CoalescingMap::new();
        let oid = ObjectId(30);
        let pool = make_pool(5, 4096);

        // Leader, no waiters
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid, cb);
        assert!(map.is_inflight(oid));

        let leader_pinned = Box::leak(Box::new(PinnedBuffer::new(4096)));
        let leader_buf = Buffer::from_pinned(leader_pinned, 99);

        map.complete(oid, &leader_buf, &Ok(4096), pool);
        assert!(!map.is_inflight(oid));

        // New read for same OID becomes leader again.
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        let result = map.try_join_or_lead(oid, cb);
        assert!(matches!(result, CoalesceResult::Leader(_)));

        std::mem::forget(leader_buf);
    }

    #[test]
    fn test_pool_exhaustion_at_fanout() {
        let map = CoalescingMap::new();
        let oid = ObjectId(40);
        // Pool with only 1 buffer — won't have any left for waiters after leader takes it.
        let pool = make_pool(0, 4096); // empty pool!

        // Leader
        let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
        map.try_join_or_lead(oid, cb);

        // 2 waiters — will get PoolExhausted
        let exhausted_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let c = exhausted_count.clone();
            let cb: CoalescedReadCallback = Box::new(move |buf, result| {
                assert!(buf.is_none());
                match result {
                    Err(StorageError::PoolExhausted) => {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => panic!("expected PoolExhausted"),
                }
            });
            map.try_join_or_lead(oid, cb);
        }

        let leader_pinned = Box::leak(Box::new(PinnedBuffer::new(4096)));
        let leader_buf = Buffer::from_pinned(leader_pinned, 99);

        let served = map.complete(oid, &leader_buf, &Ok(4096), pool);
        assert_eq!(served, 2);
        assert_eq!(exhausted_count.load(Ordering::Relaxed), 2);

        std::mem::forget(leader_buf);
    }

    #[test]
    fn test_concurrent_leader_waiter() {
        use std::thread;

        let map = Arc::new(CoalescingMap::new());
        let oid = ObjectId(50);
        let leader_count = Arc::new(AtomicUsize::new(0));
        let waiter_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let m = map.clone();
            let lc = leader_count.clone();
            let wc = waiter_count.clone();
            handles.push(thread::spawn(move || {
                let cb: CoalescedReadCallback = Box::new(|_buf, _result| {});
                match m.try_join_or_lead(oid, cb) {
                    CoalesceResult::Leader(_) => {
                        lc.fetch_add(1, Ordering::Relaxed);
                    }
                    CoalesceResult::Waiter => {
                        wc.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(leader_count.load(Ordering::Relaxed), 1);
        assert_eq!(waiter_count.load(Ordering::Relaxed), 19);
    }
}
