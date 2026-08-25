//! Generic per-key operation coalescing (singleflight pattern).
//!
//! Deduplicates concurrent operations for the same key. When multiple callers
//! request the same operation on the same key concurrently, only the first
//! (the "leader") performs the actual work. Subsequent callers ("waiters")
//! register a callback and receive their result from the leader's completion.
//!
//! This module is operation-agnostic: it does not know about buffers, NVMe,
//! memcpy, or any specific I/O path. The caller defines:
//!   - The key type (e.g., ObjectId for reads, or any Hash+Eq type)
//!   - The waiter callback signature (what each waiter receives on completion)
//!   - The completion logic (a per-waiter callback invoked by the leader)
//!
//! Example use cases:
//!   - NVMe read coalescing: leader reads, waiters get memcpy'd buffers
//!   - Write coalescing: leader writes, waiters get confirmation
//!   - Fd pooling: leader opens fd, waiters get cloned handle
//!   - Eviction coalescing: leader evicts, waiters get ack

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;

// ─── Waiter Callback ─────────────────────────────────────────────────────────

/// Generic waiter callback. The type parameter `R` is whatever the leader
/// passes to each waiter at completion time (determined by the caller's
/// completion closure).
///
/// For read coalescing: `R = (Option<Buffer>, Result<u64, StorageError>)`
/// For write coalescing: `R = Result<(), StorageError>`
/// For eviction: `R = Result<(), StorageError>`
pub type WaiterCallback<R> = Box<dyn FnOnce(R) + Send>;

// ─── Coalesce Result ─────────────────────────────────────────────────────────

/// Result of attempting to coalesce an operation via `try_join_or_lead`.
pub enum CoalesceResult<R> {
    /// Caller is the leader — must perform the actual operation.
    /// The leader's own `WaiterCallback` is returned so the caller can
    /// invoke it after completing the work and fanning out to waiters.
    Leader(WaiterCallback<R>),
    /// Caller joined as a waiter — no work needed, callback stored in map.
    Waiter,
}

// ─── Internal State ──────────────────────────────────────────────────────────

/// State for one in-flight coalesced operation.
struct CoalescedEntry<R> {
    /// Callbacks waiting for this operation to complete (waiters only).
    /// The leader's callback is NOT stored here — it travels with the operation.
    waiters: Vec<WaiterCallback<R>>,
}

// ─── CoalescingMap ───────────────────────────────────────────────────────────

/// A generic coalescing map. Keyed by `K` (any Hash+Eq type).
/// An entry exists IFF an operation for that key is currently in-flight.
///
/// The map is agnostic to what operation is being coalesced. The caller
/// provides completion logic via a closure passed to `complete()`.
///
/// Thread safety: accessed from multiple threads (e.g., main thread for
/// `try_join_or_lead`, poller thread for `complete`). Lock hold time is
/// brief (~50-100ns for HashMap lookup + Vec push or drain).
pub struct CoalescingMap<K, R>
where
    K: Hash + Eq,
{
    map: Mutex<HashMap<K, CoalescedEntry<R>>>,
}

impl<K, R> Default for CoalescingMap<K, R>
where
    K: Hash + Eq + Copy,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, R> CoalescingMap<K, R>
where
    K: Hash + Eq + Copy,
{
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// Try to join an existing in-flight operation, or become the leader.
    ///
    /// - If `key` is already in the map: registers `cb` as a waiter, returns `Waiter`.
    /// - If `key` is not in the map: inserts a new entry (empty waiter list),
    ///   returns `Leader(cb)` so the caller can invoke it after completing the work.
    pub fn try_join_or_lead(&self, key: K, cb: WaiterCallback<R>) -> CoalesceResult<R> {
        let mut map = self.map.lock().unwrap();
        if let Some(entry) = map.get_mut(&key) {
            entry.waiters.push(cb);
            CoalesceResult::Waiter
        } else {
            map.insert(
                key,
                CoalescedEntry {
                    waiters: Vec::new(),
                },
            );
            CoalesceResult::Leader(cb)
        }
    }

    /// Called when the leader's operation completes (success or failure).
    ///
    /// Drains all waiters for this key and invokes `complete_one` for each waiter.
    /// The `complete_one` closure defines what each waiter receives — this is where
    /// operation-specific logic lives (e.g., buffer acquire + memcpy for reads,
    /// or error propagation for failures).
    ///
    /// Removes the key from the map after draining.
    ///
    /// Returns the number of waiters served (for metrics).
    pub fn complete<F>(&self, key: K, complete_one: F) -> usize
    where
        F: Fn() -> R,
    {
        let waiters = {
            let mut map = self.map.lock().unwrap();
            match map.remove(&key) {
                Some(entry) => entry.waiters,
                None => return 0,
            }
        };

        let waiter_count = waiters.len();
        for cb in waiters {
            cb(complete_one());
        }
        waiter_count
    }

    /// Check if a key currently has an in-flight operation (for testing/metrics).
    #[cfg(test)]
    pub fn is_inflight(&self, key: K) -> bool {
        self.map.lock().unwrap().contains_key(&key)
    }

    /// Remove an entry without completing it (used when leader fails before starting).
    /// Invokes `on_drop` for each waiter that was waiting, allowing the caller to
    /// notify them of the failure.
    ///
    /// Returns the number of waiters that were notified.
    pub fn remove_and_notify<F>(&self, key: K, on_drop: F) -> usize
    where
        F: Fn() -> R,
    {
        let mut map = self.map.lock().unwrap();
        match map.remove(&key) {
            Some(entry) => {
                let count = entry.waiters.len();
                for cb in entry.waiters {
                    cb(on_drop());
                }
                count
            }
            None => 0,
        }
    }

    /// Remove an entry without completing or notifying waiters.
    /// Returns the number of waiters that were dropped silently.
    /// Use `remove_and_notify` instead when waiters need error notification.
    pub fn remove_without_complete(&self, key: K) -> usize {
        let mut map = self.map.lock().unwrap();
        match map.remove(&key) {
            Some(entry) => entry.waiters.len(),
            None => 0,
        }
    }

    /// Number of keys currently in-flight (for metrics).
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

// ─── Type Aliases for Read Coalescing ────────────────────────────────────────

use crate::data_type::ObjectId;
use crate::storage::shared_buffer::SharedBuffer;
use crate::storage::StorageError;

/// The result type passed to each consumer (leader + waiters) in the read coalescing path.
///
/// - `Ok(SharedBuffer)` on success — shared read-only access to the leader's buffer.
///   All consumers receive an Arc clone (zero-copy). Buffer returns to pool when last clone drops.
/// - `Err(StorageError)` on failure — leader NVMe read failed or leader couldn't acquire a buffer.
///
/// No `Option` needed: success always provides a buffer, failure never does.
pub type ReadCoalesceResult = Result<SharedBuffer, StorageError>;

/// Convenience type alias: a CoalescingMap specialized for NVMe read deduplication.
pub type ReadCoalescingMap = CoalescingMap<ObjectId, ReadCoalesceResult>;

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::{Buffer, BufferPool};
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

    // ─── Generic CoalescingMap Tests ─────────────────────────────────────

    #[test]
    fn test_leader_when_no_inflight() {
        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(1);

        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        let result = map.try_join_or_lead(object_id, cb);

        assert!(matches!(result, CoalesceResult::Leader(_)));
        assert!(map.is_inflight(object_id));
    }

    #[test]
    fn test_waiter_when_inflight_exists() {
        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(2);

        // First call: becomes leader.
        let cb1: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        let result1 = map.try_join_or_lead(object_id, cb1);
        assert!(matches!(result1, CoalesceResult::Leader(_)));

        // Second call: becomes waiter.
        let cb2: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        let result2 = map.try_join_or_lead(object_id, cb2);
        assert!(matches!(result2, CoalesceResult::Waiter));
    }

    #[test]
    fn test_multiple_waiters() {
        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(3);

        // Leader
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id, cb);

        // 10 waiters
        for _ in 0..10 {
            let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
            let result = map.try_join_or_lead(object_id, cb);
            assert!(matches!(result, CoalesceResult::Waiter));
        }

        // Verify map has the entry
        assert!(map.is_inflight(object_id));
    }

    #[test]
    fn test_complete_fires_all_waiters() {
        use crate::storage::shared_buffer::SharedBuffer;

        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(4);
        let pool = make_pool(1, 4096);

        // Leader
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id, cb);

        // 3 waiters with counters
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = counter.clone();
            let cb: WaiterCallback<ReadCoalesceResult> = Box::new(move |result| {
                assert!(result.is_ok(), "waiter should receive Ok(SharedBuffer)");
                let shared = result.unwrap();
                assert_eq!(shared.data_len(), 100);
                assert_eq!(shared.as_slice()[0], 0xAB);
                c.fetch_add(1, Ordering::Relaxed);
                // Prevent Drop from calling into uninitialized global STORAGE.
                std::mem::forget(shared);
            });
            map.try_join_or_lead(object_id, cb);
        }

        // Simulate leader completion: create SharedBuffer from a pool buffer.
        let buf = pool.get().unwrap();
        unsafe {
            std::ptr::write_bytes(buf.ptr(), 0xAB, 100);
        }
        let shared = SharedBuffer::new(buf, 100);

        // Complete: each waiter receives an Arc clone of the shared buffer (refcount bump, no copy).
        let served = map.complete(object_id, || Ok(shared.clone()));

        assert_eq!(served, 3);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert!(!map.is_inflight(object_id));

        std::mem::forget(shared);
    }

    #[test]
    fn test_complete_on_error_propagates() {
        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(5);

        // Leader
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id, cb);

        // 2 waiters
        let error_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let c = error_count.clone();
            let cb: WaiterCallback<ReadCoalesceResult> = Box::new(move |result| {
                assert!(result.is_err(), "waiter should receive Err");
                c.fetch_add(1, Ordering::Relaxed);
            });
            map.try_join_or_lead(object_id, cb);
        }

        // Leader failed — complete with error propagation closure.
        let served = map.complete(object_id, || Err(StorageError::IoError { code: -5 }));

        assert_eq!(served, 2);
        assert_eq!(error_count.load(Ordering::Relaxed), 2);
        assert!(!map.is_inflight(object_id));
    }

    #[test]
    fn test_different_object_ids_independent() {
        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id1 = ObjectId(10);
        let object_id2 = ObjectId(20);

        // Leader for object_id1
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id1, cb);

        // Leader for object_id2
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id2, cb);

        // Waiter joins object_id1
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        let result = map.try_join_or_lead(object_id1, cb);
        assert!(matches!(result, CoalesceResult::Waiter));

        // object_id2 still independent
        assert!(map.is_inflight(object_id1));
        assert!(map.is_inflight(object_id2));
    }

    #[test]
    fn test_map_empty_after_complete() {
        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(30);

        // Leader, no waiters
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id, cb);
        assert!(map.is_inflight(object_id));

        map.complete(object_id, || Err(StorageError::PoolExhausted));
        assert!(!map.is_inflight(object_id));

        // New operation for same key becomes leader again.
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        let result = map.try_join_or_lead(object_id, cb);
        assert!(matches!(result, CoalesceResult::Leader(_)));
    }

    #[test]
    fn test_no_pool_exhaustion_at_fanout() {
        // With shared buffer (Arc), fan-out never needs per-waiter buffer allocation.
        // Even with only 1 pool buffer, all waiters succeed as long as the leader's
        // read completed — they each get an Arc clone (refcount bump, no allocation).
        use crate::storage::shared_buffer::SharedBuffer;

        let map: CoalescingMap<ObjectId, ReadCoalesceResult> = CoalescingMap::new();
        let object_id = ObjectId(40);
        let pool = make_pool(1, 4096); // Only 1 buffer — just enough for leader.

        // Leader
        let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
        map.try_join_or_lead(object_id, cb);

        // 100 waiters — all succeed because no per-waiter buffer is needed.
        let success_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..100 {
            let c = success_count.clone();
            let cb: WaiterCallback<ReadCoalesceResult> = Box::new(move |result| {
                assert!(
                    result.is_ok(),
                    "all waiters should succeed with shared buffer"
                );
                let shared = result.unwrap();
                assert_eq!(shared.data_len(), 4096);
                c.fetch_add(1, Ordering::Relaxed);
                std::mem::forget(shared);
            });
            map.try_join_or_lead(object_id, cb);
        }

        // Simulate leader completion: create SharedBuffer from the single pool buffer.
        let buf = pool.get().unwrap();
        unsafe {
            std::ptr::write_bytes(buf.ptr(), 0xFF, 4096);
        }
        let shared = SharedBuffer::new(buf, 4096);

        // Each waiter receives an Arc clone (refcount bump, no buffer allocation).
        let served = map.complete(object_id, || Ok(shared.clone()));

        assert_eq!(served, 100);
        assert_eq!(success_count.load(Ordering::Relaxed), 100);
        assert!(!map.is_inflight(object_id));

        std::mem::forget(shared);
    }

    #[test]
    fn test_concurrent_leader_waiter() {
        use std::thread;

        let map: Arc<CoalescingMap<ObjectId, ReadCoalesceResult>> = Arc::new(CoalescingMap::new());
        let object_id = ObjectId(50);
        let leader_count = Arc::new(AtomicUsize::new(0));
        let waiter_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let m = map.clone();
            let lc = leader_count.clone();
            let wc = waiter_count.clone();
            handles.push(thread::spawn(move || {
                let cb: WaiterCallback<ReadCoalesceResult> = Box::new(|_result| {});
                match m.try_join_or_lead(object_id, cb) {
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

    // ─── Generic (non-read) usage test ───────────────────────────────────

    /// Demonstrates the coalescing map used for a non-read operation (e.g., eviction).
    /// The result type is just `Result<(), String>` — no buffers involved.
    #[test]
    fn test_generic_non_read_usage() {
        let map: CoalescingMap<u64, Result<(), String>> = CoalescingMap::new();

        // Leader
        let cb: WaiterCallback<Result<(), String>> = Box::new(|_result| {});
        let result = map.try_join_or_lead(42, cb);
        assert!(matches!(result, CoalesceResult::Leader(_)));

        // 2 waiters
        let success_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let c = success_count.clone();
            let cb: WaiterCallback<Result<(), String>> = Box::new(move |result| {
                assert!(result.is_ok());
                c.fetch_add(1, Ordering::Relaxed);
            });
            map.try_join_or_lead(42, cb);
        }

        // Complete — the closure defines what waiters get (pure success, no buffers).
        let served = map.complete(42, || Ok(()));
        assert_eq!(served, 2);
        assert_eq!(success_count.load(Ordering::Relaxed), 2);
    }

    /// Test remove_and_notify: leader fails before starting, waiters get error.
    #[test]
    fn test_remove_and_notify() {
        let map: CoalescingMap<u64, Result<(), String>> = CoalescingMap::new();

        // Leader
        let cb: WaiterCallback<Result<(), String>> = Box::new(|_result| {});
        map.try_join_or_lead(99, cb);

        // 3 waiters
        let error_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = error_count.clone();
            let cb: WaiterCallback<Result<(), String>> = Box::new(move |result| {
                assert!(result.is_err());
                c.fetch_add(1, Ordering::Relaxed);
            });
            map.try_join_or_lead(99, cb);
        }

        // Leader fails — notify all waiters with error.
        let notified = map.remove_and_notify(99, || Err("leader failed".to_string()));
        assert_eq!(notified, 3);
        assert_eq!(error_count.load(Ordering::Relaxed), 3);
    }
}
