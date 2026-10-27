//! NVMePool — short-lived transient I/O buffers with async queueing.
//!
//! Thin wrapper around SegmentPool. No object map, no draining.
//! Sized at startup to ceil(disk-staging-size / segment-size) uniform segments,
//! fixed thereafter — never expanded or shrunk (unlike DRAMPool).
//! StreamingContexts allocate from here and free on request completion.
//!
//! Staging Queue:
//! A request that finds staging full waits in a FIFO queue and proceeds once
//! enough buffers free up. Overload becomes added latency instead of errors.
//! - No module timeout: the client's own timeout bounds the wait.
//! - Fairness: arrival order via tokio::sync::Mutex FIFO.
//! - Off the main thread: the wait happens in the spawned task.
//! - Bounded queue: safety net for clients with no timeout (default 1024).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::context::SegmentBuffer;
use super::segment_pool::SegmentPool;

/// Outcome of an async staging buffer allocation attempt.
pub enum AllocOutcome {
    /// Got the requested window of buffers.
    Got(Vec<SegmentBuffer>),
    /// The client disconnected while queued — nothing to reply to.
    Cancelled,
    /// Queue is at capacity — reject to apply backpressure.
    QueueFull,
}

pub struct NVMePool {
    pool: SegmentPool,
    /// Wakes the head-of-queue whenever buffers are freed.
    freed: tokio::sync::Notify,
    /// FIFO gate: tokio's Mutex grants the lock in arrival order.
    /// Only the holder can attempt alloc_window; everyone else waits.
    queue: tokio::sync::Mutex<()>,
    /// One cancel signal per client with queued request(s). A disconnect
    /// fires notify_waiters, waking all of that client's queued requests.
    cancels: Mutex<HashMap<u64, Arc<tokio::sync::Notify>>>,
    /// Current number of waiters in the queue. Also the INFO metric.
    queue_len: AtomicU64,
    /// Maximum queue depth. Rejects with QueueFull above this.
    queue_capacity: u64,
}

impl NVMePool {
    pub fn new(segment_count: usize, segment_size: usize, queue_capacity: u64) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size, super::uring::PoolType::Nvme),
            freed: tokio::sync::Notify::new(),
            queue: tokio::sync::Mutex::new(()),
            cancels: Mutex::new(HashMap::new()),
            queue_len: AtomicU64::new(0),
            queue_capacity,
        }
    }

    /// Current queue length (waiters). Used by INFO.
    pub fn queue_len(&self) -> u64 {
        self.queue_len.load(Ordering::Relaxed)
    }

    /// Free multiple buffers and notify waiting allocators.
    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers);
        self.freed.notify_waiters();
    }

    /// Async allocation with FIFO queueing and cancellation support.
    ///
    /// If staging is full, the request waits in a FIFO queue until buffers free up.
    /// A client disconnect cancels the wait. If the queue itself is full, rejects
    /// immediately with `QueueFull`.
    pub async fn alloc_window_cancellable(
        &self,
        client_id: u64,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
    ) -> AllocOutcome {
        // Step 1: Queue cap check (CAS loop for exact enforcement).
        // A simple load + fetch_add has a TOCTOU gap where up to worker_threads
        // requests can all read below-capacity and all increment, overshooting.
        // compare_exchange_weak is sufficient: spurious failures just retry the
        // loop, and it avoids the extra LL/SC retry that _strong adds on ARM.
        loop {
            let current = self.queue_len.load(Ordering::Relaxed);
            if current >= self.queue_capacity {
                return AllocOutcome::QueueFull;
            }
            if self
                .queue_len
                .compare_exchange_weak(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // Step 2: Register cancel signal for this client.
        let cancel = self.register_cancel(client_id);
        let guard = CancelGuard {
            pool: self,
            client_id,
            armed: true,
        };

        // Pre-register as a waiter so a disconnect mid-wait is not lost.
        let mut cancelled = std::pin::pin!(cancel.notified());
        cancelled.as_mut().enable();

        // Step 3: Fast path — try_lock to skip the queue if nobody is waiting.
        let lock_guard = match self.queue.try_lock() {
            Ok(lock) => {
                // Nobody waiting. Try immediate allocation.
                if let Some(buffers) = self.pool.alloc_window(size, max_buffers, min_buffers) {
                    guard.disarm();
                    return AllocOutcome::Got(buffers);
                }
                // No buffers available; we already hold the lock, proceed to alloc loop.
                lock
            }
            Err(_) => {
                // Others waiting — must queue. Wait for the FIFO lock.
                tokio::select! {
                    biased;
                    _ = &mut cancelled => {
                        // Cancelled while waiting for lock.
                        return AllocOutcome::Cancelled;
                    }
                    lock = self.queue.lock() => lock,
                }
            }
        };

        // Step 4: Alloc loop (only the lock holder runs this).
        loop {
            // Register notified BEFORE trying alloc to avoid lost wakes.
            let freed_notified = self.freed.notified();

            if let Some(buffers) = self.pool.alloc_window(size, max_buffers, min_buffers) {
                drop(lock_guard); // Release lock → next waiter proceeds.
                guard.disarm();
                return AllocOutcome::Got(buffers);
            }

            // Wait for either buffers freed or cancellation.
            tokio::select! {
                biased;
                _ = &mut cancelled => {
                    return AllocOutcome::Cancelled;
                }
                _ = freed_notified => {
                    // Buffers may have been freed; loop back and retry.
                }
            }
        }
    }

    /// Cancel all queued requests for a client (called on disconnect).
    pub fn cancel_client(&self, client_id: u64) {
        let signal = self
            .cancels
            .lock()
            .expect("cancels lock unavailable")
            .get(&client_id)
            .cloned();
        if let Some(signal) = signal {
            signal.notify_waiters();
        }
    }

    /// Get or create the cancel signal for a client.
    fn register_cancel(&self, client_id: u64) -> Arc<tokio::sync::Notify> {
        self.cancels
            .lock()
            .expect("cancels lock unavailable")
            .entry(client_id)
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// Remove the client's cancel entry if no other holders remain.
    fn unregister_cancel_if_last(&self, client_id: u64) {
        let mut cancels = self.cancels.lock().expect("cancels lock unavailable");
        if let Some(notify) = cancels.get(&client_id) {
            // strong_count includes the map entry; if it's 1, we're the last holder.
            if Arc::strong_count(notify) == 1 {
                cancels.remove(&client_id);
            }
        }
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
    }

    /// Whether the segment owning `buf` is registered in the io_uring kernel
    /// buffer table (picks fixed vs non-fixed I/O). See SegmentPool.
    pub fn is_buf_io_uring_registered(&self, buf: &SegmentBuffer) -> bool {
        self.pool.is_buf_io_uring_registered(buf)
    }

    /// Mark all current segments io_uring-registered (startup, post-register).
    pub fn mark_all_registered(&self) {
        self.pool.mark_all_registered();
    }

    /// Startup iovec snapshot for this pool's ring. See `SegmentPool::startup_iovecs`.
    pub fn startup_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.startup_iovecs()
    }

    /// See `SegmentPool::rebuild_dense_iovecs`. The NVMe pool never expands or
    /// shrinks, so its ring never actually re-registers; provided only so the poller's
    /// pool-generic rebuild path is total.
    pub fn rebuild_dense_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.rebuild_dense_iovecs()
    }

    /// See `SegmentPool::io_uring_registered_count`.
    pub fn io_uring_registered_count(&self) -> usize {
        self.pool.io_uring_registered_count()
    }

    /// See `SegmentPool::rdma_registered_count`.
    pub fn rdma_registered_count(&self) -> usize {
        self.pool.rdma_registered_count()
    }

    /// Counts of (total, draining, unused) segments; total includes draining. Used by INFO largeobj.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        self.pool.segment_counts()
    }

    /// Total free-gap count across the staging pool's segments (talc fragmentation signal).
    pub fn fragment_count(&self) -> usize {
        self.pool.fragment_count()
    }

    /// Call `f` with each segment's base pointer and size. Used for EFA registration.
    pub fn with_segment_slices<F>(&self, f: F)
    where
        F: FnMut(*const u8, usize),
    {
        self.pool.with_segment_slices(f);
    }
}

/// RAII guard that decrements queue_len and cleans up cancel registry on drop.
/// On the success path, call `disarm()` to do cleanup explicitly and suppress Drop.
struct CancelGuard<'a> {
    pool: &'a NVMePool,
    client_id: u64,
    armed: bool,
}

impl CancelGuard<'_> {
    /// Explicit cleanup on the success path. Decrements queue_len, unregisters
    /// the cancel entry, and disarms Drop so it doesn't double-decrement.
    fn disarm(mut self) {
        self.pool.queue_len.fetch_sub(1, Ordering::Relaxed);
        self.pool.unregister_cancel_if_last(self.client_id);
        self.armed = false;
    }
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.pool.queue_len.fetch_sub(1, Ordering::Relaxed);
            self.pool.unregister_cancel_if_last(self.client_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    // Note: Full integration tests with actual segment allocation require
    // the module's pool infrastructure. These unit tests verify the queue
    // mechanics in isolation.

    #[test]
    fn test_queue_capacity_enforcement() {
        // Create a pool with capacity 2.
        let pool = NVMePool::new(1, 1024 * 1024, 2);

        // Manually increment queue_len to simulate waiting requests.
        pool.queue_len.fetch_add(1, Relaxed);
        pool.queue_len.fetch_add(1, Relaxed);
        assert_eq!(pool.queue_len(), 2);

        // CAS should fail when at capacity.
        let current = pool.queue_len.load(Relaxed);
        assert!(current >= pool.queue_capacity);
    }

    #[test]
    fn test_cancel_registration() {
        let pool = NVMePool::new(1, 1024 * 1024, 100);

        // Register a cancel signal.
        let cancel1 = pool.register_cancel(42);
        let cancel2 = pool.register_cancel(42);

        // Should be the same Arc.
        assert!(Arc::ptr_eq(&cancel1, &cancel2));

        // Different client gets different signal.
        let cancel3 = pool.register_cancel(43);
        assert!(!Arc::ptr_eq(&cancel1, &cancel3));
    }

    #[test]
    fn test_cancel_client_notifies() {
        let pool = NVMePool::new(1, 1024 * 1024, 100);

        let cancel = pool.register_cancel(42);
        let notified = cancel.notified();

        // Cancel the client.
        pool.cancel_client(42);

        // The notified future should be ready (we can't easily test this
        // synchronously, but we can verify no panic).
        drop(notified);
    }
}
