//! Reclaim list: objects whose memory a background job (e.g. Dram-mode shrink)
//! already freed while their keys still exist.
//!
//! Commands check it upfront and treat a listed key as missing; the scaling
//! cron deletes the keys. An oid leaves the list when its object is removed
//! (`DRAMPool::remove_object`, reached from `lo_free` once the key is deleted).
//!
//! Lock order: `RECLAIM_LIST` before `DRAMPool.objects` when both are held.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};

use crate::data_type::ObjectId;

pub static RECLAIM_LIST: LazyLock<Mutex<HashSet<ObjectId>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub fn lock() -> MutexGuard<'static, HashSet<ObjectId>> {
    RECLAIM_LIST.lock().expect("RECLAIM_LIST lock unavailable")
}

/// Cumulative reclaims completed: oids that left the list (key deleted).
pub static RECLAIM_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn contains(oid: &ObjectId) -> bool {
    lock().contains(oid)
}

/// Take `oid` off the list, counting the completed reclaim.
pub fn remove(oid: &ObjectId) {
    if lock().remove(oid) {
        RECLAIM_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}
